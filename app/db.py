"""SQLite 持久化层。

两级存储：
- 主库（batches/lines）：只存放已提交批次，所有读取都只看主库。批次可被
  永久删除（DELETE 接口）：批次与其全部行在单个事务内硬删除；提交序号
  （batches.rowid，即 rowid_seq）由 AUTOINCREMENT 分配，删除也绝不复用，
  因此旧检索游标的 rowid 范围在删除/重建批次后仍然成立。
- 每批次一个独立的临时 SQLite 文件（stage_<id>.db）：上传期间逐行落盘暂存。
  慢上传只写自己的临时文件，不与主库或其他批次争用任何锁；待 gzip 流完整
  结束、CRC 校验通过后，才以一个短事务把临时文件 ATTACH 进主库整体导入并
  提交——提交前外部对该批次完全不可见，提交失败则回滚并删除临时文件。

下载在开始响应（发出 200 响应头）前即进入一个显式只读事务固定 WAL 快照：
随后删除该批次不会打断已在进行的下载（快照里仍是删除前的完整内容），而
WAL 下读事务不阻塞写，删除也不会被慢下载阻塞；下载结束（含断连）即提交/
回滚该事务并关闭连接，持有的快照随之释放。

连接使用 autocommit（isolation_level=None），事务边界显式控制。
"""
from __future__ import annotations

import glob
import os
import sqlite3
import uuid
from contextlib import contextmanager
from typing import Iterator

from . import config

# batches 的建表语句。rowid_seq 即显式 rowid：AUTOINCREMENT 保证提交序号
# 单调递增、删除（乃至删空）后也不复用旧序号——检索游标以该序号固定快照
# 范围，新批次因此永远不会落入删除前签发的旧游标范围。
_CREATE_BATCHES = """
CREATE TABLE IF NOT EXISTS batches (
    rowid_seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    id         TEXT NOT NULL UNIQUE,
    line_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
)
"""
_CREATE_LINES = """
CREATE TABLE IF NOT EXISTS lines (
    batch_id TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    id       TEXT NOT NULL,
    message  TEXT NOT NULL,
    PRIMARY KEY (batch_id, seq)
)
"""
_CREATE_LINE_BATCH_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_lines_batch ON lines(batch_id, seq)"
)


def _db_dir() -> str:
    directory = os.path.dirname(config.DB_PATH)
    return directory or "."


def staging_dir() -> str:
    return os.path.join(_db_dir(), "staging")


# --------------------------------------------------------------------------- #
# 主库
# --------------------------------------------------------------------------- #
def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH, timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db() -> None:
    """初始化数据库目录、表结构，升级旧库，并清理残留暂存文件。

    旧版本的 batches 表以 id 为主键、没有 AUTOINCREMENT；检测到这种结构时
    原地迁移：保留每个批次的原始 rowid（即检索游标里使用的提交序号）与
    lines 表不动，因此升级后旧数据与已签发的合法旧游标仍然有效。迁移是
    单连接上的 DDL 事务（SQLite DDL 可事务化），失败则结构原样保留。
    """
    os.makedirs(_db_dir(), exist_ok=True)
    os.makedirs(staging_dir(), exist_ok=True)
    conn = connect()
    try:
        if _batches_needs_migration(conn):
            _migrate_batches(conn)
        conn.execute(_CREATE_BATCHES)
        conn.execute(_CREATE_LINES)
        conn.execute(_CREATE_LINE_BATCH_INDEX)
    finally:
        conn.close()
    for path in glob.glob(os.path.join(staging_dir(), "stage_*.db*")):
        try:
            os.remove(path)
        except OSError:
            pass


def _batches_needs_migration(conn: sqlite3.Connection) -> bool:
    """旧版 batches（id 为 PRIMARY KEY、rowid 无 AUTOINCREMENT）需迁移。"""
    schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='batches'"
    ).fetchone()
    if schema is None or not schema[0]:
        return False
    return "AUTOINCREMENT" not in schema[0].upper()


def _migrate_batches(conn: sqlite3.Connection) -> None:
    """把旧 batches 表迁到 AUTOINCREMENT 结构，逐行保留原 rowid。

    显式插入旧 rowid 后，sqlite_sequence 会记下最大序号，之后新批次的序号
    从该最大值继续分配；lines 通过批次 id 关联，内容无需改动。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("ALTER TABLE batches RENAME TO batches_old")
        conn.execute(_CREATE_BATCHES)
        conn.execute(
            """
            INSERT INTO batches (rowid_seq, id, line_count, created_at)
            SELECT rowid, id, line_count, created_at FROM batches_old
            """
        )
        conn.execute("DROP TABLE batches_old")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def list_batches(
    conn: sqlite3.Connection, limit: int, offset: int
) -> tuple[list[sqlite3.Row], int]:
    total = conn.execute("SELECT count(*) AS c FROM batches").fetchone()["c"]
    rows = conn.execute(
        """
        SELECT id, line_count, created_at
        FROM batches
        ORDER BY rowid
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    ).fetchall()
    return rows, total


def get_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, line_count, created_at FROM batches WHERE id = ?",
        (batch_id,),
    ).fetchone()


def get_batch_rowid(conn: sqlite3.Connection, batch_id: str) -> int | None:
    """批次按提交顺序的序号（batches.rowid）；未知 ID 返回 None。"""
    row = conn.execute(
        "SELECT rowid AS r FROM batches WHERE id = ?",
        (batch_id,),
    ).fetchone()
    return row["r"] if row is not None else None


def delete_batch(conn: sqlite3.Connection, batch_id: str) -> int | None:
    """永久删除批次及其全部行，返回被删行数；未知/已删除批次返回 None。

    批次行与批次记录在同一个 IMMEDIATE 事务内删除：任何一步失败都整体
    回滚，数据全部保留。WAL 下删除是一个短写事务，不等待慢下载的读事务，
    也不阻塞其他批次上传提交。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT line_count FROM batches WHERE id = ?",
            (batch_id,),
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return None
        line_count = row["line_count"]
        conn.execute("DELETE FROM lines WHERE batch_id = ?", (batch_id,))
        conn.execute("DELETE FROM batches WHERE id = ?", (batch_id,))
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
    return line_count


def open_download_snapshot(batch_id: str) -> sqlite3.Connection:
    """下载用：确认批次存在并立即进入只读事务固定删除前快照。

    必须在发出任何响应字节（200 响应头）之前调用：事务一旦开始，WAL 的
    读快照即固定，之后该批次被 DELETE 删除，本连接仍按删除前的完整内容
    逐页读取，gzip CRC/ISIZE 尾部照常生成。未知/已删除批次返回 None（此时
    尚未开始任何事务，连接直接关闭，由路由返回 404）。

    返回的连接由下载流持有：正常结束时 COMMIT、异常或客户端断连时
    ROLLBACK/关闭，快照随即释放。
    """
    conn = connect()
    try:
        conn.execute("BEGIN")
        if conn.execute(
            "SELECT 1 FROM batches WHERE id = ?", (batch_id,)
        ).fetchone() is None:
            conn.execute("ROLLBACK")
            conn.close()
            return None
    except Exception:
        conn.close()
        raise
    return conn


def get_lines(
    conn: sqlite3.Connection, batch_id: str, limit: int, offset: int
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT seq, id, message
        FROM lines
        WHERE batch_id = ?
        ORDER BY seq
        LIMIT ? OFFSET ?
        """,
        (batch_id, limit, offset),
    ).fetchall()


def iter_batch_lines(
    conn: sqlite3.Connection, batch_id: str, page_size: int, after_seq: int
) -> list[sqlite3.Row]:
    """键集分页取一批行：seq > after_seq 的至多 page_size 行，按 seq 升序。

    每次调用是独立短查询（WAL 下只瞬间持有读锁），不缓存整批、不跨页持有
    事务；调用方用完一页再取一页，下载因此与其他批次的上传提交并发进行。
    """
    return conn.execute(
        """
        SELECT seq, id, message
        FROM lines
        WHERE batch_id = ? AND seq > ?
        ORDER BY seq
        LIMIT ?
        """,
        (batch_id, after_seq, page_size),
    ).fetchall()


def committed_boundary(conn: sqlite3.Connection) -> int:
    """当前已提交批次范围的上界（batches 的最大 rowid，无批次时为 0）。

    rowid 由 AUTOINCREMENT 分配：提交顺序单调递增，删除（乃至删空全部
    批次）后也不复用旧序号。固定该上界后，之后提交的批次（rowid 更大）
    不会混入基于该边界的检索，新查询重新取边界即可看到；删除只让范围
    内的部分批次消失，旧游标续页时自然跳过它们。
    """
    return conn.execute(
        "SELECT COALESCE(MAX(rowid), 0) AS m FROM batches"
    ).fetchone()["m"]


def search_lines(
    conn: sqlite3.Connection,
    query: str,
    boundary: int,
    last: tuple[int, int],
    limit: int,
) -> list[sqlite3.Row]:
    """在 rowid <= boundary 的已提交批次内按字面子串匹配 message。

    instr() 是大小写敏感的字面子串匹配，% 与 _ 没有通配含义。
    结果按批次提交顺序（batches.rowid）、批内 seq 升序；last 为上一页
    最后一行的 (批次 rowid, seq)，键集分页每次只取其后最多 limit 行，
    不物化全部命中。CROSS JOIN 固定先按 rowid 顺序扫批次、再沿
    (batch_id, seq) 主键顺序扫行，ORDER BY 无需排序且 LIMIT 可提前终止。
    """
    return conn.execute(
        """
        SELECT l.batch_id AS batch_id, l.seq AS seq, l.id AS id,
               l.message AS message, b.rowid AS browid
        FROM batches b CROSS JOIN lines l ON l.batch_id = b.id
        WHERE b.rowid <= :boundary
          AND b.rowid >= :last_b
          AND (b.rowid > :last_b OR l.seq > :last_seq)
          AND instr(l.message, :query) > 0
        ORDER BY b.rowid, l.seq
        LIMIT :limit
        """,
        {
            "boundary": boundary,
            "last_b": last[0],
            "last_seq": last[1],
            "query": query,
            "limit": limit,
        },
    ).fetchall()


def _duplicate_groups_sql() -> str:
    """闭区间 [start_rowid, end_rowid] 内重复 message 分组的公共查询体。

    按 message 原文（列默认 COLLATE BINARY，保留大小写、空白与 Unicode
    码点差异）分组，不看 id：批内重复与跨批重复的每一行都计入 COUNT(*)。
    分组、聚合计数与排序全部在 SQLite 内完成，LIMIT/OFFSET 只取当前页，
    区间内的日志行不会整体进入 Python 内存。
    """
    return """
        SELECT l.message AS message,
               COUNT(*) AS occurrences,
               COUNT(DISTINCT b.rowid) AS batch_count
        FROM batches b CROSS JOIN lines l ON l.batch_id = b.id
        WHERE b.rowid BETWEEN :start AND :end
        GROUP BY l.message
        HAVING COUNT(*) >= 2
        ORDER BY occurrences DESC, message COLLATE BINARY ASC
        """


def count_duplicate_groups(
    conn: sqlite3.Connection, start_rowid: int, end_rowid: int
) -> int:
    """分页前的重复分组总数（与 limit/offset 无关，越界页也是同一总数）。"""
    return conn.execute(
        "SELECT COUNT(*) AS c FROM (" + _duplicate_groups_sql() + ")",
        {"start": start_rowid, "end": end_rowid},
    ).fetchone()["c"]


def list_duplicate_groups(
    conn: sqlite3.Connection,
    start_rowid: int,
    end_rowid: int,
    limit: int,
    offset: int,
) -> list[sqlite3.Row]:
    """取重复分组的一页（occurrences 降序，message BINARY 升序）。"""
    return conn.execute(
        _duplicate_groups_sql() + " LIMIT :limit OFFSET :offset",
        {
            "start": start_rowid,
            "end": end_rowid,
            "limit": limit,
            "offset": offset,
        },
    ).fetchall()


@contextmanager
def session() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 批次级临时暂存库（每次上传一个独立文件，互不干扰）
# --------------------------------------------------------------------------- #
class StagingStore:
    """上传期间的暂存库；整个上传在单事务内累积，丢弃/发布时才结束。"""

    def __init__(self, batch_id: str) -> None:
        self.batch_id = batch_id
        self.path = os.path.join(staging_dir(), f"stage_{batch_id}.db")
        self.conn = sqlite3.connect(
            self.path,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        # 临时文件无需崩溃持久性，关掉同步以获得单事务的高速写入
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = MEMORY")
        self.conn.execute(
            """
            CREATE TABLE lines (
                seq     INTEGER PRIMARY KEY,
                id      TEXT NOT NULL UNIQUE,
                message TEXT NOT NULL
            )
            """
        )
        self.conn.execute("BEGIN")
        self._count = 0

    def add_rows(self, rows: list[tuple[int, str, str]]) -> None:
        """写入一批暂存行；批内 id 重复抛 sqlite3.IntegrityError。"""
        self.conn.executemany(
            "INSERT INTO lines (seq, id, message) VALUES (?, ?, ?)", rows
        )
        self._count += len(rows)

    @property
    def count(self) -> int:
        return self._count

    def publish(
        self, main: sqlite3.Connection, created_at: str
    ) -> tuple[str, int]:
        """把暂存内容以单事务原子导入主库，成功后删除暂存文件。

        返回 (批次 ID, 行数)。任何错误都回滚，主库里绝不会出现半成品批次。
        """
        batch_id = self.batch_id
        expected = self._count
        # ATTACH 必须在事务外
        main.execute("ATTACH DATABASE ? AS stage", (self.path,))
        try:
            main.execute("BEGIN IMMEDIATE")
            try:
                count = main.execute(
                    "SELECT count(*) AS c FROM stage.lines"
                ).fetchone()["c"]
                if count != expected or count == 0:
                    raise RuntimeError("staging row count mismatch")
                main.execute(
                    "INSERT INTO batches (id, line_count, created_at)"
                    " VALUES (?, ?, ?)",
                    (batch_id, count, created_at),
                )
                main.execute(
                    """
                    INSERT INTO lines (batch_id, seq, id, message)
                    SELECT ?, seq, id, message FROM stage.lines
                    """,
                    (batch_id,),
                )
                main.execute("COMMIT")
            except Exception:
                main.execute("ROLLBACK")
                raise
        finally:
            try:
                main.execute("DETACH DATABASE stage")
            except sqlite3.Error:
                pass
        self.destroy()
        return batch_id, expected

    def destroy(self) -> None:
        """丢弃暂存：回滚事务、关闭连接并删除临时文件。"""
        try:
            self.conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        self.conn.close()
        for path in glob.glob(self.path + "*"):
            try:
                os.remove(path)
            except OSError:
                pass


def new_batch_id() -> str:
    return uuid.uuid4().hex
