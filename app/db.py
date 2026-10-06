"""SQLite 持久化层。

两级存储：
- 主库（batches/lines）：只存放已提交批次，所有读取都只看主库。
- 每批次一个独立的临时 SQLite 文件（stage_<id>.db）：上传期间逐行落盘暂存。
  慢上传只写自己的临时文件，不与主库或其他批次争用任何锁；待 gzip 流完整
  结束、CRC 校验通过后，才以一个短事务把临时文件 ATTACH 进主库整体导入并
  提交——提交前外部对该批次完全不可见，提交失败则回滚并删除临时文件。

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

_CREATE_BATCHES = """
CREATE TABLE IF NOT EXISTS batches (
    id         TEXT PRIMARY KEY,
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
    """初始化数据库目录、表结构，并清理上次崩溃残留的暂存文件。"""
    os.makedirs(_db_dir(), exist_ok=True)
    os.makedirs(staging_dir(), exist_ok=True)
    conn = connect()
    try:
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

    批次只增不改、rowid 即提交顺序：固定该上界后，之后提交的批次
    （rowid 更大）不会混入基于该边界的检索，新查询重新取边界即可看到。
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
