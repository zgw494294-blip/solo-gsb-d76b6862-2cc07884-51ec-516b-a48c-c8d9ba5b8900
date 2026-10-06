"""FastAPI 入口：gzip NDJSON 批次接收与查询。"""
from __future__ import annotations

import base64
import json
import sqlite3
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from . import db, download, ingest
from .ingest import IngestError, PayloadTooLarge

# 检索查询词长度限制（字符数，空白原样保留、不计入修剪）
QUERY_MIN_LEN = 1
QUERY_MAX_LEN = 200
# 游标版本与传输长度上限（防御性限制，超限同样判为非法游标）
CURSOR_VERSION = 1
CURSOR_MAX_LEN = 4096
# SQLite INTEGER 上限（int64）：游标中的整数超出该范围会在绑定参数时溢出
SQLITE_INT64_MAX = 9223372036854775807


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Compose 启动时即完成数据库初始化（含暂存表残留清理）
    db.init_db()
    yield


app = FastAPI(
    title="gzip NDJSON Batch API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/batches", status_code=201)
async def upload_batch(request: Request) -> dict[str, Any]:
    """上传单个 gzip 流，校验通过后整批落库；成功返回批次 ID 与行数。

    有效行在 gzip 流完整结束、CRC 正确且请求结束、整批事务提交前不可查询。
    """
    content_length = request.headers.get("content-length")
    cl = int(content_length) if content_length is not None else None

    conn = db.connect()
    try:
        try:
            batch_id, line_count = await ingest.ingest_stream(
                conn, request.stream(), cl
            )
        except PayloadTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc))
        except IngestError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        except sqlite3.Error as exc:
            raise HTTPException(status_code=500, detail=f"database error: {exc}")
        return {"batch_id": batch_id, "line_count": line_count}
    finally:
        conn.close()


@app.get("/batches")
def list_batches(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    """列出已提交批次，按提交顺序（批次 ID 序号）分页。"""
    with db.session() as conn:
        rows, total = db.list_batches(conn, limit, offset)
    return JSONResponse(
        {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [
                {
                    "batch_id": row["id"],
                    "line_count": row["line_count"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }
    )


@app.get("/batches/{batch_id}/lines")
def get_batch_lines(
    batch_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    """按批次 ID 分页读行，保持上传时的原始顺序；未知 ID 返回 404。"""
    with db.session() as conn:
        batch = db.get_batch(conn, batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        rows = db.get_lines(conn, batch_id, limit, offset)
        return JSONResponse(
            {
                "batch_id": batch_id,
                "line_count": batch["line_count"],
                "limit": limit,
                "offset": offset,
                "items": [
                    {"seq": row["seq"], "id": row["id"], "message": row["message"]}
                    for row in rows
                ],
            }
        )


@app.get("/batches/{batch_id}/download")
def download_batch(batch_id: str) -> StreamingResponse:
    """把已提交批次下载为单个完整 gzip NDJSON 流。

    未知批次必须在开始响应（发送响应头/任何响应体字节）之前返回 404：
    在同一连接里先确认批次存在，并在发出任何响应字节之前进入只读事务
    固定 WAL 快照，确认后才返回 StreamingResponse。这样一旦 200 响应头
    已发出，即使下载进行中该批次被 DELETE 永久删除，流内仍按原顺序输出
    删除前的全部记录并写出完整 gzip CRC/ISIZE 尾部；而 WAL 下读不阻塞写，
    慢下载不阻塞删除或另一批上传提交，下载结束/断连即提交事务释放快照。
    成功时 Content-Type 为 application/gzip，且不设置 Content-Encoding
    （gzip 是文件内容本身，不是传输层编码，客户端会原样保存）。
    """
    conn = db.open_download_snapshot(batch_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="batch not found")

    # batch_id 为 32 位十六进制（uuid4.hex），放入 Content-Disposition
    # 无需考虑引号或非 ASCII 转义。
    headers = {
        "Content-Disposition": (
            f'attachment; filename="batch-{batch_id}.json.gz"'
        )
    }
    return StreamingResponse(
        download.gzip_ndjson_stream(conn, batch_id),
        media_type="application/gzip",
        headers=headers,
    )


@app.delete("/batches/{batch_id}", status_code=200)
def delete_batch(batch_id: str) -> dict[str, Any]:
    """永久删除一个已提交批次：批次与其全部行在单事务内一同硬删除。

    成功返回被删批次 ID 与行数；未知或此前已删除的批次返回 404。删除是
    原子短事务：任一步失败整体回滚，批次与行全部保留。成功后该批次不再
    出现在列表、检索与统计中，按批次读行/下载返回 404；旧检索游标续页
    跳过被删记录。删除不等待进行中的慢下载（其持有删除前的只读快照），
    也不阻塞其他批次上传提交。
    """
    with db.session() as conn:
        try:
            line_count = db.delete_batch(conn, batch_id)
        except sqlite3.Error as exc:
            raise HTTPException(status_code=500, detail=f"database error: {exc}")
    if line_count is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return {"batch_id": batch_id, "line_count": line_count}


# --------------------------------------------------------------------------- #
# 批次区间重复消息
# --------------------------------------------------------------------------- #
@app.get("/logs/duplicates")
def find_duplicates(
    start_batch_id: str = Query(...),
    end_batch_id: str = Query(...),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=100),
) -> JSONResponse:
    """统计两个已提交批次（含两端，按提交顺序成闭区间）之间的重复 message。

    按 message 原文分组（保留大小写、空白与 Unicode 码点差异；不按 id
    去重，批内与跨批的每一行各计一次），只返回出现至少两次的分组。
    闭区间由两端批次的 rowid 固定，之后新提交的批次 rowid 更大，不会
    改变相同参数的结果；区间内批次若被删除，其行随之从统计中消失，
    端点批次被删除则与未知批次一样返回 404；暂存中的批次不在主库，不参与。
    聚合、计数、排序与分页全部在 SQLite 内完成，不把区间日志载入 Python。

    - 任一 batch_id 未知：404（先于区间方向判断）
    - 起点批次晚于终点批次：422
    - 排序：occurrences 降序，再按 message 的 SQLite BINARY 顺序升序
    - total 为分页前的分组总数；越界页或无重复时 items 为空列表
    """
    with db.session() as conn:
        start = db.get_batch_rowid(conn, start_batch_id)
        if start is None:
            raise HTTPException(status_code=404, detail="start batch not found")
        end = db.get_batch_rowid(conn, end_batch_id)
        if end is None:
            raise HTTPException(status_code=404, detail="end batch not found")
        if start > end:
            raise HTTPException(
                status_code=422,
                detail="start_batch_id is committed after end_batch_id",
            )
        total = db.count_duplicate_groups(conn, start, end)
        rows = db.list_duplicate_groups(conn, start, end, limit, offset)
    return JSONResponse(
        {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [
                {
                    "message": row["message"],
                    "occurrences": row["occurrences"],
                    "batch_count": row["batch_count"],
                }
                for row in rows
            ],
        }
    )


# --------------------------------------------------------------------------- #
# 日志检索：无状态游标（自包含、可重启后续用）+ 键集分页
# --------------------------------------------------------------------------- #
def _encode_cursor(query: str, boundary: int, last: tuple[int, int]) -> str:
    """把续页状态编码为无状态游标：不含任何服务端状态，重启后仍可续用。"""
    payload = json.dumps(
        {"v": CURSOR_VERSION, "q": query, "bound": boundary, "last": list(last)},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    # 去掉填充等号，游标可原样放进 URL 查询串
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_int64(value: Any) -> bool:
    """非布尔整数且落在 SQLite INTEGER 可表示的 0..2^63-1 范围内。"""
    return _is_int(value) and 0 <= value <= SQLITE_INT64_MAX


def _decode_cursor(token: str) -> tuple[str, int, tuple[int, int]]:
    """解析并校验游标；任何损坏或字段不合法都返回 422（先于执行检索）。"""
    if not token or len(token) > CURSOR_MAX_LEN:
        raise HTTPException(status_code=422, detail="invalid cursor")
    try:
        padded = token + "=" * (-len(token) % 4)
        state = json.loads(
            base64.b64decode(padded.encode("ascii"), altchars=b"-_", validate=True)
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="invalid cursor")
    if (
        not isinstance(state, dict)
        or set(state) != {"v", "q", "bound", "last"}
        # v 必须是非布尔整数 1：true、1.0、"1" 都算非法
        or not _is_int(state["v"])
        or state["v"] != CURSOR_VERSION
        or not isinstance(state["q"], str)
        or not (QUERY_MIN_LEN <= len(state["q"]) <= QUERY_MAX_LEN)
        or not _is_int64(state["bound"])
        or not isinstance(state["last"], list)
        or len(state["last"]) != 2
        or not all(_is_int64(p) for p in state["last"])
        # 续页位置不得超出首页固定的快照范围
        or state["last"][0] > state["bound"]
    ):
        raise HTTPException(status_code=422, detail="invalid cursor")
    return state["q"], state["bound"], (state["last"][0], state["last"][1])


@app.get("/logs/search")
def search_logs(
    q: str | None = Query(
        default=None, min_length=QUERY_MIN_LEN, max_length=QUERY_MAX_LEN
    ),
    page_size: int = Query(ge=1, le=100),
    cursor: str | None = Query(default=None),
) -> JSONResponse:
    """按 message 字面子串检索所有已提交批次（大小写敏感，%/_ 无通配含义）。

    首页传 q（1~200 字符，空白原样保留）与 page_size（1~100），并固定
    当时已提交的批次范围；续页只传 cursor 与 page_size，沿用首页的查询词
    与范围，翻页期间新提交的批次不会混入（提交序号 AUTOINCREMENT 单调
    递增、删除也不复用，删光全部批次后再上传同样不会落入旧游标范围）。
    范围内被永久删除的批次/行在续页时自然跳过。每页一个短查询，不缓存
    全量命中、不跨请求持有事务，WAL 下检索与上传/删除互不阻塞。
    """
    if (q is None) == (cursor is None):
        raise HTTPException(
            status_code=422, detail="provide exactly one of 'q' and 'cursor'"
        )
    if cursor is not None:
        query, boundary, last = _decode_cursor(cursor)
    else:
        # 首页：固定当前已提交批次范围（batches 最大 rowid；rowid 单调
        # 递增，删除乃至删空都不会复用）
        query, last = q, (0, 0)
        with db.session() as conn:
            boundary = db.committed_boundary(conn)

    # 多取一行判断是否还有下一页
    with db.session() as conn:
        rows = db.search_lines(conn, query, boundary, last, page_size + 1)

    items = rows[:page_size]
    next_cursor = None
    if len(rows) > page_size:
        tail = items[-1]
        next_cursor = _encode_cursor(query, boundary, (tail["browid"], tail["seq"]))
    return JSONResponse(
        {
            "items": [
                {
                    "batch_id": row["batch_id"],
                    "seq": row["seq"],
                    "id": row["id"],
                    "message": row["message"],
                }
                for row in items
            ],
            "next_cursor": next_cursor,
        }
    )
