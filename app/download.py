"""已提交批次 -> gzip NDJSON 的流式下载。

内存有界：逐页（键集分页，每页最多 DOWNLOAD_PAGE_SIZE 行）从主库读取，
逐行序列化为 NDJSON 并压入 gzip 压缩器，压缩块产出后立即下发；任何时刻
内存里只有一页记录、一行序列化字节和压缩器缓冲，不会攒齐整批 NDJSON、
完整压缩文件或完整解压结果，也不生成任何临时文件。

一致性：下载全程在同一个只读事务内逐页读取。WAL 快照隔离保证——只要
快照在返回 200 响应头之前建立，即使该批次随后被删除，本下载仍按原顺序
输出删除前的全部记录和完整 gzip 结束尾部；读事务不持有写锁，慢下载既
不阻塞该批次（或其他批次）的删除，也不阻塞另一批的上传提交。代价是
慢下载期间 WAL 文件不能被检查点完全回收（磁盘占用随并发写入增长，
下载结束即恢复），这是快照隔离换取不阻塞的正常代价。

SQLite 读取与 zlib 压缩都放在工作线程执行，慢下载只阻塞自己的响应协程，
不阻塞事件循环。结束、出错或客户端断连都会回滚读事务并关闭连接。
"""
from __future__ import annotations

import json
import sqlite3
import weakref
import zlib
from typing import AsyncIterator

from anyio import to_thread

from . import db

# 单次从库中取出的行数上限：单行正文上限 64KiB，
# 整页最坏约 64 * 64KiB = 4MiB，正常情况下远小于此。
DOWNLOAD_PAGE_SIZE = 64


def _release_conn(conn: sqlite3.Connection) -> None:
    """回滚只读事务并关闭连接（幂等，可在任意线程调用）。"""
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
    conn.close()


class _GzipNdjsonStreamer:
    """逐页读行、逐行序列化、逐块 gzip 压缩的小状态机（在线程中运行）。

    连接上已开启只读事务（见 open_streamer），所有分页读取都落在同一
    WAL 快照上；close() 回滚该事务并关闭连接。另经 weakref 终结器兜底：
    即使异步生成器从未被启动（响应尚未开始迭代即被取消），连接也会随
    流对象被回收而释放，不会泄漏读事务。
    """

    def __init__(self, conn: sqlite3.Connection, batch_id: str) -> None:
        self._conn = conn
        self._finalizer = weakref.finalize(self, _release_conn, conn)
        # wbits=31：gzip 封装；结束时由 flush() 写出 CRC32/ISIZE 校验尾部
        self._comp = zlib.compressobj(level=6, wbits=31)
        self._batch_id = batch_id
        self._after_seq = 0
        self._rows_done = False
        self._closed = False

    def produce(self) -> tuple[bytes, bool]:
        """读取并压缩下一页。

        返回 (压缩块, finished)：finished 为真时压缩块含 gzip 结束尾部
        （CRC32 + ISIZE），整个流到此完整结束。压缩块可能为空（zlib 内部
        缓冲攒批时无产出），此时 finished 必为假，调用方继续取下一页。
        """
        if self._rows_done:
            return self._finish(), True

        rows = db.iter_batch_lines(
            self._conn, self._batch_id, DOWNLOAD_PAGE_SIZE, self._after_seq
        )
        payload = bytearray()
        for row in rows:
            # 只输出 id、message；ensure_ascii=False 保留原文 UTF-8 字节，
            # 引号、反斜杠、换行/制表等由 json.dumps 按 JSON 规则转义，
            # compact 分隔符；每行（含末行）都以 \n 结束。
            line = json.dumps(
                {"id": row["id"], "message": row["message"]},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            payload.extend(line)
            payload.append(0x0A)
            self._after_seq = row["seq"]

        last_page = len(rows) < DOWNLOAD_PAGE_SIZE
        if last_page:
            self._rows_done = True

        chunk = self._comp.compress(bytes(payload)) if payload else b""
        if last_page:
            # 空批不可能出现：上传时空批返回 422，下载前还做了存在性预检
            return chunk + self._finish(), True
        return chunk, False

    def _finish(self) -> bytes:
        if self._closed:
            return b""
        self._closed = True
        # flush 输出 zlib 内部残留并追加 gzip CRC32/ISIZE 结束尾部
        return self._comp.flush()

    def close(self) -> None:
        """释放快照连接（正常结束、出错或客户端断连都必须调用；幂等）。"""
        self._finalizer()


def open_streamer(batch_id: str) -> _GzipNdjsonStreamer | None:
    """打开下载器并建立读快照；批次不存在（含已删除）返回 None。

    必须在返回 200 响应头之前调用（在工作线程中运行）：BEGIN 后的首个
    SELECT 即固定 WAL 快照，此后该批次即使被删除，本次下载仍能看到
    删除前的全部行；响应头一旦发出，输出完整性就有了保证。
    """
    conn = db.connect()
    try:
        conn.execute("BEGIN")
        if db.get_batch(conn, batch_id) is None:
            _release_conn(conn)
            return None
        return _GzipNdjsonStreamer(conn, batch_id)
    except Exception:
        _release_conn(conn)
        raise


async def gzip_ndjson_stream(
    streamer: _GzipNdjsonStreamer,
) -> AsyncIterator[bytes]:
    """逐块产出该批次的完整 gzip 流的异步生成器。

    无论正常结束、向客户端写入时出错（中途断连）还是协程被取消，
    finally 都会回滚读事务并关闭连接，快照与游标随之释放。
    """
    try:
        while True:
            chunk, finished = await to_thread.run_sync(streamer.produce)
            if chunk:
                yield chunk
            if finished:
                break
    finally:
        # 屏蔽取消也要完成关闭：连接释放不应因任务已取消而被吞掉
        from anyio import CancelScope

        with CancelScope(shield=True):
            await to_thread.run_sync(streamer.close)
