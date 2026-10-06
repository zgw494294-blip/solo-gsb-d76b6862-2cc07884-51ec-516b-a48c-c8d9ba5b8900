"""已提交批次 -> gzip NDJSON 的流式下载。

内存有界：逐页（键集分页，每页最多 DOWNLOAD_PAGE_SIZE 行）从主库读取，
逐行序列化为 NDJSON 并压入 gzip 压缩器，压缩块产出后立即下发；任何时刻
内存里只有一页记录、一行序列化字节和压缩器缓冲，不会攒齐整批 NDJSON、
完整压缩文件或完整解压结果，也不生成任何临时文件。

下载只跑独立短查询（WAL 下读不阻塞写），SQLite 读取与 zlib 压缩都放在
工作线程执行，慢下载只阻塞自己的响应协程，不阻塞事件循环，因此不阻塞
另一批上传提交，也不阻塞已完成批次的读取。批次只增不改，下载全程只读。
"""
from __future__ import annotations

import json
import zlib
from typing import AsyncIterator

from anyio import to_thread

from . import db

# 单次从库中取出的行数上限：单行正文上限 64KiB，
# 整页最坏约 64 * 64KiB = 4MiB，正常情况下远小于此。
DOWNLOAD_PAGE_SIZE = 64


class _GzipNdjsonStreamer:
    """逐页读行、逐行序列化、逐块 gzip 压缩的小状态机（在线程中运行）。"""

    def __init__(self, batch_id: str) -> None:
        self._conn = db.connect()
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
            # 空批不可能出现：上传时空批返回 422，下载前还做了 404 预检
            return chunk + self._finish(), True
        return chunk, False

    def _finish(self) -> bytes:
        if self._closed:
            return b""
        self._closed = True
        # flush 输出 zlib 内部残留并追加 gzip CRC32/ISIZE 结束尾部
        return self._comp.flush()

    def close(self) -> None:
        """释放游标与数据库连接（正常结束、出错或客户端断连都必须调用）。"""
        self._conn.close()


async def gzip_ndjson_stream(batch_id: str) -> AsyncIterator[bytes]:
    """逐块产出该批次的完整 gzip 流的异步生成器。

    连接在生成器创建时打开；无论正常结束、向客户端写入时出错（中途断连）
    还是协程被取消，finally 都会关闭连接，游标随连接一并释放。
    """
    streamer = await to_thread.run_sync(_GzipNdjsonStreamer, batch_id)
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
