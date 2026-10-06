"""流式 gzip -> NDJSON 解析器。

内存有界：任何时刻只保留当前压缩块（<=64KiB）、zlib 输出缓冲（<=64KiB）、
一条未满行（<=64KiB）以及一小批待落盘记录（累积到约 256KiB 即刷入该批次
独立的暂存 SQLite 文件），绝不缓存完整请求或完整解压结果。

异常约定：
- PayloadTooLarge -> HTTP 413（字节量超限时立即停止读取）
- IngestError      -> HTTP 422（gzip 损坏、尾随数据、非法记录等）
"""
from __future__ import annotations

import json
import sqlite3
import zlib
from datetime import datetime, timezone
from typing import AsyncIterator

from . import config, db

# 暂存待写记录的内存缓冲上限（约 256KiB 字节），达到即刷入暂存库
PENDING_BYTES_FLUSH = 256 * 1024


class PayloadTooLarge(Exception):
    """压缩/解压总量或单行字节超限。"""


class IngestError(Exception):
    """gzip 流或 NDJSON 记录非法。"""


def _duplicate_key_hook(pairs):
    """object_pairs_hook：重复键直接视为非法记录。"""
    keys = set()
    for key, _ in pairs:
        if key in keys:
            raise IngestError(f"duplicate JSON key: {key}")
        keys.add(key)
    return dict(pairs)


class IngestSession:
    """单次上传的解析状态；每个请求一个实例，独占一个暂存库文件。"""

    def __init__(self, batch_id: str) -> None:
        self.batch_id = batch_id
        self._store = db.StagingStore(batch_id)

        # wbits=31：gzip 封装；自动校验 CRC32 / ISIZE
        self._decomp = zlib.decompressobj(wbits=31)

        self._compressed_total = 0
        self._decompressed_total = 0
        self._buf = bytearray()       # 跨块的半行（原始字节）
        self._seq = 0
        self._pending: list[tuple[int, str, str]] = []
        self._pending_bytes = 0
        self.finished = False

    # ------------------------------------------------------------------ #
    # 压缩数据入口
    # ------------------------------------------------------------------ #
    def feed(self, compressed: bytes) -> None:
        """喂入一块 gzip 字节；非法/超限时抛异常，调用方必须随即停止读取。"""
        if self._decomp.eof:
            # gzip 成员已结束后又出现任何压缩侧字节，即尾随数据
            raise IngestError("trailing data after gzip stream")

        self._compressed_total += len(compressed)
        if self._compressed_total > config.MAX_COMPRESSED_BYTES:
            raise PayloadTooLarge(
                f"compressed payload exceeds {config.MAX_COMPRESSED_BYTES} bytes"
            )

        data = compressed
        while data:
            try:
                out = self._decomp.decompress(
                    data, config.ZLIB_OUTPUT_LIMIT
                )
            except zlib.error as exc:
                raise IngestError(f"corrupt gzip stream: {exc}") from exc

            # 输出受 max_length 限制时，未消费输入留在 unconsumed_tail
            data = self._decomp.unconsumed_tail
            self._account_and_absorb(out)

            if self._decomp.eof:
                # gzip 成员结束后同块（或随后）出现的任何字节都是尾随数据
                if self._decomp.unused_data:
                    raise IngestError("trailing data after gzip stream")
                break

    def _account_and_absorb(self, out: bytes) -> None:
        self._decompressed_total += len(out)
        if self._decompressed_total > config.MAX_DECOMPRESSED_BYTES:
            raise PayloadTooLarge(
                f"decompressed payload exceeds {config.MAX_DECOMPRESSED_BYTES} bytes"
            )
        if out:
            self._absorb(out)

    # ------------------------------------------------------------------ #
    # NDJSON 行切分与校验
    # ------------------------------------------------------------------ #
    def _absorb(self, data: bytes) -> None:
        self._buf.extend(data)
        start = 0
        while True:
            nl = self._buf.find(b"\n", start)
            if nl == -1:
                # 留在缓冲里的是最后一条不完整行
                tail = bytes(self._buf[start:])
                self._buf = bytearray(tail)
                if len(self._buf) > config.MAX_LINE_BYTES:
                    raise PayloadTooLarge(
                        f"line exceeds {config.MAX_LINE_BYTES} bytes"
                    )
                return
            line = bytes(self._buf[start:nl])
            start = nl + 1
            self._handle_line(line)

    def _handle_line(self, raw_line: bytes) -> None:
        if len(raw_line) > config.MAX_LINE_BYTES:
            raise PayloadTooLarge(
                f"line exceeds {config.MAX_LINE_BYTES} bytes"
            )
        if len(raw_line) == 0:
            raise IngestError("empty line is not allowed")
        try:
            text = raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IngestError(f"line is not valid UTF-8: {exc}") from exc
        try:
            record = json.loads(text, object_pairs_hook=_duplicate_key_hook)
        except json.JSONDecodeError as exc:
            raise IngestError(f"invalid JSON on line {self._seq + 1}: {exc}") from exc

        if not isinstance(record, dict) or set(record.keys()) != {"id", "message"}:
            raise IngestError(
                f"line {self._seq + 1} must contain exactly keys 'id' and 'message'"
            )
        rec_id = record["id"]
        message = record["message"]
        if (
            not isinstance(rec_id, str)
            or not isinstance(message, str)
            or rec_id == ""
            or message == ""
        ):
            raise IngestError(
                f"line {self._seq + 1}: 'id' and 'message' must be non-empty strings"
            )

        self._seq += 1
        self._pending.append((self._seq, rec_id, message))
        self._pending_bytes += len(raw_line)
        if self._pending_bytes >= PENDING_BYTES_FLUSH:
            self._flush()

    def _flush(self) -> None:
        if not self._pending:
            return
        try:
            self._store.add_rows(self._pending)
        except sqlite3.IntegrityError as exc:
            raise IngestError(f"duplicate id within batch: {exc}") from exc
        self._pending.clear()
        self._pending_bytes = 0

    # ------------------------------------------------------------------ #
    # 结束：CRC 校验 + 末行 + 整批发布
    # ------------------------------------------------------------------ #
    def finish(self, main_conn: sqlite3.Connection) -> tuple[str, int]:
        """请求体读完后调用：校验 gzip 完整性并整批落库，返回 (ID, 行数)。"""
        if self.finished:
            return self.batch_id, self._store.count

        # 最后的压缩块可能在 feed() 中就已使解压器到达 EOF
        if not self._decomp.eof:
            # flush 取出 zlib 内部尚未输出的少量字节，并触发尾部 CRC 校验
            flushed = 0
            while True:
                try:
                    piece = self._decomp.flush(config.ZLIB_OUTPUT_LIMIT)
                except zlib.error as exc:
                    raise IngestError(f"corrupt gzip stream: {exc}") from exc
                if piece:
                    flushed += len(piece)
                    self._account_and_absorb(piece)
                if self._decomp.eof or not piece:
                    break
                if flushed > config.MAX_DECOMPRESSED_BYTES:
                    raise PayloadTooLarge("decompressed payload exceeds limit")

        if not self._decomp.eof:
            # 流被截断（缺 gzip 尾部、CRC 无法校验等）
            raise IngestError("unexpected end of gzip stream")
        if self._decomp.unused_data:
            raise IngestError("trailing data after gzip stream")

        # 末行可以没有换行；没有尾随换行的缓冲即最后一行
        if self._buf:
            self._handle_line(bytes(self._buf))
            self._buf.clear()

        self._flush()
        if self._store.count == 0:
            raise IngestError("batch contains no records")

        created_at = datetime.now(timezone.utc).isoformat()
        # 提交暂存库事务，使其内容可被主库 ATTACH 读取
        self._store.conn.commit()
        try:
            result = self._store.publish(main_conn, created_at)
        except sqlite3.IntegrityError as exc:  # 理论上不会：批次 ID 为随机 UUID
            raise IngestError(f"failed to publish batch: {exc}") from exc
        self.finished = True
        return result

    def discard(self) -> None:
        """任何失败路径下丢弃暂存，保证不留可查询批次。"""
        self._store.destroy()

    @property
    def line_count(self) -> int:
        return self._store.count


async def ingest_stream(
    main_conn: sqlite3.Connection,
    stream: AsyncIterator[bytes],
    content_length: int | None,
) -> tuple[str, int]:
    """逐块读取请求流并解析，最终返回 (批次 ID, 行数)。

    HTTP 读取留在事件循环；gzip 解压与暂存写入放入线程执行。慢上传只写
    自己独立的暂存文件，不占用主库写锁，因此不会阻塞其他批次提交或已
    完成批次的读取。整个过程不缓存完整请求体或完整解压结果。
    """
    from anyio import to_thread

    if (
        content_length is not None
        and content_length > config.MAX_COMPRESSED_BYTES
    ):
        raise PayloadTooLarge("content-length exceeds compressed body limit")

    batch_id = db.new_batch_id()
    session = await to_thread.run_sync(IngestSession, batch_id)
    committed = False
    try:
        async for chunk in stream:
            if chunk:
                await to_thread.run_sync(session.feed, chunk)
        result = await to_thread.run_sync(session.finish, main_conn)
        committed = True
        return result
    except BaseException:
        # 接收中断（含客户端断连导致的取消）/ 校验失败 / 入库失败：
        # 在屏蔽取消的作用域内丢弃暂存，确保不留可查询批次。
        # 若发布事务已经成功提交则不再触碰（批次合法，仅响应无法送达）。
        if not committed:
            from anyio import CancelScope

            with CancelScope(shield=True):
                await to_thread.run_sync(session.discard)
        raise
