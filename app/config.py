"""以字节为单位的硬性限制（MiB = 1024*1024 字节）。"""
import os

MIB = 1024 * 1024

# gzip 压缩请求体上限：2 MiB
MAX_COMPRESSED_BYTES = 2 * MIB
# 解压后数据总上限：8 MiB
MAX_DECOMPRESSED_BYTES = 8 * MIB
# 单行（解压后的字节，不含换行符）上限：64 KiB
MAX_LINE_BYTES = 64 * 1024

# zlib 单次解压产出上限（HTTP 读块由 ASGI 服务器决定，同样有界）
ZLIB_OUTPUT_LIMIT = 64 * 1024

DB_PATH = os.environ.get("DB_PATH", "/data/batches.db")
