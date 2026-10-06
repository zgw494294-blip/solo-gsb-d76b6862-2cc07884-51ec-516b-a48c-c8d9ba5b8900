"""端到端检查：需要一个已启动的 uvicorn（见下方 BASE）。"""
import base64
import gzip
import io
import json
import os
import sqlite3
import threading
import time
import uuid

import httpx

BASE = os.environ.get("BASE", "http://127.0.0.1:8011")
DB_PATH = os.environ.get("CHECK_DB_PATH", "/tmp/ndjson_check/batches.db")

passed = 0
failed = []


def check(name, cond, extra=""):
    global passed
    if cond:
        passed += 1
        print(f"  PASS {name}")
    else:
        failed.append(name)
        print(f"  FAIL {name} {extra}")


def gz(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


def ndjson(records, trailing_newline=True, raw_last: bytes | None = None) -> bytes:
    buf = b"".join(
        (json.dumps(r, ensure_ascii=False).encode() + b"\n") for r in records
    )
    if not trailing_newline:
        buf = buf[:-1]
    if raw_last is not None:
        buf += raw_last
    return buf


def list_batch_ids():
    r = httpx.get(f"{BASE}/batches", params={"limit": 1000})
    r.raise_for_status()
    return {b["batch_id"] for b in r.json()["items"]}


def main():
    c = httpx.Client(base_url=BASE, timeout=30)
    # 每次运行独立的检索标记，避免与历史运行留在库里的数据互相干扰
    tok = uuid.uuid4().hex[:10]
    mark = f"slowmark{uuid.uuid4().hex[:10]}"

    print("== 正常路径 ==")
    before = list_batch_ids()
    recs = [
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "世界"},
        {"id": "c", "message": "x" * 70000},
    ]
    # 第三行正文超过 64KiB？message 70000 字节的 ASCII，整行 > 64KiB，应 413
    r = c.post("/batches", content=gz(ndjson(recs)))
    check("line >64KiB -> 413", r.status_code == 413, r.text)
    check("failed upload not listed", list_batch_ids() == before)

    recs = [
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "世界"},
        {"id": "c", "message": "ok"},
    ]
    r = c.post("/batches", content=gz(ndjson(recs, trailing_newline=False)))
    check("normal upload no trailing newline -> 201", r.status_code == 201, r.text)
    bid = r.json()["batch_id"]
    check("line_count", r.json()["line_count"] == 3)

    r = c.get(f"{BASE}/batches/{bid}/lines")
    check("read lines 200", r.status_code == 200)
    items = r.json()["items"]
    check(
        "order & content preserved",
        [(i["seq"], i["id"], i["message"]) for i in items]
        == [(1, "a", "hello"), (2, "b", "世界"), (3, "c", "ok")],
        items,
    )
    r = c.get(f"{BASE}/batches/{bid}/lines", params={"limit": 2, "offset": 1})
    check("pagination", [i["id"] for i in r.json()["items"]] == ["b", "c"])
    check("pagination total", r.json()["line_count"] == 3)

    r = c.get(f"{BASE}/batches/deadbeef/lines")
    check("unknown id -> 404", r.status_code == 404, r.status_code)

    r = c.get(f"{BASE}/batches")
    body = r.json()
    check("list has new batch", bid in {b["batch_id"] for b in body["items"]})
    check("list fields", all({"batch_id", "line_count", "created_at"} <= set(b)
                             for b in body["items"]))
    r = c.get(f"{BASE}/batches", params={"limit": 1})
    check("list pagination", len(r.json()["items"]) == 1)

    print("== 422 类 ==")
    def expect422(name, payload, headers=None):
        try:
            r = c.post("/batches", content=payload, headers=headers or {})
            check(name, r.status_code == 422, f"got {r.status_code} {r.text[:200]}")
        except httpx.HTTPError as e:
            check(name, False, f"client error {e}")

    expect422("empty body -> 422", b"")
    expect422("gzip of empty -> 422", gz(b""))
    expect422("corrupt gzip -> 422", b"\x1f\x8b\x08not really gzip")
    good = gz(ndjson([{"id": "a", "message": "m"}]))
    expect422("trailing data -> 422", good + b"X")
    expect422("truncated gzip -> 422", good[:-8])
    # 故意改坏 CRC（gzip 尾部倒数第 8 字节起）
    bad_crc = bytearray(good)
    bad_crc[-8] ^= 0xFF
    expect422("bad CRC -> 422", bytes(bad_crc))
    expect422("empty line -> 422", gz(b'{"id":"a","message":"m"}\n\n'))
    expect422("leading empty line -> 422", gz(b'\n{"id":"a","message":"m"}\n'))
    expect422("invalid JSON -> 422", gz(b'{not json\n'))
    expect422("invalid UTF-8 -> 422", gz(b'{"id":"a","message":"\xff"}\n'))
    expect422("missing key -> 422", gz(b'{"id":"a"}\n'))
    expect422("extra key -> 422", gz(b'{"id":"a","message":"m","x":1}\n'))
    expect422("empty id -> 422", gz(b'{"id":"","message":"m"}\n'))
    expect422("empty message -> 422", gz(b'{"id":"a","message":""}\n'))
    expect422("non-string id -> 422", gz(b'{"id":1,"message":"m"}\n'))
    expect422("null message -> 422", gz(b'{"id":"a","message":null}\n'))
    expect422("duplicate id in batch -> 422",
              gz(b'{"id":"a","message":"1"}\n{"id":"a","message":"2"}\n'))
    expect422("json number top-level -> 422", gz(b'123\n'))
    expect422("duplicate json key -> 422",
              gz(b'{"id":"a","id":"b","message":"m"}\n'))
    # 第二条记录坏：第一条不得变可见
    expect422("bad 2nd record -> 422",
              gz(b'{"id":"z1","message":"m"}\n{"id":"z2","message":}\n'))
    check("no zombie batch from failures", list_batch_ids() == before | {bid})

    print("== 413 类 ==")
    # 解压后 >8MiB（高压缩比，压缩体远小于 2MiB）；id 必须唯一
    big_lines = b"".join(
        b'{"id":"%07d","message":"aaaaaaaa"}\n' % i for i in range(290_000)
    )
    check("fixture decompressed size > 8MiB",
          len(big_lines) > 8 * 1024 * 1024, str(len(big_lines)))
    check("fixture compressed size < 2MiB",
          len(gz(big_lines)) <= 2 * 1024 * 1024, str(len(gz(big_lines))))
    r = c.post("/batches", content=gz(big_lines))
    check("decompressed >8MiB -> 413", r.status_code == 413, r.text[:120])

    # 边界：单行恰好 64KiB（整行字节）合法；用紧凑分隔符精确构造
    skeleton = json.dumps(
        {"id": "boundary", "message": ""}, separators=(",", ":")).encode()
    msg = "y" * (64 * 1024 - len(skeleton))
    raw = json.dumps(
        {"id": "boundary", "message": msg}, separators=(",", ":")).encode()
    check("fixture line exactly 64KiB", len(raw) == 64 * 1024, str(len(raw)))
    r = c.post("/batches", content=gz(raw))
    check("line exactly 64KiB -> 201", r.status_code == 201, r.text[:150])
    if r.status_code == 201:
        boundary_bid = r.json()["batch_id"]
    else:
        boundary_bid = None
    committed_so_far = before | {bid}
    if boundary_bid:
        committed_so_far |= {boundary_bid}

    # 单行 >64KiB（非 JSON 也先判字节）
    r = c.post("/batches", content=gz(b"x" * 70000 + b"\n"))
    check("single line >64KiB -> 413", r.status_code == 413, r.text[:120])

    # 压缩体 >2MiB：合法 gzip 封装的不可压缩数据（约 3MiB），服务端中途即停
    over = gz(os.urandom(3 * 1024 * 1024))
    try:
        r = c.post("/batches", content=over)
        check("compressed >2MiB -> 413",
              r.status_code == 413, f"got {r.status_code}")
    except httpx.HTTPError:
        # 服务端已立即回 413 并关连接，客户端写剩余 body 时可能感知到断连
        check("compressed >2MiB -> 413 (connection closed)", True)

    check("still no zombie batches", list_batch_ids() == committed_so_far)

    print("== 并发：慢上传不阻塞其它请求，且完成前不可见 ==")
    # 每条记录带不同的不可压缩内容：压缩后仍足够大，上传可持续数秒；
    # 总解压量约 500*1280B ≈ 640KB，接收中就会多次刷入独立暂存文件
    slow_payload = gz(b"".join(
        json.dumps({"id": f"s{i:06d}",
                    "message": os.urandom(600).hex() + " " + mark})
            .encode() + b"\n"
        for i in range(500)
    ))
    chunks = [slow_payload[i:i + 1000]
              for i in range(0, len(slow_payload), 1000)]
    total_seconds = len(chunks) * 0.05
    check("slow upload lasts several seconds", total_seconds > 5,
          f"{total_seconds:.1f}s, {len(slow_payload)} compressed bytes")

    def gen():
        for ch in chunks:
            time.sleep(0.05)
            yield ch

    result = {}
    def slow_post():
        try:
            with httpx.Client(base_url=BASE, timeout=60) as cc:
                result["r"] = cc.post("/batches", content=gen())
        except Exception as e:
            result["e"] = e

    t = threading.Thread(target=slow_post)
    before_slow = list_batch_ids()
    t.start()
    time.sleep(1.5)  # 慢上传进行中
    ids_mid = list_batch_ids()
    check("no new batch visible mid-flight", ids_mid == before_slow,
          f"extra={ids_mid - before_slow}")
    # 每批次独占一个暂存 SQLite 文件；接收中应已创建并有内容落盘，
    # 但主库里绝不可见
    import glob as _glob
    stage_files = [p for p in _glob.glob(
        os.path.join(os.path.dirname(DB_PATH), "staging", "stage_*.db"))]
    stage_size = sum(os.path.getsize(p) for p in stage_files)
    check("dedicated staging file present mid-flight",
          len(stage_files) == 1 and stage_size > 0,
          f"files={stage_files} size={stage_size}")
    t0 = time.time()
    r1 = c.get("/batches")
    read_ms = (time.time() - t0) * 1000
    t0 = time.time()
    r2 = c.post("/batches",
                content=gz(ndjson([{"id": "fast1", "message": "go"}])))
    post_ms = (time.time() - t0) * 1000
    fast_bid = r2.json()["batch_id"]
    check("read during slow upload fast", read_ms < 3000, f"{read_ms:.0f}ms")
    check("another submit during slow upload fast -> 201",
          r2.status_code == 201 and post_ms < 3000,
          f"{r2.status_code} {post_ms:.0f}ms")
    ids_after_fast = list_batch_ids()
    check("fast batch visible, slow still invisible",
          ids_after_fast == before_slow | {fast_bid},
          f"extra={ids_after_fast - before_slow - {fast_bid}}")
    # 暂存中的批次不参与检索：慢上传的标记此刻应查不到任何行
    r = c.get("/logs/search", params={"q": mark, "page_size": 100})
    check("staging rows not searchable mid-flight",
          r.status_code == 200 and r.json()["items"] == []
          and r.json()["next_cursor"] is None, r.text[:150])
    t.join()
    check("slow upload took its time", "r" in result
          and result["r"].status_code == 201,
          str(result.get("e", "")))
    slow_bid = result["r"].json()["batch_id"]
    check("slow batch visible after completion",
          slow_bid in list_batch_ids() and slow_bid != fast_bid)
    check("slow batch line count", result["r"].json()["line_count"] == 500)
    r = c.get(f"{BASE}/batches/{slow_bid}/lines", params={"limit": 3})
    check("slow batch lines ordered",
          [(i["seq"], i["id"]) for i in r.json()["items"]]
          == [(1, "s000000"), (2, "s000001"), (3, "s000002")])
    # 提交后整批可被检索：用游标翻页数满 500 行
    total, cursor = 0, None
    while True:
        params = {"page_size": 100}
        params["q" if cursor is None else "cursor"] = mark if cursor is None else cursor
        body = c.get("/logs/search", params=params).json()
        total += len(body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
    check("slow batch searchable after commit (500 hits)", total == 500, str(total))

    print("== 并发：慢下载不阻塞上传提交与已完成批次读取 ==")
    # 不可压缩内容：gzip 体积大，客户端慢速读取可让下载持续数秒
    slow_dl_payload = b"".join(
        json.dumps({"id": f"dl{i:06d}",
                    "message": os.urandom(200).hex()}).encode() + b"\n"
        for i in range(3000)
    )
    r = c.post("/batches", content=gz(slow_dl_payload))
    check("slow-download fixture -> 201", r.status_code == 201, r.text[:120])
    slow_dl_bid = r.json()["batch_id"]

    dl_result = {}
    def slow_download():
        try:
            with httpx.Client(base_url=BASE, timeout=60) as cc:
                with cc.stream("GET",
                               f"/batches/{slow_dl_bid}/download") as rr:
                    dl_result["status"] = rr.status_code
                    n = 0
                    for chunk in rr.iter_bytes():
                        time.sleep(0.005)
                        n += len(chunk)
                    dl_result["bytes"] = n
        except Exception as e:
            dl_result["e"] = repr(e)

    dt = threading.Thread(target=slow_download)
    dt.start()
    time.sleep(1.0)  # 慢下载进行中
    t0 = time.time()
    rr1 = c.get("/batches")
    dl_read_ms = (time.time() - t0) * 1000
    t0 = time.time()
    rr2 = c.post("/batches",
                 content=gz(ndjson([{"id": "dlfast", "message": "go"}])))
    dl_post_ms = (time.time() - t0) * 1000
    check("list during slow download fast",
          rr1.status_code == 200 and dl_read_ms < 3000, f"{dl_read_ms:.0f}ms")
    check("submit during slow download fast -> 201",
          rr2.status_code == 201 and dl_post_ms < 3000,
          f"{rr2.status_code} {dl_post_ms:.0f}ms")
    t0 = time.time()
    rr3 = c.get(f"{BASE}/batches/{slow_bid}/lines", params={"limit": 5})
    check("read completed batch during slow download fast",
          rr3.status_code == 200 and (time.time() - t0) * 1000 < 3000)
    dt.join()
    check("slow download completed with 200",
          dl_result.get("status") == 200 and "bytes" in dl_result
          and "e" not in dl_result, str(dl_result))
    # 下载只读：用 lines 接口核对批次行数未变
    dl_count = 0
    off = 0
    while True:
        page = c.get(f"{BASE}/batches/{slow_dl_bid}/lines",
                     params={"limit": 1000, "offset": off}).json()
        if not page["items"]:
            break
        dl_count += len(page["items"])
        off += 1000
    check("download did not modify batch", dl_count == 3000, str(dl_count))

    print("== 日志检索 ==")
    s1 = [
        {"id": "dup", "message": f"{tok} Error disk_full"},
        {"id": "m2", "message": f"{tok} error diskXfull"},
        {"id": "m3", "message": f"{tok} plain"},
        {"id": "m4", "message": f"{tok}_score"},
        {"id": "m5", "message": f"{tok}Xscore"},
        {"id": "m6", "message": f"{tok}%"},
    ]
    r = c.post("/batches", content=gz(ndjson(s1)))
    check("search fixture batch1 -> 201", r.status_code == 201, r.text)
    sb1 = r.json()["batch_id"]

    def search(params):
        return c.get("/logs/search", params=params)

    r = search({"q": f"{tok} Error", "page_size": 10})
    check("search 200", r.status_code == 200, r.text[:200])
    body = r.json()
    check("case sensitive: Error", [i["id"] for i in body["items"]] == ["dup"], body)
    check("single page -> null cursor", body["next_cursor"] is None, body)
    check("item fields",
          all(set(i) == {"batch_id", "seq", "id", "message"} for i in body["items"]))
    check("item batch_id & seq",
          (body["items"][0]["batch_id"], body["items"][0]["seq"]) == (sb1, 1))

    r = search({"q": f"{tok} error", "page_size": 10})
    check("case sensitive: error", [i["id"] for i in r.json()["items"]] == ["m2"])

    r = search({"q": f"{tok}%", "page_size": 10})
    check("% is literal", [i["id"] for i in r.json()["items"]] == ["m6"],
          r.text[:200])

    r = search({"q": f"{tok}_score", "page_size": 10})
    check("_ is literal", [i["id"] for i in r.json()["items"]] == ["m4"],
          r.text[:200])

    r = search({"q": f" {tok}%", "page_size": 10})
    check("whitespace preserved (not stripped)", r.json()["items"] == [])

    r = search({"q": f"{tok}-no-such", "page_size": 10})
    body = r.json()
    check("no match -> empty items + null cursor",
          body["items"] == [] and body["next_cursor"] is None, body)

    # 分页 + 快照：翻页之间提交的新批次不混入旧查询，但新查询可见
    r = search({"q": tok, "page_size": 2})
    body = r.json()
    check("page1", [(i["batch_id"], i["seq"], i["id"]) for i in body["items"]]
          == [(sb1, 1, "dup"), (sb1, 2, "m2")], body)
    check("page1 has cursor", bool(body["next_cursor"]))
    cur1 = body["next_cursor"]

    s2 = [{"id": "dup", "message": f"{tok} second batch"}]
    r = c.post("/batches", content=gz(ndjson(s2)))
    check("search fixture batch2 -> 201", r.status_code == 201, r.text)
    sb2 = r.json()["batch_id"]

    r = search({"cursor": cur1, "page_size": 2})
    body = r.json()
    check("page2 keeps snapshot (new batch not mixed in)",
          [i["id"] for i in body["items"]] == ["m3", "m4"], body)
    cur2 = body["next_cursor"]
    check("page2 has cursor", bool(cur2))

    r = search({"cursor": cur2, "page_size": 2})
    body = r.json()
    check("page3 (last page) -> null cursor",
          [i["id"] for i in body["items"]] == ["m5", "m6"]
          and body["next_cursor"] is None, body)

    r = search({"q": tok, "page_size": 100})
    body = r.json()
    ids = [i["id"] for i in body["items"]]
    check("new query sees new batch",
          ids == ["dup", "m2", "m3", "m4", "m5", "m6", "dup"], ids)
    check("same id across batches kept", ids.count("dup") == 2)
    check("ordered by commit order then seq",
          [i["batch_id"] for i in body["items"]] == [sb1] * 6 + [sb2]
          and [i["seq"] for i in body["items"]] == [1, 2, 3, 4, 5, 6, 1])

    print("== 检索参数校验 ==")
    check("no q no cursor -> 422",
          search({"page_size": 10}).status_code == 422)
    check("q and cursor -> 422",
          search({"q": tok, "cursor": cur1, "page_size": 10}).status_code == 422)
    check("empty q -> 422", search({"q": "", "page_size": 10}).status_code == 422)
    check("q 201 chars -> 422",
          search({"q": "x" * 201, "page_size": 10}).status_code == 422)
    check("q 200 chars ok",
          search({"q": "x" * 200, "page_size": 10}).status_code == 200)
    check("missing page_size -> 422", search({"q": tok}).status_code == 422)
    check("page_size 0 -> 422",
          search({"q": tok, "page_size": 0}).status_code == 422)
    check("page_size 101 -> 422",
          search({"q": tok, "page_size": 101}).status_code == 422)
    check("page_size 100 ok",
          search({"q": tok, "page_size": 100}).status_code == 200)
    check("page_size non-int -> 422",
          search({"q": tok, "page_size": "abc"}).status_code == 422)
    check("garbage cursor -> 422", search({"cursor": "!!!"}).status_code == 422)
    not_json = base64.urlsafe_b64encode(b"not json").decode().rstrip("=")
    check("base64 non-json cursor -> 422",
          search({"cursor": not_json}).status_code == 422)

    def make_cursor(obj):
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    check("cursor bad version -> 422",
          search({"cursor": make_cursor(
              {"v": 2, "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor empty q -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor q too long -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x" * 201, "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor negative bound -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": -1, "last": [1, 1]})}).status_code == 422)
    check("cursor bad last -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [1]})}).status_code == 422)
    check("cursor bool in last -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [1, True]})}).status_code == 422)
    check("cursor missing field -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1})}).status_code == 422)
    check("truncated valid cursor -> 422",
          search({"cursor": cur1[:-3]}).status_code == 422)

    print("== 游标字段严格校验 ==")
    INT64_MAX = 9223372036854775807
    # v 只能是非布尔整数 1：布尔、浮点、字符串一律非法
    check("cursor v=true -> 422",
          search({"cursor": make_cursor(
              {"v": True, "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor v=1.0 -> 422",
          search({"cursor": make_cursor(
              {"v": 1.0, "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    check("cursor v='1' -> 422",
          search({"cursor": make_cursor(
              {"v": "1", "q": "x", "bound": 1, "last": [1, 1]})}).status_code == 422)
    # bound 超出 SQLite int64 上限：不得 500（参数绑定溢出），须 422
    check("cursor bound=2^63 -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": INT64_MAX + 1,
               "last": [0, 0]})}).status_code == 422)
    check("cursor bound huge -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 10 ** 30,
               "last": [0, 0]})}).status_code == 422)
    check("cursor bound=true -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": True, "last": [1, 1]})}).status_code == 422)
    check("cursor bound=1.0 -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1.0, "last": [1, 1]})}).status_code == 422)
    # last 两项同样只允许 0..2^63-1 的非布尔整数
    check("cursor last[0]=2^63 -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1,
               "last": [INT64_MAX + 1, 0]})}).status_code == 422)
    check("cursor last[1]=2^63 -> 422 (no 500)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1,
               "last": [0, INT64_MAX + 1]})}).status_code == 422)
    check("cursor last negative -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [-1, 0]})}).status_code == 422)
    check("cursor last float -> 422",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [0, 1.0]})}).status_code == 422)
    # last[0] > bound：不得返回正常空页，须 422
    check("cursor last[0]>bound -> 422 (not empty page)",
          search({"cursor": make_cursor(
              {"v": 1, "q": "x", "bound": 1, "last": [2, 0]})}).status_code == 422)

    print("== 合法续页回归 ==")
    # 手工构造的合法游标（与服务端同格式）必须照常可用：
    # bound 取 int64 上限的边界值合法，last=[0,0] 表示从头翻
    r = search({"cursor": make_cursor(
        {"v": 1, "q": tok, "bound": INT64_MAX, "last": [0, 0]}),
        "page_size": 100})
    check("hand-built cursor bound=2^63-1 -> 200", r.status_code == 200,
          f"got {r.status_code} {r.text[:150]}")
    body = r.json()
    check("hand-built cursor full results",
          [i["id"] for i in body["items"]]
          == ["dup", "m2", "m3", "m4", "m5", "m6", "dup"]
          and body["next_cursor"] is None, body)
    # last[0]==bound 边界合法：从快照末尾批次继续翻
    r = search({"cursor": make_cursor(
        {"v": 1, "q": tok, "bound": INT64_MAX, "last": [INT64_MAX, 0]}),
        "page_size": 100})
    check("hand-built cursor last[0]==bound -> 200 empty",
          r.status_code == 200 and r.json()["items"] == []
          and r.json()["next_cursor"] is None,
          f"got {r.status_code} {r.text[:150]}")
    # 解码服务端签发的 cur1，按原样重编码后结果必须与游标直接续页一致
    raw = json.loads(base64.urlsafe_b64decode(cur1 + "=" * (-len(cur1) % 4)))
    check("server cursor format", set(raw) == {"v", "q", "bound", "last"}
          and raw["v"] == 1 and raw["q"] == tok, raw)
    rebuilt = make_cursor(raw)
    r = search({"cursor": rebuilt, "page_size": 2})
    check("re-encoded cursor same page2",
          r.status_code == 200
          and [i["id"] for i in r.json()["items"]] == ["m3", "m4"],
          f"got {r.status_code} {r.text[:200]}")
    # 续页页大小可变：同一游标用不同 page_size 不报错且从同一位置开始
    r = search({"cursor": rebuilt, "page_size": 3})
    check("resumed page_size variable",
          [i["id"] for i in r.json()["items"]] == ["m3", "m4", "m5"],
          r.text[:200])

    print("== 批次区间重复消息 ==")
    # 每个 message 都带本次运行唯一前缀，避免与历史数据互相干扰
    p = f"dupmark{tok}"
    m_aa, m_bb, m_cc, m_qq = f"{p}aa", f"{p}bb", f"{p}cc", f"{p}qq"
    m_uni = f"{p}\u00e9"       # 预组码点 U+00E9
    m_case = f"{p}Aa"     # 与 m_aa 仅大小写不同
    m_space = f"{p}aa "   # 与 m_aa 仅尾随空白不同
    dup_batches = []
    dup_specs = [
        # 提交顺序即批次 rowid 顺序
        [("d1", m_aa), ("d2", m_aa), ("d3", m_bb)],
        [("d1", m_aa), ("d2", m_cc), ("d3", m_uni), ("d4", m_qq)],
        [("d1", m_bb), ("d2", m_qq), ("d3", m_case)],
    ]
    for spec in dup_specs:
        r = c.post("/batches",
                   content=gz(ndjson([{"id": i, "message": m} for i, m in spec])))
        check("dup fixture upload -> 201", r.status_code == 201, r.text[:150])
        dup_batches.append(r.json()["batch_id"])
    b1, b2, b3 = dup_batches

    def dup(params):
        return c.get("/logs/duplicates", params=params)

    def tuples(body):
        return [(i["message"], i["occurrences"], i["batch_count"])
                for i in body["items"]]

    def bin_sorted_ties(rows):
        """occurrences 降序、同次数按 UTF-8 字节序的参考排序。"""
        return sorted(rows, key=lambda t: (-t[1], t[0].encode("utf-8")))

    # 三批合计：aa 3 次跨 2 批；bb/qq 各 2 次跨 2 批；cc/uni/Aa 各 1 次
    r = dup({"start_batch_id": b1, "end_batch_id": b3})
    check("duplicates 200", r.status_code == 200, r.text[:200])
    body = r.json()
    got = tuples(body)
    check("duplicates groups & counts",
          got == [(m_aa, 3, 2), (m_bb, 2, 2), (m_qq, 2, 2)], got)
    check("duplicates item fields",
          all(set(i) == {"message", "occurrences", "batch_count"}
              for i in body["items"]))
    check("duplicates envelope fields",
          set(body) == {"total", "limit", "offset", "items"}, set(body))
    check("duplicates total before pagination", body["total"] == 3, body)
    check("duplicates default limit/offset",
          body["limit"] == 100 and body["offset"] == 0, body)

    # 闭区间含两端：b2..b3 中 qq 在两端各一次，aa/bb 各只剩一次（被区间排除）
    r = dup({"start_batch_id": b2, "end_batch_id": b3})
    body = r.json()
    check("closed range excludes outer batch",
          tuples(body) == [(m_qq, 2, 2)] and body["total"] == 1, body)

    # 允许两端相同：批内重复可查；无重复批次为空
    r = dup({"start_batch_id": b1, "end_batch_id": b1})
    body = r.json()
    check("single-batch range counts within-batch duplicate",
          tuples(body) == [(m_aa, 2, 1)] and body["total"] == 1, body)
    r = dup({"start_batch_id": b2, "end_batch_id": b2})
    body = r.json()
    check("single batch without duplicates -> empty",
          body["items"] == [] and body["total"] == 0, body)

    # 大小写、空白与 Unicode 码点差异不合并：再传一批同形不同码点的消息
    m_uni_split = f"{p}e\u0301"  # e + 组合重音 U+0301，与预组 U+00E9 不同码点序列
    r = c.post("/batches", content=gz(ndjson([
        {"id": "u1", "message": m_uni},
        {"id": "u2", "message": m_uni_split},
        {"id": "u3", "message": m_case},
        {"id": "u4", "message": m_space},
        {"id": "u5", "message": m_space},
    ])))
    check("case/codepoint fixture -> 201", r.status_code == 201, r.text[:150])
    b4 = r.json()["batch_id"]
    r = dup({"start_batch_id": b1, "end_batch_id": b4})
    body = r.json()
    got = tuples(body)
    # Aa、aa(带空格)、预组 é 各成新组；若大小写/空白/码点被错误归一化，
    # 这些组会并入 aa 或彼此合并（occurrences/batch_count 就会变）。
    # 组合重音形态在区间内仅 1 次，不出现在结果里。
    expected = bin_sorted_ties([
        (m_aa, 3, 2),
        (m_case, 2, 2),       # b3 + b4，未并入小写 aa
        (m_space, 2, 1),      # b4 批内两次，未并入无空格 aa
        (m_bb, 2, 2),
        (m_qq, 2, 2),
        (m_uni, 2, 2),        # b2 + b4，预组码点；未并入组合重音形态
    ])
    check("case, whitespace and codepoint differences kept distinct",
          got == expected and body["total"] == len(expected), got)

    # occurrences 降序 + BINARY 升序：造同次数多组，与 Python UTF-8 字节序对照
    spec5 = [(f"t{i:02d}", f"{p}k{i%3}") for i in range(6)]  # k0/k1/k2 各 2 次
    r = c.post("/batches", content=gz(ndjson(
        [{"id": i, "message": m} for i, m in spec5])))
    check("order fixture -> 201", r.status_code == 201, r.text[:150])
    b5 = r.json()["batch_id"]
    r = dup({"start_batch_id": b5, "end_batch_id": b5})
    body = r.json()
    msgs = [i["message"] for i in body["items"]]
    check("BINARY tie order matches UTF-8 byte order",
          msgs == sorted(msgs, key=lambda s: s.encode("utf-8"))
          and all(i["occurrences"] == 2 for i in body["items"]), msgs)

    # 分页：limit/offset 切片与全量一致，total 是分页前总数，越界页为空
    all_body = dup({"start_batch_id": b1, "end_batch_id": b5}).json()
    total_groups = all_body["total"]
    # 整页排序必须与参考排序一致（occurrences 降序、BINARY 升序）
    check("full range ordering",
          tuples(all_body) == bin_sorted_ties(tuples(all_body))
          and total_groups == len(all_body["items"]), all_body)
    page_size = 2
    full = []
    for off in range(0, total_groups, page_size):
        page = dup({"start_batch_id": b1, "end_batch_id": b5,
                    "limit": page_size, "offset": off}).json()
        check(f"page offset={off} echoes params",
              page["limit"] == page_size and page["offset"] == off)
        full.extend(tuples(page))
    check("pages concatenate to full result", full == tuples(all_body), full)
    over = dup({"start_batch_id": b1, "end_batch_id": b5,
                "limit": page_size, "offset": total_groups}).json()
    check("out-of-range page empty & total unchanged",
          over["items"] == [] and over["total"] == total_groups, over)

    # 404：未知批次（先于区间方向判断）
    r = dup({"start_batch_id": "deadbeef", "end_batch_id": b1})
    check("unknown start -> 404", r.status_code == 404, r.status_code)
    r = dup({"start_batch_id": b1, "end_batch_id": "deadbeef"})
    check("unknown end -> 404", r.status_code == 404, r.status_code)

    # 422：起点晚于终点（两端都存在）
    r = dup({"start_batch_id": b3, "end_batch_id": b1})
    check("start later than end -> 422", r.status_code == 422, r.text[:120])

    # 422：参数缺失 / 非法
    check("missing start -> 422",
          dup({"end_batch_id": b1}).status_code == 422)
    check("missing end -> 422",
          dup({"start_batch_id": b1}).status_code == 422)
    check("missing both -> 422", dup({}).status_code == 422)
    check("offset -1 -> 422",
          dup({"start_batch_id": b1, "end_batch_id": b1,
               "offset": -1}).status_code == 422)
    check("limit 0 -> 422",
          dup({"start_batch_id": b1, "end_batch_id": b1,
               "limit": 0}).status_code == 422)
    check("limit 101 -> 422",
          dup({"start_batch_id": b1, "end_batch_id": b1,
               "limit": 101}).status_code == 422)
    check("limit non-int -> 422",
          dup({"start_batch_id": b1, "end_batch_id": b1,
               "limit": "x"}).status_code == 422)
    check("limit 1 ok",
          dup({"start_batch_id": b1, "end_batch_id": b1,
               "limit": 1}).status_code == 200)
    check("limit 100 ok",
          dup({"start_batch_id": b1, "end_batch_id": b1,
               "limit": 100}).status_code == 200)

    # 快照：固定区间后再上传，相同参数结果不变
    before_snap = dup({"start_batch_id": b1, "end_batch_id": b3}).json()
    r = c.post("/batches", content=gz(ndjson(
        [{"id": "late1", "message": m_aa}, {"id": "late2", "message": m_aa}])))
    check("post-snapshot fixture -> 201", r.status_code == 201, r.text[:120])
    after_snap = dup({"start_batch_id": b1, "end_batch_id": b3}).json()
    check("later uploads do not change same-range result",
          after_snap == before_snap, (before_snap, after_snap))

    print("== gzip NDJSON 下载 ==")
    # 含引号、反斜杠、\n、\t、\r、NUL、Unicode 与 emoji 的记录
    dl_records = [
        {"id": "a", "message": 'he said "hi"\nnext\tline\\end'},
        {"id": "b", "message": "世界 🌍 é\x00"},
        {"id": "c", "message": "plain \r\n crlf \\ \""},
    ]
    r = c.post("/batches", content=gz(ndjson(dl_records)))
    check("download fixture upload -> 201", r.status_code == 201, r.text[:120])
    dl_bid = r.json()["batch_id"]

    with c.stream("GET", f"{BASE}/batches/{dl_bid}/download") as r:
        check("download 200", r.status_code == 200, r.status_code)
        check("download content-type is gzip",
              r.headers.get("content-type") == "application/gzip",
              r.headers.get("content-type"))
        check("download sets no content-encoding",
              "content-encoding" not in {k.lower() for k in r.headers},
              dict(r.headers))
        check("download content-disposition",
              r.headers.get("content-disposition")
              == f'attachment; filename="batch-{dl_bid}.json.gz"',
              r.headers.get("content-disposition"))
        dl_gz = b"".join(r.iter_bytes())

    check("downloaded body is gzip", dl_gz[:2] == b"\x1f\x8b", dl_gz[:4])
    # gzip.decompress 同时校验 CRC32 与 ISIZE 结束尾部
    dl_raw = gzip.decompress(dl_gz)
    check("downloaded gzip complete (CRC/trailer)", True)
    check("download last line ends with LF", dl_raw.endswith(b"\n"))
    check("download has no blank lines", b"\n\n" not in dl_raw)
    dl_lines = dl_raw.decode("utf-8").splitlines()
    check("download line count", len(dl_lines) == len(dl_records),
          str(len(dl_lines)))
    check("download keys exactly id/message",
          all(set(json.loads(l)) == {"id", "message"} for l in dl_lines))
    check("download values match source",
          [json.loads(l) for l in dl_lines] == dl_records, dl_lines)
    check("download quotes/newlines escaped",
          b'he said \\"hi\\"\\nnext\\tline\\\\end' in dl_raw
          and b'\\r\\n crlf \\\\ \\"' in dl_raw, dl_raw[:200])
    check("download NUL escaped", b"\\u0000" in dl_raw)
    # 下载内容与库内原文（lines 分页接口）逐值一致
    db_items = []
    while len(db_items) < len(dl_records):
        page = c.get(f"{BASE}/batches/{dl_bid}/lines",
                     params={"limit": 2, "offset": len(db_items)}).json()["items"]
        db_items.extend(page)
    check("download matches database rows",
          [(row["seq"], row["id"], row["message"]) for row in db_items]
          == [(i + 1, rec["id"], rec["message"])
              for i, rec in enumerate(dl_records)])

    # 下载到的 gzip 仍在既有上传限额内，可直接重新上传
    r = c.post("/batches", content=dl_gz)
    check("re-upload downloaded gzip -> 201", r.status_code == 201, r.text[:120])
    re_bid = r.json()["batch_id"]
    with c.stream("GET", f"{BASE}/batches/{re_bid}/download") as r:
        re_gz = b"".join(r.iter_bytes())
    check("re-downloaded content identical", gzip.decompress(re_gz) == dl_raw)

    # 未知批次：响应开始前 404（JSON 错误体，而非 gzip 头）
    with c.stream("GET", f"{BASE}/batches/deadbeef/download") as r:
        check("unknown download -> 404 before body",
              r.status_code == 404
              and r.headers.get("content-type") == "application/json",
              r.status_code)
        check("unknown download has no gzip bytes", r.read()[:2] != b"\x1f\x8b")

    # 客户端中途断开：服务端须释放游标/连接并继续正常服务
    with c.stream("GET", f"{BASE}/batches/{dl_bid}/download") as r:
        partial = b""
        for chunk in r.iter_bytes():
            partial += chunk
            if len(partial) >= 8:
                break
    check("partial download received gzip header then disconnected",
          partial[:2] == b"\x1f\x8b")
    check("server healthy after disconnect",
          c.get(f"{BASE}/batches").status_code == 200
          and c.post("/batches",
                     content=gz(b'{"id":"after-dc","message":"ok"}\n')).status_code
          == 201)
    with c.stream("GET", f"{BASE}/batches/{dl_bid}/download") as r:
        check("full download after disconnect still valid",
              r.status_code == 200
              and gzip.decompress(b"".join(r.iter_bytes())) == dl_raw)

    print("== 暂存清理 ==")
    import glob as _glob
    leftovers = _glob.glob(
        os.path.join(os.path.dirname(DB_PATH), "staging", "stage_*.db*"))
    check("no staging files left", leftovers == [], str(leftovers))
    conn = sqlite3.connect(DB_PATH)
    orphan_lines = conn.execute(
        "SELECT count(*) FROM lines WHERE batch_id NOT IN (SELECT id FROM batches)"
    ).fetchone()[0]
    check("no orphan lines", orphan_lines == 0, str(orphan_lines))
    conn.close()

    print(f"\n{passed} passed, {len(failed)} failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
