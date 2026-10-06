# gzip NDJSON 批次接收 API

接收单个 **gzip 流**，解压为 **NDJSON**：每行必须是恰好包含非空字符串
`id`、`message` 两个键的 JSON 对象，批内 `id` 唯一。只有在 gzip 流完整
结束、CRC 校验通过且整批成功落库后，批次才可查询。

- 语言/框架：Python 3.12 + FastAPI + Uvicorn
- 存储：SQLite（WAL 模式），数据位于容器内 `/data`（命名卷持久化）

## 字节限制（均按字节计）

| 限制 | 阈值 | 超限结果 |
|---|---|---|
| gzip 压缩请求体 | 2 MiB | `413`，立即停止读取 |
| 解压后数据总量 | 8 MiB | `413`，立即停止读取 |
| 单行正文（不含换行符） | 64 KiB | `413`，立即停止读取 |

其他失败：gzip 损坏 / 被截断 / CRC 错误 / gzip 后有尾随数据、非法 UTF-8、
空行、非法 JSON、记录缺键/多键/类型错误/空串、批内 id 重复、批次为空等，
一律返回 `422`。

接收逐块进行，内存中任何时刻只保留一个读块、一段 zlib 输出、一条未满行和
一小批（约 256 KiB）待落盘记录，不会缓存完整请求体或完整解压内容。上传
期间记录先写入该批次独占的临时暂存库文件（`/data/staging/`），与主库写锁
完全无关；全部校验通过后用单个短事务 `ATTACH` 导入主库原子提交。接收中断、
校验失败或入库失败都会删除暂存文件，不会留下任何可查询批次；慢上传因此
不会阻塞其他批次提交，也不会阻塞已完成批次的读取。

## 启动

需要 Docker（含 Compose 插件）。单条命令完成依赖安装、数据库初始化并启动：

```bash
docker compose up --build
```

服务监听 **8000** 端口，数据保存在命名卷 `gzip_ndjson_data` 中。
数据库在容器启动时通过 `python -m app.init_db` 初始化（幂等，且会清理
上次崩溃可能残留的暂存文件），随后启动 Uvicorn。

停止：`docker compose down`（数据卷保留；加 `-v` 同时删除数据）。

## 接口

### `POST /batches` — 上传 gzip 批次

请求体就是 gzip 字节本身（不是 multipart 表单）。成功返回 `201`：

```json
{ "batch_id": "9f2c…(32位十六进制)", "line_count": 3 }
```

### `GET /batches?limit=100&offset=0` — 列出已提交批次

返回 `total` 与 `items`，每项含 `batch_id`、`line_count`、`created_at`，
按提交顺序分页。

### `GET /batches/{batch_id}/lines?limit=100&offset=0` — 分页读行

保持上传时的原始顺序，每行含 `seq`（从 1 开始）、`id`、`message`。
未知批次 ID 返回 `404`。

### `DELETE /batches/{batch_id}` — 永久删除批次

把一个**已提交**批次及其**全部行**在**单个事务内永久删除**（硬删除，不保留
墓碑）。成功返回 `200`：

```json
{ "batch_id": "9f2c…", "line_count": 3 }
```

- 未知批次 ID、或此前已经删除的批次：`404`。
- 删除是原子的：任一步失败整体回滚，批次与其行全部保留，不留半成品。
- 成功之后：该批次不再出现在 `GET /batches`；`GET /batches/{id}/lines`
  与 `GET /batches/{id}/download` 返回 `404`；检索（`/logs/search`）不再
  返回这些行，区间重复统计（`/logs/duplicates`）不再计入这些行，端点批次
  被删时按未知批次返回 `404`。
- **旧检索游标续页会跳过被删记录**（整批被删也安全跳过，不重复、不报错）。
  批次提交序号由 SQLite `AUTOINCREMENT` 分配，删除（乃至删光全部批次）后
  再上传，新批次使用更大的新序号，**不会进入删除前签发的旧游标范围**。
- 删除是 WAL 下的短写事务：**不等待进行中的慢下载**（已发出 200 响应头的
  下载持有删除前的只读快照，仍会按原顺序输出删除前的全部记录与完整 gzip
  尾部；详见下载一节），也不阻塞另一批上传提交。
- 升级/重启不影响旧数据与删除前签发的合法游标；其余接口行为不变。

### `GET /batches/{batch_id}/download` — 下载整批为 gzip NDJSON 文件

把已提交批次导出为**单个完整 gzip 流**（`Content-Type:
application/gzip`，**不设置** `Content-Encoding`：gzip 是文件内容本身，
不是传输层编码，客户端会原样落盘）。响应头附带
`Content-Disposition: attachment; filename="batch-<batch_id>.json.gz"`，
方便直接保存为文件。未知批次 ID 在**开始输出响应体之前**返回 `404`。

解压后是 **UTF-8 NDJSON**：按批内 `seq` 升序，每行一个**仅含 `id`、
`message`** 两个键的 JSON 对象，值与库内原文一致；引号、反斜杠、换行、
制表符等字符按 JSON 规则正确转义；**末行也带换行符**。gzip 流含正确的
CRC32 与 ISIZE 结束尾部（`gzip -t` 可验）。

服务端逐页（键集分页）读取记录、逐行序列化并逐块压缩下发：内存中任何
时刻只有一页记录、一行序列化字节与压缩器缓冲，**不**攒齐整批 NDJSON、
完整 gzip 文件或完整解压内容，也**不**生成完整临时文件。下载连接在发出
200 响应头**之前**即进入一个显式只读事务固定 WAL 快照，之后只执行只读
短查询（WAL 下读不阻塞写），慢下载只阻塞自己的响应协程，不阻塞另一批
上传提交，**也不阻塞删除该批次**：即使下载进行中批次被永久删除，已经
发出 200 的下载仍按快照输出删除前的全部记录（顺序不变）并写出完整
CRC32/ISIZE 尾部；删除完成后新发起的下载返回 `404`。下载正常结束或
客户端中途断连时都会提交/回滚事务、关闭连接并释放快照，删除随后即可
回收对应的 WAL 空间。

### `GET /logs/search` — 按 message 检索日志

在所有**已提交**批次中按 `message` 做**大小写敏感的字面子串**匹配：
`%`、`_` 只是普通字符（不是通配符），查询词中的空白原样保留。
暂存中（尚未提交）的批次不参与检索。

- 首页：`?q=<查询词>&page_size=<1..100>`。查询词长度 1～200 个字符；
  响应时固定当时已提交的批次范围。
- 续页：`?cursor=<上一页返回的 next_cursor>&page_size=<1..100>`，
  沿用首页的查询词与批次范围。`q` 与 `cursor` 必须且只能提供一个。
- 响应：`items` 中每行含 `batch_id`、`seq`、`id`、`message`，按批次提交
  顺序、批内 `seq` 升序排列；同一 `id` 出现在多个批次时各行都保留。
  `next_cursor` 为下一页游标，末页为 `null`；无匹配时 `items` 为空且
  `next_cursor` 为 `null`。
- 翻页期间新提交的批次不会混入进行中的查询；重新发起查询即可看到。
  翻页期间批次被永久删除时，旧游标续页**直接跳过已删记录/已删批次**
  （不报错、不重复、不漏掉仍存在的行）；删除最新乃至删光全部批次后再
  上传，新批次序号更大，也不会落入旧游标范围。
- 游标无状态，服务重启后仍可续用；游标损坏或字段不合法返回 `422`。
  游标是 URL-safe Base64（去填充）编码的 JSON 对象
  `{"v":1,"q":<查询词>,"bound":<批次范围上界>,"last":[<批次rowid>,<seq>]}`，
  字段校验规则：`v` 只能是非布尔整数 `1`（`true`、`1.0`、`"1"` 均非法）；
  `bound` 与 `last` 两项只能是 `0～9223372036854775807`（SQLite int64
  上限）的非布尔整数；且 `last[0] ≤ bound`。违规游标在执行检索前即返回
  `422`，不会返回 `500` 或正常空页。
- 检索按页做键集分页：不缓存全量命中行、不跨请求持有数据库事务，
  WAL 下检索与上传提交互不阻塞。

### `GET /logs/duplicates` — 批次区间内的重复消息

在两个**已提交**批次按提交顺序构成的**闭区间**（含两端）内，按 `message`
原文分组统计重复。分组保留大小写、空白与 Unicode 码点差异（即 SQLite
BINARY 比较），**不按 `id` 去重**：同一条消息在批内重复、或在不同批次
重复，每一行各计一次。暂存中（尚未提交）的批次不参与。

参数：

| 参数 | 要求 |
|---|---|
| `start_batch_id` | 区间起点批次 ID（必填） |
| `end_batch_id` | 区间终点批次 ID（必填，允许与起点相同） |
| `offset` | 非负整数，默认 `0` |
| `limit` | `1～100` 的整数，默认 `100` |

- 只返回出现 **至少两次**的消息；每项含 `message`、`occurrences`（区间内
  总行数）、`batch_count`（出现过该消息的不同批次数）。
- 排序：先按 `occurrences` **降序**，次数相同再按 `message` 的 SQLite
  BINARY 顺序（等价于 UTF-8 字节序）**升序**，随后按该顺序 `limit/offset`
  分页。
- 响应中的 `total` 是**分页前**的分组总数；越界页或区间内没有任何重复时
  `items` 为空列表。
- 任一 ID 是未知批次返回 `404`；起点批次晚于终点批次提交，或参数缺失/非法
  返回 `422`。
- 区间由两端批次固定，之后上传的新批次不会改变相同参数的结果；聚合、排序
  与分页全部在 SQLite 内完成，不会把区间内日志整体读入应用内存。

## curl 示例

### 正常：上传并读回

```bash
# 生成一个合法的 gzip NDJSON（末行可以没有换行符）
printf '%s\n' \
  '{"id":"a","message":"hello"}' \
  '{"id":"b","message":"world"}' \
  '{"id":"c","message":"你好"}' \
  | gzip -n > batch.json.gz

# 上传（-x 让 gzip 原样作为请求体发送）
curl -sS -X POST http://localhost:8000/batches \
  --data-binary @batch.json.gz -H 'Content-Type: application/gzip'
# 201 -> {"batch_id":"…","line_count":3}

# 列出批次
curl -sS http://localhost:8000/batches

# 按批次 ID 读行（把 BID 换成上面返回的 batch_id）
BID=…
curl -sS "http://localhost:8000/batches/$BID/lines?limit=10&offset=0"
```

### 下载保存、解压核对、在既有上传限额内重新上传

```bash
# 先上传一个含引号、换行、中文等字符的批次
printf '%s\n' \
  '{"id":"a","message":"he said \"hi\"\nnext"}' \
  '{"id":"b","message":"世界 🌍"}' \
  '{"id":"c","message":"plain"}' \
  | gzip -n > batch.json.gz
BID=$(curl -sS -X POST http://localhost:8000/batches \
  --data-binary @batch.json.gz \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')

# 1) 保存下载：-O 按服务端给的文件名保存（batch-<id>.json.gz）
curl -sS -OJ "http://localhost:8000/batches/$BID/download"
ls -l "batch-$BID.json.gz"

# 也可显式指定保存路径
curl -sS "http://localhost:8000/batches/$BID/download" -o downloaded.json.gz

# 2) 核对响应头：application/gzip 且没有 Content-Encoding
curl -sS -D - -o /dev/null "http://localhost:8000/batches/$BID/download"
# Content-Type: application/gzip
# Content-Disposition: attachment; filename="batch-<id>.json.gz"
# （没有 Content-Encoding 头）

# 3) 校验 gzip 完整性（CRC32/结束尾部），再解压核对内容
gzip -t downloaded.json.gz && echo "gzip OK"
gzip -dc downloaded.json.gz
# {"id":"a","message":"he said \"hi\"\nnext"}
# {"id":"b","message":"世界 🌍"}
# {"id":"c","message":"plain"}

# 末行带换行；每行都恰好只有 id、message 两个键
gzip -dc downloaded.json.gz | tail -c 1 | od -An -c   # 应输出 \n
gzip -dc downloaded.json.gz | python3 -c '
import json,sys
for line in sys.stdin:
    o=json.loads(line); assert set(o)=={"id","message"}
print("all lines valid, keys exact")'

# 与库内分页读行逐值核对（seq/id/message 顺序与内容一致）
curl -sS "http://localhost:8000/batches/$BID/lines?limit=1000" \
  | python3 -c '
import json,sys,gzip
db=json.load(sys.stdin)["items"]
dl=[json.loads(l) for l in gzip.open("downloaded.json.gz","rt",encoding="utf-8")]
assert [(r["seq"],r["id"],r["message"]) for r in db] == \
       [(i+1,o["id"],o["message"]) for i,o in enumerate(dl)]
print("download matches database rows")'

# 4) 在既有上传限额内重新上传下载到的文件（下载为重新压缩，体积同样
#    受 2 MiB/8 MiB/64 KiB 限制约束；内容未超限即可直接作为新批次提交）
curl -sS -X POST http://localhost:8000/batches \
  --data-binary @downloaded.json.gz -H 'Content-Type: application/gzip'
# 201 -> {"batch_id":"…（新的批次 ID）","line_count":3}

# 5) 未知批次：响应开始前就是 404（不会先吐出 gzip 头再失败）
curl -sS -o /dev/null -w '%{http_code}\n' \
  http://localhost:8000/batches/deadbeef/download
```

### 检索日志：特殊字符、翻页间上传、重启后续页

```bash
# 上传两个都含 "error" 的批次（第二个故意在翻页之间上传，见下）
printf '%s\n' \
  '{"id":"a","message":"error 100% disk_full"}' \
  '{"id":"b","message":"error diskXfull"}' \
  | gzip -n > s1.json.gz
curl -sS -X POST http://localhost:8000/batches --data-binary @s1.json.gz

# 特殊字符检索：% 与 _ 是字面字符、大小写敏感、空白原样保留
# （--data-urlencode 负责 URL 编码，查询词里的空格、% 等原样送达）
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode 'q=error 100% disk_full' --data-urlencode 'page_size=10'
# 只命中 "error 100% disk_full"：_ 不会通配出 "diskXfull"，
# 小写查询也不会命中大写 "Error"。

# 翻页：首页响应带 next_cursor，续页只带 cursor 与 page_size
CUR=$(curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode 'q=error' --data-urlencode 'page_size=1' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["next_cursor"] or "")')

# 翻页之间上传新批次：旧游标继续翻页时看不到它
printf '%s\n' '{"id":"c","message":"error from new batch"}' | gzip -n > s2.json.gz
curl -sS -X POST http://localhost:8000/batches --data-binary @s2.json.gz
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$CUR" --data-urlencode 'page_size=1'
# 上面仍只返回首页查询时已提交批次范围内的结果（不含新批次）；
# 重新发起新查询才能看到新批次：
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode 'q=error' --data-urlencode 'page_size=10'

# 游标无状态：服务重启后仍可用同一个游标续页
docker compose restart api
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$CUR" --data-urlencode 'page_size=1'
```

### 区间重复消息：上传三个批次并统计

```bash
# 批一：aa 批内重复两次，bb 一次
printf '%s\n' \
  '{"id":"1","message":"aa"}' \
  '{"id":"2","message":"aa"}' \
  '{"id":"3","message":"bb"}' | gzip -n > d1.json.gz
B1=$(curl -sS -X POST http://localhost:8000/batches \
  --data-binary @d1.json.gz | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')

# 批二：aa 再出现一次（跨批重复），cc 只出现一次
printf '%s\n' \
  '{"id":"1","message":"aa"}' \
  '{"id":"2","message":"cc"}' | gzip -n > d2.json.gz
B2=$(curl -sS -X POST http://localhost:8000/batches \
  --data-binary @d2.json.gz | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')

# 批三：bb 跨批重复；"Aa" 与 "aa" 仅大小写不同，不合并
printf '%s\n' \
  '{"id":"1","message":"bb"}' \
  '{"id":"2","message":"Aa"}' | gzip -n > d3.json.gz
B3=$(curl -sS -X POST http://localhost:8000/batches \
  --data-binary @d3.json.gz | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')

# 闭区间 [B1, B3]：aa 3 次跨 2 批，bb 2 次跨 2 批；cc、Aa 各 1 次不返回
curl -sS -G http://localhost:8000/logs/duplicates \
  --data-urlencode "start_batch_id=$B1" \
  --data-urlencode "end_batch_id=$B3"
# {"total":2,"limit":100,"offset":0,"items":[
#   {"message":"aa","occurrences":3,"batch_count":2},
#   {"message":"bb","occurrences":2,"batch_count":2}]}

# 两端相同：只统计单个批次内部的重复（aa 在批一内出现 2 次）
curl -sS -G http://localhost:8000/logs/duplicates \
  --data-urlencode "start_batch_id=$B1" --data-urlencode "end_batch_id=$B1"

# 分页：occurrences 降序、BINARY 升序后取第 2 页
curl -sS -G http://localhost:8000/logs/duplicates \
  --data-urlencode "start_batch_id=$B1" --data-urlencode "end_batch_id=$B3" \
  --data-urlencode 'limit=1' --data-urlencode 'offset=1'

# 404：未知批次
curl -sS -o /dev/null -w '%{http_code}\n' -G http://localhost:8000/logs/duplicates \
  --data-urlencode "start_batch_id=$B1" --data-urlencode 'end_batch_id=deadbeef'

# 422：起点晚于终点（B3 比 B1 晚提交）
curl -sS -o /dev/null -w '%{http_code}\n' -G http://localhost:8000/logs/duplicates \
  --data-urlencode "start_batch_id=$B3" --data-urlencode "end_batch_id=$B1"

# 422：limit 越界
curl -sS -o /dev/null -w '%{http_code}\n' -G http://localhost:8000/logs/duplicates \
  --data-urlencode "start_batch_id=$B1" --data-urlencode "end_batch_id=$B1" \
  --data-urlencode 'limit=101'
```

### 删除批次、删除后旧游标续页、慢下载中删除

```bash
# 三个都含 "tok" 的批次，便于检索演示
printf '%s\n' \
  '{"id":"a1","message":"tok one"}' \
  '{"id":"a2","message":"tok two"}' | gzip -n > b1.json.gz
printf '%s\n' \
  '{"id":"b1","message":"tok three"}' \
  '{"id":"b2","message":"tok four"}' | gzip -n > b2.json.gz
printf '%s\n' '{"id":"c1","message":"tok five"}' | gzip -n > b3.json.gz
up() { curl -sS -X POST http://localhost:8000/batches --data-binary @"$1" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])'; }
B1=$(up b1.json.gz); B2=$(up b2.json.gz); B3=$(up b3.json.gz)

# 首页（每页 2 行）：tok 共 5 行，拿到 next_cursor
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode 'q=tok' --data-urlencode 'page_size=2'
# items = a1, a2；记下返回的 next_cursor
CUR='<上一步返回的 next_cursor>'

# 删除中间整批 B2，再用删除前的旧游标续页：自动跳过 B2
curl -sS -X DELETE "http://localhost:8000/batches/$B2"
# 200 -> {"batch_id":"<B2>","line_count":2}
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$CUR" --data-urlencode 'page_size=2'
# items = c1（B2 的 b1/b2 已被跳过，不报错、不重复），next_cursor = null

# 删除成功后：列表不含、按批次读 404、下载 404、检索统计不到
curl -sS http://localhost:8000/batches | grep -q "$B2" || echo 'not listed'
curl -sS -o /dev/null -w '%{http_code}\n' "http://localhost:8000/batches/$B2/lines"     # 404
curl -sS -o /dev/null -w '%{http_code}\n' "http://localhost:8000/batches/$B2/download"  # 404
curl -sS -G http://localhost:8000/logs/search --data-urlencode 'q=three'
# {"items":[],"next_cursor":null}

# 再删一次 / 删除未知 ID：404
curl -sS -o /dev/null -w '%{http_code}\n' -X DELETE "http://localhost:8000/batches/$B2"  # 404
curl -sS -o /dev/null -w '%{http_code}\n' -X DELETE \
  http://localhost:8000/batches/deadbeefdeadbeefdeadbeefdeadbeef                        # 404

# 删除最新批次 B3 后再上传：旧游标（范围上界固定在 B3 序号）不含新批次
curl -sS -X DELETE "http://localhost:8000/batches/$B3" >/dev/null
printf '%s\n' '{"id":"d1","message":"tok six after delete"}' | gzip -n > b4.json.gz
B4=$(up b4.json.gz)
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$CUR" --data-urlencode 'page_size=10'
# items 为空：CUR 停在 a1/a2 之后，B2/B3 已删，而 B4 的序号超出旧范围上界
curl -sS -G http://localhost:8000/logs/search \
  --data-urlencode 'q=after delete' --data-urlencode 'page_size=10'
# 新查询才看得到 B4

# 慢下载进行中删除：200 响应头已发出的下载仍是删除前的完整 gzip
# （先慢速下载，1 秒后在另一终端删除；下载不被打断，删除不等待）
curl -sS "http://localhost:8000/batches/$B1/download" --limit-rate 10k \
  -o before-delete.gz &
sleep 1
curl -sS -X DELETE "http://localhost:8000/batches/$B1"   # 立即 200，不等慢下载
wait
gzip -t before-delete.gz && gzip -dc before-delete.gz    # 完整：a1、a2 + 正确尾部
# 删除完成后新请求才是 404
curl -sS -o /dev/null -w '%{http_code}\n' "http://localhost:8000/batches/$B1/download"  # 404
```

### 失败示例

```bash
# 413：压缩体 > 2 MiB（或解压后 > 8 MiB、单行 > 64 KiB）
dd if=/dev/urandom bs=1M count=3 2>/dev/null | gzip -n | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：gzip 损坏
printf '\x1f\x8b\x08not-gzip' | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：空行
printf '{"id":"a","message":"m"}\n\n' | gzip -n | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：批内 id 重复
printf '%s\n' '{"id":"a","message":"1"}' '{"id":"a","message":"2"}' | gzip -n | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：gzip 后尾随数据
{ printf '%s\n' '{"id":"a","message":"m"}' | gzip -n; printf 'X'; } | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 404：读取未知批次
curl -sS -o /dev/null -w '%{http_code}\n' \
  http://localhost:8000/batches/does-not-exist/lines

# 422：违规续页游标（URL-safe Base64 编码的 JSON，字段不合法）
# 用 python3 构造游标；以下每种都应在执行检索前返回 422，而不是 500 或空页
mkcursor() { python3 -c '
import base64, json, sys
print(base64.urlsafe_b64encode(json.dumps(json.loads(sys.argv[1])).encode()).decode().rstrip("="))
' "$1"; }

# bound 超出 SQLite int64 上限（9223372036854775807）：修复前会 500
BAD=$(mkcursor '{"v":1,"q":"error","bound":9223372036854775808,"last":[0,0]}')
curl -sS -o /dev/null -w '%{http_code}\n' -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$BAD" --data-urlencode 'page_size=10'

# last[0] > bound：修复前会返回正常空页
BAD=$(mkcursor '{"v":1,"q":"error","bound":1,"last":[2,0]}')
curl -sS -o /dev/null -w '%{http_code}\n' -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$BAD" --data-urlencode 'page_size=10'

# v 不是非布尔整数 1（true；1.0、"1" 同理非法）
BAD=$(mkcursor '{"v":true,"q":"error","bound":1,"last":[0,0]}')
curl -sS -o /dev/null -w '%{http_code}\n' -G http://localhost:8000/logs/search \
  --data-urlencode "cursor=$BAD" --data-urlencode 'page_size=10'
```

任何失败的上传都不会产生可查询批次：随后 `GET /batches` 中不会出现它。

## 本地开发（不使用 Docker）

```bash
pip install -r app/requirements.txt
export DB_PATH=./data/batches.db
python -m app.init_db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 项目结构

```
app/
  config.py     # 字节限制等配置
  db.py         # SQLite 主库 + 每批次独立暂存库、原子发布/删除、旧表迁移、键集分页读取
  ingest.py     # 流式 gzip/NDJSON 解析器（有界内存、立即超限、整批原子）
  download.py   # 流式 gzip NDJSON 下载器（删除前快照、逐页读、逐块压缩、不攒整批）
  main.py       # FastAPI 路由
  init_db.py    # 数据库初始化入口
Dockerfile
docker-compose.yml
tests/check_api.py  # 端到端检查（需先启动服务）
```
