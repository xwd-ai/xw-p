# 日志

API 与引擎默认都输出**一行一条 JSON**，字段平铺、可直接被采集端解析。本地想读
顺眼的一行时把 `XW_LOG_FORMAT` 设为 `text`，两种格式共用同一份日志内容，切换只
影响呈现。

## 为什么默认 JSON

日志最终要进集中式存储：找「某个请求的全部记录」、按 `status` 统计 5xx、按
`duration_ms` 取分位，这些操作在纯文本里只能靠正则猜。文本格式的整齐在按字段
过滤时没有任何帮助，因此把它降为可选项而不是默认值。

## 字段口径

API 侧固定字段：

| 字段 | 说明 |
| --- | --- |
| `ts` | UTC 时间，ISO 8601，毫秒精度（`2026-11-05T07:34:08.473Z`） |
| `level` | 小写级别（`info` / `warning` / `error`） |
| `request_id` | 请求标识，见下一节 |
| `tenant` | `X-Tenant-Id` 的值，未带或不合规时不出现 |
| `environment` | `X-Environment` 的值，同上 |
| `logger` | 产生日志的 logger 名（`xw.api`、`xw.access`、`xw.ratelimit` 等） |
| `message` | 人读的文案 |
| `event` | 机器读的事件名，访问日志为 `http.request` |

访问日志另带 `method` / `path` / `status` / `duration_ms` / `client`。异常日志带
`exc_type` 与 `exc`（完整堆栈）。

引擎侧由 `slog` 输出，字段为 `time` / `level` / `msg` 加业务键值（`triggerId`、
`worker`、`queue` 等）。`time.Duration` 会转成 `"2s"` 这样的字符串，而不是
`2000000000` 这样的纳秒整数——读的人不必再换算。

## 请求标识

- 客户端可以带 `X-Request-Id`。只接受 `[A-Za-z0-9._:-]{1,64}`：它会进响应头与
  日志，换行符能伪造出额外的日志行，过长会让每一行都变胖。
- 合规的原样沿用（跨服务串调用时由上游生成）；不合规或缺失时生成
  `req-<16 位十六进制>`。不做「清洗后照用」：去掉非法字符可能把两个不同的标识
  改成同一个，重试与重放的日志就分不开了。
- 服务端**始终回写** `X-Request-Id`，正常响应、4xx、429、5xx 都带。客户端报障
  时把这个值贴过来，就能定位到那一行访问日志和同一次请求的全部业务日志。
- 标识存在 `contextvars` 里，同步路由（FastAPI 放进线程池执行）同样能读到；
  请求结束即还原，上一请求的标识不会粘到下一请求。

## 访问日志

每个 HTTP 请求写一行，级别跟着状态码走，便于采集端按级别分告警：

| 状态码 | 级别 | 说明 |
| --- | --- | --- |
| 2xx / 3xx | `info` | 正常 |
| 4xx | `warning` | 调用方问题（404、401、422、429） |
| 5xx | `error` | 本服务问题，带堆栈 |

`/health` 不写访问日志：负载均衡与编排的探针按秒级频率打，这类请求会把真正
需要排查的日志淹没。探针失败会以非 200 出现在监控里，不会因为少了日志而看不见。

## 未捕获异常

未捕获异常在中间件里**就地转成 500**，而不是抛给 Starlette 的
`ServerErrorMiddleware`：那一层在中间件之外，它生成的响应不经过我们的 `send`，
`X-Request-Id` 就补不上了。响应体是 `{"detail": "服务器内部错误", "requestId": ...}`，
异常文案不外泄（可能带内部路径、SQL 片段或第三方返回），完整堆栈写进日志。

已经发出响应头的流式请求无法再改写状态码，这种情况照常向上抛。

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `XW_LOG_FORMAT` | `json` | `json` 或 `text`，其它取值按 `json` 处理 |
| `XW_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR`，写错回退 `INFO` |
| `XW_LOG_ACCESS` | 开 | 设为 `0` 关掉访问日志（压测或本机调试嫌吵时） |

这三个变量 API 与引擎共用同名约定（引擎侧只有前两个）；两个进程默认值一致，
避免「同名的变量在两处套用不同默认值」。

## 不记录的内容

请求体、`X-Api-Key`、`Authorization` 头、查询串都不进日志。触发接口的
`payload` 里可能带用户内容，落盘后很难收回；需要排查内容问题时按
`triggerId` 去活动日志与执行明细里查，那两处有明确的保留策略。

路径本身会截断到 512 字符，避免超长 URL 撑大日志。

## 关联一次触发的全链路

| 位置 | 关联字段 |
| --- | --- |
| API 访问日志 / 业务日志 | `request_id` |
| 触发响应与活动日志 | `triggerId`（等于 `acknowledgmentId`） |
| 引擎消费与投递日志 | `triggerId` |
| 执行明细 / 回调事件 | `triggerId` / `event_id` |

HTTP 请求的 `request_id` 与业务侧的 `triggerId` 是两个层次：前者标识一次调用，
后者标识一次通知。一次批量触发只有一个 `request_id`，但会产生多个 `triggerId`。

## 验证

```bash
cd api
env -u HTTP_PROXY -u HTTPS_PROXY .venv/bin/python -m pytest tests/test_logging.py -q

# 真实进程：每条日志应能解析成 JSON，响应头回写同一个标识
XW_API_WORKER=0 .venv/bin/python -m uvicorn app.main:app --port 8399
curl -si -H 'X-Request-Id: trace-1' http://127.0.0.1:8399/v1/workflows | head -8

cd ../engine
XW_LOG_FORMAT=json go run ./cmd/engine
```
