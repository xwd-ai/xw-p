# SDK 使用指南

两个客户端覆盖同一套 HTTP 接口，各自只依赖标准库，可以原样拷进项目：

| 语言 | 位置 | 覆盖范围 | 模块 |
| --- | --- | --- | --- |
| Python | `sdk/python/xw_notify` | 单条触发、批量触发、订阅者、发送偏好，另有 `request()` 兜底调用未封装的接口 | 包名 `xw_notify` |
| Go | `sdk/go/xwnotify` | 单条触发、批量触发 | 模块 `github.com/xwd-ai/xw-p/sdk/go` |

两者遵守同一组约定，换语言不必重新学一遍：

- **环境标识始终显式发送**（`X-Environment`），不依赖服务端默认值。运维切了环境却以为在看另一套数据，是这类客户端最常见的事故。
- **幂等键由调用方生成**（`Idempotency-Key`）。通知系统里重发一次 POST 就是多一条站内信，服务端无从判断这第二次是重发还是一条新评论。
- **错误按「接下来怎么办」分层**：改参数、换凭据、读回既有记录、退避重发、对齐版本。分类见 [错误与重试](#错误与重试)。
- **重试默认关闭**，打开后也只重发「值得重发的失败」且「本身可重放的请求」。
- **`payload` / `overrides` 的键名一个都不改写**。`post_id` 与 `postId` 同时出现是调用方的业务数据，通用批量改名会把它们改坏。

## 安装

Python 侧没有依赖，装进当前环境即可：

```bash
pip install -e sdk/python
```

Go 侧是独立模块。仓库内引用时用 `replace` 指向本地路径，避免依赖尚未打 tag 的版本：

```text
// go.mod
require github.com/xwd-ai/xw-p/sdk/go v0.0.0

replace github.com/xwd-ai/xw-p/sdk/go => ../xw-p/sdk/go
```

根地址只写到主机与端口（可带路径前缀），不要带 `/v1`——接口路径由 SDK 拼接。

## 快速开始

### Python

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000", environment="development")

ack = client.trigger.send("post_liked", "creator-1001", payload={"post_id": "P-2026-1001"})
print(ack.trigger_id, ack.acknowledged)
```

构造参数里三个最常改的：`api_key`（服务端为该环境配了 Key 才需要）、`environment`（`development` / `production`）、`timeout`（默认 10 秒）。

### Go

```go
client, err := xwnotify.NewClient("http://127.0.0.1:8000", xwnotify.WithEnvironment("development"))
if err != nil {
	return err
}

ack, err := client.Trigger(ctx, xwnotify.NewEvent("post_liked", "creator-1001").
	WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "小林"}))
if err != nil {
	return err
}
log.Println(ack.TriggerID)
```

`NewClient` 会把非法取值挡在构造期（地址缺协议头、环境为空、重试策略参数倒挂），而不是等第一次请求才以一句难懂的报文失败。构造完成后客户端只读，可被多个 goroutine 同时使用。

## 触发互动事件

### 点赞：把聚合键放进 payload

`post_liked` 模板的聚合步骤按 `payload.post_id` 归并：同一篇帖子的多次点赞在窗口内累加成一张卡片。所以 `post_id` 必须带，且要与帖子详情页用的是同一个值——换了写法会变成两张卡。

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")

ack = client.trigger.send(
    "post_liked",
    "creator-1001",
    payload={"post_id": "P-2026-1001", "post_title": "我的第一支 Vlog", "actor": "小林"},
)
print(ack.trigger_id)
```

### 评论：多渠道与幂等键

`channels` 留空表示由工作流的步骤决定投哪些渠道；给了列表就只投其中几路。订阅者没绑定的渠道（没有邮箱却要求发邮件）会被工作流侧跳过并写进执行明细，不会让触发失败。

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")

ack = client.trigger.send(
    "post_commented",
    "creator-1001",
    payload={
        "post_id": "P-2026-1001",
        "post_title": "我的第一支 Vlog",
        "actor": "阿澈",
        "comment": "拍得真好，求教程！",
    },
    channels=["in_app", "push", "email"],
    idempotency_key="comment-C-8842",
)
print(ack.trigger_id)
```

幂等键的取值要有业务含义（评论 ID、事件 ID 皆可），服务端只做字符串匹配：同一个键第二次到达时返回首次回执，不会再入队一条。

### @提及：批量提交

一次请求提交同一条热帖下的多条提及。批量接口的逐条结果与请求**等长且按下标对齐**，某条的工作流不存在或未启用只会让该条记为 `rejected`，同批其余照常入队——所以务必按下标对齐，不要依次取「成功的那几条」。

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")

events = [
    client.trigger.build("post_mentioned", "creator-1001", payload={"post_id": "P-2026-1002", "actor": "小林"}),
    client.trigger.build("post_mentioned", "creator-1002", payload={"post_id": "P-2026-1002", "actor": "小林"}),
]
batch = client.trigger.send_batch(events, idempotency_key="mention-P-2026-1002")

for index, item in enumerate(batch.results):
    print(index, item.status, item.trigger_id or item.error)
```

Go 侧同构，`TriggerBatch` 在本地就拒绝空批、超过 100 条以及任一条缺必填项，错误里带「第 N 条事件」：

```go
events := []xwnotify.Event{
	xwnotify.NewEvent("post_mentioned", "creator-1001").WithPayload(map[string]any{"post_id": "P-2026-1002"}),
	xwnotify.NewEvent("post_mentioned", "creator-1002").WithPayload(map[string]any{"post_id": "P-2026-1002"}),
}
batch, err := client.TriggerBatch(ctx, events, xwnotify.WithIdempotencyKey("mention-P-2026-1002"))
if err != nil {
	return err
}
for _, item := range batch.Results {
	if !item.Accepted() {
		log.Printf("第 %d 条被拒：%s", item.Index, item.Error)
	}
}
```

## 订阅者与收件地址

触发只需要一个订阅者标识，但能不能投到邮件、推送，取决于这个订阅者绑了哪些地址、开了哪些渠道。

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")

sub = client.subscribers.create(
    subscriber_id="creator-1001",
    email="creator@example.com",
    channels=["in_app", "email"],
)

client.subscribers.update("creator-1001", device_token="fcm-token-9f2c1d")   # 只补设备令牌
client.subscribers.update("creator-1001", email=None)                        # 解绑邮箱
client.subscribers.update("creator-1001", channels=[])                       # 关闭全部渠道

print(sub.id, sub.bindings)
```

`update` 区分三种意图，这是它唯一需要留神的地方：

| 写法 | 含义 |
| --- | --- |
| 不传该字段 | 保持原值 |
| 传 `None` | 解绑该地址（`email` / `phone` / `device_token`） |
| `channels=[]` | 关闭该订阅者的全部渠道，之后任何投递都会被跳过 |
| `channels=None` | 报错——它既不是「不改」也不是「全关」，猜一个会让调用方以为自己做了一件其实没做的事 |

标识重复、邮箱已被别人占用返回 `ConflictError`；标识不存在返回 `NotFoundError`。

翻页交给 `iter()`，它跟着游标走完全部，取完一页才请求下一页：

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")

for sub in client.subscribers.iter(channel="in_app", page_size=50):
    print(sub.id, sub.environment, sub.channels)
```

## 发送偏好

偏好有三层：`channel` → `workflow` → `global`。解析时从最具体的一层往上找，命中即止；一层都没记录过就按「接收」处理。`allow` / `deny` 是 `set(enabled=True/False)` 的写法糖，省掉那个容易漏掉的布尔值。

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")
prefs = client.subscribers.preferences

prefs.deny("creator-1001", "global")                              # 全部关闭
prefs.allow("creator-1001", "workflow", target="post_commented")   # 评论照旧发
prefs.deny("creator-1001", "channel", target="push")               # 但别推手机

decision = prefs.resolve("creator-1001", workflow="post_commented", channel="push")
print(decision.allowed, decision.scope, decision.reason)

prefs.delete("creator-1001", scope="channel", target="push")
print(len(prefs.list("creator-1001")))
```

`resolve()` 的返回值可以直接当布尔用（`if decision:` 即「会发出」），`scope` 与 `reason` 回答「是哪一层决定的、为什么」——排查「这条通知怎么没发」时，这句话比反复读偏好表快。`list()` 只返回**显式设置过**的记录：某个层级没出现，表示用户没表过态，与「明确设成接收」是两件事。

## 收件箱

收件箱的三个接口 SDK 没有单独建模（字段还会随展示需求变化），走 `request()` 兜底入口即可，路径与参数与 HTTP 接口一致：

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")

page = client.request("GET", "/v1/inbox", query={"subscriberId": "creator-1001", "limit": 20})
print(page["unread"], page["total"], len(page["items"]))

ids = [item["message_id"] for item in page["items"] if not item["read"]]
if ids:
    client.request("POST", "/v1/inbox/read", json_body={"subscriberId": "creator-1001", "messageIds": ids})
```

分页语义：`items` 是本页消息，`unread` / `total` 覆盖该订阅者的全部消息、不受 `limit` 影响，`matched` 是本次筛选命中的条数，`hasMore` 与 `nextCursor` 用于继续翻页。同一张聚合卡片无论被多少次互动改写都只算一条，所以 `unread` 不会因为点赞数涨到 99 而变成 99。

## 错误与重试

完整的分类表与退避规则见 [README 的错误分类与重试约定](../README.md#错误分类与重试约定)。使用上的三条要点：

1. **判定用类型，不用文案**。Python 用 `except` / `isinstance`，Go 用 `errors.As` 与 `IsNotFound` / `IsValidation` / `IsRetryable` 这些辅助函数。
2. **`ValidationError` 是本地错误**，请求根本没发出去，改参数就能解决；`TransportError` 表示没拿到服务端结论，请求可能已经被处理。
3. **`429` / `5xx` 重发有用，`501` / `505` 没有**——后者不会因为再试一次而变好，客户端刻意不把它们算进可重试集合。

```python
from xw_notify import Client, ConflictError, NotFoundError

client = Client("http://127.0.0.1:8000")

try:
    client.subscribers.create(subscriber_id="creator-1001", email="creator@example.com", channels=["in_app"])
except ConflictError:
    sub = client.subscribers.get("creator-1001")
    print("已存在", sub.id)
except NotFoundError as exc:
    print("不存在", exc.status, exc.path)
```

打开重试要显式给策略，写操作还要带幂等键，否则这个 POST 只发一次：

```python
from xw_notify import Client, RetryPolicy


def log_retry(exc, attempt, delay):
    print(f"第 {attempt} 次失败，{delay:.2f}s 后重发：{exc}")


client = Client(
    "http://127.0.0.1:8000",
    retry=RetryPolicy(max_attempts=3, base_delay=0.2, max_delay=5.0, jitter=0.3),
    on_retry=log_retry,
)

ack = client.trigger.send(
    "post_commented", "creator-1001",
    payload={"post_id": "P-2026-1001"},
    idempotency_key="comment-C-8842",   # 没有它，这个 POST 不会被自动重发
)
print(ack.trigger_id)
```

```go
client, err := xwnotify.NewClient("http://127.0.0.1:8000",
	xwnotify.WithRetry(xwnotify.DefaultRetryPolicy()), // 3 次、200ms 起、5s 封顶
)
if err != nil {
	return err
}

ack, err := client.Trigger(ctx,
	xwnotify.NewEvent("post_commented", "creator-1001").
		WithPayload(map[string]any{"post_id": "P-2026-1001"}),
	xwnotify.WithIdempotencyKey("comment-C-8842"),
)
var apiErr *xwnotify.APIError
if errors.As(err, &apiErr) {
	log.Printf("触发失败：%d %s，建议等待 %s", apiErr.StatusCode, apiErr.Detail, apiErr.RetryAfter)
}
```

Go 侧的重试等待可被 `ctx` 打断，取消时返回最后一次失败，而不是一个「等待被取消」的新错误——调用方要的是哪次请求没成。

## 排查表

| 现象 | 大概率原因 | 动作 |
| --- | --- | --- |
| 404 且文案是「工作流不存在或已停用」 | 名字拼错，或模板被停用 | 到后台看启停状态；停用不会删除已入队的消息 |
| 404 且路径里带订阅者标识 | 订阅者不存在，或不在当前环境 | 确认 `environment` 与订阅者归属一致 |
| 409 | 订阅者标识已存在，或邮箱被别的标识占用 | 读回既有记录；标识不同却撞邮箱说明是两条人 |
| 422 且 `detail` 是数组 | 字段类型或取值不对 | 按 `loc` 定位字段，如 `to.channels: 不是合法渠道` |
| 页面里所有请求都报 `Failed to fetch` | 前端与 API 不同源，且白名单没放行 | 服务端用 `XW_CORS_ORIGINS` 收紧时，要把前端所在地址加进逗号分隔的白名单（默认放行任意来源） |
| 3xx 报错并提示「不跟随重定向」 | 根地址写错（缺 `/v1` 之外的路径前缀或协议） | 检查 `base_url`，SDK 不会把 `X-Api-Key` 带到别的地址 |
| 投递记录里某渠道是「跳过」 | 订阅者没绑地址、关了渠道，或偏好拦下 | 读 `resolve()` 的 `scope` / `reason` |
| 收件箱里点赞是一串独立消息 | 触发时没带 `payload.post_id` | 聚合步骤按它归并，缺了就没有聚合键 |
| `ProtocolError` | 响应形状不对（代理返回 HTML、版本不匹配） | 对齐版本；重发无用 |

## 示例与测试

Python 侧的三段可运行示例按调用方的实际场景组织，而不是按接口罗列：

```bash
cd sdk/python
python -m examples.post_interaction        # 点赞聚合 → 带幂等键的评论 → 批量 @提及
python -m examples.subscriber_preferences  # 三层偏好的设置与逐条解析
python -m examples.inbox_review            # 收件箱分页、按工作流的未读分布、批量已读
```

用 `XW_BASE_URL` / `XW_API_KEY` / `XW_ENVIRONMENT` 指向别的部署。Go 侧的可执行示例在 `sdk/go/xwnotify/example_test.go`，随 `go test` 一起跑。

```bash
# Python 客户端（含文档示例的可执行性检查）
api/.venv/bin/python -m pytest sdk/python/tests -q

# Go 客户端
cd sdk/go && go test ./...
```

## 接口对照

| HTTP | Python | Go |
| --- | --- | --- |
| `POST /v1/events/trigger` | `client.trigger.send(...)` | `client.Trigger(ctx, event, opts...)` |
| `POST /v1/events/trigger/batch` | `client.trigger.send_batch(events)` | `client.TriggerBatch(ctx, events, opts...)` |
| `GET /v1/subscribers` | `client.subscribers.list(...)` / `.iter(...)` | — |
| `POST /v1/subscribers` | `client.subscribers.create(...)` | — |
| `GET/PUT/DELETE /v1/subscribers/{id}` | `client.subscribers.get/update/delete` | — |
| `GET /v1/subscribers/{id}/preferences` | `client.subscribers.preferences.list(...)` | — |
| `GET .../preferences/resolve` | `client.subscribers.preferences.resolve(...)` | — |
| `PUT /v1/subscribers/{id}/preferences` | `.set` / `.allow` / `.deny` | — |
| `DELETE /v1/subscribers/{id}/preferences` | `.delete` / `.clear` | — |
| `GET /health` | `client.health()` | — |
| 其余接口 | `client.request(method, path, ...)` | 用 `net/http` 直连 |

Go 客户端只覆盖触发：通知的发送方通常是业务后端，订阅者与偏好的管理多由运营后台完成，把这两块做进 Go 包只会让第一个用到它的人多背一份没人维护的代码。
