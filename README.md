# xw-notify

一个自主开发的内容社区互动触达平台（Community Engagement Notifications）。用统一的触发 API 和一套工作流模型，把点赞、评论、@提及等互动事件自动分发到创作者所在的各个渠道：收件箱、推送、邮件、短信。

## 定位

- **一套 API，多端触达**：社区业务侧只调一次触发接口，渠道选择、模板渲染、去重、聚合、重试都由平台处理。
- **为高频互动而生**：点赞是高频同质事件，一条爆款帖十分钟上千次点赞。平台按聚合键把同一篇帖子的互动压成一条，数字实时累加，而不是灌爆创作者的通知中心。
- **工作流驱动**：通知按「工作流（Workflow）+ 步骤（Step）」编排，支持收件箱、邮件、短信、推送、聊天、延时、摘要（Digest）等步骤类型。
- **订阅者与偏好**：每个创作者一个订阅者档案，按「全局 / 工作流 / 渠道」三级控制是否接收。
- **分组管理**：把创作者按主题归到分组（Topic），加入幂等、删除连带清理成员关系，分组与成员按环境隔离；按分组广播时一次请求展开成每人一条消息，收件地址取自各自的订阅者档案。
- **多语言实现**：API 与渠道集成层用 Python（FastAPI），执行引擎与调度用 Go。

## 典型互动场景

| 互动事件 | 触发标识 | 触达渠道 | 说明 |
| --- | --- | --- | --- |
| 帖子被点赞 | `post_liked` | 站内信 + 推送 | 按 `post_id` 聚合，10 分钟窗口合并为一条 |
| 帖子收到评论 | `post_commented` | 站内信 + 推送 + 邮件 | 评论内容逐条送达，不聚合 |
| 评论区被 @ | `post_mentioned` | 站内信 + 推送 | 即时提醒，延迟敏感 |

## 为什么聚合是核心

5000 次点赞不做聚合就是 5000 条通知，收件箱被刷爆、用户关掉通知；按聚合键合并后变成一条「小林等 128 人赞了你的帖子」，创作者愿意持续打开通知中心。这是整个平台最关键的设计取舍。

## 架构

```
内容社区业务 ──trigger──▶  api (Python/FastAPI)  ──入队──▶  MQ (Redis / RabbitMQ)
                                                          │
                                                          ▼
                                              engine (Go)
                                                · 工作流求值
                                                · digest 聚合
                                                · provider 分发
                                                          │
                       ┌──────────────┬──────────────┴──────────────┐
                       ▼              ▼                             ▼
                    Email/SMS/Push   In-App 收件箱               Chat(Slack…)
                          │              │
                          └──────┬───────┘
                                 ▼
                          Postgres（消息/创作者/工作流/活动日志）
```

## 目录结构

```
api/        Python FastAPI：触发 API、管理 API、渠道集成
engine/     Go：执行引擎、队列消费、provider 分发、digest、死信运维接口
dashboard/  原生 HTML/CSS/JS 展示前端（仪表盘 + 可视化工作流编排器 + 实时活动流 + 运营后台）
sdk/python/ Python 客户端：触发、订阅者与偏好接口，只依赖标准库
sdk/go/     Go 客户端：单条与批量触发，独立模块、只依赖标准库
infra/      docker-compose 与基础设施配置
docs/       文档
scripts/    演示与辅助脚本
```

## 快速开始

```bash
# 1. 启动依赖（Postgres / Redis / RabbitMQ）
docker compose -f infra/docker-compose.yml up -d

# 2. 启动 API（Python）
cd api
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --reload

# 3. 启动执行引擎（Go）
cd ../engine
go build -o bin/xw-engine ./cmd/engine && ./bin/xw-engine

# 4. 触发一条评论通知
curl -X POST http://127.0.0.1:8000/v1/events/trigger \
  -H "Content-Type: application/json" \
  -d '{"name":"post_commented","to":{"subscriberId":"creator_1","email":"hi@example.com","deviceToken":"fcm-token-9f2c1d","channels":["in_app","push","email"]},"payload":{"post_id":"P-2026-1001","post_title":"我的第一支 Vlog","actor":"小林","comment":"拍得真好，求教程！"}}'
```

前端直接打开 `dashboard/index.html` 即可看到内置示例数据；接真实后端时在页面注入 `window.XW_API_BASE` 指向 API 地址。页面与 API 通常不同源，服务端已开启 CORS（默认放行任意来源，需要收紧时用 `XW_CORS_ORIGINS` 给逗号分隔的白名单）。

服务端调用可以用 `sdk/python/` 里的客户端，它只依赖标准库，覆盖触发、批量触发、订阅者与发送偏好：

```python
from xw_notify import Client

client = Client("http://127.0.0.1:8000")
client.trigger.send("post_commented", "creator-1001", payload={"post_id": "P-2026-1001", "actor": "小林"})
```

可运行的示例在 `sdk/python/examples/`：点赞聚合与带幂等键的评论、三层发送偏好的设置与解析、收件箱读取与批量已读。在 `sdk/python` 目录下执行 `python -m examples.post_interaction`，用 `XW_BASE_URL` / `XW_API_KEY` / `XW_ENVIRONMENT` 指向别的部署。

Go 服务端可以用 `sdk/go/`（独立模块 `github.com/xwd-ai/xw-p/sdk/go`，只依赖标准库）：

```go
client, err := xwnotify.NewClient("http://127.0.0.1:8000", xwnotify.WithAPIKey(key))
if err != nil {
	return err
}
ack, err := client.Trigger(ctx,
	xwnotify.NewEvent("post_liked", "creator-1001").
		WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "小林"}),
	xwnotify.WithIdempotencyKey("post-liked-P-2026-1001"),
)
```

批量触发用 `client.TriggerBatch(ctx, events)`，逐条结果与请求等长且按下标对齐（被拒条目带原因）。

完整的 SDK 指南见 [`docs/sdk.md`](docs/sdk.md)：安装、单条与批量触发、订阅者与三层偏好、收件箱分页、错误排查表，以及各接口与两个客户端的对照。

### 错误分类与重试约定

两个 SDK 都按「接下来怎么办」给失败分类，而不是给每个状态码包一个类：

| 异常 | 典型场景 | 处理方式 |
| --- | --- | --- |
| `ValidationError`（本地校验、422） | 缺必填字段、聚合窗口写法不对、批量超 100 条 | 改参数再发 |
| `AuthenticationError` / `ForbiddenError`（401 / 403） | Key 没带、带错或越权 | 换凭据 |
| `NotFoundError`（404） | 工作流不存在或已停用、订阅者被删 | 不要重发 |
| `ConflictError`（409） | 订阅者标识重复、邮箱已被占用 | 读回既有记录 |
| `RateLimitError`（429）/ `ServerError`（5xx） | 服务端忙或过载 | 退避后重发 |
| `TransportError` | 连接失败、超时、读响应断流 | 可重发，但先确认请求可重放 |
| `ProtocolError` | 响应不是约定的形状（代理返回 HTML、版本不匹配） | 对齐版本，重发无用 |

「可以重发」不等于「会自动重发」。客户端默认一次请求就结束，打开重试要显式给策略；打开之后仍要**同时**满足两个条件才会重发：

1. 失败属于可重试的那几类（传输失败、429、5xx）。501/505 不在此列——它们不会因为再试一次而变好；
2. 请求本身可重放：`GET` / `HEAD` / `OPTIONS` / `PUT` / `DELETE` 天然幂等，`POST` 必须带幂等键。没有幂等键的 `POST` 永远只发一次，因为服务端无从判断这第二次是重发还是一条新评论。

退避是「指数 + 向下抖动」：第 n 次失败后等 `base × 2^(n-1)`，按抖动比例随机下压，封顶 `max_delay`；服务端给了 `Retry-After` 就以它为准（两类写法都支持：整数秒与 HTTP 日期），但封顶 60 秒，避免一个异常响应把调用方挂住。

```python
from xw_notify import Client, RetryPolicy, RateLimitError

client = Client("http://127.0.0.1:8000", retry=RetryPolicy(max_attempts=3))

try:
    client.trigger.send(
        "post_commented", "creator-1001",
        payload={"post_id": "P-2026-1001", "comment": "求教程"},
        idempotency_key="comment-C-8842",   # 没有它，这个 POST 不会被自动重发
    )
except RateLimitError as exc:
    print(exc.retry_after)                  # 服务端建议的等待秒数，没有则为 None
```

```go
client, err := xwnotify.NewClient("http://127.0.0.1:8000",
	xwnotify.WithRetry(xwnotify.DefaultRetryPolicy()),   // 3 次、200ms 起、5s 封顶
)
if err != nil {
	return err
}

ack, err := client.Trigger(ctx,
	xwnotify.NewEvent("post_commented", "creator-1001").
		WithPayload(map[string]any{"post_id": "P-2026-1001", "comment": "求教程"}),
	xwnotify.WithIdempotencyKey("comment-C-8842"),        // 同上：没有幂等键不重发
)
var apiErr *xwnotify.APIError
if errors.As(err, &apiErr) {
	log.Printf("触发失败：%d %s，建议等待 %s", apiErr.StatusCode, apiErr.Detail, apiErr.RetryAfter)
}
```

判定用类型而不是文案：Python 用 `isinstance` / `except`，Go 用 `errors.As` 与 `IsNotFound` / `IsValidation` / `IsUnauthorized` / `IsRetryable` 这些辅助函数。`should_retry`（Go 的 `IsRetryable`）只回答「这次失败值不值得重发」，`is_replay_safe` 只回答「这个请求重发安全不安全」，两件事分开之后，「要不要重试」在日志与代码里都能讲清楚。

运营后台在 `dashboard/admin.html`：按环境查看投递统计、渠道成功率、失败原因与活动汇总，启停或复制工作流，并用自定义 payload 对任一工作流做触发测试。触发之后回执下方会跟随该次触发的逐渠道投递明细（结果、实际实现、重试与回退、耗时、失败或跳过原因）——一条消息可能同时投往多个渠道，只有一个整体状态时看不出是哪一路出了问题。明细按「声明的渠道是否都有结果」判断是否取完，慢渠道没写出来时如实列出缺口而不是停在一个看着完整的数字上。

工作流编排页（`dashboard/index.html` 的「工作流编排」）是一个手写 SVG 编辑器：左侧选模板，中间画布按数组顺序把步骤排成执行链，右侧列出步骤并在选中后给出字段表单。添加步骤按当前选中位置插入，排序用行内上下按钮或直接拖拽列表，删除一个步骤会同时清掉其它步骤对它的 `depends_on` 引用（否则整份保存会被服务端以「依赖不存在」拒绝）。保存时按原样带上步骤标识，因此 `depends_on` 在多次保存之间保持有效；改步骤类型会保留依赖与模板，丢弃与类型无关的键（聚合键、延迟时长）。保存前先跑一遍与后端同源的校验：空步骤、缺少延迟时长、聚合窗口写法、悬空依赖与依赖环都会在发出请求之前指出具体是哪一步。

## 投递回调

引擎把执行过程中的事件 POST 到 `XW_CALLBACK_URL`。事件名同时放在 `X-XW-Event` 头与报文的 `event` 字段里，接收方不必读完整正文就能路由；事件标识在 `X-XW-Event-Id` 头与 `id` 字段里，回调没有「恰好一次」的保证（接收方超时、进程重启都会重发），按它去重。配置了 `XW_CALLBACK_SECRET` 时，报文带 `X-XW-Timestamp` 与 `X-XW-Signature` 两个头部（HMAC-SHA256，覆盖时间戳与正文），接收方据此判断报文是否被改过、是否被重放。

| 事件 | 时机 | 正文 |
| --- | --- | --- |
| `trigger.received` | 引擎取到消息、即将执行 | `trigger`：步骤数、涉及的渠道（去重、按步骤顺序，聚合步骤按它自己的卡片渠道计入）、版本、是否取自消息快照 |
| `delivery.sent` / `delivery.failed` | 单个渠道投递完成或失败 | `delivery`：步骤与类型、渠道、实际实现、尝试链路、是否回退、耗时、失败原因 |
| `digest.updated` | 聚合卡片被创建或被改写 | `digest`：聚合键（内容社区里是帖子 ID）、累计条数、是否新开卡、是否重放、窗口到期时刻 |
| `delivery.result` | 一次执行结束 | `status` + `summary` + `deliveries`：整体结果与全部渠道明细 |

同一条帖子被连续点赞时，逐次投递会收敛成同一张卡片，`digest.updated` 给出的是卡片当前的样子：

```json
{
  "id": "9f2c…",
  "event": "digest.updated",
  "occurred_at": "2026-11-05T02:31:08.412305+00:00",
  "trigger_id": "trig-7f31",
  "workflow": "post_liked",
  "subscriber_id": "creator_1001",
  "digest": {
    "step_id": "aggregate",
    "key": "P-2026-1001",
    "count": 3,
    "opened": false,
    "expires_at": "2026-11-05T02:41:08.000000+00:00"
  }
}
```

订阅哪些事件用 `XW_CALLBACK_EVENTS` 指定，逗号分隔，`all` 表示全部；不配置时只订阅 `delivery.result`。一次触发最多会产出「若干渠道事件 + 一条聚合事件 + 一条汇总」，全量订阅等于把回调量放大数倍，已有接收端没有必要为此改造。类型名写错时引擎不装配回调并在启动日志里说明原因——悄悄忽略会表现成「订阅了却收不到」，比起启动失败更难查。

### 审计与重放

回调是异步的：接收方没收到时的第一个问题是「到底发过没有、发了什么、为什么失败」。引擎把这两件事都落成可查的事实，路径由 `XW_CALLBACK_AUDIT_FILE` / `XW_CALLBACK_EVENT_FILE` 指定（写 `off` 关闭，默认在 `api/data/` 下）：

- **审计**：每一次发送尝试写一行 JSONL（`delivery.sent` 记一次成功、重试记每一次尝试），带事件标识、类型、目标、第几次尝试、状态码、错误原因、是否可重试、耗时与时间戳。队列满或订阅器关闭导致的丢弃同样记一行（`attempt=0`，理由是 `queue_full` / `closed`），否则「事件没发出去」在日志里会变成一段空白。
- **事件归档**：入队之前先把事件正文存一份，因此被丢弃的事件也能重放；归档只留最近若干条，超出上限的文件按行滚动。
- **运维接口**（监听地址 `XW_ADMIN_ADDR`，默认 `127.0.0.1:8081`）：`GET /admin/webhook/events` 按事件标识、类型、状态、时间区间翻页查审计；`POST /admin/webhook/replay` 传一组事件标识重新投递。重放保留原事件标识并把 `replay` 置为 `true`，接收方据此区分「补发」与「新事件」，不必自己记录哪条已经处理过。

重放遵循原来的订阅筛选：没订阅的类型即使存档里有，也不会因为重放而发出去——运维动作不该顺手改变接收方的流量口径。筛选掉的标识会出现在回执的 `filtered` 里，而不是静默消失。

## 事件总线

引擎内部的事件分发走一条进程内总线（`engine/internal/eventbus`）：`Publish` 只做非阻塞入队，每个订阅者一条独立队列与 goroutine，慢订阅者不会把发布方拖住；队列满时计 `dropped` 而不是无限堆积。主题支持 `*` 与 `delivery.*` 这类前缀通配，回调分发与「最近事件」环形缓冲都挂在同一条总线上，新增一个消费者只需要一次 `Subscribe`。

`GET /admin/events/recent` 返回最近若干条事件（新的在前），用于确认「刚才那条互动到底有没有产生事件」——比翻日志快，也避免为了排障把订阅者接到生产回调地址上。`Stats()` 给出按订阅者拆分的收到 / 丢弃 / 处理 / 失败计数，健康的判断标准是丢弃数长期为 0。

## 多租户

租户是比环境更高一层的数据边界：环境区分同一套业务数据的 dev / prod 副本，租户区分互不可见的客户，两者正交——一个租户有自己的 dev 与 prod。

请求用 `X-Tenant-Id` 声明租户（缺省 `default`）。标识的字符集收得很紧（小写字母、数字、连字符、下划线，≤32 位）：它会拼进数据文件路径，放开 `..` 与 `/` 就能跳出数据目录。写错或不在 `XW_TENANTS` 清单内直接 400，而不是悄悄退回默认租户——静默落到别人的数据里比起报错难查得多。清单里没有 `default` 时（只服务少数客户是常见部署），不带租户头会返回 400 并列出可用租户：这时缺的是头，不是标识写错，报「未知租户：default」只会让人去改一个没错的值。

数据按租户分区，默认租户沿用原路径，升级不必搬迁：

| 数据 | 默认租户 | 其他租户 |
| --- | --- | --- |
| 订阅者 / 分组 / 偏好 / 已读状态 | `api/data/subscribers.jsonl` 等 | `api/data/tenants/<租户>/subscribers.jsonl` 等 |
| 工作流、幂等键、活动与执行明细 | 按 `tenant` 过滤；Postgres 用 `tenant` 列 + `COALESCE(tenant,'default')` 兼容老数据 | 同左 |
| 收件箱 | 一个追加文件，条目带 `tenant` / `environment` 字段，读取时按两者过滤 | 同左 |

API Key 是有作用域的。`XW_API_KEYS` 用 JSON 数组声明 Key 绑定的租户与允许的环境：

```json
[
  {"key": "sk_live_acme", "tenant": "acme", "environments": ["production"], "label": "社区主站"},
  {"key": "sk_test_acme", "tenant": "acme", "environments": ["development"]}
]
```

配置之后：不带 Key 或 Key 不认识返回 401；Key 不覆盖当前环境也返回 401——与「Key 不存在」同一口径，否则调用方能靠状态码区分「这个 Key 是真的、只是用错了环境」，等于给了凭证探测的信号。Key 命中但请求头指明了另一个租户返回 403：这是拿自己的凭据访问别人的数据，属于越权。没配置任何 Key 时保持 demo 语义，租户由请求头决定，本地直接可用。

旧的按环境 Key（`XW_API_KEY_DEVELOPMENT` / `XW_API_KEY_PRODUCTION`）升级后行为不变：它们被折算成「绑定默认租户、只允许自己那个环境」的作用域，与显式声明的 Key 合并成同一张表。

运营后台不单独做租户切换：地址栏带 `?tenant=acme` 进一次，后续页面沿用同一个租户（记在 `localStorage`）。切租户等于换一套数据，重新进一个链接比在页面里留一个能改全局状态的下拉框更不容易出错。

隔离的边界要说清楚：租户覆盖的是业务数据（触发、订阅者、偏好、分组、工作流、活动与执行明细、收件箱）。渠道配置与投递汇总（`GET /v1/providers`、`GET /v1/dashboard/stats`）仍是部署级——一套部署一套 SMTP/SMS 凭据，把它们做进租户意味着每个租户都要单独配置通道，那是另一件事。

## API 文档

服务跑起来后，`/docs` 是 Swagger UI，`/redoc` 是 ReDoc，`/openapi.json` 是 schema 本体。schema 由 `api/app/openapi.py` 统一装配，而不只是 FastAPI 的自动生成结果：

- **分组**：12 个分组各有说明，按「读 → 写 → 运维」排列，Swagger 里不再是一长串按注册顺序堆叠的路径。
- **鉴权**：`X-Api-Key` 声明为**可选**——`security` 同时给出「带 Key」与「匿名」两种备选，与「环境配置了 Key 才校验」的实际行为一致。只写前者会让读者以为不带 Key 一定失败。
- **公共错误面**：读 `X-Environment` / `X-Tenant-Id` 的接口统一声明 400（环境头或租户写错、参数冲突）、401（Key 不匹配或不适用于该环境）与 403（跨租户越权），并给出可解析的 `Error` 响应体：校验失败时 `detail` 是逐字段的问题数组，业务错误时是字符串，调用方不必按状态码分两套解析。
- **操作标识**：由方法与路径推导，例如 `GET /v1/admin/activity/{trigger_id}` → `getV1AdminActivityByTriggerId`。函数名是实现细节，重构时不该让调用方生成的 SDK 方法名跟着变。
- **示例**：触发、订阅者、偏好、分组与工作流的请求体都带可直接提交的示例；示例本身在测试里跑过一遍（含地址规范化与工作流结构校验），不会出现「从文档复制一段就 422」。

`docs/openapi.json` 是同一份内容的导出，让接口变更能在合并请求里被 diff 出来：

```bash
# 重新导出
api/.venv/bin/python scripts/export_openapi.py

# 只校验：代码改了却忘了导出时退出码为 1
api/.venv/bin/python scripts/export_openapi.py --check
```

导出按路径与 JSON 键排序，同一份代码重复导出的字节一致。独立域名部署时用 `--server-url https://notify.example.com`（或环境变量 `XW_OPENAPI_SERVER_URL`）写入 `servers`，Swagger 的「Try it out」才不会打到本地。

## 能力亮点

- **可靠投递**：至少一次语义——消息取出即登记在途、处理成功才确认，租约过期的消息由回收流程重新入队，进程崩溃不丢消息。
- **并发消费**：可配置的 worker pool，退出时等待在途消息处理完毕。
- **失败重试与死信**：指数退避 + 抖动，重试耗尽进入死信，运维接口可查询、重放或丢弃。
- **延迟任务持久化**：delay 步骤存 Redis，重启或换实例都能接上。
- **聚合**：digest 计数并发原子累加，窗口到期重开卡。
- **多渠道**：收件箱 / 邮件 / 短信 / 推送 / 群播报，主备回退、渠道级重试。
- **可观测**：逐条执行明细 + 投递指标 + 带签名的事件回调（受理、逐渠道成败、聚合卡片状态、执行汇总四类，按订阅下发）；回调的每次尝试都留审计、事件正文可归档并按标识重放，进程内事件总线按主题订阅、慢消费者只丢自己的队列；管理统计按渠道给出成功率与耗时分布、实现与工作流排行、失败原因榜单。
- **多租户**：`X-Tenant-Id` + 带作用域的 API Key，订阅者、分组、偏好、工作流、活动与执行明细、收件箱按租户分区，跨租户访问返回 403；默认租户沿用原路径，升级不搬数据。
- **工作流治理**：模板可启停与复制。停用只翻开关、不推进版本号，单条触发、批量触发与分组广播统一拒绝新事件，已入队并携带快照的消息照常跑完；复制产出新模板（新步骤标识、`depends_on` 重映射、配置深拷贝），副本默认不启用。

实测吞吐 **约 53.8k 条/秒**（端到端 P50 400µs / P99 800µs），完整执行管线约 50k 条/秒。压测口径、参数与复现命令见 [`docs/benchmark.md`](docs/benchmark.md)。

## 测试

```bash
# Python：API 与渠道实现
api/.venv/bin/python -m pytest api/tests -q

# Python 客户端
api/.venv/bin/python -m pytest sdk/python/tests -q

# Go 客户端
cd sdk/go && go test ./...

# Go：引擎（含并发路径的竞态检测）
cd engine && go test ./... && go test -race ./internal/provider/ ./internal/pipeline/ ./internal/runner/ ./internal/webhook/ ./internal/eventbus/ ./internal/admin/ ./cmd/engine/

# OpenAPI 导出文件与当前实现是否一致
api/.venv/bin/python scripts/export_openapi.py --check

# 前端：收件箱组件与运营后台的纯逻辑测试（Node 内置 runner，无第三方依赖）
node --test 'dashboard/tests/*.test.js'
```

## License

见 [LICENSE](LICENSE)。
