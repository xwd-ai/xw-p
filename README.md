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
demo/       演示应用：内容社区业务侧经 SDK 触发互动事件
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

完整的接入示例在 [`demo/`](demo/)：一个内容社区演示应用（创作者发帖、点赞/评论/@提及经 SDK 触发通知），含运行说明与走查命令。

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

回调是异步的，接收方没收到时要能回答「发过没有、发了什么、为什么失败」。引擎把每次发送尝试写成审计（含队列满导致的丢弃），并在入队前归档事件正文，因此可以按事件标识重放；重放保留原事件标识并标记 `replay`，也遵循原来的订阅筛选。开关、运维接口与事件总线的说明见 [docs/callbacks.md](docs/callbacks.md)。

## 鉴权

两种凭据，解析结果都是「租户 + 环境」：服务端用 `X-Api-Key`（`XW_API_KEYS` 声明租户、环境与可选的 `roles`），运营后台用 `Authorization: Bearer <JWT>`——`POST /v1/auth/token` 用管理员账号口令换取，有效期由 `XW_JWT_TTL` 决定。运维接口 `/v1/admin/*` 只接受后台令牌或带 `admin` 角色的 Key；未配置 `XW_JWT_SECRET` 的部署保持 demo 语义。配置、声明与状态码见 [docs/auth.md](docs/auth.md)。部署前的安全对照清单与已知限制见 [docs/security.md](docs/security.md)。

## 多租户

请求用 `X-Tenant-Id` 声明租户（缺省 `default`），数据按租户分区：默认租户沿用原路径，其他租户落在 `api/data/tenants/<租户>/`。API Key 用 `XW_API_KEYS` 声明绑定的租户与允许的环境，跨租户访问返回 403。`X-Environment` 是另一半边界：订阅者与工作流按环境各存一份，按标识访问另一套环境的记录返回 404，写操作只落在请求头指明的环境。解析规则、隔离范围与配置示例见 [docs/isolation.md](docs/isolation.md)。

## 限流

`/v1` 下的接口共用一层令牌桶限流：带有效凭据的请求按凭据计，其余按来源 IP 计，超出额度返回 429 并给出 `Retry-After` 与剩余额度。默认关闭，配置 `XW_RATE_LIMIT_KEY_RPM` / `XW_RATE_LIMIT_IP_RPM` 开启；多实例部署让后端跟随 Redis（`XW_RATE_LIMIT_BACKEND=auto` 默认如此），同一份额度才会被共享。后端不可用时放行并标 `X-RateLimit-Degraded`，不让限流自己变成故障点。维度选择、桶参数与部署注意事项见 [docs/rate-limit.md](docs/rate-limit.md)。

## 日志

API 与引擎默认输出一行一条 JSON（`XW_LOG_FORMAT=text` 换成文本），每个请求带 `X-Request-Id`：合规的外部标识原样沿用，否则生成新标识并回写响应头，同一次请求的访问日志与业务日志共用它。`XW_LOG_LEVEL` 调级别，`XW_LOG_ACCESS=0` 关掉访问日志。字段口径、级别映射与采集建议见 [docs/logging.md](docs/logging.md)。

## 指标与健康检查

两个进程都按 Prometheus 文本格式暴露 `GET /metrics`：请求量与耗时（按路由模板，不按原始路径）、事件受理、幂等结果、限流拒绝、队列深度，引擎侧还有按渠道与实现拆开的投递计数与耗时直方图。指标名与口径见 [docs/metrics.md](docs/metrics.md)。

存活与就绪分开：`/health/live` 只回答「进程还在跑」，`/health/ready` 逐项检查总线、存储与内置消费线程，依赖不可用时返回 503 并点名失败项。引擎对应 `/healthz` 与 `/readyz`。检查项与编排示例见 [docs/health.md](docs/health.md)。

引擎可以打开 `/debug/pprof/` 采样端点（`XW_PPROF=1`，默认只允许回环地址），CPU、堆、goroutine、锁竞争与阻塞都能抓。开关、采样率与实测结果见 [docs/profiling.md](docs/profiling.md)。

站内信收件箱文件是「一行一个版本」的追加流：改写一张聚合卡片只追加一版，读取按条目取最后一行，废弃行超过比例后再压缩。格式、多实例读取与实测吞吐见 [docs/inbox.md](docs/inbox.md)。

## API 文档

服务跑起来后，`/docs` 是 Swagger UI，`/redoc` 是 ReDoc，`/openapi.json` 是 schema 本体。schema 由 `api/app/openapi.py` 统一装配，而不只是 FastAPI 的自动生成结果：

- **分组**：13 个分组各有说明，按「读 → 写 → 运维」排列，Swagger 里不再是一长串按注册顺序堆叠的路径。
- **鉴权**：`X-Api-Key` 与 `Bearer` 令牌都声明为**可选**——`security` 同时给出「带 Key」「带令牌」与「匿名」三种备选，与「环境配置了凭据才校验」的实际行为一致。只写前者会让读者以为不带凭据一定失败；运维接口则不给匿名备选。
- **公共错误面**：读 `X-Environment` / `X-Tenant-Id` 的接口统一声明 400 / 401 / 403，并给出统一的 `Error` 响应体（`detail` 支持字符串与逐字段校验数组），调用方不必按状态码分两套解析。
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
- **可观测**：逐条执行明细 + 投递指标 + 带签名的事件回调（受理、逐渠道成败、聚合卡片状态、执行汇总四类，按订阅下发），每次回调尝试可审计、事件可重放；管理统计按渠道给出成功率与耗时分布、实现与工作流排行、失败原因榜单。执行明细可选文件或 Postgres 后端，后者与查询接口共用同一张表，见 [docs/execution-store.md](docs/execution-store.md)。
- **多租户**：`X-Tenant-Id` + 作用域 API Key，业务数据按租户分区，跨租户访问返回 403。
- **幂等**：`Idempotency-Key` 去重，多实例共享同一份 Redis 时同键并发只入队一次；键已占用而回执读不出来返回 409 且不入队。语义、at-most-once 窗口与并发验证见 [`docs/idempotency.md`](docs/idempotency.md)。
- **工作流治理**：模板可启停与复制。停用只翻开关、不推进版本号，单条触发、批量触发与分组广播统一拒绝新事件，已入队并携带快照的消息照常跑完；复制产出新模板（新步骤标识、`depends_on` 重映射、配置深拷贝），副本默认不启用。

实测吞吐 **约 53.8k 条/秒**（端到端 P50 400µs / P99 800µs），完整执行管线约 50k 条/秒。压测口径、参数与复现命令见 [`docs/benchmark.md`](docs/benchmark.md)。

## 测试

```bash
# Python：API 与渠道实现
api/.venv/bin/python -m pytest api/tests -q

# 覆盖率：低于门槛（默认 85%）时非零退出，口径见 docs/testing.md
api/.venv/bin/python scripts/coverage.py

# Python 客户端
api/.venv/bin/python -m pytest sdk/python/tests -q

# 演示应用（假传输层，不联网）
api/.venv/bin/python -m pytest demo -q

# Go 客户端
cd sdk/go && go test ./...

# Go：引擎（含并发路径的竞态检测）
cd engine && go test ./... && go test -race ./internal/provider/ ./internal/pipeline/ ./internal/runner/ ./internal/webhook/ ./internal/eventbus/ ./internal/admin/ ./cmd/engine/

# OpenAPI 导出文件与当前实现是否一致
api/.venv/bin/python scripts/export_openapi.py --check

# 前端：收件箱组件与运营后台的纯逻辑测试（Node 内置 runner，无第三方依赖）
node --test 'dashboard/tests/*.test.js'

# 端到端场景验收：自起 Redis / API / 引擎三个真实进程跑多场景链路
api/.venv/bin/python scripts/e2e_scenarios.py
```

CI（GitHub Actions）按同样的口径跑 lint 与上面这些测试。容器化部署与一键启动见 [docs/deploy.md](docs/deploy.md)。

## License

见 [LICENSE](LICENSE)。
