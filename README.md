# xw-notify

一个自主开发的内容社区互动触达平台（Community Engagement Notifications）。用一个统一的触发 API 和一套工作流模型，把点赞、评论、@提及等互动事件自动分发到创作者所在的各个渠道：收件箱、推送、邮件、短信。

## 定位

- **一套 API，多端触达**：社区业务侧只调用一次触发接口，剩下的渠道选择、模板渲染、去重、聚合、重试都由平台处理。
- **为高频互动而生**：点赞是「高频同质」事件，一条爆款帖子十分钟能产生上千次点赞。平台按聚合键把同一篇帖子的互动压成一条，数字实时累加，而不是灌爆创作者的通知中心。
- **工作流驱动**：通知按「工作流（Workflow）+ 步骤（Step）」编排，支持收件箱、邮件、短信、推送、聊天、延时、摘要（Digest）等步骤类型。
- **订阅者与偏好**：每个创作者一个订阅者档案，按「全局 / 工作流 / 渠道」三级控制是否接收。
- **多语言实现**：API 与渠道集成层用 Python(FastAPI)，执行引擎与调度用 Go。

## 典型互动场景

| 互动事件 | 触发标识 | 触达渠道 | 说明 |
| --- | --- | --- | --- |
| 帖子被点赞 | `post_liked` | 站内信 + 推送 | 按 `post_id` 聚合，10 分钟窗口合并为一条 |
| 帖子收到评论 | `post_commented` | 站内信 + 推送 + 邮件 | 评论内容逐条送达，不聚合 |
| 评论区被 @ | `post_mentioned` | 站内信 + 推送 | 即时提醒，延迟敏感 |

## 为什么聚合是核心

| 不做聚合 | 做了聚合 |
| --- | --- |
| 5000 次点赞 = 5000 条通知 | 5000 次点赞 = 1 条「小林等 128 人赞了你的帖子」 |
| 收件箱被刷爆，用户关掉通知 | 创作者愿意持续打开通知中心 |
| 数据库写入压力大 | 按聚合键 + 窗口批量写，压力可控 |

聚合的难点不在「合并」，而在**并发**：同一篇帖子的点赞请求会同时到达，计数必须原子累加，卡片要原地更新而不是重复开卡。

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
dashboard/  原生 HTML/CSS/JS 展示前端（仪表盘 + 可视化工作流编排器 + 实时活动流）
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

# 3. 触发一条评论通知（demo）
curl -X POST http://127.0.0.1:8000/v1/events/trigger \
  -H "Content-Type: application/json" \
  -d '{"name":"post_commented","to":{"subscriberId":"creator_1","email":"hi@example.com","deviceToken":"fcm-token-9f2c1d","channels":["in_app","push","email"]},"payload":{"post_id":"P-2026-1001","post_title":"我的第一支 Vlog","actor":"小林","comment":"拍得真好，求教程！"}}'
```

### 批量触发

一条热帖短时间内涌入的点赞、评论往往成批到达。逐条打触发接口时，每条消息都要一次 HTTP 往返加一次 Redis 往返，互动高峰下往返开销会盖过业务本身。批量接口把整批压成一次入队、一次活动日志写入：

```bash
curl -X POST http://127.0.0.1:8000/v1/events/trigger/batch \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: post-liked-P-2026-1001-burst-1" \
  -d '{"events":[
        {"name":"post_liked","to":{"subscriberId":"creator_1"},"payload":{"post_id":"P-2026-1001","actor":"小林"}},
        {"name":"post_commented","to":{"subscriberId":"creator_1","email":"hi@example.com"},"payload":{"post_id":"P-2026-1001","actor":"阿澈","comment":"求教程"}}
      ]}'
```

回执按请求顺序给出逐条结果，与请求等长：

```json
{"acknowledged": true, "accepted": 2, "rejected": 0,
 "results": [
   {"index": 0, "name": "post_liked", "subscriberId": "creator_1", "status": "accepted",
    "triggerId": "…", "transactionId": "…", "acknowledgmentId": "…"},
   {"index": 1, "name": "post_commented", "subscriberId": "creator_1", "status": "accepted",
    "triggerId": "…", "transactionId": "…", "acknowledgmentId": "…"}
 ]}
```

几条约定：

- **部分失败不连坐**：某条事件的模板不存在、未启用或环境不匹配时记为 `rejected` 并给出 `error`，同批其余事件照常入队。全部被拒同样返回 200——这是「逐条结果」而不是「整批失败」，调用方看 `accepted` 与 `results` 即可。
- **上限 100 条**：请求体超出范围由入参校验直接拒掉（422）。上限既让往返明显摊薄，又不至于让一条坏事件逼着重发整批。
- **幂等作用于整批**：`Idempotency-Key` 相同即返回首次回执（含当时被拒的条目），不会重复入队；事件之间没有独立的幂等键。
- **一次入队**：Redis 后端把整批装进一个 pipeline（不开事务，入队之间没有需要原子化的约束）一次发出；文件后端一次打开写完全部活动日志行，Postgres 后端一个事务插入。总线只提供逐条 `publish` 时自动退回逐条发送，批量是优化而非接入前提。

入队路径本机实测（同一 Redis 实例，2000 条消息）：逐条 10.6k 条/秒，每批 100 条 55.8k 条/秒，顺序与逐条入队一致。

### 环境变量

API 侧：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_BUS` | `memory` | 消息队列后端：`memory` / `redis` |
| `XW_REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串（`XW_BUS=redis` 时生效） |
| `XW_STORAGE` | `jsonl` | 持久化后端：`jsonl` / `postgres` |
| `XW_DATABASE_URL` | `postgresql+psycopg://xwnotify:xwnotify@localhost:5432/xwnotify` | Postgres 连接串（`XW_STORAGE=postgres` 时生效） |
| `XW_API_WORKER` | 按总线决定 | API 内置消费线程开关；`XW_BUS=memory` 时默认开，`redis` 时默认关（队列归引擎消费） |
| `XW_PROVIDERS_FILE` | 空 | 渠道配置文件（JSON），与引擎侧共用同一份；为空时每个投递渠道装配一个 console 实现 |

引擎侧：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_STEP_TIMEOUT` | `10s` | 单个步骤的执行预算，可被步骤自身的 `timeout` / `timeout_ms` 覆盖 |
| `XW_WORKFLOW_TIMEOUT` | `60s` | 一条消息一次执行的总预算，耗尽后剩余步骤不再尝试 |
| `XW_DIGEST_STORE` | `redis` | 聚合卡片存储：`redis`（跨实例共享）/ `memory`（单实例） |
| `XW_DELAY_STORE` | `redis` | 延迟任务存储：`redis`（ZSET，重启与多实例都不丢）/ `memory`（进程内） |
| `XW_DELAY_PREFIX` | `xw:delay` | 延迟任务的键前缀，用于环境隔离 |
| `XW_DELAY_LEASE` | `2m` | 延迟任务认领后的可见性超时，超时未确认会被回收重投；应大于一条消息续跑剩余步骤的总预算 |
| `XW_CLAIM_LEASE` | `60s` | 消息取出后的在途租约，超时未确认视为领取者失效 |
| `XW_RECLAIM_INTERVAL` | `30s` | 在途消息回收的扫描间隔，`0` 表示只在启动时扫一轮 |
| `XW_WORKERS` | `4` | 并发消费者数量，取值 `1`–`256` |
| `XW_PREFETCH` | `1` | 每个消费者每次领取的条数，取值 `1`–`512`；大于 1 时改走批量领取（一次脚本取回一批、处理完统一确认） |
| `XW_SHUTDOWN_GRACE` | `30s` | 退出时等待在途消息处理完成的上限 |
| `XW_RETRY_MAX_ATTEMPTS` | `5` | 单条消息的最大处理次数（含首次），`0` 表示关闭退避重试 |
| `XW_RETRY_BASE_DELAY` | `2s` | 第一次重试前的等待时长，之后按 2 的幂增长 |
| `XW_RETRY_MAX_DELAY` | `1m` | 重试等待的上限 |
| `XW_RETRY_JITTER` | `0.3` | 抖动比例，实际等待在 `[d*(1-jitter), d]` 内随机，`0` 表示不抖动 |
| `XW_RETRY_INTERVAL` | `1s` | 到期重试写回队列的扫描间隔 |
| `XW_WORKFLOWS_FILE` | 空 | 工作流定义快照文件（JSON 数组）；仅用于给没有携带步骤的消息按名称回退查找 |
| `XW_PROVIDERS_FILE` | 空 | 渠道配置文件（JSON）；为空时使用内置的 console 实现 |
| `XW_PROVIDER_PROBE_INTERVAL` | `30s` | 渠道健康探测的间隔；`off` 或 `0` 表示关闭周期探测（仍可手动触发） |
| `XW_PROVIDER_PROBE_TIMEOUT` | `3s` | 单次探测的超时 |
| `XW_PROVIDER_HEALTH_TTL` | `30s` | API 侧在线探测结果的缓存时长，`0` 表示不缓存 |
| `XW_PROVIDER_HEALTH_TIMEOUT` | `2s` | API 侧单次探测的预算；与引擎侧的探测超时分开配置，两者面向不同实现 |
| `XW_EXECUTION_FILE` | `api/data/executions.jsonl` | 投递结果回写的执行明细文件，与 API 共用同一份；写 `off` 表示不回写 |
| `XW_CALLBACK_URL` | 空 | 投递结果回调地址（只接受 https，本机回环可用 http）；为空表示不回调 |
| `XW_CALLBACK_TIMEOUT` | `5s` | 单次回调的超时 |
| `XW_CALLBACK_WORKERS` | `2` | 回调的发送并发数，取值 `1`–`16` |
| `XW_CALLBACK_QUEUE` | `256` | 回调事件的缓冲队列长度，满了丢弃事件而不阻塞投递 |
| `XW_CALLBACK_SECRET` | 空 | 回调签名密钥（至少 16 字节）；为空表示不签名，配置了但过短会在装配期报错 |
| `XW_CALLBACK_RETRY_ATTEMPTS` | `3` | 单条回调的最多尝试次数（含首次），取值 `1`–`10`，`1` 表示不重试 |
| `XW_CALLBACK_RETRY_BASE` | `500ms` | 第一次重试前的等待时长，之后按 2 的幂增长 |
| `XW_CALLBACK_RETRY_MAX` | `10s` | 重试等待的上限 |
| `XW_ADMIN_ADDR` | `127.0.0.1:<ENGINE_PORT>` | 管理接口监听地址（死信运维、渠道健康与启停）；`off` 表示不启动 |

超时是链路兜底：渠道卡住时管线不再无限等待，超时的步骤与后续步骤都会被记为失败/短路并写进执行明细。超时值应大于下游的正常延迟（例如设为渠道 P99 的数倍），而不是贴着平均值设置。

消息自带步骤定义：触发 API 在入队时把命中的工作流固化成快照放进消息（`workflow` 字段，步骤结构与工作流接口一致），引擎消费时按快照执行，不回查工作流存储。这样已经在队列里的消息不会因为模板被后续编辑而半路换了步骤序列；`name` 仍保留用于日志与活动流归属。没有快照的消息（历史消息、手工构造的消息）会按 `name` 到 `XW_WORKFLOWS_FILE` 指定的定义表里查找，两者都拿不到时视为不可投递——确认丢弃并告警，而不是留在队列里反复重投。

```json
{
  "trigger_id": "…", "name": "post_liked", "subscriber_id": "creator_1",
  "payload": {"post_id": "P-2026-1001", "actor": "小林"},
  "workflow": {
    "name": "post_liked", "version": 3,
    "steps": [
      {"id": "step-1", "type": "digest", "name": "点赞聚合", "config": {"key": "post_id", "window": "10m"}},
      {"id": "step-2", "type": "in_app", "name": "站内信", "config": {"template": "{{ payload.actor }} 赞了你的帖子"}}
    ]
  }
}
```

执行结果按消息汇总成一行日志：`steps` / `sent` / `failed` / `skipped` / `shortCircuited` / `deferred` / `recorded` 与耗时，配合 `trigger_id` 可以定位到具体是哪条互动、哪一个步骤出的问题。

一条消息只应被一个消费者处理：`XW_BUS=redis` 时队列归引擎消费，API 进程的内置消费线程默认关闭（`XW_API_WORKER=1` 可强制打开）；memory 总线是单进程 demo，才由 API 自己的线程消费。

队列按**至少一次**语义消费：引擎取出消息时同步写一份在途记录（`<queue>:processing` 保存原始 payload，`<queue>:processing:lease` 保存租约到期时间），处理成功后 `HDEL`/`ZREM` 才算消费完成；进程在处理中途退出时，消息仍在在途记录里，不会随进程消失。代价是极端情况下可能重复投递，因此下游写入需保持幂等（收件箱条目按 `trigger_id:step_id` 去重）。租约长度要大于单条消息的正常处理耗时，否则消息会在处理完成前被判定失效。

在途记录由回收流程负责收尾：引擎启动时先扫一轮、之后按 `XW_RECLAIM_INTERVAL` 周期扫描，把「租约到期 + 半个租约宽限」后仍未确认的消息原子地写回队列（`ZRANGEBYSCORE` 选过期租约、`RPUSH` 回主队列、`HDEL`/`ZREM` 清理，全过程在一个 Lua 脚本里完成，多实例并发回收不会重复入队）。这条路径覆盖两类情况：本进程上一轮崩溃留下的消息，以及集群里其他实例异常退出留下的消息。

处理失败的消息走退避重试，而不是等租约到期重投：worker 把消息放进重试集合（`<queue>:retry` 记可重试时刻、`<queue>:retry:body` 存原始消息体、`<queue>:retry:record` 记次数与首次失败时间），到期后由重试泵原子地写回主队列。等待时长按 `XW_RETRY_BASE_DELAY` 起步、逐次翻倍到 `XW_RETRY_MAX_DELAY` 封顶，再按 `XW_RETRY_JITTER` 向下抖动——同一批同时失败的消息不会在同一毫秒一起回到队列，避免下游刚恢复就被打满。失败次数按消息体哈希记账并带 TTL（覆盖整个退避窗口），因此计数不随重投丢失，消息体本身始终原样写回。处理次数达到 `XW_RETRY_MAX_ATTEMPTS` 后不再重试，转入死信列表 `<queue>:dead`，保留原始消息、失败次数、首次失败时间与最后一次失败原因。把这些状态从「在途记录」摘走与写入重试集合在同一个脚本里完成，所以重试中的消息不会被租约回收重复抓取。

### 渠道配置

默认装配下五个投递渠道各有一个 console 实现，投递只打印结构化日志。要让站内信真正写进订阅者的消息中心，把 `in_app` 配成 `kind: inapp`（见下面的「站内信与消息中心」）。要指定渠道用哪种实现、优先级与凭据，用 `XW_PROVIDERS_FILE` 指向一个 JSON 文件，示例见 [`infra/providers.example.json`](infra/providers.example.json)：

```json
{
  "providers": [
    {"id": "email-smtp", "channel": "email", "kind": "smtp", "priority": 10,
     "credentials": {"host": "smtp.example.com", "from": "互动提醒 <notify@example.com>",
                     "username": "notify", "password": "…", "tls": "starttls"}},
    {"id": "email-console", "channel": "email", "kind": "console", "priority": 100},
    {"id": "sms-console", "channel": "sms", "kind": "console", "enabled": false}
  ]
}
```

字段：`id`（必填，注册表内的寻址键，启停/替换/日志都用它）、`channel`（必填，只能是 `in_app` / `email` / `sms` / `push` / `chat`）、`kind`（必填，实现种类）、`priority`（可选，小的优先，缺省 `100`）、`enabled`（可选，缺省 `true`）、`credentials`（可选，该实现需要的凭据）。

同一渠道的多条配置构成一条取用序列：投递取第一个启用的实现，其余按优先级作为备选；优先级相同时保持文件中的顺序。`enabled: false` 的条目仍会登记进注册表，便于运行期再打开，但不会参与取用。

`kind` 同时约束了渠道与凭据：

| `kind` | 可用渠道 | 必填凭据 | 可选凭据 |
| --- | --- | --- | --- |
| `console` | 全部投递渠道 | — | — |
| `inapp` | `in_app` | — | — |
| `smtp` | `email` | `host`、`from` | `port`、`username`、`password`、`tls`、`reply_to` |
| `twilio` | `sms` | `account_sid`、`auth_token`、`from` | `endpoint`、`default_country_code`、`status_callback`、`timeout` |
| `push-mock` | `push` | — | `platform`、`project_id`、`topic`、`fail_with`、`capacity` |
| `fcm` | `push` | `project_id`、`access_token` | `endpoint` |
| `apns` | `push` | `key_id`、`team_id`、`private_key` | `bundle_id`、`sandbox`、`endpoint` |
| `slack` | `chat` | `webhook_url` | `channel`、`username`、`icon_emoji`、`timeout` |
| `teams` | `chat` | `webhook_url` | — |

校验在装配阶段完成并一次列全问题：未知渠道、`kind` 与渠道不匹配、标识重复、负优先级、缺少必填凭据、拼错的凭据项（例如把 `password` 写成 `Password`，错误信息会给出建议）都会让引擎启动失败，而不是等到投递那一刻才报认证错误。配置里出现的凭据值不会进日志，启动日志只打印标识、渠道、优先级与凭据名。文件顶层的 `retries` 段用于配置按渠道的投递重试，见下面的「渠道级重试」。

`smtp`、`twilio`、`slack` 与 `push-mock` 已有真实实现（`push-mock` 是推送的校验型 mock，见下面的「推送投递」）；`inapp` 在两侧都已落地——引擎写 `pipeline.InboxEntry`、API 写 `app.message_center.Message`，共用同一份收件箱文件。`fcm` / `apns` 的报文契约已定义但真实提交尚未接入，`teams` 亦未接入：配置里写了这些种类会被跳过并在启动日志逐条告警，其余渠道照常装配；如果某个投递渠道最终没有任何启用的实现，也会在启动日志里点出来——那种情况下分发阶段会退化成 mock 打印。

引擎侧的 `twilio` 条目目前仍按「实现尚未接入」跳过，短信在引擎路径上由 `console` 种类承担（`internal/pipeline/console_sms.go`，会校验号码并按 GSM-7 / UCS-2 计算段数）。接口契约与客户端实现在 API 侧的渠道集成层，见下面的「短信投递」。

反过来，`slack` 目前接在引擎侧（消费链路是群播报的主路径），API 侧 `chat` 仍由 `console` 承担；两侧读同一份配置文件，因此 API 进程会把 `slack` 条目记进「实现尚未接入」的跳过列表，不影响其他渠道装配。

#### 主备回退

同一渠道的多条配置不只是「默认取哪个实现」，也构成一条回退链：默认实现投递失败时，按优先级接着试下一个，直到成功或整条链走完。把真实网关配在前、console 实现配在后，就是最简单的兜底——中继挂了消息仍然出得去，只是写明「由 console 兜底」。

不是所有失败都值得换备选。实现会在结果里标记「这次失败与具体实现无关」（`Result.Terminal`），回退链遇到它立即停止，避免把同一条必然被拒的消息重复发给渠道里的每个实现：

| 失败类型 | 例子 | 是否继续回退 |
| --- | --- | --- |
| 本地校验 | 缺收件邮箱 / 手机号 / device token、正文为空、内容超过长度上限 | 否 |
| 服务器明确拒收 | SMTP 5xx（地址不存在、域名不收信）、Slack 4xx（`403`/`404`/`410`/`413`） | 否 |
| 实现或链路故障 | 连接被拒、DNS 失败、超时、限流 `429`、网关 5xx | 是 |
| 无实现可试 | 渠道下没有启用的条目 | 否（退化为 mock 打印） |

整条链共用一个 `ctx`：步骤超时或引擎退出后，不会再向后尝试备选，避免取消之后仍在链路末端发起网络请求。

每次投递都会留痕：`StepResult.UsedProvider` 是实际完成投递的实现标识，`ProviderAttempts` 按尝试顺序记录每一次投递（同一实现重试时会重复出现），`ProviderFallback` 标出是否由备选完成。回退成功记 WARN 日志并带上被跳过实现的原因；整条链失败时，错误信息逐个列出实现与原因，例如：

```
投递失败（email）: 尝试 2 个实现均失败：smtp-email：SMTP 连接 10.0.0.9:587 失败：i/o timeout；console-email：发件队列不可写
```

```json
{
  "providers": [
    {"id": "smtp-email", "channel": "email", "kind": "smtp", "priority": 10,
     "credentials": {"host": "smtp.example.com", "from": "互动提醒 <notify@example.com>"}},
    {"id": "console-email", "channel": "email", "kind": "console", "priority": 100}
  ]
}
```

启动日志会把存在备选的渠道及其取用顺序打出来，便于确认主备关系：`渠道取用序列（主备） chains="map[email:[smtp-email console-email]]"`。单实现渠道不会出现在这条日志里。

#### 渠道级重试

回退处理的是「这个实现对端不可用」，重试处理的是「同一个实现对端抖了一下」。两者叠在一起：先在同一个实现上按退避重试几次，次数用尽仍未成功，再换下一个实现。策略按渠道配置，写在配置文件顶层的 `retries` 段里：

```json
{
  "providers": [
    {"id": "smtp-email", "channel": "email", "kind": "smtp", "priority": 10,
     "credentials": {"host": "smtp.example.com", "from": "互动提醒 <notify@example.com>"}},
    {"id": "console-email", "channel": "email", "kind": "console", "priority": 100}
  ],
  "retries": {
    "default": {"max_attempts": 1},
    "email": {"max_attempts": 3, "base_delay": "500ms", "max_delay": "5s", "jitter": 0.3},
    "chat": {"max_attempts": 3, "base_delay": "1s", "max_delay": "8s", "jitter": 0}
  }
}
```

| 字段 | 含义 |
| --- | --- |
| `max_attempts` | 单个实现最多尝试几次（含首次），上限 `10`；`0` 或 `1` 表示不重试。它同时是这条配置的开关 |
| `base_delay` | 首次重试前的等待，逐次翻倍到 `max_delay` 封顶 |
| `max_delay` | 等待上限，不能小于 `base_delay` |
| `jitter` | 抖动比例 `0`–`1`，只向下抖动，因此上界始终可预期 |

时长写带单位的字符串（`"500ms"`、`"2s"`）或毫秒数（`500`，数字一律按毫秒解释）。未写的字段沿用 `default` 段，`default` 段未写的沿用内置值（`500ms` 起、`5s` 封顶、抖动 `0.3`）。`default` 段缺省时所有渠道都不重试——渠道层之上还有队列层的消息重试，两层同时打开会让一次故障的请求量按乘积放大，因此这里的重试需要显式配置。

`max_attempts`、`base_delay` 越界或写错渠道名都会让引擎启动失败，问题一次列全；`digest` / `delay` 这类编排步骤不是投递渠道，出现在 `retries` 里同样会被拒绝。

重试只针对「值得再试」的失败：本地校验不过、服务器明确拒收（SMTP 5xx、Slack `403`/`404`/`410`/`413`）与回退表里的口径一致，重试只会把同一条必然被拒的消息多打几次。连接被拒、超时、限流 `429`、网关 5xx 才重试。退避等待同样感知 `ctx`：步骤超时或引擎退出时立即返回，不再发起下一次尝试。

留痕与回退一致：`ProviderAttempts` 会列出每一次尝试（重试则同一标识重复出现），`ProviderRetries` 是首次之外多打的次数，`ProviderFallback` 仍然只表示「换过实现」。重试后成功记 WARN 日志并带上尝试序列；整条链失败时错误信息带上次数，便于区分「打了一次就挂」与「重试三次仍挂」：

```
投递失败（email）: smtp-email 先后尝试 3 次均失败：SMTP 连接 10.0.0.9:587 失败：i/o timeout
投递失败（email）: 尝试 2 个实现（共 4 次）均失败：smtp-email（3 次）：i/o timeout；console-email：发件队列不可写
```

启动日志会打印实际生效的策略，例如 `渠道投递重试策略 retries="default=关闭 email=3 次 500ms→5s 抖动 0.30 chat=3 次 1s→8s"`；未配置时打印一行「渠道投递重试未启用：失败即按主备切换」。聚合卡片与普通步骤走同一条投递链，因此同样会重试与回退。


#### 邮件投递（`kind: smtp`）

引擎侧的实现是 `internal/mailer` 里的一个最小 SMTP 客户端（`internal/pipeline/smtp_email.go` 把它接成渠道实现），API 侧是 `app/providers/smtp.py`，两者按同一份凭据语义工作：

- **加密**：`tls` 取 `none` / `starttls` / `tls`（隐式 TLS，也叫 SMTPS），也接受 `ssl`、`plain` 等别名；不填时按端口推导——465 隐式 TLS、25 明文、其余 STARTTLS；不填端口时按加密方式取 465 / 25 / 587。
- **认证**：`username` + `password` 成对出现，走 `AUTH PLAIN`（Python 侧由 `smtplib` 在 PLAIN 被拒时回退 LOGIN）。口令只允许在加密连接上发送，配置成「明文 + 口令」会直接装配失败。
- **连接复用**：投递完成且事务干净的连接会回到空闲池，下一条消息直接进信封。TLS 握手、EHLO、AUTH 是每条消息的固定成本，连续发信时这部分可以整段省掉（`docs/benchmark.md` 有对比数据）。`pool_size` 控制空闲连接数（默认 4，写 `off` 表示每条消息新建连接），`idle_timeout` 控制空闲保留时长（默认 30 秒，接受 `30s` 或毫秒数）。取用前先发一次 `NOOP` 确认对端还在；投递失败或上下文被取消的连接直接关闭、不回池，避免把停在事务中间或对端已断开的连接留给下一条消息。
- **结果**：投递失败不抛异常，`reason` 里带阶段与服务器回复（如「SMTP 收件人 a@b 失败（550）：no such user」），写进执行明细；成功时记录报文标识，便于在中继日志里找到那一封。
- **边界**：连接、握手、提交共用同一个超时（默认 10 秒，Python 侧可传 `timeout`），超时或引擎退出时中断连接；主题与地址里的换行符在拼报文之前就被拒掉，避免头注入；证书默认严格校验，自签内网中继需要在代码里传入自带信任链的 `ssl.SSLContext`（Python）或 `SkipVerify`（Go，仅测试与受控内网）。
- **降级**：把 console 条目留在同一渠道、优先级调低，中继故障时由它兜底完成投递（见上面的「主备回退」）；链接层错误会继续回退，收件人被 5xx 拒收则不会。

#### 邮件正文的 HTML 形态

email 步骤的正文有两种写法，都由 render 阶段渲染：

| 步骤配置 | 产出 |
| --- | --- |
| `template` | 纯文本正文，单段 `text/plain` 报文 |
| `html` | HTML 模板，按原样输出、只有变量被转义 |
| `template` + `format: "html"` | 把纯文本套进内置版式（标题 + 正文 + 页脚，表格布局 + 内联样式） |
| `format: "text"` | 显式关闭 HTML，即使配了 `html` 也不发多段报文 |

两种正文同时存在时按 `multipart/alternative` 提交：纯文本段在前、HTML 段在后，邮件客户端从后往前挑第一个能显示的形态。只配了 `html`、没配 `template` 时，纯文本段给一句可读的降级文案，而不是留空。

```json
{
  "id": "comment_email",
  "type": "email",
  "config": {
    "subject": "《{{ payload.post_title }}》有新评论",
    "template": "{{ payload.actor }} 说：{{ payload.comment }}",
    "html": "<p><strong>{{ payload.actor }}</strong> 说：{{ payload.comment }}</p>"
  }
}
```

HTML 模板里的变量按 HTML 实体转义后再拼接，模板自身的标签不受影响：评论或帖子标题里带 `<script>` 时只会显示成文本，不会被当成页面代码执行。纯文本正文不做转义——那里的 `<` 就是一个普通字符。

#### 短信投递（`kind: twilio`）

`app/providers/sms.py` 负责号码与计费两件事，`app/providers/twilio.py` 是网关客户端。接口按「Twilio 风格的 Messages 资源」定义，凡是兼容这套报文约定的网关都能直接换上：

| 项目 | 约定 |
| --- | --- |
| 端点 | `POST {endpoint}/2010-04-01/Accounts/{account_sid}/Messages.json` |
| 认证 | `Authorization: Basic base64(account_sid:auth_token)` |
| 请求体 | `application/x-www-form-urlencoded`：`To`、`From`、`Body`，可选 `StatusCallback` |
| 成功响应 | `2xx` + JSON 消息资源（`sid`、`status`、`num_segments` …） |
| 失败响应 | `4xx` / `5xx` + JSON 错误资源（`code`、`message`） |

- **号码**：先去掉空格、连字符与括号，只允许一个前导 `+`，数字位数 6–15。号码没写国际区号时按 `default_country_code` 补上（写 `86` 或 `+86` 都行），没有配置则判失败而不是猜一个区号——猜错会把提醒发到别人的号段。发送方 `from` 可以是 E.164 号码，也可以是至多 11 位的字母数字标识。
- **计费分段**：纯 ASCII 走 GSM-7（160 / 多段 153），出现任意非 ASCII 字符整条转 UCS-2（70 / 多段 67，按 UTF-16 码元计，emoji 占两个），段数写进投递日志，也用于解释账单。正文超过 1600 字符在本地就判失败，不必等网关回 `21617`。
- **正文为空**：引擎路径上正文由 render 阶段产出，不会为空；API 内置消费者不渲染模板，此时给一句兜底文案。空正文会被网关以 `21602` 拒收，静默失败比收到一句平淡的提醒更糟。
- **结果映射**：`queued` / `accepted` / `sending` / `sent` / `delivered` 算成功；`failed` / `undelivered` / `canceled` 与任何带 `error_code` 的响应算失败，原因里带错误码与网关原文。HTTP 层单独区分——`401` 指向账户标识或口令、`404` 指向 `account_sid` 与 `endpoint`、`429` 带上 `Retry-After`、`5xx` 记为网关错误、非 JSON 响应会被指出而不是当成成功。
- **凭据**：`auth_token` 只出现在 `Authorization` 头里，不进日志；启动日志只打印标识、渠道、优先级与凭据名。

```json
{
  "id": "sms-twilio",
  "channel": "sms",
  "kind": "twilio",
  "priority": 10,
  "credentials": {
    "account_sid": "AC…",
    "auth_token": "…",
    "from": "+8613900000000",
    "default_country_code": "86",
    "status_callback": "https://notify.example.com/hooks/sms"
  }
}
```

不配凭据时 `sms` 渠道走的是 console 实现（`ConsoleSMSProvider`）：它同样规整号码、计算段数，号码不合法会如实报失败，而不是打印成功日志。

#### 推送投递（`kind: push-mock`）

推送的收件人不是邮箱或号码，而是一台设备上的应用实例（device token）；报文也不是一段文本，而是网关规定的结构化 JSON。`app/providers/push.py` 把两家网关的报文契约定下来，`app/providers/push_mock.py` 按同一套规则校验后只留痕、不联网：

| 项目 | FCM（HTTP v1） | APNs |
| --- | --- | --- |
| 端点 | `POST {endpoint}/v1/projects/{project_id}/messages:send` | `POST {endpoint}/3/device/{token}` |
| 认证 | `Authorization: Bearer <OAuth2 access_token>` | `authorization: bearer <ES256 JWT>` |
| 关键请求头 | `Content-Type: application/json` | `apns-topic`、`apns-push-type`、`apns-priority`、`apns-expiration`、`apns-id` |
| 消息体 | `{"message": {"token", "notification", "data", "android"}}` | `{"aps": {"alert" / "content-available", "sound", "badge"}, …自定义字段}` |
| 成功响应 | `200` + `{"name": "projects/…/messages/<id>"}` | `200` + 空响应体，标识在 `apns-id` 响应头 |
| 失败响应 | `{"error": {"status", "code", "details": [{"errorCode"}]}}` | `{"reason": "BadDeviceToken"}` |

- **设备 token**：APNs 是 32 字节即 64 位十六进制，统一转小写；FCM 只允许字母、数字与 `_ - : .`，长度 20–4096。两者都为空或形态不对时在进入渠道实现之前判失败，并把 token 脱敏成「头 6 位…尾 4 位（长度）」再写进日志——token 本身就是能向该设备发消息的凭据。
- **平台推断**：不写 `platform` 时按 token 形态推断，64 位十六进制按 APNs 处理，其余按 FCM。64 位十六进制的 FCM token 在形态上无法与 APNs 区分，这种情况请显式写 `platform`，不要依赖推断。
- **报文上限**：4 KiB（两家共同的限制，按实际发送的紧凑 JSON 字节数计算）。标题 200 字符、正文 2000 字符、`data` 最多 64 个字段且取值不超过 1024 字符，是本项目的本地保护——推送是用来喊一声的，超长宁可本地拒掉，也不要让网关截断或退化成一条空白通知。
- **data 字段**：只接受标量，布尔写成 `true` / `false`；APNs 的 `aps` 是保留键，混进来会覆盖报文结构，因此直接拒绝。从事件载荷里取 data 时会跳过嵌套对象——它们不是给设备端的路由提示，塞进去只会撑大报文。
- **静默推送**：标题与正文都为空时按静默推送处理，APNs 侧 `apns-push-type` 判成 `background`、优先级降到 5（该组合是硬性要求），FCM 侧 `android.priority` 用 `normal`。
- **失败语义**：错误码到「能不能重试 / 要不要删 token」的映射写在契约层。FCM 的 `UNREGISTERED`、`SENDER_ID_MISMATCH` 与 APNs 的 `BadDeviceToken`、`DeviceTokenNotForTopic`、`Unregistered` 标为 token 已失效，调用方应把它从订阅者资料里摘掉；`QUOTA_EXCEEDED`、`TooManyRequests`、`ExpiredProviderToken` 与 5xx 标为可重试。`reason` 里给的是能指导下一步动作的话（如「应从订阅者资料中移除」），不是网关原文。
- **mock 实现**：`push-mock` 校验 token、报文与 data，通过后把这一条记进内存环形缓冲（`capacity`，缺省 50），留痕含平台、脱敏 token、字节数、发送路径与成败。`fail_with` 让每条投递都按给定原因失败，用来演示「推送失败 → 降级到其他渠道」的分支，而不必真的把网关弄坏。`project_id` / `topic` 只影响留痕里的发送路径，不会凭空编造一个。

```json
{
  "id": "push-mock",
  "channel": "push",
  "kind": "push-mock",
  "priority": 10,
  "credentials": {"platform": "apns", "topic": "com.example.xw", "capacity": "100"}
}
```

真实提交（`kind: fcm` / `apns`）尚未接入：`fcm` 需要 `project_id` 与一枚有效约一小时的 OAuth2 访问令牌，`apns` 需要 `key_id`、`team_id` 与签名私钥换来的 provider token，两者都涉及凭据签发，留给实际接入时按部署方式补齐。引擎侧同一份配置里的 `push-mock` 条目同样按「实现尚未接入」跳过，推送在引擎路径上由 `console` 种类承担（`internal/pipeline/console_push.go`，会校验 token 并限制 4 KiB 载荷）。

#### 群播报投递（`kind: slack`）

chat 渠道的收件目标是群机器人而不是某个订阅者，因此没有「收件地址」这一步：webhook 地址本身就是凭据，签发与轮换在工作区一侧完成。引擎侧的实现分两层——`internal/chat` 定义报文契约与客户端，`internal/pipeline/slack_chat.go` 把它接成渠道实现：

| 项目 | 约定 |
| --- | --- |
| 端点 | `POST {webhook_url}`，地址形如 `https://hooks.slack.com/services/T000/B000/XXXX` |
| 请求体 | `application/json`：`text`（通知栏预览）+ `blocks`（消息主体），可选 `channel` / `username` / `icon_emoji` |
| 成功响应 | `200` + 纯文本 `ok` |
| 失败响应 | `4xx` / `5xx` + 纯文本错误码（`invalid_payload`、`channel_not_found`、`rate_limited` …） |

- **报文形态**：只给 `blocks` 时通知栏不会带内容，因此 `text` 固定拼一份「标题 + 正文开头」的纯文本并截断到 4000 字符；`blocks` 才是主体——标题单独一段加粗，正文按 2900 字符切片，优先在换行处断开、单行超长才按字符硬切。切片与拼接可逆，长评论不会被悄悄截掉后半段。
- **地址校验**：只接受 https，http 仅限本机回环地址（进程内测试服务器与内网代理）。webhook 路径末段就是令牌，日志、执行明细与失败原因里一律脱敏成 `scheme://host/…`，回执里也只出现脱敏形态。
- **收件地址解析**：步骤配了 `config.webhook` 就用它（同一份工作流可以把点赞聚合播报到运营群、把评论播报到作者群），没配则回退渠道配置里的 `webhook_url`；两者都没有时如实判失败，而不是打印一行成功日志。
- **失败映射**：`404` 指向「webhook 不存在或已被撤销」、`403` 指向「无权向该频道发消息」、`410` 指向频道已归档、`413` 指向长度上限、`429` 带上 `Retry-After`、`5xx` 记为「Slack 服务错误」；表里没有的错误码原样带出去，至少能拿去搜。限流与 5xx 在原因末尾标「可稍后重试」，地址与报文类问题不标——那类失败重试多少次都是同样的结果。
- **本地校验**：正文为空、超过 4000 字符、地址不是 http(s) 或缺 host，都在发出请求之前判失败，与 `console` 实现同一口径，因此把 console 换成 slack 不会改变哪些消息会被拒。
- **取消**：实现 `ContextSender`，步骤超时或引擎退出时请求立即中断，不会把连接留在后台继续跑。

```json
{
  "id": "chat-slack",
  "channel": "chat",
  "kind": "slack",
  "priority": 10,
  "credentials": {
    "webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX",
    "channel": "#ops",
    "username": "互动播报",
    "icon_emoji": ":bell:"
  }
}
```

不配凭据时 `chat` 渠道走的是 console 实现（`ConsoleChatProvider`）：同样校验 webhook 与长度，只把「推送到哪个群」打进日志而不发请求。

### API 侧的渠道装配

API 进程读同一份 `XW_PROVIDERS_FILE`，校验规则与引擎一致（渠道约束、必填凭据、拼错的凭据项），装配分四层：

- `providers/base.py`：`Provider` 抽象基类。收件人按渠道寻址（`in_app` → 订阅者标识、`email` → 邮箱、`sms` → 手机号、`push` → 设备 token，`chat` 的目标由实现自己的 webhook 决定），缺地址在进入实现之前就判失败；投递结果是 `sent` + `reason` 的固定结构，实现里抛出的异常由基类兜住转成失败结果，调用方不需要写 `try`。
- `providers/console.py`：console 实现，把投递打成一行结构化日志，是所有渠道的兜底项。
- `providers/inapp.py`：站内信实现。投递即落库——把消息写进订阅者的消息中心（`app/message_center.py`），写入按消息标识幂等，同一触发重放不会留下两条；正文优先取渲染结果，API 内置消费者不渲染模板时回落到载荷里的 `body` / `message`，两者都没有就如实判失败，不在收件箱里留一张点不动的空卡片。
- `providers/sms.py`：短信的号码规整、E.164 补全与计费分段，以及 `ConsoleSMSProvider`——会真的校验号码格式的 console 短信实现。
- `providers/smtp.py`：SMTP 实现，把 email 真正提交到中继；凭据校验（端口、加密方式、口令与连接方式的组合）在构造时就完成，投递失败按阶段给出原因。
- `providers/twilio.py`：短信网关实现，按 Messages 资源契约提交；配置校验（账户标识、发送方、端点、超时）在构造时完成，投递失败按 HTTP 状态与网关错误码给出原因。
- `providers/push.py`：推送的报文契约——设备 token 校验与脱敏、`PushMessage` 的本地限制、FCM HTTP v1 与 APNs 的请求/响应结构，以及错误码到「可重试 / token 失效」的映射。
- `providers/push_mock.py`：推送的 mock 实现，按同一套契约校验后把投递记进内存环形缓冲，可选按给定原因失败，便于演示失败与回退路径。
- `providers/registry.py`：按渠道索引实现，同一渠道按优先级取用（小的优先、同级按登记顺序），支持停用后回退、按标识原地替换、按标识摘除；读写由一把锁保护，取用返回快照。
- `providers/factory.py`：解析并校验配置，装配成注册表，报告登记了哪些、跳过了哪些（实现尚未接入的种类）、还有哪些渠道空着。

已接入 `console`、`inapp`、`smtp`、`twilio` 与 `push-mock`：`fcm` / `apns` / `slack` / `teams` 等尚未实现的种类会被跳过，其余渠道照常装配。`chat` 渠道的真实实现在引擎侧（见上面的「群播报投递」），API 侧由 `console` 承担。

### 站内信与消息中心

站内信没有外部网关，投递动作本身就是一次落库；因此消息中心（`api/app/message_center.py`）要同时承担「数据模型」与「写入路径」两件事：

- **模型**：`Message` 收口一条站内信需要的字段——`id`、`trigger_id`、`subscriber_id`、`workflow`、`step_id`、`title`、`body`、`created_at`、`updated_at`、`read`。字段名与引擎侧的 `pipeline.InboxEntry` 完全一致（snake_case），两个进程因此可以共用同一份收件箱文件：引擎渲染后写入，API 侧原样读取并渲染到收件箱界面。写入前统一裁剪空白、校验必填项，正文为空直接拒绝；`updated_at` 不会早于 `created_at`，否则「已读之后又被更新」的判断会失效。
- **写入**：按消息标识幂等。普通站内信标识取 `trigger_id:step_id`，同一触发重放时 `write` 返回 `duplicate`、不新增条目；聚合卡片要原地累加计数时用 `replace=True`，返回 `updated`，目标不存在则返回 `missing` 而不是静默新建。两种存储实现：`MemoryMessageCenter` 用于测试，`JsonlMessageCenter` 落盘到 `api/data/inbox.jsonl`（`XW_INBOX_FILE` 可覆盖，与引擎同一默认路径）。
- **文件共享的细节**：新消息走单次 `O_APPEND` 追加，改写走「同目录临时文件 + `rename`」，进程在写一半时被中断也不会留下半截文件；索引未命中时先比较文件大小，发现被其它进程追加过就重扫一遍再判断，避免在共享卷上把同一张卡片写出两份。损坏行在改写时原样保留，不为了改一条卡片丢掉别的数据。
- **已读状态不在消息中心**：它属于订阅者维度、与消息内容分开演进，由 `app/inbox.py` 的 `ReadStateStore` 维护；消息中心写入的 `read` 字段固定为 `false`，与引擎侧一致。

配置示例（`infra/providers.example.json` 里已有这一条）：

```json
{"id": "inapp", "channel": "in_app", "kind": "inapp", "priority": 100}
```

### 收件箱接口

创作者看到的那一屏由 `api/app/inbox.py` 提供。数据来自两处：引擎 `in_app` 步骤写入的收件箱文件（内容已渲染），以及投递记录里 `channels` 含 `in_app` 的记录（兜底）。两者按消息 ID 去重，引擎条目优先。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/v1/inbox?subscriberId=...&limit=50` | 消息列表 + 未读计数；`limit` 上限 200 |
| `GET` | `/v1/inbox?...&cursor=...` | 继续翻页，`cursor` 取上一页的 `nextCursor` |
| `GET` | `/v1/inbox?...&unread=true&workflow=post_liked&since=...&until=...` | 只看未读 / 只看某个工作流 / 时间区间（左闭右开） |
| `POST` | `/v1/inbox/read` | 批量标记已读：给 `messageIds` 或 `triggerIds`，都不给表示全部已读 |

响应里四个计数各有分工，别混用：

- `unread` / `total`：该订阅者的全部消息，与筛选和分页无关。角标读这两个字段，翻到第几页都不该变。
- `matched`：本次筛选命中的条数，`hasMore` / `nextCursor` 由它推出。列表为空但 `total` 不为零，就是「筛选条件没命中」，不是「没有通知」。

排序键是 `(排序时间, 消息 ID)` 倒序。排序时间优先取 `updated_at`——聚合卡片被新互动顶起来时应该往前排；回落到 `created_at`。补一个消息 ID 是因为时间只到秒级：同一秒写入的多条消息若只按时间排，顺序取决于底层读取顺序，翻页时两次请求各算出一种，同一条消息就会出现两次或者干脆漏掉。

游标编码上一页最后一条的排序键，下一次请求从它的**严格后一条**继续。这样翻页期间顶上来了新消息也不会重复返回；游标指向的消息被删掉也不会卡住（游标存的是位置而不是「从这条之后查」）。游标无法解析时返回 400，而不是悄悄回到第一页——静默回退会让调用方以为已经翻到底了。

性能上分两条路径：首屏（无筛选、无游标）直接读缓存快照，不碰收件箱文件与投递记录；带筛选或带游标时按全部消息汇总后过滤。缓存每个订阅者只留最近 200 条，拿它算筛选命中数会得到错的数，所以宁愿多读一次。缓存键是「收件箱来源版本 + 该订阅者已读版本」，任一来源有新写入或标记了已读就重算。

已读按「消息 ID + 已读时刻」记录：聚合卡片被新互动原地改写时 `updated_at` 会推进，已读时刻早于它即重新算未读——同一张卡在收件箱里始终只占一条，未读计数因此按「消息条数」而不是「互动次数」计算。

投递前先校验寻址信息，缺 `email` / `phone` / `deviceToken` 时该渠道直接记为失败：执行明细（`GET /v1/executions`）里是 `status=failed` 加具体原因，消息记录也为 `failed`，不会把「地址没配」记成已发送。配置本身写错（未知渠道、种类与渠道不匹配、缺必填凭据）会让 API 启动失败，与引擎的处理一致。

### 订阅者与渠道绑定

订阅者是触发的收件方。`api/app/subscriber_routes.py` 提供一组 CRUD 接口，数据默认落在 `api/data/subscribers.jsonl`（`XW_SUBSCRIBERS_FILE` 可覆盖）；写操作与其它接口一样走环境鉴权（`X-Environment` / `X-Api-Key`）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/subscribers` | 创建；标识可带入业务侧用户 ID，留空自动生成，标识或地址重复返回 409 |
| `GET` | `/v1/subscribers?channel=email&limit=50&cursor=...` | 列表，按创建顺序游标翻页，可按渠道过滤 |
| `GET` | `/v1/subscribers/{id}` | 单条查询 |
| `PUT` | `/v1/subscribers/{id}` | 局部更新：未出现的字段保持不变，空串表示解绑，`channels` 整体替换 |
| `DELETE` | `/v1/subscribers/{id}` | 删除；重复删除返回 404 |

绑定关系分两部分存：地址是数据（`email` / `phone` / `device_token`），`channels` 是意愿（`in_app` / `email` / `sms` / `push`）。分开存才能表达「先停掉这个渠道、地址先留着」——合成一个字段就只能靠删地址来表达停用，恢复时又得重新收集。

```json
{
  "id": "creator_42",
  "email": "creator@example.com",
  "phone": "+8613800000000",
  "device_token": "fcm-token-42",
  "channels": ["in_app", "email", "sms", "push"]
}
```

响应里多一个 `bindings`，只列出此刻真的能投递的渠道（渠道已启用且地址存在；`in_app` 以订阅者标识作收件人，不需要额外地址）。启用了邮件却没填邮箱时，`channels` 里仍有 `email`，`bindings` 里没有——前端不必自己复述这条判定。渠道取值在请求模型里就归一：重复项按首次出现去重保序，`delay` / `digest` 是编排步骤、挂在订阅者身上没有意义，直接拒绝。

地址既是收件目标，也是身份：同一个邮箱绑在两个订阅者身上，一条点赞通知会投两遍，退信又归不到人。因此建档时做两道判定。

**格式（422）**。邮箱按 RFC 5321/5322 的常见子集校验——总长 254（用户名 64、域名段落 63）、恰好一个 `@`、用户名只允许 atext 与点且首尾不能是点、域名至少要有一级后缀、国际化域名需写成 punycode；带着空格的地址与引号包裹的写法都不接收。手机号只允许数字、一个前导 `+` 与空格/连字符/括号分隔符，位数 6–15。校验在请求模型里完成，错误信息指出是哪一项、该怎么改：

```json
{"detail":[{"loc":["body","phone"],
  "msg":"Value error, 手机号：缺少国际区号（13800000000）：请写成 +8613800000000 这样的形式，或设置 XW_SUBSCRIBER_COUNTRY_CODE"}]}
```

**唯一性（409）**。存储层按「环境 + 字段 + 比较键」建索引，查重与写入在同一把锁里完成，避免先查后写在并发下漏判。比较键抹平写法差异：邮箱统一小写，手机号去掉分隔符——`Creator@Example.com` 与 `creator@example.com`、`+86 138 0000 0000` 与 `+8613800000000` 是同一个地址，后者返回 409 并指出占用者。环境参与索引是因为两套环境是两套数据：同一个人可以在 development 与 production 各有一条档案。解绑（写空串）或删除记录会释放地址，之后可以被别人认领。

存下来的也是规范形态：邮箱小写、手机号 E.164。这样模板渲染、查重与投递三处看到的是同一个字符串。手机号若只有本地号码，按 `XW_SUBSCRIBER_COUNTRY_CODE` 补区号；未配置时如实报错而不是按位数猜——猜错就是把短信发到别人的号段上，这类错误要到投递才暴露，代价是一次真实的误发。JSONL 后端多进程共用同一份文件时仍没有跨进程写入协调（与既有的追加写一致），并发写入同一地址的极端情形由后写者覆盖索引。

存储有内存与 JSONL 两种实现：新增走单次 `O_APPEND` 追加，修改与删除走「同目录临时文件 + `rename`」整体重写；读之前比较文件大小与修改时刻，发现被其它进程写过就重扫，避免在共享卷上读到过期索引。

### 收件偏好

偏好回答「这个人愿不愿意收」，地址回答「发到哪里」，两者分开存：地址在订阅者档案里（邮箱、手机号、设备 token），偏好单独一份（`api/app/preferences.py`，默认落 `api/data/preferences.jsonl`，`XW_PREFERENCES_FILE` 可覆盖）。分开之后才能表达「先停掉邮件，地址先留着」，恢复时不用让用户重新填一遍。

偏好分三级，一条记录由 `(subscriber_id, scope, target)` 唯一确定：

| `scope` | `target` | 含义 |
| --- | --- | --- |
| `global` | 空 | 这个人的总开关 |
| `workflow` | 工作流名，如 `post_liked` | 这类互动要不要通知 |
| `channel` | 渠道，如 `email` | 这类通知要不要用这个渠道 |

解析是一条覆盖链：**渠道 → 工作流 → 全局**，命中第一条就停，三层都没有记录时按「接收」处理。

```
全局关闭，渠道 email 单独开启
  email  → 接收（渠道层决定）
  sms    → 不接收（回落到全局）
```

默认「接收」是刻意的：偏好用来收窄投递范围，默认关闭会让新订阅者在没配任何东西时一条通知都收不到，而这类问题往往要等到有人抱怨才知道。

几条写进模型的规则：

- `global` 不接受 `target`。总开关带上目标就不是总开关了，两种语义混在一个字段里，之后谁也说不清哪条生效。
- `delay` / `digest` 不能作为 `channel` 的 `target`。它们是编排步骤，写进偏好等于挂了一个永远匹配不到的条件。
- 比较键与存储形态分开：工作流名裁剪空白，渠道取枚举值，全局的 `target` 归一为 `None`。同一条偏好无论写成 `Channel.EMAIL` 还是 `"email"`，落到存储里都是同一个键。
- 判定结果是可解释的：`PreferenceDecision` 带 `allowed` / `scope` / `target` / `matched` / `reason`，「为什么这条没发」在执行明细与日志里能直接读出来，不用回头猜是哪一层拦下的。

三层都是覆盖写语义，因此文件后端每次写入整体重写（一个订阅者最多三条记录），不会在文件里攒下被覆盖的旧行。

### 死信运维接口

引擎启动时会在 `XW_ADMIN_ADDR`（默认本机 `ENGINE_PORT`）上开一组管理接口，用于查看、重放与丢弃死信，查看渠道实现的健康状态与启停，以及读取投递指标。接口只绑定回环地址，需要跨机访问时显式改成 `0.0.0.0:8081` 之类的地址；`XW_ADMIN_ADDR=off` 则不启动。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/health` | 存活检查 |
| `GET` | `/admin/dead-letters?offset=0&limit=20` | 死信列表，最新在前，返回 `items` / `total`；`limit` 上限 200 |
| `POST` | `/admin/dead-letters/replay` | 请求体 `{"ids": ["…"]}`，把消息原样写回队列并移出死信 |
| `POST` | `/admin/dead-letters/discard` | 请求体同上，直接丢弃、不给第二次机会 |
| `GET` | `/admin/providers` | 各渠道实现的健康快照与汇总，返回 `providers` / `summary` |
| `POST` | `/admin/providers/disable` | 请求体 `{"ids": ["…"]}`，把实现移出取用序列（不影响其他实现） |
| `POST` | `/admin/providers/enable` | 请求体同上，放回取用序列并清空旧的失败计数 |
| `POST` | `/admin/providers/probe` | 请求体同上；省略请求体表示探测全部启用中的实现 |
| `GET` | `/admin/metrics` | 投递指标：合计 / 按渠道 / 按实现，含成功、失败、超时与延迟分布 |
| `POST` | `/admin/metrics/reset` | 清空计数并重新计时 |

```bash
# 看最近 10 条死信
curl '127.0.0.1:8081/admin/dead-letters?limit=10'
# 修好下游后把某条重放回队列（一次最多 100 条）
curl -X POST 127.0.0.1:8081/admin/dead-letters/replay \
  -H 'Content-Type: application/json' -d '{"ids":["3f2a…"]}'
```

`id` 是死信记录本身的 SHA-1：记录写入后内容不再变化，因此这个 ID 在查询与重放之间保持稳定。重放以「记录串本身」作为条件删除的依据，同一份死信被并发重放时只有一次能删掉它，消息不会重复入队；已被取走的 ID 会在响应的 `missing` 里返回，重复提交同一批 ID 是幂等的。重放的前提是失败原因已经排除，所以消息重新获得完整的重试次数（转入死信时计数键已清空），并按「优先处理」写回队列右端，而不是排在新消息后面再等一轮。

死信记录带完整原始消息，长时间不清理会让列表越来越长；重放与丢弃都要先扫一遍列表定位记录，因此运维上应按批次处理后清空，而不是把它当历史归档长期存放。

### 渠道健康与启停

每个渠道实现都有一条健康记录，观测来自两处：真实投递的结果，以及周期性的主动探测。状态只有四种——`unknown`（还没有任何观测）、`up`（最近一次成功）、`degraded`（最近一次失败，但连续次数未到阈值）、`down`（连续失败达到阈值，默认 3 次）；任意一次成功都会立刻回到 `up` 并清零连续计数。判死阈值取 3 而不是 1：单次失败常见于对端抖动或超时，一次就判死会让主备在网络毛刺上反复切换。

与实现无关的失败不计入健康。缺收件地址、正文为空、内容超长这类问题换哪个实现都是同样的结论，把它们算在渠道头上，一条格式错误的消息就能把整条通道标红。回退链上每次非终止失败各记一次，最终成功的那次也记一次，因此「重试后成功」的实现在快照里是 `up`，同时 `failures` 保留曾经失败过的次数。

主动探测只回答「通不通」，不做真实投递：SMTP 只建立 TCP 连接（不发 EHLO、不做认证，避免在中继的登录审计里留下周期性记录），站内信检查收件箱文件能否拿到追加写句柄，console 实现没有外部依赖恒为可用。Slack webhook 没有实现探测——验活只能往群里发一条消息，这不是运维检查该干的事，因此它在快照里 `probe_supported=false`，健康只由投递结果推动。探测只针对启用中的实现，逐个串行执行（实现数量是个位数，省掉并发控制换来可预期的单轮耗时），单次超时由 `XW_PROVIDER_PROBE_TIMEOUT` 约束。

健康状态本身不参与选路：谁先发、失败了换谁由回退链决定。如果健康状态也去改选路，同一个问题会有两处判断，出故障时说不清是谁做的决定。它的用途是让运维看见现状，并据此手动停用某个实现。停用是明确的运维决定，所以某个渠道的实现全部被停用时，投递记为失败并给出「实现均已停用」，而不是退化为 mock 打印并算作成功；完全没有配置实现的渠道仍保持 mock 打印，那是本地跑通链路时的默认行为。重新启用会清空旧的失败计数，否则一个因连续失败被摘掉的实现刚回到序列里就显示 `down`。

```bash
# 看汇总：几条启用中、几条 up/degraded/down
curl -s 127.0.0.1:8081/admin/providers | jq '.summary'
# 摘掉一条不稳定的实现，再手动探一次确认
curl -X POST 127.0.0.1:8081/admin/providers/disable \
  -H 'Content-Type: application/json' -d '{"ids":["smtp-email"]}'
curl -X POST 127.0.0.1:8081/admin/providers/probe
```

API 进程也暴露同一份注册表的状态与启停入口，供看板与运维脚本直接调用（上面那组接口在引擎侧、只绑回环；这组走正常的 API Key 鉴权）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/v1/providers?channel=email&deep=true&refresh=true` | 列出条目：是否启用、是否该渠道首选取用、健康结论与状态汇总，附脱敏后的配置；`deep` 触发在线探测，`refresh` 跳过缓存 |
| `GET` | `/v1/providers/{id}` | 单条状态；未登记返回 404 |
| `POST` | `/v1/providers` | 新增渠道配置：`{"channel","kind","id?","priority?","enabled?","credentials"?}`；标识重复 409，配置不合法 400 |
| `PUT` | `/v1/providers/{id}` | 改种类 / 优先级 / 启停 / 整组凭据，没传的字段保持原样；改渠道 409 |
| `PUT` | `/v1/providers/{id}/enabled` | 请求体 `{"enabled": false}`，可选 `persist` 把改动写回配置文件 |
| `DELETE` | `/v1/providers/{id}` | 摘除条目与实现，`?persist=true` 一并从配置文件里删除 |

两侧的口径不同是刻意的。引擎侧记录投递结果与周期探测，回答「这条通道最近稳不稳」；API 侧回答「此刻能不能接活」——默认只做配置检查（是否启用、是不是该渠道的首选），带 `deep=true` 才让实现自己探一次：SMTP 连一次端口就断开，站内信确认收件箱目录可写，console 与推送 mock 没有外部依赖直接报可用。结论里的 `probed` 字段区分「真的探过」与「只看了配置」，没有探测能力的实现不会假装探过。探测结果按条目缓存 `XW_PROVIDER_HEALTH_TTL`（默认 30 秒），看板轮询不会把中继连出一片连接；条目被启停替换后旧结论自动失效。

停用之后该条目不再参与取用，投递落到同渠道的次选实现；停到某个渠道一条不剩时，接口返回 `warning` 并把该渠道列进 `unboundChannels`，随后的投递会在执行明细里记为失败「渠道没有可用的实现」——比在活动流里发现成片失败要早一步。启停默认只作用于当前进程；带 `persist=true` 时按「读原文 → 改一项 → 临时文件 + `rename`」写回 `XW_PROVIDERS_FILE`，顶层 `retries` 段与条目里的凭据原样保留，写失败则请求失败、内存状态不变，不会出现「接口说已停用、重启后又回来」。已运行的引擎读的是启动时的配置，写回对它不生效，需要重启引擎或改用引擎侧的启停接口。

新增、修改、删除走同一条写入路径，语义上守住三点。标识是寻址键：`POST` 时已存在的标识按冲突返回 409，不会被隐式覆盖；`id` 留空则按渠道自动编号（`email`、`email-2`…），这种条目无法写回配置文件，因此 `persist=true` 必须带显式标识。校验先于落表：形状（渠道、种类、凭据键名）与构造（凭据能否撑起实现，例如 SMTP 配了口令却没有用户名）任何一步不过，注册表与配置簿都不会留下半个条目；尚未接入实现的种类（如 `fcm`）在这里直接拒绝，而不是登记一个永远投不出去的条目。渠道不可改：把一个标识从 email 挪到 sms，会让执行明细里的历史记录指向另一件事，需要换渠道就删除后重建。

凭据只留在 API 进程的配置簿里，注册表拿到的只是实现对象。回显因此做了脱敏：`password`、`*_token`、`*_key`、`webhook_url` 这类键统一显示为 `***` 并单列在 `secretKeys` 里，`host`、`port`、`username` 这类排查时要看的项照原样返回，空值与掩码区分开——「没配」和「配了但不给你看」不是一回事。修改时 `credentials` 是整组替换而不是逐键合并：删掉一个可选凭证与改一个值同样常见，合并语义下前者做不到。

消费进程默认运行 `XW_WORKERS` 个消费者，各自独立完成「领取 → 处理 → 确认」。领取与登记在同一个 Lua 脚本里执行，同一条消息不会被两个消费者拿到；处理顺序也不承担语义，下游步骤只依赖消息自身的 `trigger_id`。收到退出信号后消费者停止领取新消息，正在处理的那条继续跑完并确认；超过 `XW_SHUTDOWN_GRACE` 仍未结束的会被放弃等待——这些消息保持未确认状态，租约到期后由回收流程重新投递，因此关闭流程不会丢消息。

单条领取时每条消息要两次脚本往返（领取一次、确认一次），Redis 单线程逐条执行，消费者开得再多也只是把请求排进同一个执行队列。`XW_PREFETCH` 大于 1 时改用批量领取：一条脚本内取出至多 N 条并逐条登记在途，处理完再一次性确认，语义与单条一致（取出即登记、确认才删除），队列为空时仍退回单条领取借它的阻塞等待。批量越大，一批消息离租约到期越近，所以默认关闭；开启后的实测对比见 [`docs/benchmark.md`](docs/benchmark.md)。

工作流里的 `delay` 步骤（例如「评论后等 10 分钟再补一封邮件」）不会占着消费者等待，而是把「到期时刻 + 剩余步骤」登记成一条延迟任务，由调度器到点续跑。这些任务存在 Redis 里：`<XW_DELAY_PREFIX>:due` 是 ZSET，分数为到期时刻的毫秒时间戳；`<XW_DELAY_PREFIX>:items` 是任务体哈希（载荷、已重试次数）；`<XW_DELAY_PREFIX>:inflight` 是已认领未确认的任务，分数为租约截止时刻。调度器每秒用 `ZRANGEBYSCORE … LIMIT` 取一批到期任务，认领成功后执行、失败按指数退避重新排期（上限 3 次）。登记（`HSET`+`ZADD`）、取出（`ZRANGEBYSCORE`+`HGET`）、认领（`ZREM`+`ZADD`）、回收（改写任务体 + 写回时间轴 + 清在途）各自在一条 Lua 脚本里完成，读到的永远是某个时刻的完整快照，不会出现「时间轴上有 id、任务体却读不到」的中间态；因此发布重启、实例漂移都不影响未到期任务，多个实例同时扫描时同一条任务也只有一个能认领成功。

认领带可见性超时，关闭了「认领之后、执行之前」崩溃导致单条通知消失的窗口：认领时把任务挪进 `inflight` 并写下 `now + XW_DELAY_LEASE`，执行成功才确认清理，执行失败则连同新的退避时刻一起写回时间轴。如果进程在两步之间被杀，这条记录就停在 `inflight` 里，下一轮扫描（或重启后的第一轮，调度器启动即主动扫一次）发现租约过期后把它放回时间轴，到期时刻置为当前时间，同一次扫描里就会被重新认领执行；尝试次数随之累加，反复把进程带崩的载荷在达到上限后丢弃并记错误日志，不会无限重投。租约要设得比一次续跑的总预算长（默认 `2m` 对 `XW_WORKFLOW_TIMEOUT=60s`），否则任务还在跑就被判失效，同一份通知会重复投递。Handler 里的 panic 被就地兜住算作一次失败，一个畸形载荷不会把整个调度循环带走。

启用外部后端示例：

```bash
XW_BUS=redis XW_STORAGE=postgres .venv/bin/uvicorn app.main:app --reload
# 首次启用 Postgres 需先建表：
psql "$XW_DATABASE_URL" -f scripts/migrations/001_messages.sql
psql "$XW_DATABASE_URL" -f scripts/migrations/003_activity_log.sql
psql "$XW_DATABASE_URL" -f scripts/migrations/004_execution_details.sql
# 查询索引：活动流/统计、执行明细的复合索引
psql "$XW_DATABASE_URL" -f scripts/migrations/005_query_indexes.sql
psql "$XW_DATABASE_URL" -f scripts/migrations/006_execution_indexes.sql
```

### 存储后端

`XW_STORAGE` 统一切换消息、活动日志、执行明细与工作流模板的落库方式，两个后端的定位不同：

| 后端 | 定位 | 行为 |
| --- | --- | --- |
| `jsonl`（默认） | 本机开发与演示 | 追加写 `api/data/*.jsonl`，一行一条记录；查询从文件尾部倒序分块读，够一页就停。写入是「打开文件追加」而不是事务，多进程共享同一份文件没有写入协调，稀疏过滤条件仍会退到顺序扫描 |
| `postgres` | 生产 | 写入走事务与索引；查询在数据库侧完成过滤、排序与聚合（`WHERE` + `ORDER BY … LIMIT`、`count(*) FILTER`），接口层只拿一页 |

生产环境（`XW_ENV=production`）如果仍是 `jsonl`，API 会在启动日志里直接告警并提示改用 `postgres`——文件后端没有跨进程写入协调，多实例部署下会出现计数与列表对不上的情况。选择后端不影响链路语义：队列的至少一次投递、聚合去重、收件箱按 `trigger_id:step_id` 去重都在上层完成。

收件箱本身由引擎与 API 侧的站内信实现共同写入文件卷、API 读取同一份文件（默认 `api/data/inbox.jsonl`，可用 `XW_INBOX_FILE` 指定），已读状态单独记在 `api/data/read_state.jsonl`，这两者的定位与 `XW_STORAGE` 无关；跨机部署时需把该目录挂成共享卷，或自行替换为数据库实现。

### 活动流与统计的查询路径

活动流（`GET /v1/activity`）不再把整张表读进内存再筛选：过滤条件下推到存储，排序由存储保证，接口层只拿一页。

- **翻页用游标**：`limit` 给单页条数（默认 50，上限 200），响应里的 `nextCursor` 原样回传取下一页，`hasMore` 表示还有没有更多。游标是上一页最后一条记录的 `(created_at, trigger_id)`，翻页期间有新触发写入也不会漏读或重复；非法游标返回 400。用游标而不是 `offset`，是因为活动流一直在追加，`offset` 在数据变动时会跳行。
- **Postgres**：`WHERE` + `ORDER BY created_at DESC, trigger_id DESC LIMIT`，配套 `005_query_indexes.sql` 的复合索引（索引列顺序与排序键一致，过滤条件直接走索引前缀，不需要额外排序）。
- **JSONL**：从文件尾部倒着按块读，取满一页即停，读取量取决于页大小而不是文件大小。这个后端定位是 demo——过滤条件太稀疏时仍会退到顺序扫描。

执行明细（`GET /v1/executions`）用同一套翻页约定：按触发标识 / 渠道 / 状态 / 时间范围过滤，`nextCursor` 取下一页。游标是上一页最后一条记录的 `(created_at, id)`——一次触发会写出多条渠道明细，触发标识不足以区分同一次触发的记录，因此次级键取记录自身的标识（JSONL 写入时生成，Postgres 用自增主键），配套 `006_execution_indexes.sql` 的复合索引。

### 模板渲染与注入防护

步骤的标题与正文是模板，变量来自触发负载：帖子标题、评论正文、互动者昵称都由终端用户写入。这类内容一旦直接拼进渠道报文，就能改变报文结构而不是「显示成一段文字」，因此渲染阶段（`internal/pipeline/render.go` 与 `sanitize.go`）按产出形态做了分层处理：

| 产出形态 | 清理规则 | 防的是 |
| --- | --- | --- |
| 单行（邮件主题、推送/站内信标题） | 控制字符当分隔符、连续空白压成一个、去掉首尾空白 | 标题里的 `\r\n` 会往邮件里加一行头字段（`Bcc:`），也会在日志里伪造一条记录 |
| 多行正文 | 保留换行与制表符，剥掉其余控制字符，CRLF 归一成 LF | NUL、DEL、C1 控制字符让下游解析器行为不可预期 |
| HTML 正文 | 先剥控制字符，再按 HTML 转义变量值 | 评论里的 `<script>` 被当成页面代码执行 |

ANSI 转义序列按整段移除，而不是只丢 ESC：`ESC[31m` 光标色、`ESC]0;…BEL` 终端标题都会被完整吃掉，否则清理后还有一串 `[31m` 留在正文里冒充文字。变量值里的模板语法不会被二次求值——替换是单遍的，评论里写 `{{ payload.secret }}` 只会原样显示，这条性质由 `TestRenderDoesNotReexpandValues` 锁定。

两个边界条件按失败处理而不是静默截断：单次渲染输出超过 256 KiB 直接报错（拦截「模板里一堆表达式、每个变量值都很大」的放大效应），渲染后的标题为空时改用工作流名（而不是把 `{{ payload.post_title }}` 原样发出去）。

分发阶段会再清理一次标题与正文，聚合卡片走的是另一条渲染入口，渠道层不该依赖上游是否记得清理；清理函数本身幂等。页面这一层同理：`dashboard/data.js` 的 `esc()` 是拼进 `innerHTML` 之前的唯一入口，收件箱标题、评论正文、工作流名都经过它。

### 投递结果的回写

执行明细由引擎在每次投递之后写入（`engine/internal/execution`）：分发阶段拿到的每个步骤结果折算成一行 `(trigger_id, channel)` 记录，`status` 为 `sent` / `failed`，`reason` 是失败原因，`duration_ms` 是这一次投递的耗时（含回退链上每一次失败的尝试与重试的退避等待）。除 API 侧约定的字段外还带四项排查信息：`step_id`（哪个步骤）、`provider`（实际完成投递的实现）、`attempts`（按顺序试过哪些实现，同一实现重试时会重复出现）、`fallback`（是否由备选兜底）。

- **续跑也要记账**：`delay` 之后由调度器续跑的步骤同样写明细，否则触发时刻的明细只有前半程，后面的渠道查不到结果。
- **没投递就不写**：被过滤、条件不满足、上游失败短路、只是登记了延迟的步骤不产生记录。「没发」和「发失败」混在同一张表里，会让失败原因变成噪音。
- **写失败不影响投递**：记录写不进去只记 Warn——消息已经发出去了，因为它没记上账而重投一次反而会再发一遍。

回写落在 `api/data/executions.jsonl`（`XW_EXECUTION_FILE` 可指定，写 `off` 关闭），与 API 侧消费线程写入的是同一份文件，`GET /v1/executions` 一起返回：引擎是 `XW_BUS=redis` 时的实际消费方，不写这一份，后台就只能看到触发、看不到每个渠道最后有没有发出去。`XW_STORAGE=postgres` 时两者尚未合并——API 自己写的记录进数据库，引擎这一侧仍写文件，把引擎回写接进数据库需要额外的驱动依赖，暂未实现。

```json
{"id":"9e33abb09bd8e9e5efb500ba9fba88a4","trigger_id":"smoke-exec-2","workflow":"post_commented",
 "subscriber_id":"creator_1","step_id":"mail","channel":"email","status":"failed",
 "reason":"投递失败（email）: SMTP 连接 127.0.0.1:1 失败：dial tcp 127.0.0.1:1: connect: connection refused",
 "provider":"smtp-email","attempts":["smtp-email"],"duration_ms":0,
 "created_at":"2026-11-02T02:17:45.509555+00:00"}
```

时间戳固定六位小数并以 `+00:00` 表示 UTC，而不是 Go 默认的 `RFC3339Nano`：写入方与读取方是两个进程、两种语言，格式要按对方的解析能力来定。`api/tests/test_execution_log.py` 用一行硬编码的引擎记录锁住这个约定。

仪表盘统计（`GET /v1/dashboard/stats`）同样不保留原始记录：Postgres 侧用 `count(*) FILTER` / `jsonb_array_elements_text` / `date_trunc` 三条聚合查询直接出结果，JSONL 后端逐行流式累加，内存占用与记录条数无关。

收件箱（`GET /v1/inbox`）的未读计数不重复扫描：每个订阅者的快照（消息列表、未读条数、消息总数）按「收件箱来源版本 + 该订阅者已读版本」缓存，版本没变时直接返回缓存，既不读收件箱文件也不把投递记录读进内存。版本探测本身很廉价——收件箱文件取 `stat`，JSONL 投递存储取 `stat`（大小 + 修改时刻），Postgres 取 `pg_stat_user_tables` 的写入计数并叠加进程内写入计数补齐统计延迟；标记已读会让该订阅者的版本自增。缓存按最近使用淘汰，最多保留 256 个订阅者、每人 200 条消息（与接口 `limit` 上限一致），未读条数与消息总数仍按全部消息统计，不会被 `limit` 截断。自定义存储后端若没有实现 `revision()`，缓存退化为每次都重算——正确性优先于命中率。

### 投递结果的回调

执行明细是拉取式的：接入方想知道结果就得来查。配置 `XW_CALLBACK_URL` 后，每次执行结束引擎会把这条触发的各渠道结果推到接入方自己的端点（`engine/internal/webhook`），事件名 `delivery.result`：

```json
{"id":"52df32f02e7ff121a097f706b17c8e03","event":"delivery.result",
 "occurred_at":"2026-11-02T02:40:53.537140+00:00","trigger_id":"smoke-cb-1",
 "transaction_id":"tx-cb-1","workflow":"post_liked","subscriber_id":"creator_1",
 "status":"sent","summary":{"total":2,"sent":2,"failed":0},
 "deliveries":[{"step_id":"notify","channel":"in_app","status":"sent",
                "provider":"console-in-app","attempts":["console-in-app"],"duration_ms":0},
               {"step_id":"mail","channel":"email","status":"sent",
                "provider":"console-email","attempts":["console-email"],"duration_ms":0}]}
```

- **整体状态分三档**：`sent` / `failed` / `partial`。一次触发会落到多个渠道，三条里两条成功既不算成功也不算失败，分开报，由接入方按自己的容忍度决定告警还是重试。
- **只报投递过的步骤**：被过滤、条件不满足、上游短路、仅登记延迟的步骤不进 `deliveries`；一条都没有时不发回调——「没发」不是「发失败」，拿它去回调只会让接收方以为出了故障。
- **异步且有界**：事件先进缓冲队列（`XW_CALLBACK_QUEUE`，默认 256），由 `XW_CALLBACK_WORKERS`（默认 2）个 goroutine 提交，单次超时 `XW_CALLBACK_TIMEOUT`（默认 5s）。队列满时丢弃并计数，不阻塞消费路径——接收方变慢不该让通知投递停摆。
- **退出时排空**：收到退出信号后停止收新事件，等队列里的事件发完，最多等到 `XW_SHUTDOWN_GRACE`；没发完的记为丢弃并写日志，不会让一个不响应的接收方拖住进程。
- **地址约束**：只接受 https，http 仅放行本机回环（本地联调）；带用户名口令或 `#` 片段的地址在装配期就被拒绝。日志和错误信息里只出现协议与主机，路径与查询串里的令牌不会落盘。
- **与明细同源**：`delay` 之后的续跑同样补推一条，否则接入方只拿得到前半程。字段与执行明细一致（`provider` / `attempts` / `fallback` / `duration_ms` 都在），接入方不必再查一次 `GET /v1/executions`。
- **失败分级**：4xx 视为报文或鉴权问题，除 429 外不再尝试；5xx、429、连接失败与超时标为可重试。
- **可重试失败按退避再发**：单条事件默认最多试 3 次（`XW_CALLBACK_RETRY_ATTEMPTS`），间隔从 `XW_CALLBACK_RETRY_BASE` 起翻倍、到 `XW_CALLBACK_RETRY_MAX` 封顶并向下抖动。接收方给了 `Retry-After`（429/503）时按对方的时间等，但仍受封顶约束——对方要求等一小时时，这条回执早就没有意义了，继续占住发送 goroutine 更坏。重试沿用同一个 `X-XW-Event-Id`，接收方按它去重即可。退出宽限到期会打断正在等待的退避，已经发出去的请求仍会跑完。
- **报文签名**：配置 `XW_CALLBACK_SECRET` 后，每个请求带 `X-XW-Timestamp`（Unix 秒）与 `X-XW-Signature: v1=<hex>`。签名内容是 `HMAC-SHA256(secret, "<timestamp>.<body>")`，时间戳与正文一起签——只签正文，报文能被搬到新时间戳下重放；只签时间戳则护不住内容。接收方应校验签名并比较时间戳与本地时钟（默认容差 5 分钟），`engine/internal/webhook` 里的 `Verifier` 就是这套逻辑的可执行版本。头部允许空格分隔的多个签名值，密钥轮换期间接收方可以同时认新旧两把。

报文里两个头部让接收方在读完正文之前就能路由与去重：`X-XW-Event`（事件名）与 `X-XW-Event-Id`（与报文里的 `id` 相同）。

接收端校验签名只需要几行：

```python
import hashlib, hmac, time

SECRET = b"<与 XW_CALLBACK_SECRET 相同>"

def verify(headers, raw_body: bytes, tolerance: int = 300) -> bool:
    ts = headers.get("X-XW-Timestamp", "")
    sig = headers.get("X-XW-Signature", "")
    if not ts.isdigit() or abs(time.time() - int(ts)) > tolerance:
        return False                     # 缺时间戳，或超出容差（可能是重放）
    expected = "v1=" + hmac.new(SECRET, ts.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)
```

比较用固定时间实现（`compare_digest`），不要用 `==`：字符串比较在第一个不同的字节就返回，能把密钥一位一位试出来。校验必须用原始字节，先解析再重新序列化会改变字段顺序，签名随之失效。

### 投递指标

执行明细是逐条审计，回答「这一次触发到底发到哪了」；指标是聚合观察，回答「这条通道最近表现如何」。两者同源——都从同一次执行的步骤结果折算——但驻留位置和口径不同。指标累在引擎进程内存里（`engine/internal/metrics`），经运维接口读取：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/metrics` | 当前实例自启动以来的投递指标：合计、按渠道、按实现，含成功/失败/超时与延迟分布 |
| `POST` | `/admin/metrics/reset` | 清空计数并重新计时，返回重置后的空快照 |

```json
{"startedAt":"2026-11-03T02:19:02Z","uptimeSeconds":3600.5,
 "total":{"sent":18422,"failed":37,"attempted":18459,"errorRate":0.002,
          "retried":12,"fallback":3,"timeouts":1,
          "latency":{"count":18459,"maxMs":4821.2,"meanMs":7.4,
                     "p50Ms":3.1,"p90Ms":11.7,"p99Ms":219.5,
                     "buckets":[{"upperMs":1,"count":8121},{"upperMs":2,"count":4306}],
                     "overflow":0}},
 "channels":[{"channel":"in_app","sent":9100,"failed":2,"attempted":9102,
              "errorRate":0.0002,"retried":0,"fallback":0,"timeouts":0},
             {"channel":"email","sent":9322,"failed":35,"attempted":9357,
              "errorRate":0.0037,"retried":12,"fallback":3,"timeouts":1}],
 "providers":[{"provider":"console-in-app","sent":9100,"failed":2,"attempted":9102},
              {"provider":"smtp-email","sent":9319,"failed":35,"attempted":9354}]}
```

（渠道与实现条目里的 `latency` 结构与 `total.latency` 相同，示例中省去。）

口径有四处刻意的取舍：

- **只统计真实投递**。被过滤条件拦下、条件分支不满足、上游失败短路、仅登记延迟续跑、聚合卡片窗口内命中的步骤都没有投递动作，不进指标——把「按设计没发」混进失败数，告警就没法用了。某个渠道一条实现都没配置时的 mock 打印同样不计入：那是本地跑通链路的默认行为，不是一次投递。
- **超时按失败计**，单独给 `timeouts` 计数。执行明细里不记超时步骤（投递可能在被放弃等待之后才真的发出去，写成 failed 是误报），但指标必须记：超时正说明这条通道在拖住链路，是最该被看见的信号。
- **延迟样本含失败的尝试**。失败和超时同样占用了消费者的时间，只看成功的分布会把「又慢又发不出去」的通道藏起来。`duration` 取分发阶段的耗时，包含回退链上每一次失败的尝试和退避等待，因此它反映的是「这条消息在这个渠道上卡了多久」，不是单次网络往返。
- **重试与回退单独计数**。`retried` 是首次之外多打的次数，`fallback` 是备选完成投递的次数。成功数正常但这两个值在涨，说明通道正在靠重试和备选兜底，这是延迟升高之前的早期信号。

延迟用固定桶直方图而不是保留原始样本：长跑进程里一条消息一个样本，内存会随流量单调增长，而且迟早要面对「保留多久」这个没有正确答案的问题。桶上界默认 `1,2,5,10,25,50,100,250,500,1000,2500,5000,10000` 毫秒，分位数在桶内线性插值估算，并以实际观测到的最大值收口——插值不会凭空造出没发生过的延迟。落在最大桶之外的样本进 `overflow`，分位数落在这一档时回报实际最大值。因此 `p99` 是「桶宽内可信」的估计，不是精确分位；需要精确值时看 `buckets` 的分布。

指标是单实例口径：进程重启归零，多实例部署时由抓取端按实例分别取。这是刻意的——把计数搬到 Redis 会让每条消息多一次往返，而延迟本身在投递路径上，指标不该成为它的成本。目前只提供 JSON，Prometheus 文本格式与跨实例汇总尚未接入，抓取端需要自己做一次按实例的聚合。

进程退出时会把累计值打一条日志，这是内存计数消失前唯一的落点：

```
msg="[engine] 投递指标累计" attempted=2 sent=2 failed=0 timeouts=0 retried=0 fallback=0
    errorRate=0 p50ms=0.5 p99ms=0.668 uptimeSeconds=4.029 byChannel="map[email:1 in_app:1]"
```

## 测试

两个进程各有一套测试，都只依赖本机环境：Python 侧走 `pytest`（进程内 HTTP 客户端与假网关），Go 侧走标准 `testing`（Redis 相关用例默认用进程内实现，需要真实 Redis 的用例只在设了 `XW_TEST_REDIS_ADDR` 时启用）。

```bash
# API 与 Python 侧渠道实现
api/.venv/bin/python -m pytest api/tests -q

# Go 引擎；渠道、管线与装配入口这几块并发路径多，单独跑一遍竞态检测
cd engine && go test ./... && go test -race ./internal/provider/ ./internal/pipeline/ ./internal/runner/ ./internal/webhook/ ./cmd/engine/
```

渠道实现除各自的细节测试外，还有一层跨实现的契约测试（`engine/internal/pipeline/provider_contract_test.go`），只验所有实现共有的部分：标识非空、互不重复且与服务渠道一致；本地就能判定的失败（缺地址、地址非法、正文为空、载荷超限）统一报成「失败 + 终态 + 原因」，避免回退链把一条必然被拒的消息试遍备选；零值投递不 panic 且失败原因可读；没有外部依赖时探测恒为可用、需要落盘或连网的实现如实报错；并发调用不出数据竞争。新增一个渠道实现时把它加进同一张表，这些约束即自动覆盖——细节规则仍由各自的实现测试负责。

渠道层另有一条端到端用例（`engine/cmd/engine/phase3_integration_test.go`）：按启动路径装配渠道配置，邮件投到进程内的 SMTP 服务器、群播报投到进程内的 HTTP 接收端，再比对四处结论是否一致——执行明细里的实际实现与尝试序列、指标里的成败与回退计数、健康状态、回调报文。回调按接入方的约定签名，接收端用同一份密钥校验，签名不符回 401、事件不入库。覆盖四个渠道全部投出、主实现故障后重试再回退、本地校验失败不回退，以及运行期停用全部实现时如实失败四条路径。

## 压测

`engine/cmd/xwbench` 压测队列链路：按指定并发与目标速率灌入消息，再由同进程内的消费者领取、处理、确认，输出吞吐、端到端延迟分位、错误率与进程内存。默认使用独立队列键 `xw:queue:bench`，不会干扰触发链路的消息。

```bash
cd engine
go build -o bin/xwbench ./cmd/xwbench

# 只测队列机制：解码即算完成，20000 条，16 个生产者并发，8 个消费者
./bin/xwbench -total 20000 -concurrency 16 -workers 8

# 走完整执行管线：点赞聚合 → 站内信 → 推送
./bin/xwbench -total 5000 -workers 8 -pipeline

# 限速 5000 条/秒，灌完输出 JSON 便于留档比较
./bin/xwbench -total 10000 -rate 5000 -workers 8 -json
```

| 参数 | 说明 |
| --- | --- |
| `-redis` / `-queue` | Redis 地址与队列键，默认 `localhost:6379` / `xw:queue:bench` |
| `-total` / `-concurrency` | 总条数、生产者并发数 |
| `-rate` | 目标速率（条/秒），0 表示不限速 |
| `-workers` | 消费者数量 |
| `-prefetch` | 每个消费者每次领取的条数，默认 `1`（逐条）；大于 1 时走批量领取 |
| `-work` | 每条消息的模拟处理耗时，用来给慢渠道一个可解释的基线 |
| `-pipeline` | 消费者执行真实管线；不带时只解码消息 |
| `-drain` | 灌完后等待全部确认的上限，超时会标注在报告里 |
| `-flush` | 开跑前清空该队列的衍生键（主队列、在途、重试、死信） |
| `-json` | 输出 JSON 而非文本报告 |

限速走「预约发送时隙」：每个生产者从共享时钟里领走下一个应发时刻，各自 sleep 到点。落后于计划时以当前时刻为界，不做追赶补偿——补偿会把平滑的限速变成脉冲，P99 立刻失真。

报告里的几个口径需要先明确，否则数字容易被误读：

- **吞吐** = 已处理条数 / 端到端窗口（首条入队 → 最后一次确认），不是 Redis 的写入速度；窗口终点不取收尾函数返回的时刻，避免把收尾时仍在等待消息的消费者算进分母；
- **端到端延迟** = 消息体内 `bench.sent_at` 打点到确认完成的间隔，包含在队列里的排队时间。不限速灌入时它主要反映积压深度，限速到入口速率时才是真正的处理延迟；
- **错误率** = 处理失败 / 已尝试条数。失败消息仍会被确认，不让重投把统计搅成噪音；
- **内存** 取 Go 运行时口径（堆、在用、保留、累计分配、GC 次数），跨平台可得；没有进程 RSS，是因为它需要按平台各自实现。

一轮参考数据（Apple M4 Pro / 12 核、本机 Redis 8.10、单机同进程；完整的场景表与复现命令见 [`docs/benchmark.md`](docs/benchmark.md)）：

```
$ ./bin/xwbench -redis localhost:6399 -total 20000 -concurrency 16 -workers 8 -prefetch 32
压测结果  [xw:queue:bench]
  负载      20,000 条 · 生产者并发 16 · 不限速 · 消费者 8（每次领取 32 条）· 每条模拟处理 无
  写入      20,000 成功 / 0 失败，用时 371.5ms（53,839 条/秒）
  消费      20,000 处理 / 0 失败，窗口 371.6ms
  吞吐      53,823 条/秒
  端到端    P50 400µs · P90 600µs · P99 800µs · 最大 7ms · 平均 400µs
  错误率    0.00%
  内存      堆 1.9 MiB（在用 4.1 MiB）· 保留 18.8 MiB · 累计分配 73.8 MiB · GC 55 次 · goroutine 3
```

同样 8 个消费者、同样参数但逐条领取时，吞吐约 21.5k 条/秒、P99 约 600ms：消费追不上写入，
延迟全是排队时间。换成完整管线（每次领取 32 条）时吞吐约 50k 条/秒、P99 约 25ms，
差额来自聚合卡片的读改写与站内信写入这一段串行工作。

## 开发计划

见 [ROADMAP.md](ROADMAP.md)。开发顺序是：先打通端到端 demo，再逐块扩展领域模型、工作流引擎、渠道层、订阅者/偏好、管理端与 SDK。

## License

见 [LICENSE](LICENSE)。
