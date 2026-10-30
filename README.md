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
  -d '{"name":"post_commented","to":{"subscriberId":"creator_1","email":"hi@example.com","channels":["in_app","push","email"]},"payload":{"post_id":"P-2026-1001","post_title":"我的第一支 Vlog","actor":"小林","comment":"拍得真好，求教程！"}}'
```

### 环境变量

API 侧：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_BUS` | `memory` | 消息队列后端：`memory` / `redis` |
| `XW_REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串（`XW_BUS=redis` 时生效） |
| `XW_STORAGE` | `jsonl` | 持久化后端：`jsonl` / `postgres` |
| `XW_DATABASE_URL` | `postgresql+psycopg://xwnotify:xwnotify@localhost:5432/xwnotify` | Postgres 连接串（`XW_STORAGE=postgres` 时生效） |
| `XW_API_WORKER` | 按总线决定 | API 内置消费线程开关；`XW_BUS=memory` 时默认开，`redis` 时默认关（队列归引擎消费） |

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
| `XW_SHUTDOWN_GRACE` | `30s` | 退出时等待在途消息处理完成的上限 |
| `XW_RETRY_MAX_ATTEMPTS` | `5` | 单条消息的最大处理次数（含首次），`0` 表示关闭退避重试 |
| `XW_RETRY_BASE_DELAY` | `2s` | 第一次重试前的等待时长，之后按 2 的幂增长 |
| `XW_RETRY_MAX_DELAY` | `1m` | 重试等待的上限 |
| `XW_RETRY_JITTER` | `0.3` | 抖动比例，实际等待在 `[d*(1-jitter), d]` 内随机，`0` 表示不抖动 |
| `XW_RETRY_INTERVAL` | `1s` | 到期重试写回队列的扫描间隔 |
| `XW_WORKFLOWS_FILE` | 空 | 工作流定义快照文件（JSON 数组）；仅用于给没有携带步骤的消息按名称回退查找 |
| `XW_ADMIN_ADDR` | `127.0.0.1:<ENGINE_PORT>` | 管理接口监听地址（死信查询/重放）；`off` 表示不启动 |

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

执行结果按消息汇总成一行日志：`steps` / `sent` / `failed` / `skipped` / `shortCircuited` / `deferred` 与耗时，配合 `trigger_id` 可以定位到具体是哪条互动、哪一个步骤出的问题。

一条消息只应被一个消费者处理：`XW_BUS=redis` 时队列归引擎消费，API 进程的内置消费线程默认关闭（`XW_API_WORKER=1` 可强制打开）；memory 总线是单进程 demo，才由 API 自己的线程消费。

队列按**至少一次**语义消费：引擎取出消息时同步写一份在途记录（`<queue>:processing` 保存原始 payload，`<queue>:processing:lease` 保存租约到期时间），处理成功后 `HDEL`/`ZREM` 才算消费完成；进程在处理中途退出时，消息仍在在途记录里，不会随进程消失。代价是极端情况下可能重复投递，因此下游写入需保持幂等（收件箱条目按 `trigger_id:step_id` 去重）。租约长度要大于单条消息的正常处理耗时，否则消息会在处理完成前被判定失效。

在途记录由回收流程负责收尾：引擎启动时先扫一轮、之后按 `XW_RECLAIM_INTERVAL` 周期扫描，把「租约到期 + 半个租约宽限」后仍未确认的消息原子地写回队列（`ZRANGEBYSCORE` 选过期租约、`RPUSH` 回主队列、`HDEL`/`ZREM` 清理，全过程在一个 Lua 脚本里完成，多实例并发回收不会重复入队）。这条路径覆盖两类情况：本进程上一轮崩溃留下的消息，以及集群里其他实例异常退出留下的消息。

处理失败的消息走退避重试，而不是等租约到期重投：worker 把消息放进重试集合（`<queue>:retry` 记可重试时刻、`<queue>:retry:body` 存原始消息体、`<queue>:retry:record` 记次数与首次失败时间），到期后由重试泵原子地写回主队列。等待时长按 `XW_RETRY_BASE_DELAY` 起步、逐次翻倍到 `XW_RETRY_MAX_DELAY` 封顶，再按 `XW_RETRY_JITTER` 向下抖动——同一批同时失败的消息不会在同一毫秒一起回到队列，避免下游刚恢复就被打满。失败次数按消息体哈希记账并带 TTL（覆盖整个退避窗口），因此计数不随重投丢失，消息体本身始终原样写回。处理次数达到 `XW_RETRY_MAX_ATTEMPTS` 后不再重试，转入死信列表 `<queue>:dead`，保留原始消息、失败次数、首次失败时间与最后一次失败原因。把这些状态从「在途记录」摘走与写入重试集合在同一个脚本里完成，所以重试中的消息不会被租约回收重复抓取。

### 死信运维接口

引擎启动时会在 `XW_ADMIN_ADDR`（默认本机 `ENGINE_PORT`）上开一组管理接口，用于查看、重放与丢弃死信。接口只绑定回环地址，需要跨机访问时显式改成 `0.0.0.0:8081` 之类的地址；`XW_ADMIN_ADDR=off` 则不启动。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/health` | 存活检查 |
| `GET` | `/admin/dead-letters?offset=0&limit=20` | 死信列表，最新在前，返回 `items` / `total`；`limit` 上限 200 |
| `POST` | `/admin/dead-letters/replay` | 请求体 `{"ids": ["…"]}`，把消息原样写回队列并移出死信 |
| `POST` | `/admin/dead-letters/discard` | 请求体同上，直接丢弃、不给第二次机会 |

```bash
# 看最近 10 条死信
curl '127.0.0.1:8081/admin/dead-letters?limit=10'
# 修好下游后把某条重放回队列（一次最多 100 条）
curl -X POST 127.0.0.1:8081/admin/dead-letters/replay \
  -H 'Content-Type: application/json' -d '{"ids":["3f2a…"]}'
```

`id` 是死信记录本身的 SHA-1：记录写入后内容不再变化，因此这个 ID 在查询与重放之间保持稳定。重放以「记录串本身」作为条件删除的依据，同一份死信被并发重放时只有一次能删掉它，消息不会重复入队；已被取走的 ID 会在响应的 `missing` 里返回，重复提交同一批 ID 是幂等的。重放的前提是失败原因已经排除，所以消息重新获得完整的重试次数（转入死信时计数键已清空），并按「优先处理」写回队列右端，而不是排在新消息后面再等一轮。

死信记录带完整原始消息，长时间不清理会让列表越来越长；重放与丢弃都要先扫一遍列表定位记录，因此运维上应按批次处理后清空，而不是把它当历史归档长期存放。

消费进程默认运行 `XW_WORKERS` 个消费者，各自独立完成「领取 → 处理 → 确认」。领取与登记在同一个 Lua 脚本里执行，同一条消息不会被两个消费者拿到；处理顺序也不承担语义，下游步骤只依赖消息自身的 `trigger_id`。收到退出信号后消费者停止领取新消息，正在处理的那条继续跑完并确认；超过 `XW_SHUTDOWN_GRACE` 仍未结束的会被放弃等待——这些消息保持未确认状态，租约到期后由回收流程重新投递，因此关闭流程不会丢消息。

工作流里的 `delay` 步骤（例如「评论后等 10 分钟再补一封邮件」）不会占着消费者等待，而是把「到期时刻 + 剩余步骤」登记成一条延迟任务，由调度器到点续跑。这些任务存在 Redis 里：`<XW_DELAY_PREFIX>:due` 是 ZSET，分数为到期时刻的毫秒时间戳；`<XW_DELAY_PREFIX>:items` 是任务体哈希（载荷、已重试次数）；`<XW_DELAY_PREFIX>:inflight` 是已认领未确认的任务，分数为租约截止时刻。调度器每秒用 `ZRANGEBYSCORE … LIMIT` 取一批到期任务，认领成功后执行、失败按指数退避重新排期（上限 3 次）。登记（`HSET`+`ZADD`）、取出（`ZRANGEBYSCORE`+`HGET`）、认领（`ZREM`+`ZADD`）、回收（改写任务体 + 写回时间轴 + 清在途）各自在一条 Lua 脚本里完成，读到的永远是某个时刻的完整快照，不会出现「时间轴上有 id、任务体却读不到」的中间态；因此发布重启、实例漂移都不影响未到期任务，多个实例同时扫描时同一条任务也只有一个能认领成功。

认领带可见性超时，关闭了「认领之后、执行之前」崩溃导致单条通知消失的窗口：认领时把任务挪进 `inflight` 并写下 `now + XW_DELAY_LEASE`，执行成功才确认清理，执行失败则连同新的退避时刻一起写回时间轴。如果进程在两步之间被杀，这条记录就停在 `inflight` 里，下一轮扫描（或重启后的第一轮，调度器启动即主动扫一次）发现租约过期后把它放回时间轴，到期时刻置为当前时间，同一次扫描里就会被重新认领执行；尝试次数随之累加，反复把进程带崩的载荷在达到上限后丢弃并记错误日志，不会无限重投。租约要设得比一次续跑的总预算长（默认 `2m` 对 `XW_WORKFLOW_TIMEOUT=60s`），否则任务还在跑就被判失效，同一份通知会重复投递。Handler 里的 panic 被就地兜住算作一次失败，一个畸形载荷不会把整个调度循环带走。

启用外部后端示例：

```bash
XW_BUS=redis XW_STORAGE=postgres .venv/bin/uvicorn app.main:app --reload
# 首次启用 Postgres 需先建表：
psql "$XW_DATABASE_URL" -f scripts/migrations/001_messages.sql
```

## 开发计划

见 [ROADMAP.md](ROADMAP.md)。开发顺序是：先打通端到端 demo，再逐块扩展领域模型、工作流引擎、渠道层、订阅者/偏好、管理端与 SDK。

## License

见 [LICENSE](LICENSE)。
