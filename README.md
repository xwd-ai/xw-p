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
engine/     Go：执行引擎、队列消费、provider 分发、digest
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

引擎侧：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_STEP_TIMEOUT` | `10s` | 单个步骤的执行预算，可被步骤自身的 `timeout` / `timeout_ms` 覆盖 |
| `XW_WORKFLOW_TIMEOUT` | `60s` | 一条消息一次执行的总预算，耗尽后剩余步骤不再尝试 |
| `XW_DIGEST_STORE` | `redis` | 聚合卡片存储：`redis`（跨实例共享）/ `memory`（单实例） |

超时是链路兜底：渠道卡住时管线不再无限等待，超时的步骤与后续步骤都会被记为失败/短路并写进执行明细。超时值应大于下游的正常延迟（例如设为渠道 P99 的数倍），而不是贴着平均值设置。

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
