# xw-notify

一个自主开发的电商交易触达平台（E-commerce Transaction Notifications）。用一个统一的触发 API 和一套工作流模型，把订单、支付、发货、物流等交易事件自动分发到买家所在的各个渠道：收件箱、邮件、短信、推送。

## 定位

- **一套 API，多端触达**：电商业务侧只调用一次触发接口，剩下的渠道选择、模板渲染、去重、聚合、重试都由平台处理。
- **工作流驱动**：通知按「工作流（Workflow）+ 步骤（Step）」编排，支持收件箱、邮件、短信、推送、聊天、延时、摘要（Digest）等步骤类型。
- **订阅者与偏好**：每个买家一个订阅者档案，按「全局 / 工作流 / 渠道」三级控制是否接收。
- **多语言实现**：API 与渠道集成层用 Python(FastAPI)，执行引擎与调度用 Go。

## 典型交易场景

| 交易事件 | 触发标识 | 触达渠道 |
| --- | --- | --- |
| 订单支付成功 | `order_confirmed` | 站内信 + 邮件 + 短信 |
| 订单已发货 | `order_shipped` | 邮件 + 推送 |
| 订单已签收 | `order_delivered` | 站内信 |

## 架构

```
电商订单系统 ──trigger──▶  api (Python/FastAPI)  ──入队──▶  MQ (Redis / RabbitMQ)
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
                        Postgres（消息/买家/工作流/活动日志）
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

# 3. 触发一条订单通知（demo）
curl -X POST http://127.0.0.1:8000/v1/events/trigger \
  -H "Content-Type: application/json" \
  -d '{"name":"order_confirmed","to":{"subscriberId":"user_1","email":"hi@example.com"},"payload":{"customer":"Alice","order_id":"A10293","amount":"199.00"}}'
```

### 后端切换（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_BUS` | `memory` | 消息队列后端：`memory` / `redis` |
| `XW_REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串（`XW_BUS=redis` 时生效） |
| `XW_STORAGE` | `jsonl` | 持久化后端：`jsonl` / `postgres` |
| `XW_DATABASE_URL` | `postgresql+psycopg://xwnotify:xwnotify@localhost:5432/xwnotify` | Postgres 连接串（`XW_STORAGE=postgres` 时生效） |

启用外部后端示例：

```bash
XW_BUS=redis XW_STORAGE=postgres .venv/bin/uvicorn app.main:app --reload
# 首次启用 Postgres 需先建表：
psql "$XW_DATABASE_URL" -f scripts/migrations/001_messages.sql
```

## 开发计划

见 [ROADMAP.md](ROADMAP.md)。项目按 40 天逐步开发：先端到端 demo，再逐块扩展领域模型、工作流引擎、渠道层、订阅者/偏好、管理端与 SDK。

## License

见 [LICENSE](LICENSE)。
