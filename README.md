# xw-notify

一个自主开发的通知基础设施（Notification Infrastructure）。用一个统一的触发 API 和一套工作流模型，把产品、服务与 Agent 的消息分发到用户所在的各个渠道：收件箱、邮件、短信、推送、聊天（Slack / Teams 等）。

## 定位

- **一套 API，多端触达**：业务侧只调用一次触发接口，剩下的渠道选择、模板渲染、去重、聚合、重试都由平台处理。
- **工作流驱动**：通知按「工作流（Workflow）+ 步骤（Step）」编排，支持收件箱、邮件、短信、推送、聊天、延时、摘要（Digest）等步骤类型。
- **订阅者与偏好**：每个用户一个订阅者档案，按「全局 / 工作流 / 渠道」三级控制是否接收。
- **多语言实现**：API 与渠道集成层用 Python(FastAPI)，执行引擎与调度用 Java(Spring Boot)。

## 架构

```
业务系统 ──trigger──▶  api (Python/FastAPI)  ──入队──▶  MQ (Redis / RabbitMQ)
                                                        │
                                                        ▼
                                            engine (Java/Spring Boot)
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
                        Postgres（消息/订阅者/工作流/活动日志）
```

## 目录结构

```
api/        Python FastAPI：触发 API、管理 API、渠道集成
engine/     Java Spring Boot：执行引擎、队列消费、provider 分发、digest
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

# 3. 触发一条通知（demo）
curl -X POST http://127.0.0.1:8000/v1/events/trigger \
  -H "Content-Type: application/json" \
  -d '{"name":"welcome","to":{"subscriberId":"user_1","email":"hi@example.com"},"payload":{"user":"Alice"}}'
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
