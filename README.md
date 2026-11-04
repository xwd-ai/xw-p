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

# 3. 启动执行引擎（Go）
cd ../engine
go build -o bin/xw-engine ./cmd/engine && ./bin/xw-engine

# 4. 触发一条评论通知
curl -X POST http://127.0.0.1:8000/v1/events/trigger \
  -H "Content-Type: application/json" \
  -d '{"name":"post_commented","to":{"subscriberId":"creator_1","email":"hi@example.com","deviceToken":"fcm-token-9f2c1d","channels":["in_app","push","email"]},"payload":{"post_id":"P-2026-1001","post_title":"我的第一支 Vlog","actor":"小林","comment":"拍得真好，求教程！"}}'
```

前端直接打开 `dashboard/index.html` 即可看到内置示例数据；接真实后端时在页面注入 `window.XW_API_BASE` 指向 API 地址。

## 能力亮点

- **可靠投递**：至少一次语义——消息取出即登记在途、处理成功才确认，租约过期的消息由回收流程重新入队，进程崩溃不丢消息。
- **并发消费**：可配置的 worker pool，退出时等待在途消息处理完毕。
- **失败重试与死信**：指数退避 + 抖动，重试耗尽进入死信，运维接口可查询、重放或丢弃。
- **延迟任务持久化**：delay 步骤存 Redis，重启或换实例都能接上。
- **聚合**：digest 计数并发原子累加，窗口到期重开卡。
- **多渠道**：收件箱 / 邮件 / 短信 / 推送 / 群播报，主备回退、渠道级重试。
- **可观测**：逐条执行明细 + 投递指标 + 回调 HMAC-SHA256 签名；管理统计按渠道给出成功率与耗时分布、实现与工作流排行、失败原因榜单。
- **工作流治理**：模板可启停与复制。停用只翻开关、不推进版本号，单条触发、批量触发与分组广播统一拒绝新事件，已入队并携带快照的消息照常跑完；复制产出新模板（新步骤标识、`depends_on` 重映射、配置深拷贝），副本默认不启用。

实测吞吐 **约 53.8k 条/秒**（端到端 P50 400µs / P99 800µs），完整执行管线约 50k 条/秒。压测口径、参数与复现命令见 [`docs/benchmark.md`](docs/benchmark.md)。

## 测试

```bash
# Python：API 与渠道实现
api/.venv/bin/python -m pytest api/tests -q

# Go：引擎（含并发路径的竞态检测）
cd engine && go test ./... && go test -race ./internal/provider/ ./internal/pipeline/ ./internal/runner/ ./internal/webhook/ ./cmd/engine/

# 前端：收件箱组件的纯逻辑测试（Node 内置 runner，无第三方依赖）
node --test 'dashboard/tests/*.test.js'
```

## License

见 [LICENSE](LICENSE)。
