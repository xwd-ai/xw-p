# 安全清单与已知限制

部署与集成 xw-notify 时的安全对照清单。机制细节不在本文重复，见各专项
文档；本文回答的是「上线前要确认什么」和「哪些风险仍然存在」。

## 机制一览

| 关注点 | 机制 | 详细文档 |
| --- | --- | --- |
| 认证 | 服务端 `X-Api-Key`（`XW_API_KEYS` 声明租户、环境与可选 `roles`）+ 运营后台 Bearer 令牌（HS256，`XW_JWT_SECRET`） | [auth.md](auth.md) |
| 授权边界 | `X-Tenant-Id` / `X-Environment` 双边界：跨租户 403、跨环境 404，写操作只落请求头环境 | [isolation.md](isolation.md) |
| 注入防护 | 模板变量单遍替换（值里的模板语法不再求值），标题/正文/HTML 三条清理路径（控制字符、ANSI 转义、HTML 转义），输出 256 KiB 上限；前端拼接 `innerHTML` 的后端字符串统一转义 | 引擎 `internal/pipeline/sanitize.go`，前端 `dashboard/data.js` |
| 幂等 | `Idempotency-Key` 去重，键冲突 409 且不入队 | [idempotency.md](idempotency.md) |
| 限流 | 令牌桶（凭据维度优先、IP 兜底），默认关闭 | [rate-limit.md](rate-limit.md) |
| 回调验签 | HMAC-SHA256 覆盖时间戳与正文（`X-XW-Timestamp` / `X-XW-Signature`），接收端 5 分钟容差、按事件 ID 去重 | [callbacks.md](callbacks.md) |
| 密钥存放 | 环境变量与 `XW_PROVIDERS_FILE` 配置文件；镜像不携带任何密钥 | [deploy.md](deploy.md) |
| 日志卫生 | 不记录请求体、`X-Api-Key`、`Authorization`、查询串；错误文本脱敏（连接串口令） | [logging.md](logging.md)、[health.md](health.md) |
| 审计 | 回调每次尝试写审计（含丢弃）、事件入队前归档可重放；逐条执行明细 | [callbacks.md](callbacks.md) |

## 部署安全清单

上线前逐项确认：

- [ ] **配好凭据再暴露**：`XW_API_KEYS`（服务端调用）与 `XW_JWT_SECRET` + `XW_ADMIN_USERNAME` / `XW_ADMIN_PASSWORD`（运营后台）。未配置任何凭据的部署是开放的 demo 语义，所有 `/v1` 接口匿名可用，不能直接暴露公网。
- [ ] **密钥强度**：`XW_JWT_SECRET` 不低于 16 字节，接口对过短的密钥拒绝签发与校验（503）；管理员口令独立于 API Key，服务端 Key 泄露不应同时交出运维权限。
- [ ] **令牌有效期**：`XW_JWT_TTL` 按会话长度设定（默认 3600 秒）；无刷新令牌，过期后重新登录。
- [ ] **CORS 收紧**：`XW_CORS_ORIGINS` 从默认 `*` 改成实际前端来源（逗号分隔白名单）。
- [ ] **指标与运维端口只在内网**：`/metrics`（API 与引擎）不做鉴权；引擎运维端口 `XW_ADMIN_ADDR` 默认只监听 `127.0.0.1`，确需暴露时前置网络层防护。
- [ ] **回调验签**：配置 `XW_CALLBACK_SECRET`（16–512 字节），接收端验证 `X-XW-Signature` 并按 `X-XW-Event-Id` 去重——回调是 at-least-once，重复投递是正常路径。
- [ ] **限流开启**：按实际流量配置 `XW_RATE_LIMIT_KEY_RPM` / `XW_RATE_LIMIT_IP_RPM`；多实例部署让后端跟随 Redis（默认如此），同一份额度才被共享。
- [ ] **真实渠道凭据**：只放 `XW_PROVIDERS_FILE`（文件权限收紧）或环境变量，不写进代码与镜像；不用的渠道保持 mock（默认 console 实现不联网）。
- [ ] **依赖服务不暴露公网**：Redis / Postgres / RabbitMQ 只在内网可达，各自开启认证；容器共享卷权限按 [deploy.md](deploy.md) 配置。
- [ ] **环境隔离**：生产触发统一 `X-Environment: production`，与开发数据分开；生产环境的管理 Key 不与开发共用。
- [ ] **时间同步**：主机走 NTP——回调签名容差 5 分钟、令牌 `exp`、幂等键 TTL 都依赖时钟。
- [ ] **TLS 由代理终止**：API 与引擎本身不做 TLS，前置反向代理/网关终结 HTTPS。

## 已知限制

如实列出当前版本不解决或只解决一部分的问题：

- **开放模式是设计而非缺陷**：不配置任何凭据时全部接口匿名可用，方便本地联调；「先配凭据再暴露」是部署方的责任（见清单第一条）。
- **令牌生命周期**：JWT 无 refresh token、无吊销列表——签发后在有效期内无法单张作废（换 `XW_JWT_SECRET` 可整体失效）；令牌存浏览器 `localStorage`，存在 XSS 场景下被读取的风险，缓解依赖前端部署方的 XSS 防护。
- **`/metrics` 无鉴权**：抓取端通常无凭据，指标里不含请求体与凭据，但吞吐与路由结构属于部署信息，需网络层保护。
- **JSONL 存储后端无跨进程协调**：多进程并发写同一份 JSONL 文件不做锁协调，多实例部署应使用 Postgres 后端；单进程/演示用途不受影响。
- **迁移脚本不保证可重复执行**：`scripts/migrations/` 是顺序 SQL，重复执行前需按序号自行判断（幂等完善尚未做）。
- **推送渠道只有 mock 实现**：push-mock 按 FCM / APNs 的报文契约校验后只记录、不联网；把真实推送送进手机需要接入对应厂商的发送实现并配置密钥。邮件（SMTP）与短信（Twilio 契约）有真实发送实现，配置真实凭据即可出网。
- **渠道默认 mock**：不配置 `XW_PROVIDERS_FILE` 时各渠道走 console 实现（打进日志），这是为了没有真实凭据也能跑通链路。
- **回调不是恰好一次**：接收方超时、引擎重启都会导致重发；事件 ID 在重放时保留，去重责任在接收方。
- **幂等存在 at-most-once 窗口**：键写入与消息入队不是同一事务，窗口内的极端故障可能少发而不会多发，口径见 [idempotency.md](idempotency.md)。
- **不做租户级配额**：限流维度是「凭据 / IP」，没有按租户的用量配额与计费边界。
