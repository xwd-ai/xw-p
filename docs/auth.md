# 鉴权

服务有两类调用方，凭据也分两种，但解析结果相同：都是「租户 + 环境」。业务接口不区分凭据从哪来，
差别在权限——运维接口只接受后台身份。

| | API Key（服务端） | JWT（运营后台） |
| --- | --- | --- |
| 谁用 | 业务后端、脚本、SDK | 后台操作员、管理页面 |
| 形态 | `X-Api-Key: <key>` | `Authorization: Bearer <jwt>` |
| 生命周期 | 长期，随部署配置 | 默认 1 小时，过期重新登录 |
| 配置 | `XW_API_KEYS`（JSON 数组） | `XW_JWT_SECRET` + 管理员账号 |
| 权限 | 业务接口；声明 `roles` 后才能调运维接口 | 业务接口 + 运维接口 |

两种凭据同时出现时**以令牌为准**。一个请求带两种凭据必须有确定答案，否则令牌过期后同一个请求会
悄悄降级成 Key 的身份，排查时看到的是「昨天还好好的」。

## API Key

```bash
export XW_API_KEYS='[
  {"key": "sk_live_community", "tenant": "acme", "environments": ["production"], "label": "社区主站"},
  {"key": "sk_ops_console", "tenant": "acme", "environments": ["production"], "roles": ["admin"]}
]'
```

- `key` 必填，其余可选。`tenant` 缺省 `default`，`environments` 缺省不限制。
- `roles` 目前只有 `admin` 一个取值：声明后这把 Key 才能调用 `/v1/admin/*`。写错角色名在解析阶段
  就报错并列出可选值——悄悄忽略一个拼错的角色，表现是「配了却调不了」，比启动失败难查。
- 旧的环境变量 `XW_API_KEY_DEVELOPMENT` / `XW_API_KEY_PRODUCTION` 仍然可用，折算成「绑定默认租户、
  只允许自己那个环境、不带角色」，升级前后业务接口行为不变。
- 未配置任何 Key 的部署放行（demo 语义）。配置了以后：Key 缺失或不匹配 401，Key 不能用于当前环境
  401（与「Key 不存在」同一口径，避免凭证探测），Key 命中但请求头指明了别的租户 403。

## 后台令牌

### 配置

```bash
export XW_JWT_SECRET='请换成一串至少 32 字节的随机值'
export XW_JWT_TTL=3600                     # 有效期（秒），上限 86400
export XW_JWT_ISSUER=xw-notify             # 签发方，校验时比对
export XW_ADMIN_USERNAME=ops
export XW_ADMIN_PASSWORD='change-me-in-production'
export XW_ADMIN_TENANT=default             # 令牌绑定的租户
export XW_ADMIN_ENVIRONMENTS=production    # 可操作环境，逗号分隔；留空表示不限
```

密钥低于 16 字节直接拒绝签发与校验——HS256 的强度完全取决于密钥长度，短密钥等于没有签名。
口令留空等于没开通登录：一个默认口令比没有登录入口更危险。

### 换取令牌

```bash
curl -s -X POST http://localhost:8000/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username": "ops", "password": "change-me-in-production"}'
```

```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenType": "Bearer",
  "expiresIn": 3600,
  "expiresAt": "2026-11-05T10:30:00Z",
  "subject": "ops",
  "tenant": "acme",
  "environments": ["production"],
  "role": "admin"
}
```

之后每个请求带 `Authorization: Bearer <accessToken>`，`X-Environment` 必须在 `environments` 内。
`GET /v1/auth/me` 回显同一组声明，前端用它判断会话是否还有效，比等到业务接口 401 再处理更早。

### 声明

| 字段 | 含义 |
| --- | --- |
| `sub` | 管理员账号 |
| `tenant` | 令牌绑定的租户；与请求头不一致返回 403 |
| `environments` | 可操作环境；空数组表示不限 |
| `role` | 角色，当前固定 `admin` |
| `iss` | 签发方，必须与 `XW_JWT_ISSUER` 一致 |
| `iat` / `exp` | 签发与过期时刻（Unix 秒）；`exp` 必填 |
| `jti` | 令牌标识，用于排查与对账 |

### 校验规则

令牌自行实现（`api/app/tokens.py`，只依赖标准库），规则固定，没有可配置的算法列表：

- **算法写死 HS256**，`alg` 只用于比对，不接受 `none` 或别的 HMAC 变体——这是 JWT 最经典的绕过方式。
- **`exp` 必填**。缺过期的令牌就是一张永久通行证，签发侧漏一个字段不该有这样的后果。
- **签名用 `hmac.compare_digest`** 比较，避免按字节短路带来的时序差异。
- **时钟容差 60 秒**，只放宽 `exp` / `nbf`，不放宽签名。签发方与校验方不在同一台机器上时，
  几十秒的偏差很常见。
- **签发时间在未来**同样拒绝（`iat` 超出容差），否则拿到一个「还没生效」的令牌等于拿到将来的凭证。
- **租户与环境写在令牌里**：请求头指明的租户与令牌不一致返回 403；环境不在集合内返回 401。

对外错误按类给出（`令牌已过期，请重新登录` / `令牌签名不匹配` / `不支持的令牌算法` 等），
理由不涉及密钥，也不暴露服务端内部字段。

## 状态码

| 状态码 | 场景 |
| --- | --- |
| 400 | `X-Environment` / `X-Tenant-Id` 写错，或租户不在 `XW_TENANTS` 清单内 |
| 401 | 凭据缺失、不认识、令牌过期或签名不匹配、Key 不可用于当前环境 |
| 403 | 凭据有效但越权：跨租户、令牌缺 `admin` 角色、Key 没有 `roles`、登录时请求了别人的租户 |
| 503 | 服务端未配置密钥或管理员账号。这是部署状态，不该让调用方去查自己的输入 |

## 运营后台

`dashboard/admin.html` 顶栏的「登录」按钮走同一条 `/v1/auth/token`；令牌存在浏览器的
`localStorage`（键 `xw.adminToken`），页面刷新不必重登，收到 401 时自动清掉并提示重新登录。
命令行换完令牌后想直接打开页面，用 `admin.html?token=<jwt>`：读取后地址栏里的参数会被抹掉，
避免令牌留在浏览历史与转发出去的链接里。

## 已知限制

- **没有 refresh token**：过期就重新登录。后台会话是一小时量级的操作，加一层刷新意味着多一个
  可以长期驻留的凭据，收益不如直接缩短有效期。
- **没有吊销列表**：令牌在有效期内始终可用，需要立即失效时更换 `XW_JWT_SECRET`（全员重新登录）。
  单点吊销要在服务端存 `jti` 黑名单，属于「有状态鉴权」，与当前无状态校验的取舍相冲突。
- **令牌存在 `localStorage`**：页面存在 XSS 时可被读取。这里没有引入第三方脚本，所有后端字符串
  都经转义后拼进 DOM；若将来接入外部脚本，应改为 `HttpOnly` Cookie + CSRF 方案。
- **令牌是全权凭据**：拿到后台令牌即可调用全部业务接口与运维接口，没有只读模式或按接口划分的
  权限。角色目前只有 `admin` 一种，需要「只看统计、不能改模板」这类区分时应扩 `roles`，而不是
  在路由里各写一套判断。
