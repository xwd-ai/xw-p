"""运行配置：从环境变量读取，集中管理。

所有可配置项均提供安全默认值，便于 demo 阶段开箱即用。
"""
from __future__ import annotations

import os


def _env(key: str, default: str) -> str:
    value = os.getenv(key)
    return value if value is not None and value != "" else default


def _env_float(key: str, default: float, *, minimum: float = 0.0) -> float:
    """读取浮点配置：缺失或写错时回退默认值，不让一个数字配错就起不来。"""
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def _env_int(key: str, default: int, *, minimum: int = 0) -> int:
    """读取整数配置：缺失、写错或越界时回退默认值（同 ``_env_float`` 的口径）。"""
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        return default
    return value if value >= minimum else default


def _flag(raw: str | None) -> bool:
    """把 1/true/on/yes 这类写法读成布尔值，其余为假。"""
    return str(raw or "").strip().lower() in {"1", "true", "on", "yes"}


def _flag_default(raw: str | None, default: bool) -> bool:
    """同上，但未设置时取默认值。

    访问日志这类「默认开」的开关不能复用 ``_flag``：未设置时它返回假，
    等于默认把日志关掉，而排查问题时最需要的恰恰是默认就有日志。
    """
    if raw is None or str(raw).strip() == "":
        return default
    return _flag(raw)


# 消息总线后端：memory（默认）| redis
BUS_BACKEND = _env("XW_BUS", "memory")

# 日志：json（默认，一行一条，便于采集端按字段过滤）| text（本地看顺眼）。
# 级别用名字而不是数字，配置文件里 DEBUG 比 10 好读；写错时回退 INFO。
LOG_FORMAT = _env("XW_LOG_FORMAT", "json")
LOG_LEVEL = _env("XW_LOG_LEVEL", "INFO").upper()
# 每个请求一行访问日志。默认开；压测或本机调试嫌吵时关掉。
LOG_ACCESS = _flag_default(os.getenv("XW_LOG_ACCESS"), True)

# Redis 连接（BUS_BACKEND=redis 时生效）
REDIS_URL = _env("XW_REDIS_URL", "redis://localhost:6379/0")
REDIS_QUEUE_KEY = _env("XW_REDIS_QUEUE_KEY", "xw:queue:events")


def api_worker_enabled(bus_backend: str, raw: str | None = None) -> bool:
    """判断 API 进程是否启动内置消费线程。

    内置线程只服务 memory 总线的单进程 demo：redis 模式下队列由 Go 引擎消费，
    API 若再起一个消费者就会和引擎抢同一条消息（谁先抢到算谁的）。
    显式设置 ``XW_API_WORKER`` 可覆盖默认值（1/true/on 与 0/false/off）。
    """
    if raw is not None:
        text = raw.strip().lower()
        if text:
            return text not in {"0", "false", "off", "no"}
    return bus_backend != "redis"


API_WORKER = api_worker_enabled(BUS_BACKEND, os.getenv("XW_API_WORKER"))

# 持久化后端：jsonl（默认）| postgres
STORAGE_BACKEND = _env("XW_STORAGE", "jsonl")

# Postgres 连接（STORAGE_BACKEND=postgres 时生效）
DATABASE_URL = _env(
    "XW_DATABASE_URL",
    "postgresql+psycopg://xwnotify:xwnotify@localhost:5432/xwnotify",
)

# 幂等后端：memory（默认）| redis
IDEMPOTENCY_BACKEND = _env("XW_IDEMPOTENCY", "memory")

# 渠道配置文件（JSON）：与引擎侧共用的格式，用于指定每个渠道走哪种实现。
# 为空时 API 侧为每个投递渠道装配一个 console 实现。
PROVIDERS_FILE = _env("XW_PROVIDERS_FILE", "")

# 渠道实现健康检查：在线探测结果的缓存时长（秒，0 表示不缓存）与单次探测预算（秒）。
# 探测预算不与引擎侧的 XW_PROVIDER_PROBE_TIMEOUT 共用：两者面向不同的探测实现，
# 一个变量在两处套用不同默认值，改起来最容易误伤。
PROVIDER_HEALTH_TTL = _env_float("XW_PROVIDER_HEALTH_TTL", 30.0)
PROVIDER_HEALTH_TIMEOUT = _env_float("XW_PROVIDER_HEALTH_TIMEOUT", 2.0, minimum=0.1)

# 订阅者手机号的默认国家码：档案里写本地号码（13800000000）时按它补成 E.164。
# 留空表示只接受自带 ``+`` 区号的号码——按位数猜国家码会把短信发到别人的号段上。
SUBSCRIBER_COUNTRY_CODE = _env("XW_SUBSCRIBER_COUNTRY_CODE", "")

# 幂等键缓存 TTL（秒）：超过该时长后同键请求视为新请求
IDEMPOTENCY_TTL = int(_env("XW_IDEMPOTENCY_TTL", "86400"))

# 活动日志落盘位置（空表示 ``api/data/activity.jsonl``）。多实例压测与集成测试
# 用它把数据写进临时目录；与收件箱的 ``XW_INBOX_FILE``、偏好的
# ``XW_PREFERENCES_FILE`` 同一套做法。
ACTIVITY_FILE = _env("XW_ACTIVITY_FILE", "")

# 当前服务环境：development（默认）| production
# 用于区分 API Key 与数据隔离，未显式设置时回退 development，保证 demo 开箱即用。
ENV = _env("XW_ENV", "development")


def parse_origins(raw: str | None) -> list[str]:
    """解析 CORS 白名单：逗号分隔，``*`` 表示全部放行。

    空串按「不限制」处理而不是「全部禁止」：这个配置的意义是收紧，写成空值
    多半是没配，而不是要把页面挡在门外。
    """
    if raw is None or raw.strip() == "":
        return ["*"]
    origins = [item.strip().rstrip("/") for item in raw.split(",") if item.strip()]
    return origins or ["*"]


# 展示前端与 API 通常不同源：dashboard/ 是一组静态文件，由任意静态服务器打开，
# 而 API 在另一个端口。浏览器对这种跨源 fetch 先做 CORS 预检，服务端没有对应
# 响应头就直接失败——页面上表现为「Failed to fetch」，与后端是否健康无关。
# 默认放行所有来源：服务不使用 Cookie 鉴权（凭据走 X-Api-Key 头），因此不需要
# allow_credentials；需要收紧时用 XW_CORS_ORIGINS 给出逗号分隔的白名单。
CORS_ORIGINS: list[str] = parse_origins(os.getenv("XW_CORS_ORIGINS"))

# 限流：两个维度各自给出「每分钟请求数」，0（默认）表示该维度关闭。
# 关闭不是保守而是诚实——限流阈值要按实际流量调，给一个猜出来的默认值
# 只会让人以为已经保护好了。突发深度缺省取每分钟额度。
RATE_LIMIT_KEY_RPM = _env_int("XW_RATE_LIMIT_KEY_RPM", 0)
RATE_LIMIT_IP_RPM = _env_int("XW_RATE_LIMIT_IP_RPM", 0)
RATE_LIMIT_BURST = _env_int("XW_RATE_LIMIT_BURST", 0)
# 令牌桶后端：auto（默认，跟随 XW_BUS）| redis | memory
RATE_LIMIT_BACKEND = _env("XW_RATE_LIMIT_BACKEND", "auto")
RATE_LIMIT_PREFIX = _env("XW_RATE_LIMIT_PREFIX", "xw:ratelimit")
# 单次取令牌的等待上限（秒）：限流跑在每个请求前面，Redis 慢下来不能拖住链路。
RATE_LIMIT_TIMEOUT = _env_float("XW_RATE_LIMIT_TIMEOUT", 0.5, minimum=0.05)
# 是否采信 X-Forwarded-For：只有部署在会追加该头的反向代理之后才该打开。
RATE_LIMIT_TRUST_FORWARDED = _flag(os.getenv("XW_RATE_LIMIT_TRUST_FORWARDED"))


# 各环境的 API Key：仅当对应环境配置了 Key 时才强制鉴权。
# 未配置（空）时该环境的写入接口保持开放（demo / 测试友好），
# 一旦配置，请求须携带匹配的 ``X-Api-Key`` 头，否则返回 401。
API_KEYS: dict[str, str] = {
    env: _env(f"XW_API_KEY_{env.upper()}", "")
    for env in ("development", "production")
}


# 多租户：显式声明的租户清单（逗号分隔，空表示不限制），以及带作用域的
# API Key（JSON 数组，格式见 app/tenancy.py）。两者都为空时保持单租户 demo 语义。
TENANTS = _env("XW_TENANTS", "")
API_KEY_SCOPES = _env("XW_API_KEYS", "")

# 后台会话（JWT）。密钥非空即启用：后台接口（/v1/admin/*）改为只接受
# 令牌或带 admin 角色的 API Key。留空保持 demo 语义（与其它接口同一套鉴权），
# 本地与演示环境不必先配一个密钥才能打开后台。
JWT_SECRET = _env("XW_JWT_SECRET", "")
JWT_TTL = _env("XW_JWT_TTL", "3600")
JWT_ISSUER = _env("XW_JWT_ISSUER", "xw-notify")

# 管理员账号：用账号口令换令牌。口令留空等于没开通登录——一个默认口令
# 比没有登录入口更危险。
ADMIN_USERNAME = _env("XW_ADMIN_USERNAME", "")
ADMIN_PASSWORD = _env("XW_ADMIN_PASSWORD", "")

# 管理员令牌绑定的租户与可操作环境（逗号分隔，空表示不限制环境）。
ADMIN_TENANT = _env("XW_ADMIN_TENANT", "default")
ADMIN_ENVIRONMENTS = _env("XW_ADMIN_ENVIRONMENTS", "")


def api_key_for(env: str) -> str:
    """返回指定环境的 API Key（未配置则为空字符串）。"""
    return API_KEYS.get(env, "")


def storage_warning(backend: str | None = None, env: str | None = None) -> str | None:
    """生产环境仍使用文件后端时返回一条告警文案，否则返回 ``None``。

    ``jsonl`` 后端的定位是本机开发与演示：写入是「打开文件追加」而不是事务，
    多个进程或多实例共享同一份文件时没有写入协调，查询也依赖顺序扫描。
    生产环境因此固定 ``postgres``，误配时在启动日志里直接指出来，
    而不是等到数据对不上再排查。
    """
    backend = STORAGE_BACKEND if backend is None else backend
    env = ENV if env is None else env
    if backend != "jsonl" or env != "production":
        return None
    return (
        "XW_STORAGE=jsonl 仅用于本机开发与演示（追加写文件、无跨进程写入协调），"
        "生产环境请改为 XW_STORAGE=postgres"
    )
