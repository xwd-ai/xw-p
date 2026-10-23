"""运行配置：从环境变量读取，集中管理。

所有可配置项均提供安全默认值，便于 demo 阶段开箱即用。
"""
from __future__ import annotations

import os


def _env(key: str, default: str) -> str:
    value = os.getenv(key)
    return value if value is not None and value != "" else default


# 消息总线后端：memory（默认）| redis
BUS_BACKEND = _env("XW_BUS", "memory")

# Redis 连接（BUS_BACKEND=redis 时生效）
REDIS_URL = _env("XW_REDIS_URL", "redis://localhost:6379/0")
REDIS_QUEUE_KEY = _env("XW_REDIS_QUEUE_KEY", "xw:queue:events")

# 持久化后端：jsonl（默认）| postgres
STORAGE_BACKEND = _env("XW_STORAGE", "jsonl")

# Postgres 连接（STORAGE_BACKEND=postgres 时生效）
DATABASE_URL = _env(
    "XW_DATABASE_URL",
    "postgresql+psycopg://xwnotify:xwnotify@localhost:5432/xwnotify",
)

# 幂等后端：memory（默认）| redis
IDEMPOTENCY_BACKEND = _env("XW_IDEMPOTENCY", "memory")

# 幂等键缓存 TTL（秒）：超过该时长后同键请求视为新请求
IDEMPOTENCY_TTL = int(_env("XW_IDEMPOTENCY_TTL", "86400"))

# 当前服务环境：development（默认）| production
# 用于区分 API Key 与数据隔离，未显式设置时回退 development，保证 demo 开箱即用。
ENV = _env("XW_ENV", "development")

# 各环境的 API Key：仅当对应环境配置了 Key 时才强制鉴权。
# 未配置（空）时该环境的写入接口保持开放（demo / 测试友好），
# 一旦配置，请求须携带匹配的 ``X-Api-Key`` 头，否则返回 401。
API_KEYS: dict[str, str] = {
    env: _env(f"XW_API_KEY_{env.upper()}", "")
    for env in ("development", "production")
}


def api_key_for(env: str) -> str:
    """返回指定环境的 API Key（未配置则为空字符串）。"""
    return API_KEYS.get(env, "")
