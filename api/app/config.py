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


# 消息总线后端：memory（默认）| redis
BUS_BACKEND = _env("XW_BUS", "memory")

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
