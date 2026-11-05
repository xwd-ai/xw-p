"""事件幂等去重：基于幂等键（idempotency key）的请求去重。

触发 API 通过 ``Idempotency-Key`` 请求头支持客户端重试安全：
相同幂等键的重复请求会返回首次处理时缓存的回执，而不会重复入队。

后端可切换：
- memory（默认）：进程内 ``dict`` 缓存，带 TTL 过期，适合单实例与测试。
- redis：基于 ``SET key value NX EX ttl`` 的原子去重，适合多实例部署。

幂等键由调用方生成（UUID 或业务单号），服务端仅做「首次写入、后续读取」，
不尝试推导语义。缓存条目保存序列化后的回执，命中时原样返回。

**占用与回放**：三个触发入口（单条、批量、分组广播）共用 :func:`claim`。
它的返回值只有两种「可继续处理」的结论——占用成功（继续入队）或已有回执
（原样回放）；第三种情况是键已存在但回执读不出来，此时抛
:class:`IdempotencyConflict`，绝不按「未命中」继续入队。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from . import config

logger = logging.getLogger("xw.idempotency")


class IdempotencyStore(Protocol):
    """幂等存储统一接口。"""

    def get(self, key: str) -> Optional[dict[str, Any]]:
        """若该幂等键此前已处理，返回缓存回执；否则返回 None。"""
        ...

    def put_if_absent(self, key: str, value: dict[str, Any], ttl: int) -> bool:
        """原子地写入回执；返回 True 表示首次写入成功（应继续处理），
        False 表示键已存在（应返回既有回执）。"""
        ...


@dataclass
class _Entry:
    value: dict[str, Any]
    expires_at: float


class MemoryIdempotencyStore:
    """进程内幂等缓存：dict + TTL 惰性过期。"""

    def __init__(self, ttl: int = config.IDEMPOTENCY_TTL) -> None:
        self._ttl = ttl
        self._data: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def _purge_expired_locked(self, now: float) -> None:
        expired = [k for k, e in self._data.items() if e.expires_at <= now]
        for k in expired:
            del self._data[k]

    def get(self, key: str) -> Optional[dict[str, Any]]:
        now = time.time()
        with self._lock:
            self._purge_expired_locked(now)
            entry = self._data.get(key)
            if entry is None:
                return None
            return entry.value

    def put_if_absent(self, key: str, value: dict[str, Any], ttl: int) -> bool:
        now = time.time()
        with self._lock:
            self._purge_expired_locked(now)
            if key in self._data:
                return False
            self._data[key] = _Entry(value=value, expires_at=now + ttl)
            return True


class RedisIdempotencyStore:
    """基于 Redis ``SET NX EX`` 的幂等去重（多实例安全）。

    redis 客户端延迟导入，避免纯内存模式依赖 Redis。
    """

    def __init__(self, url: str, prefix: str = "xw:idem:") -> None:
        self._url = url
        self._prefix = prefix
        self._client = None

    def _get_client(self):
        if self._client is None:
            import redis

            self._client = redis.Redis.from_url(self._url, decode_responses=True)
        return self._client

    def _key(self, key: str) -> str:
        return self._prefix + key

    def get(self, key: str) -> Optional[dict[str, Any]]:
        raw = self._get_client().get(self._key(key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def put_if_absent(self, key: str, value: dict[str, Any], ttl: int) -> bool:
        raw = json.dumps(value, ensure_ascii=False)
        # SET key value NX EX ttl：仅当键不存在时写入，返回 True 表示写入成功
        return bool(self._get_client().set(self._key(key), raw, nx=True, ex=ttl))


def create_idempotency_store() -> IdempotencyStore:
    """根据配置创建幂等存储实例。"""
    if config.IDEMPOTENCY_BACKEND == "redis":
        logger.info("幂等后端：redis（%s）", config.REDIS_URL)
        return RedisIdempotencyStore(config.REDIS_URL)
    logger.info("幂等后端：memory")
    return MemoryIdempotencyStore()


class IdempotencyConflict(Exception):
    """幂等键已被占用，但既有回执读不出来。

    键在存储里存在，说明更早的一次请求已经把它占了；把「读不到回执」当成
    未命中而继续入队，同一个键就会产出两条通知，而这正好是幂等要防的事。
    因此这里给出明确的拒绝：本次请求没有入队，也没有生成触发标识。
    """

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(key)

    @property
    def detail(self) -> str:
        return (
            f"幂等键 {self.key} 已被更早的一次请求占用，但其回执读取失败，本次请求未处理。"
            "请先按触发标识查询活动日志确认前一次是否已受理，再决定是否用新键重试"
        )


@dataclass(frozen=True)
class Reservation:
    """占用幂等键的结果。

    ``claimed`` 为真表示本次请求拿到了这个键、可以继续处理；为假表示键已被
    更早的请求占用，``cached`` 是那次请求的回执，应原样返回。
    """

    claimed: bool
    cached: Optional[dict[str, Any]] = None


def claim(
    store: IdempotencyStore, key: str, value: dict[str, Any], ttl: int
) -> Reservation:
    """占用幂等键，返回本次请求的去向。

    ``put_if_absent`` 的原子性是唯一裁决点：多实例并发时只有一个请求能写入
    成功，其余都从这里拿到 ``claimed=False``。失败后回读一次是为了把「那次
    请求的回执」交还调用方——重放必须与首次一模一样，包括首次就被拒的条目。

    回读为空时不返回「未命中」：键已被占用却读不到内容，只可能是值被写坏、
    或刚好撞上 TTL 边界。这时继续入队会重复投递，直接抛
    :class:`IdempotencyConflict` 让调用方知道要做什么。
    """
    if store.put_if_absent(key, value, ttl):
        return Reservation(claimed=True)
    cached = store.get(key)
    if cached is None:
        raise IdempotencyConflict(key)
    return Reservation(claimed=False, cached=cached)


class ScopedIdempotencyStore:
    """按租户给幂等键加前缀的包装。

    幂等键由调用方生成（常见写法是业务单号），两个租户用同一个键完全正常；
    不加前缀会让后到的租户命中前一个租户的回执，拿到别人的触发标识。包装
    而不是改后端，是因为内存与 Redis 两种实现的键空间都只有「一个字符串」。
    """

    def __init__(self, inner: IdempotencyStore, tenant: str) -> None:
        self._inner = inner
        self._tenant = tenant

    def _scoped(self, key: str) -> str:
        return f"{self._tenant}:{key}"

    def get(self, key: str) -> Optional[dict[str, Any]]:
        return self._inner.get(self._scoped(key))

    def put_if_absent(self, key: str, value: dict[str, Any], ttl: int) -> bool:
        return self._inner.put_if_absent(self._scoped(key), value, ttl)


_store: Optional[IdempotencyStore] = None
_tenant_stores: dict[str, IdempotencyStore] = {}
_store_lock = threading.Lock()


def get_idempotency_store(tenant: Optional[str] = None) -> IdempotencyStore:
    """获取幂等存储（按租户隔离，惰性初始化）。"""
    from . import tenancy

    global _store

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            if _store is None:
                _store = create_idempotency_store()
            return _store
        store = _tenant_stores.get(key)
        if store is None:
            store = ScopedIdempotencyStore(create_idempotency_store(), key)
            _tenant_stores[key] = store
        return store


def set_idempotency_store(
    store: Optional[IdempotencyStore], tenant: Optional[str] = None
) -> None:
    """替换某个租户的幂等存储；传 ``None`` 表示下次取用时按配置重建。"""
    from . import tenancy

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            globals()["_store"] = store
        elif store is None:
            _tenant_stores.pop(key, None)
        else:
            _tenant_stores[key] = store
