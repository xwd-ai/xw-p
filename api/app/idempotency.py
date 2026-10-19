"""事件幂等去重：基于幂等键（idempotency key）的请求去重。

触发 API 通过 ``Idempotency-Key`` 请求头支持客户端重试安全：
相同幂等键的重复请求会返回首次处理时缓存的回执，而不会重复入队。

后端可切换：
- memory（默认）：进程内 ``dict`` 缓存，带 TTL 过期，适合单实例与测试。
- redis：基于 ``SET key value NX EX ttl`` 的原子去重，适合多实例部署。

幂等键由调用方生成（UUID 或业务单号），服务端仅做「首次写入、后续读取」，
不尝试推导语义。缓存条目保存序列化后的回执，命中时原样返回。
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


_store: Optional[IdempotencyStore] = None
_store_lock = threading.Lock()


def get_idempotency_store() -> IdempotencyStore:
    """获取全局幂等存储单例（惰性初始化）。"""
    global _store
    with _store_lock:
        if _store is None:
            _store = create_idempotency_store()
        return _store
