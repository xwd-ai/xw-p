"""幂等存储单元测试：内存 / Redis 双后端。"""
from __future__ import annotations

import time

from app.idempotency import MemoryIdempotencyStore, RedisIdempotencyStore


def test_memory_put_if_absent_first_wins():
    store = MemoryIdempotencyStore()
    assert store.get("k") is None
    assert store.put_if_absent("k", {"a": 1}, ttl=60) is True
    assert store.get("k") == {"a": 1}
    # 重复写入应失败，且不覆盖原值
    assert store.put_if_absent("k", {"a": 2}, ttl=60) is False
    assert store.get("k") == {"a": 1}


def test_memory_ttl_expiry():
    store = MemoryIdempotencyStore()
    store.put_if_absent("k", {"a": 1}, ttl=1)
    time.sleep(1.05)
    # 过期后 get 返回 None，可重新写入
    assert store.get("k") is None
    assert store.put_if_absent("k", {"a": 2}, ttl=60) is True


def test_redis_put_if_absent_with_fake(monkeypatch):
    """用假 client 验证 Redis 幂等的 SET NX EX 语义（无需真实 Redis）。"""

    class FakeRedis:
        def __init__(self):
            self.data = {}

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.data:
                return None
            self.data[key] = value
            return True

        def get(self, key):
            return self.data.get(key)

    fake = FakeRedis()
    monkeypatch.setattr("redis.Redis.from_url", lambda *a, **k: fake)

    store = RedisIdempotencyStore(url="redis://localhost:6379/0")
    assert store.put_if_absent("k", {"a": 1}, ttl=60) is True
    assert store.get("k") == {"a": 1}
    # NX 语义：键已存在时 set 返回 None（False）
    assert store.put_if_absent("k", {"a": 2}, ttl=60) is False
    assert store.get("k") == {"a": 1}
