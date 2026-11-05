"""幂等存储单元测试：内存 / Redis 双后端，以及占用幂等键的三种结论。"""
from __future__ import annotations

import time

import pytest

from app.idempotency import (
    IdempotencyConflict,
    MemoryIdempotencyStore,
    RedisIdempotencyStore,
    claim,
)


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


class _UnreadableStore:
    """键被占用、但回读永远是空的存储（值被写坏或刚好过期时就是这样）。"""

    def __init__(self) -> None:
        self.writes = 0

    def get(self, key: str):
        return None

    def put_if_absent(self, key: str, value: dict, ttl: int) -> bool:
        self.writes += 1
        return False


def test_claim_first_write_wins():
    store = MemoryIdempotencyStore()
    reserved = claim(store, "k", {"triggerId": "trg-1"}, ttl=60)
    assert reserved.claimed is True
    assert reserved.cached is None
    assert store.get("k") == {"triggerId": "trg-1"}


def test_claim_returns_existing_receipt_unchanged():
    store = MemoryIdempotencyStore()
    claim(store, "k", {"triggerId": "trg-1"}, ttl=60)
    reserved = claim(store, "k", {"triggerId": "trg-2"}, ttl=60)
    assert reserved.claimed is False
    # 回放的是首次那份回执，不是本次请求自己生成的那一份。
    assert reserved.cached == {"triggerId": "trg-1"}
    assert store.get("k") == {"triggerId": "trg-1"}


def test_claim_raises_when_receipt_unreadable():
    """键被占用却读不到回执时抛冲突，而不是当作未命中继续入队。"""
    store = _UnreadableStore()
    with pytest.raises(IdempotencyConflict) as excinfo:
        claim(store, "comment-C-8842", {"triggerId": "trg-1"}, ttl=60)
    assert excinfo.value.key == "comment-C-8842"
    # 文案要能指导下一步：说清本次没处理，并指向前一次是否已受理。
    assert "未处理" in excinfo.value.detail
    assert "comment-C-8842" in excinfo.value.detail
