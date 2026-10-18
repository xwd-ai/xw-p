"""消息总线单元测试。"""
from __future__ import annotations

import json

from app.bus import MemoryMessageBus, RedisMessageBus
from app.schemas import QueuedMessage


def _make_msg(trigger_id: str = "t-1") -> QueuedMessage:
    return QueuedMessage(
        trigger_id=trigger_id,
        transaction_id="tx-1",
        name="welcome",
        subscriber_id="user_1",
        email="hi@example.com",
        channels=["in_app"],
        payload={"user": "Alice"},
    )


def test_memory_bus_roundtrip():
    bus = MemoryMessageBus()
    msg = _make_msg()
    bus.publish(msg)
    assert bus.size() == 1

    got = bus.consume(timeout=0.1)
    assert got is not None
    assert got.trigger_id == "t-1"
    assert got.payload == {"user": "Alice"}
    assert bus.size() == 0


def test_memory_bus_empty_returns_none():
    bus = MemoryMessageBus()
    assert bus.consume(timeout=0.1) is None


def test_redis_bus_roundtrip_with_fake_client(monkeypatch):
    """用假 client 验证 Redis 总线的序列化与出队逻辑（无需真实 Redis）。"""

    class FakeRedis:
        def __init__(self):
            self.items = []

        def lpush(self, key, value):
            self.items.insert(0, value)
            return len(self.items)

        def brpop(self, key, timeout=1):
            if not self.items:
                return None
            value = self.items.pop()
            return (key, value)

        def llen(self, key):
            return len(self.items)

    fake = FakeRedis()
    monkeypatch.setattr("redis.Redis.from_url", lambda *a, **k: fake)

    bus = RedisMessageBus(url="redis://localhost:6379/0", queue_key="xw:q")
    msg = _make_msg()
    bus.publish(msg)
    assert bus.size() == 1
    # 确认入队内容是合法 JSON 且字段完整
    assert json.loads(fake.items[0])["trigger_id"] == "t-1"

    got = bus.consume(timeout=1.0)
    assert got is not None
    assert got.trigger_id == "t-1"


def test_redis_bus_empty_returns_none(monkeypatch):
    class FakeRedis:
        def lpush(self, key, value):
            return 1

        def brpop(self, key, timeout=1):
            return None

        def llen(self, key):
            return 0

    monkeypatch.setattr("redis.Redis.from_url", lambda *a, **k: FakeRedis())
    bus = RedisMessageBus(url="redis://localhost:6379/0", queue_key="xw:q")
    assert bus.consume(timeout=1.0) is None
