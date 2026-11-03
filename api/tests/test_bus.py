"""消息总线单元测试。"""
from __future__ import annotations

import json

from app.bus import MemoryMessageBus, RedisMessageBus, publish_all
from app.schemas import QueuedMessage, TriggerEvent, new_queued_message


def _make_msg(trigger_id: str = "t-1") -> QueuedMessage:
    return QueuedMessage(
        trigger_id=trigger_id,
        transaction_id="tx-1",
        name="post_liked",
        subscriber_id="user_1",
        email="hi@example.com",
        channels=["in_app"],
        payload={"user": "Alice"},
    )


class _FakePipeline:
    """只记录命令、执行时一次落到 fake client 上。"""

    def __init__(self, client) -> None:
        self._client = client
        self._commands: list[tuple[str, str]] = []

    def lpush(self, key, value):
        self._commands.append((key, value))
        return len(self._commands)

    def execute(self):
        for key, value in self._commands:
            self._client.lpush(key, value)
        self._client.round_trips += 1
        return [1] * len(self._commands)


class _BulkRedis:
    """支持 pipeline 的假客户端，用于统计批量写入的往返次数。"""

    def __init__(self) -> None:
        self.items: list[str] = []
        self.round_trips = 0
        self.pipelines = 0

    def lpush(self, key, value):
        self.items.insert(0, value)
        return len(self.items)

    def brpop(self, key, timeout=1):
        if not self.items:
            return None
        return (key, self.items.pop())

    def llen(self, key):
        return len(self.items)

    def pipeline(self, transaction=True):
        assert transaction is False, "入队之间无需事务，MULTI 只会多两次往返"
        self.pipelines += 1
        return _FakePipeline(self)


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


def test_device_token_carried_through_queue_contract():
    """push 渠道依赖 device_token：触发入参为 camelCase，队列契约为 snake_case，
    两侧都要能透传，否则 Go 引擎的 push 步骤会因缺 token 投递失败。"""
    event = TriggerEvent(
        name="post_mentioned",
        to={"subscriberId": "creator-1", "deviceToken": "fcm-token-9f2c1d"},
        payload={"post_id": "P1001", "actor": "小林"},
    )

    msg = new_queued_message(event)
    assert msg.device_token == "fcm-token-9f2c1d"
    assert msg.model_dump()["device_token"] == "fcm-token-9f2c1d"

    # 反序列化（模拟 Go 侧或 Redis 回读）后仍保留该字段
    restored = QueuedMessage.model_validate(json.loads(msg.model_dump_json()))
    assert restored.device_token == "fcm-token-9f2c1d"
    # 未提供时不报错，保持向后兼容
    assert QueuedMessage.model_validate({"trigger_id": "t", "transaction_id": "tx",
                                         "name": "post_liked", "subscriber_id": "s"}).device_token is None


# --- 批量入队 ---


def test_memory_bus_publish_many_keeps_order():
    bus = MemoryMessageBus()
    assert bus.publish_many([_make_msg(f"t-{i}") for i in range(3)]) == 3
    assert bus.size() == 3
    assert [bus.consume(timeout=0.1).trigger_id for _ in range(3)] == ["t-0", "t-1", "t-2"]


def test_memory_bus_publish_many_empty_is_noop():
    bus = MemoryMessageBus()
    assert bus.publish_many([]) == 0
    assert bus.size() == 0


def test_redis_bus_publish_many_uses_one_round_trip(monkeypatch):
    """一批消息只发一次 pipeline，且顺序与逐条 LPUSH 一致。"""
    fake = _BulkRedis()
    monkeypatch.setattr("redis.Redis.from_url", lambda *a, **k: fake)

    bus = RedisMessageBus(url="redis://localhost:6379/0", queue_key="xw:q")
    messages = [_make_msg(f"t-{i}") for i in range(50)]
    assert bus.publish_many(messages) == 50

    assert fake.pipelines == 1
    assert fake.round_trips == 1
    assert len(fake.items) == 50
    # LPUSH 到表头、消费端 BRPOP 从表尾取，先入队的先被消费
    assert [bus.consume(timeout=1.0).trigger_id for _ in range(50)] == [m.trigger_id for m in messages]


def test_redis_bus_publish_many_empty_skips_client(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("空列表不应建立连接")

    monkeypatch.setattr("redis.Redis.from_url", _boom)
    bus = RedisMessageBus(url="redis://localhost:6379/0", queue_key="xw:q")
    assert bus.publish_many([]) == 0


def test_redis_bus_publish_single_still_works(monkeypatch):
    fake = _BulkRedis()
    monkeypatch.setattr("redis.Redis.from_url", lambda *a, **k: fake)
    bus = RedisMessageBus(url="redis://localhost:6379/0", queue_key="xw:q")

    bus.publish(_make_msg("t-solo"))
    assert fake.round_trips == 0, "单条投递不该为一次写入开 pipeline"
    assert json.loads(fake.items[0])["trigger_id"] == "t-solo"


def test_publish_all_prefers_bulk_channel():
    calls = {"bulk": 0, "single": 0}

    class _Bus:
        def publish(self, message):
            calls["single"] += 1

        def publish_many(self, messages):
            calls["bulk"] += 1
            return len(list(messages))

    assert publish_all(_Bus(), [_make_msg("a"), _make_msg("b")]) == 2
    assert calls == {"bulk": 1, "single": 0}


def test_publish_all_falls_back_to_single_publish():
    """外部注入的总线可能没有批量通道：批量只是优化，不是接入前提。"""
    sent = []

    class _Bus:
        def publish(self, message):
            sent.append(message.trigger_id)

    assert publish_all(_Bus(), [_make_msg("a"), _make_msg("b")]) == 2
    assert sent == ["a", "b"]


def test_publish_all_empty_does_not_touch_bus():
    class _Bus:
        def publish(self, message):
            raise AssertionError("空列表不应入队")

        def publish_many(self, messages):
            raise AssertionError("空列表不应入队")

    assert publish_all(_Bus(), []) == 0
