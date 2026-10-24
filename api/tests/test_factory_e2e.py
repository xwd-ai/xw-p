"""后端工厂切换与端到端（内存 + JSONL）测试。"""
from __future__ import annotations

import app.bus as bus_mod
import app.storage as storage_mod
from app.bus import MemoryMessageBus
from app.schemas import QueuedMessage
from app.storage import JSONLStorage


def test_create_bus_memory_default(monkeypatch):
    monkeypatch.setattr("app.config.BUS_BACKEND", "memory")
    assert isinstance(bus_mod.create_bus(), MemoryMessageBus)


def test_create_bus_redis(monkeypatch):
    monkeypatch.setattr("app.config.BUS_BACKEND", "redis")
    b = bus_mod.create_bus()
    # 不真正连接 Redis：仅验证实例类型正确
    assert type(b).__name__ == "RedisMessageBus"


def test_create_storage_jsonl_default(monkeypatch):
    monkeypatch.setattr("app.config.STORAGE_BACKEND", "jsonl")
    assert isinstance(storage_mod.create_storage(), JSONLStorage)


def test_end_to_end_memory_jsonl(monkeypatch, tmp_path):
    """触发 → 内存队列 → worker 消费 → JSONL 落库。"""
    from app.worker import Worker

    monkeypatch.setattr("app.config.BUS_BACKEND", "memory")
    monkeypatch.setattr("app.config.STORAGE_BACKEND", "jsonl")

    bus = MemoryMessageBus()
    storage = JSONLStorage(path=tmp_path / "messages.jsonl")
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(storage_mod, "_storage", storage)

    msg = QueuedMessage(
        trigger_id="t-e2e",
        transaction_id="tx-e2e",
        name="order_confirmed",
        subscriber_id="buyer_1001",
        channels=["in_app"],
        payload={"order_id": "ORDER-2026-1001", "customer": "Alice"},
    )
    bus.publish(msg)

    worker = Worker()
    worker._process(msg)  # 直接调用处理逻辑，等价于 worker 线程消费后处理

    lines = (tmp_path / "messages.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert '"workflow": "order_confirmed"' in lines[0]
    assert '"status": "sent"' in lines[0]
