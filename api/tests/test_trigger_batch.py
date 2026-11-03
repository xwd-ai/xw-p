"""批量触发 API：逐条受理/拒绝、一次入队、整批幂等。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app


class _ActivityStore:
    """记录批量写入次数，用来确认整批只落一次。"""

    def __init__(self) -> None:
        self.records: list[dict] = []
        self.bulk_calls = 0

    def append(self, record: dict) -> None:
        self.records.append(record)

    def append_many(self, records: list[dict]) -> int:
        self.bulk_calls += 1
        self.records.extend(records)
        return len(records)

    def read_all(self) -> list[dict]:
        return list(self.records)


class _CountingBus(MemoryMessageBus):
    """统计入队调用方式：一次批量请求应只走一次 publish_many。"""

    def __init__(self) -> None:
        super().__init__()
        self.bulk_calls = 0
        self.single_calls = 0

    def publish(self, message) -> None:
        self.single_calls += 1
        super().publish(message)

    def publish_many(self, messages) -> int:
        self.bulk_calls += 1
        return super().publish_many(messages)


class Rig:
    def __init__(self, client: TestClient, bus: _CountingBus, store: _ActivityStore) -> None:
        self.client = client
        self.bus = bus
        self.store = store

    def drain(self) -> list:
        out = []
        while True:
            message = self.bus.consume(timeout=0.05)
            if message is None:
                return out
            out.append(message)


@pytest.fixture
def rig(monkeypatch) -> Rig:
    """隔离测试环境：内存总线 + 内存幂等 + 记录型活动日志。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod

    bus = _CountingBus()
    store = _ActivityStore()
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(idem_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(al_mod, "_activity_store", store)
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)
    return Rig(TestClient(app), bus, store)


def _event(name: str = "post_liked", subscriber: str = "creator_1", **payload):
    body = {"name": name, "to": {"subscriberId": subscriber}, "payload": {"user": "Alice"}}
    body["payload"].update(payload)
    return body


def test_batch_accepts_all_events_in_one_enqueue(rig):
    resp = rig.client.post(
        "/v1/events/trigger/batch",
        json={
            "events": [
                _event("post_liked", post_id="P-1001"),
                _event("post_commented", post_id="P-1001", comment="拍得真好"),
                _event("post_mentioned", post_id="P-1001"),
            ]
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["acknowledged"] is True
    assert data["accepted"] == 3
    assert data["rejected"] == 0
    assert [item["status"] for item in data["results"]] == ["accepted"] * 3
    # 回执下标与请求顺序一致，调用方按下标对齐
    assert [item["index"] for item in data["results"]] == [0, 1, 2]

    # 一次批量入队，而不是三条各发一次
    assert rig.bus.bulk_calls == 1
    assert rig.bus.single_calls == 0
    assert rig.store.bulk_calls == 1

    messages = rig.drain()
    assert [m.name for m in messages] == ["post_liked", "post_commented", "post_mentioned"]
    # 入队顺序与回执顺序一致：先受理的先被消费
    assert [m.trigger_id for m in messages] == [item["triggerId"] for item in data["results"]]
    assert all(m.workflow is not None for m in messages), "消息应携带步骤快照"
    # 活动日志与消息一一对应
    assert [r["trigger_id"] for r in rig.store.records] == [m.trigger_id for m in messages]
    assert {r["status"] for r in rig.store.records} == {"triggered"}


def test_batch_rejects_unknown_workflow_without_blocking_others(rig):
    resp = rig.client.post(
        "/v1/events/trigger/batch",
        json={
            "events": [
                _event("post_liked"),
                _event("no_such_workflow"),
                _event("post_commented"),
            ]
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 2
    assert data["rejected"] == 1

    rejected = data["results"][1]
    assert rejected["status"] == "rejected"
    assert rejected["index"] == 1
    assert rejected["name"] == "no_such_workflow"
    assert "no_such_workflow" in rejected["error"]
    assert rejected["triggerId"] is None

    # 被拒的那条不占入队位，也不写活动日志
    assert len(rig.drain()) == 2
    assert len(rig.store.records) == 2


def test_batch_all_rejected_returns_200_with_no_side_effects(rig):
    resp = rig.client.post(
        "/v1/events/trigger/batch",
        json={"events": [_event("nope_1"), _event("nope_2")]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 0
    assert data["rejected"] == 2
    assert all(item["error"] for item in data["results"])

    assert rig.drain() == []
    assert rig.store.records == []
    # 没有消息可入队时不该碰总线与活动日志的批量通道
    assert rig.bus.bulk_calls == 0
    assert rig.store.bulk_calls == 0


def test_batch_idempotency_returns_first_receipt(rig):
    body = {"events": [_event("post_liked", post_id="P-2001"), _event("post_commented")]}
    headers = {"Idempotency-Key": "burst-P-2001-1"}

    first = rig.client.post("/v1/events/trigger/batch", json=body, headers=headers)
    assert first.status_code == 200
    assert len(rig.drain()) == 2

    second = rig.client.post("/v1/events/trigger/batch", json=body, headers=headers)
    assert second.status_code == 200
    # 回执原样返回：同一批触发标识，不再生成新的
    assert second.json() == first.json()
    # 没有第二次入队
    assert rig.drain() == []
    assert len(rig.store.records) == 2


def test_batch_idempotency_replays_rejection_too(rig):
    """幂等缓存的是「那次请求的结果」，当时被拒的条目重放时仍是 rejected。"""
    body = {"events": [_event("post_liked"), _event("no_such_workflow")]}
    headers = {"Idempotency-Key": "burst-with-typo-1"}

    first = rig.client.post("/v1/events/trigger/batch", json=body, headers=headers).json()
    second = rig.client.post("/v1/events/trigger/batch", json=body, headers=headers).json()

    assert first["rejected"] == 1
    assert second == first


def test_batch_rejects_too_many_events(rig):
    resp = rig.client.post(
        "/v1/events/trigger/batch",
        json={"events": [_event("post_liked") for _ in range(101)]},
    )
    assert resp.status_code == 422


def test_batch_rejects_empty_list(rig):
    resp = rig.client.post("/v1/events/trigger/batch", json={"events": []})
    assert resp.status_code == 422


def test_batch_resolves_workflow_from_store(rig, monkeypatch):
    """批量与单条共用同一套查找：CRUD 新建的工作流同样可批量触发。"""
    import app.workflow_store as ws
    from app.models.entities import Step, Workflow

    store = ws.MemoryWorkflowStore(
        seed=[Workflow(name="post_shared", steps=[Step(id="s1", type="in_app", config={})])]
    )
    monkeypatch.setattr(ws, "_store", store)

    resp = rig.client.post(
        "/v1/events/trigger/batch",
        json={"events": [_event("post_shared"), _event("post_shared")]},
    )
    assert resp.status_code == 200
    assert resp.json()["accepted"] == 2
    assert [m.name for m in rig.drain()] == ["post_shared", "post_shared"]


def test_batch_rejects_event_from_other_environment(rig, monkeypatch):
    """环境不匹配的条目单独被拒，同批其它环境的条目照常受理。"""
    import app.workflow_store as ws
    from app.models.entities import Environment, Step, Workflow

    store = ws.MemoryWorkflowStore(
        seed=[
            Workflow(
                name="post_prod_only",
                steps=[Step(id="s1", type="in_app", config={})],
                environment=Environment.PRODUCTION,
            )
        ]
    )
    monkeypatch.setattr(ws, "_store", store)
    monkeypatch.setattr(config, "API_KEYS", {"development": "", "production": ""})

    resp = rig.client.post(
        "/v1/events/trigger/batch",
        json={"events": [_event("post_prod_only"), _event("post_liked")]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 1
    assert data["rejected"] == 1
    assert "development" in data["results"][0]["error"]
    assert len(rig.drain()) == 1
