"""触发 API 单元测试：入参校验、模板查找、幂等去重、回执字段。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app


@pytest.fixture
def client(monkeypatch):
    """隔离测试环境：内存总线 + 内存幂等 + 内存活动日志，避免污染全局单例。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod

    class _FakeActivityStore:
        def __init__(self):
            self.records = []

        def append(self, record):
            self.records.append(record)

        def read_all(self):
            return list(self.records)

    bus = MemoryMessageBus()
    idem = MemoryIdempotencyStore()
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(idem_mod, "_store", idem)
    monkeypatch.setattr(al_mod, "_activity_store", _FakeActivityStore())
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)
    return TestClient(app)


def _trigger_body(name="order_confirmed", subscriber="user_1", **extra):
    body = {"name": name, "to": {"subscriberId": subscriber}, "payload": {"user": "Alice"}}
    body.update(extra)
    return body


def test_trigger_returns_acknowledgment(client):
    resp = client.post("/v1/events/trigger", json=_trigger_body())
    assert resp.status_code == 200
    data = resp.json()
    assert data["acknowledged"] is True
    # 回执三件套：triggerId / transactionId / acknowledgmentId
    assert data["triggerId"]
    assert data["transactionId"]
    assert data["acknowledgmentId"]
    # 约定：acknowledgmentId 与 triggerId 一致
    assert data["acknowledgmentId"] == data["triggerId"]


def test_trigger_unknown_workflow_returns_404(client):
    resp = client.post("/v1/events/trigger", json=_trigger_body(name="no_such_template"))
    assert resp.status_code == 404
    assert "no_such_template" in resp.json()["detail"]


def test_trigger_workflow_from_store_fallback(client, monkeypatch):
    """CRUD 新建（仅存在于 store、不在 registry）的工作流也能被触发。"""
    import app.workflow_store as ws
    from app.models.entities import Step, Workflow

    store = ws.MemoryWorkflowStore(seed=[
        Workflow(name="onboarding", steps=[Step(id="s1", type="in_app", name="欢迎", config={})], version=1)
    ])
    monkeypatch.setattr(ws, "_store", store)

    resp = client.post("/v1/events/trigger", json=_trigger_body(name="onboarding"))
    assert resp.status_code == 200
    assert resp.json()["acknowledged"] is True


def test_trigger_inactive_store_workflow_returns_404(client, monkeypatch):
    """store 中未启用的工作流不可被触发（active=False）。"""
    import app.workflow_store as ws
    from app.models.entities import Step, Workflow

    store = ws.MemoryWorkflowStore(seed=[
        Workflow(name="off", steps=[Step(id="s1", type="in_app", name="x", config={})], version=1, active=False)
    ])
    monkeypatch.setattr(ws, "_store", store)

    resp = client.post("/v1/events/trigger", json=_trigger_body(name="off"))
    assert resp.status_code == 404


def test_trigger_missing_subscriber_returns_422(client):
    resp = client.post("/v1/events/trigger", json={"name": "welcome", "to": {}})
    assert resp.status_code == 422


def test_trigger_empty_name_returns_422(client):
    resp = client.post(
        "/v1/events/trigger", json=_trigger_body(name="")
    )
    assert resp.status_code == 422


def test_trigger_payload_not_dict_returns_422(client):
    resp = client.post(
        "/v1/events/trigger",
        json={"name": "welcome", "to": {"subscriberId": "u1"}, "payload": "not-a-dict"},
    )
    assert resp.status_code == 422


def test_idempotency_key_returns_same_receipt(client):
    """相同幂等键重复请求应返回完全相同的回执，且不重复入队。"""
    body = _trigger_body()
    headers = {"Idempotency-Key": "key-001"}

    r1 = client.post("/v1/events/trigger", json=body, headers=headers)
    r2 = client.post("/v1/events/trigger", json=body, headers=headers)

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json() == r2.json()


def test_distinct_idempotency_keys_generate_distinct_receipts(client):
    body = _trigger_body()
    r1 = client.post("/v1/events/trigger", json=body, headers={"Idempotency-Key": "k1"})
    r2 = client.post("/v1/events/trigger", json=body, headers={"Idempotency-Key": "k2"})
    assert r1.json()["triggerId"] != r2.json()["triggerId"]


def test_no_idempotency_key_generates_new_receipt_each_time(client):
    body = _trigger_body()
    r1 = client.post("/v1/events/trigger", json=body)
    r2 = client.post("/v1/events/trigger", json=body)
    assert r1.json()["triggerId"] != r2.json()["triggerId"]
