"""消息执行明细存储与查询 API 测试（Phase 1）。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.execution_log import JSONLExecutionStore, record_execution
from app.execution_routes import _filter_and_sort
from app.main import app


def _rec(trigger_id="t1", channel="email", status="sent", reason=None,
         duration_ms=0, created_at="2026-10-22T12:00:00+00:00"):
    return {
        "trigger_id": trigger_id,
        "channel": channel,
        "status": status,
        "reason": reason,
        "duration_ms": duration_ms,
        "created_at": created_at,
    }


def test_jsonl_execution_store_roundtrip(tmp_path):
    path = tmp_path / "executions.jsonl"
    store = JSONLExecutionStore(path=path)
    store.append(_rec(trigger_id="t1", channel="email"))
    store.append(_rec(trigger_id="t1", channel="sms"))
    store.append(_rec(trigger_id="t2", channel="push", status="failed", reason="timeout"))

    records = store.read_all()
    assert len(records) == 3
    assert records[0]["trigger_id"] == "t1"
    assert records[0]["channel"] == "email"
    assert records[2]["status"] == "failed"
    assert records[2]["reason"] == "timeout"


def test_jsonl_execution_store_empty_when_missing(tmp_path):
    store = JSONLExecutionStore(path=tmp_path / "nope.jsonl")
    assert store.read_all() == []


def test_record_execution_writes_and_returns(monkeypatch):
    captured = {}

    class _Store:
        def append(self, record):
            captured.update(record)

        def read_all(self):
            return []

    import app.execution_log as mod

    monkeypatch.setattr(mod, "_execution_store", _Store())

    out = record_execution(trigger_id="t9", channel="sms", status="sent")
    assert out["trigger_id"] == "t9"
    assert out["channel"] == "sms"
    assert out["status"] == "sent"
    assert out["reason"] is None
    assert out["created_at"]
    assert captured["trigger_id"] == "t9"


def test_filter_and_sort_by_trigger_channel_status():
    records = [
        _rec(trigger_id="a", channel="email"),
        _rec(trigger_id="a", channel="sms"),
        _rec(trigger_id="b", channel="push", status="failed"),
    ]
    assert len(_filter_and_sort(records, "a", None, None, 10)) == 2
    assert len(_filter_and_sort(records, "a", "email", None, 10)) == 1
    assert len(_filter_and_sort(records, None, None, "failed", 10)) == 1
    # 无匹配返回空
    assert _filter_and_sort(records, "zzz", None, None, 10) == []


@pytest.fixture
def client(monkeypatch):
    import app.execution_log as mod

    class _FakeStore:
        def __init__(self):
            self.records = [
                _rec(trigger_id="a", channel="email", status="sent"),
                _rec(trigger_id="a", channel="sms", status="sent"),
                _rec(trigger_id="b", channel="push", status="failed", reason="timeout"),
            ]

        def append(self, record):
            self.records.append(record)

        def read_all(self):
            return list(self.records)

    monkeypatch.setattr(mod, "_execution_store", _FakeStore())
    return TestClient(app)


def test_executions_endpoint_default(client):
    resp = client.get("/v1/executions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3


def test_executions_endpoint_filter(client):
    resp = client.get("/v1/executions", params={"triggerId": "a"})
    data = resp.json()
    assert data["total"] == 2
    assert all(i["trigger_id"] == "a" for i in data["items"])

    resp = client.get("/v1/executions", params={"channel": "push", "status": "failed"})
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["reason"] == "timeout"


__all__ = []
