"""In-app 收件箱 API 测试：列表过滤、未读计数、已读标记。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.inbox import _is_inapp, _to_inbox_item
from app.main import app


def _record(trigger_id="t", subscriber="u1", channels=None, days_ago=0, payload=None):
    dt = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {
        "trigger_id": trigger_id,
        "transaction_id": "tx",
        "workflow": "post_commented",
        "subscriber_id": subscriber,
        "channels": channels or ["in_app"],
        "payload": payload or {},
        "status": "sent",
        "created_at": dt,
    }


@pytest.fixture
def client(monkeypatch, tmp_path):
    import app.inbox as inbox_mod
    import app.storage as storage_mod

    records = [
        _record(trigger_id="a", subscriber="u1", channels=["in_app"], days_ago=0,
                payload={"title": "欢迎加入", "body": "你好，欢迎"}),
        _record(trigger_id="b", subscriber="u1", channels=["email"], days_ago=1),
        _record(trigger_id="c", subscriber="u1", channels=["in_app"], days_ago=2,
                payload={"title": "系统公告"}),
        _record(trigger_id="d", subscriber="u2", channels=["in_app"], days_ago=0),
    ]

    class _FakeStorage:
        def save(self, record):
            records.append(record)

        def read_all(self):
            return list(records)

    monkeypatch.setattr(storage_mod, "_storage", _FakeStorage())
    # 隔离已读状态：使用临时文件，避免污染真实 read_state.jsonl
    monkeypatch.setattr(inbox_mod, "_read_store", inbox_mod.ReadStateStore(tmp_path / "read_state.jsonl"))
    return TestClient(app)


def test_is_inapp_detects_channel():
    assert _is_inapp(_record(channels=["in_app"])) is True
    assert _is_inapp(_record(channels=["email"])) is False
    assert _is_inapp(_record(channels="in_app")) is True


def test_to_inbox_item_read_flag():
    item = _to_inbox_item(_record(payload={"title": "T", "body": "B"}), read=True)
    assert item["title"] == "T"
    assert item["body"] == "B"
    assert item["read"] is True


def test_inbox_lists_only_inapp_for_subscriber(client):
    resp = client.get("/v1/inbox", params={"subscriberId": "u1"})
    assert resp.status_code == 200
    data = resp.json()
    # u1 有 a(in_app)、b(email)、c(in_app) -> 只返回 in_app 的 a、c，最新在前
    assert data["total"] == 2
    assert [i["trigger_id"] for i in data["items"]] == ["a", "c"]
    assert data["unread"] == 2


def test_inbox_requires_subscriber(client):
    assert client.get("/v1/inbox").status_code == 422


def test_mark_read_single(client):
    resp = client.post(
        "/v1/inbox/read",
        json={"subscriberId": "u1", "triggerIds": ["a"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["marked"] == 1
    assert data["unread"] == 1  # 只剩 c 未读

    # 回读确认 a 已读
    inbox = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    by_id = {i["trigger_id"]: i["read"] for i in inbox["items"]}
    assert by_id["a"] is True
    assert by_id["c"] is False
    assert inbox["unread"] == 1


def test_mark_all_read_when_no_ids(client):
    resp = client.post("/v1/inbox/read", json={"subscriberId": "u1"})
    data = resp.json()
    assert data["marked"] == 2
    assert data["unread"] == 0


def test_mark_read_idempotent(client):
    body = {"subscriberId": "u1", "triggerIds": ["a"]}
    r1 = client.post("/v1/inbox/read", json=body).json()
    r2 = client.post("/v1/inbox/read", json=body).json()
    assert r1["marked"] == 1
    assert r2["marked"] == 0  # 二次标记不再新增


def test_mark_read_missing_subscriber_returns_422(client):
    assert client.post("/v1/inbox/read", json={"triggerIds": ["a"]}).status_code == 422
