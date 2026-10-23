"""活动流查询 API 测试：归一化、过滤、排序与端点返回结构。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.activity_routes import _filter_and_sort, _to_activity
from app.main import app


def _record(workflow="welcome", subscriber="u1", channels=None, status="sent",
            days_ago=0, payload=None, trigger_id="t"):
    dt = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {
        "trigger_id": trigger_id,
        "transaction_id": "tx",
        "workflow": workflow,
        "subscriber_id": subscriber,
        "channels": channels or ["in_app"],
        "payload": payload or {},
        "status": status,
        "created_at": dt,
    }


def test_to_activity_normalizes_channels_and_title():
    a = _to_activity(_record(channels="email", payload={"title": "订单已发货"}))
    assert a["channels"] == ["email"]
    assert a["title"] == "订单已发货"
    assert a["workflow"] == "welcome"


def test_to_activity_title_falls_back_to_workflow():
    a = _to_activity(_record())
    assert a["title"] == "welcome"


def test_filter_and_sort_newest_first():
    records = [
        _record(trigger_id="old", days_ago=2),
        _record(trigger_id="new", days_ago=0),
        _record(trigger_id="mid", days_ago=1),
    ]
    items = _filter_and_sort(records, None, None, 10)
    assert [i["trigger_id"] for i in items] == ["new", "mid", "old"]


def test_filter_by_subscriber_and_workflow():
    records = [
        _record(subscriber="u1", workflow="welcome"),
        _record(subscriber="u2", workflow="welcome"),
        _record(subscriber="u1", workflow="reset"),
    ]
    assert len(_filter_and_sort(records, "u1", None, 10)) == 2
    assert len(_filter_and_sort(records, "u1", "reset", 10)) == 1
    assert len(_filter_and_sort(records, None, "welcome", 10)) == 2


def test_filter_limit_applied():
    records = [_record(trigger_id=str(i), days_ago=0) for i in range(10)]
    assert len(_filter_and_sort(records, None, None, 3)) == 3


def test_filter_by_trigger_id():
    records = [
        _record(trigger_id="t1"),
        _record(trigger_id="t2"),
    ]
    items = _filter_and_sort(records, None, None, 10, trigger_id="t1")
    assert [i["trigger_id"] for i in items] == ["t1"]


def test_filter_by_status():
    records = [
        _record(trigger_id="s1", status="sent"),
        _record(trigger_id="s2", status="failed"),
        _record(trigger_id="s3", status="triggered"),
    ]
    items = _filter_and_sort(records, None, None, 10, status="failed")
    assert [i["trigger_id"] for i in items] == ["s2"]


def test_filter_by_time_range():
    now = datetime.now(timezone.utc)
    records = [
        _record(trigger_id="old", days_ago=10),
        _record(trigger_id="recent", days_ago=0),
    ]
    since = now - timedelta(days=5)
    until = now + timedelta(days=1)
    items = _filter_and_sort(records, None, None, 10, since=since, until=until)
    assert [i["trigger_id"] for i in items] == ["recent"]


@pytest.fixture
def client(monkeypatch):
    import app.activity_log as activity_log_mod

    class _FakeActivityStore:
        def __init__(self):
            self.records = [
                _record(trigger_id="a", workflow="welcome", subscriber="u1",
                        channels=["email"], days_ago=0),
                _record(trigger_id="b", workflow="reset", subscriber="u2",
                        channels=["sms"], days_ago=1),
                _record(trigger_id="c", workflow="welcome", subscriber="u1",
                        channels=["push"], days_ago=2),
            ]

        def append(self, record):
            self.records.append(record)

        def read_all(self):
            return list(self.records)

    monkeypatch.setattr(activity_log_mod, "_activity_store", _FakeActivityStore())
    return TestClient(app)


def test_activity_endpoint_default(client):
    resp = client.get("/v1/activity")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert [i["trigger_id"] for i in data["items"]] == ["a", "b", "c"]


def test_activity_endpoint_filter(client):
    resp = client.get("/v1/activity", params={"subscriberId": "u1", "workflow": "welcome"})
    data = resp.json()
    assert data["total"] == 2
    assert all(i["subscriber_id"] == "u1" for i in data["items"])
    assert all(i["workflow"] == "welcome" for i in data["items"])


def test_activity_endpoint_limit_bounds(client):
    # limit 越界由 FastAPI 校验拒绝
    assert client.get("/v1/activity", params={"limit": 0}).status_code == 422
    assert client.get("/v1/activity", params={"limit": 500}).status_code == 422
    assert client.get("/v1/activity", params={"limit": 1}).json()["total"] == 1
