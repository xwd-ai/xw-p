"""仪表盘统计 API 测试：聚合口径与端点返回结构。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.dashboard_routes import _compute_stats
from app.main import app


def _record(status="sent", channels=None, days_ago=0):
    dt = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {
        "trigger_id": "t",
        "transaction_id": "tx",
        "workflow": "post_liked",
        "subscriber_id": "u1",
        "channels": channels or ["in_app"],
        "payload": {},
        "status": status,
        "created_at": dt,
    }


def test_compute_stats_empty():
    stats = _compute_stats([])
    assert stats["total"] == 0
    assert stats["success_rate"] == 100.0
    assert stats["channel_distribution"] == {}
    assert len(stats["trend"]) == 7
    assert sum(d["count"] for d in stats["trend"]) == 0


def test_compute_stats_total_and_rate():
    records = [
        _record(status="sent", channels=["email"]),
        _record(status="sent", channels=["email"]),
        _record(status="failed", channels=["sms"]),
        _record(status="sent", channels=["push"]),
    ]
    stats = _compute_stats(records)
    assert stats["total"] == 4
    assert stats["success_rate"] == 75.0


def test_compute_stats_channel_distribution_multichannel():
    records = [
        _record(channels=["email", "push"]),
        _record(channels=["email"]),
        _record(channels=["sms"]),
    ]
    dist = _compute_stats(records)["channel_distribution"]
    assert dist == {"email": 2, "push": 1, "sms": 1}


def test_compute_stats_trend_groups_by_day():
    records = [
        _record(days_ago=0),
        _record(days_ago=0),
        _record(days_ago=1),
    ]
    trend = _compute_stats(records)["trend"]
    # 趋势为最近 7 天（含今天），最后一天即今天
    assert trend[-1]["count"] == 2
    assert trend[-2]["count"] == 1
    assert sum(d["count"] for d in trend) == 3


def test_compute_stats_trend_ignores_old_or_missing_ts():
    old = _record(days_ago=30)
    missing_ts = _record()
    missing_ts.pop("created_at")
    bad_ts = _record()
    bad_ts["created_at"] = "not-a-date"
    stats = _compute_stats([old, missing_ts, bad_ts])
    assert stats["total"] == 3
    assert sum(d["count"] for d in stats["trend"]) == 0


@pytest.fixture
def client(monkeypatch):
    """隔离存储单例，注入在内存里完成聚合的固定记录存储。"""
    import app.storage as storage_mod
    from app.stats import compute_stats

    class _FakeStorage:
        def __init__(self):
            self.records = [
                _record(status="sent", channels=["email", "in_app"], days_ago=0),
                _record(status="sent", channels=["push"], days_ago=0),
                _record(status="failed", channels=["sms"], days_ago=1),
            ]

        def save(self, record):
            self.records.append(record)

        def read_all(self):
            return list(self.records)

        def stats(self, trend_days=7):
            return compute_stats(self.records, trend_days)

    monkeypatch.setattr(storage_mod, "_storage", _FakeStorage())
    return TestClient(app)


def test_dashboard_stats_endpoint(client):
    resp = client.get("/v1/dashboard/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert data["success_rate"] == round(2 / 3 * 100, 1)
    assert data["channel_distribution"] == {"email": 1, "in_app": 1, "push": 1, "sms": 1}
    assert len(data["trend"]) == 7


def test_jsonl_storage_stats_matches_compute_stats(tmp_path):
    """流式累加的结果与小数据量下的整体聚合一致。"""
    from app.storage import JSONLStorage

    path = tmp_path / "messages.jsonl"
    store = JSONLStorage(path=path)
    records = [
        _record(status="sent", channels=["in_app", "push"], days_ago=0),
        _record(status="failed", channels=["email"], days_ago=0),
        _record(status="sent", channels=["sms"], days_ago=2),
        _record(status="sent", channels=["in_app"], days_ago=30),
    ]
    for r in records:
        store.save(r)

    assert store.stats() == _compute_stats(records)


def test_jsonl_storage_stats_handles_missing_file(tmp_path):
    from app.storage import JSONLStorage

    stats = JSONLStorage(path=tmp_path / "nope.jsonl").stats()
    assert stats["total"] == 0
    assert stats["success_rate"] == 100.0
    assert sum(d["count"] for d in stats["trend"]) == 0


def test_jsonl_storage_stats_skips_corrupt_lines(tmp_path):
    from app.storage import JSONLStorage

    path = tmp_path / "messages.jsonl"
    store = JSONLStorage(path=path)
    store.save(_record(status="sent"))
    with path.open("a", encoding="utf-8") as fh:
        fh.write("{broken\n")
    stats = store.stats()
    assert stats["total"] == 1
    assert stats["success_rate"] == 100.0
