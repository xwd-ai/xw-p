"""活动日志存储与写入测试（Phase 1）。"""
from __future__ import annotations

import pytest

from app.activity_log import JSONLActivityStore, record_activity


def test_jsonl_activity_store_roundtrip(tmp_path):
    path = tmp_path / "activity.jsonl"
    store = JSONLActivityStore(path=path)
    rec = {
        "trigger_id": "t1",
        "workflow": "order_shipped",
        "subscriber_id": "u1",
        "channels": ["email", "sms"],
        "status": "triggered",
        "title": "订单已发货",
        "created_at": "2026-10-22T12:00:00+00:00",
    }
    store.append(rec)
    store.append({**rec, "trigger_id": "t2"})

    records = store.read_all()
    assert len(records) == 2
    assert records[0]["trigger_id"] == "t1"
    assert records[0]["workflow"] == "order_shipped"
    assert records[0]["channels"] == ["email", "sms"]


def test_jsonl_activity_store_empty_when_missing(tmp_path):
    store = JSONLActivityStore(path=tmp_path / "nope.jsonl")
    assert store.read_all() == []


def test_record_activity_writes_and_returns_record(monkeypatch, tmp_path):
    captured = {}

    class _Store:
        def append(self, record):
            captured.update(record)

        def read_all(self):
            return []

    import app.activity_log as mod

    monkeypatch.setattr(mod, "_activity_store", _Store())

    out = record_activity(
        trigger_id="t9",
        workflow="order_delivered",
        subscriber_id="u9",
        channels=["push"],
        payload={"title": "订单已送达", "order_id": "A-1001"},
    )

    assert out["trigger_id"] == "t9"
    assert out["workflow"] == "order_delivered"
    assert out["status"] == "triggered"
    # payload.title 优先作为展示标题
    assert out["title"] == "订单已送达"
    assert out["created_at"]
    assert captured["trigger_id"] == "t9"


def test_record_activity_title_falls_back_to_workflow(monkeypatch):
    import app.activity_log as mod

    class _Store:
        def append(self, record):
            pass

        def read_all(self):
            return []

    monkeypatch.setattr(mod, "_activity_store", _Store())

    out = record_activity(
        trigger_id="t10", workflow="order_confirmed", subscriber_id="u10"
    )
    assert out["title"] == "order_confirmed"
    # 未声明渠道时回退 in_app
    assert out["channels"] == ["in_app"]


__all__ = []
