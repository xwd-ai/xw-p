"""活动日志存储与写入测试（Phase 1）。"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.activity_log import (
    JSONLActivityStore,
    MemoryActivityStore,
    build_activity,
    record_activities,
    record_activity,
)


def test_jsonl_activity_store_roundtrip(tmp_path):
    path = tmp_path / "activity.jsonl"
    store = JSONLActivityStore(path=path)
    rec = {
        "trigger_id": "t1",
        "workflow": "post_commented",
        "subscriber_id": "u1",
        "channels": ["email", "sms"],
        "status": "triggered",
        "title": "收到新评论",
        "created_at": "2026-10-22T12:00:00+00:00",
    }
    store.append(rec)
    store.append({**rec, "trigger_id": "t2"})

    records = store.read_all()
    assert len(records) == 2
    assert records[0]["trigger_id"] == "t1"
    assert records[0]["workflow"] == "post_commented"
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
        workflow="post_mentioned",
        subscriber_id="u9",
        channels=["push"],
        payload={"title": "被 @ 提及", "post_id": "A-1001"},
    )

    assert out["trigger_id"] == "t9"
    assert out["workflow"] == "post_mentioned"
    assert out["status"] == "triggered"
    # payload.title 优先作为展示标题
    assert out["title"] == "被 @ 提及"
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
        trigger_id="t10", workflow="post_commented", subscriber_id="u10"
    )
    assert out["title"] == "post_commented"
    # 未声明渠道时回退 in_app
    assert out["channels"] == ["in_app"]


# --- 批量写入 ---


def _record(trigger_id: str) -> dict:
    return build_activity(
        trigger_id=trigger_id,
        workflow="post_liked",
        subscriber_id="creator_1",
        channels=["in_app"],
        payload={"post_id": "P-1001", "actor": "小林"},
        environment="development",
    )


def test_build_activity_does_not_write(tmp_path):
    """构造与落库分开：批量路径要先把整批记录攒齐再一次性写。"""
    store = JSONLActivityStore(path=tmp_path / "activity.jsonl")
    record = _record("t-build")
    assert record["workflow"] == "post_liked"
    assert record["channels"] == ["in_app"]
    assert not (tmp_path / "activity.jsonl").exists()
    assert store.read_all() == []


def test_jsonl_append_many_writes_all_lines_in_one_open(tmp_path, monkeypatch):
    path = tmp_path / "activity.jsonl"
    store = JSONLActivityStore(path=path)

    opens: list[Path] = []
    real_open = Path.open

    def counting_open(self, *args, **kwargs):
        opens.append(self)
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    records = [_record(f"t-{i}") for i in range(5)]
    assert store.append_many(records) == 5
    # 整批只开关一次文件
    assert opens == [path]

    stored = store.read_all()
    assert [r["trigger_id"] for r in stored] == [r["trigger_id"] for r in records]
    # 标题按 payload.title → workflow 回退，payload 本身不落活动日志
    assert stored[0]["title"] == "post_liked"
    assert "payload" not in stored[0]


def test_jsonl_append_many_empty_is_noop(tmp_path, monkeypatch):
    store = JSONLActivityStore(path=tmp_path / "activity.jsonl")

    def _boom(*a, **k):
        raise AssertionError("空批次不该打开文件")

    monkeypatch.setattr(Path, "open", _boom)
    assert store.append_many([]) == 0


def test_memory_store_append_many():
    store = MemoryActivityStore()
    assert store.append_many([_record("t-1"), _record("t-2")]) == 2
    assert [r["trigger_id"] for r in store.read_all()] == ["t-1", "t-2"]
    assert store.append_many([]) == 0


def test_record_activities_uses_bulk_channel(monkeypatch):
    calls = {"bulk": 0, "single": 0}

    class _Store:
        def append(self, record):
            calls["single"] += 1

        def append_many(self, records):
            calls["bulk"] += 1
            return len(records)

    import app.activity_log as mod

    monkeypatch.setattr(mod, "_activity_store", _Store())

    assert record_activities([_record("t-a"), _record("t-b")]) == 2
    assert calls == {"bulk": 1, "single": 0}


def test_record_activities_falls_back_to_single_append(monkeypatch):
    """没有批量能力的后端逐条写，行为与逐条调用 append 一致。"""
    written: list[str] = []

    class _Store:
        def append(self, record):
            written.append(record["trigger_id"])

    import app.activity_log as mod

    monkeypatch.setattr(mod, "_activity_store", _Store())

    assert record_activities([_record("t-a"), _record("t-b")]) == 2
    assert written == ["t-a", "t-b"]


def test_record_activities_empty_skips_store(monkeypatch):
    import app.activity_log as mod

    class _Store:
        def append(self, record):
            raise AssertionError("空批次不该写")

        def append_many(self, records):
            raise AssertionError("空批次不该写")

    monkeypatch.setattr(mod, "_activity_store", _Store())
    assert record_activities([]) == 0


__all__ = []
