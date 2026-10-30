"""持久化单元测试。"""
from __future__ import annotations

import json

from app.storage import JSONLStorage


def test_jsonl_storage_appends_records(tmp_path):
    out = tmp_path / "messages.jsonl"
    storage = JSONLStorage(path=out)

    storage.save({"trigger_id": "a", "status": "sent", "payload": {"k": "v"}})
    storage.save({"trigger_id": "b", "status": "sent", "payload": {}})

    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["trigger_id"] == "a"
    assert first["payload"] == {"k": "v"}


def test_jsonl_storage_creates_parent_dir(tmp_path):
    out = tmp_path / "nested" / "dir" / "messages.jsonl"
    storage = JSONLStorage(path=out)
    storage.save({"trigger_id": "a"})
    assert out.exists()


def test_jsonl_storage_revision_tracks_appends(tmp_path):
    """版本号只靠文件元数据得出，内容变化即变化，缺文件时给固定标记。"""
    out = tmp_path / "messages.jsonl"
    storage = JSONLStorage(path=out)

    assert storage.revision() == "absent"

    storage.save({"trigger_id": "a", "status": "sent", "payload": {}})
    first = storage.revision()
    assert storage.revision() == first, "没有写入时版本保持不变"

    storage.save({"trigger_id": "b", "status": "sent", "payload": {}})
    assert storage.revision() != first
