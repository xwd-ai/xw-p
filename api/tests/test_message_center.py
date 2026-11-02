"""消息中心数据模型与存储测试：字段归一、幂等写入、原地改写与文件共享。"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app import message_center
from app.message_center import (
    MESSAGE_FIELDS,
    JsonlMessageCenter,
    MemoryMessageCenter,
    Message,
    MessageError,
    WriteOutcome,
    default_path,
    get_store,
    parse_timestamp,
    set_store,
)


def _message(message_id: str = "t1:step-1", subscriber: str = "creator_1001", body: str = "有 3 人赞了你的帖子"):
    return Message.create(
        id=message_id,
        subscriber_id=subscriber,
        title="帖子互动",
        body=body,
        trigger_id="t1",
        workflow="post_liked",
        step_id="step-1",
        now=datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------- 消息模型


def test_message_normalizes_fields():
    msg = Message.create(
        id="  t1:step-1  ",
        subscriber_id="  creator_1001 ",
        title="   ",
        body="  有 3 人赞了你的帖子  ",
        trigger_id=" t1 ",
    )
    assert msg.id == "t1:step-1"
    assert msg.subscriber_id == "creator_1001"
    assert msg.title == message_center.DEFAULT_TITLE
    assert msg.body == "有 3 人赞了你的帖子"
    assert msg.read is False


@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"id": "", "subscriber_id": "u1", "body": "x"}, "标识"),
        ({"id": "m1", "subscriber_id": "", "body": "x"}, "订阅者"),
        ({"id": "m1", "subscriber_id": "u1", "body": "   "}, "正文"),
    ],
)
def test_message_rejects_incomplete_records(kwargs, reason):
    with pytest.raises(MessageError) as info:
        Message.create(**kwargs)
    assert reason in str(info.value)


def test_message_payload_matches_engine_schema():
    data = _message().to_dict()
    assert tuple(data) == MESSAGE_FIELDS
    assert data["id"] == "t1:step-1"
    assert data["created_at"].startswith("2026-03-01T08:00:00")
    assert "+00:00" in data["created_at"]


def test_message_from_dict_reads_engine_timestamps():
    """引擎写的是带纳秒与 Z 的 RFC3339，读取方要能解析。"""
    raw = {
        "id": "t1:step-1",
        "trigger_id": "t1",
        "subscriber_id": "creator_1001",
        "workflow": "post_commented",
        "step_id": "step-1",
        "title": "新评论",
        "body": "小林：拍得真好",
        "created_at": "2026-03-01T08:00:00.123456789Z",
        "updated_at": "2026-03-01T08:05:00.5Z",
        "read": False,
    }
    msg = Message.from_dict(raw)
    assert msg.created_at == datetime(2026, 3, 1, 8, 0, 0, 123456, tzinfo=timezone.utc)
    assert msg.updated_at.minute == 5
    assert msg.workflow == "post_commented"


def test_message_from_dict_tolerates_missing_timestamps():
    msg = Message.from_dict({"id": "m1", "subscriber_id": "u1", "body": "x"})
    assert msg.created_at.tzinfo is not None
    assert msg.updated_at == msg.created_at


def test_message_rewritten_advances_updated_at():
    msg = _message()
    later = msg.updated_at + timedelta(minutes=3)
    updated = msg.rewritten(body="有 5 人赞了你的帖子", now=later)
    assert updated.id == msg.id
    assert updated.created_at == msg.created_at
    assert updated.updated_at == later
    assert updated.body == "有 5 人赞了你的帖子"
    assert updated.title == msg.title


def test_message_rewritten_never_moves_time_backwards():
    msg = _message()
    updated = msg.rewritten(body="旧内容", now=msg.created_at - timedelta(hours=1))
    assert updated.updated_at == msg.created_at


def test_parse_timestamp_handles_offsets_and_garbage():
    assert parse_timestamp("2026-03-01T08:00:00Z") == datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)
    assert parse_timestamp("2026-03-01T16:00:00+08:00") == datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)
    assert parse_timestamp("2026-03-01T08:00:00") == datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)
    assert parse_timestamp("不是时间") is None
    assert parse_timestamp(None) is None


# ---------------------------------------------------------------- 内存存储


def test_memory_store_write_outcomes():
    store = MemoryMessageCenter()
    msg = _message()

    assert store.write(msg) is WriteOutcome.CREATED
    assert store.write(msg) is WriteOutcome.DUPLICATE
    assert len(store) == 1

    changed = msg.rewritten(body="有 5 人赞了你的帖子", now=msg.updated_at + timedelta(minutes=1))
    assert store.write(changed) is WriteOutcome.DUPLICATE, "不带 replace 的写入不覆盖已有内容"
    assert store.write(changed, replace=True) is WriteOutcome.UPDATED
    assert store.get(msg.id).body == "有 5 人赞了你的帖子"
    assert store.get("missing") is None


def test_memory_store_replace_missing_target():
    store = MemoryMessageCenter()
    assert store.write(_message(), replace=True) is WriteOutcome.MISSING
    assert len(store) == 0


def test_memory_store_lists_by_subscriber():
    store = MemoryMessageCenter()
    store.write(_message("m1", subscriber="u1"))
    store.write(_message("m2", subscriber="u2"))
    store.write(_message("m3", subscriber="u1"))
    assert [m.id for m in store.list("u1")] == ["m1", "m3"]
    assert store.list("nobody") == []


# ---------------------------------------------------------------- JSONL 存储


def test_jsonl_store_persists_across_instances(tmp_path):
    path = tmp_path / "inbox.jsonl"
    first = JsonlMessageCenter(path)
    assert first.write(_message()) is WriteOutcome.CREATED

    reopened = JsonlMessageCenter(path)
    stored = reopened.get("t1:step-1")
    assert stored is not None
    assert stored.body == "有 3 人赞了你的帖子"
    assert stored.workflow == "post_liked"

    # 新实例按文件索引判重，同一条不会写第二遍
    assert reopened.write(_message()) is WriteOutcome.DUPLICATE
    assert len(path.read_text(encoding="utf-8").strip().splitlines()) == 1


def test_jsonl_store_detects_other_process_appends(tmp_path):
    """索引建好之后文件被追加：索引未命中应先重扫再判断，不能重复落一条。"""
    path = tmp_path / "inbox.jsonl"
    writer = JsonlMessageCenter(path)
    reader = JsonlMessageCenter(path)  # 索引为空

    writer.write(_message())

    assert reader.write(_message()) is WriteOutcome.DUPLICATE
    assert len(path.read_text(encoding="utf-8").strip().splitlines()) == 1


def test_jsonl_store_rewrites_in_place(tmp_path):
    path = tmp_path / "inbox.jsonl"
    store = JsonlMessageCenter(path)
    first = _message("m1")
    second = _message("m2", body="有 1 条新评论")
    store.write(first)
    store.write(second)

    changed = first.rewritten(body="有 6 人赞了你的帖子", now=first.updated_at + timedelta(minutes=2))
    assert store.write(changed, replace=True) is WriteOutcome.UPDATED

    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [line["id"] for line in lines] == ["m1", "m2"], "改写不应打乱写入顺序"
    assert lines[0]["body"] == "有 6 人赞了你的帖子"
    assert lines[0]["created_at"] == lines[1]["created_at"], "改写只推进 updated_at"
    assert lines[1]["body"] == "有 1 条新评论"


def test_jsonl_store_replace_missing_target(tmp_path):
    store = JsonlMessageCenter(tmp_path / "inbox.jsonl")
    assert store.write(_message(), replace=True) is WriteOutcome.MISSING
    assert not (tmp_path / "inbox.jsonl").exists()


def test_jsonl_store_skips_corrupt_lines(tmp_path):
    path = tmp_path / "inbox.jsonl"
    path.write_text(
        "{不是合法 JSON\n" + json.dumps(_message("m1").to_dict(), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    store = JsonlMessageCenter(path)
    assert [m.id for m in store.list("creator_1001")] == ["m1"]

    # 损坏行在改写时原样保留，不为了改一条卡片丢掉其它内容
    changed = _message("m1").rewritten(body="更新后的正文", now=datetime.now(timezone.utc))
    assert store.write(changed, replace=True) is WriteOutcome.UPDATED
    text = path.read_text(encoding="utf-8")
    assert "{不是合法 JSON" in text
    assert "更新后的正文" in text


def test_jsonl_store_revision_tracks_file_state(tmp_path):
    path = tmp_path / "inbox.jsonl"
    store = JsonlMessageCenter(path)
    before = store.revision()
    store.write(_message())
    after = store.revision()
    assert before != after
    assert after[0] == "file"


def test_jsonl_store_filters_by_subscriber_in_write_order(tmp_path):
    store = JsonlMessageCenter(tmp_path / "inbox.jsonl")
    store.write(_message("m1", subscriber="u1"))
    store.write(_message("m2", subscriber="u2"))
    store.write(_message("m3", subscriber="u1"))
    assert [m.id for m in store.list("u1")] == ["m1", "m3"]
    assert store.list("u2")[0].id == "m2"


def test_jsonl_store_handles_concurrent_writes(tmp_path):
    path = tmp_path / "inbox.jsonl"
    store = JsonlMessageCenter(path)
    workers, per_thread = 8, 10
    errors: list[BaseException] = []
    barrier = threading.Barrier(workers)

    def run(index: int) -> None:
        try:
            barrier.wait()
            for seq in range(per_thread):
                store.write(_message(f"t{index}-{seq}", subscriber=f"u{index % 3}"))
        except BaseException as exc:  # noqa: BLE001 - 线程内异常收集后断言
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    ids = [json.loads(line)["id"] for line in lines]
    assert len(ids) == workers * per_thread
    assert len(set(ids)) == len(ids), "并发写入不应产生半行或重复行"
    assert len(store) == workers * per_thread


def test_jsonl_store_rejects_non_message(tmp_path):
    store = JsonlMessageCenter(tmp_path / "inbox.jsonl")
    with pytest.raises(MessageError):
        store.write({"id": "m1"})  # type: ignore[arg-type]


# ---------------------------------------------------------------- 默认位置与单例


def test_default_path_follows_env(monkeypatch, tmp_path):
    monkeypatch.setenv("XW_INBOX_FILE", str(tmp_path / "shared.jsonl"))
    assert default_path() == tmp_path / "shared.jsonl"

    monkeypatch.setenv("XW_INBOX_FILE", "  ")
    assert default_path().name == "inbox.jsonl"


def test_store_singleton_reuses_instance(monkeypatch, tmp_path):
    monkeypatch.setenv("XW_INBOX_FILE", str(tmp_path / "inbox.jsonl"))
    set_store(None)
    try:
        first = get_store()
        assert get_store() is first
        assert isinstance(first, JsonlMessageCenter)
        assert first.path == tmp_path / "inbox.jsonl"

        custom = MemoryMessageCenter()
        set_store(custom)
        assert get_store() is custom
    finally:
        set_store(None)
