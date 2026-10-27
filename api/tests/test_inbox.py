"""In-app 收件箱 API 测试：列表过滤、未读计数、已读标记、引擎收件箱合并。"""
from __future__ import annotations

import json
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
    # 隔离引擎收件箱文件：默认不存在，模拟尚未跑过引擎
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", tmp_path / "inbox.jsonl")
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


def _engine_entry(trigger_id, subscriber="u1", title="你的帖子被点赞了", body="小林 赞了你的帖子 P1001"):
    return {
        "id": f"{trigger_id}:s1",
        "trigger_id": trigger_id,
        "subscriber_id": subscriber,
        "workflow": "post_liked",
        "step_id": "s1",
        "title": title,
        "body": body,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "read": False,
    }


def _write_engine_inbox(path, entries):
    path.write_text(
        "\n".join(json.dumps(e, ensure_ascii=False) for e in entries) + "\n",
        encoding="utf-8",
    )


def test_inbox_merges_engine_entries(client, monkeypatch, tmp_path):
    """引擎 in_app 步骤写入的条目应出现在收件箱，且与投递记录按 trigger_id 去重。"""
    import app.inbox as inbox_mod

    path = tmp_path / "engine_inbox.jsonl"
    _write_engine_inbox(
        path,
        [
            _engine_entry("a", title="渲染后的标题", body="渲染后的正文"),
            _engine_entry("e1"),
            _engine_entry("e2", subscriber="u2"),
        ],
    )
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)

    data = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    by_id = {i["trigger_id"]: i for i in data["items"]}

    # a 同时存在于引擎文件与投递记录：只保留一份，且使用引擎渲染后的内容
    assert [i["trigger_id"] for i in data["items"]].count("a") == 1
    assert by_id["a"]["title"] == "渲染后的标题"
    assert by_id["a"]["body"] == "渲染后的正文"
    # 引擎独有条目 e1 出现，u2 的 e2 不出现
    assert "e1" in by_id and "e2" not in by_id
    # u1 共 3 条：引擎 e1 + 投递记录 a（去重后仅保留引擎版本）、c
    assert data["total"] == 3
    assert data["items"][0]["trigger_id"] == "e1"
    assert data["unread"] == 3


def test_mark_all_read_includes_engine_entries(client, monkeypatch, tmp_path):
    """「全部已读」应同时覆盖引擎写入的收件箱条目。"""
    import app.inbox as inbox_mod

    path = tmp_path / "engine_inbox.jsonl"
    _write_engine_inbox(path, [_engine_entry("e1")])
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)

    resp = client.post("/v1/inbox/read", json={"subscriberId": "u1"}).json()
    assert resp["marked"] == 3  # a、c、e1
    assert resp["unread"] == 0

    inbox = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    by_id = {i["trigger_id"]: i["read"] for i in inbox["items"]}
    assert by_id["e1"] is True


def test_inbox_skips_corrupted_engine_line(client, monkeypatch, tmp_path):
    """引擎文件中的损坏行应被跳过，不影响其余条目。"""
    import app.inbox as inbox_mod

    path = tmp_path / "engine_inbox.jsonl"
    path.write_text(
        "{不是合法 JSON\n" + json.dumps(_engine_entry("e1"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)

    data = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    assert "e1" in [i["trigger_id"] for i in data["items"]]

# ---------------------------------------------------------------------------
# 聚合卡片：同一张卡在窗口内被原地改写，未读计数按消息条数联动
# ---------------------------------------------------------------------------


class _ListStorage:
    """只用于本组测试的最小存储桩：隔离掉其他用例写入的投递记录。"""

    def __init__(self, records=None):
        self.records = list(records or [])

    def save(self, record):
        self.records.append(record)

    def read_all(self):
        return list(self.records)


def _digest_entry(
    message_id="digest-abc",
    trigger_id="t1",
    subscriber="u1",
    count=3,
    created_at=None,
    updated_at=None,
):
    created = created_at or datetime.now(timezone.utc)
    return {
        "id": message_id,
        "trigger_id": trigger_id,
        "subscriber_id": subscriber,
        "workflow": "post_liked",
        "step_id": "dg1",
        "title": "互动汇总",
        "body": f"你的帖子 P1001 累计 {count} 条互动",
        "created_at": created.isoformat(),
        "updated_at": (updated_at or created).isoformat(),
        "read": False,
    }


def test_digest_card_merges_with_delivery_record(client, monkeypatch, tmp_path):
    """聚合卡片与同一次触发的投递记录只出现一条，未读不重复累加。"""
    import app.inbox as inbox_mod

    path = tmp_path / "inbox.jsonl"
    _write_engine_inbox(path, [_digest_entry(trigger_id="t1", count=5)])
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)
    # 同一次触发既写进了引擎收件箱，也留下了 in_app 投递记录
    monkeypatch.setattr(
        inbox_mod,
        "get_storage",
        lambda: _ListStorage([_record(trigger_id="t1", channels=["in_app"])]),
    )

    data = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    assert data["total"] == 1
    assert data["unread"] == 1
    assert data["items"][0]["message_id"] == "digest-abc"
    assert "累计 5 条互动" in data["items"][0]["body"]


def test_digest_card_update_marks_again_unread(client, monkeypatch, tmp_path):
    """已读的聚合卡片被新互动顶起来后应重新算未读，且仍是同一条消息。"""
    import app.inbox as inbox_mod

    path = tmp_path / "inbox.jsonl"
    now = datetime.now(timezone.utc)
    _write_engine_inbox(
        path,
        [
            _digest_entry(
                trigger_id="t1", count=2,
                created_at=now - timedelta(minutes=5),
                updated_at=now - timedelta(minutes=5),
            )
        ],
    )
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)
    monkeypatch.setattr(inbox_mod, "get_storage", lambda: _ListStorage())

    first = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    assert first["unread"] == 1

    read = client.post(
        "/v1/inbox/read", json={"subscriberId": "u1", "messageIds": ["digest-abc"]}
    ).json()
    assert read["marked"] == 1
    assert read["unread"] == 0

    # 窗口内又来互动：卡片原地改写，更新时间晚于已读时刻
    _write_engine_inbox(
        path,
        [
            _digest_entry(
                trigger_id="t3", count=4,
                created_at=now - timedelta(minutes=5),
                updated_at=now + timedelta(seconds=30),
            )
        ],
    )

    after = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    assert after["total"] == 1
    assert after["unread"] == 1, "卡片被更新后应重新回到未读"
    assert after["items"][0]["message_id"] == "digest-abc"
    assert after["items"][0]["trigger_id"] == "t3"
    assert "累计 4 条互动" in after["items"][0]["body"]


def test_read_state_compares_read_time_with_update(tmp_path):
    """已读时刻早于卡片更新时间就算未读；同一内容重复标记不重复计数。"""
    from app.inbox import ReadStateStore

    store = ReadStateStore(tmp_path / "read_state.jsonl")
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)

    # 没有更新时间的消息：标记过就算已读；同一内容重复标记幂等
    assert store.mark_read("u1", "m-plain") is True
    assert store.is_read("u1", "m-plain", None) is True
    assert store.mark_read("u1", "m-plain") is False

    # 已读后被新互动改写（更新时间晚于已读时刻）→ 重新算未读
    assert store.mark_read("u1", "m-card") is True
    assert store.is_read("u1", "m-card", past) is True
    assert store.is_read("u1", "m-card", now + timedelta(minutes=1)) is False

    # 落盘可回放：新实例读到同样的已读时刻
    reopened = ReadStateStore(tmp_path / "read_state.jsonl")
    assert reopened.is_read("u1", "m-card", past) is True
    assert reopened.is_read("u1", "m-card", now + timedelta(minutes=1)) is False


def test_read_state_loads_legacy_trigger_records(tmp_path):
    """旧格式（trigger_id、无时刻）记录仍被认作已读，不会让历史消息复活。"""
    from app.inbox import ReadStateStore

    path = tmp_path / "read_state.jsonl"
    path.write_text(
        json.dumps({"subscriber_id": "u1", "trigger_id": "old-trigger"}) + "\n",
        encoding="utf-8",
    )
    store = ReadStateStore(path)
    assert store.is_legacy_read("u1", "old-trigger") is True
    assert store.is_legacy_read("u1", "other") is False
    assert store.is_read("u1", "digest-abc", None) is False


def test_inbox_unread_counts_messages_not_updates(client, monkeypatch, tmp_path):
    """两张卡片 + 一条普通站内信：未读按条数算，与互动次数无关。"""
    import app.inbox as inbox_mod

    path = tmp_path / "inbox.jsonl"
    now = datetime.now(timezone.utc)
    plain = _engine_entry("e1")
    plain["created_at"] = (now - timedelta(minutes=3)).isoformat()
    _write_engine_inbox(
        path,
        [
            _digest_entry(
                message_id="digest-p1", trigger_id="t9", count=42,
                created_at=now - timedelta(minutes=9), updated_at=now,
            ),
            _digest_entry(
                message_id="digest-p2", trigger_id="t8", count=7,
                created_at=now - timedelta(minutes=8), updated_at=now - timedelta(minutes=1),
            ),
            plain,
        ],
    )
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)
    monkeypatch.setattr(inbox_mod, "get_storage", lambda: _ListStorage())

    data = client.get("/v1/inbox", params={"subscriberId": "u1"}).json()
    assert data["unread"] == 3
    # 按最近更新时间倒序：刚被顶起来的卡片在最前
    assert [i["message_id"] for i in data["items"]] == [
        "digest-p1",
        "digest-p2",
        "e1:s1",
    ]
