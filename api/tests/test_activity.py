"""活动流查询测试：归一化、倒序读、过滤、游标翻页与端点返回结构。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.activity_log import READ_CHUNK, ActivityQuery, JSONLActivityStore, MemoryActivityStore
from app.activity_routes import _to_activity
from app.main import app
from app.timeutil import Cursor


def _record(workflow="post_commented", subscriber="u1", channels=None, status="sent",
            days_ago=0, payload=None, trigger_id="t", at=None, environment="development",
            title=None):
    ts = at or (datetime.now(timezone.utc) - timedelta(days=days_ago))
    record = {
        "trigger_id": trigger_id,
        "workflow": workflow,
        "subscriber_id": subscriber,
        "channels": channels or ["in_app"],
        "status": status,
        "environment": environment,
        "created_at": ts.isoformat(),
    }
    if payload is not None:
        record["payload"] = payload
    if title is not None:
        record["title"] = title
    return record


def test_to_activity_normalizes_channels_and_title():
    a = _to_activity(_record(channels="email", payload={"title": "收到新评论"}))
    assert a["channels"] == ["email"]
    assert a["title"] == "收到新评论"
    assert a["workflow"] == "post_commented"


def test_to_activity_prefers_stored_title():
    a = _to_activity(_record(title="小林评论了你的帖子"))
    assert a["title"] == "小林评论了你的帖子"


def test_to_activity_title_falls_back_to_workflow():
    assert _to_activity(_record())["title"] == "post_commented"


# --- JSONL 后端：倒序读取与过滤 ---


@pytest.fixture
def store(tmp_path):
    return JSONLActivityStore(path=tmp_path / "activity.jsonl")


def test_query_returns_newest_first(store):
    base = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    for i, tid in enumerate(["old", "mid", "new"]):
        store.append(_record(trigger_id=tid, at=base + timedelta(minutes=i)))
    items = store.query(ActivityQuery(limit=10))
    assert [i["trigger_id"] for i in items] == ["new", "mid", "old"]


def test_query_empty_when_file_missing(store):
    assert store.query(ActivityQuery(limit=10)) == []


def test_query_filters_by_subscriber_workflow_status(store):
    store.append(_record(trigger_id="a", subscriber="u1", workflow="post_commented"))
    store.append(_record(trigger_id="b", subscriber="u2", workflow="post_commented"))
    store.append(_record(trigger_id="c", subscriber="u1", workflow="new_follower"))
    store.append(_record(trigger_id="d", subscriber="u1", status="failed"))

    assert [i["trigger_id"] for i in store.query(ActivityQuery(subscriber_id="u1", limit=10))] == ["d", "c", "a"]
    assert [i["trigger_id"] for i in store.query(ActivityQuery(workflow="new_follower", limit=10))] == ["c"]
    assert [i["trigger_id"] for i in store.query(ActivityQuery(status="failed", limit=10))] == ["d"]
    assert [i["trigger_id"] for i in store.query(ActivityQuery(trigger_id="b", limit=10))] == ["b"]
    assert [i["trigger_id"] for i in store.query(ActivityQuery(environment="production", limit=10))] == []


def test_query_filters_by_time_range(store):
    now = datetime.now(timezone.utc)
    store.append(_record(trigger_id="old", at=now - timedelta(days=10)))
    store.append(_record(trigger_id="recent", at=now))
    items = store.query(
        ActivityQuery(since=now - timedelta(days=5), until=now + timedelta(days=1), limit=10)
    )
    assert [i["trigger_id"] for i in items] == ["recent"]


def test_query_limit_and_cursor_walk_all_pages(store):
    base = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    total = 12
    for i in range(total):
        store.append(_record(trigger_id=f"t{i:02d}", at=base + timedelta(seconds=i)))

    seen: list[str] = []
    after = None
    for _ in range(10):  # 安全上限，防止游标不推进时死循环
        page = store.query(ActivityQuery(limit=5, after=after))
        if not page:
            break
        seen.extend(i["trigger_id"] for i in page)
        last = page[-1]
        after = Cursor(
            created_at=datetime.fromisoformat(last["created_at"]),
            trigger_id=last["trigger_id"],
        )
    # 不重不漏，且严格倒序
    assert seen == [f"t{i:02d}" for i in range(total - 1, -1, -1)]


def test_query_cursor_tie_break_on_same_timestamp(store):
    """时间戳撞车时用 trigger_id 做次级键，翻页不会重复或跳过。"""
    at = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    for tid in ["a", "b", "c", "d"]:
        store.append(_record(trigger_id=tid, at=at))

    first = store.query(ActivityQuery(limit=2))
    assert [i["trigger_id"] for i in first] == ["d", "c"]
    after = Cursor(created_at=at, trigger_id="c")
    second = store.query(ActivityQuery(limit=2, after=after))
    assert [i["trigger_id"] for i in second] == ["b", "a"]


def test_query_skips_only_what_cursor_covered(store):
    """游标之前已返回的记录不会被再次返回，即使文件顺序与时间顺序不一致。"""
    at = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    store.append(_record(trigger_id="newer", at=at + timedelta(minutes=5)))
    store.append(_record(trigger_id="pivot", at=at))
    store.append(_record(trigger_id="older", at=at - timedelta(minutes=5)))
    # 文件追加顺序被打乱后，倒序读依然按时间排序
    store.append(_record(trigger_id="latest", at=at + timedelta(minutes=9)))

    page = store.query(ActivityQuery(limit=10, after=Cursor(created_at=at, trigger_id="pivot")))
    assert [i["trigger_id"] for i in page] == ["older"]


def test_query_reads_only_file_tail(store):
    """取一页只读到文件尾部的一块，而不是整个文件（读取粒度为一个数据块）。"""
    base = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    lines = [
        json.dumps(_record(trigger_id=f"t{i}", at=base + timedelta(seconds=i)))
        for i in range(2000)
    ]
    store._path.write_text("\n".join(lines) + "\n", encoding="utf-8")  # noqa: SLF001
    size = store._path.stat().st_size  # noqa: SLF001 - 测试需要文件大小做对照

    items = store.query(ActivityQuery(limit=2))
    assert [i["trigger_id"] for i in items] == ["t1999", "t1998"]
    assert store.last_scan_bytes <= READ_CHUNK
    assert store.last_scan_bytes < size // 5, f"扫描了 {store.last_scan_bytes} / {size} 字节"


def test_query_skips_corrupt_lines(store):
    store.append(_record(trigger_id="good"))
    with store._path.open("a", encoding="utf-8") as fh:  # noqa: SLF001
        fh.write("{not json\n")
    items = store.query(ActivityQuery(limit=10))
    assert [i["trigger_id"] for i in items] == ["good"]


def test_memory_store_matches_jsonl_semantics():
    """两个后端在同样的数据上给出一致的顺序与过滤结果。"""
    records = [
        _record(trigger_id="a", subscriber="u1"),
        _record(trigger_id="b", subscriber="u2"),
        _record(trigger_id="c", subscriber="u1", status="failed"),
    ]
    mem = MemoryActivityStore(records)
    q = ActivityQuery(subscriber_id="u1", limit=10)
    assert [i["trigger_id"] for i in mem.query(q)] == ["c", "a"]


# --- Postgres 后端：SQL 构造（不连通数据库，只断言下推的谓词） ---


def test_postgres_where_pushes_filters_down():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    since = datetime(2026, 10, 1, tzinfo=timezone.utc)
    where, params = store._build_where(  # noqa: SLF001 - 断言 SQL 构造
        ActivityQuery(
            subscriber_id="u1",
            workflow="post_liked",
            status="sent",
            environment="production",
            since=since,
        )
    )
    sql = " AND ".join(where)
    assert "subscriber_id = :subscriber_id" in sql
    assert "workflow = :workflow" in sql
    assert "status = :status" in sql
    assert "environment = :environment" in sql
    assert "created_at >= :since" in sql
    assert params["subscriber_id"] == "u1"
    assert params["since"] == since


def test_postgres_where_uses_keyset_cursor():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    ts = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
    tid = "3f2a1c1e-0000-4000-8000-000000000001"
    where, params = store._build_where(  # noqa: SLF001
        ActivityQuery(limit=10, after=Cursor(created_at=ts, trigger_id=tid))
    )
    assert where == ["(created_at, trigger_id) < (:cursor_ts, :cursor_id)"]
    assert params["cursor_ts"] == ts
    # 游标里的触发标识按 UUID 传参，才能走 trigger_id 上的索引
    assert str(params["cursor_id"]) == tid


def test_postgres_query_short_circuits_on_non_uuid_trigger_id():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    # 非 UUID 的触发标识不可能命中 UUID 列：直接返回空页，不建立连接
    assert store.query(ActivityQuery(trigger_id="not-a-uuid", limit=5)) == []


def test_postgres_query_rejects_corrupt_cursor():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    with pytest.raises(ValueError):
        store.query(
            ActivityQuery(limit=5, after=Cursor(created_at=datetime.now(timezone.utc), trigger_id="x"))
        )


def test_postgres_to_record_parses_jsonb_channels():
    from app.activity_log import PostgresActivityStore

    record = PostgresActivityStore._to_record(  # noqa: SLF001
        {
            "trigger_id": "3f2a1c1e-0000-4000-8000-000000000002",
            "workflow": "post_liked",
            "subscriber_id": "creator_1",
            "channels": '["in_app", "push"]',
            "status": "triggered",
            "title": "小林赞了你的帖子",
            "environment": "development",
            "created_at": datetime(2026, 10, 20, 12, tzinfo=timezone.utc),
        }
    )
    assert record["channels"] == ["in_app", "push"]
    assert record["trigger_id"] == "3f2a1c1e-0000-4000-8000-000000000002"


# --- 端点 ---


@pytest.fixture
def client(monkeypatch):
    import app.activity_log as activity_log_mod

    base = datetime.now(timezone.utc)
    records = [
        _record(trigger_id="a", workflow="post_commented", subscriber="u1",
                channels=["email"], at=base, title="小林评论了你的帖子"),
        _record(trigger_id="b", workflow="new_follower", subscriber="u2",
                channels=["sms"], at=base - timedelta(minutes=1)),
        _record(trigger_id="c", workflow="post_commented", subscriber="u1",
                channels=["push"], at=base - timedelta(minutes=2)),
        _record(trigger_id="d", workflow="post_liked", subscriber="u1",
                channels=["in_app"], at=base - timedelta(minutes=3)),
        _record(trigger_id="e", workflow="post_liked", subscriber="u1",
                channels=["in_app"], at=base - timedelta(minutes=4)),
    ]
    monkeypatch.setattr(activity_log_mod, "_activity_store", MemoryActivityStore(records))
    return TestClient(app)


def test_activity_endpoint_default(client):
    resp = client.get("/v1/activity")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 5
    assert [i["trigger_id"] for i in data["items"]] == ["a", "b", "c", "d", "e"]
    assert data["hasMore"] is False
    assert data["nextCursor"] is None
    assert data["items"][0]["title"] == "小林评论了你的帖子"


def test_activity_endpoint_filter(client):
    resp = client.get("/v1/activity", params={"subscriberId": "u1", "workflow": "post_commented"})
    data = resp.json()
    assert data["total"] == 2
    assert all(i["subscriber_id"] == "u1" for i in data["items"])
    assert all(i["workflow"] == "post_commented" for i in data["items"])


def test_activity_endpoint_limit_bounds(client):
    # limit 越界由 FastAPI 校验拒绝
    assert client.get("/v1/activity", params={"limit": 0}).status_code == 422
    assert client.get("/v1/activity", params={"limit": 500}).status_code == 422
    assert client.get("/v1/activity", params={"limit": 1}).json()["total"] == 1


def test_activity_endpoint_cursor_pagination(client):
    first = client.get("/v1/activity", params={"limit": 2}).json()
    assert [i["trigger_id"] for i in first["items"]] == ["a", "b"]
    assert first["hasMore"] is True
    assert first["nextCursor"]

    second = client.get(
        "/v1/activity", params={"limit": 2, "cursor": first["nextCursor"]}
    ).json()
    assert [i["trigger_id"] for i in second["items"]] == ["c", "d"]

    third = client.get(
        "/v1/activity", params={"limit": 2, "cursor": second["nextCursor"]}
    ).json()
    assert [i["trigger_id"] for i in third["items"]] == ["e"]
    assert third["hasMore"] is False
    assert third["nextCursor"] is None


def test_activity_endpoint_rejects_bad_cursor(client):
    assert client.get("/v1/activity", params={"cursor": "not-a-cursor"}).status_code == 400
    bogus = json.dumps({"t": "nope", "i": "x"}).encode()
    import base64

    resp = client.get(
        "/v1/activity", params={"cursor": base64.urlsafe_b64encode(bogus).decode()}
    )
    assert resp.status_code == 400
