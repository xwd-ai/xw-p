"""消息执行明细存储与查询 API 测试（Phase 1 / R6）。

覆盖三层：
- 后端：JSONL 倒序读、过滤下推、游标翻页（时间戳撞车时用记录标识做次级键）、
  尾部读取、损坏行跳过；内存后端与 JSONL 语义一致；Postgres 分支只断言 SQL
  谓词构造（本机无实例）。
- 接口：``GET /v1/executions`` 的返回结构、过滤、游标翻页与非法游标。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.execution_log import (
    ExecutionCursor,
    ExecutionQuery,
    JSONLExecutionStore,
    MemoryExecutionStore,
    record_execution,
)
from app.jsonl_reader import READ_CHUNK
from app.main import app
from app.timeutil import parse_dt


def _rec(trigger_id="t1", channel="email", status="sent", reason=None,
         duration_ms=0, created_at="2026-10-22T12:00:00+00:00", rid=None):
    record = {
        "trigger_id": trigger_id,
        "channel": channel,
        "status": status,
        "reason": reason,
        "duration_ms": duration_ms,
        "created_at": created_at,
    }
    if rid is not None:
        record["id"] = rid
    return record


# --- JSONL 后端：写入与倒序读 ---


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
    # 写入时补上记录标识，供游标做次级键
    assert all(r["id"] for r in records)


def test_jsonl_execution_store_empty_when_missing(tmp_path):
    store = JSONLExecutionStore(path=tmp_path / "nope.jsonl")
    assert store.read_all() == []
    assert store.query(ExecutionQuery(limit=10)) == []


def test_record_execution_writes_and_returns(monkeypatch):
    captured = {}

    class _Store:
        def append(self, record):
            captured.update(record)

        def query(self, q):
            return []

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
    assert out["id"]
    assert captured["trigger_id"] == "t9"


@pytest.fixture
def store(tmp_path):
    return JSONLExecutionStore(path=tmp_path / "executions.jsonl")


def test_query_returns_newest_first(store):
    base = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
    for i, rid in enumerate(["old", "mid", "new"]):
        store.append(_rec(rid=rid, created_at=(base + timedelta(minutes=i)).isoformat()))
    items = store.query(ExecutionQuery(limit=10))
    assert [i["id"] for i in items] == ["new", "mid", "old"]


def test_query_filters_by_trigger_channel_status(store):
    store.append(_rec(trigger_id="a", channel="in_app", rid="r1"))
    store.append(_rec(trigger_id="a", channel="email", rid="r2"))
    store.append(_rec(trigger_id="b", channel="push", status="failed", reason="timeout", rid="r3"))

    assert [i["id"] for i in store.query(ExecutionQuery(trigger_id="a", limit=10))] == ["r2", "r1"]
    assert [i["id"] for i in store.query(ExecutionQuery(trigger_id="a", channel="email", limit=10))] == ["r2"]
    assert [i["id"] for i in store.query(ExecutionQuery(status="failed", limit=10))] == ["r3"]
    assert store.query(ExecutionQuery(trigger_id="zzz", limit=10)) == []


def test_query_filters_by_time_range(store):
    now = datetime.now(timezone.utc)
    store.append(_rec(rid="old", created_at=(now - timedelta(days=10)).isoformat()))
    store.append(_rec(rid="recent", created_at=now.isoformat()))
    items = store.query(
        ExecutionQuery(since=now - timedelta(days=5), until=now + timedelta(days=1), limit=10)
    )
    assert [i["id"] for i in items] == ["recent"]


def test_query_limit_and_cursor_walk_all_pages(store):
    base = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
    total = 12
    for i in range(total):
        store.append(_rec(rid=f"r{i:02d}", created_at=(base + timedelta(seconds=i)).isoformat()))

    seen: list[str] = []
    after = None
    for _ in range(10):  # 安全上限，防止游标不推进时死循环
        page = store.query(ExecutionQuery(limit=5, after=after))
        if not page:
            break
        seen.extend(i["id"] for i in page)
        last = page[-1]
        after = ExecutionCursor(
            created_at=datetime.fromisoformat(last["created_at"]),
            key=last["id"],
        )
    assert seen == [f"r{i:02d}" for i in range(total - 1, -1, -1)]


def test_query_cursor_tie_break_on_same_timestamp(store):
    """同一次触发的多条渠道明细几乎同时写入，靠记录标识做次级键翻页。"""
    at = "2026-10-22T12:00:00+00:00"
    for i, rid in enumerate(["a", "b", "c", "d"]):
        store.append(_rec(channel=f"ch{i}", rid=rid, created_at=at))

    first = store.query(ExecutionQuery(limit=2))
    assert [i["id"] for i in first] == ["d", "c"]
    second = store.query(
        ExecutionQuery(limit=2, after=ExecutionCursor(created_at=datetime.fromisoformat(at), key="c"))
    )
    assert [i["id"] for i in second] == ["b", "a"]


def test_query_cursor_skips_only_covered_records(store):
    """游标之前已返回的记录不会被再次返回，即使文件写入顺序与时间顺序不一致。"""
    at = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
    store.append(_rec(rid="newer", created_at=(at + timedelta(minutes=5)).isoformat()))
    store.append(_rec(rid="pivot", created_at=at.isoformat()))
    store.append(_rec(rid="older", created_at=(at - timedelta(minutes=5)).isoformat()))
    store.append(_rec(rid="latest", created_at=(at + timedelta(minutes=9)).isoformat()))

    page = store.query(ExecutionQuery(limit=10, after=ExecutionCursor(created_at=at, key="pivot")))
    assert [i["id"] for i in page] == ["older"]


def test_query_reads_only_file_tail(store):
    """取一页只读到文件尾部的一块，而不是整个文件。"""
    base = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
    lines = [
        json.dumps(_rec(rid=f"r{i}", created_at=(base + timedelta(seconds=i)).isoformat()))
        for i in range(2000)
    ]
    store._path.write_text("\n".join(lines) + "\n", encoding="utf-8")  # noqa: SLF001
    size = store._path.stat().st_size  # noqa: SLF001 - 测试需要文件大小做对照

    items = store.query(ExecutionQuery(limit=2))
    assert [i["id"] for i in items] == ["r1999", "r1998"]
    # 文件有 4 个块以上，取两页只读了一个块
    assert size > 4 * READ_CHUNK
    assert store.last_scan_bytes <= READ_CHUNK


def test_query_skips_corrupt_lines(store):
    store.append(_rec(rid="good"))
    with store._path.open("a", encoding="utf-8") as fh:  # noqa: SLF001
        fh.write("{not json\n")
    items = store.query(ExecutionQuery(limit=10))
    assert [i["id"] for i in items] == ["good"]


def test_memory_store_matches_jsonl_semantics():
    records = [
        _rec(trigger_id="a", channel="in_app", rid="r1"),
        _rec(trigger_id="b", channel="push", rid="r2"),
        _rec(trigger_id="a", channel="email", rid="r3"),
    ]
    mem = MemoryExecutionStore(records)
    assert [i["id"] for i in mem.query(ExecutionQuery(trigger_id="a", limit=10))] == ["r3", "r1"]
    assert [i["id"] for i in mem.query(ExecutionQuery(limit=1))] == ["r3"]


# --- Postgres 后端：SQL 构造（不连通数据库，只断言下推的谓词） ---


def test_postgres_where_pushes_filters_down():
    from app.execution_log import PostgresExecutionStore

    store = PostgresExecutionStore("postgresql+psycopg://ignored")
    since = datetime(2026, 10, 1, tzinfo=timezone.utc)
    where, params = store._build_where(  # noqa: SLF001 - 断言 SQL 构造
        ExecutionQuery(
            trigger_id="3f2a1c1e-0000-4000-8000-000000000001",
            channel="in_app",
            status="failed",
            since=since,
        )
    )
    sql = " AND ".join(where)
    assert "trigger_id = :trigger_id" in sql
    assert "channel = :channel" in sql
    assert "status = :status" in sql
    assert "created_at >= :since" in sql
    assert params["channel"] == "in_app"
    assert params["since"] == since
    assert str(params["trigger_id"]) == "3f2a1c1e-0000-4000-8000-000000000001"


def test_postgres_where_uses_keyset_cursor():
    from app.execution_log import PostgresExecutionStore

    store = PostgresExecutionStore("postgresql+psycopg://ignored")
    ts = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
    where, params = store._build_where(  # noqa: SLF001
        ExecutionQuery(limit=10, after=ExecutionCursor(created_at=ts, key="42"))
    )
    assert where == ["(created_at, id) < (:cursor_ts, :cursor_id)"]
    assert params["cursor_ts"] == ts
    # 自增主键按整数传参，才能走 (created_at, id) 上的复合索引
    assert params["cursor_id"] == 42


def test_postgres_query_short_circuits_on_non_uuid_trigger_id():
    from app.execution_log import PostgresExecutionStore

    store = PostgresExecutionStore("postgresql+psycopg://ignored")
    assert store.query(ExecutionQuery(trigger_id="not-a-uuid", limit=5)) == []


def test_postgres_query_rejects_corrupt_cursor():
    from app.execution_log import PostgresExecutionStore

    store = PostgresExecutionStore("postgresql+psycopg://ignored")
    with pytest.raises(ValueError):
        store.query(
            ExecutionQuery(
                limit=5,
                after=ExecutionCursor(created_at=datetime.now(timezone.utc), key="not-an-int"),
            )
        )


# --- 接口层 ---


@pytest.fixture
def client(monkeypatch):
    import app.execution_log as mod

    records = [
        _rec(trigger_id="a", channel="email", status="sent", rid="r1"),
        _rec(trigger_id="a", channel="sms", status="sent", rid="r2"),
        _rec(trigger_id="b", channel="push", status="failed", reason="timeout", rid="r3"),
    ]
    monkeypatch.setattr(mod, "_execution_store", MemoryExecutionStore(records))
    return TestClient(app)


def test_executions_endpoint_default(client):
    resp = client.get("/v1/executions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3
    assert data["hasMore"] is False
    assert data["nextCursor"] is None
    # 最新在前：三条同一时间戳，按记录标识倒序
    assert [i["id"] for i in data["items"]] == ["r3", "r2", "r1"]


def test_executions_endpoint_filter(client):
    resp = client.get("/v1/executions", params={"triggerId": "a"})
    data = resp.json()
    assert data["total"] == 2
    assert all(i["trigger_id"] == "a" for i in data["items"])

    resp = client.get("/v1/executions", params={"channel": "push", "status": "failed"})
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["reason"] == "timeout"


def test_executions_endpoint_cursor_pagination(client):
    seen: list[str] = []
    cursor = None
    for _ in range(5):
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        data = client.get("/v1/executions", params=params).json()
        seen.extend(i["id"] for i in data["items"])
        cursor = data["nextCursor"]
        if not cursor:
            break
    assert seen == ["r3", "r2", "r1"]
    assert data["hasMore"] is False


def test_executions_endpoint_rejects_bad_cursor(client):
    resp = client.get("/v1/executions", params={"cursor": "not-a-cursor"})
    assert resp.status_code == 400


def test_executions_endpoint_limit_bounds(client):
    assert client.get("/v1/executions", params={"limit": 0}).status_code == 422
    assert client.get("/v1/executions", params={"limit": 201}).status_code == 422


__all__ = []


# --- 引擎回写的记录 ---

# 引擎（engine/internal/execution）写出的一行：时间戳固定六位小数 + 显式偏移，
# 另带步骤、实现标识、尝试序列与回退标记。这里是硬编码的样本，用来锁住
# 跨语言的行格式：引擎改了序列化方式，这条用例会先失败。
ENGINE_LINE = (
    '{"id":"6f1c4d2a9b7e4f0c8a1b2c3d4e5f6071","trigger_id":"t-9","workflow":"post_commented",'
    '"subscriber_id":"creator_1","step_id":"notify","channel":"email","status":"failed",'
    '"reason":"SMTP 收件人 a@b 失败（550）：no such user","provider":"console-email",'
    '"attempts":["smtp-email","console-email"],"fallback":true,"duration_ms":128,'
    '"created_at":"2026-11-02T10:09:12.123456+00:00"}'
)


def test_engine_line_exposes_writeback_fields(tmp_path, monkeypatch):
    """引擎写的行能被读出来，且投递结果与排查字段都出现在接口返回里。"""
    import app.execution_log as mod

    path = tmp_path / "executions.jsonl"
    path.write_text(ENGINE_LINE + "\n", encoding="utf-8")
    monkeypatch.setattr(mod, "_execution_store", JSONLExecutionStore(path=path))

    data = TestClient(app).get("/v1/executions").json()
    assert data["total"] == 1
    item = data["items"][0]
    assert item["status"] == "failed"
    assert item["reason"] == "SMTP 收件人 a@b 失败（550）：no such user"
    assert item["duration_ms"] == 128
    assert item["step_id"] == "notify"
    assert item["provider"] == "console-email"
    assert item["attempts"] == ["smtp-email", "console-email"]
    assert item["fallback"] is True
    # 时间戳格式必须能被 API 侧解析，否则游标翻页会提前结束。
    assert parse_dt(item["created_at"]) is not None


def test_item_defaults_for_records_without_writeback_fields():
    """API 自己写的记录没有排查字段：缺失时回空值，不省略键。"""
    from app.execution_routes import _to_item

    item = _to_item(_rec(trigger_id="t1", channel="sms"))
    assert item["step_id"] is None
    assert item["provider"] is None
    assert item["attempts"] == []
    assert item["fallback"] is False
