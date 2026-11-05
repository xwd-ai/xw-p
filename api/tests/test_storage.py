"""持久化单元测试。"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from app.storage import JSONLStorage, PostgresStorage


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


# ---------------------------------------------------------------- Postgres 适配
#
# 用假引擎验证 SQL 形状与行映射，不连真库：这里要证明的是「参数有没有拼对」，
# 连上数据库只能证明这台机器装了 PG。真库上的写入与统计口径由集成测试覆盖。


class _Result:
    def __init__(self, rows, rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def mappings(self):
        return self

    def one(self):
        if not self._rows:
            raise AssertionError("期望一行结果")
        return self._rows[0]

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _Conn:
    def __init__(self, engine):
        self._engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self._engine.calls.append((str(statement), dict(params or {})))
        if self._engine.error:
            raise self._engine.error
        # 按调用序号返回预设结果：stats 一次要跑三条查询。
        index = min(len(self._engine.calls) - 1, len(self._engine.results) - 1)
        return self._engine.results[index] if self._engine.results else _Result([])


class _Engine:
    def __init__(self, results=None, error=None):
        self.calls: list[tuple[str, dict]] = []
        self.results = results or []
        self.error = error

    def begin(self):
        return _Conn(self)

    def connect(self):
        return _Conn(self)


def _pg_store(results=None, error=None) -> tuple[PostgresStorage, _Engine]:
    engine = _Engine(results=results, error=error)
    store = PostgresStorage("postgresql+psycopg://u:p@localhost/xw")
    store._engine = engine
    return store, engine


def test_postgres_save_serializes_json_columns():
    store, engine = _pg_store()
    store.save(
        {
            "trigger_id": "t-1",
            "transaction_id": "tx-1",
            "workflow": "post_liked",
            "subscriber_id": "creator_1",
            "channels": ["in_app", "push"],
            "payload": {"post_id": "P-1"},
        }
    )

    sql, params = engine.calls[0]
    assert "INSERT INTO messages" in sql
    assert params["channels"] == '["in_app", "push"]'
    assert json.loads(params["payload"]) == {"post_id": "P-1"}
    # 缺省状态按已发送落库，与调用方的字段顺序无关
    assert params["status"] == "sent"


def test_postgres_read_all_parses_json_columns():
    rows = [
        {
            "trigger_id": "t-1",
            "transaction_id": "tx-1",
            "workflow": "post_liked",
            "subscriber_id": "creator_1",
            "channels": '["in_app"]',
            "payload": '{"post_id":"P-1"}',
            "created_at": None,
        },
        {
            "trigger_id": "t-2",
            "transaction_id": "tx-2",
            "workflow": "post_commented",
            "subscriber_id": "creator_1",
            "channels": ["email"],
            "payload": None,
            "created_at": None,
        },
    ]
    store, _ = _pg_store(results=[_Result(rows)])
    records = store.read_all()

    assert records[0]["channels"] == ["in_app"]
    assert records[0]["payload"] == {"post_id": "P-1"}
    assert records[0]["status"] == "sent"
    assert records[1]["payload"] == {}
    assert records[1]["channels"] == ["email"]


def test_postgres_stats_aggregates_three_queries():
    totals = _Result([{"total": 10, "sent": 7, "skipped": 1}])
    channels = _Result([{"channel": "in_app", "n": 6}, {"channel": "email", "n": 4}])
    days = _Result([{"day": date(2026, 11, 5), "n": 3}])
    store, engine = _pg_store(results=[totals, channels, days])

    stats = store.stats(trend_days=7)

    assert stats["total"] == 10
    assert stats["skipped"] == 1
    assert stats["channel_distribution"] == {"in_app": 6, "email": 4}
    # 分母只用真正尝试过投递的条数：9 条尝试里成功 7 条
    assert stats["success_rate"] == 77.8
    assert len(stats["trend"]) == 7
    assert stats["trend"][-1] == {"date": "2026-11-05", "count": 3}
    assert len(engine.calls) == 3


def test_postgres_revision_uses_table_stats_then_falls_back():
    # 这条查询走的是位置索引（row[0..2]），不是 mapping
    row = _Result([(3, 1, 0)])
    store, _ = _pg_store(results=[row])
    assert store.revision() == "pg:3:1:0:0"

    broken = RuntimeError("统计视图不可用")
    store, _ = _pg_store(results=[_Result([])], error=broken)
    # 读不到统计时退化为进程内写入计数，不让版本探测把读取带崩
    assert store.revision() == "pg:0"


def test_postgres_ready_checks_connection(monkeypatch):
    created = {}

    class _Factory:
        def __init__(self, url, **kwargs):
            created["url"] = url
            created["kwargs"] = kwargs

        def connect(self):
            return _Conn(_Engine())

        def dispose(self):
            created["disposed"] = True

    monkeypatch.setattr("sqlalchemy.create_engine", _Factory)
    store = PostgresStorage("postgresql+psycopg://u:p@localhost/xw")
    assert store.ready() == "postgres 连接正常"
    # 连接与语句都要有超时，否则数据库半死不活时探测自己会被挂住
    assert created["kwargs"]["connect_args"]["connect_timeout"] == 2
    assert "statement_timeout=2000" in created["kwargs"]["connect_args"]["options"]
    assert created["disposed"] is True


def test_postgres_ready_skips_pg_options_for_other_dialects(monkeypatch):
    created = {}

    class _Factory:
        def __init__(self, url, **kwargs):
            created["kwargs"] = kwargs

        def connect(self):
            return _Conn(_Engine())

        def dispose(self):
            pass

    monkeypatch.setattr("sqlalchemy.create_engine", _Factory)
    PostgresStorage("sqlite:///tmp/xw.db").ready()
    assert "connect_args" not in created["kwargs"]


def test_postgres_ready_propagates_connection_error(monkeypatch):
    class _Factory:
        def __init__(self, url, **kwargs):
            pass

        def connect(self):
            raise ConnectionError("拒绝连接")

        def dispose(self):
            pass

    monkeypatch.setattr("sqlalchemy.create_engine", _Factory)
    with pytest.raises(ConnectionError):
        PostgresStorage("postgresql+psycopg://u:p@localhost/xw").ready()
