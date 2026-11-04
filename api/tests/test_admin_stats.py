"""管理统计聚合与接口测试（Phase 5）。

覆盖四层：
- 统计窗口：默认按日历日回溯、``days`` 与 ``since`` 互斥、区间倒置与超长拒绝；
- 聚合口径：跳过不计入成功率分母、渠道按尝试数排序、耗时直方图的分位估算与
  溢出桶、实现/工作流排行、失败原因归并；
- 存储扫描：JSONL 按窗口过滤并跳过损坏行、缺时间戳的记录不落进趋势、内存与
  JSONL 语义一致、Postgres 分支把窗口条件下推并流式读取（本机无实例）；
- 接口：返回结构、参数冲突 400、越界 422。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.admin_stats import (
    DEFAULT_WINDOW_DAYS,
    MAX_WINDOW_DAYS,
    REASON_MAX_CHARS,
    LatencyHistogram,
    WindowError,
    collect,
    reason_key,
    resolve_window,
)
from app.execution_log import (
    JSONLExecutionStore,
    MemoryExecutionStore,
    PostgresExecutionStore,
)
from app.main import app

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def _window(days: int = DEFAULT_WINDOW_DAYS):
    return resolve_window(days=days, now=NOW)


def _rec(
    channel: str = "email",
    status: str = "sent",
    ms: int = 0,
    reason: str | None = None,
    provider: str | None = None,
    workflow: str | None = None,
    at: datetime | None = None,
) -> dict:
    record = {
        "trigger_id": "t1",
        "channel": channel,
        "status": status,
        "duration_ms": ms,
        "created_at": (at or NOW).isoformat(),
    }
    if reason is not None:
        record["reason"] = reason
    if provider is not None:
        record["provider"] = provider
    if workflow is not None:
        record["workflow"] = workflow
    return record


# --- 统计窗口 ---


def test_default_window_covers_seven_calendar_days():
    window = resolve_window(now=NOW)
    assert window.until == NOW
    assert window.since == datetime(2026, 10, 29, tzinfo=timezone.utc)
    assert window.days == 7
    assert len(window.dates()) == 7
    assert window.dates()[0].isoformat() == "2026-10-29"
    assert window.dates()[-1].isoformat() == "2026-11-04"


def test_days_counts_back_from_until():
    until = datetime(2026, 11, 2, 9, 0, tzinfo=timezone.utc)
    window = resolve_window(days=3, until=until, now=NOW)
    # 起点取 (until 日期 - 2 天) 的 UTC 零点，因此是完整的三个日历日
    assert window.since == datetime(2026, 10, 31, tzinfo=timezone.utc)
    assert window.until == until
    assert window.days == 3


def test_explicit_since_and_until_are_kept():
    since = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)
    until = datetime(2026, 11, 3, 8, 0, tzinfo=timezone.utc)
    window = resolve_window(since=since, until=until, now=NOW)
    assert (window.since, window.until) == (since, until)
    assert window.days == 3


def test_naive_datetime_is_treated_as_utc():
    window = resolve_window(
        since=datetime(2026, 11, 1, 8, 0),
        until=datetime(2026, 11, 2, 8, 0),
        now=NOW,
    )
    assert window.since.tzinfo == timezone.utc
    assert window.since.hour == 8


@pytest.mark.parametrize(
    "kwargs",
    [
        {"days": 3, "since": datetime(2026, 11, 1, tzinfo=timezone.utc)},
        {"days": 0},
        {"days": MAX_WINDOW_DAYS + 1},
        {"days": -1},
    ],
)
def test_rejects_conflicting_or_out_of_range_days(kwargs):
    with pytest.raises(WindowError):
        resolve_window(now=NOW, **kwargs)


def test_rejects_inverted_window():
    with pytest.raises(WindowError):
        resolve_window(
            since=datetime(2026, 11, 3, tzinfo=timezone.utc),
            until=datetime(2026, 11, 1, tzinfo=timezone.utc),
            now=NOW,
        )


def test_rejects_overlong_window():
    with pytest.raises(WindowError):
        resolve_window(
            since=datetime(2026, 1, 1, tzinfo=timezone.utc),
            until=datetime(2026, 11, 1, tzinfo=timezone.utc),
            now=NOW,
        )


# --- 聚合口径 ---


def test_empty_window_has_zero_counts():
    stats = collect([], _window())
    assert stats["window"]["days"] == 7
    assert stats["window"]["until"] == NOW.isoformat()
    totals = stats["totals"]
    assert totals["records"] == 0
    assert totals["attempted"] == 0
    assert totals["success_rate"] == 100.0
    assert totals["avg_ms"] == 0.0
    assert totals["p95_ms"] == 0.0
    assert stats["channels"] == []
    assert stats["providers"] == []
    assert stats["workflows"] == []
    assert stats["failure_reasons"] == []
    assert len(stats["trend"]) == 7
    assert sum(item["sent"] + item["failed"] + item["skipped"] for item in stats["trend"]) == 0


def test_channel_breakdown_excludes_skipped_from_rate():
    records = [
        _rec("email", "sent", 10),
        _rec("email", "sent", 20),
        _rec("email", "failed", 30, reason="timeout"),
        _rec("in_app", "sent", 1),
        _rec("in_app", "skipped"),
    ]
    stats = collect(records, _window())

    totals = stats["totals"]
    assert (totals["records"], totals["attempted"], totals["sent"], totals["failed"], totals["skipped"]) == (
        5,
        4,
        3,
        1,
        1,
    )
    assert totals["success_rate"] == 75.0

    channels = {c["channel"]: c for c in stats["channels"]}
    assert channels["email"]["attempted"] == 3
    assert channels["email"]["success_rate"] == 66.7
    # 被偏好拦下的渠道：计数保留、不进成功率分母
    assert channels["in_app"]["attempted"] == 1
    assert channels["in_app"]["skipped"] == 1
    assert channels["in_app"]["success_rate"] == 100.0
    # 尝试数多的渠道排在前面
    assert [c["channel"] for c in stats["channels"]] == ["email", "in_app"]


def test_channels_with_same_attempts_sort_by_name():
    stats = collect([_rec("sms", "sent"), _rec("email", "sent")], _window())
    assert [c["channel"] for c in stats["channels"]] == ["email", "sms"]


def test_latency_percentile_and_average():
    records = [_rec("email", "sent", ms) for ms in (10, 10, 10, 10, 1000)]
    stats = collect(records, _window())
    email = stats["channels"][0]
    assert email["avg_ms"] == 208.0
    # 5 条样本的 p95 落在 (500, 1000] 桶内，按桶内线性插值估算
    assert email["p95_ms"] == 875.0
    assert email["max_ms"] == 1000.0
    assert stats["totals"]["p95_ms"] == 875.0


def test_histogram_percentile_never_exceeds_max():
    """桶上界高于样本时，插值结果按实测最大值收口。"""
    histogram = LatencyHistogram()
    for ms in (10, 10, 10, 10, 130):
        histogram.add(ms)
    assert histogram.max_ms == 130.0
    assert histogram.percentile(95) == 130.0


def test_histogram_overflow_reports_actual_max():
    histogram = LatencyHistogram()
    for ms in (5, 5, 5, 20000):
        histogram.add(ms)
    assert histogram.max_ms == 20000.0
    assert histogram.percentile(99) == 20000.0
    assert histogram.percentile(100) == 20000.0


def test_histogram_ignores_invalid_durations():
    histogram = LatencyHistogram()
    histogram.add(None)
    histogram.add("oops")
    histogram.add(-5)
    histogram.add(3)
    assert histogram.count == 4
    assert histogram.average == 0.75
    assert histogram.max_ms == 3.0


def test_skipped_records_do_not_affect_latency():
    stats = collect([_rec("in_app", "skipped", ms=999)], _window())
    assert stats["channels"][0]["max_ms"] == 0.0
    assert stats["totals"]["avg_ms"] == 0.0


def test_provider_and_workflow_rankings():
    records = [
        _rec("email", "sent", provider="console-email", workflow="post_commented"),
        _rec("email", "failed", reason="550 拒收", provider="console-email", workflow="post_commented"),
        _rec("sms", "sent", provider="console-sms", workflow="post_liked"),
        # API 侧写入的记录没有实现与工作流字段，不进这两张榜单
        _rec("push", "sent"),
    ]
    stats = collect(records, _window())

    providers = {p["provider"]: p for p in stats["providers"]}
    assert providers["console-email"]["records"] == 2
    assert providers["console-email"]["failed"] == 1
    assert providers["console-email"]["success_rate"] == 50.0
    assert stats["providers"][0]["provider"] == "console-email"

    workflows = {w["workflow"]: w for w in stats["workflows"]}
    assert workflows["post_commented"]["records"] == 2
    assert workflows["post_liked"]["records"] == 1
    assert "push" not in workflows


def test_rankings_respect_top_limit():
    records = [_rec("email", "sent", provider=f"p{i}", workflow=f"w{i}") for i in range(8)]
    stats = collect(records, _window(), top=3)
    assert len(stats["providers"]) == 3
    assert len(stats["workflows"]) == 3
    # 渠道明细不受 top 限制：它是报表主体，不是榜单
    assert len(stats["channels"]) == 1


def test_failure_reasons_merge_whitespace_and_truncate():
    long_reason = "收件人不存在 " * 40
    records = [
        _rec("email", "failed", reason="SMTP   timeout\n while sending"),
        _rec("email", "failed", reason="SMTP timeout while sending"),
        _rec("email", "failed", reason=long_reason),
        _rec("email", "failed", reason="   "),
        _rec("email", "sent"),
    ]
    reasons = collect(records, _window())["failure_reasons"]
    assert reasons[0] == {"reason": "SMTP timeout while sending", "count": 2}
    assert len(reasons) == 2
    assert reasons[1]["reason"].endswith("…")
    assert len(reasons[1]["reason"]) == REASON_MAX_CHARS


def test_reason_key_normalizes():
    assert reason_key("  a \n\t b  ") == "a b"
    assert reason_key("   ") is None
    assert reason_key(None) is None
    assert reason_key(42) is None


def test_unknown_status_counts_as_failure():
    stats = collect([_rec("email", "queued", ms=5)], _window())
    assert stats["totals"]["failed"] == 1
    assert stats["totals"]["sent"] == 0
    assert stats["channels"][0]["failed"] == 1


def test_trend_groups_by_day_and_status():
    yesterday = NOW - timedelta(days=1)
    records = [
        _rec("email", "sent", at=NOW),
        _rec("email", "failed", reason="x", at=NOW),
        _rec("in_app", "sent", at=yesterday),
    ]
    trend = collect(records, _window())["trend"]
    assert trend[-1] == {"date": "2026-11-04", "sent": 1, "failed": 1, "skipped": 0}
    assert trend[-2]["sent"] == 1
    assert sum(item["failed"] for item in trend) == 1
    # 缺失的中间日期补零，趋势不会少一天
    assert [item["date"] for item in trend][0] == "2026-10-29"


# --- 存储扫描 ---


def test_jsonl_scan_filters_by_window_and_skips_corrupt_lines(tmp_path):
    path = tmp_path / "executions.jsonl"
    store = JSONLExecutionStore(path=path)
    store.append(_rec("email", "sent", at=NOW - timedelta(days=10)))
    store.append(_rec("email", "sent", at=NOW - timedelta(days=1)))
    store.append(_rec("sms", "failed", reason="x", at=NOW))
    with path.open("a", encoding="utf-8") as fh:
        fh.write("{not json}\n")

    window = _window()
    rows = list(store.scan(since=window.since, until=window.until))
    assert [r["channel"] for r in rows] == ["email", "sms"]
    # 不给窗口就是全量（损坏行仍然跳过）
    assert len(list(store.scan())) == 3


def test_jsonl_scan_skips_records_without_timestamp_when_windowed(tmp_path):
    path = tmp_path / "executions.jsonl"
    store = JSONLExecutionStore(path=path)
    store.append(_rec("email", "sent"))
    store.append({"trigger_id": "t2", "channel": "sms", "status": "sent"})
    window = _window()
    assert [r["channel"] for r in store.scan(since=window.since, until=window.until)] == ["email"]
    assert len(list(store.scan())) == 2


def test_memory_scan_matches_jsonl(tmp_path):
    records = [
        _rec("email", "sent", at=NOW - timedelta(days=10)),
        _rec("sms", "failed", reason="x", at=NOW),
    ]
    path = tmp_path / "executions.jsonl"
    jsonl = JSONLExecutionStore(path=path)
    for record in records:
        jsonl.append(record)
    memory = MemoryExecutionStore(records)

    window = _window()
    assert [r["channel"] for r in memory.scan(since=window.since, until=window.until)] == [
        r["channel"] for r in jsonl.scan(since=window.since, until=window.until)
    ]


def test_postgres_scan_pushes_window_and_streams(monkeypatch):
    captured: dict = {}

    class _Result:
        def mappings(self):
            return []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execution_options(self, **kwargs):
            captured["options"] = kwargs
            return self

        def execute(self, statement, params):
            captured["sql"] = str(statement)
            captured["params"] = params
            return _Result()

    class _Engine:
        def connect(self):
            return _Conn()

    store = PostgresExecutionStore("postgresql+psycopg://ignored")
    monkeypatch.setattr(store, "_get_engine", lambda: _Engine())

    window = _window()
    assert list(store.scan(since=window.since, until=window.until)) == []
    assert "created_at >= :since" in captured["sql"]
    assert "created_at <= :until" in captured["sql"]
    assert "ORDER BY created_at, id" in captured["sql"]
    assert captured["options"]["stream_results"] is True
    assert captured["params"] == {"since": window.since, "until": window.until}


# --- 接口层 ---


WINDOW_PARAMS = {
    "since": "2026-11-01T00:00:00+00:00",
    "until": "2026-11-04T23:59:59+00:00",
}


@pytest.fixture
def client(monkeypatch):
    import app.execution_log as mod

    records = [
        _rec("email", "sent", 10, provider="console-email", workflow="post_commented"),
        _rec("email", "failed", 20, reason="SMTP 550", provider="console-email", workflow="post_commented"),
        _rec("in_app", "sent", 1, provider="console-inapp", workflow="post_liked"),
        _rec("in_app", "skipped"),
    ]
    monkeypatch.setattr(mod, "_execution_store", MemoryExecutionStore(records))
    return TestClient(app)


def test_admin_stats_endpoint_shape(client):
    resp = client.get("/v1/admin/stats", params=WINDOW_PARAMS)
    assert resp.status_code == 200
    data = resp.json()

    assert data["window"]["days"] == 4
    assert data["window"]["since"].startswith("2026-11-01T00:00:00")

    totals = data["totals"]
    assert totals["records"] == 4
    assert totals["attempted"] == 3
    assert totals["sent"] == 2
    assert totals["failed"] == 1
    assert totals["skipped"] == 1
    assert totals["success_rate"] == 66.7

    assert [c["channel"] for c in data["channels"]] == ["email", "in_app"]
    assert data["providers"][0]["provider"] == "console-email"
    assert data["workflows"][0]["workflow"] == "post_commented"
    assert data["failure_reasons"] == [{"reason": "SMTP 550", "count": 1}]
    assert data["trend"][-1]["date"] == "2026-11-04"


def test_admin_stats_endpoint_top_limits_rankings(client):
    data = client.get("/v1/admin/stats", params={**WINDOW_PARAMS, "top": 1}).json()
    assert len(data["providers"]) == 1
    assert len(data["workflows"]) == 1
    assert len(data["channels"]) == 2


def test_admin_stats_endpoint_empty_store(monkeypatch):
    import app.execution_log as mod

    monkeypatch.setattr(mod, "_execution_store", MemoryExecutionStore([]))
    data = TestClient(app).get("/v1/admin/stats", params=WINDOW_PARAMS).json()
    assert data["totals"]["records"] == 0
    assert data["totals"]["success_rate"] == 100.0
    assert data["channels"] == []
    assert len(data["trend"]) == 4


def test_admin_stats_endpoint_default_window(client):
    data = client.get("/v1/admin/stats").json()
    assert data["window"]["days"] == DEFAULT_WINDOW_DAYS
    assert len(data["trend"]) == DEFAULT_WINDOW_DAYS


def test_admin_stats_endpoint_rejects_conflicting_window(client):
    resp = client.get(
        "/v1/admin/stats",
        params={"days": 3, "since": "2026-11-01T00:00:00+00:00"},
    )
    assert resp.status_code == 400
    assert "不能同时" in resp.json()["detail"]


def test_admin_stats_endpoint_rejects_inverted_window(client):
    resp = client.get(
        "/v1/admin/stats",
        params={
            "since": "2026-11-04T00:00:00+00:00",
            "until": "2026-11-01T00:00:00+00:00",
        },
    )
    assert resp.status_code == 400


def test_admin_stats_endpoint_rejects_overlong_window(client):
    resp = client.get(
        "/v1/admin/stats",
        params={
            "since": "2025-01-01T00:00:00+00:00",
            "until": "2026-01-01T00:00:00+00:00",
        },
    )
    assert resp.status_code == 400


def test_admin_stats_endpoint_param_bounds(client):
    assert client.get("/v1/admin/stats", params={"days": 0}).status_code == 422
    assert client.get("/v1/admin/stats", params={"days": MAX_WINDOW_DAYS + 1}).status_code == 422
    assert client.get("/v1/admin/stats", params={"top": 0}).status_code == 422
    assert client.get("/v1/admin/stats", params={"top": 21}).status_code == 422


def test_admin_stats_endpoint_reads_jsonl_window(tmp_path, monkeypatch):
    """接口层与 JSONL 后端连起来跑一遍：窗口外的记录不参与统计。"""
    import app.execution_log as mod

    path = tmp_path / "executions.jsonl"
    store = JSONLExecutionStore(path=path)
    store.append(_rec("email", "sent", 5, provider="console-email", at=NOW))
    store.append(_rec("email", "sent", 5, provider="console-email", at=NOW - timedelta(days=30)))
    monkeypatch.setattr(mod, "_execution_store", store)

    data = TestClient(app).get("/v1/admin/stats", params=WINDOW_PARAMS).json()
    assert data["totals"]["records"] == 1
    assert data["totals"]["sent"] == 1


__all__ = []
