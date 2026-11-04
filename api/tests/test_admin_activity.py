"""管理端活动日志查询测试：过滤校验、汇总聚合、分页与下钻。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.activity_log import ActivityQuery, JSONLActivityStore, MemoryActivityStore
from app.admin_activity import (
    ActivitySummaryAccumulator,
    FilterError,
    MAX_KEYWORD_CHARS,
    check_window,
    normalize_channel,
    normalize_environment,
    normalize_keyword,
    normalize_status,
)
from app.execution_log import MemoryExecutionStore
from app.main import app
from app.timeutil import Cursor


def _record(
    trigger_id="t1",
    workflow="post_commented",
    subscriber="creator_1",
    channels=None,
    status="triggered",
    at=None,
    environment="development",
    title=None,
    payload=None,
):
    record = {
        "trigger_id": trigger_id,
        "workflow": workflow,
        "subscriber_id": subscriber,
        "channels": channels if channels is not None else ["in_app"],
        "status": status,
        "environment": environment,
        "created_at": (at or datetime.now(timezone.utc)).isoformat(),
        "title": title or f"{workflow} 通知",
    }
    if payload is not None:
        record["payload"] = payload
    return record


def _execution(trigger_id="t1", channel="in_app", status="sent", duration_ms=12, at=None, reason=None):
    return {
        "id": f"{trigger_id}-{channel}",
        "trigger_id": trigger_id,
        "channel": channel,
        "status": status,
        "reason": reason,
        "duration_ms": duration_ms,
        "created_at": (at or datetime.now(timezone.utc)).isoformat(),
    }


# --- 过滤词校验 ---


def test_normalize_environment_allows_blank_as_all():
    assert normalize_environment(None) is None
    assert normalize_environment("   ") is None
    assert normalize_environment("Production") == "production"


def test_normalize_environment_rejects_unknown():
    with pytest.raises(FilterError) as exc:
        normalize_environment("staging")
    assert "development" in str(exc.value)


def test_normalize_status_case_insensitive():
    assert normalize_status("FAILED") == "failed"
    assert normalize_status("") is None


def test_normalize_status_rejects_typo():
    with pytest.raises(FilterError) as exc:
        normalize_status("faild")
    assert "triggered" in str(exc.value)


def test_normalize_channel_accepts_delivery_channels_only():
    assert normalize_channel("IN_APP") == "in_app"
    # delay / digest 是编排步骤，接受它们只会得到永远为空的查询
    with pytest.raises(FilterError):
        normalize_channel("digest")


def test_normalize_keyword_strips_and_limits():
    assert normalize_keyword("  小林  ") == "小林"
    assert normalize_keyword("   ") is None
    with pytest.raises(FilterError):
        normalize_keyword("x" * (MAX_KEYWORD_CHARS + 1))


def test_check_window_rejects_inverted_range():
    now = datetime.now(timezone.utc)
    check_window(now - timedelta(days=1), now)
    with pytest.raises(FilterError):
        check_window(now, now - timedelta(days=1))


# --- 存储层：渠道与关键词过滤 ---


@pytest.fixture
def jsonl_store(tmp_path):
    return JSONLActivityStore(path=tmp_path / "activity.jsonl")


def test_query_filters_by_channel(jsonl_store):
    jsonl_store.append(_record(trigger_id="a", channels=["in_app", "email"]))
    jsonl_store.append(_record(trigger_id="b", channels=["push"]))
    assert [r["trigger_id"] for r in jsonl_store.query(ActivityQuery(channel="email"))] == ["a"]
    assert jsonl_store.query(ActivityQuery(channel="sms")) == []


def test_query_filters_by_keyword_across_fields(jsonl_store):
    jsonl_store.append(_record(trigger_id="a", title="小林评论了你的帖子", subscriber="creator_1"))
    jsonl_store.append(_record(trigger_id="b", title="有人赞了你", workflow="post_liked",
                               subscriber="creator_2"))
    # 标题命中
    assert [r["trigger_id"] for r in jsonl_store.query(ActivityQuery(keyword="小林"))] == ["a"]
    # 工作流命中，大小写不敏感
    assert [r["trigger_id"] for r in jsonl_store.query(ActivityQuery(keyword="POST_LIKED"))] == ["b"]
    # 订阅者标识命中
    assert [r["trigger_id"] for r in jsonl_store.query(ActivityQuery(keyword="creator_2"))] == ["b"]


def test_query_combines_channel_keyword_and_status(jsonl_store):
    jsonl_store.append(_record(trigger_id="a", channels=["email"], status="sent", title="评论提醒"))
    jsonl_store.append(_record(trigger_id="b", channels=["email"], status="failed", title="评论提醒"))
    rows = jsonl_store.query(ActivityQuery(channel="email", keyword="评论", status="failed"))
    assert [r["trigger_id"] for r in rows] == ["b"]


def test_scan_returns_all_matching_records_in_file_order(jsonl_store):
    base = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    for i in range(5):
        jsonl_store.append(
            _record(trigger_id=f"t{i}", channels=["email"] if i % 2 else ["push"],
                    at=base + timedelta(minutes=i))
        )
    scanned = [r["trigger_id"] for r in jsonl_store.scan(ActivityQuery(channel="email"))]
    assert scanned == ["t1", "t3"]


def test_scan_ignores_limit_and_skips_corrupt_lines(jsonl_store):
    for i in range(3):
        jsonl_store.append(_record(trigger_id=f"t{i}"))
    with jsonl_store._path.open("a", encoding="utf-8") as fh:  # noqa: SLF001
        fh.write("{broken\n")
    rows = list(jsonl_store.scan(ActivityQuery(limit=1)))
    assert [r["trigger_id"] for r in rows] == ["t0", "t1", "t2"]


def test_scan_on_missing_file_is_empty(jsonl_store):
    assert list(jsonl_store.scan(ActivityQuery())) == []


def test_memory_scan_matches_jsonl_semantics():
    records = [
        _record(trigger_id="a", channels=["in_app"]),
        _record(trigger_id="b", channels=["sms"], status="failed"),
    ]
    rows = list(MemoryActivityStore(records).scan(ActivityQuery(channel="sms")))
    assert [r["trigger_id"] for r in rows] == ["b"]


# --- Postgres 后端：SQL 构造 ---


def test_postgres_where_pushes_channel_as_jsonb_containment():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    where, params = store._build_where(ActivityQuery(channel="email"))  # noqa: SLF001
    assert "channels @> CAST(:channel AS jsonb)" in " AND ".join(where)
    assert json.loads(params["channel"]) == ["email"]


def test_postgres_where_escapes_like_wildcards():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    _, params = store._build_where(ActivityQuery(keyword="100%_done"))  # noqa: SLF001
    assert params["keyword"] == r"%100\%\_done%"


def test_postgres_scan_omits_cursor_predicate():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    where, params = store._build_where(  # noqa: SLF001
        ActivityQuery(limit=10, after=None), include_cursor=False
    )
    assert where == []
    assert "cursor_ts" not in params

    ts = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
    cursor = Cursor(created_at=ts, trigger_id="3f2a1c1e-0000-4000-8000-000000000001")
    where_with_cursor, params_with_cursor = store._build_where(  # noqa: SLF001
        ActivityQuery(limit=10, after=cursor)
    )
    assert "cursor_ts" in " ".join(where_with_cursor)
    assert str(params_with_cursor["cursor_id"]) == cursor.trigger_id


def test_postgres_scan_short_circuits_non_uuid_trigger():
    from app.activity_log import PostgresActivityStore

    store = PostgresActivityStore("postgresql+psycopg://ignored")
    assert list(store.scan(ActivityQuery(trigger_id="not-a-uuid"))) == []


# --- 汇总聚合 ---


def test_summary_counts_status_workflow_channel_environment():
    base = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    records = [
        _record(trigger_id="a", workflow="post_liked", channels=["in_app", "push"],
                status="sent", environment="production"),
        _record(trigger_id="b", workflow="post_liked", channels=["in_app"], status="failed"),
        _record(trigger_id="c", workflow="post_commented", channels=["email"],
                status="triggered", at=base),
    ]
    result = ActivitySummaryAccumulator().extend(records).result()

    assert result["totals"]["records"] == 3
    assert result["totals"]["byStatus"] == {
        "triggered": 1, "sent": 1, "failed": 1, "skipped": 0,
    }
    # 工作流按条数降序，失败数单独给出
    assert result["workflows"][0] == {"workflow": "post_liked", "total": 2, "failed": 1}
    # 一条记录声明两个渠道时，两个渠道各记一次
    assert {"channel": "in_app", "total": 2} in result["channels"]
    assert {"channel": "push", "total": 1} in result["channels"]
    assert {"environment": "development", "total": 2} in result["environments"]
    assert {"environment": "production", "total": 1} in result["environments"]
    assert result["subscribers"] == {"unique": 1, "capped": False}


def test_summary_buckets_unknown_status_and_skips_missing_subscriber():
    result = ActivitySummaryAccumulator().extend(
        [
            _record(trigger_id="a", status="weird"),
            _record(trigger_id="b", status="skipped"),
        ]
    ).result()
    assert result["totals"]["byStatus"]["other"] == 1
    assert result["totals"]["byStatus"]["skipped"] == 1

    # 缺订阅者标识的记录不进去重集合
    anonymous = ActivitySummaryAccumulator().extend(
        [_record(trigger_id="c", subscriber=None)]
    ).result()
    assert anonymous["totals"]["records"] == 1
    assert anonymous["subscribers"]["unique"] == 0


def test_summary_range_tracks_first_and_last_timestamp():
    base = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
    result = ActivitySummaryAccumulator().extend(
        [
            _record(trigger_id="a", at=base + timedelta(hours=2)),
            _record(trigger_id="b", at=base),
            _record(trigger_id="c", at=base + timedelta(hours=1)),
        ]
    ).result()
    assert result["range"]["firstAt"] == base.isoformat()
    assert result["range"]["lastAt"] == (base + timedelta(hours=2)).isoformat()


def test_summary_ignores_unparsable_timestamp_for_range():
    record = _record(trigger_id="a")
    record["created_at"] = "not-a-timestamp"
    result = ActivitySummaryAccumulator().extend([record]).result()
    assert result["totals"]["records"] == 1
    assert result["range"] == {"firstAt": None, "lastAt": None}


def test_summary_top_truncates_and_sorts_by_count_then_name():
    records = [_record(trigger_id=f"t{i}", workflow=wf) for i, wf in enumerate(
        ["post_liked", "post_liked", "post_mentioned", "post_commented", "post_commented"]
    )]
    result = ActivitySummaryAccumulator().extend(records).result(top=2)
    assert [w["workflow"] for w in result["workflows"]] == ["post_commented", "post_liked"]


def test_summary_caps_unique_subscribers(monkeypatch):
    import app.admin_activity as mod

    monkeypatch.setattr(mod, "MAX_UNIQUE_SUBSCRIBERS", 2)
    result = ActivitySummaryAccumulator().extend(
        [_record(trigger_id=f"t{i}", subscriber=f"u{i}") for i in range(4)]
    ).result()
    assert result["subscribers"] == {"unique": 2, "capped": True}


def test_summary_on_empty_set_has_zeroed_shape():
    result = ActivitySummaryAccumulator().result()
    assert result["totals"]["records"] == 0
    assert result["totals"]["byStatus"]["failed"] == 0
    assert result["workflows"] == [] and result["channels"] == []
    assert result["subscribers"] == {"unique": 0, "capped": False}


# --- 接口 ---


@pytest.fixture
def client(monkeypatch):
    import app.activity_log as activity_log_mod
    import app.execution_log as execution_log_mod

    base = datetime.now(timezone.utc)
    records = [
        _record(trigger_id="a", workflow="post_commented", subscriber="creator_1",
                channels=["in_app", "email"], status="sent", at=base,
                title="小林评论了你的帖子", payload={"post_id": "p1", "actor": "小林"}),
        _record(trigger_id="b", workflow="post_liked", subscriber="creator_2",
                channels=["push"], status="failed", at=base - timedelta(minutes=1),
                environment="production", title="有人赞了你的帖子"),
        _record(trigger_id="c", workflow="post_commented", subscriber="creator_1",
                channels=["email"], status="triggered", at=base - timedelta(minutes=2)),
    ]
    executions = [
        _execution(trigger_id="a", channel="in_app", status="sent", duration_ms=8),
        _execution(trigger_id="a", channel="email", status="failed", duration_ms=20,
                   reason="收件人被拒"),
    ]
    monkeypatch.setattr(activity_log_mod, "_activity_store", MemoryActivityStore(records))
    monkeypatch.setattr(execution_log_mod, "_execution_store", MemoryExecutionStore(executions))
    return TestClient(app)


def test_admin_list_spans_environments_by_default(client):
    data = client.get("/v1/admin/activity").json()
    assert [i["trigger_id"] for i in data["items"]] == ["a", "b", "c"]
    assert {i["environment"] for i in data["items"]} == {"development", "production"}


def test_admin_list_filters_by_environment(client):
    data = client.get("/v1/admin/activity", params={"environment": "production"}).json()
    assert [i["trigger_id"] for i in data["items"]] == ["b"]


def test_admin_list_filters_by_channel_and_keyword(client):
    data = client.get("/v1/admin/activity", params={"channel": "email"}).json()
    assert [i["trigger_id"] for i in data["items"]] == ["a", "c"]
    data = client.get("/v1/admin/activity", params={"keyword": "小林"}).json()
    assert [i["trigger_id"] for i in data["items"]] == ["a"]
    data = client.get("/v1/admin/activity", params={"keyword": "post_liked"}).json()
    assert [i["trigger_id"] for i in data["items"]] == ["b"]


def test_admin_list_rejects_bad_filter_values(client):
    assert client.get("/v1/admin/activity", params={"status": "faild"}).status_code == 400
    assert client.get("/v1/admin/activity", params={"channel": "digest"}).status_code == 400
    assert client.get("/v1/admin/activity", params={"environment": "staging"}).status_code == 400
    assert client.get(
        "/v1/admin/activity", params={"keyword": "x" * (MAX_KEYWORD_CHARS + 1)}
    ).status_code == 400


def test_admin_list_rejects_inverted_window(client):
    resp = client.get(
        "/v1/admin/activity",
        params={"since": "2026-10-21T00:00:00Z", "until": "2026-10-20T00:00:00Z"},
    )
    assert resp.status_code == 400
    assert "since" in resp.json()["detail"]


def test_admin_list_limit_bounds(client):
    assert client.get("/v1/admin/activity", params={"limit": 0}).status_code == 422
    assert client.get("/v1/admin/activity", params={"limit": 500}).status_code == 422


def test_admin_list_cursor_pagination(client):
    first = client.get("/v1/admin/activity", params={"limit": 2}).json()
    assert [i["trigger_id"] for i in first["items"]] == ["a", "b"]
    assert first["hasMore"] is True

    second = client.get(
        "/v1/admin/activity", params={"limit": 2, "cursor": first["nextCursor"]}
    ).json()
    assert [i["trigger_id"] for i in second["items"]] == ["c"]
    assert second["hasMore"] is False
    assert second["nextCursor"] is None


def test_admin_list_rejects_bad_cursor(client):
    assert client.get("/v1/admin/activity", params={"cursor": "nope"}).status_code == 400


def test_admin_summary_aggregates_with_filters(client):
    data = client.get("/v1/admin/activity/summary", params={"channel": "email"}).json()
    assert data["totals"]["records"] == 2
    assert data["totals"]["byStatus"]["triggered"] == 1
    assert data["totals"]["byStatus"]["sent"] == 1
    assert {"channel": "email", "total": 2} in data["channels"]
    # 命中两条记录，但只有一个订阅者
    assert data["subscribers"]["unique"] == 1
    assert data["window"] == {"since": None, "until": None}


def test_admin_summary_window_echo_and_empty_result(client):
    data = client.get(
        "/v1/admin/activity/summary",
        params={"since": "2026-01-01T00:00:00+00:00", "until": "2026-01-02T00:00:00+00:00"},
    ).json()
    assert data["totals"]["records"] == 0
    assert data["window"]["since"].startswith("2026-01-01")
    assert data["workflows"] == []


def test_admin_summary_top_bounds(client):
    assert client.get("/v1/admin/activity/summary", params={"top": 0}).status_code == 422
    assert client.get("/v1/admin/activity/summary", params={"top": 99}).status_code == 422
    data = client.get("/v1/admin/activity/summary", params={"top": 1}).json()
    assert len(data["workflows"]) == 1


def test_admin_detail_joins_executions_and_reports_gaps(client):
    data = client.get("/v1/admin/activity/a").json()
    assert data["activity"]["trigger_id"] == "a"
    assert data["activity"]["payload"]["post_id"] == "p1"
    assert data["executionSummary"]["total"] == 2
    assert data["executionSummary"]["sent"] == 1
    assert data["executionSummary"]["failed"] == 1
    assert data["executionSummary"]["durationMs"] == 28
    # a 声明了 in_app 与 email，两条都有明细
    assert data["missingChannels"] == []
    assert data["undeclaredChannels"] == []
    assert data["executionsTruncated"] is False


def test_admin_detail_reports_declared_but_undelivered_channel(client):
    data = client.get("/v1/admin/activity/c").json()
    # c 声明了 email，但没有对应执行明细：消息可能还在队列里
    assert data["missingChannels"] == ["email"]
    assert data["executionSummary"]["total"] == 0


def test_admin_detail_respects_environment(client):
    assert client.get("/v1/admin/activity/b", params={"environment": "development"}).status_code == 404
    assert client.get("/v1/admin/activity/b", params={"environment": "production"}).status_code == 200


def test_admin_detail_unknown_trigger_returns_404(client):
    assert client.get("/v1/admin/activity/does-not-exist").status_code == 404


def test_admin_detail_rejects_bad_environment(client):
    assert client.get("/v1/admin/activity/a", params={"environment": "staging"}).status_code == 400
