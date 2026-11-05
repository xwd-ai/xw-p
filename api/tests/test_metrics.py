"""指标导出与 HTTP 采集的单元测试。

指标是进程级累计量，测试之间不会互相清零，因此断言一律用「前后差值」，
而不是绝对值：绝对值写法会在单独跑某个用例时通过、整套跑时失败，这类
失败最难定位。
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import app_metrics, config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app as real_app
from app.metrics import Registry, get_registry
from app.metrics_middleware import MetricsMiddleware


# ---------------------------------------------------------------- 指标原语


def test_counter_requires_exact_labels():
    counter = Registry().counter("xw_t_total", "测试", ("a", "b"))
    with pytest.raises(ValueError):
        counter.inc(a="x")  # 少一个标签
    with pytest.raises(ValueError):
        counter.inc(a="x", b="y", c="z")  # 多一个标签


def test_counter_rejects_negative_amount():
    counter = Registry().counter("xw_t_total", "测试", ())
    with pytest.raises(ValueError):
        counter.inc(-1)


def test_counter_without_labels_renders_bare_name():
    registry = Registry()
    registry.counter("xw_t_total", "测试").inc(2)
    text = registry.render()
    assert "# TYPE xw_t_total counter" in text
    assert "\nxw_t_total 2\n" in text


def test_gauge_set_and_add():
    registry = Registry()
    gauge = registry.gauge("xw_t_current", "测试", ("k",))
    gauge.set(3, k="a")
    gauge.add(-1, k="a")
    assert gauge.value(k="a") == 2


def test_histogram_exports_cumulative_buckets():
    registry = Registry()
    hist = registry.histogram("xw_t_seconds", "测试", (), buckets=[0.1, 1])
    hist.observe(0.05)
    hist.observe(0.5)
    hist.observe(5)

    text = registry.render()
    assert 'xw_t_seconds_bucket{le="0.1"} 1' in text
    assert 'xw_t_seconds_bucket{le="1"} 2' in text
    assert 'xw_t_seconds_bucket{le="+Inf"} 3' in text
    assert "xw_t_seconds_sum 5.55" in text
    assert "xw_t_seconds_count 3" in text


def test_histogram_clamps_negative_and_rejects_empty_buckets():
    hist = Registry().histogram("xw_t_seconds", "测试", (), buckets=[0.1])
    hist.observe(-1)
    assert hist.count() == 1
    with pytest.raises(ValueError):
        Registry().histogram("xw_t_bad", "测试", (), buckets=[0, -1])


def test_render_sorts_families_and_escapes_label_values():
    registry = Registry()
    registry.counter("xw_z_total", "含 \"引号\" 与\n换行", ("k",)).inc(k='a"b\\c\nd')
    registry.gauge("xw_a_gauge", "先输出").set(1)

    text = registry.render()
    assert text.index("xw_a_gauge") < text.index("xw_z_total")
    assert r'k="a\"b\\c\nd"' in text
    # HELP 行里引号没有特殊含义，只需转义反斜杠与换行——换行不转义会把
    # 一行注释截成两行，抓取端从此解析不了后面的样本
    assert '含 "引号" 与\\n换行' in text
    assert text.count("# TYPE xw_z_total counter") == 1


def test_gauge_source_failure_skips_only_that_family():
    def boom():
        raise RuntimeError("依赖不可用")

    registry = Registry()
    registry.gauge("xw_t_depth", "测试", ("k",)).source(boom)
    registry.counter("xw_t_total", "测试").inc(1)

    text = registry.render()
    assert "跳过 xw_t_depth" in text
    assert "\nxw_t_total 1\n" in text


def test_registry_reuses_family_by_name():
    registry = Registry()
    first = registry.counter("xw_t_total", "测试")
    second = registry.counter("xw_t_total", "另一个说明")
    assert first is second


# ---------------------------------------------------------------- HTTP 采集


def _metric_app() -> FastAPI:
    app = FastAPI()

    @app.get("/v1/items/{item_id}")
    def item(item_id: str):
        return {"id": item_id}

    @app.get("/v1/boom")
    def boom():
        raise RuntimeError("炸了")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    app.add_middleware(MetricsMiddleware)
    return app


def _requests(method: str, route: str, status: int) -> float:
    return app_metrics.http_requests().value(method=method, route=route, status=str(status))


def test_request_counted_by_route_template_not_raw_path():
    client = TestClient(_metric_app())
    before = _requests("GET", "/v1/items/{item_id}", 200)
    client.get("/v1/items/abc")
    client.get("/v1/items/xyz")
    assert _requests("GET", "/v1/items/{item_id}", 200) - before == 2
    # 原始路径不能成为标签：否则每来一个标识就多一条序列
    assert _requests("GET", "/v1/items/abc", 200) == 0


def test_unmatched_route_recorded_as_unknown():
    client = TestClient(_metric_app())
    before = _requests("GET", app_metrics.UNKNOWN, 404)
    client.get("/nope")
    assert _requests("GET", app_metrics.UNKNOWN, 404) - before == 1


def test_probe_paths_are_skipped():
    client = TestClient(_metric_app())
    before = _requests("GET", "/health", 200)
    client.get("/health")
    assert _requests("GET", "/health", 200) == before


def test_in_flight_returns_to_zero():
    client = TestClient(_metric_app())
    client.get("/v1/items/1")
    assert app_metrics.http_in_flight().value() == 0


def test_unhandled_error_counted_as_500():
    client = TestClient(_metric_app(), raise_server_exceptions=False)
    before = _requests("GET", "/v1/boom", 500)
    assert client.get("/v1/boom").status_code == 500
    assert _requests("GET", "/v1/boom", 500) - before == 1


def test_duration_histogram_records_sample():
    client = TestClient(_metric_app())
    hist = app_metrics.http_duration()
    before = hist.count(method="GET", route="/v1/items/{item_id}")
    client.get("/v1/items/1")
    assert hist.count(method="GET", route="/v1/items/{item_id}") - before == 1


# ---------------------------------------------------------------- 业务指标


@pytest.fixture
def isolated(monkeypatch):
    """内存总线 / 幂等 / 活动日志，避免触发真实磁盘与全局单例。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod

    class _Store:
        def __init__(self):
            self.records = []

        def append(self, record):
            self.records.append(record)

        def read_all(self):
            return list(self.records)

    bus = MemoryMessageBus()
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(idem_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(al_mod, "_activity_store", _Store())
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    return TestClient(real_app)


def _event(name="post_liked", subscriber="creator_1"):
    return {"name": name, "to": {"subscriberId": subscriber}, "payload": {"post_id": "P-1"}}


def test_trigger_increments_event_counter(isolated):
    counter = app_metrics.events_triggered()
    before = counter.value(workflow="post_liked", environment="development", result="accepted")
    assert isolated.post("/v1/events/trigger", json=_event()).status_code == 200
    after = counter.value(workflow="post_liked", environment="development", result="accepted")
    assert after - before == 1


def test_rejected_event_does_not_label_by_request_name(isolated):
    counter = app_metrics.events_triggered()
    unknown_before = counter.value(workflow=app_metrics.UNKNOWN, environment="development", result="rejected")
    assert isolated.post("/v1/events/trigger", json=_event(name="made_up_name")).status_code == 404
    unknown_after = counter.value(workflow=app_metrics.UNKNOWN, environment="development", result="rejected")
    assert unknown_after - unknown_before == 1
    # 请求自带的名称不进标签，避免任意字符串撑大基数
    assert counter.value(workflow="made_up_name", environment="development", result="rejected") == 0


def test_idempotent_replay_counted(isolated):
    counter = app_metrics.idempotency_requests()
    replayed_before = counter.value(result="replayed")
    headers = {"Idempotency-Key": "post-liked-P-9"}
    assert isolated.post("/v1/events/trigger", json=_event(), headers=headers).status_code == 200
    assert isolated.post("/v1/events/trigger", json=_event(), headers=headers).status_code == 200
    assert counter.value(result="replayed") - replayed_before == 1


def test_batch_counts_accept_and_reject(isolated):
    accepted = app_metrics.events_triggered()
    rejected = app_metrics.events_triggered()
    before_ok = accepted.value(workflow="post_commented", environment="development", result="accepted")
    before_bad = rejected.value(workflow=app_metrics.UNKNOWN, environment="development", result="rejected")

    body = {"events": [_event(name="post_commented"), _event(name="no_such_workflow")]}
    assert isolated.post("/v1/events/trigger/batch", json=body).status_code == 200

    assert accepted.value(workflow="post_commented", environment="development", result="accepted") - before_ok == 1
    assert rejected.value(workflow=app_metrics.UNKNOWN, environment="development", result="rejected") - before_bad == 1


# ---------------------------------------------------------------- 端点


def test_metrics_endpoint_returns_text():
    response = TestClient(real_app).get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert response.headers["cache-control"] == "no-store"
    body = response.text
    assert "# TYPE xw_http_requests_total counter" in body
    assert "# TYPE xw_events_triggered_total counter" in body
    assert "# TYPE xw_queue_depth gauge" in body


def test_queue_depth_rendered_from_source():
    app_metrics.collect_queue_depth(lambda: 7)
    body = get_registry().render()
    assert f'xw_queue_depth{{backend="{config.BUS_BACKEND}"}} 7' in body


def test_queue_depth_source_failure_is_tolerated():
    def boom():
        raise RuntimeError("redis 不可用")

    app_metrics.collect_queue_depth(boom)
    body = get_registry().render()
    # 抓取不因为依赖失败而中断，其它指标照常输出
    assert "跳过 xw_queue_depth" in body
    assert "xw_http_requests_total" in body
