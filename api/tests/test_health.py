"""存活 / 就绪检查的单元测试。

重点不在「返回 200」，而在两条边界：依赖失败时必须给出 503 且说清是哪一项，
依赖返回的错误里不能带出凭据。这两条决定了探针在生产里是帮忙还是添乱。
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import config, health
from app.main import app
from app.storage import JSONLStorage


class _Bus:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.pings = 0

    def ping(self) -> None:
        self.pings += 1
        if self.fail:
            raise ConnectionError("redis 连接被拒绝")


class _Storage:
    def __init__(self, error: Exception | None = None, detail: str = "jsonl 后端，数据目录可写"):
        self.error = error
        self.detail = detail

    def ready(self) -> str:
        if self.error:
            raise self.error
        return self.detail


class _Worker:
    def __init__(self, running: bool):
        self._running = running

    def is_running(self) -> bool:
        return self._running


def test_scrub_removes_credentials():
    text = "OperationalError: postgresql://xwnotify:s3cret@db:5432/xwnotify 拒绝连接"
    scrubbed = health.scrub(text)
    assert "s3cret" not in scrubbed
    assert "postgresql://***@db:5432/xwnotify" in scrubbed


def test_check_serializes_to_dict():
    check = health.Check(name="bus", ok=True, detail="ok", duration_ms=0.5)
    assert check.as_dict() == {"name": "bus", "status": "ok", "detail": "ok", "duration_ms": 0.5}


def test_run_checks_all_ok():
    code, body = health.run_checks([health.Check("a", True, "ok", 0.1)])
    assert code == 200
    assert body["status"] == "ok"
    assert body["checks"][0]["status"] == "ok"


def test_run_checks_reports_failure_with_detail():
    code, body = health.run_checks(
        [
            health.Check("a", True, "ok", 0.1),
            health.Check("b", False, "RuntimeError: boom", 0.2),
        ]
    )
    assert code == 503
    assert body["status"] == "degraded"
    assert body["checks"][1] == {
        "name": "b",
        "status": "fail",
        "detail": "RuntimeError: boom",
        "duration_ms": 0.2,
    }


def test_check_bus_memory_is_always_ready(monkeypatch):
    from app.bus import MemoryMessageBus

    monkeypatch.setattr(health, "get_bus", lambda: MemoryMessageBus())
    result = health.check_bus()
    assert result.ok
    assert "memory" in result.detail


def test_check_bus_uses_ping(monkeypatch):
    bus = _Bus()
    monkeypatch.setattr(health, "get_bus", lambda: bus)
    assert health.check_bus().ok
    assert bus.pings == 1


def test_check_bus_failure_is_reported(monkeypatch):
    monkeypatch.setattr(health, "get_bus", lambda: _Bus(fail=True))
    result = health.check_bus()
    assert not result.ok
    assert "redis 连接被拒绝" in result.detail


def test_check_storage_reports_ready_detail(monkeypatch):
    monkeypatch.setattr(health, "get_storage", lambda: _Storage())
    result = health.check_storage()
    assert result.ok
    assert result.detail == "jsonl 后端，数据目录可写"


def test_check_storage_failure_scrubs_credentials(monkeypatch):
    error = RuntimeError("could not connect: postgresql://user:hunter2@db/xw")
    monkeypatch.setattr(health, "get_storage", lambda: _Storage(error=error))
    result = health.check_storage()
    assert not result.ok
    assert "hunter2" not in result.detail
    assert "***@" in result.detail


def test_check_storage_without_probe_is_not_a_failure(monkeypatch):
    monkeypatch.setattr(health, "get_storage", lambda: object())
    result = health.check_storage()
    assert result.ok
    assert "未提供" in result.detail


def test_jsonl_storage_ready_on_writable_dir(tmp_path):
    storage = JSONLStorage(tmp_path / "messages.jsonl")
    assert storage.ready() == "jsonl 后端，数据目录可写"


def test_jsonl_storage_ready_fails_when_dir_missing(tmp_path):
    storage = JSONLStorage(tmp_path / "missing" / "messages.jsonl")
    with pytest.raises(RuntimeError):
        storage.ready()


def test_check_worker_disabled_is_ready():
    result = health.check_worker(_Worker(running=False), enabled=False)
    assert result.ok
    assert "未启用" in result.detail


def test_check_worker_running():
    result = health.check_worker(_Worker(running=True), enabled=True)
    assert result.ok


def test_check_worker_stopped_is_failure():
    result = health.check_worker(_Worker(running=False), enabled=True)
    assert not result.ok
    assert "未在运行" in result.detail


# ---------------------------------------------------------------- 端点


def test_live_endpoint_does_not_touch_dependencies(monkeypatch):
    # 依赖全部不可用也不影响存活：这是「进程还在」的唯一含义。
    monkeypatch.setattr(health, "get_bus", lambda: _Bus(fail=True))
    response = TestClient(app).get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_returns_queue_depth(monkeypatch):
    import app.main as main_mod

    bus = type("Bus", (), {"size": lambda self: 4})()
    monkeypatch.setattr(main_mod, "get_bus", lambda: bus)
    body = TestClient(app).get("/health").json()
    assert body == {"status": "ok", "queued": 4}


def test_health_tolerates_broken_bus(monkeypatch):
    import app.main as main_mod

    def boom(_self):
        raise ConnectionError("redis 挂了")

    bus = type("Bus", (), {"size": boom})()
    monkeypatch.setattr(main_mod, "get_bus", lambda: bus)
    response = TestClient(app).get("/health")
    # 存活检查不能因为依赖抖动变成 500，否则依赖故障会升级成实例重启
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "queued": None}


def _patch_readiness(monkeypatch, *, worker_running: bool, worker_enabled: bool):
    monkeypatch.setattr(health, "get_bus", lambda: _Bus())
    monkeypatch.setattr(health, "get_storage", lambda: _Storage())
    monkeypatch.setattr(config, "API_WORKER", worker_enabled)
    import app.main as main_mod

    monkeypatch.setattr(main_mod, "_worker", _Worker(running=worker_running))


def test_ready_endpoint_ok(monkeypatch):
    _patch_readiness(monkeypatch, worker_running=True, worker_enabled=True)
    response = TestClient(app).get("/health/ready")
    assert response.status_code == 200
    names = [item["name"] for item in response.json()["checks"]]
    assert names == ["bus", "storage", "worker"]


def test_ready_endpoint_503_when_worker_stopped(monkeypatch):
    _patch_readiness(monkeypatch, worker_running=False, worker_enabled=True)
    response = TestClient(app).get("/health/ready")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    worker = next(item for item in body["checks"] if item["name"] == "worker")
    assert worker["status"] == "fail"


def test_ready_endpoint_skips_worker_when_disabled(monkeypatch):
    _patch_readiness(monkeypatch, worker_running=False, worker_enabled=False)
    response = TestClient(app).get("/health/ready")
    assert response.status_code == 200
    worker = next(item for item in response.json()["checks"] if item["name"] == "worker")
    assert worker["status"] == "ok"
