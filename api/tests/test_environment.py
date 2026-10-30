"""多环境隔离与 API Key 鉴权单元测试（Phase 1）。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app


@pytest.fixture
def client(monkeypatch):
    """隔离测试环境：内存总线 + 内存幂等 + 内存活动日志 + 关闭 API Key 鉴权。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod
    from app.activity_log import MemoryActivityStore

    bus = MemoryMessageBus()
    idem = MemoryIdempotencyStore()
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(idem_mod, "_store", idem)
    monkeypatch.setattr(al_mod, "_activity_store", MemoryActivityStore())
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)
    # 默认不配置任何 Key，保证鉴权开放（与 demo 行为一致）
    monkeypatch.setattr(config, "API_KEYS", {"development": "", "production": ""})
    monkeypatch.setattr(config, "ENV", "development")
    return TestClient(app)


def _trigger_body(name="post_commented", subscriber="creator_1", **extra):
    body = {"name": name, "to": {"subscriberId": subscriber}, "payload": {"user": "Alice"}}
    body.update(extra)
    return body


# --- 环境头解析 ---


def test_trigger_defaults_to_development_environment(client):
    resp = client.post("/v1/events/trigger", json=_trigger_body())
    assert resp.status_code == 200


def test_trigger_explicit_environment_header_accepted(client):
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Environment": "development"},
    )
    assert resp.status_code == 200


def test_trigger_unknown_environment_returns_400(client):
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Environment": "staging"},
    )
    assert resp.status_code == 400
    assert "staging" in resp.json()["detail"]


# --- API Key 鉴权 ---


def test_trigger_requires_api_key_when_configured(client, monkeypatch):
    monkeypatch.setattr(config, "API_KEYS", {"development": "dev-secret", "production": ""})

    # 未携带 Key → 401
    resp = client.post("/v1/events/trigger", json=_trigger_body())
    assert resp.status_code == 401

    # 错误 Key → 401
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Api-Key": "wrong"},
    )
    assert resp.status_code == 401

    # 正确 Key → 200
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Api-Key": "dev-secret"},
    )
    assert resp.status_code == 200


def test_trigger_api_key_scoped_per_environment(client, monkeypatch):
    monkeypatch.setattr(
        config, "API_KEYS", {"development": "dev-secret", "production": "prod-secret"}
    )

    # 用 production 的 Key 请求 development 环境 → 401
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Environment": "development", "X-Api-Key": "prod-secret"},
    )
    assert resp.status_code == 401

    # 用 production 的 Key 请求 production 环境 → 200（但需存在 production 工作流，见下）
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Environment": "production", "X-Api-Key": "prod-secret"},
    )
    # production 环境无匹配工作流 → 404（鉴权已通过，证明 Key 作用域正确）
    assert resp.status_code == 404


# --- 环境隔离（工作流作用域） ---


def test_trigger_environment_mismatch_returns_404(client):
    """development 预置工作流在 production 环境不可触发。"""
    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(name="post_commented"),
        headers={"X-Environment": "production"},
    )
    assert resp.status_code == 404


def test_trigger_environment_matched_workflow_returns_200(client, monkeypatch):
    """production 环境注册了同名工作流后即可触发。"""
    from app.models.entities import Environment, Step, Workflow

    workflow = Workflow(
        id="wf-prod-commented",
        name="post_commented",
        description="production 评论通知",
        steps=[Step(id="s1", type="in_app", name="站内信", config={})],
        environment=Environment.PRODUCTION,
        active=True,
    )
    import app.workflow as wf_mod

    monkeypatch.setitem(wf_mod.get_registry()._by_name, "post_commented", workflow)

    resp = client.post(
        "/v1/events/trigger",
        json=_trigger_body(name="post_commented"),
        headers={"X-Environment": "production"},
    )
    assert resp.status_code == 200
    assert resp.json()["acknowledged"] is True


# --- 活动日志环境打标与过滤 ---


def test_activity_records_environment(client):
    client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"X-Environment": "production"},
    )
    # production 无工作流会 404，改用 development 触发并检查打标
    client.post("/v1/events/trigger", json=_trigger_body())
    records = client.get("/v1/activity").json()["items"]
    assert records, "应至少有一条活动记录"
    assert records[0]["environment"] == "development"


def test_activity_filter_by_environment(client):
    client.post("/v1/events/trigger", json=_trigger_body())

    # 按 development 过滤能查到
    dev = client.get("/v1/activity", params={"environment": "development"}).json()
    assert dev["total"] >= 1

    # 按 production 过滤查不到（当前只有 development 记录）
    prod = client.get("/v1/activity", params={"environment": "production"}).json()
    assert prod["total"] == 0
