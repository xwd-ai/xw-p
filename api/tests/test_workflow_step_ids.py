"""工作流步骤标识的保存语义（ROADMAP Phase 5）。

编辑器保存时按整份步骤数组回写，``config.depends_on`` 里存的是步骤标识。
若每次保存都重新生成标识，依赖就会指向已经不存在的步骤——服务端返回 422，
或者更糟：依赖被悄悄丢掉，工作流从「聚合后分发」变成「各发各的」。
"""
from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.workflow_store import MemoryWorkflowStore


@pytest.fixture
def client(monkeypatch):
    import app.workflow_store as ws

    monkeypatch.setattr(ws, "_store", MemoryWorkflowStore())
    return TestClient(app)


def _steps():
    return [
        {
            "id": "step-digest",
            "type": "digest",
            "name": "点赞聚合",
            "config": {"key": "post_id", "window": "10m"},
        },
        {
            "id": "step-inapp",
            "type": "in_app",
            "name": "站内信",
            "config": {"depends_on": ["step-digest"], "template": "有人赞了你的帖子"},
        },
    ]


def _create(client, steps, name="post_liked"):
    return client.post(
        "/v1/workflows",
        json={"name": name, "description": "测试", "steps": steps},
    )


def test_create_keeps_client_supplied_ids(client):
    resp = _create(client, _steps())
    assert resp.status_code == 201
    ids = [s["id"] for s in resp.json()["steps"]]
    assert ids == ["step-digest", "step-inapp"]


def test_create_generates_id_when_omitted(client):
    resp = _create(client, [{"type": "push", "name": "推送", "config": {}}])
    assert resp.status_code == 201
    step_id = resp.json()["steps"][0]["id"]
    assert step_id
    # 生成的是 uuid，且不与客户端风格冲突
    uuid.UUID(step_id)


def test_create_blank_id_falls_back_to_generated(client):
    resp = _create(client, [{"id": "   ", "type": "push", "name": "推送", "config": {}}])
    assert resp.status_code == 201
    assert resp.json()["steps"][0]["id"].strip()


def test_create_trims_id(client):
    resp = _create(client, [{"id": "  step-a  ", "type": "push", "name": "推送", "config": {}}])
    assert resp.status_code == 201
    assert resp.json()["steps"][0]["id"] == "step-a"


@pytest.mark.parametrize(
    "step_id",
    [
        "step a",       # 空格
        "step/a",       # 路径分隔符
        "step?x=1",     # 查询串
        "步骤一",        # 非 ASCII
        "a" * 65,       # 超长
    ],
)
def test_create_rejects_unsafe_step_id(client, step_id):
    resp = _create(client, [{"id": step_id, "type": "push", "name": "推送", "config": {}}])
    assert resp.status_code == 422


def test_create_rejects_duplicate_step_ids(client):
    steps = [
        {"id": "step-a", "type": "in_app", "name": "站内信", "config": {}},
        {"id": "step-a", "type": "push", "name": "推送", "config": {}},
    ]
    resp = _create(client, steps)
    assert resp.status_code == 422


def test_create_accepts_dependency_on_supplied_id(client):
    """依赖指向客户端带来的标识时不应被判成悬空引用。"""
    resp = _create(client, _steps())
    assert resp.status_code == 201
    assert resp.json()["steps"][1]["config"]["depends_on"] == ["step-digest"]


def test_create_rejects_dependency_on_unknown_id(client):
    steps = [
        {
            "id": "step-inapp",
            "type": "in_app",
            "name": "站内信",
            "config": {"depends_on": ["step-ghost"]},
        }
    ]
    resp = _create(client, steps)
    assert resp.status_code == 422
    assert "step-ghost" in resp.json()["detail"]


def test_update_round_trip_keeps_ids_and_dependency(client):
    created = _create(client, _steps()).json()
    workflow_id = created["id"]

    # 模拟编辑器：原样回写整份步骤数组，只改一个模板
    steps = created["steps"]
    steps[1]["config"]["template"] = "又有新点赞"
    resp = client.put(f"/v1/workflows/{workflow_id}", json={"steps": steps})

    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == created["version"] + 1
    assert [s["id"] for s in body["steps"]] == ["step-digest", "step-inapp"]
    assert body["steps"][1]["config"]["depends_on"] == ["step-digest"]
    assert body["steps"][1]["config"]["template"] == "又有新点赞"


def test_update_reorders_steps_without_breaking_dependency(client):
    created = _create(client, _steps()).json()
    steps = list(reversed(created["steps"]))
    resp = client.put(f"/v1/workflows/{created['id']}", json={"steps": steps})

    assert resp.status_code == 200
    body = resp.json()
    assert [s["id"] for s in body["steps"]] == ["step-inapp", "step-digest"]
    # 依赖只认标识，不认数组下标
    assert body["steps"][0]["config"]["depends_on"] == ["step-digest"]


def test_update_rejects_dependency_on_removed_step(client):
    created = _create(client, _steps()).json()
    remaining = [created["steps"][1]]  # 删掉被依赖的聚合步骤
    resp = client.put(f"/v1/workflows/{created['id']}", json={"steps": remaining})

    assert resp.status_code == 422
    assert "step-digest" in resp.json()["detail"]


def test_update_without_ids_regenerates_them(client):
    """不带 id 的旧调用方仍按服务端生成处理，不要求客户端维护标识。"""
    created = _create(client, _steps()).json()
    resp = client.put(
        f"/v1/workflows/{created['id']}",
        json={"steps": [{"type": "in_app", "name": "站内信", "config": {}}]},
    )

    assert resp.status_code == 200
    step_id = resp.json()["steps"][0]["id"]
    assert step_id not in ("step-digest", "step-inapp")
    uuid.UUID(step_id)
