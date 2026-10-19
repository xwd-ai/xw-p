"""工作流模板 CRUD API 与存储/步骤校验的单元测试。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.entities import Step, Workflow
from app.step_validation import StepValidationError, normalize_step, validate_steps
from app.workflow_store import (
    MemoryWorkflowStore,
    WorkflowConflictError,
    WorkflowNotFoundError,
)


# ---------- 步骤校验 ----------

def _step(step_id="s1", type_="in_app", config=None):
    return Step(id=step_id, type=type_, name=step_id, config=config or {})


def test_validate_steps_ok():
    steps = [_step("s1", "email"), _step("s2", "sms")]
    validate_steps(steps)  # 不应抛出


def test_validate_steps_duplicate_id():
    with pytest.raises(StepValidationError):
        validate_steps([_step("s1"), _step("s1")])


def test_validate_steps_invalid_type():
    with pytest.raises(StepValidationError):
        # 非法类型在 JSON 反序列化（normalize_step）时被拦截
        normalize_step({"type": "not_a_channel", "name": "x", "config": {}})


def test_validate_steps_missing_dependency():
    steps = [_step("s1", "email", {"depends_on": ["ghost"]})]
    with pytest.raises(StepValidationError):
        validate_steps(steps)


def test_validate_steps_cycle_detected():
    steps = [
        _step("s1", "email", {"depends_on": ["s2"]}),
        _step("s2", "sms", {"depends_on": ["s1"]}),
    ]
    with pytest.raises(StepValidationError):
        validate_steps(steps)


# ---------- 内存存储 ----------

def _workflow(name="welcome", step_id="s1"):
    return Workflow(
        name=name,
        steps=[Step(id=step_id, type="in_app", name="欢迎", config={})],
        version=1,
    )


def test_store_create_and_get():
    store = MemoryWorkflowStore()
    wf = _workflow()
    store.create(wf)
    assert store.get(wf.id) is wf
    assert store.get_by_name("welcome") is wf
    assert store.list() == [wf]


def test_store_create_duplicate_name_conflict():
    store = MemoryWorkflowStore()
    store.create(_workflow("dup"))
    with pytest.raises(WorkflowConflictError):
        store.create(_workflow("dup"))


def test_store_update_increments_version_manually():
    store = MemoryWorkflowStore()
    wf = _workflow()
    store.create(wf)
    wf.version = 2
    store.update(wf)
    assert store.get(wf.id).version == 2


def test_store_update_missing_raises():
    store = MemoryWorkflowStore()
    with pytest.raises(WorkflowNotFoundError):
        store.update(_workflow())


def test_store_delete():
    store = MemoryWorkflowStore()
    wf = _workflow()
    store.create(wf)
    assert store.delete(wf.id) is True
    assert store.delete(wf.id) is False
    assert store.get(wf.id) is None


# ---------- CRUD API ----------

@pytest.fixture
def client(monkeypatch):
    import app.workflow_store as ws

    store = MemoryWorkflowStore()
    monkeypatch.setattr(ws, "_store", store)
    return TestClient(app)


def _create_body(name="welcome", steps=None):
    return {
        "name": name,
        "description": "测试工作流",
        "steps": steps
        or [{"type": "in_app", "name": "发送欢迎消息", "config": {"template": "你好"}}],
    }


def test_create_workflow(client):
    resp = client.post("/v1/workflows", json=_create_body())
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "welcome"
    assert data["version"] == 1
    assert data["active"] is True
    assert len(data["steps"]) == 1
    assert data["steps"][0]["type"] == "in_app"


def test_create_workflow_empty_name_422(client):
    resp = client.post("/v1/workflows", json=_create_body(name=""))
    assert resp.status_code == 422


def test_create_workflow_invalid_step_type_422(client):
    body = _create_body(steps=[{"type": "bogus", "name": "x", "config": {}}])
    resp = client.post("/v1/workflows", json=body)
    assert resp.status_code == 422


def test_create_workflow_duplicate_name_409(client):
    client.post("/v1/workflows", json=_create_body("dup"))
    resp = client.post("/v1/workflows", json=_create_body("dup"))
    assert resp.status_code == 409


def test_list_workflows(client):
    client.post("/v1/workflows", json=_create_body("a"))
    client.post("/v1/workflows", json=_create_body("b"))
    resp = client.get("/v1/workflows")
    assert resp.status_code == 200
    names = {w["name"] for w in resp.json()}
    assert names == {"a", "b"}


def test_get_workflow(client):
    created = client.post("/v1/workflows", json=_create_body()).json()
    resp = client.get(f"/v1/workflows/{created['id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == created["id"]


def test_get_workflow_404(client):
    resp = client.get("/v1/workflows/no-such-id")
    assert resp.status_code == 404


def test_update_workflow_increments_version(client):
    created = client.post("/v1/workflows", json=_create_body()).json()
    resp = client.put(
        f"/v1/workflows/{created['id']}",
        json={"description": "更新后的描述"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["description"] == "更新后的描述"
    assert data["version"] == 2
    assert data["name"] == "welcome"


def test_update_workflow_404(client):
    resp = client.put("/v1/workflows/no-such-id", json={"description": "x"})
    assert resp.status_code == 404


def test_delete_workflow(client):
    created = client.post("/v1/workflows", json=_create_body()).json()
    resp = client.delete(f"/v1/workflows/{created['id']}")
    assert resp.status_code == 204
    # 再次删除返回 404
    resp2 = client.delete(f"/v1/workflows/{created['id']}")
    assert resp2.status_code == 404


def test_delete_workflow_404(client):
    resp = client.delete("/v1/workflows/no-such-id")
    assert resp.status_code == 404
