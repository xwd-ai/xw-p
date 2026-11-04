"""工作流启停与复制的单元测试与接口测试（ROADMAP Phase 5）。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app
from app.models.entities import Environment, Step, Workflow
from app.workflow import seed_workflows
from app.workflow_ops import clone_workflow, copy_name, set_active
from app.workflow_store import MemoryWorkflowStore


# ---------- 复制：纯逻辑 ----------

def _source() -> Workflow:
    return Workflow(
        id="wf-post-liked",
        name="post_liked",
        description="点赞聚合后通知创作者",
        steps=[
            Step(
                id="step-digest",
                type="digest",
                name="点赞聚合",
                config={"key": "post_id", "window": "10m", "tags": ["like"]},
            ),
            Step(
                id="step-inapp",
                type="in_app",
                name="站内信",
                config={"depends_on": ["step-digest"], "template": "有人赞了你的帖子"},
            ),
            Step(
                id="step-push",
                type="push",
                name="推送",
                config={"depends_on": ["step-digest"], "template": "有人赞了你的帖子"},
            ),
        ],
    )


def test_clone_regenerates_workflow_and_step_ids():
    source = _source()
    copy = clone_workflow(source, name="post_liked_copy")

    assert copy.id != source.id
    assert copy.name == "post_liked_copy"
    old_ids = {step.id for step in source.steps}
    new_ids = {step.id for step in copy.steps}
    assert len(new_ids) == len(copy.steps)
    assert old_ids.isdisjoint(new_ids)


def test_clone_remaps_depends_on_to_new_ids():
    copy = clone_workflow(_source(), name="post_liked_copy")
    by_name = {step.name: step for step in copy.steps}

    digest_id = by_name["点赞聚合"].id
    # 依赖必须指向副本自己的步骤，指向源模板的步骤会变成悬空引用。
    assert by_name["站内信"].config["depends_on"] == [digest_id]
    assert by_name["推送"].config["depends_on"] == [digest_id]


def test_clone_deep_copies_config():
    source = _source()
    copy = clone_workflow(source, name="post_liked_copy")

    copy.steps[0].config["tags"].append("comment")
    copy.steps[1].config["template"] = "改动过的文案"

    assert source.steps[0].config["tags"] == ["like"]
    assert source.steps[1].config["template"] == "有人赞了你的帖子"
    assert copy.steps[0].config is not source.steps[0].config


def test_clone_resets_version_and_defaults_inactive():
    source = _source().model_copy(update={"version": 7})
    copy = clone_workflow(source, name="post_liked_copy")

    assert copy.version == 1
    assert copy.active is False
    assert clone_workflow(source, name="x", active=True).active is True


def test_clone_inherits_environment_and_accepts_override():
    source = _source()
    assert clone_workflow(source, name="a").environment == Environment.DEVELOPMENT
    promoted = clone_workflow(source, name="b", environment=Environment.PRODUCTION)
    assert promoted.environment == Environment.PRODUCTION


def test_clone_keeps_step_order_and_metadata():
    source = _source()
    copy = clone_workflow(source, name="post_liked_copy")

    assert [s.type.value for s in copy.steps] == [s.type.value for s in source.steps]
    assert [s.name for s in copy.steps] == [s.name for s in source.steps]
    assert copy.description == source.description


def test_clone_tolerates_step_without_id():
    source = Workflow(
        id="wf-1",
        name="legacy",
        steps=[Step(id="", type="in_app", name="站内信", config={})],
    )
    copy = clone_workflow(source, name="legacy_copy")
    assert len(copy.steps) == 1
    assert copy.steps[0].id


# ---------- 复制：命名 ----------

def test_copy_name_first_choice():
    assert copy_name("post_liked", []) == "post_liked_copy"


def test_copy_name_skips_taken_names():
    taken = ["post_liked", "post_liked_copy", "post_liked_copy2"]
    assert copy_name("post_liked", taken) == "post_liked_copy3"


def test_copy_name_truncates_stem_not_suffix():
    long_name = "p" * 255
    name = copy_name(long_name, [])
    assert len(name) == 255
    assert name.endswith("_copy")
    # 已占用时仍然保留编号，否则第二份副本会和第一份撞名。
    numbered = copy_name(long_name, [name])
    assert len(numbered) == 255
    assert numbered.endswith("_copy2")


def test_copy_name_blank_base_falls_back():
    assert copy_name("", []) == "workflow_copy"
    assert copy_name("   ", []) == "workflow_copy"


# ---------- 启停：纯逻辑 ----------

def test_set_active_returns_new_object():
    source = _source()
    stopped = set_active(source, False)

    assert stopped is not source
    assert stopped.active is False
    assert source.active is True
    assert stopped.version == source.version
    assert stopped.id == source.id
    assert stopped.steps == source.steps


# ---------- 接口 ----------

@pytest.fixture
def rig(monkeypatch):
    """隔离环境：内存工作流存储（含预置模板）+ 内存总线/幂等/活动日志。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod
    import app.workflow_store as ws_mod

    store = MemoryWorkflowStore(seed=seed_workflows())
    monkeypatch.setattr(ws_mod, "_store", store)
    monkeypatch.setattr(bus_mod, "_bus", MemoryMessageBus())
    monkeypatch.setattr(idem_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(al_mod, "_activity_store", al_mod.MemoryActivityStore())
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)
    monkeypatch.setattr(config, "API_KEYS", {"development": "", "production": ""})
    monkeypatch.setattr(config, "ENV", "development")

    class Rig:
        client = TestClient(app)
        workflows = store

    return Rig()


def _by_name(store: MemoryWorkflowStore, name: str) -> Workflow:
    workflow = store.get_by_name(name)
    assert workflow is not None, f"缺少工作流 {name}"
    return workflow


def _trigger(client: TestClient, name: str, environment: str = "development"):
    return client.post(
        "/v1/events/trigger",
        json={"name": name, "to": {"subscriberId": "creator_1"}, "payload": {"actor": "小林"}},
        headers={"X-Environment": environment},
    )


def test_deactivate_returns_changed_and_keeps_version(rig):
    workflow = _by_name(rig.workflows, "post_liked")
    resp = rig.client.put(f"/v1/workflows/{workflow.id}/active", json={"active": False})

    assert resp.status_code == 200
    body = resp.json()
    assert body["changed"] is True
    assert body["workflow"]["active"] is False
    # 启停不是模板修改，版本号不动。
    assert body["workflow"]["version"] == workflow.version
    assert body["workflow"]["steps"] == [
        {"id": s.id, "type": s.type.value, "name": s.name, "config": s.config}
        for s in workflow.steps
    ]


def test_activation_is_idempotent(rig):
    workflow = _by_name(rig.workflows, "post_liked")
    rig.client.put(f"/v1/workflows/{workflow.id}/active", json={"active": False})
    again = rig.client.put(f"/v1/workflows/{workflow.id}/active", json={"active": False})

    assert again.status_code == 200
    assert again.json()["changed"] is False
    assert again.json()["workflow"]["active"] is False
    assert _by_name(rig.workflows, "post_liked").version == workflow.version


def test_activate_unknown_workflow_404(rig):
    resp = rig.client.put("/v1/workflows/no-such-id/active", json={"active": False})
    assert resp.status_code == 404


def test_activation_requires_active_field(rig):
    workflow = _by_name(rig.workflows, "post_liked")
    resp = rig.client.put(f"/v1/workflows/{workflow.id}/active", json={})
    assert resp.status_code == 422


def test_deactivated_workflow_rejects_new_triggers(rig):
    workflow = _by_name(rig.workflows, "post_liked")
    assert _trigger(rig.client, "post_liked").status_code == 200

    rig.client.put(f"/v1/workflows/{workflow.id}/active", json={"active": False})
    rejected = _trigger(rig.client, "post_liked")
    assert rejected.status_code == 404
    # 文案要说明是「未启用」，否则调用方会以为模板被删了。
    assert "未启用" in rejected.json()["detail"]

    rig.client.put(f"/v1/workflows/{workflow.id}/active", json={"active": True})
    assert _trigger(rig.client, "post_liked").status_code == 200


def test_deactivation_does_not_leak_to_other_workflows(rig):
    workflow = _by_name(rig.workflows, "post_liked")
    rig.client.put(f"/v1/workflows/{workflow.id}/active", json={"active": False})

    assert _trigger(rig.client, "post_liked").status_code == 404
    assert _trigger(rig.client, "post_commented").status_code == 200


def test_duplicate_with_defaults(rig):
    source = _by_name(rig.workflows, "post_liked")
    resp = rig.client.post(f"/v1/workflows/{source.id}/duplicate")

    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == "post_liked_copy"
    assert body["id"] != source.id
    assert body["version"] == 1
    # 副本是草稿：先不接管触发，确认后再启用。
    assert body["active"] is False
    assert body["environment"] == "development"
    assert body["description"] == source.description
    assert len(body["steps"]) == len(source.steps)


def test_duplicate_rewrites_step_ids_and_dependencies(rig):
    source = _by_name(rig.workflows, "post_liked")
    copy = rig.client.post(f"/v1/workflows/{source.id}/duplicate").json()

    old_ids = {step.id for step in source.steps}
    new_ids = [step["id"] for step in copy["steps"]]
    assert old_ids.isdisjoint(new_ids)

    by_name = {step["name"]: step for step in copy["steps"]}
    digest_id = by_name["点赞聚合"]["id"]
    assert by_name["站内信"]["config"]["depends_on"] == [digest_id]
    assert by_name["推送"]["config"]["depends_on"] == [digest_id]


def test_duplicate_leaves_source_untouched(rig):
    source = _by_name(rig.workflows, "post_liked")
    before = [dict(step.config) for step in source.steps]
    rig.client.post(f"/v1/workflows/{source.id}/duplicate")

    after = _by_name(rig.workflows, "post_liked")
    assert after.name == "post_liked"
    assert after.active == source.active
    assert [dict(step.config) for step in after.steps] == before


def test_duplicate_twice_numbers_the_name(rig):
    source = _by_name(rig.workflows, "post_liked")
    assert rig.client.post(f"/v1/workflows/{source.id}/duplicate").json()["name"] == "post_liked_copy"
    assert rig.client.post(f"/v1/workflows/{source.id}/duplicate").json()["name"] == "post_liked_copy2"


def test_duplicate_uses_explicit_name_and_active_flag(rig):
    source = _by_name(rig.workflows, "post_commented")
    body = {"name": "post_commented_v2", "active": True}
    resp = rig.client.post(f"/v1/workflows/{source.id}/duplicate", json=body)

    assert resp.status_code == 201
    created = resp.json()
    assert created["name"] == "post_commented_v2"
    assert created["active"] is True
    assert _trigger(rig.client, "post_commented_v2").status_code == 200


def test_duplicate_inactive_copy_cannot_be_triggered(rig):
    source = _by_name(rig.workflows, "post_mentioned")
    copy = rig.client.post(f"/v1/workflows/{source.id}/duplicate", json={"name": "post_mentioned_draft"}).json()
    assert copy["active"] is False

    assert _trigger(rig.client, "post_mentioned_draft").status_code == 404
    rig.client.put(f"/v1/workflows/{copy['id']}/active", json={"active": True})
    assert _trigger(rig.client, "post_mentioned_draft").status_code == 200


def test_duplicate_explicit_name_conflict_409(rig):
    source = _by_name(rig.workflows, "post_liked")
    resp = rig.client.post(f"/v1/workflows/{source.id}/duplicate", json={"name": "post_commented"})
    assert resp.status_code == 409


def test_duplicate_unknown_source_404(rig):
    resp = rig.client.post("/v1/workflows/no-such-id/duplicate")
    assert resp.status_code == 404


def test_duplicate_blank_name_422(rig):
    source = _by_name(rig.workflows, "post_liked")
    resp = rig.client.post(f"/v1/workflows/{source.id}/duplicate", json={"name": "   "})
    assert resp.status_code == 422


def test_duplicate_into_other_environment(rig):
    source = _by_name(rig.workflows, "post_liked")
    body = {"name": "post_liked_prod", "environment": "production", "active": True}
    created = rig.client.post(f"/v1/workflows/{source.id}/duplicate", json=body).json()
    assert created["environment"] == "production"

    # 环境隔离仍然生效：生产副本在开发环境不可触发。
    assert _trigger(rig.client, "post_liked_prod").status_code == 404
    assert _trigger(rig.client, "post_liked_prod", environment="production").status_code == 200


def test_duplicate_preserves_digest_step_config(rig):
    source = _by_name(rig.workflows, "post_liked")
    copy = rig.client.post(f"/v1/workflows/{source.id}/duplicate").json()

    digest = next(step for step in copy["steps"] if step["type"] == "digest")
    assert digest["config"]["key"] == "post_id"
    assert digest["config"]["window"] == "10m"
