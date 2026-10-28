"""队列消息的步骤快照契约：Python 入队侧字段与 Go 引擎侧解码对齐。

引擎消费时按消息自带的步骤快照执行，因此快照的字段名、步骤结构与序列化
结果都属于跨语言契约，改动会直接影响「触发 → 执行」链路。
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app
from app.schemas import QueuedMessage, TriggerEvent, WorkflowSnapshot, new_queued_message
from app.workflow import get_registry

# Go 侧 queue.StepSpec 的字段集合：多一个少一个都会让引擎拿不到该字段。
GO_STEP_FIELDS = {"id", "type", "name", "config"}


def _event(name: str = "post_liked") -> TriggerEvent:
    return TriggerEvent.model_validate(
        {
            "name": name,
            "to": {"subscriberId": "creator_1", "channels": ["in_app"]},
            "payload": {
                "post_id": "P-2026-1001",
                "post_title": "我的第一支 Vlog",
                "actor": "小林",
            },
        }
    )


def test_snapshot_preserves_step_order_and_config():
    workflow = get_registry().get("post_liked")
    assert workflow is not None

    snapshot = WorkflowSnapshot.from_workflow(workflow)
    assert snapshot.name == "post_liked"
    assert snapshot.version == workflow.version
    # 顺序即执行顺序：点赞先聚合再分发，不能被重排。
    assert [s.type for s in snapshot.steps] == ["digest", "in_app", "push"]
    # 聚合步骤的配置原样携带（引擎侧据此决定聚合键与窗口）。
    assert snapshot.steps[0].config["key"] == "post_id"
    assert snapshot.steps[0].config["window"] == "10m"


def test_queued_message_carries_snapshot_json():
    workflow = get_registry().get("post_commented")
    assert workflow is not None

    msg = new_queued_message(_event("post_commented"), workflow)
    dumped = msg.model_dump()
    assert dumped["workflow"]["name"] == "post_commented"
    assert [s["type"] for s in dumped["workflow"]["steps"]] == ["in_app", "push", "email"]
    for step in dumped["workflow"]["steps"]:
        assert set(step) == GO_STEP_FIELDS

    # Redis 后端就是 json.dumps(model_dump())，这里确保整条消息可序列化。
    raw = json.dumps(dumped, ensure_ascii=False)
    assert json.loads(raw)["workflow"]["steps"][0]["type"] == "in_app"


def test_queued_message_without_workflow():
    msg = new_queued_message(_event())
    assert msg.workflow is None
    # 未带工作流时序列化为 null，Go 侧按「未携带」处理。
    assert msg.model_dump()["workflow"] is None


def test_snapshot_rejects_unknown_step_type_shape():
    """快照本身只做结构校验：步骤类型拼错由引擎侧按类型解析报错，
    这里保证 steps 必须是对象列表，而不是任意 JSON。"""
    with pytest.raises(ValueError):
        WorkflowSnapshot.model_validate({"name": "post_liked", "steps": ["in_app"]})


class _CapturingBus(MemoryMessageBus):
    """记录已入队消息，便于断言触发 API 写进队列的内容。"""

    def __init__(self) -> None:
        super().__init__()
        self.published: list[QueuedMessage] = []

    def publish(self, message: QueuedMessage) -> None:  # type: ignore[override]
        self.published.append(message)
        super().publish(message)


@pytest.fixture
def client_and_bus(monkeypatch):
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod

    class _FakeActivityStore:
        def append(self, record):
            pass

        def read_all(self):
            return []

    bus = _CapturingBus()
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(idem_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(al_mod, "_activity_store", _FakeActivityStore())
    return TestClient(app), bus


def test_trigger_publishes_message_with_steps(client_and_bus):
    client, bus = client_and_bus
    resp = client.post(
        "/v1/events/trigger",
        json={
            "name": "post_liked",
            "to": {"subscriberId": "creator_1", "channels": ["in_app", "push"]},
            "payload": {"post_id": "P-2026-1001", "post_title": "我的第一支 Vlog", "actor": "小林"},
        },
    )
    assert resp.status_code == 200

    assert len(bus.published) == 1
    msg = bus.published[0]
    assert msg.name == "post_liked"
    assert msg.workflow is not None
    assert [s.type for s in msg.workflow.steps] == ["digest", "in_app", "push"]
    # 触发参数不因快照而丢失
    assert msg.payload["post_id"] == "P-2026-1001"
    assert msg.channels == ["in_app", "push"]


def test_trigger_snapshot_includes_crud_workflow(client_and_bus, monkeypatch):
    """经 CRUD 新建的工作流被触发时，快照同样带上它自己的步骤。"""
    import app.workflow_store as ws_mod
    from app.models.entities import Step, Workflow

    workflow = Workflow(
        id="wf-followed",
        name="user_followed",
        description="内容社区：被关注",
        steps=[
            Step(id="step-1", type="in_app", name="站内信", config={"template": "{{ payload.actor }} 关注了你"}),
            Step(id="step-2", type="push", name="推送", config={"template": "有新粉丝"}),
        ],
    )
    monkeypatch.setattr(ws_mod, "_store", ws_mod.MemoryWorkflowStore(seed=[workflow]))

    client, bus = client_and_bus
    resp = client.post(
        "/v1/events/trigger",
        json={
            "name": "user_followed",
            "to": {"subscriberId": "creator_3", "channels": ["in_app"]},
            "payload": {"actor": "阿青"},
        },
    )
    assert resp.status_code == 200

    msg = bus.published[0]
    assert msg.workflow is not None
    assert [s.id for s in msg.workflow.steps] == ["step-1", "step-2"]
    assert msg.workflow.steps[0].config["template"] == "{{ payload.actor }} 关注了你"
