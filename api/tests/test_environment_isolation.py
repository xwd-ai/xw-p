"""环境隔离：订阅者与工作流在两套环境里的读写边界。

环境（development / production）是同一套业务数据的两份副本。隔离不完整的
表现通常不是报错，而是「改了一边、另一边跟着变」：本地给订阅者加个测试邮箱，
线上那个人的渠道绑定被一起改掉；线上停用了某个模板，本地同名模板也跟着停。
这里钉住三件事：

- 记录的键带环境：同一个标识、同一个地址在两套环境各有一份；
- 按标识访问时另一套环境的记录按「不存在」处理（404），既读不到也改不到；
- 按名称查找模板时带环境，避免两套环境的同名模板互相遮蔽。
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app import config
from app.main import app
from app.models.channel import Channel
from app.models.entities import Environment, Subscriber, Workflow
from app.subscriber_store import (
    JsonlSubscriberStore,
    MemorySubscriberStore,
    SubscriberAddressConflictError,
    SubscriberConflictError,
)
from app.workflow import seed_workflows
from app.workflow_store import MemoryWorkflowStore


def _subscriber(
    subscriber_id: str = "creator_1",
    environment: Environment = Environment.DEVELOPMENT,
    **kwargs,
) -> Subscriber:
    return Subscriber(id=subscriber_id, environment=environment, **kwargs)


# ---------------------------------------------------------------- 订阅者：存储层


def test_same_identifier_coexists_in_both_environments():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", email="creator@example.com"))
    store.create(
        _subscriber(
            "creator_1",
            Environment.PRODUCTION,
            email="creator@example.com",
            channels=[Channel.IN_APP],
        )
    )

    dev = store.get("creator_1", environment="development")
    prod = store.get("creator_1", environment="production")
    assert dev is not None and prod is not None
    assert dev.environment is Environment.DEVELOPMENT
    assert prod.environment is Environment.PRODUCTION
    # 两套环境各改各的：线上这条多绑了站内信，本地那条没有。
    assert prod.channels == [Channel.IN_APP]
    assert dev.channels == []


def test_create_conflicts_only_within_the_same_environment():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1"))
    with pytest.raises(SubscriberConflictError) as excinfo:
        store.create(_subscriber("creator_1", email="other@example.com"))
    assert "development" in str(excinfo.value)

    # 另一套环境里同样的标识可以建。
    store.create(_subscriber("creator_1", Environment.PRODUCTION))
    assert len(store.list(environment="production")[0]) == 1


def test_address_uniqueness_is_per_environment():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", email="creator@example.com"))
    store.create(
        _subscriber("creator_1", Environment.PRODUCTION, email="creator@example.com")
    )

    with pytest.raises(SubscriberAddressConflictError):
        store.create(_subscriber("creator_2", email="CREATOR@example.com"))


def test_get_without_environment_prefers_development():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", Environment.PRODUCTION, email="prod@example.com"))
    store.create(_subscriber("creator_1", email="dev@example.com"))

    found = store.get("creator_1")
    assert found is not None and found.email == "dev@example.com"
    assert store.get("ghost") is None


def test_list_is_scoped_by_environment_and_still_paginates():
    store = MemorySubscriberStore()
    store.create(_subscriber("dev_a"))
    store.create(_subscriber("dev_b"))
    store.create(_subscriber("prod_a", Environment.PRODUCTION))

    items, cursor = store.list(environment="development", limit=1)
    assert [s.id for s in items] == ["dev_a"]
    assert cursor == "dev_a"

    rest, cursor = store.list(environment="development", limit=1, cursor=cursor)
    assert [s.id for s in rest] == ["dev_b"]
    assert cursor is None

    prod, _ = store.list(environment="production")
    assert [s.id for s in prod] == ["prod_a"]


def test_delete_only_touches_the_requested_environment():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", email="dev@example.com"))
    store.create(_subscriber("creator_1", Environment.PRODUCTION, email="prod@example.com"))

    # 带了环境：只删那一条，另一套环境不受影响。
    assert store.delete("creator_1", environment="production") is True
    assert store.get("creator_1", environment="production") is None
    assert store.get("creator_1", environment="development") is not None
    assert store.delete("creator_1", environment="production") is False

    # 不带环境：删掉「按标识读得到」的那一条（开发环境优先），不会两条一起删。
    assert store.delete("creator_1") is True
    assert store.get("creator_1") is None


def test_update_requires_the_record_in_its_own_environment(tmp_path):
    store = JsonlSubscriberStore(tmp_path / "subscribers.jsonl")
    store.create(_subscriber("creator_1", email="dev@example.com"))

    moved = _subscriber(
        "creator_1", Environment.PRODUCTION, email="dev@example.com"
    )
    from app.subscriber_store import SubscriberNotFoundError

    with pytest.raises(SubscriberNotFoundError):
        store.update(moved)


def test_jsonl_store_keeps_both_environments_across_reload(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    store = JsonlSubscriberStore(path)
    store.create(_subscriber("creator_1", email="dev@example.com"))
    store.create(_subscriber("creator_1", Environment.PRODUCTION, email="prod@example.com"))

    reopened = JsonlSubscriberStore(path)
    dev = reopened.get("creator_1", environment="development")
    prod = reopened.get("creator_1", environment="production")
    assert dev is not None and dev.email == "dev@example.com"
    assert prod is not None and prod.email == "prod@example.com"


# ---------------------------------------------------------------- 工作流：存储层


def _workflow(name: str, environment: Environment = Environment.DEVELOPMENT) -> Workflow:
    return Workflow(id=f"wf-{name}-{environment.value}", name=name, environment=environment)


def test_workflow_names_are_unique_per_environment():
    store = MemoryWorkflowStore()
    store.create(_workflow("post_liked"))
    store.create(_workflow("post_liked", Environment.PRODUCTION))

    from app.workflow_store import WorkflowConflictError

    with pytest.raises(WorkflowConflictError) as excinfo:
        store.create(_workflow("post_liked"))
    assert "development" in str(excinfo.value)


def test_get_by_name_matches_environment():
    store = MemoryWorkflowStore()
    dev = store.create(_workflow("post_liked"))
    prod = store.create(_workflow("post_liked", Environment.PRODUCTION))

    assert store.get_by_name("post_liked", environment="production").id == prod.id
    assert store.get_by_name("post_liked", environment="development").id == dev.id
    assert store.get_by_name("post_liked", environment="staging") is None


def test_workflow_list_filters_environment():
    store = MemoryWorkflowStore()
    store.create(_workflow("post_liked"))
    store.create(_workflow("post_liked", Environment.PRODUCTION))
    store.create(_workflow("post_mentioned", Environment.PRODUCTION))

    assert [wf.name for wf in store.list(environment="production")] == [
        "post_liked",
        "post_mentioned",
    ]
    assert len(store.list()) == 3


# ---------------------------------------------------------------- 接口层


@pytest.fixture
def rig(monkeypatch):
    """内存订阅者 / 工作流存储 + 内存总线，避免落盘到 api/data。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod
    import app.subscriber_store as sub_mod
    import app.workflow_store as ws_mod

    subscribers = MemorySubscriberStore()
    workflows = MemoryWorkflowStore(seed=seed_workflows())
    monkeypatch.setattr(sub_mod, "_store", subscribers)
    monkeypatch.setattr(ws_mod, "_store", workflows)
    monkeypatch.setattr(bus_mod, "_bus", bus_mod.MemoryMessageBus())
    monkeypatch.setattr(idem_mod, "_store", idem_mod.MemoryIdempotencyStore())
    monkeypatch.setattr(al_mod, "_activity_store", al_mod.MemoryActivityStore())
    monkeypatch.setattr(config, "API_KEYS", {"development": "", "production": ""})
    monkeypatch.setattr(config, "ENV", "development")

    return SimpleNamespace(
        client=TestClient(app), subscribers=subscribers, workflows=workflows
    )


def _create_subscriber(client: TestClient, subscriber_id: str, email: str, environment: str):
    return client.post(
        "/v1/subscribers",
        json={"id": subscriber_id, "email": email, "channels": ["in_app"]},
        headers={"X-Environment": environment},
    )


def test_api_allows_same_identifier_in_both_environments(rig):
    dev = _create_subscriber(rig.client, "creator_1", "creator@example.com", "development")
    prod = _create_subscriber(rig.client, "creator_1", "creator@example.com", "production")
    assert dev.status_code == 201
    assert prod.status_code == 201
    assert dev.json()["environment"] == "development"
    assert prod.json()["environment"] == "production"

    listing = rig.client.get("/v1/subscribers").json()
    assert [item["id"] for item in listing["items"]] == ["creator_1"]
    assert listing["items"][0]["environment"] == "development"

    production = rig.client.get(
        "/v1/subscribers", headers={"X-Environment": "production"}
    ).json()
    assert [item["environment"] for item in production["items"]] == ["production"]


def test_api_cross_environment_read_is_404(rig):
    _create_subscriber(rig.client, "creator_1", "creator@example.com", "development")

    missing = rig.client.get(
        "/v1/subscribers/creator_1", headers={"X-Environment": "production"}
    )
    assert missing.status_code == 404
    assert "production" in missing.json()["detail"]

    assert rig.client.get("/v1/subscribers/creator_1").status_code == 200


def test_api_cross_environment_update_leaves_other_side_intact(rig):
    _create_subscriber(rig.client, "creator_1", "creator@example.com", "development")
    _create_subscriber(rig.client, "creator_1", "creator@example.com", "production")

    rejected = rig.client.put(
        "/v1/subscribers/creator_1",
        json={"email": "hijack@example.com"},
        headers={"X-Environment": "production"},
    )
    # 两边都有这个标识，因此这次更新是「改生产那条」，本地那条必须原样。
    assert rejected.status_code == 200
    dev = rig.client.get("/v1/subscribers/creator_1").json()
    assert dev["email"] == "creator@example.com"

    only_dev = rig.client.put(
        "/v1/subscribers/creator_1",
        json={"email": "other@example.com"},
        headers={"X-Environment": "development"},
    )
    assert only_dev.status_code == 200
    prod = rig.client.get(
        "/v1/subscribers/creator_1", headers={"X-Environment": "production"}
    ).json()
    assert prod["email"] == "hijack@example.com"


def test_api_cross_environment_update_is_404_when_only_one_side_exists(rig):
    _create_subscriber(rig.client, "creator_1", "creator@example.com", "development")

    response = rig.client.put(
        "/v1/subscribers/creator_1",
        json={"email": "hijack@example.com"},
        headers={"X-Environment": "production"},
    )
    assert response.status_code == 404
    assert rig.client.get("/v1/subscribers/creator_1").json()["email"] == "creator@example.com"


def test_api_delete_is_scoped_by_environment(rig):
    _create_subscriber(rig.client, "creator_1", "creator@example.com", "development")
    _create_subscriber(rig.client, "creator_1", "creator@example.com", "production")

    assert (
        rig.client.delete(
            "/v1/subscribers/creator_1", headers={"X-Environment": "production"}
        ).status_code
        == 204
    )
    assert rig.client.get("/v1/subscribers/creator_1").status_code == 200
    assert (
        rig.client.get(
            "/v1/subscribers/creator_1", headers={"X-Environment": "production"}
        ).status_code
        == 404
    )


def _create_workflow(client: TestClient, name: str, header_env: str, **extra):
    body = {
        "name": name,
        "steps": [
            {"id": "inapp", "type": "in_app", "config": {"template": "有人赞了你的帖子"}}
        ],
        **extra,
    }
    return client.post("/v1/workflows", json=body, headers={"X-Environment": header_env})


def test_api_workflow_lands_in_requested_environment(rig):
    created = _create_workflow(rig.client, "post_liked_digest", "production")
    assert created.status_code == 201
    assert created.json()["environment"] == "production"

    production = rig.client.get(
        "/v1/workflows", headers={"X-Environment": "production"}
    ).json()
    assert [item["name"] for item in production] == ["post_liked_digest"]

    # 开发环境看不到它，预置模板也还在。
    development = rig.client.get("/v1/workflows").json()
    assert "post_liked_digest" not in [item["name"] for item in development]
    assert "post_liked" in [item["name"] for item in development]


def test_api_workflow_rejects_mismatched_environment(rig):
    response = _create_workflow(
        rig.client, "post_liked_digest", "development", environment="production"
    )
    assert response.status_code == 422
    assert "X-Environment" in response.json()["detail"]
    assert not [wf for wf in rig.workflows.list() if wf.name == "post_liked_digest"]


def test_api_same_workflow_name_in_both_environments(rig):
    assert _create_workflow(rig.client, "post_liked_digest", "development").status_code == 201
    assert _create_workflow(rig.client, "post_liked_digest", "production").status_code == 201

    dev = rig.client.get("/v1/workflows").json()
    prod = rig.client.get("/v1/workflows", headers={"X-Environment": "production"}).json()
    assert [item["environment"] for item in dev if item["name"] == "post_liked_digest"] == [
        "development"
    ]
    assert [item["environment"] for item in prod if item["name"] == "post_liked_digest"] == [
        "production"
    ]


def test_api_workflow_by_id_is_404_across_environments(rig):
    created = _create_workflow(rig.client, "post_liked_digest", "production").json()

    for method, path, payload in (
        ("get", f"/v1/workflows/{created['id']}", None),
        ("put", f"/v1/workflows/{created['id']}", {"description": "改一下"}),
        ("put", f"/v1/workflows/{created['id']}/active", {"active": False}),
        ("delete", f"/v1/workflows/{created['id']}", None),
        ("post", f"/v1/workflows/{created['id']}/duplicate", None),
    ):
        request = getattr(rig.client, method)
        response = request(path, json=payload) if payload is not None else request(path)
        assert response.status_code == 404, f"{method} {path} 不该跨环境生效"
        # 文案按请求环境说明「本环境没有这个模板」，而不是暴露它属于另一套环境。
        assert "development" in response.json()["detail"]

    stored = rig.workflows.get(created["id"])
    assert stored is not None and stored.active is True and stored.description is None


def test_trigger_uses_the_template_of_its_own_environment(rig):
    """同名模板在两套环境各有一份时，触发必须落到本环境那份。"""
    created = _create_workflow(rig.client, "post_liked_weekly", "production").json()

    payload = {"name": "post_liked_weekly", "to": {"subscriberId": "creator_1"}}
    # 开发环境没有这个模板，按「不存在或未启用」拒绝。
    assert rig.client.post("/v1/events/trigger", json=payload).status_code == 404

    accepted = rig.client.post(
        "/v1/events/trigger", json=payload, headers={"X-Environment": "production"}
    )
    assert accepted.status_code == 200

    # 停用生产环境那份，开发环境新建的同名模板不受影响。
    rig.client.put(
        f"/v1/workflows/{created['id']}/active",
        json={"active": False},
        headers={"X-Environment": "production"},
    )
    rejected = rig.client.post(
        "/v1/events/trigger", json=payload, headers={"X-Environment": "production"}
    )
    assert rejected.status_code == 404

    assert _create_workflow(rig.client, "post_liked_weekly", "development").status_code == 201
    assert rig.client.post("/v1/events/trigger", json=payload).status_code == 200
