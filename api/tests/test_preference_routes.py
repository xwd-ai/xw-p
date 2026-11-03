"""偏好查询与写入接口的测试。

接口层的职责有三块：归属校验（订阅者存在且属于本环境）、覆盖写语义
（同一 ``(层级, 目标)`` 只有一条记录）、以及把三层覆盖链的结论解释清楚。
存储细节由 :mod:`tests.test_preferences` 覆盖，这里只验接口行为。
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.entities import Environment, Subscriber
from app.preferences import MemoryPreferenceStore, Scope
from app.subscriber_store import MemorySubscriberStore

BASE = "/v1/subscribers/creator_1/preferences"
GHOST = "/v1/subscribers/ghost/preferences"


def _subscriber(subscriber_id: str, environment=Environment.DEVELOPMENT) -> Subscriber:
    return Subscriber(id=subscriber_id, environment=environment, channels=[])


@pytest.fixture()
def subscribers():
    return MemorySubscriberStore(
        [
            _subscriber("creator_1"),
            _subscriber("creator_prod", Environment.PRODUCTION),
        ]
    )


@pytest.fixture()
def preferences():
    return MemoryPreferenceStore()


@pytest.fixture()
def client(monkeypatch, subscribers, preferences):
    monkeypatch.setattr(
        "app.preference_routes.get_subscriber_store", lambda: subscribers
    )
    monkeypatch.setattr(
        "app.preference_routes.get_preference_store", lambda: preferences
    )
    with TestClient(app) as test_client:
        yield test_client


def _put(client, body, subscriber="creator_1", headers=None):
    return client.put(
        f"/v1/subscribers/{subscriber}/preferences", json=body, headers=headers or {}
    )


# ---------------------------------------------------------------- 查询


def test_list_is_empty_for_a_subscriber_without_preferences(client):
    response = client.get(BASE)
    assert response.status_code == 200
    payload = response.json()
    assert payload["subscriber_id"] == "creator_1"
    assert payload["items"] == []
    # 三个层级的键恒在，调用方不必自己补零。
    assert payload["counts"] == {"global": 0, "workflow": 0, "channel": 0}
    assert payload["default_allowed"] is True


def test_list_orders_by_specificity_and_counts_each_scope(client):
    _put(client, {"scope": "global", "enabled": False})
    _put(client, {"scope": "workflow", "target": "post_liked", "enabled": True})
    _put(client, {"scope": "channel", "target": "email", "enabled": False})
    _put(client, {"scope": "channel", "target": "push", "enabled": True})

    payload = client.get(BASE).json()
    # 具体程度：渠道 → 工作流 → 全局。
    assert [(item["scope"], item["target"]) for item in payload["items"]] == [
        ("channel", "email"),
        ("channel", "push"),
        ("workflow", "post_liked"),
        ("global", None),
    ]
    assert payload["counts"] == {"global": 1, "workflow": 1, "channel": 2}


def test_list_filters_by_scope(client):
    _put(client, {"scope": "global", "enabled": False})
    _put(client, {"scope": "channel", "target": "sms", "enabled": False})

    payload = client.get(BASE, params={"scope": "channel"}).json()
    assert [item["target"] for item in payload["items"]] == ["sms"]
    # 计数跟着筛选走：筛选后的视图不该报出一个它没返回的层级。
    assert payload["counts"] == {"global": 0, "workflow": 0, "channel": 1}


@pytest.mark.parametrize("scope", ["audience", "audiences", "all"])
def test_list_rejects_unknown_scope(client, scope):
    response = client.get(BASE, params={"scope": scope})
    assert response.status_code == 422


def test_blank_scope_means_no_filter(client):
    _put(client, {"scope": "global", "enabled": False})
    payload = client.get(BASE, params={"scope": ""}).json()
    assert len(payload["items"]) == 1


# ---------------------------------------------------------------- 写入


def test_put_creates_then_overwrites(client):
    first = _put(client, {"scope": "channel", "target": "email", "enabled": False})
    assert first.status_code == 200
    assert first.json()["created"] is True
    assert first.json()["enabled"] is False

    second = _put(client, {"scope": "channel", "target": "email", "enabled": True})
    assert second.status_code == 200
    assert second.json()["created"] is False
    assert second.json()["enabled"] is True

    payload = client.get(BASE).json()
    assert len(payload["items"]) == 1, "覆盖写不该留下两条记录"
    assert payload["items"][0]["enabled"] is True


def test_put_advances_updated_at(client):
    first = _put(client, {"scope": "global", "enabled": False}).json()
    second = _put(client, {"scope": "global", "enabled": True}).json()
    assert second["updated_at"] >= first["updated_at"]


def test_put_accepts_global_without_target(client):
    response = _put(client, {"scope": "global"})
    assert response.status_code == 200
    # 不写 enabled 时按接收处理，与模型默认值一致。
    assert response.json()["enabled"] is True
    assert response.json()["target"] is None


def test_put_rejects_target_on_global_scope(client):
    response = _put(client, {"scope": "global", "target": "post_liked"})
    assert response.status_code == 422


@pytest.mark.parametrize("scope", ["workflow", "channel"])
def test_put_requires_target_for_scoped_levels(client, scope):
    response = _put(client, {"scope": scope})
    assert response.status_code == 422


@pytest.mark.parametrize("target", ["delay", "digest"])
def test_put_rejects_orchestration_steps_as_channel(client, target):
    response = _put(client, {"scope": "channel", "target": target, "enabled": False})
    assert response.status_code == 422
    assert "编排步骤" in response.json()["detail"][0]["msg"]


def test_put_rejects_unknown_channel(client):
    response = _put(client, {"scope": "channel", "target": "carrier_pigeon"})
    assert response.status_code == 422


def test_put_rejects_unknown_scope(client):
    response = _put(client, {"scope": "audience"})
    assert response.status_code == 422


def test_put_rejects_blank_workflow_name(client):
    response = _put(client, {"scope": "workflow", "target": "   "})
    assert response.status_code == 422


def test_preferences_are_isolated_between_subscribers(client, subscribers):
    subscribers.create(_subscriber("creator_2"))
    _put(client, {"scope": "global", "enabled": False})
    payload = client.get("/v1/subscribers/creator_2/preferences").json()
    assert payload["items"] == []


# ---------------------------------------------------------------- 归属与环境


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", GHOST),
        ("get", f"{GHOST}/resolve"),
        ("delete", GHOST),
    ],
)
def test_unknown_subscriber_returns_404(client, method, path):
    response = getattr(client, method)(path)
    assert response.status_code == 404


def test_put_unknown_subscriber_returns_404(client, preferences):
    response = _put(client, {"scope": "global"}, subscriber="ghost")
    assert response.status_code == 404
    # 归属校验在写入之前，不能让一份孤儿记录先落进存储。
    assert len(preferences) == 0


def test_subscriber_of_another_environment_is_not_found(client):
    response = client.get("/v1/subscribers/creator_prod/preferences")
    assert response.status_code == 404

    response = client.get(
        "/v1/subscribers/creator_1/preferences",
        headers={"X-Environment": "production"},
    )
    assert response.status_code == 404, "环境不符不能读到开发环境的订阅者"


def test_production_subscriber_reads_with_production_header(client):
    response = client.get(
        "/v1/subscribers/creator_prod/preferences",
        headers={"X-Environment": "production"},
    )
    assert response.status_code == 200
    assert response.json()["subscriber_id"] == "creator_prod"


# ---------------------------------------------------------------- 删除


def test_delete_single_record(client):
    _put(client, {"scope": "channel", "target": "sms", "enabled": False})
    _put(client, {"scope": "global", "enabled": False})

    response = client.delete(BASE, params={"scope": "channel", "target": "sms"})
    assert response.status_code == 200
    assert response.json() == {"removed": 1}

    payload = client.get(BASE).json()
    assert [(item["scope"], item["target"]) for item in payload["items"]] == [
        ("global", None)
    ]

    again = client.delete(BASE, params={"scope": "channel", "target": "sms"})
    assert again.json() == {"removed": 0}, "删一条本来不存在的记录不算错误"


def test_delete_without_scope_clears_everything(client):
    _put(client, {"scope": "global", "enabled": False})
    _put(client, {"scope": "workflow", "target": "post_liked", "enabled": False})
    _put(client, {"scope": "channel", "target": "email", "enabled": True})

    response = client.delete(BASE)
    assert response.json() == {"removed": 3}
    assert client.get(BASE).json()["items"] == []


def test_delete_rejects_target_without_scope(client):
    response = client.delete(BASE, params={"target": "email"})
    assert response.status_code == 422


def test_delete_rejects_target_on_global_scope(client):
    response = client.delete(BASE, params={"scope": "global", "target": "email"})
    assert response.status_code == 422


def test_delete_rejects_unknown_scope(client):
    response = client.delete(BASE, params={"scope": "audience"})
    assert response.status_code == 422


# ---------------------------------------------------------------- 解析


def test_resolve_without_records_allows_by_default(client):
    payload = client.get(f"{BASE}/resolve").json()
    assert payload["allowed"] is True
    assert payload["matched"] is False
    assert payload["scope"] is None
    assert "默认接收" in payload["reason"]


def test_resolve_global_switch_denies(client):
    _put(client, {"scope": "global", "enabled": False})
    payload = client.get(f"{BASE}/resolve").json()
    assert payload["allowed"] is False
    assert payload["scope"] == "global"
    assert payload["matched"] is True


def test_resolve_workflow_override_wins_over_global(client):
    _put(client, {"scope": "global", "enabled": False})
    _put(client, {"scope": "workflow", "target": "post_liked", "enabled": True})

    payload = client.get(f"{BASE}/resolve", params={"workflow": "post_liked"}).json()
    assert payload["allowed"] is True
    assert payload["scope"] == "workflow"
    assert payload["target"] == "post_liked"

    # 没设过的工作流继续落到全局总开关。
    other = client.get(f"{BASE}/resolve", params={"workflow": "post_commented"}).json()
    assert other["allowed"] is False
    assert other["scope"] == "global"


def test_resolve_channel_is_the_most_specific(client):
    _put(client, {"scope": "global", "enabled": False})
    _put(client, {"scope": "workflow", "target": "post_liked", "enabled": False})
    _put(client, {"scope": "channel", "target": "email", "enabled": True})

    payload = client.get(
        f"{BASE}/resolve", params={"workflow": "post_liked", "channel": "email"}
    ).json()
    assert payload["allowed"] is True
    assert payload["scope"] == "channel"
    assert "渠道 email 已开启" in payload["reason"]


def test_resolve_ignores_channel_record_when_channel_not_asked(client):
    _put(client, {"scope": "channel", "target": "email", "enabled": False})
    payload = client.get(f"{BASE}/resolve", params={"workflow": "post_liked"}).json()
    assert payload["allowed"] is True
    assert payload["matched"] is False


@pytest.mark.parametrize("channel", ["delay", "carrier_pigeon"])
def test_resolve_rejects_invalid_channel(client, channel):
    response = client.get(f"{BASE}/resolve", params={"channel": channel})
    assert response.status_code == 422


def test_resolve_rejects_blank_workflow_name(client):
    response = client.get(f"{BASE}/resolve", params={"workflow": "post liked"})
    assert response.status_code == 422


# ---------------------------------------------------------------- 落盘


def test_write_through_jsonl_store(tmp_path, monkeypatch):
    """接口写入要真的落到文件上：换一个存储实例仍能读到。"""
    from app.preferences import JsonlPreferenceStore

    store = JsonlPreferenceStore(tmp_path / "preferences.jsonl")
    monkeypatch.setattr(
        "app.preference_routes.get_subscriber_store",
        lambda: MemorySubscriberStore([_subscriber("creator_1")]),
    )
    monkeypatch.setattr("app.preference_routes.get_preference_store", lambda: store)

    with TestClient(app) as test_client:
        response = test_client.put(
            BASE, json={"scope": "channel", "target": "email", "enabled": False}
        )
        assert response.status_code == 200

    reopened = JsonlPreferenceStore(tmp_path / "preferences.jsonl")
    record = reopened.get("creator_1", Scope.CHANNEL, "email")
    assert record is not None
    assert record.enabled is False
