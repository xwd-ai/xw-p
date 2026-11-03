"""订阅者 CRUD、渠道绑定与存储后端的测试。"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.channel import Channel
from app.models.entities import Environment, Subscriber
from app.subscriber_store import (
    JsonlSubscriberStore,
    MemorySubscriberStore,
    SubscriberConflictError,
    SubscriberCursorError,
    SubscriberNotFoundError,
    normalize_channels,
    subscriber_from_row,
    subscriber_to_row,
)


def _subscriber(subscriber_id: str = "creator_1", **kwargs) -> Subscriber:
    return Subscriber(id=subscriber_id, **kwargs)


@pytest.fixture()
def store():
    return MemorySubscriberStore()


@pytest.fixture()
def client(monkeypatch, store):
    monkeypatch.setattr("app.subscriber_routes.get_subscriber_store", lambda: store)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------- 渠道归一


def test_normalize_channels_dedupes_and_keeps_order():
    channels = normalize_channels(["email", "in_app", "email", Channel.PUSH])
    assert [c.value for c in channels] == ["email", "in_app", "push"]


@pytest.mark.parametrize("bad", ["delay", "digest"])
def test_normalize_channels_rejects_orchestration_steps(bad):
    with pytest.raises(ValueError):
        normalize_channels([bad])


def test_normalize_channels_rejects_unknown():
    with pytest.raises(ValueError):
        normalize_channels(["carrier_pigeon"])


def test_normalize_channels_accepts_single_value():
    assert normalize_channels("email") == [Channel.EMAIL]


# ---------------------------------------------------------------- 实体方法


def test_subscriber_binds_only_channels_with_address():
    subscriber = _subscriber(
        email="creator@example.com",
        channels=[Channel.IN_APP, Channel.EMAIL, Channel.SMS],
    )
    # in_app 不需要地址，邮箱有地址；短信启用了但没有手机号。
    assert subscriber.binds(Channel.IN_APP)
    assert subscriber.binds(Channel.EMAIL)
    assert not subscriber.binds(Channel.SMS)
    assert not subscriber.binds(Channel.PUSH)


def test_subscriber_row_roundtrip():
    subscriber = _subscriber(
        email="creator@example.com",
        phone="+8613800000000",
        channels=[Channel.EMAIL, Channel.SMS],
        environment=Environment.PRODUCTION,
    )
    restored = subscriber_from_row(subscriber_to_row(subscriber))
    assert restored is not None
    assert restored.id == subscriber.id
    assert restored.email == subscriber.email
    assert restored.phone == subscriber.phone
    assert [c.value for c in restored.channels] == ["email", "sms"]
    assert restored.environment is Environment.PRODUCTION


def test_subscriber_row_skips_missing_id():
    assert subscriber_from_row({"email": "a@b.c"}) is None
    assert subscriber_from_row("not-an-object") is None


# ---------------------------------------------------------------- 存储层


def test_memory_store_conflict_and_missing():
    store = MemorySubscriberStore()
    store.create(_subscriber())
    with pytest.raises(SubscriberConflictError):
        store.create(_subscriber())
    with pytest.raises(SubscriberNotFoundError):
        store.update(_subscriber("ghost"))
    assert store.delete("ghost") is False


def test_jsonl_store_persists_across_instances(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    first = JsonlSubscriberStore(path)
    first.create(_subscriber("creator_1", email="creator@example.com", channels=[Channel.EMAIL]))

    # 新实例只靠文件恢复，等价于进程重启。
    second = JsonlSubscriberStore(path)
    restored = second.get("creator_1")
    assert restored is not None and restored.email == "creator@example.com"

    updated = restored.model_copy(update={"email": "new@example.com"})
    second.update(updated)
    assert JsonlSubscriberStore(path).get("creator_1").email == "new@example.com"

    assert second.delete("creator_1") is True
    assert JsonlSubscriberStore(path).get("creator_1") is None
    assert path.read_text(encoding="utf-8") == ""


def test_jsonl_store_sees_writes_from_other_instance(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    a = JsonlSubscriberStore(path)
    b = JsonlSubscriberStore(path)

    a.create(_subscriber("creator_1", channels=[Channel.IN_APP]))
    # b 的索引是在 a 写入之前建的：读之前要按文件变化重扫。
    assert b.get("creator_1") is not None

    b.create(_subscriber("creator_2", channels=[Channel.EMAIL]))
    items, _ = a.list()
    assert sorted(s.id for s in items) == ["creator_1", "creator_2"]


def test_jsonl_store_skips_corrupt_lines(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    path.write_text(
        '{"id":"creator_1","channels":["email"]}\n'
        "not-json\n"
        '{"email":"no-id@example.com"}\n'
        '{"id":"creator_2","channels":["carrier_pigeon"]}\n',
        encoding="utf-8",
    )
    store = JsonlSubscriberStore(path)
    assert store.get("creator_1") is not None
    broken = store.get("creator_2")
    # 渠道被改坏时按「未绑定」处理，记录本身仍然读得出来。
    assert broken is not None and broken.channels == []


def test_jsonl_store_pagination_uses_cursor(tmp_path):
    store = JsonlSubscriberStore(tmp_path / "subscribers.jsonl")
    for index in range(3):
        store.create(_subscriber(f"creator_{index}"))
    first, cursor = store.list(limit=2)
    assert len(first) == 2 and cursor == first[-1].id
    rest, cursor2 = store.list(limit=2, cursor=cursor)
    assert [s.id for s in rest] == ["creator_2"]
    assert cursor2 is None
    with pytest.raises(SubscriberCursorError):
        store.list(cursor="ghost")


# ---------------------------------------------------------------- 接口


def test_create_subscriber_generates_id(client):
    response = client.post("/v1/subscribers", json={"channels": ["in_app"]})
    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["channels"] == ["in_app"]
    # 站内信不需要地址，标识本身就是收件人。
    assert body["bindings"] == {"in_app": body["id"]}
    assert body["environment"] == "development"
    assert body["created_at"] == body["updated_at"]


def test_create_subscriber_with_all_bindings(client):
    response = client.post(
        "/v1/subscribers",
        json={
            "id": "creator_42",
            "email": " creator@example.com ",
            "phone": "+8613800000000",
            "device_token": "fcm-token-42",
            "channels": ["in_app", "email", "sms", "push"],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["id"] == "creator_42"
    assert body["email"] == "creator@example.com"
    assert body["bindings"] == {
        "in_app": "creator_42",
        "email": "creator@example.com",
        "sms": "+8613800000000",
        "push": "fcm-token-42",
    }


def test_create_subscriber_duplicate_id_conflicts(client):
    payload = {"id": "creator_42", "channels": ["in_app"]}
    assert client.post("/v1/subscribers", json=payload).status_code == 201
    conflict = client.post("/v1/subscribers", json=payload)
    assert conflict.status_code == 409
    assert "creator_42" in conflict.json()["detail"]


def test_create_subscriber_rejects_orchestration_channel(client):
    response = client.post("/v1/subscribers", json={"channels": ["delay"]})
    assert response.status_code == 422


def test_create_subscriber_dedupes_channels(client):
    response = client.post(
        "/v1/subscribers", json={"channels": ["email", "email", "in_app"]}
    )
    assert response.status_code == 201
    assert response.json()["channels"] == ["email", "in_app"]


def test_get_subscriber(client):
    client.post("/v1/subscribers", json={"id": "creator_1", "channels": ["in_app"]})
    found = client.get("/v1/subscribers/creator_1")
    assert found.status_code == 200
    assert found.json()["id"] == "creator_1"

    missing = client.get("/v1/subscribers/ghost")
    assert missing.status_code == 404


def test_update_subscriber_only_touches_given_fields(client):
    client.post(
        "/v1/subscribers",
        json={
            "id": "creator_1",
            "email": "old@example.com",
            "phone": "+8613800000000",
            "channels": ["email", "sms"],
        },
    )
    response = client.put(
        "/v1/subscribers/creator_1",
        json={"email": "new@example.com", "channels": ["email"]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["email"] == "new@example.com"
    # 没提到的手机号保持原样，只是渠道已经不含 sms，因此不出现在可投递绑定里。
    assert body["phone"] == "+8613800000000"
    assert body["channels"] == ["email"]
    assert body["bindings"] == {"email": "new@example.com"}
    assert body["updated_at"] >= body["created_at"]


def test_update_subscriber_clears_address_with_empty_string(client):
    client.post(
        "/v1/subscribers",
        json={"id": "creator_1", "phone": "+8613800000000", "channels": ["sms"]},
    )
    body = client.put("/v1/subscribers/creator_1", json={"phone": ""}).json()
    assert body["phone"] is None
    assert body["bindings"] == {}


def test_update_subscriber_missing_returns_404(client):
    response = client.put("/v1/subscribers/ghost", json={"email": "a@b.c"})
    assert response.status_code == 404


def test_delete_subscriber(client):
    client.post("/v1/subscribers", json={"id": "creator_1", "channels": ["in_app"]})
    assert client.delete("/v1/subscribers/creator_1").status_code == 204
    assert client.get("/v1/subscribers/creator_1").status_code == 404
    assert client.delete("/v1/subscribers/creator_1").status_code == 404


def test_list_subscribers_paginates(client):
    for index in range(3):
        client.post("/v1/subscribers", json={"id": f"creator_{index}"})

    first = client.get("/v1/subscribers", params={"limit": 2}).json()
    assert len(first["items"]) == 2
    assert first["hasMore"] is True

    second = client.get(
        "/v1/subscribers", params={"limit": 2, "cursor": first["nextCursor"]}
    ).json()
    assert [item["id"] for item in second["items"]] == ["creator_2"]
    assert second["hasMore"] is False
    assert second["nextCursor"] is None


def test_list_subscribers_filters_by_channel(client):
    client.post("/v1/subscribers", json={"id": "mail_only", "channels": ["email"]})
    client.post("/v1/subscribers", json={"id": "app_only", "channels": ["in_app"]})
    items = client.get("/v1/subscribers", params={"channel": "email"}).json()["items"]
    assert [item["id"] for item in items] == ["mail_only"]


def test_list_subscribers_rejects_unknown_channel_and_cursor(client):
    assert client.get("/v1/subscribers", params={"channel": "pigeon"}).status_code == 422
    # 编排步骤不会挂在订阅者身上，按它过滤会得到空页，不如直接指出取值不对。
    assert client.get("/v1/subscribers", params={"channel": "digest"}).status_code == 422
    assert client.get("/v1/subscribers", params={"cursor": "ghost"}).status_code == 400
    assert client.get("/v1/subscribers", params={"limit": 0}).status_code == 422


def test_list_bindings_skip_channels_without_address(client):
    client.post(
        "/v1/subscribers",
        json={"id": "creator_1", "channels": ["email", "in_app"]},
    )
    body = client.get("/v1/subscribers/creator_1").json()
    # 启用了邮件却没填邮箱：渠道在列表里，但不算「绑定成功」。
    assert body["channels"] == ["email", "in_app"]
    assert body["bindings"] == {"in_app": "creator_1"}


def test_subscriber_written_to_file_survives_restart(client, monkeypatch, tmp_path):
    """接口层写入落到文件后，新存储实例能读回来（进程重启后的形态）。"""
    path = tmp_path / "subscribers.jsonl"
    file_store = JsonlSubscriberStore(path)
    monkeypatch.setattr("app.subscriber_routes.get_subscriber_store", lambda: file_store)

    assert (
        client.post(
            "/v1/subscribers",
            json={"id": "creator_7", "email": "creator@example.com", "channels": ["email"]},
        ).status_code
        == 201
    )
    client.put("/v1/subscribers/creator_7", json={"channels": ["email", "in_app"]})

    reloaded = JsonlSubscriberStore(path)
    restored = reloaded.get("creator_7")
    assert restored is not None
    assert [c.value for c in restored.channels] == ["email", "in_app"]
    # 落盘的行是合法 JSON，字段名与实体一致。
    row = json.loads(path.read_text(encoding="utf-8").strip())
    assert row["id"] == "creator_7"
    assert row["channels"] == ["email", "in_app"]
