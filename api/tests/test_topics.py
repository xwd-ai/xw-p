"""分组（Topic）接口与存储的测试。

覆盖三层：存储行为（内存 / JSONL 一致、成员集合语义、脏行、跨实例可见）、
接口语义（环境隔离、404/409/422 分支、幂等加入），以及并发加入同一分组。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.entities import Environment, Subscriber, Topic
from app.subscriber_store import MemorySubscriberStore
from app.topic_store import (
    DEFAULT_MAX_MEMBERS,
    JsonlTopicStore,
    MemoryTopicStore,
    TopicCapacityError,
    TopicConflictError,
    TopicCursorError,
    TopicNotFoundError,
    create_topic_store,
)


def _topic(topic_id: str = "hot_authors", **kwargs) -> Topic:
    return Topic(id=topic_id, name=kwargs.pop("name", topic_id), **kwargs)


def _subscriber(subscriber_id: str = "creator_1", **kwargs) -> Subscriber:
    return Subscriber(id=subscriber_id, **kwargs)


# ---------------------------------------------------------------- 存储：基础


def test_create_and_get():
    store = MemoryTopicStore()
    created = store.create(_topic("hot_authors", description="活跃作者"))
    assert created.id == "hot_authors"
    assert store.get("hot_authors").description == "活跃作者"
    assert store.get("missing") is None
    assert len(store) == 1


def test_create_duplicate_conflicts():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    with pytest.raises(TopicConflictError):
        store.create(_topic("hot_authors"))


def test_update_and_missing():
    store = MemoryTopicStore()
    topic = store.create(_topic("hot_authors"))
    saved = store.update(topic.model_copy(update={"name": "热门作者"}))
    assert saved.name == "热门作者"
    assert store.get("hot_authors").name == "热门作者"
    with pytest.raises(TopicNotFoundError):
        store.update(_topic("missing"))


def test_delete_removes_members():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "creator_1")
    assert store.member_count("hot_authors") == 1
    assert store.delete("hot_authors") is True
    assert store.get("hot_authors") is None
    assert store.topics_of("creator_1") == []
    assert store.delete("hot_authors") is False


# ---------------------------------------------------------------- 存储：列表


def test_list_orders_by_creation_and_paginates():
    store = MemoryTopicStore()
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    for index in range(5):
        store.create(
            Topic(
                id=f"topic_{index}",
                name=f"分组 {index}",
                created_at=base + timedelta(seconds=index),
                updated_at=base + timedelta(seconds=index),
            )
        )

    first, cursor = store.list(limit=2)
    assert [t.id for t in first] == ["topic_0", "topic_1"]
    second, cursor = store.list(limit=2, cursor=cursor)
    assert [t.id for t in second] == ["topic_2", "topic_3"]
    third, cursor = store.list(limit=2, cursor=cursor)
    assert [t.id for t in third] == ["topic_4"]
    assert cursor is None


def test_list_filters_by_environment():
    store = MemoryTopicStore()
    store.create(_topic("dev_topic"))
    store.create(_topic("prod_topic", environment=Environment.PRODUCTION))

    dev, _ = store.list(environment=Environment.DEVELOPMENT)
    prod, _ = store.list(environment=Environment.PRODUCTION)
    everything, _ = store.list()
    assert [t.id for t in dev] == ["dev_topic"]
    assert [t.id for t in prod] == ["prod_topic"]
    assert len(everything) == 2


def test_list_rejects_unknown_cursor():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    with pytest.raises(TopicCursorError):
        store.list(cursor="gone")


def test_member_count_missing_topic():
    store = MemoryTopicStore()
    with pytest.raises(TopicNotFoundError):
        store.member_count("missing")


# ---------------------------------------------------------------- 存储：成员


def test_add_member_is_idempotent_and_keeps_first_timestamp():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    first = datetime(2026, 3, 1, tzinfo=timezone.utc)
    later = datetime(2026, 3, 2, tzinfo=timezone.utc)

    assert store.add_member("hot_authors", "creator_1", first) is True
    assert store.add_member("hot_authors", "creator_1", later) is False
    members, cursor, total = store.members("hot_authors")
    assert total == 1
    assert members[0][0] == "creator_1"
    assert members[0][1] == first
    assert cursor is None


def test_add_member_to_missing_topic():
    store = MemoryTopicStore()
    with pytest.raises(TopicNotFoundError):
        store.add_member("missing", "creator_1")


def test_remove_member():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "creator_1")
    assert store.remove_member("hot_authors", "creator_1") is True
    assert store.remove_member("hot_authors", "creator_1") is False
    assert store.member_count("hot_authors") == 0


def test_members_paginate_and_total_is_full_count():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    for index in range(5):
        store.add_member("hot_authors", f"creator_{index}", base + timedelta(seconds=index))

    first, cursor, total = store.members("hot_authors", limit=2)
    assert [sid for sid, _ in first] == ["creator_0", "creator_1"]
    assert total == 5
    second, cursor, total = store.members("hot_authors", limit=2, cursor=cursor)
    assert [sid for sid, _ in second] == ["creator_2", "creator_3"]
    third, cursor, total = store.members("hot_authors", limit=2, cursor=cursor)
    assert [sid for sid, _ in third] == ["creator_4"]
    assert cursor is None and total == 5


def test_members_reject_unknown_cursor():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    with pytest.raises(TopicCursorError):
        store.members("hot_authors", cursor="ghost")


def test_topics_of_lists_all_groups():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    store.create(_topic("vlog_creators"))
    store.add_member("hot_authors", "creator_1")
    store.add_member("vlog_creators", "creator_1")
    store.add_member("vlog_creators", "creator_2")
    assert store.topics_of("creator_1") == ["hot_authors", "vlog_creators"]
    assert store.topics_of("creator_2") == ["vlog_creators"]
    assert store.topics_of("nobody") == []


def test_concurrent_adds_keep_one_membership():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))
    results: list[bool] = []
    lock = threading.Lock()

    def join() -> None:
        added = store.add_member("hot_authors", "creator_1")
        with lock:
            results.append(added)

    threads = [threading.Thread(target=join) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results.count(True) == 1
    assert store.member_count("hot_authors") == 1


def test_concurrent_adds_of_distinct_members_are_not_lost():
    store = MemoryTopicStore()
    store.create(_topic("hot_authors"))

    def join(index: int) -> None:
        for step in range(20):
            store.add_member("hot_authors", f"creator_{index}_{step}")

    threads = [threading.Thread(target=join, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert store.member_count("hot_authors") == 160


# ---------------------------------------------------------------- 存储：JSONL


@pytest.fixture()
def topic_file(tmp_path):
    return tmp_path / "topics.jsonl"


def test_jsonl_round_trip(topic_file):
    store = JsonlTopicStore(topic_file)
    store.create(_topic("hot_authors", description="活跃作者"))
    store.add_member("hot_authors", "creator_1")
    store.add_member("hot_authors", "creator_2")

    reopened = JsonlTopicStore(topic_file)
    topic = reopened.get("hot_authors")
    assert topic.description == "活跃作者"
    assert reopened.member_count("hot_authors") == 2
    assert reopened.topics_of("creator_1") == ["hot_authors"]


def test_jsonl_writes_two_record_kinds(topic_file):
    store = JsonlTopicStore(topic_file)
    store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "creator_1")

    rows = [json.loads(line) for line in topic_file.read_text(encoding="utf-8").splitlines()]
    assert [row["type"] for row in rows] == ["topic", "member"]
    assert rows[1]["topic_id"] == "hot_authors"
    assert rows[1]["subscriber_id"] == "creator_1"


def test_jsonl_update_and_delete_rewrite_file(topic_file):
    store = JsonlTopicStore(topic_file)
    topic = store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "creator_1")
    store.add_member("hot_authors", "creator_2")
    store.update(topic.model_copy(update={"name": "热门作者"}))
    assert store.remove_member("hot_authors", "creator_1") is True

    reopened = JsonlTopicStore(topic_file)
    assert reopened.get("hot_authors").name == "热门作者"
    assert reopened.topics_of("creator_1") == []
    assert reopened.member_count("hot_authors") == 1

    assert reopened.delete("hot_authors") is True
    assert topic_file.read_text(encoding="utf-8").strip() == ""


def test_jsonl_skips_broken_lines(topic_file):
    topic_file.write_text(
        "\n".join(
            [
                "",
                "not json",
                json.dumps({"type": "topic", "id": "hot_authors", "name": "作者"}),
                json.dumps({"type": "topic"}),
                json.dumps({"type": "member", "topic_id": "hot_authors"}),
                json.dumps({"type": "member", "topic_id": "hot_authors", "subscriber_id": "creator_1"}),
                json.dumps({"type": "unknown_future_kind"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    store = JsonlTopicStore(topic_file)
    assert store.get("hot_authors") is not None
    assert store.member_count("hot_authors") == 1


def test_jsonl_drops_orphan_members(topic_file):
    """分组行丢了、成员行还在时，孤儿成员不该让「分组不存在」和「有人挂着」并存。"""
    topic_file.write_text(
        json.dumps({"type": "member", "topic_id": "ghost", "subscriber_id": "creator_1"}) + "\n",
        encoding="utf-8",
    )
    store = JsonlTopicStore(topic_file)
    assert store.topics_of("creator_1") == []
    assert store.delete("ghost") is False


def test_jsonl_sees_writes_from_other_instance(topic_file):
    first = JsonlTopicStore(topic_file)
    first.create(_topic("hot_authors"))

    second = JsonlTopicStore(topic_file)
    second.add_member("hot_authors", "creator_9")

    assert first.topics_of("creator_9") == ["hot_authors"]
    assert first.member_count("hot_authors") == 1


def test_jsonl_duplicate_member_rows_keep_earliest(topic_file):
    early = datetime(2026, 3, 1, tzinfo=timezone.utc)
    late = datetime(2026, 3, 5, tzinfo=timezone.utc)
    topic_file.write_text(
        "\n".join(
            [
                json.dumps({"type": "topic", "id": "hot_authors", "name": "作者"}),
                json.dumps(
                    {
                        "type": "member",
                        "topic_id": "hot_authors",
                        "subscriber_id": "creator_1",
                        "added_at": late.isoformat(),
                    }
                ),
                json.dumps(
                    {
                        "type": "member",
                        "topic_id": "hot_authors",
                        "subscriber_id": "creator_1",
                        "added_at": early.isoformat(),
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    store = JsonlTopicStore(topic_file)
    members, _, total = store.members("hot_authors")
    assert total == 1
    assert members[0][1] == early


def test_jsonl_add_member_after_external_append(topic_file):
    """成员加入走追加写，另一个实例追加过之后本实例不该丢掉它。"""
    first = JsonlTopicStore(topic_file)
    first.create(_topic("hot_authors"))
    second = JsonlTopicStore(topic_file)
    first.add_member("hot_authors", "creator_1")
    second.add_member("hot_authors", "creator_2")

    reopened = JsonlTopicStore(topic_file)
    assert reopened.member_count("hot_authors") == 2


# ---------------------------------------------------------------- 接口


@pytest.fixture()
def topic_store():
    return MemoryTopicStore()


@pytest.fixture()
def subscriber_store():
    store = MemorySubscriberStore(
        [
            _subscriber("creator_1", channels=["in_app"]),
            _subscriber("creator_2", channels=["in_app"]),
            _subscriber("prod_creator", environment=Environment.PRODUCTION, channels=["in_app"]),
        ]
    )
    return store


@pytest.fixture()
def client(monkeypatch, topic_store, subscriber_store):
    monkeypatch.setattr("app.topic_routes.get_topic_store", lambda: topic_store)
    monkeypatch.setattr("app.topic_routes.get_subscriber_store", lambda: subscriber_store)
    with TestClient(app) as test_client:
        yield test_client


def test_api_create_topic(client):
    response = client.post("/v1/topics", json={"id": "hot_authors", "name": "热门作者", "description": "活跃作者"})
    assert response.status_code == 201
    body = response.json()
    assert body["id"] == "hot_authors"
    assert body["name"] == "热门作者"
    assert body["memberCount"] == 0
    assert body["environment"] == "development"
    assert body["createdAt"] == body["updatedAt"]


def test_api_create_topic_generates_id_and_defaults_name(client):
    response = client.post("/v1/topics", json={})
    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["name"] == body["id"]


def test_api_create_topic_conflict(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    response = client.post("/v1/topics", json={"id": "hot_authors"})
    assert response.status_code == 409


@pytest.mark.parametrize("bad_id", ["a/b", "a?b", "a#b", "a b", "x" * 129])
def test_api_create_topic_rejects_unaddressable_id(client, bad_id):
    response = client.post("/v1/topics", json={"id": bad_id})
    assert response.status_code == 422


def test_api_list_topics_filters_environment(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    client.post("/v1/topics", json={"id": "prod_topic"}, headers={"X-Environment": "production"})

    dev = client.get("/v1/topics").json()
    prod = client.get("/v1/topics", headers={"X-Environment": "production"}).json()
    assert [item["id"] for item in dev["items"]] == ["hot_authors"]
    assert [item["id"] for item in prod["items"]] == ["prod_topic"]


def test_api_list_topics_paginates(client):
    for index in range(3):
        client.post("/v1/topics", json={"id": f"topic_{index}"})

    first = client.get("/v1/topics", params={"limit": 2}).json()
    assert [item["id"] for item in first["items"]] == ["topic_0", "topic_1"]
    assert first["hasMore"] is True

    second = client.get("/v1/topics", params={"limit": 2, "cursor": first["nextCursor"]}).json()
    assert [item["id"] for item in second["items"]] == ["topic_2"]
    assert second["hasMore"] is False


def test_api_list_topics_invalid_cursor(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    response = client.get("/v1/topics", params={"cursor": "gone"})
    assert response.status_code == 400


def test_api_get_topic_and_cross_environment(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    assert client.get("/v1/topics/hot_authors").status_code == 200
    assert (
        client.get("/v1/topics/hot_authors", headers={"X-Environment": "production"}).status_code
        == 404
    )
    assert client.get("/v1/topics/missing").status_code == 404


def test_api_update_topic_partial(client):
    client.post("/v1/topics", json={"id": "hot_authors", "name": "作者", "description": "说明"})

    renamed = client.put("/v1/topics/hot_authors", json={"name": "热门作者"}).json()
    assert renamed["name"] == "热门作者"
    assert renamed["description"] == "说明"

    cleared = client.put("/v1/topics/hot_authors", json={"description": ""}).json()
    assert cleared["name"] == "热门作者"
    assert cleared["description"] is None

    assert client.put("/v1/topics/missing", json={"name": "x"}).status_code == 404


def test_api_delete_topic_removes_members(client, topic_store):
    client.post("/v1/topics", json={"id": "hot_authors"})
    client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"})

    assert client.delete("/v1/topics/hot_authors").status_code == 204
    assert client.get("/v1/topics/hot_authors").status_code == 404
    assert topic_store.topics_of("creator_1") == []
    assert client.delete("/v1/topics/hot_authors").status_code == 404


def test_api_add_member_is_idempotent(client):
    client.post("/v1/topics", json={"id": "hot_authors"})

    first = client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"})
    assert first.status_code == 200
    assert first.json() == {
        "topicId": "hot_authors",
        "subscriberId": "creator_1",
        "added": True,
        "memberCount": 1,
        "memberLimit": DEFAULT_MAX_MEMBERS,
    }

    second = client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"})
    assert second.json()["added"] is False
    assert second.json()["memberCount"] == 1


def test_api_add_member_requires_existing_subscriber(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    assert (
        client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "ghost"}).status_code
        == 404
    )
    # 订阅者属于另一个环境，对当前环境而言同样「不存在」。
    assert (
        client.post(
            "/v1/topics/hot_authors/subscribers", json={"subscriberId": "prod_creator"}
        ).status_code
        == 404
    )


def test_api_add_member_unknown_topic(client):
    response = client.post("/v1/topics/missing/subscribers", json={"subscriberId": "creator_1"})
    assert response.status_code == 404


def test_api_add_member_rejects_blank_id(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    response = client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "  "})
    assert response.status_code == 422


def test_api_list_members_reports_total(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    for subscriber_id in ("creator_1", "creator_2"):
        client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": subscriber_id})

    body = client.get("/v1/topics/hot_authors/subscribers", params={"limit": 1}).json()
    assert body["topicId"] == "hot_authors"
    assert body["total"] == 2
    assert len(body["items"]) == 1
    assert body["hasMore"] is True
    assert body["items"][0]["addedAt"]

    rest = client.get(
        "/v1/topics/hot_authors/subscribers",
        params={"limit": 1, "cursor": body["nextCursor"]},
    ).json()
    assert len(rest["items"]) == 1
    assert rest["hasMore"] is False


def test_api_list_members_invalid_cursor(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    response = client.get("/v1/topics/hot_authors/subscribers", params={"cursor": "ghost"})
    assert response.status_code == 400


def test_api_remove_member(client, topic_store):
    client.post("/v1/topics", json={"id": "hot_authors"})
    client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"})

    assert client.delete("/v1/topics/hot_authors/subscribers/creator_1").status_code == 204
    assert topic_store.member_count("hot_authors") == 0
    assert client.delete("/v1/topics/hot_authors/subscribers/creator_1").status_code == 404
    assert client.delete("/v1/topics/missing/subscribers/creator_1").status_code == 404


def test_api_member_count_reflects_membership(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"})
    client.post("/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_2"})

    assert client.get("/v1/topics/hot_authors").json()["memberCount"] == 2
    listed = client.get("/v1/topics").json()["items"][0]
    assert listed["memberCount"] == 2


# ---------------------------------------------------------------- 成员容量


def test_member_capacity_rejects_extra():
    store = MemoryTopicStore(max_members=2)
    store.create(_topic("hot_authors"))
    assert store.add_member("hot_authors", "u1") is True
    assert store.add_member("hot_authors", "u2") is True

    with pytest.raises(TopicCapacityError) as excinfo:
        store.add_member("hot_authors", "u3")
    assert excinfo.value.topic_id == "hot_authors"
    assert excinfo.value.limit == 2
    assert excinfo.value.current == 2
    assert store.member_count("hot_authors") == 2


def test_member_capacity_duplicate_does_not_count():
    store = MemoryTopicStore(max_members=1)
    store.create(_topic("hot_authors"))
    assert store.add_member("hot_authors", "u1") is True

    # 幂等加入不占新名额，也不能因为「已经满了」而报错
    assert store.add_member("hot_authors", "u1") is False
    assert store.member_count("hot_authors") == 1


def test_member_capacity_freed_after_remove():
    store = MemoryTopicStore(max_members=1)
    store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "u1")
    with pytest.raises(TopicCapacityError):
        store.add_member("hot_authors", "u2")

    assert store.remove_member("hot_authors", "u1") is True
    assert store.add_member("hot_authors", "u2") is True


def test_member_capacity_jsonl_shared(tmp_path):
    path = tmp_path / "topics.jsonl"
    store = JsonlTopicStore(path, max_members=2)
    store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "u1")
    store.add_member("hot_authors", "u2")

    with pytest.raises(TopicCapacityError):
        store.add_member("hot_authors", "u3")

    # 另一个实例从同一份文件读回成员，容量判定与写入方一致
    other = JsonlTopicStore(path, max_members=2)
    assert other.member_count("hot_authors") == 2
    with pytest.raises(TopicCapacityError):
        other.add_member("hot_authors", "u3")


def test_member_capacity_concurrent_only_one_wins(tmp_path):
    store = JsonlTopicStore(tmp_path / "topics.jsonl", max_members=2)
    store.create(_topic("hot_authors"))
    store.add_member("hot_authors", "seed")

    added: list[bool] = []
    rejected: list[Exception] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            outcome = store.add_member("hot_authors", f"u{index}")
        except TopicCapacityError as exc:
            with lock:
                rejected.append(exc)
        else:
            with lock:
                added.append(outcome)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # 只剩一个名额，8 路并发只能有一个胜出；人数绝不能超过上限
    assert store.member_count("hot_authors") == 2
    assert added == [True]
    assert len(rejected) == 7


def test_member_limit_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("XW_TOPIC_MAX_MEMBERS", "3")
    store = create_topic_store("jsonl", tmp_path / "topics.jsonl")
    assert store.max_members == 3


def test_member_limit_env_invalid_falls_back(monkeypatch, tmp_path):
    monkeypatch.setenv("XW_TOPIC_MAX_MEMBERS", "not-a-number")
    store = create_topic_store("jsonl", tmp_path / "topics.jsonl")
    assert store.max_members == DEFAULT_MAX_MEMBERS


def test_api_add_member_over_capacity_returns_409(monkeypatch):
    store = MemoryTopicStore(max_members=1)
    store.create(_topic("hot_authors"))
    subscribers = MemorySubscriberStore(
        [
            _subscriber("creator_1", channels=["in_app"]),
            _subscriber("creator_2", channels=["in_app"]),
        ]
    )
    monkeypatch.setattr("app.topic_routes.get_topic_store", lambda: store)
    monkeypatch.setattr("app.topic_routes.get_subscriber_store", lambda: subscribers)

    with TestClient(app) as client:
        first = client.post(
            "/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"}
        )
        assert first.status_code == 200
        assert first.json()["memberLimit"] == 1

        second = client.post(
            "/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_2"}
        )
        assert second.status_code == 409
        assert "上限" in second.json()["detail"]


def test_api_membership_reports_limit(client):
    client.post("/v1/topics", json={"id": "hot_authors"})
    body = client.post(
        "/v1/topics/hot_authors/subscribers", json={"subscriberId": "creator_1"}
    ).json()
    assert body["memberLimit"] == DEFAULT_MAX_MEMBERS

    listed = client.get("/v1/topics/hot_authors/subscribers").json()
    assert listed["memberLimit"] == DEFAULT_MAX_MEMBERS
