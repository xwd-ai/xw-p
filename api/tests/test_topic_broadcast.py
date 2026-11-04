"""按分组广播的测试。

分三层：存储新增的 ``member_ids``（顺序与缺失）、广播展开本身（每个成员一条
消息、跳过两类成员、环境兜底）、接口语义（404 分支、整次幂等、空分组）。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app import config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app
from app.models.channel import Channel
from app.models.entities import Environment, Step, Subscriber, Topic, Workflow
from app.subscriber_store import MemorySubscriberStore
from app.topic_broadcast import (
    STATUS_ACCEPTED,
    STATUS_SKIPPED,
    broadcast_to_topic,
)
from app.topic_store import (
    JsonlTopicStore,
    MemoryTopicStore,
    TopicNotFoundError,
)
from app.workflow import get_registry
from app.workflow_store import MemoryWorkflowStore

BASE = "/v1/topics/hot_authors/trigger"


def _topic(topic_id: str = "hot_authors", **kwargs) -> Topic:
    return Topic(id=topic_id, name=kwargs.pop("name", topic_id), **kwargs)


def _creator(
    subscriber_id: str,
    *,
    email: str | None = None,
    phone: str | None = None,
    device_token: str | None = None,
    channels: list[Channel] | None = None,
    environment: Environment = Environment.DEVELOPMENT,
) -> Subscriber:
    return Subscriber(
        id=subscriber_id,
        email=email,
        phone=phone,
        device_token=device_token,
        channels=channels if channels is not None else [],
        environment=environment,
    )


# ---------------------------------------------------------------- 存储：名单


def test_member_ids_follow_join_order():
    store = MemoryTopicStore([_topic("hot_authors")])
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    store.add_member("hot_authors", "creator_2", added_at=base + timedelta(minutes=5))
    store.add_member("hot_authors", "creator_1", added_at=base)
    store.add_member("hot_authors", "creator_3", added_at=base + timedelta(minutes=9))

    assert store.member_ids("hot_authors") == ["creator_1", "creator_2", "creator_3"]


def test_member_ids_on_jsonl_store_matches_memory(tmp_path):
    path = tmp_path / "topics.jsonl"
    memory = MemoryTopicStore([_topic("hot_authors")])
    store = JsonlTopicStore(path)
    store.create(_topic("hot_authors"))
    for subscriber_id in ("creator_2", "creator_1"):
        memory.add_member("hot_authors", subscriber_id)
        store.add_member("hot_authors", subscriber_id)

    assert store.member_ids("hot_authors") == memory.member_ids("hot_authors")

    # 换一个实例读同一份文件，名单与顺序都不变。
    assert JsonlTopicStore(path).member_ids("hot_authors") == ["creator_2", "creator_1"]


def test_member_ids_rejects_missing_topic():
    with pytest.raises(TopicNotFoundError):
        MemoryTopicStore().member_ids("missing")


# ---------------------------------------------------------------- 广播展开


def _workflow():
    workflow = get_registry().get("post_liked")
    assert workflow is not None
    return workflow


def test_broadcast_builds_one_message_per_member():
    topics = MemoryTopicStore([_topic("hot_authors")])
    for subscriber_id in ("creator_1", "creator_2"):
        topics.add_member("hot_authors", subscriber_id)
    subscribers = MemorySubscriberStore(
        [
            _creator("creator_1", email="a@example.com", channels=[Channel.EMAIL, Channel.IN_APP]),
            _creator("creator_2", channels=[Channel.IN_APP]),
        ]
    )

    result = broadcast_to_topic(
        topic=topics.get("hot_authors"),
        workflow=_workflow(),
        topic_store=topics,
        subscriber_store=subscribers,
        environment=Environment.DEVELOPMENT,
        payload={"post_id": "P-1", "post_title": "第一支 Vlog", "actor": "小林"},
    )

    assert result.member_count == 2
    assert result.accepted == 2
    assert result.skipped == 0
    assert [m.subscriber_id for m in result.messages] == ["creator_1", "creator_2"]

    first, second = result.messages
    # 地址来自各自的档案，不是请求体；渲染变量原样带到每条消息里。
    assert first.email == "a@example.com"
    assert first.channels == ["email", "in_app"]
    assert second.email is None
    assert second.channels == ["in_app"]
    assert first.payload["post_title"] == "第一支 Vlog"
    # 一条消息一个触发标识，活动日志与消息一一对应。
    assert len({m.trigger_id for m in result.messages}) == 2
    assert [a["trigger_id"] for a in result.activities] == [
        m.trigger_id for m in result.messages
    ]


def test_broadcast_skips_member_without_profile():
    topics = MemoryTopicStore([_topic("hot_authors")])
    topics.add_member("hot_authors", "creator_1")
    topics.add_member("hot_authors", "ghost")
    subscribers = MemorySubscriberStore([_creator("creator_1", channels=[Channel.IN_APP])])

    result = broadcast_to_topic(
        topic=topics.get("hot_authors"),
        workflow=_workflow(),
        topic_store=topics,
        subscriber_store=subscribers,
        environment=Environment.DEVELOPMENT,
    )

    assert result.accepted == 1
    assert result.skipped == 1
    ghost = result.items[1]
    assert ghost.subscriber_id == "ghost"
    assert ghost.status == STATUS_SKIPPED
    assert "档案不存在" in ghost.error
    assert ghost.trigger_id is None


def test_broadcast_skips_member_without_deliverable_channel():
    topics = MemoryTopicStore([_topic("hot_authors")])
    topics.add_member("hot_authors", "quiet")
    topics.add_member("hot_authors", "no_address")
    subscribers = MemorySubscriberStore(
        [
            # 全部渠道都关掉了。
            _creator("quiet"),
            # 说了要收邮件，但没填邮箱：地址缺失，不算可投递。
            _creator("no_address", channels=[Channel.EMAIL]),
        ]
    )

    result = broadcast_to_topic(
        topic=topics.get("hot_authors"),
        workflow=_workflow(),
        topic_store=topics,
        subscriber_store=subscribers,
        environment=Environment.DEVELOPMENT,
    )

    assert result.accepted == 0
    assert [item.status for item in result.items] == [STATUS_SKIPPED, STATUS_SKIPPED]
    assert all("没有可投递的渠道" in item.error for item in result.items)


def test_broadcast_skips_member_from_other_environment():
    topics = MemoryTopicStore([_topic("hot_authors")])
    topics.add_member("hot_authors", "creator_1")
    subscribers = MemorySubscriberStore(
        [
            _creator(
                "creator_1",
                channels=[Channel.IN_APP],
                environment=Environment.PRODUCTION,
            )
        ]
    )

    result = broadcast_to_topic(
        topic=topics.get("hot_authors"),
        workflow=_workflow(),
        topic_store=topics,
        subscriber_store=subscribers,
        environment=Environment.DEVELOPMENT,
    )

    assert result.accepted == 0
    assert "另一个环境" in result.items[0].error


def test_broadcast_keeps_indices_aligned_with_member_list():
    topics = MemoryTopicStore([_topic("hot_authors")])
    for subscriber_id in ("creator_1", "ghost", "creator_2"):
        topics.add_member("hot_authors", subscriber_id)
    subscribers = MemorySubscriberStore(
        [
            _creator("creator_1", channels=[Channel.IN_APP]),
            _creator("creator_2", channels=[Channel.PUSH], device_token="fcm-token-1"),
        ]
    )

    result = broadcast_to_topic(
        topic=topics.get("hot_authors"),
        workflow=_workflow(),
        topic_store=topics,
        subscriber_store=subscribers,
        environment=Environment.DEVELOPMENT,
    )

    # 回执与名单等长、按下标对齐：调用方不必猜「第几条对应谁」。
    assert [item.index for item in result.items] == [0, 1, 2]
    assert [item.subscriber_id for item in result.items] == [
        "creator_1",
        "ghost",
        "creator_2",
    ]
    assert [item.status for item in result.items] == [
        STATUS_ACCEPTED,
        STATUS_SKIPPED,
        STATUS_ACCEPTED,
    ]


# ---------------------------------------------------------------- 接口


class _CountingBus(MemoryMessageBus):
    """统计入队方式：整次广播应只走一次 publish_many。"""

    def __init__(self) -> None:
        super().__init__()
        self.bulk_calls = 0
        self.single_calls = 0

    def publish(self, message) -> None:
        self.single_calls += 1
        super().publish(message)

    def publish_many(self, messages) -> int:
        self.bulk_calls += 1
        return super().publish_many(messages)


class _ActivityStore:
    def __init__(self) -> None:
        self.records: list[dict] = []
        self.bulk_calls = 0

    def append(self, record: dict) -> None:
        self.records.append(record)

    def append_many(self, records: list[dict]) -> int:
        self.bulk_calls += 1
        self.records.extend(records)
        return len(records)

    def read_all(self) -> list[dict]:
        return list(self.records)


@pytest.fixture()
def wired(monkeypatch):
    """隔离测试环境：内存总线、内存幂等、内存订阅者与分组存储。"""
    import app.activity_log as activity_mod
    import app.idempotency as idem_mod
    import app.topic_routes as topic_mod
    import app.trigger_core as trigger_core

    bus = _CountingBus()
    activities = _ActivityStore()
    subscribers = MemorySubscriberStore(
        [
            _creator("creator_1", email="a@example.com", channels=[Channel.EMAIL, Channel.IN_APP]),
            _creator("creator_2", channels=[Channel.IN_APP]),
            _creator("creator_3", phone="+8613800000000", channels=[Channel.SMS]),
            _creator(
                "creator_prod",
                channels=[Channel.IN_APP],
                environment=Environment.PRODUCTION,
            ),
        ]
    )
    topics = MemoryTopicStore([_topic("hot_authors")])
    for subscriber_id in ("creator_1", "creator_2", "creator_3"):
        topics.add_member("hot_authors", subscriber_id)

    # 生产环境的一份同名单步工作流：验证广播按请求环境解析模板，
    # 而不是不论哪个环境都拿预置的那份开发环境模板。
    prod_workflow = Workflow(
        id="wf-post-liked-prod",
        name="post_liked",
        steps=[Step(id="step-1", type=Channel.IN_APP, name="站内信", config={})],
        environment=Environment.PRODUCTION,
    )
    workflows = MemoryWorkflowStore([prod_workflow])

    monkeypatch.setattr(topic_mod, "get_bus", lambda: bus)
    monkeypatch.setattr(topic_mod, "get_topic_store", lambda: topics)
    monkeypatch.setattr(topic_mod, "get_subscriber_store", lambda: subscribers)
    monkeypatch.setattr(trigger_core, "get_workflow_store", lambda: workflows)
    monkeypatch.setattr(idem_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(activity_mod, "_activity_store", activities)
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)

    class Rig:
        client = TestClient(app)
        store = topics
        subs = subscribers
        bus_ref = bus
        activity_store = activities

    return Rig()


def _drain(bus: MemoryMessageBus) -> list:
    out = []
    while True:
        message = bus.consume(timeout=0.05)
        if message is None:
            return out
        out.append(message)


def _broadcast_body(name: str = "post_liked", **payload) -> dict:
    return {
        "name": name,
        "payload": {"post_id": "P-2026-1001", "post_title": "我的第一支 Vlog", **payload},
    }


def test_trigger_topic_enqueues_one_message_per_member(wired):
    resp = wired.client.post(BASE, json=_broadcast_body(actor="小林"))
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["acknowledged"] is True
    assert body["topicId"] == "hot_authors"
    assert body["name"] == "post_liked"
    assert body["memberCount"] == 3
    assert body["accepted"] == 3
    assert body["skipped"] == 0
    assert [item["subscriberId"] for item in body["results"]] == [
        "creator_1",
        "creator_2",
        "creator_3",
    ]

    # 整次广播各一次批量写入，而不是每人一次往返。
    assert wired.bus_ref.bulk_calls == 1
    assert wired.bus_ref.single_calls == 0
    assert wired.activity_store.bulk_calls == 1
    assert len(wired.activity_store.records) == 3

    messages = _drain(wired.bus_ref)
    assert len(messages) == 3
    by_subscriber = {message.subscriber_id: message for message in messages}
    assert by_subscriber["creator_1"].email == "a@example.com"
    assert by_subscriber["creator_1"].channels == ["email", "in_app"]
    assert by_subscriber["creator_3"].phone == "+8613800000000"
    assert by_subscriber["creator_3"].channels == ["sms"]
    assert by_subscriber["creator_2"].payload["actor"] == "小林"
    # 步骤快照随消息下发，引擎不必回查工作流存储。
    assert by_subscriber["creator_2"].workflow is not None
    assert by_subscriber["creator_2"].workflow.name == "post_liked"
    assert by_subscriber["creator_2"].workflow.steps


def test_trigger_topic_reports_skipped_members(wired):
    # 成员关系留着、档案被删掉：广播要能跳过并说明，而不是发一条没有地址的消息。
    wired.subs.delete("creator_2")
    wired.store.add_member("hot_authors", "ghost")

    resp = wired.client.post(BASE, json=_broadcast_body())
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["memberCount"] == 4
    assert body["accepted"] == 2
    assert body["skipped"] == 2
    skipped = [item for item in body["results"] if item["status"] == "skipped"]
    assert {item["subscriberId"] for item in skipped} == {"creator_2", "ghost"}
    assert all(item["triggerId"] is None for item in skipped)
    assert len(_drain(wired.bus_ref)) == 2


def test_trigger_topic_with_empty_group_publishes_nothing(wired):
    wired.store.create(_topic("empty_group"))
    resp = wired.client.post("/v1/topics/empty_group/trigger", json=_broadcast_body())

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["memberCount"] == 0
    assert body["accepted"] == 0
    assert body["skipped"] == 0
    assert body["results"] == []
    assert wired.bus_ref.bulk_calls == 0
    assert wired.bus_ref.single_calls == 0


def test_trigger_topic_is_idempotent(wired):
    headers = {"Idempotency-Key": "broadcast-P-2026-1001"}
    first = wired.client.post(BASE, json=_broadcast_body(), headers=headers)
    second = wired.client.post(BASE, json=_broadcast_body(), headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    # 重放只回执、不再入队：整次广播只发生一次批量写入。
    assert wired.bus_ref.bulk_calls == 1
    assert len(_drain(wired.bus_ref)) == 3


def test_trigger_topic_rejects_unknown_topic(wired):
    resp = wired.client.post("/v1/topics/missing/trigger", json=_broadcast_body())
    assert resp.status_code == 404


def test_trigger_topic_hides_topic_from_other_environment(wired):
    wired.store.create(_topic("prod_group", environment=Environment.PRODUCTION))
    wired.store.add_member("prod_group", "creator_prod")

    resp = wired.client.post("/v1/topics/prod_group/trigger", json=_broadcast_body())
    assert resp.status_code == 404

    # 带上生产环境头就能广播，收件人也取生产环境的那份档案。
    prod = wired.client.post(
        "/v1/topics/prod_group/trigger",
        json=_broadcast_body(),
        headers={"X-Environment": "production"},
    )
    assert prod.status_code == 200, prod.text
    assert prod.json()["accepted"] == 1


def test_trigger_topic_rejects_unknown_workflow(wired):
    resp = wired.client.post(BASE, json=_broadcast_body(name="post_unheard_of"))
    assert resp.status_code == 404
    assert "post_unheard_of" in resp.json()["detail"]
    assert wired.bus_ref.bulk_calls == 0


def test_trigger_topic_requires_workflow_name(wired):
    resp = wired.client.post(BASE, json={"name": "   ", "payload": {}})
    assert resp.status_code == 422
