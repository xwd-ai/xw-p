"""Phase 4 集成测试：订阅者 → 偏好 → 分组广播 → 投递判定 → 收件箱。

Phase 4 的四组能力（订阅者与渠道绑定、三级偏好、分组广播、收件箱）各自都有
单元测试，但接缝在单测里是断开的：偏好关掉一个渠道之后，执行明细、消息记录
的 ``channels``、收件箱未读、未读聚合这四处是不是同一口径？分组广播跳过一个
没有可投递渠道的成员之后，活动流与队列里还剩几条？这些只有把整条链路跑一遍
才能回答。

本文件用真实的 HTTP 接口加真实的 worker 串起这条链路：

1. 建订阅者（地址查重）→ 建分组、加成员 → 按分组广播；
2. worker 消费，先过投递前偏好判定，再逐渠道写执行明细与消息记录；
3. 站内信经 ``inapp`` 实现写进消息中心文件，收件箱按「引擎文件 + 投递记录」
   合并读取并去重；
4. 列表、未读聚合、分页、筛选、SSE 推送对同一份数据给出一致结论。

全局单例（总线、投递存储、活动日志、执行明细、幂等、订阅者 / 分组 / 偏好
存储、收件箱文件与已读状态）全部换成进程内实现或临时目录，跑完不写
``api/data`` 下的任何文件。
"""
from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from fastapi.testclient import TestClient

from app.activity_log import MemoryActivityStore
from app.bus import MemoryMessageBus
from app.execution_log import MemoryExecutionStore
from app.idempotency import MemoryIdempotencyStore
from app.inbox import iter_inbox_events
from app.main import app
from app.models.channel import Channel
from app.preferences import MemoryPreferenceStore
from app.providers import factory as provider_factory
from app.providers.factory import ProviderConfig
from app.subscriber_store import MemorySubscriberStore
from app.topic_store import MemoryTopicStore
from app.worker import Worker

# 广播用的互动事件：标题与正文会一路走到站内信与收件箱，因此用真实文案，
# 便于断言「收件箱里看到的确实是这条互动的渲染结果」。
POST_TITLE = "有人评论了你的帖子"
POST_BODY = "小柯：这段剪辑的节奏很棒，第二节的转场是怎么做的？"


class _RecordingStorage:
    """投递记录存储：进程内保存，并给出随写入变化的版本号。

    收件箱与未读聚合靠版本号判断缓存是否失效，所以这里必须实现 ``revision``：
    少了它就会退化成「每次都重算」，测不出「写入后缓存确实被换掉」。
    """

    def __init__(self) -> None:
        self._records: list[dict] = []
        self._version = 0
        self._lock = threading.Lock()

    def save(self, record: dict) -> None:
        with self._lock:
            self._records.append(record)
            self._version += 1

    def read_all(self) -> list[dict]:
        with self._lock:
            return list(self._records)

    def revision(self) -> str:
        with self._lock:
            return f"rev-{self._version}"


def _message_frame(frame: str) -> tuple[Optional[str], Optional[dict]]:
    """解析一帧 SSE 报文，返回 ``(事件名, 数据)``；注释心跳返回 ``(None, None)``。"""
    event: Optional[str] = None
    data: Optional[dict] = None
    for line in frame.splitlines():
        if line.startswith("event: "):
            event = line[len("event: ") :]
        elif line.startswith("data: "):
            data = json.loads(line[len("data: ") :])
    return event, data


@dataclass
class Rig:
    """一次集成测试用到的客户端与后端存储。"""

    client: TestClient
    bus: MemoryMessageBus
    storage: _RecordingStorage
    executions: MemoryExecutionStore

    # ------------------------------------------------------------ 驱动

    def drain(self) -> int:
        """把队列里的消息逐条交给 worker 处理，返回处理条数。

        TestClient 不走 lifespan，后台消费线程不会启动；显式驱动让每一步
        都发生在断言之间，顺序确定、失败点清楚。
        """
        worker = Worker()
        handled = 0
        while True:
            message = self.bus.consume(timeout=0.05)
            if message is None:
                return handled
            worker._process(message)
            handled += 1

    # ------------------------------------------------------------ 构造

    def subscriber(self, subscriber_id: str, **body) -> dict:
        payload = {"id": subscriber_id, **body}
        response = self.client.post("/v1/subscribers", json=payload)
        assert response.status_code == 201, response.text
        return response.json()

    def topic(self, topic_id: str, name: str = "") -> dict:
        response = self.client.post(
            "/v1/topics", json={"id": topic_id, "name": name or topic_id}
        )
        assert response.status_code == 201, response.text
        return response.json()

    def join(self, topic_id: str, subscriber_id: str) -> dict:
        response = self.client.post(
            f"/v1/topics/{topic_id}/subscribers",
            json={"subscriberId": subscriber_id},
        )
        assert response.status_code == 200, response.text
        return response.json()

    def broadcast(self, topic_id: str, name: str, payload: dict, **kwargs):
        return self.client.post(
            f"/v1/topics/{topic_id}/trigger",
            json={"name": name, "payload": payload},
            **kwargs,
        )

    def trigger(self, name: str, subscriber_id: str, channels: list[str], payload: dict):
        body = {
            "name": name,
            "to": {
                "subscriberId": subscriber_id,
                "channels": channels,
                "email": "creator@example.com",
                "phone": "+8613800000000",
                "deviceToken": "fcm-token-9f2c1d",
            },
            "payload": payload,
        }
        return self.client.post("/v1/events/trigger", json=body)

    # ------------------------------------------------------------ 读取

    def executions_of(self, trigger_id: str) -> list[dict]:
        response = self.client.get("/v1/executions", params={"triggerId": trigger_id})
        assert response.status_code == 200, response.text
        return response.json()["items"]

    def inbox(self, subscriber_id: str, **params) -> dict:
        response = self.client.get(
            "/v1/inbox", params={"subscriberId": subscriber_id, **params}
        )
        assert response.status_code == 200, response.text
        return response.json()

    def unread(self, subscriber_id: str) -> dict:
        response = self.client.get(
            "/v1/inbox/unread", params={"subscriberId": subscriber_id}
        )
        assert response.status_code == 200, response.text
        return response.json()

    def record_of(self, trigger_id: str) -> dict:
        records = [
            r for r in self.storage.read_all() if r.get("trigger_id") == trigger_id
        ]
        assert len(records) == 1, records
        return records[0]


@pytest.fixture()
def rig(monkeypatch, tmp_path) -> Rig:
    import app.activity_log as activity_mod
    import app.bus as bus_mod
    import app.execution_log as execution_mod
    import app.idempotency as idempotency_mod
    import app.inbox as inbox_mod
    import app.preferences as preferences_mod
    import app.storage as storage_mod
    import app.subscriber_store as subscriber_mod
    import app.topic_store as topic_mod

    # 收件箱文件与站内信实现指向同一份临时文件：``XW_INBOX_FILE`` 是两侧
    # 约定的路径来源，inapp 实现按环境变量构造存储，收件箱模块的常量在
    # 导入时就固定了，因此两处都要改。
    inbox_file = tmp_path / "inbox.jsonl"
    monkeypatch.setenv("XW_INBOX_FILE", str(inbox_file))
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", inbox_file)
    monkeypatch.setattr(
        inbox_mod,
        "_read_store",
        inbox_mod.ReadStateStore(tmp_path / "read_state.jsonl"),
    )
    # 两个缓存是模块级单例：跨用例复用会把上一个用例的快照带进来。
    monkeypatch.setattr(inbox_mod, "_inbox_cache", inbox_mod.InboxCache())
    monkeypatch.setattr(inbox_mod, "_unread_cache", inbox_mod.UnreadSummaryCache())

    bus = MemoryMessageBus()
    storage = _RecordingStorage()
    executions = MemoryExecutionStore()

    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(storage_mod, "_storage", storage)
    monkeypatch.setattr(activity_mod, "_activity_store", MemoryActivityStore())
    monkeypatch.setattr(execution_mod, "_execution_store", executions)
    monkeypatch.setattr(idempotency_mod, "_store", MemoryIdempotencyStore())

    monkeypatch.setattr(subscriber_mod, "_store", MemorySubscriberStore())
    monkeypatch.setattr(topic_mod, "_store", MemoryTopicStore())
    monkeypatch.setattr(preferences_mod, "_store", MemoryPreferenceStore())

    # 站内信走真实实现（写入消息中心文件），其余渠道用 console：
    # 这样收件箱读的是引擎格式的收件箱行，而不是投递记录兜底路径。
    configs = [ProviderConfig(channel=Channel.IN_APP, kind="inapp", id="inapp")]
    configs += [
        ProviderConfig(channel=channel, kind="console")
        for channel in (Channel.EMAIL, Channel.SMS, Channel.PUSH, Channel.CHAT)
    ]
    report = provider_factory.build_registry(configs)
    monkeypatch.setattr(provider_factory, "_registry", report.registry)

    return Rig(client=TestClient(app), bus=bus, storage=storage, executions=executions)


def _seed_creators(rig: Rig) -> None:
    """建四个创作者：一个渠道齐全、一个即将关掉邮件、一个只有意愿没有地址。"""
    rig.subscriber(
        "creator_alice",
        email="creator.alice@example.com",
        phone="+8613800000001",
        device_token="fcm-alice-0001",
        channels=["in_app", "email", "push"],
    )
    rig.subscriber(
        "creator_bob",
        email="creator.bob@example.com",
        channels=["in_app", "email"],
    )
    # 启用了邮件却没填邮箱：档案合法，但没有任何可投递渠道。
    rig.subscriber("creator_carol", channels=["email"])
    rig.subscriber(
        "creator_dan",
        email="creator.dan@example.com",
        channels=["in_app"],
    )


# ---------------------------------------------------------------- 主链路


def test_broadcast_reaches_inbox_with_preference_applied(rig: Rig) -> None:
    """分组广播 → worker 投递 → 收件箱 / 执行明细 / 活动流三处口径一致。"""
    _seed_creators(rig)

    # 邮箱大小写不同也算同一个人：地址在存储层统一成小写后再查重。
    conflict = rig.client.post(
        "/v1/subscribers",
        json={
            "id": "creator_eve",
            "email": "Creator.Alice@Example.com",
            "channels": ["email"],
        },
    )
    assert conflict.status_code == 409
    assert "creator_alice" in conflict.json()["detail"]

    # 渠道绑定只列出真的能投的：carol 启用了 email 但没有地址。
    carol = rig.client.get("/v1/subscribers/creator_carol").json()
    assert carol["channels"] == ["email"]
    assert carol["bindings"] == {}

    rig.topic("hot_authors", "热门作者")
    for subscriber_id in ("creator_alice", "creator_bob", "creator_carol", "creator_dan"):
        rig.join("hot_authors", subscriber_id)
    # 重复加入是幂等的：不刷新加入时刻，也不占第二个名额。
    again = rig.join("hot_authors", "creator_alice")
    assert again["added"] is False
    assert again["memberCount"] == 4

    members = rig.client.get("/v1/topics/hot_authors/subscribers").json()
    assert members["total"] == 4
    assert [m["subscriberId"] for m in members["items"]] == [
        "creator_alice",
        "creator_bob",
        "creator_carol",
        "creator_dan",
    ]

    # bob 关掉邮件渠道：只影响邮件，不收窄站内信。
    response = rig.client.put(
        "/v1/subscribers/creator_bob/preferences",
        json={"scope": "channel", "target": "email", "enabled": False},
    )
    assert response.status_code == 200
    assert response.json()["created"] is True

    # 删掉 dan 的档案，成员关系仍留在分组里——广播要能跳过并说清原因。
    assert rig.client.delete("/v1/subscribers/creator_dan").status_code == 204

    broadcast = rig.broadcast(
        "hot_authors",
        "post_commented",
        {"post_id": "P-2026-6001", "title": POST_TITLE, "body": POST_BODY},
    )
    assert broadcast.status_code == 200, broadcast.text
    receipt = broadcast.json()
    assert receipt["memberCount"] == 4
    assert receipt["accepted"] == 2
    assert receipt["skipped"] == 2
    # 回执与成员名单同序，调用方按下标对齐即可。
    assert [(r["subscriberId"], r["status"]) for r in receipt["results"]] == [
        ("creator_alice", "accepted"),
        ("creator_bob", "accepted"),
        ("creator_carol", "skipped"),
        ("creator_dan", "skipped"),
    ]
    reasons = {r["subscriberId"]: r["error"] for r in receipt["results"] if r["error"]}
    assert "没有可投递的渠道" in reasons["creator_carol"]
    assert "不存在" in reasons["creator_dan"]

    alice_trigger = receipt["results"][0]["triggerId"]
    bob_trigger = receipt["results"][1]["triggerId"]

    # 受理两人 → 队列两条；活动流也只在受理时记两笔。
    assert rig.client.get("/health").json()["queued"] == 2
    activity = rig.client.get(
        "/v1/activity", params={"workflow": "post_commented"}
    ).json()["items"]
    assert len(activity) == 2
    assert {item["subscriber_id"] for item in activity} == {
        "creator_alice",
        "creator_bob",
    }
    assert all(item["status"] == "triggered" for item in activity)

    assert rig.drain() == 2

    # alice：三个渠道全部投出。
    alice_exec = rig.executions_of(alice_trigger)
    assert sorted(item["channel"] for item in alice_exec) == ["email", "in_app", "push"]
    assert all(item["status"] == "sent" for item in alice_exec)
    assert sorted(rig.record_of(alice_trigger)["channels"]) == [
        "email",
        "in_app",
        "push",
    ]

    # bob：邮件被偏好拦下，站内信照常发出；消息记录只列实际尝试的渠道。
    bob_exec = {item["channel"]: item for item in rig.executions_of(bob_trigger)}
    assert bob_exec["in_app"]["status"] == "sent"
    assert bob_exec["email"]["status"] == "skipped"
    assert "偏好跳过" in bob_exec["email"]["reason"]
    bob_record = rig.record_of(bob_trigger)
    assert bob_record["channels"] == ["in_app"]
    assert bob_record["status"] == "sent"
    assert "偏好" in bob_record["reason"]

    # 收件箱：站内信来自消息中心文件，消息 ID 是「触发标识:步骤」，说明读的
    # 是引擎格式的收件箱行，而不是投递记录兜底。
    alice_inbox = rig.inbox("creator_alice")
    assert alice_inbox["total"] == 1
    assert alice_inbox["unread"] == 1
    item = alice_inbox["items"][0]
    assert item["message_id"] == f"{alice_trigger}:in_app"
    assert item["workflow"] == "post_commented"
    assert item["title"] == POST_TITLE
    assert POST_BODY in item["body"]

    assert rig.inbox("creator_bob")["unread"] == 1
    # 广播阶段被跳过的两人没有消息，也没有站内信卡片。
    assert rig.inbox("creator_carol")["total"] == 0
    assert rig.inbox("creator_dan")["total"] == 0

    summary = rig.unread("creator_alice")
    assert summary["unread"] == 1
    assert summary["byWorkflow"] == [
        {"workflow": "post_commented", "unread": 1, "total": 1}
    ]
    assert summary["latestAt"]

    # 标记已读后未读归零，未读聚合的缓存也跟着失效。
    read = rig.client.post(
        "/v1/inbox/read",
        json={"subscriberId": "creator_alice", "messageIds": [item["message_id"]]},
    )
    assert read.status_code == 200
    assert read.json() == {"marked": 1, "unread": 0}
    assert rig.inbox("creator_alice")["items"][0]["read"] is True
    assert rig.unread("creator_alice")["unread"] == 0


# ---------------------------------------------------------------- 偏好层级


def test_preference_layers_decide_each_channel(rig: Rig) -> None:
    """渠道层压过工作流层，工作流层压过全局层；被拦下的渠道不发也不留卡片。"""
    _seed_creators(rig)

    # 三层同时设置：全局关闭、post_liked 关闭、邮件渠道单独放开。
    for body in (
        {"scope": "global", "enabled": False},
        {"scope": "workflow", "target": "post_liked", "enabled": False},
        {"scope": "channel", "target": "email", "enabled": True},
    ):
        response = rig.client.put(
            "/v1/subscribers/creator_alice/preferences", json=body
        )
        assert response.status_code == 200, response.text

    listed = rig.client.get("/v1/subscribers/creator_alice/preferences").json()
    assert listed["counts"] == {"global": 1, "workflow": 1, "channel": 1}
    # 按具体程度排序：渠道 → 工作流 → 全局。
    assert [item["scope"] for item in listed["items"]] == [
        "channel",
        "workflow",
        "global",
    ]

    def resolve(workflow: str, channel: str) -> dict:
        response = rig.client.get(
            "/v1/subscribers/creator_alice/preferences/resolve",
            params={"workflow": workflow, "channel": channel},
        )
        assert response.status_code == 200, response.text
        return response.json()

    # 邮件渠道被单独放开：最具体的一层说了算，全局关闭与工作流关闭都不再生效。
    email_liked = resolve("post_liked", "email")
    assert email_liked["allowed"] is True
    assert email_liked["scope"] == "channel"
    assert email_liked["matched"] is True

    # 站内信没有渠道层记录，落到工作流层。
    in_app_liked = resolve("post_liked", "in_app")
    assert in_app_liked["allowed"] is False
    assert in_app_liked["scope"] == "workflow"
    assert "post_liked" in in_app_liked["reason"]

    # 另一个工作流没有工作流层记录，落到全局层。
    push_commented = resolve("post_commented", "push")
    assert push_commented["allowed"] is False
    assert push_commented["scope"] == "global"

    # 真的触发一次 post_liked：邮件发出，站内信与推送被拦。
    liked = rig.trigger(
        "post_liked",
        "creator_alice",
        ["in_app", "push", "email"],
        {"post_id": "P-2026-6002", "title": "有人赞了你的帖子", "body": "阿澈赞了你的帖子"},
    )
    assert liked.status_code == 200, liked.text
    liked_trigger = liked.json()["triggerId"]
    assert rig.drain() == 1

    liked_exec = {item["channel"]: item for item in rig.executions_of(liked_trigger)}
    assert liked_exec["email"]["status"] == "sent"
    assert liked_exec["in_app"]["status"] == "skipped"
    assert "post_liked" in liked_exec["in_app"]["reason"]
    assert liked_exec["push"]["status"] == "skipped"
    liked_record = rig.record_of(liked_trigger)
    assert liked_record["channels"] == ["email"]
    assert liked_record["status"] == "sent"
    # 被拦下的站内信不进收件箱：用户不会收到一张点开什么都没有的卡片。
    assert rig.inbox("creator_alice")["total"] == 0

    # 只剩全局关闭生效的一次触发：全部渠道被拦，整条消息记为 skipped。
    commented = rig.trigger(
        "post_commented",
        "creator_alice",
        ["in_app", "push"],
        {"post_id": "P-2026-6003", "title": POST_TITLE, "body": POST_BODY},
    )
    commented_trigger = commented.json()["triggerId"]
    assert rig.drain() == 1

    commented_exec = rig.executions_of(commented_trigger)
    assert [item["status"] for item in commented_exec] == ["skipped", "skipped"]
    assert all("全局" in item["reason"] for item in commented_exec)
    commented_record = rig.record_of(commented_trigger)
    # 「没发」与「发失败」分开记：状态不是 failed，渠道列表为空。
    assert commented_record["status"] == "skipped"
    assert commented_record["channels"] == []
    assert "偏好" in commented_record["reason"]
    assert rig.inbox("creator_alice")["total"] == 0

    # 清空偏好等于恢复默认：三层都没有记录时按接收处理。
    cleared = rig.client.delete("/v1/subscribers/creator_alice/preferences")
    assert cleared.status_code == 200
    assert cleared.json()["removed"] == 3
    after = resolve("post_commented", "push")
    assert after["allowed"] is True
    assert after["matched"] is False


# ---------------------------------------------------------------- 收件箱读取


def _walk_pages(rig: Rig, subscriber_id: str, limit: int) -> list[str]:
    """按游标逐页翻完，返回消息 ID 序列。"""
    collected: list[str] = []
    cursor: Optional[str] = None
    for _ in range(20):
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        page = rig.inbox(subscriber_id, **params)
        collected.extend(item["message_id"] for item in page["items"])
        if not page["hasMore"]:
            return collected
        cursor = page["nextCursor"]
        assert cursor
    raise AssertionError("翻页没有收敛")


def test_inbox_pagination_and_unread_summary_agree(rig: Rig) -> None:
    """分页、筛选、未读聚合与列表对同一份数据给出同一套结论。"""
    rig.subscriber(
        "creator_faye",
        email="creator.faye@example.com",
        channels=["in_app"],
    )

    plan = [
        ("post_liked", "第一波点赞"),
        ("post_commented", "第一条评论"),
        ("post_liked", "第二波点赞"),
        ("post_commented", "第二条评论"),
        ("post_liked", "第三波点赞"),
    ]
    for index, (workflow, title) in enumerate(plan):
        response = rig.trigger(
            workflow,
            "creator_faye",
            ["in_app"],
            {"post_id": f"P-2026-70{index:02d}", "title": title, "body": f"{title}的正文"},
        )
        assert response.status_code == 200, response.text
        assert rig.drain() == 1

    full = rig.inbox("creator_faye", limit=50)
    assert full["total"] == 5
    assert full["unread"] == 5
    assert set(item["workflow"] for item in full["items"]) == {
        "post_liked",
        "post_commented",
    }
    full_ids = [item["message_id"] for item in full["items"]]

    # 逐页翻完的结果与一次取全完全一致：同一条不重复，也不漏。
    walked = _walk_pages(rig, "creator_faye", limit=2)
    assert walked == full_ids
    assert len(set(walked)) == 5

    # 未读与总数覆盖全部消息，不受 limit 影响；matched 才是本页命中数。
    first_page = rig.inbox("creator_faye", limit=2)
    assert first_page["unread"] == 5
    assert first_page["total"] == 5
    assert first_page["matched"] == 5
    assert first_page["hasMore"] is True
    assert len(first_page["items"]) == 2

    # 按工作流筛选：只命中该工作流的三条，未读总数仍是 5。
    liked = rig.inbox("creator_faye", workflow="post_liked")
    assert liked["matched"] == 3
    assert {item["workflow"] for item in liked["items"]} == {"post_liked"}
    assert liked["unread"] == 5

    only_unread = rig.inbox("creator_faye", unread="true")
    assert only_unread["matched"] == 5

    # 时间区间左闭右开：一小时前到现在包含全部，一小时前为界则一条不剩。
    an_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    assert rig.inbox("creator_faye", since=an_hour_ago)["matched"] == 5
    assert rig.inbox("creator_faye", until=an_hour_ago)["matched"] == 0

    # 标记最近两条已读：未读数与按工作流的分布同步下降。
    newest_two = full_ids[:2]
    read = rig.client.post(
        "/v1/inbox/read",
        json={"subscriberId": "creator_faye", "messageIds": newest_two},
    )
    assert read.status_code == 200
    assert read.json()["unread"] == 3

    after = rig.inbox("creator_faye", limit=50)
    assert after["unread"] == 3
    assert after["total"] == 5
    read_flags = {item["message_id"]: item["read"] for item in after["items"]}
    assert read_flags[newest_two[0]] is True
    assert read_flags[newest_two[1]] is True

    summary = rig.unread("creator_faye")
    assert summary["total"] == 5
    assert summary["unread"] == 3
    expected: dict[str, int] = {}
    for item in after["items"]:
        if not item["read"]:
            expected[item["workflow"]] = expected.get(item["workflow"], 0) + 1
    by_workflow = {row["workflow"]: row for row in summary["byWorkflow"]}
    assert {name: row["unread"] for name, row in by_workflow.items() if row["unread"]} == expected
    assert sum(row["total"] for row in summary["byWorkflow"]) == 5
    # 未读多的工作流排在前面。
    unread_counts = [row["unread"] for row in summary["byWorkflow"]]
    assert unread_counts == sorted(unread_counts, reverse=True)


# ---------------------------------------------------------------- 实时推送


def test_inbox_stream_pushes_after_delivery(rig: Rig) -> None:
    """SSE 首帧给出当前状态，投递发生后推一帧 ``inbox``，未读数随之更新。"""
    rig.subscriber(
        "creator_gina",
        email="creator.gina@example.com",
        channels=["in_app"],
    )

    def deliver() -> None:
        # 在第一次轮询间隔里完成一次真实投递：来源版本因此变化，
        # 生成器应当立刻推一帧，而不是等客户端重新查一次列表。
        response = rig.trigger(
            "post_mentioned",
            "creator_gina",
            ["in_app"],
            {"post_id": "P-2026-8001", "title": "有人在帖子里 @ 了你", "body": "阿澈：@gina 看这个"},
        )
        assert response.status_code == 200
        assert rig.drain() == 1

    polls = {"count": 0}

    async def sleep(_interval: float) -> None:
        if polls["count"] == 0:
            polls["count"] = 1
            deliver()

    async def collect() -> list[str]:
        return [
            frame
            async for frame in iter_inbox_events(
                "creator_gina", limit=10, interval=0, max_events=2, sleep=sleep
            )
        ]

    frames = asyncio.run(collect())
    assert len(frames) == 2

    ready_event, ready_data = _message_frame(frames[0])
    assert ready_event == "ready"
    assert ready_data["unread"] == 0
    assert ready_data["items"] == []

    pushed_event, pushed_data = _message_frame(frames[1])
    assert pushed_event == "inbox"
    assert pushed_data["unread"] == 1
    assert pushed_data["total"] == 1
    assert pushed_data["items"][0]["workflow"] == "post_mentioned"
    assert pushed_data["items"][0]["title"] == "有人在帖子里 @ 了你"


# ---------------------------------------------------------------- 幂等与环境


def test_broadcast_is_idempotent_and_respects_environment(rig: Rig) -> None:
    """同一幂等键重放不重复入队；跨环境的订阅者不能加入本环境的分组。"""
    _seed_creators(rig)
    rig.topic("post_watchers", "热帖围观")

    for subscriber_id in ("creator_alice", "creator_bob"):
        rig.join("post_watchers", subscriber_id)

    headers = {"Idempotency-Key": "topic-post-watchers-P-2026-9001"}
    payload = {"post_id": "P-2026-9001", "title": POST_TITLE, "body": POST_BODY}

    first = rig.broadcast("post_watchers", "post_commented", payload, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["accepted"] == 2
    assert rig.client.get("/health").json()["queued"] == 2

    second = rig.broadcast("post_watchers", "post_commented", payload, headers=headers)
    assert second.status_code == 200
    # 重放原样返回首次回执，队列长度不变。
    assert second.json() == first.json()
    assert rig.client.get("/health").json()["queued"] == 2

    # 活动日志同样只有首次的两笔。
    activity = rig.client.get(
        "/v1/activity", params={"workflow": "post_commented"}
    ).json()["items"]
    assert len(activity) == 2

    # 生产环境的订阅者对开发环境的分组而言「不存在」，按 404 处理。
    production = rig.client.post(
        "/v1/subscribers",
        json={
            "id": "creator_prod",
            "email": "creator.prod@example.com",
            "channels": ["in_app"],
        },
        headers={"X-Environment": "production"},
    )
    assert production.status_code == 201
    rejected = rig.client.post(
        "/v1/topics/post_watchers/subscribers",
        json={"subscriberId": "creator_prod"},
    )
    assert rejected.status_code == 404
    assert "production" not in rejected.json()["detail"]

    # 广播后仍然只有两条消息：被拒的加入没有留下成员关系。
    assert rig.client.get("/v1/topics/post_watchers").json()["memberCount"] == 2
    assert rig.drain() == 2
    assert rig.inbox("creator_alice")["unread"] == 1
    assert rig.inbox("creator_bob")["unread"] == 1
