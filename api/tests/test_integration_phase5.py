"""Phase 5 集成测试：管理视图与运营动作在真实链路上的口径一致性。

Phase 5 交付的是「看得见、改得动」的一层：管理统计、活动检索与下钻、工作流
启停与复制、多租户与环境隔离。这些能力各自都有单元测试，但单测通常直接往
存储里塞记录；真正容易出错的是接缝——

1. 触发时声明的渠道、worker 实际尝试的渠道、执行明细里的渠道，三处是不是
   同一份列表？消息还在队列里时，下钻能不能如实报出「声明了但还没有明细」？
2. 「停用只拦新事件」「副本默认不启用」这类约定，在真实触发链路上是否成立？
   停用的模板会不会因为别处还有一份同名副本就照旧跑起来？
3. 管理统计按「投递」计数、活动汇总按「触发」计数、仪表盘按「消息」计数，
   三个口径在同一个事件上是否自洽？
4. 生产环境触发的事件，执行明细与消息记录是否真的落在生产分区，而不是
   悄悄落回默认环境？
5. 租户边界是否也覆盖管理端：别人的模板、活动、明细，读接口与下钻都该
   当作不存在。

本文件用真实 HTTP 接口、真实 worker 与进程内存储把这些接缝跑一遍。全局单例
全部换成进程内实现，跑完不写 ``api/data`` 下的任何文件。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

import pytest
from fastapi.testclient import TestClient

from app.activity_log import MemoryActivityStore
from app.bus import MemoryMessageBus
from app.execution_log import ExecutionQuery, MemoryExecutionStore
from app.idempotency import MemoryIdempotencyStore
from app.main import app
from app.models.channel import Channel
from app.preferences import MemoryPreferenceStore
from app.providers import factory as provider_factory
from app.providers.factory import ProviderConfig
from app.subscriber_store import MemorySubscriberStore
from app.workflow import seed_workflows
from app.workflow_store import MemoryWorkflowStore


class _RecordingStorage:
    """投递记录存储：进程内保存，并给出随写入变化的版本号。

    收件箱与未读聚合靠版本号判断缓存是否失效；``stats`` 供仪表盘统计使用，
    与文件后端同一口径（一条消息一个整体状态）。
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

    def stats(self, trend_days: int = 7) -> dict:
        from app.stats import compute_stats

        return compute_stats(self.read_all(), trend_days=trend_days)


def _headers(environment: Optional[str] = None, tenant: Optional[str] = None) -> dict:
    """按需拼隔离维度的请求头；留空表示使用服务端默认值。"""
    headers: dict[str, str] = {}
    if environment:
        headers["X-Environment"] = environment
    if tenant:
        headers["X-Tenant-Id"] = tenant
    return headers


@dataclass
class Rig:
    """一次集成测试用到的客户端与后端存储。"""

    client: TestClient
    bus: MemoryMessageBus
    storage: _RecordingStorage
    executions: MemoryExecutionStore

    # ------------------------------------------------------------ 驱动

    def drain(self) -> int:
        """把队列里的消息逐条交给 worker，返回处理条数。

        TestClient 不走 lifespan，后台消费线程不会启动；显式驱动让「入队」
        与「投递」两步都能被断言，顺序确定。
        """
        from app.worker import Worker

        worker = Worker()
        handled = 0
        while True:
            message = self.bus.consume(timeout=0.05)
            if message is None:
                return handled
            worker._process(message)
            handled += 1

    # ------------------------------------------------------------ 构造

    def workflow(
        self,
        name: str,
        channels: tuple[str, ...],
        *,
        description: str = "",
        headers: Optional[dict] = None,
    ) -> dict:
        """按渠道列表建一个模板；步骤标识显式给出，便于断言副本重写。"""
        steps = [
            {
                "id": f"step_{channel}",
                "type": channel,
                "name": channel,
                "config": {"template": "{{payload.actor}} 与你的帖子互动了"},
            }
            for channel in channels
        ]
        response = self.client.post(
            "/v1/workflows",
            json={"name": name, "description": description, "steps": steps},
            headers=headers or {},
        )
        assert response.status_code == 201, response.text
        return response.json()

    def trigger(
        self,
        name: str,
        *,
        channels: tuple[str, ...] = ("in_app",),
        subscriber_id: str = "creator_alice",
        payload: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> dict:
        body = {
            "name": name,
            "to": {
                "subscriberId": subscriber_id,
                "channels": list(channels),
                "email": "creator.alice@example.com",
                "phone": "+8613800000001",
                "deviceToken": "fcm-alice-0001",
            },
            "payload": payload
            or {
                "post_id": "P-2026-8001",
                "title": "小柯评论了你的帖子",
                "actor": "小柯",
                "body": "小柯：第二节的转场是怎么做的？",
            },
        }
        return self.client.post("/v1/events/trigger", json=body, headers=headers or {})

    # ------------------------------------------------------------ 读取

    def detail(self, trigger_id: str, *, headers: Optional[dict] = None) -> dict:
        response = self.client.get(
            f"/v1/admin/activity/{trigger_id}", headers=headers or {}
        )
        assert response.status_code == 200, response.text
        return response.json()

    def stats(self, *, headers: Optional[dict] = None, **params) -> dict:
        response = self.client.get("/v1/admin/stats", params=params, headers=headers or {})
        assert response.status_code == 200, response.text
        return response.json()

    def summary(self, *, headers: Optional[dict] = None, **params) -> dict:
        response = self.client.get(
            "/v1/admin/activity/summary", params=params, headers=headers or {}
        )
        assert response.status_code == 200, response.text
        return response.json()

    def executions_of(self, trigger_id: str) -> list[dict]:
        return self.executions.query(ExecutionQuery(trigger_id=trigger_id))


@pytest.fixture()
def rig(monkeypatch, tmp_path) -> Rig:
    import app.activity_log as activity_mod
    import app.bus as bus_mod
    import app.execution_log as execution_mod
    import app.idempotency as idempotency_mod
    import app.inbox as inbox_mod
    import app.message_center as message_center_mod
    import app.preferences as preferences_mod
    import app.storage as storage_mod
    import app.subscriber_store as subscriber_mod
    import app.workflow_store as workflow_mod

    # 站内信落进程内消息中心，收件箱文件指向临时目录：本组用例不碰 api/data。
    monkeypatch.setenv("XW_INBOX_FILE", str(tmp_path / "inbox.jsonl"))
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", tmp_path / "inbox.jsonl")
    monkeypatch.setattr(inbox_mod, "_read_store", inbox_mod.ReadStateStore(tmp_path / "read_state.jsonl"))
    monkeypatch.setattr(inbox_mod, "_inbox_cache", inbox_mod.InboxCache())
    monkeypatch.setattr(inbox_mod, "_unread_cache", inbox_mod.UnreadSummaryCache())
    monkeypatch.setattr(
        message_center_mod, "_store", message_center_mod.MemoryMessageCenter()
    )

    bus = MemoryMessageBus()
    storage = _RecordingStorage()
    executions = MemoryExecutionStore()

    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(storage_mod, "_storage", storage)
    monkeypatch.setattr(activity_mod, "_activity_store", MemoryActivityStore())
    monkeypatch.setattr(execution_mod, "_execution_store", executions)
    monkeypatch.setattr(idempotency_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(subscriber_mod, "_store", MemorySubscriberStore())
    monkeypatch.setattr(preferences_mod, "_store", MemoryPreferenceStore())

    # 每个租户一份预置模板：租户用例要验证「别人的模板在这里不存在」，
    # 而预置模板本身应当两个租户都能看到。
    monkeypatch.setattr(workflow_mod, "_store", MemoryWorkflowStore(seed=seed_workflows()))
    monkeypatch.setattr(
        workflow_mod,
        "_tenant_stores",
        {"acme": MemoryWorkflowStore(seed=seed_workflows())},
    )

    # 站内信走真实实现（写消息中心），其余渠道用 console。
    configs = [ProviderConfig(channel=Channel.IN_APP, kind="inapp", id="inapp")]
    configs += [
        ProviderConfig(channel=channel, kind="console")
        for channel in (Channel.EMAIL, Channel.SMS, Channel.PUSH, Channel.CHAT)
    ]
    report = provider_factory.build_registry(configs)
    monkeypatch.setattr(provider_factory, "_registry", report.registry)

    return Rig(client=TestClient(app), bus=bus, storage=storage, executions=executions)


# ---------------------------------------------------------------- 接缝一：下钻与统计


def test_drilldown_and_delivery_stats_agree_on_the_same_trigger(rig: Rig) -> None:
    """一次触发的声明渠道、逐渠道明细与管理统计必须指向同一批投递。"""
    workflow = rig.workflow("post_liked_burst", ("in_app", "email", "push"))
    assert workflow["version"] == 1 and workflow["active"] is True

    ack = rig.trigger(
        "post_liked_burst",
        channels=("in_app", "email", "push"),
        payload={
            "post_id": "P-2026-8001",
            "title": "小柯赞了你的帖子",
            "actor": "小柯",
            "body": "小柯赞了你的帖子《周末骑行路线》",
        },
    )
    assert ack.status_code == 200, ack.text
    trigger_id = ack.json()["triggerId"]
    assert rig.bus.size() == 1

    # 受理之后、消费之前：活动记录已经声明三个渠道，明细一条都没有。
    # 这正是「消息还压在队列里」的样子，下钻要用缺口说出来，而不是显示 0 条。
    pending = rig.detail(trigger_id)
    assert pending["activity"]["status"] == "triggered"
    assert pending["activity"]["channels"] == ["in_app", "email", "push"]
    assert [row for row in pending["executions"]] == []
    assert pending["missingChannels"] == ["in_app", "email", "push"]
    assert pending["undeclaredChannels"] == []
    assert pending["executionSummary"]["total"] == 0
    assert pending["executionsTruncated"] is False

    assert rig.drain() == 1

    done = rig.detail(trigger_id)
    # 明细按写入时刻倒序返回；这里只关心渠道集合与状态，声明顺序由活动记录保证。
    assert {row["channel"] for row in done["executions"]} == {"in_app", "email", "push"}
    assert {row["status"] for row in done["executions"]} == {"sent"}
    # 声明与明细一一对应之后，两类缺口都为空。
    assert done["missingChannels"] == []
    assert done["undeclaredChannels"] == []
    assert {k: v for k, v in done["executionSummary"].items() if k != "durationMs"} == {
        "total": 3,
        "sent": 3,
        "failed": 0,
        "skipped": 0,
        "other": 0,
    }

    stats = rig.stats()
    assert stats["totals"]["records"] == 3
    assert stats["totals"]["attempted"] == 3
    assert stats["totals"]["sent"] == 3
    assert stats["totals"]["success_rate"] == 100.0
    channels = {item["channel"]: item for item in stats["channels"]}
    assert set(channels) == {"in_app", "email", "push"}
    assert all(item["sent"] == 1 for item in channels.values())
    # 工作流排行来自执行明细，名字与触发时用的模板一致。
    assert [item["workflow"] for item in stats["workflows"]] == ["post_liked_burst"]

    # 活动汇总按「触发的消息」计数，与统计的「投递条数」不是一个分母。
    summary = rig.summary()
    assert summary["totals"]["records"] == 1
    assert summary["totals"]["byStatus"]["triggered"] == 1
    assert summary["workflows"] == [
        {"workflow": "post_liked_burst", "total": 1, "failed": 0}
    ]
    assert {item["channel"] for item in summary["channels"]} == {
        "in_app",
        "email",
        "push",
    }


# ---------------------------------------------------------------- 接缝二：启停与复制


def test_disabling_a_template_blocks_new_events_and_copy_takes_over(rig: Rig) -> None:
    """停用只拦新事件；副本默认不启用，启用后与原模板各自独立计数。"""
    workflow = rig.workflow("post_commented_burst", ("in_app", "email"))
    workflow_id = workflow["id"]

    first = rig.trigger("post_commented_burst", channels=("in_app", "email"))
    assert first.status_code == 200, first.text

    disabled = rig.client.put(
        f"/v1/workflows/{workflow_id}/active", json={"active": False}
    )
    assert disabled.status_code == 200, disabled.text
    assert disabled.json()["changed"] is True
    # 启停是运营状态，不是模板内容的一次修改。
    assert disabled.json()["workflow"]["version"] == 1
    assert disabled.json()["workflow"]["active"] is False

    # 已受理的那条不受影响：撤销一条已入队的通知比多发一条更难解释。
    assert rig.bus.size() == 1

    blocked = rig.trigger("post_commented_burst")
    assert blocked.status_code == 404
    assert "post_commented_burst" in blocked.json()["detail"]
    assert "development" in blocked.json()["detail"]
    assert rig.bus.size() == 1

    again = rig.client.put(
        f"/v1/workflows/{workflow_id}/active", json={"active": False}
    )
    assert again.status_code == 200
    assert again.json()["changed"] is False

    copied = rig.client.post(f"/v1/workflows/{workflow_id}/duplicate", json={})
    assert copied.status_code == 201, copied.text
    copy = copied.json()
    assert copy["name"] == "post_commented_burst_copy"
    assert copy["active"] is False and copy["version"] == 1
    # 步骤标识必须重写：副本沿用源模板的标识，后续改副本会连带改到源。
    assert {step["id"] for step in copy["steps"]}.isdisjoint(
        {step["id"] for step in workflow["steps"]}
    )

    # 副本是草稿：没启用之前同样不能触发。
    assert rig.trigger(copy["name"]).status_code == 404

    enabled = rig.client.put(
        f"/v1/workflows/{copy['id']}/active", json={"active": True}
    )
    assert enabled.status_code == 200
    assert enabled.json()["changed"] is True

    accepted = rig.trigger(copy["name"], channels=("in_app", "email"))
    assert accepted.status_code == 200, accepted.text

    assert rig.drain() == 2

    stats = rig.stats()
    attempted = {item["workflow"]: item["attempted"] for item in stats["workflows"]}
    assert attempted == {"post_commented_burst": 2, "post_commented_burst_copy": 2}

    # 停用后的模板不产生新的活动记录，历史记录仍然可查。
    summary = rig.summary(workflow="post_commented_burst")
    assert summary["totals"]["records"] == 1
    assert rig.summary(workflow=copy["name"])["totals"]["records"] == 1


# ---------------------------------------------------------------- 接缝三：跳过与分母


def test_preference_skipped_channel_stays_out_of_the_success_rate(rig: Rig) -> None:
    """被偏好拦下的渠道不算一次投递，也不该把成功率拉低。"""
    created = rig.client.post(
        "/v1/subscribers",
        json={
            "id": "creator_bob",
            "email": "creator.bob@example.com",
            "channels": ["in_app", "email"],
        },
    )
    assert created.status_code == 201, created.text

    preference = rig.client.put(
        "/v1/subscribers/creator_bob/preferences",
        json={"scope": "channel", "target": "email", "enabled": False},
    )
    assert preference.status_code == 200, preference.text

    rig.workflow("post_mentioned_burst", ("in_app", "email"))
    ack = rig.trigger(
        "post_mentioned_burst",
        channels=("in_app", "email"),
        subscriber_id="creator_bob",
        payload={
            "post_id": "P-2026-8002",
            "title": "小柯在帖子里提到了你",
            "actor": "小柯",
            "body": "小柯在《周末骑行路线》里提到了你",
        },
    )
    assert ack.status_code == 200, ack.text
    trigger_id = ack.json()["triggerId"]

    assert rig.drain() == 1

    rows = {row["channel"]: row for row in rig.executions_of(trigger_id)}
    assert rows["in_app"]["status"] == "sent"
    assert rows["email"]["status"] == "skipped"
    assert "email" in (rows["email"]["reason"] or "")

    # 消息记录只列真正尝试过的渠道：被拦下的渠道不进渠道分布。
    record = [r for r in rig.storage.read_all() if r["trigger_id"] == trigger_id][0]
    assert record["channels"] == ["in_app"]
    assert record["status"] == "sent"

    # 活动记录保持触发时声明的渠道，两种口径互不干扰。
    detail = rig.detail(trigger_id)
    assert detail["activity"]["channels"] == ["in_app", "email"]
    assert detail["missingChannels"] == []
    assert detail["executionSummary"]["sent"] == 1
    assert detail["executionSummary"]["skipped"] == 1

    stats = rig.stats()
    assert stats["totals"]["records"] == 2
    # 分母只算尝试过的投递：一条成功、一条跳过 → 100%，而不是 50%。
    assert stats["totals"]["attempted"] == 1
    assert stats["totals"]["sent"] == 1
    assert stats["totals"]["skipped"] == 1
    assert stats["totals"]["success_rate"] == 100.0
    channels = {item["channel"]: item for item in stats["channels"]}
    assert channels["email"]["attempted"] == 0
    assert channels["email"]["skipped"] == 1

    # 仪表盘按消息计数：一条消息、一个整体状态，与上面的投递口径不同。
    dashboard = rig.client.get("/v1/dashboard/stats").json()
    assert dashboard["total"] == 1
    assert dashboard["success_rate"] == 100.0
    assert dashboard["channel_distribution"] == {"in_app": 1}


# ---------------------------------------------------------------- 接缝四：环境隔离


def test_environment_flows_through_the_whole_trigger_path(rig: Rig) -> None:
    """生产环境触发的事件，声明、明细与统计都要落在生产分区。"""
    dev = rig.workflow("post_liked_env", ("in_app", "email"))
    prod = rig.workflow(
        "post_liked_env",
        ("in_app",),
        description="生产环境只发站内信",
        headers=_headers("production"),
    )
    assert dev["id"] != prod["id"]
    assert dev["environment"] == "development"
    assert prod["environment"] == "production"

    dev_ack = rig.trigger(
        "post_liked_env", channels=("in_app", "email"), headers=_headers("development")
    )
    prod_ack = rig.trigger(
        "post_liked_env", channels=("in_app",), headers=_headers("production")
    )
    assert dev_ack.status_code == 200, dev_ack.text
    assert prod_ack.status_code == 200, prod_ack.text
    dev_trigger = dev_ack.json()["triggerId"]
    prod_trigger = prod_ack.json()["triggerId"]

    # 触发时按请求头的环境取模板：两条走的是两份不同的模板定义。
    assert rig.detail(dev_trigger)["activity"]["channels"] == ["in_app", "email"]
    assert rig.detail(prod_trigger)["activity"]["channels"] == ["in_app"]
    assert rig.detail(prod_trigger)["activity"]["environment"] == "production"

    assert rig.drain() == 2

    # 执行明细由 worker 按消息里携带的环境写入；这里落错分区，
    # 生产环境的投递就会统计到开发环境名下。
    assert [row["environment"] for row in rig.executions_of(dev_trigger)] == [
        "development",
        "development",
    ]
    assert [row["environment"] for row in rig.executions_of(prod_trigger)] == ["production"]

    # 管理端默认跨环境：两套都要能看到。
    listed = rig.client.get("/v1/admin/activity").json()
    assert listed["total"] == 2
    only_prod = rig.client.get(
        "/v1/admin/activity", params={"environment": "production"}
    ).json()
    assert [item["trigger_id"] for item in only_prod["items"]] == [prod_trigger]

    summary = rig.summary()
    assert {item["environment"]: item["total"] for item in summary["environments"]} == {
        "development": 1,
        "production": 1,
    }

    # 停用开发环境的模板不影响生产环境同名模板。
    assert (
        rig.client.put(f"/v1/workflows/{dev['id']}/active", json={"active": False})
        .status_code
        == 200
    )
    assert rig.trigger("post_liked_env").status_code == 404
    still_there = rig.trigger("post_liked_env", headers=_headers("production"))
    assert still_there.status_code == 200, still_there.text

    # 统计按租户汇总、不按环境拆分：一次部署的开发与生产一起算。
    assert rig.stats()["totals"]["records"] == 3


# ---------------------------------------------------------------- 接缝五：租户边界


def test_tenant_boundaries_cover_the_admin_surface(rig: Rig) -> None:
    """管理端不能绕过租户：模板、活动、明细与下钻都按租户收窄。"""
    acme_headers = _headers(tenant="acme")
    acme = rig.workflow("post_mentioned_acme", ("in_app",), headers=acme_headers)

    # 默认租户看不到、也触发不了别人的模板。
    default_names = [item["name"] for item in rig.client.get("/v1/workflows").json()]
    assert "post_mentioned_acme" not in default_names
    acme_names = [
        item["name"] for item in rig.client.get("/v1/workflows", headers=acme_headers).json()
    ]
    assert "post_mentioned_acme" in acme_names
    assert (
        rig.trigger("post_mentioned_acme").status_code == 404
    )

    ack = rig.trigger("post_mentioned_acme", headers=acme_headers)
    assert ack.status_code == 200, ack.text
    trigger_id = ack.json()["triggerId"]
    assert rig.drain() == 1

    # 统计与活动汇总按租户收窄：默认租户这边是空的。
    assert rig.stats()["totals"]["records"] == 0
    assert rig.stats(headers=acme_headers)["totals"]["records"] == 1
    assert rig.summary()["totals"]["records"] == 0
    assert rig.summary(headers=acme_headers)["totals"]["records"] == 1

    # 下钻按租户查找：拿到标识也读不到别人的活动。
    assert (
        rig.client.get(f"/v1/admin/activity/{trigger_id}").status_code == 404
    )
    assert rig.detail(trigger_id, headers=acme_headers)["activity"]["workflow"] == (
        "post_mentioned_acme"
    )

    # 幂等键也是按租户分区的：默认租户用同一个键不会被别人的回执顶掉。
    first = rig.client.post(
        "/v1/events/trigger",
        json={
            "name": "post_mentioned_acme",
            "to": {"subscriberId": "creator_acme"},
            "payload": {"post_id": "P-2026-8003"},
        },
        headers={"X-Tenant-Id": "acme", "Idempotency-Key": "evt-acme-1"},
    )
    assert first.status_code == 200
    replay = rig.client.post(
        "/v1/events/trigger",
        json={
            "name": "post_mentioned_acme",
            "to": {"subscriberId": "creator_acme"},
            "payload": {"post_id": "P-2026-8003"},
        },
        headers={"X-Tenant-Id": "acme", "Idempotency-Key": "evt-acme-1"},
    )
    assert replay.json()["triggerId"] == first.json()["triggerId"]

    # 租户标识写错是 400，而不是悄悄落回默认租户。
    invalid = rig.client.get("/v1/admin/activity", headers={"X-Tenant-Id": "../etc"})
    assert invalid.status_code == 400
    assert "租户标识" in invalid.json()["detail"]
