"""投递前偏好判定：判定结论、读取失败放行、worker 跳过与统计口径。

判定逻辑本身是纯函数式的（读一次快照，逐渠道给结论），因此大部分用例直接
对 ``DeliveryGate`` 断言；worker 与统计的用例验证「结论怎么落到执行明细、
消息记录与成功率上」——这三处才是排查时真正会看到的东西。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.delivery_gate import (
    DeliveryGate,
    GateDecision,
    get_delivery_gate,
    set_delivery_gate,
    summarize,
)
from app.models.channel import Channel
from app.preferences import MemoryPreferenceStore, Preference, Scope
from app.providers import DeliveryResult


def _pref(
    *,
    subscriber: str = "creator_1001",
    scope: Scope = Scope.CHANNEL,
    target: str | None = None,
    enabled: bool = True,
) -> Preference:
    return Preference(subscriber_id=subscriber, scope=scope, target=target, enabled=enabled)


def _store(*records: Preference) -> MemoryPreferenceStore:
    store = MemoryPreferenceStore()
    for record in records:
        store.upsert(record)
    return store


def _by_channel(decisions: list[GateDecision]) -> dict[str, GateDecision]:
    return {decision.channel: decision for decision in decisions}


# --------------------------------------------------------------------------- #
# 判定结论
# --------------------------------------------------------------------------- #


def test_without_records_everything_is_allowed():
    gate = DeliveryGate(_store())
    decisions = gate.check("creator_1001", "post_liked", [Channel.IN_APP, Channel.EMAIL])

    assert [d.channel for d in decisions] == ["in_app", "email"]
    assert all(d.allowed for d in decisions)
    # 没有任何记录意味着「没表态」，不是「拒绝」：结论可解释，且不带层级。
    assert all(d.matched is False and d.scope is None for d in decisions)
    assert "默认接收" in decisions[0].reason


def test_closed_channel_blocks_only_that_channel():
    gate = DeliveryGate(_store(_pref(scope=Scope.CHANNEL, target="email", enabled=False)))
    decisions = _by_channel(
        gate.check("creator_1001", "post_liked", ["in_app", "email", "push"])
    )

    assert decisions["in_app"].allowed
    assert decisions["push"].allowed
    assert decisions["email"].allowed is False
    assert decisions["email"].scope == "channel"
    assert decisions["email"].target == "email"
    assert decisions["email"].matched is True
    # 写进执行明细的文案要能自己说明白，不用再回头查记录。
    assert decisions["email"].skip_reason.startswith("偏好跳过：")
    assert "email" in decisions["email"].skip_reason


def test_closed_workflow_blocks_all_its_channels_only():
    gate = DeliveryGate(_store(_pref(scope=Scope.WORKFLOW, target="post_liked", enabled=False)))
    decisions = gate.check("creator_1001", "post_liked", ["in_app", "email", "push"])

    assert [d.allowed for d in decisions] == [False, False, False]
    assert {d.scope for d in decisions} == {"workflow"}
    # 另一条工作流不受影响：关闭的是「这类互动」，不是这个人。
    other = gate.check("creator_1001", "post_commented", ["in_app"])
    assert other[0].allowed


def test_channel_layer_overrides_workflow_layer():
    gate = DeliveryGate(
        _store(
            _pref(scope=Scope.WORKFLOW, target="post_liked", enabled=False),
            _pref(scope=Scope.CHANNEL, target="in_app", enabled=True),
        )
    )
    decisions = _by_channel(gate.check("creator_1001", "post_liked", ["in_app", "email"]))

    assert decisions["in_app"].allowed is True
    assert decisions["in_app"].scope == "channel"
    assert decisions["email"].allowed is False
    assert decisions["email"].scope == "workflow"


def test_global_switch_blocks_everything_but_channel_can_reopen():
    gate = DeliveryGate(
        _store(
            _pref(scope=Scope.GLOBAL, enabled=False),
            _pref(scope=Scope.CHANNEL, target="in_app", enabled=True),
        )
    )
    decisions = _by_channel(
        gate.check("creator_1001", "post_commented", [Channel.IN_APP, Channel.SMS])
    )

    assert decisions["in_app"].allowed is True
    assert decisions["in_app"].scope == "channel"
    assert decisions["sms"].allowed is False
    assert decisions["sms"].scope == "global"


def test_preferences_are_per_subscriber():
    gate = DeliveryGate(_store(_pref(scope=Scope.CHANNEL, target="email", enabled=False)))
    other = gate.check("creator_2002", "post_liked", ["email"])
    assert other[0].allowed is True


# --------------------------------------------------------------------------- #
# 读不到偏好时的行为
# --------------------------------------------------------------------------- #


class _BrokenStore:
    """任何读取都失败，模拟文件损坏或后端不可用。"""

    def __init__(self) -> None:
        self.reads = 0

    def snapshot(self):  # noqa: D401 - 测试替身
        self.reads += 1
        raise OSError("preferences unavailable")


def test_store_failure_falls_back_to_allow():
    store = _BrokenStore()
    decisions = DeliveryGate(store).check("creator_1001", "post_liked", ["in_app", "email"])

    assert store.reads == 1
    assert [d.allowed for d in decisions] == [True, True]
    assert all("偏好读取失败" in d.reason for d in decisions)
    assert all(d.matched is False for d in decisions)


def test_missing_subscriber_id_does_not_touch_store():
    store = _BrokenStore()
    decisions = DeliveryGate(store).check(None, "post_liked", ["in_app"])

    # 没有订阅者就无从判定：连存储都不该读，否则每条系统通知都白跑一次 I/O。
    assert store.reads == 0
    assert decisions[0].allowed is True
    assert "未携带订阅者标识" in decisions[0].reason


class _CountingStore:
    """记录快照读取次数，用于验证「一批渠道只读一次」。"""

    def __init__(self, inner: MemoryPreferenceStore) -> None:
        self._inner = inner
        self.reads = 0

    def snapshot(self):
        self.reads += 1
        return self._inner.snapshot()


def test_one_snapshot_per_channel_batch():
    store = _CountingStore(_store(_pref(scope=Scope.CHANNEL, target="email", enabled=False)))
    gate = DeliveryGate(store)
    gate.check("creator_1001", "post_liked", ["in_app", "email", "push", "sms"])

    assert store.reads == 1


# --------------------------------------------------------------------------- #
# 输入边界
# --------------------------------------------------------------------------- #


def test_invalid_workflow_name_skips_only_that_layer():
    gate = DeliveryGate(_store(_pref(scope=Scope.CHANNEL, target="email", enabled=False)))
    decisions = _by_channel(
        gate.check("creator_1001", "post liked", ["in_app", "email"])
    )

    # 工作流名带空格不能成立，工作流层不参与判定；渠道层照常生效。
    assert decisions["in_app"].allowed is True
    assert decisions["email"].allowed is False
    assert decisions["email"].scope == "channel"


def test_non_delivery_channels_are_not_judged():
    gate = DeliveryGate(_store(_pref(scope=Scope.GLOBAL, enabled=False)))
    # delay / digest 是编排步骤，走不到投递，判定这一层不制造第二种错误。
    assert gate.check("creator_1001", "post_liked", [Channel.DIGEST, "nope"]) == []


def test_summarize_reports_blocked_channels_only():
    allowed = GateDecision(channel="in_app", allowed=True, reason="未设置偏好，按默认接收")
    blocked = GateDecision(
        channel="email",
        allowed=False,
        scope="channel",
        target="email",
        matched=True,
        reason="渠道 email 已关闭",
    )
    assert summarize([allowed]) == "偏好判定放行 1 个渠道"
    text = summarize([allowed, blocked])
    assert "email" in text and "in_app" not in text


# --------------------------------------------------------------------------- #
# worker：判定落到执行明细与消息记录
# --------------------------------------------------------------------------- #


class _Recorder:
    """内存化的消息存储。"""

    def __init__(self) -> None:
        self.records: list[dict] = []

    def save(self, record: dict) -> None:
        self.records.append(record)

    def read_all(self) -> list[dict]:
        return list(self.records)


class _StubProvider:
    """只记录调用次数的渠道实现。"""

    channel: Channel

    def __init__(self, channel: Channel) -> None:
        self.channel = channel
        self.calls: list = []

    def send(self, delivery):
        self.calls.append(delivery)
        return DeliveryResult.ok(f"stub-{self.channel.value}", self.channel, 1)


class _StubRegistry:
    def __init__(self, providers: list[_StubProvider]) -> None:
        self._by_channel = {provider.channel: provider for provider in providers}

    def get(self, channel: Channel):
        return self._by_channel.get(channel)


@pytest.fixture
def worker_env(monkeypatch):
    """装好 worker 的依赖替身，返回 (执行明细, 消息存储, 渠道实现, gate)。"""
    from app.schemas import QueuedMessage  # noqa: F401 - 断言类型用

    executions: list[dict] = []
    storage = _Recorder()
    providers = {
        Channel.IN_APP: _StubProvider(Channel.IN_APP),
        Channel.EMAIL: _StubProvider(Channel.EMAIL),
    }
    monkeypatch.setattr("app.worker.record_execution", lambda **kw: executions.append(kw))
    monkeypatch.setattr("app.worker.get_storage", lambda: storage)
    monkeypatch.setattr("app.worker.get_registry", lambda: _StubRegistry(list(providers.values())))

    def _use(store: MemoryPreferenceStore) -> None:
        monkeypatch.setattr(
            "app.worker.get_delivery_gate", lambda: DeliveryGate(store)
        )

    _use(_store())
    return executions, storage, providers, _use


def _message(channels: list[str], subscriber: str = "creator_1001"):
    from app.schemas import QueuedMessage

    return QueuedMessage(
        trigger_id="t-gate",
        transaction_id="tx-gate",
        name="post_liked",
        subscriber_id=subscriber,
        channels=channels,
        payload={"post_id": "P-2026-0001", "title": "帖子收到了新互动"},
    )


def test_worker_skips_channel_closed_by_preferences(worker_env):
    from app.worker import Worker

    executions, storage, providers, use = worker_env
    use(_store(_pref(scope=Scope.CHANNEL, target="email", enabled=False)))

    Worker()._process(_message(["in_app", "email"]))

    assert len(providers[Channel.IN_APP].calls) == 1
    # 被偏好关掉的渠道不发请求：连实现都不取，不能靠「发出去再报失败」表达。
    assert providers[Channel.EMAIL].calls == []

    statuses = {item["channel"]: item["status"] for item in executions}
    assert statuses == {"in_app": "sent", "email": "skipped"}
    skipped = next(item for item in executions if item["status"] == "skipped")
    assert skipped["reason"].startswith("偏好跳过：")

    # 部分渠道被跳过不影响整条消息的性质：发出去的算成功。
    assert storage.records[0]["status"] == "sent"
    # 消息记录只列实际尝试投递的渠道，被拦下的那条不进渠道分布。
    assert storage.records[0]["channels"] == ["in_app"]


def test_worker_marks_message_skipped_when_every_channel_is_closed(worker_env):
    from app.worker import Worker

    executions, storage, providers, use = worker_env
    use(
        _store(
            _pref(scope=Scope.WORKFLOW, target="post_liked", enabled=False),
        )
    )

    Worker()._process(_message(["in_app", "email"]))

    assert providers[Channel.IN_APP].calls == []
    assert providers[Channel.EMAIL].calls == []
    assert [item["status"] for item in executions] == ["skipped", "skipped"]

    record = storage.records[0]
    assert record["status"] == "skipped"
    # 同一条偏好拦下多个渠道时原因只写一遍。
    assert record["reason"] == "偏好跳过：工作流 post_liked 已关闭"
    # 一个渠道都没尝试：渠道列表为空，否则渠道分布与收件箱会把没发的东西算进去。
    assert record["channels"] == []


def test_worker_keeps_failed_status_when_a_sent_channel_fails(worker_env):
    from app.worker import Worker

    executions, storage, providers, use = worker_env
    use(_store(_pref(scope=Scope.CHANNEL, target="email", enabled=False)))

    def _fail(delivery):
        providers[Channel.IN_APP].calls.append(delivery)
        return DeliveryResult.failed("stub-in_app", Channel.IN_APP, "收件箱不可写")

    providers[Channel.IN_APP].send = _fail

    Worker()._process(_message(["in_app", "email"]))

    assert storage.records[0]["status"] == "failed"
    assert "收件箱不可写" in storage.records[0]["reason"]
    statuses = {item["channel"]: item["status"] for item in executions}
    assert statuses == {"in_app": "failed", "email": "skipped"}


def test_worker_delivers_when_preference_store_is_unreadable(worker_env):
    from app.worker import Worker

    _, storage, providers, use = worker_env
    use(_BrokenStore())

    Worker()._process(_message(["in_app", "email"]))

    # 读不到偏好按默认接收：宁可多发一条，也不要静默丢消息。
    assert len(providers[Channel.IN_APP].calls) == 1
    assert len(providers[Channel.EMAIL].calls) == 1
    assert storage.records[0]["status"] == "sent"


def test_suppressed_in_app_leaves_no_inbox_card(worker_env, monkeypatch, tmp_path):
    """被偏好拦下的站内信不能变成收件箱里的一张空卡片与一条未读。"""
    from fastapi.testclient import TestClient

    import app.inbox as inbox_mod
    import app.storage as storage_mod
    from app.main import app
    from app.worker import Worker

    _, storage, providers, use = worker_env
    use(_store(_pref(scope=Scope.GLOBAL, enabled=False)))
    monkeypatch.setattr(storage_mod, "_storage", storage)
    monkeypatch.setattr(
        inbox_mod, "_read_store", inbox_mod.ReadStateStore(tmp_path / "read_state.jsonl")
    )
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", tmp_path / "inbox.jsonl")

    Worker()._process(_message(["in_app"]))

    assert providers[Channel.IN_APP].calls == []
    with TestClient(app) as client:
        data = client.get("/v1/inbox", params={"subscriberId": "creator_1001"}).json()
    assert data["total"] == 0
    assert data["unread"] == 0


# --------------------------------------------------------------------------- #
# 统计口径与查询
# --------------------------------------------------------------------------- #


def _stored(status: str, channels: list[str] | None = None) -> dict:
    return {
        "trigger_id": "t",
        "transaction_id": "tx",
        "workflow": "post_liked",
        "subscriber_id": "creator_1001",
        "channels": channels or ["in_app"],
        "payload": {},
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def test_stats_exclude_skipped_from_success_rate():
    from app.stats import compute_stats

    stats = compute_stats([_stored("sent"), _stored("skipped"), _stored("failed")])

    assert stats["total"] == 3
    assert stats["skipped"] == 1
    # 分母只算尝试过投递的两条：被偏好拦下的那条既没成功也没失败。
    assert stats["success_rate"] == 50.0


def test_stats_treat_all_skipped_as_no_attempt():
    from app.stats import compute_stats

    stats = compute_stats([_stored("skipped"), _stored("skipped")])

    assert stats["skipped"] == 2
    assert stats["success_rate"] == 100.0


def test_execution_query_filters_skipped(tmp_path):
    from app.execution_log import ExecutionQuery, JSONLExecutionStore

    store = JSONLExecutionStore(path=tmp_path / "executions.jsonl")
    store.append({"trigger_id": "t1", "channel": "email", "status": "skipped", "reason": "偏好"})
    store.append({"trigger_id": "t1", "channel": "in_app", "status": "sent"})

    records = store.query(ExecutionQuery(trigger_id="t1", status="skipped", limit=10))

    assert [record["channel"] for record in records] == ["email"]
    assert records[0]["reason"] == "偏好"


def test_gate_singleton_can_be_replaced():
    gate = DeliveryGate(_store())
    set_delivery_gate(gate)
    try:
        assert get_delivery_gate() is gate
    finally:
        set_delivery_gate(None)
