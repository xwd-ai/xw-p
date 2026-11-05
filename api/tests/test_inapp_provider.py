"""站内信 Provider 测试：内容回落、标识幂等、失败路径、工厂装配与收件箱联动。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.message_center import JsonlMessageCenter, MemoryMessageCenter, MessageError, WriteOutcome
from app.models.channel import Channel
from app.providers import Delivery, InAppProvider, ProviderConfig, Recipient, build_registry
from app.providers.factory import ProviderConfigError
from app.providers.inapp import message_id

RECIPIENT = Recipient(subscriber_id="creator_1001")


def _delivery(**kwargs) -> Delivery:
    """构造一条站内信投递：默认沿用引擎下游渲染好的标题与正文。"""
    params = {
        "channel": Channel.IN_APP,
        "recipient": RECIPIENT,
        "subject": "有人赞了你的帖子",
        "body": "小林 赞了你的《我的第一支 Vlog》",
        "payload": {"post_id": "P-2026-1001", "actor": "小林"},
        "metadata": {"trigger_id": "t-like-1", "workflow": "post_liked", "step_id": "step-1"},
    }
    params.update(kwargs)
    return Delivery(**params)


def _provider(store=None) -> InAppProvider:
    return InAppProvider(
        Channel.IN_APP,
        store=store if store is not None else MemoryMessageCenter(),
        identifier="inapp",
        now=lambda: datetime(2026, 3, 1, 9, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------- 写入


def test_writes_message_with_rendered_content():
    store = MemoryMessageCenter()
    result = _provider(store).send(_delivery())

    assert result.sent is True
    assert result.provider_id == "inapp"
    messages = store.list("creator_1001")
    assert len(messages) == 1
    msg = messages[0]
    assert msg.id == "t-like-1:step-1"
    assert msg.title == "有人赞了你的帖子"
    assert msg.body == "小林 赞了你的《我的第一支 Vlog》"
    assert msg.workflow == "post_liked"
    assert msg.step_id == "step-1"
    assert msg.created_at == datetime(2026, 3, 1, 9, 0, tzinfo=timezone.utc)


def test_falls_back_to_payload_when_body_not_rendered():
    """API 内置消费者不渲染模板，正文与标题回落到载荷里的字段。"""
    store = MemoryMessageCenter()
    result = _provider(store).send(
        _delivery(
            subject="",
            body="",
            payload={"post_id": "P-2026-1001", "title": "新评论", "message": "小林：拍得真好"},
        )
    )

    assert result.sent is True
    msg = store.list("creator_1001")[0]
    assert msg.title == "新评论"
    assert msg.body == "小林：拍得真好"


def test_payload_title_wins_over_workflow_subject():
    """API 内置消费者把工作流名当 subject 传下来，展示上应以载荷里的业务标题为准。"""
    store = MemoryMessageCenter()
    _provider(store).send(
        _delivery(subject="post_liked", payload={"title": "有人赞了你的帖子", "body": "小林 赞了你的帖子"})
    )
    assert store.list("creator_1001")[0].title == "有人赞了你的帖子"


def test_title_falls_back_to_workflow_name():
    store = MemoryMessageCenter()
    _provider(store).send(_delivery(subject="", payload={"body": "正文"}))
    assert store.list("creator_1001")[0].title == "post_liked"


def test_same_trigger_is_idempotent():
    store = MemoryMessageCenter()
    provider = _provider(store)

    first = provider.send(_delivery())
    second = provider.send(_delivery())

    assert first.sent is True and second.sent is True
    assert len(store) == 1, "同一触发重放不应在收件箱里留下两条"
    assert "幂等" not in (second.reason or "")


def test_message_id_prefers_trigger_and_step():
    assert message_id("t1", "step-1") == "t1:step-1"
    assert message_id("t1", "") == "t1:in_app"
    fallback = message_id("", "step-1")
    assert fallback.startswith("inapp-")
    assert fallback != message_id("", "step-1")


# ---------------------------------------------------------------- 失败路径


def test_rejects_empty_body():
    store = MemoryMessageCenter()
    result = _provider(store).send(_delivery(subject="标题", body="", payload={"post_id": "P-1"}))

    assert result.sent is False
    assert "正文" in (result.reason or "")
    assert len(store) == 0


def test_reports_missing_subscriber():
    store = MemoryMessageCenter()
    result = _provider(store).send(_delivery(recipient=Recipient(subscriber_id="   ")))

    assert result.sent is False
    assert "缺少投递地址" in (result.reason or "")
    assert len(store) == 0


def test_reports_store_failure():
    class BrokenStore(MemoryMessageCenter):
        def write(self, message, *, replace=False):  # noqa: ANN001 - 与契约一致
            raise MessageError("磁盘只读")

    result = _provider(BrokenStore()).send(_delivery())

    assert result.sent is False
    assert "磁盘只读" in (result.reason or "")


def test_rejects_non_in_app_channel():
    with pytest.raises(ValueError):
        InAppProvider(Channel.EMAIL, store=MemoryMessageCenter())


def test_reports_channel_mismatch():
    provider = _provider()
    result = provider.send(_delivery(channel=Channel.EMAIL))
    assert result.sent is False
    assert "渠道不匹配" in (result.reason or "")


# ---------------------------------------------------------------- 与收件箱联动


def test_message_center_entry_appears_in_inbox(monkeypatch, tmp_path):
    """写入消息中心的消息应能被收件箱接口读到，并计为未读。"""
    import app.inbox as inbox_mod
    import app.storage as storage_mod

    path = tmp_path / "inbox.jsonl"
    monkeypatch.setattr(storage_mod, "_storage", type("_Empty", (), {"read_all": lambda self: []})())
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", path)
    monkeypatch.setattr(inbox_mod, "_read_store", inbox_mod.ReadStateStore(tmp_path / "read_state.jsonl"))
    monkeypatch.setattr(inbox_mod, "_inbox_cache", inbox_mod.InboxCache())

    store = JsonlMessageCenter(path)
    assert _provider(store).send(_delivery()).sent is True

    client = TestClient(app)
    data = client.get("/v1/inbox", params={"subscriberId": "creator_1001"}).json()

    assert data["total"] == 1
    assert data["unread"] == 1
    item = data["items"][0]
    assert item["message_id"] == "t-like-1:step-1"
    assert item["title"] == "有人赞了你的帖子"
    assert item["body"] == "小林 赞了你的《我的第一支 Vlog》"
    assert item["read"] is False

    # 标记已读后未读归零
    resp = client.post(
        "/v1/inbox/read",
        json={"subscriberId": "creator_1001", "messageIds": ["t-like-1:step-1"]},
    ).json()
    assert resp["marked"] == 1
    assert resp["unread"] == 0


# ---------------------------------------------------------------- 工厂装配


def test_factory_builds_inapp_provider(monkeypatch, tmp_path):
    path = tmp_path / "shared_inbox.jsonl"
    monkeypatch.setenv("XW_INBOX_FILE", str(path))

    report = build_registry(
        [ProviderConfig(channel=Channel.IN_APP, kind="inapp", id="inapp")]
    )

    provider = report.registry.get(Channel.IN_APP)
    assert isinstance(provider, InAppProvider)
    assert report.skipped == ()
    # 只配了站内信，其余渠道仍在报告里列为缺口
    assert Channel.IN_APP not in report.unbound_channels
    assert Channel.EMAIL in report.unbound_channels

    assert provider.send(_delivery()).sent is True
    assert path.exists()
    assert JsonlMessageCenter(path).get("t-like-1:step-1") is not None


def test_factory_rejects_unknown_inapp_credential():
    with pytest.raises(ProviderConfigError) as info:
        build_registry(
            [
                ProviderConfig(
                    channel=Channel.IN_APP,
                    kind="inapp",
                    credentials={"inbox": "/tmp/x.jsonl"},
                )
            ]
        )
    assert "inbox" in str(info.value)


def test_factory_reports_unwritable_message_center(monkeypatch, tmp_path):
    """存储路径不可写时在装配阶段失败，而不是等第一条站内信投递。"""
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    (blocked / "inbox.jsonl").mkdir()  # 同名目录：打开文件必然失败
    monkeypatch.setenv("XW_INBOX_FILE", str(blocked / "inbox.jsonl"))

    with pytest.raises(ProviderConfigError):
        build_registry([ProviderConfig(channel=Channel.IN_APP, kind="inapp")])


# ---------------------------------------------------------------- 消费链路


class _Recorder:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def save(self, record: dict) -> None:
        self.records.append(record)


def test_worker_delivers_in_app_through_message_center(monkeypatch, tmp_path):
    from app.schemas import QueuedMessage
    from app.worker import Worker

    path = tmp_path / "inbox.jsonl"
    monkeypatch.setenv("XW_INBOX_FILE", str(path))
    executions: list[dict] = []
    storage = _Recorder()
    registry = build_registry(
        [ProviderConfig(channel=Channel.IN_APP, kind="inapp", id="inapp")]
    ).registry
    monkeypatch.setattr("app.worker.record_execution", lambda **kw: executions.append(kw))
    monkeypatch.setattr("app.worker.get_storage", lambda: storage)
    monkeypatch.setattr("app.worker.get_registry", lambda: registry)

    Worker()._process(
        QueuedMessage(
            trigger_id="t-inapp",
            transaction_id="tx-inapp",
            name="post_liked",
            subscriber_id="creator_1001",
            channels=["in_app"],
            payload={"post_id": "P-2026-1001", "title": "帖子互动", "body": "有 3 人赞了你的帖子"},
        )
    )

    assert len(executions) == 1
    execution = dict(executions[0])
    duration = execution.pop("duration_ms")
    assert duration >= 0
    assert execution == {
        "trigger_id": "t-inapp",
        "channel": "in_app",
        "status": "sent",
        "reason": None,
        # 执行记录带上消息所属的租户与环境：同一份明细里要能分辨数据分区。
        "tenant": "default",
        "environment": "development",
        # 归属信息供管理统计的工作流与实现排行使用。
        "workflow": "post_liked",
        "provider": "inapp",
    }
    assert storage.records[0]["status"] == "sent"
    stored = JsonlMessageCenter(path).get("t-inapp:in_app")
    assert stored is not None
    assert stored.body == "有 3 人赞了你的帖子"
    assert stored.workflow == "post_liked"


def test_write_outcome_ok_flag():
    assert WriteOutcome.CREATED.ok is True
    assert WriteOutcome.DUPLICATE.ok is True
    assert WriteOutcome.UPDATED.ok is True
    assert WriteOutcome.MISSING.ok is False
