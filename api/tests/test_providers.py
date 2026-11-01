"""渠道 Provider 层测试：抽象基类、注册表、配置工厂与 worker 接线。"""
from __future__ import annotations

import json
import threading

import pytest

import app.providers.factory as factory_mod
from app.models.channel import Channel
from app.providers import (
    Delivery,
    DeliveryResult,
    Provider,
    ProviderConfig,
    ProviderConfigError,
    ProviderRegistry,
    Recipient,
    ConsoleProvider,
    build_registry,
    default_configs,
    get_registry,
    load_configs,
    parse_configs,
    validate_config,
)
from app.providers.base import DEFAULT_PRIORITY

RECIPIENT = Recipient(
    subscriber_id="creator_1001",
    email="creator@example.com",
    phone="+8613800000000",
    device_token="fcm-token-9f2c1d",
)


def _delivery(channel: Channel = Channel.EMAIL, recipient: Recipient = RECIPIENT) -> Delivery:
    return Delivery(
        channel=channel,
        recipient=recipient,
        subject="post_commented",
        payload={"post_id": "P-2026-1001", "actor": "小林"},
    )


class BoomProvider(Provider):
    """投递时抛异常的实现，用来验证基类兜底。"""

    kind = "boom"

    def _deliver(self, delivery: Delivery) -> DeliveryResult:  # pragma: no cover - 只被调用
        raise RuntimeError("下游不可达")


class StaticProvider(Provider):
    """总是投递成功的实现，便于注册表测试。"""

    kind = "static"

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        return DeliveryResult.ok(self.identifier or "static", delivery.channel)


# ---------------------------------------------------------------- 抽象基类


def test_console_provider_reports_success_and_identity():
    provider = ConsoleProvider(Channel.EMAIL, identifier="email-console")
    result = provider.send(_delivery())

    assert result.sent is True
    assert result.reason is None
    assert result.provider_id == "email-console"
    assert result.channel is Channel.EMAIL
    assert result.duration_ms >= 0


def test_missing_address_fails_before_delivery():
    provider = ConsoleProvider(Channel.EMAIL)
    result = provider.send(_delivery(recipient=Recipient(subscriber_id="creator_1001")))

    assert result.sent is False
    assert "邮箱地址" in result.reason


def test_in_app_uses_subscriber_id_as_address():
    provider = ConsoleProvider(Channel.IN_APP)
    assert provider.send(_delivery(Channel.IN_APP)).sent is True


def test_chat_channel_needs_no_per_recipient_address():
    provider = ConsoleProvider(Channel.CHAT)
    result = provider.send(_delivery(Channel.CHAT, recipient=Recipient(subscriber_id="room-1")))
    assert result.sent is True


def test_channel_mismatch_is_reported():
    provider = ConsoleProvider(Channel.SMS)
    result = provider.send(_delivery(Channel.EMAIL))
    assert result.sent is False
    assert "渠道不匹配" in result.reason


def test_provider_exception_becomes_failed_result():
    result = BoomProvider(Channel.EMAIL).send(_delivery())
    assert result.sent is False
    assert "RuntimeError" in result.reason


def test_failed_result_always_carries_reason():
    result = DeliveryResult.failed("p", Channel.SMS, "   ")
    assert result.reason == "渠道未给出失败原因"
    ok = DeliveryResult(provider_id="p", channel=Channel.SMS, sent=True, reason="残留")
    assert ok.reason is None


def test_provider_rejects_orchestration_channel():
    with pytest.raises(ValueError):
        ConsoleProvider(Channel.DIGEST)
    with pytest.raises(TypeError):
        ConsoleProvider("email")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ConsoleProvider(Channel.EMAIL, priority=-1)


# ---------------------------------------------------------------- 注册表


def test_registry_auto_numbers_identifiers():
    registry = ProviderRegistry()
    first = registry.register(ConsoleProvider(Channel.EMAIL))
    second = registry.register(ConsoleProvider(Channel.EMAIL))
    third = registry.register(ConsoleProvider(Channel.EMAIL))

    assert [first.id, second.id, third.id] == ["email", "email-2", "email-3"]


def test_registry_prefers_lower_priority():
    registry = ProviderRegistry()
    registry.register(StaticProvider(Channel.EMAIL, identifier="mock"), priority=100)
    real = registry.register(StaticProvider(Channel.EMAIL, identifier="smtp"), priority=10)

    assert registry.get(Channel.EMAIL) is real.provider
    assert [p.identifier for p in registry.candidates(Channel.EMAIL)] == ["smtp", "mock"]


def test_registry_disabled_entry_falls_back():
    registry = ProviderRegistry()
    primary = registry.register(StaticProvider(Channel.EMAIL, identifier="smtp"), priority=10)
    backup = registry.register(StaticProvider(Channel.EMAIL, identifier="mock"), priority=100)

    registry.set_enabled(primary.id, False)
    assert registry.get(Channel.EMAIL) is backup.provider
    assert registry.entry(primary.id).enabled is False
    assert len(registry) == 2  # 停用不等于摘除


def test_registry_replaces_by_identifier_in_place():
    registry = ProviderRegistry()
    registry.register(StaticProvider(Channel.EMAIL, identifier="first"), priority=50)
    registry.register(StaticProvider(Channel.EMAIL, identifier="second"), priority=60)

    replacement = StaticProvider(Channel.EMAIL, identifier="second")
    entry = registry.register(replacement, identifier="second", priority=10)

    assert entry.provider is replacement
    assert [e.id for e in registry.entries()] == ["second", "first"]
    assert registry.get(Channel.EMAIL) is replacement


def test_registry_rejects_cross_channel_replacement_and_bad_input():
    registry = ProviderRegistry()
    registry.register(StaticProvider(Channel.EMAIL, identifier="primary"))

    with pytest.raises(ValueError):
        registry.register(StaticProvider(Channel.SMS, identifier="primary"))
    with pytest.raises(TypeError):
        registry.register(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        registry.register(StaticProvider(Channel.EMAIL), priority=-5)
    with pytest.raises(KeyError):
        registry.set_enabled("missing", False)
    with pytest.raises(KeyError):
        registry.remove("missing")


def test_registry_remove_and_missing_channels():
    registry = ProviderRegistry()
    registry.register(ConsoleProvider(Channel.EMAIL))
    registry.register(ConsoleProvider(Channel.SMS, identifier="sms-console"))

    missing = set(registry.missing_channels())
    assert Channel.PUSH in missing and Channel.EMAIL not in missing
    assert "sms-console" in registry

    registry.remove("sms-console")
    assert Channel.SMS in set(registry.missing_channels())
    assert "sms-console" not in registry
    assert registry.entry("sms-console") is None


def test_registry_snapshot_is_isolated():
    registry = ProviderRegistry()
    registry.register(ConsoleProvider(Channel.EMAIL))
    snapshot = registry.entries()
    registry.register(ConsoleProvider(Channel.SMS))
    assert len(snapshot) == 1
    assert len(registry.entries()) == 2


def test_registry_concurrent_registration_and_selection():
    registry = ProviderRegistry()
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            for step in range(50):
                registry.register(
                    StaticProvider(Channel.PUSH, identifier=f"push-{index}-{step}"),
                    priority=step,
                )
                registry.get(Channel.PUSH)
        except BaseException as exc:  # pragma: no cover - 失败时给断言用
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(registry) == 400
    head = registry.entries()[0]
    assert head.channel is Channel.PUSH and head.priority == 0
    assert registry.get(Channel.PUSH) is head.provider


# ---------------------------------------------------------------- 配置与工厂


def test_parse_configs_accepts_both_shapes():
    item = {"id": "email-console", "channel": "email", "kind": "console", "priority": 5}
    as_list = parse_configs([item])
    as_object = parse_configs({"providers": [item]})

    assert as_list == as_object
    assert as_list[0].channel is Channel.EMAIL
    assert as_list[0].priority == 5


@pytest.mark.parametrize(
    "bad, keyword",
    [
        ({"items": []}, "未知字段"),
        ([{"channel": "email", "kind": "console", "extra": 1}], "未知字段"),
        ([{"channel": "delay", "kind": "console"}], "编排步骤"),
        ([{"channel": "pigeon", "kind": "console"}], "不存在"),
        ([{"channel": "email", "kind": "carrier-pigeon"}], "种类"),
        ([{"channel": "email", "kind": "console", "priority": "high"}], "priority"),
        ([{"channel": "email", "kind": "console", "priority": -1}], "负数"),
        ([{"channel": "email", "kind": "console", "enabled": "yes"}], "enabled"),
        ([{"channel": "email", "kind": "console", "credentials": []}], "credentials"),
    ],
)
def test_parse_configs_rejects_bad_shape(bad, keyword):
    with pytest.raises(ProviderConfigError) as info:
        parse_configs(bad)
    assert keyword in str(info.value)


def test_validate_config_reports_channel_mismatch_and_credentials():
    wrong_channel = ProviderConfig(channel=Channel.SMS, kind="smtp", credentials={"host": "h", "from": "f"})
    problems = validate_config(wrong_channel)
    assert any("只能用于" in p for p in problems)

    typo = ProviderConfig(
        channel=Channel.EMAIL,
        kind="smtp",
        credentials={"host": "smtp.example.com", "from": "notify@example.com", "Password": "x"},
    )
    problems = validate_config(typo)
    assert any("是否想写 password" in p for p in problems)

    missing = ProviderConfig(channel=Channel.EMAIL, kind="smtp")
    assert any("缺少必填凭据" in p for p in validate_config(missing))


def test_default_configs_cover_every_delivery_channel():
    configs = default_configs()
    assert len(configs) == len(Channel.delivery_channels())
    assert {cfg.kind for cfg in configs} == {"console"}

    report = build_registry()
    assert {entry.channel for entry in report.bound} == set(Channel.delivery_channels())
    assert report.unbound_channels == ()
    assert report.skipped == ()
    assert report.registry.get(Channel.DIGEST) is None


def test_build_registry_skips_kinds_without_implementation():
    configs = [
        ProviderConfig(channel=Channel.EMAIL, kind="console", id="email-console"),
        ProviderConfig(
            channel=Channel.EMAIL,
            kind="smtp",
            id="email-smtp",
            priority=10,
            credentials={"host": "smtp.example.com", "from": "notify@example.com"},
        ),
        ProviderConfig(
            channel=Channel.PUSH,
            kind="fcm",
            id="push-fcm",
            priority=10,
            credentials={"server_key": "key"},
        ),
    ]
    report = build_registry(configs)

    # console 与 smtp 都已接入，fcm 仍未接入：跳过并记进报告。
    assert [entry.id for entry in report.bound] == ["email-console", "email-smtp"]
    assert report.skipped and report.skipped[0][0] == "push-fcm"
    assert "尚未接入" in report.skipped[0][1]
    # smtp 优先级更高，应当被选为 email 的默认实现。
    assert report.registry.get(Channel.EMAIL).identifier == "email-smtp"
    assert Channel.PUSH in report.unbound_channels


def test_build_registry_rejects_invalid_config_with_all_problems():
    configs = [
        ProviderConfig(channel=Channel.SMS, kind="smtp", credentials={"host": "h", "from": "f"}),
        ProviderConfig(channel=Channel.EMAIL, kind="twilio", credentials={}),
    ]
    with pytest.raises(ProviderConfigError) as info:
        build_registry(configs)
    message = str(info.value)
    assert "只能用于" in message
    assert "缺少必填凭据" in message


def test_load_configs_reads_file_and_reports_path(tmp_path):
    path = tmp_path / "providers.json"
    path.write_text(
        json.dumps({"providers": [{"channel": "in_app", "kind": "console"}]}),
        encoding="utf-8",
    )
    configs = load_configs(path)
    assert configs[0].channel is Channel.IN_APP

    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    with pytest.raises(ProviderConfigError) as info:
        load_configs(broken)
    assert str(broken) in str(info.value)


def test_create_registry_honours_configured_file(tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    path.write_text(
        json.dumps({"providers": [{"id": "only-chat", "channel": "chat", "kind": "console"}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(path))

    registry = factory_mod.create_registry()
    assert registry.entry("only-chat") is not None
    assert registry.get(Channel.CHAT) is not None
    assert registry.get(Channel.EMAIL) is None


def test_get_registry_caches_and_can_be_replaced(monkeypatch):
    factory_mod.set_registry(None)
    first = get_registry()
    assert get_registry() is first

    replacement = ProviderRegistry()
    factory_mod.set_registry(replacement)
    assert get_registry() is replacement

    factory_mod.set_registry(None)  # 复位，避免影响其它用例
    assert factory_mod.get_registry() is not first


def test_registry_summary_lists_entries():
    report = build_registry([ProviderConfig(channel=Channel.EMAIL, kind="console", id="email-console")])
    text = report.describe()
    assert "email-console" in text
    assert "channel=email" in text
    assert "console" in text
    # 摘要里不应出现凭据值
    assert "secret" not in text.lower()


# ---------------------------------------------------------------- worker 接线


class _RecordingStorage:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def save(self, record: dict) -> None:
        self.records.append(record)


def _run_worker(monkeypatch, channels, registry=None, **contact):
    from app.schemas import QueuedMessage
    from app.worker import Worker

    executions: list[dict] = []
    storage = _RecordingStorage()
    monkeypatch.setattr("app.worker.record_execution", lambda **kw: executions.append(kw))
    monkeypatch.setattr("app.worker.get_storage", lambda: storage)
    monkeypatch.setattr(
        "app.worker.get_registry", lambda: registry or build_registry().registry
    )

    msg = QueuedMessage(
        trigger_id="t-1",
        transaction_id="tx-1",
        name="post_commented",
        subscriber_id="creator_1001",
        channels=channels,
        payload={"post_id": "P-2026-1001", "actor": "小林"},
        **contact,
    )
    Worker()._process(msg)
    return executions, storage.records[0]


def test_worker_dispatches_through_registry(monkeypatch):
    executions, record = _run_worker(
        monkeypatch,
        ["in_app", "push", "email"],
        email="creator@example.com",
        device_token="fcm-token-9f2c1d",
    )

    assert record["status"] == "sent"
    assert "reason" not in record
    assert [(e["channel"], e["status"]) for e in executions] == [
        ("in_app", "sent"),
        ("push", "sent"),
        ("email", "sent"),
    ]
    assert all(e["duration_ms"] >= 0 for e in executions)


def test_worker_records_failure_for_missing_address(monkeypatch):
    executions, record = _run_worker(monkeypatch, ["in_app", "email"])

    assert record["status"] == "failed"
    assert "email" in record["reason"] and "邮箱地址" in record["reason"]
    assert executions[0]["status"] == "sent"
    assert executions[1]["status"] == "failed"
    assert "邮箱地址" in executions[1]["reason"]


def test_worker_reports_unknown_channel_and_missing_provider(monkeypatch):
    registry = build_registry(
        [ProviderConfig(channel=Channel.IN_APP, kind="console", id="inapp-console")]
    ).registry
    executions, record = _run_worker(
        monkeypatch, ["pigeon", "email", "in_app"], registry=registry
    )

    assert record["status"] == "failed"
    assert executions[0]["channel"] == "pigeon" and "未知渠道" in executions[0]["reason"]
    assert "没有可用的渠道实现" in executions[1]["reason"]
    assert executions[2]["status"] == "sent"


def test_worker_marks_failed_when_provider_raises(monkeypatch):
    registry = ProviderRegistry()
    registry.register(BoomProvider(Channel.EMAIL))
    executions, record = _run_worker(monkeypatch, ["email"], registry=registry, email="a@example.com")

    assert record["status"] == "failed"
    assert "RuntimeError" in executions[0]["reason"]


def test_app_startup_builds_registry(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app

    factory_mod.set_registry(None)
    try:
        with TestClient(app):
            registry = get_registry()
            assert registry.get(Channel.EMAIL) is not None
            assert registry.get(Channel.PUSH) is not None
    finally:
        factory_mod.set_registry(None)


def test_app_startup_rejects_invalid_provider_config(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app

    path = tmp_path / "providers.json"
    path.write_text(
        json.dumps({"providers": [{"channel": "email", "kind": "smtp"}]}), encoding="utf-8"
    )
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(path))
    factory_mod.set_registry(None)
    try:
        with pytest.raises(ProviderConfigError):
            with TestClient(app):
                pass
    finally:
        factory_mod.set_registry(None)
