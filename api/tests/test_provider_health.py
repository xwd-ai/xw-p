"""渠道实现健康检查与启停测试：探测契约、缓存、注册表状态与接口。"""
from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import app.config as config_mod
import app.providers.factory as factory_mod
import app.providers.health as health_mod
from app.main import app
from app.message_center import JsonlMessageCenter, MemoryMessageCenter
from app.models.channel import Channel
from app.providers import (
    ConsoleProvider,
    Delivery,
    DeliveryResult,
    HealthMonitor,
    HealthState,
    InAppProvider,
    ProbeResult,
    Provider,
    ProviderRegistry,
    SMTPProvider,
    probe_provider,
    summarize,
)


# --------------------------------------------------------------------------- 辅助


class CountingProvider(Provider):
    """可计数、可指定探测结论的实现，用来观察探测次数与缓存。"""

    kind = "counting"

    def __init__(self, channel: Channel = Channel.EMAIL, *, result: ProbeResult | None = None, **kwargs):
        super().__init__(channel, **kwargs)
        self._result = result or ProbeResult.ok("探测通过")
        self.probes: list[float] = []

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        return DeliveryResult.ok(self._id_for_result, delivery.channel)

    def probe(self, timeout: float) -> ProbeResult:
        self.probes.append(timeout)
        return self._result


class BoomProbeProvider(Provider):
    """探测时抛异常的实现。"""

    kind = "boom"

    def _deliver(self, delivery: Delivery) -> DeliveryResult:  # pragma: no cover - 不投递
        return DeliveryResult.ok(self._id_for_result, delivery.channel)

    def probe(self, timeout: float) -> ProbeResult:
        raise RuntimeError("探测通道炸了")


class BoolProbeProvider(Provider):
    """只返回布尔值的实现（旧式约定）。"""

    kind = "bool"

    def __init__(self, channel: Channel = Channel.SMS, *, ok: bool = True, **kwargs):
        super().__init__(channel, **kwargs)
        self._ok = ok

    def _deliver(self, delivery: Delivery) -> DeliveryResult:  # pragma: no cover - 不投递
        return DeliveryResult.ok(self._id_for_result, delivery.channel)

    def probe(self, timeout: float) -> bool:
        return self._ok


@pytest.fixture
def registry():
    """两个 email 实现（主备）+ 一个站内信实现，并装进全局注册表。"""
    reg = ProviderRegistry()
    reg.register(ConsoleProvider(Channel.EMAIL, identifier="console-email", priority=10))
    reg.register(ConsoleProvider(Channel.EMAIL, identifier="backup-email", priority=100))
    reg.register(ConsoleProvider(Channel.IN_APP, identifier="console-inapp", priority=10))
    factory_mod.set_registry(reg)
    health_mod.set_monitor(HealthMonitor(reg))
    try:
        yield reg
    finally:
        factory_mod.set_registry(None)
        health_mod.set_monitor(None)


@pytest.fixture
def client(registry):
    return TestClient(app)


def _free_port() -> int:
    """占一个端口拿号后立刻释放，得到一个「没人监听」的端口号。"""
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


# ---------------------------------------------------------------------- 探测契约


def test_probe_provider_without_probe_is_config_only():
    class NoProbe(ConsoleProvider):
        probe = None  # type: ignore[assignment]

    result = probe_provider(NoProbe(Channel.EMAIL, identifier="no-probe"))
    assert result.state is HealthState.OK
    assert "不提供在线探测" in result.detail


def test_console_probe_reports_no_dependency():
    result = probe_provider(ConsoleProvider(Channel.SMS, identifier="console-sms"))
    assert result.state is HealthState.OK
    assert "日志" in result.detail


def test_probe_exception_becomes_unavailable():
    result = probe_provider(BoomProbeProvider(Channel.EMAIL, identifier="boom"))
    assert result.state is HealthState.UNAVAILABLE
    assert "RuntimeError" in result.detail
    assert "炸了" in result.detail


def test_probe_bool_result_is_tolerated():
    assert probe_provider(BoolProbeProvider(ok=True, identifier="b1")).state is HealthState.OK
    assert (
        probe_provider(BoolProbeProvider(ok=False, identifier="b2")).state
        is HealthState.UNAVAILABLE
    )


def test_probe_timeout_passed_to_implementation():
    provider = CountingProvider(identifier="count")
    probe_provider(provider, timeout=0.75)
    assert provider.probes == [0.75]


def test_smtp_probe_reachable_and_closed_port():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    try:
        provider = SMTPProvider(
            Channel.EMAIL, host="127.0.0.1", port=port, sender="notice@example.com", tls="none"
        )
        result = provider.probe(1.0)
    finally:
        listener.close()
    assert result.state is HealthState.OK
    assert str(port) in result.detail
    assert "未验证认证" in result.detail

    closed = SMTPProvider(
        Channel.EMAIL, host="127.0.0.1", port=_free_port(), sender="notice@example.com", tls="none"
    )
    result = closed.probe(0.5)
    assert result.state is HealthState.UNAVAILABLE
    assert "失败" in result.detail or "超时" in result.detail


def test_inapp_probe_memory_and_file(tmp_path):
    memory = InAppProvider(Channel.IN_APP, store=MemoryMessageCenter(), identifier="mem")
    assert memory.probe(0.5).state is HealthState.OK

    target = tmp_path / "inbox.jsonl"
    store = JsonlMessageCenter(target)
    provider = InAppProvider(Channel.IN_APP, store=store, identifier="file")
    result = provider.probe(0.5)
    assert result.state is HealthState.OK
    assert str(target) in result.detail

    # 路径被目录占位：构造时就拒绝，因此这里直接替换内部存储验证探测分支
    blocked = tmp_path / "blocked.jsonl"
    blocked.mkdir()
    provider._store = type("FakeStore", (), {"path": blocked})()
    result = provider.probe(0.5)
    assert result.state is HealthState.UNAVAILABLE
    assert "不是文件" in result.detail


@pytest.mark.skipif(os.getuid() == 0, reason="root 无视目录权限")
def test_inapp_probe_unwritable_dir(tmp_path):
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o500)
    try:
        provider = InAppProvider(
            Channel.IN_APP, store=JsonlMessageCenter(readonly / "inbox.jsonl"), identifier="ro"
        )
        result = provider.probe(0.5)
    finally:
        readonly.chmod(0o700)
    assert result.state is HealthState.UNAVAILABLE
    assert "不可写" in result.detail


# ---------------------------------------------------------------------- 监视器


def test_monitor_marks_disabled_and_primary(registry):
    registry.set_enabled("backup-email", False)
    monitor = HealthMonitor(registry)
    items = monitor.check()
    by_id = {item.id: item for item in items}

    assert by_id["console-email"].primary is True
    assert by_id["console-email"].state is HealthState.OK
    assert by_id["backup-email"].enabled is False
    assert by_id["backup-email"].primary is False
    assert by_id["backup-email"].state is HealthState.DISABLED
    assert "已停用" in by_id["backup-email"].detail


def test_monitor_config_check_does_not_probe(registry):
    provider = CountingProvider(identifier="count-email")
    registry.register(provider, priority=5)
    monitor = HealthMonitor(registry)

    item = monitor.check_one("count-email")
    assert item.probed is False
    assert item.latency_ms == 0
    assert "未做在线探测" in item.detail
    assert provider.probes == []


def test_monitor_deep_probe_records_latency(registry):
    provider = CountingProvider(identifier="count-email")
    registry.register(provider, priority=5)
    monitor = HealthMonitor(registry, timeout=0.4)

    item = monitor.check_one("count-email", deep=True)
    assert item.probed is True
    assert item.latency_ms >= 0
    assert provider.probes == [0.4]


def test_monitor_cache_and_refresh(registry):
    provider = CountingProvider(identifier="count-email")
    registry.register(provider, priority=5)
    clock = {"now": 1000.0}
    monitor = HealthMonitor(registry, ttl=30.0, clock=lambda: clock["now"])

    first = monitor.check_one("count-email", deep=True)
    assert first.cached is False
    assert len(provider.probes) == 1

    clock["now"] += 5
    second = monitor.check_one("count-email", deep=True)
    assert second.cached is True
    assert len(provider.probes) == 1

    clock["now"] += 40  # 超过 TTL
    third = monitor.check_one("count-email", deep=True)
    assert third.cached is False
    assert len(provider.probes) == 2

    monitor.check_one("count-email", deep=True, refresh=True)
    assert len(provider.probes) == 3


def test_monitor_cache_disabled_with_zero_ttl(registry):
    provider = CountingProvider(identifier="count-email")
    registry.register(provider, priority=5)
    monitor = HealthMonitor(registry, ttl=0)

    monitor.check_one("count-email", deep=True)
    monitor.check_one("count-email", deep=True)
    assert len(provider.probes) == 2


def test_monitor_cache_drops_after_toggle(registry):
    provider = CountingProvider(identifier="count-email")
    registry.register(provider, priority=5)
    monitor = HealthMonitor(registry, ttl=60.0)

    monitor.check_one("count-email", deep=True)
    registry.set_enabled("count-email", False)
    item = monitor.check_one("count-email", deep=True)
    assert item.cached is False
    assert item.state is HealthState.DISABLED


def test_monitor_filter_by_channel_and_identifier(registry):
    monitor = HealthMonitor(registry)
    only_inapp = monitor.check(channel=Channel.IN_APP)
    assert [item.id for item in only_inapp] == ["console-inapp"]

    subset = monitor.check(identifiers=["backup-email", "console-email"])
    assert [item.id for item in subset] == ["console-email", "backup-email"]

    assert monitor.check(identifiers=["nope"]) == []
    assert monitor.check_one("nope") is None


def test_summarize_counts_states(registry):
    registry.set_enabled("backup-email", False)
    items = HealthMonitor(registry).check()
    summary = summarize(items)
    assert summary["total"] == 3
    assert summary["enabled"] == 2
    assert summary["ok"] == 2
    assert summary["disabled"] == 1
    assert summary["probed"] == 0


# ------------------------------------------------------------------------ 接口


def test_list_providers(client):
    response = client.get("/v1/providers")
    assert response.status_code == 200
    body = response.json()
    ids = [item["id"] for item in body["items"]]
    # 同一优先级按登记顺序，priority=100 的备选排在最后
    assert ids == ["console-email", "console-inapp", "backup-email"]
    assert body["summary"]["total"] == 3
    assert body["channels"] == ["email", "in_app"]
    assert "sms" in body["unboundChannels"]
    assert body["deep"] is False
    assert body["items"][0]["primary"] is True
    assert body["items"][1]["primary"] is True
    assert body["items"][2]["primary"] is False


def test_list_providers_filters_and_deep(client, registry):
    provider = CountingProvider(identifier="count-email")
    registry.register(provider, priority=5)

    filtered = client.get("/v1/providers", params={"channel": "in_app"}).json()
    assert [item["id"] for item in filtered["items"]] == ["console-inapp"]

    deep = client.get("/v1/providers", params={"deep": "true", "channel": "email"}).json()
    assert deep["deep"] is True
    assert all(item["probed"] for item in deep["items"])
    assert provider.probes  # 在线探测被真正调用

    bad = client.get("/v1/providers", params={"channel": "order"})
    assert bad.status_code == 400
    assert "未知渠道" in bad.json()["detail"]


def test_get_single_provider(client):
    ok = client.get("/v1/providers/console-email")
    assert ok.status_code == 200
    assert ok.json()["provider"]["id"] == "console-email"

    missing = client.get("/v1/providers/nope")
    assert missing.status_code == 404


def test_disable_primary_falls_back(client, registry):
    response = client.put("/v1/providers/console-email/enabled", json={"enabled": False})
    assert response.status_code == 200
    body = response.json()
    assert body["changed"] is True
    assert body["persisted"] is False
    assert body["provider"]["enabled"] is False
    assert body["provider"]["state"] == "disabled"
    assert body["warning"] == "渠道 email 改由 backup-email 承接"
    assert registry.get(Channel.EMAIL) is registry.entry("backup-email").provider

    again = client.put("/v1/providers/console-email/enabled", json={"enabled": False})
    assert again.json()["changed"] is False

    restored = client.put("/v1/providers/console-email/enabled", json={"enabled": True})
    assert restored.json()["provider"]["primary"] is True
    assert restored.json()["warning"] is None


def test_disable_last_provider_warns(client, registry):
    client.put("/v1/providers/console-email/enabled", json={"enabled": False})
    response = client.put("/v1/providers/backup-email/enabled", json={"enabled": False})
    body = response.json()
    assert body["warning"] == "渠道 email 已没有启用的实现，投递会失败"
    assert "email" in body["unboundChannels"]
    assert registry.get(Channel.EMAIL) is None


def test_toggle_unknown_identifier(client):
    response = client.put("/v1/providers/nope/enabled", json={"enabled": False})
    assert response.status_code == 404


def test_toggle_requires_body(client):
    assert client.put("/v1/providers/console-email/enabled", json={}).status_code == 422


def test_toggle_persists_to_file(client, registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    path.write_text(
        json.dumps(
            {
                "providers": [
                    {"id": "console-email", "channel": "email", "kind": "console", "priority": 10},
                    {"id": "backup-email", "channel": "email", "kind": "console", "priority": 100},
                ],
                "retries": {"default": {"attempts": 2}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(config_mod, "PROVIDERS_FILE", str(path))

    response = client.put(
        "/v1/providers/console-email/enabled", json={"enabled": False, "persist": True}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["persisted"] is True
    assert body["persistPath"] == str(path)

    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["providers"][0]["enabled"] is False
    assert "enabled" not in saved["providers"][1]
    assert saved["retries"] == {"default": {"attempts": 2}}
    assert registry.entry("console-email").enabled is False


def test_toggle_without_config_file_keeps_state(client, registry, monkeypatch):
    monkeypatch.setattr(config_mod, "PROVIDERS_FILE", "")
    response = client.put(
        "/v1/providers/console-email/enabled", json={"enabled": False, "persist": True}
    )
    assert response.status_code == 400
    assert "XW_PROVIDERS_FILE" in response.json()["detail"]
    assert registry.entry("console-email").enabled is True


def test_toggle_persist_missing_entry_keeps_state(client, registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    path.write_text(
        json.dumps({"providers": [{"id": "backup-email", "channel": "email", "kind": "console"}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(config_mod, "PROVIDERS_FILE", str(path))

    response = client.put(
        "/v1/providers/console-email/enabled", json={"enabled": False, "persist": True}
    )
    assert response.status_code == 400
    assert "没有 id" in response.json()["detail"]
    assert registry.entry("console-email").enabled is True


def test_toggle_persist_broken_file_keeps_state(client, registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    path.write_text("{ 不是 json", encoding="utf-8")
    monkeypatch.setattr(config_mod, "PROVIDERS_FILE", str(path))

    response = client.put(
        "/v1/providers/console-email/enabled", json={"enabled": False, "persist": True}
    )
    assert response.status_code == 400
    assert registry.entry("console-email").enabled is True


def test_config_file_with_engine_retries_section_loads():
    """引擎侧写进同一份文件的 retries 段不该让 API 起不来。"""
    raw = {
        "providers": [
            {"id": "console-email", "channel": "email", "kind": "console"},
            {"id": "console-inapp", "channel": "in_app", "kind": "console"},
        ],
        "retries": {"default": {"attempts": 3, "base_delay": 500}},
    }
    configs = factory_mod.parse_configs(raw)
    assert [cfg.channel for cfg in configs] == [Channel.EMAIL, Channel.IN_APP]

    with pytest.raises(factory_mod.ProviderConfigError):
        factory_mod.parse_configs({**raw, "unexpected": 1})


def test_health_snapshot_has_iso_timestamp(client):
    item = client.get("/v1/providers").json()["items"][0]
    parsed = datetime.fromisoformat(item["checked_at"])
    assert parsed.tzinfo is not None
    assert parsed.tzinfo.utcoffset(parsed) == timezone.utc.utcoffset(parsed)
