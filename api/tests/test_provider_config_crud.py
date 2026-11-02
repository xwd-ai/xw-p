"""渠道配置的增删改：校验、登记、配置簿与写回配置文件，以及管理接口。

覆盖四层：

- :mod:`app.providers.manage` 的纯逻辑（不经过 HTTP），用独立的注册表与配置簿，
  便于断言「失败之后什么都不该留下」；
- 凭据回显的脱敏规则；
- 配置文件的写回（追加、改写、删除、保留引擎读取的 ``retries`` 段）；
- ``/v1/providers`` 上的增删改接口与错误码。
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

import app.config as config_mod
import app.providers.factory as factory_mod
import app.providers.health as health_mod
from app.main import app
from app.models.channel import Channel
from app.providers import ConsoleProvider, ProviderRegistry
from app.providers.factory import ConfigBook, ProviderConfig
from app.providers.manage import (
    MASK,
    ProviderConflictError,
    ProviderNotFoundError,
    create_config,
    delete_config,
    describe_config,
    is_secret,
    mask_credentials,
    secret_keys,
    update_config,
    warning_for,
)


# --------------------------------------------------------------------------- 辅助


def _console(channel: Channel = Channel.EMAIL, identifier: str = "console-email", priority: int = 10) -> ProviderConfig:
    return ProviderConfig(channel=channel, kind="console", id=identifier, priority=priority)


def _smtp(**overrides) -> ProviderConfig:
    credentials = {
        "host": "smtp.example.com",
        "from": "notify@example.com",
        "username": "notify",
        "password": "s3cr3t-token",
    }
    credentials.update(overrides.pop("credentials", {}))
    return ProviderConfig(channel=Channel.EMAIL, kind="smtp", id="mail", credentials=credentials, **overrides)


@pytest.fixture
def book():
    """独立的配置簿，替换全局实例，测试之间不互相污染。"""
    bk = ConfigBook()
    factory_mod.set_book(bk)
    try:
        yield bk
    finally:
        factory_mod.set_book(None)


@pytest.fixture
def registry(book):
    """一个 email 主实现 + 一个 email 备选 + 一个站内信实现，配置同时进配置簿。"""
    reg = ProviderRegistry()
    for cfg, priority in ((_console(priority=10), 10), (_console(identifier="backup-email", priority=100), 100)):
        reg.register(
            ConsoleProvider(cfg.channel, identifier=cfg.id, priority=priority),
            identifier=cfg.id,
            priority=priority,
        )
        book.put(cfg.id, cfg)
    inapp = ProviderConfig(channel=Channel.IN_APP, kind="console", id="console-inapp", priority=10)
    reg.register(
        ConsoleProvider(Channel.IN_APP, identifier="console-inapp", priority=10),
        identifier="console-inapp",
        priority=10,
    )
    book.put("console-inapp", inapp)
    factory_mod.set_registry(reg)
    health_mod.set_monitor(health_mod.HealthMonitor(reg))
    try:
        yield reg
    finally:
        factory_mod.set_registry(None)
        health_mod.set_monitor(None)


@pytest.fixture
def client(registry):
    return TestClient(app)


def _write_file(path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _read_file(path):
    return json.loads(path.read_text(encoding="utf-8"))


# ------------------------------------------------------------------------ 新增


def test_create_registers_entry_and_records_config(registry, book):
    result = create_config(_console(identifier="secondary-email", priority=5))

    assert result.created is True
    assert result.changed is True
    assert result.persisted is None
    assert result.entry.id == "secondary-email"
    assert result.entry.priority == 5
    assert registry.entry("secondary-email").kind == "console"
    record = book.get("secondary-email")
    assert record is not None and record.source == "runtime"
    assert record.config == _console(identifier="secondary-email", priority=5)


def test_create_auto_identifier_numbers_per_channel(registry, book):
    first = create_config(ProviderConfig(channel=Channel.SMS, kind="console"))
    second = create_config(ProviderConfig(channel=Channel.SMS, kind="console"))

    assert first.entry.id == "sms"
    assert second.entry.id == "sms-2"
    assert [record.id for record in book.items() if record.config.channel is Channel.SMS] == ["sms", "sms-2"]


def test_create_duplicate_identifier_conflicts(registry):
    with pytest.raises(ProviderConflictError) as err:
        create_config(_console(identifier="console-email"))
    assert "console-email" in str(err.value)
    # 原有条目不受影响
    assert registry.entry("console-email").priority == 10


def test_create_invalid_credentials_leaves_no_entry(registry, book):
    missing_from = ProviderConfig(
        channel=Channel.EMAIL,
        kind="smtp",
        id="bad-mail",
        credentials={"host": "smtp.example.com"},
    )
    with pytest.raises(factory_mod.ProviderConfigError) as err:
        create_config(missing_from)
    assert "缺少必填凭据" in str(err.value) and "from" in str(err.value)
    assert registry.entry("bad-mail") is None
    assert book.get("bad-mail") is None

    # 形状对了但撑不起实现（端口不是数字）同样在落表之前被拦下
    bad_port = ProviderConfig(
        channel=Channel.EMAIL,
        kind="smtp",
        id="bad-port",
        credentials={"host": "smtp.example.com", "from": "notify@example.com", "port": "not-a-port"},
    )
    with pytest.raises(factory_mod.ProviderConfigError):
        create_config(bad_port)
    assert registry.entry("bad-port") is None


def test_create_unsupported_kind_rejected(registry):
    """种类有定义但实现尚未接入：单条新增必须报错，不能登记一个投不出去的条目。"""
    fcm = ProviderConfig(
        channel=Channel.PUSH,
        kind="fcm",
        id="fcm-primary",
        credentials={"project_id": "demo", "access_token": "token"},
    )
    with pytest.raises(factory_mod.ProviderConfigError) as err:
        create_config(fcm)
    assert "尚未接入" in str(err.value)
    assert registry.entry("fcm-primary") is None


def test_create_rejects_unknown_credential_key(registry):
    cfg = ProviderConfig(
        channel=Channel.EMAIL,
        kind="smtp",
        id="typo-mail",
        credentials={"host": "h", "form": "notify@example.com"},
    )
    with pytest.raises(factory_mod.ProviderConfigError) as err:
        create_config(cfg)
    assert "form" in str(err.value)


def test_create_requires_identifier_when_persisting(registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    _write_file(path, {"providers": []})
    monkeypatch.setattr(config_mod, "PROVIDERS_FILE", str(path))
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(path))

    with pytest.raises(factory_mod.ProviderConfigError) as err:
        create_config(ProviderConfig(channel=Channel.SMS, kind="console"), persist=True)
    assert "显式 id" in str(err.value)
    assert len(registry.entries()) == 3
    assert _read_file(path)["providers"] == []


# ------------------------------------------------------------------------ 修改


def test_update_only_changes_given_fields(registry, book):
    result = update_config("console-email", priority=7)

    assert result.changed is True
    assert result.entry.priority == 7
    # 没传的字段保持原样
    assert result.entry.kind == "console"
    assert result.entry.enabled is True
    assert book.get("console-email").config.credentials == {}


def test_update_credentials_replace_wholesale(registry, book):
    create_config(_smtp())
    result = update_config("mail", credentials={"host": "smtp2.example.com", "from": "hi@example.com"})

    assert result.entry.kind == "smtp"
    assert dict(result.config.credentials) == {"host": "smtp2.example.com", "from": "hi@example.com"}
    # 旧口令没有残留，否则一次「删掉密码」的修改会悄悄保留原值
    assert "password" not in result.config.credentials
    assert book.get("mail").config.credentials.get("username") is None


def test_update_kind_rebuilds_implementation(registry):
    create_config(_console(identifier="switchable"))
    result = update_config("switchable", kind="smtp", credentials={"host": "h", "from": "a@b.c"})

    assert result.entry.kind == "smtp"
    assert registry.entry("switchable").kind == "smtp"
    # 取用位置不变：同优先级下仍在原处
    ids = [entry.id for entry in registry.entries()]
    assert ids.index("switchable") < ids.index("backup-email")


def test_update_without_changes_is_noop(registry):
    result = update_config("console-email", kind="console", priority=10, enabled=True)
    assert result.changed is False
    assert result.persisted is None


def test_update_unknown_identifier(book):
    reg = ProviderRegistry()
    with pytest.raises(ProviderNotFoundError):
        update_config("nope", priority=1, registry=reg, book=book)


def test_update_invalid_priority_rejected(registry):
    with pytest.raises(factory_mod.ProviderConfigError):
        update_config("console-email", priority=-1)
    assert registry.entry("console-email").priority == 10


def test_update_empty_kind_rejected(registry):
    with pytest.raises(factory_mod.ProviderConfigError):
        update_config("console-email", kind="   ")


# ------------------------------------------------------------------------ 删除


def test_delete_removes_entry_and_record(registry, book):
    create_config(_console(identifier="temp-email"))
    result = delete_config("temp-email")

    assert result.entry.id == "temp-email"
    assert result.config is not None
    assert registry.entry("temp-email") is None
    assert book.get("temp-email") is None


def test_delete_last_provider_warns(registry):
    delete_config("backup-email")
    result = delete_config("console-email")
    assert result.warning == "渠道 email 已没有启用的实现，投递会失败"
    assert registry.get(Channel.EMAIL) is None


def test_delete_unknown_identifier(registry):
    with pytest.raises(ProviderNotFoundError):
        delete_config("nope")


# ------------------------------------------------------------------ 凭据回显


def test_mask_credentials_hides_secrets():
    credentials = {
        "host": "smtp.example.com",
        "port": "587",
        "username": "notify",
        "password": "p@ss",
        "access_token": "tok",
        "webhook_url": "https://hooks.example.com/services/T000/B000/XXXX",
        "empty_secret": "   ",
    }
    masked = mask_credentials(credentials)

    assert masked["host"] == "smtp.example.com"
    assert masked["port"] == "587"
    assert masked["username"] == "notify"
    assert masked["password"] == MASK
    assert masked["access_token"] == MASK
    assert masked["webhook_url"] == MASK
    # 空值保持空串：回显要能区分「没配」与「配了但我不给你看」
    assert masked["empty_secret"] == "   "
    assert secret_keys(credentials) == ["access_token", "password", "webhook_url"]


def test_is_secret_matches_common_names():
    assert is_secret("auth_token")
    assert is_secret("PRIVATE_KEY")
    assert not is_secret("host")
    assert not is_secret("status_callback")


def test_describe_config_lists_keys_but_not_values(registry, book):
    create_config(_smtp())
    payload = describe_config(book.get("mail"))

    assert payload["secretKeys"] == ["password"]
    assert payload["credentials"]["password"] == MASK
    assert payload["credentials"]["host"] == "smtp.example.com"
    assert payload["source"] == "runtime"
    assert "s3cr3t-token" not in json.dumps(payload)


# ---------------------------------------------------------------- 配置文件写回


def test_persist_config_appends_and_preserves_retries(tmp_path):
    path = tmp_path / "providers.json"
    _write_file(
        path,
        {
            "providers": [{"id": "console-email", "channel": "email", "kind": "console"}],
            "retries": {"default": {"attempts": 3}},
        },
    )

    factory_mod.persist_config(_smtp(), path=path)
    saved = _read_file(path)

    assert saved["retries"] == {"default": {"attempts": 3}}
    assert [item["id"] for item in saved["providers"]] == ["console-email", "mail"]
    assert saved["providers"][1]["credentials"]["host"] == "smtp.example.com"

    # 再写一次是整条改写，不是追加第二条
    factory_mod.persist_config(_smtp(priority=3), path=path)
    saved = _read_file(path)
    assert len(saved["providers"]) == 2
    assert saved["providers"][1]["priority"] == 3
    # 重新读回来仍然是一份合法配置
    assert [cfg.kind for cfg in factory_mod.load_configs(path)] == ["console", "smtp"]


def test_persist_config_accepts_top_level_array(tmp_path):
    path = tmp_path / "providers.json"
    _write_file(path, [{"id": "console-email", "channel": "email", "kind": "console"}])

    factory_mod.persist_config(
        ProviderConfig(channel=Channel.CHAT, kind="console", id="console-chat"), path=path
    )

    saved = _read_file(path)
    assert isinstance(saved, list)
    assert [item["id"] for item in saved] == ["console-email", "console-chat"]


def test_persist_removal_tolerates_missing_entry(tmp_path):
    path = tmp_path / "providers.json"
    _write_file(
        path,
        {"providers": [{"id": "console-email", "channel": "email", "kind": "console"}], "retries": {"default": {}}},
    )

    target, removed = factory_mod.persist_removal("absent", path=path)
    assert removed == 0
    assert _read_file(path)["providers"]

    target, removed = factory_mod.persist_removal("console-email", path=path)
    assert removed == 1
    saved = _read_file(path)
    assert saved["providers"] == []
    assert saved["retries"] == {"default": {}}


def test_create_with_persist_writes_file(registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    _write_file(path, {"providers": []})
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(path))

    result = create_config(_console(identifier="persisted-email", priority=42), persist=True)

    assert result.persisted == path
    saved = _read_file(path)
    assert saved["providers"][0]["id"] == "persisted-email"
    assert saved["providers"][0]["priority"] == 42
    assert registry.entry("persisted-email").priority == 42


def test_delete_with_persist_removes_from_file(registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    _write_file(
        path,
        {
            "providers": [
                {"id": "console-email", "channel": "email", "kind": "console"},
                {"id": "console-inapp", "channel": "in_app", "kind": "console"},
            ],
            "retries": {"default": {"attempts": 2}},
        },
    )
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(path))

    result = delete_config("console-email", persist=True)

    assert result.persisted == path
    saved = _read_file(path)
    assert [item["id"] for item in saved["providers"]] == ["console-inapp"]
    assert saved["retries"] == {"default": {"attempts": 2}}


def test_persist_failure_keeps_previous_state(registry, tmp_path, monkeypatch):
    """配置文件写不进去时，内存里的条目不能被改掉。"""
    broken = tmp_path / "providers.json"
    broken.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(broken))

    with pytest.raises(factory_mod.ProviderConfigError):
        update_config("console-email", priority=3, persist=True)
    assert registry.entry("console-email").priority == 10

    with pytest.raises(factory_mod.ProviderConfigError):
        create_config(_console(identifier="new-email"), persist=True)
    assert registry.entry("new-email") is None


# ------------------------------------------------------------------------ 接口


def test_api_create_provider(client, registry):
    response = client.post(
        "/v1/providers",
        json={
            "channel": "email",
            "kind": "smtp",
            "id": "api-mail",
            "priority": 20,
            "credentials": {
                "host": "smtp.example.com",
                "from": "notify@example.com",
                "port": 587,
                "username": "notify",
                "password": "p@ss",
            },
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["created"] is True
    assert body["removed"] is False
    assert body["persisted"] is False
    assert body["runtimeOnly"] is True
    assert body["provider"]["id"] == "api-mail"
    assert body["provider"]["kind"] == "smtp"
    assert body["provider"]["state"] == "ok"
    # 数字端口按字符串收下
    assert body["config"]["credentials"]["port"] == "587"
    # 口令只回掩码
    assert body["config"]["credentials"]["password"] == MASK
    assert body["config"]["secretKeys"] == ["password"]
    assert "p@ss" not in response.text
    assert registry.entry("api-mail") is not None

    listed = client.get("/v1/providers").json()["items"]
    assert "api-mail" in [item["id"] for item in listed]


def test_api_create_duplicate_conflicts(client):
    response = client.post(
        "/v1/providers",
        json={"channel": "email", "kind": "console", "id": "console-email"},
    )
    assert response.status_code == 409
    assert "console-email" in response.json()["detail"]


def test_api_create_rejects_invalid_input(client, registry):
    unknown_channel = client.post("/v1/providers", json={"channel": "order", "kind": "console"})
    assert unknown_channel.status_code == 400

    step_channel = client.post("/v1/providers", json={"channel": "delay", "kind": "console"})
    assert step_channel.status_code == 400
    assert "编排步骤" in step_channel.json()["detail"]

    missing_credentials = client.post(
        "/v1/providers", json={"channel": "email", "kind": "smtp", "id": "no-creds"}
    )
    assert missing_credentials.status_code == 400
    assert "缺少必填凭据" in missing_credentials.json()["detail"]

    blank_kind = client.post("/v1/providers", json={"channel": "email", "kind": "  "})
    assert blank_kind.status_code == 400

    assert registry.entry("no-creds") is None


def test_api_create_auto_identifier(client):
    response = client.post("/v1/providers", json={"channel": "push", "kind": "push-mock"})
    assert response.status_code == 201
    assert response.json()["provider"]["id"] == "push"


def test_api_update_provider(client, registry):
    created = client.post(
        "/v1/providers",
        json={"channel": "email", "kind": "smtp", "id": "api-mail", "credentials": {"host": "h", "from": "a@b.c"}},
    )
    assert created.status_code == 201

    response = client.put(
        "/v1/providers/api-mail",
        json={
            "priority": 3,
            "enabled": False,
            "credentials": {"host": "h2", "from": "a@b.c", "username": "u", "password": "pw"},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["changed"] is True
    assert body["provider"]["priority"] == 3
    assert body["provider"]["enabled"] is False
    assert body["provider"]["state"] == "disabled"
    assert body["config"]["credentials"] == {
        "from": "a@b.c",
        "host": "h2",
        "password": MASK,
        "username": "u",
    }
    assert body["warning"] == "渠道 email 改由 console-email 承接"

    unchanged = client.put("/v1/providers/api-mail", json={"priority": 3})
    assert unchanged.json()["changed"] is False


def test_api_update_rejects_channel_change(client):
    response = client.put("/v1/providers/console-email", json={"channel": "sms"})
    assert response.status_code == 409
    assert "删除再新增" in response.json()["detail"]

    # 传同样的渠道不算冲突
    same = client.put("/v1/providers/console-email", json={"channel": "email", "priority": 10})
    assert same.status_code == 200


def test_api_update_unknown_identifier(client):
    assert client.put("/v1/providers/nope", json={"priority": 1}).status_code == 404


def test_api_update_needs_body(client):
    assert client.put("/v1/providers/console-email").status_code == 422


def test_api_delete_provider(client, registry):
    client.post("/v1/providers", json={"channel": "sms", "kind": "console", "id": "temp-sms"})

    response = client.delete("/v1/providers/temp-sms")
    assert response.status_code == 200
    body = response.json()
    assert body["removed"] is True
    assert body["changed"] is True
    assert body["provider"]["state"] == "removed"
    assert body["config"]["id"] == "temp-sms"
    assert registry.entry("temp-sms") is None
    assert client.get("/v1/providers/temp-sms").status_code == 404
    assert "temp-sms" not in [item["id"] for item in client.get("/v1/providers").json()["items"]]


def test_api_delete_unknown_identifier(client):
    assert client.delete("/v1/providers/nope").status_code == 404


def test_api_delete_persists_removal(client, registry, tmp_path, monkeypatch):
    path = tmp_path / "providers.json"
    _write_file(
        path,
        {
            "providers": [
                {"id": "console-email", "channel": "email", "kind": "console"},
                {"id": "console-inapp", "channel": "in_app", "kind": "console"},
            ]
        },
    )
    monkeypatch.setattr(factory_mod.config, "PROVIDERS_FILE", str(path))

    response = client.delete("/v1/providers/console-inapp", params={"persist": "true"})

    assert response.status_code == 200
    assert response.json()["persisted"] is True
    assert [item["id"] for item in _read_file(path)["providers"]] == ["console-email"]


def test_api_list_shows_redacted_config(client):
    item = next(
        entry for entry in client.get("/v1/providers").json()["items"] if entry["id"] == "console-email"
    )
    assert item["config"]["kind"] == "console"
    assert item["config"]["source"] == "runtime"
    assert item["config"]["credentials"] == {}


def test_api_health_and_config_stay_in_step(client):
    """改完之后列表里的 kind/priority 与配置回显一致，不会一个变了另一个没变。"""
    client.put("/v1/providers/console-email", json={"priority": 2})

    item = next(
        entry for entry in client.get("/v1/providers").json()["items"] if entry["id"] == "console-email"
    )
    assert item["priority"] == 2
    assert item["config"]["priority"] == 2
    assert item["primary"] is True
