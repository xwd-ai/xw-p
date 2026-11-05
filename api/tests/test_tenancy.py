"""多租户隔离与 API Key 作用域的测试。

覆盖三层：

- 解析层：租户标识的字符集（会拼进文件路径）、``XW_API_KEYS`` 的解析与旧环境
  Key 的合并；
- 存储层：各存储按租户分文件、按租户过滤，默认租户沿用原路径；
- 接口层：``X-Tenant-Id`` 与 ``X-Api-Key`` 怎么决定租户，越权与越界分别是什么状态码。

约定与实现一致：没有 ``tenant`` 字段的老数据算默认租户，因此升级后原有数据
既不会消失、也不会被别的租户看到。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import config, tenancy
from app.main import app


@pytest.fixture()
def iso(monkeypatch, tmp_path):
    """把租户相关的存储挪到临时目录，并在每个用例前清空单例与租户缓存。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.execution_log as ex_mod
    import app.idempotency as idem_mod
    import app.inbox as inbox_mod
    import app.preferences as prefs_mod
    import app.subscriber_store as sub_mod
    import app.topic_store as topic_mod
    import app.workflow_store as wf_mod
    from app.activity_log import MemoryActivityStore
    from app.bus import MemoryMessageBus
    from app.execution_log import MemoryExecutionStore

    monkeypatch.setenv("XW_SUBSCRIBERS_FILE", str(tmp_path / "subscribers.jsonl"))
    monkeypatch.setenv("XW_TOPICS_FILE", str(tmp_path / "topics.jsonl"))
    monkeypatch.setenv("XW_PREFERENCES_FILE", str(tmp_path / "preferences.jsonl"))
    monkeypatch.setattr(inbox_mod, "_INBOX_FILE", tmp_path / "inbox.jsonl")
    monkeypatch.setattr(inbox_mod, "_READ_STATE_FILE", tmp_path / "read_state.jsonl")
    monkeypatch.setattr(inbox_mod, "_read_store", None)
    monkeypatch.setattr(inbox_mod, "_tenant_read_stores", {})
    monkeypatch.setattr(inbox_mod, "_inbox_cache", None)
    monkeypatch.setattr(inbox_mod, "_unread_cache", None)

    class _EmptyStorage:
        """投递记录来源：本组用例只关心收件箱文件与已读状态。"""

        def read_all(self):
            return []

    monkeypatch.setattr(inbox_mod, "get_storage", lambda: _EmptyStorage())

    for mod in (sub_mod, topic_mod, prefs_mod, idem_mod, wf_mod):
        monkeypatch.setattr(mod, "_store", None)
        monkeypatch.setattr(mod, "_tenant_stores", {})
    monkeypatch.setattr(al_mod, "_activity_store", MemoryActivityStore())
    monkeypatch.setattr(ex_mod, "_execution_store", MemoryExecutionStore())
    monkeypatch.setattr(bus_mod, "_bus", MemoryMessageBus())

    monkeypatch.setattr(config, "STORAGE_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)
    monkeypatch.setattr(config, "API_KEYS", {})
    monkeypatch.setattr(config, "API_KEY_SCOPES", "")
    monkeypatch.setattr(config, "TENANTS", "")
    monkeypatch.setattr(config, "ENV", "development")
    return tmp_path


@pytest.fixture()
def client(iso):
    with TestClient(app) as test_client:
        yield test_client


def _trigger_body(subscriber="creator_1", **extra):
    body = {
        "name": "post_commented",
        "to": {"subscriberId": subscriber},
        "payload": {"post_id": "P-1", "user": "Alice"},
    }
    body.update(extra)
    return body


def _subscriber_body(subscriber_id="creator_1", email="creator@example.com"):
    return {"id": subscriber_id, "email": email, "channels": ["email"]}


# ---------------------------------------------------------------- 租户标识


def test_normalize_tenant_defaults_and_lowercases():
    assert tenancy.normalize_tenant(None) == tenancy.DEFAULT_TENANT
    assert tenancy.normalize_tenant("   ") == tenancy.DEFAULT_TENANT
    assert tenancy.normalize_tenant(" ACME ") == "acme"
    assert tenancy.normalize_tenant("team-a_1") == "team-a_1"


@pytest.mark.parametrize(
    "raw",
    [
        "../etc",
        "a/b",
        "a.b",
        "-acme",
        "acme-",
        "acme corp",
        "中文租户",
        "x" * 33,
    ],
)
def test_normalize_tenant_rejects_unsafe_identifiers(raw):
    """租户标识会拼进文件路径，放开 ``.`` 与 ``/`` 就能跳出数据目录。"""
    with pytest.raises(tenancy.TenantError):
        tenancy.normalize_tenant(raw)


def test_scoped_path_keeps_default_tenant_on_legacy_path():
    base = Path("/data/subscribers.jsonl")
    assert tenancy.scoped_path(base, "default") == base
    assert tenancy.scoped_path(base, "acme") == Path("/data/tenants/acme/subscribers.jsonl")


def test_tenant_of_treats_legacy_records_as_default():
    assert tenancy.tenant_of({}) == tenancy.DEFAULT_TENANT
    assert tenancy.tenant_of({"tenant": "acme"}) == "acme"


def test_use_tenant_sets_context():
    token = tenancy.use_tenant("acme")
    try:
        assert tenancy.current_tenant() == "acme"
    finally:
        tenancy.reset_tenant(token)
    assert tenancy.current_tenant() == tenancy.DEFAULT_TENANT


# ---------------------------------------------------------------- Key 作用域


def test_parse_scopes_reads_tenant_and_environments():
    scopes = tenancy.parse_scopes(
        json.dumps(
            [
                {"key": "sk_a", "tenant": "acme", "environments": ["production"], "label": "主站"},
                {"key": "sk_b", "tenant": "beta"},
                {"key": "sk_c", "tenant": "gamma", "environments": "development, production"},
            ]
        )
    )
    assert scopes["sk_a"].tenant == "acme"
    assert scopes["sk_a"].environments == ("production",)
    assert scopes["sk_a"].label == "主站"
    # 不声明 environments 表示不限环境
    assert scopes["sk_b"].allows_env("production")
    # 也接受逗号分隔的字符串写法
    assert scopes["sk_c"].environments == ("development", "production")


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        '{"key": "sk_a"}',
        '[{"tenant": "acme"}]',
        '[{"key": "sk_a", "tenant": "acme", "environments": ["staging"]}]',
        '[{"key": "sk_a", "tenant": "acme", "environments": 3}]',
        '[{"key": "sk_a", "tenant": "../etc"}]',
    ],
)
def test_parse_scopes_rejects_broken_config(raw):
    """写错的配置要吵，不能悄悄跳过——表现会是「配了 Key 却一直 401」。"""
    with pytest.raises(tenancy.TenantError):
        tenancy.parse_scopes(raw)


def test_load_scopes_merges_legacy_environment_keys():
    old_axes = config.API_KEYS
    config.API_KEYS = {"development": "legacy-dev", "production": "legacy-prod"}
    try:
        scopes = tenancy.load_scopes()
    finally:
        config.API_KEYS = old_axes
    assert scopes["legacy-dev"].tenant == tenancy.DEFAULT_TENANT
    assert scopes["legacy-dev"].environments == ("development",)
    assert scopes["legacy-prod"].environments == ("production",)
    # 同一个 Key 出现在两个环境：合并成一条，两个环境都放行
    config.API_KEYS = {"development": "shared", "production": "shared"}
    try:
        merged = tenancy.load_scopes()["shared"]
    finally:
        config.API_KEYS = old_axes
    assert merged.environments == ("development", "production")


# ---------------------------------------------------------------- 接口层


def test_unknown_tenant_header_rejected(client):
    resp = client.get("/v1/activity", headers={"X-Tenant-Id": "../etc"})
    assert resp.status_code == 400
    assert "租户" in resp.json()["detail"]


def test_tenant_allowlist_rejects_undeclared_tenant(client, monkeypatch):
    monkeypatch.setattr(config, "TENANTS", "acme,beta")
    assert client.get("/v1/activity", headers={"X-Tenant-Id": "acme"}).status_code == 200
    resp = client.get("/v1/activity", headers={"X-Tenant-Id": "other"})
    assert resp.status_code == 400
    assert "other" in resp.json()["detail"]


def test_tenant_allowlist_without_default_requires_header(client, monkeypatch):
    """清单里没有 default 时，缺头要指出「缺头」而不是「租户不存在」。"""
    monkeypatch.setattr(config, "TENANTS", "acme,beta")
    resp = client.get("/v1/activity")
    assert resp.status_code == 400
    assert "X-Tenant-Id" in resp.json()["detail"]
    assert "acme" in resp.json()["detail"]


def test_subscribers_are_isolated_per_tenant(client, iso):
    created = client.post(
        "/v1/subscribers",
        json=_subscriber_body(email="acme@example.com"),
        headers={"X-Tenant-Id": "acme"},
    )
    assert created.status_code == 201

    acme_items = client.get("/v1/subscribers", headers={"X-Tenant-Id": "acme"}).json()["items"]
    assert [s["id"] for s in acme_items] == ["creator_1"]
    assert client.get("/v1/subscribers").json()["items"] == []

    # 同一标识在另一个租户里不存在，读单个也拿不到
    assert client.get("/v1/subscribers/creator_1").status_code == 404
    assert (
        client.get("/v1/subscribers/creator_1", headers={"X-Tenant-Id": "acme"}).status_code
        == 200
    )

    # 数据落在租户目录下，默认租户仍用原路径
    assert (iso / "tenants" / "acme" / "subscribers.jsonl").exists()


def test_scoped_key_binds_tenant(client, monkeypatch, iso):
    monkeypatch.setattr(
        config,
        "API_KEY_SCOPES",
        json.dumps([{"key": "sk_acme", "tenant": "acme", "environments": ["development"]}]),
    )
    client.post(
        "/v1/subscribers",
        json=_subscriber_body(email="acme@example.com"),
        headers={"X-Api-Key": "sk_acme"},
    )

    # 带 Key 不带租户头：租户由 Key 决定
    listed = client.get("/v1/subscribers", headers={"X-Api-Key": "sk_acme"})
    assert listed.status_code == 200
    assert [s["id"] for s in listed.json()["items"]] == ["creator_1"]

    # 带 Key 又指明另一个租户：越权，403
    conflict = client.get(
        "/v1/subscribers",
        headers={"X-Api-Key": "sk_acme", "X-Tenant-Id": "other"},
    )
    assert conflict.status_code == 403
    assert "acme" in conflict.json()["detail"]

    # 同租户的租户头与 Key 一致时放行
    same = client.get(
        "/v1/subscribers",
        headers={"X-Api-Key": "sk_acme", "X-Tenant-Id": "acme"},
    )
    assert same.status_code == 200


def test_scoped_key_rejects_wrong_and_out_of_scope(client, monkeypatch):
    monkeypatch.setattr(
        config,
        "API_KEY_SCOPES",
        json.dumps([{"key": "sk_prod", "tenant": "acme", "environments": ["production"]}]),
    )
    # 没带 Key：401
    assert client.get("/v1/activity").status_code == 401
    # 带错 Key：401
    assert client.get("/v1/activity", headers={"X-Api-Key": "sk_other"}).status_code == 401
    # Key 不在该环境的作用域内：401（与「Key 不存在」同一口径，不给凭证探测信号）
    assert client.get("/v1/activity", headers={"X-Api-Key": "sk_prod"}).status_code == 401
    assert (
        client.get(
            "/v1/activity",
            headers={"X-Api-Key": "sk_prod", "X-Environment": "production"},
        ).status_code
        == 200
    )


def test_idempotency_keys_are_scoped_per_tenant(client):
    first = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"Idempotency-Key": "evt-1", "X-Tenant-Id": "acme"},
    )
    replay = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"Idempotency-Key": "evt-1", "X-Tenant-Id": "acme"},
    )
    other_tenant = client.post(
        "/v1/events/trigger",
        json=_trigger_body(),
        headers={"Idempotency-Key": "evt-1", "X-Tenant-Id": "beta"},
    )
    assert first.status_code == replay.status_code == other_tenant.status_code == 200
    # 同租户同键：命中缓存，回执是首次那次
    assert replay.json()["triggerId"] == first.json()["triggerId"]
    # 换租户同键：另一份幂等空间，不该被判成重复
    assert other_tenant.json()["triggerId"] != first.json()["triggerId"]


def test_workflow_store_isolated_per_tenant(client):
    body = {
        "name": "acme_only",
        "trigger": "post_liked",
        "steps": [{"type": "in_app", "channel": "in_app"}],
    }
    created = client.post("/v1/workflows", json=body, headers={"X-Tenant-Id": "acme"})
    assert created.status_code == 201

    acme = client.get("/v1/workflows", headers={"X-Tenant-Id": "acme"}).json()
    assert "acme_only" in [w["name"] for w in acme]
    default = client.get("/v1/workflows").json()
    assert "acme_only" not in [w["name"] for w in default]


def test_activity_log_filtered_by_tenant(client, monkeypatch):
    import app.activity_log as al_mod

    al_mod._activity_store.append(
        al_mod.build_activity(
            trigger_id="t-acme",
            workflow="post_liked",
            subscriber_id="creator_1",
            tenant="acme",
        )
    )
    al_mod._activity_store.append(
        al_mod.build_activity(
            trigger_id="t-default",
            workflow="post_liked",
            subscriber_id="creator_1",
            tenant="default",
        )
    )

    acme = client.get("/v1/activity", headers={"X-Tenant-Id": "acme"}).json()
    assert [i["trigger_id"] for i in acme["items"]] == ["t-acme"]
    default = client.get("/v1/activity").json()
    assert [i["trigger_id"] for i in default["items"]] == ["t-default"]


def test_execution_log_filtered_by_tenant(client, monkeypatch):
    import app.activity_log as al_mod
    import app.execution_log as ex_mod

    for tenant in ("acme", "default"):
        al_mod._activity_store.append(
            al_mod.build_activity(
                trigger_id=f"t-{tenant}",
                workflow="post_liked",
                subscriber_id="creator_1",
                tenant=tenant,
            )
        )
        ex_mod.record_execution(
            trigger_id=f"t-{tenant}",
            channel="in_app",
            status="sent",
            tenant=tenant,
            environment="development",
        )

    acme = client.get("/v1/executions", headers={"X-Tenant-Id": "acme"}).json()
    assert {i["trigger_id"] for i in acme["items"]} == {"t-acme"}
    default = client.get("/v1/executions").json()
    assert {i["trigger_id"] for i in default["items"]} == {"t-default"}


def _write_inbox(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def test_inbox_entries_filtered_by_tenant_and_environment(client, iso):
    _write_inbox(
        iso / "inbox.jsonl",
        [
            {
                "id": "msg-acme",
                "trigger_id": "t-acme",
                "subscriber_id": "creator_1",
                "workflow": "post_liked",
                "title": "acme 的通知",
                "created_at": "2026-05-01T10:00:00+00:00",
                "tenant": "acme",
                "environment": "development",
            },
            {
                "id": "msg-default",
                "trigger_id": "t-default",
                "subscriber_id": "creator_1",
                "workflow": "post_liked",
                "title": "默认租户的通知",
                "created_at": "2026-05-01T09:00:00+00:00",
            },
            {
                "id": "msg-prod",
                "trigger_id": "t-prod",
                "subscriber_id": "creator_1",
                "workflow": "post_liked",
                "title": "生产环境的通知",
                "created_at": "2026-05-01T11:00:00+00:00",
                "tenant": "acme",
                "environment": "production",
            },
        ],
    )

    acme = client.get(
        "/v1/inbox", params={"subscriberId": "creator_1"}, headers={"X-Tenant-Id": "acme"}
    ).json()
    assert [i["message_id"] for i in acme["items"]] == ["msg-acme"]

    # 老数据没有 tenant 字段：仍然属于默认租户
    default = client.get("/v1/inbox", params={"subscriberId": "creator_1"}).json()
    assert [i["message_id"] for i in default["items"]] == ["msg-default"]

    # 环境也参与过滤：acme 的 production 条目只在 production 出现
    prod = client.get(
        "/v1/inbox",
        params={"subscriberId": "creator_1"},
        headers={"X-Tenant-Id": "acme", "X-Environment": "production"},
    ).json()
    assert [i["message_id"] for i in prod["items"]] == ["msg-prod"]


def test_read_state_isolated_per_tenant(client, iso):
    _write_inbox(
        iso / "inbox.jsonl",
        [
            {
                "id": "msg-acme",
                "trigger_id": "t-acme",
                "subscriber_id": "creator_1",
                "workflow": "post_liked",
                "title": "acme 的通知",
                "created_at": "2026-05-01T10:00:00+00:00",
                "tenant": "acme",
            },
            {
                "id": "msg-default",
                "trigger_id": "t-default",
                "subscriber_id": "creator_1",
                "workflow": "post_liked",
                "title": "默认租户的通知",
                "created_at": "2026-05-01T09:00:00+00:00",
            },
        ],
    )

    marked = client.post(
        "/v1/inbox/read",
        json={"subscriberId": "creator_1", "messageIds": ["msg-acme"]},
        headers={"X-Tenant-Id": "acme"},
    )
    assert marked.status_code == 200
    assert marked.json()["marked"] == 1

    acme = client.get(
        "/v1/inbox", params={"subscriberId": "creator_1"}, headers={"X-Tenant-Id": "acme"}
    ).json()
    assert acme["unread"] == 0
    default = client.get("/v1/inbox", params={"subscriberId": "creator_1"}).json()
    assert default["unread"] == 1
    # 已读状态按租户分文件，互不覆盖
    assert (iso / "tenants" / "acme" / "read_state.jsonl").exists()


def test_topic_store_isolated_per_tenant(client):
    created = client.post(
        "/v1/topics",
        json={"name": "acme 分组", "members": ["creator_1"]},
        headers={"X-Tenant-Id": "acme"},
    )
    assert created.status_code == 201
    acme = client.get("/v1/topics", headers={"X-Tenant-Id": "acme"}).json()
    assert "acme 分组" in [t["name"] for t in acme["items"]]
    assert client.get("/v1/topics").json()["items"] == []
