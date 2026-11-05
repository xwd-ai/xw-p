"""后台令牌（JWT）鉴权与运维接口权限的测试。

分三层：

- 密码学与声明校验：签名、算法、有效期、时钟容差、签发方、租户与环境；
  另有一条用**独立实现**（测试里手写 hmac）算出的固定向量，锁住跨语言的
  报文格式——只用自己的编码器验自己的输出，等于没验。
- 账号口令：比较恒定、错误统一文案、未配置时不冒充「口令错」。
- 接口层：登录换令牌、``/v1/auth/me`` 回显、业务接口用令牌通过、运维接口
  只接受令牌或带 admin 角色的 Key、以及未配置密钥时保持原有 demo 语义。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import pytest
from fastapi.testclient import TestClient

from app import admin_auth, auth, config, tenancy, tokens
from app.main import app

SECRET = "test-secret-0123456789abcdef"


# --- 夹具 ---------------------------------------------------------------------


@pytest.fixture()
def isolated(monkeypatch, tmp_path):
    """内存存储 + 临时文件，避免用例写进 api/data。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.execution_log as ex_mod
    import app.idempotency as idem_mod
    import app.inbox as inbox_mod
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
    for mod in (sub_mod, topic_mod, idem_mod, wf_mod):
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
    monkeypatch.setattr(config, "JWT_SECRET", "")
    monkeypatch.setattr(config, "JWT_TTL", "3600")
    monkeypatch.setattr(config, "JWT_ISSUER", "xw-notify")
    monkeypatch.setattr(config, "ADMIN_USERNAME", "")
    monkeypatch.setattr(config, "ADMIN_PASSWORD", "")
    monkeypatch.setattr(config, "ADMIN_TENANT", "default")
    monkeypatch.setattr(config, "ADMIN_ENVIRONMENTS", "")
    return tmp_path


@pytest.fixture()
def client(isolated):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def enabled(monkeypatch):
    """启用后台鉴权：密钥 + 管理员账号。"""
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    monkeypatch.setattr(config, "ADMIN_USERNAME", "ops")
    monkeypatch.setattr(config, "ADMIN_PASSWORD", "s3cret-pass")
    return SECRET


def _login(client: TestClient, username="ops", password="s3cret-pass", **extra) -> dict:
    body = {"username": username, "password": password}
    body.update(extra)
    response = client.post("/v1/auth/token", json=body)
    assert response.status_code == 200, response.text
    return response.json()


# --- 令牌编解码 ---------------------------------------------------------------


def test_roundtrip_claims(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", tenant="acme", environments=["production"], now=1000)
    claims = tokens.verify(issued.token, now=1000)
    assert claims.subject == "ops"
    assert claims.tenant == "acme"
    assert claims.environments == ("production",)
    assert claims.role == tenancy.ROLE_ADMIN
    assert claims.expires_at == 1000 + 3600
    assert claims.issuer == "xw-notify"
    assert len(claims.token_id) == 16


def test_token_ids_are_unique(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    first = tokens.issue(subject="ops", now=1000)
    second = tokens.issue(subject="ops", now=1000)
    assert first.claims.token_id != second.claims.token_id
    # 声明相同但标识不同，编码结果因此也不同：令牌可以按标识对账。
    assert first.token != second.token


def test_cross_implementation_vector(monkeypatch):
    """用独立实现拼一个令牌，验证接收方认这个格式。"""
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    now = 1_700_000_000
    issued = tokens.issue(subject="ops", tenant="default", environments=[], now=now)

    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    header_part, payload_part, signature_part = issued.token.split(".")
    payload = json.loads(base64.urlsafe_b64decode(payload_part + "=" * (-len(payload_part) % 4)))
    assert json.loads(base64.urlsafe_b64decode(header_part + "=" * (-len(header_part) % 4))) == {
        "alg": "HS256",
        "typ": "JWT",
    }
    expected = b64(
        hmac.new(SECRET.encode(), f"{header_part}.{payload_part}".encode(), hashlib.sha256).digest()
    )
    assert signature_part == expected
    assert payload["exp"] == now + 3600
    assert payload["iss"] == "xw-notify"
    assert payload["role"] == "admin"


def test_expired_token(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", now=1000, ttl=60)
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(issued.token, now=1000 + 60 + tokens.CLOCK_LEEWAY + 1)
    assert exc.value.reason == "expired"


def test_clock_leeway_absorbs_small_skew(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", now=1000, ttl=60)
    # 刚过期 30 秒：仍在容差内，按有效处理
    assert tokens.verify(issued.token, now=1000 + 90).subject == "ops"


def test_tampered_payload_breaks_signature(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", tenant="default", now=1000)
    header_part, payload_part, signature_part = issued.token.split(".")
    payload = json.loads(base64.urlsafe_b64decode(payload_part + "=" * (-len(payload_part) % 4)))
    payload["tenant"] = "other"
    forged = (
        base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    )
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(f"{header_part}.{forged}.{signature_part}", now=1000)
    assert exc.value.reason == "signature"


def test_wrong_secret(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", now=1000)
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(issued.token, now=1000, key="another-secret-0123456789")
    assert exc.value.reason == "signature"


def test_algorithm_none_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)

    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    header = b64(json.dumps({"alg": "none", "typ": "JWT"}).encode())
    payload = b64(json.dumps({"sub": "ops", "exp": int(time.time()) + 60}).encode())
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(f"{header}.{payload}.", now=int(time.time()))
    assert exc.value.reason in {"malformed", "algorithm"}


def test_other_algorithm_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", now=1000)
    header_part, payload_part, signature_part = issued.token.split(".")
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS512", "typ": "JWT"}).encode()).rstrip(b"=")
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(f"{header.decode()}.{payload_part}.{signature_part}", now=1000)
    assert exc.value.reason == "algorithm"


def test_missing_exp_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    token = tokens.encode({"sub": "ops", "iss": "xw-notify"})
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(token, now=1000)
    assert exc.value.reason == "claims"


def test_wrong_issuer(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    issued = tokens.issue(subject="ops", now=1000)
    monkeypatch.setattr(config, "JWT_ISSUER", "someone-else")
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(issued.token, now=1000)
    assert exc.value.reason == "issuer"


def test_not_yet_valid(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    token = tokens.encode(
        {"sub": "ops", "iss": "xw-notify", "exp": 5000, "nbf": 4000}
    )
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(token, now=1000)
    assert exc.value.reason == "not_yet_valid"


def test_future_issued_at_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    token = tokens.encode({"sub": "ops", "iss": "xw-notify", "exp": 5000, "iat": 4000})
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(token, now=1000)
    assert exc.value.reason == "not_yet_valid"


@pytest.mark.parametrize("token", ["", "abc", "a.b", "a.b.c.d", ".."])
def test_malformed_tokens(monkeypatch, token):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(token, now=1000)
    assert exc.value.reason == "malformed"


def test_unknown_environment_in_claims(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    token = tokens.encode(
        {"sub": "ops", "iss": "xw-notify", "exp": 5000, "environments": ["staging"]}
    )
    with pytest.raises(tokens.JwtError) as exc:
        tokens.verify(token, now=1000)
    assert exc.value.reason == "claims"


def test_short_secret_is_refused(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", "short")
    with pytest.raises(tokens.JwtConfigError):
        tokens.secret()
    with pytest.raises(tokens.JwtConfigError):
        tokens.issue(subject="ops")


def test_ttl_parsing(monkeypatch):
    assert tokens.ttl_seconds("120") == 120
    assert tokens.ttl_seconds("abc") == tokens.DEFAULT_TTL
    assert tokens.ttl_seconds("0") == tokens.DEFAULT_TTL
    assert tokens.ttl_seconds("-5") == tokens.DEFAULT_TTL
    assert tokens.ttl_seconds(str(tokens.MAX_TTL * 10)) == tokens.MAX_TTL


def test_admin_tenant_and_environments_validation(monkeypatch):
    monkeypatch.setattr(config, "ADMIN_TENANT", "Acme-Prod")
    assert tokens.admin_tenant() == "acme-prod"
    monkeypatch.setattr(config, "ADMIN_TENANT", "../etc")
    with pytest.raises(tokens.JwtConfigError):
        tokens.admin_tenant()
    monkeypatch.setattr(config, "ADMIN_ENVIRONMENTS", "production, staging")
    with pytest.raises(tokens.JwtConfigError):
        tokens.admin_environments()
    monkeypatch.setattr(config, "ADMIN_ENVIRONMENTS", "production")
    assert tokens.admin_environments() == ("production",)


# --- 账号口令 ---------------------------------------------------------------


def test_authenticate_success(monkeypatch):
    monkeypatch.setattr(config, "ADMIN_USERNAME", "ops")
    monkeypatch.setattr(config, "ADMIN_PASSWORD", "s3cret-pass")
    assert tokens.authenticate("ops", "s3cret-pass") == "ops"
    assert tokens.authenticate(" ops ", "s3cret-pass") == "ops"


@pytest.mark.parametrize(
    "username,password",
    [("ops", "wrong"), ("nobody", "s3cret-pass"), ("", ""), ("ops", "")],
)
def test_authenticate_failures_share_one_message(monkeypatch, username, password):
    monkeypatch.setattr(config, "ADMIN_USERNAME", "ops")
    monkeypatch.setattr(config, "ADMIN_PASSWORD", "s3cret-pass")
    with pytest.raises(tokens.JwtError) as exc:
        tokens.authenticate(username, password)
    assert exc.value.reason == "credentials"
    # 不区分「账号不存在」与「口令错误」：区分了就等于给出账号枚举。
    assert str(exc.value) == "账号或口令不正确"


def test_authenticate_requires_configuration(monkeypatch):
    monkeypatch.setattr(config, "ADMIN_USERNAME", "ops")
    monkeypatch.setattr(config, "ADMIN_PASSWORD", "")
    with pytest.raises(tokens.JwtConfigError):
        tokens.authenticate("ops", "")


# --- 登录接口 -----------------------------------------------------------------


def test_login_issues_token(client, enabled, monkeypatch):
    body = _login(client)
    assert body["tokenType"] == "Bearer"
    assert body["expiresIn"] == 3600
    assert body["subject"] == "ops"
    assert body["tenant"] == "default"
    assert body["role"] == "admin"
    claims = tokens.verify(body["accessToken"], key=enabled)
    assert claims.subject == "ops"
    assert claims.expires_at - claims.issued_at == 3600


def test_login_honours_configured_ttl(client, enabled, monkeypatch):
    monkeypatch.setattr(config, "JWT_TTL", "120")
    assert _login(client)["expiresIn"] == 120


def test_login_wrong_password(client, enabled):
    response = client.post("/v1/auth/token", json={"username": "ops", "password": "nope"})
    assert response.status_code == 401
    assert response.json()["detail"] == "账号或口令不正确"


def test_login_without_secret_is_503(client, monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", "")
    response = client.post("/v1/auth/token", json={"username": "ops", "password": "x"})
    assert response.status_code == 503
    assert "XW_JWT_SECRET" in response.json()["detail"]


def test_login_without_credentials_is_503(client, monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    response = client.post("/v1/auth/token", json={"username": "ops", "password": "x"})
    assert response.status_code == 503
    assert "XW_ADMIN_USERNAME" in response.json()["detail"]


def test_login_for_own_tenant(client, enabled):
    assert _login(client, tenant="default")["tenant"] == "default"


def test_login_cannot_target_another_tenant(client, enabled):
    response = client.post(
        "/v1/auth/token", json={"username": "ops", "password": "s3cret-pass", "tenant": "other"}
    )
    assert response.status_code == 403
    assert "租户" in response.json()["detail"]


def test_me_echoes_claims(client, enabled, monkeypatch):
    monkeypatch.setattr(config, "ADMIN_ENVIRONMENTS", "production")
    token = _login(client)["accessToken"]
    response = client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    body = response.json()
    assert body["subject"] == "ops"
    assert body["environments"] == ["production"]
    assert body["expiresAt"]
    assert body["tokenId"]


def test_me_requires_token(client):
    assert client.get("/v1/auth/me").status_code == 401


def test_me_rejects_garbage(client, enabled):
    response = client.get("/v1/auth/me", headers={"Authorization": "Bearer not-a-token"})
    assert response.status_code == 401


# --- 业务接口用令牌 -----------------------------------------------------------


def test_token_works_on_business_endpoints(client, enabled, monkeypatch):
    """装了后台密钥、又配了服务端 Key 时，令牌应当照样能调业务接口。"""
    monkeypatch.setattr(
        config, "API_KEYS", {"development": "server-key-dev", "production": ""}
    )
    token = _login(client)["accessToken"]
    assert client.get("/v1/workflows").status_code == 401
    response = client.get("/v1/workflows", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200


def test_token_takes_precedence_over_key(client, enabled, monkeypatch):
    monkeypatch.setattr(config, "API_KEYS", {"development": "server-key-dev", "production": ""})
    response = client.get(
        "/v1/workflows",
        headers={"Authorization": "Bearer broken", "X-Api-Key": "server-key-dev"},
    )
    # 令牌坏了就是坏了，不降级成 Key：降级会让「过期后还能用」变成默认行为。
    assert response.status_code == 401


@pytest.mark.parametrize(
    "header",
    ["Basic abc", "Token abc", "Bearer", "Bearer   "],
)
def test_bad_authorization_header(client, enabled, header):
    response = client.get("/v1/workflows", headers={"Authorization": header})
    assert response.status_code == 401


# --- 运维接口权限 -------------------------------------------------------------


def test_admin_requires_login_when_enabled(client, enabled):
    response = client.get("/v1/admin/stats")
    assert response.status_code == 401
    assert "Bearer" in response.json()["detail"]


def test_admin_accepts_token(client, enabled):
    token = _login(client)["accessToken"]
    response = client.get("/v1/admin/stats", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert "totals" in response.json()


def test_admin_accepts_token_on_activity(client, enabled):
    token = _login(client)["accessToken"]
    response = client.get("/v1/admin/activity", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200


def test_admin_rejects_expired_token(client, enabled, monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    expired = tokens.issue(subject="ops", now=int(time.time()) - 7200, ttl=60).token
    response = client.get("/v1/admin/stats", headers={"Authorization": f"Bearer {expired}"})
    assert response.status_code == 401
    assert "过期" in response.json()["detail"]


def test_admin_rejects_environment_outside_token(client, enabled, monkeypatch):
    monkeypatch.setattr(config, "ADMIN_ENVIRONMENTS", "production")
    token = _login(client)["accessToken"]
    response = client.get(
        "/v1/admin/stats",
        headers={"Authorization": f"Bearer {token}", "X-Environment": "development"},
    )
    assert response.status_code == 401
    assert "production" in response.json()["detail"]


def test_admin_rejects_tenant_mismatch(client, enabled):
    token = _login(client)["accessToken"]
    response = client.get(
        "/v1/admin/stats",
        headers={"Authorization": f"Bearer {token}", "X-Tenant-Id": "other"},
    )
    assert response.status_code == 403


def test_admin_accepts_admin_role_key(client, enabled, monkeypatch):
    monkeypatch.setattr(
        config,
        "API_KEY_SCOPES",
        json.dumps([{"key": "ops-key", "tenant": "default", "roles": ["admin"]}]),
    )
    response = client.get("/v1/admin/stats", headers={"X-Api-Key": "ops-key"})
    assert response.status_code == 200


def test_admin_rejects_key_without_admin_role(client, enabled, monkeypatch):
    monkeypatch.setattr(
        config,
        "API_KEY_SCOPES",
        json.dumps([{"key": "server-key", "tenant": "default"}]),
    )
    response = client.get("/v1/admin/stats", headers={"X-Api-Key": "server-key"})
    assert response.status_code == 403
    assert "admin" in response.json()["detail"]


def test_admin_rejects_unknown_key_when_enabled(client, enabled, monkeypatch):
    monkeypatch.setattr(
        config, "API_KEY_SCOPES", json.dumps([{"key": "ops-key", "roles": ["admin"]}])
    )
    response = client.get("/v1/admin/stats", headers={"X-Api-Key": "ghost"})
    assert response.status_code == 401


def test_admin_is_open_in_demo_mode(client):
    """没配密钥也没配后台账号：与业务接口同一套（demo）语义。"""
    assert client.get("/v1/admin/stats").status_code == 200
    assert client.get("/v1/admin/activity/summary").status_code == 200


def test_admin_keeps_key_semantics_when_jwt_disabled(client, monkeypatch):
    """配了服务端 Key 但没启用后台鉴权：行为与改动前一致。"""
    monkeypatch.setattr(config, "API_KEYS", {"development": "server-key-dev", "production": ""})
    assert client.get("/v1/admin/stats").status_code == 401
    assert (
        client.get("/v1/admin/stats", headers={"X-Api-Key": "server-key-dev"}).status_code
        == 200
    )


def test_principal_reports_credential_source(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", SECRET)
    monkeypatch.setattr(
        config, "API_KEY_SCOPES", json.dumps([{"key": "ops-key", "roles": ["admin"]}])
    )
    resolved = auth.resolve_context(
        "development", "default", "ops-key", None, required_role=tenancy.ROLE_ADMIN
    )
    assert resolved.source == "api-key"
    assert resolved.role == tenancy.ROLE_ADMIN
    token = tokens.issue(subject="ops").token
    resolved = auth.resolve_context("development", "default", None, token)
    assert resolved.source == "jwt"
    assert resolved.subject == "ops"


@pytest.mark.parametrize(
    "scopes,expected",
    [
        ('[{"key":"k","roles":["admin"]}]', ("admin",)),
        ('[{"key":"k","roles":"admin,admin"}]', ("admin",)),
        ('[{"key":"k"}]', ()),
        ('[{"key":"k","roles":[]}]', ()),
    ],
)
def test_api_key_roles_parsing(scopes, expected):
    assert tenancy.parse_scopes(scopes)["k"].roles == expected


def test_api_key_unknown_role_is_rejected():
    with pytest.raises(tenancy.TenantError) as exc:
        tenancy.parse_scopes('[{"key":"k","roles":["root"]}]')
    assert "未知角色" in str(exc.value)


def test_legacy_environment_keys_have_no_admin_role(monkeypatch):
    """旧的环境 Key 只代表「能发通知」，不自动获得运维权限。"""
    monkeypatch.setattr(config, "API_KEY_SCOPES", "")
    monkeypatch.setattr(config, "API_KEYS", {"development": "legacy-key", "production": ""})
    scope = tenancy.load_scopes()["legacy-key"]
    assert scope.tenant == tenancy.DEFAULT_TENANT
    assert not scope.has_role(tenancy.ROLE_ADMIN)


def test_admin_principal_dataclass_is_frozen():
    principal = admin_auth.AdminPrincipal(
        environment="development", tenant="default", subject="ops", role="admin", credential="jwt"
    )
    with pytest.raises(Exception):
        principal.subject = "other"  # type: ignore[misc]


def test_stale_token_is_ignored_when_backend_auth_disabled(client, monkeypatch):
    """浏览器里存着旧令牌、部署又没开后台鉴权：不该把整个页面挡在 503 上。"""
    monkeypatch.setattr(config, "JWT_SECRET", "")
    response = client.get("/v1/workflows", headers={"Authorization": "Bearer old.token.value"})
    assert response.status_code == 200


def test_stale_token_still_needs_a_key_when_keys_configured(client, monkeypatch):
    """忽略令牌不等于放行：配了 Key 的部署照样按 Key 判定。"""
    monkeypatch.setattr(config, "JWT_SECRET", "")
    monkeypatch.setattr(config, "API_KEYS", {"development": "server-key-dev", "production": ""})
    url = "/v1/workflows"
    assert client.get(url, headers={"Authorization": "Bearer old.token.value"}).status_code == 401
    assert (
        client.get(
            url,
            headers={"Authorization": "Bearer old.token.value", "X-Api-Key": "server-key-dev"},
        ).status_code
        == 200
    )


def test_me_endpoint_reports_missing_secret(client, monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", "")
    response = client.get("/v1/auth/me", headers={"Authorization": "Bearer a.b.c"})
    assert response.status_code == 503
    assert "XW_JWT_SECRET" in response.json()["detail"]
