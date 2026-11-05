"""后台会话令牌：JWT（HS256）的签发与校验。

只依赖标准库。不引第三方 JWT 库的原因不是省一个依赖，而是这里只需要一种
算法、一组固定声明：引进来的是一个能配 ``algorithms`` 列表、能解 JWK、
能自动续期的库，用错一处（比如把 ``none`` 留在允许列表里）就是一个静默的
越权入口。四十行 HMAC 加显式校验更容易被审阅。

格式与通用实现一致，接收方可以用任意 JWT 库验证：

    {"alg": "HS256", "typ": "JWT"} . {"sub": ..., "exp": ...} . <签名>

几条刻意的取舍：

- **算法写死在代码里**，不从令牌头读取。``alg`` 只用于校验「是不是 HS256」，
  出现 ``none`` / ``HS512`` 一律拒绝——这是 JWT 最经典的绕过方式。
- **``exp`` 必填**。缺过期的令牌等于一张永久通行证，签发侧漏写一个字段
  就能让它永不过期。
- **签名比较用 ``hmac.compare_digest``**，避免按字节短路带来的时序差异。
- **校验与错误文案分离**：对外给「过期 / 签名不匹配 / 格式不正确」，足以
  指导调用方下一步，又不含密钥、不含内部字段。``JwtError`` 带稳定的
  ``reason``，日志与测试按它判定。
- **密钥最短 16 字节**，低于下限时直接拒绝签发与校验，而不是照常签出一个
  弱密钥令牌——HS256 的强度完全取决于密钥长度。
- **时钟容差 60 秒**：签发方与校验方不在同一台机器上时，几十秒的偏差很常见；
  容差只放宽过期与生效时刻，不放宽签名。
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import secrets
import time
from dataclasses import dataclass
from typing import Iterable, Optional

from . import config, tenancy
from .models.entities import Environment

#: 唯一支持的签名算法
ALGORITHM = "HS256"

#: 允许的时钟偏差（秒）
CLOCK_LEEWAY = 60

#: 密钥下限（字节）：HS256 的强度取决于密钥，短密钥等于没有签名
MIN_SECRET_BYTES = 16

#: 令牌有效期的默认值与上限（秒）
DEFAULT_TTL = 3600
MAX_TTL = 86400

#: 后台操作员角色
ROLE_ADMIN = "admin"

#: base64url 的字符集；用它先排除非法字符，再交给 base64 解码
_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]*$")

_VALID_ENVS = {e.value for e in Environment}


class JwtError(ValueError):
    """令牌不合法或已失效。

    ``reason`` 是稳定的分类（``malformed`` / ``algorithm`` / ``signature`` /
    ``expired`` / ``not_yet_valid`` / ``issuer`` / ``claims``），测试与日志按它
    判定；``str(exc)`` 是可以直接返回给调用方的说明。
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class JwtConfigError(RuntimeError):
    """服务端配置缺失或不合法（未设密钥、密钥过短、未配管理员口令）。"""


@dataclass(frozen=True)
class Claims:
    """令牌声明。"""

    subject: str
    tenant: str
    environments: tuple[str, ...]
    role: str
    issued_at: int
    expires_at: int
    issuer: str
    token_id: str

    def allows_env(self, environment: str) -> bool:
        return not self.environments or environment in self.environments


@dataclass(frozen=True)
class IssuedToken:
    """签发结果：令牌本体 + 声明，回执里两样都要。"""

    token: str
    claims: Claims


# --- 配置 ---------------------------------------------------------------------


def secret() -> str:
    """返回签名密钥；未配置或过短时抛 ``JwtConfigError``。"""
    raw = (config.JWT_SECRET or "").strip()
    if not raw:
        raise JwtConfigError("未配置 XW_JWT_SECRET，后台令牌鉴权未启用")
    if len(raw.encode("utf-8")) < MIN_SECRET_BYTES:
        raise JwtConfigError(
            f"XW_JWT_SECRET 至少需要 {MIN_SECRET_BYTES} 字节（当前 {len(raw.encode('utf-8'))}）"
        )
    return raw


def enabled() -> bool:
    """后台令牌鉴权是否启用。密钥写错（过短）也算启用：此时应当报错而不是退回开放。"""
    return bool((config.JWT_SECRET or "").strip())


def ttl_seconds(raw: Optional[str] = None) -> int:
    """令牌有效期：未配置或写错时回退默认值，并夹紧到上限。"""
    text = raw if raw is not None else config.JWT_TTL
    try:
        value = int(str(text).strip())
    except (TypeError, ValueError):
        return DEFAULT_TTL
    if value <= 0:
        return DEFAULT_TTL
    return min(value, MAX_TTL)


def issuer() -> str:
    return (config.JWT_ISSUER or "").strip() or "xw-notify"


def admin_tenant() -> str:
    try:
        return tenancy.normalize_tenant(config.ADMIN_TENANT)
    except tenancy.TenantError as exc:  # 配置写错不该让登录静默落到默认租户
        raise JwtConfigError(f"XW_ADMIN_TENANT 不合法：{exc}") from exc


def admin_environments() -> tuple[str, ...]:
    """后台账号可操作的环境：逗号分隔，空表示不限制。"""
    raw = config.ADMIN_ENVIRONMENTS or ""
    envs = tuple(part.strip().lower() for part in raw.split(",") if part.strip())
    unknown = [env for env in envs if env not in _VALID_ENVS]
    if unknown:
        raise JwtConfigError(
            f"XW_ADMIN_ENVIRONMENTS 包含未知环境：{', '.join(unknown)}"
            f"（支持：{', '.join(sorted(_VALID_ENVS))}）"
        )
    return envs


def login_configured() -> bool:
    """是否配置了管理员账号；口令为空等于没开通登录。"""
    return bool((config.ADMIN_USERNAME or "").strip()) and bool(config.ADMIN_PASSWORD)


def authenticate(username: Optional[str], password: Optional[str]) -> str:
    """校验管理员账号口令，成功返回规范化的用户名。

    两次比较都执行完再判定，且都用 ``compare_digest``：先比用户名、命中才比
    口令，等于把「这个账号存在」按响应时间告诉对方。
    """
    if not login_configured():
        raise JwtConfigError(
            "未配置管理员账号（XW_ADMIN_USERNAME / XW_ADMIN_PASSWORD），无法登录"
        )
    expected_user = (config.ADMIN_USERNAME or "").strip()
    expected_pass = config.ADMIN_PASSWORD or ""
    given_user = (username or "").strip()
    given_pass = password or ""
    user_ok = hmac.compare_digest(given_user.encode("utf-8"), expected_user.encode("utf-8"))
    pass_ok = hmac.compare_digest(given_pass.encode("utf-8"), expected_pass.encode("utf-8"))
    if not (user_ok and pass_ok):
        raise JwtError("credentials", "账号或口令不正确")
    return expected_user


# --- 编解码 -------------------------------------------------------------------


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64url(text: str) -> bytes:
    if not _B64URL_RE.match(text):
        raise JwtError("malformed", "令牌格式不正确")
    try:
        return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError) as exc:
        raise JwtError("malformed", "令牌格式不正确") from exc


def _json_bytes(value: dict) -> bytes:
    # 键排序 + 紧凑分隔符：同样的声明在任何机器上编码出同样的字节，
    # 跨语言向量测试才有意义。
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _sign(signing_input: bytes, key: str) -> bytes:
    return hmac.new(key.encode("utf-8"), signing_input, hashlib.sha256).digest()


def encode(payload: dict, key: Optional[str] = None) -> str:
    """把声明签成令牌。``key`` 仅供测试注入，生产走配置。"""
    key = key if key is not None else secret()
    header = {"alg": ALGORITHM, "typ": "JWT"}
    signing_input = _b64url(_json_bytes(header)) + "." + _b64url(_json_bytes(payload))
    signature = _sign(signing_input.encode("ascii"), key)
    return signing_input + "." + _b64url(signature)


def issue(
    *,
    subject: str,
    tenant: Optional[str] = None,
    environments: Optional[Iterable[str]] = None,
    role: str = ROLE_ADMIN,
    ttl: Optional[int] = None,
    now: Optional[int] = None,
) -> IssuedToken:
    """签发一个后台令牌。租户与环境缺省取管理员配置。"""
    issued_at = int(time.time()) if now is None else int(now)
    expires_at = issued_at + (ttl_seconds() if ttl is None else int(ttl))
    tenant_value = tenancy.normalize_tenant(tenant if tenant is not None else admin_tenant())
    envs = tuple(environments) if environments is not None else admin_environments()
    claims = Claims(
        subject=subject,
        tenant=tenant_value,
        environments=tuple(envs),
        role=role,
        issued_at=issued_at,
        expires_at=expires_at,
        issuer=issuer(),
        token_id=secrets.token_hex(8),
    )
    payload = {
        "sub": claims.subject,
        "tenant": claims.tenant,
        "environments": list(claims.environments),
        "role": claims.role,
        "iss": claims.issuer,
        "iat": claims.issued_at,
        "exp": claims.expires_at,
        "jti": claims.token_id,
    }
    return IssuedToken(token=encode(payload), claims=claims)


def verify(token: str, *, now: Optional[int] = None, key: Optional[str] = None) -> Claims:
    """校验令牌并返回声明；任何一项不通过都抛 ``JwtError``。"""
    key = key if key is not None else secret()
    parts = (token or "").strip().split(".")
    if len(parts) != 3 or not all(parts):
        raise JwtError("malformed", "令牌格式不正确")

    header_part, payload_part, signature_part = parts
    try:
        header = json.loads(_unb64url(header_part).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JwtError("malformed", "令牌格式不正确") from exc
    if not isinstance(header, dict):
        raise JwtError("malformed", "令牌格式不正确")

    # 算法只认写死的这一种：把 "none" 或别的 HMAC 变体留在允许列表里，
    # 就等于允许令牌自己声明怎么验签。
    if header.get("alg") != ALGORITHM:
        raise JwtError("algorithm", f"不支持的令牌算法：{header.get('alg')}")
    if "typ" in header and header.get("typ") != "JWT":
        raise JwtError("algorithm", f"不支持的令牌类型：{header.get('typ')}")

    expected = _sign((header_part + "." + payload_part).encode("ascii"), key)
    try:
        given = _unb64url(signature_part)
    except JwtError as exc:
        raise JwtError("signature", "令牌签名不匹配") from exc
    if not hmac.compare_digest(expected, given):
        raise JwtError("signature", "令牌签名不匹配")

    try:
        payload = json.loads(_unb64url(payload_part).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JwtError("malformed", "令牌格式不正确") from exc
    if not isinstance(payload, dict):
        raise JwtError("malformed", "令牌格式不正确")

    current = int(time.time()) if now is None else int(now)

    # 过期时间必填：漏写就等于签发了一张永久凭证。
    exp = payload.get("exp")
    if not isinstance(exp, int) or isinstance(exp, bool):
        raise JwtError("claims", "令牌缺少过期时间")
    if current > exp + CLOCK_LEEWAY:
        raise JwtError("expired", "令牌已过期，请重新登录")

    nbf = payload.get("nbf")
    if nbf is not None:
        if not isinstance(nbf, int) or isinstance(nbf, bool):
            raise JwtError("claims", "令牌的生效时间不合法")
        if current + CLOCK_LEEWAY < nbf:
            raise JwtError("not_yet_valid", "令牌尚未生效")

    iat = payload.get("iat")
    if iat is not None:
        if not isinstance(iat, int) or isinstance(iat, bool):
            raise JwtError("claims", "令牌的签发时间不合法")
        if iat > current + CLOCK_LEEWAY:
            raise JwtError("not_yet_valid", "令牌签发时间在未来")

    if payload.get("iss") != issuer():
        raise JwtError("issuer", "令牌签发方不匹配")

    subject = payload.get("sub")
    if not isinstance(subject, str) or not subject.strip():
        raise JwtError("claims", "令牌缺少主体标识")

    try:
        tenant = tenancy.normalize_tenant(payload.get("tenant"))
    except tenancy.TenantError as exc:
        raise JwtError("claims", f"令牌的租户不合法：{exc}") from exc

    raw_envs = payload.get("environments") or []
    if not isinstance(raw_envs, list):
        raise JwtError("claims", "令牌的环境列表不合法")
    environments = tuple(str(part).strip().lower() for part in raw_envs if str(part).strip())
    unknown = [env for env in environments if env not in _VALID_ENVS]
    if unknown:
        raise JwtError("claims", f"令牌包含未知环境：{', '.join(unknown)}")

    role = payload.get("role") or ""
    if not isinstance(role, str):
        raise JwtError("claims", "令牌的角色不合法")

    token_id = payload.get("jti") or ""
    if not isinstance(token_id, str):
        raise JwtError("claims", "令牌标识不合法")

    return Claims(
        subject=subject.strip(),
        tenant=tenant,
        environments=environments,
        role=role.strip(),
        issued_at=int(iat) if isinstance(iat, int) and not isinstance(iat, bool) else 0,
        expires_at=int(exp),
        issuer=str(payload.get("iss")),
        token_id=token_id,
    )


__all__ = [
    "ALGORITHM",
    "CLOCK_LEEWAY",
    "DEFAULT_TTL",
    "MAX_TTL",
    "MIN_SECRET_BYTES",
    "ROLE_ADMIN",
    "Claims",
    "IssuedToken",
    "JwtConfigError",
    "JwtError",
    "admin_environments",
    "admin_tenant",
    "authenticate",
    "enabled",
    "encode",
    "issue",
    "issuer",
    "login_configured",
    "ttl_seconds",
    "verify",
]
