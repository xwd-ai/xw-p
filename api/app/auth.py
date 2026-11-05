"""环境隔离、租户隔离与请求鉴权。

提供统一的 FastAPI 依赖 ``require_env``，解析请求的 ``X-Environment``、
``X-Tenant-Id`` 与凭据：

- 环境：默认 ``development``，写错直接 400。
- 租户：默认 ``default``。配置了 ``XW_TENANTS`` 时只接受清单内的租户。
- 凭据有两条路：服务端用 ``X-Api-Key``（长期），运营后台用
  ``Authorization: Bearer <JWT>``（会话）。两者都解析出「环境 + 租户」，
  业务接口不区分它们从哪来；差别在权限——``/v1/admin/*`` 由
  ``app.admin_auth.require_admin`` 校验，只接受后台令牌或带 ``admin``
  角色的 Key。

未配置任何凭据时放行（demo / 测试开箱即用）。一旦配置了 API Key，
请求必须携带匹配的 Key，且 Key 绑定的租户与环境必须与请求一致；配置了
``XW_JWT_SECRET`` 时后台接口只接受令牌或管理 Key。

依赖返回 ``EnvContext``，携带解析出的环境与租户。租户同时写入请求上下文
（``app.tenancy``），存储层据此选择数据分区——每个路由不必手工把租户
传进每一次取存储的调用。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException, status

from . import config, tenancy, tokens
from .models.entities import Environment

# 请求头名称：环境标识、租户标识、API Key 与后台令牌
ENV_HEADER = "X-Environment"
TENANT_HEADER = tenancy.TENANT_HEADER
API_KEY_HEADER = "X-Api-Key"
AUTHORIZATION_HEADER = "Authorization"
BEARER_SCHEME = "bearer"

# 支持的环境集合（与 Environment 枚举保持一致）
_VALID_ENVS = {e.value for e in Environment}

#: 凭据不可用时的统一文案。Key 不存在、Key 不能用于当前环境、令牌签名不对，
#: 对外都是同一句话：多一种说法就多一条「这个标识是真的」的线索。
_CREDENTIALS_INVALID = "API Key 缺失或不匹配"


@dataclass(frozen=True)
class EnvContext:
    """鉴权依赖的返回上下文：解析出的隔离环境与租户。"""

    environment: str
    tenant: str = tenancy.DEFAULT_TENANT


@dataclass(frozen=True)
class Resolved:
    """一次凭据解析的完整结果：作用域之外的「谁」也在里面。

    业务接口只用 ``context``；运维接口还要知道凭据来源与角色，用于区分
    「令牌的有效期」和「Key 的长期授权」这两种生命周期不同的身份。
    """

    context: EnvContext
    subject: str
    role: str
    #: 凭据来源：``jwt`` / ``api-key`` / ``demo``
    source: str


def resolve_env(raw: Optional[str]) -> str:
    """解析环境头：为空回退默认环境；非法值直接拒绝，避免静默错环境。"""
    if raw is None or raw.strip() == "":
        return config.ENV
    env = raw.strip().lower()
    if env not in _VALID_ENVS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"未知环境：{raw}（支持：{', '.join(sorted(_VALID_ENVS))}）",
        )
    return env


def resolve_tenant(raw: Optional[str]) -> str:
    """解析租户头：字符集不合法或不在清单内时拒绝。"""
    allowed = tenancy.allowed_tenants()

    # 声明了租户清单又没有 default 时，不带租户头不能静默落到一个不存在的
    # 租户上：报「未知租户：default」会让人以为是标识写错了，实际上缺的是头。
    if (raw is None or raw.strip() == "") and allowed and tenancy.DEFAULT_TENANT not in allowed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"缺少 {TENANT_HEADER}（可用：{', '.join(sorted(allowed))}）",
        )

    try:
        tenant = tenancy.normalize_tenant(raw)
    except tenancy.TenantError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    if allowed and tenant not in allowed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"未知租户：{tenant}（支持：{', '.join(sorted(allowed))}）",
        )
    return tenant


def parse_bearer(raw: Optional[str]) -> Optional[str]:
    """从 ``Authorization`` 头取出令牌；不是 Bearer 写法时拒绝而不是忽略。

    忽略一个写错的鉴权头，表现是「明明带了令牌却按匿名处理」，比直接报错
    难查得多。
    """
    if raw is None or raw.strip() == "":
        return None
    scheme, _, value = raw.strip().partition(" ")
    if scheme.lower() != BEARER_SCHEME:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"不支持的鉴权方式：{scheme}（后台令牌写作 Authorization: Bearer <token>）",
        )
    token = value.strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization: Bearer 后缺少令牌"
        )
    return token


def verify_token(token: str) -> tokens.Claims:
    """校验后台令牌，把失败折成 401 / 503。"""
    try:
        return tokens.verify(token)
    except tokens.JwtConfigError as exc:
        # 服务端配置问题不该被当成「调用方凭据错误」——那样运维会一直去查
        # 调用方的令牌。
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)
        ) from exc
    except tokens.JwtError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc


def _check_scope(
    scope: tenancy.ApiKeyScope, env: str, header_tenant: str, *, tenant_explicit: bool
) -> EnvContext:
    """校验 Key 的作用域并返回请求上下文（环境 + Key 绑定的租户）。"""
    if not scope.allows_env(env):
        allowed_envs = ", ".join(scope.environments)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"API Key 不适用于环境 {env}（允许：{allowed_envs}）",
        )
    if tenant_explicit and header_tenant != scope.tenant:
        # 带着自己的凭据访问另一个租户的数据，是越权而不是凭据无效。
        # 只在请求显式指定了租户时判定：不带租户头是「用 Key 自己的租户」，
        # 不是「要访问 default」。
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"API Key 属于租户 {scope.tenant}，不能访问租户 {header_tenant}",
        )
    return EnvContext(environment=env, tenant=scope.tenant)


def resolve_context(
    env: str,
    header_tenant: str,
    api_key: Optional[str],
    bearer: Optional[str] = None,
    *,
    tenant_explicit: bool = False,
    required_role: Optional[str] = None,
    missing_detail: str = _CREDENTIALS_INVALID,
) -> Resolved:
    """按「令牌优先、Key 兜底」解析凭据，返回完整身份。

    令牌优先而不是「两者都试」：一个请求同时带两种凭据时，该按哪一个判定
    必须有确定答案，否则同一份请求在令牌过期后会悄悄降级成 Key 的身份。

    未启用后台密钥（``XW_JWT_SECRET`` 为空）时忽略 ``Authorization`` 头：
    浏览器里存着一个旧令牌、而当前部署没开后台鉴权是很常见的情形，这时报
    「未配置密钥」只会把整个页面挡在 503 上。忽略它不构成绕过——配了 Key 的
    部署仍然按 Key 判定，没配 Key 的部署本来就是开放的。
    """
    if bearer is not None and tokens.enabled():
        claims = verify_token(bearer)
        if not claims.allows_env(env):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"令牌不适用于环境 {env}"
                f"（允许：{', '.join(claims.environments) or '全部'}）",
            )
        if tenant_explicit and header_tenant != claims.tenant:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"令牌属于租户 {claims.tenant}，不能访问租户 {header_tenant}",
            )
        if required_role and claims.role != required_role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"令牌缺少 {required_role} 角色，不能调用运维接口",
            )
        return Resolved(
            context=EnvContext(environment=env, tenant=claims.tenant),
            subject=claims.subject,
            role=claims.role,
            source="jwt",
        )

    scopes = tenancy.load_scopes()
    if not scopes:
        # 要求角色的接口不接受「谁都没配凭据」这条路径：此时没有任何可判定的
        # 身份，放行等于把运维接口暴露给匿名调用方。
        if required_role:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=missing_detail)
        # demo 语义：租户由请求头决定。
        return Resolved(
            context=EnvContext(environment=env, tenant=header_tenant),
            subject=header_tenant,
            role="",
            source="demo",
        )

    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=missing_detail)
    scope = scopes.get(api_key)
    if scope is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=missing_detail)
    context = _check_scope(scope, env, header_tenant, tenant_explicit=tenant_explicit)
    if required_role and not scope.has_role(required_role):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"该 API Key 缺少 {required_role} 角色，不能调用运维接口",
        )
    return Resolved(
        context=context,
        subject=scope.label or "api-key",
        role=required_role or "",
        source="api-key",
    )


def apply_context(context: EnvContext) -> EnvContext:
    """把上下文写进请求作用域，存储层随后可直接读取。"""
    tenancy.use_tenant(context.tenant)
    tenancy.use_environment(context.environment)
    return context


async def require_env(
    x_environment: Optional[str] = Header(
        default=None, alias=ENV_HEADER, description="隔离环境：development（默认） | production"
    ),
    x_tenant: Optional[str] = Header(
        default=None, alias=TENANT_HEADER, description="租户标识（默认 default）"
    ),
    x_api_key: Optional[str] = Header(
        default=None, alias=API_KEY_HEADER, description="该环境的 API Key（配置了 Key 时必填）"
    ),
    authorization: Optional[str] = Header(
        default=None,
        alias=AUTHORIZATION_HEADER,
        description="后台令牌：Authorization: Bearer <token>（与 X-Api-Key 二选一）",
    ),
) -> EnvContext:
    """解析环境、租户并校验凭据（API Key 或后台令牌）。

    语义：
    - 环境头为空时使用服务默认环境（``XW_ENV``）。
    - 带 ``Authorization: Bearer`` 时按令牌判定：环境须在令牌允许集合内，
      租户取令牌的绑定值，与请求头不一致返回 403。
    - 否则按 API Key 判定：未配置任何 Key 时放行、租户由请求头决定；
      配置后 Key 必须命中，租户取 Key 的绑定值。
    - Key 不覆盖当前环境时返回 401，与「Key 不存在」同一口径：403 会让
      调用方区分出「这个 Key 是真的、只是用错了环境」，等于给出凭证探测的
      信号；用错环境本来就是这个 Key 在当前请求里不成立。
    - Key 命中但请求头指明了另一个租户时返回 403——这是拿着自己的凭据
      访问别人的数据，属于越权而不是凭证无效。
    """
    env = resolve_env(x_environment)
    header_tenant = resolve_tenant(x_tenant)
    bearer = parse_bearer(authorization)
    resolved = resolve_context(
        env,
        header_tenant,
        x_api_key,
        bearer,
        tenant_explicit=bool(x_tenant and x_tenant.strip()),
    )
    return apply_context(resolved.context)


__all__ = [
    "API_KEY_HEADER",
    "AUTHORIZATION_HEADER",
    "BEARER_SCHEME",
    "ENV_HEADER",
    "TENANT_HEADER",
    "EnvContext",
    "Resolved",
    "apply_context",
    "parse_bearer",
    "require_env",
    "resolve_context",
    "resolve_env",
    "resolve_tenant",
    "verify_token",
]
