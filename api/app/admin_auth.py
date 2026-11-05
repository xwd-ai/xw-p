"""运维接口（``/v1/admin/*``）的鉴权。

与业务接口共用同一套「环境 + 租户」解析，多一道角色判定：

- 配置了 ``XW_JWT_SECRET``（后台鉴权启用）时，只接受两种凭据——后台登录
  换来的 JWT，或 ``XW_API_KEYS`` 里显式声明了 ``roles: ["admin"]`` 的 Key。
  服务端那把长期 Key 默认不能调运维接口：它每天要发几百万条通知，一旦泄露，
  「能发通知」和「能停用模板、看全量统计」是两件严重程度不同的事。
- 未配置密钥时保持 demo 语义，与其它接口同一套判定。本地起一个进程就要先
  配密钥、先登录，会让「打开页面看一眼」变成五步操作。

失败面比业务接口细一点：

- 没有凭据 / 凭据不认识 / 令牌过期，401；
- 凭据有效但不是后台身份（Key 缺 admin 角色、令牌角色不对、租户不匹配），403；
- 服务端没配好（密钥过短），503——这不该让调用方去查自己的令牌。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException, status

from . import auth, tenancy, tokens

#: 后台接口缺少凭据时的提示；比业务接口的具体一点——这里只有一种正确做法。
_MISSING = "后台接口需要登录：先 POST /v1/auth/token 换取令牌，再带 Authorization: Bearer <token>"


@dataclass(frozen=True)
class AdminPrincipal:
    """通过后台鉴权后得到的身份。"""

    environment: str
    tenant: str
    subject: str
    role: str
    #: 凭据来源：``jwt`` / ``api-key`` / ``demo``
    credential: str


async def require_admin(
    x_environment: Optional[str] = Header(
        default=None, alias=auth.ENV_HEADER, description="隔离环境：development（默认） | production"
    ),
    x_tenant: Optional[str] = Header(
        default=None, alias=auth.TENANT_HEADER, description="租户标识（默认 default）"
    ),
    x_api_key: Optional[str] = Header(
        default=None, alias=auth.API_KEY_HEADER, description="带 admin 角色的 API Key（服务间调用）"
    ),
    authorization: Optional[str] = Header(
        default=None,
        alias=auth.AUTHORIZATION_HEADER,
        description="后台令牌：Authorization: Bearer <token>",
    ),
) -> AdminPrincipal:
    """校验运维接口的凭据，返回后台身份并把上下文写入请求作用域。"""
    env = auth.resolve_env(x_environment)
    header_tenant = auth.resolve_tenant(x_tenant)
    bearer = auth.parse_bearer(authorization)
    tenant_explicit = bool(x_tenant and x_tenant.strip())

    if tokens.enabled() and bearer is None and not x_api_key:
        # 早一步给出「该怎么做」，而不是让业务接口那句「API Key 缺失」把
        # 人引到错误的方向上。
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=_MISSING)

    # 未启用后台密钥时不加角色要求，保持 demo 语义：运维接口与业务接口同一
    # 套判定，配置了 Key 的部署行为也不会因为这次改动而变。
    required_role = tenancy.ROLE_ADMIN if tokens.enabled() else None
    resolved = auth.resolve_context(
        env,
        header_tenant,
        x_api_key,
        bearer,
        tenant_explicit=tenant_explicit,
        required_role=required_role,
        missing_detail=_MISSING,
    )
    auth.apply_context(resolved.context)
    return AdminPrincipal(
        environment=resolved.context.environment,
        tenant=resolved.context.tenant,
        subject=resolved.subject,
        role=resolved.role or tenancy.ROLE_ADMIN,
        credential=resolved.source,
    )


__all__ = ["AdminPrincipal", "require_admin"]
