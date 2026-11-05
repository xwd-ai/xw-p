"""后台登录：用管理员账号口令换取 JWT，以及查询当前令牌的身份。

两条接口都不读 ``X-Environment``：登录发生在「进入某个环境」之前，令牌里
带着可操作的环境集合，之后每次请求的头必须在集合内。把环境放进请求头而不是
登录表单，是为了让同一个会话能在环境之间切换查看——切环境不该要求重新登录。

状态码：

- 401：账号或口令不正确（与「令牌无效」同一口径，不区分是账号还是口令错）；
- 403：请求的租户不是该管理员所属的租户；
- 503：服务端未配置密钥或管理员账号。这是部署状态，不该让调用方去查自己的
  输入。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, status

from . import auth, tenancy, tokens
from .auth_schemas import PrincipalResponse, TokenRequest, TokenResponse

router = APIRouter(prefix="/v1/auth", tags=["auth"])

_NOT_ENABLED = (
    "后台令牌鉴权未启用：请配置 XW_JWT_SECRET（≥16 字节）与 XW_ADMIN_USERNAME / XW_ADMIN_PASSWORD"
)


def current_claims(
    authorization: Optional[str] = Header(
        default=None,
        alias=auth.AUTHORIZATION_HEADER,
        description="后台令牌：Authorization: Bearer <token>",
    ),
) -> tokens.Claims:
    """取出并校验请求里的后台令牌。"""
    token = auth.parse_bearer(authorization)
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="缺少 Authorization: Bearer 令牌",
        )
    return auth.verify_token(token)


def _require_enabled() -> None:
    if not tokens.enabled():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=_NOT_ENABLED)
    if not tokens.login_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="未配置管理员账号（XW_ADMIN_USERNAME / XW_ADMIN_PASSWORD），无法登录",
        )


def _to_dt(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


@router.post(
    "/token",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
    summary="后台登录：用管理员账号口令换取令牌",
    description=(
        "校验管理员账号口令，签发一个 HS256 的 JWT，后续请求带 "
        "``Authorization: Bearer <token>`` 即可调用业务接口与运维接口。\n\n"
        "**有效期**：由 ``XW_JWT_TTL`` 决定（默认 3600 秒，上限 24 小时），"
        "回执里的 ``expiresIn`` 是本次实际值，客户端按它安排续期。\n\n"
        "**租户与环境**：令牌绑定 ``XW_ADMIN_TENANT`` 指定的租户，可操作的环境由 "
        "``XW_ADMIN_ENVIRONMENTS`` 限定（留空表示不限）。请求时 ``X-Environment`` "
        "必须在允许集合内，``X-Tenant-Id`` 与令牌租户不一致返回 403。\n\n"
        "**状态码**：口令错误 401（不区分账号还是口令）；请求了不属于自己的租户 403；"
        "服务端未配置密钥或管理员账号 503。"
    ),
    responses={
        200: {"description": "签发成功"},
        401: {"description": "账号或口令不正确"},
        403: {"description": "请求的租户不属于该管理员"},
        503: {"description": "服务端未启用后台鉴权或未配置账号"},
    },
)
def issue_token(payload: TokenRequest) -> TokenResponse:
    """校验账号口令并签发令牌。"""
    _require_enabled()

    try:
        subject = tokens.authenticate(payload.username, payload.password)
    except tokens.JwtConfigError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except tokens.JwtError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    try:
        bound_tenant = tokens.admin_tenant()
        environments = tokens.admin_environments()
    except tokens.JwtConfigError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    # 只能给自己的租户换令牌：允许登录时指定任意租户，等于把「有一个管理员
    # 账号」变成「可以进任何一个租户」。
    if payload.tenant is not None and payload.tenant.strip():
        try:
            requested = tenancy.normalize_tenant(payload.tenant)
        except tenancy.TenantError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
            ) from exc
        if requested != bound_tenant:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"管理员账号属于租户 {bound_tenant}，不能为租户 {requested} 签发令牌",
            )

    issued = tokens.issue(
        subject=subject, tenant=bound_tenant, environments=environments, role=tenancy.ROLE_ADMIN
    )
    claims = issued.claims
    return TokenResponse(
        access_token=issued.token,
        expires_in=claims.expires_at - claims.issued_at,
        expires_at=_to_dt(claims.expires_at),
        subject=claims.subject,
        tenant=claims.tenant,
        environments=list(claims.environments),
        role=claims.role,
    )


@router.get(
    "/me",
    response_model=PrincipalResponse,
    summary="当前令牌的身份与有效期",
    description=(
        "回显 ``Authorization`` 里令牌的声明：主体、租户、可操作环境、角色与过期时刻。\n\n"
        "用于两件事：前端判断会话是否还有效（比等到业务接口返回 401 再处理更早），"
        "以及排查「为什么这个令牌改不了生产环境」——把 ``environments`` 与实际请求头"
        "摆在一起看，比翻配置快。"
    ),
    responses={
        200: {"description": "令牌有效"},
        401: {"description": "缺少令牌、签名不匹配或已过期"},
        503: {"description": "服务端未配置签名密钥"},
    },
)
def read_principal(claims: tokens.Claims = Depends(current_claims)) -> PrincipalResponse:
    """返回当前令牌的声明。"""
    return PrincipalResponse(
        subject=claims.subject,
        tenant=claims.tenant,
        environments=list(claims.environments),
        role=claims.role,
        issued_at=_to_dt(claims.issued_at) if claims.issued_at else None,
        expires_at=_to_dt(claims.expires_at),
        token_id=claims.token_id,
    )


__all__ = ["router"]
