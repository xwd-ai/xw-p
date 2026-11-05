"""环境隔离、租户隔离与 API Key 鉴权。

提供统一的 FastAPI 依赖 ``require_env``，解析请求的 ``X-Environment``、
``X-Tenant-Id`` 与 ``X-Api-Key``：

- 环境：默认 ``development``，写错直接 400。
- 租户：默认 ``default``。配置了 ``XW_TENANTS`` 时只接受清单内的租户。
- Key：未配置任何 Key 时放行（demo / 测试开箱即用）；配置后必须携带
  匹配的 Key，且 Key 绑定的租户与环境必须与请求一致，否则 401 / 403。

依赖返回 ``EnvContext``，携带解析出的环境与租户。租户同时写入请求上下文
（``app.tenancy``），存储层据此选择数据分区——每个路由不必手工把租户
传进每一次取存储的调用。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException, status

from . import config, tenancy
from .models.entities import Environment

# 请求头名称：环境标识、租户标识与 API Key
ENV_HEADER = "X-Environment"
TENANT_HEADER = tenancy.TENANT_HEADER
API_KEY_HEADER = "X-Api-Key"

# 支持的环境集合（与 Environment 枚举保持一致）
_VALID_ENVS = {e.value for e in Environment}


def _resolve_env(raw: Optional[str]) -> str:
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


def _resolve_tenant(raw: Optional[str]) -> str:
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


@dataclass(frozen=True)
class EnvContext:
    """鉴权依赖的返回上下文：解析出的隔离环境与租户。"""

    environment: str
    tenant: str = tenancy.DEFAULT_TENANT


async def require_env(
    x_environment: Optional[str] = Header(
        default=None, alias=ENV_HEADER, description="隔离环境：development（默认）| production"
    ),
    x_tenant: Optional[str] = Header(
        default=None, alias=TENANT_HEADER, description="租户标识（默认 default）"
    ),
    x_api_key: Optional[str] = Header(
        default=None, alias=API_KEY_HEADER, description="该环境的 API Key（配置了 Key 时必填）"
    ),
) -> EnvContext:
    """解析环境、租户并校验 API Key。

    语义：
    - 环境头为空时使用服务默认环境（``XW_ENV``）。
    - 未配置任何 Key：租户由 ``X-Tenant-Id`` 决定，缺省 ``default``。
    - 已配置 Key：Key 必须命中，租户取 Key 的绑定值。
    - Key 不覆盖当前环境时返回 401，与「Key 不存在」同一口径：403 会让
      调用方区分出「这个 Key 是真的、只是用错了环境」，等于给出凭证探测的
      信号；用错环境本来就是这个 Key 在当前请求里不成立。
    - Key 命中但请求头指明了另一个租户时返回 403——这是拿着自己的凭据
      访问别人的数据，属于越权而不是凭证无效。
    """
    env = _resolve_env(x_environment)
    header_tenant = _resolve_tenant(x_tenant)

    scopes = tenancy.load_scopes()
    if scopes:
        if not x_api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API Key 缺失或不匹配",
            )
        scope = scopes.get(x_api_key)
        if scope is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API Key 缺失或不匹配",
            )
        if not scope.allows_env(env):
            allowed_envs = ", ".join(scope.environments)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"API Key 不适用于环境 {env}（允许：{allowed_envs}）",
            )
        if x_tenant and x_tenant.strip() and header_tenant != scope.tenant:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"API Key 属于租户 {scope.tenant}，不能访问租户 {header_tenant}",
            )
        tenant = scope.tenant
    else:
        tenant = header_tenant

    # 租户写进请求上下文：存储层据此选择数据分区。同步路由在线程池里执行时
    # 会继承这份上下文，因此路由内部不必显式传租户。
    tenancy.use_tenant(tenant)
    tenancy.use_environment(env)
    return EnvContext(environment=env, tenant=tenant)


__all__ = [
    "API_KEY_HEADER",
    "ENV_HEADER",
    "TENANT_HEADER",
    "EnvContext",
    "require_env",
]
