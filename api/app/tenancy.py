"""多租户上下文：租户解析、API Key 作用域与数据路径。

租户是比环境更高一层的数据边界：环境区分同一套业务数据的 dev / prod 副本，
租户区分互不可见的客户。两者正交——一个租户有自己的 dev 与 prod。

本模块只负责三件事：

- 把请求里的 ``X-Tenant-Id`` 与 ``X-Api-Key`` 解析成一个租户；
- 校验 Key 的作用域（绑定的租户、允许的环境），越界直接拒绝；
- 给出该租户的数据路径：非默认租户的数据落在 ``data/tenants/<租户>/`` 下，
  默认租户沿用原有路径，已有部署升级后不必搬迁数据。

Key 作用域由 ``XW_API_KEYS``（JSON 数组）声明：

    [{"key": "sk_live_xxx", "tenant": "acme", "environments": ["production"],
      "label": "社区主站"}]

未配置任何 Key 时保持 demo 语义：请求头决定租户，缺省 ``default``。
一旦配置了 Key，请求必须携带匹配的 Key，租户由 Key 决定；此时再带一个不同
的 ``X-Tenant-Id`` 会被拒绝——「用 A 的 Key 读 B 的数据」正是租户隔离要挡住
的事。
"""
from __future__ import annotations

import contextvars
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import config
from .models.entities import Environment

#: 请求头名称
TENANT_HEADER = "X-Tenant-Id"

#: 未指定租户时的缺省值
DEFAULT_TENANT = "default"

#: 已知角色。只开放 ``admin`` 一种：权限模型越少，配错的空间越小；
#: 需要更细的读写分离时再扩，而不是先留一个没人用的 roles 字段。
ROLE_ADMIN = "admin"
KNOWN_ROLES = (ROLE_ADMIN,)

#: 租户标识的字符集：小写字母数字开头，允许连字符与下划线。
#:
#: 收紧到这套字符集是因为租户标识会拼进文件路径：放开 ``/`` 与 ``.``
#: 就能用 ``../`` 跳出数据目录。
_TENANT_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]{0,30}[a-z0-9])?$")

#: 当前请求的租户。由鉴权依赖写入，存储层读取。
_current: contextvars.ContextVar[str] = contextvars.ContextVar(
    "xw_tenant", default=DEFAULT_TENANT
)

#: 当前请求的环境。与租户一起由鉴权依赖写入；收件箱这类要按环境过滤
#: 读取结果的模块不必再从请求对象里取一遍。
_environment: contextvars.ContextVar[str] = contextvars.ContextVar(
    "xw_environment", default="development"
)


class TenantError(ValueError):
    """租户配置或请求头不合法。"""


@dataclass(frozen=True)
class ApiKeyScope:
    """一个 API Key 的授权范围。"""

    key: str
    tenant: str
    #: 允许使用的环境；空元组表示不限制（仅在未声明 environments 时出现）。
    environments: tuple[str, ...] = ()
    label: str = ""
    #: 角色。目前只有 ``admin``：运营后台的接口只接受带该角色的 Key 或后台令牌。
    #: 未声明角色的 Key 只能调用业务接口——服务端的长期凭据不该顺带拿到
    #: 「停用模板、看全量统计」这类运维权限。
    roles: tuple[str, ...] = ()

    def allows_env(self, environment: str) -> bool:
        return not self.environments or environment in self.environments

    def has_role(self, role: str) -> bool:
        return role in self.roles


def is_valid_tenant(value: str) -> bool:
    return bool(_TENANT_RE.match(value or ""))


def normalize_tenant(raw: Optional[str]) -> str:
    """校验并归一化租户标识；空值回退默认租户。"""
    if raw is None or raw.strip() == "":
        return DEFAULT_TENANT
    tenant = raw.strip().lower()
    if not is_valid_tenant(tenant):
        raise TenantError(
            "租户标识只能包含小写字母、数字、连字符与下划线，长度不超过 32"
        )
    return tenant


def parse_scopes(raw: str) -> dict[str, ApiKeyScope]:
    """解析 ``XW_API_KEYS`` 的 JSON 数组；写错时抛错而不是悄悄忽略。

    悄悄跳过一条写错的配置，表现是「配了 Key 却一直 401」，比起动失败难查。
    """
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TenantError(f"XW_API_KEYS 不是合法 JSON：{exc}") from exc
    if not isinstance(items, list):
        raise TenantError("XW_API_KEYS 需为 JSON 数组")

    scopes: dict[str, ApiKeyScope] = {}
    valid_envs = {e.value for e in Environment}
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise TenantError(f"XW_API_KEYS 第 {index + 1} 项需为对象")
        key = str(item.get("key") or "").strip()
        if not key:
            raise TenantError(f"XW_API_KEYS 第 {index + 1} 项缺少 key")
        tenant = normalize_tenant(item.get("tenant"))
        raw_envs = item.get("environments")
        if raw_envs is None:
            envs: tuple[str, ...] = ()
        elif isinstance(raw_envs, str):
            envs = tuple(part.strip().lower() for part in raw_envs.split(",") if part.strip())
        elif isinstance(raw_envs, list):
            envs = tuple(str(part).strip().lower() for part in raw_envs if str(part).strip())
        else:
            raise TenantError(f"XW_API_KEYS 第 {index + 1} 项的 environments 需为数组")
        unknown = [env for env in envs if env not in valid_envs]
        if unknown:
            raise TenantError(
                f"XW_API_KEYS 第 {index + 1} 项包含未知环境：{', '.join(unknown)}"
            )
        roles = _parse_roles(item.get("roles"), index)
        scopes[key] = ApiKeyScope(
            key=key,
            tenant=tenant,
            environments=envs,
            label=str(item.get("label") or "").strip(),
            roles=roles,
        )
    return scopes


def _parse_roles(raw: object, index: int) -> tuple[str, ...]:
    """解析 Key 的角色声明：字符串或数组，只接受已知角色。"""
    if raw is None:
        return ()
    if isinstance(raw, str):
        parts = [part.strip().lower() for part in raw.split(",") if part.strip()]
    elif isinstance(raw, list):
        parts = [str(part).strip().lower() for part in raw if str(part).strip()]
    else:
        raise TenantError(f"XW_API_KEYS 第 {index + 1} 项的 roles 需为数组")
    unknown = [role for role in parts if role not in KNOWN_ROLES]
    if unknown:
        raise TenantError(
            f"XW_API_KEYS 第 {index + 1} 项包含未知角色：{', '.join(unknown)}"
            f"（支持：{', '.join(KNOWN_ROLES)}）"
        )
    # 去重保序：同一个角色写两遍不该产生两条
    seen: list[str] = []
    for role in parts:
        if role not in seen:
            seen.append(role)
    return tuple(seen)


def load_scopes() -> dict[str, ApiKeyScope]:
    """返回当前生效的 Key 作用域表。

    合并两个来源：``XW_API_KEYS`` 的显式声明，以及按环境配置的旧变量
    ``XW_API_KEY_DEVELOPMENT`` / ``XW_API_KEY_PRODUCTION``。旧变量绑定默认
    租户与自身环境，升级前后行为一致。
    """
    scopes = parse_scopes(config.API_KEY_SCOPES)
    for environment, key in (config.API_KEYS or {}).items():
        key = (key or "").strip()
        if not key:
            continue
        existing = scopes.get(key)
        if existing is not None:
            merged = tuple(sorted(set(existing.environments) | {environment}))
            scopes[key] = ApiKeyScope(
                key=key,
                tenant=existing.tenant,
                environments=merged,
                label=existing.label,
            )
            continue
        scopes[key] = ApiKeyScope(
            key=key,
            tenant=DEFAULT_TENANT,
            environments=(environment,),
            label="环境 Key",
        )
    return scopes


def allowed_tenants() -> set[str]:
    """返回显式声明的租户集合；空集表示未限制。"""
    raw = config.TENANTS or ""
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def current_tenant() -> str:
    """返回当前请求的租户（无请求上下文时为默认租户）。"""
    return _current.get()


def use_tenant(tenant: str):
    """把租户写入当前上下文，返回可用于恢复的 token。"""
    return _current.set(normalize_tenant(tenant))


def reset_tenant(token) -> None:
    _current.reset(token)


def current_environment() -> str:
    """返回当前请求的环境（无请求上下文时为开发环境）。"""
    return _environment.get()


def use_environment(environment: str):
    """把环境写入当前上下文，返回可用于恢复的 token。"""
    return _environment.set((environment or "development").strip().lower())


def tenant_of(record: dict) -> str:
    """取一条历史记录的租户；老数据没有该字段，按默认租户处理。"""
    return str(record.get("tenant") or DEFAULT_TENANT)


def scoped_path(base: Path | str, tenant: Optional[str] = None) -> Path:
    """返回某租户的数据文件路径。

    默认租户沿用原路径（升级不搬数据）；其余租户放到同级的
    ``tenants/<租户>/`` 目录下，文件名不变。
    """
    path = Path(base)
    tenant = normalize_tenant(tenant) if tenant is not None else current_tenant()
    if tenant == DEFAULT_TENANT:
        return path
    return path.parent / "tenants" / tenant / path.name


__all__ = [
    "DEFAULT_TENANT",
    "KNOWN_ROLES",
    "ROLE_ADMIN",
    "TENANT_HEADER",
    "ApiKeyScope",
    "TenantError",
    "allowed_tenants",
    "current_environment",
    "current_tenant",
    "is_valid_tenant",
    "load_scopes",
    "normalize_tenant",
    "parse_scopes",
    "reset_tenant",
    "scoped_path",
    "tenant_of",
    "use_tenant",
]
