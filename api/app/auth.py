"""环境隔离与 API Key 鉴权（ROADMAP Phase 1）。

提供统一的 FastAPI 依赖 ``require_env``，解析请求的 ``X-Environment`` 头
（默认 ``development``），并按该环境校验 ``X-Api-Key``：

- 未配置该环境 Key 时，鉴权放行（demo / 测试开箱即用）。
- 已配置时，请求必须携带匹配的 ``X-Api-Key``，否则返回 401。

依赖返回值 ``EnvContext`` 携带解析出的环境，供各端点做数据隔离与落库打标。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException, status

from . import config
from .models.entities import Environment

# 请求头名称：环境标识与 API Key
ENV_HEADER = "X-Environment"
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


@dataclass(frozen=True)
class EnvContext:
    """鉴权依赖的返回上下文：解析出的隔离环境。"""

    environment: str


def require_env(
    x_environment: Optional[str] = Header(
        default=None, alias=ENV_HEADER, description="隔离环境：development（默认）| production"
    ),
    x_api_key: Optional[str] = Header(
        default=None, alias=API_KEY_HEADER, description="该环境的 API Key（配置了 Key 时必填）"
    ),
) -> EnvContext:
    """解析环境并校验 API Key。

    语义：
    - 环境头为空时使用服务默认环境（``XW_ENV``）。
    - 目标环境未配置 Key：放行（保持 demo/测试友好）。
    - 目标环境已配置 Key：``X-Api-Key`` 必须精确匹配，否则 401。
    """
    env = _resolve_env(x_environment)
    expected = config.api_key_for(env)
    if expected:
        if x_api_key is None or x_api_key != expected:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API Key 缺失或不匹配",
            )
    return EnvContext(environment=env)


__all__ = [
    "ENV_HEADER",
    "API_KEY_HEADER",
    "EnvContext",
    "require_env",
]
