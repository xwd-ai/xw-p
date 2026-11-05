"""后台登录接口的请求与响应模型。

回执用驼峰（``accessToken`` / ``expiresIn``），与触发回执、分页游标一致：
响应体是给前端与 SDK 读的，同一份接口里不该一半驼峰一半下划线。请求体只有
账号口令两项，不做脱敏以外的加工——口令不落日志、不进错误文案。
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TokenRequest(BaseModel):
    """用管理员账号口令换取后台令牌。"""

    username: str = Field(min_length=1, max_length=128, description="管理员账号")
    password: str = Field(min_length=1, max_length=256, description="管理员口令")
    tenant: Optional[str] = Field(
        default=None,
        description="目标租户；留空取 XW_ADMIN_TENANT。只影响令牌里的租户声明，不能越权到清单外的租户",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [{"username": "ops", "password": "change-me-in-production"}]
        }
    }


class TokenResponse(BaseModel):
    """登录成功后的令牌与声明。"""

    access_token: str = Field(alias="accessToken", description="JWT，放在 Authorization: Bearer 后")
    token_type: str = Field(default="Bearer", alias="tokenType", description="令牌类型，固定 Bearer")
    expires_in: int = Field(alias="expiresIn", description="有效期（秒）")
    expires_at: datetime = Field(alias="expiresAt", description="过期时刻（UTC）")
    subject: str = Field(description="令牌主体：管理员账号")
    tenant: str = Field(description="令牌绑定的租户")
    environments: list[str] = Field(
        default_factory=list, description="令牌可操作的环境；空数组表示不限"
    )
    role: str = Field(description="角色，当前固定 admin")

    model_config = {"populate_by_name": True}


class PrincipalResponse(BaseModel):
    """当前令牌的身份。"""

    subject: str = Field(description="令牌主体")
    tenant: str = Field(description="令牌绑定的租户")
    environments: list[str] = Field(default_factory=list, description="可操作的环境；空表示不限")
    role: str = Field(description="角色")
    issued_at: Optional[datetime] = Field(default=None, alias="issuedAt", description="签发时刻")
    expires_at: datetime = Field(alias="expiresAt", description="过期时刻")
    token_id: str = Field(default="", alias="tokenId", description="令牌标识，用于排查与吊销对账")

    model_config = {"populate_by_name": True}


__all__ = ["PrincipalResponse", "TokenRequest", "TokenResponse"]
