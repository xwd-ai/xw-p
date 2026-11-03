"""订阅者接口的请求与响应模型。

请求模型只做「能在这里判定」的事：裁剪空白、校验渠道取值、拒绝编排步骤；
地址格式与唯一性属于业务规则，放在接口层判定，错误信息才能指出是哪一项
冲突，而不是笼统的 422。
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator

from .models.channel import Channel
from .models.entities import Subscriber
from .subscriber_store import normalize_channels

#: 标识长度上限：订阅者标识会出现在队列消息、收件箱与执行明细里，
#: 过长只会让这些地方的可读性变差，这里统一挡在入口。
MAX_ID_LENGTH = 128


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class SubscriberCreate(BaseModel):
    """创建订阅者。标识可留空，由服务端生成。"""

    id: Optional[str] = Field(default=None, description="订阅者标识；留空自动生成")
    email: Optional[str] = Field(default=None, description="邮箱地址")
    phone: Optional[str] = Field(default=None, description="手机号（E.164 或本地格式）")
    device_token: Optional[str] = Field(default=None, description="推送设备 token")
    channels: list[Channel] = Field(
        default_factory=list, description="启用的渠道：in_app / email / sms / push / chat"
    )

    @field_validator("id", "email", "phone", "device_token")
    @classmethod
    def _trim(cls, value: Optional[str]) -> Optional[str]:
        return _clean(value)

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and len(value) > MAX_ID_LENGTH:
            raise ValueError(f"订阅者标识不能超过 {MAX_ID_LENGTH} 个字符")
        return value

    @field_validator("channels")
    @classmethod
    def _check_channels(cls, value: list[Channel]) -> list[Channel]:
        return normalize_channels(value)


class SubscriberUpdate(BaseModel):
    """更新订阅者：未出现的字段保持不变，显式给空串表示解除绑定。"""

    email: Optional[str] = Field(default=None, description="邮箱地址；空串表示解绑")
    phone: Optional[str] = Field(default=None, description="手机号；空串表示解绑")
    device_token: Optional[str] = Field(default=None, description="设备 token；空串表示解绑")
    channels: Optional[list[Channel]] = Field(
        default=None, description="启用的渠道；给出即整体替换，空列表表示全部关闭"
    )

    @field_validator("email", "phone", "device_token")
    @classmethod
    def _trim_or_clear(cls, value: Optional[str]) -> Optional[str]:
        return _clean(value)

    @field_validator("channels")
    @classmethod
    def _check_channels(cls, value: Optional[list[Channel]]) -> Optional[list[Channel]]:
        if value is None:
            return None
        return normalize_channels(value)


class SubscriberOut(BaseModel):
    """订阅者的对外表示。"""

    id: str
    email: Optional[str] = None
    phone: Optional[str] = None
    device_token: Optional[str] = None
    channels: list[str] = Field(default_factory=list, description="启用的渠道")
    bindings: dict[str, str] = Field(
        default_factory=dict, description="渠道 → 收件地址；只列出实际可投递的渠道"
    )
    environment: str
    created_at: str
    updated_at: str


class SubscriberListOut(BaseModel):
    """订阅者列表：``nextCursor`` 为空表示没有下一页。"""

    items: list[SubscriberOut]
    nextCursor: Optional[str] = None
    hasMore: bool = False


def to_subscriber_out(subscriber: Subscriber) -> SubscriberOut:
    """把实体折算成响应模型，顺带给出「哪些渠道现在真的能投」。"""
    bindings: dict[str, str] = {}
    for channel in subscriber.channels:
        if not subscriber.binds(channel):
            continue
        address = subscriber.address_for(channel)
        if address:
            bindings[channel.value] = address
    return SubscriberOut(
        id=subscriber.id,
        email=subscriber.email,
        phone=subscriber.phone,
        device_token=subscriber.device_token,
        channels=[c.value for c in subscriber.channels],
        bindings=bindings,
        environment=subscriber.environment.value,
        created_at=subscriber.created_at.isoformat(),
        updated_at=subscriber.updated_at.isoformat(),
    )


__all__ = [
    "MAX_ID_LENGTH",
    "SubscriberCreate",
    "SubscriberListOut",
    "SubscriberOut",
    "SubscriberUpdate",
    "to_subscriber_out",
]
