"""触发事件与订阅者的数据模型（Pydantic v2）。"""
from __future__ import annotations

import uuid
from typing import Any, Optional

from pydantic import BaseModel, Field


class SubscriberTarget(BaseModel):
    """触发目标订阅者。"""
    subscriber_id: str = Field(alias="subscriberId", min_length=1, description="订阅者唯一标识")
    email: Optional[str] = None
    phone: Optional[str] = None
    channels: Optional[list[str]] = None


class TriggerEvent(BaseModel):
    """触发请求体。"""
    name: str = Field(min_length=1, max_length=255, description="工作流/模板标识，例如 welcome")
    to: SubscriberTarget
    payload: dict[str, Any] = Field(default_factory=dict, description="渲染变量（键值对）")
    overrides: dict[str, Any] = Field(default_factory=dict, description="覆盖模板默认配置")


class TriggerResponse(BaseModel):
    """触发成功后的回执。"""
    acknowledged: bool = True
    trigger_id: str = Field(alias="triggerId")
    transaction_id: str = Field(alias="transactionId")
    acknowledgment_id: str = Field(alias="acknowledgmentId")

    model_config = {"populate_by_name": True}


class QueuedMessage(BaseModel):
    """入队后的内部消息。"""
    trigger_id: str
    transaction_id: str
    name: str
    subscriber_id: str
    email: Optional[str] = None
    phone: Optional[str] = None
    channels: Optional[list[str]] = None
    payload: dict[str, Any] = Field(default_factory=dict)
    overrides: dict[str, Any] = Field(default_factory=dict)


def new_queued_message(event: TriggerEvent) -> QueuedMessage:
    return QueuedMessage(
        trigger_id=str(uuid.uuid4()),
        transaction_id=str(uuid.uuid4()),
        name=event.name,
        subscriber_id=event.to.subscriber_id,
        email=event.to.email,
        phone=event.to.phone,
        channels=event.to.channels,
        payload=event.payload,
        overrides=event.overrides,
    )
