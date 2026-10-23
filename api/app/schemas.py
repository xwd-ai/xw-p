"""触发事件与订阅者的数据模型（Pydantic v2）。"""
from __future__ import annotations

import uuid
from typing import Any, Optional

from pydantic import BaseModel, Field


class SubscriberTarget(BaseModel):
    """触发目标订阅者。"""

    subscriber_id: str = Field(alias="subscriberId", min_length=1, description="订阅者唯一标识")
    email: Optional[str] = Field(default=None, description="接收邮件的地址（渠道含 email 时用）")
    phone: Optional[str] = Field(default=None, description="接收短信的手机号（渠道含 sms 时用）")
    channels: Optional[list[str]] = Field(
        default=None, description="分发渠道列表，如 in_app / email / sms / push"
    )

    model_config = {"populate_by_name": True}


class TriggerEvent(BaseModel):
    """触发请求体。

    一条交易事件触发后，按 ``name`` 匹配工作流模板，渲染 ``payload`` 中的变量，
    并将通知分发到 ``to`` 指定的订阅者所在的多个渠道。

    示例（订单支付成功，站内信 + 邮件 + 短信多渠道触达）：::

        {
            "name": "order_confirmed",
            "to": {
                "subscriberId": "buyer-1001",
                "email": "buyer@example.com",
                "phone": "+8613800000000",
                "channels": ["in_app", "email", "sms"]
            },
            "payload": {
                "order_id": "ORDER-2026-1001",
                "customer": "张三",
                "amount": "199.00"
            }
        }
    """

    name: str = Field(
        min_length=1,
        max_length=255,
        description="工作流/模板标识，例如 order_confirmed / order_shipped / order_delivered",
        examples=["order_confirmed"],
    )
    to: SubscriberTarget = Field(description="触发目标订阅者及其渠道")
    payload: dict[str, Any] = Field(default_factory=dict, description="渲染变量（键值对）")
    overrides: dict[str, Any] = Field(default_factory=dict, description="覆盖模板默认配置")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "order_confirmed",
                    "to": {
                        "subscriberId": "buyer-1001",
                        "email": "buyer@example.com",
                        "phone": "+8613800000000",
                        "channels": ["in_app", "email", "sms"],
                    },
                    "payload": {
                        "order_id": "ORDER-2026-1001",
                        "customer": "张三",
                        "amount": "199.00",
                    },
                    "overrides": {},
                }
            ]
        }
    }


class TriggerResponse(BaseModel):
    """触发成功后的回执。"""

    acknowledged: bool = Field(default=True, description="是否已受理")
    trigger_id: str = Field(alias="triggerId", description="触发事件唯一标识")
    transaction_id: str = Field(alias="transactionId", description="事务标识（幂等键未命中时生成）")
    acknowledgment_id: str = Field(alias="acknowledgmentId", description="回执标识，与 trigger_id 一致")

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
