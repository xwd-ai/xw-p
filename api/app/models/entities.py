"""核心领域实体定义。

对应 ROADMAP Phase 1：Workflow / Step / Subscriber / Message / Environment / Channel。
使用 Pydantic v2 描述实体结构与校验规则；Channel 枚举见 ``channel.py``。
这些实体是纯领域模型（无持久化逻辑），持久化由 ``db.py`` 的 ORM 模型承担。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from .channel import Channel


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Environment(str, Enum):
    """隔离环境：development / production。

    同一套实体在不同环境间数据隔离，API Key 按环境区分（后续 Phase 落地）。
    """

    DEVELOPMENT = "development"
    PRODUCTION = "production"


class Step(BaseModel):
    """工作流中的一个步骤。

    顺序由父 Workflow 的 ``steps`` 列表天然决定；``type`` 引用 Channel 枚举，
    ``config`` 承载该步骤的私有参数（如模板、过滤条件、延迟时长）。
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="步骤唯一标识")
    type: Channel = Field(description="步骤类型，对应渠道或编排步骤")
    name: Optional[str] = Field(default=None, description="可读名称")
    config: dict[str, Any] = Field(default_factory=dict, description="步骤配置")

    def is_delivery(self) -> bool:
        return self.type in Channel.delivery_channels()


class Workflow(BaseModel):
    """工作流/通知模板：定义从触发到投递的步骤序列。"""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="工作流唯一标识")
    name: str = Field(description="触发标识（业务侧使用），例如 post_liked")
    description: Optional[str] = None
    steps: list[Step] = Field(default_factory=list, description="步骤序列（有序）")
    version: int = Field(default=1, ge=1, description="模板版本号")
    environment: Environment = Field(default=Environment.DEVELOPMENT)
    active: bool = Field(default=True, description="是否启用")
    created_at: datetime = Field(default_factory=_now_utc)

    def delivery_steps(self) -> list[Step]:
        """返回真正执行投递的步骤（跳过 delay/digest 编排步骤）。"""
        return [s for s in self.steps if s.is_delivery()]


class Subscriber(BaseModel):
    """订阅者：通知的接收方，可绑定多种渠道的联系方式。

    渠道绑定用「地址字段 + 启用的渠道列表」两个部分描述：地址是数据，
    ``channels`` 是意愿。同一个订阅者可以只绑邮箱、只绑设备 token，也可以
    同时绑多种；某个渠道是否真的能投，由地址是否存在与偏好判定共同决定。
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="订阅者唯一标识")
    email: Optional[str] = Field(default=None, description="邮箱地址")
    phone: Optional[str] = Field(default=None, description="手机号")
    device_token: Optional[str] = Field(default=None, description="移动设备 token")
    channels: list[Channel] = Field(default_factory=list, description="启用的渠道")
    environment: Environment = Field(default=Environment.DEVELOPMENT)
    created_at: datetime = Field(default_factory=_now_utc)
    updated_at: datetime = Field(default_factory=_now_utc)

    def address_for(self, channel: Channel) -> Optional[str]:
        """返回某渠道对应的收件地址；未绑定或渠道无需地址时返回 ``None``。"""
        mapping = {
            Channel.EMAIL: self.email,
            Channel.SMS: self.phone,
            Channel.PUSH: self.device_token,
            Channel.IN_APP: self.id,
        }
        return mapping.get(channel)

    def binds(self, channel: Channel) -> bool:
        """该渠道是否既在启用列表里、又有可用地址。"""
        if channel not in self.channels:
            return False
        if channel is Channel.IN_APP:
            return True
        return bool(self.address_for(channel))


class Topic(BaseModel):
    """分组：按主题归在一起的订阅者集合。

    分组只描述「谁属于哪一组」，成员各自的渠道绑定与偏好照旧在投递阶段
    生效；触发时按分组广播，业务侧不必把一长串订阅者标识塞进请求体。
    标识是寻址键（触发与成员接口都按它定位），``name`` 只用于展示。
    """

    id: str = Field(description="分组唯一标识")
    name: str = Field(description="分组名称（展示用）")
    description: Optional[str] = Field(default=None, description="分组说明")
    environment: Environment = Field(default=Environment.DEVELOPMENT)
    created_at: datetime = Field(default_factory=_now_utc)
    updated_at: datetime = Field(default_factory=_now_utc)


class Message(BaseModel):
    """消息：一次触发投递产生的执行实例。

    关联到工作流与订阅者；``status`` 走消息状态机
    （created → queued → processing → sent / failed）。
    """

    class Status(str, Enum):
        CREATED = "created"
        QUEUED = "queued"
        PROCESSING = "processing"
        SENT = "sent"
        FAILED = "failed"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="消息唯一标识")
    workflow_id: str = Field(description="所属工作流")
    subscriber_id: str = Field(description="目标订阅者")
    channel: Channel = Field(description="投递渠道")
    payload: dict[str, Any] = Field(default_factory=dict, description="渲染后的消息内容")
    status: Status = Field(default=Status.CREATED)
    error: Optional[str] = Field(default=None, description="失败原因")
    created_at: datetime = Field(default_factory=_now_utc)
    updated_at: datetime = Field(default_factory=_now_utc)


__all__ = [
    "Environment",
    "Step",
    "Workflow",
    "Subscriber",
    "Message",
]
