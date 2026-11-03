"""触发事件与订阅者的数据模型（Pydantic v2）。"""
from __future__ import annotations

import uuid
from typing import Any, Optional

from pydantic import BaseModel, Field

from .models.entities import Workflow


class SubscriberTarget(BaseModel):
    """触发目标订阅者。"""

    subscriber_id: str = Field(alias="subscriberId", min_length=1, description="订阅者唯一标识")
    email: Optional[str] = Field(default=None, description="接收邮件的地址（渠道含 email 时用）")
    phone: Optional[str] = Field(default=None, description="接收短信的手机号（渠道含 sms 时用）")
    device_token: Optional[str] = Field(
        default=None, alias="deviceToken", description="移动推送的 device token（渠道含 push 时用）"
    )
    channels: Optional[list[str]] = Field(
        default=None, description="分发渠道列表，如 in_app / email / sms / push"
    )

    model_config = {"populate_by_name": True}


class TriggerEvent(BaseModel):
    """触发请求体。

    一条互动事件触发后，按 ``name`` 匹配工作流模板，渲染 ``payload`` 中的变量，
    并将通知分发到 ``to`` 指定的订阅者所在的多个渠道。

    示例（帖子收到评论，站内信 + 推送 + 邮件多渠道触达）：::

        {
            "name": "post_commented",
            "to": {
                "subscriberId": "creator-1001",
                "email": "creator@example.com",
                "phone": "+8613800000000",
                "deviceToken": "fcm-token-9f2c1d",
                "channels": ["in_app", "push", "email"]
            },
            "payload": {
                "post_id": "P-2026-1001",
                "post_title": "我的第一支 Vlog",
                "actor": "小林",
                "comment": "拍得真好，求教程！"
            }
        }
    """

    name: str = Field(
        min_length=1,
        max_length=255,
        description="工作流/模板标识，例如 post_liked / post_commented / post_mentioned",
        examples=["post_commented"],
    )
    to: SubscriberTarget = Field(description="触发目标订阅者及其渠道")
    payload: dict[str, Any] = Field(default_factory=dict, description="渲染变量（键值对）")
    overrides: dict[str, Any] = Field(default_factory=dict, description="覆盖模板默认配置")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "post_commented",
                    "to": {
                        "subscriberId": "creator-1001",
                        "email": "creator@example.com",
                        "phone": "+8613800000000",
                        "deviceToken": "fcm-token-9f2c1d",
                        "channels": ["in_app", "push", "email"],
                    },
                    "payload": {
                        "post_id": "P-2026-1001",
                        "post_title": "我的第一支 Vlog",
                        "actor": "小林",
                        "comment": "拍得真好，求教程！",
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


# MAX_BATCH_EVENTS 是单次批量触发的上限。
#
# 批量接口的目的是把「N 条消息 N 次往返」压成一次，因此上限要够大才有意义；
# 但一次请求里的全部事件会在同一个事务边界内校验、同一次写队列，过大就会
# 把单请求的失败面放大到难以重试的程度（一条坏事件要重发整批）。100 条是
# 一个既能明显摊薄往返、又便于整批重试的折中。
MAX_BATCH_EVENTS = 100


class BatchTriggerRequest(BaseModel):
    """批量触发请求体。

    ``events`` 逐条按单条触发的语义处理：工作流不存在、未启用或环境不匹配
    的条目单独判失败，不影响同批其它事件入队。

    示例（一次提交同一篇帖子的多条互动）：::

        {
            "events": [
                {"name": "post_liked", "to": {"subscriberId": "creator-1001"},
                 "payload": {"post_id": "P-2026-1001", "actor": "小林"}},
                {"name": "post_commented", "to": {"subscriberId": "creator-1001",
                 "email": "creator@example.com"},
                 "payload": {"post_id": "P-2026-1001", "actor": "阿澈", "comment": "求教程"}}
            ]
        }
    """

    events: list[TriggerEvent] = Field(
        min_length=1,
        max_length=MAX_BATCH_EVENTS,
        description=f"待触发的事件列表，1–{MAX_BATCH_EVENTS} 条",
    )


class BatchTriggerItem(BaseModel):
    """批量触发中单条事件的处理结果。"""

    index: int = Field(description="该事件在请求列表中的下标，从 0 开始")
    name: str = Field(description="事件对应的工作流标识")
    subscriber_id: str = Field(alias="subscriberId", description="订阅者标识")
    status: str = Field(description="处理结果：accepted 已入队 / rejected 被拒")
    trigger_id: Optional[str] = Field(default=None, alias="triggerId", description="触发标识（被拒时为空）")
    transaction_id: Optional[str] = Field(default=None, alias="transactionId")
    acknowledgment_id: Optional[str] = Field(default=None, alias="acknowledgmentId")
    error: Optional[str] = Field(default=None, description="被拒原因")

    model_config = {"populate_by_name": True}


class BatchTriggerResponse(BaseModel):
    """批量触发回执：整批计数 + 逐条结果。

    结果按请求顺序给出且与请求等长，调用方按下标对齐即可，不必依赖
    「成功的那几条按顺序出现」这种隐含约定。
    """

    acknowledged: bool = Field(default=True, description="整批是否已受理（部分被拒仍为 true）")
    accepted: int = Field(description="已入队的条数")
    rejected: int = Field(description="被拒的条数")
    results: list[BatchTriggerItem] = Field(description="逐条结果，与请求顺序一致")


class WorkflowStepSnapshot(BaseModel):
    """步骤定义的传输形态，与 Go 侧 ``queue.StepSpec`` 字段一一对应。"""

    id: str
    type: str = Field(description="步骤类型：in_app / email / sms / push / chat / delay / digest")
    name: Optional[str] = None
    config: dict[str, Any] = Field(default_factory=dict)


class WorkflowSnapshot(BaseModel):
    """随消息下发的步骤定义快照。

    触发时把工作流的步骤序列固化进消息，引擎消费时直接按快照执行，
    不必回查工作流存储。这样已经入队的消息不会因为模板被后续编辑而
    半路换了步骤，也不必为「取步骤」多一次跨进程查询。
    """

    name: str
    version: int = 1
    steps: list[WorkflowStepSnapshot] = Field(default_factory=list)

    @classmethod
    def from_workflow(cls, workflow: Workflow) -> "WorkflowSnapshot":
        """按工作流实体生成快照（步骤配置原样携带，不做求值）。"""
        return cls(
            name=workflow.name,
            version=workflow.version,
            steps=[
                WorkflowStepSnapshot(
                    id=step.id,
                    type=getattr(step.type, "value", str(step.type)),
                    name=step.name,
                    config=dict(step.config or {}),
                )
                for step in workflow.steps
            ],
        )


class QueuedMessage(BaseModel):
    """入队后的内部消息。"""
    trigger_id: str
    transaction_id: str
    name: str
    subscriber_id: str
    email: Optional[str] = None
    phone: Optional[str] = None
    device_token: Optional[str] = None
    channels: Optional[list[str]] = None
    payload: dict[str, Any] = Field(default_factory=dict)
    overrides: dict[str, Any] = Field(default_factory=dict)
    workflow: Optional[WorkflowSnapshot] = Field(
        default=None, description="触发时刻的步骤定义快照，引擎按它执行工作流"
    )


def new_queued_message(event: TriggerEvent, workflow: Optional[Workflow] = None) -> QueuedMessage:
    """由触发请求构造入队消息；传入工作流时附带其步骤快照。"""
    return QueuedMessage(
        trigger_id=str(uuid.uuid4()),
        transaction_id=str(uuid.uuid4()),
        name=event.name,
        subscriber_id=event.to.subscriber_id,
        email=event.to.email,
        phone=event.to.phone,
        device_token=event.to.device_token,
        channels=event.to.channels,
        payload=event.payload,
        overrides=event.overrides,
        workflow=WorkflowSnapshot.from_workflow(workflow) if workflow is not None else None,
    )
