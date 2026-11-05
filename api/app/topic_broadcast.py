"""按分组广播：把一个互动事件投递给分组里的全部成员。

广播与逐条触发的差别只在**寻址方式**：事件本身（工作流、载荷）只有一份，
收件人却是分组里的每个人。因此这里的职责是「把一份事件展开成 N 条消息」，
其余语义与单条触发完全一致——每条消息有自己的触发标识、各自的订阅者地址、
以及同一份步骤快照。

两条判定值得写清楚：

- **成员档案不存在就跳过**。成员关系是一行独立的记录，订阅者档案被删掉之后
  它仍会留在分组里。给这样一条成员关系入队，只会得到一条没有收件地址的消息，
  问题要等到投递失败才暴露；直接跳过并给出原因，调用方能看见「名单里有个人
  已经不在了」。
- **没有可投递渠道就跳过**。订阅者把渠道全关了、或者只留了一个没有地址的渠道，
  这个人此刻不该被通知。跳过与投递失败是两件事：前者是「按设置不发」，
  后者是「该发的没发出去」，混在一起会让广播结果的成败无从判断。

写入与单条、批量路径一致：活动日志整批写一次，消息整批入队一次。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol

from .activity_log import build_activity
from .models.entities import Environment, Subscriber, Topic, Workflow
from .schemas import QueuedMessage
from .topic_store import TopicStore
from .trigger_core import message_for, target_for_subscriber


class SubscriberReader(Protocol):
    """广播只需要「按标识读订阅者」这一项能力。

    读取时带上环境：同一个标识在两套环境各有一份档案，不带环境的调用会拿到
    开发环境优先的那一份，广播就会拿错人的地址。
    """

    def get(
        self, subscriber_id: str, *, environment: object = None
    ) -> Optional[Subscriber]:
        ...


#: 结果状态：受理 / 跳过。与批量触发的 ``accepted`` / ``rejected`` 同一个词表。
STATUS_ACCEPTED = "accepted"
STATUS_SKIPPED = "skipped"


@dataclass
class BroadcastItem:
    """单个成员的处理结果。"""

    index: int
    subscriber_id: str
    status: str
    trigger_id: Optional[str] = None
    transaction_id: Optional[str] = None
    error: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "subscriberId": self.subscriber_id,
            "status": self.status,
            "triggerId": self.trigger_id,
            "transactionId": self.transaction_id,
            "error": self.error,
        }


@dataclass
class BroadcastResult:
    """一次广播的完整结果：消息、活动日志草稿与逐人回执。"""

    topic_id: str
    member_count: int
    messages: list[QueuedMessage] = field(default_factory=list)
    activities: list[dict[str, Any]] = field(default_factory=list)
    items: list[BroadcastItem] = field(default_factory=list)

    @property
    def accepted(self) -> int:
        return len(self.messages)

    @property
    def skipped(self) -> int:
        return len(self.items) - len(self.messages)


def broadcast_to_topic(
    *,
    topic: Topic,
    workflow: Workflow,
    topic_store: TopicStore,
    subscriber_store: SubscriberReader,
    environment: Environment,
    payload: Optional[dict[str, Any]] = None,
    overrides: Optional[dict[str, Any]] = None,
) -> BroadcastResult:
    """把一份事件展开成「每人一条」的消息与活动日志。

    这里只做构造，不写队列也不落库：调用方拿到 :class:`BroadcastResult` 之后
    统一走批量通道一次性写入，两条入口共用同一次 I/O。
    """
    member_ids = topic_store.member_ids(topic.id)
    result = BroadcastResult(topic_id=topic.id, member_count=len(member_ids))

    for index, subscriber_id in enumerate(member_ids):
        subscriber = subscriber_store.get(subscriber_id, environment=environment.value)
        if subscriber is None:
            # 没有本环境的档案。若别的环境有同名标识，说明这份成员关系是手工
            # 改数据留下的不一致，原因写得更准一些。
            elsewhere = subscriber_store.get(subscriber_id)
            reason = (
                f"订阅者属于另一个环境：{subscriber_id}"
                if elsewhere is not None
                else f"订阅者档案不存在：{subscriber_id}"
            )
            result.items.append(
                BroadcastItem(
                    index=index,
                    subscriber_id=subscriber_id,
                    status=STATUS_SKIPPED,
                    error=reason,
                )
            )
            continue
        if subscriber.environment is not environment:
            # 分组与成员各自带环境，正常情况下加入时就校验过；这里兜住
            # 「档案被换到另一个环境」这种手工改数据留下的不一致。
            result.items.append(
                BroadcastItem(
                    index=index,
                    subscriber_id=subscriber_id,
                    status=STATUS_SKIPPED,
                    error=f"订阅者属于另一个环境：{subscriber_id}",
                )
            )
            continue

        target = target_for_subscriber(subscriber)
        if not target.channels:
            result.items.append(
                BroadcastItem(
                    index=index,
                    subscriber_id=subscriber_id,
                    status=STATUS_SKIPPED,
                    error="订阅者没有可投递的渠道",
                )
            )
            continue

        message = message_for(
            workflow.name,
            target,
            workflow,
            payload=payload,
            overrides=overrides,
        )
        result.messages.append(message)
        result.items.append(
            BroadcastItem(
                index=index,
                subscriber_id=subscriber_id,
                status=STATUS_ACCEPTED,
                trigger_id=message.trigger_id,
                transaction_id=message.transaction_id,
            )
        )
        result.activities.append(
            build_activity(
                trigger_id=message.trigger_id,
                workflow=workflow.name,
                subscriber_id=subscriber_id,
                channels=message.channels,
                status="triggered",
                payload=message.payload,
                environment=environment.value,
            )
        )

    return result


__all__ = [
    "STATUS_ACCEPTED",
    "STATUS_SKIPPED",
    "BroadcastItem",
    "BroadcastResult",
    "SubscriberReader",
    "broadcast_to_topic",
]
