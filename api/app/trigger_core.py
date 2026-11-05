"""触发链路的共用判定：工作流解析与「按订阅者构造消息」。

单条触发、批量触发、按分组广播三条入口最终都要落到同一件事上——按名称
找到一个当前环境里可触发的工作流，再为某个订阅者生成一条入队消息。这三处
若各写一遍，最容易走偏的是环境匹配与「未启用算不算可触发」这两条判定：
一处放宽、另一处收紧，同一个工作流会在不同入口上得到不同答案。

因此判定与构造收在这里，路由层只负责入参校验、幂等与写入。
"""
from __future__ import annotations

from typing import Any, Optional

from .models.entities import Subscriber, Workflow
from .schemas import QueuedMessage, SubscriberTarget, TriggerEvent, new_queued_message
from .workflow import get_registry
from .workflow_store import get_workflow_store


def env_mismatch(workflow_env: object, request_env: str) -> bool:
    """判断工作流环境与请求环境是否不匹配。

    环境一致（或工作流环境未知、回退默认）视为匹配，返回 ``False``。
    """
    wf_env = workflow_env.value if hasattr(workflow_env, "value") else str(workflow_env)
    if not wf_env:
        return False
    return wf_env != request_env


def resolve_workflow(name: str, environment: str) -> Optional[Workflow]:
    """按名称查找当前环境下可触发的工作流，找不到或未启用返回 ``None``。

    解析顺序是「先存储、后预置」：

    - 工作流存储是运营写入的那一份——启停、改名、改步骤都落在这里，因此存储
      里存在同名且同环境的记录时以它为准，**停用即返回 None，不再回落到预置**。
      反过来（预置优先）会让后台点了停用仍然能触发，启停等于没做。
    - 预置模板只兜住「存储里还没有这条记录」的情况，例如 Postgres 后端尚未
      导入预置模板、或测试替换掉了存储。

    两个来源都按环境查找：同名模板在开发与生产各有一份，按名称查而不带环境
    会拿到另一套环境的那份，「停用」或改名都会作用到错误的模板上。
    """
    stored = get_workflow_store().get_by_name(name, environment=environment)
    if stored is not None and not env_mismatch(stored.environment, environment):
        return stored if stored.active else None

    preset = get_registry().get(name)
    if preset is None or env_mismatch(preset.environment, environment):
        return None
    return preset


def missing_workflow_detail(name: str, environment: str) -> str:
    """统一「工作流不可触发」的文案，各入口的措辞保持一致。"""
    return f"工作流模板不存在或未启用：{name}（环境 {environment}）"


def target_for_subscriber(subscriber: Subscriber) -> SubscriberTarget:
    """把订阅者档案折算成触发目标。

    地址直接取档案：广播时业务侧不提供收件人信息，唯一可信的来源就是订阅者
    自己填过的那几个字段。渠道只留**实际可投递**的那些（``binds`` 同时看
    「是否启用」与「地址是否存在」）——把一个只有意愿没有地址的渠道带进消息，
    只会在投递阶段换回一条「缺少邮箱」的失败，而这条失败对排查没有任何信息量。
    """
    channels = [channel.value for channel in subscriber.channels if subscriber.binds(channel)]
    return SubscriberTarget(
        subscriberId=subscriber.id,
        email=subscriber.email,
        phone=subscriber.phone,
        deviceToken=subscriber.device_token,
        channels=channels,
    )


def message_for(
    name: str,
    target: SubscriberTarget,
    workflow: Workflow,
    *,
    payload: Optional[dict[str, Any]] = None,
    overrides: Optional[dict[str, Any]] = None,
) -> QueuedMessage:
    """为单个目标构造入队消息（附带触发时刻的步骤快照）。"""
    event = TriggerEvent(
        name=name,
        to=target,
        payload=dict(payload or {}),
        overrides=dict(overrides or {}),
    )
    return new_queued_message(event, workflow)


__all__ = [
    "env_mismatch",
    "message_for",
    "missing_workflow_detail",
    "resolve_workflow",
    "target_for_subscriber",
]
