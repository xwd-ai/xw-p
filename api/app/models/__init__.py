"""xw-notify 领域模型层。

Phase 1 起引入核心实体，用于描述通知平台的领域对象。
当前模块划分：
- ``channel``   —— 通知渠道枚举与 Provider 抽象接口
- ``entities``  —— Workflow / Step / Subscriber / Message / Topic / Environment 等领域实体
- ``db``        —— SQLAlchemy ORM 模型（对应 Postgres 表结构）
"""
from .channel import Channel
from .entities import (
    Environment,
    Message,
    Step,
    Subscriber,
    Topic,
    Workflow,
)

__all__ = [
    "Channel",
    "Environment",
    "Message",
    "Step",
    "Subscriber",
    "Topic",
    "Workflow",
]
