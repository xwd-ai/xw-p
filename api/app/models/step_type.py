"""步骤类型（StepType）枚举（ROADMAP Phase 2）。

工作流引擎把每个步骤划分为「做什么」的类型，StepType 即这一层级的枚举：

- 投递步骤（delivery）：``in_app`` / ``email`` / ``sms`` / ``push`` / ``chat``，
  每种类型绑定到一个确定的投递渠道（:class:`~.channel.Channel`），由 pipeline 的
  ``dispatch`` 阶段真正把消息发出去。
- 编排步骤（orchestration）：``delay``（定时延迟）与 ``digest``（摘要聚合），
  不直接投递，而是影响消息的时序与聚合方式。

与 :class:`~.channel.Channel` 的关系：StepType 是「步骤语义」，Channel 是「投递媒介」。
二者一一对应（投递步骤类型 ↔ 投递渠道），本模块通过 ``channel()`` / ``from_channel()``
提供双向映射，供步骤管线在 filter → transform → render → dispatch 之间换算。
"""
from __future__ import annotations

from enum import Enum

from .channel import Channel


class StepType(str, Enum):
    """工作流步骤类型。

    值即存储/传输标识，与 Channel 枚举的取值保持同名同义，
    以便旧数据（``Step.type`` 直接存渠道名）平滑兼容。
    """

    IN_APP = "in_app"  # 应用内收件箱
    EMAIL = "email"  # 邮件
    SMS = "sms"  # 短信
    PUSH = "push"  # 移动推送
    CHAT = "chat"  # 聊天工具（webhook）
    DELAY = "delay"  # 定时延迟（编排）
    DIGEST = "digest"  # 摘要聚合（编排）

    @classmethod
    def delivery_types(cls) -> list["StepType"]:
        """返回真正执行投递的步骤类型（排除 delay / digest 编排步骤）。"""
        return [t for t in cls if t not in (cls.DELAY, cls.DIGEST)]

    @classmethod
    def orchestration_types(cls) -> list["StepType"]:
        """返回编排步骤类型（delay / digest）。"""
        return [cls.DELAY, cls.DIGEST]

    def is_delivery(self) -> bool:
        """是否为投递步骤类型。"""
        return self not in (self.DELAY, self.DIGEST)

    def channel(self) -> Channel:
        """返回该步骤类型绑定的投递渠道。

        投递类型 → 同名渠道；``delay`` / ``digest`` 无投递渠道，抛出 ``ValueError``。
        """
        if not self.is_delivery():
            raise ValueError(f"编排步骤 {self.value!r} 无对应投递渠道")
        return Channel(self.value)

    @classmethod
    def from_channel(cls, channel: Channel) -> "StepType":
        """由投递渠道反查对应的步骤类型。"""
        return cls(channel.value)

    @classmethod
    def parse(cls, value: str) -> "StepType":
        """按存储标识解析步骤类型；未知值抛出 ``ValueError``。"""
        try:
            return cls(value)
        except ValueError as exc:
            raise ValueError(f"非法步骤类型：{value!r}") from exc


__all__ = ["StepType"]
