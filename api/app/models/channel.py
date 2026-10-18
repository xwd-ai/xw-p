"""通知渠道（Channel）枚举与 Provider 抽象接口。

Channel 枚举描述一条通知可以经由的投递渠道；Provider 是「把消息真正发出去」
的适配器占位接口，具体实现（console mock / SMTP / webhook 等）将在后续 Phase
逐渠道落地。本模块只定义契约，不引入任何第三方品牌或专有 SDK。
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Protocol, runtime_checkable


class Channel(str, Enum):
    """通知投递渠道。

    使用 ``str`` 混入便于序列化与 JSON 往返，值即存储/传输标识。
    """

    IN_APP = "in_app"  # 应用内收件箱
    EMAIL = "email"  # 邮件
    SMS = "sms"  # 短信
    PUSH = "push"  # 移动推送
    CHAT = "chat"  # 聊天工具（webhook）
    DELAY = "delay"  # 定时延迟（非投递渠道，为步骤编排保留）
    DIGEST = "digest"  # 摘要聚合（非投递渠道，为步骤编排保留）

    @classmethod
    def delivery_channels(cls) -> list["Channel"]:
        """返回真正执行投递的渠道（排除 delay/digest 这类编排步骤）。"""
        return [c for c in cls if c not in (cls.DELAY, cls.DIGEST)]


class Provider(Protocol):
    """Provider 占位接口：把一个已渲染好的消息投递到对应渠道。

    契约约定：每个 Provider 负责单一渠道；``send`` 返回投递结果，
    异常由上层统一捕获并写入消息状态机。
    """

    channel: Channel

    def send(self, message: Any) -> dict[str, Any]:
        """执行投递并返回结果（如 ``{"status": "sent"}``）。"""
        ...


@runtime_checkable
class ProviderRegistry(Protocol):
    """按渠道注册/获取 Provider 的注册表契约（后续 Phase 落地实现）。"""

    def register(self, provider: Provider) -> None: ...

    def get(self, channel: Channel) -> Provider | None: ...
