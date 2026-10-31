"""渠道 Provider 抽象基类：投递契约、收件人寻址与投递结果。

``models.channel`` 里的 ``Provider`` 只声明了「有 channel、有 send」这一层协议；
本模块把它落成可继承的基类，收口三件在各渠道实现里会重复写的事情：

1. **收件人寻址**：一条消息要发给谁，取决于渠道——邮件看邮箱、短信看手机号、
   推送看设备 token、站内信看订阅者标识。由 :class:`Recipient` 统一寻址，
   缺地址时在进入渠道实现之前就判失败，而不是等 SMTP 握手时报认证错误。
2. **结果结构**：:class:`DeliveryResult` 用 ``sent`` + ``reason`` 表达成功与失败，
   渠道实现不向上抛异常（网络抖动、限流、模板报错都是正常的投递结果），
   由基类兜住意外异常并转成失败结果，调用方只处理一种形态。
3. **耗时与标识**：投递耗时由基类打点，日志与执行明细不用各自计时。

具体实现（console / SMTP / webhook 等）继承 :class:`Provider`，只写
``_deliver``；渠道与标识在构造时固定，注册表按渠道索引。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from time import perf_counter
from typing import Any, Mapping, Optional

from ..models.channel import Channel

# 默认优先级：数值越小越先取用，与引擎侧注册表的默认值保持一致。
DEFAULT_PRIORITY = 100

# 各投递渠道的寻址字段：渠道 → Recipient 上承载地址的字段名。
# chat 的投递目标由 provider 自己的 webhook 配置决定，不依赖逐条消息的地址，
# 因此不在表内。
ADDRESS_FIELDS: dict[Channel, str] = {
    Channel.IN_APP: "subscriber_id",
    Channel.EMAIL: "email",
    Channel.SMS: "phone",
    Channel.PUSH: "device_token",
}

# 渠道 → 缺地址时给调用方看的字段说明。
_FIELD_LABEL: dict[str, str] = {
    "subscriber_id": "订阅者标识",
    "email": "邮箱地址",
    "phone": "手机号",
    "device_token": "设备 token",
}


@dataclass(frozen=True)
class Recipient:
    """投递目标的联系信息。

    字段可空：一次触发只带某几个渠道的联系方式时，其余渠道在投递前就会被判
    「缺地址」，不需要调用方为每个渠道各构造一个对象。
    """

    subscriber_id: str
    email: Optional[str] = None
    phone: Optional[str] = None
    device_token: Optional[str] = None

    def address(self, channel: Channel) -> Optional[str]:
        """返回该渠道的寻址值；渠道无需逐条地址（如 chat）时返回 ``None``。"""
        name = ADDRESS_FIELDS.get(channel)
        if name is None:
            return None
        value = getattr(self, name, None)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def missing(self, channel: Channel) -> Optional[str]:
        """返回该渠道缺失的寻址字段说明；地址齐备或渠道不需地址时返回 ``None``。"""
        name = ADDRESS_FIELDS.get(channel)
        if name is None or self.address(channel) is not None:
            return None
        return _FIELD_LABEL.get(name, name)


@dataclass(frozen=True)
class Delivery:
    """一次投递请求：投递渠道 + 收件人 + 已渲染的内容。

    ``body`` 可以是空串——API 侧的内置消费者不渲染模板（渲染由引擎的
    render 阶段完成），此时渠道实现只记下将要发送的事件与载荷。
    """

    channel: Channel
    recipient: Recipient
    subject: str = ""
    body: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeliveryResult:
    """一次投递的结果。

    成功与失败用同一结构表达：``sent`` 为假时 ``reason`` 说明原因，
    成功时 ``reason`` 一律为空，避免调用方读到上一轮的残留字段。
    """

    provider_id: str
    channel: Channel
    sent: bool
    reason: Optional[str] = None
    duration_ms: int = 0

    def __post_init__(self) -> None:
        if self.sent:
            object.__setattr__(self, "reason", None)
        else:
            reason = (self.reason or "").strip()
            object.__setattr__(self, "reason", reason or "渠道未给出失败原因")

    @classmethod
    def ok(cls, provider_id: str, channel: Channel, duration_ms: int = 0) -> "DeliveryResult":
        return cls(provider_id=provider_id, channel=channel, sent=True, duration_ms=duration_ms)

    @classmethod
    def failed(
        cls,
        provider_id: str,
        channel: Channel,
        reason: str,
        duration_ms: int = 0,
    ) -> "DeliveryResult":
        return cls(
            provider_id=provider_id,
            channel=channel,
            sent=False,
            reason=reason,
            duration_ms=duration_ms,
        )


class Provider(ABC):
    """渠道 Provider 基类。

    子类只需实现 :meth:`_deliver`；渠道、标识与优先级在构造时确定：

    >>> class EchoProvider(Provider):
    ...     def _deliver(self, delivery):
    ...         return DeliveryResult.ok(self.identifier, delivery.channel)
    >>> EchoProvider(Channel.EMAIL, identifier="echo").channel
    <Channel.EMAIL: 'email'>

    ``send`` 是模板方法：先校验渠道与地址，再调用 ``_deliver``，最后补齐
    原因与耗时。子类里抛出的异常会被转成失败结果——投递路径上的调用方
    因此不必写 ``try``，也不会因为某个渠道实现漏了异常处理而中断整条分发。

    优先级只作为登记时的缺省值（:attr:`default_priority`）；同一个实现可以
    被登记到不同注册表、以不同优先级取用，所以生效值记在注册表的条目上。
    """

    #: 实现种类标识，与渠道配置里的 ``kind`` 对应（console / smtp / twilio …）。
    kind: str = ""

    #: 是否要求收件人有该渠道的地址。mock 实现同样要求，缺地址时如实判失败，
    #: 免得演示时把「地址没配」误当成「已经发出去了」。
    requires_address: bool = True

    def __init__(
        self,
        channel: Channel,
        *,
        identifier: str = "",
        priority: int = DEFAULT_PRIORITY,
    ) -> None:
        if not isinstance(channel, Channel):
            raise TypeError(f"渠道必须是 Channel 枚举：{channel!r}")
        if not channel.is_delivery():
            raise ValueError(f"{channel.value!r} 是编排步骤，不能作为投递渠道")
        if priority < 0:
            raise ValueError("优先级不能为负数")
        self._channel = channel
        self._identifier = identifier.strip()
        self._priority = int(priority)

    @property
    def channel(self) -> Channel:
        """该实现负责的投递渠道（构造时固定，运行期不变）。"""
        return self._channel

    @property
    def identifier(self) -> str:
        """实现标识；为空时由注册表按渠道自动编号。"""
        return self._identifier

    @property
    def default_priority(self) -> int:
        """登记时使用的缺省优先级；注册表里以条目上的优先级为准。"""
        return self._priority

    def supports(self, channel: Channel) -> bool:
        """是否负责该渠道。"""
        return channel == self._channel

    def send(self, delivery: Delivery) -> DeliveryResult:
        """执行投递，返回结构化结果（不抛异常）。"""
        started = perf_counter()
        result = self._check(delivery)
        if result is None:
            try:
                result = self._deliver(delivery)
            except Exception as exc:  # 渠道实现的意外异常统一转成失败结果
                result = DeliveryResult.failed(
                    provider_id=self._id_for_result,
                    channel=delivery.channel,
                    reason=f"{type(exc).__name__}: {exc}",
                )
        return replace(result, duration_ms=self._elapsed_ms(started))

    def _check(self, delivery: Delivery) -> Optional[DeliveryResult]:
        """投递前的前置校验；通过时返回 ``None``。"""
        if delivery.channel != self._channel:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=(
                    f"渠道不匹配：{self._id_for_result} 只负责 {self._channel.value}，"
                    f"收到 {delivery.channel.value}"
                ),
            )
        if self.requires_address:
            missing = delivery.recipient.missing(self._channel)
            if missing is not None:
                return DeliveryResult.failed(
                    provider_id=self._id_for_result,
                    channel=delivery.channel,
                    reason=f"缺少投递地址：{missing}",
                )
        return None

    @abstractmethod
    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        """真正把消息送出去；失败请返回 ``DeliveryResult.failed``。"""

    @property
    def _id_for_result(self) -> str:
        return self._identifier or f"{self.kind or 'provider'}:{self._channel.value}"

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(0, int((perf_counter() - started) * 1000))

    def __repr__(self) -> str:  # 日志里能直接看出是哪个实现
        return (
            f"<{type(self).__name__} id={self._id_for_result!r} "
            f"channel={self._channel.value!r} default_priority={self._priority}>"
        )


__all__ = [
    "ADDRESS_FIELDS",
    "DEFAULT_PRIORITY",
    "Delivery",
    "DeliveryResult",
    "Provider",
    "Recipient",
]
