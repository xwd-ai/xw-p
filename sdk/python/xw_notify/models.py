"""接口数据的只读表示。

两条约定贯穿本模块：

1. **线上是驼峰，本地是下划线**。接口沿用 ``subscriberId`` / ``triggerId`` /
   ``nextCursor`` 这一套写法，Python 侧则用 ``subscriber_id`` / ``next_cursor``。
   转换只做在**已知字段**上（见各 ``from_payload``），不对字典做通用的驼峰↔
   下划线批量改写：``payload`` 与 ``overrides`` 里的键是调用方的业务数据，
   里面有 ``post_id``、``postId`` 或中文键都很正常，通用改写会把它们改坏。
2. **解析失败要说清缺了什么**。字段比 SDK 预期少时抛
   :class:`~xw_notify.errors.ProtocolError` 并指出字段名，而不是等到调用方
   使用属性时抛出一个没有上下文的 ``AttributeError``。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from .errors import ProtocolError


def _require(data: Mapping[str, Any], key: str, *, context: str) -> Any:
    if key not in data:
        raise ProtocolError(f"{context} 缺少字段 {key}，可能是 SDK 与服务端版本不匹配")
    return data[key]


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _text_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(item) for item in value)


@dataclass(frozen=True)
class Health:
    """``GET /health`` 的结果。"""

    status: str
    queued: int

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "Health":
        return cls(
            status=str(data.get("status", "")),
            queued=int(data.get("queued") or 0),
        )


@dataclass(frozen=True)
class Event:
    """一条待触发的互动事件。

    与接口里的「工作流标识 + 收件人 + 渲染变量」一一对应，但用起来像数据：
    同一个事件既可以单条发送，也可以放进列表交给批量接口，构造一次即可复用。

    ``channels`` 为 ``None`` 表示**不指定**，由工作流的步骤决定投哪些渠道；给
    列表则只投其中的渠道。空列表与 ``None`` 不同——空列表会写成 ``[]``，
    服务端按「调用方声明了零个渠道」处理。
    """

    name: str
    subscriber_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    channels: Optional[Sequence[str]] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    device_token: Optional[str] = None
    overrides: Mapping[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        """折成请求体。每次调用都新建字典，不把内部引用交出去。"""
        target: dict[str, Any] = {"subscriberId": self.subscriber_id}
        if self.email is not None:
            target["email"] = self.email
        if self.phone is not None:
            target["phone"] = self.phone
        if self.device_token is not None:
            target["deviceToken"] = self.device_token
        if self.channels is not None:
            target["channels"] = [str(item) for item in self.channels]

        body: dict[str, Any] = {
            "name": self.name,
            "to": target,
            "payload": dict(self.payload),
        }
        if self.overrides:
            body["overrides"] = dict(self.overrides)
        return body

    @classmethod
    def from_wire(cls, data: Mapping[str, Any]) -> "Event":
        target = data.get("to") or {}
        if not isinstance(target, Mapping):
            raise ProtocolError("触发事件缺少 to 对象")
        channels = target.get("channels")
        return cls(
            name=str(_require(data, "name", context="触发事件")),
            subscriber_id=str(
                _require(target, "subscriberId", context="触发事件的 to")
            ),
            payload=dict(data.get("payload") or {}),
            channels=tuple(channels) if channels is not None else None,
            email=_text(target.get("email")),
            phone=_text(target.get("phone")),
            device_token=_text(target.get("deviceToken")),
            overrides=dict(data.get("overrides") or {}),
        )


@dataclass(frozen=True)
class TriggerAck:
    """单条触发的回执。"""

    trigger_id: str
    transaction_id: str
    acknowledgment_id: str
    acknowledged: bool = True

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "TriggerAck":
        context = "触发回执"
        return cls(
            trigger_id=str(_require(data, "triggerId", context=context)),
            transaction_id=str(_require(data, "transactionId", context=context)),
            acknowledgment_id=str(_require(data, "acknowledgmentId", context=context)),
            acknowledged=bool(data.get("acknowledged", True)),
        )


@dataclass(frozen=True)
class BatchItem:
    """批量触发中单条事件的结果。

    被拒的条目没有触发标识，``error`` 里是原因；用 :attr:`accepted` 判断，
    不要拿 ``trigger_id`` 是否为空当条件——空字符串与 ``None`` 都会出现。
    """

    index: int
    name: str
    subscriber_id: str
    status: str
    trigger_id: Optional[str] = None
    transaction_id: Optional[str] = None
    acknowledgment_id: Optional[str] = None
    error: Optional[str] = None

    @property
    def accepted(self) -> bool:
        return self.status == "accepted"

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "BatchItem":
        context = "批量触发结果"
        return cls(
            index=int(_require(data, "index", context=context)),
            name=str(_require(data, "name", context=context)),
            subscriber_id=str(_require(data, "subscriberId", context=context)),
            status=str(_require(data, "status", context=context)),
            trigger_id=_text(data.get("triggerId")),
            transaction_id=_text(data.get("transactionId")),
            acknowledgment_id=_text(data.get("acknowledgmentId")),
            error=_text(data.get("error")),
        )


@dataclass(frozen=True)
class TriggerBatch:
    """批量回执：计数 + 与请求等长的逐条结果。"""

    accepted: int
    rejected: int
    results: tuple[BatchItem, ...]

    def __iter__(self) -> Iterator[BatchItem]:
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)

    @property
    def accepted_items(self) -> tuple[BatchItem, ...]:
        return tuple(item for item in self.results if item.accepted)

    @property
    def rejected_items(self) -> tuple[BatchItem, ...]:
        return tuple(item for item in self.results if not item.accepted)

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "TriggerBatch":
        context = "批量触发回执"
        raw = _require(data, "results", context=context)
        if not isinstance(raw, (list, tuple)):
            raise ProtocolError("批量触发回执的 results 不是数组")
        return cls(
            accepted=int(data.get("accepted") or 0),
            rejected=int(data.get("rejected") or 0),
            results=tuple(BatchItem.from_payload(item) for item in raw),
        )


@dataclass(frozen=True)
class Subscriber:
    """订阅者。

    ``bindings`` 只列出**当前真的能投**的渠道（渠道已启用且地址存在），
    排查「为什么没收到邮件」时先看这里，比逐个判断 ``email`` 是否为空准确。
    """

    id: str
    environment: str
    channels: tuple[str, ...] = ()
    bindings: Mapping[str, str] = field(default_factory=dict)
    email: Optional[str] = None
    phone: Optional[str] = None
    device_token: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def binds(self, channel: str) -> bool:
        """该订阅者能否经这个渠道收到通知。"""
        return channel in self.bindings

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "Subscriber":
        context = "订阅者"
        bindings = data.get("bindings") or {}
        return cls(
            id=str(_require(data, "id", context=context)),
            environment=str(data.get("environment") or ""),
            channels=_text_list(data.get("channels")),
            bindings={str(k): str(v) for k, v in dict(bindings).items()},
            email=_text(data.get("email")),
            phone=_text(data.get("phone")),
            device_token=_text(data.get("deviceToken")),
            created_at=_text(data.get("created_at")),
            updated_at=_text(data.get("updated_at")),
        )


@dataclass(frozen=True)
class SubscriberPage:
    """一页订阅者。``next_cursor`` 为空表示已经是最后一页。"""

    items: tuple[Subscriber, ...]
    next_cursor: Optional[str] = None

    def __iter__(self) -> Iterator[Subscriber]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    @property
    def has_more(self) -> bool:
        return bool(self.next_cursor)

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "SubscriberPage":
        raw = data.get("items")
        if not isinstance(raw, (list, tuple)):
            raise ProtocolError("订阅者列表缺少 items 数组")
        return cls(
            items=tuple(Subscriber.from_payload(item) for item in raw),
            next_cursor=_text(data.get("nextCursor")) or None,
        )


@dataclass(frozen=True)
class Preference:
    """一条发送偏好记录。

    ``target`` 的含义取决于 ``scope``：``channel`` 层是渠道名，``workflow``
    层是工作流名，``global`` 层为空。
    """

    scope: str
    enabled: bool
    target: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "Preference":
        context = "偏好记录"
        return cls(
            scope=str(_require(data, "scope", context=context)),
            enabled=bool(_require(data, "enabled", context=context)),
            target=_text(data.get("target")),
            updated_at=_text(data.get("updated_at")),
        )


@dataclass(frozen=True)
class PreferenceList:
    """某个订阅者显式设置过的全部偏好。

    ``counts`` 恒有三个键（global / workflow / channel），值为该层的条数；
    某个层级为 0 表示「没表过态」，解析时会继续往上一层找。
    """

    subscriber_id: str
    items: tuple[Preference, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)
    default_allowed: bool = True

    def __iter__(self) -> Iterator[Preference]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "PreferenceList":
        raw = data.get("items") or []
        counts = data.get("counts") or {}
        return cls(
            subscriber_id=str(_require(data, "subscriber_id", context="偏好列表")),
            items=tuple(Preference.from_payload(item) for item in raw),
            counts={str(k): int(v) for k, v in dict(counts).items()},
            default_allowed=bool(data.get("default_allowed", True)),
        )


@dataclass(frozen=True)
class PreferenceUpsert:
    """写入回执。``created`` 为假表示覆盖了已有记录。"""

    created: bool
    preference: Preference

    @property
    def scope(self) -> str:
        return self.preference.scope

    @property
    def target(self) -> Optional[str]:
        return self.preference.target

    @property
    def enabled(self) -> bool:
        return self.preference.enabled

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "PreferenceUpsert":
        return cls(
            created=bool(_require(data, "created", context="偏好写入回执")),
            preference=Preference.from_payload(data),
        )


@dataclass(frozen=True)
class Resolution:
    """一次偏好解析的结论。

    可以直接当布尔用（``if resolution:`` 即「会发出」），也可以读
    ``scope`` / ``reason`` 回答「是哪一层决定的、为什么」。
    """

    allowed: bool
    matched: bool
    reason: str = ""
    scope: Optional[str] = None
    target: Optional[str] = None

    def __bool__(self) -> bool:
        return self.allowed

    @classmethod
    def from_payload(cls, data: Mapping[str, Any]) -> "Resolution":
        context = "偏好解析结果"
        return cls(
            allowed=bool(_require(data, "allowed", context=context)),
            matched=bool(_require(data, "matched", context=context)),
            reason=str(data.get("reason") or ""),
            scope=_text(data.get("scope")),
            target=_text(data.get("target")),
        )


def events_to_wire(events: Iterable[Event]) -> list[dict[str, Any]]:
    """把事件列表折成请求体数组。"""
    return [event.to_wire() for event in events]


__all__ = [
    "BatchItem",
    "Event",
    "Health",
    "Preference",
    "PreferenceList",
    "PreferenceUpsert",
    "Resolution",
    "Subscriber",
    "SubscriberPage",
    "TriggerAck",
    "TriggerBatch",
    "events_to_wire",
]
