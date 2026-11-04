"""触发接口：单条、批量，以及事件对象的构造。

触发是整个系统的入口，也是最需要「重发安全」的地方：一条评论通知重复投递
会让创作者收到两遍。因此 ``idempotency_key`` 在这里是一等参数，单条与批量
都直接暴露；批量接口把它作用在**整批**上（服务端的语义也是如此），不是
逐条各来一个键。

批量上限与服务端 ``MAX_BATCH_EVENTS`` 保持一致：超限的请求一定会被 422
拒绝，与其发出去换一张错误单，不如在本地就指出是哪条超了。
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence

from .models import Event, TriggerAck, TriggerBatch

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型检查
    from .client import Client

#: 单次批量触发的上限，与服务端 ``MAX_BATCH_EVENTS`` 对齐。
MAX_BATCH_EVENTS = 100

TRIGGER_PATH = "/v1/events/trigger"
BATCH_PATH = "/v1/events/trigger/batch"


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class TriggerResource:
    """``/v1/events/trigger`` 相关接口。"""

    def __init__(self, client: "Client") -> None:
        self._client = client

    # -- 构造 -------------------------------------------------------------

    def build(
        self,
        name: str,
        subscriber_id: str,
        *,
        payload: Optional[Mapping[str, object]] = None,
        channels: Optional[Sequence[str]] = None,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        device_token: Optional[str] = None,
        overrides: Optional[Mapping[str, object]] = None,
    ) -> Event:
        """构造一个事件对象，不发请求。

        批量场景下先用它组好整批再一次性提交；同一个事件也可以重复使用，
        ``to_wire`` 每次都返回新的字典。
        """
        if not _clean(name):
            raise ValueError("name 不能为空（例如 post_commented）")
        if not _clean(subscriber_id):
            raise ValueError("subscriber_id 不能为空")
        return Event(
            name=str(name).strip(),
            subscriber_id=str(subscriber_id).strip(),
            payload=dict(payload or {}),
            channels=list(channels) if channels is not None else None,
            email=_clean(email),
            phone=_clean(phone),
            device_token=_clean(device_token),
            overrides=dict(overrides or {}),
        )

    # -- 单条 -------------------------------------------------------------

    def send(
        self,
        name: str,
        subscriber_id: str,
        *,
        payload: Optional[Mapping[str, object]] = None,
        channels: Optional[Sequence[str]] = None,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        device_token: Optional[str] = None,
        overrides: Optional[Mapping[str, object]] = None,
        idempotency_key: Optional[str] = None,
    ) -> TriggerAck:
        """触发一条互动事件，返回回执。

        ``channels`` 留空表示由工作流的步骤决定投哪些渠道；传列表则只投其中
        的渠道（订阅者没绑定的渠道会被工作流侧跳过，不会报错）。
        """
        event = self.build(
            name,
            subscriber_id,
            payload=payload,
            channels=channels,
            email=email,
            phone=phone,
            device_token=device_token,
            overrides=overrides,
        )
        return self.send_event(event, idempotency_key=idempotency_key)

    def send_event(self, event: Event, *, idempotency_key: Optional[str] = None) -> TriggerAck:
        """发送一个已构造好的事件。"""
        key = _clean(idempotency_key)
        data = self._client.request(
            "POST",
            TRIGGER_PATH,
            json_body=event.to_wire(),
            headers={"Idempotency-Key": key} if key else None,
        )
        return TriggerAck.from_payload(data)

    # -- 批量 -------------------------------------------------------------

    def send_batch(
        self, events: Iterable[Event], *, idempotency_key: Optional[str] = None
    ) -> TriggerBatch:
        """一次提交多条事件。

        回执里的 ``results`` 与入参顺序一致：某条工作流不存在或未启用只会让
        该条记为 ``rejected``，同批其余照常入队，所以务必按下标（而非「成功
        的那几条」）对齐。

        :param events: 事件对象列表；不带事件时会本地报错，不发空请求。
        :param idempotency_key: 作用于整批的幂等键。
        """
        items = list(events)
        if not items:
            raise ValueError("批量触发至少需要一条事件")
        if len(items) > MAX_BATCH_EVENTS:
            raise ValueError(
                f"单次批量最多 {MAX_BATCH_EVENTS} 条，本次 {len(items)} 条；请分批提交"
            )
        for index, item in enumerate(items):
            if not isinstance(item, Event):
                raise TypeError(f"第 {index} 条不是 Event：{type(item).__name__}")

        key = _clean(idempotency_key)
        data = self._client.request(
            "POST",
            BATCH_PATH,
            json_body={"events": [item.to_wire() for item in items]},
            headers={"Idempotency-Key": key} if key else None,
        )
        return TriggerBatch.from_payload(data)


__all__ = ["MAX_BATCH_EVENTS", "TriggerResource"]
