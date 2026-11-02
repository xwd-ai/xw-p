"""推送渠道的 mock 实现：不联网，但按真实契约校验并留痕。

用途是「没有网关凭据时把推送链路跑通」：它不接受任何 silently-succeed 的捷径——
设备 token 按平台校验、报文按 4 KiB 计算、``data`` 只留标量，任何一项不合法都
如实返回失败并给出原因。通过校验后把这一条记进内存环形缓冲，测试与演示可以回看
「刚才到底打算发什么、多少字节、走哪条路径」。

``fail_with`` 用来演示失败与回退路径：配置了它之后每条投递都按这个原因失败，
便于在看板上看到「推送失败 → 降级到邮件」这类分支，而不需要真的把网关弄坏。

与 :mod:`.console` 的分工：console 只打一行日志、不做渠道级校验；mock 是推送渠道
的专用替身，校验与真实实现同一套规则，区别只在于最后一步不发出网络请求。
"""
from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from ..models.channel import Channel
from .base import DEFAULT_PRIORITY, Delivery, DeliveryResult, Provider
from .health import DEFAULT_PROBE_TIMEOUT, ProbeResult
from .push import (
    Platform,
    PushConfigError,
    PushMessage,
    apns_send_path,
    detect_platform,
    fcm_send_path,
    mask_token,
    normalize_token,
    payload_size,
)

logger = logging.getLogger("xw.provider.push_mock")

#: 环形缓冲的缺省容量：足够回看最近一批投递，又不会把进程内存拖大。
DEFAULT_CAPACITY = 50

#: 容量上限。
MAX_CAPACITY = 10_000


@dataclass(frozen=True)
class PushRecord:
    """一次推送投递的留痕（token 已脱敏，不含任何凭据）。"""

    platform: Platform
    token: str
    title: str
    body: str
    data: Mapping[str, str]
    payload_bytes: int
    path: str
    sent: bool
    reason: str
    at: datetime

    def describe(self) -> str:
        """一行摘要，供日志与调试接口使用。"""
        state = "sent" if self.sent else f"failed（{self.reason}）"
        target = self.path or "-"
        return (
            f"{self.platform.value} {self.token} {self.payload_bytes}B "
            f"path={target} {state}"
        )


class MockPushProvider(Provider):
    """按真实契约校验、把投递记进内存的 push 渠道实现。"""

    kind = "push-mock"

    def __init__(
        self,
        channel: Channel = Channel.PUSH,
        *,
        platform: str = "",
        project_id: str = "",
        topic: str = "",
        fail_with: str = "",
        capacity: int = DEFAULT_CAPACITY,
        identifier: str = "",
        priority: int = DEFAULT_PRIORITY,
        log: Optional[logging.Logger] = None,
    ) -> None:
        if channel is not Channel.PUSH:
            raise PushConfigError("push-mock 只能用于 push 渠道")
        super().__init__(channel, identifier=identifier, priority=priority)
        self._platform = _parse_platform(platform)
        self._project_id = str(project_id or "").strip()
        self._topic = str(topic or "").strip()
        self._fail_with = str(fail_with or "").strip()
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise PushConfigError(f"capacity 必须是整数，收到 {capacity!r}")
        if not 1 <= capacity <= MAX_CAPACITY:
            raise PushConfigError(f"capacity 应在 1–{MAX_CAPACITY} 之间，收到 {capacity}")
        self._records: deque[PushRecord] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._log = log or logger

    # ------------------------------------------------------------- 只读属性

    @property
    def platform(self) -> Optional[Platform]:
        """固定平台；``None`` 表示按 token 形态推断。"""
        return self._platform

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def fail_with(self) -> str:
        return self._fail_with

    def records(self) -> tuple[PushRecord, ...]:
        """返回已记录投递的快照（时间顺序，最旧在前）。"""
        with self._lock:
            return tuple(self._records)

    def last_record(self) -> Optional[PushRecord]:
        with self._lock:
            return self._records[-1] if self._records else None

    def clear(self) -> None:
        with self._lock:
            self._records.clear()

    def probe(self, timeout: float = DEFAULT_PROBE_TIMEOUT) -> ProbeResult:
        """mock 不联网，能接收投递就算可用；如实写明它不会真的推送到设备。"""
        platform = self._platform.value if self._platform else "按 token 推断"
        return ProbeResult.ok(f"推送 mock 只在进程内留痕（平台：{platform}），不联网")

    # ----------------------------------------------------------------- 投递

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        raw_token = delivery.recipient.address(Channel.PUSH) or ""
        try:
            platform = self._platform or detect_platform(raw_token)
            token = normalize_token(raw_token, platform)
            message = PushMessage(
                title=delivery.subject,
                body=delivery.body,
                data=_data_from(delivery),
            )
            size = payload_size(platform, token, message)
        except PushConfigError as exc:
            return self._failed(delivery, str(exc))

        path = self._path(platform, token)
        if self._fail_with:
            self._remember(platform, token, message, size, path, sent=False, reason=self._fail_with)
            return self._failed(delivery, self._fail_with)

        self._remember(platform, token, message, size, path, sent=True, reason="")
        self._log.info(
            "[push-mock] 投递成功 provider=%s platform=%s token=%s bytes=%d path=%s",
            self._id_for_result,
            platform.value,
            mask_token(token),
            size,
            path or "-",
        )
        return DeliveryResult.ok(self._id_for_result, delivery.channel)

    def _path(self, platform: Platform, token: str) -> str:
        """算出发送路径；缺 project_id 时留空（只是演示，不去编造一个项目标识）。"""
        if platform is Platform.APNS:
            return apns_send_path(token)
        if not self._project_id:
            return ""
        try:
            return fcm_send_path(self._project_id)
        except PushConfigError:
            return ""

    def _remember(
        self,
        platform: Platform,
        token: str,
        message: PushMessage,
        size: int,
        path: str,
        *,
        sent: bool,
        reason: str,
    ) -> None:
        record = PushRecord(
            platform=platform,
            token=mask_token(token),
            title=message.title,
            body=message.body,
            data=dict(message.data),
            payload_bytes=size,
            path=path,
            sent=sent,
            reason=reason,
            at=datetime.now(timezone.utc),
        )
        with self._lock:
            self._records.append(record)

    def _failed(self, delivery: Delivery, reason: str) -> DeliveryResult:
        self._log.warning(
            "[push-mock] 投递失败 provider=%s subscriber=%s reason=%s",
            self._id_for_result,
            delivery.recipient.subscriber_id,
            reason,
        )
        return DeliveryResult.failed(
            provider_id=self._id_for_result,
            channel=delivery.channel,
            reason=reason,
        )


def _parse_platform(raw: object) -> Optional[Platform]:
    text = str(raw or "").strip().lower()
    if text in ("", "auto"):
        return None
    try:
        return Platform(text)
    except ValueError as exc:
        allowed = "、".join(item.value for item in Platform)
        raise PushConfigError(f"platform 只能是 {allowed} 或 auto，收到 {text!r}") from exc


def _data_from(delivery: Delivery) -> dict[str, Any]:
    """从投递内容里取出要带进推送的 data。

    优先用 ``metadata['data']``（调用方显式指定），否则用事件载荷。事件载荷里可能
    有嵌套对象，而推送的 data 只能是字符串，这类字段直接跳过——它们不是给设备端
    的路由提示，塞进去只会撑大报文。
    """
    source: Mapping[str, Any] = delivery.metadata.get("data")  # type: ignore[assignment]
    if not isinstance(source, Mapping):
        source = delivery.payload
    data: dict[str, Any] = {}
    for key, value in (source or {}).items():
        if isinstance(value, (Mapping, list, tuple, set)):
            continue
        data[str(key)] = value
    return data


__all__ = [
    "DEFAULT_CAPACITY",
    "MAX_CAPACITY",
    "MockPushProvider",
    "PushRecord",
]
