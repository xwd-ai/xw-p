"""站内信 Provider：把投递写进订阅者的消息中心。

站内信没有外部网关，投递动作就是一次消息落库（见 :mod:`app.message_center`）。
因此这个实现不做网络调用，只负责三件事：

1. **取内容**：正文优先取 ``body``（引擎路径下它是渲染结果），API 内置消费者
   不渲染模板，正文回落到载荷里的 ``body`` / ``message``；标题优先取载荷里的
   ``title``——那是事件自带的业务标题（如「有人赞了你的帖子」），比渠道层传下来
   的 ``subject`` 更贴近收件箱里的展示需求，没有时才退回 ``subject`` 与工作流名。
   正文与载荷里都没有内容时如实判失败——收件箱里多一条点不动的空卡片比投递失败
   更难排查。
2. **定标识**：``trigger_id:step_id``。同一触发重放时标识不变，写入按标识幂等，
   收件箱不会出现两条一样的通知；没有触发标识时退化为带随机后缀的标识
   （重放会新增一条，属可接受的降级）。
3. **落库并回报**：写入结果（新建 / 重复 / 改写）记进日志，投递结果为成功；
   存储报错则转成带原因的失败，由调用方写进执行明细。

引擎侧的 ``internal/pipeline/console_inapp.go`` 做的是同一件事，区别在于引擎
负责模板渲染与聚合卡片，本实现只处理「已经渲染好的、逐条送达」的站内信。
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Optional
from uuid import uuid4

from ..message_center import (
    DEFAULT_TITLE,
    Message,
    MessageError,
    MessageStore,
    WriteOutcome,
    get_store,
)
from ..models.channel import Channel
from .base import Delivery, DeliveryResult, Provider
from .health import DEFAULT_PROBE_TIMEOUT, ProbeResult

logger = logging.getLogger("xw.provider.inapp")

#: 正文在载荷里的备选字段，与收件箱接口读取投递记录时的回退顺序一致。
_BODY_KEYS = ("body", "message", "text", "content")

#: 没有步骤标识时用在消息标识里的占位，保证同一触发重复投递仍然幂等。
_DEFAULT_STEP = "in_app"


def _first_text(*values: object) -> str:
    """返回第一个非空字符串；非标量（对象、数组）视为没有取到。"""
    for value in values:
        if isinstance(value, str):
            text = value.strip()
        elif isinstance(value, bool):
            continue
        elif isinstance(value, (int, float)):
            text = str(value).strip()
        else:
            continue
        if text:
            return text
    return ""


def _payload_body(payload: Mapping[str, object]) -> str:
    """从载荷里取正文：按 ``_BODY_KEYS`` 顺序找第一个非空值。"""
    for key in _BODY_KEYS:
        text = _first_text(payload.get(key))
        if text:
            return text
    return ""


def message_id(trigger_id: str, step_id: str) -> str:
    """生成消息标识：``trigger_id:step_id``，缺触发标识时带随机后缀。"""
    trigger = trigger_id.strip()
    step = step_id.strip() or _DEFAULT_STEP
    if trigger:
        return f"{trigger}:{step}"
    return f"inapp-{uuid4().hex}"


class InAppProvider(Provider):
    """把站内信写入消息中心的渠道实现（``kind = "inapp"``）。"""

    kind = "inapp"

    def __init__(
        self,
        channel: Channel = Channel.IN_APP,
        *,
        store: Optional[MessageStore] = None,
        identifier: str = "",
        priority: int = 100,
        now: Optional[Callable[[], datetime]] = None,
        log: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(channel, identifier=identifier, priority=priority)
        if channel is not Channel.IN_APP:
            raise ValueError(f"站内信实现只能用于 in_app 渠道，收到 {channel.value}")
        self._store = store
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._log = log or logger

    @property
    def store(self) -> MessageStore:
        """该实现使用的消息中心存储（首次取用时按 ``XW_INBOX_FILE`` 构造）。"""
        if self._store is None:
            self._store = get_store()
        return self._store

    def probe(self, timeout: float = DEFAULT_PROBE_TIMEOUT) -> ProbeResult:
        """确认消息中心可写。

        站内信没有外部网关，唯一的失败来源是存储：文件被目录占位、目录不可写。
        这里只做「目录存在且可写」的判断，不写测试消息——健康检查不该在收件箱
        里留下痕迹。进程内存储没有外部依赖，直接通过。
        """
        target = getattr(self.store, "path", None)
        if target is None:
            return ProbeResult.ok("消息中心为进程内存储，无外部依赖")
        path = Path(str(target))
        if path.exists() and not path.is_file():
            return ProbeResult.unavailable(f"消息中心路径不是文件：{path}")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return ProbeResult.unavailable(f"消息中心目录不可用：{path.parent}（{exc}）")
        if not os.access(path.parent, os.W_OK):
            return ProbeResult.unavailable(f"消息中心目录不可写：{path.parent}")
        return ProbeResult.ok(f"消息中心可写：{path}")

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        payload = dict(delivery.payload or {})
        metadata = dict(delivery.metadata or {})

        subscriber = delivery.recipient.address(Channel.IN_APP) or ""
        trigger_id = _first_text(metadata.get("trigger_id"), payload.get("trigger_id"))
        step_id = _first_text(metadata.get("step_id"), payload.get("step_id"))
        workflow = _first_text(metadata.get("workflow"), payload.get("workflow"))

        title = _first_text(payload.get("title"), delivery.subject, workflow, DEFAULT_TITLE)
        body = _first_text(delivery.body, _payload_body(payload))
        if not body:
            self._log.warning(
                "[in_app] 投递失败：正文为空 subscriber=%s workflow=%s trigger=%s",
                subscriber,
                workflow,
                trigger_id,
            )
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason="站内信正文为空",
            )

        message = Message.create(
            id=message_id(trigger_id, step_id),
            subscriber_id=subscriber,
            title=title,
            body=body,
            trigger_id=trigger_id,
            workflow=workflow,
            step_id=step_id,
            now=self._now(),
        )

        try:
            outcome = self.store.write(message)
        except (MessageError, OSError) as exc:
            self._log.warning(
                "[in_app] 投递失败：写入消息中心出错 id=%s subscriber=%s error=%s",
                message.id,
                message.subscriber_id,
                exc,
            )
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"写入消息中心失败：{exc}",
            )

        if outcome is WriteOutcome.MISSING:  # 只在 replace 时出现，普通写入不会走到
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"消息不存在（id={message.id}）",
            )

        self._log.info(
            "[in_app] %s id=%s subscriber=%s title=%r workflow=%s step=%s",
            "已存在同一条站内信（幂等）" if outcome is WriteOutcome.DUPLICATE else "已写入消息中心",
            message.id,
            message.subscriber_id,
            message.title,
            message.workflow or "-",
            message.step_id or "-",
        )
        return DeliveryResult.ok(self._id_for_result, delivery.channel)


__all__ = ["InAppProvider", "message_id"]
