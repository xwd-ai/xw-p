"""console 渠道实现：把投递打印成结构化日志。

本地跑通链路、没有真实渠道凭据时使用。它不写收件箱、不发网络请求，
只把「谁发给谁、发的是哪条事件」记进日志，因此适合作为默认实现与
渠道配置里的兜底项。真实实现（SMTP / 短信网关 / 推送）接入后按优先级
排在它前面，console 留作降级路径。
"""
from __future__ import annotations

import json
import logging

from ..models.channel import Channel
from .base import Delivery, DeliveryResult, Provider

logger = logging.getLogger("xw.provider.console")


def _preview(value: object, limit: int = 160) -> str:
    """把载荷压成一行做日志，超长截断，避免一条日志刷满屏。"""
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        text = repr(value)
    return text if len(text) <= limit else text[:limit] + "…"


class ConsoleProvider(Provider):
    """按渠道打印投递内容的 mock 实现。"""

    kind = "console"

    def __init__(
        self,
        channel: Channel,
        *,
        identifier: str = "",
        priority: int = 100,
        log: logging.Logger | None = None,
    ) -> None:
        super().__init__(channel, identifier=identifier, priority=priority)
        self._log = log or logger

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        address = delivery.recipient.address(delivery.channel) or delivery.recipient.subscriber_id
        body = delivery.body.strip()
        self._log.info(
            "[console] channel=%s provider=%s to=%s subject=%r body=%r payload=%s",
            delivery.channel.value,
            self._id_for_result,
            address,
            delivery.subject,
            body,
            _preview(dict(delivery.payload)),
        )
        return DeliveryResult.ok(self._id_for_result, delivery.channel)


__all__ = ["ConsoleProvider"]
