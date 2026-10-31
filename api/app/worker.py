"""后台 worker：消费队列，经渠道 Provider 投递并落库。

只在 memory 总线的单进程 demo 里使用（启停条件见 ``config.api_worker_enabled``）：
redis 模式下队列由 Go 引擎消费，由引擎执行工作流步骤与渠道分发。

这里的实现保持最简：按请求声明的渠道逐个投递——从注册表取该渠道当前可用的
实现、把 ``SubscriberTarget`` 的联系方式交给它，投递结果（成功 / 失败原因 /
耗时）逐条写入执行明细。渠道没配实现、缺地址、实现内部报错都记为失败并保留
原因，不再无条件写「已发送」；一条消息只要有渠道失败，整条记录即为失败。

落库后端由 ``XW_STORAGE`` 决定（jsonl / postgres），见 ``storage.py``。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from .bus import get_bus
from .execution_log import record_execution
from .models.channel import Channel
from .providers import Delivery, Recipient, get_registry
from .schemas import QueuedMessage
from .storage import get_storage

logger = logging.getLogger("xw.worker")


class Worker:
    """持续消费消息总线的后台线程。"""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="xw-worker", daemon=True)
        self._thread.start()
        logger.info("worker 已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3.0)
        logger.info("worker 已停止")

    def _run(self) -> None:
        bus = get_bus()
        while not self._stop.is_set():
            msg: QueuedMessage | None = bus.consume(timeout=1.0)
            if msg is None:
                continue
            self._process(msg)

    def _process(self, msg: QueuedMessage) -> None:
        """按渠道投递一条消息，并把结果写入持久化后端。"""
        channels = msg.channels or ["in_app"]
        recipient = Recipient(
            subscriber_id=msg.subscriber_id,
            email=msg.email,
            phone=msg.phone,
            device_token=msg.device_token,
        )
        registry = get_registry()
        failures: list[str] = []

        for raw in channels:
            channel = _parse_channel(raw)
            if channel is None:
                reason = f"未知渠道：{raw!r}"
                record_execution(
                    trigger_id=msg.trigger_id, channel=str(raw), status="failed", reason=reason
                )
                failures.append(reason)
                continue

            provider = registry.get(channel)
            if provider is None:
                reason = f"渠道 {channel.value} 没有可用的渠道实现"
                record_execution(
                    trigger_id=msg.trigger_id, channel=channel.value, status="failed", reason=reason
                )
                failures.append(f"{channel.value}: {reason}")
                continue

            result = provider.send(
                Delivery(
                    channel=channel,
                    recipient=recipient,
                    subject=msg.name,
                    payload=msg.payload,
                    metadata={
                        "trigger_id": msg.trigger_id,
                        "transaction_id": msg.transaction_id,
                        "workflow": msg.name,
                    },
                )
            )
            record_execution(
                trigger_id=msg.trigger_id,
                channel=channel.value,
                status="sent" if result.sent else "failed",
                reason=result.reason,
                duration_ms=result.duration_ms,
            )
            if result.sent:
                logger.info(
                    "已发送: %s -> %s (%s, %s, %dms)",
                    msg.name,
                    msg.subscriber_id,
                    channel.value,
                    result.provider_id,
                    result.duration_ms,
                )
            else:
                failures.append(f"{channel.value}: {result.reason}")
                logger.warning(
                    "投递失败: %s -> %s (%s)：%s",
                    msg.name,
                    msg.subscriber_id,
                    channel.value,
                    result.reason,
                )

        record = {
            "trigger_id": msg.trigger_id,
            "transaction_id": msg.transaction_id,
            "workflow": msg.name,
            "subscriber_id": msg.subscriber_id,
            "channels": channels,
            "payload": msg.payload,
            "status": "failed" if failures else "sent",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if failures:
            record["reason"] = "; ".join(failures)
        get_storage().save(record)


def _parse_channel(raw: object) -> Optional[Channel]:
    """把渠道名解析为枚举；非投递渠道或未知名称返回 ``None``。"""
    if not isinstance(raw, str):
        return None
    try:
        channel = Channel(raw.strip())
    except ValueError:
        return None
    return channel if channel.is_delivery() else None
