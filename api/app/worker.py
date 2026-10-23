"""后台 worker：消费队列，mock 发送并落库。

后续（Phase 2 起）会由 Go engine 接管真正的工作流求值与 provider 分发，
此处的 Python worker 仅作为 demo 阶段的端到端打通。

落库后端由 ``XW_STORAGE`` 决定（jsonl / postgres），见 ``storage.py``。
逐渠道投递时同步写入执行明细（``execution_log``），供后台排查。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

from .bus import get_bus
from .execution_log import record_execution
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
        # mock 发送：demo 阶段仅记录到持久化后端并打印
        channels = msg.channels or ["in_app"]
        record = {
            "trigger_id": msg.trigger_id,
            "transaction_id": msg.transaction_id,
            "workflow": msg.name,
            "subscriber_id": msg.subscriber_id,
            "channels": channels,
            "payload": msg.payload,
            "status": "sent",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        get_storage().save(record)

        # 逐渠道写入执行明细（execution details）
        for ch in channels:
            record_execution(trigger_id=msg.trigger_id, channel=ch, status="sent")

        logger.info("已发送: %s -> %s (%s)", msg.name, msg.subscriber_id, channels)
