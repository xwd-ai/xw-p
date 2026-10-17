"""后台 worker：消费队列，mock 发送并落库为 JSONL。

后续（Phase 2 起）会由 Java engine 接管真正的工作流求值与 provider 分发，
此处的 Python worker 仅作为 demo 阶段的端到端打通。
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from .bus import get_bus
from .schemas import QueuedMessage

logger = logging.getLogger("xw.worker")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUT_FILE = DATA_DIR / "messages.jsonl"


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
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        while not self._stop.is_set():
            msg: QueuedMessage | None = bus.consume(timeout=1.0)
            if msg is None:
                continue
            self._process(msg)

    def _process(self, msg: QueuedMessage) -> None:
        # mock 发送：demo 阶段仅记录到文件并打印
        record = {
            "trigger_id": msg.trigger_id,
            "transaction_id": msg.transaction_id,
            "workflow": msg.name,
            "subscriber_id": msg.subscriber_id,
            "channels": msg.channels or ["in_app"],
            "payload": msg.payload,
            "status": "sent",
        }
        with OUT_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.info("已发送: %s -> %s (%s)", msg.name, msg.subscriber_id, record["channels"])
