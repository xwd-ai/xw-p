"""消息总线：demo 阶段的内存队列。

Phase 0 先跑通链路；任务 2 会替换为 Redis 队列（通过环境变量切换）。
"""
from __future__ import annotations

import queue
import threading
from typing import Optional

from .schemas import QueuedMessage


class MessageBus:
    """线程安全的内存 FIFO 队列。"""

    def __init__(self) -> None:
        self._q: "queue.Queue[QueuedMessage]" = queue.Queue()

    def publish(self, message: QueuedMessage) -> None:
        self._q.put(message)

    def consume(self, timeout: float = 1.0) -> Optional[QueuedMessage]:
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def size(self) -> int:
        return self._q.qsize()


_bus: Optional[MessageBus] = None
_bus_lock = threading.Lock()


def get_bus() -> MessageBus:
    """获取全局总线单例。"""
    global _bus
    with _bus_lock:
        if _bus is None:
            _bus = MessageBus()
        return _bus
