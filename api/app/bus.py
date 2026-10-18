"""消息总线：内存 / Redis 双后端，通过环境变量切换。

- memory（默认）：线程安全的内存 FIFO 队列，demo 与单测用。
- redis：基于 Redis List（LPUSH/BRPOP）的持久化队列，生产链路用。

通过 ``XW_BUS`` 环境变量选择后端，``get_bus()`` 返回全局单例。
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from typing import Optional, Protocol

from . import config
from .schemas import QueuedMessage

logger = logging.getLogger("xw.bus")


class MessageBus(Protocol):
    """总线后端统一接口：publish 入队，consume 阻塞出队，size 返回待消费数量。"""

    def publish(self, message: QueuedMessage) -> None: ...

    def consume(self, timeout: float = 1.0) -> Optional[QueuedMessage]: ...

    def size(self) -> int: ...


class MemoryMessageBus:
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


class RedisMessageBus:
    """基于 Redis List 的持久化消息总线。

    使用 LPUSH 入队、BRPOP 出队，序列化为 JSON；redis 客户端按需延迟导入，
    避免未安装 redis 依赖时（纯内存模式）影响启动。
    """

    def __init__(self, url: str, queue_key: str) -> None:
        self._url = url
        self._queue_key = queue_key
        self._client = None

    def _get_client(self):
        if self._client is None:
            import redis  # 延迟导入：仅 Redis 模式需要

            self._client = redis.Redis.from_url(self._url, decode_responses=True)
        return self._client

    def publish(self, message: QueuedMessage) -> None:
        payload = json.dumps(message.model_dump(by_alias=False), ensure_ascii=False)
        self._get_client().lpush(self._queue_key, payload)

    def consume(self, timeout: float = 1.0) -> Optional[QueuedMessage]:
        # BRPOP 阻塞秒数必须为整数，且至少为 1 秒
        block = max(1, int(timeout))
        item = self._get_client().brpop(self._queue_key, timeout=block)
        if item is None:
            return None
        _, payload = item
        return QueuedMessage.model_validate(json.loads(payload))

    def size(self) -> int:
        return int(self._get_client().llen(self._queue_key))


def create_bus() -> MessageBus:
    """根据配置创建总线后端实例。"""
    if config.BUS_BACKEND == "redis":
        logger.info("消息总线后端：redis（%s）", config.REDIS_URL)
        return RedisMessageBus(config.REDIS_URL, config.REDIS_QUEUE_KEY)
    logger.info("消息总线后端：memory")
    return MemoryMessageBus()


_bus: Optional[MessageBus] = None
_bus_lock = threading.Lock()


def get_bus() -> MessageBus:
    """获取全局总线单例（惰性初始化）。"""
    global _bus
    with _bus_lock:
        if _bus is None:
            _bus = create_bus()
        return _bus
