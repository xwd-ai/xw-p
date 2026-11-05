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
from typing import Iterable, Optional, Protocol

from . import app_metrics, config
from .schemas import QueuedMessage

logger = logging.getLogger("xw.bus")


class MessageBus(Protocol):
    """总线后端统一接口：publish 入队，consume 阻塞出队，size 返回待消费数量。"""

    def publish(self, message: QueuedMessage) -> None: ...

    def publish_many(self, messages: Iterable[QueuedMessage]) -> int: ...

    def consume(self, timeout: float = 1.0) -> Optional[QueuedMessage]: ...

    def size(self) -> int: ...


class MemoryMessageBus:
    """线程安全的内存 FIFO 队列。"""

    def __init__(self) -> None:
        self._q: "queue.Queue[QueuedMessage]" = queue.Queue()

    def publish(self, message: QueuedMessage) -> None:
        self._q.put(message)

    def publish_many(self, messages: Iterable[QueuedMessage]) -> int:
        """逐条放入内存队列；数量以实际放入的条数为准。"""
        count = 0
        for message in messages:
            self._q.put(message)
            count += 1
        return count

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

            # 必须带 socket 超时：没有它时 Redis 一旦半死不活（接受连接但不
            # 回包），请求会一直挂在这里，就绪探测也拿不到结论。超时值大于
            # 消费端的阻塞秒数（``consume`` 最多阻塞 1 秒），否则 BRPOP 会
            # 在正常等待时被判成读超时。
            self._client = redis.Redis.from_url(
                self._url,
                decode_responses=True,
                socket_timeout=config.REDIS_TIMEOUT,
                socket_connect_timeout=config.REDIS_TIMEOUT,
            )
        return self._client

    def ping(self) -> None:
        """连通性探测，供就绪检查使用；失败时抛 redis 自己的异常。"""
        self._get_client().ping()

    def publish(self, message: QueuedMessage) -> None:
        self._get_client().lpush(self._queue_key, _encode(message))

    def publish_many(self, messages: Iterable[QueuedMessage]) -> int:
        """一批消息一次往返写入。

        逐条 ``LPUSH`` 的代价不在命令本身，而在每条一次往返：批量触发 100 条
        就是 100 次网络等待。这里把它们装进同一个 pipeline（不开事务：入队
        之间没有需要原子化的约束，``MULTI`` 只会多两次往返）一次发出。
        顺序与原列表一致——逐条 ``LPUSH`` 时最后一条在最左，消费端 ``BRPOP``
        从右端取，先入队的先被消费。
        """
        payloads = [_encode(message) for message in messages]
        if not payloads:
            return 0
        pipe = self._get_client().pipeline(transaction=False)
        for payload in payloads:
            pipe.lpush(self._queue_key, payload)
        pipe.execute()
        return len(payloads)

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


def _encode(message: QueuedMessage) -> str:
    """把消息序列化成入队载荷；字段名按内部契约（snake_case）落盘。"""
    return json.dumps(message.model_dump(by_alias=False), ensure_ascii=False)


def publish_all(bus: MessageBus, messages: Iterable[QueuedMessage]) -> int:
    """把一批消息交给总线，返回入队条数。

    后端支持 ``publish_many`` 时走批量通道（Redis 下一次往返发完），否则
    退回逐条 ``publish``。回退分支是给外部注入的总线实现留的余地：批量
    只是吞吐优化，不该成为接入的硬要求。

    入队计数在这里记而不在每个调用点：这是「消息真正进入队列」的唯一出口，
    三个触发入口都经过它，放在调用点就得维护三份且迟早漏一处。
    """
    items = list(messages)
    if not items:
        return 0
    bulk = getattr(bus, "publish_many", None)
    if callable(bulk):
        count = int(bulk(items))
    else:
        for message in items:
            bus.publish(message)
        count = len(items)
    app_metrics.record_enqueued(count)
    return count


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
