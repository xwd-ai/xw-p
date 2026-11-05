"""健康检查：存活（liveness）与就绪（readiness）是两件事。

- **存活**只回答「进程还在不在跑」。它不碰任何外部依赖：数据库慢、Redis
  抖动都说明依赖有问题，而不是这个进程该被重启；把依赖塞进存活检查，一次
  依赖故障会演变成整批实例被编排器杀掉，把局部问题放大成全站故障。
- **就绪**回答「现在能不能接流量」。消息总线连不上时，触发请求会在入队那
  一步失败或堆积，此时把实例从负载均衡里摘掉才是对的——它是可恢复的临时
  状态，依赖恢复后实例会自动回到池子里。

每个检查自己负责限时：Redis 客户端带 socket 超时、Postgres 探测用独立的
短连接并设语句超时。这里不做统一的「线程 + 超时」包装——那种写法在依赖
挂死时会不断留下阻塞线程，长期运行比一次探测失败更糟。

返回体里不回显连接串与凭据：把 ``postgresql://user:pass@host`` 原样放进
错误详情，等于把口令写进任何能看到探测结果的地方。
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from . import config
from .bus import MemoryMessageBus, get_bus
from .storage import get_storage

#: 连接串里的口令段：``scheme://user:secret@host`` → ``scheme://***@host``。
_CREDENTIALS = re.compile(r"://[^/\s@]*@")


def scrub(text: str) -> str:
    """去掉错误信息里可能出现的凭据，其余原样保留。"""
    return _CREDENTIALS.sub("://***@", str(text))


@dataclass(frozen=True)
class Check:
    """一次依赖探测的结果。"""

    name: str
    ok: bool
    detail: str
    duration_ms: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": "ok" if self.ok else "fail",
            "detail": self.detail,
            "duration_ms": self.duration_ms,
        }


def _timed(name: str, probe: Callable[[], Optional[str]]) -> Check:
    started = time.perf_counter()
    try:
        detail = probe() or "ok"
        ok = True
    except Exception as exc:  # noqa: BLE001 - 探测失败要如实报告，而不是抛给调用方
        detail = scrub(f"{type(exc).__name__}: {exc}")
        ok = False
    elapsed = (time.perf_counter() - started) * 1000
    return Check(name=name, ok=ok, detail=detail, duration_ms=round(elapsed, 2))


def check_bus() -> Check:
    """消息总线：内存后端恒可用，Redis 后端用 PING 验证。"""

    def probe() -> str:
        bus = get_bus()
        if isinstance(bus, MemoryMessageBus):
            return "memory 后端，无外部依赖"
        ping = getattr(bus, "ping", None)
        if ping is None:
            return "未提供连通性探测"
        # redis-py 在客户端配置了 socket 超时，这里不会无限等待。
        ping()
        return "redis 可达"

    return _timed("bus", probe)


def check_storage() -> Check:
    """持久化后端：文件后端看目录可写，Postgres 执行一次 ``SELECT 1``。"""

    def probe() -> str:
        storage = get_storage()
        ready = getattr(storage, "ready", None)
        if ready is None:
            return "该后端未提供就绪探测"
        return str(ready())

    return _timed("storage", probe)


def check_worker(worker, enabled: bool) -> Check:
    """内置消费线程：只在 ``XW_API_WORKER`` 开启时才有意义。

    线程如果意外退出，队列会一直涨而接口看起来完全正常——这正是就绪检查
    要拦下的情况。
    """

    def probe() -> str:
        if not enabled:
            return "未启用（队列由引擎消费）"
        running = getattr(worker, "is_running", None)
        if callable(running) and not running():
            raise RuntimeError("内置消费线程未在运行")
        return "内置消费线程运行中"

    return _timed("worker", probe)


def run_checks(checks: Iterable[Check]) -> tuple[int, dict[str, Any]]:
    """把检查结果拼成响应；任一失败即 503。"""
    results = list(checks)
    healthy = all(item.ok for item in results)
    body = {
        "status": "ok" if healthy else "degraded",
        "checks": [item.as_dict() for item in results],
    }
    return (200 if healthy else 503), body


def readiness(worker=None) -> tuple[int, dict[str, Any]]:
    """按当前配置跑一遍就绪检查。"""
    return run_checks(
        [
            check_bus(),
            check_storage(),
            check_worker(worker, config.API_WORKER),
        ]
    )


__all__ = [
    "Check",
    "check_bus",
    "check_storage",
    "check_worker",
    "readiness",
    "run_checks",
    "scrub",
]
