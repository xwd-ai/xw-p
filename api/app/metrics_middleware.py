"""HTTP 指标中间件。

注册在限流与 CORS 的内层、路由的外层：限流拒绝的 429 与鉴权失败的 401 都
经过这里，因此「被拒了多少」与「服务端处理了多久」是同一份数据；而 CORS 的
预检请求在这一层之外，不会被算成业务请求。

耗时只在响应开始时量一次：写响应体（尤其是 SSE）的时长属于客户端读取速度，
计入 P99 会把服务端的健康度掩盖成网络状况。需要看完整请求时长时用访问日志
里的 ``duration_ms``，那是另一套口径，两者不混用。
"""
from __future__ import annotations

import time
from typing import Awaitable, Callable, Iterable

from . import app_metrics

Scope = dict
Message = dict
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

#: 不参与统计的路径：探针按秒打、抓取端点自己抓自己，都会把真实流量淹没。
DEFAULT_SKIP_PREFIXES: tuple[str, ...] = ("/health", "/metrics")


class MetricsMiddleware:
    def __init__(
        self,
        app: Callable[[Scope, Receive, Send], Awaitable[None]],
        skip_prefixes: Iterable[str] = DEFAULT_SKIP_PREFIXES,
    ) -> None:
        self.app = app
        self._skip = tuple(skip_prefixes)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path") or ""
        if path.startswith(self._skip):
            await self.app(scope, receive, send)
            return

        method = scope.get("method") or "GET"
        app_metrics.http_in_flight().add(1)
        started = time.perf_counter()
        status = 500  # 未走到响应就异常退出时按 500 计，不留一个「0 状态码」

        async def send_with_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_with_status)
        finally:
            app_metrics.http_in_flight().add(-1)
            # ``scope["route"]`` 由路由在匹配成功后写入，必须等内层返回才读得到；
            # 未匹配时没有这个键，按 unknown 记。
            route = getattr(scope.get("route"), "path", None)
            app_metrics.observe_http(method, route, status, time.perf_counter() - started)
