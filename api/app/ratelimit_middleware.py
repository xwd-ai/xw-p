"""限流的 ASGI 中间件：在最外层判定，超出额度时直接回 429。

放在中间件而不是每个路由的依赖里，是因为要保护的是**所有** ``/v1`` 接口：
逐个路由挂依赖总有漏一个的时候，而漏掉的那个恰好是攻击者最爱打的写接口。
判定必须在解析请求体之前完成——限流如果先让服务把 10 MB 的 payload 反序列化
一遍再拒绝，保护的就只有下游而不是自己。

响应头在 ``http.response.start`` 上补齐，因此被限流的是后续请求时，正常响应
里也能看到剩余额度；拒绝时返回与其它错误一致的 ``{"detail": ...}`` 形状。
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

from .ratelimit import Decision, RateLimiter, client_ip, get_limiter

logger = logging.getLogger("xw.ratelimit")

#: 受限路径前缀：业务接口都在 ``/v1`` 下，``/health`` 与文档端点不受限——
#: 负载均衡器的健康探测被自己的限流挡住，表现是「服务正常但一直被摘掉」。
LIMITED_PREFIX = "/v1/"

#: 拒绝日志的最小间隔（秒）。一个来源打爆服务时，每个被拒请求都写一行日志，
#: 日志本身会成为第二个瓶颈。
_REJECT_LOG_INTERVAL = 5.0


class RateLimitMiddleware:
    """按凭据 / IP 消耗令牌，放行时补额度响应头，拒绝时短路。"""

    def __init__(
        self,
        app,
        limiter: Optional[RateLimiter] = None,
        prefix: str = LIMITED_PREFIX,
    ) -> None:
        self.app = app
        self._limiter = limiter
        self._prefix = prefix
        self._lock = threading.Lock()
        self._last_log = 0.0
        self._suppressed = 0

    async def __call__(self, scope, receive, send) -> None:
        if not self._applies(scope):
            await self.app(scope, receive, send)
            return
        limiter = self._limiter or get_limiter()
        if not limiter.enabled:
            await self.app(scope, receive, send)
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        address = client_ip(scope, headers, trust_forwarded=limiter.trust_forwarded)
        # 令牌桶可能走 Redis：同步客户端不能占着事件循环等网络。
        decision = await run_in_threadpool(limiter.check, headers, address)
        if decision is None:
            await self.app(scope, receive, send)
            return

        extra = decision.headers()
        if not decision.allowed:
            self._log_rejection(decision)
            response = JSONResponse(
                {"detail": decision.message()}, status_code=429, headers=extra
            )
            await response(scope, receive, send)
            return

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                existing = {name.lower() for name, _ in message.get("headers", [])}
                prepared = list(message.get("headers", []))
                for name, value in extra.items():
                    if name.lower().encode("latin-1") not in existing:
                        prepared.append((name.lower().encode("latin-1"), value.encode("latin-1")))
                message = {**message, "headers": prepared}
            await send(message)

        await self.app(scope, receive, send_with_headers)

    def _applies(self, scope) -> bool:
        if scope.get("type") != "http":
            return False
        # 预检请求不带凭据、也不该消耗额度：浏览器对每个跨源写请求都会先发一次
        # OPTIONS，把它计入限额等于让 CORS 本身吃掉一半额度。
        if (scope.get("method") or "").upper() == "OPTIONS":
            return False
        return (scope.get("path") or "").startswith(self._prefix)

    def _log_rejection(self, decision: Decision) -> None:
        now = time.monotonic()
        with self._lock:
            if now - self._last_log < _REJECT_LOG_INTERVAL:
                self._suppressed += 1
                return
            suppressed, self._suppressed = self._suppressed, 0
            self._last_log = now
        logger.warning(
            "限流拒绝：%s（%s %d 次/分钟，等待 %.1fs，剩余 %d）；此前 %d 次拒绝已合并",
            decision.identity.label,
            decision.identity.dimension,
            decision.identity.quota.rpm,
            decision.retry_after,
            max(0, decision.remaining),
            suppressed,
        )


__all__ = ["LIMITED_PREFIX", "RateLimitMiddleware"]
