"""访问日志与请求标识中间件。

一次请求只写一行访问日志，字段是 ``method`` / ``path`` / ``status`` /
``duration_ms`` / ``client``，请求标识、租户与环境由日志上下文补齐。之所以不用
uvicorn 自带的 access log：那一层拿不到我们解析出的租户与请求标识，也没法把
「响应头里回写同一个标识」和日志放在一处维护。

中间件注册在最外层（``main.py`` 里最后添加）：限流拒绝、鉴权失败、未捕获异常
都要带上请求标识，只有包住全部内层才能做到。响应头在 ``http.response.start``
上补齐，客户端因此能从任何一个响应（包括 4xx/5xx）里拿到本次请求的标识，
报障时把它贴过来即可定位。

未捕获异常在这里就地转成 500，而不是往上抛给 Starlette 的
``ServerErrorMiddleware``：那一层在我们的中间件之外，它生成的错误响应不经过
这里的 ``send``，响应头就补不上了。「客户端拿到的每个响应都带标识」与
「日志里有完整堆栈」只能二选一的话，两者其实都要——所以处理方式是自己回一个
带标识的 500，同时在访问日志里按 ERROR 级别记录 ``exc_info``，堆栈一点都不少。
已经发过响应头的流式请求无法再改写，此时照常向上抛。

``/health`` 不写访问日志：负载均衡与容器编排的探针按秒级频率打，日志会被这类
请求淹没，真正需要排查的请求反而查不到。探针失败会以非 200 出现在监控里，
不会因为少了这一行日志而看不见。
"""
from __future__ import annotations

import logging
import time
from typing import Iterable, Optional

from starlette.responses import JSONResponse

from .log_context import REQUEST_ID_HEADER, bind_request, normalize_request_id

logger = logging.getLogger("xw.access")

#: 不写访问日志的路径前缀（健康探针）。
SKIP_PREFIXES = ("/health",)

#: 路径落盘前的截断长度：超长 URL 会把一行日志撑到几 KB，采集端按行读取时
#: 反而更难处理。
_MAX_PATH = 512


class RequestLogMiddleware:
    """绑定请求标识、回写响应头，并在请求结束时落一行访问日志。"""

    def __init__(
        self,
        app,
        *,
        access_log: bool = True,
        skip_prefixes: Iterable[str] = SKIP_PREFIXES,
    ) -> None:
        self.app = app
        self.access_log = access_log
        self.skip_prefixes = tuple(skip_prefixes)

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        # 未带或不合规时换一个新标识：回写的头必须与日志里的一致，
        # 否则客户端拿着头里的值查不到任何记录。
        rid = normalize_request_id(headers.get("x-request-id"))
        started = time.perf_counter()
        status = 500  # 响应未开始就异常退出时按 500 记，不留一个「0 状态码」
        failure: Optional[BaseException] = None
        started_sending = False

        with bind_request(
            rid,
            tenant=headers.get("x-tenant-id"),
            environment=headers.get("x-environment"),
        ) as request_id:
            async def send_with_id(message):
                nonlocal status, started_sending
                if message["type"] == "http.response.start":
                    status = message["status"]
                    started_sending = True
                    message = {
                        **message,
                        "headers": _with_request_id(message.get("headers", []), request_id),
                    }
                await send(message)

            try:
                await self.app(scope, receive, send_with_id)
            except Exception as exc:  # noqa: BLE001 - 记完再决定怎么回
                failure = exc
                # 已经发过响应头（流式输出到一半）就没法再补一个 500；
                # 非 ``Exception`` 的退出信号（取消、KeyboardInterrupt）也不该吞掉。
                if started_sending:
                    raise
                await _internal_error(scope, receive, send_with_id, request_id)
            finally:
                if self.access_log and self._should_log(scope):
                    self._record(scope, status, started, failure)

    def _should_log(self, scope) -> bool:
        path = scope.get("path") or ""
        return not any(path.startswith(prefix) for prefix in self.skip_prefixes)

    def _record(self, scope, status: int, started: float, failure: Optional[BaseException]) -> None:
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        method = (scope.get("method") or "").upper()
        path = (scope.get("path") or "")[:_MAX_PATH]
        client = scope.get("client")
        fields = {
            "event": "http.request",
            "method": method,
            "path": path,
            "status": status,
            "duration_ms": duration_ms,
        }
        if client:
            fields["client"] = client[0]

        # 级别跟着状态码：4xx 是调用方问题、5xx 是本服务问题，采集端按级别分流
        # 告警比对每个状态码写规则省事。
        if status >= 500 or failure is not None:
            logger.error("请求失败 %s %s -> %d（%.1f ms）", method, path, status, duration_ms,
                         extra=fields, exc_info=failure)
        elif status >= 400:
            logger.warning("请求被拒 %s %s -> %d（%.1f ms）", method, path, status, duration_ms,
                           extra=fields)
        else:
            logger.info("请求完成 %s %s -> %d（%.1f ms）", method, path, status, duration_ms,
                        extra=fields)


async def _internal_error(scope, receive, send, request_id: str) -> None:
    """未捕获异常时回一个带请求标识的 500。

    响应体只给一句结论加标识，不暴露异常文案：异常消息里常带内部路径、
    SQL 片段或第三方返回，它是排查线索而不是给调用方看的东西。
    """
    response = JSONResponse(
        {"detail": "服务器内部错误", "requestId": request_id},
        status_code=500,
    )
    await response(scope, receive, send)


def _with_request_id(headers, request_id: str):
    """在响应头上补 ``X-Request-Id``，已有同名头时不重复添加。"""
    if not request_id:
        return headers
    for name, _value in headers:
        if name.decode("latin-1").lower() == REQUEST_ID_HEADER.lower():
            return headers
    return list(headers) + [
        (REQUEST_ID_HEADER.lower().encode("latin-1"), request_id.encode("latin-1"))
    ]


__all__ = ["RequestLogMiddleware", "SKIP_PREFIXES"]
