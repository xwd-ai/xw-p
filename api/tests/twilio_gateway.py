"""测试用的进程内短信网关，实现 Messages 资源的最小可用面。

只覆盖客户端行为需要的部分：接收 ``POST …/Messages.json``、校验 Basic 认证头
是否存在、把表单字段交给用例断言、按预设返回 JSON 响应。不做真实下发、状态
回调与分页，也不校验业务字段——那些属于网关自己的规则。

响应可以按顺序排队：第一轮成功、第二轮失败这类场景不必重启服务器。
"""
from __future__ import annotations

import json
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class RecordedRequest:
    """一次到达网关的请求；字段与客户端断言直接对应。"""

    method: str
    path: str
    headers: Mapping[str, str]
    form: Mapping[str, str]

    @property
    def authorization(self) -> str:
        return self.headers.get("authorization", "")


@dataclass(frozen=True)
class CannedResponse:
    """预设响应。``body`` 会被 JSON 序列化，除非它是字符串。"""

    status: int = 201
    body: Any = field(default_factory=dict)
    content_type: str = "application/json"
    headers: Mapping[str, str] = field(default_factory=dict)

    def payload(self) -> bytes:
        if isinstance(self.body, str):
            return self.body.encode("utf-8")
        return json.dumps(self.body, ensure_ascii=False).encode("utf-8")


def accepted(sid: str = "SM0f1c9a2e5b7d4c8e", status: str = "queued", segments: int = 1) -> CannedResponse:
    """网关受理一条短信时的响应。"""
    return CannedResponse(
        status=201,
        body={"sid": sid, "status": status, "num_segments": str(segments), "error_code": None},
    )


def rejected(code: int, message: str, status: int = 400) -> CannedResponse:
    """网关拒绝请求时的错误资源。"""
    return CannedResponse(status=status, body={"code": code, "message": message, "status": status})


class FakeTwilioGateway:
    """监听在回环地址上的最小 Messages 端点。"""

    def __init__(
        self,
        *,
        responses: Optional[list[CannedResponse]] = None,
        default: Optional[CannedResponse] = None,
        delay: float = 0.0,
    ) -> None:
        self.delay = delay
        self.requests: list[RecordedRequest] = []
        self._responses = list(responses or [])
        self._default = default or accepted()
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(self))
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="fake-twilio", daemon=True)

    # ---- 生命周期 ----

    @property
    def endpoint(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> "FakeTwilioGateway":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> "FakeTwilioGateway":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # ---- 供 handler 调用 ----

    def record(self, request: RecordedRequest) -> CannedResponse:
        with self._lock:
            self.requests.append(request)
            response = self._responses.pop(0) if self._responses else self._default
        if self.delay:
            time.sleep(self.delay)
        return response

    @property
    def count(self) -> int:
        with self._lock:
            return len(self.requests)


def _handler_factory(gateway: FakeTwilioGateway) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler 约定
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length)
            form = dict(urllib.parse.parse_qsl(raw.decode("utf-8"), keep_blank_values=True))
            record = RecordedRequest(
                method="POST",
                path=self.path,
                headers={key.lower(): value for key, value in self.headers.items()},
                form=form,
            )
            response = gateway.record(record)
            payload = response.payload()
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(payload)))
            for key, value in response.headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args: object) -> None:  # 静默，避免测试输出被刷屏
            return

    return Handler


__all__ = ["CannedResponse", "FakeTwilioGateway", "RecordedRequest", "accepted", "rejected"]
