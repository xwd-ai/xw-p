"""测试用的传输层替身：记录请求、按需返回响应。

用假传输而不是起一个 HTTP 服务，是为了让「客户端发了什么」可以被逐字节断言
——请求头、路径编码、请求体字段顺序这些正是 SDK 的产出，绕过它们去断言
「服务端收到了什么」等于把 SDK 的行为交给服务端解释。真实网络交互另有一组
用例（``test_http.py``）专门覆盖标准库实现。
"""
from __future__ import annotations

import json
from typing import Any, Callable, Optional

from xw_notify import Client
from xw_notify._http import Request, Response

Responder = Callable[[Request], Response]


def json_response(status: int, payload: Any) -> Response:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return Response(
        status=status,
        headers={"Content-Type": "application/json"},
        body=body,
    )


def text_response(status: int, text: str) -> Response:
    return Response(
        status=status,
        headers={"Content-Type": "text/html; charset=utf-8"},
        body=text.encode("utf-8"),
    )


def empty_response(status: int = 204) -> Response:
    return Response(status=status, headers={}, body=b"")


class FakeTransport:
    """按 responder 返回响应，并把每次请求留在 ``requests`` 里。"""

    def __init__(self, responder: Optional[Responder] = None) -> None:
        self.requests: list[Request] = []
        self._responder: Responder = responder or (lambda request: json_response(200, {}))

    def send(self, request: Request) -> Response:
        self.requests.append(request)
        return self._responder(request)

    @property
    def last(self) -> Request:
        if not self.requests:
            raise AssertionError("没有发出任何请求")
        return self.requests[-1]

    def last_json(self) -> Any:
        body = self.last.body
        if body is None:
            raise AssertionError("最后一个请求没有请求体")
        return json.loads(body.decode("utf-8"))


def make_client(responder: Optional[Responder] = None, **kwargs: Any) -> tuple[Client, FakeTransport]:
    """构造一个指向假地址的客户端，返回 (client, transport)。"""
    transport = FakeTransport(responder)
    kwargs.setdefault("base_url", "http://api.test")
    return Client(transport=transport, **kwargs), transport
