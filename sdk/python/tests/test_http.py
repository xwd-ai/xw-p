"""标准库传输层：真实起一个本地 HTTP 服务验证收发行为。"""
from __future__ import annotations

import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from xw_notify import ProtocolError, TransportError
from xw_notify._http import Request, Response, UrllibTransport, quote_segment


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # 静音：测试输出里不需要访问日志
        return

    def _write(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            # 超时用例里客户端先断开了，服务端写回失败属于预期，不必冒泡。
            pass

    def do_GET(self) -> None:  # noqa: N802 - 框架要求的命名
        if self.path == "/json":
            self._write(200, '{"status": "ok"}'.encode("utf-8"))
        elif self.path == "/missing":
            self._write(404, '{"detail": "订阅者不存在"}'.encode("utf-8"))
        elif self.path == "/html":
            self._write(200, b"<html>oops</html>", content_type="text/html")
        elif self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/json")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self.path == "/slow":
            time.sleep(0.6)
            self._write(200, b'{"status": "late"}')
        else:
            self._write(404, b'{"detail": "unknown"}')

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        self._write(200, body)


@pytest.fixture()
def server():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    httpd.daemon_threads = True
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def test_get_returns_status_and_body(server):
    response = UrllibTransport(timeout=5).send(Request(method="GET", url=f"{server}/json"))
    assert response.status == 200
    assert response.json() == {"status": "ok"}


def test_error_status_is_returned_not_raised(server):
    response = UrllibTransport(timeout=5).send(Request(method="GET", url=f"{server}/missing"))
    assert response.status == 404
    assert response.json()["detail"] == "订阅者不存在"


def test_redirect_is_not_followed(server):
    response = UrllibTransport(timeout=5).send(Request(method="GET", url=f"{server}/redirect"))
    assert response.status == 302
    assert any(key.lower() == "location" for key in response.headers)


def test_body_is_sent_verbatim(server):
    payload = '{"actor": "小林"}'.encode("utf-8")
    response = UrllibTransport(timeout=5).send(
        Request(
            method="POST",
            url=f"{server}/echo",
            headers={"Content-Type": "application/json"},
            body=payload,
        )
    )
    assert response.body == payload


def test_connection_refused_becomes_transport_error():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    with pytest.raises(TransportError):
        UrllibTransport(timeout=2).send(
            Request(method="GET", url=f"http://127.0.0.1:{port}/json")
        )


def test_timeout_becomes_transport_error(server):
    with pytest.raises(TransportError):
        UrllibTransport(timeout=0.2).send(Request(method="GET", url=f"{server}/slow"))


def test_invalid_timeout_is_rejected():
    with pytest.raises(ValueError, match="timeout"):
        UrllibTransport(timeout=0)


def test_empty_body_is_protocol_error():
    with pytest.raises(ProtocolError, match="响应体为空"):
        Response(status=204, headers={}, body=b"").json()


def test_html_body_is_protocol_error_with_preview():
    with pytest.raises(ProtocolError) as excinfo:
        Response(status=200, headers={}, body=b"<html>oops</html>").json()
    assert "oops" in str(excinfo.value)


def test_preview_truncates_long_body():
    response = Response(status=500, headers={}, body=b"x" * 1000)
    assert len(response.preview()) <= 201


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("creator-1001", "creator-1001"),
        ("creator/1001", "creator%2F1001"),
        ("小林 1", "%E5%B0%8F%E6%9E%97%201"),
        ("a?b=c", "a%3Fb%3Dc"),
    ],
)
def test_quote_segment_escapes_reserved_characters(value, expected):
    assert quote_segment(value) == expected
