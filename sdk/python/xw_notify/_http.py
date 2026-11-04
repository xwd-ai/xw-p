"""HTTP 传输层：请求/响应的最小契约与基于标准库的实现。

分成「契约」与「实现」两半，是因为测试与嵌入场景需要一层可替换的边界：

- 契约只有 :class:`Request` / :class:`Response` / :class:`Transport`，
  都不带任何 urllib 类型，替换实现时不必伪造 urllib 的细节；
- 默认实现 :class:`UrllibTransport` 只用标准库，SDK 因此没有第三方依赖，
  可以原样拷进任何 Python 项目。

传输层**不做状态码判定**：4xx/5xx 也照样返回 :class:`Response`，由客户端
决定抛哪个异常。``urllib`` 默认对 4xx 抛 ``HTTPError``，这里统一折回响应，
否则每个调用点都要写两套错误分支。
"""
from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol
from urllib.parse import quote

from .errors import ProtocolError, TransportError

#: 打印非 JSON 响应时保留的字节数，够看出是 HTML 错误页还是被截断的 JSON。
PREVIEW_LENGTH = 200


def quote_segment(value: Any) -> str:
    """把路径片段编码成可以安全拼进 URL 的形式。

    订阅者标识由调用方提供，可能带空格、斜杠甚至中文；``safe=""`` 让它整体
    作为一个片段转义，否则标识里的 ``/`` 会被当成路径分隔符，请求打到别的
    路由上（表现为莫名其妙的 404）。
    """
    return quote(str(value), safe="")


@dataclass(frozen=True)
class Request:
    """一次待发送的请求。"""

    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    body: Optional[bytes] = None


@dataclass(frozen=True)
class Response:
    """一次已完成的响应，``status`` 原样保留（含 4xx/5xx）。"""

    status: int
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""

    def json(self) -> Any:
        """解析响应体；空体或非法 JSON 抛 :class:`ProtocolError`。

        错误里带上状态码与响应体开头——排查时最先要确认的是「这到底是不是
        我们那个 API 回的」，一段 HTML 预览比任何堆栈都有用。
        """
        if not self.body:
            raise ProtocolError(f"响应体为空（HTTP {self.status}）")
        try:
            return json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise ProtocolError(
                f"响应体不是合法 JSON（HTTP {self.status}）：{self.preview()}"
            ) from exc

    def preview(self) -> str:
        text = self.body.decode("utf-8", errors="replace").strip()
        if len(text) > PREVIEW_LENGTH:
            text = text[:PREVIEW_LENGTH] + "…"
        return text or "(空)"


class Transport(Protocol):
    """传输层契约：拿到 :class:`Request`，返回 :class:`Response`。"""

    def send(self, request: Request) -> Response:
        """发送请求。网络层失败应抛 :class:`TransportError`。"""
        ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """不跟随重定向。

    跟随重定向意味着把 ``X-Api-Key`` 原样再发一次，而目标地址由服务端
    （或被中间人改写过的响应）决定。API 客户端没有理由跨地址搬运凭据，
    因此这里让 urllib 抛 ``HTTPError``，再由上层把 3xx 报成错误。
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102 - 覆写父类
        return None


class UrllibTransport:
    """标准库实现：单次请求、连接不复用。

    ``timeout`` 同时作用于连接与读取（urllib 只提供这一个超时）。SDK 的使用
    场景是「触发一条互动事件」，请求数远小于连接建立的开销，因此不复用连接
    换来的简单性更划算；需要连接池时替换成本类的实现即可。
    """

    def __init__(self, timeout: float = 10.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout 必须大于 0")
        self.timeout = float(timeout)
        self._opener = urllib.request.build_opener(_NoRedirect())

    def send(self, request: Request) -> Response:
        native = urllib.request.Request(
            url=request.url,
            data=request.body,
            headers=dict(request.headers),
            method=request.method,
        )
        try:
            with self._opener.open(native, timeout=self.timeout) as raw:
                return Response(
                    status=raw.status,
                    headers=dict(raw.headers.items()),
                    body=raw.read() or b"",
                )
        except urllib.error.HTTPError as exc:
            # 4xx/5xx/3xx 走这里：读掉响应体交给上层判定，不在这里抛异常。
            body = b""
            try:
                body = exc.read() or b""
            except OSError:  # pragma: no cover - 流已关闭时取不到体
                body = b""
            return Response(
                status=exc.code,
                headers=dict(exc.headers.items()) if exc.headers else {},
                body=body,
            )
        except urllib.error.URLError as exc:
            raise TransportError(f"请求失败：{_reason_text(exc.reason)}") from exc
        except (TimeoutError, socket.timeout) as exc:
            raise TransportError("请求超时") from exc
        except OSError as exc:
            raise TransportError(f"请求失败：{exc}") from exc


def _reason_text(reason: object) -> str:
    """``URLError.reason`` 可能是异常对象，也可能是字符串。"""
    if isinstance(reason, BaseException):
        text = str(reason)
        return text or reason.__class__.__name__
    return str(reason)


__all__ = ["Request", "Response", "Transport", "UrllibTransport", "quote_segment"]
