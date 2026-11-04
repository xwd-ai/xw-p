"""客户端主体：地址拼装、请求头、状态码判定与响应解析。

三件事集中在这里做，是为了让资源层（``trigger`` / ``subscribers`` /
``preferences``）只剩「拼路径 + 取字段」：

- **请求头**。环境标识始终显式带上——不写就落到服务端的默认环境，运维切了
  环境却以为在看另一套数据，是这类 SDK 最常见的事故；API Key 只在配置了
  才带，避免把空值发出去被当成「带了一个错 Key」。
- **状态码**。传输层把 4xx/5xx 也当普通响应对待，判定在这里统一做：2xx 才
  解析，3xx 单独报错（详见 :class:`~xw_notify._http.UrllibTransport` 里
  不跟随重定向的原因），其余按状态码映射成异常。
- **响应体**。空体（204）返回 ``None``；非 JSON 抛
  :class:`~xw_notify.errors.ProtocolError`，错误里带响应体开头，能一眼看出
  是不是反向代理返回的 HTML 错误页。

请求体用 ``json.dumps`` 而不是表单编码：接口只收 JSON，且 ``ensure_ascii``
关掉之后日志与抓包里能直接读到中文正文。
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional
from urllib.parse import urlencode

from ._http import Request, Response, Transport, UrllibTransport, quote_segment
from .errors import APIError, ProtocolError, error_for_status
from .models import Health

#: 服务端默认监听地址，与 README 里的启动命令一致。
DEFAULT_BASE_URL = "http://127.0.0.1:8000"

#: 服务端默认环境。SDK 显式发送同名请求头，不依赖服务端配置。
DEFAULT_ENVIRONMENT = "development"

DEFAULT_TIMEOUT = 10.0


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        # FastAPI 的布尔查询参数接受 true/false；Python 的 True/False 也能收，
        # 但大小写敏感的实现（其它服务端）会认不出来，统一发小写。
        return "true" if value else "false"
    return str(value)


def _header(headers: Mapping[str, str], name: str) -> Optional[str]:
    """按名称取响应头（HTTP 头大小写不敏感，字典不一定）。"""
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


class Client:
    """xw-notify 的 Python 客户端。

    典型用法::

        client = Client("http://127.0.0.1:8000", api_key="...", environment="production")
        ack = client.trigger.send(
            "post_commented", "creator-1001",
            payload={"post_id": "P-2026-1001", "actor": "小林", "comment": "求教程"},
            channels=["in_app", "email"],
        )

    资源挂在实例上：``client.trigger``、``client.subscribers``、
    ``client.subscribers.preferences``。方法都是同步阻塞的，一次调用一次请求。

    :param base_url: API 根地址，例如 ``http://127.0.0.1:8000``；路径前缀由
        SDK 拼接，这里不要带 ``/v1``。
    :param api_key: 该环境的 API Key。服务端未配置 Key 的环境（本地开发默认
        如此）可以不传。
    :param environment: 隔离环境，``development`` 或 ``production``。
    :param timeout: 连接与读取超时（秒）。仅在未注入 ``transport`` 时生效。
    :param transport: 自定义传输层，用于替换 HTTP 实现或在测试里注入假响应。
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: Optional[str] = None,
        environment: str = DEFAULT_ENVIRONMENT,
        timeout: float = DEFAULT_TIMEOUT,
        transport: Optional[Transport] = None,
    ) -> None:
        text = str(base_url or "").strip()
        if not text:
            raise ValueError("base_url 不能为空")
        if not text.lower().startswith(("http://", "https://")):
            # 少写协议头时 urllib 会报 "unknown url type"，那句话看不出是哪里错。
            raise ValueError(f"base_url 需要以 http:// 或 https:// 开头：{base_url}")
        self.base_url = text.rstrip("/")
        self.api_key = (api_key or "").strip() or None
        self.environment = str(environment or DEFAULT_ENVIRONMENT).strip() or DEFAULT_ENVIRONMENT
        self.timeout = float(timeout)
        self._transport: Transport = transport or UrllibTransport(timeout=self.timeout)

        # 资源在构造时装配：调用方不必记「先取资源再调方法」的顺序。
        from .preferences import PreferencesResource
        from .subscribers import SubscribersResource
        from .trigger import TriggerResource

        self.trigger = TriggerResource(self)
        self.subscribers = SubscribersResource(self, preferences=PreferencesResource(self))

    # -- 底层请求 ---------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        query: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """发一次请求并返回解析后的响应体（空体返回 ``None``）。

        这是 SDK 未封装的接口（收件箱、管理统计、死信运维等）的兜底入口::

            data = client.request("GET", "/v1/inbox", query={"subscriberId": "creator-1001"})

        参数名用 ``json_body`` 而不是 ``json``，是为了不和 ``json`` 模块同名。
        """
        request = Request(
            method=method.upper(),
            url=self._build_url(path, query),
            headers=self._build_headers(json_body is not None, headers),
            body=self._encode(json_body),
        )
        return self._handle(method.upper(), path, self._transport.send(request))

    def health(self) -> Health:
        """读一次 ``GET /health``，可用于启动时探活。"""
        return Health.from_payload(self.request("GET", "/health"))

    # -- 生命周期 ---------------------------------------------------------

    def close(self) -> None:
        """释放资源。当前实现无长连接，保留该方法以便替换传输层时语义一致。"""
        return None

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        key = "已配置" if self.api_key else "未配置"
        return (
            f"Client(base_url={self.base_url!r}, environment={self.environment!r}, "
            f"api_key={key})"
        )

    # -- 内部实现 ---------------------------------------------------------

    def _build_url(self, path: str, query: Optional[Mapping[str, Any]]) -> str:
        suffix = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{suffix}"
        if query:
            # 值为 None 的参数整体略过：调用方用 None 表示「不筛这一维」，
            # 发一个空串过去反而会变成一条假的筛选条件。
            pairs = [(key, _query_value(value)) for key, value in query.items() if value is not None]
            if pairs:
                url = f"{url}?{urlencode(pairs)}"
        return url

    def _build_headers(
        self, has_body: bool, extra: Optional[Mapping[str, str]]
    ) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "X-Environment": self.environment,
            "User-Agent": f"xw-notify-python/{_version()}",
        }
        if has_body:
            headers["Content-Type"] = "application/json; charset=utf-8"
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        if extra:
            headers.update({str(k): str(v) for k, v in extra.items() if v is not None})
        return headers

    def _encode(self, body: Any) -> Optional[bytes]:
        if body is None:
            return None
        return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def _handle(self, method: str, path: str, response: Response) -> Any:
        if 300 <= response.status < 400:
            location = _header(response.headers, "Location") or "(未给出 Location)"
            # 不跟随重定向，也就不可能自动把 API Key 发到新地址去（见 _http 模块）。
            raise APIError(
                response.status,
                f"服务端要求重定向到 {location}；SDK 不跟随重定向，请确认 base_url 是否写对",
                method=method,
                path=path,
                payload=response.preview(),
            )
        if not 200 <= response.status < 300:
            raise error_for_status(
                response.status,
                _error_payload(response),
                method=method,
                path=path,
            )
        if not response.body:
            return None
        return response.json()


def _error_payload(response: Response) -> Any:
    """非 2xx 响应体：能解析成 JSON 就用结构化结果，否则给一段预览文本。"""
    try:
        return response.json()
    except ProtocolError:
        return response.preview()


def _version() -> str:
    from . import __version__

    return __version__


__all__ = ["Client", "DEFAULT_BASE_URL", "DEFAULT_ENVIRONMENT", "quote_segment"]
