"""SDK 的异常层次。

调用方真正要判断的是「接下来怎么办」：换个时间重发、改参数重发、还是把原因
原样展示给人看。所以这里按**处理方式**分层，而不是简单地给每个 HTTP 状态码
包一个类：

- :class:`TransportError` —— 请求没送到服务端（连接被拒、DNS 失败、超时）。
  业务上什么都没发生，退避之后重发是安全的选择。
- :class:`ProtocolError` —— 响应到了，但不是约定的形状（响应体不是 JSON、
  必填字段缺失）。几乎总是客户端与服务端的版本对不上，重发没有意义。
- :class:`APIError` —— 服务端明确拒绝。按状态码再分细类，``message`` 里是
  服务端给出的原因，可以直接放进日志或返回给调用方。
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional

#: 单条错误信息保留的最大长度。FastAPI 的校验错误里可能带 ``input`` 字段，
#: 那份原始请求体对读日志的人没有帮助，截断后只留定位信息。
MAX_MESSAGE_LENGTH = 500


class XWNotifyError(Exception):
    """SDK 所有异常的基类。

    捕获这一个类型即可兜住「调用失败」的全部情况，再按需细分。
    """


class TransportError(XWNotifyError):
    """请求没有到达服务端：连接失败、DNS 解析失败或超时。

    ``reason`` 是底层给出的原因（可能是中文系统错误信息），保留原文便于排查。
    """

    def __init__(self, message: str) -> None:
        self.reason = message
        super().__init__(message)


class ProtocolError(XWNotifyError):
    """服务端响应不符合约定形状。

    常见来源：反向代理返回了 HTML 错误页、服务端版本低于 SDK 预期的接口、
    响应体被中间件改写。这类问题重试无用，需要人看一眼。
    """


class APIError(XWNotifyError):
    """服务端返回了非 2xx 响应。

    - ``status``：HTTP 状态码；
    - ``message``：从响应体里整理出的可读原因；
    - ``method`` / ``path``：出错的请求，日志里不必再自己拼；
    - ``payload``：原始响应体，排查时能拿到 FastAPI 完整的 ``detail``；
    - ``retry_after``：服务端给出的等待秒数（``Retry-After`` 头，没有则 ``None``）；
      是否应该重发由 :func:`xw_notify.retry.should_retry` 按状态码判断。
    """

    def __init__(
        self,
        status: int,
        message: str,
        *,
        method: str = "",
        path: str = "",
        payload: Any = None,
        retry_after: Optional[float] = None,
    ) -> None:
        self.status = status
        self.message = message or "服务端未给出原因"
        self.method = method
        self.path = path
        self.payload = payload
        self.retry_after = retry_after
        super().__init__(self._describe())

    def _describe(self) -> str:
        prefix = " ".join(part for part in (self.method, self.path) if part)
        if prefix:
            return f"{prefix} 返回 {self.status}：{self.message}"
        return f"返回 {self.status}：{self.message}"

    @property
    def detail(self) -> Any:
        """响应体里的 ``detail`` 字段（没有则返回整个响应体）。"""
        if isinstance(self.payload, Mapping) and "detail" in self.payload:
            return self.payload["detail"]
        return self.payload


class BadRequestError(APIError):
    """400：请求本身有问题（未知环境、游标不合法等），改请求才能成功。"""


class AuthenticationError(APIError):
    """401：该环境配置了 API Key，但请求没带或带错。"""


class ForbiddenError(APIError):
    """403：身份没问题，但没有执行该操作的权限。"""


class NotFoundError(APIError):
    """404：目标不存在。

    订阅者被删、工作流未启用、偏好挂在别的环境的订阅者上都落在这一类；
    具体是哪种看 ``message``。
    """


class ConflictError(APIError):
    """409：与既有数据冲突（标识重复、邮箱已被别人绑定）。"""


class ValidationError(APIError):
    """422：字段级校验失败，``message`` 已按字段路径拼好。

    ``errors`` 保留服务端逐个字段给出的原因，表单回填时按字段取用更准。
    """

    @property
    def errors(self) -> list[Mapping[str, Any]]:
        detail = self.detail
        if isinstance(detail, list):
            return [item for item in detail if isinstance(item, Mapping)]
        return []


class RateLimitError(APIError):
    """429：触发限流，退避后重试。"""


class ServerError(APIError):
    """5xx：服务端内部错误，通常可重试。"""


_STATUS_ERRORS: dict[int, type[APIError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
    409: ConflictError,
    422: ValidationError,
    429: RateLimitError,
}


def _format_location(loc: Any) -> str:
    """把 FastAPI 的 ``loc`` 拼成字段路径。

    ``loc`` 的首项是取值位置（``body`` / ``query`` / ``path`` / ``header``），
    对调用方来说是冗余的——消息里已经说了「这个请求的某个字段有问题」；
    去掉它之后 ``to.email`` 这样的路径本身就够定位了。
    """
    if not isinstance(loc, (list, tuple)):
        return ""
    parts = [str(item) for item in loc]
    if parts and parts[0] in ("body", "query", "path", "header", "cookie"):
        parts = parts[1:]
    return ".".join(parts)


def _describe_item(item: Any) -> str:
    if isinstance(item, Mapping):
        text = item.get("msg") or item.get("message") or item.get("detail")
        if isinstance(text, str) and text.strip():
            where = _format_location(item.get("loc"))
            return f"{where}: {text.strip()}" if where else text.strip()
    return describe_detail(item)


def _compact(value: Any) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        text = str(value)
    if len(text) > MAX_MESSAGE_LENGTH:
        text = text[:MAX_MESSAGE_LENGTH] + "…"
    return text


def describe_detail(detail: Any) -> str:
    """把 ``detail`` 折成一句可读的原因。

    FastAPI 的 ``detail`` 有两种形态：业务代码抛出的字符串，以及请求模型校验
    失败时的对象数组。直接把后者 ``str()`` 出来是一坨带 ``input`` 的字典，
    读日志的人还得自己去对应字段，所以这里按字段路径拼。
    """
    if detail is None:
        return ""
    if isinstance(detail, str):
        return detail.strip()
    if isinstance(detail, Mapping):
        if not detail:
            return ""
        for key in ("msg", "message", "detail"):
            value = detail.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return _compact(detail)
    if isinstance(detail, (list, tuple)):
        if not detail:
            return ""
        parts = [piece for piece in (_describe_item(item) for item in detail) if piece]
        return "；".join(parts) if parts else _compact(detail)
    return str(detail).strip()


def error_for_status(
    status: int,
    payload: Any,
    *,
    method: str = "",
    path: str = "",
    reason: Optional[str] = None,
    retry_after: Optional[float] = None,
) -> APIError:
    """按状态码与响应体构造异常实例。"""
    detail = payload.get("detail") if isinstance(payload, Mapping) else None
    message = describe_detail(detail) or describe_detail(payload) or (reason or "")
    cls = _STATUS_ERRORS.get(status)
    if cls is None:
        cls = ServerError if status >= 500 else APIError
    return cls(
        status,
        message,
        method=method,
        path=path,
        payload=payload,
        retry_after=retry_after,
    )


__all__ = [
    "APIError",
    "AuthenticationError",
    "BadRequestError",
    "ConflictError",
    "ForbiddenError",
    "NotFoundError",
    "ProtocolError",
    "RateLimitError",
    "ServerError",
    "TransportError",
    "ValidationError",
    "XWNotifyError",
    "describe_detail",
    "error_for_status",
]
