"""请求级日志上下文：把 ``request id`` 与请求归属放在 ``contextvars`` 里。

日志里要能回答「这一行属于哪次请求、哪个租户、哪个环境」，而这三项都不该靠
每个调用点自己往下传：业务函数多一层参数、漏传一处就少一段线索。``contextvars``
在协程与线程池之间都会随上下文复制，因此同步路由（FastAPI 把它放进线程池执行）
里读到的值与中间件写入的一致，不需要额外处理。

字段只在请求处理期间有值：中间件退出时按 ``token`` 还原，避免上一请求的标识
粘到下一请求的日志上——那种错位比没有标识更难查。
"""
from __future__ import annotations

import contextvars
import re
import uuid
from contextlib import contextmanager
from typing import Iterator, Optional

#: 请求标识头。客户端可以自带，服务端始终回写，便于跨服务串起一次调用。
REQUEST_ID_HEADER = "X-Request-Id"

#: 接受外部传入的标识时只允许这套字符集：它会进响应头与日志，
#: 换行符能伪造出额外的日志行，过长则让每一行都变胖。
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")

#: 租户 / 环境只作为日志字段使用，同样限制字符集与长度。
_LABEL_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")

_request_id: contextvars.ContextVar[str] = contextvars.ContextVar("xw_request_id", default="")
_tenant: contextvars.ContextVar[str] = contextvars.ContextVar("xw_log_tenant", default="")
_environment: contextvars.ContextVar[str] = contextvars.ContextVar("xw_log_environment", default="")


def normalize_request_id(raw: Optional[str]) -> str:
    """把外部传入的请求标识收敛成可安全记录的形态，不合规返回空串。

    不做「清洗后照用」：去掉非法字符可能把两个不同的标识改成同一个，
    重试与重放的日志就分不开了。宁可换一个新标识。
    """
    if not raw:
        return ""
    value = raw.strip()
    return value if _REQUEST_ID_RE.match(value) else ""


def new_request_id() -> str:
    """生成请求标识：前缀标明来源，后缀 16 位足够单机内不重复。"""
    return "req-" + uuid.uuid4().hex[:16]


def _label(raw: Optional[str]) -> str:
    if not raw:
        return ""
    value = raw.strip()
    return value if _LABEL_RE.match(value) else ""


def log_fields() -> dict[str, str]:
    """当前上下文里可用的日志字段，空值不出现（避免每行带一串空串）。"""
    fields = {
        "request_id": _request_id.get(),
        "tenant": _tenant.get(),
        "environment": _environment.get(),
    }
    return {key: value for key, value in fields.items() if value}


@contextmanager
def bind_request(
    request_id: str,
    *,
    tenant: Optional[str] = None,
    environment: Optional[str] = None,
) -> Iterator[str]:
    """在请求处理期间绑定标识字段，退出时还原。

    返回值是最终生效的请求标识：调用方传进来的值可能不合规而被替换，
    响应头与日志必须用同一个值。
    """
    rid = normalize_request_id(request_id) or new_request_id()
    tokens = [
        _request_id.set(rid),
        _tenant.set(_label(tenant)),
        _environment.set(_label(environment)),
    ]
    try:
        yield rid
    finally:
        _environment.reset(tokens[2])
        _tenant.reset(tokens[1])
        _request_id.reset(tokens[0])


__all__ = [
    "REQUEST_ID_HEADER",
    "bind_request",
    "log_fields",
    "new_request_id",
    "normalize_request_id",
]
