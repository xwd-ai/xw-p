"""Twilio 风格的短信网关接口：Messages 资源契约与渠道实现。

「Twilio 风格」指下面这套最小报文约定，凡是兼容它的短信网关（含自建的测试
替身）都能直接换上，不需要改调用方：

===================  ==========================================================
端点                  ``POST {endpoint}/2010-04-01/Accounts/{account_sid}/Messages.json``
认证                  ``Authorization: Basic base64(account_sid:auth_token)``
请求体                ``application/x-www-form-urlencoded``，字段 ``To`` / ``From`` /
                      ``Body``，可选 ``StatusCallback``
成功响应              ``2xx`` + JSON 消息资源（``sid`` / ``status`` / ``num_segments`` …）
失败响应              ``4xx`` / ``5xx`` + JSON 错误资源（``code`` / ``message``）
===================  ==========================================================

配置项与引擎侧共用同一份 ``XW_PROVIDERS_FILE``：

======================  ==================================================
``account_sid``         账户标识，``AC`` 开头的长串（必填）
``auth_token``          账户口令（必填，不进日志）
``from``                发送方：E.164 号码，或至多 11 位字母数字发送方标识
``endpoint``            网关地址，缺省 ``https://api.twilio.com``
``default_country_code`` 号码没写国际区号时补上的国家码，如 ``86``
``status_callback``     可选，投递回执回调地址
``timeout``             单次请求超时（秒），缺省 10
======================  ==================================================

投递失败不抛异常：号码不合法、网关拒绝、限流、超时都表达成
``DeliveryResult.failed`` 并带上原因，调用方把它写进执行明细。状态码到原因的
映射按「运维看到这句话知道下一步查什么」来写，而不是照抄网关原文。
"""
from __future__ import annotations

import base64
import json
import logging
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from ..models.channel import Channel
from .base import Delivery, DeliveryResult, Provider
from .sms import PhoneNumberError, country_code, normalize_phone, sms_segments, to_e164

logger = logging.getLogger("xw.provider.twilio")

#: Messages 资源的接口版本，与网关约定的路径一起构成端点。
API_VERSION = "2010-04-01"

#: 缺省网关地址；测试与私有部署用 ``endpoint`` 覆盖。
DEFAULT_ENDPOINT = "https://api.twilio.com"

#: 单次请求的缺省超时（秒）。
DEFAULT_TIMEOUT = 10.0

#: 单条短信正文长度上限（网关超长会以 21617 拒收，这里提前挡住）。
MAX_BODY_LENGTH = 1600

#: 正文为空时的兜底文案。
#:
#: 引擎路径上正文由 render 阶段产出，不会为空；API 内置消费者不渲染模板，
#: 走到这里时正文是空串。空正文会被网关以 21602 拒收，因此给一句通用文案——
#: 与邮件实现缺标题时的兜底主题同一思路，宁可收到一句平淡的提醒，也不要整条
#: 通知因为「没渲染」而静默失败。
DEFAULT_BODY = "你有一条新的互动提醒"

#: 表示「已受理/在途/已送达」的状态，都属于投递成功。
SUCCESS_STATUSES = frozenset({"queued", "accepted", "scheduled", "sending", "sent", "delivered"})

#: 表示投递最终失败的状态。
FAILURE_STATUSES = frozenset({"failed", "undelivered", "canceled"})

#: 请求头里的客户端标识，便于网关侧定位来源。
USER_AGENT = "xw-notify-sms/1.0"

#: 字母数字发送方标识的最大长度。
_MAX_SENDER_LABEL = 11


class TwilioConfigError(ValueError):
    """短信网关配置不合法（账户标识、口令、发送方或端点）。"""


class GatewayError(Exception):
    """请求没走到网关：连接失败、DNS 失败或超时。"""


def _split_endpoint(raw: object) -> str:
    """校验端点并去掉结尾斜杠；只接受 http/https 且不带查询串。"""
    text = str(raw or "").strip() or DEFAULT_ENDPOINT
    parts = urllib.parse.urlsplit(text)
    if parts.scheme not in ("http", "https"):
        raise TwilioConfigError(f"endpoint 必须是 http:// 或 https:// 地址，收到 {text!r}")
    if not parts.hostname:
        raise TwilioConfigError(f"endpoint 缺少主机名：{text!r}")
    if parts.query or parts.fragment:
        raise TwilioConfigError(f"endpoint 不能带查询串或片段：{text!r}")
    path = parts.path.rstrip("/")
    return f"{parts.scheme}://{parts.netloc}{path}"


def _check_account_sid(raw: object) -> str:
    """账户标识：``AC`` 开头、至少 10 位的字母数字串。"""
    text = str(raw or "").strip()
    if not text:
        raise TwilioConfigError("缺少 account_sid")
    if not text.startswith("AC"):
        raise TwilioConfigError(f"account_sid 应以 AC 开头，收到 {text!r}")
    if len(text) < 10 or not text.isalnum():
        raise TwilioConfigError(f"account_sid 格式不合法（AC 加至少 8 位字母数字）：{text!r}")
    return text


def _check_sender(raw: object) -> str:
    """发送方：E.164 号码，或至多 11 位的字母数字发送方标识。"""
    text = str(raw or "").strip()
    if not text:
        raise TwilioConfigError("缺少 from（发送方号码或发送方标识）")
    if text.startswith("+"):
        if normalize_phone(text) is None:
            raise TwilioConfigError(f"from 不是合法的 E.164 号码：{text!r}")
        return text.replace(" ", "")
    if len(text) > _MAX_SENDER_LABEL:
        raise TwilioConfigError(
            f"from 作为发送方标识最多 {_MAX_SENDER_LABEL} 位，收到 {len(text)} 位：{text!r}"
        )
    if not all(char.isalnum() or char in " -_" for char in text) or not text[0].isalnum():
        raise TwilioConfigError(f"from 含不允许的字符（只允许字母、数字、空格、- 与 _）：{text!r}")
    return text


@dataclass(frozen=True)
class TwilioConfig:
    """一份已校验的短信网关配置。"""

    account_sid: str
    auth_token: str
    sender: str
    endpoint: str = DEFAULT_ENDPOINT
    default_country_code: str = ""
    status_callback: str = ""
    timeout: float = DEFAULT_TIMEOUT

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_sid", _check_account_sid(self.account_sid))
        token = str(self.auth_token or "")
        if not token.strip():
            raise TwilioConfigError("缺少 auth_token")
        if "\n" in token or "\r" in token:
            raise TwilioConfigError("auth_token 不能包含换行符")
        object.__setattr__(self, "auth_token", token)
        object.__setattr__(self, "sender", _check_sender(self.sender))
        object.__setattr__(self, "endpoint", _split_endpoint(self.endpoint))

        try:
            object.__setattr__(self, "default_country_code", country_code(self.default_country_code))
        except PhoneNumberError as exc:
            raise TwilioConfigError(str(exc)) from exc

        callback = str(self.status_callback or "").strip()
        if callback:
            parts = urllib.parse.urlsplit(callback)
            if parts.scheme not in ("http", "https") or not parts.hostname:
                raise TwilioConfigError(f"status_callback 必须是 http(s) 地址：{callback!r}")
        object.__setattr__(self, "status_callback", callback)

        timeout = self.timeout
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            raise TwilioConfigError(f"timeout 必须是大于 0 的秒数，收到 {timeout!r}")
        object.__setattr__(self, "timeout", float(timeout))

    @property
    def authenticates_with(self) -> str:
        """该配置对应的请求头值；只在拼请求时用，不写日志。"""
        token = base64.b64encode(f"{self.account_sid}:{self.auth_token}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"

    @classmethod
    def from_credentials(
        cls,
        credentials: Mapping[str, Any],
        *,
        timeout: Optional[float] = None,
    ) -> "TwilioConfig":
        """按渠道配置里的 ``credentials`` 构造；``timeout`` 参数可覆盖配置项。"""
        raw_timeout = credentials.get("timeout", "")
        resolved = timeout
        if resolved is None:
            text = str(raw_timeout or "").strip()
            if text:
                try:
                    resolved = float(text)
                except ValueError as exc:
                    raise TwilioConfigError(f"timeout 必须是数字，收到 {text!r}") from exc
            else:
                resolved = DEFAULT_TIMEOUT
        return cls(
            account_sid=str(credentials.get("account_sid", "")),
            auth_token=str(credentials.get("auth_token", "")),
            sender=str(credentials.get("from", "")),
            endpoint=str(credentials.get("endpoint", "") or DEFAULT_ENDPOINT),
            default_country_code=str(credentials.get("default_country_code", "")),
            status_callback=str(credentials.get("status_callback", "")),
            timeout=resolved,
        )


@dataclass(frozen=True)
class MessageRequest:
    """一条待提交的短信：Messages 资源的请求侧形态。"""

    to: str
    sender: str
    body: str
    status_callback: str = ""

    def form(self) -> list[tuple[str, str]]:
        """请求体字段；顺序固定，便于网关侧日志与用例断言。"""
        fields = [("To", self.to), ("From", self.sender), ("Body", self.body)]
        if self.status_callback:
            fields.append(("StatusCallback", self.status_callback))
        return fields

    def path(self, account_sid: str) -> str:
        return f"/{API_VERSION}/Accounts/{account_sid}/Messages.json"


@dataclass(frozen=True)
class MessageResponse:
    """网关返回的消息资源；只取调用方用得到的字段。"""

    sid: str = ""
    status: str = ""
    error_code: Optional[int] = None
    error_message: str = ""
    segments: int = 0

    @property
    def accepted(self) -> bool:
        """网关是否已受理这条短信（含在途与已送达）。"""
        return self.status in SUCCESS_STATUSES

    @property
    def failed(self) -> bool:
        """是否为最终失败状态，或带着错误码。"""
        return self.status in FAILURE_STATUSES or self.error_code is not None

    def failure_reason(self) -> str:
        code = f"（{self.error_code}）" if self.error_code else ""
        detail = self.error_message or "网关未给出原因"
        return f"短信网关返回 {self.status or '未知状态'}{code}：{detail}"


@dataclass(frozen=True)
class _HttpResponse:
    status: int
    payload: Any
    headers: Mapping[str, str] = field(default_factory=dict)


def _as_int(value: object) -> Optional[int]:
    """把网关返回的数字（可能是字符串）转成 int；转不了返回 ``None``。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text, 10)
    except ValueError:
        return None


def parse_message_resource(raw: Mapping[str, Any]) -> MessageResponse:
    """把 JSON 消息资源解析成 :class:`MessageResponse`。"""
    return MessageResponse(
        sid=str(raw.get("sid") or ""),
        status=str(raw.get("status") or "").strip().lower(),
        error_code=_as_int(raw.get("error_code")),
        error_message=str(raw.get("error_message") or "").strip(),
        segments=_as_int(raw.get("num_segments")) or 0,
    )


def _snippet(payload: object, limit: int = 120) -> str:
    """把非 JSON 响应压成一行做原因，避免把整页 HTML 塞进执行明细。"""
    text = " ".join(str(payload or "").split())
    return text[:limit] + "…" if len(text) > limit else text


class TwilioSMSProvider(Provider):
    """按 Messages 资源契约提交短信的 sms 渠道实现。"""

    kind = "twilio"

    def __init__(
        self,
        channel: Channel = Channel.SMS,
        *,
        account_sid: str,
        auth_token: str,
        sender: str,
        endpoint: str = DEFAULT_ENDPOINT,
        default_country_code: str = "",
        status_callback: str = "",
        identifier: str = "",
        priority: int = 100,
        timeout: float = DEFAULT_TIMEOUT,
        log: logging.Logger | None = None,
    ) -> None:
        if channel != Channel.SMS:
            raise TwilioConfigError("twilio 只能用于 sms 渠道")
        self._config = TwilioConfig(
            account_sid=account_sid,
            auth_token=auth_token,
            sender=sender,
            endpoint=endpoint,
            default_country_code=default_country_code,
            status_callback=status_callback,
            timeout=timeout,
        )
        super().__init__(channel, identifier=identifier, priority=priority)
        self._log = log or logger

    # ---- 只读属性：便于测试与启动日志 ----

    @property
    def endpoint(self) -> str:
        return self._config.endpoint

    @property
    def sender(self) -> str:
        return self._config.sender

    @property
    def default_country_code(self) -> str:
        return self._config.default_country_code

    @classmethod
    def from_credentials(
        cls,
        channel: Channel,
        credentials: Mapping[str, Any],
        *,
        identifier: str = "",
        priority: int = 100,
        timeout: Optional[float] = None,
        log: logging.Logger | None = None,
    ) -> "TwilioSMSProvider":
        """按渠道配置里的 ``credentials`` 构造实现。"""
        cfg = TwilioConfig.from_credentials(credentials, timeout=timeout)
        return cls(
            channel,
            account_sid=cfg.account_sid,
            auth_token=cfg.auth_token,
            sender=cfg.sender,
            endpoint=cfg.endpoint,
            default_country_code=cfg.default_country_code,
            status_callback=cfg.status_callback,
            identifier=identifier,
            priority=priority,
            timeout=cfg.timeout,
            log=log,
        )

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        raw_to = delivery.recipient.address(Channel.SMS) or ""
        try:
            to = to_e164(raw_to, self._config.default_country_code)
        except PhoneNumberError as exc:
            return self._failed(delivery, str(exc))

        body = (delivery.body or "").strip() or DEFAULT_BODY
        if len(body) > MAX_BODY_LENGTH:
            return self._failed(
                delivery,
                f"短信正文超长（{len(body)} 字符，单条上限 {MAX_BODY_LENGTH}）",
            )

        request = MessageRequest(
            to=to,
            sender=self._config.sender,
            body=body,
            status_callback=self._config.status_callback,
        )
        try:
            response = self._post(request)
        except GatewayError as exc:
            return self._failed(delivery, str(exc))

        reason = self._rejection(response)
        if reason is not None:
            return self._failed(delivery, reason)

        message = parse_message_resource(response.payload)
        segments = message.segments or sms_segments(body)
        self._log.info(
            "[twilio] 投递成功 provider=%s to=%s sid=%s status=%s segments=%s",
            self._id_for_result,
            to,
            message.sid or "-",
            message.status or "-",
            segments,
        )
        return DeliveryResult.ok(self._id_for_result, delivery.channel)

    def _failed(self, delivery: Delivery, reason: str) -> DeliveryResult:
        self._log.warning(
            "[twilio] 投递失败 provider=%s subscriber=%s reason=%s",
            self._id_for_result,
            delivery.recipient.subscriber_id,
            reason,
        )
        return DeliveryResult.failed(
            provider_id=self._id_for_result,
            channel=delivery.channel,
            reason=reason,
        )

    # ---- 内部：请求与响应 ----

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": self._config.authenticates_with,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }

    def _post(self, request: MessageRequest) -> _HttpResponse:
        """提交一条短信；网络层失败抛 :class:`GatewayError`，HTTP 错误码正常返回。"""
        url = self._config.endpoint + request.path(self._config.account_sid)
        data = urllib.parse.urlencode(request.form()).encode("utf-8")
        http_request = urllib.request.Request(url, data=data, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(http_request, timeout=self._config.timeout) as raw_response:
                status = raw_response.status
                headers = {key.lower(): value for key, value in raw_response.getheaders()}
                raw = raw_response.read()
        except urllib.error.HTTPError as exc:
            # 4xx / 5xx 同样是可用信息（错误码与原因都在报文里），不当网络故障处理。
            status = exc.code
            headers = {key.lower(): value for key, value in (exc.headers or {}).items()}
            raw = exc.read()
        except urllib.error.URLError as exc:
            raise self._transport_error(exc.reason) from exc
        except (socket.timeout, TimeoutError) as exc:
            raise self._transport_error(exc) from exc
        except OSError as exc:
            raise self._transport_error(exc) from exc

        return _HttpResponse(status=status, payload=_decode(raw, headers.get("content-type", "")), headers=headers)

    def _transport_error(self, reason: object) -> GatewayError:
        endpoint = self._config.endpoint
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return GatewayError(f"连接 {endpoint} 超时（{self._config.timeout:g} 秒）")
        return GatewayError(f"连接 {endpoint} 失败：{reason}")

    def _rejection(self, response: _HttpResponse) -> Optional[str]:
        """把「网关没收下这条短信」翻译成一句可行动的原因；成功时返回 ``None``。"""
        status = response.status
        payload = response.payload if isinstance(response.payload, Mapping) else {}
        code = _as_int(payload.get("code")) or _as_int(payload.get("error_code"))
        detail = str(payload.get("message") or payload.get("error_message") or "").strip()
        code_text = f"（{code}）" if code else ""

        if status in (401, 403):
            return f"网关认证失败（HTTP {status}）：account_sid 或 auth_token 不正确"
        if status == 404:
            return f"网关接口不存在（HTTP 404）：请检查 endpoint 与 account_sid（{self._config.endpoint}）"
        if status == 429:
            retry_after = str(response.headers.get("retry-after", "")).strip()
            hint = f"，{retry_after} 秒后可重试" if retry_after else ""
            return f"网关限流（HTTP 429）{hint}"
        if status >= 500:
            return f"网关错误（HTTP {status}）{code_text}{detail or '网关未给出原因'}"
        if status >= 400:
            return f"网关拒绝请求（HTTP {status}）{code_text}{detail or '网关未给出原因'}"

        if not isinstance(response.payload, Mapping):
            return f"网关响应不是 JSON（HTTP {status}）：{_snippet(response.payload) or '空响应'}"

        message = parse_message_resource(payload)
        if message.failed:
            return message.failure_reason()
        return None


def _decode(raw: bytes, content_type: str) -> Any:
    """按 Content-Type 解析响应体；解析不了就把文本原样返回，交给上层判定。"""
    text = raw.decode("utf-8", "replace").strip()
    if not text:
        return ""
    if "json" not in content_type.lower():
        return text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


__all__ = [
    "API_VERSION",
    "DEFAULT_BODY",
    "DEFAULT_ENDPOINT",
    "DEFAULT_TIMEOUT",
    "FAILURE_STATUSES",
    "MAX_BODY_LENGTH",
    "SUCCESS_STATUSES",
    "GatewayError",
    "MessageRequest",
    "MessageResponse",
    "TwilioConfig",
    "TwilioConfigError",
    "TwilioSMSProvider",
    "parse_message_resource",
]
