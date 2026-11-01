"""推送渠道的接口定义：FCM HTTP v1 与 APNs token 认证两套报文契约。

推送与邮件、短信的差别在于「发给谁」不是邮箱或号码，而是一台设备上的应用实例
（device token）；报文也不是一段文本，而是网关规定的结构化 JSON。本模块只定义
契约，不发网络请求：

- :class:`PushMessage`：渠道无关的消息体（标题、正文、自定义 data、角标、提示音），
  负责本地校验，并能拼出两种网关的消息体；
- :class:`FCMSendRequest` / :class:`FCMResponse`：FCM HTTP v1 的 ``messages:send``；
- :class:`APNsSendRequest` / :class:`APNsResponse`：APNs 的 ``/3/device/<token>``；
- :func:`normalize_token` / :func:`detect_platform` / :func:`mask_token`：设备 token。

把契约单独成一层的理由是**失败语义**：设备 token 失效是推送最常见的失败，也是
唯一需要回写到订阅者资料的失败（FCM 的 ``UNREGISTERED``、APNs 的 ``Unregistered``）。
把错误码到「能不能重试 / 要不要删 token」的映射写在这里，渠道实现（真实提交或
mock）就只负责传输，不用各自猜一遍。

报文口径（两家共同约束，不做本地发挥）：

======================  ====================================================
载荷上限                4 KiB（FCM 数据消息与 APNs 通知都是这个数）
FCM 端点                ``POST {endpoint}/v1/projects/{project_id}/messages:send``
FCM 认证                ``Authorization: Bearer <OAuth2 access_token>``
FCM 成功响应            ``200`` + ``{"name": "projects/…/messages/<id>"}``
FCM 失败响应            ``4xx/5xx`` + ``{"error": {"status", "code", "details"}}``
APNs 端点               ``POST {endpoint}/3/device/{token}``
APNs 认证               ``authorization: bearer <ES256 JWT>``
APNs 成功响应           ``200`` + 空响应体，消息标识在 ``apns-id`` 响应头
APNs 失败响应           ``4xx/5xx`` + ``{"reason": "BadDeviceToken"}``
======================  ====================================================

标题与正文的长度上限是**本项目的本地保护**，不是网关的规定：推送是用来喊一声的，
不是用来贴全文的，超过就拒掉比让网关截断更容易排查。自定义 ``data`` 的键值约束
同理——FCM 与 APNs 都只接受字符串值，因此这里在组装阶段就把非标量收口掉。
"""
from __future__ import annotations

import json
import re
import urllib.parse
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional

#: FCM 缺省端点；自建代理或测试替身用 ``endpoint`` 覆盖。
DEFAULT_FCM_ENDPOINT = "https://fcm.googleapis.com"

#: APNs 生产与沙箱端点。
DEFAULT_APNS_ENDPOINT = "https://api.push.apple.com"
APNS_SANDBOX_ENDPOINT = "https://api.sandbox.push.apple.com"

#: 单条推送的载荷上限（字节），FCM 与 APNs 都是 4 KiB。
MAX_PAYLOAD_BYTES = 4096

#: 标题与正文的本地长度上限（字符）。
MAX_TITLE_CHARS = 200
MAX_BODY_CHARS = 2000

#: 自定义 data 的条目数与单个取值的长度上限。
MAX_DATA_KEYS = 64
MAX_DATA_VALUE_CHARS = 1024

#: FCM 设备 token 的长度范围（本地保护；真实 token 通常在 140 字符以上）。
FCM_TOKEN_MIN = 20
FCM_TOKEN_MAX = 4096

#: APNs 设备 token 固定为 32 字节，即 64 位十六进制。
APNS_TOKEN_LENGTH = 64

#: 请求头里的客户端标识。
FCM_USER_AGENT = "xw-notify-push/1.0 (fcm)"
APNS_USER_AGENT = "xw-notify-push/1.0 (apns)"

_FCM_TOKEN_RE = re.compile(r"^[A-Za-z0-9_\-:.]+$")
_APNS_TOKEN_RE = re.compile(r"^[0-9A-Fa-f]{%d}$" % APNS_TOKEN_LENGTH)
_DATA_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
_PROJECT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{1,62}[a-z0-9]$")
_SOUND_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
_TOPIC_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,155}$")

#: ``data`` 里不能出现的键：APNs 的 ``aps`` 是保留字段，混进来会覆盖报文结构。
RESERVED_DATA_KEYS = frozenset({"aps"})

#: FCM 认为「这个 token 已经没有用了」的错误码。
FCM_INVALID_TOKEN_CODES = frozenset({"UNREGISTERED", "SENDER_ID_MISMATCH"})

#: FCM 侧可重试的状态（限流与服务端故障）。
FCM_RETRYABLE_STATUSES = frozenset({"UNAVAILABLE", "INTERNAL", "QUOTA_EXCEEDED", "THROTTLED"})

#: APNs 的错误原因 → (可读说明, 是否可重试, token 是否已失效)。
APNS_REASONS: dict[str, tuple[str, bool, bool]] = {
    "BadDeviceToken": ("设备 token 格式不合法或与当前环境（生产/沙箱）不匹配", False, True),
    "DeviceTokenNotForTopic": ("设备 token 不属于该 topic（bundle id 写错）", False, True),
    "Unregistered": ("设备已卸载应用或 token 已失效", False, True),
    "ExpiredToken": ("设备 token 已过期", False, True),
    "BadTopic": ("apns-topic 不合法", False, False),
    "TopicDisallowed": ("该应用不允许使用此 topic", False, False),
    "MissingTopic": ("缺少 apns-topic", False, False),
    "PayloadEmpty": ("报文为空：标题、正文与 data 至少要有一项", False, False),
    "PayloadTooLarge": ("报文超过 4 KiB 上限", False, False),
    "BadCollapseId": ("apns-collapse-id 不合法或过长", False, False),
    "BadExpirationDate": ("apns-expiration 不合法", False, False),
    "BadPriority": ("apns-priority 只能是 1、5 或 10", False, False),
    "BadPushType": ("apns-push-type 与报文内容不匹配", False, False),
    "MissingProviderToken": ("缺少 provider token（签名用的 JWT）", False, False),
    "InvalidProviderToken": ("provider token 校验失败：检查 key_id / team_id / 私钥", False, False),
    "ExpiredProviderToken": ("provider token 已过期，需要重新签发", True, False),
    "TooManyProviderTokenUpdates": ("provider token 刷新过于频繁", True, False),
    "TooManyRequests": ("同一 token 的请求过于频繁", True, False),
    "IdleTimeout": ("连接空闲超时", True, False),
    "InternalServerError": ("APNs 服务端错误", True, False),
    "ServiceUnavailable": ("APNs 暂时不可用", True, False),
    "Shutdown": ("APNs 正在关闭连接", True, False),
}


class PushConfigError(ValueError):
    """推送配置或报文不合法（token、主题、端点、凭据）。"""


class DeviceTokenError(PushConfigError):
    """设备 token 不合法或无法识别。"""


class PayloadError(PushConfigError):
    """消息体不合法（内容为空、超长、data 类型不允许）。"""


class Platform(str, Enum):
    """推送网关平台，值即渠道配置里的 ``platform``。"""

    FCM = "fcm"
    APNS = "apns"


# --------------------------------------------------------------------- token


def _strip(value: object) -> str:
    return str(value or "").strip()


def detect_platform(raw: object) -> Platform:
    """按 token 形态推断平台：64 位十六进制按 APNs 处理，其余按 FCM。

    64 位十六进制的 FCM token 在形态上无法与 APNs 区分（都被字符集覆盖），
    因此这种情况按 APNs 解释；确实要用在 FCM 上时请在渠道配置里显式写
    ``platform``，不要依赖推断。
    """
    text = _strip(raw)
    if not text:
        raise DeviceTokenError("缺少设备 token")
    if _APNS_TOKEN_RE.match(text):
        return Platform.APNS
    if _FCM_TOKEN_RE.match(text) and FCM_TOKEN_MIN <= len(text) <= FCM_TOKEN_MAX:
        return Platform.FCM
    raise DeviceTokenError(
        f"无法识别的设备 token（{mask_token(text)}）：既不是 64 位十六进制的 APNs token，"
        f"也不是长度 {FCM_TOKEN_MIN}–{FCM_TOKEN_MAX} 的 FCM token"
    )


def normalize_token(raw: object, platform: Platform) -> str:
    """按平台校验设备 token 并归一化（APNs 统一转小写）。"""
    text = _strip(raw)
    if not text:
        raise DeviceTokenError("缺少设备 token")
    if platform is Platform.APNS:
        if not _APNS_TOKEN_RE.match(text):
            raise DeviceTokenError(
                f"APNs 设备 token 必须是 {APNS_TOKEN_LENGTH} 位十六进制，"
                f"收到 {len(text)} 位：{mask_token(text)}"
            )
        return text.lower()
    if not _FCM_TOKEN_RE.match(text):
        bad = sorted({char for char in text if not _FCM_TOKEN_RE.match(char)})
        raise DeviceTokenError(
            f"FCM 设备 token 含不允许的字符（{''.join(bad) or '空白'}）：{mask_token(text)}"
        )
    if not FCM_TOKEN_MIN <= len(text) <= FCM_TOKEN_MAX:
        raise DeviceTokenError(
            f"FCM 设备 token 长度应在 {FCM_TOKEN_MIN}–{FCM_TOKEN_MAX} 之间，"
            f"收到 {len(text)}：{mask_token(text)}"
        )
    return text


def mask_token(raw: object) -> str:
    """把 token 压成可写日志的形态：保留头 6 位与尾 4 位。

    设备 token 是可以直接向该设备发消息的凭据，执行明细与日志里只出现脱敏形态。
    """
    text = _strip(raw)
    if not text:
        return "（空）"
    if len(text) <= 12:
        return text[:2] + "…" + text[-2:]
    return f"{text[:6]}…{text[-4:]}（{len(text)} 位）"


def fcm_send_path(project_id: str) -> str:
    """FCM HTTP v1 的发送路径（不含端点）。"""
    return f"/v1/projects/{_check_project_id(project_id)}/messages:send"


def apns_send_path(token: str) -> str:
    """APNs 的发送路径（不含端点）。"""
    return f"/3/device/{token}"


def _check_project_id(raw: object) -> str:
    text = _strip(raw)
    if not text:
        raise PushConfigError("缺少 project_id（FCM HTTP v1 的发送路径需要它）")
    if not _PROJECT_ID_RE.match(text):
        raise PushConfigError(
            f"project_id 只能是小写字母、数字与连字符，且以字母或数字开头结尾：{text!r}"
        )
    return text


def _check_endpoint(raw: object, default: str) -> str:
    """校验端点：只接受 http/https，去掉结尾斜杠，不允许查询串。"""
    text = _strip(raw) or default
    parts = urllib.parse.urlsplit(text)
    if parts.scheme not in ("http", "https"):
        raise PushConfigError(f"endpoint 必须是 http:// 或 https:// 地址，收到 {text!r}")
    if not parts.hostname:
        raise PushConfigError(f"endpoint 缺少主机名：{text!r}")
    if parts.query or parts.fragment:
        raise PushConfigError(f"endpoint 不能带查询串或片段：{text!r}")
    return f"{parts.scheme}://{parts.netloc}{parts.path.rstrip('/')}"


# ------------------------------------------------------------------- 消息体


def _scalar_text(value: object) -> str:
    """把标量转成 data 需要的字符串；布尔按 ``true`` / ``false`` 写。"""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


@dataclass(frozen=True)
class PushMessage:
    """渠道无关的推送消息体。

    标题与正文都为空时是**静默推送**（只带 data，由客户端自己决定是否提示），
    APNs 侧会据此把 ``apns-push-type`` 判成 ``background``、优先级降到 5。
    """

    title: str = ""
    body: str = ""
    data: Mapping[str, str] = field(default_factory=dict)
    badge: Optional[int] = None
    sound: str = ""

    def __post_init__(self) -> None:
        title = _strip(self.title)
        body = _strip(self.body)
        if len(title) > MAX_TITLE_CHARS:
            raise PayloadError(f"推送标题 {len(title)} 字符，超过上限 {MAX_TITLE_CHARS}")
        if len(body) > MAX_BODY_CHARS:
            raise PayloadError(f"推送正文 {len(body)} 字符，超过上限 {MAX_BODY_CHARS}")

        data: dict[str, str] = {}
        for key, value in dict(self.data or {}).items():
            name = _strip(key)
            if not name or not _DATA_KEY_RE.match(name):
                raise PayloadError(
                    f"推送 data 的键不合法：{key!r}（只允许字母、数字、下划线、点与连字符）"
                )
            if name in RESERVED_DATA_KEYS:
                raise PayloadError(f"推送 data 不能使用保留键 {name!r}")
            if isinstance(value, (Mapping, list, tuple, set)):
                raise PayloadError(
                    f"推送 data 只接受标量值，字段 {name!r} 是 {type(value).__name__}"
                )
            text = _scalar_text(value)
            if len(text) > MAX_DATA_VALUE_CHARS:
                raise PayloadError(
                    f"推送 data 的取值过长：{name!r} 有 {len(text)} 字符，上限 {MAX_DATA_VALUE_CHARS}"
                )
            data[name] = text
        if len(data) > MAX_DATA_KEYS:
            raise PayloadError(f"推送 data 有 {len(data)} 个字段，上限 {MAX_DATA_KEYS}")

        if not title and not body and not data:
            raise PayloadError("推送内容为空：标题、正文与 data 至少要有一项")

        badge = self.badge
        if badge is not None:
            if isinstance(badge, bool) or not isinstance(badge, int):
                raise PayloadError(f"badge 必须是整数，收到 {badge!r}")
            if badge < 0:
                raise PayloadError(f"badge 不能为负数：{badge}")

        sound = _strip(self.sound)
        if sound and not _SOUND_RE.match(sound):
            raise PayloadError(f"sound 只能包含字母、数字、下划线、点与连字符：{sound!r}")

        object.__setattr__(self, "title", title)
        object.__setattr__(self, "body", body)
        object.__setattr__(self, "data", data)
        object.__setattr__(self, "badge", badge)
        object.__setattr__(self, "sound", sound)

    @property
    def silent(self) -> bool:
        """是否是没有可见文案的静默推送。"""
        return not self.title and not self.body

    def notification(self) -> dict[str, str]:
        """FCM ``notification`` 字段；静默推送时为空字典。"""
        payload: dict[str, str] = {}
        if self.title:
            payload["title"] = self.title
        if self.body:
            payload["body"] = self.body
        return payload

    def fcm_envelope(self, token: str) -> dict[str, Any]:
        """FCM HTTP v1 的请求体：``{"message": {...}}``。"""
        message: dict[str, Any] = {"token": token}
        notification = self.notification()
        if notification:
            message["notification"] = notification
        if self.data:
            message["data"] = dict(self.data)
        # FCM 的 Android 端优先级与「是否允许延迟投递」由 android 段控制；
        # 通知类消息一律按 high，静默推送按 normal，避免把后台任务当成急件。
        message["android"] = {"priority": "normal" if self.silent else "high"}
        return {"message": message}

    def apns_envelope(self) -> dict[str, Any]:
        """APNs 的请求体：``aps`` 段 + 顶层自定义字段。"""
        aps: dict[str, Any] = {}
        if self.silent:
            aps["content-available"] = 1
        else:
            alert: dict[str, str] = {}
            if self.title:
                alert["title"] = self.title
            if self.body:
                alert["body"] = self.body
            aps["alert"] = alert
            if self.sound:
                aps["sound"] = self.sound
            if self.badge is not None:
                aps["badge"] = self.badge
        envelope: dict[str, Any] = {"aps": aps}
        envelope.update(dict(self.data))
        return envelope


def _encode(payload: Mapping[str, Any]) -> bytes:
    """压缩序列化：报文字节数按实际发送形态计算，不按缩进后的形态。"""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def payload_size(platform: Platform, token: str, message: PushMessage) -> int:
    """返回报文字节数；超过 4 KiB 上限时抛 :class:`PayloadError`。"""
    body = message.fcm_envelope(token) if platform is Platform.FCM else message.apns_envelope()
    size = len(_encode(body))
    if size > MAX_PAYLOAD_BYTES:
        raise PayloadError(
            f"推送报文 {size} 字节，超过 {MAX_PAYLOAD_BYTES} 字节上限（缩短正文或减少 data 字段）"
        )
    return size


def _snippet(payload: object, limit: int = 120) -> str:
    """把非 JSON 响应压成一行，避免把整页 HTML 塞进执行明细。"""
    text = " ".join(str(payload or "").split())
    return text[:limit] + "…" if len(text) > limit else text


# ----------------------------------------------------------------------- FCM


@dataclass(frozen=True)
class FCMSendRequest:
    """一次 FCM HTTP v1 发送请求。

    ``access_token`` 是 OAuth2 访问令牌（作用域 ``firebase.messaging``），有效期约
    一小时，刷新由调用方负责——本项目不内置服务账号签名，凭据由使用者提供。
    """

    project_id: str
    token: str
    message: PushMessage
    access_token: str = ""
    endpoint: str = DEFAULT_FCM_ENDPOINT

    def __post_init__(self) -> None:
        object.__setattr__(self, "project_id", _check_project_id(self.project_id))
        object.__setattr__(self, "token", normalize_token(self.token, Platform.FCM))
        if not isinstance(self.message, PushMessage):
            raise PushConfigError(f"message 必须是 PushMessage：{self.message!r}")
        access_token = _strip(self.access_token)
        if not access_token:
            raise PushConfigError("缺少 access_token（FCM HTTP v1 需要 OAuth2 访问令牌）")
        if any(char.isspace() for char in access_token):
            raise PushConfigError("access_token 不能包含空白字符")
        object.__setattr__(self, "access_token", access_token)
        object.__setattr__(self, "endpoint", _check_endpoint(self.endpoint, DEFAULT_FCM_ENDPOINT))
        payload_size(Platform.FCM, self.token, self.message)

    @property
    def path(self) -> str:
        return fcm_send_path(self.project_id)

    @property
    def url(self) -> str:
        return self.endpoint + self.path

    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "User-Agent": FCM_USER_AGENT,
        }

    def body(self) -> dict[str, Any]:
        return self.message.fcm_envelope(self.token)

    @property
    def payload_bytes(self) -> int:
        return len(_encode(self.body()))


@dataclass(frozen=True)
class FCMResponse:
    """FCM 的响应：HTTP 状态 + 消息标识，或错误信封。

    ``token_invalid`` 表示网关明确说这个 token 没用了（``UNREGISTERED`` /
    ``SENDER_ID_MISMATCH``），调用方应把它从订阅者资料里摘掉；``retryable`` 表示
    这次失败值得退避重试，而不是改配置。
    """

    http_status: int
    message_id: str = ""
    error_status: str = ""
    error_code: str = ""
    error_message: str = ""
    retryable: bool = False
    token_invalid: bool = False

    @property
    def accepted(self) -> bool:
        return 200 <= self.http_status < 300 and bool(self.message_id)

    @classmethod
    def from_payload(cls, http_status: int, payload: Any) -> "FCMResponse":
        """按 HTTP 状态与响应体构造；响应体不是 JSON 也照常返回（记录原文摘要）。"""
        if not isinstance(payload, Mapping):
            return cls(http_status=http_status, error_message=_snippet(payload))
        if 200 <= http_status < 300:
            return cls(http_status=http_status, message_id=_strip(payload.get("name")))

        error = payload.get("error")
        if not isinstance(error, Mapping):
            return cls(http_status=http_status, error_message=_snippet(payload))

        status_text = _strip(error.get("status")).upper()
        code = ""
        details = error.get("details")
        if isinstance(details, list):
            for item in details:
                if isinstance(item, Mapping) and _strip(item.get("errorCode")):
                    code = _strip(item.get("errorCode")).upper()
                    break
        retryable = status_text in FCM_RETRYABLE_STATUSES or http_status == 429 or http_status >= 500
        token_invalid = code in FCM_INVALID_TOKEN_CODES
        return cls(
            http_status=http_status,
            error_status=status_text,
            error_code=code,
            error_message=_strip(error.get("message")),
            retryable=retryable,
            token_invalid=token_invalid,
        )

    def failure_reason(self) -> str:
        """把拒绝原因翻译成一句能指导下一步动作的话；成功时返回空串。"""
        status = self.http_status
        label = " ".join(part for part in (self.error_status, self.error_code) if part)
        code_text = f"（{label}）" if label else ""
        detail = self.error_message or "FCM 未给出原因"

        if self.accepted:
            return ""
        if self.token_invalid:
            return f"FCM 报告设备 token 已失效{code_text}：应从订阅者资料中移除"
        if status in (401, 403):
            return f"FCM 认证失败（HTTP {status}）：access_token 无效、过期或缺少发送权限"
        if status == 404:
            return f"FCM 接口或项目不存在（HTTP 404）：检查 project_id 与 endpoint"
        if status == 429 or self.error_status == "QUOTA_EXCEEDED":
            return f"FCM 限流（HTTP {status}）{code_text}：配额用尽，退避后可重试"
        if status >= 500:
            return f"FCM 服务端错误（HTTP {status}）{code_text}：{detail}"
        return f"FCM 拒绝消息（HTTP {status}）{code_text}：{detail}"


# ---------------------------------------------------------------------- APNs


def _check_uuid(raw: object) -> str:
    """APNs 的 ``apns-id`` 必须是 UUID；空串表示由本地生成。"""
    text = _strip(raw)
    if not text:
        return str(uuid.uuid4())
    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError, TypeError) as exc:
        raise PushConfigError(f"apns-id 必须是 UUID：{text!r}") from exc


@dataclass(frozen=True)
class APNsSendRequest:
    """一次 APNs 发送请求（token 认证，HTTP/2 + TLS）。

    ``provider_token`` 是调用方用 team id / key id / 私钥签好的 ES256 JWT，本模块
    只透传不签发：签名需要私钥，私钥留在使用者的配置里比扩散到代码中更安全。
    ``push_type`` 留空时按内容推导——有文案是 ``alert``，只有 data 是 ``background``。
    """

    token: str
    message: PushMessage
    topic: str
    provider_token: str
    endpoint: str = DEFAULT_APNS_ENDPOINT
    push_type: str = ""
    priority: Optional[int] = None
    expiration: int = 0
    collapse_id: str = ""
    apns_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "token", normalize_token(self.token, Platform.APNS))
        if not isinstance(self.message, PushMessage):
            raise PushConfigError(f"message 必须是 PushMessage：{self.message!r}")

        topic = _strip(self.topic)
        if not topic:
            raise PushConfigError("缺少 topic（APNs 的 apns-topic，通常是应用的 bundle id）")
        if not _TOPIC_RE.match(topic):
            raise PushConfigError(f"topic 含不允许的字符：{topic!r}")
        object.__setattr__(self, "topic", topic)

        token_text = _strip(self.provider_token)
        if not token_text:
            raise PushConfigError("缺少 provider_token（APNs 需要 ES256 签名的 JWT）")
        if any(char.isspace() for char in token_text):
            raise PushConfigError("provider_token 不能包含空白字符")
        object.__setattr__(self, "provider_token", token_text)
        object.__setattr__(self, "endpoint", _check_endpoint(self.endpoint, DEFAULT_APNS_ENDPOINT))

        resolved_type = _strip(self.push_type).lower() or ("background" if self.message.silent else "alert")
        if resolved_type not in ("alert", "background"):
            raise PushConfigError(f"push_type 只能是 alert 或 background，收到 {self.push_type!r}")
        if resolved_type == "background" and not self.message.silent:
            raise PushConfigError("push_type=background 的消息不能带标题或正文，请改用 alert")
        object.__setattr__(self, "push_type", resolved_type)

        priority = self.priority
        if priority is None:
            priority = 5 if resolved_type == "background" else 10
        if isinstance(priority, bool) or priority not in (1, 5, 10):
            raise PushConfigError(f"apns-priority 只能是 1、5 或 10，收到 {priority!r}")
        if resolved_type == "background" and priority != 5:
            raise PushConfigError("静默推送（background）的 apns-priority 必须是 5")
        object.__setattr__(self, "priority", int(priority))

        expiration = self.expiration
        if isinstance(expiration, bool) or not isinstance(expiration, int) or expiration < 0:
            raise PushConfigError(f"expiration 必须是大于等于 0 的整数（秒），收到 {expiration!r}")
        object.__setattr__(self, "expiration", expiration)

        collapse_id = _strip(self.collapse_id)
        if len(collapse_id.encode("utf-8")) > 64:
            raise PushConfigError("collapse_id 最长 64 字节")
        object.__setattr__(self, "collapse_id", collapse_id)
        object.__setattr__(self, "apns_id", _check_uuid(self.apns_id))

        payload_size(Platform.APNS, self.token, self.message)

    @property
    def path(self) -> str:
        return apns_send_path(self.token)

    @property
    def url(self) -> str:
        return self.endpoint + self.path

    def headers(self) -> dict[str, str]:
        headers = {
            "authorization": f"bearer {self.provider_token}",
            "apns-topic": self.topic,
            "apns-push-type": self.push_type,
            "apns-priority": str(self.priority),
            "apns-expiration": str(self.expiration),
            "apns-id": self.apns_id,
            "content-type": "application/json",
            "user-agent": APNS_USER_AGENT,
        }
        if self.collapse_id:
            headers["apns-collapse-id"] = self.collapse_id
        return headers

    def body(self) -> dict[str, Any]:
        return self.message.apns_envelope()

    @property
    def payload_bytes(self) -> int:
        return len(_encode(self.body()))


@dataclass(frozen=True)
class APNsResponse:
    """APNs 的响应：成功是 ``200`` 空体，失败是 ``{"reason": …}``。"""

    http_status: int
    apns_id: str = ""
    reason: str = ""
    retryable: bool = False
    token_invalid: bool = False
    detail: str = ""

    @property
    def accepted(self) -> bool:
        return 200 <= self.http_status < 300

    @classmethod
    def from_payload(
        cls,
        http_status: int,
        payload: Any,
        headers: Optional[Mapping[str, str]] = None,
    ) -> "APNsResponse":
        lowered = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        apns_id = lowered.get("apns-id", "")
        if 200 <= http_status < 300:
            return cls(http_status=http_status, apns_id=apns_id)

        reason = ""
        if isinstance(payload, Mapping):
            reason = _strip(payload.get("reason"))
        detail = _snippet(payload) if not isinstance(payload, Mapping) else ""
        spec = APNS_REASONS.get(reason)
        retryable = bool(spec[1]) if spec else http_status in (429, 500, 503)
        token_invalid = bool(spec[2]) if spec else False
        return cls(
            http_status=http_status,
            apns_id=apns_id,
            reason=reason,
            retryable=retryable,
            token_invalid=token_invalid,
            detail=detail,
        )

    def failure_reason(self) -> str:
        """把 ``reason`` 翻成一句可读原因；成功时返回空串。"""
        if self.accepted:
            return ""
        spec = APNS_REASONS.get(self.reason)
        if spec is not None:
            return f"APNs 拒绝推送（HTTP {self.http_status} {self.reason}）：{spec[0]}"
        if self.reason:
            return f"APNs 拒绝推送（HTTP {self.http_status}）：{self.reason}"
        return f"APNs 拒绝推送（HTTP {self.http_status}）：{self.detail or '未给出原因'}"


__all__ = [
    "APNS_REASONS",
    "APNS_SANDBOX_ENDPOINT",
    "APNS_TOKEN_LENGTH",
    "APNsResponse",
    "APNsSendRequest",
    "DEFAULT_APNS_ENDPOINT",
    "DEFAULT_FCM_ENDPOINT",
    "DeviceTokenError",
    "FCM_INVALID_TOKEN_CODES",
    "FCM_RETRYABLE_STATUSES",
    "FCM_TOKEN_MAX",
    "FCM_TOKEN_MIN",
    "FCMResponse",
    "FCMSendRequest",
    "MAX_BODY_CHARS",
    "MAX_DATA_KEYS",
    "MAX_DATA_VALUE_CHARS",
    "MAX_PAYLOAD_BYTES",
    "MAX_TITLE_CHARS",
    "PayloadError",
    "Platform",
    "PushConfigError",
    "PushMessage",
    "RESERVED_DATA_KEYS",
    "apns_send_path",
    "detect_platform",
    "fcm_send_path",
    "mask_token",
    "normalize_token",
    "payload_size",
]
