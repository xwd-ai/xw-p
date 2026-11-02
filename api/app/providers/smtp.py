"""SMTP 渠道实现：把邮件真正提交到中继服务器。

与 :mod:`.console` 实现同一份 :class:`~app.providers.base.Provider` 契约，
区别只在最后一步是真连网还是打日志：console 是本地跑链路时的兜底，SMTP 是
配好凭据后的真实路径。两者按优先级并存——中继不通时把 console 调低优先级
仍可作为降级项。

配置项与引擎侧共用同一份 ``XW_PROVIDERS_FILE``：

==================  ====================================================
``host``            中继地址（必填，不含协议与端口）
``from``            发件人地址（必填，可带显示名）
``port``            端口，不填按加密方式取 587 / 465 / 25
``username``        认证用户，与 ``password`` 成对出现
``password``        认证口令，只允许在加密连接上发送
``tls``             ``none`` / ``starttls`` / ``tls``，不填按端口推导
``reply_to``        可选回信地址
==================  ====================================================

投递失败不抛异常：连接被拒、认证失败、收件人被退信都表达成
``DeliveryResult.failed`` 并带上原因，调用方（内置消费者）把它写进执行明细。
"""
from __future__ import annotations

import logging
import smtplib
import socket
import ssl
from email.message import EmailMessage
from email.utils import formataddr, make_msgid, parseaddr
from typing import Mapping, Optional

from ..models.channel import Channel
from .base import Delivery, DeliveryResult, Provider
from .health import DEFAULT_PROBE_TIMEOUT, ProbeResult

logger = logging.getLogger("xw.provider.smtp")

#: 单次投递（连接 + 握手 + 提交）的默认超时（秒）。
DEFAULT_TIMEOUT = 10.0

#: 步骤没配标题时的兜底主题。
DEFAULT_SUBJECT = "你有一条新的互动提醒"

#: 各加密方式的惯例端口。
_PORT_BY_MODE = {"tls": 465, "starttls": 587, "none": 25}
_MODE_BY_PORT = {465: "tls", 25: "none"}

#: ``tls`` 配置项的别名。
_TLS_ALIASES = {
    "none": "none",
    "off": "none",
    "false": "none",
    "plain": "none",
    "no": "none",
    "starttls": "starttls",
    "true": "starttls",
    "on": "starttls",
    "yes": "starttls",
    "tls": "tls",
    "ssl": "tls",
    "smtps": "tls",
}


class SMTPConfigError(ValueError):
    """SMTP 配置不合法（地址、端口、加密方式或凭据组合）。"""


def _tls_mode(raw: str, port: int) -> str:
    """归一化加密方式；未配置时按端口推导。"""
    text = (raw or "").strip().lower()
    if not text:
        return _MODE_BY_PORT.get(port, "starttls")
    mode = _TLS_ALIASES.get(text)
    if mode is None:
        raise SMTPConfigError(f"SMTP 加密方式 {raw!r} 不支持，可选：none / starttls / tls")
    return mode


def _default_port(mode: str) -> int:
    return _PORT_BY_MODE.get(mode, 587)


def _addr_spec(value: str, field: str) -> tuple[str, str]:
    """解析地址，返回 ``(显示名, addr-spec)``。

    头字段里出现换行意味着可以往邮件里加额外的头，因此在拼接报文之前拒绝。
    """
    text = (value or "").strip()
    if not text:
        raise SMTPConfigError(f"SMTP {field}为空")
    if "\r" in text or "\n" in text:
        raise SMTPConfigError(f"SMTP {field}不能包含换行符")
    name, addr = parseaddr(text)
    addr = addr.strip()
    if not addr or "@" not in addr or addr.startswith("@") or addr.endswith("@"):
        raise SMTPConfigError(f"SMTP {field}地址不合法：{value!r}")
    if " " in addr or "<" in name or ">" in name:
        raise SMTPConfigError(f"SMTP {field}地址含非法字符：{value!r}")
    return name, addr


class SMTPProvider(Provider):
    """经 SMTP 提交邮件的 email 渠道实现。"""

    kind = "smtp"

    def __init__(
        self,
        channel: Channel = Channel.EMAIL,
        *,
        host: str,
        sender: str,
        port: Optional[int] = None,
        username: str = "",
        password: str = "",
        tls: str = "",
        reply_to: str = "",
        identifier: str = "",
        priority: int = 100,
        timeout: float = DEFAULT_TIMEOUT,
        ssl_context: ssl.SSLContext | None = None,
        log: logging.Logger | None = None,
    ) -> None:
        if channel != Channel.EMAIL:
            raise SMTPConfigError("smtp 只能用于 email 渠道")
        self._host = (host or "").strip()
        if not self._host:
            raise SMTPConfigError("SMTP 配置缺少 host")
        if "://" in self._host or "/" in self._host or " " in self._host:
            raise SMTPConfigError(f"SMTP host 只能是主机名或 IP，收到 {host!r}")

        if port is None:
            if (tls or "").strip():
                port = _default_port(_tls_mode(tls, 587))
            else:
                port = _default_port("starttls")
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise SMTPConfigError(f"SMTP 端口必须是 1–65535，收到 {port!r}")
        self._port = port
        self._mode = _tls_mode(tls, port)

        sender_name, sender_addr = _addr_spec(sender, "发件人")
        self._from_name = sender_name
        self._from_addr = sender_addr
        if reply_to:
            self._reply_to = _addr_spec(reply_to, "回信")[1]
        else:
            self._reply_to = ""

        self._username = (username or "").strip()
        self._password = password or ""
        if self._username and not self._password:
            raise SMTPConfigError("SMTP 配置了 username 却没有 password")
        if self._password and not self._username:
            raise SMTPConfigError("SMTP 配置了 password 却没有 username")
        if self._password and self._mode == "none":
            # 明文连接 + 口令等于把凭据广播到链路上。
            raise SMTPConfigError("SMTP 口令不能在明文连接上发送，请开启 STARTTLS 或隐式 TLS")

        if timeout <= 0:
            raise SMTPConfigError("SMTP 超时必须大于 0")
        self._timeout = float(timeout)

        super().__init__(channel, identifier=identifier, priority=priority)
        # 内网自签证书的中继需要自带信任链；默认用系统根证书。
        self._context = ssl_context
        self._log = log or logger

    # ---- 只读属性：便于测试与启动日志 ----

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def tls_mode(self) -> str:
        return self._mode

    @property
    def sender(self) -> str:
        return self._from_addr

    def probe(self, timeout: float = DEFAULT_PROBE_TIMEOUT) -> ProbeResult:
        """连接中继的端口后立刻断开，判断「这条路通不通」。

        只验 TCP 可达，不做握手与认证：健康检查是高频动作，真跑一遍 EHLO/AUTH
        既有副作用（部分中继会记账）也更慢。结论里写明这一点，免得把
        「端口能连上」当成「凭据没问题」。原因不带用户名与口令。
        """
        budget = float(timeout) if timeout and timeout > 0 else DEFAULT_PROBE_TIMEOUT
        try:
            with socket.create_connection((self._host, self._port), timeout=budget):
                pass
        except socket.timeout:
            return ProbeResult.unavailable(
                f"连接 {self._host}:{self._port} 超时（{budget:.1f}s）"
            )
        except OSError as exc:
            return ProbeResult.unavailable(
                f"连接 {self._host}:{self._port} 失败：{exc}"
            )
        return ProbeResult.ok(
            f"中继 {self._host}:{self._port} 可建立 TCP 连接（未验证认证与投递权限）"
        )

    @classmethod
    def from_credentials(
        cls,
        channel: Channel,
        credentials: Mapping[str, str],
        *,
        identifier: str = "",
        priority: int = 100,
        timeout: float = DEFAULT_TIMEOUT,
        log: logging.Logger | None = None,
    ) -> "SMTPProvider":
        """按渠道配置里的 ``credentials`` 构造实现。"""
        raw_port = str(credentials.get("port", "") or "").strip()
        port: Optional[int] = None
        if raw_port:
            try:
                port = int(raw_port)
            except ValueError as exc:
                raise SMTPConfigError(f"SMTP 端口必须是整数，收到 {raw_port!r}") from exc
        return cls(
            channel,
            host=str(credentials.get("host", "")),
            sender=str(credentials.get("from", "")),
            port=port,
            username=str(credentials.get("username", "")),
            password=str(credentials.get("password", "")),
            tls=str(credentials.get("tls", "")),
            reply_to=str(credentials.get("reply_to", "")),
            identifier=identifier,
            priority=priority,
            timeout=timeout,
            log=log,
        )

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        to = delivery.recipient.address(Channel.EMAIL)
        if not to:  # 基类已校验过，这里兜底防止绕过基类直接调 _deliver
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason="收件邮箱为空",
            )
        subject = (delivery.subject or "").strip() or DEFAULT_SUBJECT
        if "\r" in subject or "\n" in subject:
            # 头字段里的换行可以伪造额外的收件人，必须在拼报文之前挡住。
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason="主题不能包含换行符",
            )
        message = self._build_message(to, subject, delivery.body)
        server: Optional[smtplib.SMTP] = None
        try:
            server = self._connect()
            server.send_message(message)
        except smtplib.SMTPAuthenticationError as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"SMTP 认证失败（{exc.smtp_code}）：{_decode(exc.smtp_error)}",
            )
        except smtplib.SMTPRecipientsRefused as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"收件人被拒绝：{_recipients(exc.recipients)}",
            )
        except smtplib.SMTPSenderRefused as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"发件人被拒绝（{exc.smtp_code}）：{_decode(exc.smtp_error)}",
            )
        except smtplib.SMTPDataError as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"服务器拒收报文（{exc.smtp_code}）：{_decode(exc.smtp_error)}",
            )
        except smtplib.SMTPServerDisconnected as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"SMTP 服务器提前断开：{exc}",
            )
        except ssl.SSLError as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"SMTP TLS 握手失败：{exc}",
            )
        except (socket.timeout, TimeoutError) as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"连接 {self._endpoint} 超时：{exc}",
            )
        except (OSError, smtplib.SMTPException) as exc:
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason=f"连接 {self._endpoint} 失败：{_decode(exc)}",
            )
        finally:
            # 成功与失败都走同一处收尾：失败时连接可能已经半死，QUIT 失败就
            # 直接关掉，别把一个错误状态盖在真正的失败原因上面。
            if server is not None:
                try:
                    server.quit()
                except (smtplib.SMTPException, OSError):
                    server.close()

        message_id = str(message["Message-ID"])
        self._log.info(
            "[smtp] 投递成功 host=%s to=%s subject=%r messageId=%s",
            self._endpoint,
            to,
            subject,
            message_id,
        )
        return DeliveryResult.ok(self._id_for_result, delivery.channel)

    # ---- 内部：连接与报文 ----

    @property
    def _endpoint(self) -> str:
        return f"{self._host}:{self._port}"

    def _ssl_context(self) -> ssl.SSLContext:
        return self._context or ssl.create_default_context()

    def _connect(self) -> smtplib.SMTP:
        """建立连接、按需升级 TLS 并登录；调用方负责 close。"""
        if self._mode == "tls":
            server: smtplib.SMTP = smtplib.SMTP_SSL(
                self._host, self._port, timeout=self._timeout, context=self._ssl_context()
            )
        else:
            server = smtplib.SMTP(self._host, self._port, timeout=self._timeout)
        server.ehlo_or_helo_if_needed()
        if self._mode == "starttls":
            server.starttls(context=self._ssl_context())
            server.ehlo()
        if self._username:
            server.login(self._username, self._password)
        return server

    def _build_message(self, to: str, subject: str, body: str) -> EmailMessage:
        message = EmailMessage()
        message["From"] = formataddr((self._from_name, self._from_addr)) if self._from_name else self._from_addr
        message["To"] = to
        if self._reply_to:
            message["Reply-To"] = self._reply_to
        message["Subject"] = subject
        message["Message-ID"] = make_msgid(domain=self._from_addr.split("@")[-1])
        message.set_content(body or "", subtype="plain", charset="utf-8")
        return message


def _decode(value: object) -> str:
    """把 smtplib 回复里的 bytes 转成可读文本。"""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _recipients(refused: Mapping[str, tuple[int, bytes]]) -> str:
    parts = [f"{addr}（{code}）" for addr, (code, _) in refused.items()]
    return "、".join(parts) or "全部收件人"


__all__ = [
    "DEFAULT_SUBJECT",
    "DEFAULT_TIMEOUT",
    "SMTPConfigError",
    "SMTPProvider",
]
