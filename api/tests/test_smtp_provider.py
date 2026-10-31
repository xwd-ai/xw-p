"""SMTP 渠道实现测试：配置校验、报文构造、投递成功与各类失败路径。

投递用例连的是进程内的最小 SMTP 服务器（``tests/smtp_server.py``），
不依赖外网中继；需要加密通道的用例用 openssl 现场签一张自签证书，
本机没有 openssl 时跳过而不是伪造通过。
"""
from __future__ import annotations

import email
import shutil
import socket
import ssl
import subprocess
from email import policy
from pathlib import Path

import pytest

from app.models.channel import Channel
from app.providers import (
    Delivery,
    ProviderConfig,
    ProviderConfigError,
    Recipient,
    SMTPConfigError,
    SMTPProvider,
    build_registry,
)
from tests.smtp_server import FakeSMTPServer

SENDER = "互动提醒 <notify@example.com>"
CREATOR = "creator@example.com"


def _delivery(**overrides: object) -> Delivery:
    recipient = overrides.pop("recipient", None) or Recipient(
        subscriber_id="creator_1001", email=CREATOR
    )
    data = {
        "channel": Channel.EMAIL,
        "recipient": recipient,
        "subject": "阿岚 评论了你的帖子《周末骑行》",
        "body": "「这段路线我也骑过，风景很好。」\n第二行",
    }
    data.update(overrides)
    return Delivery(**data)  # type: ignore[arg-type]


def _provider(server: FakeSMTPServer, **overrides: object) -> SMTPProvider:
    options = {
        "host": server.host,
        "port": server.port,
        "sender": SENDER,
        "tls": "none",  # 测试服务器是明文监听，加密路径由专门的用例覆盖
        "timeout": 2.0,
    }
    options.update(overrides)
    return SMTPProvider(Channel.EMAIL, **options)  # type: ignore[arg-type]


def _parse(raw: bytes) -> email.message.Message:
    return email.message_from_bytes(raw, policy=policy.default)


# ---- 投递路径 ----


def test_delivers_mail_with_headers_and_body():
    with FakeSMTPServer() as server:
        provider = _provider(server)
        result = provider.send(_delivery())

        assert result.sent, result.reason
        assert result.provider_id == "smtp:email"
        assert result.duration_ms >= 0

        assert len(server.messages) == 1
        sent = server.messages[0]
        assert sent["from"] == "notify@example.com"
        assert sent["to"] == [CREATOR]

        parsed = _parse(sent["data"])  # type: ignore[arg-type]
        assert parsed["To"] == CREATOR
        assert "notify@example.com" in parsed["From"]
        assert parsed["Subject"] == "阿岚 评论了你的帖子《周末骑行》"
        assert parsed["Message-ID"]
        assert parsed["MIME-Version"] == "1.0"
        # 报文按 RFC 5322 用 CRLF 续行，比对前统一成 \n。
        body = parsed.get_content().replace("\r\n", "\n").strip()
        assert body == _delivery().body.strip()


def test_missing_recipient_address_fails_before_dialling():
    with FakeSMTPServer() as server:
        provider = _provider(server)
        result = provider.send(_delivery(recipient=Recipient(subscriber_id="creator_1001")))

        assert not result.sent
        assert "邮箱" in result.reason
        assert server.accepted == 0


def test_header_injection_in_subject_is_rejected():
    with FakeSMTPServer() as server:
        provider = _provider(server)
        result = provider.send(_delivery(subject="新点赞\r\nBcc: victim@example.com"))

        assert not result.sent
        assert "换行" in result.reason
        assert server.accepted == 0


def test_recipient_refused_is_reported():
    with FakeSMTPServer(reject_recipient=CREATOR) as server:
        provider = _provider(server)
        result = provider.send(_delivery())

        assert not result.sent
        assert "收件人被拒绝" in result.reason
        assert CREATOR in result.reason
        assert server.messages == []


def test_connection_failure_is_reported():
    # 先占一个端口再关掉，保证这个端口上没有服务在听。
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    provider = SMTPProvider(
        Channel.EMAIL,
        host="127.0.0.1",
        port=port,
        sender=SENDER,
        timeout=0.5,
    )
    result = provider.send(_delivery())

    assert not result.sent
    assert "连接" in result.reason


def test_starttls_not_advertised_fails():
    with FakeSMTPServer() as server:
        provider = _provider(server, tls="starttls")
        result = provider.send(_delivery())

        assert not result.sent
        assert "STARTTLS" in result.reason


# ---- 加密通道与认证 ----


def _tls_material(tmp_path: Path) -> tuple[Path, Path]:
    """用 openssl 现场签一张自签证书；没有 openssl 时跳过用例。"""
    if shutil.which("openssl") is None:
        pytest.skip("本机没有 openssl，跳过需要 TLS 的用例")
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    command = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-sha256",
        "-nodes",
        "-days",
        "1",
        "-keyout",
        str(key),
        "-out",
        str(cert),
        "-subj",
        "/CN=localhost",
        "-addext",
        "subjectAltName=IP:127.0.0.1,DNS:localhost",
    ]
    proc = subprocess.run(command, capture_output=True, text=True)
    if proc.returncode != 0:
        pytest.skip(f"生成测试证书失败：{proc.stderr.strip()[:120]}")
    return cert, key


def _tls_server(tmp_path: Path, *, auth: tuple[str, str] | None) -> FakeSMTPServer:
    cert, key = _tls_material(tmp_path)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    return FakeSMTPServer(advertise_starttls=True, tls_context=context, auth=auth)


def _trust(cert: Path) -> ssl.SSLContext:
    return ssl.create_default_context(cafile=str(cert))


def test_starttls_upgrade_and_authentication(tmp_path: Path):
    cert, _ = _tls_material(tmp_path)
    server = _tls_server(tmp_path, auth=("notify", "s3cret"))
    with server:
        provider = _provider(
            server,
            tls="starttls",
            username="notify",
            password="s3cret",
            ssl_context=_trust(cert),
        )
        result = provider.send(_delivery())

        assert result.sent, result.reason
        assert server.upgrades == 1
        assert server.messages[0]["auth_user"] == "notify"

        parsed = _parse(server.messages[0]["data"])  # type: ignore[arg-type]
        assert parsed["Subject"] == _delivery().subject


def test_authentication_failure_reports_code(tmp_path: Path):
    cert, _ = _tls_material(tmp_path)
    server = _tls_server(tmp_path, auth=("notify", "right"))
    with server:
        provider = _provider(
            server,
            tls="starttls",
            username="notify",
            password="wrong",
            ssl_context=_trust(cert),
        )
        result = provider.send(_delivery())

        assert not result.sent
        assert "认证失败" in result.reason
        assert "535" in result.reason
        assert "wrong" not in result.reason
        assert server.messages == []


# ---- 配置校验 ----


def test_default_port_follows_tls_mode():
    plain = SMTPProvider(Channel.EMAIL, host="smtp.example.com", sender=SENDER)
    assert plain.port == 587
    assert plain.tls_mode == "starttls"

    implicit = SMTPProvider(Channel.EMAIL, host="smtp.example.com", sender=SENDER, tls="ssl")
    assert implicit.port == 465
    assert implicit.tls_mode == "tls"

    by_port = SMTPProvider(Channel.EMAIL, host="smtp.example.com", sender=SENDER, port=25)
    assert by_port.tls_mode == "none"


def test_password_over_plaintext_is_rejected():
    with pytest.raises(SMTPConfigError, match="明文"):
        SMTPProvider(
            Channel.EMAIL,
            host="smtp.example.com",
            sender=SENDER,
            port=25,
            username="notify",
            password="s3cret",
        )


@pytest.mark.parametrize(
    "options, message",
    [
        ({"host": ""}, "host"),
        ({"host": "smtp://smtp.example.com"}, "主机名"),
        ({"sender": "notify"}, "发件人"),
        ({"sender": "notify@example.com", "tls": "pgp"}, "加密方式"),
        ({"sender": SENDER, "port": 70000}, "端口"),
        ({"sender": SENDER, "username": "notify"}, "password"),
        ({"sender": SENDER, "password": "x"}, "username"),
        ({"sender": SENDER, "timeout": 0}, "超时"),
    ],
)
def test_invalid_config_raises(options: dict[str, object], message: str):
    base = {"host": "smtp.example.com", "sender": SENDER}
    base.update(options)
    with pytest.raises(SMTPConfigError, match=message):
        SMTPProvider(Channel.EMAIL, **base)  # type: ignore[arg-type]


def test_channel_must_be_email():
    with pytest.raises(SMTPConfigError, match="email"):
        SMTPProvider(Channel.SMS, host="smtp.example.com", sender=SENDER)


def test_from_credentials_maps_all_fields():
    provider = SMTPProvider.from_credentials(
        Channel.EMAIL,
        {
            "host": "smtp.example.com",
            "port": "2465",
            "from": SENDER,
            "username": "notify",
            "password": "s3cret",
            "tls": "tls",
            "reply_to": "reply@example.com",
        },
        identifier="email-smtp",
        priority=10,
    )
    assert provider.port == 2465
    assert provider.tls_mode == "tls"
    assert provider.identifier == "email-smtp"
    assert provider.default_priority == 10
    assert provider.sender == "notify@example.com"


def test_from_credentials_rejects_non_numeric_port():
    with pytest.raises(SMTPConfigError, match="端口"):
        SMTPProvider.from_credentials(
            Channel.EMAIL,
            {"host": "smtp.example.com", "from": SENDER, "port": "abc"},
        )


# ---- 工厂装配 ----


def test_factory_binds_smtp_provider():
    report = build_registry(
        [
            ProviderConfig(
                channel=Channel.EMAIL,
                kind="smtp",
                id="email-smtp",
                priority=10,
                credentials={"host": "smtp.example.com", "from": SENDER},
            )
        ]
    )
    provider = report.registry.get(Channel.EMAIL)
    assert isinstance(provider, SMTPProvider)
    assert provider.identifier == "email-smtp"


def test_factory_reports_invalid_smtp_credentials():
    with pytest.raises(ProviderConfigError, match="端口"):
        build_registry(
            [
                ProviderConfig(
                    channel=Channel.EMAIL,
                    kind="smtp",
                    credentials={"host": "smtp.example.com", "from": SENDER, "port": "abc"},
                )
            ]
        )
