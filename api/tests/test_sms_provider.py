"""短信渠道测试：号码规整、计费分段、console mock 与 Messages 资源客户端。

投递用例连的是进程内网关（``tests/twilio_gateway.py``），不访问外网；状态码与
错误码的映射逐条断言，因为运维排查时读到的就是那句 reason。
"""
from __future__ import annotations

import base64
import socket

import pytest

from app.models.channel import Channel
from app.providers import (
    ConsoleSMSProvider,
    Delivery,
    PhoneNumberError,
    ProviderConfig,
    ProviderConfigError,
    Recipient,
    TwilioConfigError,
    TwilioSMSProvider,
    build_registry,
    normalize_phone,
    sms_segments,
    to_e164,
)
from app.providers.twilio import (
    DEFAULT_BODY,
    DEFAULT_ENDPOINT,
    MAX_BODY_LENGTH,
    MessageRequest,
    TwilioConfig,
    parse_message_resource,
)
from tests.twilio_gateway import CannedResponse, FakeTwilioGateway, accepted, rejected

ACCOUNT_SID = "AC" + "0123456789abcdef" * 2
AUTH_TOKEN = "tok_test_9f2c1d"
SENDER = "+8613900000000"
CREATOR_PHONE = "+8613800000000"


def _delivery(**overrides: object) -> Delivery:
    recipient = overrides.pop(
        "recipient", None
    ) or Recipient(subscriber_id="creator_1001", phone=CREATOR_PHONE)
    data = {
        "channel": Channel.SMS,
        "recipient": recipient,
        "subject": "",
        "body": "小林赞了你的帖子",
        "payload": {"post_id": "P-2026-1001"},
    }
    data.update(overrides)
    return Delivery(**data)


def _provider(endpoint: str, **overrides: object) -> TwilioSMSProvider:
    kwargs = {
        "account_sid": ACCOUNT_SID,
        "auth_token": AUTH_TOKEN,
        "sender": SENDER,
        "endpoint": endpoint,
    }
    kwargs.update(overrides)
    return TwilioSMSProvider(Channel.SMS, **kwargs)


# ---------------------------------------------------------------- 号码与计费


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+8613800000000", "+8613800000000"),
        (" +86 138 0000 0000 ", "+8613800000000"),
        ("+86-138-0000-0000", "+8613800000000"),
        ("(+86) 13800000000", "+8613800000000"),
        ("13800000000", "13800000000"),
        ("", None),
        ("   ", None),
        (None, None),
        ("+86abc", None),
        ("+86+1380000000", None),
        ("12345", None),  # 位数不足
        ("+1234567890123456", None),  # 超过 15 位
        ("＋8613800000000", None),  # 全角加号不认
    ],
)
def test_normalize_phone(raw, expected):
    assert normalize_phone(raw) == expected


def test_to_e164_fills_country_code_and_rejects_missing_one():
    assert to_e164("13800000000", "86") == "+8613800000000"
    assert to_e164("13800000000", "+86") == "+8613800000000"
    assert to_e164("+8613800000000", "86") == "+8613800000000"

    with pytest.raises(PhoneNumberError) as info:
        to_e164("13800000000")
    assert "国际区号" in str(info.value)

    with pytest.raises(PhoneNumberError):
        to_e164("abc", "86")

    with pytest.raises(PhoneNumberError) as info:
        to_e164("138000000000000", "86")  # 补国家码后越界
    assert "15" in str(info.value)


@pytest.mark.parametrize(
    "body, segments",
    [
        ("", 0),
        ("a" * 160, 1),
        ("a" * 161, 2),
        ("a" * 306, 2),
        ("a" * 307, 3),
        ("赞" * 70, 1),
        ("赞" * 71, 2),
        ("赞" * 134, 2),
        ("赞" * 135, 3),
        ("a" * 159 + "赞", 3),  # 出现非 ASCII 整条转 UCS-2
    ],
)
def test_sms_segments_follow_billing_rules(body, segments):
    assert sms_segments(body) == segments


def test_sms_segments_counts_utf16_code_units():
    # emoji 在 UTF-16 里占两个码元：40 个字符 = 80 码元，按 UCS-2 多段 67 计为 2 段。
    assert sms_segments("😀" * 40) == 2


# ---------------------------------------------------------------- console mock


def test_console_sms_logs_normalized_number_and_segments(caplog):
    caplog.set_level("INFO", logger="xw.provider.sms")
    provider = ConsoleSMSProvider(identifier="console-sms")

    result = provider.send(_delivery(body="赞" * 80))

    assert result.sent is True and result.reason is None
    assert "+8613800000000" in caplog.text
    assert "segments=2" in caplog.text


def test_console_sms_rejects_bad_number():
    provider = ConsoleSMSProvider(identifier="console-sms")
    result = provider.send(_delivery(recipient=Recipient(subscriber_id="creator_1001", phone="13900001111x")))

    assert result.sent is False
    assert "格式非法" in result.reason


def test_console_sms_reports_missing_phone_before_delivery():
    provider = ConsoleSMSProvider()
    result = provider.send(_delivery(recipient=Recipient(subscriber_id="creator_1001")))

    assert result.sent is False
    assert "手机号" in result.reason


def test_console_sms_rejects_other_channels():
    with pytest.raises(ValueError):
        ConsoleSMSProvider(Channel.EMAIL)


# ---------------------------------------------------------------- 请求形态


def test_message_request_form_and_path():
    request = MessageRequest(to="+8613800000000", sender=SENDER, body="hi")
    assert request.form() == [("To", "+8613800000000"), ("From", SENDER), ("Body", "hi")]
    assert request.path(ACCOUNT_SID) == f"/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json"

    with_callback = MessageRequest(
        to="+8613800000000", sender=SENDER, body="hi", status_callback="https://example.com/cb"
    )
    assert with_callback.form()[-1] == ("StatusCallback", "https://example.com/cb")


def test_provider_posts_twilio_shaped_request():
    with FakeTwilioGateway() as gateway:
        provider = _provider(gateway.endpoint, identifier="sms-twilio")
        result = provider.send(_delivery())

        assert result.sent is True, result.reason
        assert gateway.count == 1
        request = gateway.requests[0]
        assert request.method == "POST"
        assert request.path == f"/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json"
        expected = base64.b64encode(f"{ACCOUNT_SID}:{AUTH_TOKEN}".encode()).decode()
        assert request.authorization == f"Basic {expected}"
        assert request.headers["content-type"] == "application/x-www-form-urlencoded"
        assert request.headers["accept"] == "application/json"
        assert request.form["To"] == CREATOR_PHONE
        assert request.form["From"] == SENDER
        assert request.form["Body"] == "小林赞了你的帖子"
        assert "StatusCallback" not in request.form


def test_provider_applies_default_country_code_and_sends_callback():
    with FakeTwilioGateway() as gateway:
        provider = _provider(
            gateway.endpoint,
            default_country_code="86",
            status_callback="https://example.com/hooks/sms",
        )
        result = provider.send(_delivery(recipient=Recipient(subscriber_id="creator_1001", phone="138 0000 0000")))

        assert result.sent is True, result.reason
        assert gateway.requests[0].form["To"] == "+8613800000000"
        assert gateway.requests[0].form["StatusCallback"] == "https://example.com/hooks/sms"


def test_provider_reports_missing_country_code_without_calling_gateway():
    with FakeTwilioGateway() as gateway:
        result = _provider(gateway.endpoint).send(
            _delivery(recipient=Recipient(subscriber_id="creator_1001", phone="13800000000"))
        )

        assert result.sent is False
        assert "国际区号" in result.reason
        assert gateway.count == 0


def test_provider_falls_back_to_default_body_when_empty():
    with FakeTwilioGateway() as gateway:
        result = _provider(gateway.endpoint).send(_delivery(body="   "))

        assert result.sent is True, result.reason
        assert gateway.requests[0].form["Body"] == DEFAULT_BODY


def test_provider_rejects_oversized_body_without_calling_gateway():
    with FakeTwilioGateway() as gateway:
        result = _provider(gateway.endpoint).send(_delivery(body="赞" * (MAX_BODY_LENGTH + 1)))

        assert result.sent is False
        assert "超长" in result.reason
        assert gateway.count == 0


# ---------------------------------------------------------------- 结果映射


@pytest.mark.parametrize("status", ["queued", "accepted", "sending", "sent", "delivered"])
def test_provider_accepts_in_flight_statuses(status):
    with FakeTwilioGateway(responses=[accepted(status=status, segments=2)]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is True, result.reason


def test_provider_reports_final_failure_status():
    response = accepted(sid="SM1", status="undelivered")
    with FakeTwilioGateway(responses=[response]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is False
        assert "undelivered" in result.reason


def test_provider_reports_message_level_error_code():
    response = CannedResponse(
        status=201,
        body={
            "sid": "SM2",
            "status": "failed",
            "error_code": 30003,
            "error_message": "Unreachable destination handset",
        },
    )
    with FakeTwilioGateway(responses=[response]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is False
        assert "30003" in result.reason and "Unreachable" in result.reason


def test_provider_maps_invalid_number_rejection():
    with FakeTwilioGateway(responses=[rejected(21211, "The 'To' number is not a valid phone number.")]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is False
        assert "21211" in result.reason and "not a valid phone number" in result.reason


@pytest.mark.parametrize(
    "response, keyword",
    [
        (rejected(20003, "Authentication Error", status=401), "认证失败"),
        (rejected(20404, "not found", status=404), "接口不存在"),
        (rejected(20429, "Too Many Requests", status=429), "限流"),
        (rejected(20500, "internal", status=500), "网关错误"),
        (rejected(21617, "Body too long", status=400), "网关拒绝请求"),
    ],
)
def test_provider_maps_http_status_codes(response, keyword):
    with FakeTwilioGateway(responses=[response]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is False
        assert keyword in result.reason


def test_provider_reports_rate_limit_retry_after():
    response = CannedResponse(
        status=429,
        body={"code": 20429, "message": "Too Many Requests"},
        headers={"Retry-After": "7"},
    )
    with FakeTwilioGateway(responses=[response]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is False
        assert "7 秒" in result.reason


def test_provider_rejects_non_json_success_body():
    response = CannedResponse(status=200, body="<html>maintenance</html>", content_type="text/html")
    with FakeTwilioGateway(responses=[response]) as gateway:
        result = _provider(gateway.endpoint).send(_delivery())

        assert result.sent is False
        assert "不是 JSON" in result.reason
        assert "maintenance" in result.reason


def test_provider_reports_timeout():
    with FakeTwilioGateway(delay=1.0) as gateway:
        provider = _provider(gateway.endpoint, timeout=0.2)
        result = provider.send(_delivery())

        assert result.sent is False
        assert "超时" in result.reason


def test_provider_reports_connection_failure():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()  # 端口空出来，连接会被拒绝

    result = _provider(f"http://127.0.0.1:{port}").send(_delivery())

    assert result.sent is False
    assert "连接" in result.reason and "失败" in result.reason


# ---------------------------------------------------------------- 配置校验


def test_config_from_credentials_maps_fields():
    cfg = TwilioConfig.from_credentials(
        {
            "account_sid": ACCOUNT_SID,
            "auth_token": AUTH_TOKEN,
            "from": SENDER,
            "default_country_code": "+86",
            "status_callback": "https://example.com/cb",
            "timeout": "2.5",
        }
    )
    assert cfg.endpoint == DEFAULT_ENDPOINT
    assert cfg.default_country_code == "86"
    assert cfg.timeout == 2.5


@pytest.mark.parametrize(
    "credentials, keyword",
    [
        ({"auth_token": AUTH_TOKEN, "from": SENDER}, "account_sid"),
        ({"account_sid": "AC123", "auth_token": AUTH_TOKEN, "from": SENDER}, "account_sid"),
        ({"account_sid": ACCOUNT_SID, "from": SENDER}, "auth_token"),
        ({"account_sid": ACCOUNT_SID, "auth_token": AUTH_TOKEN}, "from"),
        ({"account_sid": ACCOUNT_SID, "auth_token": AUTH_TOKEN, "from": "not a phone!"}, "from"),
        (
            {"account_sid": ACCOUNT_SID, "auth_token": AUTH_TOKEN, "from": "a-very-long-sender"},
            "11",
        ),
        (
            {"account_sid": ACCOUNT_SID, "auth_token": AUTH_TOKEN, "from": SENDER, "endpoint": "ftp://x"},
            "endpoint",
        ),
        (
            {"account_sid": ACCOUNT_SID, "auth_token": AUTH_TOKEN, "from": SENDER, "default_country_code": "86a"},
            "国家码",
        ),
        (
            {"account_sid": ACCOUNT_SID, "auth_token": AUTH_TOKEN, "from": SENDER, "timeout": "0"},
            "timeout",
        ),
    ],
)
def test_config_rejects_invalid_credentials(credentials, keyword):
    with pytest.raises(TwilioConfigError) as info:
        TwilioConfig.from_credentials(credentials)
    assert keyword in str(info.value)


def test_config_accepts_alphanumeric_sender_label():
    cfg = TwilioConfig(account_sid=ACCOUNT_SID, auth_token=AUTH_TOKEN, sender="XW-NOTIFY")
    assert cfg.sender == "XW-NOTIFY"


def test_provider_rejects_other_channels():
    with pytest.raises(TwilioConfigError):
        TwilioSMSProvider(Channel.EMAIL, account_sid=ACCOUNT_SID, auth_token=AUTH_TOKEN, sender=SENDER)


def test_parse_message_resource_tolerates_string_numbers():
    parsed = parse_message_resource(
        {"sid": "SM1", "status": "Queued", "num_segments": "3", "error_code": "21610"}
    )
    assert (parsed.sid, parsed.status, parsed.segments, parsed.error_code) == ("SM1", "queued", 3, 21610)
    assert parsed.failed is True


# ---------------------------------------------------------------- 工厂装配


def test_factory_builds_twilio_for_sms_channel():
    report = build_registry(
        [
            ProviderConfig(
                channel=Channel.SMS,
                kind="twilio",
                id="sms-twilio",
                priority=10,
                credentials={
                    "account_sid": ACCOUNT_SID,
                    "auth_token": AUTH_TOKEN,
                    "from": SENDER,
                    "default_country_code": "86",
                },
            )
        ]
    )

    assert [entry.id for entry in report.bound] == ["sms-twilio"]
    chosen = report.registry.get(Channel.SMS)
    assert isinstance(chosen, TwilioSMSProvider)
    assert chosen.default_country_code == "86"


def test_factory_reports_twilio_credential_problems():
    with pytest.raises(ProviderConfigError) as info:
        build_registry(
            [
                ProviderConfig(
                    channel=Channel.SMS,
                    kind="twilio",
                    id="sms-twilio",
                    credentials={"account_sid": "AC1", "auth_token": "", "from": ""},
                )
            ]
        )
    message = str(info.value)
    assert "sms-twilio" in message and "auth_token" in message


def test_default_configs_use_sms_console_implementation():
    report = build_registry()
    chosen = report.registry.get(Channel.SMS)
    assert isinstance(chosen, ConsoleSMSProvider)


# ---------------------------------------------------------------- 消费链路


class _Recorder:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def save(self, record: dict) -> None:
        self.records.append(record)


def test_worker_delivers_sms_through_gateway(monkeypatch):
    from app.schemas import QueuedMessage
    from app.worker import Worker

    executions: list[dict] = []
    storage = _Recorder()
    monkeypatch.setattr("app.worker.record_execution", lambda **kw: executions.append(kw))
    monkeypatch.setattr("app.worker.get_storage", lambda: storage)

    with FakeTwilioGateway() as gateway:
        registry = build_registry(
            [
                ProviderConfig(
                    channel=Channel.SMS,
                    kind="twilio",
                    id="sms-twilio",
                    credentials={
                        "account_sid": ACCOUNT_SID,
                        "auth_token": AUTH_TOKEN,
                        "from": SENDER,
                        "endpoint": gateway.endpoint,
                    },
                )
            ]
        ).registry
        monkeypatch.setattr("app.worker.get_registry", lambda: registry)

        message = QueuedMessage(
            trigger_id="t-sms",
            transaction_id="tx-sms",
            name="post_liked",
            subscriber_id="creator_1001",
            phone=CREATOR_PHONE,
            channels=["sms"],
            payload={"post_id": "P-2026-1001", "actor": "小林"},
        )
        Worker()._process(message)

        assert gateway.count == 1
        assert gateway.requests[0].form["To"] == CREATOR_PHONE
        assert executions[0]["channel"] == "sms"
        assert executions[0]["status"] == "sent"
        assert storage.records[0]["workflow"] == "post_liked"
        assert storage.records[0]["status"] == "sent"
