"""推送渠道测试：设备 token、报文契约（FCM HTTP v1 / APNs）与 mock 实现。

契约层不发网络请求，因此这里逐条断言「拼出来的请求长什么样」「网关拒绝时翻译成
哪句原因」——这两件事决定了执行明细里显示什么，也决定了 token 失效能不能被发现。
mock 实现按同一套规则校验，测试同时覆盖它「该失败时真的失败」的那一面。
"""
from __future__ import annotations

import json

import pytest

from app.models.channel import Channel
from app.providers import (
    Delivery,
    DeviceTokenError,
    MockPushProvider,
    PayloadError,
    Platform,
    ProviderConfig,
    ProviderConfigError,
    PushConfigError,
    PushMessage,
    Recipient,
    build_registry,
    validate_config,
)
from app.providers.push import (
    APNS_REASONS,
    APNS_SANDBOX_ENDPOINT,
    MAX_BODY_CHARS,
    MAX_DATA_VALUE_CHARS,
    MAX_PAYLOAD_BYTES,
    MAX_TITLE_CHARS,
    APNsResponse,
    APNsSendRequest,
    FCMResponse,
    FCMSendRequest,
    detect_platform,
    fcm_send_path,
    mask_token,
    normalize_token,
    payload_size,
)

FCM_TOKEN = "fMx9q2Lk3Rt7Yb1Ns8Vc4Wd6Za0Pf5Gh2Jk9Lm3Nq7Rb1Tc4Vx8Yz2Ab5Cd0Ef6Gh1Ij3Kl9Mn2Op4Qr"
APNS_TOKEN = "9f2c1d4b6a8e0f3c5d7b9a1e2c4f6d8b0a2c4e6f8b1d3a5c7e9f0b2d4a6c8e1f"
PROVIDER_TOKEN = "eyJhbGciOiJFUzI1NiIsImtpZCI6" + "x" * 40


def delivery(
    *,
    token: str = FCM_TOKEN,
    subject: str = "小林赞了你的帖子",
    body: str = "《我的第一支 Vlog》收到了新的点赞",
    payload: dict | None = None,
    metadata: dict | None = None,
) -> Delivery:
    return Delivery(
        channel=Channel.PUSH,
        recipient=Recipient(subscriber_id="creator_demo", device_token=token),
        subject=subject,
        body=body,
        payload={"post_id": "P-2026-1001", "actor": "小林"} if payload is None else payload,
        metadata=metadata or {},
    )


# --------------------------------------------------------------------- token


def test_normalize_fcm_token_accepts_gateway_charset():
    assert normalize_token(FCM_TOKEN, Platform.FCM) == FCM_TOKEN
    # 冒号与下划线是 FCM token 常见字符，不应被拒。
    assert normalize_token("abc:APA91b_" + "x" * 40, Platform.FCM).endswith("xxxx")


def test_normalize_apns_token_requires_64_hex_and_lowercases():
    assert normalize_token(APNS_TOKEN.upper(), Platform.APNS) == APNS_TOKEN
    with pytest.raises(DeviceTokenError) as info:
        normalize_token(APNS_TOKEN[:-2], Platform.APNS)
    assert "64 位十六进制" in str(info.value)
    with pytest.raises(DeviceTokenError):
        normalize_token("z" * 64, Platform.APNS)


def test_normalize_fcm_token_rejects_short_token_and_bad_chars():
    with pytest.raises(DeviceTokenError) as info:
        normalize_token("abc", Platform.FCM)
    assert "长度" in str(info.value)
    with pytest.raises(DeviceTokenError) as info:
        normalize_token("tok en with spaces in it 0123456789", Platform.FCM)
    assert "不允许的字符" in str(info.value)


def test_missing_token_is_reported_not_assumed():
    with pytest.raises(DeviceTokenError) as info:
        normalize_token("   ", Platform.FCM)
    assert "缺少设备 token" in str(info.value)


def test_detect_platform_uses_token_shape():
    assert detect_platform(APNS_TOKEN) is Platform.APNS
    assert detect_platform(FCM_TOKEN) is Platform.FCM
    with pytest.raises(DeviceTokenError):
        detect_platform("short")


def test_mask_token_keeps_only_edges():
    masked = mask_token(FCM_TOKEN)
    assert FCM_TOKEN[:6] in masked
    assert FCM_TOKEN[-4:] in masked
    assert FCM_TOKEN[10:20] not in masked
    assert mask_token("") == "（空）"


# ------------------------------------------------------------------- 消息体


def test_message_normalizes_text_and_data():
    message = PushMessage(title="  有人赞了你  ", body="《Vlog》", data={"post_id": "P-1", "count": 2})
    assert message.title == "有人赞了你"
    assert message.data == {"post_id": "P-1", "count": "2"}
    assert message.notification() == {"title": "有人赞了你", "body": "《Vlog》"}
    assert not message.silent


def test_message_silent_when_only_data():
    message = PushMessage(data={"post_id": "P-1"})
    assert message.silent
    assert message.notification() == {}
    assert message.fcm_envelope("t" * 30)["message"]["android"]["priority"] == "normal"
    assert message.apns_envelope() == {"aps": {"content-available": 1}, "post_id": "P-1"}


def test_message_rejects_empty_content():
    with pytest.raises(PayloadError) as info:
        PushMessage(title="   ", body="\t")
    assert "推送内容为空" in str(info.value)


def test_message_rejects_long_text_and_long_data_value():
    with pytest.raises(PayloadError) as info:
        PushMessage(title="x" * (MAX_TITLE_CHARS + 1))
    assert "推送标题" in str(info.value)
    with pytest.raises(PayloadError):
        PushMessage(body="x" * (MAX_BODY_CHARS + 1))
    with pytest.raises(PayloadError) as info:
        PushMessage(data={"post_id": "x" * (MAX_DATA_VALUE_CHARS + 1)})
    assert "取值过长" in str(info.value)


def test_message_rejects_nested_data_and_reserved_key():
    with pytest.raises(PayloadError) as info:
        PushMessage(data={"post": {"id": 1}})
    assert "只接受标量值" in str(info.value)
    with pytest.raises(PayloadError) as info:
        PushMessage(data={"aps": "1"})
    assert "保留键" in str(info.value)
    with pytest.raises(PayloadError) as info:
        PushMessage(data={"bad key": "1"})
    assert "键不合法" in str(info.value)


def test_message_rejects_bad_badge_and_sound():
    with pytest.raises(PayloadError):
        PushMessage(title="t", badge=-1)
    with pytest.raises(PayloadError):
        PushMessage(title="t", badge=True)
    with pytest.raises(PayloadError):
        PushMessage(title="t", sound="叮咚 1")


def test_payload_size_guards_4k_limit():
    with pytest.raises(PayloadError) as info:
        payload_size(Platform.FCM, FCM_TOKEN, PushMessage(data={f"k{i}": "v" * 200 for i in range(30)}))
    assert str(MAX_PAYLOAD_BYTES) in str(info.value)
    # 正常消息远小于上限。
    assert payload_size(Platform.FCM, FCM_TOKEN, PushMessage(title="t", body="b")) < 600


# ----------------------------------------------------------------------- FCM


def test_fcm_request_shape():
    request = FCMSendRequest(
        project_id="xw-notify",
        token=FCM_TOKEN,
        message=PushMessage(title="小林赞了你的帖子", body="《我的第一支 Vlog》", data={"post_id": "P-1"}),
        access_token="ya29.test-token",
    )
    assert request.path == "/v1/projects/xw-notify/messages:send"
    assert request.url == "https://fcm.googleapis.com/v1/projects/xw-notify/messages:send"
    assert request.headers()["Authorization"] == "Bearer ya29.test-token"
    body = request.body()["message"]
    assert body["token"] == FCM_TOKEN
    assert body["notification"] == {"title": "小林赞了你的帖子", "body": "《我的第一支 Vlog》"}
    assert body["data"] == {"post_id": "P-1"}
    assert body["android"] == {"priority": "high"}
    assert request.payload_bytes == len(
        json.dumps(request.body(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def test_fcm_request_validates_project_and_credentials():
    message = PushMessage(title="t")
    with pytest.raises(PushConfigError) as info:
        FCMSendRequest(project_id="", token=FCM_TOKEN, message=message, access_token="a")
    assert "project_id" in str(info.value)
    with pytest.raises(PushConfigError):
        FCMSendRequest(project_id="XW Notify", token=FCM_TOKEN, message=message, access_token="a")
    with pytest.raises(PushConfigError) as info:
        FCMSendRequest(project_id="xw-notify", token=FCM_TOKEN, message=message, access_token="")
    assert "access_token" in str(info.value)
    with pytest.raises(PushConfigError):
        FCMSendRequest(
            project_id="xw-notify",
            token=FCM_TOKEN,
            message=message,
            access_token="a b",
        )


def test_fcm_send_path_rejects_bad_project_id():
    with pytest.raises(PushConfigError):
        fcm_send_path("XW-Notify")


def test_fcm_response_success_and_failures():
    ok = FCMResponse.from_payload(200, {"name": "projects/xw-notify/messages/0:1712"})
    assert ok.accepted
    assert ok.message_id == "projects/xw-notify/messages/0:1712"
    assert ok.failure_reason() == ""

    gone = FCMResponse.from_payload(
        404,
        {
            "error": {
                "status": "NOT_FOUND",
                "message": "Requested entity was not found.",
                "details": [{"errorCode": "UNREGISTERED"}],
            }
        },
    )
    assert not gone.accepted
    assert gone.token_invalid
    assert "token 已失效" in gone.failure_reason()

    quota = FCMResponse.from_payload(
        429,
        {"error": {"status": "RESOURCE_EXHAUSTED", "details": [{"errorCode": "QUOTA_EXCEEDED"}]}},
    )
    assert quota.retryable
    assert "限流" in quota.failure_reason()

    auth = FCMResponse.from_payload(403, {"error": {"status": "PERMISSION_DENIED"}})
    assert not auth.retryable
    assert "认证失败" in auth.failure_reason()

    broken = FCMResponse.from_payload(502, "<html>bad gateway</html>")
    assert not broken.accepted
    assert "502" in broken.failure_reason()


# ---------------------------------------------------------------------- APNs


def test_apns_request_shape_and_headers():
    request = APNsSendRequest(
        token=APNS_TOKEN,
        message=PushMessage(
            title="小林赞了你的帖子",
            body="《我的第一支 Vlog》",
            data={"post_id": "P-1"},
            badge=3,
            sound="default",
        ),
        topic="com.example.xw",
        provider_token=PROVIDER_TOKEN,
    )
    headers = request.headers()
    assert request.path == f"/3/device/{APNS_TOKEN}"
    assert headers["authorization"] == f"bearer {PROVIDER_TOKEN}"
    assert headers["apns-topic"] == "com.example.xw"
    assert headers["apns-push-type"] == "alert"
    assert headers["apns-priority"] == "10"
    assert headers["apns-expiration"] == "0"
    assert len(headers["apns-id"]) == 36

    body = request.body()
    assert body["aps"]["alert"] == {"title": "小林赞了你的帖子", "body": "《我的第一支 Vlog》"}
    assert body["aps"]["badge"] == 3
    assert body["post_id"] == "P-1"


def test_apns_silent_push_is_background_with_priority_5():
    request = APNsSendRequest(
        token=APNS_TOKEN,
        message=PushMessage(data={"post_id": "P-1"}),
        topic="com.example.xw",
        provider_token=PROVIDER_TOKEN,
    )
    assert request.push_type == "background"
    assert request.headers()["apns-priority"] == "5"
    assert request.body() == {"aps": {"content-available": 1}, "post_id": "P-1"}

    with pytest.raises(PushConfigError) as info:
        APNsSendRequest(
            token=APNS_TOKEN,
            message=PushMessage(title="有文案"),
            topic="com.example.xw",
            provider_token=PROVIDER_TOKEN,
            push_type="background",
        )
    assert "不能带标题或正文" in str(info.value)

    with pytest.raises(PushConfigError) as info:
        APNsSendRequest(
            token=APNS_TOKEN,
            message=PushMessage(data={"post_id": "P-1"}),
            topic="com.example.xw",
            provider_token=PROVIDER_TOKEN,
            priority=10,
        )
    assert "必须是 5" in str(info.value)


def test_apns_request_validates_topic_token_and_ids():
    message = PushMessage(title="t")
    with pytest.raises(PushConfigError) as info:
        APNsSendRequest(token=APNS_TOKEN, message=message, topic="", provider_token=PROVIDER_TOKEN)
    assert "topic" in str(info.value)
    with pytest.raises(PushConfigError) as info:
        APNsSendRequest(token=APNS_TOKEN, message=message, topic="com.example.xw", provider_token="")
    assert "provider_token" in str(info.value)
    with pytest.raises(PushConfigError):
        APNsSendRequest(
            token=APNS_TOKEN,
            message=message,
            topic="com.example.xw",
            provider_token=PROVIDER_TOKEN,
            apns_id="not-a-uuid",
        )
    with pytest.raises(PushConfigError):
        APNsSendRequest(
            token=APNS_TOKEN,
            message=message,
            topic="com.example.xw",
            provider_token=PROVIDER_TOKEN,
            priority=3,
        )
    with pytest.raises(PushConfigError):
        APNsSendRequest(
            token=APNS_TOKEN,
            message=message,
            topic="com.example.xw",
            provider_token=PROVIDER_TOKEN,
            expiration=-1,
        )


def test_apns_sandbox_endpoint_is_accepted():
    request = APNsSendRequest(
        token=APNS_TOKEN,
        message=PushMessage(title="t"),
        topic="com.example.xw",
        provider_token=PROVIDER_TOKEN,
        endpoint=APNS_SANDBOX_ENDPOINT,
    )
    assert request.endpoint == APNS_SANDBOX_ENDPOINT
    with pytest.raises(PushConfigError):
        APNsSendRequest(
            token=APNS_TOKEN,
            message=PushMessage(title="t"),
            topic="com.example.xw",
            provider_token=PROVIDER_TOKEN,
            endpoint="ftp://push.example.com",
        )


def test_apns_response_maps_reasons():
    ok = APNsResponse.from_payload(200, "", {"apns-id": "0f6c1f0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"})
    assert ok.accepted
    assert ok.apns_id.startswith("0f6c1f0e")
    assert ok.failure_reason() == ""

    unregistered = APNsResponse.from_payload(410, {"reason": "Unregistered"})
    assert unregistered.token_invalid
    assert not unregistered.retryable
    assert "卸载应用" in unregistered.failure_reason()

    bad_token = APNsResponse.from_payload(400, {"reason": "BadDeviceToken"})
    assert bad_token.token_invalid

    throttle = APNsResponse.from_payload(429, {"reason": "TooManyRequests"})
    assert throttle.retryable

    expired = APNsResponse.from_payload(403, {"reason": "ExpiredProviderToken"})
    assert expired.retryable
    assert not expired.token_invalid

    unknown = APNsResponse.from_payload(500, {"reason": "SomethingNew"})
    assert unknown.retryable
    assert "SomethingNew" in unknown.failure_reason()

    empty = APNsResponse.from_payload(503, "")
    assert empty.retryable
    assert "503" in empty.failure_reason()


def test_apns_reason_table_covers_token_failures():
    for reason in ("BadDeviceToken", "DeviceTokenNotForTopic", "Unregistered"):
        assert APNS_REASONS[reason][2] is True
    for reason in ("BadTopic", "PayloadTooLarge", "MissingProviderToken"):
        assert APNS_REASONS[reason][2] is False


# ---------------------------------------------------------------------- mock


def test_mock_provider_records_successful_delivery():
    provider = MockPushProvider(platform="fcm", project_id="xw-notify")
    result = provider.send(delivery())

    assert result.sent
    assert result.reason is None
    record = provider.last_record()
    assert record is not None
    assert record.platform is Platform.FCM
    assert record.sent
    assert record.path == "/v1/projects/xw-notify/messages:send"
    assert record.title == "小林赞了你的帖子"
    assert record.data == {"post_id": "P-2026-1001", "actor": "小林"}
    assert 0 < record.payload_bytes <= MAX_PAYLOAD_BYTES
    # 留痕里的 token 是脱敏形态，不能是可直接使用的凭据。
    assert FCM_TOKEN not in record.token
    assert record.describe().startswith("fcm ")


def test_mock_provider_detects_platform_from_token():
    provider = MockPushProvider(topic="com.example.xw")
    assert provider.send(delivery(token=APNS_TOKEN)).sent
    record = provider.last_record()
    assert record.platform is Platform.APNS
    assert record.path == f"/3/device/{APNS_TOKEN}"


def test_mock_provider_fails_without_token_instead_of_pretending():
    provider = MockPushProvider()
    result = provider.send(delivery(token=""))
    assert not result.sent
    assert "缺少投递地址" in result.reason
    assert provider.records() == ()


def test_mock_provider_fails_on_invalid_token_and_oversized_payload():
    apns = MockPushProvider(platform="apns")
    result = apns.send(delivery(token="not-a-token"))
    assert not result.sent
    assert "64 位十六进制" in result.reason

    fcm = MockPushProvider(platform="fcm")
    huge = delivery(payload={f"k{i}": "v" * 200 for i in range(30)})
    result = fcm.send(huge)
    assert not result.sent
    assert str(MAX_PAYLOAD_BYTES) in result.reason


def test_mock_provider_skips_nested_payload_fields():
    provider = MockPushProvider(platform="fcm")
    result = provider.send(
        delivery(payload={"post_id": "P-1", "post": {"id": "P-1", "title": "Vlog"}, "count": 3})
    )
    assert result.sent
    assert provider.last_record().data == {"post_id": "P-1", "count": "3"}


def test_mock_provider_reads_explicit_metadata_data():
    provider = MockPushProvider(platform="fcm")
    provider.send(delivery(payload={"post_id": "ignored"}, metadata={"data": {"post_id": "P-9"}}))
    assert provider.last_record().data == {"post_id": "P-9"}


def test_mock_provider_fail_with_marks_attempt_as_failed():
    provider = MockPushProvider(platform="fcm", fail_with="设备未注册（演示失败路径）")
    result = provider.send(delivery())
    assert not result.sent
    assert "设备未注册" in result.reason
    record = provider.last_record()
    assert record.sent is False
    assert "failed" in record.describe()


def test_mock_provider_ring_buffer_keeps_latest_records():
    provider = MockPushProvider(platform="fcm", capacity=3)
    for index in range(5):
        provider.send(delivery(payload={"post_id": f"P-{index}"}))
    records = provider.records()
    assert [record.data["post_id"] for record in records] == ["P-2", "P-3", "P-4"]
    provider.clear()
    assert provider.records() == ()


def test_mock_provider_rejects_wrong_channel_and_bad_config():
    with pytest.raises(PushConfigError):
        MockPushProvider(Channel.EMAIL)
    with pytest.raises(PushConfigError) as info:
        MockPushProvider(platform="wns")
    assert "platform" in str(info.value)
    with pytest.raises(PushConfigError):
        MockPushProvider(capacity=0)
    with pytest.raises(PushConfigError):
        MockPushProvider(capacity=10**6)


# ------------------------------------------------------------------- factory


def test_factory_assembles_push_mock():
    configs = [
        ProviderConfig(
            channel=Channel.PUSH,
            kind="push-mock",
            id="push-mock",
            credentials={"platform": "apns", "topic": "com.example.xw", "capacity": "10"},
        )
    ]
    report = build_registry(configs)
    assert [entry.id for entry in report.bound] == ["push-mock"]
    assert report.skipped == ()
    assert Channel.PUSH not in report.unbound_channels

    provider = report.registry.get(Channel.PUSH)
    assert isinstance(provider, MockPushProvider)
    assert provider.platform is Platform.APNS
    assert provider.topic == "com.example.xw"

    assert provider.send(delivery(token=APNS_TOKEN)).sent
    assert len(provider.records()) == 1


def test_factory_rejects_bad_push_mock_credentials():
    for credentials in ({"platform": "wns"}, {"capacity": "ten"}, {"capacity": "0"}):
        configs = [
            ProviderConfig(channel=Channel.PUSH, kind="push-mock", id="p", credentials=credentials)
        ]
        with pytest.raises(ProviderConfigError):
            build_registry(configs)


def test_push_mock_kind_spec_and_validation():
    cfg = ProviderConfig(channel=Channel.PUSH, kind="push-mock", credentials={"platform": "fcm"})
    assert validate_config(cfg) == []
    # 拼错的凭据项会被指出来。
    typo = ProviderConfig(channel=Channel.PUSH, kind="push-mock", credentials={"platfrom": "fcm"})
    assert any("platfrom" in problem for problem in validate_config(typo))
    # push-mock 只服务 push。
    wrong = ProviderConfig(channel=Channel.SMS, kind="push-mock")
    assert any("只能用于" in problem for problem in validate_config(wrong))
    # fcm 的凭据按 HTTP v1 约定：项目标识 + OAuth2 访问令牌。
    fcm = ProviderConfig(channel=Channel.PUSH, kind="fcm", credentials={"project_id": "xw-notify"})
    assert any("access_token" in problem for problem in validate_config(fcm))
