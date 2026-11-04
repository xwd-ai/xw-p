"""客户端主体：地址、请求头、状态码与响应解析。"""
from __future__ import annotations

import json

import pytest

from xw_notify import APIError, Client, Health, ProtocolError, TransportError
from xw_notify._http import Response

from fakes import empty_response, json_response, make_client, text_response


def test_base_url_is_normalized():
    client, transport = make_client(base_url="http://api.test:8000/")
    client.health()
    assert client.base_url == "http://api.test:8000"
    assert transport.last.url == "http://api.test:8000/health"


@pytest.mark.parametrize("base_url", ["", "   "])
def test_empty_base_url_is_rejected(base_url):
    with pytest.raises(ValueError, match="base_url 不能为空"):
        Client(base_url)


def test_base_url_without_scheme_is_rejected():
    with pytest.raises(ValueError, match="http:// 或 https://"):
        Client("api.test:8000")


def test_path_without_leading_slash_still_resolves():
    client, transport = make_client(base_url="http://api.test")
    client.request("GET", "v1/subscribers")
    assert transport.last.url == "http://api.test/v1/subscribers"


def test_default_headers_declare_environment_but_no_key():
    client, transport = make_client()
    client.health()
    headers = transport.last.headers
    assert headers["Accept"] == "application/json"
    assert headers["X-Environment"] == "development"
    assert headers["User-Agent"].startswith("xw-notify-python/")
    assert "X-Api-Key" not in headers
    # GET 没有请求体，不该声明 Content-Type。
    assert "Content-Type" not in headers


def test_api_key_and_environment_are_sent_when_given():
    client, transport = make_client(api_key="k-123", environment="production")
    client.health()
    assert transport.last.headers["X-Api-Key"] == "k-123"
    assert transport.last.headers["X-Environment"] == "production"


def test_blank_api_key_is_not_sent():
    client, transport = make_client(api_key="   ")
    client.health()
    assert "X-Api-Key" not in transport.last.headers


def test_repr_does_not_leak_key():
    client, _ = make_client(api_key="s3cr3t")
    assert "s3cr3t" not in repr(client)
    assert "已配置" in repr(client)
    assert "未配置" in repr(make_client()[0])


def test_query_skips_none_and_lowercases_booleans():
    client, transport = make_client()
    client.request(
        "GET",
        "/v1/inbox",
        query={"subscriberId": "creator-1001", "unread": True, "workflow": None, "limit": 20},
    )
    assert transport.last.url == (
        "http://api.test/v1/inbox?subscriberId=creator-1001&unread=true&limit=20"
    )


def test_empty_query_does_not_add_question_mark():
    client, transport = make_client()
    client.request("GET", "/health", query={"a": None})
    assert transport.last.url == "http://api.test/health"


def test_body_is_compact_json_keeping_chinese_readable():
    client, transport = make_client()
    client.request("POST", "/v1/events/trigger", json_body={"actor": "小林", "n": 1})
    raw = transport.last.body
    assert raw is not None
    assert "小林".encode("utf-8") in raw
    assert b" " not in raw
    assert json.loads(raw.decode("utf-8")) == {"actor": "小林", "n": 1}
    assert transport.last.headers["Content-Type"] == "application/json; charset=utf-8"


def test_no_content_returns_none():
    client, _ = make_client(lambda request: empty_response(204))
    assert client.request("DELETE", "/v1/subscribers/a") is None


def test_non_json_success_body_is_protocol_error():
    client, _ = make_client(lambda request: text_response(200, "<html>hello</html>"))
    with pytest.raises(ProtocolError) as excinfo:
        client.request("GET", "/v1/subscribers")
    assert "hello" in str(excinfo.value)
    assert "200" in str(excinfo.value)


def test_redirect_is_reported_not_followed():
    def responder(request):
        return Response(
            status=302,
            headers={"Location": "https://elsewhere.example/v1"},
            body=b"",
        )

    client, _ = make_client(responder)
    with pytest.raises(APIError) as excinfo:
        client.request("GET", "/v1/subscribers")
    assert excinfo.value.status == 302
    assert "elsewhere.example" in str(excinfo.value)


def test_error_response_is_parsed_for_detail():
    client, _ = make_client(
        lambda request: json_response(404, {"detail": "订阅者不存在：creator-1001"})
    )
    with pytest.raises(Exception) as excinfo:
        client.request("GET", "/v1/subscribers/creator-1001")
    assert excinfo.value.status == 404
    assert "订阅者不存在" in str(excinfo.value)


def test_error_response_with_html_body_keeps_status():
    client, _ = make_client(lambda request: text_response(502, "<html>Bad Gateway</html>"))
    with pytest.raises(APIError) as excinfo:
        client.request("GET", "/health")
    assert excinfo.value.status == 502
    assert "Bad Gateway" in str(excinfo.value)


def test_transport_failure_propagates():
    def responder(request):
        raise TransportError("请求失败：Connection refused")

    client, _ = make_client(responder)
    with pytest.raises(TransportError, match="Connection refused"):
        client.health()


def test_health_parses_queue_size():
    client, _ = make_client(lambda request: json_response(200, {"status": "ok", "queued": 3}))
    health = client.health()
    assert health == Health(status="ok", queued=3)


def test_client_works_as_context_manager():
    with make_client()[0] as client:
        assert isinstance(client, Client)


def test_resources_are_wired_on_construction():
    client, _ = make_client()
    assert client.trigger is not None
    assert client.subscribers is not None
    assert client.subscribers.preferences is not None


def test_missing_field_in_response_is_protocol_error():
    from xw_notify.models import TriggerAck

    with pytest.raises(ProtocolError, match="triggerId"):
        TriggerAck.from_payload({"transactionId": "t", "acknowledgmentId": "a"})
