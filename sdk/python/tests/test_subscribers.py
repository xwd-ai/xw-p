"""订阅者接口：请求形态、翻页、更新语义。"""
from __future__ import annotations

import pytest

from xw_notify import ConflictError, NotFoundError, Subscriber

from fakes import empty_response, json_response, make_client

SUBSCRIBER = {
    "id": "creator-1001",
    "email": "creator@example.com",
    "phone": None,
    "device_token": None,
    "channels": ["in_app", "email"],
    "bindings": {"in_app": "creator-1001", "email": "creator@example.com"},
    "environment": "development",
    "created_at": "2026-11-04T02:10:00+00:00",
    "updated_at": "2026-11-04T02:10:00+00:00",
}


def test_create_sends_only_given_fields():
    client, transport = make_client(lambda request: json_response(201, SUBSCRIBER))
    created = client.subscribers.create(subscriber_id="creator-1001", email="creator@example.com")

    assert transport.last.method == "POST"
    assert transport.last.url == "http://api.test/v1/subscribers"
    assert transport.last_json() == {"id": "creator-1001", "email": "creator@example.com"}
    assert isinstance(created, Subscriber)
    assert created.id == "creator-1001"
    assert created.channels == ("in_app", "email")
    assert created.binds("email") is True
    assert created.binds("sms") is False


def test_create_without_any_field_sends_empty_body():
    client, transport = make_client(lambda request: json_response(201, SUBSCRIBER))
    client.subscribers.create()
    assert transport.last_json() == {}


def test_create_sends_channel_list_verbatim():
    client, transport = make_client(lambda request: json_response(201, SUBSCRIBER))
    client.subscribers.create(subscriber_id="c1", channels=["in_app", "push"])
    assert transport.last_json()["channels"] == ["in_app", "push"]


def test_duplicate_id_surfaces_as_conflict():
    client, _ = make_client(
        lambda request: json_response(409, {"detail": "订阅者已存在：creator-1001"})
    )
    with pytest.raises(ConflictError, match="已存在"):
        client.subscribers.create(subscriber_id="creator-1001")


def test_subscriber_id_is_quoted_in_path():
    client, transport = make_client(lambda request: json_response(200, SUBSCRIBER))
    client.subscribers.get("creator/1001 小林")
    assert transport.last.url == "http://api.test/v1/subscribers/creator%2F1001%20%E5%B0%8F%E6%9E%97"


def test_blank_id_is_rejected_before_sending():
    client, transport = make_client()
    with pytest.raises(ValueError, match="subscriber_id 不能为空"):
        client.subscribers.get("   ")
    assert transport.requests == []


def test_list_passes_filters_and_parses_page():
    payload = {"items": [SUBSCRIBER], "nextCursor": "cur-2", "hasMore": True}
    client, transport = make_client(lambda request: json_response(200, payload))
    page = client.subscribers.list(channel="in_app", limit=10, cursor="cur-1")

    assert transport.last.url == (
        "http://api.test/v1/subscribers?channel=in_app&limit=10&cursor=cur-1"
    )
    assert len(page) == 1
    assert page.has_more is True
    assert page.next_cursor == "cur-2"


def test_list_without_filters_sends_no_query():
    client, transport = make_client(
        lambda request: json_response(200, {"items": [], "nextCursor": None})
    )
    page = client.subscribers.list()
    assert transport.last.url == "http://api.test/v1/subscribers"
    assert page.has_more is False
    assert list(page) == []


@pytest.mark.parametrize("limit", [0, 201, -5])
def test_list_rejects_out_of_range_limit(limit):
    client, transport = make_client()
    with pytest.raises(ValueError, match="limit"):
        client.subscribers.list(limit=limit)
    assert transport.requests == []


def test_iter_follows_cursor_until_null():
    pages = {
        None: {"items": [SUBSCRIBER], "nextCursor": "c2"},
        "c2": {"items": [dict(SUBSCRIBER, id="creator-1002")], "nextCursor": None},
    }

    def responder(request):
        cursor = None
        if "?" in request.url:
            from urllib.parse import parse_qs, urlparse

            cursor = parse_qs(urlparse(request.url).query).get("cursor", [None])[0]
        return json_response(200, pages[cursor])

    client, transport = make_client(responder)
    ids = [item.id for item in client.subscribers.iter(page_size=1)]
    assert ids == ["creator-1001", "creator-1002"]
    assert len(transport.requests) == 2


def test_iter_stops_when_cursor_repeats():
    def responder(request):
        return json_response(200, {"items": [SUBSCRIBER], "nextCursor": "same"})

    client, transport = make_client(responder)
    items = list(client.subscribers.iter())
    # 服务端没有推进游标时只取一页就停：第二次请求是为了发现「游标没动」，
    # 但它返回的仍是同一批数据，不能再交出去，否则调用方会看到重复项。
    assert len(items) == 1
    assert len(transport.requests) == 2


def test_update_omits_untouched_fields():
    client, transport = make_client(lambda request: json_response(200, SUBSCRIBER))
    client.subscribers.update("creator-1001", device_token="fcm-2")
    assert transport.last.method == "PUT"
    assert transport.last_json() == {"device_token": "fcm-2"}


def test_update_with_none_unbinds_address():
    client, transport = make_client(lambda request: json_response(200, SUBSCRIBER))
    client.subscribers.update("creator-1001", email=None, phone=None)
    assert transport.last_json() == {"email": None, "phone": None}


def test_update_channels_replacement_and_clear():
    client, transport = make_client(lambda request: json_response(200, SUBSCRIBER))
    client.subscribers.update("creator-1001", channels=["in_app"])
    assert transport.last_json() == {"channels": ["in_app"]}

    client.subscribers.update("creator-1001", channels=[])
    assert transport.last_json() == {"channels": []}


def test_update_with_none_channels_is_rejected():
    client, transport = make_client()
    with pytest.raises(ValueError, match="channels"):
        client.subscribers.update("creator-1001", channels=None)
    assert transport.requests == []


def test_update_without_fields_is_rejected():
    client, transport = make_client()
    with pytest.raises(ValueError, match="没有需要更新的字段"):
        client.subscribers.update("creator-1001")
    assert transport.requests == []


def test_delete_uses_item_path_and_accepts_no_content():
    client, transport = make_client(lambda request: empty_response(204))
    assert client.subscribers.delete("creator-1001") is None
    assert transport.last.method == "DELETE"
    assert transport.last.url == "http://api.test/v1/subscribers/creator-1001"
    assert transport.last.body is None


def test_delete_missing_subscriber_raises():
    client, _ = make_client(lambda request: json_response(404, {"detail": "订阅者不存在：ghost"}))
    with pytest.raises(NotFoundError):
        client.subscribers.delete("ghost")
