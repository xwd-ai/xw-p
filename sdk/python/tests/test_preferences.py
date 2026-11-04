"""偏好接口：写入、删除、解析。"""
from __future__ import annotations

import pytest

from xw_notify import Preference, PreferenceList, Resolution

from fakes import json_response, make_client

LISTING = {
    "subscriber_id": "creator-1001",
    "items": [
        {"scope": "channel", "target": "email", "enabled": False, "updated_at": "2026-11-04T02:10:00+00:00"},
        {"scope": "global", "target": None, "enabled": True, "updated_at": "2026-11-04T01:00:00+00:00"},
    ],
    "counts": {"global": 1, "workflow": 0, "channel": 1},
    "default_allowed": True,
}


def test_list_parses_records_and_counts():
    client, transport = make_client(lambda request: json_response(200, LISTING))
    result = client.subscribers.preferences.list("creator-1001")

    assert transport.last.url == "http://api.test/v1/subscribers/creator-1001/preferences"
    assert isinstance(result, PreferenceList)
    assert len(result) == 2
    assert result.counts == {"global": 1, "workflow": 0, "channel": 1}
    assert result.default_allowed is True
    assert result.items[0] == Preference(
        scope="channel", target="email", enabled=False, updated_at="2026-11-04T02:10:00+00:00"
    )


def test_list_filters_by_scope():
    client, transport = make_client(lambda request: json_response(200, LISTING))
    client.subscribers.preferences.list("creator-1001", scope="CHANNEL ")
    assert transport.last.url.endswith("/preferences?scope=channel")


@pytest.mark.parametrize("scope", ["tenant", "", "  "])
def test_unknown_scope_is_rejected_locally(scope):
    client, transport = make_client()
    with pytest.raises(ValueError, match="未知层级"):
        client.subscribers.preferences.list("creator-1001", scope=scope)
    assert transport.requests == []


def test_set_global_has_no_target():
    client, transport = make_client(
        lambda request: json_response(
            200, {"scope": "global", "target": None, "enabled": False, "updated_at": "t", "created": True}
        )
    )
    result = client.subscribers.preferences.deny("creator-1001", "global")

    assert transport.last.method == "PUT"
    assert transport.last_json() == {"scope": "global", "target": None, "enabled": False}
    assert result.created is True
    assert result.enabled is False
    assert result.scope == "global"


def test_set_channel_requires_target():
    client, transport = make_client()
    with pytest.raises(ValueError, match="必须给出 target"):
        client.subscribers.preferences.set("creator-1001", "channel")
    assert transport.requests == []


def test_global_scope_rejects_target():
    client, transport = make_client()
    with pytest.raises(ValueError, match="global 层没有 target"):
        client.subscribers.preferences.set("creator-1001", "global", target="email")
    assert transport.requests == []


def test_allow_and_deny_flip_enabled():
    client, transport = make_client(
        lambda request: json_response(
            200, {"scope": "workflow", "target": "post_liked", "enabled": True, "updated_at": "t", "created": False}
        )
    )
    allowed = client.subscribers.preferences.allow("creator-1001", "workflow", target="post_liked")
    assert transport.last_json()["enabled"] is True
    assert allowed.created is False

    client.subscribers.preferences.deny("creator-1001", "workflow", target="post_liked")
    assert transport.last_json()["enabled"] is False


def test_delete_one_record_returns_removed_count():
    client, transport = make_client(lambda request: json_response(200, {"removed": 1}))
    removed = client.subscribers.preferences.delete(
        "creator-1001", scope="channel", target="email"
    )
    assert transport.last.method == "DELETE"
    assert transport.last.url.endswith("/preferences?scope=channel&target=email")
    assert removed == 1


def test_clear_removes_every_record():
    client, transport = make_client(lambda request: json_response(200, {"removed": 3}))
    assert client.subscribers.preferences.clear("creator-1001") == 3
    assert transport.last.url.endswith("/preferences")
    assert "?" not in transport.last.url


def test_delete_with_target_requires_scope():
    client, transport = make_client()
    with pytest.raises(ValueError, match="同时给出 scope"):
        client.subscribers.preferences.delete("creator-1001", target="email")
    assert transport.requests == []


def test_resolve_returns_decision_with_reason():
    client, transport = make_client(
        lambda request: json_response(
            200,
            {
                "allowed": False,
                "scope": "channel",
                "target": "email",
                "matched": True,
                "reason": "渠道 email 已关闭（偏好：渠道）",
            },
        )
    )
    decision = client.subscribers.preferences.resolve(
        "creator-1001", workflow="post_commented", channel="email"
    )

    assert transport.last.url.endswith(
        "/preferences/resolve?workflow=post_commented&channel=email"
    )
    assert isinstance(decision, Resolution)
    assert decision.allowed is False
    assert bool(decision) is False
    assert decision.scope == "channel"
    assert "已关闭" in decision.reason


def test_resolve_without_filters_sends_no_query():
    client, transport = make_client(
        lambda request: json_response(
            200, {"allowed": True, "matched": False, "reason": "未设置偏好，按接收处理"}
        )
    )
    decision = client.subscribers.preferences.resolve("creator-1001")
    assert transport.last.url.endswith("/preferences/resolve")
    assert bool(decision) is True
    assert decision.matched is False
