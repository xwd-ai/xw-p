"""示例的可执行说明：跑一遍每个示例，断言它发了什么、打印了什么。

示例最容易腐烂——SDK 改了字段名、服务端换了路由，示例还在照着旧的写，而没人
会去跑它。这里用假传输层把三个示例各跑一遍：请求序列、请求体、幂等键、输出
文案都在断言范围内，改坏任何一个都会红。假传输层而不是真起服务，是为了让
「示例发了什么」可以被逐字节断言。
"""
from __future__ import annotations

import io
from typing import Any, Callable, Dict, List, Mapping, Optional
from urllib.parse import parse_qs, urlsplit

import pytest

from xw_notify import ProtocolError
from xw_notify._http import Request, Response

from examples import _shared, inbox_review, post_interaction, subscriber_preferences
from fakes import json_response, make_client

Handler = Callable[[Request], Response]


class Router:
    """按 ``(方法, 路径)`` 分发，并记录全部请求。

    未登记的请求直接断言失败：示例打到不存在的地址，是这类代码最典型的错，
    让它在测试里响一声，比在本地跑起来看到一句 404 强。
    """

    def __init__(self, routes: Dict[tuple, Handler]) -> None:
        self.routes = routes
        self.calls: List[Request] = []

    def __call__(self, request: Request) -> Response:
        self.calls.append(request)
        key = (request.method, urlsplit(request.url).path)
        handler = self.routes.get(key)
        if handler is None:
            raise AssertionError(f"示例发出了未登记的请求：{key}")
        return handler(request)

    # -- 断言辅助 ---------------------------------------------------------

    @property
    def sequence(self) -> List[tuple]:
        return [(call.method, urlsplit(call.url).path) for call in self.calls]

    def bodies(self, method: str, path: str) -> List[Any]:
        import json

        result = []
        for call in self.calls:
            if call.method == method and urlsplit(call.url).path == path:
                result.append(json.loads(call.body.decode("utf-8")) if call.body else None)
        return result

    def queries(self, method: str, path: str) -> List[Dict[str, List[str]]]:
        result = []
        for call in self.calls:
            if call.method == method and urlsplit(call.url).path == path:
                result.append(parse_qs(urlsplit(call.url).query))
        return result


def client_for(routes: Dict[tuple, Handler]) -> tuple:
    router = Router(routes)
    client, _ = make_client(router)
    return client, router


def ack_handler(counter: List[int]) -> Handler:
    """每次回执给一个新的触发标识，便于按顺序断言。"""

    def handler(request: Request) -> Response:
        counter[0] += 1
        return json_response(
            200,
            {
                "acknowledged": True,
                "triggerId": f"trg-{counter[0]}",
                "transactionId": f"txn-{counter[0]}",
                "acknowledgmentId": f"trg-{counter[0]}",
            },
        )

    return handler


def subscriber_payload(**overrides: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "id": "creator-1001",
        "environment": "development",
        "channels": ["in_app", "email", "push"],
        "bindings": {
            "in_app": "creator-1001",
            "email": "creator@example.com",
            "push": "fcm-9f2c1d",
        },
        "email": "creator@example.com",
    }
    payload.update(overrides)
    return payload


def batch_payload(*statuses: str) -> Dict[str, Any]:
    """按入参顺序造回执；``rejected`` 的条目没有触发标识，只有原因。"""
    results = []
    for index, status in enumerate(statuses):
        item = {
            "index": index,
            "name": "post_mentioned",
            "subscriberId": "creator-1001",
            "status": status,
        }
        if status == "accepted":
            item.update(
                {
                    "triggerId": f"trg-b{index}",
                    "transactionId": f"txn-b{index}",
                    "acknowledgmentId": f"trg-b{index}",
                }
            )
        else:
            item["error"] = "工作流不存在或未启用：post_mentioned"
        results.append(item)
    accepted = sum(1 for status in statuses if status == "accepted")
    return {
        "accepted": accepted,
        "rejected": len(statuses) - accepted,
        "results": results,
    }


# ---------------------------------------------------------------------------
# post_interaction
# ---------------------------------------------------------------------------

def test_post_interaction_walks_the_whole_flow():
    counter = [0]
    client, router = client_for(
        {
            ("POST", "/v1/subscribers"): lambda request: json_response(201, subscriber_payload()),
            ("POST", "/v1/events/trigger"): ack_handler(counter),
            ("POST", "/v1/events/trigger/batch"): lambda request: json_response(
                200, batch_payload("accepted", "rejected")
            ),
        }
    )
    out = io.StringIO()

    result = post_interaction.run(client, out)

    assert router.sequence == [
        ("POST", "/v1/subscribers"),
        ("POST", "/v1/events/trigger"),
        ("POST", "/v1/events/trigger"),
        ("POST", "/v1/events/trigger"),
        ("POST", "/v1/events/trigger"),
        ("POST", "/v1/events/trigger/batch"),
    ]
    text = out.getvalue()
    assert "新建订阅者 creator-1001，可投递渠道：email / in_app / push" in text
    assert "小林 点赞 → 已受理（触发 trg-1）" in text
    assert "共 2 条：受理 1 / 未受理 1" in text
    assert result["subscriber"]["created"] is True
    assert [item["trigger_id"] for item in result["likes"]] == ["trg-1", "trg-2", "trg-3"]
    assert result["comment"]["trigger_id"] == "trg-4"
    assert result["mentions"]["accepted"] == 1
    assert result["mentions"]["items"][1]["error"] == "工作流不存在或未启用：post_mentioned"


def test_post_interaction_aggregates_on_the_same_post_id():
    counter = [0]
    client, router = client_for(
        {
            ("POST", "/v1/subscribers"): lambda request: json_response(201, subscriber_payload()),
            ("POST", "/v1/events/trigger"): ack_handler(counter),
            ("POST", "/v1/events/trigger/batch"): lambda request: json_response(
                200, batch_payload("accepted", "accepted")
            ),
        }
    )

    post_interaction.run(client, io.StringIO())

    bodies = router.bodies("POST", "/v1/events/trigger")
    likes = [body for body in bodies if body["name"] == "post_liked"]
    assert len(likes) == 3
    # 聚合键必须一致，否则三条点赞会变成三张卡片。
    assert {body["payload"]["post_id"] for body in likes} == {post_interaction.POST_ID}
    assert [body["payload"]["actor"] for body in likes] == ["小林", "阿澈", "阿澈"]


def test_comment_carries_idempotency_key_and_channels():
    counter = [0]
    client, router = client_for(
        {
            ("POST", "/v1/subscribers"): lambda request: json_response(201, subscriber_payload()),
            ("POST", "/v1/events/trigger"): ack_handler(counter),
            ("POST", "/v1/events/trigger/batch"): lambda request: json_response(
                200, batch_payload("accepted", "accepted")
            ),
        }
    )

    post_interaction.run(client, io.StringIO())

    comment_calls = [
        call
        for call in router.calls
        if call.method == "POST" and urlsplit(call.url).path == "/v1/events/trigger"
        and b"post_commented" in (call.body or b"")
    ]
    assert len(comment_calls) == 1
    assert comment_calls[0].headers["Idempotency-Key"] == post_interaction.COMMENT_KEY


def test_existing_subscriber_is_reused_instead_of_failing():
    counter = [0]
    client, router = client_for(
        {
            ("POST", "/v1/subscribers"): lambda request: json_response(
                409, {"detail": "订阅者已存在：creator-1001"}
            ),
            ("GET", "/v1/subscribers/creator-1001"): lambda request: json_response(
                200, subscriber_payload()
            ),
            ("POST", "/v1/events/trigger"): ack_handler(counter),
            ("POST", "/v1/events/trigger/batch"): lambda request: json_response(
                200, batch_payload("accepted", "accepted")
            ),
        }
    )
    out = io.StringIO()

    result = post_interaction.run(client, out)

    assert router.sequence[:2] == [("POST", "/v1/subscribers"), ("GET", "/v1/subscribers/creator-1001")]
    assert "已存在订阅者 creator-1001" in out.getvalue()
    assert result["subscriber"]["created"] is False


def test_disabled_workflow_is_reported_per_event():
    client, router = client_for(
        {
            ("POST", "/v1/subscribers"): lambda request: json_response(201, subscriber_payload()),
            ("POST", "/v1/events/trigger"): lambda request: json_response(
                404, {"detail": "工作流不存在或未启用：post_liked"}
            ),
            ("POST", "/v1/events/trigger/batch"): lambda request: json_response(
                200, batch_payload("rejected", "rejected")
            ),
        }
    )
    out = io.StringIO()

    result = post_interaction.run(client, out)

    text = out.getvalue()
    assert "未受理：工作流不存在或未启用：post_liked" in text
    assert all(item["trigger_id"] is None for item in result["likes"])
    assert all(item["error"] for item in result["likes"])
    assert result["comment"]["error"] == "工作流不存在或未启用：post_liked"
    assert result["mentions"]["accepted"] == 0


# ---------------------------------------------------------------------------
# subscriber_preferences
# ---------------------------------------------------------------------------

def resolve_payload(allowed: bool, scope: Optional[str], target: Optional[str] = None) -> Dict[str, Any]:
    return {
        "allowed": allowed,
        "matched": scope is not None,
        "scope": scope,
        "target": target,
        "reason": "命中 %s 层" % scope if scope else "没有记录，按默认接收",
    }


def preferences_routes() -> Dict[tuple, Handler]:
    # 清空之后服务端就没有记录了，解析应当回到「默认接收」——用一个状态位
    # 模拟这条前后差别，否则「清空后仍然被拦」这种回归在测试里看不出来。
    state = {"cleared": False}

    def resolve(request: Request) -> Response:
        if state["cleared"]:
            return json_response(200, resolve_payload(True, None))
        query = parse_qs(urlsplit(request.url).query)
        workflow = (query.get("workflow") or [None])[0]
        channel = (query.get("channel") or [None])[0]
        if channel == "push":
            return json_response(200, resolve_payload(True, "channel", "push"))
        if workflow == "post_mentioned":
            return json_response(200, resolve_payload(True, "workflow", "post_mentioned"))
        return json_response(200, resolve_payload(False, "global"))

    def listing(request: Request) -> Response:
        if state["cleared"]:
            return json_response(
                200,
                {
                    "subscriber_id": subscriber_preferences.SUBSCRIBER,
                    "items": [],
                    "counts": {"global": 0, "workflow": 0, "channel": 0},
                    "default_allowed": True,
                },
            )
        return json_response(
            200,
            {
                "subscriber_id": subscriber_preferences.SUBSCRIBER,
                "items": [
                    {"scope": "channel", "target": "push", "enabled": True},
                    {"scope": "workflow", "target": "post_mentioned", "enabled": True},
                    {"scope": "global", "target": None, "enabled": False},
                ],
                "counts": {"global": 1, "workflow": 1, "channel": 1},
                "default_allowed": True,
            },
        )

    def remove(request: Request) -> Response:
        state["cleared"] = True
        return json_response(200, {"removed": 3})

    return {
        ("POST", "/v1/subscribers"): lambda request: json_response(
            201, subscriber_payload(id=subscriber_preferences.SUBSCRIBER)
        ),
        ("PUT", "/v1/subscribers/%s/preferences" % subscriber_preferences.SUBSCRIBER): lambda request: json_response(
            200, {"created": True, "scope": "global", "enabled": False}
        ),
        ("GET", "/v1/subscribers/%s/preferences" % subscriber_preferences.SUBSCRIBER): listing,
        ("GET", "/v1/subscribers/%s/preferences/resolve" % subscriber_preferences.SUBSCRIBER): resolve,
        ("DELETE", "/v1/subscribers/%s/preferences" % subscriber_preferences.SUBSCRIBER): remove,
    }


def test_preferences_example_sets_three_layers_in_order():
    client, router = client_for(preferences_routes())

    result = subscriber_preferences.run(client, io.StringIO())

    writes = router.bodies(
        "PUT", "/v1/subscribers/%s/preferences" % subscriber_preferences.SUBSCRIBER
    )
    assert writes == [
        {"scope": "global", "target": None, "enabled": False},
        {"scope": "workflow", "target": "post_mentioned", "enabled": True},
        {"scope": "channel", "target": "push", "enabled": True},
    ]
    assert [row["scope"] for row in result["resolutions"]] == ["workflow", "global", "channel", "global"]
    assert result["removed"] == 3
    assert result["after_clear_allowed"] is True


def test_preferences_example_prints_which_layer_decided():
    client, _ = client_for(preferences_routes())
    out = io.StringIO()

    subscriber_preferences.run(client, out)

    text = out.getvalue()
    assert "被 @ 时收到站内信：会发（命中 workflow）" in text
    assert "被点赞时收到站内信：不发（命中 global）" in text
    assert "被点赞时收到推送：会发（命中 channel）" in text
    assert "共 3 条（global 1 / workflow 1 / channel 1）" in text


def test_preferences_clear_restores_default():
    client, router = client_for(preferences_routes())
    out = io.StringIO()

    subscriber_preferences.run(client, out)

    # 清空之后示例会再解析一次，此时服务端已经没有记录，命中层级为空。
    assert "命中 默认" in out.getvalue()
    assert router.queries(
        "DELETE", "/v1/subscribers/%s/preferences" % subscriber_preferences.SUBSCRIBER
    ) == [{}]


# ---------------------------------------------------------------------------
# inbox_review
# ---------------------------------------------------------------------------

def inbox_item(message_id: str, title: str, workflow: str, read: bool = False) -> Dict[str, Any]:
    return {
        "message_id": message_id,
        "trigger_id": message_id.split(":")[0],
        "title": title,
        "body": "正文",
        "workflow": workflow,
        "read": read,
        "created_at": "2026-11-04T09:30:00+00:00",
        "updated_at": "2026-11-04T09:30:00+00:00",
    }


def inbox_routes(items: List[Any], *, unread: int = 2, total: int = 4) -> Dict[tuple, Handler]:
    return {
        ("GET", "/v1/inbox"): lambda request: json_response(
            200,
            {
                "items": items,
                "unread": unread,
                "total": total,
                "matched": len(items),
                "hasMore": False,
                "nextCursor": None,
            },
        ),
        ("GET", "/v1/inbox/unread"): lambda request: json_response(
            200,
            {
                "unread": unread,
                "total": total,
                "byWorkflow": [
                    {"workflow": "post_liked", "unread": 1, "total": 2},
                    {"workflow": "post_mentioned", "unread": 1, "total": 1},
                ],
                "latestAt": "2026-11-04T09:30:00+00:00",
            },
        ),
        ("POST", "/v1/inbox/read"): lambda request: json_response(
            200, {"marked": 2, "unread": 1}
        ),
    }


def test_inbox_example_reads_and_marks_the_first_page():
    items = [
        inbox_item("trg-1:in_app", "3 人点赞了你的帖子", "post_liked"),
        inbox_item("trg-2:in_app", "阿澈 评论了你的帖子", "post_commented"),
    ]
    client, router = client_for(inbox_routes(items))
    out = io.StringIO()

    result = inbox_review.run(client, out)

    assert router.queries("GET", "/v1/inbox") == [
        {"subscriberId": [inbox_review.SUBSCRIBER], "limit": [str(inbox_review.PAGE_SIZE)], "unread": ["true"]}
    ]
    assert router.bodies("POST", "/v1/inbox/read") == [
        {
            "subscriberId": inbox_review.SUBSCRIBER,
            "messageIds": ["trg-1:in_app", "trg-2:in_app"],
        }
    ]
    text = out.getvalue()
    assert "未读 2 条 / 共 4 条，本页 2 条" in text
    assert "[未读] 3 人点赞了你的帖子（post_liked）" in text
    assert "实际变更 2 条，剩余未读 1 条" in text
    assert result["marked"] == 2
    assert result["unread_after"] == 1
    assert result["distribution"][0] == {"workflow": "post_liked", "unread": 1}


def test_inbox_example_skips_read_request_when_page_is_empty():
    client, router = client_for(inbox_routes([], unread=0, total=0))
    out = io.StringIO()

    result = inbox_review.run(client, out)

    assert ("POST", "/v1/inbox/read") not in router.sequence
    assert "本页没有可标记的消息" in out.getvalue()
    assert result["marked"] == 0


def test_inbox_example_ignores_malformed_rows_and_missing_ids():
    items = [
        "这不是一条消息",
        {"title": None, "workflow": None},
        {"message_id": "trg-9:in_app", "title": "阿澈 @ 了你", "workflow": "post_mentioned"},
    ]
    client, router = client_for(inbox_routes(items, unread=3, total=3))
    out = io.StringIO()

    inbox_review.run(client, out)

    # 没有标识的条目不能进 messageIds：发过去只会得到 marked=0 的假回执。
    assert router.bodies("POST", "/v1/inbox/read") == [
        {"subscriberId": inbox_review.SUBSCRIBER, "messageIds": ["trg-9:in_app"]}
    ]
    assert "[未读] 新通知（未知工作流）" in out.getvalue()


def test_inbox_example_rejects_unexpected_response_shape():
    client, _ = client_for(
        {
            ("GET", "/v1/inbox"): lambda request: json_response(200, []),
        }
    )

    with pytest.raises(ProtocolError) as info:
        inbox_review.run(client, io.StringIO())

    assert "收件箱列表" in str(info.value)


# ---------------------------------------------------------------------------
# 共用的装配逻辑
# ---------------------------------------------------------------------------

def test_client_from_env_uses_overrides():
    client = _shared.client_from_env(
        {
            _shared.BASE_URL_ENV: " https://notify.example.com ",
            _shared.API_KEY_ENV: " key-123 ",
            _shared.ENVIRONMENT_ENV: " production ",
        }
    )

    assert client.base_url == "https://notify.example.com"
    assert client.api_key == "key-123"
    assert client.environment == "production"


def test_client_from_env_falls_back_to_defaults():
    client = _shared.client_from_env({})

    assert client.base_url == "http://127.0.0.1:8000"
    assert client.environment == "development"
    assert client.api_key is None


def test_client_from_env_treats_blank_key_as_absent():
    client = _shared.client_from_env({_shared.API_KEY_ENV: "   "})

    assert client.api_key is None
    assert "未配置" in repr(client)


def test_every_example_exposes_run_and_main():
    for module in (post_interaction, subscriber_preferences, inbox_review):
        assert callable(module.run)
        assert callable(module.main)
        # run 的第一个参数是 client、第二个是输出流：示例因此可以离线跑。
        params = list(module.run.__code__.co_varnames[:2])
        assert params == ["client", "out"]
