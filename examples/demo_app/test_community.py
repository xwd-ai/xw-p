"""演示应用的服务层测试：假传输层记录请求，不联网。

断言的重点是「业务侧把什么交给平台」：触发了哪个工作流、收件地址有没有
带上、幂等键有没有传。这些是接入 SDK 的产出；至于平台收到之后怎么分发，
由平台自己的测试负责。
"""
from __future__ import annotations

import json
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlsplit

from xw_notify import Client
from xw_notify._http import Request, Response

from examples.demo_app.community import Community, CommunityError


class FakeTransport:
    """记录请求、按路径返回预置响应的传输层替身。"""

    def __init__(self) -> None:
        self.requests: List[Request] = []
        self.inbox_payload: Dict[str, Any] = {"items": [], "unread": 0, "total": 0}
        self._triggered = 0

    def send(self, request: Request) -> Response:
        self.requests.append(request)
        path = urlsplit(request.url).path
        if request.method == "POST" and path == "/v1/events/trigger":
            self._triggered += 1
            return _json(200, {
                "triggerId": f"trig-{self._triggered}",
                "transactionId": f"tx-{self._triggered}",
                "acknowledgmentId": f"ack-{self._triggered}",
                "acknowledged": True,
            })
        if request.method == "GET" and path == "/v1/inbox":
            return _json(200, self.inbox_payload)
        return _json(404, {"detail": f"没有登记的路径：{request.method} {path}"})

    # -- 断言辅助 ---------------------------------------------------------

    @property
    def trigger_requests(self) -> List[Request]:
        return [r for r in self.requests if urlsplit(r.url).path == "/v1/events/trigger"]

    @property
    def last_trigger(self) -> Dict[str, Any]:
        return json.loads(self.trigger_requests[-1].body.decode("utf-8"))


def _json(status: int, payload: Any) -> Response:
    return Response(
        status=status,
        headers={"Content-Type": "application/json"},
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def _community() -> tuple[Community, FakeTransport]:
    transport = FakeTransport()
    community = Community(Client("http://xw.test", transport=transport))
    community.register_creator(
        "creator-1001", name="小林",
        email="lin@example.com", device_token="fcm-9f2c1d",
    )
    community.create_post("P-2026-1001", creator_id="creator-1001", title="我的第一支 Vlog")
    return community, transport


# -- 三类互动 ------------------------------------------------------------

def test_like_sends_post_liked_with_addresses() -> None:
    community, transport = _community()
    receipt = community.like("P-2026-1001", actor="阿澈")

    body = transport.last_trigger
    assert body["name"] == "post_liked"
    assert body["to"] == {
        "subscriberId": "creator-1001",
        "email": "lin@example.com",
        "deviceToken": "fcm-9f2c1d",
    }
    assert body["payload"] == {
        "post_id": "P-2026-1001",
        "post_title": "我的第一支 Vlog",
        "actor": "阿澈",
    }
    assert receipt.trigger_id == "trig-1"
    assert receipt.acknowledged is True


def test_comment_attaches_idempotency_key() -> None:
    community, transport = _community()
    community.comment("P-2026-1001", actor="阿澈", text="求教程", comment_id="C-8842")
    community.comment("P-2026-1001", actor="阿澈", text="求教程", comment_id="C-8842")

    requests = transport.trigger_requests
    assert len(requests) == 2
    # 同一条评论重发：两次请求带同一个幂等键，服务端只入队一次。
    assert [r.headers["Idempotency-Key"] for r in requests] == ["comment-C-8842"] * 2
    assert json.loads(requests[0].body)["name"] == "post_commented"
    assert json.loads(requests[0].body)["payload"]["comment"] == "求教程"


def test_comment_without_comment_id_sends_no_key() -> None:
    community, transport = _community()
    community.comment("P-2026-1001", actor="阿澈", text="沙发")

    request = transport.trigger_requests[0]
    assert "Idempotency-Key" not in request.headers


def test_mention_notifies_mentioned_creator() -> None:
    community, transport = _community()
    community.register_creator("creator-2002", name="阿澈", device_token="fcm-2222")
    community.mention("P-2026-1001", actor="小林", target="creator-2002", text="@阿澈 你也来看看")

    body = transport.last_trigger
    assert body["name"] == "post_mentioned"
    # 被 @ 的人收到通知，不是帖子作者。
    assert body["to"]["subscriberId"] == "creator-2002"
    assert body["to"]["deviceToken"] == "fcm-2222"
    assert body["payload"]["comment"] == "@阿澈 你也来看看"


# -- 校验与边界 ----------------------------------------------------------

def test_unknown_post_raises() -> None:
    community, _ = _community()
    try:
        community.like("P-missing", actor="阿澈")
    except CommunityError as error:
        assert "帖子不存在" in str(error)
    else:  # pragma: no cover - 断言失败路径
        raise AssertionError("预期 CommunityError")


def test_unknown_creator_raises() -> None:
    community, _ = _community()
    try:
        community.create_post("P-2", creator_id="nobody", title="x")
    except CommunityError as error:
        assert "创作者不存在" in str(error)
    else:  # pragma: no cover
        raise AssertionError("预期 CommunityError")


def test_empty_fields_raise() -> None:
    community, _ = _community()
    try:
        community.comment("P-2026-1001", actor="阿澈", text="   ")
    except CommunityError as error:
        assert "text 不能为空" in str(error)
    else:  # pragma: no cover
        raise AssertionError("预期 CommunityError")


def test_mention_requires_target_profile() -> None:
    community, transport = _community()
    try:
        community.mention("P-2026-1001", actor="小林", target="ghost")
    except CommunityError:
        pass
    else:  # pragma: no cover
        raise AssertionError("预期 CommunityError")
    # 档案缺失时请求不该发出去。
    assert not transport.trigger_requests


# -- 收件箱 --------------------------------------------------------------

def test_inbox_reads_page() -> None:
    community, transport = _community()
    transport.inbox_payload = {
        "items": [
            {"message_id": "m-1", "title": "3 人点赞了你的帖子", "workflow": "post_liked", "read": False},
            {"message_id": "m-2", "title": "小林评论了你的帖子", "workflow": "post_commented", "read": True},
        ],
        "unread": 1,
        "total": 2,
    }
    page = community.inbox("creator-1001")

    assert page["unread"] == 1 and page["total"] == 2
    assert [item["message_id"] for item in page["items"]] == ["m-1", "m-2"]
    request = transport.requests[-1]
    query = parse_qs(urlsplit(request.url).query)
    assert query["subscriberId"] == ["creator-1001"]
    assert query["limit"] == ["20"]
    assert "unread" not in query  # 只在显式要求时才过滤


def test_inbox_unread_flag_and_limit_clamped() -> None:
    community, transport = _community()
    community.inbox("creator-1001", unread=True, limit=1000)
    query = parse_qs(urlsplit(transport.requests[-1].url).query)
    assert query["unread"] == ["true"]
    assert query["limit"] == ["100"]
