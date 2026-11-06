"""HTTP 路由层测试：FastAPI TestClient + 假传输层。

路由只是「解析请求 → 调服务 → 折响应」，这里只验接线正确：状态码、
回执透传、业务错误折成 404。通知语义在 test_community.py 里断言。
"""
from __future__ import annotations

import json
from typing import List

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402
from xw_notify import Client  # noqa: E402
from xw_notify._http import Request, Response  # noqa: E402

from demo.app import create_app  # noqa: E402


class FakeTransport:
    def __init__(self) -> None:
        self.requests: List[Request] = []
        self._triggered = 0

    def send(self, request: Request) -> Response:
        self.requests.append(request)
        if request.method == "GET" and "/v1/inbox?" in request.url:
            return Response(status=200, headers={}, body=b'{"items":[],"unread":0,"total":0}')
        if request.method == "POST" and request.url.endswith("/v1/events/trigger"):
            self._triggered += 1
            body = json.dumps({
                "triggerId": f"trig-{self._triggered}",
                "transactionId": "tx-1",
                "acknowledgmentId": "ack-1",
                "acknowledged": True,
            }).encode("utf-8")
            return Response(status=200, headers={"Content-Type": "application/json"}, body=body)
        return Response(status=404, headers={}, body=b"{}")


def _client() -> tuple[TestClient, FakeTransport]:
    transport = FakeTransport()
    app = create_app(Client("http://xw.test", transport=transport))
    return TestClient(app), transport


def _seed(client: TestClient, auth: tuple[str, str] | None = None) -> None:
    assert client.post("/creators", json={
        "id": "creator-1001", "name": "小林",
        "email": "lin@example.com", "device_token": "fcm-9f2c1d",
    }, auth=auth).status_code == 201
    assert client.post("/posts", json={
        "id": "P-1", "creator_id": "creator-1001", "title": "第一支 Vlog",
    }, auth=auth).status_code == 201


def test_like_endpoint_returns_ack() -> None:
    client, transport = _client()
    _seed(client)
    response = client.post("/posts/P-1/like", json={"actor": "阿澈"})
    assert response.status_code == 200
    assert response.json() == {
        "triggerId": "trig-1",
        "transactionId": "tx-1",
        "acknowledgmentId": "ack-1",
        "acknowledged": True,
    }
    assert json.loads(transport.requests[-1].body)["name"] == "post_liked"


def test_unknown_post_maps_to_404() -> None:
    client, _ = _client()
    _seed(client)
    response = client.post("/posts/P-missing/like", json={"actor": "阿澈"})
    assert response.status_code == 404
    assert "帖子不存在" in response.json()["detail"]


def test_comment_endpoint_passes_idempotency_key() -> None:
    client, transport = _client()
    _seed(client)
    client.post("/posts/P-1/comment", json={
        "actor": "阿澈", "text": "求教程", "comment_id": "C-8842",
    })
    assert transport.requests[-1].headers["Idempotency-Key"] == "comment-C-8842"


def test_health_endpoint() -> None:
    client, _ = _client()
    assert client.get("/health").json() == {"status": "ok"}


def test_guest_can_interact_but_cannot_publish(monkeypatch) -> None:
    monkeypatch.setenv("XW_DEMO_OWNER_PASSWORD", "private-creator")
    monkeypatch.setenv("XW_DEMO_GUEST_PASSWORD", "community-demo")
    client, transport = _client()
    owner = ("owner", "private-creator")
    guest = ("guest", "community-demo")
    _seed(client, auth=owner)

    post = {"id": "P-2", "creator_id": "creator-1001", "title": "第二支 Vlog"}
    assert client.get("/session", auth=guest).json() == {"role": "guest"}
    assert client.get("/session", auth=owner).json() == {"role": "owner"}
    assert client.get("/posts").json() == [{
        "post_id": "P-1", "creator_id": "creator-1001", "creator_name": "小林",
        "title": "第一支 Vlog", "body": "", "likes": 0, "comments": [],
    }]
    assert client.post("/posts", json=post).status_code == 401
    assert client.post("/posts", json=post, auth=("owner", guest[1])).status_code == 401
    assert client.post("/posts", json=post, auth=guest).status_code == 403
    assert client.post("/creators", json={"id": "other", "name": "小夏"}, auth=guest).status_code == 403
    assert client.get("/creators/creator-1001/inbox", auth=guest).status_code == 403
    assert client.post("/posts/P-1/like", json={"actor": "阿澈"}).status_code == 401
    assert client.post("/posts/P-1/like", json={"actor": "阿澈"}, auth=guest).status_code == 200
    assert client.get("/community-overview", auth=guest).status_code == 403
    assert client.post("/posts/P-1/comment", json={"actor": "owner", "text": "好看"}, auth=guest).status_code == 200
    assert client.post("/posts/P-1/mention", json={"actor": "阿澈", "target": "creator-1001"}, auth=guest).status_code == 403
    assert [json.loads(request.body)["name"] for request in transport.requests] == ["post_liked", "post_commented"]
    visible = client.get("/posts").json()[0]
    assert visible["likes"] == 1
    assert visible["comments"] == [{"id": "trig-2", "actor": "guest", "text": "好看"}]
    assert client.get("/community-overview", auth=owner).json() == {
        "posts": 1, "likes": 1, "unread": 0, "notifications": [],
    }


def test_community_page_serves_frontend_assets() -> None:
    client, _ = _client()
    page = client.get("/", follow_redirects=False)
    assert page.status_code == 200
    assert "社区动态" in page.text
    assert client.get("/community-assets/community.js").status_code == 200
    assert client.get("/community-assets/community.css").status_code == 200


def test_guest_requires_distinct_owner_password(monkeypatch) -> None:
    monkeypatch.setenv("XW_DEMO_OWNER_PASSWORD", "community-demo")
    monkeypatch.setenv("XW_DEMO_GUEST_PASSWORD", "community-demo")
    with pytest.raises(ValueError, match="不同的口令"):
        _client()
