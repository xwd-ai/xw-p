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


def _seed(client: TestClient) -> None:
    assert client.post("/creators", json={
        "id": "creator-1001", "name": "小林",
        "email": "lin@example.com", "device_token": "fcm-9f2c1d",
    }).status_code == 201
    assert client.post("/posts", json={
        "id": "P-1", "creator_id": "creator-1001", "title": "第一支 Vlog",
    }).status_code == 201


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
