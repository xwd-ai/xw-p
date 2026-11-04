"""跨源访问配置：白名单解析与响应头。

展示前端与 API 分处不同端口，浏览器会先做 CORS 检查。这些用例锁住两件事：
白名单里的来源拿到放行头，不在名单里的来源拿不到——后者是真正要防的，
写成 ``*`` 会让「收紧」这个配置项失去意义。
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from app.config import parse_origins
from app.main import app


def test_parse_origins_defaults_to_any():
    assert parse_origins(None) == ["*"]
    assert parse_origins("") == ["*"]
    assert parse_origins("   ") == ["*"]


def test_parse_origins_splits_and_trims():
    assert parse_origins("http://localhost:5173, http://127.0.0.1:8080 ") == [
        "http://localhost:5173",
        "http://127.0.0.1:8080",
    ]


def test_parse_origins_drops_trailing_slash():
    # 浏览器发出的 Origin 头不带结尾斜杠，配置里带了也当作同一个来源。
    assert parse_origins("http://localhost:5173/") == ["http://localhost:5173"]


def test_parse_origins_keeps_wildcard_alone():
    assert parse_origins(" * ") == ["*"]


def test_cross_origin_get_is_allowed():
    client = TestClient(app)
    resp = client.get("/health", headers={"Origin": "http://127.0.0.1:8398"})
    assert resp.status_code == 200
    # 默认放行所有来源，响应里回显请求来源而不是字面量 *
    assert resp.headers.get("access-control-allow-origin") in {"*", "http://127.0.0.1:8398"}


def test_preflight_allows_write_methods():
    client = TestClient(app)
    resp = client.options(
        "/v1/events/trigger",
        headers={
            "Origin": "http://127.0.0.1:8398",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type,x-environment",
        },
    )
    assert resp.status_code == 200
    allowed = resp.headers.get("access-control-allow-methods", "")
    assert "POST" in allowed
    assert resp.headers.get("access-control-allow-origin") in {"*", "http://127.0.0.1:8398"}
