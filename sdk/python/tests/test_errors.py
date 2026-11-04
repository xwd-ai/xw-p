"""异常映射与错误信息整理。"""
from __future__ import annotations

import pytest

from xw_notify import (
    APIError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    RateLimitError,
    ServerError,
    ValidationError,
    XWNotifyError,
)
from xw_notify.errors import describe_detail, error_for_status


def test_status_maps_to_dedicated_class():
    assert isinstance(error_for_status(400, {"detail": "未知环境"}), BadRequestError)
    assert isinstance(error_for_status(401, {"detail": "缺少 Key"}), AuthenticationError)
    assert isinstance(error_for_status(404, {"detail": "不存在"}), NotFoundError)
    assert isinstance(error_for_status(409, {"detail": "重复"}), ConflictError)
    assert isinstance(error_for_status(422, {"detail": []}), ValidationError)
    assert isinstance(error_for_status(429, {"detail": "太快"}), RateLimitError)
    assert isinstance(error_for_status(503, {"detail": "维护中"}), ServerError)


def test_unknown_status_falls_back_to_base_error():
    error = error_for_status(418, {"detail": "我是茶壶"})
    assert type(error) is APIError
    assert error.status == 418


def test_message_includes_request_and_server_reason():
    error = error_for_status(
        404, {"detail": "订阅者不存在：creator-1001"}, method="GET", path="/v1/subscribers/creator-1001"
    )
    assert str(error) == "GET /v1/subscribers/creator-1001 返回 404：订阅者不存在：creator-1001"
    assert error.message == "订阅者不存在：creator-1001"
    assert error.detail == "订阅者不存在：creator-1001"


def test_validation_detail_uses_field_paths():
    detail = [
        {
            "type": "value_error",
            "loc": ["body", "to", "email"],
            "msg": "邮箱格式不正确",
            "input": "not-an-email",
        },
        {
            "type": "missing",
            "loc": ["body", "name"],
            "msg": "Field required",
        },
    ]
    error = error_for_status(422, {"detail": detail})
    assert isinstance(error, ValidationError)
    assert error.message == "to.email: 邮箱格式不正确；name: Field required"
    assert len(error.errors) == 2


def test_query_location_is_kept_without_container_prefix():
    detail = [{"loc": ["query", "limit"], "msg": "必须小于等于 200"}]
    assert error_for_status(422, {"detail": detail}).message == "limit: 必须小于等于 200"


def test_detail_as_object_uses_msg():
    error = error_for_status(400, {"detail": {"msg": "游标不合法"}})
    assert error.message == "游标不合法"


def test_non_json_error_body_keeps_preview():
    error = error_for_status(502, "<html>Bad Gateway</html>")
    assert "Bad Gateway" in error.message
    assert error.payload == "<html>Bad Gateway</html>"


def test_empty_detail_explains_itself():
    error = error_for_status(500, {})
    assert error.message == "服务端未给出原因"


def test_all_errors_share_base_class():
    assert issubclass(NotFoundError, XWNotifyError)
    assert issubclass(ServerError, APIError)


def test_long_preview_is_truncated():
    detail = [{"msg": "x" * 2000}]
    message = error_for_status(422, {"detail": detail}).message
    assert len(message) < 2100


def test_describe_detail_handles_none_and_scalars():
    assert describe_detail(None) == ""
    assert describe_detail("  空白  ") == "空白"
    assert describe_detail(42) == "42"


@pytest.mark.parametrize("status", [400, 401, 404, 409, 422, 429])
def test_client_errors_are_not_server_errors(status):
    error = error_for_status(status, {"detail": "原因"})
    assert not isinstance(error, ServerError)
