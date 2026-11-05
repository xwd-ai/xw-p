"""结构化日志测试：字段口径、请求标识生成与贯穿、访问日志状态分级。"""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import config
from app.log_context import (
    bind_request,
    log_fields,
    new_request_id,
    normalize_request_id,
)
from app.logging_setup import (
    JsonFormatter,
    TextFormatter,
    build_formatter,
    configure_logging,
    resolve_level,
)
from app.request_log import SKIP_PREFIXES, RequestLogMiddleware

_counter = 0


def _logger_with(formatter: logging.Formatter) -> tuple[logging.Logger, io.StringIO]:
    """独立的 logger + 内存 handler：不受根 logger 上其它 handler 干扰。"""
    global _counter
    _counter += 1
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)
    logger = logging.getLogger(f"xw.test.logging.{_counter}")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return logger, stream


def _last(stream: io.StringIO) -> dict:
    lines = [line for line in stream.getvalue().splitlines() if line.strip()]
    assert lines, "没有写出日志"
    return json.loads(lines[-1])


def _restore_logging() -> None:
    configure_logging("INFO", "json", force=True)


# --------------------------------------------------------------------------
# formatter
# --------------------------------------------------------------------------


def test_json_formatter_shape():
    logger, stream = _logger_with(JsonFormatter())
    logger.info("已触发：%s", "post_liked", extra={"event": "trigger", "status": 200})

    payload = _last(stream)
    assert payload["level"] == "info"
    assert payload["logger"].startswith("xw.test.logging.")
    assert payload["message"] == "已触发：post_liked"
    assert payload["event"] == "trigger"
    assert payload["status"] == 200
    # 时间戳可直接解析，且带 UTC 偏移标记
    assert datetime.fromisoformat(payload["ts"].replace("Z", "+00:00")).tzinfo is not None


def test_json_formatter_keeps_non_ascii():
    logger, stream = _logger_with(JsonFormatter())
    logger.warning("工作流不存在：%s", "post_mentioned")

    raw = stream.getvalue()
    assert "工作流不存在" in raw  # 不转成 \uXXXX，日志文件里能直接读
    assert "\\u" not in raw


def test_json_formatter_stringifies_unknown_values():
    logger, stream = _logger_with(JsonFormatter())

    class Opaque:
        def __str__(self) -> str:
            return "opaque-value"

    logger.info("x", extra={"detail": Opaque(), "nested": {"count": 3, "tags": ("a",)}})

    payload = _last(stream)
    assert payload["detail"] == "opaque-value"
    assert payload["nested"] == {"count": 3, "tags": ["a"]}


def test_json_formatter_includes_exception():
    logger, stream = _logger_with(JsonFormatter())
    try:
        raise ValueError("坏消息")
    except ValueError:
        logger.exception("处理失败")

    payload = _last(stream)
    assert payload["exc_type"] == "ValueError"
    assert "坏消息" in payload["exc"]
    assert "Traceback" in payload["exc"]


def test_context_fields_cannot_be_overridden_by_extra():
    """``request_id`` 由上下文注入：extra 传同名键会被 logging 拒绝。

    这是「同一个标识只有一个来源」的保证——否则某个调用点随手传的
    ``request_id`` 会让同一行日志里的标识与响应头对不上。
    """
    logger, _stream = _logger_with(JsonFormatter())
    with bind_request("req-real"):
        with pytest.raises(KeyError):
            logger.info("x", extra={"request_id": "req-forged"})


def test_uvicorn_color_message_is_dropped():
    """uvicorn 的 ``color_message`` 只是带 ANSI 的同一句话，不落进 JSON。"""
    logger, stream = _logger_with(JsonFormatter())
    logger.info("Started server process", extra={"color_message": "\x1b[36m[1]\x1b[0m"})

    payload = _last(stream)
    assert payload["message"] == "Started server process"
    assert "color_message" not in payload
    assert "\\u001b" not in stream.getvalue()


def test_json_formatter_never_raises_on_broken_record():
    formatter = JsonFormatter()
    record = logging.LogRecord("xw.test", logging.INFO, __file__, 1, "msg", (), None)
    # 消息格式化本身炸掉（参数与占位符对不上）时不能把日志系统一起带崩
    record.msg = "%d"
    record.args = ("不是数字",)
    output = formatter.format(record)
    assert json.loads(output)["message"]


# --------------------------------------------------------------------------
# 请求上下文
# --------------------------------------------------------------------------


def test_bind_request_sets_and_resets_fields():
    assert log_fields() == {}
    with bind_request("req-1", tenant="acme", environment="production") as rid:
        assert rid == "req-1"
        assert log_fields() == {
            "request_id": "req-1",
            "tenant": "acme",
            "environment": "production",
        }
    assert log_fields() == {}


def test_bind_request_nested_restores_outer():
    with bind_request("req-outer", tenant="acme"):
        with bind_request("req-inner", tenant="other"):
            assert log_fields()["request_id"] == "req-inner"
        assert log_fields()["request_id"] == "req-outer"
        assert log_fields()["tenant"] == "acme"


def test_bind_request_replaces_invalid_id():
    with bind_request("bad id\twith spaces") as rid:
        assert rid.startswith("req-")
        assert rid == log_fields()["request_id"]
    assert log_fields() == {}


def test_normalize_request_id_rules():
    assert normalize_request_id("trace-abc_1.2:3") == "trace-abc_1.2:3"
    assert normalize_request_id("  padded  ") == "padded"
    assert normalize_request_id("has\nnewline") == ""
    assert normalize_request_id("x" * 65) == ""
    assert normalize_request_id("x" * 64) == "x" * 64
    assert normalize_request_id("") == ""
    assert normalize_request_id(None) == ""


def test_new_request_id_unique_and_prefixed():
    ids = {new_request_id() for _ in range(200)}
    assert len(ids) == 200
    assert all(item.startswith("req-") and len(item) == 20 for item in ids)


def test_invalid_tenant_label_is_dropped():
    with bind_request("req-1", tenant="bad tenant", environment="production"):
        fields = log_fields()
    assert "tenant" not in fields
    assert fields["environment"] == "production"


# --------------------------------------------------------------------------
# 装配
# --------------------------------------------------------------------------


def test_configure_logging_is_idempotent():
    try:
        first = configure_logging("INFO", "json", force=True)
        second = configure_logging("DEBUG", "json")
        handlers = [
            item for item in logging.getLogger().handlers if getattr(item, "_xw_handler", False)
        ]
        assert len(handlers) == 1
        assert handlers[0] is first is second
        assert logging.getLogger().level == logging.DEBUG
        assert isinstance(first.formatter, JsonFormatter)
    finally:
        _restore_logging()


def test_configure_logging_switches_format_in_place():
    try:
        handler = configure_logging("INFO", "json", force=True)
        configure_logging("INFO", "text")
        assert isinstance(handler.formatter, TextFormatter)
        configure_logging("INFO", "json")
        assert isinstance(handler.formatter, JsonFormatter)
    finally:
        _restore_logging()


def test_unknown_format_falls_back_to_json_and_level_to_info():
    assert isinstance(build_formatter("yaml"), JsonFormatter)
    assert isinstance(build_formatter("TEXT"), TextFormatter)
    assert resolve_level("nope") == logging.INFO
    assert resolve_level("debug") == logging.DEBUG
    assert resolve_level(30) == logging.WARNING


def test_configure_logging_writes_parseable_lines():
    stream = io.StringIO()
    try:
        configure_logging("INFO", "json", stream=stream, force=True)
        with bind_request("req-42", tenant="acme", environment="production"):
            logging.getLogger("xw.test.wire").info("hello", extra={"event": "demo"})
        payload = json.loads(stream.getvalue().strip().splitlines()[-1])
        assert payload["request_id"] == "req-42"
        assert payload["tenant"] == "acme"
        assert payload["environment"] == "production"
        assert payload["event"] == "demo"
        assert payload["message"] == "hello"
    finally:
        _restore_logging()


def test_text_formatter_carries_request_id():
    formatter = TextFormatter()
    record = logging.LogRecord("xw.test", logging.INFO, __file__, 1, "done", (), None)
    with bind_request("req-text"):
        output = formatter.format(record)
    assert "done" in output
    assert "request_id=req-text" in output


# --------------------------------------------------------------------------
# 中间件
# --------------------------------------------------------------------------


def _build_app(*, access_log: bool = True, skip_prefixes=SKIP_PREFIXES) -> FastAPI:
    app = FastAPI()

    @app.get("/ok")
    def ok() -> dict:
        # 同步路由在 FastAPI 里跑在线程池：这条日志能带出请求标识，
        # 才说明上下文确实随任务一起复制过去了。
        logging.getLogger("xw.test.route").info("处理完成")
        return {"ok": True}

    @app.get("/boom")
    def boom() -> dict:
        raise RuntimeError("故意失败")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    app.add_middleware(
        RequestLogMiddleware, access_log=access_log, skip_prefixes=skip_prefixes
    )
    return app


def _access_records(caplog) -> list[logging.LogRecord]:
    return [item for item in caplog.records if item.name == "xw.access"]


def test_request_id_is_echoed_and_used_in_route_logs(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app())
    response = client.get("/ok", headers={"X-Tenant-Id": "acme", "X-Environment": "production"})

    rid = response.headers["X-Request-Id"]
    assert rid.startswith("req-")

    access = _access_records(caplog)
    assert len(access) == 1
    assert access[0].request_id == rid
    assert access[0].method == "GET"
    assert access[0].path == "/ok"
    assert access[0].status == 200
    assert access[0].event == "http.request"
    assert access[0].duration_ms >= 0
    assert access[0].tenant == "acme"
    assert access[0].environment == "production"

    route = [item for item in caplog.records if item.name == "xw.test.route"]
    assert route and route[0].request_id == rid


def test_incoming_request_id_is_preserved(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app())
    response = client.get("/ok", headers={"X-Request-Id": "trace-abc.1"})

    assert response.headers["X-Request-Id"] == "trace-abc.1"
    assert _access_records(caplog)[0].request_id == "trace-abc.1"


def test_invalid_incoming_request_id_is_replaced(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app())
    response = client.get("/ok", headers={"X-Request-Id": "bad\nvalue"})

    rid = response.headers["X-Request-Id"]
    assert rid.startswith("req-")
    assert _access_records(caplog)[0].request_id == rid


def test_client_error_is_logged_as_warning(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app())
    response = client.get("/nope")

    assert response.status_code == 404
    record = _access_records(caplog)[0]
    assert record.status == 404
    assert record.levelno == logging.WARNING


def test_unhandled_exception_is_logged_as_error(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app(), raise_server_exceptions=False)
    response = client.get("/boom")

    assert response.status_code == 500
    rid = response.headers["X-Request-Id"]
    assert rid.startswith("req-")
    # 500 是就地生成的：响应体同样给出标识，调用方不必只依赖响应头
    assert response.json()["requestId"] == rid

    record = _access_records(caplog)[0]
    assert record.status == 500
    assert record.levelno == logging.ERROR
    assert record.request_id == rid
    assert record.exc_info and record.exc_info[0] is RuntimeError


def test_health_probe_is_not_access_logged(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app())
    response = client.get("/health")

    assert response.status_code == 200
    assert response.headers["X-Request-Id"].startswith("req-")
    assert _access_records(caplog) == []


def test_access_log_can_be_disabled(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app(access_log=False))
    response = client.get("/ok")

    assert response.headers["X-Request-Id"].startswith("req-")
    assert _access_records(caplog) == []


def test_context_does_not_leak_between_requests(caplog):
    caplog.set_level(logging.INFO)
    client = TestClient(_build_app())
    first = client.get("/ok").headers["X-Request-Id"]
    second = client.get("/ok").headers["X-Request-Id"]

    assert first != second
    assert [item.request_id for item in _access_records(caplog)] == [first, second]
    assert log_fields() == {}


def test_real_app_returns_request_id():
    """真实应用上同样生效：中间件确实装进了 app。"""
    from app.main import app

    client = TestClient(app)
    response = client.get("/v1/providers", headers={"X-Environment": "development"})

    assert response.status_code == 200
    assert response.headers["X-Request-Id"].startswith("req-")


def test_log_config_defaults_are_sane():
    assert config.LOG_FORMAT in ("json", "text")
    assert config.LOG_LEVEL in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
    assert isinstance(config.LOG_ACCESS, bool)


@pytest.mark.parametrize("status", [200, 401, 429, 500])
def test_status_level_mapping(caplog, status):
    """状态码到日志级别的映射是采集端告警规则的依据，逐档钉住。"""
    caplog.set_level(logging.INFO)
    app = FastAPI()

    @app.get("/thing")
    def thing():
        from fastapi.responses import JSONResponse

        return JSONResponse({"ok": status < 400}, status_code=status)

    app.add_middleware(RequestLogMiddleware)
    TestClient(app).get("/thing")

    record = _access_records(caplog)[0]
    expected = logging.ERROR if status >= 500 else logging.WARNING if status >= 400 else logging.INFO
    assert record.levelno == expected
