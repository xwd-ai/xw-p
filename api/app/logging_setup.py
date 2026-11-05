"""日志装配：默认输出一行一条 JSON，字段稳定、可直接被采集端解析。

为什么默认 JSON：这套服务的日志会被收集到集中式存储里，靠人翻文件的阶段早已
过去；文本格式的「看起来整齐」在按字段过滤（找出某个 ``request_id`` 的全部记录、
按 ``status`` 统计 5xx）时毫无用处。本地想看顺眼的一行时把 ``XW_LOG_FORMAT``
设为 ``text`` 即可，两种格式共用同一份 record，切换不影响日志内容。

字段口径固定为 ``ts`` / ``level`` / ``request_id`` / ``tenant`` / ``environment`` /
``logger`` / ``event`` / ``message``，其余业务字段（``status``、``duration_ms`` 等）
平铺在同一层。不嵌一层 ``extra``：采集端按字段过滤时不用先展开一层 JSON。

formatter 自己也要足够健壮——它挂在根 logger 上，一旦抛异常，日志会变成
``--- Logging error ---`` 且丢掉原本要记录的内容。因此序列化失败降级为字符串，
格式化整体失败时退回裸消息。
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from .log_context import log_fields

#: 这些键由 formatter 决定，业务代码通过 ``extra`` 传同名键不得覆盖它们。
_RESERVED = {
    "ts",
    "level",
    "logger",
    "message",
    "asctime",
    "taskName",
    # 上下文字段只有一个来源（``log_fields``）：允许 extra 覆盖会让同一行里
    # 出现两个来源不同的租户值。
    "request_id",
    "tenant",
    "environment",
} | set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)

#: 合法级别名。用名字而不是数字，配置文件里 ``XW_LOG_LEVEL=DEBUG`` 比 ``10`` 好读。
LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}

#: uvicorn 在 record 上挂的带色文案：内容与 ``message`` 相同，只是多了 ANSI 转义，
#: 落到 JSON 里是一串 ``\u001b[36m``，既不增量信息又干扰阅读。
_DROPPED = frozenset({"color_message"})

_handler: Optional[logging.Handler] = None
_factory_installed = False


def _timestamp() -> str:
    """RFC 3339 / ISO 8601，毫秒精度，UTC。

    统一用 UTC 而不是本地时间：跨机器比对日志时，时区差一小时会让「谁先谁后」
    完全反过来。需要本地时间的地方由采集端按偏移量转换。
    """
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _jsonable(value: Any) -> Any:
    """尽量保留原始类型，不行就转成字符串。

    直接 ``json.dumps(default=str)`` 会让整个对象变成一个字符串、丢掉结构；
    这里逐值判断，常见类型（dict/list/数字/布尔/None）保持原样。
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    return str(value)


class JsonFormatter(logging.Formatter):
    """把一条 LogRecord 序列化成单行 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        try:
            payload: dict[str, Any] = {
                "ts": _timestamp(),
                "level": record.levelname.lower(),
            }
            payload.update(log_fields())
            payload["logger"] = record.name
            payload["message"] = record.getMessage()

            for key, value in record.__dict__.items():
                if key in _RESERVED or key in _DROPPED or key.startswith("_"):
                    continue
                payload[key] = _jsonable(value)

            if record.exc_info:
                payload["exc_type"] = getattr(record.exc_info[0], "__name__", "")
                payload["exc"] = self.formatException(record.exc_info)
            if record.stack_info:
                payload["stack"] = self.formatStack(record.stack_info)

            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        except Exception:  # noqa: BLE001 - formatter 绝不能让日志本身炸掉
            return json.dumps(
                {"ts": _timestamp(), "level": "error", "logger": record.name,
                 "message": "日志格式化失败", "fallback": str(record.msg)},
                ensure_ascii=False,
            )


class TextFormatter(logging.Formatter):
    """本地开发用的一行文本格式，带请求标识后缀。"""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        fields = log_fields()
        suffix = " ".join(f"{key}={value}" for key, value in fields.items())
        return f"{base} [{suffix}]" if suffix else base


def build_formatter(fmt: str) -> logging.Formatter:
    return TextFormatter() if str(fmt).strip().lower() == "text" else JsonFormatter()


def resolve_level(level: str | int) -> int:
    """把级别名（或数字）转成 logging 常量，无法识别时回退 INFO。"""
    if isinstance(level, int):
        return level
    return LOG_LEVELS.get(str(level or "").strip().upper(), logging.INFO)


def _install_record_factory() -> None:
    """让每条 LogRecord 自带请求上下文字段。

    只在 formatter 里读上下文不够：任何直接读 record 的消费方（测试的 ``caplog``、
    自定义 handler、采集端 SDK 的 record 钩子）都拿不到标识。工厂在 record 创建时
    写入，等于在日志产生的源头把线索钉上。

    用 ``hasattr`` 判断而不是覆盖：调用方显式 ``extra`` 传了同名值时以它为准，
    但 ``logging`` 本身会拒绝与 record 已有属性同名的 extra，实际不会被改写。
    """
    global _factory_installed
    if _factory_installed:
        return
    previous = logging.getLogRecordFactory()

    def factory(*args, **kwargs):
        record = previous(*args, **kwargs)
        for key, value in log_fields().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return record

    logging.setLogRecordFactory(factory)
    _factory_installed = True


# 导入即装配：即使调用方还没来得及 configure_logging，启动阶段的日志也已经带上
# 上下文字段；工厂本身是幂等的，重复调用只保留第一个。
_install_record_factory()


def configure_logging(
    level: str | int = "INFO",
    fmt: str = "json",
    *,
    stream=None,
    force: bool = False,
) -> logging.Handler:
    """装配根 logger，重复调用只更新级别与格式。

    只替换自己装上去的 handler（带 ``_xw_handler`` 标记），不清理根 logger 上的
    其它 handler：测试框架、交互式环境会往根上挂自己的收集器，一把清空等于把
    它们的输出一起关掉。

    ``uvicorn`` 的两个 logger 改为向上冒泡，避免同一行既被 uvicorn 自带的文本
    handler 打一遍、又被 JSON handler 打一遍；``uvicorn.access`` 直接停用，
    访问日志由 ``RequestLogMiddleware`` 输出——那一层才知道请求标识与耗时。
    """
    global _handler

    _install_record_factory()
    formatter = build_formatter(fmt)
    level_value = resolve_level(level)
    root = logging.getLogger()

    if _handler is not None and not force:
        _handler.setFormatter(formatter)
        root.setLevel(level_value)
        _silence_uvicorn()
        return _handler

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(formatter)
    # 标记用于识别「这是本模块装的」：重复装配时只摘掉旧的自己。
    handler._xw_handler = True  # type: ignore[attr-defined]

    for existing in [item for item in root.handlers if getattr(item, "_xw_handler", False)]:
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level_value)
    _handler = handler
    _silence_uvicorn()
    return handler


def _silence_uvicorn() -> None:
    for name in ("uvicorn", "uvicorn.error"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
    logging.getLogger("uvicorn.access").disabled = True


def configure_from_config() -> logging.Handler:
    """按 ``XW_LOG_FORMAT`` / ``XW_LOG_LEVEL`` 装配，供应用入口调用。"""
    from . import config

    return configure_logging(config.LOG_LEVEL, config.LOG_FORMAT)


__all__ = [
    "LOG_LEVELS",
    "JsonFormatter",
    "TextFormatter",
    "build_formatter",
    "configure_from_config",
    "configure_logging",
    "resolve_level",
]
