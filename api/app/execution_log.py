"""消息执行明细（execution details）存储与写入（ROADMAP Phase 1）。

与活动日志（``activity_log``，一条触发一条记录）互补，执行明细记录
「一次触发 → 多个渠道」的逐渠道投递结果：每条执行明细对应一个
``(trigger_id, channel)`` 组合，捕获投递状态（``sent`` / ``failed``）、
失败原因与耗时，供后台排查与「执行详情」查询使用。

双后端：``jsonl``（默认，追加 ``api/data/executions.jsonl``）与 ``postgres``
（``execution_details`` 表），由 ``XW_STORAGE`` 环境变量统一切换。

字段约定（每条执行明细）：
- ``trigger_id``：所属触发的唯一标识。
- ``channel``：投递渠道（in_app / email / sms / push / chat）。
- ``status``：投递结果（``sent`` / ``failed``）。
- ``reason``：失败原因（成功时为空）。
- ``duration_ms``：投递耗时（毫秒，mock 阶段为 0）。
- ``created_at``：ISO 8601 时间戳。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

from . import config

logger = logging.getLogger("xw.execution_log")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
JSONL_FILE = _DATA_DIR / "executions.jsonl"


class ExecutionStore(Protocol):
    """执行明细后端统一接口。"""

    def append(self, record: dict[str, Any]) -> None: ...

    def read_all(self) -> list[dict[str, Any]]: ...


class JSONLExecutionStore:
    """追加写入 JSONL 文件（一行一条执行明细）。"""

    def __init__(self, path: Path = JSONL_FILE) -> None:
        self._path = path
        self._lock = threading.Lock()

    def append(self, record: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def read_all(self) -> list[dict[str, Any]]:
        with self._lock:
            if not self._path.exists():
                return []
            records: list[dict[str, Any]] = []
            with self._path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning("跳过损坏的执行明细行：%s", line[:80])
            return records


class PostgresExecutionStore:
    """写入 Postgres ``execution_details`` 表（SQLAlchemy Core，连接惰性建立）。"""

    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self._url)
        return self._engine

    def append(self, record: dict[str, Any]) -> None:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO execution_details
                        (trigger_id, channel, status, reason, duration_ms)
                    VALUES
                        (:trigger_id, :channel, :status, :reason, :duration_ms)
                    """
                ),
                {
                    "trigger_id": record["trigger_id"],
                    "channel": record["channel"],
                    "status": record.get("status", "sent"),
                    "reason": record.get("reason"),
                    "duration_ms": record.get("duration_ms", 0),
                },
            )

    def read_all(self) -> list[dict[str, Any]]:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT trigger_id, channel, status, reason, duration_ms, "
                    "created_at FROM execution_details ORDER BY created_at"
                )
            ).mappings().all()
        records: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            records.append(
                {
                    "trigger_id": d["trigger_id"],
                    "channel": d["channel"],
                    "status": d.get("status", "sent"),
                    "reason": d.get("reason"),
                    "duration_ms": d.get("duration_ms", 0),
                    "created_at": d.get("created_at"),
                }
            )
        return records


def create_execution_store() -> ExecutionStore:
    """根据配置创建执行明细后端实例。"""
    if config.STORAGE_BACKEND == "postgres":
        return PostgresExecutionStore(config.DATABASE_URL)
    return JSONLExecutionStore()


_execution_store: Optional[ExecutionStore] = None
_execution_store_lock = threading.Lock()


def get_execution_store() -> ExecutionStore:
    """获取全局执行明细单例（惰性初始化）。"""
    global _execution_store
    with _execution_store_lock:
        if _execution_store is None:
            _execution_store = create_execution_store()
        return _execution_store


def record_execution(
    *,
    trigger_id: str,
    channel: str,
    status: str = "sent",
    reason: Optional[str] = None,
    duration_ms: int = 0,
) -> dict[str, Any]:
    """写入一条执行明细并返回落库记录。

    供 worker 在逐渠道投递时调用：每个 ``(trigger_id, channel)`` 各写一条。
    """
    record = {
        "trigger_id": trigger_id,
        "channel": channel,
        "status": status,
        "reason": reason,
        "duration_ms": duration_ms,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    get_execution_store().append(record)
    return record


__all__ = [
    "ExecutionStore",
    "JSONLExecutionStore",
    "PostgresExecutionStore",
    "get_execution_store",
    "record_execution",
]
