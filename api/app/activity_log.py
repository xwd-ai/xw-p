"""活动日志（activity feed）存储与写入（ROADMAP Phase 1）。

与 ``storage``（消息投递记录）解耦，维护一条独立的「触发事件」时间线：
- 触发 API 返回回执前，同步写入一条活动日志（``record_activity``），
  使前端活动流能即时看到「已触发」而非等 worker 落库。
- 双后端：``jsonl``（默认，追加 ``api/data/activity.jsonl``）与 ``postgres``
  （``activity_log`` 表），由 ``XW_STORAGE`` 环境变量统一切换。

字段约定（每条活动日志）：
- ``trigger_id``：触发唯一标识。
- ``workflow``：工作流/模板名。
- ``subscriber_id``：订阅者标识。
- ``channels``：目标渠道列表（触发入参声明的渠道，未指定回退 in_app）。
- ``status``：活动状态（``triggered`` / ``sent`` / ``failed``）。
- ``created_at``：ISO 8601 时间戳。
- ``title``：面向展示的摘要。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

from . import config

logger = logging.getLogger("xw.activity_log")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
JSONL_FILE = _DATA_DIR / "activity.jsonl"


class ActivityStore(Protocol):
    """活动日志后端统一接口。"""

    def append(self, record: dict[str, Any]) -> None: ...

    def read_all(self) -> list[dict[str, Any]]: ...


class JSONLActivityStore:
    """追加写入 JSONL 文件（一行一条活动）。"""

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
                        logger.warning("跳过损坏的活动日志行：%s", line[:80])
            return records


class PostgresActivityStore:
    """写入 Postgres ``activity_log`` 表（SQLAlchemy Core，连接惰性建立）。"""

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
                    INSERT INTO activity_log
                        (trigger_id, workflow, subscriber_id, channels, status, title, environment)
                    VALUES
                        (:trigger_id, :workflow, :subscriber_id, :channels, :status, :title, :environment)
                    """
                ),
                {
                    "trigger_id": record["trigger_id"],
                    "workflow": record["workflow"],
                    "subscriber_id": record["subscriber_id"],
                    "channels": json.dumps(record.get("channels") or [], ensure_ascii=False),
                    "status": record.get("status", "triggered"),
                    "title": record.get("title") or record["workflow"],
                    "environment": record.get("environment") or config.ENV,
                },
            )

    def read_all(self) -> list[dict[str, Any]]:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT trigger_id, workflow, subscriber_id, channels, status, "
                    "title, environment, created_at FROM activity_log ORDER BY created_at"
                )
            ).mappings().all()
        records: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            channels = d.get("channels")
            records.append(
                {
                    "trigger_id": d["trigger_id"],
                    "workflow": d["workflow"],
                    "subscriber_id": d["subscriber_id"],
                    "channels": json.loads(channels) if isinstance(channels, str) else (channels or []),
                    "status": d.get("status", "triggered"),
                    "title": d.get("title"),
                    "environment": d.get("environment") or config.ENV,
                    "created_at": d.get("created_at"),
                }
            )
        return records


def create_activity_store() -> ActivityStore:
    """根据配置创建活动日志后端实例。"""
    if config.STORAGE_BACKEND == "postgres":
        return PostgresActivityStore(config.DATABASE_URL)
    return JSONLActivityStore()


_activity_store: Optional[ActivityStore] = None
_activity_store_lock = threading.Lock()


def get_activity_store() -> ActivityStore:
    """获取全局活动日志单例（惰性初始化）。"""
    global _activity_store
    with _activity_store_lock:
        if _activity_store is None:
            _activity_store = create_activity_store()
        return _activity_store


def record_activity(
    *,
    trigger_id: str,
    workflow: str,
    subscriber_id: str,
    channels: Optional[list[str]] = None,
    status: str = "triggered",
    title: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    environment: Optional[str] = None,
) -> dict[str, Any]:
    """写入一条活动日志并返回落库记录。

    供触发 API 在返回回执前调用，令活动流即时反映「已触发」状态。
    展示标题优先取 ``payload.title``，其次 ``title``，最后工作流名。
    ``environment`` 打标隔离环境（默认 development），供活动流按环境过滤。
    """
    resolved_title = title or ((payload or {}).get("title") if payload else None) or workflow
    record = {
        "trigger_id": trigger_id,
        "workflow": workflow,
        "subscriber_id": subscriber_id,
        "channels": channels or ["in_app"],
        "status": status,
        "title": resolved_title,
        "environment": environment or config.ENV,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    get_activity_store().append(record)
    return record


__all__ = [
    "ActivityStore",
    "JSONLActivityStore",
    "PostgresActivityStore",
    "get_activity_store",
    "record_activity",
]
