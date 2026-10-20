"""消息持久化：JSONL / Postgres 双后端，通过环境变量切换。

- jsonl（默认）：追加写入 ``api/data/messages.jsonl``，demo 与无外部依赖时使用。
- postgres：写入 ``messages`` 表，连接与建表由脚本 / 迁移负责。

通过 ``XW_STORAGE`` 环境变量选择，``get_storage()`` 返回全局单例。
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Optional, Protocol

from . import config
from .schemas import QueuedMessage

logger = logging.getLogger("xw.storage")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
JSONL_FILE = DATA_DIR / "messages.jsonl"


class Storage(Protocol):
    """持久化后端统一接口。"""

    def save(self, record: dict[str, Any]) -> None: ...

    def read_all(self) -> list[dict[str, Any]]: ...


class JSONLStorage:
    """追加写入 JSONL 文件（一行一条消息）。"""

    def __init__(self, path: Path = JSONL_FILE) -> None:
        self._path = path
        self._lock = threading.Lock()

    def save(self, record: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部已落库记录（统计/活动流用）。文件不存在返回空列表。"""
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
                        logger.warning("跳过损坏的 JSONL 行：%s", line[:80])
            return records


class PostgresStorage:
    """写入 Postgres ``messages`` 表（SQLAlchemy Core，连接惰性建立）。"""

    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self._url)
        return self._engine

    def save(self, record: dict[str, Any]) -> None:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO messages
                        (trigger_id, transaction_id, workflow, subscriber_id,
                         channels, payload, status)
                    VALUES
                        (:trigger_id, :transaction_id, :workflow, :subscriber_id,
                         :channels, :payload, :status)
                    """
                ),
                {
                    "trigger_id": record["trigger_id"],
                    "transaction_id": record["transaction_id"],
                    "workflow": record["workflow"],
                    "subscriber_id": record["subscriber_id"],
                    "channels": json.dumps(record.get("channels") or [], ensure_ascii=False),
                    "payload": json.dumps(record.get("payload") or {}, ensure_ascii=False),
                    "status": record.get("status", "sent"),
                },
            )

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部消息记录（统计/活动流用）。"""
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT trigger_id, transaction_id, workflow, subscriber_id, "
                    "channels, payload, status, created_at FROM messages "
                    "ORDER BY created_at"
                )
            ).mappings().all()
        records: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            channels = d.get("channels")
            payload = d.get("payload")
            records.append(
                {
                    "trigger_id": d["trigger_id"],
                    "transaction_id": d["transaction_id"],
                    "workflow": d["workflow"],
                    "subscriber_id": d["subscriber_id"],
                    "channels": json.loads(channels) if isinstance(channels, str) else (channels or []),
                    "payload": json.loads(payload) if isinstance(payload, str) else (payload or {}),
                    "status": d.get("status", "sent"),
                    "created_at": d.get("created_at"),
                }
            )
        return records


def create_storage() -> Storage:
    """根据配置创建持久化后端实例。"""
    if config.STORAGE_BACKEND == "postgres":
        logger.info("持久化后端：postgres")
        return PostgresStorage(config.DATABASE_URL)
    logger.info("持久化后端：jsonl（%s）", JSONL_FILE)
    return JSONLStorage()


_storage: Optional[Storage] = None
_storage_lock = threading.Lock()


def get_storage() -> Storage:
    """获取全局持久化单例（惰性初始化）。"""
    global _storage
    with _storage_lock:
        if _storage is None:
            _storage = create_storage()
        return _storage
