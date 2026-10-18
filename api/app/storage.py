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
