"""消息持久化：JSONL / Postgres 双后端，通过环境变量切换。

- jsonl（默认）：追加写入 ``api/data/messages.jsonl``，demo 与无外部依赖时使用。
- postgres：写入 ``messages`` 表，连接与建表由脚本 / 迁移负责。

通过 ``XW_STORAGE`` 环境变量选择，``get_storage()`` 返回全局单例。

统计（``stats``）不保留原始记录：JSONL 后端逐行流式累加，Postgres 后端交给
聚合查询，两者都只把计数拿回来。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

from . import config
from .schemas import QueuedMessage
from .stats import TREND_DAYS, StatsAccumulator, build_stats

logger = logging.getLogger("xw.storage")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
JSONL_FILE = DATA_DIR / "messages.jsonl"


class Storage(Protocol):
    """持久化后端统一接口。"""

    def save(self, record: dict[str, Any]) -> None: ...

    def read_all(self) -> list[dict[str, Any]]: ...

    def revision(self) -> str: ...

    def stats(self, trend_days: int = TREND_DAYS) -> dict[str, Any]: ...


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
        """读取全部已落库记录（导出/排查用，统计接口不再走这条路径）。"""
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

    def revision(self) -> str:
        """内容版本：文件大小 + 修改时刻，用于让派生的缓存（如未读计数）失效。

        只取 ``stat``，不读文件内容；文件不存在时给固定标记。
        """
        try:
            st = self._path.stat()
        except OSError:
            return "absent"
        return f"{st.st_size}:{st.st_mtime_ns}"

    def ready(self) -> str:
        """就绪探测：数据目录存在且可写。

        只看目录权限而不落一个探针文件：就绪检查是周期性的，每次探测都写盘
        会在数据目录里堆出一串空文件，也会让只读挂载的部署莫名其妙地产生
        写流量。目录可写是「下一条消息能落盘」的充分条件。
        """
        parent = self._path.parent
        if not parent.is_dir():
            raise RuntimeError(f"数据目录不存在：{parent}")
        if not os.access(parent, os.W_OK):
            raise PermissionError(f"数据目录不可写：{parent}")
        return "jsonl 后端，数据目录可写"

    def stats(self, trend_days: int = TREND_DAYS) -> dict[str, Any]:
        """流式逐行累加统计量，内存占用与记录条数无关。"""
        acc = StatsAccumulator(trend_days=trend_days)
        with self._lock:
            if not self._path.exists():
                return acc.result()
            with self._path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        acc.add(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning("跳过损坏的 JSONL 行：%s", line[:80])
        return acc.result()


class PostgresStorage:
    """写入 Postgres ``messages`` 表（SQLAlchemy Core，连接惰性建立）。"""

    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._engine = None
        self._writes = 0

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
        self._writes += 1

    def revision(self) -> str:
        """表写入计数作为版本，用于让派生的缓存（如未读计数）失效。

        ``pg_stat_user_tables`` 的计数由统计收集器刷新，有秒级延迟，因此再叠加
        进程内写入计数，补齐刚提交、尚未反映到统计视图的那部分。查不到统计行
        （表尚未建立或视图不可用）时只用进程内计数。
        """
        from sqlalchemy import text

        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables "
                        "WHERE schemaname = current_schema() AND relname = 'messages'"
                    )
                ).first()
        except Exception:  # noqa: BLE001 - 版本探测不应让读取失败
            logger.warning("读取 messages 表写入计数失败，仅用进程内计数", exc_info=True)
            row = None
        if row is None:
            return f"pg:{self._writes}"
        return f"pg:{row[0]}:{row[1]}:{row[2]}:{self._writes}"

    def ready(self, timeout: float = 2.0) -> str:
        """就绪探测：用一次独立的短连接执行 ``SELECT 1``。

        不复用业务引擎是有意的：业务引擎的连接池按生产负载配置，可能排队
        等几十秒；探测要的是一个「两秒内能不能连上并查一句」的确定答案。
        独立连接还带上了连接与语句超时，数据库半死不活时探测自己会结束，
        不会把探针和请求一起挂在 socket 上。
        """
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import NullPool

        kwargs: dict[str, Any] = {"poolclass": NullPool}
        if self._url.startswith("postgresql"):
            seconds = max(1, int(timeout))
            kwargs["connect_args"] = {
                "connect_timeout": seconds,
                "options": f"-c statement_timeout={seconds * 1000}",
            }
        probe = create_engine(self._url, **kwargs)
        try:
            with probe.connect() as conn:
                conn.execute(text("SELECT 1"))
        finally:
            probe.dispose()
        return "postgres 连接正常"

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部消息记录（导出/排查用，统计接口不再走这条路径）。"""
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

    def stats(self, trend_days: int = TREND_DAYS) -> dict[str, Any]:
        """由 SQL 聚合出统计量，只回传计数行，不搬运原始记录。"""
        from sqlalchemy import text

        since = datetime.now(timezone.utc) - timedelta(days=trend_days)
        engine = self._get_engine()
        with engine.connect() as conn:
            totals = conn.execute(
                text(
                    "SELECT count(*) AS total, "
                    "count(*) FILTER (WHERE status = 'sent') AS sent, "
                    "count(*) FILTER (WHERE status = 'skipped') AS skipped FROM messages"
                )
            ).mappings().one()
            channel_rows = conn.execute(
                text(
                    "SELECT t.channel AS channel, count(*) AS n "
                    "FROM messages CROSS JOIN LATERAL jsonb_array_elements_text(channels) AS t(channel) "
                    "GROUP BY t.channel"
                )
            ).mappings().all()
            day_rows = conn.execute(
                text(
                    "SELECT (created_at AT TIME ZONE 'UTC')::date AS day, count(*) AS n "
                    "FROM messages WHERE created_at >= :since GROUP BY 1"
                ),
                {"since": since},
            ).mappings().all()

        per_day: dict[str, int] = {}
        for row in day_rows:
            day = row["day"]
            per_day[day.isoformat() if hasattr(day, "isoformat") else str(day)] = int(row["n"])

        return build_stats(
            total=int(totals["total"]),
            sent=int(totals["sent"]),
            channel_counts={str(r["channel"]): int(r["n"]) for r in channel_rows},
            per_day=per_day,
            trend_days=trend_days,
            skipped=int(totals["skipped"]),
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
