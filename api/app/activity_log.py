"""活动日志（activity feed）存储与查询。

维护一条独立的「触发事件」时间线：
- 触发 API 返回回执前，同步写入一条活动日志（``record_activity``），
  使前端活动流能即时看到「已触发」而非等 worker 落库。
- 双后端：``jsonl``（默认，追加 ``api/data/activity.jsonl``）与 ``postgres``
  （``activity_log`` 表），由 ``XW_STORAGE`` 环境变量统一切换。

查询走 ``query``：按 ``(created_at, trigger_id)`` 倒序做游标翻页，过滤条件下推
到后端。JSONL 后端从文件尾部倒着读，取满一页即停，读取量与页大小相关而不是
文件大小（条件过于稀疏时仍会退化为顺序扫描，见 README 的说明）；Postgres 后端
拼 WHERE + ``ORDER BY … LIMIT``，依赖 ``005_query_indexes.sql`` 建的复合索引。

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
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

from . import config
from .jsonl_reader import READ_CHUNK, ReverseLineReader
from .timeutil import Cursor, parse_dt

logger = logging.getLogger("xw.activity_log")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
JSONL_FILE = _DATA_DIR / "activity.jsonl"


@dataclass(frozen=True)
class ActivityQuery:
    """活动流查询条件（过滤 + 游标翻页）。

    ``after`` 是上一页最后一条记录的排序键，只返回比它更旧（同时间戳时
    ``trigger_id`` 更小）的记录；``limit`` 是本页最多返回的条数。
    """

    subscriber_id: Optional[str] = None
    workflow: Optional[str] = None
    trigger_id: Optional[str] = None
    status: Optional[str] = None
    environment: Optional[str] = None
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    limit: int = 50
    after: Optional[Cursor] = None


class ActivityStore(Protocol):
    """活动日志后端统一接口。"""

    def append(self, record: dict[str, Any]) -> None: ...

    def append_many(self, records: list[dict[str, Any]]) -> int: ...

    def query(self, q: ActivityQuery) -> list[dict[str, Any]]: ...


def _channels_of(record: dict[str, Any]) -> list[str]:
    channels = record.get("channels") or []
    if isinstance(channels, str):
        return [channels]
    return list(channels)


def sort_key(record: dict[str, Any]) -> tuple[float, str]:
    """记录的排序键：时间戳（缺失视为最旧）+ 触发标识。"""
    dt = parse_dt(record.get("created_at"))
    return (dt.timestamp() if dt is not None else float("-inf"), str(record.get("trigger_id") or ""))


def is_older_than(record: dict[str, Any], cursor: Cursor) -> bool:
    """记录是否严格早于游标（翻页时用于跳过已返回的部分）。"""
    dt = parse_dt(record.get("created_at"))
    if dt is None:
        return True
    return (dt, str(record.get("trigger_id") or "")) < (
        cursor.created_at,
        str(cursor.trigger_id),
    )


def matches(record: dict[str, Any], q: ActivityQuery) -> bool:
    """按查询条件判断一条记录是否命中（JSONL / 内存后端共用）。"""
    if q.environment:
        if (record.get("environment") or "development") != q.environment:
            return False
    if q.subscriber_id and record.get("subscriber_id") != q.subscriber_id:
        return False
    if q.workflow:
        if (record.get("workflow") or record.get("name")) != q.workflow:
            return False
    if q.trigger_id and record.get("trigger_id") != q.trigger_id:
        return False
    if q.status and record.get("status") != q.status:
        return False
    if q.since is not None or q.until is not None:
        dt = parse_dt(record.get("created_at"))
        if dt is None:
            return False
        if q.since is not None and dt < q.since:
            return False
        if q.until is not None and dt > q.until:
            return False
    return True


class JSONLActivityStore:
    """追加写入 JSONL 文件（一行一条活动），查询从尾部倒序读取。"""

    def __init__(self, path: Path = JSONL_FILE) -> None:
        self._path = path
        self._lock = threading.Lock()
        self.last_scan_bytes = 0

    def append(self, record: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def append_many(self, records: list[dict[str, Any]]) -> int:
        """把一批记录拼成一次追加写。

        逐条 ``append`` 时每条都要开关一次文件，批量触发会把这个开销乘以
        条数。这里在锁内一次打开、一次写出全部行；写失败时异常向上抛，
        调用方据此知道这批活动日志没有落盘。
        """
        if not records:
            return 0
        blob = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(blob)
        return len(records)

    def query(self, q: ActivityQuery) -> list[dict[str, Any]]:
        """按条件返回最新在前的一页记录；取满 ``limit`` 条即停止读取。"""
        limit = max(1, int(q.limit))
        out: list[dict[str, Any]] = []
        with self._lock:
            if not self._path.exists():
                self.last_scan_bytes = 0
                return []
            reader = ReverseLineReader(self._path)
            for raw in reader.lines():
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("跳过损坏的活动日志行：%s", raw[:80])
                    continue
                if not isinstance(record, dict) or not matches(record, q):
                    continue
                if q.after is not None and not is_older_than(record, q.after):
                    continue
                out.append(record)
                if len(out) >= limit:
                    break
            self.last_scan_bytes = reader.bytes_read
        return out

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部记录（导出/排查用，查询接口不再走这条路径）。"""
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


class MemoryActivityStore:
    """进程内活动日志（测试注入与无持久化演示用）。"""

    def __init__(self, records: Optional[list[dict[str, Any]]] = None) -> None:
        self._records: list[dict[str, Any]] = list(records or [])
        self._lock = threading.Lock()

    def append(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._records.append(record)

    def append_many(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        with self._lock:
            self._records.extend(records)
        return len(records)

    def query(self, q: ActivityQuery) -> list[dict[str, Any]]:
        with self._lock:
            selected = [
                r
                for r in self._records
                if matches(r, q) and (q.after is None or is_older_than(r, q.after))
            ]
        selected.sort(key=sort_key, reverse=True)
        return selected[: max(1, int(q.limit))]

    def read_all(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._records)


class PostgresActivityStore:
    """读写 Postgres ``activity_log`` 表（SQLAlchemy Core，连接惰性建立）。"""

    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self._url)
        return self._engine

    def append(self, record: dict[str, Any]) -> None:
        self.append_many([record])

    def append_many(self, records: list[dict[str, Any]]) -> int:
        """一批记录在一个事务里写入。

        逐条提交会让批量触发在数据库上排出一串小事务；这里用一条语句配
        多组参数，要么全进要么全不进，调用方也能从异常里判断整批是否落盘。
        """
        if not records:
            return 0
        from sqlalchemy import text

        engine = self._get_engine()
        params = [
            {
                "trigger_id": record["trigger_id"],
                "workflow": record["workflow"],
                "subscriber_id": record["subscriber_id"],
                "channels": json.dumps(record.get("channels") or [], ensure_ascii=False),
                "status": record.get("status", "triggered"),
                "title": record.get("title") or record["workflow"],
                "environment": record.get("environment") or config.ENV,
            }
            for record in records
        ]
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
                params,
            )
        return len(params)

    @staticmethod
    def _as_uuid(value: object) -> Optional[uuid.UUID]:
        try:
            return uuid.UUID(str(value))
        except (ValueError, AttributeError, TypeError):
            return None

    def _build_where(self, q: ActivityQuery) -> tuple[list[str], dict[str, Any]]:
        where: list[str] = []
        params: dict[str, Any] = {}
        if q.environment:
            where.append("environment = :environment")
            params["environment"] = q.environment
        if q.subscriber_id:
            where.append("subscriber_id = :subscriber_id")
            params["subscriber_id"] = q.subscriber_id
        if q.workflow:
            where.append("workflow = :workflow")
            params["workflow"] = q.workflow
        if q.trigger_id:
            where.append("trigger_id = :trigger_id")
            params["trigger_id"] = self._as_uuid(q.trigger_id)
        if q.status:
            where.append("status = :status")
            params["status"] = q.status
        if q.since is not None:
            where.append("created_at >= :since")
            params["since"] = q.since
        if q.until is not None:
            where.append("created_at <= :until")
            params["until"] = q.until
        if q.after is not None:
            where.append("(created_at, trigger_id) < (:cursor_ts, :cursor_id)")
            params["cursor_ts"] = q.after.created_at
            params["cursor_id"] = self._as_uuid(q.after.trigger_id)
        return where, params

    def query(self, q: ActivityQuery) -> list[dict[str, Any]]:
        from sqlalchemy import text

        # trigger_id 是 UUID 列：非 UUID 的过滤值不可能命中任何记录，
        # 直接返回空页，避免把非法值送进数据库触发类型错误。
        if q.trigger_id and self._as_uuid(q.trigger_id) is None:
            return []
        if q.after is not None and self._as_uuid(q.after.trigger_id) is None:
            raise ValueError("invalid cursor")

        where, params = self._build_where(q)
        params["limit"] = max(1, int(q.limit))
        sql = (
            "SELECT trigger_id, workflow, subscriber_id, channels, status, title, "
            "environment, created_at FROM activity_log"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, trigger_id DESC LIMIT :limit"

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return [self._to_record(dict(r)) for r in rows]

    @staticmethod
    def _to_record(d: dict[str, Any]) -> dict[str, Any]:
        channels = d.get("channels")
        return {
            "trigger_id": str(d["trigger_id"]),
            "workflow": d["workflow"],
            "subscriber_id": d["subscriber_id"],
            "channels": json.loads(channels) if isinstance(channels, str) else (channels or []),
            "status": d.get("status", "triggered"),
            "title": d.get("title"),
            "environment": d.get("environment") or config.ENV,
            "created_at": d.get("created_at"),
        }

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部记录（导出/排查用，查询接口不再走这条路径）。"""
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT trigger_id, workflow, subscriber_id, channels, status, "
                    "title, environment, created_at FROM activity_log "
                    "ORDER BY created_at, trigger_id"
                )
            ).mappings().all()
        return [self._to_record(dict(r)) for r in rows]


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


def build_activity(
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
    """构造一条活动日志记录（不落库）。

    展示标题优先取 ``payload.title``，其次 ``title``，最后工作流名。
    ``environment`` 打标隔离环境（默认 development），供活动流按环境过滤。
    """
    resolved_title = title or ((payload or {}).get("title") if payload else None) or workflow
    return {
        "trigger_id": trigger_id,
        "workflow": workflow,
        "subscriber_id": subscriber_id,
        "channels": channels or ["in_app"],
        "status": status,
        "title": resolved_title,
        "environment": environment or config.ENV,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


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
    """
    record = build_activity(
        trigger_id=trigger_id,
        workflow=workflow,
        subscriber_id=subscriber_id,
        channels=channels,
        status=status,
        title=title,
        payload=payload,
        environment=environment,
    )
    get_activity_store().append(record)
    return record


def record_activities(records: list[dict[str, Any]]) -> int:
    """写入一批活动日志，返回条数。

    后端有 ``append_many`` 时一次写入（文件后端一次打开、数据库后端一个
    事务），没有则逐条追加。活动流是触发链路上的旁路记录，这里的取舍是
    「写失败向上抛、由调用方决定是否让请求失败」，而不是静默丢一行。
    """
    if not records:
        return 0
    store = get_activity_store()
    bulk = getattr(store, "append_many", None)
    if callable(bulk):
        return int(bulk(records))
    for record in records:
        store.append(record)
    return len(records)


__all__ = [
    "ActivityQuery",
    "ActivityStore",
    "JSONLActivityStore",
    "MemoryActivityStore",
    "PostgresActivityStore",
    "ReverseLineReader",
    "build_activity",
    "get_activity_store",
    "is_older_than",
    "matches",
    "record_activity",
    "record_activities",
    "sort_key",
]
