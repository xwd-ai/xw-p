"""消息执行明细（execution details）存储与查询（ROADMAP Phase 1 / R6）。

与活动日志（``activity_log``，一条触发一条记录）互补，执行明细记录
「一次触发 → 多个渠道」的逐渠道投递结果：每条执行明细对应一个
``(trigger_id, channel)`` 组合，捕获投递状态（``sent`` / ``failed``）、
失败原因与耗时，供后台排查与「执行详情」查询使用。

双后端：``jsonl``（默认，追加 ``api/data/executions.jsonl``）与 ``postgres``
（``execution_details`` 表），由 ``XW_STORAGE`` 环境变量统一切换。

查询走 ``query``：按 ``(created_at, id)`` 倒序做游标翻页，过滤条件下推到后端。
一次触发会写出多条渠道记录，``trigger_id`` 不足以区分同一次触发的两条明细，
因此次级排序键取记录自身的标识（JSONL 写入时生成，Postgres 用自增主键）。
JSONL 后端从文件尾部倒着读，取满一页即停；Postgres 后端拼 ``WHERE`` +
``ORDER BY … LIMIT``，依赖 ``006_execution_indexes.sql`` 的复合索引。

统计聚合走 ``scan``：按时间窗顺序遍历记录，供 ``admin_stats`` 逐条累加。这里
刻意不返回列表——窗口内的记录数可能很大，聚合只需要每条的渠道、状态与耗时，
把整窗读进内存没有意义。JSONL 后端顺序读文件、Postgres 后端用流式游标。

字段约定（每条执行明细）：
- ``id``：明细记录标识（翻页次级键）。
- ``trigger_id``：所属触发的唯一标识。
- ``channel``：投递渠道（in_app / email / sms / push / chat）。
- ``status``：投递结果（``sent`` / ``failed`` / ``skipped``）。``skipped`` 表示
  订阅者偏好关闭了该渠道：没有取实现、没有发请求，``reason`` 给出命中的层级。
- ``reason``：失败原因（成功时为空）。
- ``duration_ms``：投递耗时（毫秒，mock 阶段为 0）。
- ``created_at``：ISO 8601 时间戳。

引擎侧（``engine/internal/execution``）写同一份文件时还会带上
``workflow`` / ``subscriber_id`` / ``step_id`` / ``provider`` / ``attempts`` /
``fallback``：哪个步骤、实际完成投递的渠道实现、按顺序试过哪些实现、是否
发生过主备回退。这几个键对本模块是透明的——读取按字典取已知字段，多出来的
键原样保留，``GET /v1/executions`` 会把它们一并回给调用方。
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Protocol

from . import config
from .jsonl_reader import ReverseLineReader
from .timeutil import parse_dt

logger = logging.getLogger("xw.execution_log")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
JSONL_FILE = _DATA_DIR / "executions.jsonl"


@dataclass(frozen=True)
class ExecutionCursor:
    """执行明细的翻页游标：上一页最后一条记录的 ``(created_at, id)``。

    ``created_at`` 可能撞车（同一批渠道几乎同时写入），用记录标识做次级键
    才能保证翻页不跳不重。
    """

    created_at: datetime
    key: str


@dataclass(frozen=True)
class ExecutionQuery:
    """执行明细查询条件（过滤 + 游标翻页）。

    ``tenant`` / ``environment`` 是隔离维度：不传表示不按该维度收窄，
    接口层总会传入当前请求的租户，管理端再按需指定环境。
    """

    tenant: Optional[str] = None
    environment: Optional[str] = None
    trigger_id: Optional[str] = None
    channel: Optional[str] = None
    status: Optional[str] = None
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    limit: int = 50
    after: Optional[ExecutionCursor] = None


class ExecutionStore(Protocol):
    """执行明细后端统一接口。"""

    def append(self, record: dict[str, Any]) -> None: ...

    def query(self, q: ExecutionQuery) -> list[dict[str, Any]]: ...

    def read_all(self) -> list[dict[str, Any]]: ...

    def scan(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        tenant: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]: ...


def record_key(record: dict[str, Any]) -> str:
    """记录的次级排序键；缺失时退化为空串（排在同时间戳记录的最后）。"""
    return str(record.get("id") or "")


def sort_key(record: dict[str, Any]) -> tuple[float, str]:
    """记录的排序键：时间戳（缺失视为最旧）+ 记录标识。"""
    dt = parse_dt(record.get("created_at"))
    return (
        dt.timestamp() if dt is not None else float("-inf"),
        record_key(record),
    )


def is_older_than(record: dict[str, Any], cursor: ExecutionCursor) -> bool:
    """记录是否严格早于游标（翻页时用于跳过已返回的部分）。"""
    dt = parse_dt(record.get("created_at"))
    if dt is None:
        return True
    return (dt, record_key(record)) < (cursor.created_at, cursor.key)


def matches(record: dict[str, Any], q: ExecutionQuery) -> bool:
    """按查询条件判断一条记录是否命中（JSONL / 内存后端共用）。"""
    if q.tenant and (record.get("tenant") or "default") != q.tenant:
        return False
    if q.environment and (record.get("environment") or "development") != q.environment:
        return False
    if q.trigger_id and record.get("trigger_id") != q.trigger_id:
        return False
    if q.channel and record.get("channel") != q.channel:
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


def _in_tenant(record: dict[str, Any], tenant: Optional[str]) -> bool:
    """记录是否属于指定租户；``tenant`` 为空表示不按租户收窄。"""
    if not tenant:
        return True
    return (record.get("tenant") or "default") == tenant


def _in_window(
    record: dict[str, Any],
    since: Optional[datetime],
    until: Optional[datetime],
) -> bool:
    """记录是否落在时间窗内（含边界）。

    给了窗口却解析不出时间戳的记录一律排除：统计里把它算进任何一天都是错的，
    静默归到「今天」只会让数字对不上。
    """
    if since is None and until is None:
        return True
    dt = parse_dt(record.get("created_at"))
    if dt is None:
        return False
    if since is not None and dt < since:
        return False
    if until is not None and dt > until:
        return False
    return True


def _with_id(record: dict[str, Any]) -> dict[str, Any]:
    """确保记录带标识（翻页次级键）；已有则原样返回。"""
    if record.get("id"):
        return record
    return {**record, "id": uuid.uuid4().hex}


class JSONLExecutionStore:
    """追加写入 JSONL 文件（一行一条执行明细），查询从尾部倒序读取。"""

    def __init__(self, path: Path = JSONL_FILE) -> None:
        self._path = path
        self._lock = threading.Lock()
        self.last_scan_bytes = 0

    def append(self, record: dict[str, Any]) -> None:
        record = _with_id(record)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def query(self, q: ExecutionQuery) -> list[dict[str, Any]]:
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
                    logger.warning("跳过损坏的执行明细行：%s", raw[:80])
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
                        logger.warning("跳过损坏的执行明细行：%s", line[:80])
            return records

    def scan(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        tenant: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        """按时间窗顺序遍历记录（统计聚合用）。

        只持有文件句柄、不持有写锁：写入方用另一个描述符追加，互不阻塞。逐行
        解析后立即交给调用方，窗口再大也只占一条记录的内存。
        """
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("跳过损坏的执行明细行：%s", line[:80])
                    continue
                if (
                    isinstance(record, dict)
                    and _in_window(record, since, until)
                    and _in_tenant(record, tenant)
                ):
                    yield record


class MemoryExecutionStore:
    """进程内执行明细存储（测试注入与无持久化演示用）。"""

    def __init__(self, records: Optional[list[dict[str, Any]]] = None) -> None:
        self._records: list[dict[str, Any]] = [_with_id(r) for r in (records or [])]
        self._lock = threading.Lock()

    def append(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._records.append(_with_id(record))

    def query(self, q: ExecutionQuery) -> list[dict[str, Any]]:
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

    def scan(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        tenant: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        """按时间窗遍历记录；先取快照再迭代，聚合期间的新写入不改变本次结果。"""
        with self._lock:
            snapshot = list(self._records)
        for record in snapshot:
            if _in_window(record, since, until) and _in_tenant(record, tenant):
                yield record


class PostgresExecutionStore:
    """读写 Postgres ``execution_details`` 表（SQLAlchemy Core，连接惰性建立）。"""

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
                        (trigger_id, channel, status, reason, duration_ms, tenant, environment)
                    VALUES
                        (:trigger_id, :channel, :status, :reason, :duration_ms, :tenant, :environment)
                    """
                ),
                {
                    "trigger_id": record["trigger_id"],
                    "channel": record["channel"],
                    "status": record.get("status", "sent"),
                    "reason": record.get("reason"),
                    "duration_ms": record.get("duration_ms", 0),
                    "tenant": record.get("tenant") or "default",
                    "environment": record.get("environment") or "development",
                },
            )

    @staticmethod
    def _as_uuid(value: object) -> Optional[uuid.UUID]:
        try:
            return uuid.UUID(str(value))
        except (ValueError, AttributeError, TypeError):
            return None

    @staticmethod
    def _as_int(value: object) -> Optional[int]:
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    def _build_where(self, q: ExecutionQuery) -> tuple[list[str], dict[str, Any]]:
        where: list[str] = []
        params: dict[str, Any] = {}
        if q.tenant:
            where.append("COALESCE(tenant, 'default') = :tenant")
            params["tenant"] = q.tenant
        if q.environment:
            where.append("COALESCE(environment, 'development') = :environment")
            params["environment"] = q.environment
        if q.trigger_id:
            where.append("trigger_id = :trigger_id")
            params["trigger_id"] = self._as_uuid(q.trigger_id)
        if q.channel:
            where.append("channel = :channel")
            params["channel"] = q.channel
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
            where.append("(created_at, id) < (:cursor_ts, :cursor_id)")
            params["cursor_ts"] = q.after.created_at
            params["cursor_id"] = self._as_int(q.after.key)
        return where, params

    def query(self, q: ExecutionQuery) -> list[dict[str, Any]]:
        from sqlalchemy import text

        # trigger_id 是 UUID 列：非 UUID 的过滤值不可能命中任何记录，
        # 直接返回空页，避免把非法值送进数据库触发类型错误。
        if q.trigger_id and self._as_uuid(q.trigger_id) is None:
            return []
        if q.after is not None and self._as_int(q.after.key) is None:
            raise ValueError("invalid cursor")

        where, params = self._build_where(q)
        params["limit"] = max(1, int(q.limit))
        sql = (
            "SELECT id, trigger_id, channel, status, reason, duration_ms, created_at "
            "FROM execution_details"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, id DESC LIMIT :limit"

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return [self._to_record(dict(r)) for r in rows]

    @staticmethod
    def _to_record(d: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(d["id"]) if d.get("id") is not None else "",
            "trigger_id": str(d["trigger_id"]),
            "channel": d["channel"],
            "status": d.get("status", "sent"),
            "reason": d.get("reason"),
            "duration_ms": d.get("duration_ms", 0),
            "tenant": d.get("tenant") or "default",
            "environment": d.get("environment") or "development",
            "created_at": d.get("created_at"),
        }

    def scan(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        tenant: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        """按时间窗遍历记录（统计聚合用，服务端游标流式返回）。"""
        from sqlalchemy import text

        where: list[str] = []
        params: dict[str, Any] = {}
        if tenant:
            where.append("COALESCE(tenant, 'default') = :tenant")
            params["tenant"] = tenant
        if since is not None:
            where.append("created_at >= :since")
            params["since"] = since
        if until is not None:
            where.append("created_at <= :until")
            params["until"] = until
        sql = (
            "SELECT id, trigger_id, channel, status, reason, duration_ms, created_at "
            "FROM execution_details"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at, id"

        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(text(sql), params)
            for row in result.mappings():
                yield self._to_record(dict(row))

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部记录（导出/排查用，查询接口不再走这条路径）。"""
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, trigger_id, channel, status, reason, duration_ms, "
                    "created_at FROM execution_details ORDER BY created_at, id"
                )
            ).mappings().all()
        return [self._to_record(dict(r)) for r in rows]


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
    tenant: Optional[str] = None,
    environment: Optional[str] = None,
) -> dict[str, Any]:
    """写入一条执行明细并返回落库记录。

    供 worker 在逐渠道投递时调用：每个 ``(trigger_id, channel)`` 各写一条。
    """
    if tenant is None or environment is None:
        from . import tenancy

        tenant = tenant or tenancy.current_tenant()
        environment = environment or config.ENV
    record = {
        "id": uuid.uuid4().hex,
        "tenant": tenant,
        "environment": environment,
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
    "ExecutionCursor",
    "ExecutionQuery",
    "ExecutionStore",
    "JSONLExecutionStore",
    "MemoryExecutionStore",
    "PostgresExecutionStore",
    "get_execution_store",
    "is_older_than",
    "matches",
    "record_execution",
    "sort_key",
]
