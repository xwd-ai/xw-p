"""In-app 收件箱（ROADMAP Phase S）。

提供订阅者维度的 In-app 通知中心能力：
- ``GET /v1/inbox``：返回某订阅者的 In-app 消息列表与未读计数，支持游标翻页
  与筛选（只未读、按工作流、按时间区间）。
- ``POST /v1/inbox/read``：标记一条或多条消息为已读（未指定则全部已读）。

数据来源：
- 引擎写入的收件箱文件（``api/data/inbox.jsonl``，由 Go 引擎的 in_app 步骤落库）；
- ``storage`` 中已落库的投递记录（``channels`` 含 ``in_app``）作为兜底来源。
两个来源按「消息 ID」去重，引擎条目优先（正文为步骤模板渲染后的真实内容）。

消息 ID 与已读语义：
- 消息 ID 取引擎条目的 ``id``（普通站内信为 ``trigger_id:step_id``，聚合卡片为
  ``digest-*`` 且窗口内保持不变），兜底来源以 ``trigger_id`` 充当；
- 已读状态按「消息 ID + 已读时刻」记录。聚合卡片被新互动原地改写时 ``updated_at``
  会推进，已读时刻早于它即重新算未读——同一张卡在收件箱里始终只占一条，
  未读计数也就自然按「消息条数」而不是「互动次数」计算。
已读状态由 ``ReadStateStore`` 维护，JSONL 持久化到 ``api/data/read_state.jsonl``，
与消息存储的 JSONL 风格保持一致（进程内缓存 + 追加落盘）。

未读计数走缓存：``InboxCache`` 以「收件箱来源版本 + 该订阅者已读版本」为键保存
最近一次算好的快照（消息列表 / 未读条数 / 消息总数）。版本没变时直接复用，
既不读收件箱文件、也不把投递记录读进内存；任一来源有新写入或该订阅者标记了
已读，版本变化即重算。版本探测本身是廉价操作：收件箱文件取 ``stat``，
JSONL 投递存储取文件 ``stat``，Postgres 取表的写入计数。

翻页与筛选：
- 排序键是 ``(排序时间, 消息 ID)``，倒序。排序时间优先取 ``updated_at``（聚合卡片
  被新互动顶起来时应该往前排），回落到 ``created_at``；带上消息 ID 是为了定序——
  同一秒内写入的多条消息若只按时间排，翻页时顺序会在两次请求之间漂移，
  同一条消息可能出现在两页里。
- 游标编码上一页最后一条的排序键，下一次请求从它的**严格后一条**继续。
  这样即使翻页期间顶上来了新消息，也不会重复返回或漏掉后面的内容。
- 未读与总数覆盖该订阅者的全部消息（与是否筛选、是否截断无关），
  ``matched`` 是本页筛选命中的条数，``hasMore`` / ``nextCursor`` 用于继续翻页。
- 首屏（无筛选、无游标）走缓存快照；筛选或深翻页要按全部消息算命中数，
  因此改为整份汇总后过滤——缓存里只留最近 200 条，用它算筛选结果会得到错的数。
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import itertools
import json
import logging
import os
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from . import config
from .auth import EnvContext, require_env
from .storage import get_storage

logger = logging.getLogger("xw.inbox")

router = APIRouter(prefix="/v1/inbox", tags=["inbox"])

_DATA_DIR = config.data_dir()
_READ_STATE_FILE = _DATA_DIR / "read_state.jsonl"

# 引擎侧 in_app 步骤写入的收件箱文件（与 engine 默认路径一致）。
# 引擎可通过 XW_INBOX_FILE 覆盖路径，此处同样读取该环境变量。
_INBOX_FILE = Path(os.getenv("XW_INBOX_FILE") or _DATA_DIR / "inbox.jsonl")

# In-app 渠道标识（后端落库用下划线命名）
_INAPP_CHANNEL = "in_app"

# 单页条数的默认值与上限。上限与缓存保留条数一致：再大的页也拿不到更早的消息，
# 只会让人以为「消息就这么点」。
DEFAULT_INBOX_LIMIT = 50
MAX_INBOX_LIMIT = 200

# 排序兜底时间：缺失/无法解析 created_at 的条目排在最后
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


class ReadRequest(BaseModel):
    """标记已读请求体。

    ``messageIds`` 是当前接口的写法；``triggerIds`` 保留给早期调用方，
    服务端会按 trigger_id 反查对应的消息 ID。
    """

    subscriber_id: str = Field(alias="subscriberId", min_length=1, description="订阅者唯一标识")
    message_ids: Optional[list[str]] = Field(
        default=None, alias="messageIds", description="要标记为已读的消息 ID 列表；缺省表示全部已读"
    )
    trigger_ids: Optional[list[str]] = Field(
        default=None, alias="triggerIds", description="按 trigger_id 标记已读（兼容旧调用方）"
    )

    model_config = {"populate_by_name": True}


class ReadStateStore:
    """订阅者已读状态存储：进程内缓存 + JSONL 追加落盘。

    以 ``(subscriber_id, message_id)`` 为粒度记录已读时刻；启动时从文件回放，
    运行期写入即时更新缓存并追加一行落盘。

    早期版本以 ``trigger_id`` 为粒度记录（没有时刻），这类历史记录单独存一份，
    只用于「这条消息以前读过」的兼容判断——没有时刻就无法参与「卡片被更新后
    是否重新未读」的比较。
    """

    def __init__(self, path: Path = _READ_STATE_FILE) -> None:
        self._path = path
        # subscriber_id -> {message_id: 已读时刻}
        self._read: dict[str, dict[str, datetime]] = {}
        # subscriber_id -> {trigger_id}（历史记录，仅作兼容）
        self._legacy: dict[str, set[str]] = {}
        # subscriber_id -> 已读版本号（每次真正写入自增，供未读缓存失效）
        self._versions: dict[str, int] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("跳过损坏的已读状态行：%s", line[:80])
                        continue
                    sub = item.get("subscriber_id")
                    if not sub:
                        continue
                    message_id = item.get("message_id")
                    if message_id:
                        at = _parse_dt(item.get("read_at")) or _EPOCH
                        self._read.setdefault(sub, {})[message_id] = at
                        continue
                    tid = item.get("trigger_id")
                    if tid:
                        self._legacy.setdefault(sub, set()).add(tid)
        except OSError:
            logger.warning("读取已读状态文件失败：%s", self._path)

    def _append(self, subscriber_id: str, message_id: str, read_at: datetime) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(
                        {
                            "subscriber_id": subscriber_id,
                            "message_id": message_id,
                            "read_at": read_at.astimezone(timezone.utc).isoformat(),
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        except OSError:
            logger.warning("写入已读状态文件失败：%s", self._path)

    @staticmethod
    def _is_current(read_at: datetime, updated_at: Optional[datetime]) -> bool:
        """已读时刻是否覆盖了当前内容：卡片在这之后又被更新过就算未读。"""
        if updated_at is None:
            return True
        return read_at >= updated_at

    def mark_read(
        self, subscriber_id: str, message_id: str, updated_at: Optional[datetime] = None
    ) -> bool:
        """标记某条消息已读；返回 True 表示本次写入（首次标记或刷新已读时刻）。"""
        with self._lock:
            bucket = self._read.setdefault(subscriber_id, {})
            read_at = bucket.get(message_id)
            if read_at is not None and self._is_current(read_at, updated_at):
                return False
            now = datetime.now(timezone.utc)
            bucket[message_id] = now
            self._versions[subscriber_id] = self._versions.get(subscriber_id, 0) + 1
            self._append(subscriber_id, message_id, now)
            return True

    def revision(self, subscriber_id: str) -> int:
        """该订阅者的已读版本号：每次标记已读自增，用于让未读缓存失效。"""
        with self._lock:
            return self._versions.get(subscriber_id, 0)

    def is_read(
        self, subscriber_id: str, message_id: str, updated_at: Optional[datetime] = None
    ) -> bool:
        with self._lock:
            read_at = (self._read.get(subscriber_id) or {}).get(message_id)
        if read_at is None:
            return False
        return self._is_current(read_at, updated_at)

    def is_legacy_read(self, subscriber_id: str, trigger_id: str) -> bool:
        """历史记录里这条 trigger_id 是否被标记过已读。"""
        with self._lock:
            return trigger_id in self._legacy.get(subscriber_id, set())


_read_store: Optional[ReadStateStore] = None
_tenant_read_stores: dict[str, ReadStateStore] = {}
_read_store_lock = threading.Lock()


def get_read_store(tenant: Optional[str] = None) -> ReadStateStore:
    """获取已读状态存储（按租户隔离，惰性初始化）。

    已读状态以 ``(subscriber_id, message_id)`` 为键，两个租户的订阅者标识
    完全可能重名，因此不能共用一份文件。
    """
    from . import tenancy

    global _read_store

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _read_store_lock:
        if key == tenancy.DEFAULT_TENANT:
            if _read_store is None:
                _read_store = ReadStateStore()
            return _read_store
        store = _tenant_read_stores.get(key)
        if store is None:
            store = ReadStateStore(tenancy.scoped_path(_READ_STATE_FILE, key))
            _tenant_read_stores[key] = store
        return store


# ---------------------------------------------------------------------------
# 未读计数缓存
# ---------------------------------------------------------------------------

# 缓存保留的订阅者数量与单个订阅者的消息条数上限。
# 上限取接口允许的最大 limit（200），更早的消息只计入 total / unread。
CACHE_MAX_SUBSCRIBERS = 256
CACHE_MAX_ITEMS = 200

# 实时推送的默认轮询间隔与心跳节奏。文件后端没有变更通知，只能按版本号轮询；
# 1 秒是「人眼能感觉到实时」与「不为空转烧 CPU」之间的折中，客户端可以按需调大。
DEFAULT_STREAM_INTERVAL = 1.0
MAX_STREAM_INTERVAL = 30.0
# 连续这么多次轮询没有变化时发一条 SSE 注释心跳：中间的代理会掐掉长时间无数据的连接。
STREAM_HEARTBEAT_POLLS = 15
# 每次推送携带的条目数：首帧要够填满首屏，又不该把整份收件箱塞进一个事件。
DEFAULT_STREAM_ITEMS = 20

# 存储后端未提供 revision 时使用的兜底序号：每次都视为已变化，
# 即退化为不缓存——正确性优先于命中率。
_NO_REVISION_SEQ = itertools.count(1)


class InboxSnapshot(NamedTuple):
    """一次收件箱读取的结果：按时间倒序的消息、未读条数、消息总数。"""

    items: list[dict]
    unread: int
    total: int


class InboxCache:
    """按版本号缓存每个订阅者的收件箱快照（LRU，容量有限）。

    快照的键是「来源版本 + 已读版本」，命中时整个读取路径不产生 I/O；
    未命中时重算并回填。缓存只保存最新的 ``max_items`` 条消息，
    未读条数与消息总数仍按全部消息统计。
    """

    def __init__(
        self,
        max_subscribers: int = CACHE_MAX_SUBSCRIBERS,
        max_items: int = CACHE_MAX_ITEMS,
    ) -> None:
        self._max_subscribers = max(1, max_subscribers)
        self._max_items = max(1, max_items)
        self._entries: "OrderedDict[str, tuple[object, InboxSnapshot]]" = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, subscriber_id: str, revision: object) -> Optional[InboxSnapshot]:
        """命中则返回快照；版本不符或不存在返回 None。"""
        with self._lock:
            entry = self._entries.get(subscriber_id)
            if entry is not None and entry[0] == revision:
                self._entries.move_to_end(subscriber_id)
                self._hits += 1
                return entry[1]
            self._misses += 1
            return None

    def put(self, subscriber_id: str, revision: object, snapshot: InboxSnapshot) -> InboxSnapshot:
        """写入快照（只保留最新的 ``max_items`` 条），返回实际缓存的内容。"""
        trimmed = InboxSnapshot(
            snapshot.items[: self._max_items], snapshot.unread, snapshot.total
        )
        with self._lock:
            self._entries[subscriber_id] = (revision, trimmed)
            self._entries.move_to_end(subscriber_id)
            while len(self._entries) > self._max_subscribers:
                self._entries.popitem(last=False)
        return trimmed

    def invalidate(self, subscriber_id: Optional[str] = None) -> None:
        """丢弃某个订阅者（缺省为全部）的缓存。"""
        with self._lock:
            if subscriber_id is None:
                self._entries.clear()
            else:
                self._entries.pop(subscriber_id, None)

    def stats(self) -> dict:
        """缓存命中情况，便于排查「一直未命中」这类退化。"""
        with self._lock:
            return {
                "subscribers": len(self._entries),
                "hits": self._hits,
                "misses": self._misses,
            }


_inbox_cache: Optional[InboxCache] = None
_inbox_cache_lock = threading.Lock()


def get_inbox_cache() -> InboxCache:
    """获取全局收件箱缓存单例（惰性初始化）。"""
    global _inbox_cache
    with _inbox_cache_lock:
        if _inbox_cache is None:
            _inbox_cache = InboxCache()
        return _inbox_cache


def _engine_inbox_revision() -> tuple:
    """引擎收件箱文件的版本：取文件元数据，不读内容。"""
    path = _INBOX_FILE
    try:
        st = path.stat()
    except OSError:
        return ("absent", str(path))
    return ("file", str(path), st.st_dev, st.st_ino, st.st_size, str(st.st_mtime_ns))


def _storage_revision() -> object:
    """投递存储的版本号；后端未实现 ``revision`` 时退化为每次都变。"""
    store = get_storage()
    fn = getattr(store, "revision", None)
    if callable(fn):
        try:
            return ("rev", fn())
        except Exception:  # noqa: BLE001 - 版本探测失败不能影响读取
            logger.warning("读取投递存储版本失败，本轮不缓存", exc_info=True)
    return ("norev", next(_NO_REVISION_SEQ))


def _read_revision(subscriber_id: str) -> object:
    """该订阅者的已读版本；存储未实现 ``revision`` 时退化为每次都变。"""
    store = get_read_store()
    fn = getattr(store, "revision", None)
    if callable(fn):
        return ("read", fn(subscriber_id))
    return ("noreadrev", next(_NO_REVISION_SEQ))


def _revision(subscriber_id: str) -> tuple:
    """合成快照版本：收件箱来源 + 投递存储 + 该订阅者的已读状态。"""
    return (
        _engine_inbox_revision(),
        _storage_revision(),
        _read_revision(subscriber_id),
    )


def _build_snapshot(subscriber_id: str) -> InboxSnapshot:
    """扫描全部来源，算出该订阅者的收件箱快照。"""
    items = _collect_inbox(subscriber_id)
    unread = sum(1 for i in items if not i["read"])
    return InboxSnapshot(items, unread, len(items))


def _cache_key(subscriber_id: str) -> str:
    """缓存键：租户 + 环境 + 订阅者。

    同一订阅者标识可能出现在多个租户里，只用标识做键会让一个租户读到自己
    不该看到的收件箱快照。
    """
    from . import tenancy

    return f"{tenancy.current_tenant()}:{tenancy.current_environment()}:{subscriber_id}"


def _snapshot(subscriber_id: str) -> InboxSnapshot:
    """取收件箱快照：版本未变直接复用缓存，否则重算并回填。"""
    cache = get_inbox_cache()
    key = _cache_key(subscriber_id)
    revision = _revision(subscriber_id)
    cached = cache.get(key, revision)
    if cached is not None:
        return cached
    return cache.put(key, revision, _build_snapshot(subscriber_id))


def _refresh_snapshot(subscriber_id: str) -> InboxSnapshot:
    """强制重算并回填缓存（读状态刚被改写时用，避免多扫一遍旧数据）。"""
    cache = get_inbox_cache()
    key = _cache_key(subscriber_id)
    revision = _revision(subscriber_id)
    return cache.put(key, revision, _build_snapshot(subscriber_id))



def _is_inapp(record: dict) -> bool:
    """判断一条投递记录是否属于 In-app 渠道。"""
    channels = record.get("channels") or []
    if isinstance(channels, str):
        channels = [channels]
    return _INAPP_CHANNEL in channels


def _parse_dt(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _message_id(record: dict) -> Optional[str]:
    """取消息 ID：引擎条目用自身 ``id``，投递记录兜底用 ``trigger_id``。

    引擎条目的 ID 在整个聚合窗口内保持不变，因此同一张卡片被多次互动改写后
    仍然只有一条消息，不会在收件箱里复制出多份。
    """
    if not isinstance(record, dict):
        return None
    raw = record.get("id") or record.get("trigger_id")
    mid = str(raw).strip() if raw else ""
    return mid or None


def _read_flag(
    store: "ReadStateStore",
    subscriber_id: str,
    message_id: Optional[str],
    updated_at: Optional[datetime],
    trigger_id: Optional[str],
) -> bool:
    """综合「已读时刻 vs 更新时间」与历史记录，判定一条消息是否已读。"""
    if not message_id:
        return True
    if store.is_read(subscriber_id, message_id, updated_at):
        return True
    # 历史记录以 trigger_id 为粒度，没有时刻；命中即视为已读，避免旧消息复活。
    if trigger_id and trigger_id != message_id and store.is_legacy_read(subscriber_id, trigger_id):
        return True
    return False


def _to_inbox_item(record: dict, read: bool) -> dict:
    payload = record.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    workflow = record.get("workflow") or record.get("name") or "unknown"
    title = payload.get("title") or workflow
    body = payload.get("body") or payload.get("message") or ""
    return {
        "message_id": _message_id(record),
        "trigger_id": record.get("trigger_id"),
        "workflow": workflow,
        "title": title,
        "body": body,
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "read": read,
    }


def _in_scope(entry: dict, tenant: str, environment: str) -> bool:
    """判断一条收件箱来源记录是否属于当前租户与环境。

    老数据没有这两个字段：按默认租户与开发环境处理，升级后原有收件箱
    内容不会突然消失。
    """
    if (entry.get("tenant") or "default") != tenant:
        return False
    return (entry.get("environment") or "development") == environment


def _engine_inbox_items(subscriber_id: str, store: "ReadStateStore") -> list[dict]:
    """读取引擎 in_app 步骤写入的收件箱文件，转换为收件箱条目。

    文件不存在（尚未运行引擎）或单行损坏时跳过，不影响兜底来源。
    文件是多个租户共用的追加流，这里按租户与环境过滤。

    同一条消息被改写后会多出一行新版本，旧版本留在文件里，因此按消息 ID
    只保留最后一行：聚合卡片每命中一次互动就会改写，取先出现的那行会读到
    过期的计数。
    """
    from . import tenancy

    tenant = tenancy.current_tenant()
    environment = tenancy.current_environment()
    path = _INBOX_FILE
    if not path.exists():
        return []
    latest: dict[str, dict] = {}
    anonymous: list[dict] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("跳过损坏的收件箱行：%s", line[:80])
                    continue
                if entry.get("subscriber_id") != subscriber_id:
                    continue
                if not _in_scope(entry, tenant, environment):
                    continue
                tid = entry.get("trigger_id")
                message_id = _message_id(entry)
                created_at = entry.get("created_at")
                updated_at = entry.get("updated_at") or created_at
                item = {
                    "message_id": message_id,
                    "trigger_id": tid,
                    "workflow": entry.get("workflow") or "unknown",
                    "title": entry.get("title") or entry.get("workflow") or "新通知",
                    "body": entry.get("body") or "",
                    "created_at": created_at,
                    "updated_at": entry.get("updated_at"),
                    "read": _read_flag(
                        store, subscriber_id, message_id, _parse_dt(updated_at), tid
                    ),
                }
                if message_id:
                    latest[message_id] = item
                else:
                    anonymous.append(item)
    except OSError:
        logger.warning("读取引擎收件箱文件失败：%s", path)
    return [*latest.values(), *anonymous]


def _collect_inbox(subscriber_id: str) -> list[dict]:
    """汇总某订阅者的全部 In-app 消息（引擎收件箱 + 投递记录兜底）。

    两个来源按消息 ID 去重，并额外按 trigger_id 互斥：同一次触发可能既有引擎
    写入的收件箱条目、又有 API 侧的投递记录，引擎条目内容更准确，优先保留。
    """
    store = get_read_store()
    seen_messages: set[str] = set()
    seen_triggers: set[str] = set()
    items: list[dict] = []

    for item in _engine_inbox_items(subscriber_id, store):
        mid = item.get("message_id")
        tid = item.get("trigger_id")
        if mid and mid in seen_messages:
            continue
        if tid and tid in seen_triggers:
            continue
        if mid:
            seen_messages.add(mid)
        if tid:
            seen_triggers.add(tid)
        items.append(item)

    from . import tenancy

    tenant = tenancy.current_tenant()
    environment = tenancy.current_environment()
    for r in get_storage().read_all():
        if r.get("subscriber_id") != subscriber_id:
            continue
        if not _in_scope(r, tenant, environment):
            continue
        if not _is_inapp(r):
            continue
        mid = _message_id(r)
        tid = r.get("trigger_id")
        if mid and mid in seen_messages:
            continue
        if tid and tid in seen_triggers:
            continue
        if mid:
            seen_messages.add(mid)
        if tid:
            seen_triggers.add(tid)
        items.append(
            _to_inbox_item(
                r,
                _read_flag(store, subscriber_id, mid, _parse_dt(r.get("updated_at")), tid),
            )
        )

    items.sort(key=_page_key, reverse=True)
    return items


def _sort_ts(item: dict) -> datetime:
    """排序时间：优先最近更新时间（聚合卡片被顶起来），回落到创建时间。"""
    return _parse_dt(item.get("updated_at")) or _parse_dt(item.get("created_at")) or _EPOCH


def _page_key(item: dict) -> tuple[datetime, str]:
    """排序与游标共用的键：``(排序时间, 消息 ID)``。

    时间只到秒级，同一秒内写入的消息若只按时间排，顺序取决于底层读取顺序；
    翻页时两次请求各算出一种顺序，同一条消息就会出现在两页里，或者干脆漏掉。
    补一个消息 ID 让顺序完全确定。
    """
    return (_sort_ts(item), str(item.get("message_id") or ""))


class InboxCursorError(ValueError):
    """翻页游标无法解析（抄错了，或者不是本接口返回的形态）。"""


def encode_cursor(item: dict) -> str:
    """把一条消息的排序键编码成游标：不透明字符串，调用方不必关心内容。"""
    stamp, message_id = _page_key(item)
    payload = f"{stamp.isoformat()}|{message_id}"
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(raw: str) -> tuple[datetime, str]:
    """还原游标；形态不对时抛 :class:`InboxCursorError`。"""
    text = str(raw).strip()
    padded = text + "=" * (-len(text) % 4)
    try:
        payload = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, UnicodeEncodeError) as exc:
        raise InboxCursorError("游标格式不正确，请原样传回上一页的 nextCursor") from exc
    stamp_text, _, message_id = payload.partition("|")
    stamp = _parse_dt(stamp_text)
    if stamp is None:
        raise InboxCursorError("游标里的时间戳无法解析，请从第一页重新翻")
    return stamp, message_id


def _filter_items(
    items: list[dict],
    *,
    unread_only: bool = False,
    workflow: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list[dict]:
    """按「只未读 / 工作流 / 时间区间」过滤，输入已是倒序列表。"""
    selected: list[dict] = []
    for item in items:
        if unread_only and item.get("read"):
            continue
        if workflow is not None and str(item.get("workflow") or "") != workflow:
            continue
        stamp = _sort_ts(item)
        if since is not None and stamp < since:
            continue
        if until is not None and stamp >= until:
            continue
        selected.append(item)
    return selected


def _page(
    items: list[dict],
    *,
    limit: int,
    cursor_key: Optional[tuple[datetime, str]] = None,
) -> tuple[list[dict], bool]:
    """从倒序列表里取一页，返回 ``(本页消息, 还有没有下一页)``。

    游标指向上一页最后一条，本页从它的**严格后一条**开始：翻页期间顶上来了
    新消息也不会重复返回，游标指向的消息被删掉也不会卡住。
    """
    start = 0
    if cursor_key is not None:
        for index, item in enumerate(items):
            if _page_key(item) < cursor_key:
                start = index
                break
        else:
            return [], False
    window = items[start : start + limit]
    return window, start + len(window) < len(items)


def _list_inbox(subscriber_id: str, limit: int) -> list[dict]:
    """按时间倒序返回某订阅者的 In-app 消息列表（最多 limit 条）。"""
    page, _ = _page(_snapshot(subscriber_id).items, limit=limit)
    return page


def _time_bound(raw: Optional[str], name: str) -> Optional[datetime]:
    """解析时间筛选参数；空值返回 ``None``，写法不对直接 400。"""
    if raw is None or not raw.strip():
        return None
    stamp = _parse_dt(raw)
    if stamp is None:
        raise HTTPException(
            status_code=400,
            detail=f"{name} 不是合法的时间，请用 ISO 8601（如 2026-11-01T00:00:00Z）",
        )
    return stamp


# ---------------------------------------------------------------- 实时推送


def _sse(event: str, payload: dict, event_id: Optional[str] = None) -> str:
    """拼一帧 SSE 报文：``id`` 可选，``data`` 一定是单行 JSON。"""
    lines: list[str] = []
    if event_id:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append("data: " + json.dumps(payload, ensure_ascii=False))
    return "\n".join(lines) + "\n\n"


def _stream_payload(snapshot: "InboxSnapshot", limit: int) -> dict:
    return {
        "unread": snapshot.unread,
        "total": snapshot.total,
        "items": snapshot.items[:limit],
    }


async def iter_inbox_events(
    subscriber_id: str,
    *,
    limit: int = DEFAULT_STREAM_ITEMS,
    interval: float = DEFAULT_STREAM_INTERVAL,
    max_events: Optional[int] = None,
    max_polls: Optional[int] = None,
    sleep=asyncio.sleep,
):
    """按版本变化持续产出收件箱事件（SSE）。

    首帧 ``ready`` 带上当前列表，客户端不必再发一次 ``GET /v1/inbox``；之后只在
    「收件箱来源 + 已读状态」的版本变化时推 ``inbox``，没变化就只发注释心跳。
    连接断开时 ``StreamingResponse`` 会关闭生成器，不需要额外的停止信号。

    写成异步生成器是为了不占线程池：同步生成器的 ``sleep`` 在线程池线程里执行，
    几十个空闲连接就能把默认容量占满，连带拖慢其它同步端点。这里只有读文件
    那一步走 ``to_thread``，等待期间不占线程。

    两个上限参数只给测试与短连接用：``max_events`` 限制产出的帧数，
    ``max_polls`` 限制轮询轮数，端点不传时按连接生命周期一直跑。
    """
    snapshot = await asyncio.to_thread(_snapshot, subscriber_id)
    revision = await asyncio.to_thread(_revision, subscriber_id)
    yield _sse("ready", _stream_payload(snapshot, limit))
    emitted = 1
    if max_events is not None and emitted >= max_events:
        return

    idle = 0
    polls = 0
    while True:
        if max_polls is not None and polls >= max_polls:
            return
        await sleep(interval)
        polls += 1
        idle += 1
        if idle % STREAM_HEARTBEAT_POLLS == 0:
            yield ": keep-alive\n\n"
            emitted += 1
            if max_events is not None and emitted >= max_events:
                return

        current = await asyncio.to_thread(_revision, subscriber_id)
        if current == revision:
            continue
        revision = current
        snapshot = await asyncio.to_thread(_snapshot, subscriber_id)
        yield _sse("inbox", _stream_payload(snapshot, limit))
        emitted += 1
        if max_events is not None and emitted >= max_events:
            return


@router.get(
    "/stream",
    summary="实时订阅收件箱变化（SSE）",
    description=(
        "以 ``text/event-stream`` 持续推送收件箱变化：首帧 ``ready`` 给出当前"
        "未读数、总数与最近消息，之后每次来源或已读状态变化推一帧 ``inbox``。\n\n"
        "事件格式::\n\n"
        "    event: inbox\n"
        "    data: {\"unread\": 3, \"total\": 12, \"items\": [...]}\n\n"
        "连接空闲时每 15 个轮询周期发一条注释心跳（``: keep-alive``），"
        "避免中间代理掐掉长时间无数据的连接。客户端断开即结束。"
    ),
)
def stream_inbox(
    subscriber_id: str = Query(alias="subscriberId", min_length=1, description="订阅者唯一标识"),
    limit: int = Query(
        default=DEFAULT_STREAM_ITEMS, ge=1, le=MAX_INBOX_LIMIT, description="每帧携带的条数"
    ),
    interval: float = Query(
        default=DEFAULT_STREAM_INTERVAL,
        ge=0.1,
        le=MAX_STREAM_INTERVAL,
        description="轮询间隔（秒），越小越实时，越大越省资源",
    ),
    _env: EnvContext = Depends(require_env),
) -> StreamingResponse:
    return StreamingResponse(
        iter_inbox_events(subscriber_id, limit=limit, interval=interval),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # 反向代理（nginx 等）默认把响应攒满缓冲区再发，SSE 需要逐帧下发。
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------- 未读聚合


class UnreadSummary(NamedTuple):
    """未读计数的聚合结果：总数 + 按工作流分组 + 最近一条时间。"""

    unread: int
    total: int
    by_workflow: list[dict]
    latest_at: Optional[str]


def _build_unread_summary(subscriber_id: str) -> UnreadSummary:
    """扫描收件箱，按工作流汇总未读与总数。"""
    items = _collect_inbox(subscriber_id)
    buckets: dict[str, list[int]] = {}
    latest: Optional[datetime] = None
    for item in items:
        workflow = item.get("workflow") or "unknown"
        bucket = buckets.setdefault(workflow, [0, 0])
        bucket[1] += 1
        if not item.get("read"):
            bucket[0] += 1
        stamp = _sort_ts(item)
        if latest is None or stamp > latest:
            latest = stamp

    rows = [
        {"workflow": name, "unread": counts[0], "total": counts[1]}
        for name, counts in buckets.items()
    ]
    # 未读多的排前面；同未读按总量、再按名称，保证顺序稳定。
    rows.sort(key=lambda row: (-row["unread"], -row["total"], row["workflow"]))
    return UnreadSummary(
        unread=sum(row["unread"] for row in rows),
        total=len(items),
        by_workflow=rows,
        latest_at=None if latest is None or latest == _EPOCH else latest.isoformat(),
    )


class UnreadSummaryCache:
    """按版本号缓存未读聚合（LRU，容量与收件箱快照一致）。

    不复用 ``InboxCache`` 的原因：快照只保留最近 200 条，用它算不出「按工作流
    分组」的完整分布；聚合结果轻得多（每个工作流一行），可以单独缓存。
    """

    def __init__(self, max_subscribers: int = CACHE_MAX_SUBSCRIBERS) -> None:
        self._max_subscribers = max(1, max_subscribers)
        self._entries: "OrderedDict[str, tuple[object, UnreadSummary]]" = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, subscriber_id: str, revision: object) -> Optional[UnreadSummary]:
        with self._lock:
            entry = self._entries.get(subscriber_id)
            if entry is not None and entry[0] == revision:
                self._entries.move_to_end(subscriber_id)
                self._hits += 1
                return entry[1]
            self._misses += 1
            return None

    def put(
        self, subscriber_id: str, revision: object, summary: UnreadSummary
    ) -> UnreadSummary:
        with self._lock:
            self._entries[subscriber_id] = (revision, summary)
            self._entries.move_to_end(subscriber_id)
            while len(self._entries) > self._max_subscribers:
                self._entries.popitem(last=False)
        return summary

    def invalidate(self, subscriber_id: Optional[str] = None) -> None:
        with self._lock:
            if subscriber_id is None:
                self._entries.clear()
            else:
                self._entries.pop(subscriber_id, None)

    def stats(self) -> dict:
        with self._lock:
            return {
                "subscribers": len(self._entries),
                "hits": self._hits,
                "misses": self._misses,
            }


_unread_cache: Optional[UnreadSummaryCache] = None
_unread_cache_lock = threading.Lock()


def get_unread_cache() -> UnreadSummaryCache:
    """获取全局未读聚合缓存单例（惰性初始化）。"""
    global _unread_cache
    with _unread_cache_lock:
        if _unread_cache is None:
            _unread_cache = UnreadSummaryCache()
        return _unread_cache


def unread_summary(subscriber_id: str) -> UnreadSummary:
    """取未读聚合：版本未变复用缓存，否则重算并回填。"""
    cache = get_unread_cache()
    revision = _revision(subscriber_id)
    cached = cache.get(subscriber_id, revision)
    if cached is not None:
        return cached
    return cache.put(subscriber_id, revision, _build_unread_summary(subscriber_id))


@router.get(
    "/unread",
    summary="未读计数聚合（按工作流）",
    description=(
        "返回该订阅者的未读总数、消息总数、按工作流分组的未读分布，以及最近一条"
        "消息的时间。只统计 In-app 消息，口径与 ``GET /v1/inbox`` 一致：同一张聚合"
        "卡片无论被多少次互动改写都只算一条。结果按版本号缓存，任一来源写入或标记"
        "已读后自动重算。"
    ),
)
def get_unread_summary(
    subscriber_id: str = Query(alias="subscriberId", min_length=1, description="订阅者唯一标识"),
    _env: EnvContext = Depends(require_env),
) -> dict:
    summary = unread_summary(subscriber_id)
    return {
        "unread": summary.unread,
        "total": summary.total,
        "byWorkflow": summary.by_workflow,
        "latestAt": summary.latest_at,
    }


@router.get(
    "",
    summary="读取收件箱",
    description=(
        "返回某订阅者的 In-app 消息与未读计数，按「排序时间 + 消息标识」倒序。\n\n"
        "**计数口径**：``unread`` / ``total`` 覆盖全部消息，不受筛选与 ``limit`` 影响；"
        "``matched`` 是本次筛选命中的条数，翻页时以它为准。同一张聚合卡片无论被多少次"
        "互动改写都只算一条消息，所以点赞涨到 99 条未读也不会变成 99。\n\n"
        "**翻页**：游标编码上一页最后一条的排序键，从它的严格后一条继续；翻页期间顶上来的"
        "新消息不会重复返回，游标指向的消息被删掉也不会卡住。游标解析失败返回 400，"
        "而不是静默回到第一页。"
    ),
)
def get_inbox(
    subscriber_id: str = Query(alias="subscriberId", min_length=1, description="订阅者唯一标识"),
    limit: int = Query(
        default=DEFAULT_INBOX_LIMIT, ge=1, le=MAX_INBOX_LIMIT, description="每页条数"
    ),
    cursor: Optional[str] = Query(default=None, description="上一页返回的 nextCursor"),
    unread: bool = Query(default=False, description="只看未读"),
    workflow: Optional[str] = Query(
        default=None, description="只看某个工作流的消息，如 post_liked"
    ),
    since: Optional[str] = Query(default=None, description="时间下界（含），ISO 8601"),
    until: Optional[str] = Query(default=None, description="时间上界（不含），ISO 8601"),
    _env: EnvContext = Depends(require_env),
) -> dict:
    """返回某订阅者的 In-app 收件箱消息与未读计数。

    未读数按消息条数统计：同一张聚合卡片无论被多少次互动改写，都只算一条。
    未读与总数覆盖该订阅者的全部消息，不受筛选与 ``limit`` 影响；``matched``
    是本次筛选命中的条数，翻页时以它为准。内容没变化时直接复用缓存快照，
    不重新扫描收件箱文件与投递记录。
    """
    since_dt = _time_bound(since, "since")
    until_dt = _time_bound(until, "until")
    if since_dt is not None and until_dt is not None and since_dt >= until_dt:
        raise HTTPException(status_code=400, detail="since 必须早于 until")

    cursor_key: Optional[tuple[datetime, str]] = None
    if cursor:
        try:
            cursor_key = decode_cursor(cursor)
        except InboxCursorError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    workflow_filter = (workflow or "").strip() or None
    narrowed = (
        unread or workflow_filter is not None or since_dt is not None or until_dt is not None
    )

    snapshot = _snapshot(subscriber_id)
    if not narrowed and cursor_key is None:
        # 首屏：直接用缓存快照，不额外扫描。
        page, _ = _page(snapshot.items, limit=limit)
        matched = snapshot.total
        has_more = snapshot.total > len(page)
    else:
        # 筛选或深翻页要按全部消息算命中数，缓存里只有最近 200 条，不能拿来算。
        selected = _filter_items(
            _collect_inbox(subscriber_id),
            unread_only=unread,
            workflow=workflow_filter,
            since=since_dt,
            until=until_dt,
        )
        page, has_more = _page(selected, limit=limit, cursor_key=cursor_key)
        matched = len(selected)

    return {
        "items": page,
        "unread": snapshot.unread,
        "total": snapshot.total,
        "matched": matched,
        "hasMore": has_more,
        "nextCursor": encode_cursor(page[-1]) if has_more and page else None,
    }


@router.post(
    "/read",
    summary="标记消息已读",
    description=(
        "支持三种粒度：给 ``messageIds`` 标记若干条、给 ``triggerIds`` 按触发粒度标记"
        "（兼容早期调用方）、两者都不给则整份标记已读。\n\n"
        "回执给出本次实际变更的条数（``marked``）与最新未读数。重复标记是幂等的；标记一条不存在的"
        "消息不会报错，``marked=0`` 就是结论——调用方的目的已经达成。"
    ),
)
def mark_read(req: ReadRequest, _env: EnvContext = Depends(require_env)) -> dict:
    """标记消息已读，返回本次操作影响的消息数与最新未读计数。"""
    store = get_read_store()
    # 标记目标需要覆盖全部消息（含超过缓存条数上限的部分），这里取完整列表。
    items = _collect_inbox(req.subscriber_id)
    by_message = {i["message_id"]: i for i in items if i.get("message_id")}
    by_trigger = {i["trigger_id"]: i for i in items if i.get("trigger_id")}

    if req.message_ids is not None:
        targets = [by_message.get(mid) for mid in req.message_ids]
    elif req.trigger_ids is not None:
        targets = [by_trigger.get(tid) for tid in req.trigger_ids]
    else:
        targets = list(items)

    changed = 0
    for item in targets:
        if not item:
            continue
        mid = item.get("message_id")
        if not mid:
            continue
        if store.mark_read(req.subscriber_id, mid, _parse_dt(item.get("updated_at"))):
            changed += 1

    # 读状态变了：重算一次快照回填缓存，后续读取直接命中。
    snapshot = _refresh_snapshot(req.subscriber_id) if changed else _snapshot(req.subscriber_id)
    return {"marked": changed, "unread": snapshot.unread}


__all__ = [
    "DEFAULT_INBOX_LIMIT",
    "DEFAULT_STREAM_INTERVAL",
    "DEFAULT_STREAM_ITEMS",
    "MAX_INBOX_LIMIT",
    "MAX_STREAM_INTERVAL",
    "router",
    "InboxCache",
    "InboxCursorError",
    "InboxSnapshot",
    "ReadStateStore",
    "UnreadSummary",
    "UnreadSummaryCache",
    "decode_cursor",
    "encode_cursor",
    "get_inbox_cache",
    "get_read_store",
    "get_unread_cache",
    "iter_inbox_events",
    "unread_summary",
    "_collect_inbox",
    "_filter_items",
    "_is_inapp",
    "_list_inbox",
    "_message_id",
    "_page",
    "_page_key",
    "_snapshot",
    "_sort_ts",
    "_to_inbox_item",
]

