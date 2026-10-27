"""In-app 收件箱（ROADMAP Phase S）。

提供订阅者维度的 In-app 通知中心能力：
- ``GET /v1/inbox``：返回某订阅者的 In-app 消息列表与未读计数。
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
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from .storage import get_storage

logger = logging.getLogger("xw.inbox")

router = APIRouter(prefix="/v1/inbox", tags=["inbox"])

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_READ_STATE_FILE = _DATA_DIR / "read_state.jsonl"

# 引擎侧 in_app 步骤写入的收件箱文件（与 engine 默认路径一致）。
# 引擎可通过 XW_INBOX_FILE 覆盖路径，此处同样读取该环境变量。
_INBOX_FILE = Path(os.getenv("XW_INBOX_FILE") or _DATA_DIR / "inbox.jsonl")

# In-app 渠道标识（后端落库用下划线命名）
_INAPP_CHANNEL = "in_app"

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
            self._append(subscriber_id, message_id, now)
            return True

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
_read_store_lock = threading.Lock()


def get_read_store() -> ReadStateStore:
    """获取全局已读状态单例（惰性初始化）。"""
    global _read_store
    with _read_store_lock:
        if _read_store is None:
            _read_store = ReadStateStore()
        return _read_store


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


def _engine_inbox_items(subscriber_id: str, store: "ReadStateStore") -> list[dict]:
    """读取引擎 in_app 步骤写入的收件箱文件，转换为收件箱条目。

    文件不存在（尚未运行引擎）或单行损坏时跳过，不影响兜底来源。
    """
    path = _INBOX_FILE
    if not path.exists():
        return []
    items: list[dict] = []
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
                tid = entry.get("trigger_id")
                message_id = _message_id(entry)
                created_at = entry.get("created_at")
                updated_at = entry.get("updated_at") or created_at
                items.append(
                    {
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
                )
    except OSError:
        logger.warning("读取引擎收件箱文件失败：%s", path)
    return items


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

    for r in get_storage().read_all():
        if r.get("subscriber_id") != subscriber_id:
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

    items.sort(key=lambda i: _sort_ts(i), reverse=True)
    return items


def _sort_ts(item: dict) -> datetime:
    """排序时间：优先最近更新时间（聚合卡片被顶起来），回落到创建时间。"""
    return _parse_dt(item.get("updated_at")) or _parse_dt(item.get("created_at")) or _EPOCH


def _list_inbox(subscriber_id: str, limit: int) -> list[dict]:
    """按时间倒序返回某订阅者的 In-app 消息列表（最多 limit 条）。"""
    return _collect_inbox(subscriber_id)[:limit]


@router.get("")
def get_inbox(
    subscriber_id: str = Query(alias="subscriberId", min_length=1, description="订阅者唯一标识"),
    limit: int = Query(default=50, ge=1, le=200, description="返回条数上限"),
) -> dict:
    """返回某订阅者的 In-app 收件箱消息与未读计数。

    未读数按消息条数统计：同一张聚合卡片无论被多少次互动改写，都只算一条。
    """
    items = _list_inbox(subscriber_id, limit)
    unread = sum(1 for i in items if not i["read"])
    return {"items": items, "unread": unread, "total": len(items)}


@router.post("/read")
def mark_read(req: ReadRequest) -> dict:
    """标记消息已读，返回本次操作影响的消息数与最新未读计数。"""
    store = get_read_store()
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

    unread = sum(1 for i in _collect_inbox(req.subscriber_id) if not i["read"])
    return {"marked": changed, "unread": unread}


__all__ = [
    "router",
    "ReadStateStore",
    "get_read_store",
    "_is_inapp",
    "_message_id",
    "_to_inbox_item",
    "_collect_inbox",
    "_list_inbox",
]
