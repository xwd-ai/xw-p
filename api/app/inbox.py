"""In-app 收件箱（ROADMAP Phase S）。

提供订阅者维度的 In-app 通知中心能力：
- ``GET /v1/inbox``：返回某订阅者的 In-app 消息列表与未读计数。
- ``POST /v1/inbox/read``：标记一条或多条消息为已读（未指定则全部已读）。

数据来源：
- 消息体复用 ``storage`` 中已落库的投递记录（``channels`` 含 ``in_app``）。
- 已读状态由 ``ReadStateStore`` 维护，JSONL 持久化到 ``api/data/read_state.jsonl``，
  与消息存储的 JSONL 风格保持一致（进程内缓存 + 追加落盘）。
"""
from __future__ import annotations

import json
import logging
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

# In-app 渠道标识（后端落库用下划线命名）
_INAPP_CHANNEL = "in_app"


class ReadRequest(BaseModel):
    """标记已读请求体。"""

    subscriber_id: str = Field(alias="subscriberId", min_length=1, description="订阅者唯一标识")
    trigger_ids: Optional[list[str]] = Field(
        default=None, alias="triggerIds", description="要标记为已读的 trigger_id 列表；缺省表示全部已读"
    )

    model_config = {"populate_by_name": True}


class ReadStateStore:
    """订阅者已读状态存储：进程内缓存 + JSONL 追加落盘。

    以 ``(subscriber_id, trigger_id)`` 为粒度记录已读；启动时从文件回放，
    运行期写入即时更新缓存并追加一行落盘。
    """

    def __init__(self, path: Path = _READ_STATE_FILE) -> None:
        self._path = path
        # subscriber_id -> set[trigger_id]
        self._read: dict[str, set[str]] = {}
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
                    tid = item.get("trigger_id")
                    if sub and tid:
                        self._read.setdefault(sub, set()).add(tid)
        except OSError:
            logger.warning("读取已读状态文件失败：%s", self._path)

    def _append(self, subscriber_id: str, trigger_id: str) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(
                        {"subscriber_id": subscriber_id, "trigger_id": trigger_id},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        except OSError:
            logger.warning("写入已读状态文件失败：%s", self._path)

    def mark_read(self, subscriber_id: str, trigger_id: str) -> bool:
        """标记某条消息已读；返回 True 表示本次新增（首次标记）。"""
        with self._lock:
            s = self._read.setdefault(subscriber_id, set())
            if trigger_id in s:
                return False
            s.add(trigger_id)
            self._append(subscriber_id, trigger_id)
            return True

    def is_read(self, subscriber_id: str, trigger_id: str) -> bool:
        with self._lock:
            return trigger_id in self._read.get(subscriber_id, set())


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


def _to_inbox_item(record: dict, read: bool) -> dict:
    payload = record.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    workflow = record.get("workflow") or record.get("name") or "unknown"
    title = payload.get("title") or workflow
    body = payload.get("body") or payload.get("message") or ""
    return {
        "trigger_id": record.get("trigger_id"),
        "workflow": workflow,
        "title": title,
        "body": body,
        "created_at": record.get("created_at"),
        "read": read,
    }


def _list_inbox(subscriber_id: str, limit: int) -> list[dict]:
    """按时间倒序返回某订阅者的 In-app 消息列表。"""
    store = get_read_store()
    items: list[dict] = []
    for r in get_storage().read_all():
        if r.get("subscriber_id") != subscriber_id:
            continue
        if not _is_inapp(r):
            continue
        tid = r.get("trigger_id")
        items.append(_to_inbox_item(r, store.is_read(subscriber_id, tid) if tid else True))

    def _sort_key(item: dict):
        dt = _parse_dt(item.get("created_at"))
        return dt.timestamp() if dt else float("-inf")

    items.sort(key=_sort_key, reverse=True)
    return items[:limit]


@router.get("")
def get_inbox(
    subscriber_id: str = Query(alias="subscriberId", min_length=1, description="订阅者唯一标识"),
    limit: int = Query(default=50, ge=1, le=200, description="返回条数上限"),
) -> dict:
    """返回某订阅者的 In-app 收件箱消息与未读计数。"""
    items = _list_inbox(subscriber_id, limit)
    unread = sum(1 for i in items if not i["read"])
    return {"items": items, "unread": unread, "total": len(items)}


@router.post("/read")
def mark_read(req: ReadRequest) -> dict:
    """标记消息已读，返回本次操作影响的消息数与最新未读计数。"""
    store = get_read_store()
    # 收集该订阅者当前全部 In-app 消息的 trigger_id
    existing_ids = [
        r.get("trigger_id")
        for r in get_storage().read_all()
        if r.get("subscriber_id") == req.subscriber_id and _is_inapp(r)
    ]
    targets = req.trigger_ids if req.trigger_ids is not None else existing_ids

    changed = 0
    for tid in targets:
        if not tid:
            continue
        if store.mark_read(req.subscriber_id, tid):
            changed += 1

    unread = sum(1 for tid in existing_ids if not store.is_read(req.subscriber_id, tid))
    return {"marked": changed, "unread": unread}


__all__ = ["router", "ReadStateStore", "get_read_store", "_is_inapp", "_list_inbox"]
