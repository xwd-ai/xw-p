"""活动流查询 API。

提供 ``GET /v1/activity``，从活动日志存储（``activity_log``）读取触发事件，
返回按时间倒序的活动列表，供前端「活动流」消费。

过滤维度：
- ``subscriber_id``：按订阅者过滤。
- ``workflow``：按工作流名过滤。
- ``trigger_id``：按触发唯一标识精确查询。
- ``status``：按活动状态过滤（``triggered`` / ``sent`` / ``failed``）。
- ``since`` / ``until``：按创建时间范围过滤（ISO 8601，含边界）。
- ``environment``：按隔离环境过滤。

分页用游标而不是 offset：响应里的 ``nextCursor`` 原样回传即可取下一页。
游标是上一页最后一条记录的 ``(created_at, trigger_id)``，翻页期间有新记录写入
也不会导致漏读或重复。过滤与排序由存储后端完成（JSONL 从文件尾部倒序读、
Postgres 走 INDEX + LIMIT），接口层不再把整表读进内存筛选。

数据来源为独立的活动日志（见 ``activity_log.py``），在触发 API 返回回执前
同步写入，因此活动流能即时反映「已触发」状态。

字段约定（每项）：
- ``trigger_id``：触发唯一标识。
- ``workflow``：工作流/模板名。
- ``subscriber_id``：订阅者标识。
- ``channels``：目标渠道列表。
- ``status``：活动状态（``triggered`` / ``sent`` / ``failed``）。
- ``created_at``：ISO 8601 时间戳。
- ``title``：面向展示的摘要（默认取工作流名，可在 payload 覆盖）。
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .activity_log import ActivityQuery, get_activity_store
from .timeutil import Cursor, decode_cursor, encode_cursor, parse_dt as _parse_dt

router = APIRouter(prefix="/v1", tags=["activity"])

# 默认返回条数上限（活动流一次展示的量级）
_DEFAULT_LIMIT = 50
_MAX_LIMIT = 200


def _to_activity(record: dict) -> dict:
    """把一条存储记录归一化为活动条目。"""
    channels = record.get("channels") or []
    if isinstance(channels, str):
        channels = [channels]

    payload = record.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    workflow = record.get("workflow") or record.get("name") or "unknown"
    # 展示标题：优先已落库的 title（写入时取自 payload.title），其次 payload，最后工作流名
    title = record.get("title") or payload.get("title") or workflow

    return {
        "trigger_id": record.get("trigger_id"),
        "workflow": workflow,
        "subscriber_id": record.get("subscriber_id"),
        "channels": channels,
        "status": record.get("status", "sent"),
        "created_at": record.get("created_at"),
        "title": title,
        "environment": record.get("environment") or "development",
    }


def _cursor_of(item: dict) -> Optional[str]:
    """由条目构造下一页游标；时间戳不可解析时返回 None（提前结束翻页）。"""
    created_at = _parse_dt(item.get("created_at"))
    if created_at is None:
        return None
    return encode_cursor(
        Cursor(created_at=created_at, trigger_id=str(item.get("trigger_id") or ""))
    )


@router.get("/activity")
def list_activity(
    subscriber_id: Optional[str] = Query(default=None, alias="subscriberId", description="按订阅者过滤"),
    workflow: Optional[str] = Query(default=None, description="按工作流名过滤"),
    trigger_id: Optional[str] = Query(default=None, alias="triggerId", description="按触发唯一标识精确查询"),
    status: Optional[str] = Query(default=None, description="按活动状态过滤（triggered/sent/failed）"),
    since: Optional[datetime] = Query(default=None, description="起始时间（ISO 8601，含）"),
    until: Optional[datetime] = Query(default=None, description="截止时间（ISO 8601，含）"),
    environment: Optional[str] = Query(default=None, description="按隔离环境过滤（development/production）"),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT, description="单页条数上限"),
    cursor: Optional[str] = Query(default=None, description="上一页返回的 nextCursor"),
) -> dict:
    """返回一页活动（触发事件），最新在前；``nextCursor`` 为空表示没有更多。"""
    after: Optional[Cursor] = None
    if cursor:
        try:
            after = decode_cursor(cursor)
        except ValueError:
            raise HTTPException(status_code=400, detail="cursor 无效")

    # 多取一条用于判断是否还有下一页
    rows = get_activity_store().query(
        ActivityQuery(
            subscriber_id=subscriber_id,
            workflow=workflow,
            trigger_id=trigger_id,
            status=status,
            environment=environment,
            since=since,
            until=until,
            limit=limit + 1,
            after=after,
        )
    )
    has_more = len(rows) > limit
    items = [_to_activity(r) for r in rows[:limit]]
    next_cursor = _cursor_of(items[-1]) if has_more and items else None
    return {
        "items": items,
        "total": len(items),
        "hasMore": bool(next_cursor),
        "nextCursor": next_cursor,
    }


__all__ = ["router", "_to_activity", "_parse_dt"]
