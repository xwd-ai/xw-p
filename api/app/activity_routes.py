"""活动流查询 API（ROADMAP Phase S）。

提供 ``GET /v1/activity``，从活动日志存储（``activity_log``）读取触发事件，
返回按时间倒序的活动列表，供前端「活动流」消费。

过滤维度：
- ``subscriber_id``：按订阅者过滤。
- ``workflow``：按工作流名过滤。
- ``trigger_id``：按触发唯一标识精确查询。
- ``status``：按活动状态过滤（``triggered`` / ``sent`` / ``failed``）。
- ``since`` / ``until``：按创建时间范围过滤（ISO 8601，含边界）。

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

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query

from .activity_log import get_activity_store

router = APIRouter(prefix="/v1", tags=["activity"])

# 默认返回条数上限（活动流一次展示的量级）
_DEFAULT_LIMIT = 50
_MAX_LIMIT = 200


def _parse_dt(value: object) -> Optional[datetime]:
    """把记录里的时间戳解析为带时区的 datetime；失败返回 None。"""
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


def _to_activity(record: dict) -> dict:
    """把一条存储记录归一化为活动条目。"""
    channels = record.get("channels") or []
    if isinstance(channels, str):
        channels = [channels]

    payload = record.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    workflow = record.get("workflow") or record.get("name") or "unknown"
    # 展示标题：优先 payload 里的 title，其次工作流名
    title = payload.get("title") or workflow

    return {
        "trigger_id": record.get("trigger_id"),
        "workflow": workflow,
        "subscriber_id": record.get("subscriber_id"),
        "channels": channels,
        "status": record.get("status", "sent"),
        "created_at": record.get("created_at"),
        "title": title,
    }


def _filter_and_sort(
    records: list[dict],
    subscriber_id: Optional[str],
    workflow: Optional[str],
    limit: int,
    trigger_id: Optional[str] = None,
    status: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list[dict]:
    """按条件过滤并返回按时间倒序的活动列表。"""
    activities = []
    for r in records:
        # 订阅者过滤
        if subscriber_id and r.get("subscriber_id") != subscriber_id:
            continue
        # 工作流过滤
        wf = r.get("workflow") or r.get("name")
        if workflow and wf != workflow:
            continue
        # 触发标识精确过滤
        if trigger_id and r.get("trigger_id") != trigger_id:
            continue
        # 状态过滤
        if status and r.get("status") != status:
            continue
        # 时间范围过滤（含边界）
        dt = _parse_dt(r.get("created_at"))
        if since is not None and (dt is None or dt < since):
            continue
        if until is not None and (dt is None or dt > until):
            continue
        activities.append(_to_activity(r))

    # 按 created_at 倒序（最新在前）；时间缺失的排在最后
    def _sort_key(a: dict):
        dt = _parse_dt(a.get("created_at"))
        return dt.timestamp() if dt else float("-inf")

    activities.sort(key=_sort_key, reverse=True)
    return activities[:limit]


@router.get("/activity")
def list_activity(
    subscriber_id: Optional[str] = Query(default=None, alias="subscriberId", description="按订阅者过滤"),
    workflow: Optional[str] = Query(default=None, description="按工作流名过滤"),
    trigger_id: Optional[str] = Query(default=None, alias="triggerId", description="按触发唯一标识精确查询"),
    status: Optional[str] = Query(default=None, description="按活动状态过滤（triggered/sent/failed）"),
    since: Optional[datetime] = Query(default=None, description="起始时间（ISO 8601，含）"),
    until: Optional[datetime] = Query(default=None, description="截止时间（ISO 8601，含）"),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT, description="返回条数上限"),
) -> dict:
    """返回最近的活动（触发事件），最新在前。"""
    records = get_activity_store().read_all()
    items = _filter_and_sort(
        records,
        subscriber_id,
        workflow,
        limit,
        trigger_id=trigger_id,
        status=status,
        since=since,
        until=until,
    )
    return {"items": items, "total": len(items)}


__all__ = ["router", "_to_activity", "_filter_and_sort", "_parse_dt"]
