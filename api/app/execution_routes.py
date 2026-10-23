"""消息执行明细查询 API（ROADMAP Phase 1）。

提供 ``GET /v1/executions``，从执行明细存储（``execution_log``）读取逐渠道
投递结果，支持按触发标识 / 渠道 / 状态过滤，供后台「执行详情」排查使用。

字段约定（每项）：
- ``trigger_id``：所属触发的唯一标识。
- ``channel``：投递渠道。
- ``status``：投递结果（``sent`` / ``failed``）。
- ``reason``：失败原因（成功时为空）。
- ``duration_ms``：投递耗时（毫秒）。
- ``created_at``：ISO 8601 时间戳。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query

from .execution_log import get_execution_store

router = APIRouter(prefix="/v1", tags=["execution"])

# 默认返回条数上限
_DEFAULT_LIMIT = 50
_MAX_LIMIT = 200


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


def _filter_and_sort(
    records: list[dict],
    trigger_id: Optional[str],
    channel: Optional[str],
    status: Optional[str],
    limit: int,
) -> list[dict]:
    """按条件过滤并返回按时间倒序的执行明细列表。"""
    items: list[dict] = []
    for r in records:
        if trigger_id and r.get("trigger_id") != trigger_id:
            continue
        if channel and r.get("channel") != channel:
            continue
        if status and r.get("status") != status:
            continue
        items.append(
            {
                "trigger_id": r.get("trigger_id"),
                "channel": r.get("channel"),
                "status": r.get("status", "sent"),
                "reason": r.get("reason"),
                "duration_ms": r.get("duration_ms", 0),
                "created_at": r.get("created_at"),
            }
        )

    def _sort_key(item: dict):
        dt = _parse_dt(item.get("created_at"))
        return dt.timestamp() if dt else float("-inf")

    items.sort(key=_sort_key, reverse=True)
    return items[:limit]


@router.get("/executions")
def list_executions(
    trigger_id: Optional[str] = Query(default=None, alias="triggerId", description="按触发唯一标识过滤"),
    channel: Optional[str] = Query(default=None, description="按投递渠道过滤"),
    status: Optional[str] = Query(default=None, description="按投递结果过滤（sent/failed）"),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT, description="返回条数上限"),
) -> dict:
    """返回最近的执行明细（逐渠道投递结果），最新在前。"""
    records = get_execution_store().read_all()
    items = _filter_and_sort(records, trigger_id, channel, status, limit)
    return {"items": items, "total": len(items)}


__all__ = ["router", "_filter_and_sort", "_parse_dt"]
