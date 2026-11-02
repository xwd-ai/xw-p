"""消息执行明细查询 API（ROADMAP Phase 1 / R6）。

提供 ``GET /v1/executions``，从执行明细存储（``execution_log``）读取逐渠道
投递结果，供后台「执行详情」排查使用。

过滤维度：
- ``triggerId``：按触发唯一标识过滤。
- ``channel``：按投递渠道过滤。
- ``status``：按投递结果过滤（``sent`` / ``failed``）。
- ``since`` / ``until``：按写入时间范围过滤（ISO 8601，含边界）。

分页用游标而不是 offset：响应里的 ``nextCursor`` 原样回传即可取下一页。游标是
上一页最后一条记录的 ``(created_at, id)``——一次触发会写出多条渠道明细，
触发标识不足以区分它们，因此次级键取记录自身的标识。过滤与排序由存储后端完成
（JSONL 从文件尾部倒序读、Postgres 走 INDEX + LIMIT），接口层不再把记录读进
内存筛选。

字段约定（每项）：
- ``id``：明细记录标识。
- ``trigger_id``：所属触发的唯一标识。
- ``channel``：投递渠道。
- ``status``：投递结果（``sent`` / ``failed``）。
- ``reason``：失败原因（成功时为空）。
- ``duration_ms``：投递耗时（毫秒）。
- ``created_at``：ISO 8601 时间戳。
- ``step_id`` / ``provider`` / ``attempts`` / ``fallback``：排查用的补充信息，
  由引擎回写时给出——哪个步骤、实际完成投递的实现、按顺序试过哪些实现、
  是否发生过主备回退。API 侧消费线程写入的记录没有这几项，一律回空值。
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .execution_log import ExecutionCursor, ExecutionQuery, get_execution_store
from .timeutil import decode_position, encode_position, parse_dt

router = APIRouter(prefix="/v1", tags=["execution"])

# 默认返回条数上限
_DEFAULT_LIMIT = 50
_MAX_LIMIT = 200


def _to_item(record: dict) -> dict:
    """把一条存储记录归一化为执行明细条目。

    引擎回写的记录比 API 侧还带几个排查字段；它们不是每条记录都有
    （API 自己写的记录、Postgres 后端只落固定几列），缺失时给空值而不省略键，
    前端与调用方不必逐字段判空。
    """
    attempts = record.get("attempts")
    return {
        "id": record.get("id") or "",
        "trigger_id": record.get("trigger_id"),
        "channel": record.get("channel"),
        "status": record.get("status", "sent"),
        "reason": record.get("reason"),
        "duration_ms": record.get("duration_ms", 0),
        "created_at": record.get("created_at"),
        "step_id": record.get("step_id"),
        "provider": record.get("provider"),
        "attempts": list(attempts) if isinstance(attempts, (list, tuple)) else [],
        "fallback": bool(record.get("fallback")),
    }


def _cursor_of(item: dict) -> Optional[str]:
    """由条目构造下一页游标；时间戳不可解析时返回 None（提前结束翻页）。"""
    created_at = parse_dt(item.get("created_at"))
    if created_at is None:
        return None
    return encode_position(created_at, str(item.get("id") or ""))


@router.get("/executions")
def list_executions(
    trigger_id: Optional[str] = Query(default=None, alias="triggerId", description="按触发唯一标识过滤"),
    channel: Optional[str] = Query(default=None, description="按投递渠道过滤"),
    status: Optional[str] = Query(default=None, description="按投递结果过滤（sent/failed）"),
    since: Optional[datetime] = Query(default=None, description="起始时间（ISO 8601，含）"),
    until: Optional[datetime] = Query(default=None, description="截止时间（ISO 8601，含）"),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT, description="单页条数上限"),
    cursor: Optional[str] = Query(default=None, description="上一页返回的 nextCursor"),
) -> dict:
    """返回一页执行明细（逐渠道投递结果），最新在前；``nextCursor`` 为空表示没有更多。"""
    after: Optional[ExecutionCursor] = None
    if cursor:
        try:
            created_at, key = decode_position(cursor)
        except ValueError:
            raise HTTPException(status_code=400, detail="cursor 无效")
        after = ExecutionCursor(created_at=created_at, key=key)

    # 多取一条用于判断是否还有下一页
    rows = get_execution_store().query(
        ExecutionQuery(
            trigger_id=trigger_id,
            channel=channel,
            status=status,
            since=since,
            until=until,
            limit=limit + 1,
            after=after,
        )
    )
    has_more = len(rows) > limit
    items = [_to_item(r) for r in rows[:limit]]
    next_cursor = _cursor_of(items[-1]) if has_more and items else None
    return {
        "items": items,
        "total": len(items),
        "hasMore": bool(next_cursor),
        "nextCursor": next_cursor,
    }


__all__ = ["router", "_to_item", "_cursor_of"]
