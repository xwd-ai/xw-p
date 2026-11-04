"""管理端活动日志查询 API（ROADMAP Phase 5）。

面向运维与后台的三条接口，都是「只读排查」：

- ``GET /v1/admin/activity``：跨环境分页查询触发事件。与前端活动流
  （``GET /v1/activity``）的区别在于默认**不按环境收窄**——管理端要看的往往是
  「两个环境里都发生了什么」；再补 ``channel`` 与 ``keyword`` 两个维度：排查时
  手里常常只有一个渠道名或一句标题片段。
- ``GET /v1/admin/activity/summary``：同样的过滤条件下给一份汇总，回答「这段时间
  哪个工作流最多、哪个渠道涉及最多、多少人受影响、有没有失败」。汇总走存储层的
  流式扫描，不把命中集合读成列表。
- ``GET /v1/admin/activity/{triggerId}``：单次触发的下钻，把活动日志与执行明细
  并排放出来，并给出「声明了但没有任何投递记录」的渠道——消息卡在队列里、
  被偏好拦下却没写明细，都会在这里以缺口的形式暴露。

分页与前端活动流共用 ``(created_at, trigger_id)`` 游标语义，因此翻页期间有新记录
写入也不会重复或漏读。``total`` 是本页条数而不是命中总数：精确总数需要扫完整个
命中集合，放在每次翻页里做代价太大，需要总数时请求 ``summary``。
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .activity_log import ActivityQuery, get_activity_store
from .activity_routes import _to_activity
from .admin_activity import (
    DEFAULT_LIMIT,
    DEFAULT_TOP,
    MAX_KEYWORD_CHARS,
    MAX_LIMIT,
    MAX_TOP,
    ActivitySummaryAccumulator,
    FilterError,
    check_window,
    normalize_channel,
    normalize_environment,
    normalize_keyword,
    normalize_status,
)
from .execution_log import ExecutionQuery, get_execution_store
from .execution_routes import _to_item
from .timeutil import Cursor, decode_cursor, encode_cursor, parse_dt

router = APIRouter(prefix="/v1/admin", tags=["admin"])

# 下钻时带出的执行明细条数上限：一次触发的渠道数是有限的，超过上限说明数据
# 本身有问题，如实截断并标记，而不是无限读下去。
MAX_EXECUTION_ROWS = 200


def _build_query(
    *,
    environment: Optional[str],
    subscriber_id: Optional[str],
    workflow: Optional[str],
    trigger_id: Optional[str],
    status: Optional[str],
    channel: Optional[str],
    keyword: Optional[str],
    since: Optional[datetime],
    until: Optional[datetime],
    limit: int = DEFAULT_LIMIT,
    after: Optional[Cursor] = None,
) -> ActivityQuery:
    """校验过滤参数并构造查询；任何一项不合法都抛 ``FilterError``。"""
    # 不带时区的时间按 UTC 处理：存储里的时间戳都是带时区的，混着比较会在
    # 「naive 与 aware 相减」上抛 TypeError。
    since = parse_dt(since)
    until = parse_dt(until)
    try:
        check_window(since, until)
        return ActivityQuery(
            subscriber_id=subscriber_id or None,
            workflow=workflow or None,
            trigger_id=trigger_id or None,
            status=normalize_status(status),
            environment=normalize_environment(environment),
            channel=normalize_channel(channel),
            keyword=normalize_keyword(keyword),
            since=since,
            until=until,
            limit=limit,
            after=after,
        )
    except FilterError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _cursor_of(item: dict) -> Optional[str]:
    """由条目构造下一页游标；时间戳不可解析时返回 None（提前结束翻页）。"""
    created_at = parse_dt(item.get("created_at"))
    if created_at is None:
        return None
    return encode_cursor(
        Cursor(created_at=created_at, trigger_id=str(item.get("trigger_id") or ""))
    )


@router.get("/activity")
def admin_list_activity(
    environment: Optional[str] = Query(
        default=None, description="按环境过滤；留空表示所有环境"
    ),
    subscriber_id: Optional[str] = Query(
        default=None, alias="subscriberId", description="按订阅者过滤"
    ),
    workflow: Optional[str] = Query(default=None, description="按工作流名过滤"),
    trigger_id: Optional[str] = Query(
        default=None, alias="triggerId", description="按触发唯一标识精确查询"
    ),
    status: Optional[str] = Query(
        default=None, description="按状态过滤（triggered/sent/failed/skipped）"
    ),
    channel: Optional[str] = Query(default=None, description="按目标渠道过滤"),
    keyword: Optional[str] = Query(
        default=None, description=f"标题/工作流/订阅者标识的子串匹配（最多 {MAX_KEYWORD_CHARS} 字符）"
    ),
    since: Optional[datetime] = Query(default=None, description="起始时间（ISO 8601，含）"),
    until: Optional[datetime] = Query(default=None, description="截止时间（ISO 8601，含）"),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="单页条数上限"),
    cursor: Optional[str] = Query(default=None, description="上一页返回的 nextCursor"),
) -> dict:
    """返回一页活动日志（所有环境，除非显式指定），最新在前。"""
    after: Optional[Cursor] = None
    if cursor:
        try:
            after = decode_cursor(cursor)
        except ValueError:
            raise HTTPException(status_code=400, detail="cursor 无效")

    query = _build_query(
        environment=environment,
        subscriber_id=subscriber_id,
        workflow=workflow,
        trigger_id=trigger_id,
        status=status,
        channel=channel,
        keyword=keyword,
        since=since,
        until=until,
        limit=limit + 1,
        after=after,
    )
    rows = get_activity_store().query(query)
    has_more = len(rows) > limit
    items = [_to_activity(r) for r in rows[:limit]]
    next_cursor = _cursor_of(items[-1]) if has_more and items else None
    return {
        "items": items,
        "total": len(items),
        "hasMore": bool(next_cursor),
        "nextCursor": next_cursor,
    }


@router.get("/activity/summary")
def admin_activity_summary(
    environment: Optional[str] = Query(default=None, description="按环境过滤；留空表示所有环境"),
    subscriber_id: Optional[str] = Query(default=None, alias="subscriberId"),
    workflow: Optional[str] = Query(default=None),
    trigger_id: Optional[str] = Query(default=None, alias="triggerId"),
    status: Optional[str] = Query(default=None),
    channel: Optional[str] = Query(default=None),
    keyword: Optional[str] = Query(default=None),
    since: Optional[datetime] = Query(default=None, description="起始时间（ISO 8601，含）"),
    until: Optional[datetime] = Query(default=None, description="截止时间（ISO 8601，含）"),
    top: int = Query(default=DEFAULT_TOP, ge=1, le=MAX_TOP, description="榜单返回条数"),
) -> dict:
    """按过滤条件汇总命中记录；不受分页影响。"""
    query = _build_query(
        environment=environment,
        subscriber_id=subscriber_id,
        workflow=workflow,
        trigger_id=trigger_id,
        status=status,
        channel=channel,
        keyword=keyword,
        since=since,
        until=until,
    )
    summary = ActivitySummaryAccumulator().extend(get_activity_store().scan(query)).result(top=top)
    return {
        "window": {
            "since": since.isoformat() if since else None,
            "until": until.isoformat() if until else None,
        },
        **summary,
    }


@router.get("/activity/{trigger_id}")
def admin_activity_detail(
    trigger_id: str,
    environment: Optional[str] = Query(default=None, description="按环境过滤；留空表示所有环境"),
) -> dict:
    """单次触发的下钻：活动记录 + 逐渠道执行明细 + 渠道缺口。"""
    if not trigger_id.strip():
        raise HTTPException(status_code=400, detail="triggerId 不能为空")

    query = _build_query(
        environment=environment,
        subscriber_id=None,
        workflow=None,
        trigger_id=trigger_id.strip(),
        status=None,
        channel=None,
        keyword=None,
        since=None,
        until=None,
        limit=1,
    )
    rows = get_activity_store().query(query)
    if not rows:
        raise HTTPException(status_code=404, detail=f"未找到触发记录：{trigger_id}")

    record = rows[0]
    declared = [str(c) for c in (_to_activity(record).get("channels") or [])]

    executions = get_execution_store().query(
        ExecutionQuery(trigger_id=str(record.get("trigger_id")), limit=MAX_EXECUTION_ROWS + 1)
    )
    truncated = len(executions) > MAX_EXECUTION_ROWS
    items = [_to_item(e) for e in executions[:MAX_EXECUTION_ROWS]]

    counts = {"sent": 0, "failed": 0, "skipped": 0, "other": 0}
    duration_ms = 0
    delivered: list[str] = []
    for item in items:
        status = item.get("status") or "other"
        counts[status if status in counts else "other"] += 1
        try:
            duration_ms += int(item.get("duration_ms") or 0)
        except (TypeError, ValueError):
            pass
        channel = item.get("channel")
        if channel and channel not in delivered:
            delivered.append(str(channel))

    activity = _to_activity(record)
    payload = record.get("payload")
    if isinstance(payload, dict):
        activity["payload"] = payload

    return {
        "activity": activity,
        "executions": items,
        "executionSummary": {"total": len(items), "durationMs": duration_ms, **counts},
        # 声明了却没有对应明细：消息可能还压在队列里，或投递路径被提前拦下。
        "missingChannels": [c for c in declared if c not in delivered],
        "undeclaredChannels": [c for c in delivered if c not in declared],
        "executionsTruncated": truncated,
    }


__all__ = ["router"]
