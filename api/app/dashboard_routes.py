"""仪表盘统计 API（ROADMAP Phase S）。

提供 ``GET /v1/dashboard/stats``，聚合消息存储中的发送记录，返回：
总发送量、投递成功率、渠道分布、最近 7 天趋势。数据来源为 ``storage``
持久化的消息记录（jsonl / postgres 双后端统一读取）。

统计口径：
- ``total``：已落库的消息总数（worker 每投递一条即落库一条）。
- ``success_rate``：status 为 ``sent`` 的记录占比（百分比，0–100）。
- ``channel_distribution``：按渠道（channels 数组展开）计数。
- ``trend``：最近 7 天每日发送量（按 created_at 归并，无记录的天补 0）。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter

from .storage import get_storage

router = APIRouter(prefix="/v1/dashboard", tags=["dashboard"])

# 7 日趋势窗口（含今天）
_TREND_DAYS = 7


def _parse_dt(value: object) -> datetime | None:
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


def _compute_stats(records: list[dict]) -> dict:
    """从原始记录列表聚合出仪表盘统计。"""
    total = len(records)
    sent = sum(1 for r in records if r.get("status") == "sent")
    success_rate = round((sent / total) * 100, 1) if total else 100.0

    # 渠道分布：一条消息可能经由多个渠道，逐渠道计数
    channel_counts: dict[str, int] = {}
    for r in records:
        channels = r.get("channels") or []
        if isinstance(channels, str):
            channels = [channels]
        for ch in channels:
            channel_counts[ch] = channel_counts.get(ch, 0) + 1

    # 最近 7 天趋势：按 UTC 日期归并
    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=i)) for i in range(_TREND_DAYS - 1, -1, -1)]
    per_day: dict[str, int] = {d.isoformat(): 0 for d in days}
    for r in records:
        dt = _parse_dt(r.get("created_at"))
        if dt is None:
            continue
        day = dt.date().isoformat()
        if day in per_day:
            per_day[day] += 1

    trend = [
        {"date": d.isoformat(), "count": per_day[d.isoformat()]}
        for d in days
    ]

    return {
        "total": total,
        "success_rate": success_rate,
        "channel_distribution": channel_counts,
        "trend": trend,
    }


@router.get("/stats")
def dashboard_stats() -> dict:
    """返回仪表盘聚合统计。"""
    records = get_storage().read_all()
    return _compute_stats(records)


__all__ = ["router", "_compute_stats"]
