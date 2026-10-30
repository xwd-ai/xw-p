"""仪表盘统计 API。

提供 ``GET /v1/dashboard/stats``，聚合消息存储中的发送记录，返回：
总发送量、投递成功率、渠道分布、最近 7 天趋势。数据来源为 ``storage``
持久化的消息记录（jsonl / postgres 双后端统一读取）。

统计口径：
- ``total``：已落库的消息总数（worker 每投递一条即落库一条）。
- ``success_rate``：status 为 ``sent`` 的记录占比（百分比，0–100）。
- ``channel_distribution``：按渠道（channels 数组展开）计数。
- ``trend``：最近 7 天每日发送量（按 created_at 归并，无记录的天补 0）。

聚合由存储后端完成（``Storage.stats``）：Postgres 走 ``count`` / ``GROUP BY``
查询，JSONL 逐行流式累加。接口层不把记录读进内存，也不会随数据量线性变慢。
"""
from __future__ import annotations

from fastapi import APIRouter

from .stats import TREND_DAYS, compute_stats
from .storage import get_storage

router = APIRouter(prefix="/v1/dashboard", tags=["dashboard"])

# 7 日趋势窗口（含今天）
_TREND_DAYS = TREND_DAYS


def _compute_stats(records: list[dict]) -> dict:
    """从记录序列聚合出仪表盘统计（小数据量与测试使用）。"""
    return compute_stats(records)


@router.get("/stats")
def dashboard_stats() -> dict:
    """返回仪表盘聚合统计。"""
    return get_storage().stats(trend_days=_TREND_DAYS)


__all__ = ["router", "_compute_stats"]
