"""仪表盘统计聚合（增量累加，不保留原始记录）。

两条读取路径共用同一份口径：

- JSONL 后端逐行流式读出记录喂给 ``StatsAccumulator``，内存占用与记录数无关；
- Postgres 后端由 SQL 直接聚合，只回传几行结果。

统计口径与 ``/v1/dashboard/stats`` 的返回字段一一对应：``total`` 是记录条数，
``success_rate`` 为 ``status == "sent"`` 在**实际尝试投递的记录**中的占比，
``skipped`` 是全部渠道都被偏好拦下的条数——它们没有尝试投递，放进成功率分母
会让一次正确的用户设置看起来像一次故障；一条尝试记录都没有时按 100 计。
``channel_distribution`` 按渠道数组逐项计数，``trend`` 是最近 ``trend_days``
天（含今天）的每日条数，窗口外的记录计入总数但不进趋势。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from .timeutil import parse_dt

TREND_DAYS = 7


def recent_days(trend_days: int = TREND_DAYS, today: Optional[date] = None) -> list[date]:
    """返回趋势窗口内的日期，从最早到最近（含今天）。"""
    anchor = today or datetime.now(timezone.utc).date()
    return [anchor - timedelta(days=i) for i in range(trend_days - 1, -1, -1)]


def _channels_of(record: dict[str, Any]) -> list[str]:
    channels = record.get("channels") or []
    if isinstance(channels, str):
        return [channels]
    return list(channels)


def build_stats(
    *,
    total: int,
    sent: int,
    channel_counts: dict[str, int],
    per_day: dict[str, int],
    trend_days: int = TREND_DAYS,
    today: Optional[date] = None,
    skipped: int = 0,
) -> dict[str, Any]:
    """把已聚合的计数拼成接口返回结构，补齐窗口内没有记录的天。"""
    days = recent_days(trend_days, today)
    # 分母只算真正尝试过投递的条数：全部渠道被偏好拦下的消息既没成功也没
    # 失败，计入分母会把成功率拉低，而那次投递根本没有发生。
    attempted = max(0, total - skipped)
    return {
        "total": total,
        "skipped": skipped,
        "success_rate": round((sent / attempted) * 100, 1) if attempted else 100.0,
        "channel_distribution": dict(channel_counts),
        "trend": [{"date": d.isoformat(), "count": per_day.get(d.isoformat(), 0)} for d in days],
    }


class StatsAccumulator:
    """逐条累加统计量，可用于流式读取（不需要把整表装进内存）。"""

    def __init__(self, trend_days: int = TREND_DAYS, today: Optional[date] = None) -> None:
        self._trend_days = trend_days
        self._today = today
        self._per_day: dict[str, int] = {d.isoformat(): 0 for d in recent_days(trend_days, today)}
        self.total = 0
        self.sent = 0
        self.skipped = 0
        self.channel_counts: dict[str, int] = {}

    def add(self, record: dict[str, Any]) -> None:
        self.total += 1
        status = record.get("status")
        if status == "sent":
            self.sent += 1
        elif status == "skipped":
            self.skipped += 1
        for ch in _channels_of(record):
            self.channel_counts[ch] = self.channel_counts.get(ch, 0) + 1
        dt = parse_dt(record.get("created_at"))
        if dt is None:
            return
        day = dt.date().isoformat()
        if day in self._per_day:
            self._per_day[day] += 1

    def extend(self, records: Iterable[dict[str, Any]]) -> None:
        for record in records:
            self.add(record)

    def result(self) -> dict[str, Any]:
        return build_stats(
            total=self.total,
            sent=self.sent,
            channel_counts=self.channel_counts,
            per_day=self._per_day,
            trend_days=self._trend_days,
            today=self._today,
            skipped=self.skipped,
        )


def compute_stats(records: Iterable[dict[str, Any]], trend_days: int = TREND_DAYS) -> dict[str, Any]:
    """一次性聚合一个记录序列（供小数据量场景与单元测试使用）。"""
    acc = StatsAccumulator(trend_days=trend_days)
    acc.extend(records)
    return acc.result()


__all__ = ["TREND_DAYS", "StatsAccumulator", "build_stats", "compute_stats", "recent_days"]
