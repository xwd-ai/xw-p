"""管理端统计聚合（渠道成败、耗时分布与失败原因归集）。

数据源是执行明细（``execution_log``）：一条记录对应一次「触发 × 渠道」的投递，
带状态、失败原因、耗时与实际完成投递的实现标识。仪表盘接口
（``/v1/dashboard/stats``）按消息记录给出总量与趋势，回答「发了多少」；这里回答
「哪个渠道在失败、慢在哪里、为什么失败」，两者的成功率分母口径一致：``skipped``
（被订阅者偏好拦下）既没成功也没失败，不进分母。

为什么按明细而不是按消息记录聚合：一次触发会分发到多个渠道，消息记录上只有一个
整体状态，拿它算渠道成功率会把「邮件失败、站内信成功」记成一次失败；渠道维度的
成败只能在明细层看到。

耗时用固定桶直方图而不是保留原始样本：窗口内的记录数没有上限，而分位数只需要
算出落在哪个区间；桶内按线性插值估算，估算值以实测最大值封顶（桶的上界可能高于
所有样本），因此内存占用与流量无关。分位数是估算值，粒度受桶边界约束。
"""
from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Optional

from .timeutil import parse_dt

# 默认统计窗口（天）与上限：上限同时也是趋势图的桶数上限
DEFAULT_WINDOW_DAYS = 7
MAX_WINDOW_DAYS = 90

# 榜单默认条数
DEFAULT_TOP = 5
MAX_TOP = 20

# 耗时直方图的桶边界（毫秒，含上界）；最后一段之外进溢出桶
LATENCY_BUCKETS_MS: tuple[int, ...] = (1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000)

# 失败原因按文本归并，过长的截断，避免一条带堆栈的原因占满整行
REASON_MAX_CHARS = 160

_STATUSES = ("sent", "failed", "skipped")


class WindowError(ValueError):
    """统计窗口参数不合法（接口层折成 400）。"""


@dataclass(frozen=True)
class StatsWindow:
    """统计窗口：左闭右闭的 UTC 时间区间。"""

    since: datetime
    until: datetime

    @property
    def days(self) -> int:
        """窗口覆盖的日历天数（含首尾）。

        与 ``dates()`` 的长度一致：趋势图每个桶对应一天，桶数不能和对外报的
        天数对不上。
        """
        start = self.since.astimezone(timezone.utc).date()
        end = self.until.astimezone(timezone.utc).date()
        return max(1, (end - start).days + 1)

    def dates(self) -> list[date]:
        """窗口覆盖的日期（UTC，从早到晚，含首尾）。"""
        start = self.since.astimezone(timezone.utc).date()
        end = self.until.astimezone(timezone.utc).date()
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def resolve_window(
    *,
    days: Optional[int] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    now: Optional[datetime] = None,
) -> StatsWindow:
    """解析统计窗口。

    ``days`` 与 ``since`` 互斥：同时给出时无法判断调用方想要「最近 N 天」还是
    「从某天起」，猜一个会让返回的数字与请求对不上，直接拒绝更省事。
    ``until`` 单独给出表示「截止到某个时刻的最近 N 天」。

    「最近 N 天」按日历日计：起点取 N-1 天前的 UTC 零点，因此窗口覆盖 N 个日期、
    趋势正好 N 个桶，第一天的数据也完整。
    """
    if days is not None and since is not None:
        raise WindowError("days 与 since 不能同时指定")
    if days is not None and not (1 <= days <= MAX_WINDOW_DAYS):
        raise WindowError(f"days 取值需在 1–{MAX_WINDOW_DAYS} 之间")

    anchor = _as_utc(now) or datetime.now(timezone.utc)
    end = _as_utc(until) or anchor

    start = _as_utc(since)
    if start is None:
        span = days or DEFAULT_WINDOW_DAYS
        start_date = end.astimezone(timezone.utc).date() - timedelta(days=span - 1)
        start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)

    if start > end:
        raise WindowError("since 不能晚于 until")

    window = StatsWindow(since=start, until=end)
    if window.days > MAX_WINDOW_DAYS:
        raise WindowError(f"时间窗不能超过 {MAX_WINDOW_DAYS} 天")
    return window


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """统一成带时区的 UTC 时间；无时区的按 UTC 解释。"""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class LatencyHistogram:
    """固定桶的耗时直方图，用于估算平均与分位。"""

    def __init__(self, buckets: tuple[int, ...] = LATENCY_BUCKETS_MS) -> None:
        self._buckets = tuple(sorted(buckets)) or LATENCY_BUCKETS_MS
        self._counts = [0] * len(self._buckets)
        self._overflow = 0
        self.count = 0
        self.total_ms = 0.0
        self.max_ms = 0.0

    def add(self, value: object) -> None:
        """记一次耗时（毫秒）。非法值按 0 计，负值同样归零。"""
        try:
            ms = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            ms = 0.0
        if ms < 0 or math.isnan(ms):
            ms = 0.0
        self.count += 1
        self.total_ms += ms
        self.max_ms = max(self.max_ms, ms)
        for index, bound in enumerate(self._buckets):
            if ms <= bound:
                self._counts[index] += 1
                return
        self._overflow += 1

    @property
    def average(self) -> float:
        return self.total_ms / self.count if self.count else 0.0

    def percentile(self, q: float) -> float:
        """估算分位耗时：桶内按线性插值，结果不超过实测最大值。

        桶的上界可能远高于样本本身（例如样本最大值 130ms、它落在 (100, 200] 桶），
        单纯插值会得出 175ms 这种比所有样本都大的「分位」；最大值是精确已知的，
        用它收口即可。
        """
        if self.count == 0:
            return 0.0
        rank = min(float(self.count), max(1.0, (q / 100.0) * self.count))
        lower = 0.0
        cumulative = 0
        for bound, n in zip(self._buckets, self._counts):
            cumulative += n
            if cumulative >= rank and n:
                upper = float(bound)
                before = cumulative - n
                frac = (rank - before) / n
                return min(lower + (upper - lower) * frac, self.max_ms)
            lower = float(bound)
        return self.max_ms


@dataclass
class Tally:
    """一组投递结果计数。"""

    sent: int = 0
    failed: int = 0
    skipped: int = 0

    @property
    def attempted(self) -> int:
        """真正尝试过投递的条数（跳过的不算）。"""
        return self.sent + self.failed

    def add(self, status: str) -> None:
        if status == "sent":
            self.sent += 1
        elif status == "skipped":
            self.skipped += 1
        else:
            self.failed += 1


@dataclass
class ChannelStat:
    """单个渠道的投递结果与耗时分布。"""

    channel: str
    tally: Tally = field(default_factory=Tally)
    latency: LatencyHistogram = field(default_factory=LatencyHistogram)


def success_rate(sent: int, attempted: int) -> float:
    """成功率百分比；没有尝试过投递时按 100 计（与仪表盘一致）。"""
    return round((sent / attempted) * 100, 1) if attempted else 100.0


def reason_key(reason: object) -> Optional[str]:
    """把失败原因归一成用于归并的键：压缩空白、截断过长文本。"""
    if not isinstance(reason, str):
        return None
    text = " ".join(reason.split())
    if not text:
        return None
    if len(text) > REASON_MAX_CHARS:
        return text[: REASON_MAX_CHARS - 1] + "…"
    return text


class AdminStatsAccumulator:
    """逐条累加管理统计量。"""

    def __init__(self, window: StatsWindow, top: int = DEFAULT_TOP) -> None:
        self._window = window
        self._top = max(1, min(int(top), MAX_TOP))
        self._channels: dict[str, ChannelStat] = {}
        self._providers: dict[str, Tally] = {}
        self._workflows: dict[str, Tally] = {}
        self._reasons: Counter[str] = Counter()
        self._overall = Tally()
        self._latency = LatencyHistogram()
        self._records = 0
        self._trend: dict[str, dict[str, int]] = {
            d.isoformat(): {"sent": 0, "failed": 0, "skipped": 0} for d in window.dates()
        }

    def add(self, record: dict[str, Any]) -> None:
        """累加一条执行明细。非 ``sent``/``skipped`` 的取值按失败计。"""
        self._records += 1
        status = str(record.get("status") or "sent").strip().lower()
        if status not in _STATUSES:
            status = "failed"

        channel = str(record.get("channel") or "unknown")
        stat = self._channels.get(channel)
        if stat is None:
            stat = ChannelStat(channel=channel)
            self._channels[channel] = stat

        self._overall.add(status)
        stat.tally.add(status)

        if status != "skipped":
            duration = record.get("duration_ms", 0)
            stat.latency.add(duration)
            self._latency.add(duration)

        provider = record.get("provider")
        if isinstance(provider, str) and provider.strip():
            self._providers.setdefault(provider.strip(), Tally()).add(status)

        workflow = record.get("workflow")
        if isinstance(workflow, str) and workflow.strip():
            self._workflows.setdefault(workflow.strip(), Tally()).add(status)

        if status == "failed":
            key = reason_key(record.get("reason"))
            if key:
                self._reasons[key] += 1

        dt = parse_dt(record.get("created_at"))
        if dt is not None:
            bucket = self._trend.get(dt.date().isoformat())
            if bucket is not None:
                bucket[status] += 1

    def extend(self, records: Iterable[dict[str, Any]]) -> "AdminStatsAccumulator":
        for record in records:
            self.add(record)
        return self

    def result(self) -> dict[str, Any]:
        """拼出接口返回结构。"""
        channels = sorted(
            self._channels.values(),
            key=lambda c: (-c.tally.attempted, c.channel),
        )
        return {
            "window": {
                "since": self._window.since.isoformat(),
                "until": self._window.until.isoformat(),
                "days": self._window.days,
            },
            "totals": {
                "records": self._records,
                "attempted": self._overall.attempted,
                "sent": self._overall.sent,
                "failed": self._overall.failed,
                "skipped": self._overall.skipped,
                "success_rate": success_rate(self._overall.sent, self._overall.attempted),
                "avg_ms": round(self._latency.average, 1),
                "p95_ms": round(self._latency.percentile(95), 1),
                "max_ms": round(self._latency.max_ms, 1),
            },
            "channels": [self._render_channel(c) for c in channels],
            "providers": self._render_tallies(self._providers, "provider"),
            "workflows": self._render_tallies(self._workflows, "workflow"),
            "failure_reasons": [
                {"reason": reason, "count": count}
                for reason, count in sorted(self._reasons.items(), key=lambda kv: (-kv[1], kv[0]))[
                    : self._top
                ]
            ],
            "trend": [
                {"date": day, "sent": counts["sent"], "failed": counts["failed"], "skipped": counts["skipped"]}
                for day, counts in self._trend.items()
            ],
        }

    def _render_channel(self, stat: ChannelStat) -> dict[str, Any]:
        return {
            "channel": stat.channel,
            "attempted": stat.tally.attempted,
            "sent": stat.tally.sent,
            "failed": stat.tally.failed,
            "skipped": stat.tally.skipped,
            "success_rate": success_rate(stat.tally.sent, stat.tally.attempted),
            "avg_ms": round(stat.latency.average, 1),
            "p95_ms": round(stat.latency.percentile(95), 1),
            "max_ms": round(stat.latency.max_ms, 1),
        }

    def _render_tallies(self, tallies: dict[str, Tally], key: str) -> list[dict[str, Any]]:
        ordered = sorted(tallies.items(), key=lambda kv: (-kv[1].attempted, kv[0]))[: self._top]
        return [
            {
                key: name,
                "records": tally.sent + tally.failed + tally.skipped,
                "attempted": tally.attempted,
                "sent": tally.sent,
                "failed": tally.failed,
                "skipped": tally.skipped,
                "success_rate": success_rate(tally.sent, tally.attempted),
            }
            for name, tally in ordered
        ]


def collect(
    records: Iterable[dict[str, Any]],
    window: StatsWindow,
    top: int = DEFAULT_TOP,
) -> dict[str, Any]:
    """一次性聚合一组记录（供小数据量场景与测试使用）。"""
    return AdminStatsAccumulator(window, top=top).extend(records).result()


__all__ = [
    "AdminStatsAccumulator",
    "ChannelStat",
    "DEFAULT_TOP",
    "DEFAULT_WINDOW_DAYS",
    "LATENCY_BUCKETS_MS",
    "LatencyHistogram",
    "MAX_TOP",
    "MAX_WINDOW_DAYS",
    "REASON_MAX_CHARS",
    "StatsWindow",
    "Tally",
    "WindowError",
    "collect",
    "reason_key",
    "resolve_window",
    "success_rate",
]
