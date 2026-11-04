"""活动日志的管理端过滤校验与汇总聚合。

``GET /v1/activity`` 服务于前端活动流：默认只看当前环境、按页返回、用于展示。
管理端的问题不一样——「这段时间里哪些订阅者一直在失败」「哪个工作流被打得
最多」「某个人到底收到了什么」，因此这里补三件事：

1. **过滤词校验**：状态、渠道、环境、关键词都在接口层收口，写错立刻返回 400
   并列出可选值，而不是回一个空列表让人以为「确实没有记录」。
2. **汇总聚合**：一次遍历给出状态分布、工作流与渠道排行、环境分布、去重后的
   订阅者数与首末时间，供后台概览。
3. **去重计数上限**：订阅者数按集合去重，集合规模设上限；超过后停止扩张并
   标记 ``capped``——汇总宁可少报也不能把内存交给一个没有上界的维基。

聚合只做统计，不改变记录本身；排序与截断在 ``result`` 里统一处理，调用方不必
自己再排一遍。
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Optional

from .models.channel import Channel
from .models.entities import Environment
from .timeutil import parse_dt

# 分页与榜单的边界
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
DEFAULT_TOP = 5
MAX_TOP = 20

# 关键词长度上限：再长也不会是「一个片段」，只会拖慢顺序扫描
MAX_KEYWORD_CHARS = 120

# 去重订阅者的集合上限，超过后停止扩张并标记 capped
MAX_UNIQUE_SUBSCRIBERS = 20000

# 活动日志可能出现的结果状态；``skipped`` 参与过滤是为了与执行明细对齐
ACTIVITY_STATUSES: tuple[str, ...] = ("triggered", "sent", "failed", "skipped")

# 不认识的（或缺失的）状态归到这一桶，避免汇总里凭空少一截记录
OTHER_STATUS = "other"


class FilterError(ValueError):
    """过滤参数不合法（接口层折成 400）。"""


def normalize_environment(value: Optional[str]) -> Optional[str]:
    """归一化环境过滤；空值表示「所有环境」。"""
    if value is None or not value.strip():
        return None
    env = value.strip().lower()
    allowed = {e.value for e in Environment}
    if env not in allowed:
        raise FilterError(f"未知环境：{value}（支持：{', '.join(sorted(allowed))}）")
    return env


def normalize_status(value: Optional[str]) -> Optional[str]:
    """归一化状态过滤；非法取值直接报错并给出可选值。"""
    if value is None or not value.strip():
        return None
    status = value.strip().lower()
    if status not in ACTIVITY_STATUSES:
        raise FilterError(
            f"未知状态：{value}（支持：{', '.join(ACTIVITY_STATUSES)}）"
        )
    return status


def normalize_channel(value: Optional[str]) -> Optional[str]:
    """归一化渠道过滤；只接受可投递渠道。

    ``delay`` / ``digest`` 是编排步骤而不是投递渠道，活动日志里不会出现它们；
    接受这类取值只会得到永远为空的查询。
    """
    if value is None or not value.strip():
        return None
    channel = value.strip().lower()
    allowed = [c.value for c in Channel.delivery_channels()]
    if channel not in allowed:
        raise FilterError(f"未知渠道：{value}（支持：{', '.join(allowed)}）")
    return channel


def normalize_keyword(value: Optional[str]) -> Optional[str]:
    """归一化关键词：去首尾空白，空串视为不过滤，超长直接拒绝。"""
    if value is None:
        return None
    keyword = value.strip()
    if not keyword:
        return None
    if len(keyword) > MAX_KEYWORD_CHARS:
        raise FilterError(f"关键词过长（最多 {MAX_KEYWORD_CHARS} 个字符）")
    return keyword


def check_window(since: Optional[datetime], until: Optional[datetime]) -> None:
    """区间倒置时直接报错。

    倒置的区间在存储层会被当成「什么都不命中」，但调用方看到的是空列表而不是
    参数写错——把两个时间填反是很容易犯的错，这里明确指出来。
    """
    if since is not None and until is not None and since > until:
        raise FilterError("since 不能晚于 until")


def iso(value: Any) -> Optional[str]:
    """把记录里的时间戳统一成 ISO 字符串（解析不了时原样返回字符串）。"""
    if value is None:
        return None
    dt = parse_dt(value)
    if dt is not None:
        return dt.isoformat()
    return str(value)


@dataclass
class ActivitySummaryAccumulator:
    """逐条累加活动日志的汇总统计。

    只要一行记录就能推进全部维度，调用方可以直接喂 ``store.scan`` 的迭代器，
    不需要先把命中集合读成列表。
    """

    records: int = 0
    statuses: Counter = field(default_factory=Counter)
    workflows: Counter = field(default_factory=Counter)
    workflow_failures: Counter = field(default_factory=Counter)
    channels: Counter = field(default_factory=Counter)
    environments: Counter = field(default_factory=Counter)
    first_at: Optional[datetime] = None
    last_at: Optional[datetime] = None
    _subscribers: set[str] = field(default_factory=set)
    _subscribers_capped: bool = False

    def add(self, record: dict[str, Any]) -> None:
        self.records += 1

        status = str(record.get("status") or "").strip().lower()
        self.statuses[status if status in ACTIVITY_STATUSES else OTHER_STATUS] += 1

        workflow = str(record.get("workflow") or record.get("name") or "unknown")
        self.workflows[workflow] += 1
        if status in ("failed",):
            self.workflow_failures[workflow] += 1

        channels = record.get("channels") or []
        if isinstance(channels, str):
            channels = [channels]
        # 一条记录声明多个渠道时每个渠道各记一次，渠道排行回答的是「发往哪里」，
        # 不是「多少条记录涉及这个地方」。
        for channel in {str(c) for c in channels if c}:
            self.channels[channel] += 1

        self.environments[str(record.get("environment") or "development")] += 1

        subscriber = record.get("subscriber_id")
        if subscriber:
            if len(self._subscribers) < MAX_UNIQUE_SUBSCRIBERS:
                self._subscribers.add(str(subscriber))
            elif str(subscriber) not in self._subscribers:
                self._subscribers_capped = True

        dt = parse_dt(record.get("created_at"))
        if dt is not None:
            if self.first_at is None or dt < self.first_at:
                self.first_at = dt
            if self.last_at is None or dt > self.last_at:
                self.last_at = dt

    def extend(self, records: Iterable[dict[str, Any]]) -> "ActivitySummaryAccumulator":
        for record in records:
            self.add(record)
        return self

    def result(self, top: int = DEFAULT_TOP) -> dict[str, Any]:
        """输出汇总结构；榜单按「条数降序、名称升序」截断到 ``top``。"""
        top = max(1, min(int(top), MAX_TOP))

        by_status = {status: self.statuses.get(status, 0) for status in ACTIVITY_STATUSES}
        if self.statuses.get(OTHER_STATUS):
            by_status[OTHER_STATUS] = self.statuses[OTHER_STATUS]

        workflows = [
            {
                "workflow": name,
                "total": count,
                "failed": self.workflow_failures.get(name, 0),
            }
            for name, count in sorted(
                self.workflows.items(), key=lambda item: (-item[1], item[0])
            )[:top]
        ]
        channels = [
            {"channel": name, "total": count}
            for name, count in sorted(self.channels.items(), key=lambda item: (-item[1], item[0]))
        ]
        environments = [
            {"environment": name, "total": count}
            for name, count in sorted(
                self.environments.items(), key=lambda item: (-item[1], item[0])
            )
        ]
        return {
            "totals": {
                "records": self.records,
                "byStatus": by_status,
            },
            "workflows": workflows,
            "channels": channels,
            "environments": environments,
            "subscribers": {
                "unique": len(self._subscribers),
                "capped": self._subscribers_capped,
            },
            "range": {"firstAt": iso(self.first_at), "lastAt": iso(self.last_at)},
        }


__all__ = [
    "ACTIVITY_STATUSES",
    "ActivitySummaryAccumulator",
    "DEFAULT_LIMIT",
    "DEFAULT_TOP",
    "FilterError",
    "MAX_KEYWORD_CHARS",
    "MAX_LIMIT",
    "MAX_TOP",
    "MAX_UNIQUE_SUBSCRIBERS",
    "OTHER_STATUS",
    "check_window",
    "iso",
    "normalize_channel",
    "normalize_environment",
    "normalize_keyword",
    "normalize_status",
]
