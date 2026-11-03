"""投递前的偏好判定：把「偏好设置」变成「这条通知的这个渠道发不发」。

``app.preferences`` 回答「这个人设了什么」，本模块回答「按这些设置，现在这条
通知要不要发出去」。两者分开是因为判定涉及的上下文比记录本身多：一次投递要知道
订阅者、工作流与渠道三样东西，而偏好记录只是三层里的一条。

三条规则写在这里，接口与消费路径共用同一份结论：

1. **一次判定只取一次快照。** 一条消息往往要发往多个渠道，逐渠道读存储会在
   同一个人身上做多次读取，还可能读到两个不同时刻的视图——同一个请求里
   「邮件关了、推送没关」这种结论应当来自同一份数据。
2. **读不到偏好时照常发送。** 存储损坏、文件权限、后端连不上，都属于「不知道
   用户怎么设置」，而不是「用户不想收」。通知的价值在于送达，静默丢消息比多发
   一条更难被发现，因此失败一律按默认接收处理并留下告警日志。
3. **判定只做否决。** 偏好用于收窄投递范围，不放宽别处的限制：渠道没配实现、
   缺收件地址这类问题仍由投递路径报失败，不因为偏好允许就被绕过。

判定本身不写任何存储，也不修改消息；跳过与否的记录由调用方（``app.worker``）
写进执行明细，状态为 ``skipped`` 并带上命中的层级与目标，便于回答
「这个人为什么没收到这封邮件」。
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from .models.channel import Channel
from .preferences import (
    PreferenceSet,
    PreferenceStore,
    Scope,
    get_preference_store,
    normalize_subscriber_id,
    normalize_target,
)

logger = logging.getLogger("xw.delivery_gate")


@dataclass(frozen=True)
class GateDecision:
    """某个渠道上的判定结论。"""

    channel: str
    allowed: bool
    scope: Optional[str] = None
    target: Optional[str] = None
    matched: bool = False
    reason: str = ""

    @property
    def skip_reason(self) -> str:
        """写进执行明细的原因文案。"""
        return f"偏好跳过：{self.reason}"

    def as_dict(self) -> dict:
        return {
            "channel": self.channel,
            "allowed": self.allowed,
            "scope": self.scope,
            "target": self.target,
            "matched": self.matched,
            "reason": self.reason,
        }


def _channel_value(raw: object) -> Optional[Channel]:
    """把渠道收敛成枚举；只接受可投递渠道。

    非投递渠道（``delay`` / ``digest``）与未知名字都不在这里判失败：它们
    根本走不到投递，交给投递路径按原有方式报错，判定这一层不制造第二种错误
    文案。返回 ``None`` 表示「不参与偏好判定」。
    """
    if isinstance(raw, Channel):
        return raw if raw.is_delivery() else None
    if not isinstance(raw, str):
        return None
    try:
        channel = Channel(raw.strip())
    except ValueError:
        return None
    return channel if channel.is_delivery() else None


class DeliveryGate:
    """按订阅者偏好判定某条通知能否发往某个渠道。"""

    def __init__(self, store: Optional[PreferenceStore] = None) -> None:
        # 不在这里取单例：构造时取会把「创建门」与「创建存储」绑死，测试与
        # 启动顺序都不方便。取用时机放在每次判定，仍然只读一次快照。
        self._store = store

    def _store_ref(self) -> PreferenceStore:
        return self._store if self._store is not None else get_preference_store()

    def _snapshot(self) -> Optional[PreferenceSet]:
        """取一次快照；读取失败返回 ``None``，由调用方按默认接收处理。"""
        try:
            return self._store_ref().snapshot()
        except Exception:  # noqa: BLE001 - 判定失败不能升级成投递失败
            logger.warning("读取发送偏好失败，本次投递按默认接收处理", exc_info=True)
            return None

    def check(
        self,
        subscriber_id: Optional[str],
        workflow: Optional[str],
        channels: Sequence[Channel | str],
    ) -> list[GateDecision]:
        """按顺序给出每个渠道的判定结论。

        返回列表与 ``channels`` 等长、同序：调用方按位置取用即可，不必再拼
        一次键。``subscriber_id`` 为空（历史消息或系统通知）时不做判定——
        偏好是挂在具体订阅者身上的，没有人就无从判定。
        """
        wanted = [channel for channel in (_channel_value(c) for c in channels) if channel]
        if not wanted:
            return []

        subscriber = _text(subscriber_id)
        if subscriber is None:
            return [
                GateDecision(
                    channel=channel.value,
                    allowed=True,
                    reason="未被订阅者偏好拦截（消息未携带订阅者标识）",
                )
                for channel in wanted
            ]

        try:
            subscriber = normalize_subscriber_id(subscriber)
        except ValueError:
            # 标识超长之类的问题在写偏好时就被挡住了，这里遇到说明消息本身
            # 不合法；判定放行，让投递路径按原有方式处理。
            logger.warning("订阅者标识不合法，跳过偏好判定：%r", subscriber_id)
            return [
                GateDecision(
                    channel=channel.value,
                    allowed=True,
                    reason="未被订阅者偏好拦截（订阅者标识不合法）",
                )
                for channel in wanted
            ]

        workflow_name = _workflow_name(workflow)
        snapshot = self._snapshot()
        if snapshot is None:
            return [
                GateDecision(
                    channel=channel.value,
                    allowed=True,
                    reason="未被订阅者偏好拦截（偏好读取失败，按默认接收）",
                )
                for channel in wanted
            ]

        decisions: list[GateDecision] = []
        for channel in wanted:
            decision = snapshot.resolve(subscriber, workflow=workflow_name, channel=channel)
            decisions.append(
                GateDecision(
                    channel=channel.value,
                    allowed=decision.allowed,
                    scope=decision.scope.value if decision.scope else None,
                    target=decision.target,
                    matched=decision.matched,
                    reason=decision.reason,
                )
            )
        return decisions


def _text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _workflow_name(raw: object) -> Optional[str]:
    """校验工作流名；不合法时返回 ``None`` 并告警。

    工作流名来自队列消息，正常路径已经过触发 API 校验。真出现空白字符或超长
    时不必让整条通知作废：退化成「不按工作流层判定」，渠道层与全局层仍然生效。
    """
    text = _text(raw)
    if text is None:
        return None
    try:
        return normalize_target(Scope.WORKFLOW, text)
    except ValueError:
        logger.warning("工作流名不合法，工作流层偏好不参与判定：%r", raw)
        return None


def summarize(decisions: Iterable[GateDecision]) -> str:
    """把一批判定压成一行摘要，用于日志。"""
    items = list(decisions)
    blocked = [item for item in items if not item.allowed]
    if not blocked:
        return f"偏好判定放行 {len(items)} 个渠道"
    parts = [f"{item.channel}（{item.reason}）" for item in blocked]
    return "偏好拦截：" + "、".join(parts)


_gate: Optional[DeliveryGate] = None
_gate_lock = threading.Lock()


def get_delivery_gate() -> DeliveryGate:
    """获取全局判定器单例（惰性初始化，偏好存储在首次判定时才读）。"""
    global _gate
    with _gate_lock:
        if _gate is None:
            _gate = DeliveryGate()
        return _gate


def set_delivery_gate(gate: Optional[DeliveryGate]) -> None:
    """替换全局判定器；传 ``None`` 表示下次取用时重建（测试用）。"""
    global _gate
    with _gate_lock:
        _gate = gate


__all__ = [
    "DeliveryGate",
    "GateDecision",
    "get_delivery_gate",
    "set_delivery_gate",
    "summarize",
]
