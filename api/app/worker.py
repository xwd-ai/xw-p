"""后台 worker：消费队列，经渠道 Provider 投递并落库。

只在 memory 总线的单进程 demo 里使用（启停条件见 ``config.api_worker_enabled``）：
redis 模式下队列由 Go 引擎消费，由引擎执行工作流步骤与渠道分发。

这里的实现保持最简：按请求声明的渠道逐个投递——从注册表取该渠道当前可用的
实现、把 ``SubscriberTarget`` 的联系方式交给它，投递结果（成功 / 失败原因 /
耗时）逐条写入执行明细。渠道没配实现、缺地址、实现内部报错都记为失败并保留
原因，不再无条件写「已发送」；一条消息只要有渠道失败，整条记录即为失败。

投递之前先过一遍订阅者偏好（``app.delivery_gate``）：被关闭的渠道不取实现、
不发请求，直接以 ``skipped`` 写入执行明细并带上命中的层级。整条消息全部渠道
都被拦下时，消息记录的状态也是 ``skipped``——「没发」与「发失败」是两件事，
混进失败率会让一次正确的用户设置看起来像一次故障。

消息记录里的 ``channels`` 只列**实际尝试投递**的渠道：被偏好拦下的渠道既不
会进渠道分布，也不会让收件箱多出一张空卡片（收件箱按 ``channels`` 是否含
``in_app`` 决定要不要给这个人显示一条站内信）。被拦下的渠道另有去处——执行
明细里那条 ``skipped``。活动日志仍按触发时声明的渠道记录，两处口径互不干扰。

落库后端由 ``XW_STORAGE`` 决定（jsonl / postgres），见 ``storage.py``。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from .bus import get_bus
from .delivery_gate import GateDecision, get_delivery_gate, summarize
from .execution_log import record_execution
from .models.channel import Channel
from .providers import Delivery, Recipient, get_registry
from .schemas import QueuedMessage
from .storage import get_storage

logger = logging.getLogger("xw.worker")


def _record(msg: QueuedMessage, **fields) -> dict:
    """写一条执行明细，补齐该消息的租户、环境与工作流。

    后台线程没有请求上下文，租户必须从消息里取；漏掉这一补，租户 B 的投递
    明细会落在默认租户下，只有默认租户看得见。工作流名一并带上，管理统计
    才能按模板给出排行。
    """
    fields.setdefault("workflow", msg.name)
    return record_execution(
        trigger_id=msg.trigger_id,
        tenant=msg.tenant,
        environment=msg.environment,
        **fields,
    )


class Worker:
    """持续消费消息总线的后台线程。"""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="xw-worker", daemon=True)
        self._thread.start()
        logger.info("worker 已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3.0)
        logger.info("worker 已停止")

    def _run(self) -> None:
        bus = get_bus()
        while not self._stop.is_set():
            msg: QueuedMessage | None = bus.consume(timeout=1.0)
            if msg is None:
                continue
            self._process(msg)

    def _process(self, msg: QueuedMessage) -> None:
        """按渠道投递一条消息，并把结果写入持久化后端。"""
        channels = msg.channels or ["in_app"]
        recipient = Recipient(
            subscriber_id=msg.subscriber_id,
            email=msg.email,
            phone=msg.phone,
            device_token=msg.device_token,
        )
        registry = get_registry()
        failures: list[str] = []
        skipped: list[GateDecision] = []
        sent = 0
        # 实际尝试投递的渠道（按声明顺序）：被偏好拦下的不进这里。
        attempted: list[str] = []

        # 先解析渠道、再统一判定：一条消息的多个渠道共用同一份偏好快照，
        # 既省掉重复读取，也保证「邮件关了、推送没关」这类结论来自同一时刻。
        parsed = [_parse_channel(raw) for raw in channels]
        decisions = {
            decision.channel: decision
            for decision in get_delivery_gate().check(
                msg.subscriber_id,
                msg.name,
                [channel for channel in parsed if channel is not None],
            )
        }
        if any(not decision.allowed for decision in decisions.values()):
            logger.info(
                "偏好判定: %s -> %s %s",
                msg.name,
                msg.subscriber_id,
                summarize(decisions.values()),
            )

        for raw, channel in zip(channels, parsed):
            if channel is None:
                reason = f"未知渠道：{raw!r}"
                attempted.append(str(raw))
                _record(msg, channel=str(raw), status="failed", reason=reason
                )
                failures.append(reason)
                continue

            decision = decisions.get(channel.value)
            if decision is not None and not decision.allowed:
                # 被偏好拦下的渠道连实现都不取：既不发请求，也不该出现在
                # 「实现不可用」之类的失败原因里——它压根没打算发。
                skipped.append(decision)
                _record(msg, channel=channel.value,
                    status="skipped",
                    reason=decision.skip_reason,
                )
                continue

            attempted.append(channel.value)
            provider = registry.get(channel)
            if provider is None:
                reason = f"渠道 {channel.value} 没有可用的渠道实现"
                _record(msg, channel=channel.value, status="failed", reason=reason
                )
                failures.append(f"{channel.value}: {reason}")
                continue

            result = provider.send(
                Delivery(
                    channel=channel,
                    recipient=recipient,
                    subject=msg.name,
                    payload=msg.payload,
                    metadata={
                        "trigger_id": msg.trigger_id,
                        "transaction_id": msg.transaction_id,
                        "workflow": msg.name,
                    },
                )
            )
            _record(msg, channel=channel.value,
                status="sent" if result.sent else "failed",
                reason=result.reason,
                duration_ms=result.duration_ms,
                provider=result.provider_id,
            )
            if result.sent:
                sent += 1
                logger.info(
                    "已发送: %s -> %s (%s, %s, %dms)",
                    msg.name,
                    msg.subscriber_id,
                    channel.value,
                    result.provider_id,
                    result.duration_ms,
                )
            else:
                failures.append(f"{channel.value}: {result.reason}")
                logger.warning(
                    "投递失败: %s -> %s (%s)：%s",
                    msg.name,
                    msg.subscriber_id,
                    channel.value,
                    result.reason,
                )

        # 三种状态对应三种事实：有渠道失败、全部被偏好拦下、至少发出去一条。
        # 把「全部拦下」记成 failed 会让成功率随用户设置波动，排查时也分不清
        # 是渠道故障还是用户主动关闭。
        if failures:
            outcome = "failed"
        elif skipped and sent == 0:
            outcome = "skipped"
        else:
            outcome = "sent"

        record = {
            "tenant": msg.tenant,
            "environment": msg.environment,
            "trigger_id": msg.trigger_id,
            "transaction_id": msg.transaction_id,
            "workflow": msg.name,
            "subscriber_id": msg.subscriber_id,
            "channels": attempted,
            "payload": msg.payload,
            "status": outcome,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        # 同一条记录只说明原因，不按渠道重复：多个渠道被同一条偏好拦下时，
        # 「全局总开关已关闭」写一遍就够了。
        reasons = list(dict.fromkeys(failures or [d.skip_reason for d in skipped]))
        if reasons:
            record["reason"] = "; ".join(reasons)
        get_storage().save(record)


def _parse_channel(raw: object) -> Optional[Channel]:
    """把渠道名解析为枚举；非投递渠道或未知名称返回 ``None``。"""
    if not isinstance(raw, str):
        return None
    try:
        channel = Channel(raw.strip())
    except ValueError:
        return None
    return channel if channel.is_delivery() else None
