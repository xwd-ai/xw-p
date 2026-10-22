"""工作流模板注册表：触发时的模板查找。

触发 API 按 ``name`` 查找启用的工作流模板。当前为进程内注册表
（预置电商订单示例模板，方便 demo 与测试开箱即用）；
后续 Phase 1 的模板 CRUD API 会接入 Postgres 持久化，本注册表作为
内存实现保留，通过统一接口屏蔽差异。
"""
from __future__ import annotations

import logging
import threading
from typing import Optional

from .models.channel import Channel
from .models.entities import Environment, Step, Workflow

logger = logging.getLogger("xw.workflow")


def _order_confirmed_workflow() -> Workflow:
    """预置示例：订单支付成功（站内 + 邮件 + 短信多渠道触达）。"""
    return Workflow(
        id="wf-order-confirmed",
        name="order_confirmed",
        description="电商：订单支付成功，站内信 + 邮件 + 短信多渠道通知",
        steps=[
            Step(
                id="step-1",
                type=Channel.IN_APP,
                name="站内信",
                config={"template": "订单 {{payload.order_id}} 支付成功，商家正在备货。"},
            ),
            Step(
                id="step-2",
                type=Channel.EMAIL,
                name="邮件",
                config={"template": "您好 {{payload.customer}}，订单 {{payload.order_id}} 已支付 ¥{{payload.amount}}。"},
            ),
            Step(
                id="step-3",
                type=Channel.SMS,
                name="短信",
                config={"template": "【订单】{{payload.order_id}} 已支付成功。"},
            ),
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


def _order_shipped_workflow() -> Workflow:
    """预置示例：订单已发货（邮件 + 推送通知）。"""
    return Workflow(
        id="wf-order-shipped",
        name="order_shipped",
        description="电商：订单发货，邮件 + 推送通知物流信息",
        steps=[
            Step(
                id="step-1",
                type=Channel.EMAIL,
                name="邮件",
                config={"template": "您的订单 {{payload.order_id}} 已发货，运单号 {{payload.tracking_no}}。"},
            ),
            Step(
                id="step-2",
                type=Channel.PUSH,
                name="推送",
                config={"template": "订单 {{payload.order_id}} 已发货。"},
            ),
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


def _order_delivered_workflow() -> Workflow:
    """预置示例：订单已签收（站内信提醒）。"""
    return Workflow(
        id="wf-order-delivered",
        name="order_delivered",
        description="电商：订单签收，站内信提醒并引导评价",
        steps=[
            Step(
                id="step-1",
                type=Channel.IN_APP,
                name="站内信",
                config={"template": "订单 {{payload.order_id}} 已签收，感谢您的购买！"},
            ),
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


def seed_workflows() -> list[Workflow]:
    """返回预置的电商订单示例工作流（demo 开箱即用）。"""
    return [
        _order_confirmed_workflow(),
        _order_shipped_workflow(),
        _order_delivered_workflow(),
    ]


class WorkflowRegistry:
    """进程内工作流模板注册表（线程安全）。"""

    def __init__(self) -> None:
        self._by_name: dict[str, Workflow] = {}
        self._lock = threading.Lock()
        # 预置电商订单示例模板
        for wf in seed_workflows():
            self._by_name[wf.name] = wf

    def get(self, name: str) -> Optional[Workflow]:
        """按名称查找启用的模板；不存在或未启用返回 None。"""
        with self._lock:
            wf = self._by_name.get(name)
            if wf is None:
                return None
            if not wf.active:
                return None
            return wf

    def register(self, workflow: Workflow) -> None:
        """注册（或覆盖）一个模板。"""
        with self._lock:
            self._by_name[workflow.name] = workflow

    def names(self) -> list[str]:
        """返回所有已注册模板的名称（测试与调试用）。"""
        with self._lock:
            return list(self._by_name.keys())


_registry: Optional[WorkflowRegistry] = None
_registry_lock = threading.Lock()


def get_registry() -> WorkflowRegistry:
    """获取全局模板注册表单例。"""
    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = WorkflowRegistry()
        return _registry
