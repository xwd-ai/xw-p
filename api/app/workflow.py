"""工作流模板注册表：触发时的模板查找。

触发 API 按 ``name`` 查找启用的工作流模板。当前为进程内注册表
（预置一个 ``welcome`` 示例模板，方便 demo 与测试开箱即用）；
后续 Phase 1 的模板 CRUD API 会接入 Postgres 持久化，本注册表作为
内存实现保留，通过统一接口屏蔽差异。
"""
from __future__ import annotations

import logging
import threading
from typing import Optional

from .models.entities import Environment, Step, Workflow

logger = logging.getLogger("xw.workflow")


def _welcome_workflow() -> Workflow:
    """预置示例模板：welcome（in-app 欢迎通知）。"""
    return Workflow(
        id="wf-welcome",
        name="welcome",
        description="示例：新订阅者欢迎通知",
        steps=[
            Step(
                id="step-1",
                type="in_app",
                name="发送欢迎消息",
                config={"template": "你好 {{payload.user}}，欢迎使用 xw-notify！"},
            )
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


class WorkflowRegistry:
    """进程内工作流模板注册表（线程安全）。"""

    def __init__(self) -> None:
        self._by_name: dict[str, Workflow] = {}
        self._lock = threading.Lock()
        # 预置示例模板
        self._by_name["welcome"] = _welcome_workflow()

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
