"""工作流模板注册表：触发时的模板查找。

触发 API 按 ``name`` 查找启用的工作流模板。当前为进程内注册表
（预置内容社区互动示例模板，方便 demo 与测试开箱即用）；
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


def _post_liked_workflow() -> Workflow:
    """预置示例：帖子被点赞（按帖子聚合后站内信 + 推送）。"""
    return Workflow(
        id="wf-post-liked",
        name="post_liked",
        description="内容社区：帖子被点赞，按帖子聚合后通知创作者",
        steps=[
            Step(
                id="step-1",
                type=Channel.DIGEST,
                name="点赞聚合",
                config={
                    # 聚合键：同一篇帖子的点赞合并成一张卡片，避免刷屏；
                    # 窗口内的后续点赞在地图上原地累加计数（引擎 digest.go）。
                    "key": "post_id",
                    "window": "10m",
                    "title": "你的帖子有新互动",
                    "template": "{{ digest.actor }} 等 {{ digest.count }} 人赞了你的帖子《{{ payload.post_title }}》",
                },
            ),
            Step(
                id="step-2",
                type=Channel.IN_APP,
                name="站内信",
                config={
                    "depends_on": ["step-1"],
                    "template": "{{ payload.actor }} 赞了你的帖子《{{ payload.post_title }}》",
                },
            ),
            Step(
                id="step-3",
                type=Channel.PUSH,
                name="推送",
                config={
                    "depends_on": ["step-1"],
                    "template": "{{ payload.actor }} 赞了你的帖子",
                },
            ),
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


def _post_commented_workflow() -> Workflow:
    """预置示例：帖子收到评论（站内信 + 推送 + 邮件）。"""
    return Workflow(
        id="wf-post-commented",
        name="post_commented",
        description="内容社区：帖子收到评论，站内信 + 推送 + 邮件通知创作者",
        steps=[
            Step(
                id="step-1",
                type=Channel.IN_APP,
                name="站内信",
                config={
                    "template": "{{payload.actor}} 评论了你的帖子《{{payload.post_title}}》：{{payload.comment}}"
                },
            ),
            Step(
                id="step-2",
                type=Channel.PUSH,
                name="推送",
                config={"template": "{{payload.actor}} 评论了你的帖子"},
            ),
            Step(
                id="step-3",
                type=Channel.EMAIL,
                name="邮件",
                config={
                    "template": "你的帖子《{{payload.post_title}}》收到新评论：{{payload.comment}}"
                },
            ),
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


def _post_mentioned_workflow() -> Workflow:
    """预置示例：评论区被 @（站内信 + 推送）。"""
    return Workflow(
        id="wf-post-mentioned",
        name="post_mentioned",
        description="内容社区：被其他用户 @ 提及，站内信 + 推送即时提醒",
        steps=[
            Step(
                id="step-1",
                type=Channel.IN_APP,
                name="站内信",
                config={"template": "{{payload.actor}} 在《{{payload.post_title}}》的评论区提到了你"},
            ),
            Step(
                id="step-2",
                type=Channel.PUSH,
                name="推送",
                config={"template": "{{payload.actor}} 在评论里 @ 了你"},
            ),
        ],
        environment=Environment.DEVELOPMENT,
        active=True,
    )


def seed_workflows() -> list[Workflow]:
    """返回预置的内容社区互动示例工作流（demo 开箱即用）。"""
    return [
        _post_liked_workflow(),
        _post_commented_workflow(),
        _post_mentioned_workflow(),
    ]


class WorkflowRegistry:
    """进程内工作流模板注册表（线程安全）。"""

    def __init__(self) -> None:
        self._by_name: dict[str, Workflow] = {}
        self._lock = threading.Lock()
        # 预置内容社区互动示例模板
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
