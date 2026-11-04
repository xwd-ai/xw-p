"""工作流的运营操作：启停与复制。

这两件事都只改「运营状态」，不改模板内容，因此与 ``PUT /v1/workflows/{id}``
分开：

- **启停**只翻 ``active``。模板内容没变，版本号就不变——把「点一下停用」记成
  一次模板变更，会让按版本回溯模板的人分不清哪一版才是真正的改动。停用只拦
  新的触发：已经入队、携带着步骤快照的消息照常执行完，撤销一条已经受理的通知
  比多发一条更难解释。
- **复制**产出的是新模板：新标识、新步骤标识，``depends_on`` 按原步骤标识
  重映射。复制不是浅拷贝——``config`` 里可能有嵌套的列表与对象，与源共享
  会让改副本连带改到源。副本默认不启用，先当草稿看，确认后再打开。

判定「能不能触发」仍然只有一处（``trigger_core.resolve_workflow``），这里只
负责把状态改对，避免同一件事在路由与解析两处各写一套规则。
"""
from __future__ import annotations

import copy
import uuid
from datetime import datetime, timezone
from typing import Collection, Optional

from .models.entities import Environment, Step, Workflow

# 工作流名称的存储与校验上限（与 ``WorkflowCreate.name`` 的 max_length 一致）。
MAX_NAME_LENGTH = 255

# 自动命名时最多重试几次；只在名字是自动生成的情况下重试，调用方指定的名字
# 撞车要如实报 409，不能悄悄换成另一个名字。
AUTO_NAME_ATTEMPTS = 5


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _with_suffix(stem: str, suffix: str) -> str:
    """把后缀接到词干上，超出长度上限时截断词干而不是截断后缀。

    ``_copy2`` 被截成 ``_cop`` 就失去了「第几个副本」的信息，而词干少几个字符
    不影响识别。
    """
    room = MAX_NAME_LENGTH - len(suffix)
    if room <= 0:  # 后缀本身超长，理论上不会发生，兜底只留后缀
        return suffix[:MAX_NAME_LENGTH]
    return f"{stem[:room]}{suffix}"


def copy_name(base: str, taken: Collection[str]) -> str:
    """为副本生成一个未被占用的名称：``x`` → ``x_copy`` → ``x_copy2`` …"""
    stem = (base or "").strip() or "workflow"
    existing = set(taken)
    candidate = _with_suffix(stem, "_copy")
    if candidate not in existing:
        return candidate
    index = 2
    while True:
        candidate = _with_suffix(stem, f"_copy{index}")
        if candidate not in existing:
            return candidate
        index += 1


def set_active(workflow: Workflow, active: bool) -> Workflow:
    """返回把 ``active`` 改成目标值的工作流副本（版本号与时间戳保持原样）。

    返回新对象而不是原地改写：入参可能是存储里的共享实例，也可能正被另一个
    请求读取，就地修改会让别人看到半个状态。
    """
    return workflow.model_copy(update={"active": bool(active)})


def clone_workflow(
    source: Workflow,
    *,
    name: str,
    environment: Optional[Environment] = None,
    active: bool = False,
) -> Workflow:
    """按源模板克隆一份新模板。

    - 步骤标识全部重新生成，``config.depends_on`` 里的引用同步替换——直接沿用
      旧标识会让两份模板在日志与执行明细里无法区分，而只换标识不换引用会让
      副本一创建就带着悬空依赖。
    - ``config`` 深拷贝，嵌套结构不与源共享。
    - 版本号回到 1：副本是一份新的模板，不是源的下一版。
    - ``environment`` 不传时跟随源，避免把生产模板复制到开发环境却毫无提示。
    """
    id_map: dict[str, str] = {}
    for step in source.steps:
        if step.id:
            id_map[step.id] = str(uuid.uuid4())

    steps: list[Step] = []
    for step in source.steps:
        config = copy.deepcopy(step.config or {})
        depends_on = config.get("depends_on")
        if isinstance(depends_on, list):
            config["depends_on"] = [id_map.get(dep, dep) for dep in depends_on]
        steps.append(
            Step(
                id=id_map.get(step.id) or str(uuid.uuid4()),
                type=step.type,
                name=step.name,
                config=config,
            )
        )

    return Workflow(
        id=str(uuid.uuid4()),
        name=name,
        description=source.description,
        steps=steps,
        version=1,
        environment=environment if environment is not None else source.environment,
        active=bool(active),
        created_at=_now_utc(),
    )


__all__ = [
    "AUTO_NAME_ATTEMPTS",
    "MAX_NAME_LENGTH",
    "clone_workflow",
    "copy_name",
    "set_active",
]
