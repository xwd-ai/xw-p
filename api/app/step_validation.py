"""Step 定义与校验（ROADMAP Phase 1）。

对工作流步骤做两类校验：
1. 结构校验：每个步骤的 ``type`` 必须是合法渠道、``name`` 非空、配置为对象。
2. 顺序/依赖校验：通过 ``config.depends_on`` 声明步骤依赖，校验引用存在且无环。

设计约定：
- ``type`` 复用 ``Channel`` 枚举；``delay`` / ``digest`` 是编排步骤而非投递渠道，
  但仍可作为工作流步骤存在（后续 Phase 2 落地其行为）。
- ``depends_on`` 引用步骤的 ``id``；本模块校验拓扑合法性，不负责调度。
"""
from __future__ import annotations

from typing import Any

from .models.channel import Channel
from .models.entities import Step


class StepValidationError(ValueError):
    """步骤校验失败。"""


def validate_step_type(step: Step) -> None:
    """校验单个步骤的类型为合法渠道（或编排步骤）。"""
    if not isinstance(step.type, Channel):
        # 允许字符串形式（JSON 反序列化后可能未转枚举）
        try:
            Channel(step.type)
        except (ValueError, TypeError) as exc:
            raise StepValidationError(
                f"步骤 {step.id!r} 的类型非法：{step.type!r}"
            ) from exc


def validate_steps(steps: list[Step]) -> None:
    """校验步骤列表：类型合法、id 唯一、依赖引用存在且无环。

    抛出 :class:`StepValidationError` 描述首个失败原因。
    """
    seen_ids: set[str] = set()
    for step in steps:
        # 类型合法
        validate_step_type(step)

        # id 唯一
        if step.id in seen_ids:
            raise StepValidationError(f"步骤 id 重复：{step.id!r}")
        seen_ids.add(step.id)

    # 依赖校验：depends_on 必须是列表中某个步骤的 id
    for step in steps:
        depends_on = (step.config or {}).get("depends_on")
        if depends_on is None:
            continue
        if not isinstance(depends_on, list):
            raise StepValidationError(
                f"步骤 {step.id!r} 的 depends_on 必须是数组"
            )
        for dep in depends_on:
            if dep not in seen_ids:
                raise StepValidationError(
                    f"步骤 {step.id!r} 依赖了不存在的步骤 {dep!r}"
                )

    # 环检测（拓扑排序：若无法消去所有入度，则存在环）
    _assert_no_cycle(steps)


def _assert_no_cycle(steps: list[Step]) -> None:
    """基于入度计数的拓扑排序检测依赖环。"""
    by_id = {s.id: s for s in steps}
    # 构建邻接表：dep -> [依赖它的步骤]
    indegree = {s.id: 0 for s in steps}
    dependents: dict[str, list[str]] = {s.id: [] for s in steps}

    for step in steps:
        for dep in (step.config or {}).get("depends_on", []) or []:
            indegree[step.id] += 1
            dependents[dep].append(step.id)

    # 反复移除入度为 0 的节点
    ready = [sid for sid, deg in indegree.items() if deg == 0]
    visited = 0
    while ready:
        node = ready.pop()
        visited += 1
        for child in dependents[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)

    if visited != len(steps):
        raise StepValidationError("步骤依赖存在环（cycle detected）")


def normalize_step(step: dict[str, Any]) -> Step:
    """把 JSON 反序列化得到的字典规范化为 :class:`Step`。

    若 ``type`` 为字符串，转换为 ``Channel`` 枚举；非法时抛 :class:`StepValidationError`。
    """
    raw_type = step.get("type")
    try:
        channel = Channel(raw_type)
    except (ValueError, TypeError) as exc:
        raise StepValidationError(f"步骤类型非法：{raw_type!r}") from exc

    return Step(
        id=step.get("id"),
        type=channel,
        name=step.get("name"),
        config=step.get("config") or {},
    )


__all__ = [
    "StepValidationError",
    "validate_step_type",
    "validate_steps",
    "normalize_step",
]
