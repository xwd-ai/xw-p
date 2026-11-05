"""工作流模板 CRUD API（ROADMAP Phase 1）。

提供工作流模板的创建/查询/列表/更新/删除端点，接入 ``workflow_store``
存储层（内存 / Postgres 双后端）。创建与更新时做步骤校验，更新时版本号自增。

另有两条运营端点（ROADMAP Phase 5）：

- ``PUT /v1/workflows/{id}/active``：启停。只翻 ``active`` 且不动版本号，
  停用后触发链路按「模板不存在或未启用」拒绝新事件。
- ``POST /v1/workflows/{id}/duplicate``：复制。新标识、新步骤标识、依赖重映射，
  副本默认不启用。

两者与 ``PUT /v1/workflows/{id}`` 的差别写在 ``workflow_ops`` 的模块说明里。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, status

from .models.entities import Step, Workflow
from .step_validation import StepValidationError
from .workflow_ops import AUTO_NAME_ATTEMPTS, clone_workflow, copy_name, set_active
from .workflow_schemas import (
    StepOut,
    WorkflowActivationIn,
    WorkflowActivationOut,
    WorkflowCreate,
    WorkflowDuplicateIn,
    WorkflowOut,
    WorkflowUpdate,
)
from .workflow_store import (
    WorkflowConflictError,
    WorkflowNotFoundError,
    get_workflow_store,
    validate_workflow,
)

router = APIRouter(prefix="/v1/workflows", tags=["workflows"])


def _steps_out(workflow: Workflow) -> list[StepOut]:
    return [
        StepOut(
            id=s.id,
            type=s.type.value if hasattr(s.type, "value") else str(s.type),
            name=s.name,
            config=s.config,
        )
        for s in workflow.steps
    ]


def _workflow_out(workflow: Workflow) -> WorkflowOut:
    env = workflow.environment
    return WorkflowOut(
        id=workflow.id,
        name=workflow.name,
        description=workflow.description,
        steps=_steps_out(workflow),
        version=workflow.version,
        environment=env.value if hasattr(env, "value") else str(env),
        active=workflow.active,
        created_at=workflow.created_at.isoformat(),
    )


@router.post(
    "",
    response_model=WorkflowOut,
    status_code=status.HTTP_201_CREATED,
    summary="创建工作流模板",
    description=(
        "创建一个工作流模板，版本号从 1 开始，默认启用。\n\n"
        "步骤按数组顺序执行；步骤标识可以自带（客户端维护 ``config.depends_on`` 时需要它稳定），"
        "留空由服务端生成。同名工作流在同一环境内唯一，重复创建返回 409。\n\n"
        "创建时会按步骤类型校验依赖：悬空引用、自依赖与依赖环都在这一层拒绝，"
        "不会等到第一条消息投递失败才暴露。"
    ),
)
def create_workflow(body: WorkflowCreate) -> WorkflowOut:
    """创建一个工作流模板（版本号初始为 1）。"""
    steps = [
        Step(
            id=s.id or str(uuid.uuid4()),
            type=s.type,
            name=s.name,
            config=s.config,
        )
        for s in body.steps
    ]
    workflow = Workflow(
        id=str(uuid.uuid4()),
        name=body.name,
        description=body.description,
        steps=steps,
        version=1,
        environment=body.environment,
        active=True,
        created_at=datetime.now(timezone.utc),
    )
    try:
        validate_workflow(workflow)
    except (StepValidationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))

    store = get_workflow_store()
    try:
        created = store.create(workflow)
    except WorkflowConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return _workflow_out(created)


@router.get(
    "",
    response_model=list[WorkflowOut],
    summary="列出工作流模板",
    description=(
        "返回模板列表，包含全部环境（条目里带 ``environment`` 字段），"
        "客户端按需过滤。列表不分页：模板数量由运营维护，量级与消息量无关。"
    ),
)
def list_workflows() -> list[WorkflowOut]:
    """列出所有工作流模板。"""
    return [_workflow_out(wf) for wf in get_workflow_store().list()]


@router.get(
    "/{workflow_id}",
    response_model=WorkflowOut,
    summary="按标识读取工作流模板",
    description="读取单个模板的当前版本。已停用的模板照样能读到，``active`` 标明状态。",
)
def get_workflow(workflow_id: str) -> WorkflowOut:
    """按 id 查询工作流模板。"""
    workflow = get_workflow_store().get(workflow_id)
    if workflow is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")
    return _workflow_out(workflow)


@router.put(
    "/{workflow_id}",
    response_model=WorkflowOut,
    summary="更新工作流模板",
    description=(
        "整体更新模板内容，版本号自增 1。只改请求里出现的字段：没传 ``steps`` 就保留原步骤，"
        "没传 ``active`` 就保留当前启停状态。\n\n"
        "版本号是「模板内容改过一次」的标记。只是启停请走 ``PUT /v1/workflows/{id}/active``，"
        "那条路径不动版本号，按版本回溯模板时才分得清哪一版是真正的改动。\n\n"
        "已入队并携带步骤快照的消息按快照执行，不受本次更新影响。"
    ),
)
def update_workflow(workflow_id: str, body: WorkflowUpdate) -> WorkflowOut:
    """更新工作流模板（版本号自增 1）。"""
    store = get_workflow_store()
    existing = store.get(workflow_id)
    if existing is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")

    new_steps = existing.steps
    if body.steps is not None:
        new_steps = [
            Step(id=s.id or str(uuid.uuid4()), type=s.type, name=s.name, config=s.config)
            for s in body.steps
        ]

    updated = Workflow(
        id=existing.id,
        name=body.name if body.name is not None else existing.name,
        description=body.description if body.description is not None else existing.description,
        steps=new_steps,
        version=existing.version + 1,
        environment=body.environment if body.environment is not None else existing.environment,
        active=body.active if body.active is not None else existing.active,
        created_at=existing.created_at,
    )

    try:
        validate_workflow(updated)
    except (StepValidationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))

    try:
        result = store.update(updated)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except WorkflowConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return _workflow_out(result)


@router.delete(
    "/{workflow_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="删除工作流模板",
    description=(
        "删除模板后不再能触发，已入队的消息按快照跑完。"
        "只删模板而不清理历史，活动日志与执行明细里的旧记录仍然可查——"
        "排查历史投递时它们仍是唯一线索。"
    ),
)
def delete_workflow(workflow_id: str) -> None:
    """删除工作流模板。"""
    deleted = get_workflow_store().delete(workflow_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")
    return None


@router.put(
    "/{workflow_id}/active",
    response_model=WorkflowActivationOut,
    summary="启用或停用一个工作流",
    description=(
        "翻转模板的 ``active`` 状态。与 ``PUT /v1/workflows/{id}`` 的区别：\n\n"
        "- 版本号不变——启停是运营状态，不是模板内容的一次修改；\n"
        "- 停用后触发链路（单条 / 批量 / 分组广播）按「模板不存在或未启用」拒绝新事件；\n"
        "- 已经入队并携带步骤快照的消息照常执行，撤销一条已受理的通知比多发一条更难解释。\n\n"
        "重复设置同一状态不产生写入，回执里 ``changed=false``。"
    ),
    responses={
        200: {"description": "启停结果"},
        404: {"description": "工作流不存在"},
    },
)
def set_workflow_active(workflow_id: str, body: WorkflowActivationIn) -> WorkflowActivationOut:
    """启用或停用工作流（幂等，不改版本号）。"""
    store = get_workflow_store()
    existing = store.get(workflow_id)
    if existing is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")

    if existing.active == body.active:
        # 幂等：状态本来就对，不写库也不推进版本号。
        return WorkflowActivationOut(workflow=_workflow_out(existing), changed=False)

    updated = set_active(existing, body.active)
    try:
        stored = store.update(updated)
    except WorkflowNotFoundError as exc:
        # 读到与写入之间被别的请求删掉了。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except WorkflowConflictError as exc:
        # 名称没变，理论上不会冲突；仍然如实报出来而不是吞掉。
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return WorkflowActivationOut(workflow=_workflow_out(stored), changed=True)


@router.post(
    "/{workflow_id}/duplicate",
    response_model=WorkflowOut,
    status_code=status.HTTP_201_CREATED,
    summary="复制一个工作流",
    description=(
        "按源模板复制出一份新模板：新标识、新步骤标识，``depends_on`` 按原步骤"
        "标识重映射，``config`` 深拷贝，版本号从 1 开始。\n\n"
        "副本默认不启用（``active=false``），确认步骤与文案后再启用；也可以在"
        "请求体里直接给 ``active=true``。\n\n"
        "``name`` 留空时按「源名称 + ``_copy``」生成，重复复制依次得到 "
        "``_copy2`` / ``_copy3``；显式指定的名称已被占用返回 409，不会悄悄改名。"
    ),
    responses={
        201: {"description": "副本已创建"},
        404: {"description": "源工作流不存在"},
        409: {"description": "指定的名称已存在"},
        422: {"description": "名称或环境不合法"},
    },
)
def duplicate_workflow(
    workflow_id: str, body: Optional[WorkflowDuplicateIn] = None
) -> WorkflowOut:
    """复制工作流模板。"""
    store = get_workflow_store()
    source = store.get(workflow_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")

    requested = body.name.strip() if body and body.name else None
    if requested == "":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="副本名称不能为空白"
        )
    environment = body.environment if body else None
    active = bool(body.active) if body else False

    # 自动命名时允许重试：重名多半是并发复制撞上了，换下一个候选即可。
    attempts = 1 if requested else AUTO_NAME_ATTEMPTS
    for attempt in range(attempts):
        if requested:
            name = requested
        else:
            name = copy_name(source.name, [wf.name for wf in store.list()])

        duplicate = clone_workflow(
            source, name=name, environment=environment, active=active
        )
        try:
            validate_workflow(duplicate)
        except (StepValidationError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
            )

        try:
            created = store.create(duplicate)
        except WorkflowConflictError as exc:
            if requested or attempt == attempts - 1:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT, detail=str(exc)
                )
            continue
        return _workflow_out(created)

    # 循环只在「每次都被抢占」时走完，正常路径不会走到这里。
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT, detail="副本名称连续冲突，请稍后重试"
    )


__all__ = ["router"]
