"""工作流模板 CRUD API（ROADMAP）。

提供工作流模板的创建/查询/列表/更新/删除端点，接入 ``workflow_store``
存储层（内存 / Postgres 双后端）。创建与更新时做步骤校验，更新时版本号自增。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status

from .models.entities import Step, Workflow
from .step_validation import StepValidationError
from .workflow_schemas import (
    StepOut,
    WorkflowCreate,
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


@router.post("", response_model=WorkflowOut, status_code=status.HTTP_201_CREATED)
def create_workflow(body: WorkflowCreate) -> WorkflowOut:
    """创建一个工作流模板（版本号初始为 1）。"""
    steps = [
        Step(
            id=str(uuid.uuid4()),
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


@router.get("", response_model=list[WorkflowOut])
def list_workflows() -> list[WorkflowOut]:
    """列出所有工作流模板。"""
    return [_workflow_out(wf) for wf in get_workflow_store().list()]


@router.get("/{workflow_id}", response_model=WorkflowOut)
def get_workflow(workflow_id: str) -> WorkflowOut:
    """按 id 查询工作流模板。"""
    workflow = get_workflow_store().get(workflow_id)
    if workflow is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")
    return _workflow_out(workflow)


@router.put("/{workflow_id}", response_model=WorkflowOut)
def update_workflow(workflow_id: str, body: WorkflowUpdate) -> WorkflowOut:
    """更新工作流模板（版本号自增 1）。"""
    store = get_workflow_store()
    existing = store.get(workflow_id)
    if existing is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")

    new_steps = existing.steps
    if body.steps is not None:
        new_steps = [
            Step(id=str(uuid.uuid4()), type=s.type, name=s.name, config=s.config)
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


@router.delete("/{workflow_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_workflow(workflow_id: str) -> None:
    """删除工作流模板。"""
    deleted = get_workflow_store().delete(workflow_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"工作流不存在：{workflow_id}")
    return None


__all__ = ["router"]
