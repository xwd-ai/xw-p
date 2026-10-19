"""工作流模板存储层：内存 / Postgres 双后端（ROADMAP）。

提供工作流模板的增删改查与版本管理。通过 ``XW_STORAGE`` 环境变量选择后端，
与消息持久化保持一致；默认使用内存后端，demo 与测试无需外部依赖。

版本号语义：
- 创建时版本为 1。
- 更新时版本 +1（不可回退）；调用方也可显式传入更高版本号。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Optional, Protocol

from .models.entities import Step, Workflow
from .step_validation import normalize_step, validate_steps

logger = logging.getLogger("xw.workflowstore")


class WorkflowNotFoundError(KeyError):
    """工作流不存在。"""


class WorkflowConflictError(ValueError):
    """名称冲突（创建时已存在同名模板）。"""


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _workflow_from_row(row: dict[str, Any]) -> Workflow:
    """把存储行（dict）转换为领域实体，steps 由 JSON 规范化。"""
    steps_raw = row.get("steps") or []
    steps = [normalize_step(s) for s in steps_raw]
    return Workflow(
        id=row["id"],
        name=row["name"],
        description=row.get("description"),
        steps=steps,
        version=int(row.get("version", 1)),
        environment=row.get("environment", "development"),
        active=bool(row.get("active", True)),
        created_at=row.get("created_at") or _now_utc(),
    )


def _workflow_to_row(workflow: Workflow) -> dict[str, Any]:
    """把领域实体转换为存储行（steps 序列化为 JSON 友好结构）。"""
    return {
        "id": workflow.id,
        "name": workflow.name,
        "description": workflow.description,
        "steps": [
            {"id": s.id, "type": s.type.value, "name": s.name, "config": s.config}
            for s in workflow.steps
        ],
        "version": workflow.version,
        "environment": workflow.environment.value
        if hasattr(workflow.environment, "value")
        else str(workflow.environment),
        "active": workflow.active,
        "created_at": workflow.created_at,
    }


class WorkflowStore(Protocol):
    """工作流模板存储统一接口。"""

    def create(self, workflow: Workflow) -> Workflow: ...

    def get(self, workflow_id: str) -> Optional[Workflow]: ...

    def get_by_name(self, name: str) -> Optional[Workflow]: ...

    def list(self) -> list[Workflow]: ...

    def update(self, workflow: Workflow) -> Workflow: ...

    def delete(self, workflow_id: str) -> bool: ...


class MemoryWorkflowStore:
    """进程内工作流存储（线程安全，测试与 demo 使用）。"""

    def __init__(self, seed: Optional[list[Workflow]] = None) -> None:
        self._by_id: dict[str, Workflow] = {}
        self._lock = threading.Lock()
        for wf in seed or []:
            self._by_id[wf.id] = wf

    def _ensure_unique_name(self, name: str, exclude_id: Optional[str] = None) -> None:
        for wf in self._by_id.values():
            if wf.name == name and wf.id != exclude_id:
                raise WorkflowConflictError(f"工作流名称已存在：{name}")

    def create(self, workflow: Workflow) -> Workflow:
        with self._lock:
            self._ensure_unique_name(workflow.name)
            self._by_id[workflow.id] = workflow
            return workflow

    def get(self, workflow_id: str) -> Optional[Workflow]:
        with self._lock:
            return self._by_id.get(workflow_id)

    def get_by_name(self, name: str) -> Optional[Workflow]:
        with self._lock:
            for wf in self._by_id.values():
                if wf.name == name:
                    return wf
            return None

    def list(self) -> list[Workflow]:
        with self._lock:
            return list(self._by_id.values())

    def update(self, workflow: Workflow) -> Workflow:
        with self._lock:
            if workflow.id not in self._by_id:
                raise WorkflowNotFoundError(f"工作流不存在：{workflow.id}")
            self._ensure_unique_name(workflow.name, exclude_id=workflow.id)
            self._by_id[workflow.id] = workflow
            return workflow

    def delete(self, workflow_id: str) -> bool:
        with self._lock:
            if workflow_id not in self._by_id:
                return False
            del self._by_id[workflow_id]
            return True


class PostgresWorkflowStore:
    """Postgres 工作流存储（SQLAlchemy Core，惰性建连）。

    使用 ``workflows`` 表（见 ``scripts/migrations/002_core_entities.sql``）。
    连接由 ``XW_DATABASE_URL`` 配置，未就绪时操作会抛出连接异常。
    """

    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self._url)
        return self._engine

    def create(self, workflow: Workflow) -> Workflow:
        from sqlalchemy import text

        row = _workflow_to_row(workflow)
        engine = self._get_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO workflows
                        (id, name, description, steps, version, environment, active, created_at)
                    VALUES
                        (:id, :name, :description, :steps, :version, :environment, :active, :created_at)
                    """
                ),
                {
                    "id": row["id"],
                    "name": row["name"],
                    "description": row["description"],
                    "steps": json.dumps(row["steps"], ensure_ascii=False),
                    "version": row["version"],
                    "environment": row["environment"],
                    "active": row["active"],
                    "created_at": row["created_at"],
                },
            )
        return workflow

    def get(self, workflow_id: str) -> Optional[Workflow]:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM workflows WHERE id = :id"),
                {"id": workflow_id},
            ).mappings().first()
        return _workflow_from_row(dict(result)) if result else None

    def get_by_name(self, name: str) -> Optional[Workflow]:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM workflows WHERE name = :name"),
                {"name": name},
            ).mappings().first()
        return _workflow_from_row(dict(result)) if result else None

    def list(self) -> list[Workflow]:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM workflows ORDER BY created_at")
            ).mappings().all()
        return [_workflow_from_row(dict(r)) for r in rows]

    def update(self, workflow: Workflow) -> Workflow:
        from sqlalchemy import text

        row = _workflow_to_row(workflow)
        engine = self._get_engine()
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE workflows
                    SET description = :description, steps = :steps, version = :version,
                        environment = :environment, active = :active
                    WHERE id = :id
                    """
                ),
                {
                    "id": row["id"],
                    "description": row["description"],
                    "steps": json.dumps(row["steps"], ensure_ascii=False),
                    "version": row["version"],
                    "environment": row["environment"],
                    "active": row["active"],
                },
            )
            if result.rowcount == 0:
                raise WorkflowNotFoundError(f"工作流不存在：{workflow.id}")
        return workflow

    def delete(self, workflow_id: str) -> bool:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM workflows WHERE id = :id"), {"id": workflow_id}
            )
            return result.rowcount > 0


def create_workflow_store(backend: str, database_url: str) -> WorkflowStore:
    """根据配置创建工作流存储实例。"""
    if backend == "postgres":
        logger.info("工作流存储后端：postgres")
        return PostgresWorkflowStore(database_url)
    logger.info("工作流存储后端：memory")
    return MemoryWorkflowStore()


_store: Optional[WorkflowStore] = None
_store_lock = threading.Lock()


def get_workflow_store() -> WorkflowStore:
    """获取全局工作流存储单例（惰性初始化，后端由配置决定）。"""
    global _store
    with _store_lock:
        if _store is None:
            from . import config

            _store = create_workflow_store(config.STORAGE_BACKEND, config.DATABASE_URL)
        return _store


def validate_workflow(workflow: Workflow) -> None:
    """校验工作流结构：名称非空、步骤合法。"""
    if not workflow.name or not workflow.name.strip():
        raise ValueError("工作流名称不能为空")
    validate_steps(workflow.steps)


__all__ = [
    "WorkflowNotFoundError",
    "WorkflowConflictError",
    "WorkflowStore",
    "MemoryWorkflowStore",
    "PostgresWorkflowStore",
    "create_workflow_store",
    "validate_workflow",
]
