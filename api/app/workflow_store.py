"""工作流模板存储层：内存 / Postgres 双后端（ROADMAP Phase 1）。

提供工作流模板的增删改查与版本管理。通过 ``XW_STORAGE`` 环境变量选择后端，
与消息持久化保持一致；默认使用内存后端，demo 与测试无需外部依赖。

版本号语义：
- 创建时版本为 1。
- 更新时版本 +1（不可回退）；调用方也可显式传入更高版本号。

**环境是模板身份的一部分**：同名模板在开发与生产各有一份，名称唯一性也按
环境判定——线上已经跑着 ``post_liked``，不该因为本地要改文案就不让建。按名称
查找模板时必须带上环境，否则两套环境里同名的模板会互相遮蔽：生产环境触发到
本地那份（或者反过来）只是改个名字就会发生的故障。
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


def environment_of(workflow: Workflow) -> str:
    """取工作流所属环境（实体上可能是枚举，也可能是字符串）。"""
    value = workflow.environment
    return value.value if hasattr(value, "value") else str(value)


def _wanted_env(value: object) -> Optional[str]:
    """把 ``environment`` 参数归一成字符串；``None`` / 空白表示「不限环境」。"""
    if value is None:
        return None
    if hasattr(value, "value"):
        value = value.value
    text = str(value).strip().lower()
    return text or None


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

    def get_by_name(
        self, name: str, *, environment: object = None
    ) -> Optional[Workflow]: ...

    def list(self, *, environment: object = None) -> list[Workflow]: ...

    def update(self, workflow: Workflow) -> Workflow: ...

    def delete(self, workflow_id: str) -> bool: ...


class MemoryWorkflowStore:
    """进程内工作流存储（线程安全，测试与 demo 使用）。"""

    def __init__(self, seed: Optional[list[Workflow]] = None) -> None:
        self._by_id: dict[str, Workflow] = {}
        self._lock = threading.Lock()
        for wf in seed or []:
            self._by_id[wf.id] = wf

    def _ensure_unique_name(
        self,
        workflow: Workflow,
        exclude_id: Optional[str] = None,
    ) -> None:
        """名称在同一环境内唯一；另一套环境的同名模板不参与比较。"""
        environment = environment_of(workflow)
        for other in self._by_id.values():
            if (
                other.name == workflow.name
                and other.id != exclude_id
                and environment_of(other) == environment
            ):
                raise WorkflowConflictError(
                    f"工作流名称已存在：{workflow.name}（环境 {environment}）"
                )

    def create(self, workflow: Workflow) -> Workflow:
        with self._lock:
            self._ensure_unique_name(workflow)
            self._by_id[workflow.id] = workflow
            return workflow

    def get(self, workflow_id: str) -> Optional[Workflow]:
        with self._lock:
            return self._by_id.get(workflow_id)

    def get_by_name(
        self, name: str, *, environment: object = None
    ) -> Optional[Workflow]:
        wanted = _wanted_env(environment)
        with self._lock:
            for wf in self._by_id.values():
                if wf.name != name:
                    continue
                if wanted is not None and environment_of(wf) != wanted:
                    continue
                return wf
            return None

    def list(self, *, environment: object = None) -> list[Workflow]:
        wanted = _wanted_env(environment)
        with self._lock:
            items = list(self._by_id.values())
        if wanted is None:
            return items
        return [wf for wf in items if environment_of(wf) == wanted]

    def update(self, workflow: Workflow) -> Workflow:
        with self._lock:
            if workflow.id not in self._by_id:
                raise WorkflowNotFoundError(f"工作流不存在：{workflow.id}")
            self._ensure_unique_name(workflow, exclude_id=workflow.id)
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

    def __init__(self, database_url: str, tenant: str = "default") -> None:
        self._url = database_url
        self._tenant = tenant
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
                        (id, tenant, name, description, steps, version, environment, active, created_at)
                    VALUES
                        (:id, :tenant, :name, :description, :steps, :version, :environment, :active, :created_at)
                    """
                ),
                {
                    "id": row["id"],
                    "tenant": self._tenant,
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
                text("SELECT * FROM workflows WHERE id = :id AND tenant = :tenant"),
                {"id": workflow_id, "tenant": self._tenant},
            ).mappings().first()
        return _workflow_from_row(dict(result)) if result else None

    def get_by_name(
        self, name: str, *, environment: object = None
    ) -> Optional[Workflow]:
        from sqlalchemy import text

        wanted = _wanted_env(environment)
        query = "SELECT * FROM workflows WHERE name = :name AND tenant = :tenant"
        params: dict[str, Any] = {"name": name, "tenant": self._tenant}
        if wanted is not None:
            query += " AND environment = :environment"
            params["environment"] = wanted
        query += " ORDER BY created_at LIMIT 1"

        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params).mappings().first()
        return _workflow_from_row(dict(result)) if result else None

    def list(self, *, environment: object = None) -> list[Workflow]:
        from sqlalchemy import text

        wanted = _wanted_env(environment)
        query = "SELECT * FROM workflows WHERE tenant = :tenant"
        params: dict[str, Any] = {"tenant": self._tenant}
        if wanted is not None:
            query += " AND environment = :environment"
            params["environment"] = wanted
        query += " ORDER BY created_at"

        engine = self._get_engine()
        with engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
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
                    WHERE id = :id AND tenant = :tenant
                    """
                ),
                {
                    "id": row["id"],
                    "tenant": self._tenant,
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
                text("DELETE FROM workflows WHERE id = :id AND tenant = :tenant"),
                {"id": workflow_id, "tenant": self._tenant},
            )
            return result.rowcount > 0


def create_workflow_store(
    backend: str, database_url: str, tenant: str = "default"
) -> WorkflowStore:
    """根据配置创建工作流存储实例（按租户隔离）。"""
    if backend == "postgres":
        logger.info("工作流存储后端：postgres（租户 %s）", tenant)
        return PostgresWorkflowStore(database_url, tenant)
    logger.info("工作流存储后端：memory")
    return MemoryWorkflowStore(seed=_seed_workflows())


def _seed_workflows() -> list[Workflow]:
    """内存后端预置示例工作流，与触发注册表保持一致（demo 开箱即用）。"""
    from .workflow import seed_workflows

    return seed_workflows()


_store: Optional[WorkflowStore] = None
_tenant_stores: dict[str, WorkflowStore] = {}
_store_lock = threading.Lock()


def get_workflow_store(tenant: Optional[str] = None) -> WorkflowStore:
    """获取工作流存储（按租户隔离，惰性初始化，后端由配置决定）。

    内存后端每个租户各一份预置模板：两个租户都能看到 ``post_liked``，
    但改其中一个不会影响另一个。Postgres 后端按 ``tenant`` 列过滤。
    """
    from . import config, tenancy

    global _store

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            if _store is None:
                _store = create_workflow_store(
                    config.STORAGE_BACKEND, config.DATABASE_URL, key
                )
            return _store
        store = _tenant_stores.get(key)
        if store is None:
            store = create_workflow_store(config.STORAGE_BACKEND, config.DATABASE_URL, key)
            _tenant_stores[key] = store
        return store


def set_workflow_store(
    store: Optional[WorkflowStore], tenant: Optional[str] = None
) -> None:
    """替换某个租户的工作流存储；传 ``None`` 表示下次取用时按配置重建。"""
    from . import tenancy

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            globals()["_store"] = store
        elif store is None:
            _tenant_stores.pop(key, None)
        else:
            _tenant_stores[key] = store


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
