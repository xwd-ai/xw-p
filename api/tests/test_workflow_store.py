"""工作流存储层的单元测试：Postgres 适配、行映射与租户隔离。

Postgres 适配器不连真库：这里是单元测试，要验的是「SQL 形状与参数映射
对不对」——租户有没有进 WHERE、环境过滤是不是按需拼接、行怎么还原成实体。
这些逻辑错了在任何数据库上都错，而连真库只能证明「这台机器装了 PG」。
真库上的迁移与并发由集成测试覆盖，两者不互相替代。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app.models.entities import Environment, Step, Workflow
from app.step_validation import StepValidationError
from app.workflow_store import (
    MemoryWorkflowStore,
    PostgresWorkflowStore,
    WorkflowConflictError,
    WorkflowNotFoundError,
    create_workflow_store,
    environment_of,
    validate_workflow,
)


# ---------------------------------------------------------------- 假引擎


class _Result:
    def __init__(self, rows, rowcount: int) -> None:
        self._rows = list(rows)
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _Connection:
    def __init__(self, engine: "_Engine") -> None:
        self._engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self._engine.calls.append((str(statement), dict(params or {})))
        return _Result(self._engine.rows, self._engine.rowcount)


class _Engine:
    """记录每次执行，按预设返回行与 rowcount。"""

    def __init__(self, rows=None, rowcount: int = 1) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.rows = rows or []
        self.rowcount = rowcount

    def begin(self):
        return _Connection(self)

    def connect(self):
        return _Connection(self)


def _store(**kwargs) -> tuple[PostgresWorkflowStore, _Engine]:
    engine = _Engine(**kwargs)
    store = PostgresWorkflowStore("postgresql+psycopg://u:p@localhost/xw", tenant="acme")
    # 直接注入引擎：_get_engine 只在没有缓存时建连接，注入后不会碰真库。
    store._engine = engine
    return store, engine


def _workflow(name="post_liked", environment=Environment.DEVELOPMENT, workflow_id="wf-1") -> Workflow:
    return Workflow(
        id=workflow_id,
        name=name,
        description="帖子被点赞",
        steps=[Step(id="s1", type="in_app", name="站内信", config={"template": "hi"})],
        version=2,
        environment=environment,
    )


def _row(**overrides) -> dict:
    row = {
        "id": "wf-1",
        "name": "post_liked",
        "description": "帖子被点赞",
        "steps": [{"id": "s1", "type": "in_app", "name": "站内信", "config": {}}],
        "version": 3,
        "environment": "production",
        "active": True,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------- 行映射


def test_workflow_from_row_normalizes_steps():
    from app.workflow_store import _workflow_from_row

    workflow = _workflow_from_row(_row())
    assert workflow.id == "wf-1"
    assert workflow.version == 3
    assert environment_of(workflow) == "production"
    assert workflow.steps[0].type.value == "in_app"


def test_workflow_from_row_fills_defaults():
    from app.workflow_store import _workflow_from_row

    workflow = _workflow_from_row({"id": "wf-2", "name": "post_commented"})
    assert workflow.version == 1
    assert workflow.active is True
    assert workflow.steps == []
    assert environment_of(workflow) == "development"


def test_workflow_to_row_serializes_enum_and_steps():
    from app.workflow_store import _workflow_to_row

    row = _workflow_to_row(_workflow(environment=Environment.PRODUCTION))
    assert row["environment"] == "production"
    assert row["steps"] == [{"id": "s1", "type": "in_app", "name": "站内信", "config": {"template": "hi"}}]


def test_wanted_env_normalizes():
    from app.workflow_store import _wanted_env

    assert _wanted_env(None) is None
    assert _wanted_env("  ") is None
    assert _wanted_env(Environment.PRODUCTION) == "production"
    assert _wanted_env(" PRODUCTION ") == "production"


# ---------------------------------------------------------------- Postgres 适配


def test_postgres_create_writes_tenant_and_json_steps():
    store, engine = _store()
    store.create(_workflow(environment=Environment.PRODUCTION))

    sql, params = engine.calls[0]
    assert "INSERT INTO workflows" in sql
    assert params["tenant"] == "acme"
    assert params["environment"] == "production"
    assert params["version"] == 2
    # steps 以 JSON 文本落库：驱动不认 Python 对象
    assert json.loads(params["steps"])[0]["type"] == "in_app"


def test_postgres_get_filters_by_tenant_and_maps_row():
    store, engine = _store(rows=[_row()])
    workflow = store.get("wf-1")

    assert workflow is not None and workflow.version == 3
    sql, params = engine.calls[0]
    assert "tenant = :tenant" in sql
    assert params == {"id": "wf-1", "tenant": "acme"}


def test_postgres_get_missing_returns_none():
    store, _ = _store(rows=[])
    assert store.get("nope") is None


def test_postgres_get_by_name_adds_environment_filter_only_when_given():
    store, engine = _store(rows=[_row()])
    store.get_by_name("post_liked", environment=Environment.PRODUCTION)
    sql, params = engine.calls[0]
    assert "environment = :environment" in sql
    assert params["environment"] == "production"

    store.get_by_name("post_liked")
    sql, params = engine.calls[1]
    # 不指定环境 = 不限环境，不能悄悄按默认环境过滤
    assert "environment = :environment" not in sql
    assert "environment" not in params


def test_postgres_list_orders_and_filters():
    store, engine = _store(rows=[_row(), _row(id="wf-2")])
    items = store.list(environment="production")
    assert [w.id for w in items] == ["wf-1", "wf-2"]

    sql, params = engine.calls[0]
    assert "ORDER BY created_at" in sql
    assert params["environment"] == "production"


def test_postgres_update_raises_when_no_row():
    store, _ = _store(rowcount=0)
    with pytest.raises(WorkflowNotFoundError):
        store.update(_workflow())


def test_postgres_update_returns_workflow():
    store, engine = _store(rowcount=1)
    workflow = store.update(_workflow())
    assert workflow.name == "post_liked"
    sql, params = engine.calls[0]
    assert "UPDATE workflows" in sql
    assert params["tenant"] == "acme"


def test_postgres_delete_follows_rowcount():
    store, _ = _store(rowcount=1)
    assert store.delete("wf-1") is True
    store, _ = _store(rowcount=0)
    assert store.delete("wf-1") is False


def test_postgres_delete_statement_carries_tenant():
    store, engine = _store(rowcount=1)
    store.delete("wf-1")
    sql, params = engine.calls[0]
    assert "DELETE FROM workflows" in sql
    assert params == {"id": "wf-1", "tenant": "acme"}


# ---------------------------------------------------------------- 内存后端与工厂


def test_memory_store_name_conflict_is_per_environment():
    store = MemoryWorkflowStore()
    store.create(_workflow(environment=Environment.DEVELOPMENT))
    # 同一环境重名：冲突
    with pytest.raises(WorkflowConflictError):
        store.create(_workflow(environment=Environment.DEVELOPMENT))
    # 另一套环境：允许
    store.create(_workflow(environment=Environment.PRODUCTION, workflow_id="wf-2"))
    assert len(store.list()) == 2
    assert len(store.list(environment="production")) == 1


def test_memory_store_update_missing_raises():
    store = MemoryWorkflowStore()
    with pytest.raises(WorkflowNotFoundError):
        store.update(_workflow())


def test_memory_store_delete_reports_presence():
    store = MemoryWorkflowStore(seed=[_workflow()])
    assert store.delete("wf-1") is True
    assert store.delete("wf-1") is False


def test_memory_store_get_by_name_respects_environment():
    store = MemoryWorkflowStore()
    store.create(_workflow(environment=Environment.DEVELOPMENT))
    store.create(_workflow(environment=Environment.PRODUCTION, workflow_id="wf-2"))

    assert environment_of(store.get_by_name("post_liked", environment="production")) == "production"
    assert environment_of(store.get_by_name("post_liked", environment="development")) == "development"
    assert store.get_by_name("post_liked") is not None


def test_create_workflow_store_selects_backend():
    memory = create_workflow_store("jsonl", "unused")
    assert isinstance(memory, MemoryWorkflowStore)
    # 内存后端带预置模板：demo 起来就能触发
    assert memory.get_by_name("post_liked", environment="development") is not None

    postgres = create_workflow_store("postgres", "postgresql+psycopg://u:p@localhost/xw", "acme")
    assert isinstance(postgres, PostgresWorkflowStore)


def test_validate_workflow_rejects_blank_name():
    workflow = _workflow(name="   ")
    with pytest.raises(ValueError):
        validate_workflow(workflow)


def test_validate_workflow_rejects_duplicate_step_ids():
    workflow = Workflow(
        name="post_liked",
        steps=[Step(id="s1", type="in_app"), Step(id="s1", type="email")],
    )
    with pytest.raises(StepValidationError):
        validate_workflow(workflow)
