"""SQLAlchemy ORM 模型：核心实体的 Postgres 表结构。

对应 ROADMAP Phase 1 的建表 DDL。采用 SQLAlchemy 2.0 声明式风格，为后续
Alembic 迁移预留；同时提供一份等价的纯 SQL 迁移脚本（见
``scripts/migrations/002_core_entities.sql``）。

设计约定：
- 主键统一为 ``UUID`` 字符串，业务侧生成，避免依赖数据库序列。
- 时间戳统一 ``TIMESTAMPTZ``（UTC）。
- JSON 字段使用 ``JSONB`` 承载可变结构（payload / config / channels）。
- 状态字段存字符串（对应 ``Message.Status`` 的 ``str`` 值）。
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """所有 ORM 模型的公共基类。"""


class WorkflowModel(Base):
    __tablename__ = "workflows"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    steps: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    environment: Mapped[str] = mapped_column(String(32), nullable=False, default="development")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_utc)


class SubscriberModel(Base):
    __tablename__ = "subscribers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device_token: Mapped[str | None] = mapped_column(String(512), nullable=True)
    channels: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    environment: Mapped[str] = mapped_column(String(32), nullable=False, default="development")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_utc)


class MessageModel(Base):
    __tablename__ = "messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("workflows.id"), nullable=False
    )
    subscriber_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("subscribers.id"), nullable=False
    )
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="created")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now_utc)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now_utc, onupdate=_now_utc
    )


# 常见查询索引：按订阅者、工作流、状态与时间。
Index("idx_messages_subscriber", MessageModel.subscriber_id)
Index("idx_messages_workflow", MessageModel.workflow_id)
Index("idx_messages_status", MessageModel.status)
Index("idx_messages_created_at", MessageModel.created_at)
