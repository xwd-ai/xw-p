"""领域模型层单元测试：Channel 枚举、核心实体、ORM 模型。"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models.channel import Channel
from app.models.db import Base, MessageModel, SubscriberModel, WorkflowModel
from app.models.entities import (
    Environment,
    Message,
    Step,
    Subscriber,
    Workflow,
)


# --- Channel 枚举 ---


def test_channel_values_and_delivery_subset():
    assert Channel.IN_APP.value == "in_app"
    assert Channel.DELAY.value == "delay"
    delivery = Channel.delivery_channels()
    assert Channel.DELAY not in delivery
    assert Channel.DIGEST not in delivery
    assert Channel.EMAIL in delivery


def test_channel_json_serialization():
    import json

    assert json.dumps(Channel.PUSH) == '"push"'


# --- 核心实体 ---


def test_workflow_defaults_and_delivery_steps():
    wf = Workflow(name="post_liked", steps=[Step(type=Channel.EMAIL), Step(type=Channel.DELAY)])
    assert wf.version == 1
    assert wf.environment == Environment.DEVELOPMENT
    assert wf.active is True
    assert [s.type for s in wf.delivery_steps()] == [Channel.EMAIL]


def test_step_is_delivery():
    assert Step(type=Channel.SMS).is_delivery() is True
    assert Step(type=Channel.DIGEST).is_delivery() is False


def test_subscriber_channels_as_enum_list():
    sub = Subscriber(email="a@example.com", channels=[Channel.EMAIL, Channel.IN_APP])
    assert sub.channels == [Channel.EMAIL, Channel.IN_APP]


def test_message_status_default_created():
    msg = Message(workflow_id="w1", subscriber_id="s1", channel=Channel.EMAIL)
    assert msg.status == Message.Status.CREATED
    assert msg.status.value == "created"


def test_message_rejects_invalid_status():
    with pytest.raises(ValidationError):
        Message(workflow_id="w1", subscriber_id="s1", channel=Channel.EMAIL, status="bogus")


def test_workflow_version_ge_one():
    with pytest.raises(ValidationError):
        Workflow(name="x", version=0)


# --- ORM 模型（表结构）---


def test_orm_tables_registered():
    tables = set(Base.metadata.tables)
    assert {"workflows", "subscribers", "workflow_messages"} <= tables


def test_orm_message_columns():
    cols = MessageModel.__table__.columns
    names = {c.name for c in cols}
    assert {
        "id",
        "workflow_id",
        "subscriber_id",
        "channel",
        "payload",
        "status",
        "error",
        "created_at",
        "updated_at",
    } <= names


def test_orm_message_foreign_keys():
    fks = {fk.target_fullname for fk in MessageModel.__table__.foreign_keys}
    assert fks == {"workflows.id", "subscribers.id"}
