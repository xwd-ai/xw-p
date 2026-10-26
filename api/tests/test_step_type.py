"""StepType 枚举测试（ROADMAP Phase 2）。"""
import pytest

from app.models.channel import Channel
from app.models.step_type import StepType


def test_all_seven_types():
    """枚举覆盖 in_app / email / sms / push / chat / delay / digest。"""
    assert set(StepType) == {
        StepType.IN_APP,
        StepType.EMAIL,
        StepType.SMS,
        StepType.PUSH,
        StepType.CHAT,
        StepType.DELAY,
        StepType.DIGEST,
    }
    assert {t.value for t in StepType} == {
        "in_app",
        "email",
        "sms",
        "push",
        "chat",
        "delay",
        "digest",
    }


def test_delivery_and_orchestration_partition():
    """投递类型与编排类型互斥且覆盖全集。"""
    delivery = StepType.delivery_types()
    orchestration = StepType.orchestration_types()
    assert set(delivery) == {
        StepType.IN_APP,
        StepType.EMAIL,
        StepType.SMS,
        StepType.PUSH,
        StepType.CHAT,
    }
    assert set(orchestration) == {StepType.DELAY, StepType.DIGEST}
    assert set(delivery).isdisjoint(orchestration)
    assert set(delivery) | set(orchestration) == set(StepType)


def test_is_delivery():
    assert StepType.EMAIL.is_delivery() is True
    assert StepType.IN_APP.is_delivery() is True
    assert StepType.DELAY.is_delivery() is False
    assert StepType.DIGEST.is_delivery() is False


def test_channel_mapping_delivery():
    """投递步骤类型映射到同名渠道。"""
    assert StepType.EMAIL.channel() is Channel.EMAIL
    assert StepType.SMS.channel() is Channel.SMS
    assert StepType.PUSH.channel() is Channel.PUSH
    assert StepType.IN_APP.channel() is Channel.IN_APP
    assert StepType.CHAT.channel() is Channel.CHAT


def test_channel_mapping_orchestration_raises():
    """编排步骤无投递渠道，映射时抛出 ValueError。"""
    for t in (StepType.DELAY, StepType.DIGEST):
        with pytest.raises(ValueError):
            t.channel()


def test_from_channel_roundtrip():
    """投递渠道 → 步骤类型可无损往返。"""
    for c in Channel.delivery_channels():
        assert StepType.from_channel(c).channel() is c


def test_parse_known_and_unknown():
    assert StepType.parse("email") is StepType.EMAIL
    assert StepType.parse("digest") is StepType.DIGEST
    with pytest.raises(ValueError):
        StepType.parse("not_a_step")  # 不在枚举内的类型
