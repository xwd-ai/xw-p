"""API 内置消费线程的启停条件。"""
from __future__ import annotations

from app.config import api_worker_enabled


def test_memory_bus_enables_worker_by_default():
    assert api_worker_enabled("memory") is True


def test_redis_bus_disables_worker_by_default():
    # redis 模式下队列由引擎消费，API 再起消费者会抢同一条消息。
    assert api_worker_enabled("redis") is False


def test_explicit_override_wins():
    assert api_worker_enabled("redis", "1") is True
    assert api_worker_enabled("redis", "on") is True
    assert api_worker_enabled("memory", "0") is False
    assert api_worker_enabled("memory", "false") is False


def test_blank_override_falls_back_to_backend():
    assert api_worker_enabled("memory", "   ") is True
    assert api_worker_enabled("redis", "") is False
