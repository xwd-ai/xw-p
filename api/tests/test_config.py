"""API 内置消费线程的启停条件与存储后端的运行环境约束。"""
from __future__ import annotations

from app.config import api_worker_enabled, storage_warning


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


def test_storage_warning_flags_jsonl_in_production():
    hint = storage_warning("jsonl", "production")
    assert hint is not None
    assert "postgres" in hint


def test_storage_warning_silent_for_supported_combinations():
    # 默认的 jsonl + development 是开箱即用的演示组合，不该报错
    assert storage_warning("jsonl", "development") is None
    assert storage_warning("postgres", "production") is None
    assert storage_warning("postgres", "development") is None
