"""Phase 1 集成测试：贯穿触发 → 队列 → worker → 活动流 → 执行明细 → 收件箱。

通过 FastAPI TestClient 走完整 HTTP 链路，验证 Phase 1 各组件协作一致：
1. 触发一条社区互动事件（post_commented），拿到回执。
2. worker 消费队列，逐渠道落库消息并写执行明细。
3. 活动流可见「已触发」；执行明细按渠道/状态/triggerId 可查。
4. 收件箱对 In-app 渠道可见未读，标记已读后未读归零。

所有单例（bus / storage / activity / execution / read_state / idempotency）
均用 monkeypatch 换成内存/临时实现，避免污染全局与真实 data/*.jsonl。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import config
from app.activity_log import MemoryActivityStore
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app


class _InMemoryListStore:
    """进程内 append/read_all 存储，替代 JSONL 后端。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._records: list[dict] = []

    def append(self, record: dict) -> None:
        with self._lock:
            self._records.append(record)

    def save(self, record: dict) -> None:
        """消息投递后端（storage）的接口名，与 append 语义一致。"""
        self.append(record)

    def read_all(self) -> list[dict]:
        with self._lock:
            return list(self._records)

    def query(self, q):
        """执行明细查询接口（与真实后端同一套过滤/游标语义）。"""
        from app.execution_log import MemoryExecutionStore

        return MemoryExecutionStore(self.read_all()).query(q)

    def stats(self, trend_days: int = 7) -> dict:
        """与真实后端同口径的统计聚合（仪表盘接口用）。"""
        from app.stats import compute_stats

        return compute_stats(self.read_all(), trend_days)


class _InMemoryReadState:
    """进程内已读状态存储，替代 JSONL ReadStateStore。

    与真实实现同样按「消息 ID + 已读时刻」记录：内容更新时间晚于已读时刻时
    重新算未读（聚合卡片被新互动改写后的行为）。
    """

    def __init__(self):
        self._read: dict[str, dict[str, datetime]] = {}
        self._versions: dict[str, int] = {}

    def is_read(self, subscriber_id: str, message_id: str, updated_at=None) -> bool:
        read_at = (self._read.get(subscriber_id) or {}).get(message_id)
        if read_at is None:
            return False
        return updated_at is None or read_at >= updated_at

    def mark_read(self, subscriber_id: str, message_id: str, updated_at=None) -> bool:
        bucket = self._read.setdefault(subscriber_id, {})
        read_at = bucket.get(message_id)
        if read_at is not None and (updated_at is None or read_at >= updated_at):
            return False
        bucket[message_id] = datetime.now(timezone.utc)
        self._versions[subscriber_id] = self._versions.get(subscriber_id, 0) + 1
        return True

    def revision(self, subscriber_id: str) -> int:
        return self._versions.get(subscriber_id, 0)

    def is_legacy_read(self, subscriber_id: str, trigger_id: str) -> bool:
        return False


@pytest.fixture
def client(monkeypatch):
    """隔离全部全局单例为进程内实现，返回 TestClient。

    关键点：worker 通过 ``get_bus()``/``get_storage()``/``record_execution``
    读取同一套被替换的单例，因此 worker 消费后，查询 API 能看到结果。
    """
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.execution_log as ex_mod
    import app.idempotency as idem_mod
    import app.inbox as inbox_mod
    import app.storage as storage_mod

    bus = MemoryMessageBus()
    storage = _InMemoryListStore()
    activity = MemoryActivityStore()
    execution = _InMemoryListStore()
    read_state = _InMemoryReadState()
    idem = MemoryIdempotencyStore()

    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(storage_mod, "_storage", storage)
    monkeypatch.setattr(al_mod, "_activity_store", activity)
    monkeypatch.setattr(ex_mod, "_execution_store", execution)
    monkeypatch.setattr(inbox_mod, "_read_store", read_state)
    monkeypatch.setattr(idem_mod, "_store", idem)

    monkeypatch.setattr(config, "BUS_BACKEND", "memory")
    monkeypatch.setattr(config, "STORAGE_BACKEND", "jsonl")
    monkeypatch.setattr(config, "IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)

    # 返回 (client, bus) 以便手动驱动 worker 消费（TestClient 的 lifespan
    # 不在测试中启动后台线程，显式消费保证确定性）。
    return TestClient(app), bus


def _drain_worker(bus, worker):
    """手动驱动 worker 消费队列直至清空（测试内确定性执行）。"""
    while True:
        msg = bus.consume(timeout=0.1)
        if msg is None:
            break
        worker._process(msg)


def _trigger(client, name="post_commented", subscriber="creator_1001", channels=None, **extra):
    if channels is None:
        channels = {"post_liked": ["in_app", "push"],
                    "post_commented": ["in_app", "push", "email"],
                    "post_mentioned": ["in_app", "push"]}[name]
    body = {
        "name": name,
        "to": {"subscriberId": subscriber, "channels": channels},
        "payload": {"post_id": "P-2026-1001", "post_title": "我的第一支 Vlog", "actor": "小林"},
    }
    body.update(extra)
    return client.post("/v1/events/trigger", json=body)


def test_full_pipeline_trigger_to_inbox(client):
    """端到端：触发评论事件 → worker 消费 → 活动流/执行明细/收件箱全可见。"""
    from app.worker import Worker

    c, bus = client
    worker = Worker()

    # 1. 触发 post_commented（站内信 + 推送 + 邮件）
    resp = _trigger(c)
    assert resp.status_code == 200
    receipt = resp.json()
    assert receipt["acknowledged"] is True
    trigger_id = receipt["triggerId"]

    # 2. 触发前活动流已可见「已触发」
    r = c.get("/v1/activity", params={"workflow": "post_commented"})
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["trigger_id"] == trigger_id
    assert items[0]["status"] == "triggered"
    assert items[0]["subscriber_id"] == "creator_1001"

    # 3. worker 消费队列，落库并写执行明细
    _drain_worker(bus, worker)

    # 4. 执行明细：post_commented 三条渠道（in_app / push / email）
    r = c.get("/v1/executions", params={"triggerId": trigger_id})
    assert r.status_code == 200
    exec_items = r.json()["items"]
    assert len(exec_items) == 3
    channels = sorted(i["channel"] for i in exec_items)
    assert channels == ["email", "in_app", "push"]
    assert all(i["status"] == "sent" for i in exec_items)

    # 5. 执行明细按渠道过滤
    r = c.get("/v1/executions", params={"triggerId": trigger_id, "channel": "email"})
    assert r.status_code == 200
    assert len(r.json()["items"]) == 1
    assert r.json()["items"][0]["channel"] == "email"

    # 6. 收件箱：In-app 渠道未读 = 1
    r = c.get("/v1/inbox", params={"subscriberId": "creator_1001"})
    assert r.status_code == 200
    inbox = r.json()
    assert inbox["unread"] == 1
    assert inbox["total"] == 1
    assert inbox["items"][0]["trigger_id"] == trigger_id
    assert inbox["items"][0]["workflow"] == "post_commented"

    # 7. 标记已读后未读归零
    r = c.post(
        "/v1/inbox/read",
        json={"subscriberId": "creator_1001", "triggerIds": [trigger_id]},
    )
    assert r.status_code == 200
    assert r.json()["unread"] == 0
    assert r.json()["marked"] == 1

    # 8. 再次查询收件箱，该条已读
    r = c.get("/v1/inbox", params={"subscriberId": "creator_1001"})
    assert r.json()["unread"] == 0
    assert r.json()["items"][0]["read"] is True


def test_pipeline_post_liked_multichannel(client):
    """post_liked（站内信 + 推送）走完整链路，执行明细为两条渠道。"""
    from app.worker import Worker

    c, bus = client
    worker = Worker()

    resp = _trigger(c, name="post_liked", subscriber="creator_2002",
                    payload={"post_id": "P-2026-2002", "actor": "阿澈", "count": 128})
    assert resp.status_code == 200
    trigger_id = resp.json()["triggerId"]

    _drain_worker(bus, worker)

    r = c.get("/v1/executions", params={"triggerId": trigger_id})
    channels = sorted(i["channel"] for i in r.json()["items"])
    assert channels == ["in_app", "push"]


def test_environment_isolation(client):
    """production 环境无法触发 development 环境的工作流（环境隔离）。"""
    c, bus = client
    resp = c.post(
        "/v1/events/trigger",
        json={
            "name": "post_commented",
            "to": {"subscriberId": "creator_1"},
            "payload": {"post_id": "P-1"},
        },
        headers={"X-Environment": "production"},
    )
    # 预置工作流均为 development，production 下应 404
    assert resp.status_code == 404


def test_idempotency_within_pipeline(client):
    """同一幂等键重复触发，worker 消费后仅产生一份投递与执行明细。"""
    from app.worker import Worker

    c, bus = client
    worker = Worker()
    headers = {"Idempotency-Key": "post-commented-P-2026-1001"}

    r1 = _trigger(c)
    r2 = c.post(
        "/v1/events/trigger",
        json={
            "name": "post_commented",
            "to": {"subscriberId": "creator_1001", "channels": ["in_app", "push", "email"]},
            "payload": {"post_id": "P-2026-1001"},
        },
        headers=headers,
    )
    # 首次（无幂等键）+ 带幂等键一次：两条应入队；但带幂等键重复两次应去重
    r3 = c.post(
        "/v1/events/trigger",
        json={
            "name": "post_commented",
            "to": {"subscriberId": "creator_1001", "channels": ["in_app", "push", "email"]},
            "payload": {"post_id": "P-2026-1001"},
        },
        headers=headers,
    )
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r3.status_code == 200
    # 幂等键重复：r2 与 r3 回执一致
    assert r2.json()["triggerId"] == r3.json()["triggerId"]

    _drain_worker(bus, worker)

    # 执行明细应只有 1（幂等命中）+ 1（首次）= 2 次触发 × 3 渠道 = 6 条
    r = c.get("/v1/executions")
    assert r.json()["total"] == 6


def test_health_reflects_queued(client):
    """健康检查反映当前队列长度。"""
    c, bus = client
    # 触发后未消费前，队列应 > 0
    _trigger(c)
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    assert r.json()["queued"] >= 1
