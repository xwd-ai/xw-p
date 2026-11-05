"""幂等键的并发正确性：同键并发只入队一次，异键并发不误伤。

三组用例按「离线上限」到「真链路」排列：

1. 进程内多线程打同一个接口——验证占位与入队之间没有可乘之隙；
2. 多个独立进程各自持有一个 Redis 客户端占同一个键——验证裁决点是
   ``SET NX``，而不是某个进程内的锁；
3. 两个 uvicorn 实例共享同一份 Redis，同键请求分散打过去——验证真实部署
   形态下队列里只多出一条消息。

第三组会起进程，因此把环境变量、临时数据目录与清理都交给
``scripts/idem_stress.py``：单独跑那份脚本也能得到同样的结论。
"""
from __future__ import annotations

import importlib.util
import json
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import config
from app.bus import MemoryMessageBus
from app.idempotency import MemoryIdempotencyStore
from app.main import app

ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "scripts" / "idem_stress.py"


def _load_harness():
    """按路径加载压测脚本；注册进 ``sys.modules`` 以便 dataclass 解析注解。"""
    spec = importlib.util.spec_from_file_location("idem_stress", HARNESS)
    module = importlib.util.module_from_spec(spec)
    sys.modules["idem_stress"] = module
    spec.loader.exec_module(module)
    return module


stress = _load_harness()


# --------------------------------------------------------------------------
# 进程内：多个线程同时打同一个接口
# --------------------------------------------------------------------------


class _ActivityStore:
    """记录活动日志条数：同键并发只该产生一条。"""

    def __init__(self) -> None:
        self.records: list[dict] = []
        self._lock = threading.Lock()

    def append(self, record: dict) -> None:
        with self._lock:
            self.records.append(record)

    def append_many(self, records: list[dict]) -> int:
        with self._lock:
            self.records.extend(records)
        return len(records)

    def read_all(self) -> list[dict]:
        return list(self.records)


@pytest.fixture
def wired(monkeypatch):
    """内存总线 + 内存幂等 + 计数活动存储，避免污染全局单例。"""
    import app.activity_log as al_mod
    import app.bus as bus_mod
    import app.idempotency as idem_mod

    bus = MemoryMessageBus()
    activity = _ActivityStore()
    monkeypatch.setattr(bus_mod, "_bus", bus)
    monkeypatch.setattr(idem_mod, "_store", MemoryIdempotencyStore())
    monkeypatch.setattr(al_mod, "_activity_store", activity)
    monkeypatch.setattr(config, "IDEMPOTENCY_TTL", 86400)
    return TestClient(app), bus, activity


def _body(name: str = "post_liked", subscriber: str = "creator_1001") -> dict:
    return {
        "name": name,
        "to": {"subscriberId": subscriber},
        "payload": {"post_id": "P-2026-1001", "post_title": "手写 SVG 工作流编辑器", "actor": "小林"},
    }


def test_same_key_concurrent_requests_enqueue_once(wired):
    client, bus, activity = wired
    key = "post-liked-P-2026-1001"
    results: list[tuple[int, str]] = []
    lock = threading.Lock()

    def call() -> None:
        resp = client.post("/v1/events/trigger", json=_body(), headers={"Idempotency-Key": key})
        with lock:
            results.append((resp.status_code, resp.json().get("triggerId", "")))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: call(), range(32)))

    assert [status for status, _ in results] == [200] * 32
    # 32 个并发请求只应产出 1 个触发标识、1 条入队消息、1 条活动日志。
    assert {trigger_id for _, trigger_id in results} == {results[0][1]}
    assert bus.size() == 1
    assert len(activity.records) == 1


def test_distinct_keys_concurrent_requests_all_enqueued(wired):
    client, bus, activity = wired

    def call(index: int) -> int:
        resp = client.post(
            "/v1/events/trigger",
            json=_body(),
            headers={"Idempotency-Key": f"post-liked-P-2026-1001-{index}"},
        )
        return resp.status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        statuses = list(pool.map(call, range(24)))

    assert statuses == [200] * 24
    assert bus.size() == 24
    assert len({record["trigger_id"] for record in activity.records}) == 24


def test_batch_same_key_concurrent_requests_enqueue_once(wired):
    client, bus, activity = wired
    key = "post-liked-P-2026-1001-burst-1"

    def call() -> tuple[int, int]:
        resp = client.post(
            "/v1/events/trigger/batch",
            json={"events": [_body(), _body(subscriber="creator_1002")]},
            headers={"Idempotency-Key": key},
        )
        data = resp.json()
        return resp.status_code, data["accepted"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _: call(), range(16)))

    assert outcomes == [(200, 2)] * 16
    # 整批 2 条消息只入队一次。
    assert bus.size() == 2
    assert len(activity.records) == 2


def _unreadable_store(monkeypatch):
    """把默认租户的幂等存储换成一个「写不进、也读不出」的存储。"""
    import app.idempotency as idem_mod

    class _Store:
        def get(self, key: str):
            return None

        def put_if_absent(self, key: str, value: dict, ttl: int) -> bool:
            return False

    monkeypatch.setattr(idem_mod, "_store", _Store())


def test_conflict_returns_409_without_enqueue(wired, monkeypatch):
    """键已占用却读不到回执：409，且消息没有进队列。"""
    client, bus, activity = wired
    key = "comment-C-8842"
    _unreadable_store(monkeypatch)

    resp = client.post("/v1/events/trigger", json=_body(), headers={"Idempotency-Key": key})

    assert resp.status_code == 409
    assert key in resp.json()["detail"]
    assert bus.size() == 0
    assert activity.records == []


def test_conflict_on_batch_returns_409(wired, monkeypatch):
    """整批同样是「读不到回执就拒绝」，不会因为整批语义就放行一次重复入队。"""
    client, bus, activity = wired
    _unreadable_store(monkeypatch)

    batch = client.post(
        "/v1/events/trigger/batch",
        json={"events": [_body()]},
        headers={"Idempotency-Key": "burst-409"},
    )
    assert batch.status_code == 409
    assert bus.size() == 0
    assert activity.records == []


# --------------------------------------------------------------------------
# 跨进程：每个进程一个 Redis 客户端，同时占同一个键
# --------------------------------------------------------------------------

_WORKER = """
import json, sys, time
from app.idempotency import RedisIdempotencyStore, claim

url, key, start_at = sys.argv[1], sys.argv[2], float(sys.argv[3])
store = RedisIdempotencyStore(url)
while time.time() < start_at:
    pass
reserved = claim(store, key, {"triggerId": "trg-" + key}, 60)
print("claimed" if reserved.claimed else "replay")
"""


def _redis_binary() -> str:
    return subprocess.run(
        ["sh", "-c", "command -v redis-server"], capture_output=True, text=True
    ).stdout.strip()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def redis_server(tmp_path: Path):
    binary = _redis_binary()
    if not binary:
        pytest.skip("本机没有 redis-server")
    port = _free_port()
    process = subprocess.Popen(
        [binary, "--port", str(port), "--save", "", "--appendonly", "no", "--dir", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"redis://127.0.0.1:{port}/0"
    try:
        import redis

        client = redis.Redis.from_url(url, socket_timeout=2.0)
        deadline = time.monotonic() + 5
        while True:
            try:
                client.ping()
                client.close()
                break
            except Exception:  # noqa: BLE001 —— 启动中的连接失败是预期
                if time.monotonic() > deadline:
                    pytest.skip("redis-server 未能启动")
                time.sleep(0.05)
        yield url
    finally:
        process.terminate()
        process.wait(timeout=5)


def test_single_winner_across_processes(redis_server):
    """4 个进程各自建一个 Redis 客户端，同一时刻抢同一个键：只有 1 个成功。"""
    key = f"proc-race-{time.time_ns()}"
    api_dir = ROOT / "api"
    start_at = time.time() + 1.5  # 给 4 个解释器留出导入时间
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER, redis_server, key, repr(start_at)],
            cwd=str(api_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    try:
        outcomes = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=30)
            assert process.returncode == 0, stderr
            outcomes.append(stdout.strip())
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()

    # 恰好一个进程「首次写入」成功，其余都读到同一份回执。
    assert outcomes.count("claimed") == 1, outcomes
    assert outcomes.count("replay") == 3, outcomes


# --------------------------------------------------------------------------
# 多实例：两个 uvicorn 共享同一份 Redis，同键请求分散打过去
# --------------------------------------------------------------------------


def _activity_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def test_two_instances_enqueue_once(redis_server, tmp_path: Path):
    """真实部署形态：两个实例、同一套 Redis，200 个同键请求只入队 1 条。"""
    workdir = tmp_path / "stress"
    workdir.mkdir()
    queue_key = f"xw:queue:test:{time.time_ns()}"

    instances = stress.launch_instances(
        2, redis_url=str(redis_server), queue_key=queue_key, workdir=workdir
    )
    try:
        report = stress.run(
            [instance.url for instance in instances],
            requests=60,
            concurrency=24,
            redis_url=str(redis_server),
            queue_key=queue_key,
            key=f"multi-{time.time_ns()}",
            distinct_round=True,
        )
    finally:
        stress.stop_instances(instances)

    assert report.problems() == []
    assert report.same_key.statuses == {200: 60}
    assert report.same_key.distinct_trigger_ids == 1
    assert report.queued_after_same_key == 1
    # 异键轮反向验证：去重没有过头，60 条各入一条。
    assert report.distinct_keys is not None
    assert report.queued_after_distinct_keys == 60
    # 活动日志落在临时目录，而不是本地 demo 数据里。
    rows = _activity_lines(workdir / "activity.jsonl")
    assert len(rows) == 61
    assert len({row["trigger_id"] for row in rows}) == 61
