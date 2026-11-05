#!/usr/bin/env python3
"""幂等键并发正确性压测：同键并发只入队一次。

两轮请求打在同一个（或同一组）触发接口上：

1. **同键轮**：N 个请求携带同一个 ``Idempotency-Key``，均匀分散到各实例。
   正确的实现只应有一个请求真正入队，其余请求拿到同一份回执——回执里的
   触发标识必须完全相同。
2. **异键轮**：同样是 N 个并发请求，但每个请求一个键。这一轮用来反向验证
   去重没有过头：每条都该入队，键之间不该互相顶掉。

结论按「队列里实际多出几条消息」判定，而不是只看回执：回执是服务端自己写的，
队列长度才是投递链路真正会发生的事。队列长度直接读 Redis（``LLEN``），因此
脚本要求总线是 Redis 后端。

用法::

    # 自己起 2 个实例（需要本机有 redis-server，临时实例用完即关）
    python scripts/idem_stress.py --instances 2 --requests 200 --concurrency 32

    # 打已有部署（不给 --redis 时跳过队列长度核对，只看回执）
    python scripts/idem_stress.py --base-url http://127.0.0.1:8000 --redis redis://127.0.0.1:6379/0
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
API_DIR = ROOT / "api"

#: 压测用的工作流：预置的社区互动模板，进程起来就能触发。
DEFAULT_WORKFLOW = "post_liked"
DEFAULT_SUBSCRIBER = "creator-1001"


def free_port() -> int:
    """取一个当前空闲的端口。"""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _clean_env() -> dict[str, str]:
    """子进程环境：清掉继承来的 XW_* 与代理配置。

    代理是这里的常见坑——本机设了 ``HTTP_PROXY`` 时，连 ``127.0.0.1`` 的请求
    也会被送去代理，表现为「服务没起来」，而服务其实好好的。
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("XW_")}
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    env["PYTHONUNBUFFERED"] = "1"
    return env


# --------------------------------------------------------------------------
# Redis：临时实例与队列长度读取
# --------------------------------------------------------------------------


class RedisHandle:
    """临时 redis-server 实例（用完即关）。"""

    def __init__(self, workdir: Path) -> None:
        self._workdir = workdir
        self._process: Optional[subprocess.Popen] = None
        self.url = ""

    def start(self) -> "RedisHandle":
        binary = subprocess.run(
            ["sh", "-c", "command -v redis-server"], capture_output=True, text=True
        ).stdout.strip()
        if not binary:
            raise RuntimeError("本机没有 redis-server，请用 --redis 指定一个可用实例")
        port = free_port()
        self._process = subprocess.Popen(
            [binary, "--port", str(port), "--save", "", "--appendonly", "no", "--dir", str(self._workdir)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.url = f"redis://127.0.0.1:{port}/0"
        self._wait_ready()
        return self

    def _wait_ready(self, timeout: float = 5.0) -> None:
        import redis

        client = redis.Redis.from_url(self.url, socket_timeout=1.0)
        deadline = time.monotonic() + timeout
        while True:
            try:
                client.ping()
                client.close()
                return
            except Exception:  # noqa: BLE001 —— 启动中的连接失败是预期
                if time.monotonic() > deadline:
                    raise RuntimeError("redis-server 启动超时") from None
                time.sleep(0.05)

    def stop(self) -> None:
        if self._process is None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover —— 极少发生
            self._process.kill()
        self._process = None


def queue_depth(redis_url: str, queue_key: str) -> int:
    """队列里待消费的消息条数（总线用 ``LPUSH`` 入队）。"""
    import redis

    client = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=2.0)
    try:
        return int(client.llen(queue_key))
    finally:
        client.close()


def drop_keys(redis_url: str, keys: Iterable[str]) -> None:
    """清理本次压测写入的键（只删自己生成的，不碰别人）。"""
    import redis

    client = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=2.0)
    try:
        pending = [key for key in keys if key]
        if pending:
            client.delete(*pending)
    finally:
        client.close()


# --------------------------------------------------------------------------
# 实例：自起 uvicorn 或直接打已有部署
# --------------------------------------------------------------------------


@dataclass
class Instance:
    """一个 API 实例：地址 + 子进程（外部部署时为空）。"""

    url: str
    process: Optional[subprocess.Popen] = None
    log_path: Optional[Path] = None

    def log_tail(self, limit: int = 40) -> str:
        if self.log_path is None or not self.log_path.exists():
            return ""
        lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-limit:])


def instance_env(
    *,
    redis_url: str,
    queue_key: str,
    activity_file: Path,
) -> dict[str, str]:
    """单个实例的环境：Redis 总线 + Redis 幂等 + 不内置消费线程。

    内置消费线程必须关掉：它会把消息从队列里拿走，``LLEN`` 就数不到入队条数，
    而这里要验的正是「入队几条」。
    """
    env = _clean_env()
    env.update(
        {
            "XW_BUS": "redis",
            "XW_REDIS_URL": redis_url,
            "XW_REDIS_QUEUE_KEY": queue_key,
            "XW_IDEMPOTENCY": "redis",
            "XW_API_WORKER": "0",
            "XW_STORAGE": "jsonl",
            "XW_ACTIVITY_FILE": str(activity_file),
        }
    )
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(API_DIR) + (os.pathsep + existing if existing else "")
    return env


def launch_instances(
    count: int,
    *,
    redis_url: str,
    queue_key: str,
    workdir: Path,
    python: Optional[str] = None,
) -> list[Instance]:
    """启动 ``count`` 个 uvicorn 实例（各自一个进程，互不共享内存）。"""
    python = python or sys.executable
    instances: list[Instance] = []
    try:
        for index in range(count):
            port = free_port()
            log_path = workdir / f"instance-{index}.log"
            handle = log_path.open("wb")
            process = subprocess.Popen(
                [
                    python,
                    "-m",
                    "uvicorn",
                    "app.main:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--log-level",
                    "warning",
                ],
                cwd=str(API_DIR),
                env=instance_env(
                    redis_url=redis_url,
                    queue_key=queue_key,
                    activity_file=workdir / "activity.jsonl",
                ),
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
            handle.close()
            instances.append(
                Instance(url=f"http://127.0.0.1:{port}", process=process, log_path=log_path)
            )
        wait_ready(instances)
    except BaseException:
        stop_instances(instances)
        raise
    return instances


def wait_ready(instances: list[Instance], timeout: float = 30.0) -> None:
    """等所有实例的 ``/health`` 返回 200；进程先退出时立刻报错并带上日志。"""
    deadline = time.monotonic() + timeout
    for instance in instances:
        while True:
            process = instance.process
            if process is not None and process.poll() is not None:
                raise RuntimeError(
                    f"实例 {instance.url} 启动即退出（code={process.returncode}）：\n"
                    f"{instance.log_tail()}"
                )
            try:
                with _opener().open(f"{instance.url}/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except (urllib.error.URLError, OSError):
                pass
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"实例 {instance.url} 未在 {timeout:.0f}s 内就绪：\n{instance.log_tail()}"
                )
            time.sleep(0.05)


def stop_instances(instances: list[Instance]) -> None:
    for instance in instances:
        process = instance.process
        if process is None or process.poll() is not None:
            continue
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover
            process.kill()
            process.wait(timeout=5)


# --------------------------------------------------------------------------
# 请求
# --------------------------------------------------------------------------


def _opener() -> urllib.request.OpenerDirector:
    """不走代理的 opener：本机代理会把 127.0.0.1 也代理走。"""
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def trigger_body(workflow: str, subscriber: str) -> dict[str, Any]:
    return {
        "name": workflow,
        "to": {"subscriberId": subscriber},
        "payload": {
            "post_id": "P-2026-1001",
            "post_title": "手写 SVG 工作流编辑器",
            "actor": "小林",
        },
    }


def _post(url: str, body: bytes, key: str, timeout: float) -> tuple[int, dict[str, Any] | str]:
    request = urllib.request.Request(
        f"{url}/v1/events/trigger",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
    )
    try:
        with _opener().open(request, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:  # 4xx/5xx 也是结论，不是异常
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw
    except (urllib.error.URLError, OSError) as exc:
        return 0, f"连接失败：{exc}"


@dataclass
class FireResult:
    """一轮并发的观测结果。"""

    statuses: dict[int, int] = field(default_factory=dict)
    trigger_ids: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    elapsed: float = 0.0
    requests: int = 0

    @property
    def distinct_trigger_ids(self) -> int:
        return len(set(self.trigger_ids))

    def record(self, status: int, payload: dict[str, Any] | str) -> None:
        self.requests += 1
        self.statuses[status] = self.statuses.get(status, 0) + 1
        if status == 200 and isinstance(payload, dict):
            trigger_id = payload.get("triggerId")
            if isinstance(trigger_id, str) and trigger_id:
                self.trigger_ids.append(trigger_id)
                return
        detail = payload.get("detail") if isinstance(payload, dict) else payload
        if len(self.failures) < 5:
            self.failures.append(f"HTTP {status}: {detail}")


def fire(
    urls: list[str],
    *,
    keys: list[str],
    workflow: str = DEFAULT_WORKFLOW,
    subscriber: str = DEFAULT_SUBSCRIBER,
    concurrency: int = 16,
    timeout: float = 10.0,
) -> FireResult:
    """并发发出请求，``keys[i]`` 与 ``urls[i % len(urls)]`` 对应第 i 条。"""
    body = json.dumps(trigger_body(workflow, subscriber), ensure_ascii=False).encode("utf-8")
    result = FireResult()
    lock = threading.Lock()
    started = time.monotonic()

    def worker(index: int) -> None:
        status, payload = _post(urls[index % len(urls)], body, keys[index], timeout)
        with lock:
            result.record(status, payload)

    with ThreadPoolExecutor(max_workers=max(1, min(concurrency, len(keys)))) as pool:
        list(pool.map(worker, range(len(keys))))
    result.elapsed = time.monotonic() - started
    return result


# --------------------------------------------------------------------------
# 结论
# --------------------------------------------------------------------------


@dataclass
class Report:
    """一次压测的结论。"""

    same_key: FireResult
    distinct_keys: Optional[FireResult] = None
    queued_after_same_key: Optional[int] = None
    queued_after_distinct_keys: Optional[int] = None
    redis_url: Optional[str] = None

    def problems(self) -> list[str]:
        issues: list[str] = []
        same = self.same_key
        if same.statuses.get(200, 0) != same.requests:
            issues.append(f"同键轮存在非 200 响应：{same.statuses}")
        if same.distinct_trigger_ids != 1:
            issues.append(f"同键轮出现 {same.distinct_trigger_ids} 个触发标识，应只有 1 个")
        if self.queued_after_same_key is not None and self.queued_after_same_key != 1:
            issues.append(f"同键轮入队 {self.queued_after_same_key} 条，应只有 1 条")

        other = self.distinct_keys
        if other is not None:
            if other.statuses.get(200, 0) != other.requests:
                issues.append(f"异键轮存在非 200 响应：{other.statuses}")
            if other.distinct_trigger_ids != other.requests:
                issues.append(
                    f"异键轮 {other.requests} 条请求只产出 {other.distinct_trigger_ids} 个触发标识"
                )
            if self.queued_after_distinct_keys is not None and (
                self.queued_after_distinct_keys != other.requests
            ):
                issues.append(
                    f"异键轮入队 {self.queued_after_distinct_keys} 条，应为 {other.requests} 条"
                )
        return issues

    def as_dict(self) -> dict[str, Any]:
        def dump(result: Optional[FireResult]) -> Optional[dict[str, Any]]:
            if result is None:
                return None
            return {
                "requests": result.requests,
                "statuses": {str(k): v for k, v in sorted(result.statuses.items())},
                "distinctTriggerIds": result.distinct_trigger_ids,
                "elapsedSeconds": round(result.elapsed, 3),
                "failures": result.failures,
            }

        return {
            "sameKey": dump(self.same_key),
            "distinctKeys": dump(self.distinct_keys),
            "queuedAfterSameKey": self.queued_after_same_key,
            "queuedAfterDistinctKeys": self.queued_after_distinct_keys,
            "verdict": "pass" if not self.problems() else "fail",
            "problems": self.problems(),
        }


def run(
    urls: list[str],
    *,
    requests: int = 100,
    concurrency: int = 16,
    redis_url: Optional[str] = None,
    queue_key: Optional[str] = None,
    key: Optional[str] = None,
    keys: Optional[list[str]] = None,
    workflow: str = DEFAULT_WORKFLOW,
    subscriber: str = DEFAULT_SUBSCRIBER,
    distinct_round: bool = True,
) -> Report:
    """跑两轮并发并给出结论。

    不给 ``redis_url`` 时跳过队列核对，只按回执判定——打别人的部署时拿不到
    队列是常态，但「N 个请求只拿到 1 个触发标识」本身已经能说明去重生效。
    """
    same_key = key or f"stress-{uuid.uuid4()}"
    before = queue_depth(redis_url, queue_key) if redis_url and queue_key else None

    first = fire(
        urls,
        keys=[same_key] * requests,
        workflow=workflow,
        subscriber=subscriber,
        concurrency=concurrency,
    )
    after_same = queue_depth(redis_url, queue_key) if redis_url and queue_key else None

    second: Optional[FireResult] = None
    after_distinct: Optional[int] = None
    distinct_key_list: list[str] = []
    if distinct_round:
        distinct_key_list = [f"stress-{uuid.uuid4()}" for _ in range(requests)]
        second = fire(
            urls,
            keys=distinct_key_list,
            workflow=workflow,
            subscriber=subscriber,
            concurrency=concurrency,
        )
        if redis_url and queue_key:
            after_distinct = queue_depth(redis_url, queue_key) - (after_same or 0)

    report = Report(
        same_key=first,
        distinct_keys=second,
        queued_after_same_key=None if before is None else (after_same - before),
        queued_after_distinct_keys=after_distinct,
        redis_url=redis_url,
    )
    if redis_url:
        drop_keys(redis_url, [f"xw:idem:{same_key}"] + [f"xw:idem:{k}" for k in distinct_key_list])
    return report


def render(report: Report) -> str:
    lines: list[str] = []

    def block(title: str, result: Optional[FireResult], queued: Optional[int]) -> None:
        if result is None:
            return
        lines.append(f"{title}：{result.requests} 个请求 / {result.elapsed:.2f}s")
        statuses = "、".join(f"{code or '连接失败'}×{count}" for code, count in sorted(result.statuses.items()))
        lines.append(f"  响应：{statuses}")
        lines.append(f"  触发标识：{result.distinct_trigger_ids} 个")
        if queued is not None:
            lines.append(f"  入队：{queued} 条")
        for failure in result.failures:
            lines.append(f"  失败样例：{failure}")

    block("同键并发", report.same_key, report.queued_after_same_key)
    block("异键并发", report.distinct_keys, report.queued_after_distinct_keys)
    issues = report.problems()
    lines.append("结论：" + ("通过" if not issues else "不通过"))
    for issue in issues:
        lines.append(f"  - {issue}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="幂等键并发正确性压测：同键并发应只入队一次",
    )
    parser.add_argument("--instances", type=int, default=2, help="自起实例数（默认 2）")
    parser.add_argument("--base-url", action="append", default=[], help="已有部署地址，可重复")
    parser.add_argument("--requests", type=int, default=100, help="每轮请求数（默认 100）")
    parser.add_argument("--concurrency", type=int, default=16, help="并发度（默认 16）")
    parser.add_argument("--redis", default="", help="Redis 地址；留空时自起临时实例")
    parser.add_argument("--queue-key", default="", help="队列键；留空时用随机键")
    parser.add_argument("--key", default="", help="同键轮使用的幂等键；留空时随机生成")
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW, help="触发的工作流名")
    parser.add_argument("--subscriber", default=DEFAULT_SUBSCRIBER, help="订阅者标识")
    parser.add_argument("--no-distinct-round", action="store_true", help="只跑同键轮")
    parser.add_argument("--json", metavar="PATH", default="", help="把结论写成 JSON")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    workdir = Path(os.environ.get("XW_STRESS_WORKDIR") or ROOT / ".pytmp" / "idem-stress")
    workdir.mkdir(parents=True, exist_ok=True)

    queue_key = args.queue_key or f"xw:queue:stress:{uuid.uuid4().hex[:8]}"
    redis_handle: Optional[RedisHandle] = None
    instances: list[Instance] = []
    try:
        urls = list(args.base_url)
        redis_url = args.redis or ""

        if not urls:
            if not redis_url:
                redis_handle = RedisHandle(workdir).start()
                redis_url = redis_handle.url
            instances = launch_instances(
                max(1, args.instances), redis_url=redis_url, queue_key=queue_key, workdir=workdir
            )
            urls = [instance.url for instance in instances]
            print(f"已启动 {len(instances)} 个实例：{' '.join(urls)}")

        report = run(
            urls,
            requests=max(1, args.requests),
            concurrency=max(1, args.concurrency),
            redis_url=redis_url,
            queue_key=queue_key if redis_url else None,
            key=args.key or None,
            workflow=args.workflow,
            subscriber=args.subscriber,
            distinct_round=not args.no_distinct_round,
        )
        print(render(report))
        if args.json:
            Path(args.json).write_text(
                json.dumps(report.as_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
        return 0 if not report.problems() else 1
    finally:
        stop_instances(instances)
        if redis_handle is not None:
            redis_handle.stop()


if __name__ == "__main__":
    raise SystemExit(main())
