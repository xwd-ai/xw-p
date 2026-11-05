#!/usr/bin/env python3
"""端到端场景验收：真实 API + 真实引擎 + 真实队列跑通多条业务链路。

与 ``api/tests`` 下的集成测试的分工：那些测试用进程内存储验证「接口与组件
协作一致」，本脚本跨进程验证「Python API 入队 → Redis 队列 → Go 引擎消费 →
落库 → API 读回」这条真实链路，覆盖五个场景：

1. **完整投递**：``post_commented`` 触发后三个渠道的执行明细齐全，站内信进入收件箱；
2. **聚合卡片**：同一篇帖子三次 ``post_liked`` 收敛成一张卡片，计数累加到 3；
3. **幂等去重**：同一 ``Idempotency-Key`` 触发两次只入队一次，回执标识一致；
4. **偏好跳过**：关闭 email 偏好后，执行明细里 email 为 ``skipped``、其余渠道照常；
5. **环境隔离**：生产环境触发的事件只出现在生产收件箱，开发环境看不见。

用法::

    python scripts/e2e_scenarios.py                # 自起临时 Redis 与全部进程
    python scripts/e2e_scenarios.py --keep         # 保留临时目录便于排查
    python scripts/e2e_scenarios.py --scenario digest,idem   # 只跑部分场景

前置依赖：``redis-server``（本机可执行文件）、Go 工具链（编译引擎）、
``api`` 虚拟环境或当前解释器已安装 ``api/requirements.txt``。
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
API_DIR = ROOT / "api"
ENGINE_DIR = ROOT / "engine"

#: 场景名 → 处理函数（登记顺序即执行顺序）。
SCENARIOS: dict[str, Callable[["Harness", str], None]] = {}


def scenario(name: str):
    """注册一个场景。"""

    def register(fn: Callable[["Harness", str], None]):
        SCENARIOS[name] = fn
        return fn

    return register


def free_port() -> int:
    """取一个当前空闲的端口。"""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _clean_env() -> dict[str, str]:
    """子进程环境：清掉继承来的 XW_* 与代理配置。

    代理是这里的常见坑——本机设了 ``HTTP_PROXY`` 时连 ``127.0.0.1`` 也会被
    送去代理，表现为「服务没起来」，而服务其实好好的。
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("XW_")}
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    env["PYTHONUNBUFFERED"] = "1"
    return env


def http_json(
    method: str,
    url: str,
    body: Optional[dict] = None,
    *,
    environment: str = "development",
    headers: Optional[dict[str, str]] = None,
) -> tuple[int, Any]:
    """发起一次 JSON 请求，返回（状态码，响应体）。"""
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    request.add_header("X-Environment", environment)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=5.0) as response:
            raw = response.read().decode("utf-8")
            return response.status, (json.loads(raw) if raw.strip() else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            return exc.code, raw


def wait_until(label: str, probe: Callable[[], bool], timeout: float = 20.0) -> None:
    """轮询直到条件成立；超时抛错并点名在等什么。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(0.3)
    raise AssertionError(f"等待超时（{timeout:.0f}s）：{label}")


# --------------------------------------------------------------------------
# 进程编排：临时 Redis、API、引擎
# --------------------------------------------------------------------------


class RedisHandle:
    """临时 redis-server 实例（用完即关）。"""

    def __init__(self, workdir: Path) -> None:
        self._workdir = workdir
        self._process: Optional[subprocess.Popen] = None
        self.port = 0

    def start(self) -> "RedisHandle":
        binary = subprocess.run(
            ["sh", "-c", "command -v redis-server"], capture_output=True, text=True
        ).stdout.strip()
        if not binary:
            raise RuntimeError("本机没有 redis-server，无法搭建端到端环境")
        self._workdir.mkdir(parents=True, exist_ok=True)
        self.port = free_port()
        self._process = subprocess.Popen(
            [binary, "--port", str(self.port), "--save", "", "--appendonly", "no",
             "--dir", str(self._workdir)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
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

    @property
    def url(self) -> str:
        return f"redis://127.0.0.1:{self.port}/0"

    @property
    def addr(self) -> str:
        return f"127.0.0.1:{self.port}"

    def stop(self) -> None:
        if self._process is None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover —— 极少发生
            self._process.kill()
        self._process = None


@dataclass
class Service:
    """一个子进程服务（API / 引擎）。"""

    name: str
    url: str
    process: subprocess.Popen
    log_path: Path

    def log_tail(self, limit: int = 30) -> str:
        if not self.log_path.exists():
            return ""
        lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-limit:])


def go_binary() -> str:
    """定位 go：PATH 优先，其次用户级安装目录。"""
    found = shutil.which("go")
    if found:
        return found
    candidate = Path.home() / ".local" / "go" / "bin" / "go"
    if candidate.exists():
        return str(candidate)
    raise RuntimeError("找不到 go 工具链（PATH 或 ~/.local/go/bin），无法编译引擎")


def python_binary() -> str:
    """优先用项目虚拟环境，其次当前解释器。"""
    candidate = API_DIR / ".venv" / "bin" / "python"
    return str(candidate) if candidate.exists() else sys.executable


@dataclass
class Harness:
    """端到端环境：临时 Redis + API + 引擎 + 共享数据目录。"""

    workdir: Path
    redis: RedisHandle
    api: Service
    engine: Service
    admin_url: str
    subscribers: dict[str, dict] = field(default_factory=dict)

    # ------------------------------------------------------------- 触发与查询

    def trigger(
        self,
        name: str,
        subscriber: str,
        payload: dict,
        *,
        channels: Optional[list[str]] = None,
        environment: str = "development",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        # 收件地址随触发请求带上：引擎从消息里取地址，不回查订阅者档案。
        target: dict[str, Any] = {
            "subscriberId": subscriber,
            "email": f"{subscriber}@example.com",
            "deviceToken": f"fcm-{subscriber}",
        }
        if channels:
            target["channels"] = channels
        body: dict[str, Any] = {"name": name, "to": target, "payload": payload}
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        status, data = http_json(
            "POST", f"{self.api.url}/v1/events/trigger", body,
            environment=environment, headers=headers,
        )
        assert status == 200, f"触发 {name} 失败：{status} {data}"
        return data

    def inbox(self, subscriber: str, *, environment: str = "development") -> list[dict]:
        status, data = http_json(
            "GET",
            f"{self.api.url}/v1/inbox?subscriberId={subscriber}",
            environment=environment,
        )
        assert status == 200, f"读取收件箱失败：{status} {data}"
        return list(data.get("items") or [])

    def executions(self, trigger_id: str, *, environment: str = "development") -> list[dict]:
        status, data = http_json(
            "GET",
            f"{self.api.url}/v1/executions?triggerId={trigger_id}",
            environment=environment,
        )
        assert status == 200, f"读取执行明细失败：{status} {data}"
        return list(data.get("items") or [])

    def create_subscriber(self, subscriber: str, channels: list[str]) -> None:
        status, data = http_json(
            "POST",
            f"{self.api.url}/v1/subscribers",
            {
                "id": subscriber,
                "email": f"{subscriber}@example.com",
                # 推送要真实令牌：缺令牌的 push 步骤会失败并短路后续渠道，
                # 场景要验的「三个渠道各自的结果」就被一条失败带偏了。
                "device_token": f"fcm-{subscriber}",
                "channels": channels,
            },
        )
        assert status == 201, f"创建订阅者 {subscriber} 失败：{status} {data}"
        self.subscribers[subscriber] = data

    def stop(self) -> None:
        """按「引擎 → API → Redis」的顺序收尾，全部容忍已退出的进程。"""
        for service in (self.engine, self.api):
            if service.process.poll() is None:
                service.process.terminate()
        for service in (self.engine, self.api):
            try:
                service.process.wait(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover —— 极少发生
                service.process.kill()
        self.redis.stop()


def build_harness(workdir: Path) -> Harness:
    """搭建端到端环境并等待两个服务就绪。"""
    redis_handle = RedisHandle(workdir / "redis").start()
    data_dir = workdir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # --- 编译引擎（一次即可，后续场景共用） ---
    engine_bin = workdir / "xw-engine"
    build = subprocess.run(
        [go_binary(), "build", "-o", str(engine_bin), "./cmd/engine"],
        cwd=str(ENGINE_DIR),
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        raise RuntimeError(f"编译引擎失败：\n{build.stderr}")

    queue_key = "xw:queue:events"

    # --- API ---
    api_port = free_port()
    api_log = workdir / "api.log"
    api_env = _clean_env()
    api_env.update(
        {
            "XW_DATA_DIR": str(data_dir),
            "XW_BUS": "redis",
            "XW_REDIS_URL": redis_handle.url,
            "XW_REDIS_QUEUE_KEY": queue_key,
            "XW_API_WORKER": "0",  # 由引擎消费，保证入队与消费是两个真实进程
            "XW_STORAGE": "jsonl",
        }
    )
    existing = api_env.get("PYTHONPATH")
    api_env["PYTHONPATH"] = str(API_DIR) + (os.pathsep + existing if existing else "")
    api_process = subprocess.Popen(
        [python_binary(), "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(api_port), "--log-level", "warning"],
        cwd=str(API_DIR),
        env=api_env,
        stdout=api_log.open("wb"),
        stderr=subprocess.STDOUT,
    )
    api = Service("api", f"http://127.0.0.1:{api_port}", api_process, api_log)

    # --- 引擎 ---
    engine_port = free_port()
    engine_log = workdir / "engine.log"
    engine_env = _clean_env()
    engine_env.update(
        {
            "XW_REDIS_ADDR": redis_handle.addr,
            "XW_REDIS_QUEUE_KEY": queue_key,
            "XW_INBOX_FILE": str(data_dir / "inbox.jsonl"),
            "XW_EXECUTION_FILE": str(data_dir / "executions.jsonl"),
            "XW_PREFERENCES_FILE": str(data_dir / "preferences.jsonl"),
            "XW_CALLBACK_AUDIT_FILE": str(data_dir / "callback_audit.jsonl"),
            "XW_CALLBACK_EVENT_FILE": str(data_dir / "callback_events.jsonl"),
            "XW_DIGEST_STORE": "redis",
            "XW_DELAY_STORE": "redis",
            "XW_ADMIN_ADDR": f"127.0.0.1:{engine_port}",
            "XW_LOG_FORMAT": "json",
        }
    )
    engine_process = subprocess.Popen(
        [str(engine_bin)],
        cwd=str(workdir),  # 避免默认落盘路径写进仓库
        env=engine_env,
        stdout=engine_log.open("wb"),
        stderr=subprocess.STDOUT,
    )
    engine = Service("engine", f"http://127.0.0.1:{engine_port}", engine_process, engine_log)

    harness = Harness(
        workdir=workdir,
        redis=redis_handle,
        api=api,
        engine=engine,
        admin_url=f"http://127.0.0.1:{engine_port}",
    )
    try:
        wait_until("API /health/ready", lambda: _probe(f"{api.url}/health/ready"))
        wait_until("引擎 /healthz", lambda: _probe(f"{engine.url}/healthz"))
    except BaseException:
        harness.stop()
        raise
    return harness


def _probe(url: str) -> bool:
    try:
        status, _ = http_json("GET", url)
        return status == 200
    except (urllib.error.URLError, OSError):
        return False


# --------------------------------------------------------------------------
# 场景
# --------------------------------------------------------------------------


@scenario("chain")
def scenario_full_chain(h: Harness, tag: str) -> None:
    """完整投递：一次评论触发三个渠道，执行明细与收件箱都能读回。"""
    subscriber = f"creator-{tag}-chain"
    h.create_subscriber(subscriber, ["in_app", "push", "email"])
    receipt = h.trigger(
        "post_commented",
        subscriber,
        {
            "post_id": f"P-{tag}-1",
            "post_title": "端到端测试帖",
            "actor": "小林",
            "comment": "写得真好",
        },
        channels=["in_app", "push", "email"],
    )
    trigger_id = receipt["triggerId"]

    wait_until(
        "三个渠道的执行明细",
        lambda: {item["channel"] for item in h.executions(trigger_id)}
        >= {"in_app", "push", "email"},
    )
    details = {item["channel"]: item for item in h.executions(trigger_id)}
    for channel in ("in_app", "push", "email"):
        assert details[channel]["status"] == "sent", f"{channel} 未送达：{details[channel]}"

    wait_until("收件箱出现站内信", lambda: len(h.inbox(subscriber)) >= 1)
    items = h.inbox(subscriber)
    assert any("写得真好" in (item.get("body") or "") for item in items), (
        f"收件箱缺少渲染后的评论内容：{items}"
    )


@scenario("digest")
def scenario_digest(h: Harness, tag: str) -> None:
    """聚合卡片：同一篇帖子三次点赞收敛为一张卡片，计数累加到 3。"""
    subscriber = f"creator-{tag}-digest"
    h.create_subscriber(subscriber, ["in_app", "push"])
    for actor in ("Alice", "Bob", "Carol"):
        h.trigger(
            "post_liked",
            subscriber,
            {"post_id": f"P-{tag}-digest", "post_title": "聚合测试帖", "actor": actor},
        )

    def digest_card() -> Optional[dict]:
        cards = [
            item for item in h.inbox(subscriber)
            if str(item.get("message_id") or "").startswith("digest-")
        ]
        return cards[0] if cards else None

    wait_until("聚合卡片出现", lambda: digest_card() is not None)
    wait_until("聚合计数累加到 3", lambda: "3 人" in (digest_card() or {}).get("body", ""))
    cards = [
        item for item in h.inbox(subscriber)
        if str(item.get("message_id") or "").startswith("digest-")
    ]
    assert len(cards) == 1, f"同一篇帖子应只有一张聚合卡片，实际 {len(cards)} 张"
    assert "3 人" in cards[0]["body"], f"聚合卡片计数不对：{cards[0]['body']}"


@scenario("idem")
def scenario_idempotency(h: Harness, tag: str) -> None:
    """幂等去重：同键两次触发只入队一次，回执标识一致。"""
    subscriber = f"creator-{tag}-idem"
    h.create_subscriber(subscriber, ["in_app"])
    key = f"e2e-{tag}-idem"
    payload = {"post_id": f"P-{tag}-idem", "post_title": "幂等测试帖", "actor": "小周"}
    first = h.trigger("post_mentioned", subscriber, payload, idempotency_key=key)
    second = h.trigger("post_mentioned", subscriber, payload, idempotency_key=key)
    assert first["triggerId"] == second["triggerId"], (
        f"同键两次触发应回同一标识：{first['triggerId']} vs {second['triggerId']}"
    )

    def delivered() -> bool:
        return any(
            item.get("trigger_id") == first["triggerId"] for item in h.inbox(subscriber)
        )

    wait_until("去重后的唯一一条消息送达", delivered)
    time.sleep(1.0)  # 留出「重复入队」的窗口：真重复了会多出第二条
    mine = [item for item in h.inbox(subscriber) if item.get("workflow") == "post_mentioned"]
    assert len(mine) == 1, f"同键触发应只产生一条消息，实际 {len(mine)} 条"


@scenario("pref")
def scenario_preference_skip(h: Harness, tag: str) -> None:
    """偏好跳过：关闭 email 后该渠道记为 skipped，其余渠道照常投递。"""
    subscriber = f"creator-{tag}-pref"
    h.create_subscriber(subscriber, ["in_app", "push", "email"])
    status, data = http_json(
        "PUT",
        f"{h.api.url}/v1/subscribers/{subscriber}/preferences",
        {"scope": "channel", "target": "email", "enabled": False},
    )
    assert status == 200, f"写入偏好失败：{status} {data}"

    receipt = h.trigger(
        "post_commented",
        subscriber,
        {
            "post_id": f"P-{tag}-pref",
            "post_title": "偏好测试帖",
            "actor": "小吴",
            "comment": "应该跳过邮件",
        },
        channels=["in_app", "push", "email"],
    )
    trigger_id = receipt["triggerId"]

    def complete() -> bool:
        channels = {item["channel"] for item in h.executions(trigger_id)}
        return channels >= {"in_app", "push", "email"}

    wait_until("三条渠道的执行明细都已写出", complete)
    details = {item["channel"]: item for item in h.executions(trigger_id)}
    assert details["email"]["status"] == "skipped", (
        f"关闭偏好后 email 应为 skipped：{details['email']}"
    )
    assert details["in_app"]["status"] == "sent", f"in_app 不应受影响：{details['in_app']}"
    assert details["push"]["status"] == "sent", f"push 不应受影响：{details['push']}"


@scenario("env")
def scenario_environment_isolation(h: Harness, tag: str) -> None:
    """环境隔离：生产环境的事件只出现在生产收件箱。"""
    subscriber = f"creator-{tag}-env"
    workflow_name = f"post_prod_ping_{tag}"
    status, data = http_json(
        "POST",
        f"{h.api.url}/v1/workflows",
        {
            "name": workflow_name,
            "steps": [
                {
                    "type": "in_app",
                    "name": "站内信",
                    "config": {"template": "生产环境的 {{payload.actor}} 提到了你"},
                }
            ],
        },
        environment="production",
    )
    assert status == 201, f"创建生产工作流失败：{status} {data}"

    h.trigger(
        workflow_name,
        subscriber,
        {"post_id": f"P-{tag}-env", "post_title": "生产帖", "actor": "小郑"},
        environment="production",
    )
    wait_until("生产收件箱出现消息", lambda: len(h.inbox(subscriber, environment="production")) >= 1)
    assert h.inbox(subscriber, environment="development") == [], (
        "生产环境的事件不应出现在开发环境收件箱"
    )
    body = h.inbox(subscriber, environment="production")[0].get("body") or ""
    assert "小郑" in body, f"收件箱正文未按模板渲染：{body}"


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------


def run(selected: Optional[list[str]], keep: bool) -> int:
    names = selected or list(SCENARIOS)
    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        print(f"未知场景：{unknown}（可选：{', '.join(SCENARIOS)}）", file=sys.stderr)
        return 2

    root = Path(os.environ.get("XW_E2E_WORKDIR") or "") if os.environ.get("XW_E2E_WORKDIR") else None
    if root is None:
        import tempfile

        root = Path(tempfile.mkdtemp(prefix="xw-e2e-"))
    root.mkdir(parents=True, exist_ok=True)
    print(f"工作目录：{root}")

    harness = build_harness(root)
    failures: list[str] = []
    try:
        for index, name in enumerate(names):
            started = time.monotonic()
            try:
                SCENARIOS[name](harness, f"{index}{name}")
            except Exception as exc:  # noqa: BLE001 —— 逐场景记录失败后继续
                failures.append(name)
                print(f"[FAIL] {name}（{time.monotonic() - started:.1f}s）：{exc}")
                for service in (harness.api, harness.engine):
                    tail = service.log_tail(12)
                    if tail:
                        print(f"  --- {service.name} 日志尾部 ---\n{tail}")
            else:
                print(f"[ ok ] {name}（{time.monotonic() - started:.1f}s）")
    finally:
        harness.stop()
        if keep:
            print(f"已保留工作目录：{root}")
        else:
            shutil.rmtree(root, ignore_errors=True)

    if failures:
        print(f"\n失败场景：{', '.join(failures)}")
        return 1
    print(f"\n全部通过（{len(names)} 个场景）")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="xw-notify 端到端场景验收")
    parser.add_argument("--scenario", help="只跑指定场景（逗号分隔）：chain,digest,idem,pref,env")
    parser.add_argument("--keep", action="store_true", help="保留临时目录以便排查")
    args = parser.parse_args()
    selected = [item.strip() for item in args.scenario.split(",")] if args.scenario else None
    sys.exit(run(selected, args.keep))
