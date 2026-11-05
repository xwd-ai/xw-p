"""限流：配额解析、令牌桶、身份判定与中间件行为。

三层分开测：
- 桶本身（补桶速率、突发、并发、淘汰）——用注入的时钟，不睡真实时间；
- 身份（凭据 vs IP）——凭据只认「服务端认可的」，伪造的 Key 不能换一个新桶；
- 中间件——429 的形状、响应头、豁免路径，以及后端故障时放行而不是把服务拒掉。
"""
from __future__ import annotations

import json
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import config, ratelimit, tokens
from app.ratelimit import (
    DIMENSION_IP,
    DIMENSION_KEY,
    Decision,
    Identity,
    MemoryTokenBucket,
    Quota,
    RateLimiter,
    RedisTokenBucket,
    client_ip,
    make_quota,
    resolve_backend,
)
from app.ratelimit_middleware import RateLimitMiddleware


class _Clock:
    """可推进的单调时钟。"""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# --------------------------------------------------------------------------
# 配额
# --------------------------------------------------------------------------


def test_make_quota_zero_disables_dimension():
    quota = make_quota(0)
    assert quota.enabled is False
    assert quota.rate == 0.0
    assert (quota.rpm, quota.burst) == (0, 0)


def test_make_quota_defaults_burst_to_minute_quota():
    quota = make_quota(600)
    assert quota.enabled is True
    assert quota.burst == 600
    assert quota.rate == pytest.approx(10.0)


def test_make_quota_keeps_explicit_burst_and_rejects_negative():
    assert make_quota(60, 5).burst == 5
    # 写错（负数）按缺省处理：一个数字写错不该让服务起不来。
    assert make_quota(60, -3).burst == 60


def test_quota_ttl_covers_full_refill():
    quota = make_quota(30, 30)
    # 补满需要 burst / rate = 60s，另加一分钟余量。
    assert quota.ttl_ms() >= 60_000
    assert quota.ttl_ms() == 120_000


def test_quota_ttl_survives_zero_rate():
    assert Quota(rpm=0, burst=0).ttl_ms() == 60_000


# --------------------------------------------------------------------------
# 内存令牌桶
# --------------------------------------------------------------------------


def test_memory_bucket_allows_burst_then_rejects():
    bucket = MemoryTokenBucket()
    quota = make_quota(60, 3)
    results = [bucket.take("key", quota, 0.0) for _ in range(4)]
    assert [r[0] for r in results] == [True, True, True, False]
    assert [r[1] for r in results] == [2, 1, 0, 0]
    # 桶空后要等 1 / rate = 1 秒才补出一个令牌。
    assert results[-1][2] == pytest.approx(1.0)


def test_memory_bucket_refills_at_configured_rate():
    bucket = MemoryTokenBucket()
    quota = make_quota(120, 1)  # 每秒 2 个
    assert bucket.take("key", quota, 0.0)[0] is True
    assert bucket.take("key", quota, 0.0)[0] is False
    # 半秒补一个。
    assert bucket.take("key", quota, 0.5)[0] is True
    assert bucket.take("key", quota, 0.5)[0] is False


def test_memory_bucket_does_not_exceed_capacity_after_long_idle():
    bucket = MemoryTokenBucket()
    quota = make_quota(60, 5)
    bucket.take("key", quota, 0.0)
    allowed, remaining, _ = bucket.take("key", quota, 3600.0)
    assert allowed is True
    # 空闲一年也只补到容量 5，扣掉本次的剩 4。
    assert remaining == 4


def test_memory_bucket_ignores_clock_going_backwards():
    bucket = MemoryTokenBucket()
    quota = make_quota(60, 1)
    assert bucket.take("key", quota, 100.0)[0] is True
    # 时钟回拨不退款：否则把时钟调回去就能无限刷。
    assert bucket.take("key", quota, 50.0)[0] is False


def test_memory_bucket_keeps_keys_apart():
    bucket = MemoryTokenBucket()
    quota = make_quota(60, 1)
    assert bucket.take("key:a", quota, 0.0)[0] is True
    assert bucket.take("key:b", quota, 0.0)[0] is True
    assert bucket.take("key:a", quota, 0.0)[0] is False


def test_memory_bucket_evicts_least_recently_used():
    bucket = MemoryTokenBucket(max_entries=4)
    quota = make_quota(60, 2)
    for index, name in enumerate(("a", "b", "c", "d", "e")):
        bucket.take(name, quota, float(index))
    # 上限之上按最久未用裁到九成，进程内存不会随来源数无限增长。
    assert bucket.size() <= 4
    assert bucket.take("e", quota, 10.0)[0] is True
    # 最早被访问的桶被淘汰，重新出现时拿到完整的容量。
    assert bucket.take("a", quota, 10.0)[1] == 1


def test_memory_bucket_concurrent_take_never_exceeds_burst():
    bucket = MemoryTokenBucket()
    quota = make_quota(60, 20)
    allowed: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        for _ in range(50):
            ok, _, _ = bucket.take("shared", quota, 0.0)
            with lock:
                allowed.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert allowed.count(True) == 20


# --------------------------------------------------------------------------
# Redis 令牌桶（假客户端：只验证脚本参数与返回值解析）
# --------------------------------------------------------------------------


class _ScriptRedis:
    def __init__(self, result=(1, 4, "0.000")) -> None:
        self.calls: list[tuple] = []
        self._result = result
        self.round_trips = 0

    def eval(self, script, numkeys, *args):
        self.calls.append((script, numkeys, args))
        self.round_trips += 1
        return list(self._result)


def test_redis_bucket_sends_script_with_quota(monkeypatch):
    fake = _ScriptRedis(result=(1, 4, "0.000"))
    monkeypatch.setattr("redis.Redis.from_url", lambda *a, **k: fake)
    bucket = RedisTokenBucket("redis://localhost:6379/0", "rate:test")
    allowed, remaining, retry = bucket.take("ip:1.2.3.4", make_quota(600, 10), 0.0)

    assert (allowed, remaining, retry) == (True, 4, 0.0)
    assert bucket.key("ip:1.2.3.4") == "rate:test:ip:1.2.3.4"
    script, numkeys, args = fake.calls[0]
    assert numkeys == 1
    assert "redis.call('TIME')" in script  # 时间取服务端，避免实例间时钟偏差
    assert args[0] == "rate:test:ip:1.2.3.4"
    assert args[1] == pytest.approx(10.0)  # 600/60
    assert args[2] == 10
    assert args[3] == 61_000  # 补满 10/10s + 60s 余量


def test_redis_bucket_parses_rejection():
    fake = _ScriptRedis(result=(0, 0, "2.500"))
    bucket = RedisTokenBucket("redis://localhost:6379/0", "rate", timeout=0.2)
    bucket._client = fake  # 跳过连接建立
    allowed, remaining, retry = bucket.take("key:x", make_quota(60, 1), 0.0)
    assert allowed is False
    assert remaining == 0
    assert retry == pytest.approx(2.5)


def test_redis_bucket_normalizes_prefix():
    assert RedisTokenBucket("redis://x", "rate:").key("ip:1") == "rate:ip:1"
    assert RedisTokenBucket("redis://x", "  ").key("ip:1") == "xw:ratelimit:ip:1"


def test_resolve_backend_follows_bus_by_default():
    assert resolve_backend("redis", "auto") == "redis"
    assert resolve_backend("memory", "auto") == "memory"
    assert resolve_backend("memory", "") == "memory"
    # 显式指定优先于总线：读写分离或有独立 Redis 的部署会这么配。
    assert resolve_backend("redis", "memory") == "memory"
    assert resolve_backend("memory", "redis") == "redis"


# --------------------------------------------------------------------------
# 身份判定
# --------------------------------------------------------------------------


def _scopes(monkeypatch, *keys: str) -> None:
    payload = [
        {"key": key, "tenant": "default", "environments": ["development"], "label": f"k{index}"}
        for index, key in enumerate(keys)
    ]
    monkeypatch.setattr(config, "API_KEY_SCOPES", json.dumps(payload))
    monkeypatch.setattr(config, "API_KEYS", {})


def test_identity_prefers_configured_api_key(monkeypatch):
    _scopes(monkeypatch, "k-live-1")
    limiter = RateLimiter(make_quota(60), make_quota(30), MemoryTokenBucket())
    identity = limiter.identify({"x-api-key": "k-live-1"}, "10.0.0.1")
    assert identity is not None
    assert identity.dimension == DIMENSION_KEY
    # 键里放摘要，不落凭据原值。
    assert "k-live-1" not in identity.digest
    assert "k-live-1" not in identity.label
    assert identity.quota.rpm == 60


def test_identity_falls_back_to_ip_for_unknown_key(monkeypatch):
    _scopes(monkeypatch, "k-live-1")
    limiter = RateLimiter(make_quota(60), make_quota(30), MemoryTokenBucket())
    identity = limiter.identify({"x-api-key": "made-up"}, "10.0.0.1")
    assert identity is not None
    # 没配 Key 的部署或伪造的 Key 不能靠换头换桶。
    assert identity.dimension == DIMENSION_IP
    assert identity.digest == "10.0.0.1"


def test_identity_falls_back_to_ip_when_no_keys_configured(monkeypatch):
    monkeypatch.setattr(config, "API_KEY_SCOPES", "")
    monkeypatch.setattr(config, "API_KEYS", {})
    limiter = RateLimiter(make_quota(60), make_quota(30), MemoryTokenBucket())
    identity = limiter.identify({"x-api-key": "anything"}, "10.0.0.1")
    assert identity is not None and identity.dimension == DIMENSION_IP


def test_identity_groups_tokens_by_subject(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", "x" * 32)
    _scopes(monkeypatch)
    limiter = RateLimiter(make_quota(60), make_quota(30), MemoryTokenBucket())
    first = tokens.issue(subject="ops", tenant="acme", environments=["development"])
    second = tokens.issue(subject="ops", tenant="acme", environments=["development"])
    other = tokens.issue(subject="someone", tenant="acme", environments=["development"])

    def call(token: str):
        return limiter.identify({"authorization": f"Bearer {token}"}, "10.0.0.1")

    a, b, c = call(first.token), call(second.token), call(other.token)
    assert a and b and c
    # 重新登录换了令牌，不该重置额度。
    assert (a.dimension, a.digest) == (DIMENSION_KEY, b.digest)
    assert c.digest != a.digest
    assert "ops" not in a.digest


def test_invalid_token_falls_back_to_ip(monkeypatch):
    monkeypatch.setattr(config, "JWT_SECRET", "x" * 32)
    _scopes(monkeypatch)
    limiter = RateLimiter(make_quota(60), make_quota(30), MemoryTokenBucket())
    identity = limiter.identify({"authorization": "Bearer not-a-token"}, "10.0.0.1")
    assert identity is not None and identity.dimension == DIMENSION_IP


def test_identity_ignores_credentials_when_key_dimension_off(monkeypatch):
    _scopes(monkeypatch, "k-live-1")
    limiter = RateLimiter(make_quota(0), make_quota(30), MemoryTokenBucket())
    identity = limiter.identify({"x-api-key": "k-live-1"}, "10.0.0.1")
    assert identity is not None and identity.dimension == DIMENSION_IP


def test_no_identity_when_all_dimensions_off():
    limiter = RateLimiter(make_quota(0), make_quota(0), MemoryTokenBucket())
    assert limiter.enabled is False
    assert limiter.identify({}, "10.0.0.1") is None
    assert limiter.check({}, "10.0.0.1") is None


# --------------------------------------------------------------------------
# 判定结论
# --------------------------------------------------------------------------


def _identity(quota: Quota, dimension: str = DIMENSION_IP) -> Identity:
    return Identity(dimension=dimension, digest="1.2.3.4", label="来源 1.2.3.4", quota=quota)


def test_decision_headers_for_allowed_request():
    quota = make_quota(60, 10)
    headers = Decision(allowed=True, identity=_identity(quota), remaining=7).headers()
    assert headers["X-RateLimit-Limit"] == "60"
    assert headers["X-RateLimit-Remaining"] == "7"
    assert headers["X-RateLimit-Dimension"] == "ip"
    assert "Retry-After" not in headers
    assert "X-RateLimit-Degraded" not in headers


def test_decision_headers_for_rejected_request():
    quota = make_quota(60, 10)
    headers = Decision(
        allowed=False, identity=_identity(quota), remaining=0, retry_after=0.2
    ).headers()
    # 不足一秒也报 1 秒：``Retry-After`` 只认整数秒，报 0 等于让人立刻重试。
    assert headers["Retry-After"] == "1"


def test_decision_flags_degraded_and_message_is_readable():
    quota = make_quota(60, 10)
    decision = Decision(
        allowed=True, identity=_identity(quota), remaining=10, degraded=True
    )
    assert decision.headers()["X-RateLimit-Degraded"] == "true"
    rejected = Decision(
        allowed=False, identity=_identity(quota), remaining=0, retry_after=2.2
    )
    assert "每分钟最多 60 次" in rejected.message()
    assert "3 秒后重试" in rejected.message()


class _BrokenBucket:
    def take(self, key, quota, now):
        raise ConnectionError("redis 挂了")


def test_check_fails_open_when_backend_is_down():
    limiter = RateLimiter(make_quota(60), make_quota(30), _BrokenBucket())
    decision = limiter.check({}, "10.0.0.1")
    assert decision is not None
    assert decision.allowed is True
    assert decision.degraded is True


def test_check_ignores_broken_credential_table(monkeypatch):
    """凭据表写坏时中间件不能成为 500 的来源：判定放弃，请求继续走鉴权。"""

    def boom():
        raise ValueError("XW_API_KEYS 不是合法 JSON")

    monkeypatch.setattr(ratelimit.tenancy, "load_scopes", boom)
    limiter = RateLimiter(make_quota(60), make_quota(30), MemoryTokenBucket())
    assert limiter.check({"x-api-key": "k"}, "10.0.0.1") is None


def test_check_uses_clock_and_counts_down():
    clock = _Clock()
    limiter = RateLimiter(make_quota(0), make_quota(60, 2), MemoryTokenBucket(), clock=clock)
    first = limiter.check({}, "10.0.0.1")
    second = limiter.check({}, "10.0.0.1")
    third = limiter.check({}, "10.0.0.1")
    assert [d.allowed for d in (first, second, third)] == [True, True, False]
    assert second.remaining == 0
    clock.advance(1.0)
    assert limiter.check({}, "10.0.0.1").allowed is True


def test_describe_states_quota_and_disabled_reason():
    disabled = RateLimiter(make_quota(0), make_quota(0), MemoryTokenBucket())
    assert "未启用" in disabled.describe()
    enabled = RateLimiter(make_quota(600), make_quota(120, 20), MemoryTokenBucket())
    text = enabled.describe()
    assert "凭据 600/分钟·突发 600" in text
    assert "IP 120/分钟·突发 20" in text
    assert "MemoryTokenBucket" in text


# --------------------------------------------------------------------------
# 来源地址
# --------------------------------------------------------------------------


def test_client_ip_ignores_forwarded_header_by_default():
    scope = {"client": ("10.0.0.9", 1234)}
    assert client_ip(scope, {"x-forwarded-for": "1.1.1.1"}, trust_forwarded=False) == "10.0.0.9"


def test_client_ip_uses_last_forwarded_hop_when_trusted():
    scope = {"client": ("10.0.0.9", 1234)}
    headers = {"x-forwarded-for": "1.1.1.1, 203.0.113.7"}
    # 左边可能是调用方自己填的，最右一段才是最近一跳代理写下的。
    assert client_ip(scope, headers, trust_forwarded=True) == "203.0.113.7"


def test_client_ip_falls_back_to_empty_string():
    assert client_ip({}, {}, trust_forwarded=True) == ""


# --------------------------------------------------------------------------
# 中间件
# --------------------------------------------------------------------------


def _build_app(limiter: RateLimiter) -> FastAPI:
    app = FastAPI()

    @app.get("/v1/ping")
    def ping():
        return {"ok": True}

    @app.get("/v1/inbox/stream")
    def stream():
        return {"ok": True}

    @app.get("/health")
    def health():
        return {"status": "ok"}

    app.add_middleware(RateLimitMiddleware, limiter=limiter)
    return app


def test_middleware_allows_within_quota_and_reports_headers():
    limiter = RateLimiter(make_quota(0), make_quota(60, 2), MemoryTokenBucket(), clock=_Clock())
    client = TestClient(_build_app(limiter))
    first = client.get("/v1/ping")
    second = client.get("/v1/ping")
    assert first.status_code == second.status_code == 200
    assert first.headers["X-RateLimit-Limit"] == "60"
    assert first.headers["X-RateLimit-Remaining"] == "1"
    assert second.headers["X-RateLimit-Remaining"] == "0"


def test_middleware_rejects_with_429_and_retry_after():
    limiter = RateLimiter(make_quota(0), make_quota(60, 1), MemoryTokenBucket(), clock=_Clock())
    client = TestClient(_build_app(limiter))
    assert client.get("/v1/ping").status_code == 200
    rejected = client.get("/v1/ping")
    assert rejected.status_code == 429
    assert rejected.headers["Retry-After"] == "1"
    assert rejected.headers["X-RateLimit-Remaining"] == "0"
    detail = rejected.json()["detail"]
    # 文案要说清「谁、多少、等多久」，否则调用方只能靠猜。
    assert "来源 testclient" in detail
    assert "每分钟最多 60 次" in detail


def test_middleware_skips_health_and_docs():
    limiter = RateLimiter(make_quota(0), make_quota(60, 1), MemoryTokenBucket(), clock=_Clock())
    client = TestClient(_build_app(limiter))
    for _ in range(5):
        assert client.get("/health").status_code == 200
    assert client.get("/v1/ping").headers["X-RateLimit-Remaining"] == "0"


def test_middleware_skips_options_preflight():
    limiter = RateLimiter(make_quota(0), make_quota(60, 1), MemoryTokenBucket(), clock=_Clock())
    client = TestClient(_build_app(limiter))
    for _ in range(3):
        # 没有注册 OPTIONS 路由，404 说明请求进了应用层而不是被限流拦下；
        # 关键是它没有消耗额度。
        assert client.options("/v1/ping").status_code != 429
    assert client.get("/v1/ping").status_code == 200


def test_middleware_passes_through_when_disabled():
    limiter = RateLimiter(make_quota(0), make_quota(0), MemoryTokenBucket())
    client = TestClient(_build_app(limiter))
    response = client.get("/v1/ping")
    assert response.status_code == 200
    assert "X-RateLimit-Limit" not in response.headers


def test_middleware_fails_open_when_backend_breaks():
    limiter = RateLimiter(make_quota(0), make_quota(60, 1), _BrokenBucket())
    client = TestClient(_build_app(limiter))
    response = client.get("/v1/ping")
    assert response.status_code == 200
    assert response.headers["X-RateLimit-Degraded"] == "true"


def test_middleware_keeps_existing_headers_untouched():
    app = FastAPI()

    @app.get("/v1/echo")
    def echo():
        from fastapi.responses import JSONResponse

        return JSONResponse({"ok": True}, headers={"X-RateLimit-Limit": "from-route"})

    app.add_middleware(
        RateLimitMiddleware,
        limiter=RateLimiter(make_quota(0), make_quota(60, 5), MemoryTokenBucket(), clock=_Clock()),
    )
    response = TestClient(app).get("/v1/echo")
    # 路由显式设置的同名头不被中间件覆盖，避免出现两份值。
    assert response.headers["X-RateLimit-Limit"] == "from-route"
    assert response.headers["X-RateLimit-Remaining"] == "4"


def test_main_app_wires_middleware_and_cors_wraps_rejections(monkeypatch):
    """真实应用上验证：中间件确实装上，且 429 也带跨源响应头。

    限流如果在 CORS 外层，被拒的请求就是一个浏览器读不到原因的 Failed to
    fetch——前端只能显示「网络错误」，与后端实际给出的「等 1 秒再试」脱节。
    """
    from app.main import app

    limiter = RateLimiter(make_quota(0), make_quota(60, 1), MemoryTokenBucket(), clock=_Clock())
    monkeypatch.setattr(ratelimit, "_limiter", limiter)
    client = TestClient(app)
    origin = {"Origin": "http://127.0.0.1:8398"}

    first = client.get("/v1/workflows", headers=origin)
    assert first.status_code == 200
    assert first.headers["access-control-allow-origin"] in {"*", origin["Origin"]}

    rejected = client.get("/v1/workflows", headers=origin)
    assert rejected.status_code == 429
    # 429 同样带跨源响应头（默认放行任意来源时值是 ``*``）。
    assert rejected.headers["access-control-allow-origin"] in {"*", origin["Origin"]}
    assert rejected.headers["Retry-After"] == "1"


# --------------------------------------------------------------------------
# 配置默认值
# --------------------------------------------------------------------------


def test_rate_limit_defaults_are_off():
    assert config.RATE_LIMIT_KEY_RPM == 0
    assert config.RATE_LIMIT_IP_RPM == 0
    assert config.RATE_LIMIT_BACKEND in {"auto", "redis", "memory"}
    assert config.RATE_LIMIT_TRUST_FORWARDED is False


def test_disabled_limiter_is_reported_in_startup_summary(monkeypatch):
    monkeypatch.setattr(config, "RATE_LIMIT_KEY_RPM", 0)
    monkeypatch.setattr(config, "RATE_LIMIT_IP_RPM", 0)
    limiter = ratelimit.create_limiter()
    assert limiter.enabled is False


def test_create_limiter_reads_env_quotas(monkeypatch):
    monkeypatch.setattr(config, "RATE_LIMIT_KEY_RPM", 600)
    monkeypatch.setattr(config, "RATE_LIMIT_IP_RPM", 120)
    monkeypatch.setattr(config, "RATE_LIMIT_BURST", 30)
    monkeypatch.setattr(config, "RATE_LIMIT_BACKEND", "memory")
    monkeypatch.setattr(config, "RATE_LIMIT_TRUST_FORWARDED", True)
    limiter = ratelimit.create_limiter()
    assert limiter.enabled is True
    assert limiter.trust_forwarded is True
    identity = limiter.identify({}, "10.0.0.1")
    assert identity is not None
    assert (identity.quota.rpm, identity.quota.burst) == (120, 30)


# --------------------------------------------------------------------------
# 真 Redis：脚本的原子性与补桶行为只有跑一遍才算数
# --------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def redis_server(tmp_path: Path):
    binary = subprocess.run(
        ["sh", "-c", "command -v redis-server"], capture_output=True, text=True
    ).stdout.strip()
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

        client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=2.0)
        deadline = time.monotonic() + 5
        while True:
            try:
                client.ping()
                break
            except Exception:  # noqa: BLE001 —— 启动中的连接失败是预期
                if time.monotonic() > deadline:
                    pytest.skip("redis-server 未能启动")
                time.sleep(0.05)
        yield url
    finally:
        process.terminate()
        process.wait(timeout=5)


def test_redis_bucket_honours_burst_and_refills(redis_server):
    bucket = RedisTokenBucket(redis_server, "xw:test:rate", timeout=2.0)
    quota = make_quota(120, 3)  # 每秒 2 个
    name = f"ip:{_free_port()}"

    results = [bucket.take(name, quota, 0.0)[0] for _ in range(4)]
    assert results == [True, True, True, False]

    # 补桶真实发生：半秒后至少能再过一条。
    time.sleep(0.6)
    assert bucket.take(name, quota, 0.0)[0] is True


def test_redis_bucket_is_atomic_under_concurrency(redis_server):
    bucket = RedisTokenBucket(redis_server, "xw:test:race", timeout=2.0)
    quota = make_quota(60, 5)
    name = f"key:{_free_port()}"
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        for _ in range(4):
            ok, _, _ = bucket.take(name, quota, 0.0)
            with lock:
                results.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # 脚本内「读 → 补 → 扣 → 写」不可分割，24 次并发只应放出 5 次。
    assert results.count(True) == 5


def test_redis_bucket_sets_expiry(redis_server):
    import redis

    bucket = RedisTokenBucket(redis_server, "xw:test:ttl", timeout=2.0)
    quota = make_quota(60, 2)
    key = f"key:{_free_port()}"
    bucket.take(key, quota, 0.0)
    client = redis.Redis.from_url(redis_server, decode_responses=True)
    ttl = client.pttl(bucket.key(key))
    # 补满 2/1s = 2s，加一分钟余量；只断言「活着且不会永久留存」。
    assert 0 < ttl <= 120_000
