"""按凭据 / 来源 IP 的令牌桶限流。

**为什么是令牌桶而不是固定窗口**：固定窗口在窗口交界处允许「两倍额度」通过
（59 秒打满、61 秒再打满），而通知接口的流量本来就是一问一答式的短请求，
用「容量 + 恒定补充速率」描述更贴近实际：允许短时突发（一条热帖带来的批量
点赞），长期速率仍被压住。桶里剩多少就还能打几次，不需要对齐任何时间边界。

**维度**：一次请求只消耗一个桶，按「凭据优先、无凭据按 IP」选择：

- 凭据维度：``X-Api-Key`` 命中已配置的 Key，或 Bearer 令牌验签通过。键里放
  凭据的摘要，不落原值——限流键会出现在 Redis 里、也可能出现在排查日志里，
  长期 Key 是能发通知的凭证，不该因为「顺手记一下」而泄漏。令牌按「租户 +
  主体」聚合而不是按令牌串，重新登录拿到新令牌不该重置额度。
- IP 维度：没有有效凭据时的兜底。**只有会校验的凭据才立项**：没配 Key 的
  部署里调用方随便填一个 ``X-Api-Key`` 都能换一个新桶，等于关掉了限流。

两个维度同时配置时不叠加判定。多一次判定就是每条请求多一次 Redis 往返，
而这一层要保护的是「一个来源打爆服务」，凭据维度已经把可控来源收住，
匿名来源落到 IP 维度，没必要为同一件事付两次往返。额度建议按「凭据维度
显著宽于 IP 维度」设置。

**后端**：Redis 用 Lua 脚本原子完成「取服务器时间 → 按经过时间补桶 → 扣一个
令牌」，多实例共享同一个桶；时钟取 Redis 服务端时间而不是各进程的本地时钟，
否则机器间几十毫秒的偏差会让补桶量随实例漂移。内存后端给单进程 demo 与
测试用，接受注入的时钟。后端不可用时**放行并告警**：限流是为了保住服务，
它自己坏掉不该把整个通知接口变成 503；告警按窗口节流，避免 Redis 抖动时
日志跟着刷屏。

默认关闭（两个维度的额度都为 0）。演示与压测不希望被自己配的阈值绊住，
而限流是一个需要按实际流量调的参数，给一个猜出来的默认值只会让人以为
「已经保护好了」。
"""
from __future__ import annotations

import hashlib
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Mapping, Optional, Protocol

from . import config, tenancy, tokens

logger = logging.getLogger("xw.ratelimit")

DIMENSION_KEY = "key"
DIMENSION_IP = "ip"

#: 后端故障告警的最小间隔（秒）。Redis 挂掉时每条请求都会走失败分支，
#: 不节流的话日志量会比正常请求还大。
_WARN_INTERVAL = 30.0


@dataclass(frozen=True)
class Quota:
    """一个维度的额度：每分钟请求数，以及允许的突发深度。"""

    rpm: int
    burst: int

    @property
    def enabled(self) -> bool:
        return self.rpm > 0 and self.burst > 0

    @property
    def rate(self) -> float:
        """每秒补充的令牌数。"""
        if self.rpm <= 0:
            return 0.0
        return self.rpm / 60.0

    def ttl_ms(self) -> int:
        """Redis 键的存活时间：从空桶补满所需时间再加一分钟余量。

        桶补满之后的状态与「没有这条记录」等价，留着只是占内存；加上余量是
        为了避免补充过程中刚好过期，让调用方凭空拿回一整个满桶。
        """
        if self.rate <= 0:
            return 60_000
        return int(math.ceil(self.burst / self.rate * 1000)) + 60_000


def make_quota(rpm: int, burst: int = 0) -> Quota:
    """由每分钟额度与突发深度构造配额；``rpm<=0`` 表示该维度关闭。

    ``burst`` 缺省取每分钟额度，语义是「一分钟的量可以一次用完」，比给一个
    与速率无关的固定常量好解释。写错（负数）按缺省处理，不因为一个数字
    让服务起不来。
    """
    if rpm <= 0:
        return Quota(rpm=0, burst=0)
    capacity = burst if burst > 0 else rpm
    return Quota(rpm=int(rpm), burst=max(1, int(capacity)))


@dataclass(frozen=True)
class Identity:
    """一次请求落到的限流身份。"""

    dimension: str
    #: 落进限流键的一段：凭据是摘要，IP 是地址本身（便于运维直接看出来源）。
    digest: str
    #: 日志与响应头里给人看的标识，不含凭据原值。
    label: str
    quota: Quota


@dataclass(frozen=True)
class Decision:
    """一次限流判定的结论。"""

    allowed: bool
    identity: Identity
    remaining: int
    retry_after: float = 0.0
    #: 后端不可用而放行时为真；响应头里标出来，免得「没被限」被误读成「额度够」。
    degraded: bool = False

    def headers(self) -> dict[str, str]:
        headers = {
            "X-RateLimit-Limit": str(self.identity.quota.rpm),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Dimension": self.identity.dimension,
        }
        if self.degraded:
            headers["X-RateLimit-Degraded"] = "true"
        if not self.allowed:
            headers["Retry-After"] = str(max(1, int(math.ceil(self.retry_after))))
        return headers

    def message(self) -> str:
        seconds = max(1, int(math.ceil(self.retry_after)))
        return (
            f"请求过于频繁：{self.identity.label} 每分钟最多 "
            f"{self.identity.quota.rpm} 次，请在 {seconds} 秒后重试"
        )


class Bucket(Protocol):
    """令牌桶后端：消耗一个令牌，返回（是否放行、剩余、需要等待的秒数）。"""

    def take(self, key: str, quota: Quota, now: float) -> tuple[bool, int, float]: ...


@dataclass
class _State:
    tokens: float
    updated: float


class MemoryTokenBucket:
    """进程内令牌桶，按需补桶，惰性淘汰。

    只有被访问过的键才存在，因此不需要后台清理线程；条目数超过上限时按
    「最久未用」淘汰到上限的九成。淘汰会让那个来源重新拿到一整个满桶，
    但只有在同时存在上万个活跃来源时才会发生——那种规模下这一点点放宽
    与「进程内存被限流键撑爆」相比是更小的代价。
    """

    def __init__(self, max_entries: int = 10_000) -> None:
        self._max = max(1, int(max_entries))
        self._states: dict[str, _State] = {}
        self._lock = threading.Lock()

    def size(self) -> int:
        with self._lock:
            return len(self._states)

    def take(self, key: str, quota: Quota, now: float) -> tuple[bool, int, float]:
        rate = quota.rate
        with self._lock:
            state = self._states.get(key)
            if state is None:
                tokens = float(quota.burst)
            else:
                elapsed = now - state.updated
                # 时钟回拨（测试注入、NTP 调整）按「没经过时间」处理，
                # 否则负的 elapsed 会把桶扣成负数，恢复后要等很久才放行。
                tokens = min(float(quota.burst), state.tokens + max(0.0, elapsed) * rate)
            if tokens >= 1.0:
                allowed = True
                tokens -= 1.0
                retry = 0.0
            else:
                allowed = False
                retry = (1.0 - tokens) / rate
            self._states[key] = _State(tokens=tokens, updated=now)
            self._evict_locked()
            return allowed, int(tokens), retry

    def _evict_locked(self) -> None:
        if len(self._states) <= self._max:
            return
        keep = max(1, int(self._max * 0.9))
        ordered = sorted(self._states.items(), key=lambda item: item[1].updated)
        for key, _ in ordered[: len(self._states) - keep]:
            del self._states[key]


#: 取令牌的 Lua 脚本。整段脚本在 Redis 里串行执行，因此「读桶 → 补桶 → 扣令牌
#: → 写回」之间不会插进别的实例的请求；时间取 ``TIME`` 而不是调用方传参，
#: 多实例共享同一个桶时不必要求机器时钟同步。
_TAKE_SCRIPT = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + elapsed * rate)
local allowed = 0
local retry = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
else
  retry = (1 - tokens) / rate
end
redis.call('HSET', key, 'tokens', string.format('%.6f', tokens), 'ts', string.format('%.6f', now))
redis.call('PEXPIRE', key, ttl)
return {allowed, math.floor(tokens), string.format('%.3f', retry)}
"""


class RedisTokenBucket:
    """多实例共享的令牌桶。

    连接按需建立；``socket_timeout`` 与 ``socket_connect_timeout`` 都收到
    亚秒级——限流跑在每个请求的前面，Redis 慢下来时不能让整条链路跟着等，
    超时后按后端不可用（放行）处理。
    """

    def __init__(self, url: str, prefix: str, timeout: float = 0.5) -> None:
        self._url = url
        self._prefix = (prefix or "").strip().rstrip(":") or "xw:ratelimit"
        self._timeout = timeout
        self._client = None
        self._lock = threading.Lock()

    def key(self, name: str) -> str:
        return f"{self._prefix}:{name}"

    def _get_client(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    import redis  # 延迟导入：内存后端的部署不必装 redis

                    self._client = redis.Redis.from_url(
                        self._url,
                        decode_responses=True,
                        socket_timeout=self._timeout,
                        socket_connect_timeout=self._timeout,
                    )
        return self._client

    def take(self, key: str, quota: Quota, now: float) -> tuple[bool, int, float]:
        result = self._get_client().eval(
            _TAKE_SCRIPT, 1, self.key(key), quota.rate, quota.burst, quota.ttl_ms()
        )
        allowed, remaining, retry = result
        return int(allowed) == 1, int(remaining), float(retry)


def resolve_backend(bus_backend: str, raw: str) -> str:
    """选择限流后端：``redis`` / ``memory``；``auto`` 跟随消息总线。

    ``auto`` 跟随总线是因为两种部署形态的重合度很高：用 Redis 总线的部署
    一定已经有多实例与 Redis，用内存总线的部署通常只有单进程。
    """
    text = (raw or "").strip().lower()
    if text in {"redis", "memory"}:
        return text
    return "redis" if (bus_backend or "").strip().lower() == "redis" else "memory"


def create_bucket(backend: Optional[str] = None) -> Bucket:
    """按配置创建令牌桶后端。"""
    name = backend or resolve_backend(config.BUS_BACKEND, config.RATE_LIMIT_BACKEND)
    if name == "redis":
        return RedisTokenBucket(
            config.REDIS_URL, config.RATE_LIMIT_PREFIX, timeout=config.RATE_LIMIT_TIMEOUT
        )
    return MemoryTokenBucket()


def _digest(value: str) -> str:
    """凭据的摘要：键里、日志里都不出现原值。"""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _bearer(headers: Mapping[str, str]) -> Optional[str]:
    raw = (headers.get("authorization") or "").strip()
    if not raw:
        return None
    scheme, _, value = raw.partition(" ")
    if scheme.lower() != "bearer":
        return None
    token = value.strip()
    return token or None


def client_ip(scope: Mapping, headers: Mapping[str, str], *, trust_forwarded: bool) -> str:
    """取来源地址。

    ``X-Forwarded-For`` 默认不信：任何调用方都能自己填一个值，直接采信等于
    把 IP 维度变成「随便换个头就换一个桶」。只有部署在会**追加**该头的反向
    代理之后才打开开关，并且取最右一段——那是最近一跳代理写下的真实地址，
    左边的部分仍可能是调用方伪造的。
    """
    if trust_forwarded:
        raw = headers.get("x-forwarded-for") or ""
        parts = [part.strip() for part in raw.split(",") if part.strip()]
        if parts:
            return parts[-1]
    client = scope.get("client")
    if client:
        return str(client[0])
    return ""


def _credential_identity(
    headers: Mapping[str, str], key_quota: Quota
) -> Optional[Identity]:
    """从请求头解析出可立项的凭据身份；没有就不返回，交由 IP 维度兜底。"""
    token = _bearer(headers)
    if token and tokens.enabled():
        try:
            claims = tokens.verify(token)
        except (tokens.JwtError, tokens.JwtConfigError):
            # 令牌坏了不该在这里报错，交给鉴权依赖给出一致的 401；
            # 这里只负责「这条请求算谁的额度」，算不出来就按匿名处理。
            claims = None
        if claims is not None:
            digest = _digest(f"{claims.tenant}\n{claims.subject}")
            return Identity(
                dimension=DIMENSION_KEY,
                digest=digest,
                label=f"令牌 {claims.subject}",
                quota=key_quota,
            )

    raw_key = (headers.get("x-api-key") or "").strip()
    if raw_key:
        scopes = tenancy.load_scopes()
        if raw_key in scopes:
            return Identity(
                dimension=DIMENSION_KEY,
                digest=_digest(raw_key),
                label=f"API Key {scopes[raw_key].label or _digest(raw_key)[:8]}",
                quota=key_quota,
            )
    return None


class RateLimiter:
    """把「谁」映射到「哪个桶」，并给出可回给调用方的结论。"""

    def __init__(
        self,
        key_quota: Quota,
        ip_quota: Quota,
        bucket: Optional[Bucket] = None,
        *,
        clock=time.monotonic,
        trust_forwarded: bool = False,
    ) -> None:
        self._key_quota = key_quota
        self._ip_quota = ip_quota
        self._bucket = bucket if bucket is not None else MemoryTokenBucket()
        self._clock = clock
        self._trust_forwarded = trust_forwarded
        self._warn_lock = threading.Lock()
        self._warned_at = 0.0
        self._suppressed = 0

    @property
    def enabled(self) -> bool:
        return self._key_quota.enabled or self._ip_quota.enabled

    @property
    def trust_forwarded(self) -> bool:
        return self._trust_forwarded

    def describe(self) -> str:
        """启动日志用的一句话摘要。"""
        if not self.enabled:
            return (
                "未启用（设置 XW_RATE_LIMIT_KEY_RPM 或 XW_RATE_LIMIT_IP_RPM 开启，"
                "两个维度都为 0 时不拦截）"
            )
        parts = []
        if self._key_quota.enabled:
            parts.append(f"凭据 {self._key_quota.rpm}/分钟·突发 {self._key_quota.burst}")
        if self._ip_quota.enabled:
            parts.append(f"IP {self._ip_quota.rpm}/分钟·突发 {self._ip_quota.burst}")
        backend = type(self._bucket).__name__
        return "、".join(parts) + f"；后端 {backend}"

    def identify(self, headers: Mapping[str, str], address: str) -> Optional[Identity]:
        """选出这次请求的限流身份；两个维度都关闭时返回 ``None``。"""
        if self._key_quota.enabled:
            identity = _credential_identity(headers, self._key_quota)
            if identity is not None:
                return identity
        if self._ip_quota.enabled and address:
            return Identity(
                dimension=DIMENSION_IP,
                digest=address,
                label=f"来源 {address}",
                quota=self._ip_quota,
            )
        return None

    def check(self, headers: Mapping[str, str], address: str) -> Optional[Decision]:
        """判定一次请求；返回 ``None`` 表示这一层没有可用的额度配置。"""
        try:
            identity = self.identify(headers, address)
        except Exception as exc:  # noqa: BLE001 —— 见下方对后端故障的同一取舍
            # 凭据表写坏时不要让限流中间件成为 500 的来源：鉴权那一步会给出
            # 自己的结论，这里只是「算不出该记谁的额度」。
            self._warn_degraded(exc)
            return None
        if identity is None:
            return None
        name = f"{identity.dimension}:{identity.digest}"
        try:
            allowed, remaining, retry = self._bucket.take(name, identity.quota, self._clock())
        except Exception as exc:  # noqa: BLE001 —— 后端故障一律放行，见模块文档
            self._warn_degraded(exc)
            return Decision(
                allowed=True,
                identity=identity,
                remaining=identity.quota.burst,
                degraded=True,
            )
        return Decision(
            allowed=allowed,
            identity=identity,
            remaining=remaining,
            retry_after=retry,
        )

    def _warn_degraded(self, exc: Exception) -> None:
        now = time.monotonic()
        with self._warn_lock:
            if now - self._warned_at < _WARN_INTERVAL:
                self._suppressed += 1
                return
            suppressed, self._suppressed = self._suppressed, 0
            self._warned_at = now
        logger.warning(
            "限流判定不可用（%s），请求按放行处理；此前 %d 次同类告警已合并", exc, suppressed
        )


def create_limiter() -> RateLimiter:
    """按配置装配限流器（读取环境变量在调用时生效，避免导入期固化）。"""
    return RateLimiter(
        make_quota(config.RATE_LIMIT_KEY_RPM, config.RATE_LIMIT_BURST),
        make_quota(config.RATE_LIMIT_IP_RPM, config.RATE_LIMIT_BURST),
        create_bucket(),
        trust_forwarded=bool(config.RATE_LIMIT_TRUST_FORWARDED),
    )


_limiter: Optional[RateLimiter] = None
_limiter_lock = threading.Lock()


def get_limiter() -> RateLimiter:
    """全局限流器单例（惰性初始化）。"""
    global _limiter
    with _limiter_lock:
        if _limiter is None:
            _limiter = create_limiter()
        return _limiter


def reset_limiter() -> None:
    """丢弃单例，下一次 ``get_limiter`` 按当前配置重建（测试与热更新用）。"""
    global _limiter
    with _limiter_lock:
        _limiter = None


__all__ = [
    "DIMENSION_IP",
    "DIMENSION_KEY",
    "Bucket",
    "Decision",
    "Identity",
    "MemoryTokenBucket",
    "Quota",
    "RateLimiter",
    "RedisTokenBucket",
    "client_ip",
    "create_bucket",
    "create_limiter",
    "get_limiter",
    "make_quota",
    "reset_limiter",
    "resolve_backend",
]
