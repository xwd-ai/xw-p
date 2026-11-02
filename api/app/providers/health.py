"""渠道实现的健康检查。

一条通知发不出去，排查时第一个要回答的问题是「这个渠道现在是通的吗」。本模块
把这个问题拆成两层，避免把「配置对不对」和「对端通不通」混在一起：

1. **配置检查**（默认）：只看注册表里的登记状态——是否启用、是不是该渠道
   当前的首选取用实现。它不产生任何网络调用，因此可以随便调，适合列表页。
2. **在线探测**（``deep=True``）：交给实现自己的 ``probe``。有外部依赖的实现
   （SMTP 中继、站内信文件）在这里各查各的：SMTP 建立一次 TCP 连接即断开，
   站内信确认目录可写。没有外部依赖的实现（console、推送 mock）直接报
   「按配置检查通过」，不假装做了一次探测——``probed`` 字段如实区分这两种情况。

探测结果按条目缓存 ``XW_PROVIDER_HEALTH_TTL``（默认 30 秒）：列表页轮询不会
每次都去连中继，想看当前状态可以带 ``refresh`` 跳过缓存。缓存键里带着条目
快照，启停之后条目被替换，旧结果自然失效，不需要额外清理。

探测失败不抛异常，一律折成 :class:`HealthState` 与一句原因：健康检查本身
不该成为新的故障点。凭据不进日志，也不进返回内容（实现的 ``probe`` 自己负责）。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Iterable, Optional, Protocol, Sequence, Union

from .. import config
from ..models.channel import Channel
from .registry import Entry, ProviderRegistry

logger = logging.getLogger("xw.provider.health")

#: 单次在线探测的默认预算（秒）。
DEFAULT_PROBE_TIMEOUT = 2.0

#: 探测结果的默认缓存时长（秒）；0 表示不缓存。
DEFAULT_TTL = 30.0


class HealthState(str, Enum):
    """渠道实现的可用状态。"""

    #: 可用（配置检查通过，或探测通过）。
    OK = "ok"
    #: 能连上但有不完全确定的部分（例如 TCP 可达、认证尚未验证）。
    DEGRADED = "degraded"
    #: 明确不可用（连不上、目录不可写）。
    UNAVAILABLE = "unavailable"
    #: 被停用，不参与取用。
    DISABLED = "disabled"

    @property
    def healthy(self) -> bool:
        """是否还能承接投递（``ok`` 与 ``degraded`` 都算）。"""
        return self in (HealthState.OK, HealthState.DEGRADED)


@dataclass(frozen=True)
class ProbeResult:
    """一次在线探测的结果。"""

    state: HealthState
    detail: str

    @classmethod
    def ok(cls, detail: str) -> "ProbeResult":
        return cls(HealthState.OK, detail)

    @classmethod
    def degraded(cls, detail: str) -> "ProbeResult":
        return cls(HealthState.DEGRADED, detail)

    @classmethod
    def unavailable(cls, detail: str) -> "ProbeResult":
        return cls(HealthState.UNAVAILABLE, detail)


class Prober(Protocol):
    """可做在线探测的实现所需的最小接口。"""

    def probe(self, timeout: float) -> ProbeResult:
        """在 ``timeout`` 秒内判断自己是否可用；结果用 :class:`ProbeResult` 表达。"""
        ...


@dataclass(frozen=True)
class ProviderHealth:
    """一个渠道实现在某一时刻的健康快照。"""

    id: str
    channel: Channel
    kind: str
    enabled: bool
    priority: int
    #: 是否为该渠道当前的首选取用实现（停用的条目不可能是首选）。
    primary: bool
    state: HealthState
    detail: str
    #: 是否真的执行了在线探测；``False`` 表示结论只来自配置检查。
    probed: bool
    latency_ms: int
    checked_at: datetime
    cached: bool = False

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "channel": self.channel.value,
            "kind": self.kind,
            "enabled": self.enabled,
            "priority": self.priority,
            "primary": self.primary,
            "state": self.state.value,
            "detail": self.detail,
            "probed": self.probed,
            "latency_ms": self.latency_ms,
            "checked_at": self.checked_at.isoformat(),
            "cached": self.cached,
        }


def probe_provider(provider: object, timeout: float = DEFAULT_PROBE_TIMEOUT) -> ProbeResult:
    """调用实现的 ``probe``；没有探测能力时按配置检查通过。

    实现里抛出的异常在这里兜住：探测失败是正常结论，不该把健康检查变成
    异常来源。
    """
    method = getattr(provider, "probe", None)
    if not callable(method):
        return ProbeResult.ok("按配置检查通过（该实现不提供在线探测）")
    budget = timeout if timeout and timeout > 0 else DEFAULT_PROBE_TIMEOUT
    try:
        result = method(budget)
    except Exception as exc:  # 探测过程中的任何异常都折成不可用
        return ProbeResult.unavailable(f"探测失败：{type(exc).__name__}: {exc}")
    if isinstance(result, ProbeResult):
        detail = result.detail.strip() or "探测未给出说明"
        return ProbeResult(result.state, detail)
    if isinstance(result, bool):  # 容忍只返回布尔值的实现
        return ProbeResult(HealthState.OK if result else HealthState.UNAVAILABLE, "探测返回布尔结果")
    return ProbeResult.unavailable(f"探测返回值不可识别：{type(result).__name__}")


RegistrySource = Union[ProviderRegistry, Callable[[], ProviderRegistry]]


class HealthMonitor:
    """按条目做健康检查，并按 TTL 缓存在线探测的结果。"""

    def __init__(
        self,
        registry: Optional[RegistrySource] = None,
        *,
        ttl: Optional[float] = None,
        timeout: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
        now: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._source = registry
        self._ttl = float(config.PROVIDER_HEALTH_TTL if ttl is None else ttl)
        self._timeout = float(config.PROVIDER_HEALTH_TIMEOUT if timeout is None else timeout)
        self._clock = clock
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._cache: dict[tuple[str, bool], tuple[Entry, float, ProviderHealth]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ 装配

    @property
    def registry(self) -> ProviderRegistry:
        """当前注册表；装配时给的是工厂函数则每次取用时重新解析。"""
        source = self._source
        if source is None:
            # 延迟导入：factory 依赖各渠道实现，实现又依赖本模块，顶层导入会成环。
            from . import factory

            return factory.get_registry()
        if isinstance(source, ProviderRegistry):
            return source
        return source()

    @property
    def ttl(self) -> float:
        return self._ttl

    @property
    def timeout(self) -> float:
        return self._timeout

    def bind(self, registry: RegistrySource) -> None:
        """替换注册表来源（测试与热更新用），同时清空缓存。"""
        with self._lock:
            self._source = registry
            self._cache.clear()

    def invalidate(self, identifier: Optional[str] = None) -> None:
        """清掉某个条目（或全部）的缓存，启停之后调用。"""
        with self._lock:
            if identifier is None:
                self._cache.clear()
                return
            for key in [key for key in self._cache if key[0] == identifier]:
                self._cache.pop(key, None)

    # ------------------------------------------------------------------ 检查

    def check(
        self,
        *,
        deep: bool = False,
        refresh: bool = False,
        identifiers: Optional[Iterable[str]] = None,
        channel: Optional[Channel] = None,
    ) -> list[ProviderHealth]:
        """检查注册表里的条目，按取用顺序返回。

        ``identifiers`` 用于只查几条（请求的是没登记的标识时不报错，由调用方
        自行判断）；顺序与注册表一致，不随入参顺序变化。
        """
        registry = self.registry
        wanted = None if identifiers is None else {str(i).strip() for i in identifiers}
        items: list[ProviderHealth] = []
        for entry in registry.entries():
            if wanted is not None and entry.id not in wanted:
                continue
            if channel is not None and entry.channel != channel:
                continue
            items.append(self._check_entry(registry, entry, deep=deep, refresh=refresh))
        return items

    def check_one(
        self,
        identifier: str,
        *,
        deep: bool = False,
        refresh: bool = False,
    ) -> Optional[ProviderHealth]:
        """检查单个条目；未登记时返回 ``None``。"""
        items = self.check(deep=deep, refresh=refresh, identifiers=[identifier])
        return items[0] if items else None

    # ------------------------------------------------------------------ 内部

    def _check_entry(
        self,
        registry: ProviderRegistry,
        entry: Entry,
        *,
        deep: bool,
        refresh: bool,
    ) -> ProviderHealth:
        started_at = self._clock()
        if not refresh and self._ttl > 0:
            hit = self._lookup(entry, deep, started_at)
            if hit is not None:
                return hit

        primary = entry.enabled and _is_primary(registry, entry)
        if not entry.enabled:
            health = self._make(
                entry,
                primary=False,
                state=HealthState.DISABLED,
                detail="已停用，不参与取用",
                probed=False,
                latency_ms=0,
            )
        elif not deep:
            health = self._make(
                entry,
                primary=primary,
                state=HealthState.OK,
                detail="按配置检查通过（未做在线探测）",
                probed=False,
                latency_ms=0,
            )
        else:
            began = time.perf_counter()
            result = probe_provider(entry.provider, self._timeout)
            latency = max(0, int((time.perf_counter() - began) * 1000))
            health = self._make(
                entry,
                primary=primary,
                state=result.state,
                detail=result.detail,
                probed=True,
                latency_ms=latency,
            )

        if self._ttl > 0:
            with self._lock:
                self._cache[(entry.id, deep)] = (entry, started_at + self._ttl, health)
        return health

    def _lookup(self, entry: Entry, deep: bool, now_mono: float) -> Optional[ProviderHealth]:
        with self._lock:
            hit = self._cache.get((entry.id, deep))
        if hit is None:
            return None
        cached_entry, expires_at, health = hit
        # 条目被替换（启停、优先级变更）后旧结论不再适用。
        if cached_entry != entry or now_mono >= expires_at:
            return None
        return replace(health, cached=True)

    def _make(
        self,
        entry: Entry,
        *,
        primary: bool,
        state: HealthState,
        detail: str,
        probed: bool,
        latency_ms: int,
    ) -> ProviderHealth:
        return ProviderHealth(
            id=entry.id,
            channel=entry.channel,
            kind=entry.kind,
            enabled=entry.enabled,
            priority=entry.priority,
            primary=primary,
            state=state,
            detail=detail,
            probed=probed,
            latency_ms=latency_ms,
            checked_at=self._now(),
        )


def _is_primary(registry: ProviderRegistry, entry: Entry) -> bool:
    """该条目是不是所在渠道当前的首选取用实现。"""
    return registry.get(entry.channel) is entry.provider


def summarize(items: Sequence[ProviderHealth]) -> dict:
    """按状态汇总，供列表接口的头部数字使用。"""
    counts = {state.value: 0 for state in HealthState}
    for item in items:
        counts[item.state.value] += 1
    return {
        "total": len(items),
        "enabled": sum(1 for item in items if item.enabled),
        "probed": sum(1 for item in items if item.probed),
        "cached": sum(1 for item in items if item.cached),
        **counts,
    }


_monitor: Optional[HealthMonitor] = None
_monitor_lock = threading.Lock()


def get_monitor() -> HealthMonitor:
    """获取全局健康检查器（首次取用时构造，注册表按需解析）。"""
    global _monitor
    with _monitor_lock:
        if _monitor is None:
            _monitor = HealthMonitor()
        return _monitor


def set_monitor(monitor: Optional[HealthMonitor]) -> None:
    """替换全局健康检查器；传 ``None`` 表示下次取用时重新构造（测试用）。"""
    global _monitor
    with _monitor_lock:
        _monitor = monitor


__all__ = [
    "DEFAULT_PROBE_TIMEOUT",
    "DEFAULT_TTL",
    "HealthMonitor",
    "HealthState",
    "ProbeResult",
    "Prober",
    "ProviderHealth",
    "get_monitor",
    "probe_provider",
    "set_monitor",
    "summarize",
]
