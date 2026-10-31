"""渠道实现注册表：按渠道索引 Provider，并在同渠道内排出取用顺序。

一条通知按渠道分发时，需要问「这个渠道现在用哪个实现」。同一渠道可能有多个
实现并存：真实实现优先、console mock 兜底，或者两个都启用做灰度。注册表把
这些实现按渠道收在一处，用两个字段决定取用：

- ``priority``：数值小的先取用，缺省 :data:`~.base.DEFAULT_PRIORITY`；
- ``enabled``：为假的条目留在表里但不参与取用，便于运行期临时摘除再放回。

标识（``id``）是寻址键：启停、替换、日志都指向它。没给标识时按渠道自动编号
（第一个叫 ``email``，其后是 ``email-2``、``email-3``）；显式给了标识且已存在，
按原地替换处理并保留原有取用位置，这样「换掉某个实现」不会打乱顺序。

读写用一把可重入锁保护：投递路径上多个线程并发按渠道取用，管理路径随时可能
写入，取用返回的是条目快照，不会读到半更新的列表。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Iterator, Optional, Sequence

from ..models.channel import Channel
from .base import DEFAULT_PRIORITY, Provider


@dataclass(frozen=True)
class Entry:
    """注册表中的一条登记。"""

    id: str
    channel: Channel
    priority: int
    enabled: bool
    provider: Provider

    @property
    def kind(self) -> str:
        """实现种类（console / smtp / …），未声明时为空串。"""
        return self.provider.kind

    def describe(self) -> str:
        """一行摘要，用于启动日志与排查（不含任何凭据）。"""
        state = "enabled" if self.enabled else "disabled"
        return f"{self.id} channel={self.channel.value} kind={self.kind or '-'} priority={self.priority} {state}"


class ProviderRegistry:
    """按渠道索引的 Provider 注册表（线程安全）。"""

    def __init__(self, entries: Sequence[Entry] = ()) -> None:
        self._mu = threading.RLock()
        self._entries: list[Entry] = []
        self._position: dict[str, int] = {}
        for entry in entries:
            self.register(
                entry.provider,
                identifier=entry.id,
                priority=entry.priority,
                enabled=entry.enabled,
            )

    # ------------------------------------------------------------------ 写入

    def register(
        self,
        provider: Provider,
        *,
        identifier: str = "",
        priority: Optional[int] = None,
        enabled: bool = True,
    ) -> Entry:
        """登记一个渠道实现，返回落表后的条目。

        ``identifier`` 省略时优先用实现自带的标识，仍为空则按渠道自动编号。
        显式标识已存在时按原地替换处理；替换到别的渠道会报错，避免一个标识
        在两个渠道间漂移。
        """
        if not isinstance(provider, Provider):
            raise TypeError(f"登记的实现必须继承 Provider：{provider!r}")
        explicit = (identifier or provider.identifier).strip()
        value = provider.default_priority if priority is None else int(priority)
        if value < 0:
            raise ValueError(f"优先级不能为负数：{value}")

        with self._mu:
            entry_id = explicit or self._unique_id(provider.channel)
            entry = Entry(
                id=entry_id,
                channel=provider.channel,
                priority=value,
                enabled=bool(enabled),
                provider=provider,
            )
            index = self._position.get(entry_id)
            if index is not None:
                current = self._entries[index]
                if current.channel != entry.channel:
                    raise ValueError(
                        f"标识 {entry_id!r} 已登记在渠道 {current.channel.value!r}，"
                        f"不能改为 {entry.channel.value!r}"
                    )
                # 原地替换：位置不变，因此同优先级下的相对顺序也不变。
                self._entries[index] = entry
                return entry
            self._position[entry_id] = len(self._entries)
            self._entries.append(entry)
            return entry

    def set_enabled(self, identifier: str, enabled: bool) -> Entry:
        """启用或停用一个条目（条目不参与取用但保留在表里）。"""
        with self._mu:
            index = self._position.get(identifier.strip())
            if index is None:
                raise KeyError(f"未登记的标识：{identifier!r}")
            current = self._entries[index]
            entry = Entry(
                id=current.id,
                channel=current.channel,
                priority=current.priority,
                enabled=bool(enabled),
                provider=current.provider,
            )
            self._entries[index] = entry
            return entry

    def remove(self, identifier: str) -> Entry:
        """摘除一个条目并返回它；未登记时抛 ``KeyError``。"""
        with self._mu:
            index = self._position.pop(identifier.strip(), None)
            if index is None:
                raise KeyError(f"未登记的标识：{identifier!r}")
            entry = self._entries.pop(index)
            for position, remaining in enumerate(self._entries):
                self._position[remaining.id] = position
            return entry

    # ------------------------------------------------------------------ 读取

    def get(self, channel: Channel) -> Optional[Provider]:
        """返回该渠道当前应使用的实现（优先级最高且启用）；没有则 ``None``。"""
        for entry in self.entries():
            if entry.channel == channel and entry.enabled:
                return entry.provider
        return None

    def candidates(self, channel: Channel) -> list[Provider]:
        """返回该渠道按优先级排好的启用实现，供主备切换依次尝试。"""
        return [entry.provider for entry in self.entries() if entry.channel == channel and entry.enabled]

    def entry(self, identifier: str) -> Optional[Entry]:
        """按标识取条目。"""
        with self._mu:
            index = self._position.get(identifier.strip())
            return None if index is None else self._entries[index]

    def entries(self) -> tuple[Entry, ...]:
        """返回全部条目的快照，按取用顺序排列（渠道内按优先级、同级按登记顺序）。"""
        with self._mu:
            snapshot = tuple(self._entries)
        order = {entry.id: index for index, entry in enumerate(snapshot)}
        return tuple(sorted(snapshot, key=lambda e: (e.priority, order[e.id])))

    def channels(self) -> list[Channel]:
        """返回当前有启用实现的渠道。"""
        seen: list[Channel] = []
        for entry in self.entries():
            if entry.enabled and entry.channel not in seen:
                seen.append(entry.channel)
        return seen

    def missing_channels(self) -> list[Channel]:
        """返回没有任何启用实现的可投递渠道。"""
        ready = set(self.channels())
        return [channel for channel in Channel.delivery_channels() if channel not in ready]

    def summary(self) -> str:
        """多行摘要，供启动日志打印注册表内容。"""
        entries = self.entries()
        if not entries:
            return "（注册表为空）"
        return "\n".join(f"- {entry.describe()}" for entry in entries)

    # ------------------------------------------------------------------ 其它

    def __len__(self) -> int:
        with self._mu:
            return len(self._entries)

    def __contains__(self, identifier: object) -> bool:
        if not isinstance(identifier, str):
            return False
        with self._mu:
            return identifier.strip() in self._position

    def __iter__(self) -> Iterator[Entry]:
        return iter(self.entries())

    def _unique_id(self, channel: Channel) -> str:
        base = channel.value
        if base not in self._position:
            return base
        suffix = 2
        while f"{base}-{suffix}" in self._position:
            suffix += 1
        return f"{base}-{suffix}"


__all__ = ["Entry", "ProviderRegistry"]
