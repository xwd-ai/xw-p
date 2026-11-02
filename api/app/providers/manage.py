"""运行期渠道配置的增删改：校验、构造实现、登记，可选写回配置文件。

注册表是投递路径上的读模型（按渠道取用实现），配置簿是管理路径上的写模型
（保存凭据与来源）。本模块把两者接在一起，并守住三条不变量：

1. **先校验再落表**：一条配置要同时通过形状校验（:func:`~.factory.validate_config`）
   与构造校验（凭据能否撑起实现）。任何一步失败都不会留下半个条目。
2. **标识是寻址键**：显式 ``id`` 已存在时新增按冲突处理，不做隐式覆盖；修改
   走 :func:`update_config`，只改传进来的字段，其余保持原样。渠道不可改——把
   一个标识从 email 挪到 sms 会让执行明细里的历史记录指向另一件事，确实要换
   就删除后重建。
3. **文件写在内存前面**：``persist=True`` 时先写配置文件再改注册表，写文件失败
   则整次请求失败、内存状态不变，避免「接口说改好了、重启又变回去」。

凭据只在这一层出现。响应里走 :func:`mask_credentials` 脱敏，日志里只出现条目
标识与凭据键名。
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Optional

from . import factory
from .factory import ConfigBook, ConfigRecord, ProviderConfig, ProviderConfigError
from .registry import Entry, ProviderRegistry

logger = logging.getLogger("xw.provider.manage")

#: 名字里带这些片段的凭据视为敏感项，回显时只给掩码。
#: 判定偏保守——多遮几个不影响排查，漏遮一个就是把口令发到了浏览器。
_SECRET_HINTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "webhook",
    "credential",
    "auth",
    "private",
    "key",
)

#: 敏感项在响应里的占位值。
MASK = "***"

#: 增删改串行执行：注册表本身线程安全，但「校验 → 写文件 → 登记」这一串不是
#: 原子操作，两个并发的新增可能各自通过重名检查后互相覆盖。
_lock = threading.RLock()


class ProviderNotFoundError(LookupError):
    """按标识找不到已登记的渠道实现。"""


class ProviderConflictError(ValueError):
    """请求与现有状态冲突（标识重复、试图改渠道）。"""


@dataclass(frozen=True)
class MutationResult:
    """一次增删改的结果。"""

    entry: Entry
    config: Optional[ProviderConfig]
    created: bool = False
    changed: bool = True
    #: 实际写入的配置文件；只作用于当前进程时为 ``None``。
    persisted: Optional[Path] = None
    #: 给请求方的一句提示（例如渠道已经没有实现承接）。
    warning: Optional[str] = None


def is_secret(name: str) -> bool:
    """该凭据项是否需要脱敏。"""
    lowered = name.strip().lower()
    return any(hint in lowered for hint in _SECRET_HINTS)


def mask_credentials(credentials: Mapping[str, str]) -> dict[str, str]:
    """按键名脱敏，键顺序固定，便于对比两次响应。"""
    masked: dict[str, str] = {}
    for name in sorted(credentials):
        value = str(credentials[name])
        masked[name] = MASK if is_secret(name) and value.strip() else value
    return masked


def secret_keys(credentials: Mapping[str, str]) -> list[str]:
    """被脱敏的键名列表（值是空的项不算）。"""
    return [name for name in sorted(credentials) if is_secret(name) and str(credentials[name]).strip()]


def describe_config(record: ConfigRecord) -> dict:
    """配置簿里一条记录的可回显形态（凭据已脱敏）。"""
    cfg: ProviderConfig = record.config
    return {
        "id": record.id,
        "channel": cfg.channel.value,
        "kind": cfg.kind,
        "priority": cfg.priority,
        "enabled": cfg.enabled,
        "credentials": mask_credentials(cfg.credentials),
        "secretKeys": secret_keys(cfg.credentials),
        "source": record.source,
    }


def warning_for(registry: ProviderRegistry, entry: Entry, *, removed: bool = False) -> Optional[str]:
    """增删改之后给请求方的一句提示：这个渠道还有没有人接、接下来由谁接。

    ``removed=True`` 用于删除：条目已经不在了，即便它原本是启用的，也要重新
    判断渠道还有没有别的实现承接。
    """
    if entry.enabled and not removed:
        return None
    if registry.get(entry.channel) is None:
        return f"渠道 {entry.channel.value} 已没有启用的实现，投递会失败"
    for candidate in registry.entries():
        if candidate.channel == entry.channel and candidate.enabled:
            return f"渠道 {entry.channel.value} 改由 {candidate.id} 承接"
    return None


def _registry(registry: Optional[ProviderRegistry]) -> ProviderRegistry:
    return factory.get_registry() if registry is None else registry


def _book(book: Optional[ConfigBook]) -> ConfigBook:
    return factory.get_book() if book is None else book


def _baseline(entry: Entry, record: Optional[ConfigRecord]) -> ProviderConfig:
    """修改时的基线配置：状态取自注册表，凭据取自配置簿。

    两者可能不同步——直接调 ``registry.set_enabled`` 的路径只改注册表，配置簿
    停在旧值。状态（渠道、种类、优先级、启停）以注册表为准，否则「再停用一次」
    会被误判成没有变化；凭据只有配置簿知道，因此在种类与渠道都对得上时才
    取用，对不上宁可当作未知，也不把另一份配置的口令套到这个实现上。
    """
    credentials: dict[str, str] = {}
    if record is not None and record.config.kind == entry.kind and record.config.channel == entry.channel:
        credentials = dict(record.config.credentials)
    return ProviderConfig(
        channel=entry.channel,
        kind=entry.kind,
        id=entry.id,
        priority=entry.priority,
        enabled=entry.enabled,
        credentials=credentials,
    )


def create_config(
    config: ProviderConfig,
    *,
    persist: bool = False,
    registry: Optional[ProviderRegistry] = None,
    book: Optional[ConfigBook] = None,
) -> MutationResult:
    """新增一条渠道配置并登记实现。

    ``config.id`` 为空时由注册表按渠道自动编号（``email``、``email-2``…）；
    这种条目无法写回配置文件——文件里的条目要能按 ``id`` 定位，所以
    ``persist=True`` 必须带显式标识。
    """
    reg = _registry(registry)
    bk = _book(book)
    identifier = config.id.strip()
    if persist and not identifier:
        raise ProviderConfigError("写回配置文件需要显式 id")

    with _lock:
        if identifier:
            existing = reg.entry(identifier)
            if existing is not None:
                raise ProviderConflictError(
                    f"标识 {identifier!r} 已登记在渠道 {existing.channel.value}"
                    f"（kind={existing.kind or '-'}），修改请用 PUT /v1/providers/{identifier}"
                )
        # 校验与构造都在落表之前：凭据撑不起实现时不会有半条记录留下。
        provider = factory.build_provider(config)
        persisted = factory.persist_config(config) if persist else None
        try:
            entry = reg.register(
                provider,
                identifier=identifier,
                priority=config.priority,
                enabled=config.enabled,
            )
        except ValueError as exc:  # 并发下标识被抢先登记到别的渠道
            raise ProviderConflictError(str(exc)) from exc
        record = bk.put(entry.id, replace(config, id=entry.id))

    logger.info(
        "新增渠道配置：%s（channel=%s kind=%s priority=%d enabled=%s）",
        entry.id,
        entry.channel.value,
        entry.kind or "-",
        entry.priority,
        entry.enabled,
    )
    return MutationResult(
        entry=entry,
        config=record.config,
        created=True,
        persisted=persisted,
        warning=warning_for(reg, entry),
    )


def update_config(
    identifier: str,
    *,
    kind: Optional[str] = None,
    priority: Optional[int] = None,
    enabled: Optional[bool] = None,
    credentials: Optional[Mapping[str, object]] = None,
    persist: bool = False,
    persist_upsert: bool = True,
    registry: Optional[ProviderRegistry] = None,
    book: Optional[ConfigBook] = None,
) -> MutationResult:
    """修改一条渠道配置；只改传入的字段。

    ``credentials`` 传入时整组替换而不是逐键合并：删掉一个凭据项（例如把
    ``reply_to`` 去掉）与改一个值同样常见，合并语义下前者做不到。没传则保持
    原样。``kind`` 变更会按新种类重新构造实现，同一标识的取用位置不变。
    """
    reg = _registry(registry)
    bk = _book(book)
    with _lock:
        entry = reg.entry(identifier)
        if entry is None:
            raise ProviderNotFoundError(f"未登记的渠道实现：{identifier}")

        record = bk.get(entry.id)
        base = _baseline(entry, record)

        new_kind = base.kind if kind is None else kind.strip()
        if not new_kind:
            raise ProviderConfigError(f"{entry.id}：kind 不能为空")
        new_priority = base.priority if priority is None else int(priority)
        if new_priority < 0:
            raise ProviderConfigError(f"{entry.id}：priority 不能为负数")
        new_enabled = base.enabled if enabled is None else bool(enabled)
        new_credentials = (
            dict(base.credentials)
            if credentials is None
            else {str(key): str(value) for key, value in credentials.items()}
        )

        candidate = ProviderConfig(
            channel=entry.channel,
            kind=new_kind,
            id=entry.id,
            priority=new_priority,
            enabled=new_enabled,
            credentials=new_credentials,
        )
        if candidate == base:
            return MutationResult(
                entry=entry,
                config=base,
                created=False,
                changed=False,
                warning=warning_for(reg, entry),
            )

        provider = factory.build_provider(candidate)
        persisted = (
            factory.persist_config(candidate, upsert=persist_upsert) if persist else None
        )
        entry = reg.register(
            provider,
            identifier=entry.id,
            priority=candidate.priority,
            enabled=candidate.enabled,
        )
        record = bk.put(entry.id, candidate)

    logger.info(
        "修改渠道配置：%s（kind=%s priority=%d enabled=%s，凭据项 %s）",
        entry.id,
        entry.kind or "-",
        entry.priority,
        entry.enabled,
        "、".join(sorted(candidate.credentials)) or "无",
    )
    return MutationResult(
        entry=entry,
        config=record.config,
        created=False,
        persisted=persisted,
        warning=warning_for(reg, entry),
    )


def delete_config(
    identifier: str,
    *,
    persist: bool = False,
    registry: Optional[ProviderRegistry] = None,
    book: Optional[ConfigBook] = None,
) -> MutationResult:
    """删除一条渠道配置（连同实现）。

    这是唯一会让条目消失的操作，因此 ``warning`` 会说明该渠道是否还有实现
    承接。``persist=True`` 时从配置文件里移除同名条目；文件里本来就没有它
    （运行期新增、从未写回）不算失败。
    """
    reg = _registry(registry)
    bk = _book(book)
    with _lock:
        entry = reg.entry(identifier)
        if entry is None:
            raise ProviderNotFoundError(f"未登记的渠道实现：{identifier}")
        record = bk.get(entry.id)
        persisted: Optional[Path] = None
        if persist:
            persisted, removed = factory.persist_removal(entry.id)
            if not removed:
                logger.info("配置文件里没有 %s，删除只作用于当前进程", entry.id)
        reg.remove(entry.id)
        bk.drop(entry.id)

    logger.info("删除渠道配置：%s（配置来源 %s）", entry.id, record.source if record else "-")
    return MutationResult(
        entry=entry,
        config=record.config if record is not None else None,
        created=False,
        persisted=persisted,
        warning=warning_for(reg, entry, removed=True),
    )


__all__ = [
    "MASK",
    "MutationResult",
    "ProviderConflictError",
    "ProviderNotFoundError",
    "create_config",
    "delete_config",
    "describe_config",
    "is_secret",
    "mask_credentials",
    "secret_keys",
    "update_config",
    "warning_for",
]
