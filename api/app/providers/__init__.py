"""渠道 Provider 层：投递契约、注册表与装配工厂。

- :mod:`.base`：:class:`Provider` 抽象基类，收口收件人寻址、结果结构与耗时打点；
- :mod:`.console`：console mock 实现，把投递打印成结构化日志；
- :mod:`.smtp`：SMTP 实现，把 email 真正提交到中继服务器；
- :mod:`.registry`：按渠道索引实现，按优先级取用，支持启停与原地替换；
- :mod:`.factory`：读取与引擎共用的渠道配置，校验后装配注册表。

投递结果的两种形态（成功 / 带原因的失败）由 :class:`DeliveryResult` 表达，
渠道实现不向上抛异常。
"""
from __future__ import annotations

from .base import (
    ADDRESS_FIELDS,
    DEFAULT_PRIORITY,
    Delivery,
    DeliveryResult,
    Provider,
    Recipient,
)
from .console import ConsoleProvider
from .factory import (
    KIND_SPECS,
    SUPPORTED_KINDS,
    BuildReport,
    KindSpec,
    ProviderConfig,
    ProviderConfigError,
    build_registry,
    create_registry,
    default_configs,
    get_registry,
    load_configs,
    parse_configs,
    set_registry,
    validate_config,
)
from .registry import Entry, ProviderRegistry
from .smtp import DEFAULT_TIMEOUT as SMTP_DEFAULT_TIMEOUT
from .smtp import SMTPConfigError, SMTPProvider

__all__ = [
    "ADDRESS_FIELDS",
    "DEFAULT_PRIORITY",
    "KIND_SPECS",
    "SUPPORTED_KINDS",
    "BuildReport",
    "ConsoleProvider",
    "Delivery",
    "DeliveryResult",
    "Entry",
    "KindSpec",
    "Provider",
    "ProviderConfig",
    "ProviderConfigError",
    "ProviderRegistry",
    "Recipient",
    "SMTPConfigError",
    "SMTPProvider",
    "SMTP_DEFAULT_TIMEOUT",
    "build_registry",
    "create_registry",
    "default_configs",
    "get_registry",
    "load_configs",
    "parse_configs",
    "set_registry",
    "validate_config",
]
