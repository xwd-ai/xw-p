"""渠道 Provider 层：投递契约、注册表与装配工厂。

- :mod:`.base`：:class:`Provider` 抽象基类，收口收件人寻址、结果结构与耗时打点；
- :mod:`.console`：console mock 实现，把投递打印成结构化日志；
- :mod:`.sms`：短信的号码规整、计费分段，以及会校验号码的 console 短信实现；
- :mod:`.smtp`：SMTP 实现，把 email 真正提交到中继服务器；
- :mod:`.twilio`：Twilio 风格的短信网关实现（Messages 资源契约）；
- :mod:`.push`：推送的报文契约（FCM HTTP v1 与 APNs token 认证）与设备 token 处理；
- :mod:`.push_mock`：推送的 mock 实现，按同一套契约校验后只留痕、不联网；
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
from .push import (
    DeviceTokenError,
    PayloadError,
    Platform,
    PushConfigError,
    PushMessage,
)
from .push_mock import MockPushProvider, PushRecord
from .registry import Entry, ProviderRegistry
from .sms import (
    ConsoleSMSProvider,
    PhoneNumberError,
    normalize_phone,
    sms_segments,
    to_e164,
)
from .smtp import DEFAULT_TIMEOUT as SMTP_DEFAULT_TIMEOUT
from .smtp import SMTPConfigError, SMTPProvider
from .twilio import TwilioConfigError, TwilioSMSProvider

__all__ = [
    "ADDRESS_FIELDS",
    "DEFAULT_PRIORITY",
    "KIND_SPECS",
    "SUPPORTED_KINDS",
    "BuildReport",
    "ConsoleProvider",
    "ConsoleSMSProvider",
    "Delivery",
    "DeliveryResult",
    "DeviceTokenError",
    "Entry",
    "KindSpec",
    "MockPushProvider",
    "PayloadError",
    "PhoneNumberError",
    "Platform",
    "Provider",
    "ProviderConfig",
    "ProviderConfigError",
    "ProviderRegistry",
    "PushConfigError",
    "PushMessage",
    "PushRecord",
    "Recipient",
    "SMTPConfigError",
    "SMTPProvider",
    "SMTP_DEFAULT_TIMEOUT",
    "TwilioConfigError",
    "TwilioSMSProvider",
    "build_registry",
    "create_registry",
    "default_configs",
    "get_registry",
    "load_configs",
    "normalize_phone",
    "parse_configs",
    "set_registry",
    "sms_segments",
    "to_e164",
    "validate_config",
]
