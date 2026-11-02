"""渠道配置与工厂：把配置文件里的条目装配成注册表。

配置文件与引擎侧共用一份（``XW_PROVIDERS_FILE``，JSON），字段一致：
``id`` / ``channel`` / ``kind`` / ``priority`` / ``enabled`` / ``credentials``。
这样同一份「哪个渠道走哪种实现」的约定不需要在两层各写一遍，API 进程与引擎
对渠道的理解不会漂移。

装配分三步，职责分开：

1. :func:`parse_configs` 只做形状校验（字段名、类型、枚举取值），不判断
   「这个种类能不能用」——形状错了是配置写错，与实现是否接入无关。
2. :func:`validate_config` 按 :data:`KIND_SPECS` 校验渠道约束与凭据：种类与渠道
   是否匹配、必填凭据是否齐、有没有拼错的凭据项。问题一次列全，而不是修一个
   报一个。
3. :func:`build_registry` 构造实现并登记。尚未接入的种类（短信网关、推送等）
   跳过并记进报告，其余照常装配；某个渠道最终没有任何启用实现时同样写进报告。

当前接入的实现是 ``console``（含短信专用的号码校验版本）、``inapp``、``smtp``、
``twilio`` 与 ``push-mock``：console 把所有渠道的投递打进行日志，inapp 把站内信
写进订阅者的消息中心，smtp 把 email 提交到中继，twilio 按 Messages 资源契约提交
短信，push-mock 按 FCM / APNs 的报文契约校验后只记录不联网。其余种类的配置会被
识别、校验、跳过，等对应实现落地后只需往 :data:`BUILDERS` 里加一项。
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Mapping, Optional

from .. import config
from ..message_center import MessageError, create_store
from ..models.channel import Channel
from .base import DEFAULT_PRIORITY, Provider
from .console import ConsoleProvider
from .inapp import InAppProvider
from .push_mock import DEFAULT_CAPACITY as PUSH_MOCK_CAPACITY
from .push_mock import MockPushProvider
from .push import PushConfigError
from .registry import Entry, ProviderRegistry
from .sms import ConsoleSMSProvider
from .smtp import SMTPConfigError, SMTPProvider
from .twilio import TwilioConfigError, TwilioSMSProvider

logger = logging.getLogger("xw.provider.factory")


class ProviderConfigError(ValueError):
    """渠道配置不合法（形状、渠道约束或凭据问题）。"""


@dataclass(frozen=True)
class KindSpec:
    """一种实现种类允许的渠道与凭据。

    ``required`` 是缺了就跑不起来的项，``optional`` 是可选配置；两者之外的键
    一律视为拼写错误——把 ``password`` 写成 ``passwd`` 时，最坏的结果不是启动
    失败，而是投递那一刻才报认证失败，所以在装配阶段就指出来。
    """

    channels: tuple[Channel, ...]
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()


_DELIVERY = tuple(Channel.delivery_channels())

KIND_SPECS: dict[str, KindSpec] = {
    "console": KindSpec(channels=_DELIVERY),
    "inapp": KindSpec(channels=(Channel.IN_APP,)),
    "smtp": KindSpec(
        channels=(Channel.EMAIL,),
        required=("host", "from"),
        optional=("port", "username", "password", "tls", "reply_to"),
    ),
    "twilio": KindSpec(
        channels=(Channel.SMS,),
        required=("account_sid", "auth_token", "from"),
        optional=("endpoint", "default_country_code", "status_callback", "timeout"),
    ),
    "fcm": KindSpec(
        channels=(Channel.PUSH,),
        required=("project_id", "access_token"),
        optional=("endpoint",),
    ),
    "apns": KindSpec(
        channels=(Channel.PUSH,),
        required=("key_id", "team_id", "private_key"),
        optional=("bundle_id", "sandbox", "endpoint"),
    ),
    "push-mock": KindSpec(
        channels=(Channel.PUSH,),
        optional=("platform", "project_id", "topic", "fail_with", "capacity"),
    ),
    "slack": KindSpec(channels=(Channel.CHAT,), required=("webhook_url",)),
    "teams": KindSpec(channels=(Channel.CHAT,), required=("webhook_url",)),
}

# 配置项允许的字段，多写一个都视为拼错。
_CONFIG_FIELDS = ("id", "channel", "kind", "priority", "enabled", "credentials")


@dataclass(frozen=True)
class ProviderConfig:
    """一条渠道配置（配置文件里的一项）。"""

    channel: Channel
    kind: str
    id: str = ""
    priority: int = DEFAULT_PRIORITY
    enabled: bool = True
    credentials: Mapping[str, str] = field(default_factory=dict)

    @property
    def label(self) -> str:
        """日志与错误信息里的称呼：优先用显式标识，否则 ``kind:channel``。"""
        return self.id or f"{self.kind}:{self.channel.value}"


def _build_console(cfg: ProviderConfig) -> Provider:
    # 短信走专用实现：号码格式与计费段数在这里就判定，而不是无条件报成功。
    if cfg.channel is Channel.SMS:
        return ConsoleSMSProvider(cfg.channel, identifier=cfg.id, priority=cfg.priority)
    return ConsoleProvider(cfg.channel, identifier=cfg.id, priority=cfg.priority)


def _build_smtp(cfg: ProviderConfig) -> Provider:
    """构造 SMTP 实现；凭据组合不合法时转成配置错误而不是启动崩溃。"""
    try:
        return SMTPProvider.from_credentials(
            cfg.channel,
            cfg.credentials,
            identifier=cfg.id,
            priority=cfg.priority,
        )
    except SMTPConfigError as exc:
        raise ProviderConfigError(f"{cfg.label}：{exc}") from exc


def _build_twilio(cfg: ProviderConfig) -> Provider:
    """构造短信网关实现；账户标识、发送方或端点不合法时装配阶段就失败。"""
    try:
        return TwilioSMSProvider.from_credentials(
            cfg.channel,
            cfg.credentials,
            identifier=cfg.id,
            priority=cfg.priority,
        )
    except TwilioConfigError as exc:
        raise ProviderConfigError(f"{cfg.label}：{exc}") from exc


def _build_push_mock(cfg: ProviderConfig) -> Provider:
    """构造推送 mock：平台、项目标识与容量不合法时装配阶段就失败。"""
    raw_capacity = str(cfg.credentials.get("capacity", "")).strip()
    capacity = PUSH_MOCK_CAPACITY
    if raw_capacity:
        try:
            capacity = int(raw_capacity, 10)
        except ValueError as exc:
            raise ProviderConfigError(
                f"{cfg.label}：capacity 必须是整数，收到 {raw_capacity!r}"
            ) from exc
    try:
        return MockPushProvider(
            cfg.channel,
            platform=str(cfg.credentials.get("platform", "")),
            project_id=str(cfg.credentials.get("project_id", "")),
            topic=str(cfg.credentials.get("topic", "")),
            fail_with=str(cfg.credentials.get("fail_with", "")),
            capacity=capacity,
            identifier=cfg.id,
            priority=cfg.priority,
        )
    except PushConfigError as exc:
        raise ProviderConfigError(f"{cfg.label}：{exc}") from exc


def _build_inapp(cfg: ProviderConfig) -> Provider:
    """构造站内信实现：写入与引擎共用的消息中心文件。

    存储位置由 ``XW_INBOX_FILE`` 决定（缺省 ``api/data/inbox.jsonl``），与引擎
    侧的默认路径一致，因此 API 内置消费者写下的通知也能在收件箱里读到。
    """
    try:
        store = create_store()
    except MessageError as exc:
        raise ProviderConfigError(f"{cfg.label}：{exc}") from exc
    return InAppProvider(
        cfg.channel,
        store=store,
        identifier=cfg.id,
        priority=cfg.priority,
    )


#: 种类 → 构造函数。没列在这里的种类会在装配时跳过（实现尚未接入）。
BUILDERS: dict[str, Callable[[ProviderConfig], Provider]] = {
    "console": _build_console,
    "inapp": _build_inapp,
    "smtp": _build_smtp,
    "twilio": _build_twilio,
    "push-mock": _build_push_mock,
}

#: 已接入实现的种类。
SUPPORTED_KINDS = frozenset(BUILDERS)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_item(raw: object, index: int) -> ProviderConfig:
    where = f"第 {index + 1} 项"
    if not isinstance(raw, Mapping):
        raise ProviderConfigError(f"{where} 必须是对象，收到 {type(raw).__name__}")
    unknown = [str(key) for key in raw if key not in _CONFIG_FIELDS]
    if unknown:
        raise ProviderConfigError(f"{where} 含未知字段：{'、'.join(sorted(unknown))}")

    raw_channel = raw.get("channel")
    if not isinstance(raw_channel, str) or not raw_channel.strip():
        raise ProviderConfigError(f"{where} 缺少 channel")
    try:
        channel = Channel(raw_channel.strip())
    except ValueError as exc:
        allowed = "、".join(c.value for c in Channel.delivery_channels())
        raise ProviderConfigError(f"{where} 的渠道 {raw_channel!r} 不存在，可选：{allowed}") from exc
    if not channel.is_delivery():
        raise ProviderConfigError(f"{where} 的渠道 {channel.value!r} 是编排步骤，不能作为投递渠道")

    raw_kind = raw.get("kind")
    if not isinstance(raw_kind, str) or not raw_kind.strip():
        raise ProviderConfigError(f"{where} 缺少 kind")
    kind = raw_kind.strip()
    if kind not in KIND_SPECS:
        allowed = "、".join(sorted(KIND_SPECS))
        raise ProviderConfigError(f"{where} 的种类 {kind!r} 不存在，可选：{allowed}")

    identifier = raw.get("id", "")
    if not isinstance(identifier, str):
        raise ProviderConfigError(f"{where} 的 id 必须是字符串")

    priority = raw.get("priority", DEFAULT_PRIORITY)
    if not _is_int(priority):
        raise ProviderConfigError(f"{where} 的 priority 必须是整数")
    if priority < 0:
        raise ProviderConfigError(f"{where} 的 priority 不能为负数")

    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ProviderConfigError(f"{where} 的 enabled 必须是布尔值")

    raw_credentials = raw.get("credentials", {})
    if not isinstance(raw_credentials, Mapping):
        raise ProviderConfigError(f"{where} 的 credentials 必须是对象")
    credentials: dict[str, str] = {}
    for key, value in raw_credentials.items():
        if not isinstance(key, str) or not isinstance(value, (str, int, float, bool)):
            raise ProviderConfigError(f"{where} 的凭据 {key!r} 取值必须是标量")
        credentials[key] = str(value)

    return ProviderConfig(
        channel=channel,
        kind=kind,
        id=identifier.strip(),
        priority=int(priority),
        enabled=enabled,
        credentials=credentials,
    )


def parse_configs(raw: object) -> list[ProviderConfig]:
    """解析配置（顶层数组或 ``{"providers": [...]}``），返回条目列表。"""
    if isinstance(raw, Mapping):
        unknown = [str(key) for key in raw if key != "providers"]
        if unknown:
            raise ProviderConfigError(f"顶层含未知字段：{'、'.join(sorted(unknown))}")
        items = raw.get("providers")
    else:
        items = raw
    if not isinstance(items, list):
        raise ProviderConfigError("渠道配置必须是数组，或含 providers 数组的对象")
    return [_parse_item(item, index) for index, item in enumerate(items)]


def load_configs(path: Path | str) -> list[ProviderConfig]:
    """从 JSON 文件读取配置；解析失败时把路径带进错误信息。"""
    target = Path(path)
    try:
        text = target.read_text(encoding="utf-8")
    except OSError as exc:
        raise ProviderConfigError(f"读取渠道配置失败：{target}（{exc}）") from exc
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProviderConfigError(f"解析渠道配置失败：{target}（第 {exc.lineno} 行：{exc.msg}）") from exc
    try:
        return parse_configs(raw)
    except ProviderConfigError as exc:
        raise ProviderConfigError(f"{target}: {exc}") from exc


def validate_config(cfg: ProviderConfig) -> list[str]:
    """校验一条配置的渠道约束与凭据，返回全部问题（空列表表示通过）。"""
    spec = KIND_SPECS.get(cfg.kind)
    if spec is None:
        return [f"未知的种类：{cfg.kind}"]
    problems: list[str] = []
    if cfg.channel not in spec.channels:
        allowed = "、".join(channel.value for channel in spec.channels)
        problems.append(f"种类 {cfg.kind} 只能用于 {allowed}，收到 {cfg.channel.value}")
    allowed_credentials = tuple(spec.required) + tuple(spec.optional)
    known = set(allowed_credentials)
    for key in sorted(cfg.credentials):
        if key in known:
            continue
        hint = ""
        for candidate in allowed_credentials:
            if candidate.lower() == key.lower() or candidate.lower().replace("_", "") == key.lower().replace("_", ""):
                hint = f"（是否想写 {candidate}？）"
                break
        problems.append(f"未知的凭据项 {key!r}{hint}")
    missing = [name for name in spec.required if not str(cfg.credentials.get(name, "")).strip()]
    if missing:
        problems.append(f"缺少必填凭据：{'、'.join(missing)}")
    return problems


def default_configs() -> list[ProviderConfig]:
    """默认装配：每个投递渠道一个 console 实现。"""
    return [
        ProviderConfig(channel=channel, kind="console") for channel in Channel.delivery_channels()
    ]


@dataclass(frozen=True)
class BuildReport:
    """装配结果：登记了哪些、跳过了哪些、还有哪些渠道空着。"""

    registry: ProviderRegistry
    bound: tuple[Entry, ...] = ()
    skipped: tuple[tuple[str, str], ...] = ()
    unbound_channels: tuple[Channel, ...] = ()

    def describe(self) -> str:
        lines = [f"已登记 {len(self.bound)} 个渠道实现"]
        lines.extend(f"- {entry.describe()}" for entry in self.bound)
        if self.skipped:
            lines.append(f"跳过 {len(self.skipped)} 项（实现尚未接入）")
            lines.extend(f"- {name}：{reason}" for name, reason in self.skipped)
        if self.unbound_channels:
            names = "、".join(channel.value for channel in self.unbound_channels)
            lines.append(f"以下渠道没有启用的实现，投递会失败：{names}")
        return "\n".join(lines)


def build_registry(configs: Optional[Iterable[ProviderConfig]] = None) -> BuildReport:
    """按配置装配注册表。

    配置不合法（渠道约束或凭据）直接抛 :class:`ProviderConfigError` 并一次列出
    全部问题；种类尚未接入则跳过并记进报告，不影响其他渠道装配。
    """
    items = list(default_configs() if configs is None else configs)
    problems: list[str] = []
    for cfg in items:
        for problem in validate_config(cfg):
            problems.append(f"{cfg.label}：{problem}")
    if problems:
        raise ProviderConfigError("渠道配置不合法：\n" + "\n".join(f"- {p}" for p in problems))

    registry = ProviderRegistry()
    bound: list[Entry] = []
    skipped: list[tuple[str, str]] = []
    for cfg in items:
        builder = BUILDERS.get(cfg.kind)
        if builder is None:
            skipped.append((cfg.label, f"种类 {cfg.kind} 的实现尚未接入"))
            continue
        entry = registry.register(
            builder(cfg),
            identifier=cfg.id,
            priority=cfg.priority,
            enabled=cfg.enabled,
        )
        bound.append(entry)

    return BuildReport(
        registry=registry,
        bound=tuple(bound),
        skipped=tuple(skipped),
        unbound_channels=tuple(registry.missing_channels()),
    )


def create_report() -> BuildReport:
    """按 ``XW_PROVIDERS_FILE`` 装配；未配置时使用默认 console 实现。"""
    path = config.PROVIDERS_FILE
    configs = load_configs(path) if path else None
    return build_registry(configs)


def create_registry() -> ProviderRegistry:
    """装配并返回注册表（进程启动时调用一次）。"""
    report = create_report()
    logger.info("渠道注册表：\n%s", report.describe())
    return report.registry


_registry: Optional[ProviderRegistry] = None
_registry_lock = threading.Lock()


def get_registry() -> ProviderRegistry:
    """获取全局注册表（首次调用时装配，失败向上抛配置错误）。"""
    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = create_registry()
        return _registry


def set_registry(registry: Optional[ProviderRegistry]) -> None:
    """替换全局注册表；传 ``None`` 表示下次取用时重新装配（供测试与热更新）。"""
    global _registry
    with _registry_lock:
        _registry = registry


__all__ = [
    "BUILDERS",
    "KIND_SPECS",
    "SUPPORTED_KINDS",
    "BuildReport",
    "KindSpec",
    "ProviderConfig",
    "ProviderConfigError",
    "build_registry",
    "create_registry",
    "create_report",
    "default_configs",
    "get_registry",
    "load_configs",
    "parse_configs",
    "set_registry",
    "validate_config",
]
