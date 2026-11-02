"""渠道管理 API：配置的增删改查与运行期启停、健康检查。

四类动作，按「改什么」分开：

- **查**：``GET /v1/providers`` 给出注册表里每个条目的状态——是否启用、是不是
  该渠道的首选取用实现、健康结论，外带脱敏后的配置（凭据只给键名与掩码）。
  默认只做配置检查（不产生网络调用）；带 ``deep=true`` 才让实现自己做一次
  在线探测（SMTP 连一次端口、站内信确认目录可写），结果按 TTL 缓存，
  ``refresh=true`` 跳过缓存。
- **增**：``POST /v1/providers`` 登记一条新的渠道配置。标识为空时按渠道自动
  编号；校验（渠道约束、必填凭据、实现构造）全部通过才落表，任何一种失败都
  不会留下半个条目。
- **改**：``PUT /v1/providers/{id}`` 改种类、优先级、启停或整组凭据，没传的
  字段保持原样；``PUT /v1/providers/{id}/enabled`` 是它的快捷写法。停用后该
  条目不再参与取用，投递自动落到同渠道的次选实现；停到最后一个时响应里的
  ``warning`` 会说明这个渠道的投递会失败——比让调用方在活动流里发现大量失败
  要早一步。
- **删**：``DELETE /v1/providers/{id}`` 摘除条目。

改动默认只作用于当前进程（注册表与配置簿都是进程内状态），带 ``persist=true``
时同时写回渠道配置文件（``XW_PROVIDERS_FILE``），下次启动仍然生效。写文件失败
则整条请求失败、内存状态不变，不会出现「接口说已停用、重启后又回来」的错位。
引擎进程在启动时读取同一份文件，因此写回对已运行的引擎不生效，需要重启引擎。
"""
from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from .auth import EnvContext, require_env
from .models.channel import Channel
from .providers import factory as provider_factory
from .providers import health as health_mod
from .providers import manage
from .providers.base import DEFAULT_PRIORITY
from .providers.factory import ConfigRecord, ProviderConfigError
from .providers.health import ProviderHealth
from .providers.manage import ProviderConflictError, ProviderNotFoundError
from .providers.registry import ProviderRegistry

logger = logging.getLogger("xw.api.providers")

router = APIRouter(prefix="/v1/providers", tags=["providers"])

_DEFAULT_DEEP = False


class ToggleRequest(BaseModel):
    """启停请求。"""

    enabled: bool = Field(..., description="true 启用 / false 停用")
    persist: bool = Field(
        default=False,
        description="是否同时写回渠道配置文件（XW_PROVIDERS_FILE），使重启后仍然生效",
    )


class ProviderCreateRequest(BaseModel):
    """新增渠道配置。

    ``id`` 留空时按渠道自动编号；``credentials`` 的取值统一按字符串处理，
    数字与布尔会被转成对应的字面量（``port: 587`` → ``"587"``）。
    """

    channel: str = Field(..., description="投递渠道：in_app / email / sms / push / chat")
    kind: str = Field(..., description="实现种类，如 console / smtp / twilio / inapp / push-mock")
    id: str = Field(default="", description="条目标识；留空则按渠道自动编号")
    priority: int = Field(default=DEFAULT_PRIORITY, ge=0, description="数值小的先取用")
    enabled: bool = Field(default=True, description="是否参与取用")
    credentials: Mapping[str, Any] = Field(
        default_factory=dict,
        description="实现所需的凭据，按键取值；不认识的键会被拒绝",
    )
    persist: bool = Field(default=False, description="是否同时写回渠道配置文件")


class ProviderUpdateRequest(BaseModel):
    """修改渠道配置；只有传进来的字段会变。

    ``credentials`` 是整组替换而不是逐键合并：删掉一个可选凭证与改一个值同样
    常见，合并语义下前者做不到。``channel`` 可以带上，但必须与原值一致——
    把一个标识从 email 挪到 sms 会让执行明细里的历史记录指向另一件事。
    """

    kind: Optional[str] = Field(default=None, description="实现种类")
    priority: Optional[int] = Field(default=None, ge=0, description="数值小的先取用")
    enabled: Optional[bool] = Field(default=None, description="是否参与取用")
    credentials: Optional[Mapping[str, Any]] = Field(
        default=None, description="整组替换的凭据；不传则保持原样"
    )
    channel: Optional[str] = Field(default=None, description="仅用于校验，必须与原值一致")
    persist: bool = Field(default=False, description="是否同时写回渠道配置文件")


def _parse_channel(raw: Optional[str]) -> Optional[Channel]:
    """解析渠道过滤参数；空值表示不过滤，非法值 400。"""
    if raw is None or not raw.strip():
        return None
    try:
        return Channel(raw.strip())
    except ValueError:
        allowed = "、".join(channel.value for channel in Channel.delivery_channels())
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"未知渠道：{raw}（可选：{allowed}）",
        )


def _config_view(registry: ProviderRegistry, identifier: str) -> Optional[dict]:
    """该条目的配置回显；配置簿里没有对应记录（或渠道对不上）时返回 ``None``。

    渠道比对是为了防一份过期记录挂到别处：标识是人起的名字，两个测试或两次
    装配用同一个名字指向不同渠道时，宁可少显示一段也不显示错的。
    """
    record = provider_factory.get_book().get(identifier)
    if record is None:
        return None
    entry = registry.entry(identifier)
    if entry is not None and record.config.channel != entry.channel:
        return None
    return manage.describe_config(record)


def _decorate(registry: ProviderRegistry, item: ProviderHealth) -> dict:
    payload = item.to_dict()
    config = _config_view(registry, item.id)
    if config is not None:
        payload["config"] = config
    return payload


def _removed_view(entry) -> dict:
    """删除后的条目视图：健康状态已无意义，只回显它原本是什么。"""
    return {
        "id": entry.id,
        "channel": entry.channel.value,
        "kind": entry.kind,
        "enabled": entry.enabled,
        "priority": entry.priority,
        "state": "removed",
    }


def _snapshot(registry: ProviderRegistry, items: list[ProviderHealth], *, deep: bool) -> dict:
    """列表与单条接口共用的响应体。"""
    return {
        "items": [_decorate(registry, item) for item in items],
        "summary": health_mod.summarize(items),
        "channels": [channel.value for channel in registry.channels()],
        "unboundChannels": [channel.value for channel in registry.missing_channels()],
        "deep": deep,
    }


def _bounds(registry: ProviderRegistry) -> dict:
    """渠道现状：哪些渠道有实现、哪些空着。"""
    return {
        "channels": [channel.value for channel in registry.channels()],
        "unboundChannels": [channel.value for channel in registry.missing_channels()],
    }


def _mutation_response(
    registry: ProviderRegistry,
    result: manage.MutationResult,
    *,
    removed: bool = False,
) -> dict:
    """增删改共用的响应体。"""
    record = provider_factory.get_book().get(result.entry.id)
    config = record.config if record is not None else result.config
    source = record.source if record is not None else "removed"
    return {
        "provider": (
            _removed_view(result.entry)
            if removed
            else _decorate(registry, _health_of(result.entry.id))
        ),
        "config": (
            None
            if config is None
            else manage.describe_config(ConfigRecord(result.entry.id, config, source))
        ),
        "created": result.created,
        "changed": result.changed,
        "removed": removed,
        "persisted": result.persisted is not None,
        "persistPath": str(result.persisted) if result.persisted is not None else None,
        "runtimeOnly": result.persisted is None,
        "warning": result.warning,
        **_bounds(registry),
    }


def _health_of(identifier: str) -> ProviderHealth:
    health = health_mod.get_monitor().check_one(identifier, refresh=True)
    if health is None:  # 刚取下条目又被并发摘除，理论上不会发生
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未登记的渠道实现：{identifier}",
        )
    return health


def _fail(exc: Exception) -> HTTPException:
    """把管理层的异常翻译成 HTTP 状态码。"""
    if isinstance(exc, ProviderNotFoundError):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, ProviderConflictError):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    if isinstance(exc, ProviderConfigError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    raise exc


@router.get("", summary="列出渠道实现及其健康状态")
def list_providers(
    channel: Optional[str] = Query(default=None, description="只看某个渠道（如 email）"),
    deep: bool = Query(default=_DEFAULT_DEEP, description="是否做在线探测（会对渠道发起连接）"),
    refresh: bool = Query(default=False, description="忽略缓存重新检查"),
) -> dict:
    """返回注册表里的全部条目，按取用顺序排列。"""
    registry = provider_factory.get_registry()
    monitor = health_mod.get_monitor()
    items = monitor.check(deep=deep, refresh=refresh, channel=_parse_channel(channel))
    return _snapshot(registry, items, deep=deep)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="新增渠道配置",
    responses={400: {"description": "配置不合法"}, 409: {"description": "标识已存在"}},
)
def create_provider(
    body: ProviderCreateRequest,
    env: EnvContext = Depends(require_env),
) -> dict:
    """登记一条渠道配置并构造对应实现。"""
    channel = _parse_channel(body.channel)
    if channel is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="缺少 channel")
    if not channel.is_delivery():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"渠道 {channel.value} 是编排步骤，不能作为投递渠道",
        )
    if not body.kind.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="缺少 kind")

    config = provider_factory.ProviderConfig(
        channel=channel,
        kind=body.kind.strip(),
        id=body.id.strip(),
        priority=body.priority,
        enabled=body.enabled,
        credentials={str(key): str(value) for key, value in body.credentials.items()},
    )
    try:
        result = manage.create_config(config, persist=body.persist)
    except (ProviderConfigError, ProviderConflictError, ProviderNotFoundError) as exc:
        raise _fail(exc)

    monitor = health_mod.get_monitor()
    monitor.invalidate(result.entry.id)
    logger.info(
        "渠道配置已新增：%s（环境 %s，写回配置：%s）",
        result.entry.id,
        env.environment,
        result.persisted or "否",
    )
    return _mutation_response(provider_factory.get_registry(), result)


@router.get("/{identifier}", summary="单个渠道实现的健康状态")
def get_provider(
    identifier: str,
    deep: bool = Query(default=_DEFAULT_DEEP, description="是否做在线探测"),
    refresh: bool = Query(default=False, description="忽略缓存重新检查"),
) -> dict:
    """按标识查询一个条目；未登记时 404。"""
    monitor = health_mod.get_monitor()
    item = monitor.check_one(identifier, deep=deep, refresh=refresh)
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未登记的渠道实现：{identifier}",
        )
    return {"provider": _decorate(provider_factory.get_registry(), item), "deep": deep}


@router.put(
    "/{identifier}",
    summary="修改渠道配置",
    responses={
        400: {"description": "配置不合法"},
        404: {"description": "未登记"},
        409: {"description": "试图修改渠道"},
    },
)
def update_provider(
    identifier: str,
    body: ProviderUpdateRequest,
    env: EnvContext = Depends(require_env),
) -> dict:
    """按字段修改一条配置；未传的字段保持原样。"""
    registry = provider_factory.get_registry()
    entry = registry.entry(identifier)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未登记的渠道实现：{identifier}",
        )
    if body.channel is not None:
        target = _parse_channel(body.channel)
        if target is not None and target != entry.channel:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"条目 {entry.id} 属于渠道 {entry.channel.value}，不能改为 {target.value}；"
                    "需要换渠道请先删除再新增"
                ),
            )

    try:
        result = manage.update_config(
            entry.id,
            kind=body.kind,
            priority=body.priority,
            enabled=body.enabled,
            credentials=body.credentials,
            persist=body.persist,
        )
    except (ProviderConfigError, ProviderConflictError, ProviderNotFoundError) as exc:
        raise _fail(exc)

    health_mod.get_monitor().invalidate(result.entry.id)
    logger.info(
        "渠道配置已修改：%s（环境 %s，有变化：%s，写回配置：%s）",
        result.entry.id,
        env.environment,
        result.changed,
        result.persisted or "否",
    )
    return _mutation_response(registry, result)


@router.delete(
    "/{identifier}",
    summary="删除渠道配置",
    responses={404: {"description": "未登记"}},
)
def delete_provider(
    identifier: str,
    persist: bool = Query(default=False, description="是否同时从渠道配置文件里移除"),
    env: EnvContext = Depends(require_env),
) -> dict:
    """摘除条目与实现；``persist=true`` 时一并从配置文件里删除。"""
    try:
        result = manage.delete_config(identifier, persist=persist)
    except (ProviderConfigError, ProviderConflictError, ProviderNotFoundError) as exc:
        raise _fail(exc)

    registry = provider_factory.get_registry()
    health_mod.get_monitor().invalidate(result.entry.id)
    logger.info(
        "渠道配置已删除：%s（环境 %s，写回配置：%s）",
        result.entry.id,
        env.environment,
        result.persisted or "否",
    )
    return _mutation_response(registry, result, removed=True)


@router.put("/{identifier}/enabled", summary="启用或停用一个渠道实现")
def set_provider_enabled(
    identifier: str,
    body: ToggleRequest,
    env: EnvContext = Depends(require_env),
) -> dict:
    """切换运行期启停状态，并可选择写回配置文件。"""
    entry = provider_factory.get_registry().entry(identifier)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未登记的渠道实现：{identifier}",
        )

    try:
        result = manage.update_config(
            entry.id,
            enabled=body.enabled,
            persist=body.persist,
            # 启停只改一项：配置文件里没有这条时如实报错，不顺手把它固化下来。
            persist_upsert=False,
        )
    except (ProviderConfigError, ProviderConflictError, ProviderNotFoundError) as exc:
        raise _fail(exc)

    health_mod.get_monitor().invalidate(result.entry.id)
    logger.info(
        "渠道实现 %s 已%s（环境 %s，写回配置：%s）",
        result.entry.id,
        "启用" if result.entry.enabled else "停用",
        env.environment,
        result.persisted or "否",
    )
    return _mutation_response(provider_factory.get_registry(), result)


__all__ = [
    "ProviderCreateRequest",
    "ProviderUpdateRequest",
    "ToggleRequest",
    "router",
]
