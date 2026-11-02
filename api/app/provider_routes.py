"""渠道实现管理 API：健康检查与运行期启停。

两个动作：

- **看**：``GET /v1/providers`` 给出注册表里每个条目的状态——是否启用、是不是
  该渠道的首选取用实现、健康结论。默认只做配置检查（不产生网络调用）；
  带 ``deep=true`` 才让实现自己做一次在线探测（SMTP 连一次端口、站内信确认
  目录可写），结果按 TTL 缓存，``refresh=true`` 跳过缓存。
- **改**：``PUT /v1/providers/{id}/enabled`` 启用或停用一个条目。停用后该条目
  不再参与取用，投递自动落到同渠道的次选实现；停到最后一个时接口会返回
  ``warning``，说明这个渠道的投递会失败——比让调用方在活动流里发现大量失败
  要早一步。

启停默认只作用于当前进程（注册表是进程内状态）。带 ``persist=true`` 时同时
写回渠道配置文件，下次启动仍然生效；写文件失败则整条请求失败、内存状态不变，
不会出现「接口说已停用、重启后又回来」的错位。引擎进程在启动时读取同一份
文件，因此写回对已运行的引擎不生效，需要重启引擎。
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from .auth import EnvContext, require_env
from .models.channel import Channel
from .providers import factory as provider_factory
from .providers import health as health_mod
from .providers.factory import ProviderConfigError
from .providers.health import ProviderHealth
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


def _snapshot(registry: ProviderRegistry, items: list[ProviderHealth], *, deep: bool) -> dict:
    """列表与单条接口共用的响应体。"""
    return {
        "items": [item.to_dict() for item in items],
        "summary": health_mod.summarize(items),
        "channels": [channel.value for channel in registry.channels()],
        "unboundChannels": [channel.value for channel in registry.missing_channels()],
        "deep": deep,
    }


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
    return {"provider": item.to_dict(), "deep": deep}


@router.put("/{identifier}/enabled", summary="启用或停用一个渠道实现")
def set_provider_enabled(
    identifier: str,
    body: ToggleRequest,
    env: EnvContext = Depends(require_env),
) -> dict:
    """切换运行期启停状态，并可选择写回配置文件。"""
    registry = provider_factory.get_registry()
    entry = registry.entry(identifier)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未登记的渠道实现：{identifier}",
        )

    changed = entry.enabled != body.enabled
    persist_path = None
    if changed:
        # 先写文件再改内存：写失败时请求失败、状态不变，避免接口与实际不一致。
        if body.persist:
            try:
                persist_path = provider_factory.persist_enabled(entry.id, body.enabled)
            except ProviderConfigError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
        entry = registry.set_enabled(entry.id, body.enabled)
        monitor = health_mod.get_monitor()
        monitor.invalidate(entry.id)
        logger.info(
            "渠道实现 %s 已%s（环境 %s，写回配置：%s）",
            entry.id,
            "启用" if entry.enabled else "停用",
            env.environment,
            persist_path or "否",
        )

    monitor = health_mod.get_monitor()
    health = monitor.check_one(entry.id, refresh=True)
    if health is None:  # 刚取下条目又被并发摘除，理论上不会发生
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未登记的渠道实现：{identifier}",
        )
    return {
        "provider": health.to_dict(),
        "changed": changed,
        "persisted": persist_path is not None,
        "persistPath": str(persist_path) if persist_path is not None else None,
        "warning": _warning(registry, entry),
        "channels": [channel.value for channel in registry.channels()],
        "unboundChannels": [channel.value for channel in registry.missing_channels()],
    }


def _warning(registry: ProviderRegistry, entry) -> Optional[str]:
    """启停之后给请求方的一句提示：渠道是否还有人接、接下来由谁接。"""
    if entry.enabled:
        return None
    if registry.get(entry.channel) is None:
        return f"渠道 {entry.channel.value} 已没有启用的实现，投递会失败"
    for candidate in registry.entries():
        if candidate.channel == entry.channel and candidate.enabled:
            return f"渠道 {entry.channel.value} 改由 {candidate.id} 承接"
    return None


__all__ = ["router", "ToggleRequest"]
