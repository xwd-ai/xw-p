"""订阅者 CRUD 接口。

订阅者是触发的收件方：一条记录里既有身份，也有邮箱 / 手机号 / 设备 token
与「启用了哪些渠道」。地址的格式校验在请求模型里（422），唯一性在存储层
（409）——邮箱与手机号都是「一个人一个」的身份信息，同一个地址绑两个订阅者
会让一条通知投两遍、退信归不到人。

约定：

- 写操作走 ``require_env``，记录按请求头里的环境打标，唯一性判定也按环境分隔；
- 渠道列表整体替换而不是增量增删，避免「先查再改」在并发下丢更新；
- 删除成功返回 204，重复删除返回 404 —— 让调用方知道这次是不是白跑一趟。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from .auth import EnvContext, require_env
from .models.channel import Channel
from .models.entities import Environment, Subscriber
from .subscriber_schemas import (
    SubscriberCreate,
    SubscriberListOut,
    SubscriberOut,
    SubscriberUpdate,
    to_subscriber_out,
)
from .subscriber_store import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    SubscriberAddressConflictError,
    SubscriberConflictError,
    SubscriberCursorError,
    SubscriberNotFoundError,
    get_subscriber_store,
)

router = APIRouter(prefix="/v1/subscribers", tags=["subscribers"])


def _environment(raw: str) -> Environment:
    try:
        return Environment(raw)
    except ValueError:
        return Environment.DEVELOPMENT


def _now() -> datetime:
    return datetime.now(timezone.utc)


@router.post("", response_model=SubscriberOut, status_code=status.HTTP_201_CREATED)
def create_subscriber(
    body: SubscriberCreate,
    env: EnvContext = Depends(require_env),
) -> SubscriberOut:
    """创建一个订阅者。

    标识可由调用方指定（把业务侧的用户 ID 带进来），留空则生成 UUID。
    标识重复、邮箱或手机号已被别人绑定时返回 409 而不是覆盖：订阅者的渠道
    绑定被悄悄换掉，比创建失败更难查。邮箱统一存小写，手机号存 E.164。
    """
    stamp = _now()
    subscriber = Subscriber(
        id=body.id or str(uuid.uuid4()),
        email=body.email,
        phone=body.phone,
        device_token=body.device_token,
        channels=body.channels,
        environment=_environment(env.environment),
        created_at=stamp,
        updated_at=stamp,
    )
    try:
        created = get_subscriber_store().create(subscriber)
    except SubscriberConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except SubscriberAddressConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return to_subscriber_out(created)


@router.get("", response_model=SubscriberListOut)
def list_subscribers(
    channel: str | None = Query(default=None, description="只看启用了该渠道的订阅者"),
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 nextCursor"),
) -> SubscriberListOut:
    """列出订阅者，按创建顺序翻页。"""
    try:
        selected = Channel(channel) if channel else None
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"未知渠道：{channel}",
        )
    if selected is not None and not selected.is_delivery():
        # 编排步骤不会出现在订阅者的渠道列表里，按它过滤只会得到空页，
        # 让调用方以为「没人订阅」比直接指出取值不对更容易误导。
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{selected.value} 是编排步骤，不能作为订阅者的投递渠道",
        )

    try:
        items, next_cursor = get_subscriber_store().list(
            channel=selected, limit=limit, cursor=cursor
        )
    except SubscriberCursorError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return SubscriberListOut(
        items=[to_subscriber_out(s) for s in items],
        nextCursor=next_cursor,
        hasMore=next_cursor is not None,
    )


@router.get("/{subscriber_id}", response_model=SubscriberOut)
def get_subscriber(subscriber_id: str) -> SubscriberOut:
    """按标识读取订阅者。"""
    subscriber = get_subscriber_store().get(subscriber_id)
    if subscriber is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"订阅者不存在：{subscriber_id}",
        )
    return to_subscriber_out(subscriber)


@router.put("/{subscriber_id}", response_model=SubscriberOut)
def update_subscriber(
    subscriber_id: str,
    body: SubscriberUpdate,
    _env: EnvContext = Depends(require_env),
) -> SubscriberOut:
    """更新订阅者。

    只改请求里出现的字段：没写 ``phone`` 就保持原手机号，写空串才解绑。
    渠道列表整体替换，传空列表等同于把该订阅者的所有渠道关掉。
    改成别人已占用的邮箱或手机号返回 409；把地址解绑之后再被别人认领是允许的。
    """
    store = get_subscriber_store()
    current = store.get(subscriber_id)
    if current is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"订阅者不存在：{subscriber_id}",
        )

    provided = body.model_dump(exclude_unset=True)
    updated = current.model_copy(
        update={
            "email": provided.get("email", current.email),
            "phone": provided.get("phone", current.phone),
            "device_token": provided.get("device_token", current.device_token),
            "channels": provided.get("channels", current.channels),
            "updated_at": _now(),
        }
    )
    try:
        saved = store.update(updated)
    except SubscriberNotFoundError as exc:
        # 读到与写入之间被别的请求删掉了：按不存在处理，别把它「复活」。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except SubscriberAddressConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return to_subscriber_out(saved)


@router.delete("/{subscriber_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_subscriber(
    subscriber_id: str,
    _env: EnvContext = Depends(require_env),
) -> Response:
    """删除订阅者。"""
    if not get_subscriber_store().delete(subscriber_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"订阅者不存在：{subscriber_id}",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


__all__ = ["router"]
