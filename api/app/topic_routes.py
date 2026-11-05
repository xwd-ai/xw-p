"""分组（Topic）接口：创建、查询、更新、删除，成员的加入与移出，以及按分组广播。

分组是「一次通知发给一群人」的寻址单位。业务侧不必在触发请求里塞一长串
订阅者标识，把创作者按主题归到分组里，触发时按分组广播即可。

约定：

- 写操作与广播走 ``require_env``；分组按请求头里的环境打标，列表也只返回当前
  环境的分组——两套环境是两套数据，混在一起会让 dev 的测试分组出现在生产列表里。
- 加入分组要校验订阅者存在且属于同一环境：指向一个不存在的标识，广播时
  只会得到一条无法投递的消息，而问题要到投递失败才暴露。
- 加入是幂等的：重复加入返回 ``added=false``，不报错也不刷新加入时刻。
- 删除分组连带清掉成员关系；重复删除返回 404，让调用方知道这次是不是白跑。
- 广播的收件人来自成员名单，地址取自各自的订阅者档案；接受者不是订阅者本人，
  而是「这个分组」，因此请求体里没有 ``to``。
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status

from . import config
from .activity_log import record_activities
from .auth import EnvContext, require_env
from .bus import get_bus, publish_all
from .idempotency import get_idempotency_store
from .models.entities import Environment, Topic
from .subscriber_store import get_subscriber_store
from .topic_broadcast import broadcast_to_topic
from .topic_schemas import (
    TopicBroadcastItem,
    TopicBroadcastRequest,
    TopicBroadcastResponse,
    TopicCreate,
    TopicListOut,
    TopicMemberListOut,
    TopicMemberOut,
    TopicMembershipIn,
    TopicMembershipOut,
    TopicOut,
    TopicUpdate,
)
from .topic_store import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    TopicCapacityError,
    TopicConflictError,
    TopicCursorError,
    TopicNotFoundError,
    get_topic_store,
)
from .trigger_core import missing_workflow_detail, resolve_workflow

logger = logging.getLogger("xw.topics")

router = APIRouter(prefix="/v1/topics", tags=["topics"])


def _environment(raw: str) -> Environment:
    try:
        return Environment(raw)
    except ValueError:
        return Environment.DEVELOPMENT


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _to_out(topic: Topic, member_count: int) -> TopicOut:
    return TopicOut(
        id=topic.id,
        name=topic.name,
        description=topic.description,
        environment=topic.environment.value,
        memberCount=member_count,
        createdAt=topic.created_at.isoformat(),
        updatedAt=topic.updated_at.isoformat(),
    )


def _load_topic(topic_id: str, environment: Environment) -> Topic:
    """按标识取出当前环境的分组；不存在或属于另一个环境一律 404。

    跨环境的分组对被隔离的那一侧而言「不存在」，与订阅者、偏好接口同一口径：
    说「这是另一个环境的」等于承认了它的存在。
    """
    topic = get_topic_store().get(topic_id)
    if topic is None or topic.environment is not environment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"分组不存在：{topic_id}（环境 {environment.value}）",
        )
    return topic


@router.post(
    "",
    response_model=TopicOut,
    status_code=status.HTTP_201_CREATED,
    summary="创建分组",
    description=(
        "建一个可整体广播的收件人集合。标识可自带（业务侧的话题名、作者名带进来都可读），"
        "留空生成 UUID；同名标识重复返回 409 而不是覆盖。名称留空时按标识回填，"
        "避免列表里出现空白项。"
    ),
)
def create_topic(
    body: TopicCreate,
    env: EnvContext = Depends(require_env),
) -> TopicOut:
    """创建一个分组。

    标识可由调用方指定（业务侧的话题名、作者名带进来都可读），留空则生成
    UUID；标识重复返回 409 而不是覆盖——同名分组被悄悄改写，比创建失败更难查。
    名称留空时按标识回填。
    """
    environment = _environment(env.environment)
    topic_id = body.id or str(uuid.uuid4())
    stamp = _now()
    topic = Topic(
        id=topic_id,
        name=body.name or topic_id,
        description=body.description,
        environment=environment,
        created_at=stamp,
        updated_at=stamp,
    )
    try:
        created = get_topic_store().create(topic)
    except TopicConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return _to_out(created, 0)


@router.get(
    "",
    response_model=TopicListOut,
    summary="列出分组",
    description="按创建顺序翻页返回当前环境的分组；``nextCursor`` 为空表示没有更多。",
)
def list_topics(
    env: EnvContext = Depends(require_env),
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 nextCursor"),
) -> TopicListOut:
    """列出当前环境的分组，按创建顺序翻页。"""
    environment = _environment(env.environment)
    store = get_topic_store()
    try:
        items, next_cursor = store.list(
            environment=environment, limit=limit, cursor=cursor
        )
    except TopicCursorError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return TopicListOut(
        items=[_to_out(t, store.member_count(t.id)) for t in items],
        nextCursor=next_cursor,
        hasMore=next_cursor is not None,
    )


@router.get(
    "/{topic_id}",
    response_model=TopicOut,
    summary="按标识读取分组",
    description="读取分组本身（名称、说明、成员上限）。成员列表走 ``/subscribers`` 子路径。",
)
def get_topic(topic_id: str, env: EnvContext = Depends(require_env)) -> TopicOut:
    """按标识读取分组。"""
    environment = _environment(env.environment)
    topic = _load_topic(topic_id, environment)
    return _to_out(topic, get_topic_store().member_count(topic.id))


@router.put(
    "/{topic_id}",
    response_model=TopicOut,
    summary="更新分组",
    description=(
        "只改请求里出现的字段：没写 ``name`` 就保持原名，``description`` 传空串表示清空说明。"
        "标识不可改——它是指向成员关系的键，改名会让已有成员挂空。"
    ),
)
def update_topic(
    topic_id: str,
    body: TopicUpdate,
    env: EnvContext = Depends(require_env),
) -> TopicOut:
    """更新分组名称与说明。

    只改请求里出现的字段：没写 ``name`` 就保持原名，``description`` 传空串
    表示清空说明。标识不可改——它是指向成员关系的键，改名会让已有成员挂空。
    """
    environment = _environment(env.environment)
    store = get_topic_store()
    current = _load_topic(topic_id, environment)

    provided = body.model_dump(exclude_unset=True)
    updated = current.model_copy(
        update={
            "name": provided.get("name") or current.name,
            "description": provided.get("description", current.description),
            "updated_at": _now(),
        }
    )
    try:
        saved = store.update(updated)
    except TopicNotFoundError as exc:
        # 读到与写入之间被别的请求删掉了：按不存在处理，别把它「复活」。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    return _to_out(saved, store.member_count(saved.id))


@router.delete(
    "/{topic_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="删除分组",
    description=(
        "删除分组并连带清掉成员关系，不会留下「分组没了、成员还挂着」的行。"
        "订阅者档案本身不受影响。"
    ),
)
def delete_topic(topic_id: str, env: EnvContext = Depends(require_env)) -> Response:
    """删除分组及其全部成员关系。"""
    environment = _environment(env.environment)
    _load_topic(topic_id, environment)
    if not get_topic_store().delete(topic_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"分组不存在：{topic_id}",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{topic_id}/subscribers",
    response_model=TopicMemberListOut,
    summary="列出分组成员",
    description=(
        "按加入时刻翻页返回成员。``total`` 是全量成员数，不受分页影响——"
        "广播规模要能一眼看到，而不是把每页加起来。"
    ),
)
def list_topic_members(
    topic_id: str,
    env: EnvContext = Depends(require_env),
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 nextCursor"),
) -> TopicMemberListOut:
    """列出分组成员，按加入时刻翻页。

    ``total`` 是全量成员数，不受分页影响——广播规模要能一眼看到，而不是
    把每页加起来。
    """
    environment = _environment(env.environment)
    topic = _load_topic(topic_id, environment)
    try:
        members, next_cursor, total = get_topic_store().members(
            topic.id, limit=limit, cursor=cursor
        )
    except TopicCursorError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return TopicMemberListOut(
        topicId=topic.id,
        items=[
            TopicMemberOut(subscriberId=sid, addedAt=added_at.isoformat())
            for sid, added_at in members
        ],
        total=total,
        memberLimit=get_topic_store().max_members,
        nextCursor=next_cursor,
        hasMore=next_cursor is not None,
    )


@router.post(
    "/{topic_id}/subscribers",
    response_model=TopicMembershipOut,
    status_code=status.HTTP_200_OK,
    summary="加入分组",
    description=(
        "把订阅者加入分组。订阅者必须存在且属于同一环境，否则 404——分组指向一个"
        "不存在的标识，广播时只会产生一条无法投递的消息。\n\n"
        "加入是集合语义：重复加入返回 ``added=false`` 且不刷新加入时刻，成员顺序因此"
        "不会随重复调用漂移。组满时返回 409 并给出上限，重复加入一个已在名单里的成员"
        "不占新名额、也不因满员报错。"
    ),
)
def add_topic_member(
    topic_id: str,
    body: TopicMembershipIn,
    env: EnvContext = Depends(require_env),
) -> TopicMembershipOut:
    """把订阅者加入分组。

    订阅者必须存在且属于同一环境，否则 404：分组指向一个不存在的标识，
    广播时只会产生一条无法投递的消息，问题要到投递失败才暴露。
    重复加入返回 ``added=false``，是幂等的。
    """
    environment = _environment(env.environment)
    topic = _load_topic(topic_id, environment)

    subscriber = get_subscriber_store().get(body.subscriberId)
    if subscriber is None or subscriber.environment is not environment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"订阅者不存在：{body.subscriberId}（环境 {environment.value}）",
        )

    store = get_topic_store()
    try:
        added = store.add_member(topic.id, subscriber.id)
    except TopicNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except TopicCapacityError as exc:
        # 容量是业务规则上的冲突（分组满了），不是请求格式错误：
        # 409 让调用方知道「换个分组或先腾位置」才有用。
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return TopicMembershipOut(
        topicId=topic.id,
        subscriberId=subscriber.id,
        added=added,
        memberCount=store.member_count(topic.id),
        memberLimit=store.max_members,
    )


@router.delete(
    "/{topic_id}/subscribers/{subscriber_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="移出分组",
    description=(
        "把订阅者移出分组并释放名额。订阅者档案是否还在不影响这一步：档案被删掉之后，"
        "残留的成员关系更要能清掉。不在名单里返回 404。"
    ),
)
def remove_topic_member(
    topic_id: str,
    subscriber_id: str,
    env: EnvContext = Depends(require_env),
) -> Response:
    """把订阅者移出分组。

    订阅者档案是否还在不影响这一步：档案被删掉之后，残留的成员关系更要能清掉。
    """
    environment = _environment(env.environment)
    topic = _load_topic(topic_id, environment)
    try:
        removed = get_topic_store().remove_member(topic.id, subscriber_id)
    except TopicNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"订阅者不在该分组中：{subscriber_id}",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{topic_id}/trigger",
    response_model=TopicBroadcastResponse,
    status_code=status.HTTP_200_OK,
    summary="按分组广播一条互动事件",
    description=(
        "把一条互动事件分发给分组里的全部成员：工作流按 ``name`` 解析一次，"
        "每位成员各自生成一条消息（地址取自其订阅者档案），活动日志与入队各走一次"
        "批量通道。\n\n"
        "**逐人回执**：``results`` 与成员名单顺序一致。档案已被删除、或没有任何"
        "可投递渠道的成员记为 ``skipped`` 并给出原因——「按设置不发」与「发失败」"
        "分开记，广播结果的成败才判得清。\n\n"
        "**幂等语义**：``Idempotency-Key`` 作用于整次广播，相同键重复请求返回首次回执。"
    ),
)
def trigger_topic(
    topic_id: str,
    body: TopicBroadcastRequest,
    env: EnvContext = Depends(require_env),
    idempotency_key: Optional[str] = Header(
        default=None,
        alias="Idempotency-Key",
        description="整次广播的幂等键：相同键重复请求返回首次回执",
        examples=["topic-hot-authors-P-2026-1001"],
    ),
) -> TopicBroadcastResponse:
    """按分组成员广播：解析工作流 → 逐人构造消息 → 整批入队。"""
    environment = _environment(env.environment)
    topic = _load_topic(topic_id, environment)

    # 工作流只解析一次：整次广播共用同一份步骤快照，不会出现同一次广播里
    # 前半段用旧模板、后半段用新模板的情况。
    workflow = resolve_workflow(body.name, env.environment)
    if workflow is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=missing_workflow_detail(body.name, env.environment),
        )

    store = get_idempotency_store()
    if idempotency_key:
        cached = store.get(idempotency_key)
        if cached is not None:
            logger.info("幂等命中：%s（返回缓存的广播回执）", idempotency_key)
            return TopicBroadcastResponse(**cached)

    result = broadcast_to_topic(
        topic=topic,
        workflow=workflow,
        topic_store=get_topic_store(),
        subscriber_store=get_subscriber_store(),
        environment=environment,
        payload=body.payload,
        overrides=body.overrides,
    )
    response = TopicBroadcastResponse(
        topicId=topic.id,
        name=workflow.name,
        memberCount=result.member_count,
        accepted=result.accepted,
        skipped=result.skipped,
        results=[TopicBroadcastItem(**item.as_dict()) for item in result.items],
    )

    # 与批量触发同样的顺序：先登记幂等键，再写活动日志与队列，避免并发同键
    # 请求各发一遍。
    if idempotency_key:
        inserted = store.put_if_absent(
            idempotency_key, response.model_dump(by_alias=True), config.IDEMPOTENCY_TTL
        )
        if not inserted:
            cached = store.get(idempotency_key)
            if cached is not None:
                return TopicBroadcastResponse(**cached)

    if result.messages:
        record_activities(result.activities)
        publish_all(get_bus(), result.messages)

    logger.info(
        "分组广播：%s -> %s，成员 %d，受理 %d，跳过 %d",
        topic.id,
        workflow.name,
        result.member_count,
        response.accepted,
        response.skipped,
    )
    return response


__all__ = ["router"]
