"""发送偏好的查询与写入接口。

偏好有三层（全局 / 工作流 / 渠道），存储层按 ``(订阅者, 层级, 目标)`` 覆盖写；
这里把它包成 REST 形状，并补两件存储层回答不了的事：

1. **归属校验**。偏好挂在订阅者身上，写入一个不存在的订阅者只会留下一份
   永远匹配不到任何人的记录。接口先按请求头里的环境取出订阅者，取不到或
   环境不符一律 404——订阅者标识在环境之间并不共享，本环境没有这个人，
   读他的偏好就没有意义。
2. **解析出的结论**。``GET .../preferences/resolve`` 把「渠道 → 工作流 → 全局」
   这条覆盖链跑一遍，返回发不发、是哪一层决定的、以及一句可读的原因。运维
   与客服排查「为什么这个人没收到邮件」时，需要的正是这个结论，而不是让
   他们自己在三条记录之间推。

约定：

- 写入是**覆盖写**而不是增删：同一个 ``(scope, target)`` 重复提交只更新
  ``enabled`` 与时刻，回执里的 ``created`` 说明这次是新建还是更新；
- 删除按「层级 + 目标」定位，也可以整份清空（不带 ``scope``），回执给出
  实际删掉的条数——「本来就没有」不该报成错误；
- 渠道目标只接受可投递渠道，``delay`` / ``digest`` 是编排步骤，挂到偏好上
  等于写了一个永远匹配不到的条件。
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, model_validator

from .auth import EnvContext, require_env
from .preferences import (
    Preference,
    PreferenceDecision,
    PreferenceStore,
    Scope,
    get_preference_store,
    normalize_target,
)
from .subscriber_store import get_subscriber_store

router = APIRouter(prefix="/v1/subscribers", tags=["preferences"])


class PreferenceOut(BaseModel):
    """一条偏好记录的对外表示。"""

    scope: str = Field(description="层级：global / workflow / channel")
    target: Optional[str] = Field(
        default=None, description="工作流名或渠道；全局层级为空"
    )
    enabled: bool = Field(description="是否接收")
    updated_at: str = Field(description="最近一次写入时刻（UTC）")


class PreferenceListOut(BaseModel):
    """某个订阅者的全部偏好记录。"""

    subscriber_id: str
    items: list[PreferenceOut] = Field(
        default_factory=list, description="按具体程度排序：渠道 → 工作流 → 全局"
    )
    counts: dict[str, int] = Field(
        default_factory=dict, description="各层级的记录条数，三个键恒在"
    )
    default_allowed: bool = Field(
        default=True, description="三层都没有记录时的结论：按接收处理"
    )


class PreferenceWriteOut(PreferenceOut):
    """写入回执：记录本身 + 这次是新建还是更新。"""

    created: bool = Field(description="true 表示新建，false 表示覆盖了已有记录")


class PreferenceDeleteOut(BaseModel):
    """删除回执：实际删掉的条数，0 表示本来就没有。"""

    removed: int


class PreferenceUpsert(BaseModel):
    """写入一条偏好。

    三层对 ``target`` 的要求不同，校验复用存储层的同一处判定，避免接口与
    存储在「什么算合法目标」上各写一套。
    """

    scope: Scope = Field(description="层级：global / workflow / channel")
    target: Optional[str] = Field(
        default=None, description="工作流名或渠道；全局层级不要传"
    )
    enabled: bool = Field(default=True, description="true 接收，false 不接收")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"scope": "channel", "target": "email", "enabled": False},
                {"scope": "workflow", "target": "post_liked", "enabled": False},
                {"scope": "global", "enabled": True},
            ]
        }
    }

    @model_validator(mode="after")
    def _check_target(self) -> "PreferenceUpsert":
        try:
            self.target = normalize_target(self.scope, self.target)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return self


class ResolutionOut(BaseModel):
    """一次偏好解析的结论。"""

    allowed: bool = Field(description="这条通知能不能发")
    scope: Optional[str] = Field(default=None, description="最终生效的层级")
    target: Optional[str] = Field(default=None, description="生效层级的目标")
    matched: bool = Field(description="是否命中了某条记录")
    reason: str = Field(description="可读的原因，直接用于排查与展示")


def _to_out(record: Preference) -> PreferenceOut:
    return PreferenceOut(
        scope=record.scope.value,
        target=record.target,
        enabled=record.enabled,
        updated_at=record.updated_at.isoformat(),
    )


def _to_resolution(decision: PreferenceDecision) -> ResolutionOut:
    return ResolutionOut(**decision.as_dict())


def _scope_or_422(raw: Optional[str]) -> Optional[Scope]:
    if raw is None or not raw.strip():
        return None
    try:
        return Scope(raw.strip().lower())
    except ValueError:
        allowed = ", ".join(scope.value for scope in Scope)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"未知层级：{raw}（支持：{allowed}）",
        )


def _require_subscriber(subscriber_id: str, env: EnvContext) -> str:
    """按环境取出订阅者；不存在或不属于本环境一律 404。

    记录的键是「环境 + 标识」，同一标识在两套环境各有一份档案，查询时带上
    环境即可区分；「本环境没有这个人」也包括「这个标识只属于另一个环境」——
    偏好跟着订阅者走，不能跨环境读别人的设置。
    """
    subscriber = get_subscriber_store().get(subscriber_id, environment=env.environment)
    if subscriber is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"订阅者不存在：{subscriber_id}（环境 {env.environment}）",
        )
    return subscriber.id


def _store() -> PreferenceStore:
    return get_preference_store()


@router.get(
    "/{subscriber_id}/preferences",
    response_model=PreferenceListOut,
    summary="查询订阅者的发送偏好",
)
def list_preferences(
    subscriber_id: str,
    scope: Optional[str] = Query(
        default=None, description="只看某一层级：global / workflow / channel"
    ),
    env: EnvContext = Depends(require_env),
) -> PreferenceListOut:
    """列出该订阅者设置的偏好记录。

    只返回**显式设置过**的记录：没有记录的层级意味着「没表态」，解析时继续
    往上一层找，三层都空按接收处理。把默认值也铺成记录会让「用户关了这个
    渠道」与「用户没表过态」在数据里看不出区别。
    """
    wanted = _scope_or_422(scope)
    subscriber = _require_subscriber(subscriber_id, env)
    records = _store().list(subscriber_id=subscriber, scope=wanted)
    counts = {item.value: 0 for item in Scope}
    for record in records:
        counts[record.scope.value] += 1
    return PreferenceListOut(
        subscriber_id=subscriber,
        items=[_to_out(record) for record in records],
        counts=counts,
    )


@router.put(
    "/{subscriber_id}/preferences",
    response_model=PreferenceWriteOut,
    summary="写入一条发送偏好",
)
def upsert_preference(
    subscriber_id: str,
    body: PreferenceUpsert,
    env: EnvContext = Depends(require_env),
) -> PreferenceWriteOut:
    """新建或覆盖一条偏好记录。

    覆盖写而不是追加：同一个 ``(层级, 目标)`` 只有一条记录，重复提交只更新
    开关与时刻，因此调用方不需要先查再改。``enabled=true`` 是一条**有意义的**
    记录——它比上层（更宽）的关闭更具体，能单独把某个渠道或某类互动放开。
    """
    subscriber = _require_subscriber(subscriber_id, env)
    store = _store()
    existing = store.get(subscriber, body.scope, body.target)
    record = Preference(
        subscriber_id=subscriber,
        scope=body.scope,
        target=body.target,
        enabled=body.enabled,
    )
    saved = store.upsert(record)
    return PreferenceWriteOut(created=existing is None, **_to_out(saved).model_dump())


@router.delete(
    "/{subscriber_id}/preferences",
    response_model=PreferenceDeleteOut,
    summary="删除订阅者的发送偏好",
)
def delete_preferences(
    subscriber_id: str,
    scope: Optional[str] = Query(
        default=None, description="要删除的层级；不传表示清空该订阅者的全部偏好"
    ),
    target: Optional[str] = Query(
        default=None, description="该层级的目标（工作流名或渠道）"
    ),
    env: EnvContext = Depends(require_env),
) -> PreferenceDeleteOut:
    """按层级 + 目标删除一条，或整份清空。

    清空相当于「恢复默认」：三层都没有记录时按接收处理，这正是新订阅者的
    初始状态。删除一条本来就不存在的记录返回 ``removed=0`` 而不是 404——
    调用方的目的（这条记录不要存在）已经达成。
    """
    subscriber = _require_subscriber(subscriber_id, env)
    wanted = _scope_or_422(scope)
    store = _store()

    if wanted is None:
        if target is not None and target.strip():
            # 目标离不开层级：只给 target 无法确定要删的是工作流还是渠道。
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="只给 target 无法定位记录，请同时给出 scope",
            )
        records = store.list(subscriber_id=subscriber)
        removed = sum(1 for record in records if store.delete(
            subscriber, record.scope, record.target
        ))
        return PreferenceDeleteOut(removed=removed)

    try:
        normalized = normalize_target(wanted, target)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        )
    removed = 1 if store.delete(subscriber, wanted, normalized) else 0
    return PreferenceDeleteOut(removed=removed)


@router.get(
    "/{subscriber_id}/preferences/resolve",
    response_model=ResolutionOut,
    summary="解析某条通知是否会发出",
)
def resolve_preference(
    subscriber_id: str,
    workflow: Optional[str] = Query(
        default=None, description="工作流名，如 post_liked"
    ),
    channel: Optional[str] = Query(default=None, description="渠道，如 email"),
    env: EnvContext = Depends(require_env),
) -> ResolutionOut:
    """按「渠道 → 工作流 → 全局」的顺序解析，返回结论与原因。

    三个参数都可以不传，此时只看全局总开关。解析只读当前快照，不做写入，
    也不会因为某层没有记录而报错——「没设置」本身就是一个合法的结论。
    """
    subscriber = _require_subscriber(subscriber_id, env)
    if channel is not None and channel.strip():
        try:
            normalize_target(Scope.CHANNEL, channel)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
            )
    if workflow is not None and workflow.strip():
        try:
            normalize_target(Scope.WORKFLOW, workflow)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
            )

    decision = _store().snapshot().resolve(
        subscriber, workflow=workflow, channel=channel
    )
    return _to_resolution(decision)


__all__ = ["router"]
