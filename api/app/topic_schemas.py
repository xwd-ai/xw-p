"""分组接口的请求与响应模型。

请求模型只做能在这一层判定的事：裁剪空白、限制长度、拒绝会破坏路由的标识
（斜杠、问号、井号会让 ``/v1/topics/{id}`` 无法寻址）。分组标识是否已存在、
订阅者是否存在属于存储与业务规则，放在路由层判定。

标识与名称分开：标识是寻址键（一旦被触发配置、活动日志引用就不该再变），
名称是展示文案，随时可改。名称留空时按标识回填，避免列表里出现空白项。
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

#: 标识长度上限：会出现在 URL、活动日志与触发配置里，过长只降低可读性。
MAX_TOPIC_ID_LENGTH = 128
MAX_TOPIC_NAME_LENGTH = 120
MAX_TOPIC_DESCRIPTION_LENGTH = 500

#: 标识里不允许出现的字符：前三个会破坏路径寻址，其余是控制字符。
_BAD_ID_CHARS = set("/?#")


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _check_id(value: Optional[str]) -> Optional[str]:
    text = _clean(value)
    if text is None:
        return None
    if len(text) > MAX_TOPIC_ID_LENGTH:
        raise ValueError(f"分组标识不能超过 {MAX_TOPIC_ID_LENGTH} 个字符")
    if any(ch in _BAD_ID_CHARS for ch in text):
        raise ValueError("分组标识不能包含 / ? # 字符")
    if any(ch.isspace() or ord(ch) < 0x20 for ch in text):
        raise ValueError("分组标识不能包含空白或控制字符")
    return text


def _check_name(value: Optional[str]) -> Optional[str]:
    text = _clean(value)
    if text is not None and len(text) > MAX_TOPIC_NAME_LENGTH:
        raise ValueError(f"分组名称不能超过 {MAX_TOPIC_NAME_LENGTH} 个字符")
    return text


def _check_description(value: Optional[str]) -> Optional[str]:
    text = _clean(value)
    if text is not None and len(text) > MAX_TOPIC_DESCRIPTION_LENGTH:
        raise ValueError(f"分组说明不能超过 {MAX_TOPIC_DESCRIPTION_LENGTH} 个字符")
    return text


class TopicCreate(BaseModel):
    """创建分组。标识可留空，由服务端生成 UUID。"""

    id: Optional[str] = Field(default=None, description="分组标识；留空自动生成")
    name: Optional[str] = Field(default=None, description="分组名称；留空按标识回填")
    description: Optional[str] = Field(default=None, description="分组说明")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "id": "post-P-2026-1001-subscribers",
                    "name": "热帖 P-2026-1001 的订阅读者",
                    "description": "这篇帖子的关注者，新评论时整组触达",
                }
            ]
        }
    }

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: Optional[str]) -> Optional[str]:
        return _check_id(value)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: Optional[str]) -> Optional[str]:
        return _check_name(value)

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: Optional[str]) -> Optional[str]:
        return _check_description(value)


class TopicUpdate(BaseModel):
    """更新分组：只改请求里出现的字段，说明传空串表示清空。"""

    name: Optional[str] = Field(default=None, description="分组名称")
    description: Optional[str] = Field(default=None, description="分组说明；空串表示清空")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: Optional[str]) -> Optional[str]:
        return _check_name(value)

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: Optional[str]) -> Optional[str]:
        return _check_description(value)


class TopicOut(BaseModel):
    """分组的对外表示。"""

    id: str
    name: str
    description: Optional[str] = None
    environment: str
    memberCount: int = Field(default=0, description="成员数")
    createdAt: str
    updatedAt: str


class TopicListOut(BaseModel):
    """分组列表：``nextCursor`` 为空表示没有下一页。"""

    items: list[TopicOut]
    nextCursor: Optional[str] = None
    hasMore: bool = False


class TopicMembershipIn(BaseModel):
    """加入分组的请求体。"""

    subscriberId: str = Field(description="订阅者标识")

    @field_validator("subscriberId")
    @classmethod
    def _validate(cls, value: str) -> str:
        text = _clean(value)
        if text is None:
            raise ValueError("订阅者标识不能为空")
        return text


class TopicMembershipOut(BaseModel):
    """加入分组的回执。

    ``added`` 区分「这次真的加进去了」与「本来就是成员」：重复加入是幂等的，
    但调用方需要知道本次是否产生了变更（比如据此决定要不要重算广播规模）。
    ``memberLimit`` 给出当前分组的成员上限，调用方据此判断还剩多少余量，
    不必再单独查一次配置。
    """

    topicId: str
    subscriberId: str
    added: bool
    memberCount: int
    memberLimit: int = Field(default=0, description="分组成员上限")


class TopicMemberOut(BaseModel):
    """成员条目。"""

    subscriberId: str
    addedAt: str


class TopicMemberListOut(BaseModel):
    """成员列表：``total`` 是全量成员数，不受分页影响。"""

    topicId: str
    items: list[TopicMemberOut]
    total: int = 0
    memberLimit: int = Field(default=0, description="分组成员上限")
    nextCursor: Optional[str] = None
    hasMore: bool = False


class TopicBroadcastRequest(BaseModel):
    """按分组广播的请求体。

    与单条触发相比少了 ``to``：收件人来自分组成员，地址来自各自的订阅者档案。
    事件名与载荷仍是同一套语义，逐人渲染。

    示例（新帖发布，广播给关注该作者的读者）：::

        {
            "name": "post_published",
            "payload": {"post_id": "P-2026-1001", "post_title": "我的第一支 Vlog",
                        "author": "阿澈"}
        }
    """

    name: str = Field(
        min_length=1,
        max_length=255,
        description="工作流/模板标识，例如 post_liked / post_commented",
    )
    payload: dict[str, Any] = Field(default_factory=dict, description="渲染变量（键值对）")
    overrides: dict[str, Any] = Field(default_factory=dict, description="覆盖模板默认配置")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        text = _clean(value)
        if text is None:
            raise ValueError("工作流标识不能为空")
        return text


class TopicBroadcastItem(BaseModel):
    """广播中单个成员的处理结果。"""

    index: int = Field(description="该成员在名单中的下标，从 0 开始")
    subscriberId: str
    status: str = Field(description="处理结果：accepted 已入队 / skipped 未发")
    triggerId: Optional[str] = Field(default=None, description="触发标识（跳过时为空）")
    transactionId: Optional[str] = None
    error: Optional[str] = Field(default=None, description="跳过原因")


class TopicBroadcastResponse(BaseModel):
    """广播回执：成员规模 + 受理/跳过计数 + 逐人结果。

    ``memberCount`` 是广播时刻的成员名单长度，等于 ``accepted + skipped``：
    三者放在一起才能回答「名单里 20 个人，为什么只发了 18 条」。
    """

    acknowledged: bool = Field(default=True, description="整批是否已受理（部分跳过仍为 true）")
    topicId: str
    name: str = Field(description="本次广播使用的工作流标识")
    memberCount: int = Field(description="广播时的成员数")
    accepted: int = Field(description="已入队的条数")
    skipped: int = Field(description="未发起的条数（档案缺失或没有可投递渠道）")
    results: list[TopicBroadcastItem] = Field(description="逐人结果，与成员名单顺序一致")


__all__ = [
    "MAX_TOPIC_DESCRIPTION_LENGTH",
    "MAX_TOPIC_ID_LENGTH",
    "MAX_TOPIC_NAME_LENGTH",
    "TopicBroadcastItem",
    "TopicBroadcastRequest",
    "TopicBroadcastResponse",
    "TopicCreate",
    "TopicListOut",
    "TopicMemberListOut",
    "TopicMemberOut",
    "TopicMembershipIn",
    "TopicMembershipOut",
    "TopicOut",
    "TopicUpdate",
]
