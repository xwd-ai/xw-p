"""工作流模板 CRUD API 的请求/响应模型（Pydantic v2）。"""
from __future__ import annotations

import re
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .models.channel import Channel
from .models.entities import Environment

# 步骤标识的允许写法：参与 depends_on 引用，限定在无需转义的字符集内。
_STEP_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")


class StepIn(BaseModel):
    """创建/更新时的步骤定义。"""

    id: Optional[str] = Field(
        default=None,
        description=(
            "步骤标识。留空由服务端生成；带值则按原样保存，"
            "客户端维护 config.depends_on 时需要它保持稳定"
        ),
    )
    type: Channel = Field(description="步骤类型（渠道或编排步骤）")
    name: Optional[str] = Field(default=None, description="可读名称")
    config: dict[str, Any] = Field(default_factory=dict, description="步骤配置（可含 depends_on）")

    # 允许传入的 type 为字符串（如 "in_app"），由 Channel 枚举自动校验
    model_config = ConfigDict(use_enum_values=False)

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: Optional[str]) -> Optional[str]:
        """步骤标识只接受安全字符；空白视为未提供。

        标识会进 ``config.depends_on`` 与执行明细，允许任意字符会让引用变成
        一件需要转义的事。空白串按「未提供」处理：前端输入框清空后提交的
        空值不应该让整次保存失败，服务端生成一个即可。
        """
        if value is None:
            return None
        text = value.strip()
        if not text:
            return None
        if not _STEP_ID_PATTERN.fullmatch(text):
            raise ValueError("步骤 id 只能包含字母、数字与 . _ : -，长度 1–64")
        return text



class StepOut(BaseModel):
    """步骤的对外表示。"""

    id: str
    type: str
    name: Optional[str] = None
    config: dict[str, Any] = Field(default_factory=dict)


class WorkflowCreate(BaseModel):
    """创建工作流请求体。"""

    name: str = Field(min_length=1, max_length=255, description="工作流标识（业务侧使用）")
    description: Optional[str] = Field(default=None, description="描述")
    steps: list[StepIn] = Field(default_factory=list, description="步骤序列（有序）")
    environment: Environment = Field(default=Environment.DEVELOPMENT)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "post_liked_digest",
                    "description": "同一篇帖子的点赞在 10 分钟内聚合成一张卡片，再发站内信与推送",
                    "environment": "development",
                    "steps": [
                        {
                            "id": "digest",
                            "type": "digest",
                            "name": "按帖子聚合点赞",
                            "config": {
                                "key": "payload.post_id",
                                "window": "10m",
                                "title": "《{{payload.post_title}}》收到新的点赞",
                            },
                        },
                        {
                            "id": "inapp",
                            "type": "in_app",
                            "name": "站内信",
                            "config": {
                                "depends_on": ["digest"],
                                "template": "{{payload.actor}} 赞了你的帖子《{{payload.post_title}}》",
                            },
                        },
                        {
                            "id": "push",
                            "type": "push",
                            "name": "推送",
                            "config": {
                                "depends_on": ["digest"],
                                "template": "{{payload.actor}} 赞了你的帖子",
                            },
                        },
                    ],
                }
            ]
        }
    }


class WorkflowUpdate(BaseModel):
    """更新工作流请求体（全部可选，仅更新传入字段）。"""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = None
    steps: Optional[list[StepIn]] = None
    environment: Optional[Environment] = None
    active: Optional[bool] = None


class WorkflowOut(BaseModel):
    """工作流对外表示。"""

    id: str
    name: str
    description: Optional[str] = None
    steps: list[StepOut]
    version: int
    environment: str
    active: bool
    created_at: str


class WorkflowActivationIn(BaseModel):
    """启停请求体。"""

    active: bool = Field(description="true 启用、false 停用；重复设置同一状态不产生写入")


class WorkflowActivationOut(BaseModel):
    """启停回执：改后的模板与本次是否真的写入了。"""

    workflow: WorkflowOut
    changed: bool = Field(description="是否发生状态变化；重复调用返回 false")


class WorkflowDuplicateIn(BaseModel):
    """复制请求体（整体可选：不带请求体时按源模板自动命名）。"""

    name: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="副本名称，留空按「源名称 + _copy」生成",
    )
    environment: Optional[Environment] = Field(
        default=None, description="副本所属环境，留空跟随源模板"
    )
    active: bool = Field(
        default=False, description="副本是否立即启用，默认关闭（副本先当草稿）"
    )


__all__ = [
    "StepIn",
    "StepOut",
    "WorkflowActivationIn",
    "WorkflowActivationOut",
    "WorkflowCreate",
    "WorkflowDuplicateIn",
    "WorkflowUpdate",
    "WorkflowOut",
]
