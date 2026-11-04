"""工作流模板 CRUD API 的请求/响应模型（Pydantic v2）。"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from .models.channel import Channel
from .models.entities import Environment


class StepIn(BaseModel):
    """创建/更新时的步骤定义。"""

    type: Channel = Field(description="步骤类型（渠道或编排步骤）")
    name: Optional[str] = Field(default=None, description="可读名称")
    config: dict[str, Any] = Field(default_factory=dict, description="步骤配置（可含 depends_on）")

    # 允许传入的 type 为字符串（如 "in_app"），由 Channel 枚举自动校验
    model_config = ConfigDict(use_enum_values=False)


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
