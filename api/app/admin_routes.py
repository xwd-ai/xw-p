"""管理端统计 API（ROADMAP Phase 5）。

``GET /v1/admin/stats`` 面向运维与后台：在仪表盘的总量、成功率与渠道分布之外，
给出每次投递的成败拆分、耗时分布、实际完成投递的实现以及失败原因榜单，回答
「哪个渠道在失败、慢在哪里、为什么失败」。

统计窗口有三种写法：

- 只给 ``days``：最近 N 天（默认 7，上限 90）；
- 只给 ``since`` / ``until``：指定区间（含边界），最多 90 天；
- ``days`` + ``until``：截止到某个时刻的最近 N 天。

``days`` 与 ``since`` 同时出现时返回 400——两种写法指向的窗口不同，猜一个会让
返回的数字和请求对不上。

数据来自执行明细，因此统计口径是「投递」而不是「消息」：一次触发分发到三个渠道
就是三条记录，某个渠道失败不影响同一次触发里其它渠道的成功。``skipped`` 是被
订阅者偏好拦下的渠道，没有尝试投递，不进成功率的分母。

口径限制：执行明细目前不携带环境标记，所以这里给出的是整个部署的汇总，不按
``X-Environment`` 拆分；分环境统计要等明细带上环境字段之后才能做。
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .admin_stats import (
    DEFAULT_TOP,
    DEFAULT_WINDOW_DAYS,
    MAX_TOP,
    MAX_WINDOW_DAYS,
    AdminStatsAccumulator,
    WindowError,
    resolve_window,
)
from .execution_log import get_execution_store

router = APIRouter(prefix="/v1/admin", tags=["admin"])


@router.get(
    "/stats",
    summary="管理统计：渠道成败、耗时分布与失败原因",
    description=(
        "按执行明细聚合一段时间内的投递结果。\n\n"
        "**返回内容**：``totals`` 为整体计数与成功率、平均/分位/最大耗时；"
        "``channels`` 按渠道拆分（尝试数、成功、失败、跳过、成功率、耗时）；"
        "``providers`` 与 ``workflows`` 给出实际完成投递的实现与工作流排行；"
        "``failure_reasons`` 是失败原因归并后的榜单；``trend`` 按天给出成败分布。\n\n"
        "**窗口**：``days`` 与 ``since`` 互斥，``until`` 可与任一搭配；窗口最长 90 天。\n\n"
        "**口径**：一条记录对应一次「触发 × 渠道」的投递；``skipped`` 表示该渠道被"
        "订阅者偏好关闭，没有尝试投递，不计入成功率分母。耗时分位由固定桶直方图估算，"
        "受桶边界粒度约束。"
    ),
    responses={
        200: {"description": "统计结果"},
        400: {"description": "窗口参数冲突或区间不合法"},
        422: {"description": "参数越界（days / top）"},
    },
)
def admin_stats(
    days: Optional[int] = Query(
        default=None,
        ge=1,
        le=MAX_WINDOW_DAYS,
        description=f"统计窗口天数（默认 {DEFAULT_WINDOW_DAYS}，与 since 互斥）",
    ),
    since: Optional[datetime] = Query(default=None, description="窗口起点（ISO 8601，含）"),
    until: Optional[datetime] = Query(default=None, description="窗口终点（ISO 8601，含，默认当前时刻）"),
    top: int = Query(default=DEFAULT_TOP, ge=1, le=MAX_TOP, description="各榜单返回条数"),
) -> dict:
    """聚合一段窗口内的投递明细。"""
    try:
        window = resolve_window(days=days, since=since, until=until)
    except WindowError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # 逐条流式读取：窗口内的记录不落成列表，内存占用与投递量无关。
    records = get_execution_store().scan(since=window.since, until=window.until)
    return AdminStatsAccumulator(window, top=top).extend(records).result()


__all__ = ["router"]
