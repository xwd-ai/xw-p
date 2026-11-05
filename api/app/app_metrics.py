"""本项目的指标定义与记录入口。

所有指标集中在这里，而不是散落在调用点上各写一遍名字：名字、标签集合与
含义写在一处，看板与告警才对得上，改标签时也只需要改一个地方。

标签基数的边界是刻意收窄的：

- HTTP 用**路由模板**（``/v1/workflows/{workflow_id}``）而不是原始路径。按
  原始路径记，每来一个工作流标识就多一条序列，抓取端的内存会跟着请求量涨。
- 未知工作流的名称不进标签（请求可以随便填），统一记成 ``unknown``。
- 其余标签的取值范围都来自配置或枚举（渠道、环境、结果），不会随流量膨胀。
"""
from __future__ import annotations

from typing import Optional

from . import config
from .metrics import Registry, get_registry

#: 未匹配到路由（404）或名称不可信时使用的占位标签值。
UNKNOWN = "unknown"

#: 路由标签的最大长度；超长的路径只可能来自构造出来的请求。
_MAX_ROUTE = 96


def _registry() -> Registry:
    return get_registry()


# 指标族在首次访问时创建；同一进程里重复调用返回同一份计数。
def http_requests():
    return _registry().counter(
        "xw_http_requests_total",
        "按方法、路由模板与状态码统计的请求数",
        ("method", "route", "status"),
    )


def http_duration():
    return _registry().histogram(
        "xw_http_request_duration_seconds",
        "请求处理耗时（秒），含响应写出",
        ("method", "route"),
    )


def http_in_flight():
    return _registry().gauge(
        "xw_http_in_flight_requests",
        "当前正在处理中的请求数",
    )


def events_triggered():
    return _registry().counter(
        "xw_events_triggered_total",
        "按工作流、环境与结果统计的互动事件受理数",
        ("workflow", "environment", "result"),
    )


def idempotency_requests():
    return _registry().counter(
        "xw_idempotency_requests_total",
        "幂等键的处理结果：claimed（首次受理）/ replayed（返回既有回执）/ conflict（键不可读）",
        ("result",),
    )


def rate_limit_rejected():
    return _registry().counter(
        "xw_rate_limit_rejected_total",
        "被限流拒绝的请求数（按判定维度）",
        ("dimension",),
    )


def queue_depth():
    return _registry().gauge(
        "xw_queue_depth",
        "消息总线上待消费的消息数",
        ("backend",),
    )


def queue_enqueued():
    return _registry().counter(
        "xw_queue_enqueued_total",
        "写入消息总线的消息数",
        ("backend",),
    )


def build_info():
    return _registry().gauge(
        "xw_build_info",
        "进程身份信息，值恒为 1；用于在抓取端按版本与后端分组",
        ("version", "storage", "bus", "worker"),
    )


def ensure_registered() -> None:
    """把所有指标族在 registry 里登记一遍。

    指标族是首次访问时创建的，若某个族还没有任何样本，渲染时就不会出现——
    看板上「指标消失」和「值为 0」是两种完全不同的信号，抓取端不该因为
    服务刚启动就少收到一半指标名。启动与抓取各调一次即可。
    """
    for family in (
        http_requests,
        http_duration,
        http_in_flight,
        events_triggered,
        idempotency_requests,
        rate_limit_rejected,
        queue_depth,
        queue_enqueued,
        build_info,
    ):
        family()


def route_label(route: Optional[str]) -> str:
    """把路由模板收敛成标签值。

    路由已匹配时 Starlette 会在 ``scope["route"].path`` 给出模板；未匹配时
    没有这个键，调用方传 ``None``。原始路径不参与标签，避免按请求参数分叉。
    """
    if not route:
        return UNKNOWN
    return route[:_MAX_ROUTE]


def observe_http(method: str, route: Optional[str], status: int, seconds: float) -> None:
    """记一次请求：计数 + 耗时。异常不应影响请求本身，由调用方兜住。"""
    label = route_label(route)
    http_requests().inc(method=method.upper(), route=label, status=str(status))
    http_duration().observe(seconds, method=method.upper(), route=label)


def record_event(workflow: Optional[str], environment: str, result: str) -> None:
    """记一条被受理（或回放、拒绝）的互动事件。"""
    events_triggered().inc(
        workflow=(workflow or UNKNOWN)[:64],
        environment=(environment or UNKNOWN)[:32],
        result=result,
    )


def record_idempotency(result: str) -> None:
    idempotency_requests().inc(result=result)


def record_rate_limited(dimension: str) -> None:
    rate_limit_rejected().inc(dimension=dimension)


def record_enqueued(count: int = 1) -> None:
    if count > 0:
        queue_enqueued().inc(float(count), backend=config.BUS_BACKEND)


def mark_build_info(version: str, *, worker: bool) -> None:
    """在启动时写入一次进程身份，便于按部署维度过滤。"""
    build_info().set(
        1.0,
        version=version,
        storage=config.STORAGE_BACKEND,
        bus=config.BUS_BACKEND,
        worker="on" if worker else "off",
    )


def collect_queue_depth(source) -> None:
    """给队列深度挂一个抓取时的取值函数。

    ``source`` 返回待消费条数；异常由指标层吞掉并跳过这一族——抓取端拿不到
    队列深度可以接受，整次抓取失败则会让所有指标一起断流。
    """
    queue_depth().source(lambda: [({"backend": config.BUS_BACKEND}, float(source()))])
