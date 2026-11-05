"""``/metrics``：把进程内指标按 Prometheus 文本格式暴露出去。

为什么不是按租户拆的接口：指标描述的是这个进程本身（吞吐、延迟、队列深度），
它不属于任何租户，也不该因为请求头不同而返回不同内容。抓取端只有一条配置，
把它放在部署级是唯一合理的做法。也因为它不读环境头，暴露面需要部署侧控制
（内网监听、网关鉴权或抓取白名单），这一点写在 docs/metrics.md 里。
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from . import app_metrics
from .metrics import CONTENT_TYPE, get_registry

router = APIRouter()


@router.get(
    "/metrics",
    tags=["system"],
    summary="Prometheus 指标",
    description=(
        "按 Prometheus 文本格式（version 0.0.4）输出本实例自启动以来的指标：\n\n"
        "- ``xw_http_requests_total`` / ``xw_http_request_duration_seconds``：按方法、"
        "路由模板与状态码的请求量与耗时；\n"
        "- ``xw_events_triggered_total``：按工作流、环境与结果（accepted / replayed / "
        "rejected）的互动事件受理数；\n"
        "- ``xw_idempotency_requests_total``：幂等键的三种处理结果；\n"
        "- ``xw_rate_limit_rejected_total``：被限流拒绝的请求数；\n"
        "- ``xw_queue_depth`` / ``xw_queue_enqueued_total``：队列深度与入队量；\n"
        "- ``xw_build_info``：版本与后端组合，值恒为 1。\n\n"
        "进程重启后计数归零，跨实例汇总由抓取端完成。"
    ),
    response_class=PlainTextResponse,
    responses={200: {"content": {CONTENT_TYPE: {}}, "description": "指标文本"}},
)
def metrics() -> PlainTextResponse:
    # 先登记全部指标族：没有任何样本的族也要出现在输出里，否则抓取端会
    # 把「还没有流量」误读成「这个指标没了」。
    app_metrics.ensure_registered()
    return PlainTextResponse(
        get_registry().render(),
        media_type=CONTENT_TYPE,
        headers={"Cache-Control": "no-store"},
    )


def install_queue_depth(source) -> None:
    """把队列深度接到抓取时的取值函数上（启动时调用一次）。"""
    app_metrics.collect_queue_depth(source)
