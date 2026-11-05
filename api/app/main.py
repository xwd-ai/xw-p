"""xw-notify 触发 API（FastAPI）。

Phase 0：提供健康检查与事件触发端点，内存队列 + 后台 worker 打通端到端链路。
Phase 1：触发端点完整实现 —— 入参校验（Pydantic）、模板查找、
幂等键去重、返回 acknowledgmentId。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import app_metrics, config, openapi as openapi_docs, tokens
from .activity_log import build_activity, record_activities, record_activity
from .admin_activity_routes import router as admin_activity_router
from .admin_routes import router as admin_router
from .auth import EnvContext, require_env
from .auth_routes import router as auth_router
from .activity_routes import router as activity_router
from .bus import get_bus, publish_all
from .dashboard_routes import router as dashboard_router
from .execution_routes import router as execution_router
from .health import readiness
from .idempotency import IdempotencyConflict, claim, get_idempotency_store
from .inbox import router as inbox_router
from .logging_setup import configure_from_config
from .metrics_middleware import MetricsMiddleware
from .metrics_routes import install_queue_depth
from .metrics_routes import router as metrics_router
from .preference_routes import router as preference_router
from .ratelimit import get_limiter
from .ratelimit_middleware import RateLimitMiddleware
from .providers import factory as provider_factory
from .provider_routes import router as provider_router
from .request_log import RequestLogMiddleware
from .schemas import (
    BatchTriggerItem,
    BatchTriggerRequest,
    BatchTriggerResponse,
    QueuedMessage,
    TriggerEvent,
    TriggerResponse,
    new_queued_message,
)
from .subscriber_routes import router as subscriber_router
from .topic_routes import router as topic_router
from .trigger_core import missing_workflow_detail, resolve_workflow
from .worker import Worker
from .workflow_routes import router as workflow_router

# 日志按 XW_LOG_FORMAT / XW_LOG_LEVEL 装配；在导入时完成，uvicorn 的默认文本
# handler 会被就地替换，启动过程本身也走同一套格式。
configure_from_config()

logger = logging.getLogger("xw.api")

_worker = Worker()


def _admin_auth_note() -> str:
    """后台鉴权的启动摘要。配置不完整时给出原因，而不是一句「已启用」。"""
    try:
        tokens.secret()
        if not tokens.login_configured():
            return "已启用，但未配置 XW_ADMIN_USERNAME / XW_ADMIN_PASSWORD，登录接口会返回 503"
        envs = ", ".join(tokens.admin_environments()) or "全部"
        return (
            f"已启用，管理员 {config.ADMIN_USERNAME} · 租户 {tokens.admin_tenant()} · "
            f"环境 {envs} · 有效期 {tokens.ttl_seconds()}s"
        )
    except tokens.JwtConfigError as exc:
        # 密钥缺失与配置写错都落到这里：后者（密钥过短、租户或环境非法）
        # 虽然 enabled() 为真，但校验必然失败，启动时就该说清楚。
        return f"未启用（{exc}），运维接口与业务接口同一套判定"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 生产环境误配文件后端时先指出来，避免带着非持久化语义上线。
    storage_hint = config.storage_warning()
    if storage_hint:
        logger.warning(storage_hint)
    # 后台鉴权的三种状态都在启动日志里说清：启用了但没配账号（登录必然 503）、
    # 启用了但密钥过短（校验直接报错），都是「配置以为生效了、实际用不了」的
    # 典型情形，等到第一次登录失败再来查代价更大。
    logger.info("后台令牌鉴权：%s", _admin_auth_note())
    # 限流默认关闭；配了阈值才在启动日志里说明维度、额度与后端，方便对照
    # 「被拦了」与「没配」这两种完全不同的现象。
    logger.info("请求限流：%s", get_limiter().describe())
    # 渠道注册表在启动时装配：配置写错直接暴露出来，而不是等到第一条消息
    # 投递失败才发现；尚未接入的种类与空着的渠道会在日志里逐条列出。
    provider_factory.get_registry()
    # 进程身份写进指标：抓取端据此区分版本与后端组合，重发布后看板能立刻
    # 分清新旧实例，而不是靠时间猜测。
    app_metrics.mark_build_info(openapi_docs.API_VERSION, worker=config.API_WORKER)
    # 队列深度在抓取时现算：它随流量变化，提前缓存一个值只会让人读到过期数据。
    install_queue_depth(lambda: get_bus().size())
    app_metrics.ensure_registered()
    # redis 总线由 Go 引擎消费；API 内置线程只在 memory 总线的单进程 demo 里起，
    # 否则两个消费者会抢同一条消息。
    if config.API_WORKER:
        _worker.start()
    else:
        logger.info("总线后端为 %s，API 内置消费线程不启动（由引擎消费队列）", config.BUS_BACKEND)
    yield
    if config.API_WORKER:
        _worker.stop()


app = FastAPI(
    title=openapi_docs.API_TITLE,
    version=openapi_docs.API_VERSION,
    description=openapi_docs.API_DESCRIPTION,
    openapi_tags=openapi_docs.TAGS,
    lifespan=lifespan,
)

# 指标放在最内层：限流拒绝的 429 也要被统计到，但 CORS 的预检请求在它外面，
# 不会被算成业务请求；请求标识与访问日志仍由更外层的中间件负责。
app.add_middleware(MetricsMiddleware)

# 限流注册在 CORS 之前：Starlette 里后注册的中间件在更外层，因此 CORS 会包住
# 限流——被拒绝的 429 也带着跨源响应头，浏览器才读得到「为什么被限流」，
# 而不是一个与后端无关的 Failed to fetch。
app.add_middleware(RateLimitMiddleware)

# 展示前端是独立的一组静态文件，与 API 不同源；没有这层中间件时，浏览器会把
# 页面里的 fetch 全部挡在 CORS 检查上。放行名单由 XW_CORS_ORIGINS 控制。
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    max_age=600,
)

# 请求标识与访问日志放在最外层：限流的 429、鉴权的 401、未捕获异常的 500 都要
# 带上同一个标识并回写响应头，只有包住全部内层才做得到。
app.add_middleware(RequestLogMiddleware, access_log=config.LOG_ACCESS)

app.include_router(auth_router)
app.include_router(workflow_router)
app.include_router(dashboard_router)
app.include_router(admin_router)
app.include_router(admin_activity_router)
app.include_router(activity_router)
app.include_router(execution_router)
app.include_router(inbox_router)
app.include_router(provider_router)
app.include_router(subscriber_router)
app.include_router(preference_router)
app.include_router(topic_router)
app.include_router(metrics_router)

# 文档 schema 在路由注册完成后装配：分组说明、鉴权与公共错误响应都依赖最终
# 的路由表，提前安装只会拿到一份不完整的 paths。
openapi_docs.install(app)


@app.exception_handler(IdempotencyConflict)
def _idempotency_conflict(_request: Request, exc: IdempotencyConflict) -> JSONResponse:
    """幂等键被占用却读不到回执：409，且明确告诉调用方本次没有入队。

    这条分支只在存储里的值不可读时出现（写坏、TTL 边界），但它是「重复投递」
    与「明确失败」的分界：按未命中继续往下走，同一个键会发出第二条通知。
    """
    logger.warning("幂等键冲突：%s（回执不可读，本次未入队）", exc.key)
    return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": exc.detail})


@app.get(
    "/health",
    tags=["system"],
    summary="健康检查",
    description=(
        "返回服务状态与当前总线里的待消费消息数。供负载均衡与部署脚本探测，"
        "不读环境头、不校验 Key。\n\n"
        "这是存活口径：不因为依赖不可用而返回非 200。需要按依赖判定的探针用 "
        "``/health/ready``，只需要「进程还在」的用 ``/health/live``。"
    ),
)
def health() -> dict:
    # 队列长度要读总线，Redis 不可用时这里会抛异常。存活检查不能因此变成
    # 500——那会把「依赖抖动」升级成「实例被编排器重启」，反而丢掉在途请求。
    try:
        queued = get_bus().size()
    except Exception as exc:  # noqa: BLE001 - 探活不承担诊断职责
        logger.warning("读取队列长度失败：%s", exc)
        queued = None
    return {"status": "ok", "queued": queued}


@app.get(
    "/health/live",
    tags=["system"],
    summary="存活检查",
    description="只回答「进程还在跑」，不触碰任何外部依赖。容器编排的 livenessProbe 用它。",
)
def health_live() -> dict:
    return {"status": "ok"}


@app.get(
    "/health/ready",
    tags=["system"],
    summary="就绪检查",
    description=(
        "逐项检查外部依赖：消息总线（Redis PING）、持久化后端（目录可写或 "
        "``SELECT 1``）、内置消费线程（仅在 ``XW_API_WORKER`` 开启时）。\n\n"
        "全部通过返回 200；任一失败返回 503 并给出失败项的原因，供编排器把"
        "实例摘出负载均衡。检查带超时，不会因为依赖挂死而拖住探针。"
    ),
    responses={503: {"description": "至少一项依赖不可用"}},
)
def health_ready() -> JSONResponse:
    status_code, body = readiness(worker=_worker)
    return JSONResponse(status_code=status_code, content=body)


@app.post(
    "/v1/events/trigger",
    response_model=TriggerResponse,
    tags=["trigger"],
    status_code=status.HTTP_200_OK,
    summary="触发一条互动事件通知",
    description=(
        "触发一条内容社区互动事件（点赞 / 评论 / @提及），按 ``name`` 匹配工作流模板，"
        "将通知分发到订阅者所在的多个渠道（站内信 / 邮件 / 短信 / 推送）。\n\n"
        "处理链路：鉴权（环境 + API Key）→ 模板查找 → 幂等去重 → 入队 → 返回回执。\n\n"
        "**环境隔离**：通过 ``X-Environment`` 指定隔离环境（默认 ``development``），"
        "模板查找与活动日志均打上该环境标记；配置了 API Key 的环境需携带 ``X-Api-Key`` 头。\n\n"
        "**幂等语义**：携带 ``Idempotency-Key`` 的请求若此前已成功处理，直接返回缓存回执，"
        "不会重复入队；未携带则每次请求都会生成新回执。"
    ),
    responses={
        status.HTTP_200_OK: {"description": "触发成功，返回回执（acknowledged=true）"},
        status.HTTP_400_BAD_REQUEST: {"description": "入参校验失败或未知环境"},
        status.HTTP_401_UNAUTHORIZED: {"description": "API Key 缺失或不匹配"},
        status.HTTP_404_NOT_FOUND: {"description": "工作流模板不存在或未启用"},
    },
)
def trigger_event(
    event: TriggerEvent,
    env: EnvContext = Depends(require_env),
    idempotency_key: Optional[str] = Header(
        default=None,
        alias="Idempotency-Key",
        description="幂等键：相同键重复请求返回首次回执",
        examples=["post-commented-P-2026-1001"],
    ),
) -> TriggerResponse:
    """触发一条通知：鉴权（环境 + API Key）→ 模板查找 → 幂等去重 → 入队 → 返回回执。

    环境隔离：请求通过 ``X-Environment`` 指定隔离环境（默认 development），
    模板查找与活动日志均打上该环境标记；配置了 API Key 的环境需携带
    ``X-Api-Key`` 头。

    幂等语义：携带 ``Idempotency-Key`` 的请求若此前已成功处理，直接返回缓存回执，
    不会重复入队；未携带则该键的请求每次都会生成新回执。
    """
    # 1. 模板查找：触发标识必须对应一个已启用的工作流（含环境匹配）。
    workflow = resolve_workflow(event.name, env.environment)
    if workflow is None:
        # 名称不进标签：请求可以填任意字符串，按它建序列等于让调用方决定
        # 指标基数。
        app_metrics.record_event(None, env.environment, "rejected")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=missing_workflow_detail(event.name, env.environment),
        )

    store = get_idempotency_store()

    # 2. 幂等去重：命中缓存则直接返回既有回执
    if idempotency_key:
        cached = store.get(idempotency_key)
        if cached is not None:
            logger.info("幂等命中：%s（返回缓存回执）", idempotency_key)
            app_metrics.record_idempotency("replayed")
            return TriggerResponse(**cached)

    # 3. 生成回执并入队（消息携带工作流的步骤快照，引擎据此执行）
    message = new_queued_message(event, workflow)
    response = TriggerResponse(
        trigger_id=message.trigger_id,
        transaction_id=message.transaction_id,
        acknowledgment_id=message.trigger_id,
    )

    # 4. 入队前登记幂等键（若被并发请求抢先，则返回那份回执，避免重复入队）
    if idempotency_key:
        reserved = claim(
            store, idempotency_key, response.model_dump(by_alias=True), config.IDEMPOTENCY_TTL
        )
        if not reserved.claimed:
            logger.info("幂等命中：%s（并发同键，返回先写入的回执）", idempotency_key)
            return TriggerResponse(**reserved.cached)

    # 5. 写入活动日志（活动流即时可见「已触发」，随后 worker 投递更新状态）
    record_activity(
        trigger_id=message.trigger_id,
        workflow=event.name,
        subscriber_id=message.subscriber_id,
        channels=message.channels,
        status="triggered",
        payload=event.payload,
        environment=env.environment,
    )

    get_bus().publish(message)
    # 单条入队不走 publish_all，计数在这里补一次；事件数按工作流维度记，
    # 名称来自已匹配成功的模板，基数受配置约束。
    app_metrics.record_enqueued(1)
    app_metrics.record_event(event.name, env.environment, "accepted")
    logger.info("已触发：%s -> %s（ack=%s）", event.name, message.subscriber_id, message.trigger_id)
    return response


@app.post(
    "/v1/events/trigger/batch",
    response_model=BatchTriggerResponse,
    tags=["trigger"],
    status_code=status.HTTP_200_OK,
    summary="批量触发互动事件通知",
    description=(
        "一次提交多条互动事件，按单条触发的同一套语义逐条处理：模板查找、环境匹配、"
        "生成回执，最后一次性把受理的事件写进队列。\n\n"
        "**为什么批量**：逐条触发时每条消息都要一次 HTTP 往返加一次 Redis 往返，"
        "互动高峰（比如一条热帖短时间涌入大量点赞）下往返开销会超过业务本身；"
        "这个接口把整批压成一次入队、一次活动日志写入。\n\n"
        "**部分失败**：某条事件的工作流不存在、未启用或环境不匹配时，该条记为 "
        "``rejected`` 并给出原因，同批其余事件照常入队；回执里的 ``results`` 与请求"
        "顺序一致，调用方按下标对齐即可。全部被拒时同样返回 200，逐条原因看 ``error``。\n\n"
        "**幂等语义**：``Idempotency-Key`` 作用于整批——相同键重复请求返回首次的回执，"
        "不会重复入队；逐条事件不带独立的幂等键。"
    ),
    responses={
        status.HTTP_200_OK: {"description": "整批已处理，逐条结果见 results"},
        status.HTTP_400_BAD_REQUEST: {"description": "未知环境"},
        status.HTTP_401_UNAUTHORIZED: {"description": "API Key 缺失或不匹配"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"description": "事件列表为空或超过上限"},
    },
)
def trigger_events_batch(
    payload: BatchTriggerRequest,
    env: EnvContext = Depends(require_env),
    idempotency_key: Optional[str] = Header(
        default=None,
        alias="Idempotency-Key",
        description="整批幂等键：相同键重复请求返回首次回执",
        examples=["post-liked-P-2026-1001-burst-1"],
    ),
) -> BatchTriggerResponse:
    """批量触发：逐条判定可触发性，受理的合并成一次入队。"""
    store = get_idempotency_store()

    # 1. 整批幂等：命中则不再校验也不再入队。
    #    与单条触发同样的取舍——缓存的是「那次请求的结果」，不是「这些事件」，
    #    所以重放要原样返回，包括哪些条目当时就被拒。
    if idempotency_key:
        cached = store.get(idempotency_key)
        if cached is not None:
            logger.info("幂等命中：%s（返回缓存回执，%d 条）", idempotency_key, len(cached.get("results") or []))
            app_metrics.record_idempotency("replayed")
            return BatchTriggerResponse(**cached)

    results: list[BatchTriggerItem] = []
    messages: list[QueuedMessage] = []
    activities: list[dict] = []

    for index, event in enumerate(payload.events):
        workflow = resolve_workflow(event.name, env.environment)
        if workflow is None:
            app_metrics.record_event(None, env.environment, "rejected")
            results.append(
                BatchTriggerItem(
                    index=index,
                    name=event.name,
                    subscriber_id=event.to.subscriber_id,
                    status="rejected",
                    error=missing_workflow_detail(event.name, env.environment),
                )
            )
            continue

        app_metrics.record_event(event.name, env.environment, "accepted")
        message = new_queued_message(event, workflow)
        messages.append(message)
        results.append(
            BatchTriggerItem(
                index=index,
                name=event.name,
                subscriber_id=message.subscriber_id,
                status="accepted",
                trigger_id=message.trigger_id,
                transaction_id=message.transaction_id,
                acknowledgment_id=message.trigger_id,
            )
        )
        activities.append(
            build_activity(
                trigger_id=message.trigger_id,
                workflow=event.name,
                subscriber_id=message.subscriber_id,
                channels=message.channels,
                status="triggered",
                payload=event.payload,
                environment=env.environment,
            )
        )

    response = BatchTriggerResponse(
        accepted=len(messages),
        rejected=len(results) - len(messages),
        results=results,
    )

    # 2. 入队前登记整批幂等键：并发同键请求只有一个能把消息发出去。
    if idempotency_key:
        reserved = claim(
            store, idempotency_key, response.model_dump(by_alias=True), config.IDEMPOTENCY_TTL
        )
        if not reserved.claimed:
            logger.info("幂等命中：%s（并发同键，返回先写入的整批回执）", idempotency_key)
            return BatchTriggerResponse(**reserved.cached)

    if messages:
        # 活动日志先写、再入队：活动流据此即时反映「已触发」，顺序与单条
        # 触发一致。两处都走批量通道，整批各自一次写入。
        record_activities(activities)
        publish_all(get_bus(), messages)

    logger.info(
        "批量触发：%d 条，受理 %d，拒绝 %d",
        len(results),
        response.accepted,
        response.rejected,
    )
    return response
