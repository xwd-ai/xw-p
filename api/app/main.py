"""xw-notify 触发 API（FastAPI）。

Phase 0：提供健康检查与事件触发端点，内存队列 + 后台 worker 打通端到端链路。
Phase 1：触发端点完整实现 —— 入参校验（Pydantic）、模板查找、
幂等键去重、返回 acknowledgmentId。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, status

from . import config
from .bus import get_bus
from .idempotency import get_idempotency_store
from .schemas import TriggerEvent, TriggerResponse, new_queued_message
from .worker import Worker
from .workflow import get_registry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

logger = logging.getLogger("xw.api")

_worker = Worker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _worker.start()
    yield
    _worker.stop()


app = FastAPI(
    title="xw-notify API",
    version="0.1.0",
    description="统一通知触发 API",
    lifespan=lifespan,
)


@app.get("/health", tags=["system"])
def health() -> dict:
    return {"status": "ok", "queued": get_bus().size()}


@app.post("/v1/events/trigger", response_model=TriggerResponse, tags=["trigger"])
def trigger_event(
    event: TriggerEvent,
    idempotency_key: Optional[str] = Header(
        default=None, alias="Idempotency-Key", description="幂等键：相同键重复请求返回首次回执"
    ),
) -> TriggerResponse:
    """触发一条通知：校验 → 模板查找 → 幂等去重 → 入队 → 返回回执。

    幂等语义：携带 ``Idempotency-Key`` 的请求若此前已成功处理，直接返回缓存回执，
    不会重复入队；未携带则该键的请求每次都会生成新回执。
    """
    # 1. 模板查找：触发标识必须对应一个已启用的工作流
    workflow = get_registry().get(event.name)
    if workflow is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"工作流模板不存在或未启用：{event.name}",
        )

    store = get_idempotency_store()

    # 2. 幂等去重：命中缓存则直接返回既有回执
    if idempotency_key:
        cached = store.get(idempotency_key)
        if cached is not None:
            logger.info("幂等命中：%s（返回缓存回执）", idempotency_key)
            return TriggerResponse(**cached)

    # 3. 生成回执并入队
    message = new_queued_message(event)
    response = TriggerResponse(
        trigger_id=message.trigger_id,
        transaction_id=message.transaction_id,
        acknowledgment_id=message.trigger_id,
    )

    # 4. 入队前登记幂等键（若重复，则返回缓存回执，避免并发重复入队）
    if idempotency_key:
        payload = response.model_dump(by_alias=True)
        inserted = store.put_if_absent(idempotency_key, payload, config.IDEMPOTENCY_TTL)
        if not inserted:
            # 并发场景下被其他请求抢先写入：回读并返回
            cached = store.get(idempotency_key)
            if cached is not None:
                return TriggerResponse(**cached)

    get_bus().publish(message)
    logger.info("已触发：%s -> %s（ack=%s）", event.name, message.subscriber_id, message.trigger_id)
    return response
