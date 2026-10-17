"""xw-notify 触发 API（FastAPI）。

Phase 0：提供健康检查与事件触发端点，内存队列 + 后台 worker 打通端到端链路。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .bus import get_bus
from .schemas import TriggerEvent, TriggerResponse, new_queued_message
from .worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

_worker = Worker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _worker.start()
    yield
    _worker.stop()


app = FastAPI(
    title="xw-notify API",
    version="0.1.0",
    description="统一通知触发 API（demo 阶段）",
    lifespan=lifespan,
)


@app.get("/health", tags=["system"])
def health() -> dict:
    return {"status": "ok", "queued": get_bus().size()}


@app.post("/v1/events/trigger", response_model=TriggerResponse, tags=["trigger"])
def trigger_event(event: TriggerEvent) -> TriggerResponse:
    message = new_queued_message(event)
    get_bus().publish(message)
    return TriggerResponse(trigger_id=message.trigger_id, transaction_id=message.transaction_id)
