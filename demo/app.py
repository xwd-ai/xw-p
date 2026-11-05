"""演示应用的 HTTP 入口：FastAPI 路由接到 Community 服务。

    cd <仓库根目录>
    api/.venv/bin/uvicorn demo.app:app --port 8100

路由只做「解析请求 → 调服务 → 把回执折成响应」，通知语义全部在
:mod:`demo.community` 与 SDK 里。应用不落库，帖子与档案
存在进程内存里，重启即空——演示项目要展示的是接入方式，不是存储。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .community import Community, CommunityError, build_client  # 先装配 SDK 路径
from xw_notify import TriggerAck


class CreatorIn(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    device_token: Optional[str] = None


class PostIn(BaseModel):
    id: str
    creator_id: str
    title: str


class ActorIn(BaseModel):
    actor: str


class CommentIn(BaseModel):
    actor: str
    text: str
    comment_id: Optional[str] = None


class MentionIn(BaseModel):
    actor: str
    target: str
    text: str = Field(default="")


def _ack(receipt: TriggerAck) -> Dict[str, Any]:
    return {
        "triggerId": receipt.trigger_id,
        "transactionId": receipt.transaction_id,
        "acknowledgmentId": receipt.acknowledgment_id,
        "acknowledged": receipt.acknowledged,
    }


def create_app(client: Optional[Any] = None) -> FastAPI:
    """装配应用；测试里传入带假传输层的客户端，运行时从环境变量取。"""
    app = FastAPI(title="xw-notify 内容社区演示", version="0.1.0")
    community = Community(client if client is not None else build_client())

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/creators", status_code=201)
    def register_creator(body: CreatorIn) -> Dict[str, Any]:
        return _call(community.register_creator, body.id, name=body.name,
                     email=body.email, device_token=body.device_token)

    @app.post("/posts", status_code=201)
    def create_post(body: PostIn) -> Dict[str, Any]:
        return _call(community.create_post, body.id,
                     creator_id=body.creator_id, title=body.title)

    @app.post("/posts/{post_id}/like")
    def like(post_id: str, body: ActorIn) -> Dict[str, Any]:
        return _ack(_call(community.like, post_id, actor=body.actor))

    @app.post("/posts/{post_id}/comment")
    def comment(post_id: str, body: CommentIn) -> Dict[str, Any]:
        return _ack(_call(community.comment, post_id, actor=body.actor,
                          text=body.text, comment_id=body.comment_id))

    @app.post("/posts/{post_id}/mention")
    def mention(post_id: str, body: MentionIn) -> Dict[str, Any]:
        return _ack(_call(community.mention, post_id, actor=body.actor,
                          target=body.target, text=body.text))

    @app.get("/creators/{creator_id}/inbox")
    def inbox(creator_id: str, unread: bool = False, limit: int = 20) -> Dict[str, Any]:
        return _call(community.inbox, creator_id, unread=unread, limit=limit)

    return app


def _call(fn, *args: Any, **kwargs: Any) -> Any:
    """把业务错误折成 404，其余异常照常上抛。"""
    try:
        return fn(*args, **kwargs)
    except CommunityError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


app = create_app()
