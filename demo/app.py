"""演示应用的 HTTP 入口：FastAPI 路由接到 Community 服务。

    cd <仓库根目录>
    api/.venv/bin/uvicorn demo.app:app --port 8100

路由只做「解析请求 → 调服务 → 把回执折成响应」，通知语义全部在
:mod:`demo.community` 与 SDK 里。应用不落库，帖子与档案
存在进程内存里，重启即空——演示项目要展示的是接入方式，不是存储。
"""
from __future__ import annotations

import os
from pathlib import Path
from secrets import compare_digest
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
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
    body: str = Field(default="", max_length=4000)


class ActorIn(BaseModel):
    actor: str


class CommentIn(BaseModel):
    actor: str
    text: str = Field(max_length=1000)
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
    if os.getenv("XW_DEMO_SEED") == "1":
        community.register_creator("demo-creator", name="演示创作者")
        for post_id, title, body in (
            ("plants", "阳台的一平方米春天", "把耐阴植物放在内侧，给窗边留一点位置晒太阳。"),
            ("street", "周六的城市漫步", "从老街一路走到车站，路上遇见几家还想再来的小店。"),
            ("coast", "在海边等一场日落", "傍晚的风刚刚好。带一件外套，等天色慢慢变蓝。"),
            ("room", "把客厅留给午后的光", "换了一盏暖灯，把常看的书放在手边。"),
            ("food", "一碗清爽的周末午餐", "蔬菜烤到边缘微焦，再拌一点柠檬汁和橄榄油。"),
            ("mountain", "初秋徒步小记", "早一点出发，给沿途停下来拍照和休息留足时间。"),
            ("coffee", "街角咖啡与慢下来的上午", "点了一杯手冲，在窗边读了半小时书。"),
            ("demo-post", "周末骑行路线", "周末准备沿江骑行。你最喜欢哪一段路线？"),
        ):
            community.create_post(post_id, creator_id="demo-creator", title=title, body=body)
    owner_password = os.getenv("XW_DEMO_OWNER_PASSWORD", "")
    guest_password = os.getenv("XW_DEMO_GUEST_PASSWORD", "")
    if guest_password and (not owner_password or guest_password == owner_password):
        raise ValueError("请为 owner 和 guest 分别配置不同的口令")

    basic = HTTPBasic(auto_error=False)

    def account(credentials: Optional[HTTPBasicCredentials] = Depends(basic)) -> str:
        if not owner_password:
            return "owner"
        if credentials:
            expected = {"owner": owner_password, "guest": guest_password}.get(credentials.username)
            if expected and compare_digest(credentials.password.encode(), expected.encode()):
                return credentials.username
        raise HTTPException(status_code=401, detail="账号或口令不正确",
                            headers={"WWW-Authenticate": "Basic"})

    def require_owner(role: str = Depends(account)) -> None:
        if role != "owner":
            raise HTTPException(status_code=403, detail="此操作仅对 owner 开放")

    assets = Path(__file__).resolve().parents[1] / "dashboard" / "community"
    app.mount("/community-assets", StaticFiles(directory=assets), name="community-assets")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(assets / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/session")
    def session(role: str = Depends(account)) -> Dict[str, str]:
        return {"role": role}

    @app.get("/posts")
    def posts() -> list[Dict[str, Any]]:
        return community.list_posts()

    @app.get("/community-overview", dependencies=[Depends(require_owner)])
    def overview() -> Dict[str, Any]:
        entries = community.list_posts()
        inboxes = [community.inbox(creator_id) for creator_id in dict.fromkeys(
            post["creator_id"] for post in entries
        )]
        return {
            "posts": len(entries),
            "likes": sum(post["likes"] for post in entries),
            "unread": sum(inbox["unread"] for inbox in inboxes),
            "notifications": [item for inbox in inboxes for item in inbox["items"]],
        }

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/creators", status_code=201, dependencies=[Depends(require_owner)])
    def register_creator(body: CreatorIn) -> Dict[str, Any]:
        return _call(community.register_creator, body.id, name=body.name,
                     email=body.email, device_token=body.device_token)

    @app.post("/posts", status_code=201, dependencies=[Depends(require_owner)])
    def create_post(body: PostIn) -> Dict[str, Any]:
        return _call(community.create_post, body.id,
                     creator_id=body.creator_id, title=body.title, body=body.body)

    @app.post("/posts/{post_id}/like", dependencies=[Depends(account)])
    def like(post_id: str, body: ActorIn) -> Dict[str, Any]:
        return _ack(_call(community.like, post_id, actor=body.actor))

    @app.post("/posts/{post_id}/comment")
    def comment(post_id: str, body: CommentIn, role: str = Depends(account)) -> Dict[str, Any]:
        return _ack(_call(community.comment, post_id, actor="guest" if role == "guest" else body.actor,
                          text=body.text, comment_id=body.comment_id))

    @app.post("/posts/{post_id}/mention", dependencies=[Depends(require_owner)])
    def mention(post_id: str, body: MentionIn) -> Dict[str, Any]:
        return _ack(_call(community.mention, post_id, actor=body.actor,
                          target=body.target, text=body.text))

    @app.get("/creators/{creator_id}/inbox", dependencies=[Depends(require_owner)])
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
