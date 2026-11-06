"""演示应用的 HTTP 入口：FastAPI 路由接到 Community 服务。

    cd <仓库根目录>
    api/.venv/bin/uvicorn demo.app:app --port 8100

路由只做「解析请求 → 调服务 → 把回执折成响应」，通知语义全部在
:mod:`demo.community` 与 SDK 里。配置数据库文件后，账号和社区状态可跨重启保留。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .community import Community, CommunityError, build_client  # 先装配 SDK 路径
from .store import Store, guest_name_valid, import_posts
from xw_notify import TriggerAck

OWNER_CREATOR_ID = "demo-creator"


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


class GuestIn(BaseModel):
    username: str
    password: str = Field(min_length=8, max_length=128, pattern=r"^[ -~]+$")


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
    database = os.getenv("XW_DEMO_DB")
    store = Store(database)
    snapshot = Path(database).with_name("import-posts.json") if database else None
    if snapshot and snapshot.is_file() and store.load() is None:
        store.save(import_posts(snapshot))
    state = store.load()
    single_creator = os.getenv("XW_DEMO_SINGLE_CREATOR") == "1"
    if single_creator and state is not None:
        creators = state["creators"]
        owner = creators.get(OWNER_CREATOR_ID) or creators.get("community-owner") or {}
        state["creators"] = {
            OWNER_CREATOR_ID: {**owner, "creator_id": OWNER_CREATOR_ID, "name": "owner"},
        }
        for post in state["posts"].values():
            post["creator_id"] = OWNER_CREATOR_ID
        store.save(state)
    seed = state is None and os.getenv("XW_DEMO_SEED") == "1"
    community = Community(client if client is not None else build_client(), store if database else None)
    if seed:
        community.register_creator(OWNER_CREATOR_ID, name="owner" if single_creator else "演示创作者")
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
            community.create_post(post_id, creator_id=OWNER_CREATOR_ID, title=title, body=body)
    elif single_creator and state is None:
        community.register_creator(OWNER_CREATOR_ID, name="owner")
    owner_password = os.getenv("XW_DEMO_OWNER_PASSWORD", "")
    guest_password = os.getenv("XW_DEMO_GUEST_PASSWORD", "")
    if guest_password and (not owner_password or guest_password == owner_password):
        raise ValueError("请为 owner 和 guest 分别配置不同的口令")
    if database and not owner_password:
        raise ValueError("持久化部署需要配置 owner 口令")
    if owner_password:
        store.set_account("owner", owner_password, "owner", bootstrap=True)
    if guest_password:
        store.set_account("guest", guest_password, "guest", bootstrap=True)

    basic = HTTPBasic(auto_error=False)

    def identity(credentials: Optional[HTTPBasicCredentials] = Depends(basic)) -> tuple[str, str]:
        if not owner_password:
            return "owner", "owner"
        if credentials:
            role = store.authenticate(credentials.username, credentials.password)
            if role:
                return credentials.username, role
        raise HTTPException(status_code=401, detail="账号或口令不正确",
                            headers={"WWW-Authenticate": "Basic"})

    def account(user: tuple[str, str] = Depends(identity)) -> str:
        return user[1]

    def require_owner(role: str = Depends(account)) -> None:
        if role != "owner":
            raise HTTPException(status_code=403, detail="此操作仅对 owner 开放")

    assets = Path(__file__).resolve().parents[1] / "dashboard" / "community"
    app.mount("/community-assets", StaticFiles(directory=assets), name="community-assets")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(assets / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/session")
    def session(user: tuple[str, str] = Depends(identity)) -> Dict[str, str]:
        return {"username": user[0], "role": user[1]}

    @app.get("/posts", dependencies=[Depends(account)])
    def posts(response: Response) -> list[Dict[str, Any]]:
        response.headers["Cache-Control"] = "no-store"
        return community.list_posts()

    @app.get("/guest-accounts", dependencies=[Depends(require_owner)])
    def guest_accounts() -> list[str]:
        return store.guest_names()

    @app.put("/guest-accounts", dependencies=[Depends(require_owner)])
    def set_guest(body: GuestIn) -> Dict[str, str]:
        if not guest_name_valid(body.username) or body.password == owner_password:
            raise HTTPException(status_code=422, detail="请使用独立的访客账号和口令")
        store.set_account(body.username, body.password, "guest")
        return {"username": body.username}

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
        if single_creator and not ({"email", "device_token"} & body.model_fields_set):
            return {"creator_id": OWNER_CREATOR_ID, "name": "owner"}
        return _call(community.register_creator, OWNER_CREATOR_ID if single_creator else body.id,
                     name="owner" if single_creator else body.name,
                     email=body.email, device_token=body.device_token)

    @app.post("/posts", status_code=201, dependencies=[Depends(require_owner)])
    def create_post(body: PostIn) -> Dict[str, Any]:
        return _call(community.create_post, body.id,
                     creator_id=OWNER_CREATOR_ID if single_creator else body.creator_id,
                     title=body.title, body=body.body)

    @app.post("/posts/{post_id}/like")
    def like(post_id: str, body: ActorIn, user: tuple[str, str] = Depends(identity)) -> Dict[str, Any]:
        return _ack(_call(community.like, post_id, actor=user[0] if user[1] == "guest" else body.actor))

    @app.post("/posts/{post_id}/comment")
    def comment(post_id: str, body: CommentIn, user: tuple[str, str] = Depends(identity)) -> Dict[str, Any]:
        return _ack(_call(community.comment, post_id, actor=user[0] if user[1] == "guest" else body.actor,
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
