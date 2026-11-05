"""内容社区业务侧：创作者、帖子与三类互动，通知全部交给 xw-notify。

演示项目的边界很明确：只做业务建模。点赞、评论、@提及发生时通过
:mod:`xw_notify` 触发对应事件，渠道选择、模板渲染、聚合与重试都由平台
负责，业务侧不碰这些细节——这正是「一套 API，多端触达」的接入方式。

收件箱读取走客户端的底层 :meth:`~xw_notify.Client.request`：收件箱接口
目前没有独立资源类，按 SDK 示例同样的方式直接取 ``/v1/inbox``。

演示项目不强制安装 SDK：仓库内的 ``sdk/python`` 直接加进模块搜索路径，
克隆下来就能跑。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from xw_notify import Client, TriggerAck  # noqa: E402  （需在路径装配之后）


class CommunityError(Exception):
    """业务侧的可预期错误（帖子或创作者不存在、入参为空）。"""


class Community:
    """内存版内容社区：帖子与互动只管业务，通知经客户端触发。

    用法::

        community = Community(Client("http://127.0.0.1:8000"))
        community.register_creator("creator-1001", name="小林",
                                   email="lin@example.com", device_token="fcm-...")
        community.create_post("P-1", creator_id="creator-1001", title="第一支 Vlog")
        community.like("P-1", actor="阿澈")
    """

    def __init__(self, client: Client) -> None:
        self._client = client
        self._creators: Dict[str, Dict[str, Any]] = {}
        self._posts: Dict[str, Dict[str, Any]] = {}

    # -- 档案与帖子 -------------------------------------------------------

    def register_creator(
        self,
        creator_id: str,
        *,
        name: str,
        email: Optional[str] = None,
        device_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """登记创作者档案。

        ``email`` 与 ``device_token`` 是通知的收件地址：平台从触发请求的
        ``to`` 取地址而不回查业务库，触发时会把这两个值一并带上。
        """
        key = self._required(creator_id, "creator_id")
        record = {
            "creator_id": key,
            "name": self._required(name, "name"),
            "email": (email or "").strip() or None,
            "device_token": (device_token or "").strip() or None,
        }
        self._creators[key] = record
        return dict(record)

    def create_post(self, post_id: str, *, creator_id: str, title: str) -> Dict[str, Any]:
        key = self._required(post_id, "post_id")
        self._creator(creator_id)
        record = {
            "post_id": key,
            "creator_id": creator_id,
            "title": self._required(title, "title"),
        }
        self._posts[key] = record
        return dict(record)

    # -- 互动 -----------------------------------------------------------

    def like(self, post_id: str, *, actor: str) -> TriggerAck:
        """帖子被点赞 → ``post_liked``（按帖子聚合，多次点赞收敛为一张卡片）。"""
        post = self._post(post_id)
        return self._notify("post_liked", post, actor=self._required(actor, "actor"))

    def comment(
        self,
        post_id: str,
        *,
        actor: str,
        text: str,
        comment_id: Optional[str] = None,
    ) -> TriggerAck:
        """帖子收到评论 → ``post_commented``。

        给了 ``comment_id`` 就作为幂等键：同一条评论重发（客户端重试、用户
        双击）不会让创作者收到两遍。
        """
        post = self._post(post_id)
        key = f"comment-{self._required(comment_id, 'comment_id')}" if comment_id else None
        return self._notify(
            "post_commented",
            post,
            actor=self._required(actor, "actor"),
            comment=self._required(text, "text"),
            idempotency_key=key,
        )

    def mention(
        self,
        post_id: str,
        *,
        actor: str,
        target: str,
        text: str = "",
    ) -> TriggerAck:
        """评论区有人被 @ → ``post_mentioned``，通知被提及的人。"""
        post = self._post(post_id)
        self._creator(target)  # 被提及者必须有档案，否则没有收件地址
        return self._notify(
            "post_mentioned",
            post,
            actor=self._required(actor, "actor"),
            comment=text or None,
            subscriber_id=target,
        )

    # -- 收件箱 ----------------------------------------------------------

    def inbox(
        self,
        creator_id: str,
        *,
        unread: bool = False,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """读创作者的站内信收件箱，返回 ``items`` / ``unread`` / ``total``。"""
        self._creator(creator_id)
        query: Dict[str, Any] = {"subscriberId": creator_id, "limit": max(1, min(int(limit), 100))}
        if unread:
            query["unread"] = True
        page = self._client.request("GET", "/v1/inbox", query=query)
        if not isinstance(page, Mapping):
            raise CommunityError("收件箱响应不是对象")
        return {
            "items": list(page.get("items") or []),
            "unread": int(page.get("unread") or 0),
            "total": int(page.get("total") or 0),
        }

    # -- 内部 -----------------------------------------------------------

    def _notify(
        self,
        name: str,
        post: Mapping[str, Any],
        *,
        actor: str,
        comment: Optional[str] = None,
        subscriber_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> TriggerAck:
        subscriber = self._creator(subscriber_id or post["creator_id"])
        payload: Dict[str, Any] = {
            "post_id": post["post_id"],
            "post_title": post["title"],
            "actor": actor,
        }
        if comment is not None:
            payload["comment"] = comment
        # 收件地址取自档案并随触发请求发出：平台按消息里的地址投递。
        return self._client.trigger.send(
            name,
            subscriber["creator_id"],
            payload=payload,
            email=subscriber.get("email"),
            device_token=subscriber.get("device_token"),
            idempotency_key=idempotency_key,
        )

    def _creator(self, creator_id: str) -> Dict[str, Any]:
        record = self._creators.get(creator_id)
        if record is None:
            raise CommunityError(f"创作者不存在：{creator_id}")
        return record

    def _post(self, post_id: str) -> Dict[str, Any]:
        record = self._posts.get(post_id)
        if record is None:
            raise CommunityError(f"帖子不存在：{post_id}")
        return record

    @staticmethod
    def _required(value: str, field: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise CommunityError(f"{field} 不能为空")
        return text


def build_client() -> Client:
    """从环境变量装配客户端，与 SDK 示例同一组变量。

    ``XW_BASE_URL`` / ``XW_API_KEY`` / ``XW_ENVIRONMENT``，未配置时指向
    本机默认部署（``http://127.0.0.1:8000``，开发环境，无凭据）。
    """
    return Client(
        os.environ.get("XW_BASE_URL", "http://127.0.0.1:8000"),
        api_key=os.environ.get("XW_API_KEY"),
        environment=os.environ.get("XW_ENVIRONMENT", "development"),
    )
