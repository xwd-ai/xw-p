"""订阅者接口：创建、读取、翻页、更新、删除。

两个容易踩的细节在这里显式建模：

1. **更新要区分「不传」与「传空」**。``update(sub)`` 不动邮箱，``update(sub,
   email=None)`` 把邮箱解绑；用 ``None`` 一个值顶替两种意图是做不到的，因此
   除 ``channels`` 外的字段默认值是 :data:`~xw_notify._unset.UNSET`。
2. **``channels`` 传空列表与不传不同**。不传表示不改，传 ``[]`` 表示关闭该
   订阅者的全部渠道（之后任何投递都会被跳过）；传 ``None`` 是最常见的误用，
   直接报错并说明该传什么，避免它被当成「不改」而让人以为渠道已经关掉。

翻页由游标驱动（服务端返回 ``nextCursor``）：:meth:`SubscribersResource.iter`
负责跟着游标走完全部，:meth:`SubscribersResource.list` 只取一页。
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Optional, Sequence

from ._http import quote_segment
from ._unset import UNSET
from .models import Subscriber, SubscriberPage

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型检查
    from .client import Client
    from .preferences import PreferencesResource

SUBSCRIBERS_PATH = "/v1/subscribers"

#: 服务端单页上限，超过会被 422 拒绝。
MAX_PAGE_SIZE = 200


def _item_path(subscriber_id: str) -> str:
    """单个订阅者的路径；标识为空时在本地报错，不去换一个 404。"""
    text = str(subscriber_id or "").strip()
    if not text:
        raise ValueError("subscriber_id 不能为空")
    return f"{SUBSCRIBERS_PATH}/{quote_segment(text)}"


class SubscribersResource:
    """``/v1/subscribers`` 相关接口，另挂 ``preferences`` 子资源。"""

    def __init__(self, client: "Client", *, preferences: "PreferencesResource") -> None:
        self._client = client
        self.preferences = preferences

    # -- 写 ---------------------------------------------------------------

    def create(
        self,
        *,
        subscriber_id: Optional[str] = None,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        device_token: Optional[str] = None,
        channels: Optional[Sequence[str]] = None,
    ) -> Subscriber:
        """创建订阅者，返回服务端落库后的记录。

        ``subscriber_id`` 留空由服务端生成（UUID）。传业务侧的用户 ID 更便于
        触发时直接引用。``channels`` 不传等于「没有可用渠道」——这样的订阅者
        能建出来，但任何投递都会被跳过，通常不是想要的结果。

        标识重复、邮箱或手机号已被别人占用时抛
        :class:`~xw_notify.errors.ConflictError`。
        """
        body: dict[str, Any] = {}
        if subscriber_id is not None and str(subscriber_id).strip():
            body["id"] = str(subscriber_id).strip()
        if email is not None and str(email).strip():
            body["email"] = str(email).strip()
        if phone is not None and str(phone).strip():
            body["phone"] = str(phone).strip()
        if device_token is not None and str(device_token).strip():
            body["device_token"] = str(device_token).strip()
        if channels is not None:
            body["channels"] = [str(item) for item in channels]
        return Subscriber.from_payload(self._client.request("POST", SUBSCRIBERS_PATH, json_body=body))

    def update(
        self,
        subscriber_id: str,
        *,
        email: Any = UNSET,
        phone: Any = UNSET,
        device_token: Any = UNSET,
        channels: Any = UNSET,
    ) -> Subscriber:
        """更新订阅者；只提交显式给出的字段。

        - 省略某个字段：保持原值；
        - 传 ``None``：解绑该地址（``email`` / ``phone`` / ``device_token``）；
        - ``channels=[]``：关闭该订阅者的全部渠道。

        ``channels=None`` 会报错——它既不是「不改」也不是「全关」，两种解释都
        会让调用方以为自己做了一件其实没做的事。
        """
        body: dict[str, Any] = {}
        for name, value in (
            ("email", email),
            ("phone", phone),
            ("device_token", device_token),
        ):
            if value is UNSET:
                continue
            body[name] = None if value is None else str(value).strip()

        if channels is not UNSET:
            if channels is None:
                raise ValueError("channels 传空列表表示全部关闭；不想改就不要传这个参数")
            body["channels"] = [str(item) for item in channels]

        if not body:
            # 空请求体在服务端是一次「什么都没改」的写入，还会推进 updated_at；
            # 这种调用几乎都是参数拼错了，本地直接指出。
            raise ValueError("没有需要更新的字段")
        return Subscriber.from_payload(
            self._client.request("PUT", _item_path(subscriber_id), json_body=body)
        )

    def delete(self, subscriber_id: str) -> None:
        """删除订阅者。不存在时抛 :class:`~xw_notify.errors.NotFoundError`。"""
        self._client.request("DELETE", _item_path(subscriber_id))

    # -- 读 ---------------------------------------------------------------

    def get(self, subscriber_id: str) -> Subscriber:
        """按标识读取订阅者。"""
        return Subscriber.from_payload(self._client.request("GET", _item_path(subscriber_id)))

    def list(
        self,
        *,
        channel: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> SubscriberPage:
        """取一页订阅者。

        :param channel: 只看启用了该渠道的订阅者（``delay`` / ``digest`` 不是
            投递渠道，会得到 422）。
        :param limit: 单页条数，服务端默认 50、上限 200。
        :param cursor: 上一页返回的 ``next_cursor``；首页留空。
        """
        if limit is not None and not 1 <= int(limit) <= MAX_PAGE_SIZE:
            raise ValueError(f"limit 取值 1–{MAX_PAGE_SIZE}")
        data = self._client.request(
            "GET",
            SUBSCRIBERS_PATH,
            query={
                "channel": channel,
                "limit": int(limit) if limit is not None else None,
                "cursor": cursor,
            },
        )
        return SubscriberPage.from_payload(data)

    def iter(
        self, *, channel: Optional[str] = None, page_size: Optional[int] = None
    ) -> Iterator[Subscriber]:
        """跟着游标翻完全部订阅者（惰性，取完一页才请求下一页）。

        游标重复出现时停止。判定放在**交出本页之前**：重复意味着服务端没有
        推进分页（例如翻页期间数据被清空，它又回到了同一个锚点），此时这一页
        与上一页是同一批数据，交出去只会让调用方拿到重复项；继续请求则会
        原地打转。代价是要多请求一次才能发现重复，这比无限循环便宜得多。
        """
        seen: set[str] = set()
        cursor: Optional[str] = None
        while True:
            page = self.list(channel=channel, limit=page_size, cursor=cursor)
            if page.next_cursor and page.next_cursor in seen:
                return
            for item in page.items:
                yield item
            if not page.next_cursor:
                return
            seen.add(page.next_cursor)
            cursor = page.next_cursor


__all__ = ["MAX_PAGE_SIZE", "SubscribersResource"]
