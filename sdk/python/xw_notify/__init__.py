"""xw-notify 的 Python 客户端。

面向「内容社区互动触达」场景的同步 SDK：一次点赞、一条评论、一次 @提及，
按工作流模板分发到订阅者所在的站内信 / 推送 / 邮件 / 短信。依赖只有标准库，
可以直接拷进项目，不必为一个通知接口引入 HTTP 客户端生态。

快速上手::

    from xw_notify import Client

    client = Client("http://127.0.0.1:8000")

    # 帖子被点赞 → 按 post_id 聚合到一条卡片
    client.trigger.send(
        "post_liked", "creator-1001",
        payload={"post_id": "P-2026-1001", "post_title": "我的第一支 Vlog", "actor": "小林"},
    )

    # 帖子收到评论 → 站内信 + 推送 + 邮件
    client.trigger.send(
        "post_commented", "creator-1001",
        payload={"post_id": "P-2026-1001", "actor": "阿澈", "comment": "求教程"},
        channels=["in_app", "push", "email"],
        idempotency_key="comment-C-8842",
    )

    # 订阅者与偏好
    client.subscribers.create(
        subscriber_id="creator-1001",
        email="creator@example.com",
        channels=["in_app", "email"],
    )
    client.subscribers.preferences.deny("creator-1001", "channel", target="email")
    if not client.subscribers.preferences.resolve("creator-1001", workflow="post_commented", channel="email"):
        print("邮件已被该订阅者关闭")

异常按处理方式分层（见 :mod:`xw_notify.errors`）：网络层失败抛
``TransportError``，服务端拒绝抛 ``APIError`` 的子类，响应形状不对抛
``ProtocolError``。
"""
from __future__ import annotations

from .client import (
    DEFAULT_BASE_URL,
    DEFAULT_ENVIRONMENT,
    Client,
    quote_segment,
)
from .errors import (
    APIError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ProtocolError,
    RateLimitError,
    ServerError,
    TransportError,
    ValidationError,
    XWNotifyError,
)
from .models import (
    BatchItem,
    Event,
    Health,
    Preference,
    PreferenceList,
    PreferenceUpsert,
    Resolution,
    Subscriber,
    SubscriberPage,
    TriggerAck,
    TriggerBatch,
)
from .preferences import PreferencesResource
from .subscribers import SubscribersResource
from .trigger import MAX_BATCH_EVENTS, TriggerResource

__version__ = "0.1.0"

__all__ = [
    "APIError",
    "AuthenticationError",
    "BadRequestError",
    "BatchItem",
    "Client",
    "ConflictError",
    "DEFAULT_BASE_URL",
    "DEFAULT_ENVIRONMENT",
    "Event",
    "ForbiddenError",
    "Health",
    "MAX_BATCH_EVENTS",
    "NotFoundError",
    "Preference",
    "PreferenceList",
    "PreferenceUpsert",
    "PreferencesResource",
    "ProtocolError",
    "RateLimitError",
    "Resolution",
    "ServerError",
    "Subscriber",
    "SubscriberPage",
    "SubscribersResource",
    "TransportError",
    "TriggerAck",
    "TriggerBatch",
    "TriggerResource",
    "ValidationError",
    "XWNotifyError",
    "__version__",
    "quote_segment",
]
