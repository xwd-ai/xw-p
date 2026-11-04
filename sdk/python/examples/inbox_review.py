"""收件箱：读未读、看按工作流的分布、批量标记已读。

    cd sdk/python
    python -m examples.inbox_review

收件箱与管理统计这类接口 SDK 没有逐个封装，用 :meth:`xw_notify.Client.request`
兜底：拼路径、发请求、拿回解析后的字典。代价是响应形状不受模型约束，示例里
因此先过一道形状检查——服务端版本比 SDK 新、字段改名时，报出来的是「哪个字段
不对」，而不是某个 ``AttributeError``。

三个调用对应界面上的三块内容：列表（``GET /v1/inbox``）、顶部未读角标与按工作流
的筛选入口（``GET /v1/inbox/unread``）、点开之后的已读回写
（``POST /v1/inbox/read``）。
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, TextIO

from xw_notify import Client, ProtocolError

from . import _shared

INBOX_PATH = "/v1/inbox"
UNREAD_PATH = "/v1/inbox/unread"
READ_PATH = "/v1/inbox/read"

#: 与 ``post_interaction`` 共用同一个创作者：先跑那个示例，这里才有消息可看。
SUBSCRIBER = "creator-1001"
#: 首屏条数。
PAGE_SIZE = 5


def _mapping(value: Any, context: str) -> Mapping[str, Any]:
    """响应必须是对象；不是就报清是哪个接口，别让调用方去猜。"""
    if not isinstance(value, Mapping):
        raise ProtocolError(f"{context} 返回的不是 JSON 对象：{type(value).__name__}")
    return value


def _rows(value: Any) -> List[Mapping[str, Any]]:
    """取对象数组，丢掉非对象元素（坏数据不该让整页读不出来）。"""
    if not isinstance(value, (list, tuple)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def run(client: Client, out: Optional[TextIO] = None) -> Dict[str, Any]:
    """读一页未读、看工作流分布、把本页标记为已读。"""
    def say(text: str = "") -> None:
        _shared.emit(out, text)

    say("== 未读列表 ==")
    page = _mapping(
        client.request(
            "GET",
            INBOX_PATH,
            query={"subscriberId": SUBSCRIBER, "limit": PAGE_SIZE, "unread": True},
        ),
        "收件箱列表",
    )
    items = _rows(page.get("items"))
    say(f"  未读 {page.get('unread', 0)} 条 / 共 {page.get('total', 0)} 条，本页 {len(items)} 条")
    for item in items:
        # 聚合卡片的标题会随互动人数变化（「3 人点赞了你的帖子」），消息 ID 不变。
        say(
            "  [{}] {}（{}）".format(
                "未读" if not item.get("read") else "已读",
                item.get("title") or "新通知",
                item.get("workflow") or "未知工作流",
            )
        )

    say("")
    say("== 按工作流的未读分布 ==")
    summary = _mapping(
        client.request("GET", UNREAD_PATH, query={"subscriberId": SUBSCRIBER}),
        "未读聚合",
    )
    distribution = _rows(summary.get("byWorkflow"))
    if not distribution:
        say("  暂无未读")
    for row in distribution[:5]:
        say(f"  {row.get('workflow') or '未知工作流'}：未读 {row.get('unread', 0)} / 共 {row.get('total', 0)}")

    say("")
    say("== 标记本页为已读 ==")
    message_ids = [str(item["message_id"]) for item in items if item.get("message_id")]
    if not message_ids:
        # 没有消息就不发空请求：空列表在服务端等价于「全部已读」，是另一种语义。
        say("  本页没有可标记的消息")
        marked, unread = 0, int(summary.get("unread") or 0)
    else:
        receipt = _mapping(
            client.request(
                "POST",
                READ_PATH,
                json_body={"subscriberId": SUBSCRIBER, "messageIds": message_ids},
            ),
            "标记已读",
        )
        marked = int(receipt.get("marked") or 0)
        unread = int(receipt.get("unread") or 0)
        say(f"  提交 {len(message_ids)} 条，实际变更 {marked} 条，剩余未读 {unread} 条")

    return {
        "unread": int(page.get("unread") or 0),
        "total": int(page.get("total") or 0),
        "items": [
            {
                "message_id": item.get("message_id"),
                "title": item.get("title"),
                "workflow": item.get("workflow"),
                "read": bool(item.get("read")),
            }
            for item in items
        ],
        "distribution": [
            {"workflow": row.get("workflow"), "unread": int(row.get("unread") or 0)}
            for row in distribution
        ],
        "marked": marked,
        "unread_after": unread,
    }


def main() -> Dict[str, Any]:
    """从环境变量装配客户端并跑一遍。"""
    with _shared.client_from_env() as client:
        result = run(client)
    return result


if __name__ == "__main__":  # pragma: no cover - 手动运行时才走这里
    main()
