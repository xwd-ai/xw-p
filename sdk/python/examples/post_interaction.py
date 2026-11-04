"""互动事件从发生到入队：点赞聚合、评论幂等、批量 @提及。

    cd sdk/python
    python -m examples.post_interaction

示例覆盖调用方最常写的三条路径：

1. **同一篇帖子被连续点赞**。三条事件带同一个 ``post_id``，工作流里的聚合步骤
   以它为聚合键，在时间窗口内把互动累加到同一张卡片上，创作者看到的是「3 人
   点赞」而不是三条通知。聚合由引擎执行；API 进程内置的消费只按渠道投递，用
   内存总线跑这个示例时收件箱里会是三条独立站内信，接上引擎后才是合并后的卡片。
   SDK 侧不需要为聚合做任何事，只要每次把 ``post_id`` 放进 ``payload``。
2. **评论带幂等键**。重试是客户端的事，去重是服务端的事：同一个
   ``comment-C-8842`` 第二次提交不会让创作者再收到一遍。
3. **批量 @提及**。整批一次提交，回执与入参按下标对齐——某条工作流未启用只会
   让那一条记为未受理，同批其余照常入队。

工作流未启用时触发接口返回 404，示例把这种「这条没进去」记在结果里而不是让
整段流程中断：真实场景里一条通知发不出去，不该拖垮同一次请求里的其它事件。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, TextIO

from xw_notify import Client, NotFoundError, TriggerAck

from . import _shared

#: 创作者（订阅者）标识，由业务侧传入，便于触发时直接引用。
CREATOR = "creator-1001"
#: 帖子标识：既是渲染变量，也是聚合键。
POST_ID = "P-2026-1001"
POST_TITLE = "我的第一支 Vlog：从素材到成片"
#: 评论的幂等键。真实业务里用评论 ID，重发同一句话不会产生第二条通知。
COMMENT_KEY = "comment-C-8842"


def _trigger(
    client: Client, name: str, payload: Dict[str, Any], **kwargs: Any
) -> tuple[Optional[TriggerAck], Optional[str]]:
    """发一条事件；工作流不存在或未启用时返回原因，不抛给调用方。"""
    try:
        return client.trigger.send(name, CREATOR, payload=payload, **kwargs), None
    except NotFoundError as error:
        # NotFoundError 的 message 已经是「工作流不存在或未启用」这类可读句子。
        return None, error.message


def _describe(ack: Optional[TriggerAck], error: Optional[str]) -> str:
    if ack is not None:
        return f"已受理（触发 {ack.trigger_id}）"
    return f"未受理：{error or '未知原因'}"


def run(client: Client, out: Optional[TextIO] = None) -> Dict[str, Any]:
    """跑完整段流程，返回每一步的结果。"""
    def say(text: str = "") -> None:
        _shared.emit(out, text)

    say("== 创作者订阅者 ==")
    subscriber, created = _shared.ensure_subscriber(
        client,
        CREATOR,
        email="creator@example.com",
        channels=["in_app", "push", "email"],
    )
    bindings = " / ".join(sorted(subscriber.bindings)) or "无（任何投递都会被跳过）"
    say(f"{'新建' if created else '已存在'}订阅者 {subscriber.id}，可投递渠道：{bindings}")

    say("")
    say("== 点赞：同一篇帖子的互动交给聚合步骤 ==")
    likes: List[Dict[str, Any]] = []
    for actor in ("小林", "阿澈", "阿澈"):
        ack, error = _trigger(
            client,
            "post_liked",
            {"post_id": POST_ID, "post_title": POST_TITLE, "actor": actor},
        )
        likes.append(
            {
                "actor": actor,
                "trigger_id": ack.trigger_id if ack else None,
                "error": error,
            }
        )
        say(f"  {actor} 点赞 → {_describe(ack, error)}")
    say(f"  三条事件都带 post_id={POST_ID}，聚合步骤以它为键，在窗口内累加到同一张卡片")

    say("")
    say("== 评论：带幂等键，重发不会多一条通知 ==")
    comment_ack, comment_error = _trigger(
        client,
        "post_commented",
        {
            "post_id": POST_ID,
            "post_title": POST_TITLE,
            "actor": "阿澈",
            "comment": "求教程，尤其是调色那一段",
        },
        channels=["in_app", "push", "email"],
        idempotency_key=COMMENT_KEY,
    )
    say(f"  幂等键 {COMMENT_KEY} → {_describe(comment_ack, comment_error)}")

    say("")
    say("== @提及：一次提交多条 ==")
    mentions = [
        client.trigger.build(
            "post_mentioned",
            CREATOR,
            payload={"post_id": POST_ID, "post_title": POST_TITLE, "actor": actor},
        )
        for actor in ("阿澈", "小林")
    ]
    batch = client.trigger.send_batch(mentions, idempotency_key="mention-batch-1")
    mention_rows: List[Dict[str, Any]] = []
    for item in batch.results:
        # 回执与入参同序：按下标对齐，不要按「成功的那几条」重新编号。
        mention_rows.append(
            {
                "index": item.index,
                "status": item.status,
                "trigger_id": item.trigger_id,
                "error": item.error,
            }
        )
        detail = f"已受理（触发 {item.trigger_id}）" if item.accepted else f"未受理：{item.error}"
        say(f"  #{item.index + 1} {item.name} → {detail}")
    say(f"  共 {len(batch)} 条：受理 {batch.accepted} / 未受理 {batch.rejected}")

    return {
        "subscriber": {
            "id": subscriber.id,
            "created": created,
            "bindings": dict(subscriber.bindings),
        },
        "likes": likes,
        "comment": {
            "trigger_id": comment_ack.trigger_id if comment_ack else None,
            "error": comment_error,
        },
        "mentions": {
            "accepted": batch.accepted,
            "rejected": batch.rejected,
            "items": mention_rows,
        },
    }


def main() -> Dict[str, Any]:
    """从环境变量装配客户端并跑一遍。"""
    with _shared.client_from_env() as client:
        result = run(client)
    return result


if __name__ == "__main__":  # pragma: no cover - 手动运行时才走这里
    main()
