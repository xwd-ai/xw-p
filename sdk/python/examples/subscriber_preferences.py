"""订阅者档案与三层发送偏好：谁、经哪个渠道、在什么情况下愿意收。

    cd sdk/python
    python -m examples.subscriber_preferences

偏好有三层，解析时从最具体的一层往上找，命中第一条即返回，三层都没有记录
按「接收」处理：

    channel（渠道）  >  workflow（工作流）  >  global（总开关）

示例设置一套「平时别吵我，但 @我 要告诉我，推送除外」的组合，再用解析接口
逐条确认结论——排查「这条为什么没发」时，看到的是哪一层决定的，而不是一个
孤零零的 false。

最后用 ``clear()`` 恢复默认，重复运行不会叠加记录。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, TextIO

from xw_notify import Client

from . import _shared

#: 被设置的订阅者。与 ``post_interaction`` 用不同标识，两个示例互不干扰。
SUBSCRIBER = "creator-2002"


def run(client: Client, out: Optional[TextIO] = None) -> Dict[str, Any]:
    """建档案、设置三层偏好、逐条解析，返回每一步的结果。"""
    def say(text: str = "") -> None:
        _shared.emit(out, text)

    say("== 订阅者档案 ==")
    subscriber, created = _shared.ensure_subscriber(
        client,
        SUBSCRIBER,
        email="creator-2002@example.com",
        channels=["in_app", "email", "push"],
    )
    bindings = " / ".join(sorted(subscriber.bindings)) or "无"
    say(f"{'新建' if created else '已存在'}订阅者 {subscriber.id}，可投递渠道：{bindings}")

    say("")
    say("== 设置三层偏好 ==")
    preferences = client.subscribers.preferences
    preferences.deny(SUBSCRIBER, "global")
    say("  global    关闭：默认不再推任何互动")
    preferences.allow(SUBSCRIBER, "workflow", target="post_mentioned")
    say("  workflow  post_mentioned 打开：被 @ 的提醒仍然要收")
    preferences.allow(SUBSCRIBER, "channel", target="push")
    say("  channel   push 打开：推到手机的那条不受上面两层影响")

    say("")
    say("== 逐条解析 ==")
    cases: List[tuple[str, Dict[str, str]]] = [
        ("被 @ 时收到站内信", {"workflow": "post_mentioned", "channel": "in_app"}),
        ("被点赞时收到站内信", {"workflow": "post_liked", "channel": "in_app"}),
        ("被点赞时收到推送", {"workflow": "post_liked", "channel": "push"}),
        ("只看总开关", {}),
    ]
    resolutions: List[Dict[str, Any]] = []
    for label, query in cases:
        decision = preferences.resolve(SUBSCRIBER, **query)
        resolutions.append(
            {
                "label": label,
                "allowed": decision.allowed,
                "scope": decision.scope,
                "matched": decision.matched,
                "reason": decision.reason,
            }
        )
        # 命中层级为空表示三层都没有记录，走了「默认接收」。
        source = decision.scope or "默认"
        say(f"  {label}：{'会发' if decision else '不发'}（命中 {source}）")

    say("")
    say("== 显式设置过的记录 ==")
    listing = preferences.list(SUBSCRIBER)
    counts = " / ".join(f"{scope} {listing.counts.get(scope, 0)}" for scope in ("global", "workflow", "channel"))
    say(f"  共 {len(listing)} 条（{counts}）")
    for item in listing:
        target = item.target or "—"
        say(f"  {item.scope:<8} {target:<16} {'接收' if item.enabled else '不接收'}")

    say("")
    removed = preferences.clear(SUBSCRIBER)
    say(f"== 清空 {removed} 条记录，恢复默认 ==")
    after = preferences.resolve(SUBSCRIBER, workflow="post_liked", channel="in_app")
    say(f"  被点赞时收到站内信：{'会发' if after else '不发'}（命中 {after.scope or '默认'}）")

    return {
        "subscriber": {
            "id": subscriber.id,
            "created": created,
            "bindings": dict(subscriber.bindings),
        },
        "resolutions": resolutions,
        "counts": dict(listing.counts),
        "removed": removed,
        "after_clear_allowed": after.allowed,
    }


def main() -> Dict[str, Any]:
    """从环境变量装配客户端并跑一遍。"""
    with _shared.client_from_env() as client:
        result = run(client)
    return result


if __name__ == "__main__":  # pragma: no cover - 手动运行时才走这里
    main()
