"""发送偏好接口：查询、写入、删除、解析。

偏好有三层（``channel`` → ``workflow`` → ``global``），解析时从最具体的
一层往上找，都没有记录就按「接收」处理。SDK 把这条规则原样透出，但额外给
了两个便于阅读的写法：:meth:`PreferencesResource.allow` 与
:meth:`PreferencesResource.deny`——线上是一个 ``enabled`` 布尔，调用方照着
文档写 ``set(..., enabled=False)`` 很容易漏掉那个 ``False``。

``global`` 层不带 ``target``，另两层必须带；这条规则在本地先判一次，只是
为了省一次往返并给出中文提示，真正的判定仍在服务端（客户端多写一遍规则是
为了快，不是为了取代它）。
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ._http import quote_segment
from .models import PreferenceList, PreferenceUpsert, Resolution

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型检查
    from .client import Client

#: 三层偏好的取值，服务端只接受这三个。
SCOPES = ("global", "workflow", "channel")


def _scope_path(subscriber_id: str) -> str:
    return f"/v1/subscribers/{quote_segment(subscriber_id)}/preferences"


def _normalize_scope(scope: str) -> str:
    text = str(scope or "").strip().lower()
    if text not in SCOPES:
        raise ValueError(f"未知层级：{scope}（支持：{' / '.join(SCOPES)}）")
    return text


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class PreferencesResource:
    """挂在订阅者下的偏好接口。

    通过 ``client.subscribers.preferences`` 访问——偏好在服务端的路径就在订阅
    者之下，SDK 保持同一层级，看代码时不必再对照一份路由表。
    """

    def __init__(self, client: "Client") -> None:
        self._client = client

    # -- 读 ---------------------------------------------------------------

    def list(self, subscriber_id: str, *, scope: Optional[str] = None) -> PreferenceList:
        """列出该订阅者**显式设置过**的记录。

        没有记录的层级不返回：那表示「没表过态」，与「明确设成接收」是两件事，
        混在一起就分不出用户到底有没有表过态。
        """
        wanted = _normalize_scope(scope) if scope is not None else None
        data = self._client.request(
            "GET",
            _scope_path(subscriber_id),
            query={"scope": wanted},
        )
        return PreferenceList.from_payload(data)

    def resolve(
        self,
        subscriber_id: str,
        *,
        workflow: Optional[str] = None,
        channel: Optional[str] = None,
    ) -> Resolution:
        """解析「这条通知会不会发」，并给出是哪一层决定的。

        三个参数都可以不传，此时只看全局总开关。返回值可以直接当布尔用。
        """
        data = self._client.request(
            "GET",
            f"{_scope_path(subscriber_id)}/resolve",
            query={"workflow": _clean(workflow), "channel": _clean(channel)},
        )
        return Resolution.from_payload(data)

    # -- 写 ---------------------------------------------------------------

    def set(
        self,
        subscriber_id: str,
        scope: str,
        *,
        target: Optional[str] = None,
        enabled: bool = True,
    ) -> PreferenceUpsert:
        """新建或覆盖一条偏好（覆盖写，不需要先查再改）。

        ``enabled=True`` 是一条有意义的记录：它比上层更具体，能把被上层关掉的
        某个渠道单独放开。
        """
        normalized = _normalize_scope(scope)
        cleaned = _clean(target)
        if normalized == "global" and cleaned is not None:
            raise ValueError("global 层没有 target，请省略该参数")
        if normalized != "global" and cleaned is None:
            raise ValueError(f"{normalized} 层必须给出 target（工作流名或渠道名）")
        data = self._client.request(
            "PUT",
            _scope_path(subscriber_id),
            json_body={"scope": normalized, "target": cleaned, "enabled": bool(enabled)},
        )
        return PreferenceUpsert.from_payload(data)

    def allow(
        self, subscriber_id: str, scope: str, *, target: Optional[str] = None
    ) -> PreferenceUpsert:
        """把某一层设为接收。"""
        return self.set(subscriber_id, scope, target=target, enabled=True)

    def deny(
        self, subscriber_id: str, scope: str, *, target: Optional[str] = None
    ) -> PreferenceUpsert:
        """把某一层设为不接收。"""
        return self.set(subscriber_id, scope, target=target, enabled=False)

    def delete(
        self,
        subscriber_id: str,
        *,
        scope: Optional[str] = None,
        target: Optional[str] = None,
    ) -> int:
        """删除一条偏好，返回实际删掉的条数。

        不带 ``scope`` 时清空全部（相当于恢复默认）；删除不存在的记录返回 0，
        不抛异常——调用方的目的（这条记录不要存在）已经达成。
        """
        normalized = _normalize_scope(scope) if scope is not None else None
        cleaned = _clean(target)
        if normalized is None and cleaned is not None:
            # 只给 target 无法确定删的是工作流还是渠道，服务端同样拒绝。
            raise ValueError("给出 target 时必须同时给出 scope")
        removed = self._client.request(
            "DELETE",
            _scope_path(subscriber_id),
            query={"scope": normalized, "target": cleaned},
        )
        return int((removed or {}).get("removed") or 0)

    def clear(self, subscriber_id: str) -> int:
        """清空该订阅者的全部偏好，返回删掉的条数。"""
        return self.delete(subscriber_id)


__all__ = ["PreferencesResource", "SCOPES"]
