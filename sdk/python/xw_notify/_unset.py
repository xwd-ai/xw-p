"""「没有提供这个参数」的哨兵。

更新接口需要区分三种意图：改字段、把字段解绑（显式传 ``None``）、完全不碰
（不传）。``None`` 已经被「解绑」占用，只能另造一个哨兵——否则
``update(email=None)`` 到底是清空邮箱还是保持原样，就只能靠文档约定。
"""
from __future__ import annotations


class _Unset:
    """哨兵类型：全局只有一个实例。"""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET = _Unset()


__all__ = ["UNSET"]
