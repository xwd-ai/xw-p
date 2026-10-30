"""时间戳解析与游标编码（活动流分页、统计聚合共用）。

存储后端返回的时间可能来自 JSONL 文本（ISO 8601 字符串）或 Postgres
（``datetime``）。这里统一解析为带时区的 UTC ``datetime``，无法解析时返回
``None``，由调用方决定跳过还是报错。
"""
from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def parse_dt(value: object) -> Optional[datetime]:
    """把时间戳解析为带时区的 UTC datetime；缺失或格式错误返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class Cursor:
    """翻页游标：上一页最后一条记录的排序键。

    排序键取 ``(created_at, trigger_id)``——``created_at`` 可能重复（同一毫秒
    的多条触发），用触发标识做次级排序键才能保证翻页不跳不重。
    """

    created_at: datetime
    trigger_id: str


def encode_cursor(cursor: Cursor) -> str:
    """把游标编码成 URL 安全的字符串。"""
    payload = {"t": cursor.created_at.isoformat(), "i": cursor.trigger_id}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def encode_position(created_at: datetime, key: str) -> str:
    """把「时间戳 + 记录键」编码成 URL 安全的游标串。

    与 ``encode_cursor`` 的区别是次级键由调用方决定：活动流的次级键是触发标识，
    执行明细的次级键是记录自身的标识（一次触发会写出多条渠道记录，触发标识
    不足以区分）。
    """
    payload = {"t": created_at.isoformat(), "k": key}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_position(value: str) -> tuple[datetime, str]:
    """解码 ``encode_position`` 生成的游标；内容不合法时抛 ``ValueError``。"""
    padded = value + "=" * (-len(value) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("invalid cursor") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid cursor")
    ts = parse_dt(payload.get("t"))
    key = payload.get("k")
    if ts is None or key is None:
        raise ValueError("invalid cursor")
    return ts, str(key)


def decode_cursor(value: str) -> Cursor:
    """解码游标；内容不合法时抛 ``ValueError``。"""
    padded = value + "=" * (-len(value) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("invalid cursor") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid cursor")
    ts = parse_dt(payload.get("t"))
    trigger_id = payload.get("i")
    if ts is None or trigger_id is None:
        raise ValueError("invalid cursor")
    return Cursor(created_at=ts, trigger_id=str(trigger_id))


__all__ = [
    "Cursor",
    "parse_dt",
    "encode_cursor",
    "decode_cursor",
    "encode_position",
    "decode_position",
]
