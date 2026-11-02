"""站内信消息中心：消息模型与写入路径。

站内信与 email / sms / push 的区别在于「投递即落库」：没有外部网关，投递动作
本身就是往订阅者的消息中心写一条记录，等客户端来拉。本模块收口这件事的两半：

1. **数据模型** :class:`Message`——一条站内信需要的字段（标识、订阅者、工作流、
   步骤、标题、正文、创建/更新时间），写入前统一裁剪与校验，并保证与引擎侧的
   ``pipeline.InboxEntry`` 使用同一套 JSON 字段名（snake_case），两个进程因此
   可以共用同一份收件箱文件：引擎渲染后写入，API 侧读取渲染结果。
2. **写入路径** :class:`MessageStore`——按消息标识幂等写入。同一条触发消息被
   重放时不会在收件箱里变成两条；聚合卡片（digest）要原地改写同一条记录时走
   ``replace=True``，不新增条目。

两种存储实现：

- :class:`MemoryMessageCenter`：进程内字典，用于单元测试与无持久化场景；
- :class:`JsonlMessageCenter`：一行一个 JSON 追加写入文件（默认
  ``api/data/inbox.jsonl``，可由 ``XW_INBOX_FILE`` 覆盖）。追加是单次
  ``O_APPEND`` 写，改写走「同目录临时文件 + rename」，进程在写一半时被中断也
  不会留下半截文件；索引未命中时按文件大小判断是否被其它进程追加过，变了就
  重扫一遍再下结论，避免在共享卷上写出重复条目。

已读状态不在这里：它属于订阅者维度、与消息内容分开演进，由 ``app.inbox`` 的
``ReadStateStore`` 维护；本模块写入的 ``read`` 字段固定为 ``false``，与引擎侧
的写入保持一致。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterator, Optional, Protocol

logger = logging.getLogger("xw.message_center")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

#: 消息在 JSON 里的字段名，与引擎侧 InboxEntry 的 tag 一一对应。
MESSAGE_FIELDS = (
    "id",
    "trigger_id",
    "subscriber_id",
    "workflow",
    "step_id",
    "title",
    "body",
    "created_at",
    "updated_at",
    "read",
)

#: 标题缺省值：订阅者至少要能看出这是一条通知。
DEFAULT_TITLE = "新通知"


class MessageError(ValueError):
    """消息不满足写入条件（缺标识、缺订阅者、正文为空等）。"""


class WriteOutcome(str, Enum):
    """一次写入的结果。

    ``MISSING`` 只会在 ``replace=True`` 时出现：调用方要求改写一条并不存在的
    消息，此时不静默新建，而是把「目标不在」如实回报，由调用方决定是否重投。
    """

    CREATED = "created"
    DUPLICATE = "duplicate"
    UPDATED = "updated"
    MISSING = "missing"

    @property
    def ok(self) -> bool:
        """是否算写入成功（重复与原地改写都是成功）。"""
        return self is not WriteOutcome.MISSING


def default_path() -> Path:
    """消息中心文件的默认位置：``XW_INBOX_FILE`` 优先，否则 ``api/data/inbox.jsonl``。"""
    raw = (os.getenv("XW_INBOX_FILE") or "").strip()
    return Path(raw) if raw else _DATA_DIR / "inbox.jsonl"


def _text(value: object) -> str:
    """把任意取值压成去空白的字符串；``None`` 视为空串。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def parse_timestamp(value: object) -> Optional[datetime]:
    """解析时间戳（RFC3339 或 ``YYYY-MM-DDTHH:MM:SS``），失败返回 ``None``。

    Go 侧写出的时间带纳秒与 ``Z`` 后缀，Python ``fromisoformat`` 在 3.11 之前
    不认 ``Z``，这里统一替换；无时区的按 UTC 处理，避免同一份文件里的时间混用
    本地时区后排序错乱。
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    """序列化为 UTC 的 ISO8601 字符串（带 ``+00:00`` 偏移）。"""
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class Message:
    """消息中心里的一条站内信。

    ``created_at`` 是首次写入的时间，``updated_at`` 是内容最近一次变更的时间：
    普通站内信两者相同，聚合卡片被新互动改写时会推进 ``updated_at``，
    收件箱据此把卡片重新顶起来并重新算未读。
    """

    id: str
    subscriber_id: str
    title: str
    body: str
    created_at: datetime
    updated_at: datetime
    trigger_id: str = ""
    workflow: str = ""
    step_id: str = ""
    read: bool = False

    def __post_init__(self) -> None:
        message_id = _text(self.id)
        if not message_id:
            raise MessageError("消息缺少标识")
        subscriber = _text(self.subscriber_id)
        if not subscriber:
            raise MessageError("消息缺少订阅者标识")
        body = _text(self.body)
        if not body:
            raise MessageError("消息正文为空")
        created = parse_timestamp(self.created_at)
        updated = parse_timestamp(self.updated_at)
        if created is None or updated is None:
            raise MessageError("消息时间戳不合法")
        if updated < created:
            # 改写把时间拨回去会让「已读后又更新」的判断失效，直接纠正。
            updated = created
        object.__setattr__(self, "id", message_id)
        object.__setattr__(self, "subscriber_id", subscriber)
        object.__setattr__(self, "body", body)
        object.__setattr__(self, "title", _text(self.title) or DEFAULT_TITLE)
        object.__setattr__(self, "created_at", created)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "trigger_id", _text(self.trigger_id))
        object.__setattr__(self, "workflow", _text(self.workflow))
        object.__setattr__(self, "step_id", _text(self.step_id))
        object.__setattr__(self, "read", bool(self.read))

    @classmethod
    def create(
        cls,
        *,
        id: str,
        subscriber_id: str,
        body: str,
        title: str = "",
        trigger_id: str = "",
        workflow: str = "",
        step_id: str = "",
        now: Optional[datetime] = None,
    ) -> "Message":
        """构造一条新消息：以当前时间作为创建与更新时间。"""
        stamp = now or datetime.now(timezone.utc)
        return cls(
            id=id,
            subscriber_id=subscriber_id,
            title=title,
            body=body,
            created_at=stamp,
            updated_at=stamp,
            trigger_id=trigger_id,
            workflow=workflow,
            step_id=step_id,
        )

    def rewritten(
        self,
        *,
        title: Optional[str] = None,
        body: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> "Message":
        """返回内容被更新、``updated_at`` 推进的同 ID 消息（聚合卡片用）。"""
        stamp = parse_timestamp(now or datetime.now(timezone.utc)) or self.updated_at
        return Message(
            id=self.id,
            subscriber_id=self.subscriber_id,
            title=self.title if title is None else title,
            body=self.body if body is None else body,
            created_at=self.created_at,
            updated_at=max(stamp, self.created_at),
            trigger_id=self.trigger_id,
            workflow=self.workflow,
            step_id=self.step_id,
            read=self.read,
        )

    def to_dict(self) -> dict:
        """转成与引擎侧一致的 JSON 结构（时间用 ISO8601 字符串）。"""
        return {
            "id": self.id,
            "trigger_id": self.trigger_id,
            "subscriber_id": self.subscriber_id,
            "workflow": self.workflow,
            "step_id": self.step_id,
            "title": self.title,
            "body": self.body,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "read": self.read,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "Message":
        """从 JSON 结构还原；字段缺失或非法时抛 :class:`MessageError`。

        时间缺失时按同一时刻补齐，而不是整条丢弃——收件箱里少一条消息比时间
        略有偏差更糟。
        """
        if not isinstance(raw, dict):
            raise MessageError("消息必须是对象")
        created = parse_timestamp(raw.get("created_at"))
        updated = parse_timestamp(raw.get("updated_at")) or created
        stamp = created or updated or datetime.now(timezone.utc)
        return cls(
            id=_text(raw.get("id")),
            subscriber_id=_text(raw.get("subscriber_id")),
            title=_text(raw.get("title")),
            body=_text(raw.get("body")),
            created_at=stamp,
            updated_at=updated or stamp,
            trigger_id=_text(raw.get("trigger_id")),
            workflow=_text(raw.get("workflow")),
            step_id=_text(raw.get("step_id")),
            read=bool(raw.get("read")),
        )


class MessageStore(Protocol):
    """消息中心存储契约：按标识幂等写入、按订阅者读取。"""

    def write(self, message: Message, *, replace: bool = False) -> WriteOutcome:
        """写入一条消息。

        ``replace=False`` 时同 ID 已存在返回 ``DUPLICATE``（不覆盖）；
        ``replace=True`` 时覆盖同 ID 消息，目标不存在返回 ``MISSING``。
        """
        ...

    def get(self, message_id: str) -> Optional[Message]:
        """按标识取一条消息，不存在返回 ``None``。"""
        ...

    def list(self, subscriber_id: str) -> list[Message]:
        """返回某订阅者的全部消息（写入顺序）。"""
        ...

    def revision(self) -> object:
        """来源版本号：内容变化时返回不同的值，供读取方做缓存失效。"""
        ...


class MemoryMessageCenter:
    """进程内消息中心，线程安全，按写入顺序保存。"""

    def __init__(self) -> None:
        self._messages: dict[str, Message] = {}
        self._revision = 0
        self._lock = threading.Lock()

    def write(self, message: Message, *, replace: bool = False) -> WriteOutcome:
        with self._lock:
            existing = self._messages.get(message.id)
            if existing is None:
                if replace:
                    return WriteOutcome.MISSING
                self._messages[message.id] = message
                self._revision += 1
                return WriteOutcome.CREATED
            if not replace:
                return WriteOutcome.DUPLICATE
            self._messages[message.id] = message
            self._revision += 1
            return WriteOutcome.UPDATED

    def get(self, message_id: str) -> Optional[Message]:
        with self._lock:
            return self._messages.get(_text(message_id))

    def list(self, subscriber_id: str) -> list[Message]:
        target = _text(subscriber_id)
        with self._lock:
            return [m for m in self._messages.values() if m.subscriber_id == target]

    def revision(self) -> object:
        with self._lock:
            return ("memory", self._revision)

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)


class JsonlMessageCenter:
    """以「一行一个 JSON」保存消息的存储，供引擎与 API 共用同一份文件。

    写路径分两种：新消息走单次 ``O_APPEND`` 追加（小行写为原子操作），
    改写走「全量重写 + 临时文件 rename」。两者都在同一把锁内完成，
    同进程内不会交叉；跨进程靠 ``ensure_fresh`` 观察文件大小变化后重扫索引。
    """

    def __init__(self, path: Optional[Path | str] = None) -> None:
        self._path = Path(path) if path is not None else default_path()
        self._ids: dict[str, int] = {}
        self._size = 0
        self._lock = threading.RLock()
        self._reload()

    @property
    def path(self) -> Path:
        """消息中心文件路径。"""
        return self._path

    # ---------------------------------------------------------------- 读取

    def _iter_raw(self) -> Iterator[dict]:
        """逐行读取文件，跳过损坏行（坏数据不应让整份收件箱读不出来）。"""
        try:
            with self._path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("跳过损坏的消息行：%s", line[:80])
                        continue
                    if isinstance(raw, dict):
                        yield raw
        except FileNotFoundError:
            return
        except OSError:
            logger.warning("读取消息中心文件失败：%s", self._path)

    def _reload(self) -> None:
        """重建标识索引，并记下当前文件字节数；调用方需持有锁。"""
        self._ids = {}
        self._size = 0
        try:
            size = self._path.stat().st_size
        except OSError:
            return
        self._ids = {
            _text(raw.get("id")): index
            for index, raw in enumerate(self._iter_raw())
            if _text(raw.get("id"))
        }
        self._size = size

    def _ensure_fresh(self) -> None:
        """文件被其它进程追加或重写后重建索引；调用方需持有锁。"""
        try:
            size = self._path.stat().st_size
        except OSError:
            if self._size:
                self._ids = {}
                self._size = 0
            return
        if size != self._size:
            self._reload()

    # ---------------------------------------------------------------- 写入

    def write(self, message: Message, *, replace: bool = False) -> WriteOutcome:
        if not isinstance(message, Message):
            raise MessageError("只能写入 Message 对象")
        with self._lock:
            self._ensure_fresh()
            if message.id in self._ids:
                if not replace:
                    return WriteOutcome.DUPLICATE
                return self._rewrite(message)
            if replace:
                return WriteOutcome.MISSING
            return self._append(message)

    def _append(self, message: Message) -> WriteOutcome:
        line = json.dumps(message.to_dict(), ensure_ascii=False) + "\n"
        payload = line.encode("utf-8")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
            try:
                written = os.write(fd, payload)
            finally:
                os.close(fd)
        except OSError as exc:
            raise MessageError(f"写入消息中心失败：{exc}") from exc
        if written != len(payload):
            # 半行落盘会让整份文件从这一行起不可解析，宁可报错让调用方重投。
            raise MessageError(f"消息只写入 {written}/{len(payload)} 字节")
        self._ids[message.id] = len(self._ids)
        self._size += len(payload)
        return WriteOutcome.CREATED

    def _rewrite(self, message: Message) -> WriteOutcome:
        line = json.dumps(message.to_dict(), ensure_ascii=False)
        try:
            with self._path.open("r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()
        except FileNotFoundError:
            self._ids.pop(message.id, None)
            return WriteOutcome.MISSING
        except OSError as exc:
            raise MessageError(f"读取消息中心失败：{exc}") from exc

        out: list[str] = []
        replaced = False
        for raw in lines:
            trimmed = raw.strip()
            if not trimmed:
                continue
            if not replaced:
                try:
                    existing = json.loads(trimmed)
                except json.JSONDecodeError:
                    existing = None
                if isinstance(existing, dict) and _text(existing.get("id")) == message.id:
                    out.append(line)
                    replaced = True
                    continue
            # 损坏行原样保留：为了改一条卡片丢掉其它数据更糟。
            out.append(trimmed)
        if not replaced:
            self._ids.pop(message.id, None)
            return WriteOutcome.MISSING
        content = "\n".join(out) + "\n"
        self._write_atomic(content)
        self._size = len(content.encode("utf-8"))
        return WriteOutcome.UPDATED

    def _write_atomic(self, content: str) -> None:
        """整体替换文件：先写同目录临时文件，再 ``os.replace`` 顶替。"""
        tmp_name: Optional[str] = None
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(
                dir=str(self._path.parent), prefix=self._path.name + ".", suffix=".tmp"
            )
            try:
                os.write(fd, content.encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp_name, self._path)
            tmp_name = None
        except OSError as exc:
            raise MessageError(f"重写消息中心失败：{exc}") from exc
        finally:
            if tmp_name is not None:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    # ---------------------------------------------------------------- 读取

    def get(self, message_id: str) -> Optional[Message]:
        target = _text(message_id)
        if not target:
            return None
        for raw in self._iter_raw():
            if _text(raw.get("id")) != target:
                continue
            try:
                return Message.from_dict(raw)
            except MessageError as exc:
                logger.warning("跳过不合法的消息 %s：%s", target, exc)
                return None
        return None

    def list(self, subscriber_id: str) -> list[Message]:
        target = _text(subscriber_id)
        if not target:
            return []
        out: list[Message] = []
        for raw in self._iter_raw():
            if _text(raw.get("subscriber_id")) != target:
                continue
            try:
                out.append(Message.from_dict(raw))
            except MessageError as exc:
                logger.warning("跳过不合法的消息：%s", exc)
        return out

    def revision(self) -> object:
        try:
            st = self._path.stat()
        except OSError:
            return ("absent", str(self._path))
        return ("file", str(self._path), st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)

    def __len__(self) -> int:
        with self._lock:
            self._ensure_fresh()
            return len(self._ids)


_store: Optional[MessageStore] = None
_store_lock = threading.Lock()


def create_store(path: Optional[Path | str] = None) -> MessageStore:
    """按路径构造消息中心存储；路径为空时使用默认位置。

    构造前先确认目标是文件、目录存在：文件被目录占位或父目录无法创建时抛出
    :class:`MessageError`，让装配阶段就失败，而不是等到第一条站内信投递才报错。
    """
    target = Path(path) if path is not None else default_path()
    if target.exists() and not target.is_file():
        raise MessageError(f"消息中心路径不是文件：{target}")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise MessageError(f"消息中心目录不可用：{target.parent}（{exc}）") from exc
    return JsonlMessageCenter(target)


def get_store() -> MessageStore:
    """获取全局消息中心单例（首次取用时按 ``XW_INBOX_FILE`` 构造）。"""
    global _store
    with _store_lock:
        if _store is None:
            _store = create_store()
        return _store


def set_store(store: Optional[MessageStore]) -> None:
    """替换全局消息中心；传 ``None`` 表示下次取用时重新构造（供测试与热更新）。"""
    global _store
    with _store_lock:
        _store = store


__all__ = [
    "DEFAULT_TITLE",
    "MESSAGE_FIELDS",
    "JsonlMessageCenter",
    "MemoryMessageCenter",
    "Message",
    "MessageError",
    "MessageStore",
    "WriteOutcome",
    "create_store",
    "default_path",
    "get_store",
    "parse_timestamp",
    "set_store",
]
