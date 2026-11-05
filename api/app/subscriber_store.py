"""订阅者存储：内存 / JSONL 双后端。

订阅者是通知的接收方，一条记录里既有身份（标识、环境），也有渠道绑定
（邮箱、手机号、设备 token 与启用的渠道列表）。本模块负责**存取**与
**地址唯一性**：字段清洗、渠道合法性与地址格式由请求模型与
:mod:`app.subscriber_identity` 承担，这里只按它们算出的比较键判重。

两种实现：

- :class:`MemorySubscriberStore`：进程内字典，测试与无持久化场景使用；
- :class:`JsonlSubscriberStore`：一行一个 JSON 落文件（默认
  ``api/data/subscribers.jsonl``，可由 ``XW_SUBSCRIBERS_FILE`` 覆盖）。
  新增走单次 ``O_APPEND`` 追加，修改与删除走「同目录临时文件 + rename」，
  进程在写一半时被中断不会留下半截文件。读之前按文件大小与修改时刻判断
  是否被其它进程写过，变了就重扫，避免在共享卷上读到过期索引。

地址唯一性按「环境 + 字段 + 比较键」建索引：同一个邮箱不能在两个订阅者身上，
但开发环境与生产环境是两套数据，各自可以有同一个邮箱。比较键由
:func:`app.subscriber_identity.address_key` 算出，因此 ``Creator@Example.com``
与 ``creator@example.com``、``+86 138 0000 0000`` 与 ``+8613800000000``
都被视为同一个地址。索引在进程内随写入原子更新；JSONL 后端多进程共用同一份
文件时没有跨进程写入协调（与既有的追加写一致），并发写入同一地址的极端情形
由后写者覆盖索引，读路径不受影响。

列表按 ``(created_at, id)`` 排序并用游标翻页：订阅者数量增长后，「把所有
记录读进内存再切片」既费内存也不稳定（同一时刻创建的记录顺序会漂移），
游标指定上一页最后一条的位置，下一页从它之后继续。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Protocol

from .models.channel import Channel
from .models.entities import Environment, Subscriber
from .subscriber_identity import ADDRESS_FIELDS, address_key

logger = logging.getLogger("xw.subscribers")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_SUBSCRIBERS_PATH = _DATA_DIR / "subscribers.jsonl"

#: 单页条数的默认值与上限。上限存在的意义是防止一次请求把整张表拖出来。
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


class SubscriberNotFoundError(KeyError):
    """订阅者不存在。"""


class SubscriberConflictError(ValueError):
    """标识冲突（创建时已存在同一标识）。"""


class SubscriberAddressConflictError(ValueError):
    """邮箱或手机号已被别的订阅者绑定。

    带上字段名与占用者的标识：调用方要能一眼看出是「谁占了哪个地址」，
    只回一句「冲突」会让人把请求体从头核一遍。
    """

    def __init__(self, field: str, value: str, subscriber_id: str) -> None:
        self.field = field
        self.value = value
        self.subscriber_id = subscriber_id
        label = "邮箱" if field == "email" else "手机号"
        super().__init__(f"{label}已被订阅者 {subscriber_id} 绑定：{value}")


class SubscriberCursorError(ValueError):
    """翻页游标指向的记录不存在（可能是删掉了，或者抄错了标识）。"""


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: object) -> Optional[str]:
    """把可选文本字段裁剪空白；空串按「未绑定」处理。"""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _iso(stamp: datetime) -> str:
    return stamp.astimezone(timezone.utc).isoformat()


def _parse_timestamp(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip().replace("Z", "+00:00")
    try:
        stamp = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def normalize_channels(channels: object) -> list[Channel]:
    """把渠道列表收敛成有序去重的投递渠道。

    只保留真正能投递的渠道（``delay`` / ``digest`` 是编排步骤，挂在订阅者身上
    没有意义）；重复项按首次出现的位置去重，顺序保持不变，便于前端原样回显。
    """
    if channels is None:
        return []
    if isinstance(channels, (str, Channel)):
        channels = [channels]

    out: list[Channel] = []
    for item in channels:
        try:
            channel = item if isinstance(item, Channel) else Channel(str(item).strip())
        except ValueError as exc:
            raise ValueError(f"未知渠道：{item}") from exc
        if not channel.is_delivery():
            raise ValueError(f"{channel.value} 是编排步骤，不能作为订阅者的投递渠道")
        if channel not in out:
            out.append(channel)
    return out


def subscriber_to_row(subscriber: Subscriber) -> dict:
    """把实体转成落盘的 JSON 行（时间用 ISO8601 字符串）。"""
    return {
        "id": subscriber.id,
        "email": subscriber.email,
        "phone": subscriber.phone,
        "device_token": subscriber.device_token,
        "channels": [c.value for c in subscriber.channels],
        "environment": subscriber.environment.value,
        "created_at": _iso(subscriber.created_at),
        "updated_at": _iso(subscriber.updated_at),
    }


def subscriber_from_row(raw: object) -> Optional[Subscriber]:
    """从存储行还原实体；缺标识的行返回 ``None``（脏行跳过，不牵连整次读取）。"""
    if not isinstance(raw, dict):
        return None
    subscriber_id = _text(raw.get("id"))
    if subscriber_id is None:
        return None
    try:
        channels = normalize_channels(raw.get("channels") or [])
    except ValueError:
        # 文件里的渠道被改坏时按「未绑定」处理，而不是让整份列表读不出来。
        channels = []
    created = _parse_timestamp(raw.get("created_at")) or _now_utc()
    updated = _parse_timestamp(raw.get("updated_at")) or created
    environment = _text(raw.get("environment")) or Environment.DEVELOPMENT.value
    try:
        env = Environment(environment)
    except ValueError:
        env = Environment.DEVELOPMENT
    return Subscriber(
        id=subscriber_id,
        email=_text(raw.get("email")),
        phone=_text(raw.get("phone")),
        device_token=_text(raw.get("device_token")),
        channels=channels,
        environment=env,
        created_at=created,
        updated_at=max(updated, created),
    )


def _sort_key(subscriber: Subscriber) -> tuple[datetime, str]:
    return (subscriber.created_at, subscriber.id)


#: 地址索引：``(环境, 字段, 比较键) -> 订阅者标识``。加环境是因为两套环境是
#: 两套数据，同一个人可以在 development 与 production 各有一条档案。
AddressIndex = dict[tuple[str, str, str], str]


def _address_entries(subscriber: Subscriber) -> list[tuple[str, str]]:
    """列出一条记录参与查重的 ``(字段, 比较键)``；未绑定的字段不参与。"""
    entries: list[tuple[str, str]] = []
    for field in ADDRESS_FIELDS:
        value = getattr(subscriber, field, None)
        if not value:
            continue
        key = address_key(field, value)
        if key:
            entries.append((field, key))
    return entries


def _build_address_index(subscribers: Iterable[Subscriber]) -> AddressIndex:
    """按当前记录集合重建索引；同一地址在历史数据里出现多次时后写者占位。"""
    index: AddressIndex = {}
    for subscriber in subscribers:
        for field, key in _address_entries(subscriber):
            index[(subscriber.environment.value, field, key)] = subscriber.id
    return index


def _address_conflict(
    index: AddressIndex, subscriber: Subscriber
) -> Optional[tuple[str, str, str]]:
    """返回 ``(字段, 比较键, 占用者标识)``；没有冲突返回 ``None``。

    占用者是自己不算冲突：更新时把同一个邮箱再写一遍是常态。
    """
    for field, key in _address_entries(subscriber):
        owner = index.get((subscriber.environment.value, field, key))
        if owner is not None and owner != subscriber.id:
            return field, key, owner
    return None


class SubscriberStore(Protocol):
    """订阅者存储契约。"""

    def create(self, subscriber: Subscriber) -> Subscriber:
        """写入新订阅者。

        标识已存在时抛 :class:`SubscriberConflictError`；邮箱或手机号已被
        别的订阅者占用时抛 :class:`SubscriberAddressConflictError`。
        """
        ...

    def get(self, subscriber_id: str) -> Optional[Subscriber]:
        """按标识读取，不存在返回 ``None``。"""
        ...

    def list(
        self,
        *,
        channel: Optional[Channel] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[Subscriber], Optional[str]]:
        """按创建顺序翻页，返回 ``(本页记录, 下一页游标)``。"""
        ...

    def update(self, subscriber: Subscriber) -> Subscriber:
        """整体覆盖同标识记录；不存在或地址与他人重复时抛错。"""
        ...

    def delete(self, subscriber_id: str) -> bool:
        """删除记录，返回是否真的删掉了一条。"""
        ...


def _page(
    items: list[Subscriber],
    *,
    channel: Optional[Channel],
    limit: int,
    cursor: Optional[str],
) -> tuple[list[Subscriber], Optional[str]]:
    """在已排序的记录上做过滤与游标翻页。

    内存与文件后端共用这一处逻辑：两边各写一遍，游标语义迟早会不一致。
    """
    ordered = sorted(items, key=_sort_key)
    if channel is not None:
        ordered = [s for s in ordered if channel in s.channels]

    start = 0
    if cursor:
        position = next(
            (i for i, s in enumerate(ordered) if s.id == cursor.strip()), None
        )
        if position is None:
            raise SubscriberCursorError(f"游标指向的订阅者不存在：{cursor}")
        start = position + 1

    size = max(1, min(int(limit), MAX_PAGE_SIZE))
    window = ordered[start : start + size]
    has_more = start + size < len(ordered)
    next_cursor = window[-1].id if window and has_more else None
    return window, next_cursor


class MemorySubscriberStore:
    """进程内订阅者存储（线程安全）。"""

    def __init__(self, seed: Optional[list[Subscriber]] = None) -> None:
        self._by_id: dict[str, Subscriber] = {}
        self._addresses: AddressIndex = {}
        self._lock = threading.Lock()
        for subscriber in seed or []:
            self._by_id[subscriber.id] = subscriber
        self._addresses = _build_address_index(self._by_id.values())

    def _reindex(self) -> None:
        self._addresses = _build_address_index(self._by_id.values())

    def _reject_duplicate_address(self, subscriber: Subscriber) -> None:
        conflict = _address_conflict(self._addresses, subscriber)
        if conflict is not None:
            raise SubscriberAddressConflictError(*conflict)

    def create(self, subscriber: Subscriber) -> Subscriber:
        with self._lock:
            if subscriber.id in self._by_id:
                raise SubscriberConflictError(f"订阅者已存在：{subscriber.id}")
            self._reject_duplicate_address(subscriber)
            self._by_id[subscriber.id] = subscriber
            self._reindex()
            return subscriber

    def get(self, subscriber_id: str) -> Optional[Subscriber]:
        with self._lock:
            return self._by_id.get(str(subscriber_id).strip())

    def list(
        self,
        *,
        channel: Optional[Channel] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[Subscriber], Optional[str]]:
        with self._lock:
            snapshot = list(self._by_id.values())
        return _page(snapshot, channel=channel, limit=limit, cursor=cursor)

    def update(self, subscriber: Subscriber) -> Subscriber:
        with self._lock:
            if subscriber.id not in self._by_id:
                raise SubscriberNotFoundError(f"订阅者不存在：{subscriber.id}")
            self._reject_duplicate_address(subscriber)
            self._by_id[subscriber.id] = subscriber
            self._reindex()
            return subscriber

    def delete(self, subscriber_id: str) -> bool:
        with self._lock:
            removed = self._by_id.pop(str(subscriber_id).strip(), None)
            if removed is None:
                return False
            self._reindex()
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_id)


class JsonlSubscriberStore:
    """以一行一个 JSON 保存订阅者的存储。"""

    def __init__(self, path: str | os.PathLike[str] = DEFAULT_SUBSCRIBERS_PATH) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._by_id: dict[str, Subscriber] = {}
        self._addresses: AddressIndex = {}
        self._stamp: Optional[tuple[int, int]] = None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._reload()

    # ---------------------------------------------------------------- 文件
    @property
    def path(self) -> Path:
        return self._path

    def _file_stamp(self) -> Optional[tuple[int, int]]:
        try:
            stat = self._path.stat()
        except FileNotFoundError:
            return None
        return (stat.st_size, stat.st_mtime_ns)

    def _reload(self) -> None:
        """整份重读；一行坏掉只跳过这一行，不影响其它订阅者。"""
        records: dict[str, Subscriber] = {}
        if self._path.exists():
            with self._path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("订阅者文件存在无法解析的行，已跳过：%s", self._path)
                        continue
                    subscriber = subscriber_from_row(raw)
                    if subscriber is not None:
                        records[subscriber.id] = subscriber
        self._by_id = records
        self._addresses = _build_address_index(records.values())
        self._stamp = self._file_stamp()

    def _reindex(self) -> None:
        self._addresses = _build_address_index(self._by_id.values())

    def _reject_duplicate_address(self, subscriber: Subscriber) -> None:
        conflict = _address_conflict(self._addresses, subscriber)
        if conflict is not None:
            raise SubscriberAddressConflictError(*conflict)

    def _ensure_fresh(self) -> None:
        """被别人改过就重读，保证查询结果不是上一轮的快照。"""
        if self._file_stamp() != self._stamp:
            self._reload()

    def _rewrite(self) -> None:
        """把当前索引整体写回文件：临时文件 + ``os.replace``。"""
        rows = sorted(self._by_id.values(), key=_sort_key)
        payload = "".join(
            json.dumps(subscriber_to_row(s), ensure_ascii=False) + "\n" for s in rows
        )
        fd, tmp_name = tempfile.mkstemp(
            prefix=".subscribers-", suffix=".tmp", dir=str(self._path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._path)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise
        self._stamp = self._file_stamp()

    def _append(self, subscriber: Subscriber) -> None:
        line = json.dumps(subscriber_to_row(subscriber), ensure_ascii=False) + "\n"
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        self._stamp = self._file_stamp()

    # ---------------------------------------------------------------- 接口
    def create(self, subscriber: Subscriber) -> Subscriber:
        with self._lock:
            self._ensure_fresh()
            if subscriber.id in self._by_id:
                raise SubscriberConflictError(f"订阅者已存在：{subscriber.id}")
            self._reject_duplicate_address(subscriber)
            self._append(subscriber)
            self._by_id[subscriber.id] = subscriber
            self._reindex()
            return subscriber

    def get(self, subscriber_id: str) -> Optional[Subscriber]:
        with self._lock:
            self._ensure_fresh()
            return self._by_id.get(str(subscriber_id).strip())

    def list(
        self,
        *,
        channel: Optional[Channel] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[Subscriber], Optional[str]]:
        with self._lock:
            self._ensure_fresh()
            snapshot = list(self._by_id.values())
        return _page(snapshot, channel=channel, limit=limit, cursor=cursor)

    def update(self, subscriber: Subscriber) -> Subscriber:
        with self._lock:
            self._ensure_fresh()
            if subscriber.id not in self._by_id:
                raise SubscriberNotFoundError(f"订阅者不存在：{subscriber.id}")
            self._reject_duplicate_address(subscriber)
            self._by_id[subscriber.id] = subscriber
            self._rewrite()
            self._reindex()
            return subscriber

    def delete(self, subscriber_id: str) -> bool:
        key = str(subscriber_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._by_id:
                return False
            del self._by_id[key]
            self._rewrite()
            self._reindex()
            return True

    def __len__(self) -> int:
        with self._lock:
            self._ensure_fresh()
            return len(self._by_id)


def create_subscriber_store(backend: str, path: str | os.PathLike[str] | None = None) -> SubscriberStore:
    """按配置创建订阅者存储。

    目前只有内存与 JSONL 两种实现：订阅者数据量小、写入稀疏，文件后端足够
    支撑 demo；``postgres`` 与消息、工作流共用一张库表时再接（订阅者参与
    偏好判定之后才有跨进程共享的需求）。
    """
    if backend == "postgres":
        logger.warning("订阅者存储暂只提供 memory/jsonl，postgres 后端按 jsonl 处理")
    target = Path(path) if path else Path(
        os.getenv("XW_SUBSCRIBERS_FILE") or DEFAULT_SUBSCRIBERS_PATH
    )
    logger.info("订阅者存储后端：jsonl（%s）", target)
    return JsonlSubscriberStore(target)


_store: Optional[SubscriberStore] = None
# 非默认租户的存储实例：按租户标识缓存，各自落在
# ``data/tenants/<租户>/subscribers.jsonl``。默认租户沿用 ``_store``，
# 保持既有部署的路径与测试的替换点不变。
_tenant_stores: dict[str, SubscriberStore] = {}
_store_lock = threading.Lock()


def get_subscriber_store(tenant: Optional[str] = None) -> SubscriberStore:
    """获取订阅者存储（按租户隔离，惰性初始化）。

    不传租户时取请求上下文里的租户；后台线程没有请求上下文，拿到的是
    默认租户——那里的写入必须显式传租户，否则会落到默认租户的档案里。
    """
    from . import tenancy

    global _store

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            if _store is None:
                base = Path(os.getenv("XW_SUBSCRIBERS_FILE") or DEFAULT_SUBSCRIBERS_PATH)
                _store = create_subscriber_store("jsonl", tenancy.scoped_path(base, key))
            return _store
        store = _tenant_stores.get(key)
        if store is None:
            base = Path(os.getenv("XW_SUBSCRIBERS_FILE") or DEFAULT_SUBSCRIBERS_PATH)
            store = create_subscriber_store("jsonl", tenancy.scoped_path(base, key))
            _tenant_stores[key] = store
        return store


def set_subscriber_store(
    store: Optional[SubscriberStore], tenant: Optional[str] = None
) -> None:
    """替换某个租户的存储单例；传 ``None`` 表示下次取用时按配置重建。"""
    from . import tenancy

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            globals()["_store"] = store
        elif store is None:
            _tenant_stores.pop(key, None)
        else:
            _tenant_stores[key] = store


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_SUBSCRIBERS_PATH",
    "MAX_PAGE_SIZE",
    "JsonlSubscriberStore",
    "MemorySubscriberStore",
    "SubscriberAddressConflictError",
    "SubscriberConflictError",
    "SubscriberCursorError",
    "SubscriberNotFoundError",
    "SubscriberStore",
    "create_subscriber_store",
    "get_subscriber_store",
    "normalize_channels",
    "set_subscriber_store",
    "subscriber_from_row",
    "subscriber_to_row",
]
