"""分组（Topic）存储：内存 / JSONL 双后端。

分组解决的是「一次通知发给一群人」：把订阅者按主题归到一起（关注某个作者、
订阅某个话题、某个社区的活跃创作者），触发时按分组广播，而不是把一长串
订阅者标识塞进请求体。分组只负责**成员关系**，不参与投递判定——成员各自的
渠道绑定与偏好仍然在投递阶段生效。

一条记录分两种：

- ``topic``：分组本身（标识、名称、描述、环境、创建/更新时间）；
- ``member``：成员关系（分组标识、订阅者标识、加入时刻）。

两种记录写在同一个文件里（默认 ``api/data/topics.jsonl``，可由
``XW_TOPICS_FILE`` 覆盖），靠 ``type`` 字段区分。合成一个文件而不是两个，
是因为删除分组要连带清掉成员：同一次 rewrite 里一起写出去，不会出现
「分组没了、成员还挂在一个不存在的分组上」。

写入策略与订阅者存储一致：新增走单次 ``O_APPEND``（一条记录一行，多进程
追加不会把两条拼成一行），修改与删除走「同目录临时文件 + rename」，读到一半
被中断也不会留下半截文件；读之前按文件大小与修改时刻判断是否被其它进程写过。

成员关系是集合语义：同一个订阅者重复加入同一分组只保留一条，``add_member``
返回是否**新增**了成员。加入时刻取首次加入的时间，重复加入不会把它推到最新，
否则成员列表的顺序会随重复调用漂移。
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

from .models.entities import Environment, Topic

logger = logging.getLogger("xw.topics")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_TOPICS_PATH = _DATA_DIR / "topics.jsonl"

#: 单页条数的默认值与上限（与订阅者列表同口径）。
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200

#: 单个分组的成员数上限。广播会把名单一次展开成 N 条消息，没有上限时一个
#: 误写的循环就能把整条投递链路灌满；默认值按「正常运营的社区分组」取，
#: 需要更大规模时用 ``XW_TOPIC_MAX_MEMBERS`` 调高。
DEFAULT_MAX_MEMBERS = 10_000

_TOPIC_KIND = "topic"
_MEMBER_KIND = "member"


class TopicNotFoundError(KeyError):
    """分组不存在。"""


class TopicConflictError(ValueError):
    """标识冲突（创建时已存在同一标识）。"""


class TopicCursorError(ValueError):
    """翻页游标指向的记录不存在（可能是删掉了，或者抄错了标识）。"""


class TopicCapacityError(ValueError):
    """分组成员数已达上限。

    带上限、当前人数与分组标识：调用方需要知道「差多少」，而不是只看到一句
    「加不进去」。重复加入一个已经在名单里的订阅者不算超额，因此这个错误
    只在真的要新增成员时抛出。
    """

    def __init__(self, topic_id: str, limit: int, current: int) -> None:
        self.topic_id = topic_id
        self.limit = limit
        self.current = current
        super().__init__(
            f"分组 {topic_id} 的成员数已达上限 {limit}（当前 {current}），"
            "请先移出部分成员或拆分分组"
        )


def _max_members_from_env() -> int:
    """读取成员上限配置；写错时退回默认值并留一条日志，不让进程起不来。"""
    raw = os.getenv("XW_TOPIC_MAX_MEMBERS")
    if raw is None or not raw.strip():
        return DEFAULT_MAX_MEMBERS
    try:
        value = int(raw)
    except ValueError:
        logger.warning("XW_TOPIC_MAX_MEMBERS 不是整数（%r），按默认 %s 处理", raw, DEFAULT_MAX_MEMBERS)
        return DEFAULT_MAX_MEMBERS
    if value < 1:
        logger.warning("XW_TOPIC_MAX_MEMBERS 必须为正数（%s），按默认 %s 处理", raw, DEFAULT_MAX_MEMBERS)
        return DEFAULT_MAX_MEMBERS
    return value


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def _text(value: object) -> Optional[str]:
    """裁剪空白；空串按「未填写」处理。"""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# ---------------------------------------------------------------- 行格式


def topic_to_row(topic: Topic) -> dict:
    """分组行：``type`` 是为了与成员行共用一个文件。"""
    return {
        "type": _TOPIC_KIND,
        "id": topic.id,
        "name": topic.name,
        "description": topic.description,
        "environment": topic.environment.value,
        "created_at": _iso(topic.created_at),
        "updated_at": _iso(topic.updated_at),
    }


def member_to_row(topic_id: str, subscriber_id: str, added_at: datetime) -> dict:
    return {
        "type": _MEMBER_KIND,
        "topic_id": topic_id,
        "subscriber_id": subscriber_id,
        "added_at": _iso(added_at),
    }


def topic_from_row(raw: object) -> Optional[Topic]:
    """从存储行还原分组；缺标识的行返回 ``None``（脏行跳过）。"""
    if not isinstance(raw, dict):
        return None
    topic_id = _text(raw.get("id"))
    if topic_id is None:
        return None
    created = _parse_timestamp(raw.get("created_at")) or _now_utc()
    updated = _parse_timestamp(raw.get("updated_at")) or created
    try:
        environment = Environment(_text(raw.get("environment")) or Environment.DEVELOPMENT.value)
    except ValueError:
        environment = Environment.DEVELOPMENT
    return Topic(
        id=topic_id,
        name=_text(raw.get("name")) or topic_id,
        description=_text(raw.get("description")),
        environment=environment,
        created_at=created,
        updated_at=max(updated, created),
    )


def member_from_row(raw: object) -> Optional[tuple[str, str, datetime]]:
    """从存储行还原成员关系，返回 ``(分组标识, 订阅者标识, 加入时刻)``。"""
    if not isinstance(raw, dict):
        return None
    topic_id = _text(raw.get("topic_id"))
    subscriber_id = _text(raw.get("subscriber_id"))
    if topic_id is None or subscriber_id is None:
        return None
    return topic_id, subscriber_id, _parse_timestamp(raw.get("added_at")) or _now_utc()


#: ``topic_id -> {subscriber_id: added_at}``。用字典而不是列表，重复加入
#: 天然收敛成一条，「是否已是成员」也不必再扫一遍。
Membership = dict[str, dict[str, datetime]]


def _sort_key(topic: Topic) -> tuple[datetime, str]:
    return (topic.created_at, topic.id)


def _page_topics(
    topics: Iterable[Topic],
    *,
    environment: Optional[Environment],
    limit: int,
    cursor: Optional[str],
) -> tuple[list[Topic], Optional[str]]:
    """排序、按环境过滤、游标翻页。

    内存与文件后端共用这一处逻辑：两边各写一遍，游标语义迟早会不一致。
    """
    ordered = sorted(topics, key=_sort_key)
    if environment is not None:
        ordered = [t for t in ordered if t.environment is environment]

    start = 0
    if cursor:
        position = next(
            (i for i, t in enumerate(ordered) if t.id == cursor.strip()), None
        )
        if position is None:
            raise TopicCursorError(f"游标指向的分组不存在：{cursor}")
        start = position + 1

    size = max(1, min(int(limit), MAX_PAGE_SIZE))
    window = ordered[start : start + size]
    has_more = start + size < len(ordered)
    next_cursor = window[-1].id if window and has_more else None
    return window, next_cursor


def _sorted_members(entries: dict[str, datetime]) -> list[tuple[str, datetime]]:
    """成员按「加入时刻 + 订阅者标识」排序，时刻相同也不会来回换位。"""
    return sorted(entries.items(), key=lambda item: (item[1], item[0]))


def _page_members(
    entries: dict[str, datetime],
    *,
    limit: int,
    cursor: Optional[str],
) -> tuple[list[tuple[str, datetime]], Optional[str], int]:
    """返回 ``(本页成员, 下一页游标, 成员总数)``。"""
    ordered = _sorted_members(entries)
    start = 0
    if cursor:
        key = cursor.strip()
        position = next((i for i, (sid, _) in enumerate(ordered) if sid == key), None)
        if position is None:
            raise TopicCursorError(f"游标指向的成员不存在：{cursor}")
        start = position + 1

    size = max(1, min(int(limit), MAX_PAGE_SIZE))
    window = ordered[start : start + size]
    has_more = start + size < len(ordered)
    next_cursor = window[-1][0] if window and has_more else None
    return window, next_cursor, len(ordered)


class TopicStore(Protocol):
    """分组存储契约。"""

    @property
    def max_members(self) -> int:
        """单个分组的成员数上限；容量判定由存储层在同一把锁内完成。"""
        ...

    def create(self, topic: Topic) -> Topic:
        """写入新分组；标识已存在时抛 :class:`TopicConflictError`。"""
        ...

    def get(self, topic_id: str) -> Optional[Topic]:
        """按标识读取，不存在返回 ``None``。"""
        ...

    def list(
        self,
        *,
        environment: Optional[Environment] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[Topic], Optional[str]]:
        """按创建顺序翻页，返回 ``(本页记录, 下一页游标)``。"""
        ...

    def update(self, topic: Topic) -> Topic:
        """整体覆盖同标识分组；不存在时抛 :class:`TopicNotFoundError`。"""
        ...

    def delete(self, topic_id: str) -> bool:
        """删除分组及其全部成员关系，返回是否真的删掉了一条。"""
        ...

    def add_member(
        self, topic_id: str, subscriber_id: str, added_at: Optional[datetime] = None
    ) -> bool:
        """加入分组，返回是否新增；已是成员返回 ``False``（不刷新加入时刻）。"""
        ...

    def remove_member(self, topic_id: str, subscriber_id: str) -> bool:
        """移出分组，返回是否真的删掉了一条成员关系。"""
        ...

    def members(
        self,
        topic_id: str,
        *,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[tuple[str, datetime]], Optional[str], int]:
        """成员列表：``(本页成员, 下一页游标, 成员总数)``。"""
        ...

    def member_count(self, topic_id: str) -> int:
        """分组成员数。"""
        ...

    def member_ids(self, topic_id: str) -> list[str]:
        """全量成员标识，按加入时刻排序。

        广播要的是整份成员名单而不是某一页：分页接口的游标是给「人看列表」
        用的，若拿它一页页循环取完，中途有人加入或退出就会漏掉或重复。
        """
        ...

    def topics_of(self, subscriber_id: str) -> list[str]:
        """某订阅者所在的分组标识（按分组标识排序）。"""
        ...


class MemoryTopicStore:
    """进程内分组存储（线程安全）。"""

    def __init__(
        self,
        seed: Optional[list[Topic]] = None,
        max_members: int = DEFAULT_MAX_MEMBERS,
    ) -> None:
        self._topics: dict[str, Topic] = {}
        self._members: Membership = {}
        self._max_members = max(1, int(max_members))
        self._lock = threading.Lock()
        for topic in seed or []:
            self._topics[topic.id] = topic
            self._members.setdefault(topic.id, {})

    @property
    def max_members(self) -> int:
        return self._max_members

    def create(self, topic: Topic) -> Topic:
        with self._lock:
            if topic.id in self._topics:
                raise TopicConflictError(f"分组已存在：{topic.id}")
            self._topics[topic.id] = topic
            self._members.setdefault(topic.id, {})
            return topic

    def get(self, topic_id: str) -> Optional[Topic]:
        with self._lock:
            return self._topics.get(str(topic_id).strip())

    def list(
        self,
        *,
        environment: Optional[Environment] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[Topic], Optional[str]]:
        with self._lock:
            snapshot = list(self._topics.values())
        return _page_topics(snapshot, environment=environment, limit=limit, cursor=cursor)

    def update(self, topic: Topic) -> Topic:
        with self._lock:
            if topic.id not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{topic.id}")
            self._topics[topic.id] = topic
            return topic

    def delete(self, topic_id: str) -> bool:
        key = str(topic_id).strip()
        with self._lock:
            if self._topics.pop(key, None) is None:
                return False
            self._members.pop(key, None)
            return True

    def add_member(
        self, topic_id: str, subscriber_id: str, added_at: Optional[datetime] = None
    ) -> bool:
        key = str(topic_id).strip()
        member = str(subscriber_id).strip()
        with self._lock:
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            bucket = self._members.setdefault(key, {})
            if member in bucket:
                return False
            if len(bucket) >= self._max_members:
                raise TopicCapacityError(key, self._max_members, len(bucket))
            bucket[member] = added_at or _now_utc()
            return True

    def remove_member(self, topic_id: str, subscriber_id: str) -> bool:
        key = str(topic_id).strip()
        member = str(subscriber_id).strip()
        with self._lock:
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            return self._members.setdefault(key, {}).pop(member, None) is not None

    def members(
        self,
        topic_id: str,
        *,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[tuple[str, datetime]], Optional[str], int]:
        key = str(topic_id).strip()
        with self._lock:
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            bucket = dict(self._members.get(key, {}))
        return _page_members(bucket, limit=limit, cursor=cursor)

    def member_count(self, topic_id: str) -> int:
        key = str(topic_id).strip()
        with self._lock:
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            return len(self._members.get(key, {}))

    def member_ids(self, topic_id: str) -> list[str]:
        key = str(topic_id).strip()
        with self._lock:
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            ordered = _sorted_members(dict(self._members.get(key, {})))
        return [subscriber_id for subscriber_id, _ in ordered]

    def topics_of(self, subscriber_id: str) -> list[str]:
        member = str(subscriber_id).strip()
        with self._lock:
            return sorted(
                topic_id for topic_id, bucket in self._members.items() if member in bucket
            )

    def __len__(self) -> int:
        with self._lock:
            return len(self._topics)


class JsonlTopicStore:
    """以一行一个 JSON 保存分组与成员关系的存储。"""

    def __init__(
        self,
        path: str | os.PathLike[str] = DEFAULT_TOPICS_PATH,
        max_members: int = DEFAULT_MAX_MEMBERS,
    ) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._topics: dict[str, Topic] = {}
        self._members: Membership = {}
        self._stamp: Optional[tuple[int, int]] = None
        self._max_members = max(1, int(max_members))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._reload()

    @property
    def max_members(self) -> int:
        return self._max_members

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
        """整份重读；一行坏掉只跳过这一行，不影响其它分组与成员。"""
        topics: dict[str, Topic] = {}
        members: Membership = {}
        if self._path.exists():
            with self._path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("分组文件存在无法解析的行，已跳过：%s", self._path)
                        continue
                    if not isinstance(raw, dict):
                        continue
                    kind = raw.get("type")
                    if kind == _TOPIC_KIND:
                        topic = topic_from_row(raw)
                        if topic is not None:
                            topics[topic.id] = topic
                    elif kind == _MEMBER_KIND:
                        entry = member_from_row(raw)
                        if entry is not None:
                            topic_id, subscriber_id, added_at = entry
                            # 同一成员出现两次时保留最早的时刻：加入时刻是
                            # 「第一次加入」的时间，不该被后来的一行改写。
                            bucket = members.setdefault(topic_id, {})
                            current = bucket.get(subscriber_id)
                            if current is None or added_at < current:
                                bucket[subscriber_id] = added_at
                    # 其它 type 属于未来版本的行，跳过即可。
        # 分组已被删除、成员行还留在文件里的情况：丢掉孤儿成员，
        # 免得「分组不存在」和「有人挂着」同时成立。
        members = {k: v for k, v in members.items() if k in topics}
        self._topics = topics
        self._members = members
        self._stamp = self._file_stamp()

    def _ensure_fresh(self) -> None:
        """被别人改过就重读，保证查询结果不是上一轮的快照。"""
        if self._file_stamp() != self._stamp:
            self._reload()

    def _rewrite(self) -> None:
        """把当前内容整体写回文件：临时文件 + ``os.replace``。"""
        lines: list[str] = [
            json.dumps(topic_to_row(t), ensure_ascii=False)
            for t in sorted(self._topics.values(), key=_sort_key)
        ]
        for topic_id in sorted(self._members):
            for subscriber_id, added_at in _sorted_members(self._members[topic_id]):
                lines.append(
                    json.dumps(
                        member_to_row(topic_id, subscriber_id, added_at),
                        ensure_ascii=False,
                    )
                )
        payload = "".join(line + "\n" for line in lines)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".topics-", suffix=".tmp", dir=str(self._path.parent)
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

    def _append(self, row: dict) -> None:
        line = json.dumps(row, ensure_ascii=False) + "\n"
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        self._stamp = self._file_stamp()

    # ---------------------------------------------------------------- 接口
    def create(self, topic: Topic) -> Topic:
        with self._lock:
            self._ensure_fresh()
            if topic.id in self._topics:
                raise TopicConflictError(f"分组已存在：{topic.id}")
            self._append(topic_to_row(topic))
            self._topics[topic.id] = topic
            self._members.setdefault(topic.id, {})
            return topic

    def get(self, topic_id: str) -> Optional[Topic]:
        with self._lock:
            self._ensure_fresh()
            return self._topics.get(str(topic_id).strip())

    def list(
        self,
        *,
        environment: Optional[Environment] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[Topic], Optional[str]]:
        with self._lock:
            self._ensure_fresh()
            snapshot = list(self._topics.values())
        return _page_topics(snapshot, environment=environment, limit=limit, cursor=cursor)

    def update(self, topic: Topic) -> Topic:
        with self._lock:
            self._ensure_fresh()
            if topic.id not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{topic.id}")
            self._topics[topic.id] = topic
            self._rewrite()
            return topic

    def delete(self, topic_id: str) -> bool:
        key = str(topic_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._topics:
                return False
            del self._topics[key]
            self._members.pop(key, None)
            self._rewrite()
            return True

    def add_member(
        self, topic_id: str, subscriber_id: str, added_at: Optional[datetime] = None
    ) -> bool:
        key = str(topic_id).strip()
        member = str(subscriber_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            bucket = self._members.setdefault(key, {})
            if member in bucket:
                return False
            if len(bucket) >= self._max_members:
                raise TopicCapacityError(key, self._max_members, len(bucket))
            stamp = added_at or _now_utc()
            self._append(member_to_row(key, member, stamp))
            bucket[member] = stamp
            return True

    def remove_member(self, topic_id: str, subscriber_id: str) -> bool:
        key = str(topic_id).strip()
        member = str(subscriber_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            bucket = self._members.setdefault(key, {})
            if member not in bucket:
                return False
            del bucket[member]
            self._rewrite()
            return True

    def members(
        self,
        topic_id: str,
        *,
        limit: int = DEFAULT_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> tuple[list[tuple[str, datetime]], Optional[str], int]:
        key = str(topic_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            bucket = dict(self._members.get(key, {}))
        return _page_members(bucket, limit=limit, cursor=cursor)

    def member_count(self, topic_id: str) -> int:
        key = str(topic_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            return len(self._members.get(key, {}))

    def member_ids(self, topic_id: str) -> list[str]:
        key = str(topic_id).strip()
        with self._lock:
            self._ensure_fresh()
            if key not in self._topics:
                raise TopicNotFoundError(f"分组不存在：{key}")
            ordered = _sorted_members(dict(self._members.get(key, {})))
        return [subscriber_id for subscriber_id, _ in ordered]

    def topics_of(self, subscriber_id: str) -> list[str]:
        member = str(subscriber_id).strip()
        with self._lock:
            self._ensure_fresh()
            return sorted(
                topic_id for topic_id, bucket in self._members.items() if member in bucket
            )

    def __len__(self) -> int:
        with self._lock:
            self._ensure_fresh()
            return len(self._topics)


def create_topic_store(
    backend: str, path: str | os.PathLike[str] | None = None
) -> TopicStore:
    """按配置创建分组存储。

    与订阅者存储一样只提供 memory / jsonl：分组是配置型数据，写入稀疏、
    量级小，文件后端足够；真正需要跨实例共享时再与消息表一起迁到 Postgres。
    """
    if backend == "postgres":
        logger.warning("分组存储暂只提供 memory/jsonl，postgres 后端按 jsonl 处理")
    target = Path(path) if path else Path(
        os.getenv("XW_TOPICS_FILE") or DEFAULT_TOPICS_PATH
    )
    limit = _max_members_from_env()
    logger.info("分组存储后端：jsonl（%s，成员上限 %s）", target, limit)
    return JsonlTopicStore(target, max_members=limit)


_store: Optional[TopicStore] = None
_tenant_stores: dict[str, TopicStore] = {}
_store_lock = threading.Lock()


def get_topic_store(tenant: Optional[str] = None) -> TopicStore:
    """获取分组存储（按租户隔离，惰性初始化）。"""
    from . import tenancy

    global _store

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            if _store is None:
                base = Path(os.getenv("XW_TOPICS_FILE") or DEFAULT_TOPICS_PATH)
                _store = create_topic_store("jsonl", tenancy.scoped_path(base, key))
            return _store
        store = _tenant_stores.get(key)
        if store is None:
            base = Path(os.getenv("XW_TOPICS_FILE") or DEFAULT_TOPICS_PATH)
            store = create_topic_store("jsonl", tenancy.scoped_path(base, key))
            _tenant_stores[key] = store
        return store


def set_topic_store(store: Optional[TopicStore], tenant: Optional[str] = None) -> None:
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
    "DEFAULT_MAX_MEMBERS",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_TOPICS_PATH",
    "MAX_PAGE_SIZE",
    "JsonlTopicStore",
    "MemoryTopicStore",
    "TopicCapacityError",
    "TopicConflictError",
    "TopicCursorError",
    "TopicNotFoundError",
    "TopicStore",
    "create_topic_store",
    "get_topic_store",
    "member_from_row",
    "member_to_row",
    "set_topic_store",
    "topic_from_row",
    "topic_to_row",
]
