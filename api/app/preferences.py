"""发送偏好的三层模型：全局 / 工作流 / 渠道。

一条偏好记录回答一个问题：「这个订阅者是否愿意在某个层级上收通知」。

======================  ==========================  ====================================
层级                     ``target``                 解决的问题
======================  ==========================  ====================================
``global``              空                          这个人要不要收任何通知（总开关）
``workflow``            工作流名，如 ``post_liked``  这类互动要不要通知我
``channel``             渠道，如 ``email``           这类通知要不要用这个渠道发
======================  ==========================  ====================================

三层不是三张互相独立的表，而是**一条覆盖链**：解析时从最具体的一层往上找，
找到第一条记录就停。渠道关了但工作流开着，这条通知就不走这个渠道；渠道没配
但工作流关了，整条通知就跳过。三层都没有记录时按「接收」处理——偏好是收窄
投递范围的手段，默认打开才不会让新订阅者收不到任何东西。

只有「开关」没有「地址」：地址属于订阅者档案（邮箱、手机号、设备 token），
偏好只表达意愿。两者分开才能做到「先停掉这个渠道，地址先留着」，恢复时不用
重新收集。

本模块只做模型、解析与存储；查询与写入接口在 ``app.preference_routes``，
投递前的判定由消费路径接入。
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
from typing import Iterable, Optional, Protocol

from pydantic import BaseModel, Field, field_validator, model_validator

from .models.channel import Channel

logger = logging.getLogger("xw.preferences")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_PREFERENCES_PATH = _DATA_DIR / "preferences.jsonl"

#: 订阅者标识与工作流名的长度上限。工作流名会出现在队列消息与执行明细里，
#: 过长只会让这些地方难读，挡在入口即可。
MAX_SUBSCRIBER_ID_LENGTH = 128
MAX_WORKFLOW_LENGTH = 64


class Scope(str, Enum):
    """偏好的层级。"""

    GLOBAL = "global"
    WORKFLOW = "workflow"
    CHANNEL = "channel"

    def label(self) -> str:
        return {"global": "全局", "workflow": "工作流", "channel": "渠道"}[self.value]


#: 解析顺序：从最具体的一层往上。渠道关掉只影响这一个渠道，工作流关掉影响它
#: 下面所有的渠道，因此渠道先于工作流、工作流先于全局。
SPECIFICITY: tuple[Scope, ...] = (Scope.CHANNEL, Scope.WORKFLOW, Scope.GLOBAL)
_SPECIFICITY_INDEX = {scope: index for index, scope in enumerate(SPECIFICITY)}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: object) -> Optional[str]:
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
    try:
        stamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def normalize_target(scope: Scope, target: object) -> Optional[str]:
    """把 ``target`` 收敛成该层级的合法取值；不成立时抛出 :class:`ValueError`。

    三层的规则各不相同，放在一处判定，存储与接口就不会各写一套：
    全局不接受 ``target``（它本来就是一个人的总开关，带上目标只会让语义含糊），
    工作流要一个名字，渠道要是**可投递**渠道——``delay`` / ``digest`` 是编排
    步骤，挂到偏好上等于写了一个永远匹配不到的条件。
    """
    # 枚举成员直接取 ``value``：``str(Channel.EMAIL)`` 在 3.11+ 给出的是
    # ``Channel.EMAIL`` 而不是 ``email``，按字符串处理会把合法渠道判成未知。
    text = target.value if isinstance(target, Channel) else _text(target)
    if scope is Scope.GLOBAL:
        if text is not None:
            raise ValueError("全局偏好不接受 target：它作用于所有工作流与渠道")
        return None

    if text is None:
        raise ValueError(f"{scope.label()}偏好必须指定 target")

    if scope is Scope.CHANNEL:
        try:
            channel = Channel(text)
        except ValueError as exc:
            raise ValueError(f"未知渠道：{text}") from exc
        if not channel.is_delivery():
            raise ValueError(f"{channel.value} 是编排步骤，不能作为偏好的渠道")
        return channel.value

    if len(text) > MAX_WORKFLOW_LENGTH:
        raise ValueError(f"工作流名不能超过 {MAX_WORKFLOW_LENGTH} 个字符")
    if any(char.isspace() for char in text):
        raise ValueError("工作流名不能包含空白字符")
    return text


def normalize_subscriber_id(raw: object) -> str:
    """校验偏好归属的订阅者标识。"""
    text = _text(raw)
    if text is None:
        raise ValueError("订阅者标识不能为空")
    if len(text) > MAX_SUBSCRIBER_ID_LENGTH:
        raise ValueError(f"订阅者标识不能超过 {MAX_SUBSCRIBER_ID_LENGTH} 个字符")
    return text


class Preference(BaseModel):
    """一条偏好记录，键是 ``(订阅者, 层级, 目标)``。"""

    subscriber_id: str = Field(description="订阅者标识")
    scope: Scope = Field(description="层级：global / workflow / channel")
    target: Optional[str] = Field(default=None, description="工作流名或渠道；全局为空")
    enabled: bool = Field(default=True, description="是否接收")
    updated_at: datetime = Field(default_factory=_now_utc)

    @field_validator("subscriber_id")
    @classmethod
    def _check_subscriber(cls, value: str) -> str:
        return normalize_subscriber_id(value)

    @field_validator("target")
    @classmethod
    def _trim_target(cls, value: Optional[str]) -> Optional[str]:
        return _text(value)

    @model_validator(mode="after")
    def _check_target(self) -> "Preference":
        # 在这里再判一次而不是只依赖构造参数校验：从文件或请求还原记录时，
        # 层级与目标可能来自两个不同来源，错配必须在进内存之前暴露。
        self.target = normalize_target(self.scope, self.target)
        return self

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.subscriber_id, self.scope.value, self.target or "")

    def describe(self) -> str:
        """一句话描述这条记录，用于日志与错误信息。"""
        state = "接收" if self.enabled else "不接收"
        if self.scope is Scope.GLOBAL:
            return f"{self.subscriber_id} 全局{state}"
        return f"{self.subscriber_id} 的{self.scope.label()} {self.target} {state}"


def preference_to_row(preference: Preference) -> dict:
    """落盘用的行（时间统一 ISO8601 字符串）。"""
    return {
        "subscriber_id": preference.subscriber_id,
        "scope": preference.scope.value,
        "target": preference.target,
        "enabled": preference.enabled,
        "updated_at": _iso(preference.updated_at),
    }


def preference_from_row(raw: object) -> Optional[Preference]:
    """从存储行还原记录；坏行返回 ``None``（跳过，不牵连整份读取）。"""
    if not isinstance(raw, dict):
        return None
    try:
        scope = Scope(str(raw.get("scope", "")).strip())
    except ValueError:
        return None
    try:
        return Preference(
            subscriber_id=raw.get("subscriber_id"),
            scope=scope,
            target=raw.get("target"),
            enabled=bool(raw.get("enabled", True)),
            updated_at=_parse_timestamp(raw.get("updated_at")) or _now_utc(),
        )
    except ValueError:
        return None


@dataclass(frozen=True)
class PreferenceDecision:
    """一次解析的结论：发不发、是哪一层决定的、为什么。"""

    allowed: bool
    scope: Optional[Scope]
    target: Optional[str]
    matched: bool
    reason: str

    def as_dict(self) -> dict:
        return {
            "allowed": self.allowed,
            "scope": self.scope.value if self.scope else None,
            "target": self.target,
            "matched": self.matched,
            "reason": self.reason,
        }


class PreferenceSet:
    """某个时刻的偏好快照，负责回答「这条通知能不能发」。

    用快照而不是每次查存储：一次投递要问三层，逐层查会在同一个订阅者上做三次
    读取；快照还能保证一次判定里看到的是一致的视图。
    """

    def __init__(self, records: Iterable[Preference] = ()) -> None:
        self._by_key: dict[tuple[str, str, str], Preference] = {}
        for record in records:
            # 后出现的覆盖先出现的：与存储的 upsert 语义一致，重复键不会
            # 让「哪一条生效」取决于遍历顺序之外的东西。
            self._by_key[record.key] = record

    def __len__(self) -> int:
        return len(self._by_key)

    def get(
        self, subscriber_id: str, scope: Scope, target: object = None
    ) -> Optional[Preference]:
        key = (
            normalize_subscriber_id(subscriber_id),
            scope.value,
            normalize_target(scope, target) or "",
        )
        return self._by_key.get(key)

    def records(self, subscriber_id: Optional[str] = None) -> list[Preference]:
        """按「订阅者 → 具体程度 → 目标」排序返回记录。"""
        items = list(self._by_key.values())
        if subscriber_id is not None:
            wanted = normalize_subscriber_id(subscriber_id)
            items = [item for item in items if item.subscriber_id == wanted]
        items.sort(
            key=lambda item: (
                item.subscriber_id,
                _SPECIFICITY_INDEX[item.scope],
                item.target or "",
            )
        )
        return items

    def resolve(
        self,
        subscriber_id: str,
        *,
        workflow: Optional[str] = None,
        channel: Channel | str | None = None,
    ) -> PreferenceDecision:
        """按「渠道 → 工作流 → 全局」的顺序解析，命中第一条即返回。"""
        candidates: list[tuple[Scope, Optional[str]]] = []
        if channel is not None:
            candidates.append((Scope.CHANNEL, normalize_target(Scope.CHANNEL, channel)))
        if workflow is not None:
            candidates.append((Scope.WORKFLOW, normalize_target(Scope.WORKFLOW, workflow)))
        candidates.append((Scope.GLOBAL, None))

        for scope, target in candidates:
            record = self.get(subscriber_id, scope, target)
            if record is None:
                continue
            return PreferenceDecision(
                allowed=record.enabled,
                scope=scope,
                target=target,
                matched=True,
                reason=_describe_decision(scope, target, record.enabled),
            )
        return PreferenceDecision(
            allowed=True,
            scope=None,
            target=None,
            matched=False,
            reason="未设置偏好，按默认接收",
        )


def _describe_decision(scope: Scope, target: Optional[str], enabled: bool) -> str:
    state = "已开启" if enabled else "已关闭"
    if scope is Scope.GLOBAL:
        return f"全局总开关{state}"
    return f"{scope.label()} {target} {state}"


class PreferenceStore(Protocol):
    """偏好存储契约：按 ``(订阅者, 层级, 目标)`` 覆盖写。"""

    def upsert(self, preference: Preference) -> Preference:
        """写入或覆盖一条记录。"""
        ...

    def get(
        self, subscriber_id: str, scope: Scope, target: object = None
    ) -> Optional[Preference]:
        """按主键读取。"""
        ...

    def list(
        self, *, subscriber_id: Optional[str] = None, scope: Optional[Scope] = None
    ) -> list[Preference]:
        """列出记录，可按订阅者与层级过滤。"""
        ...

    def delete(self, subscriber_id: str, scope: Scope, target: object = None) -> bool:
        """删除一条记录，返回是否真的删掉了一条。"""
        ...

    def snapshot(self) -> PreferenceSet:
        """取当前快照，供投递路径解析使用。"""
        ...


def _sort_key(preference: Preference) -> tuple[str, int, str]:
    return (
        preference.subscriber_id,
        _SPECIFICITY_INDEX[preference.scope],
        preference.target or "",
    )


def _ordered(items: Iterable[Preference]) -> list[Preference]:
    return sorted(items, key=_sort_key)


class MemoryPreferenceStore:
    """进程内偏好存储（线程安全）。"""

    def __init__(self, seed: Iterable[Preference] = ()) -> None:
        self._by_key: dict[tuple[str, str, str], Preference] = {}
        self._lock = threading.Lock()
        for record in seed:
            self._by_key[record.key] = record

    def upsert(self, preference: Preference) -> Preference:
        with self._lock:
            self._by_key[preference.key] = preference
            return preference

    def get(
        self, subscriber_id: str, scope: Scope, target: object = None
    ) -> Optional[Preference]:
        key = (
            normalize_subscriber_id(subscriber_id),
            scope.value,
            normalize_target(scope, target) or "",
        )
        with self._lock:
            return self._by_key.get(key)

    def list(
        self, *, subscriber_id: Optional[str] = None, scope: Optional[Scope] = None
    ) -> list[Preference]:
        with self._lock:
            items = list(self._by_key.values())
        if subscriber_id is not None:
            wanted = normalize_subscriber_id(subscriber_id)
            items = [item for item in items if item.subscriber_id == wanted]
        if scope is not None:
            items = [item for item in items if item.scope is scope]
        return _ordered(items)

    def delete(self, subscriber_id: str, scope: Scope, target: object = None) -> bool:
        key = (
            normalize_subscriber_id(subscriber_id),
            scope.value,
            normalize_target(scope, target) or "",
        )
        with self._lock:
            return self._by_key.pop(key, None) is not None

    def snapshot(self) -> PreferenceSet:
        with self._lock:
            return PreferenceSet(self._by_key.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_key)


class JsonlPreferenceStore:
    """以一行一条记录保存偏好的存储。

    写入走「整体重写 + rename」而不是追加：偏好是覆盖写语义，一个订阅者最多
    三条记录，整体重写比「追加 + 定期压缩」简单，也不会在文件里攒下一堆被覆盖
    的旧行。读取之前按文件大小与修改时刻判断是否被别的进程写过，变了就重扫，
    避免在共享卷上拿到上一轮的视图。
    """

    def __init__(self, path: str | os.PathLike[str] = DEFAULT_PREFERENCES_PATH) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._by_key: dict[tuple[str, str, str], Preference] = {}
        self._stamp: Optional[tuple[int, int]] = None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._reload()

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
        records: dict[tuple[str, str, str], Preference] = {}
        if self._path.exists():
            with self._path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("偏好文件存在无法解析的行，已跳过：%s", self._path)
                        continue
                    preference = preference_from_row(raw)
                    if preference is not None:
                        records[preference.key] = preference
        self._by_key = records
        self._stamp = self._file_stamp()

    def _ensure_fresh(self) -> None:
        if self._file_stamp() != self._stamp:
            self._reload()

    def _rewrite(self) -> None:
        payload = "".join(
            json.dumps(preference_to_row(item), ensure_ascii=False) + "\n"
            for item in _ordered(self._by_key.values())
        )
        fd, tmp_name = tempfile.mkstemp(
            prefix=".preferences-", suffix=".tmp", dir=str(self._path.parent)
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

    def upsert(self, preference: Preference) -> Preference:
        with self._lock:
            self._ensure_fresh()
            self._by_key[preference.key] = preference
            self._rewrite()
            return preference

    def get(
        self, subscriber_id: str, scope: Scope, target: object = None
    ) -> Optional[Preference]:
        key = (
            normalize_subscriber_id(subscriber_id),
            scope.value,
            normalize_target(scope, target) or "",
        )
        with self._lock:
            self._ensure_fresh()
            return self._by_key.get(key)

    def list(
        self, *, subscriber_id: Optional[str] = None, scope: Optional[Scope] = None
    ) -> list[Preference]:
        with self._lock:
            self._ensure_fresh()
            items = list(self._by_key.values())
        if subscriber_id is not None:
            wanted = normalize_subscriber_id(subscriber_id)
            items = [item for item in items if item.subscriber_id == wanted]
        if scope is not None:
            items = [item for item in items if item.scope is scope]
        return _ordered(items)

    def delete(self, subscriber_id: str, scope: Scope, target: object = None) -> bool:
        key = (
            normalize_subscriber_id(subscriber_id),
            scope.value,
            normalize_target(scope, target) or "",
        )
        with self._lock:
            self._ensure_fresh()
            if key not in self._by_key:
                return False
            del self._by_key[key]
            self._rewrite()
            return True

    def snapshot(self) -> PreferenceSet:
        with self._lock:
            self._ensure_fresh()
            return PreferenceSet(self._by_key.values())

    def __len__(self) -> int:
        with self._lock:
            self._ensure_fresh()
            return len(self._by_key)


def create_preference_store(
    backend: str = "jsonl", path: str | os.PathLike[str] | None = None
) -> PreferenceStore:
    """按配置创建偏好存储；``memory`` 用于测试与临时场景。"""
    if backend == "memory":
        return MemoryPreferenceStore()
    target = Path(path) if path else Path(
        os.getenv("XW_PREFERENCES_FILE") or DEFAULT_PREFERENCES_PATH
    )
    logger.info("偏好存储后端：jsonl（%s）", target)
    return JsonlPreferenceStore(target)


_store: Optional[PreferenceStore] = None
_tenant_stores: dict[str, PreferenceStore] = {}
_store_lock = threading.Lock()


def get_preference_store(tenant: Optional[str] = None) -> PreferenceStore:
    """获取偏好存储（按租户隔离，惰性初始化）。"""
    from . import tenancy

    global _store

    key = tenancy.normalize_tenant(tenant) if tenant else tenancy.current_tenant()
    with _store_lock:
        if key == tenancy.DEFAULT_TENANT:
            if _store is None:
                base = Path(os.getenv("XW_PREFERENCES_FILE") or DEFAULT_PREFERENCES_PATH)
                _store = create_preference_store("jsonl", tenancy.scoped_path(base, key))
            return _store
        store = _tenant_stores.get(key)
        if store is None:
            base = Path(os.getenv("XW_PREFERENCES_FILE") or DEFAULT_PREFERENCES_PATH)
            store = create_preference_store("jsonl", tenancy.scoped_path(base, key))
            _tenant_stores[key] = store
        return store


def set_preference_store(
    store: Optional[PreferenceStore], tenant: Optional[str] = None
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
    "DEFAULT_PREFERENCES_PATH",
    "MAX_SUBSCRIBER_ID_LENGTH",
    "MAX_WORKFLOW_LENGTH",
    "SPECIFICITY",
    "JsonlPreferenceStore",
    "MemoryPreferenceStore",
    "Preference",
    "PreferenceDecision",
    "PreferenceSet",
    "PreferenceStore",
    "Scope",
    "create_preference_store",
    "get_preference_store",
    "normalize_subscriber_id",
    "normalize_target",
    "preference_from_row",
    "preference_to_row",
    "set_preference_store",
]
