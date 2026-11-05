"""进程内指标与 Prometheus 文本暴露格式。

指标面很窄——HTTP 请求量与耗时、事件受理、幂等命中、限流拒绝、队列深度。
抓取端只要求文本格式（exposition format 0.0.4），因此这里自己实现最小子集
（counter / gauge / histogram），而不是引入一个客户端库：库会把「名字后缀
怎么拼、默认 registry 何时收集、进程内还藏了哪些指标」一并带进来，排查时
反而要多解释一层。自己维护的代价是要把转义、排序、累积桶这些细节写对，
这些都有测试盯住。

几条固定口径：

- **标签值必须转义**。反斜杠、双引号、换行会破坏文本格式；渠道与工作流名
  来自配置或请求，不能假定它们干净。
- **输出顺序稳定**。metric family 按名字排序，同一 family 内的序列按标签
  值排序；否则每次抓取的行序都在变，diff 与快照测试都无从下手。
- **渲染不抛异常**。抓取失败等于监控断流，比少一个指标严重得多；单个
  family 出问题就跳过它并在输出里留一行注释。
- **进程内计数，重启归零**。跨实例与跨重启的累计交给抓取端做 `sum by`，
  这里不假装自己是一份持久账本。
"""
from __future__ import annotations

import math
import threading
from typing import Callable, Iterable, Mapping, Sequence

#: 内容类型，抓取端按它选择解析器。
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

#: HTTP 耗时桶（秒）。覆盖本机亚毫秒响应到慢存储的秒级尾延迟；
#: 超过 10 秒的值进 +Inf 桶，由 `_count - sum(le)` 得到。
DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (
    0.001,
    0.002,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


def _escape(value: str) -> str:
    """转义标签值：反斜杠、双引号与换行是文本格式里唯三需要处理的字符。"""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _fmt(value: float) -> str:
    """把数值写成 Prometheus 认的形态。

    整数不带小数点、无穷写成 ``+Inf``（桶上界必须这么写），其余用最短的
    可往返表示。``nan`` 理论上不会出现，真出现了也照实输出，让抓取端自己
    判定，而不是悄悄改成 0。
    """
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if math.isnan(value):
        return "NaN"
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


def _labels_text(names: Sequence[str], values: Mapping[str, str]) -> str:
    if not names:
        return ""
    parts = [f'{name}="{_escape(str(values[name]))}"' for name in names]
    return "{" + ",".join(parts) + "}"


class _Family:
    """一组同名、同标签集合的序列。"""

    kind = "untyped"

    def __init__(self, name: str, help_text: str, label_names: Sequence[str]) -> None:
        if not name:
            raise ValueError("指标名不能为空")
        self.name = name
        self.help = help_text
        self.label_names = tuple(label_names)
        self._lock = threading.Lock()

    def _check(self, labels: Mapping[str, str]) -> tuple[str, ...]:
        """校验标签集合，返回按声明顺序排列的值元组。

        多一个少一个都是编码错误：多传的标签会被静默丢掉、少传的会变成空串，
        两种都会让同一份看板在改动后悄悄错位，所以在写入时就报错。
        """
        if set(labels) != set(self.label_names):
            raise ValueError(
                f"指标 {self.name} 的标签应为 {sorted(self.label_names)}，实际 {sorted(labels)}"
            )
        return tuple(str(labels[name]) for name in self.label_names)

    def _series(self) -> list[tuple[tuple[str, ...], object]]:
        raise NotImplementedError


class Counter(_Family):
    """只增不减的累计量。"""

    kind = "counter"

    def __init__(self, name, help_text, label_names=()) -> None:
        super().__init__(name, help_text, label_names)
        self._values: dict[tuple[str, ...], float] = {}

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        key = self._check(labels)
        if amount < 0:
            raise ValueError("counter 不能减少")
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + float(amount)

    def value(self, **labels: str) -> float:
        key = self._check(labels)
        with self._lock:
            return self._values.get(key, 0.0)

    def _series(self):
        with self._lock:
            return sorted(self._values.items())


class Gauge(_Family):
    """可增可减的瞬时值；也可以挂一个取值函数，在抓取时现算。"""

    kind = "gauge"

    def __init__(self, name, help_text, label_names=()) -> None:
        super().__init__(name, help_text, label_names)
        self._values: dict[tuple[str, ...], float] = {}
        self._source: Callable[[], Iterable[tuple[dict[str, str], float]]] | None = None

    def set(self, value: float, **labels: str) -> None:
        key = self._check(labels)
        with self._lock:
            self._values[key] = float(value)

    def add(self, amount: float, **labels: str) -> None:
        key = self._check(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + float(amount)

    def value(self, **labels: str) -> float:
        key = self._check(labels)
        with self._lock:
            return self._values.get(key, 0.0)

    def source(self, fn: Callable[[], Iterable[tuple[dict[str, str], float]]]) -> None:
        """挂一个抓取时调用的取值函数（例如队列深度）。

        函数抛异常时这一族指标整体跳过：查询外部依赖是我方行为，不能因为
        Redis 暂时不可用就让整个抓取返回 500。
        """
        self._source = fn

    def _series(self):
        with self._lock:
            rows = list(self._values.items())
        if self._source is not None:
            for labels, value in self._source():
                rows.append((self._check(labels), float(value)))
        return sorted(rows)


class Histogram(_Family):
    """定桶直方图；桶是累积的，输出时逐桶累加。"""

    kind = "histogram"

    def __init__(self, name, help_text, label_names=(), buckets=DEFAULT_DURATION_BUCKETS) -> None:
        super().__init__(name, help_text, label_names)
        clean = sorted({float(b) for b in buckets if b > 0 and math.isfinite(b)})
        if not clean:
            raise ValueError("直方图至少需要一个正的桶上界")
        self._bounds = tuple(clean)
        self._counts: dict[tuple[str, ...], list[int]] = {}
        self._sums: dict[tuple[str, ...], float] = {}
        self._totals: dict[tuple[str, ...], int] = {}

    @property
    def bounds(self) -> tuple[float, ...]:
        return self._bounds

    def observe(self, value: float, **labels: str) -> None:
        key = self._check(labels)
        if math.isnan(value) or value < 0:
            # 负耗时只可能来自时钟回拨或调用方算错，记 0 而不是让分位数变形。
            value = 0.0
        index = 0
        while index < len(self._bounds) and value > self._bounds[index]:
            index += 1
        with self._lock:
            counts = self._counts.get(key)
            if counts is None:
                counts = [0] * (len(self._bounds) + 1)
                self._counts[key] = counts
            counts[index] += 1
            self._sums[key] = self._sums.get(key, 0.0) + float(value)
            self._totals[key] = self._totals.get(key, 0) + 1

    def count(self, **labels: str) -> int:
        key = self._check(labels)
        with self._lock:
            return self._totals.get(key, 0)

    def _series(self):
        with self._lock:
            keys = sorted(set(self._counts))
            return [
                (
                    key,
                    (
                        list(self._counts[key]),
                        self._sums.get(key, 0.0),
                        self._totals.get(key, 0),
                    ),
                )
                for key in keys
            ]


class Registry:
    """指标集合：负责创建、复用与渲染。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._families: dict[str, _Family] = {}

    def _get(self, family_cls, name: str, help_text: str, labels, **kwargs):
        with self._lock:
            existing = self._families.get(name)
            if existing is not None:
                # 同名重复注册多半是模块被导入两次或两处各自建了一份；返回
                # 既有实例，让计数汇到同一处，而不是分成两份谁也看不全。
                return existing
            family = family_cls(name, help_text, labels, **kwargs)
            self._families[name] = family
            return family

    def counter(self, name: str, help_text: str, labels: Sequence[str] = ()) -> Counter:
        return self._get(Counter, name, help_text, labels)

    def gauge(self, name: str, help_text: str, labels: Sequence[str] = ()) -> Gauge:
        return self._get(Gauge, name, help_text, labels)

    def histogram(
        self,
        name: str,
        help_text: str,
        labels: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_DURATION_BUCKETS,
    ) -> Histogram:
        return self._get(Histogram, name, help_text, labels, buckets=buckets)

    def render(self) -> str:
        """按文本格式输出全部指标。"""
        with self._lock:
            families = [self._families[name] for name in sorted(self._families)]
        lines: list[str] = []
        for family in families:
            try:
                rows = list(family._series())
            except Exception:  # noqa: BLE001 - 单族失败不能拖垮整次抓取
                lines.append(f"# 跳过 {family.name}：取值失败")
                continue
            lines.append(f"# HELP {family.name} {_escape_help(family.help)}")
            lines.append(f"# TYPE {family.name} {family.kind}")
            for key, payload in rows:
                labels = dict(zip(family.label_names, key))
                if isinstance(family, Histogram):
                    lines.extend(_histogram_lines(family, labels, payload))
                else:
                    lines.append(f"{family.name}{_labels_text(family.label_names, labels)} {_fmt(payload)}")
        return "\n".join(lines) + "\n"


def _escape_help(text: str) -> str:
    """HELP 行只允许反斜杠转义与换行——引号在这里没有特殊含义。"""
    return str(text).replace("\\", "\\\\").replace("\n", "\\n")


def _histogram_lines(family: Histogram, labels: dict[str, str], payload) -> list[str]:
    counts, total, count = payload
    lines: list[str] = []
    cumulative = 0
    for bound, hits in zip(family.bounds, counts):
        cumulative += hits
        with_le = dict(labels)
        with_le["le"] = _fmt(bound)
        names = list(family.label_names) + ["le"]
        lines.append(f"{family.name}_bucket{_labels_text(names, with_le)} {cumulative}")
    with_inf = dict(labels)
    with_inf["le"] = "+Inf"
    names = list(family.label_names) + ["le"]
    lines.append(f"{family.name}_bucket{_labels_text(names, with_inf)} {count}")
    lines.append(f"{family.name}_sum{_labels_text(family.label_names, labels)} {_fmt(total)}")
    lines.append(f"{family.name}_count{_labels_text(family.label_names, labels)} {count}")
    return lines


_registry: Registry | None = None
_registry_lock = threading.Lock()


def get_registry() -> Registry:
    """返回进程级指标集合（惰性初始化）。"""
    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = Registry()
        return _registry
