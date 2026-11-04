"""重试约定：哪种失败可以再发一次、隔多久发、什么情况下绝不重发。

重试在通知系统里不是「多试几次更稳」，而是一个会**制造重复通知**的动作：
触发接口一旦入队成功，重发就是第二条站内信。因此这里把两件事分开：

1. **这次失败值不值得重发**（:func:`should_retry`）。只有「没拿到服务端结论」
   与「服务端暂时不行」两类算数——连接失败、超时、读响应断流（
   :class:`~xw_notify.errors.TransportError`），以及 408 / 425 / 429 / 5xx
   （服务端忙或过载）。参数错误、鉴权失败、资源不存在、响应形状不对，重发
   只会得到同样的答案，把它们重试三遍只是把一次失败变慢三倍。
2. **这次请求重发安全不安全**（:func:`is_replay_safe`）。GET / HEAD / OPTIONS /
   PUT / DELETE 天然幂等；POST 只有在带 ``Idempotency-Key`` 时才安全——服务端
   按这个键去重，重复提交返回同一份回执。没有幂等键的 POST 一律不自动重发，
   哪怕调用方打开了重试策略。

两者同时成立才动手。默认策略是关闭的（``max_attempts=1``）：SDK 不替调用方
决定「评论通知可以发两遍」，需要重试的地方显式传 ``retry=RetryPolicy(...)``，
并给写操作带上幂等键。

等待时长用指数退避加向下抖动：第 n 次失败后等 ``base_delay * 2**(n-1)``，
按 ``jitter`` 比例随机下压，封顶 ``max_delay``。向下抖动（而不是上下浮动）
保证等待不会超过上限，也不会出现「第一次就等得比基准还久」的观感。
服务端给了 ``Retry-After`` 时以它为准——它知道自己的负载什么时候能缓过来，
但这个值封顶在 :data:`MAX_RETRY_AFTER`，避免一个异常响应把调用方挂住一小时。
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Mapping, Optional

from .errors import APIError, TransportError, XWNotifyError

#: 值得重发的状态码：请求超时、Too Early、限流，以及服务端过载类 5xx。
#: 501（未实现）与 505（协议版本不支持）不在此列：它们不会因为再试一次而变好。
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

#: 天然幂等的方法。POST 不在此列——它在通知系统里代表「新增一次投递」。
REPLAY_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})

#: 服务端 ``Retry-After`` 的最大采纳值（秒）。超过这个数就不等了，直接报错
#: 交给调用方决定，免得多半已经没在等的请求把线程按住。
MAX_RETRY_AFTER = 60.0


def should_retry(exc: BaseException) -> bool:
    """报告这次失败是否值得重发。

    只回答「失败本身」的语义，不判断请求是否可重放——那件事交给
    :func:`is_replay_safe` 与 :func:`run_with_retry`。
    """
    if isinstance(exc, APIError):
        return exc.status in RETRYABLE_STATUS
    # 传输层失败：请求没拿到结论，服务端可能处理了也可能没有，重发要看请求
    # 本身是否可重放。协议错误说明版本对不上，重发无用。
    return isinstance(exc, TransportError)


def is_replay_safe(method: str, headers: Optional[Mapping[str, str]] = None) -> bool:
    """报告这个请求重发一次会不会产生额外副作用。"""
    if str(method or "").upper() in REPLAY_SAFE_METHODS:
        return True
    for key, value in (headers or {}).items():
        if str(key).lower() == "idempotency-key" and str(value or "").strip():
            return True
    return False


def header_value(headers: Mapping[str, str], name: str) -> Optional[str]:
    """按名称取响应头（HTTP 头大小写不敏感，字典不一定）。"""
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            return str(value)
    return None


def retry_after_seconds(headers: Optional[Mapping[str, str]]) -> Optional[float]:
    """从响应头里读出建议的等待秒数。

    两种写法服务端都允许：整数秒（``Retry-After: 5``），或 HTTP 日期
    （``Retry-After: Wed, 21 Oct 2026 07:28:00 GMT``）。解析不出、是负数、
    或超过上限都按「没有这个头」处理——一个坏值不该让重试直接跳过等待。
    """
    if not headers:
        return None
    raw = header_value(headers, "Retry-After")
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            moment = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if moment is None:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        seconds = (moment - datetime.now(timezone.utc)).total_seconds()
    if seconds < 0:
        return None
    return min(seconds, MAX_RETRY_AFTER)


@dataclass(frozen=True)
class RetryPolicy:
    """重试策略。

    :param max_attempts: 总尝试次数（含首次），``1`` 表示关闭重试。
    :param base_delay: 第一次退避的基准秒数。
    :param max_delay: 指数退避的封顶秒数。
    :param jitter: 向下抖动比例（0–1）：实际等待落在 ``[w*(1-jitter), w]``。
    :param honor_retry_after: 是否采纳服务端给出的 ``Retry-After``。
    """

    max_attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0
    jitter: float = 0.3
    honor_retry_after: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts 至少为 1（1 表示不重试）")
        if self.base_delay < 0:
            raise ValueError("base_delay 不能为负")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay 不应小于 base_delay，否则退避曲线没有意义")
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError("jitter 需要落在 0 到 1 之间")

    @classmethod
    def disabled(cls) -> "RetryPolicy":
        """关闭重试的策略，等同 SDK 的默认行为。"""
        return cls(max_attempts=1)

    @property
    def enabled(self) -> bool:
        return self.max_attempts > 1

    def delay_for(
        self,
        attempt: int,
        *,
        retry_after: Optional[float] = None,
        rand: Optional[Callable[[], float]] = None,
    ) -> float:
        """第 ``attempt`` 次失败之后等多久（``attempt`` 从 1 开始）。"""
        exponent = min(self.max_delay, self.base_delay * (2 ** max(0, attempt - 1)))
        if self.jitter > 0:
            draw = rand() if rand is not None else random.random()
            exponent *= 1.0 - self.jitter * draw
        if self.honor_retry_after and retry_after is not None:
            # 服务端明确说了要等多久，就以它为准；它知道自己的负载。上界仍是
            # MAX_RETRY_AFTER，超过就当作「暂时别发了」，由调用方决定下一步。
            exponent = max(exponent, min(retry_after, MAX_RETRY_AFTER))
        return max(0.0, exponent)


def run_with_retry(
    operation: Callable[[], object],
    policy: Optional[RetryPolicy],
    *,
    replay_safe: bool = True,
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Optional[Callable[[BaseException, int, float], None]] = None,
    rand: Optional[Callable[[], float]] = None,
) -> object:
    """执行 ``operation``，按策略决定是否再试。

    ``operation`` 每次调用都要完整地发一次请求（同一个请求对象重复发送即可，
    它不可变）；抛出的异常若不是「可重试 + 可重放」，原样上抛。

    :param policy: 策略；``None`` 等同 :meth:`RetryPolicy.disabled`。
    :param replay_safe: 这次请求重发是否安全；不安全时只尝试一次。
    :param sleep: 等待函数，测试里换成记录器即可不真的等。
    :param on_retry: 每次准备重发前的回调，参数为（异常, 已失败的次数, 等待秒数），
        便于调用方把重试写进自己的日志或指标。
    """
    if policy is None or not policy.enabled or not replay_safe:
        return operation()

    attempt = 1
    while True:
        try:
            return operation()
        except XWNotifyError as exc:
            if attempt >= policy.max_attempts or not should_retry(exc):
                raise
            delay = policy.delay_for(
                attempt,
                retry_after=getattr(exc, "retry_after", None),
                rand=rand,
            )
            if on_retry is not None:
                on_retry(exc, attempt, delay)
            sleep(delay)
            attempt += 1


__all__ = [
    "MAX_RETRY_AFTER",
    "REPLAY_SAFE_METHODS",
    "RETRYABLE_STATUS",
    "RetryPolicy",
    "header_value",
    "is_replay_safe",
    "retry_after_seconds",
    "run_with_retry",
    "should_retry",
]
