"""重试约定的测试：判定、退避曲线，以及接到客户端之后的实际请求次数。

重点不在「重试能重试」，而在**边界**：不可重放的 POST 不重试、不可重试的
失败不重试、次数用尽后抛出最后一次的异常、服务端给的 ``Retry-After`` 生效
但不至于把调用方挂住。
"""
from __future__ import annotations

import pytest

from xw_notify import Client, RetryPolicy
from xw_notify._http import Response
from xw_notify.errors import (
    BadRequestError,
    NotFoundError,
    ProtocolError,
    RateLimitError,
    ServerError,
    TransportError,
    ValidationError,
)
from xw_notify.retry import (
    MAX_RETRY_AFTER,
    RETRYABLE_STATUS,
    is_replay_safe,
    retry_after_seconds,
    run_with_retry,
    should_retry,
)

from fakes import FakeTransport, json_response, text_response

ACK = {
    "acknowledged": True,
    "triggerId": "trg-1",
    "transactionId": "txn-1",
    "acknowledgmentId": "trg-1",
}


class Clock:
    """记录每次等待，不真的睡。"""

    def __init__(self) -> None:
        self.waits: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)


class Scripted:
    """按脚本依次返回响应或抛异常，并记录每次请求。"""

    def __init__(self, steps) -> None:
        self.steps = list(steps)
        self.requests = []
        self.calls = 0

    def send(self, request):
        self.requests.append(request)
        step = self.steps[min(self.calls, len(self.steps) - 1)]
        self.calls += 1
        if isinstance(step, BaseException):
            raise step
        return step


def scripted(*steps) -> Scripted:
    return Scripted(steps)


def with_retry_after(response: Response, value: str) -> Response:
    headers = dict(response.headers)
    headers["Retry-After"] = value
    return Response(status=response.status, headers=headers, body=response.body)


def ok_response() -> Response:
    return json_response(200, {"status": "ok", "queued": 0})


# -- 判定 -----------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,expected",
    [
        (TransportError("连接被拒"), True),
        (ServerError(500, "内部错误"), True),
        (ServerError(503, "维护中"), True),
        (RateLimitError(429, "太频繁"), True),
        (BadRequestError(400, "环境未知"), False),
        (NotFoundError(404, "没有这个工作流"), False),
        (ValidationError(422, "字段不合法"), False),
        (ProtocolError("响应不是 JSON"), False),
        (ValueError("本地参数错"), False),
    ],
)
def test_should_retry(exc, expected):
    assert should_retry(exc) is expected


def test_retryable_status_set_is_intentional():
    # 501/505 不重试：它们不会因为再试一次而变好。408/425 属于「这一次没来得及」。
    assert 408 in RETRYABLE_STATUS and 425 in RETRYABLE_STATUS
    assert 501 not in RETRYABLE_STATUS and 505 not in RETRYABLE_STATUS


@pytest.mark.parametrize(
    "method,safe",
    [("GET", True), ("HEAD", True), ("PUT", True), ("DELETE", True), ("POST", False)],
)
def test_replay_safe_methods(method, safe):
    assert is_replay_safe(method) is safe


def test_post_is_replay_safe_only_with_idempotency_key():
    assert is_replay_safe("POST", {"Content-Type": "application/json"}) is False
    assert is_replay_safe("post", {"idempotency-key": "comment-C-8842"}) is True
    # 空值不算：发一个空键出去，服务端多半当成普通请求处理。
    assert is_replay_safe("POST", {"Idempotency-Key": "   "}) is False


# -- 策略与退避 -------------------------------------------------------------


def test_policy_rejects_nonsense():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay=-1)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay=2.0, max_delay=1.0)
    with pytest.raises(ValueError):
        RetryPolicy(jitter=1.5)


def test_disabled_policy_makes_one_attempt():
    policy = RetryPolicy.disabled()
    assert policy.enabled is False
    assert policy.max_attempts == 1


def test_backoff_doubles_and_caps():
    policy = RetryPolicy(max_attempts=5, base_delay=0.2, max_delay=1.0, jitter=0.0)
    assert [policy.delay_for(n) for n in (1, 2, 3, 4, 5)] == [0.2, 0.4, 0.8, 1.0, 1.0]


def test_jitter_only_shortens_the_wait():
    policy = RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=10.0, jitter=0.5)
    assert policy.delay_for(1, rand=lambda: 0.0) == 1.0
    assert policy.delay_for(1, rand=lambda: 1.0) == 0.5
    # 抖动不会把等待推到封顶之上，也不会出现负数。
    assert 0.0 <= policy.delay_for(4, rand=lambda: 1.0) <= 10.0


def test_retry_after_wins_over_backoff_but_is_capped():
    policy = RetryPolicy(base_delay=0.1, max_delay=5.0, jitter=0.0)
    assert policy.delay_for(1, retry_after=2.0) == 2.0
    # 退避本身更长时保留退避：服务端只说了「至少等这么久」的意思。
    assert policy.delay_for(1, retry_after=0.05) == 0.1
    assert policy.delay_for(1, retry_after=10_000.0) == MAX_RETRY_AFTER


def test_retry_after_can_be_ignored():
    policy = RetryPolicy(base_delay=0.2, jitter=0.0, honor_retry_after=False)
    assert policy.delay_for(1, retry_after=30.0) == 0.2


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({}, None),
        ({"Retry-After": ""}, None),
        ({"retry-after": "3"}, 3.0),
        ({"Retry-After": "0"}, 0.0),
        ({"Retry-After": "-5"}, None),
        ({"Retry-After": "abc"}, None),
        ({"Retry-After": "99999"}, MAX_RETRY_AFTER),
    ],
)
def test_retry_after_header_parsing(headers, expected):
    assert retry_after_seconds(headers) == expected


def test_retry_after_accepts_http_date():
    # 已经过去的时刻等于「不用等了」，按「没有这个头」处理。
    past = "Wed, 21 Oct 2026 07:28:00 GMT"
    value = retry_after_seconds({"Retry-After": past})
    assert value is None


# -- run_with_retry --------------------------------------------------------


def test_run_with_retry_returns_first_success():
    calls = {"n": 0}

    def operation():
        calls["n"] += 1
        return "ok"

    clock = Clock()
    assert run_with_retry(operation, RetryPolicy(), sleep=clock) == "ok"
    assert calls["n"] == 1 and clock.waits == []


def test_run_with_retry_stops_at_max_attempts_and_raises_last_error():
    calls = {"n": 0}

    def operation():
        calls["n"] += 1
        raise ServerError(500, f"第 {calls['n']} 次")

    clock = Clock()
    with pytest.raises(ServerError) as info:
        run_with_retry(operation, RetryPolicy(max_attempts=3, jitter=0.0), sleep=clock)
    assert calls["n"] == 3
    assert info.value.message == "第 3 次"
    assert len(clock.waits) == 2


def test_run_with_retry_skips_unreplayable_request():
    calls = {"n": 0}

    def operation():
        calls["n"] += 1
        raise ServerError(503, "维护中")

    clock = Clock()
    with pytest.raises(ServerError):
        run_with_retry(operation, RetryPolicy(max_attempts=3), replay_safe=False, sleep=clock)
    assert calls["n"] == 1 and clock.waits == []


def test_run_with_retry_does_not_retry_permanent_errors():
    calls = {"n": 0}

    def operation():
        calls["n"] += 1
        raise ValidationError(422, "to.email 格式不正确")

    clock = Clock()
    with pytest.raises(ValidationError):
        run_with_retry(operation, RetryPolicy(max_attempts=3), sleep=clock)
    assert calls["n"] == 1 and clock.waits == []


def test_run_with_retry_reports_each_retry():
    seen = []

    def operation():
        raise TransportError("连接被拒")

    with pytest.raises(TransportError):
        run_with_retry(
            operation,
            RetryPolicy(max_attempts=2, base_delay=0.5, jitter=0.0),
            sleep=Clock(),
            on_retry=lambda exc, attempt, delay: seen.append(
                (type(exc).__name__, attempt, delay)
            ),
        )
    assert seen == [("TransportError", 1, 0.5)]


def test_run_with_retry_without_policy_is_single_attempt():
    calls = {"n": 0}

    def operation():
        calls["n"] += 1
        raise TransportError("断网")

    with pytest.raises(TransportError):
        run_with_retry(operation, None)
    assert calls["n"] == 1


# -- 客户端接线 ------------------------------------------------------------


def test_client_retries_get_on_server_error():
    transport = scripted(json_response(503, {"detail": "维护中"}), ok_response())
    clock = Clock()
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(base_delay=0.2, jitter=0.0),
        sleep=clock,
    )
    assert client.health().status == "ok"
    assert transport.calls == 2
    assert clock.waits == [0.2]


def test_client_does_not_retry_post_without_idempotency_key():
    transport = scripted(json_response(503, {"detail": "维护中"}), json_response(200, ACK))
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(max_attempts=3),
        sleep=Clock(),
    )
    with pytest.raises(ServerError):
        client.trigger.send("post_liked", "creator-1001", payload={"post_id": "P-1"})
    assert transport.calls == 1


def test_client_retries_post_with_idempotency_key():
    transport = scripted(
        json_response(503, {"detail": "维护中"}),
        json_response(200, ACK),
    )
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
        sleep=Clock(),
    )
    ack = client.trigger.send(
        "post_commented",
        "creator-1001",
        payload={"post_id": "P-2026-1001"},
        idempotency_key="comment-C-8842",
    )
    assert ack.trigger_id == "trg-1"
    assert transport.calls == 2
    # 两次请求必须逐字节一致：路径、幂等键、请求体都不能变，否则服务端会把
    # 重发当成另一条评论。
    assert transport.requests[0] == transport.requests[1]
    assert transport.requests[1].headers["Idempotency-Key"] == "comment-C-8842"


def test_client_honours_retry_after_from_429():
    transport = scripted(
        with_retry_after(json_response(429, {"detail": "触发太频繁"}), "2"),
        ok_response(),
    )
    clock = Clock()
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(max_attempts=2, base_delay=0.1, jitter=0.0),
        sleep=clock,
    )
    assert client.health().status == "ok"
    assert clock.waits == [2.0]


def test_client_retries_gateway_html_error():
    # 502 是反向代理回的 HTML：状态码本身可重试，退避后重发是合理的。
    transport = scripted(text_response(502, "<html>Bad Gateway</html>"), ok_response())
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
        sleep=Clock(),
    )
    assert client.health().status == "ok"
    assert transport.calls == 2


def test_client_does_not_retry_success_status_with_bad_body():
    # 2xx 但响应体不是 JSON：请求已成功，问题在版本或代理，重发没有意义。
    transport = scripted(text_response(200, "<html>login</html>"), ok_response())
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(max_attempts=3, jitter=0.0),
        sleep=Clock(),
    )
    with pytest.raises(ProtocolError):
        client.health()
    assert transport.calls == 1


def test_client_retries_transport_error():
    transport = scripted(TransportError("连接被重置"), ok_response())
    client = Client(
        "http://api.test",
        transport=transport,
        retry=RetryPolicy(max_attempts=3, jitter=0.0),
        sleep=Clock(),
    )
    assert client.health().status == "ok"
    assert transport.calls == 2


def test_client_records_retry_after_on_error():
    transport = scripted(with_retry_after(json_response(429, {"detail": "触发太频繁"}), "7"))
    client = Client("http://api.test", transport=transport)
    with pytest.raises(RateLimitError) as info:
        client.health()
    assert info.value.retry_after == 7.0


def test_client_without_retry_policy_makes_single_attempt():
    transport = FakeTransport(lambda request: json_response(500, {"detail": "boom"}))
    client = Client("http://api.test", transport=transport)
    with pytest.raises(ServerError):
        client.health()
    assert len(transport.requests) == 1
