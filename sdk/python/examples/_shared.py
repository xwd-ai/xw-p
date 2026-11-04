"""示例共用的小工具：装配客户端、统一输出、幂等建订阅者。

示例反复运行是常态（改完代码再跑一遍），所以订阅者不能建一次就 409 一次；
``ensure_subscriber`` 把这个差异收在一处，示例主体只关心互动流程。
"""
from __future__ import annotations

import os
import sys
from typing import Mapping, Optional, Sequence, TextIO, Tuple

from xw_notify import Client, ConflictError, Subscriber
from xw_notify.client import DEFAULT_BASE_URL, DEFAULT_ENVIRONMENT

#: 指向部署地址；不设置时用 SDK 的默认值（本机 8000 端口）。
BASE_URL_ENV = "XW_BASE_URL"
#: 该环境的 API Key；服务端未配置 Key 的本地环境可以不设。
API_KEY_ENV = "XW_API_KEY"
#: 隔离环境，``development`` / ``production``。
ENVIRONMENT_ENV = "XW_ENVIRONMENT"


def client_from_env(environ: Optional[Mapping[str, str]] = None) -> Client:
    """按环境变量装配客户端。

    环境标识始终显式传给 SDK，不依赖服务端默认值——读的是一套数据、以为在看
    另一套，是这类接口最常见的事故。
    """
    source = os.environ if environ is None else environ

    def read(name: str) -> str:
        return (source.get(name) or "").strip()

    return Client(
        read(BASE_URL_ENV) or DEFAULT_BASE_URL,
        api_key=read(API_KEY_ENV) or None,
        environment=read(ENVIRONMENT_ENV) or DEFAULT_ENVIRONMENT,
    )


def stream(out: Optional[TextIO]) -> TextIO:
    """输出流；``None`` 落到标准输出。"""
    return sys.stdout if out is None else out


def emit(out: Optional[TextIO], text: str = "") -> None:
    """打印一行。所有示例共用同一个出口，输出顺序与内容都能被断言。"""
    print(text, file=stream(out))


def ensure_subscriber(
    client: Client,
    subscriber_id: str,
    *,
    email: Optional[str] = None,
    channels: Optional[Sequence[str]] = None,
) -> Tuple[Subscriber, bool]:
    """取订阅者，没有就建一个，返回 ``(订阅者, 是否新建)``。

    409 在这里不是错误：标识已被占用说明上一次运行留下的档案还在，读回来
    继续用即可。真正需要处理的冲突（同一个邮箱被别的订阅者占了）也会是 409，
    但它只会发生在标识不同、地址重复的场景，那时读回自己这条会 404，异常照常
    抛给调用方，不会被这里悄悄吞掉。
    """
    try:
        created = client.subscribers.create(
            subscriber_id=subscriber_id,
            email=email,
            channels=channels,
        )
        return created, True
    except ConflictError:
        return client.subscribers.get(subscriber_id), False
