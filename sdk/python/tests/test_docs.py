"""校验 ``docs/sdk.md`` 里的示例仍然可执行、仍然指向存在的接口。

文档腐化通常有两种形态，都不报错、只是慢慢失效：

1. 示例里的方法被改名或删除——读者复制粘贴后拿到 ``AttributeError``；
2. 示例之间偷偷有了依赖——单独复制一段就跑不通，而整篇从头读到尾看不出来。

所以这里把每个 Python 代码块**各自放在一个空白命名空间里执行一遍**：跑得通
说明这段代码自身完整，且用到的接口都还在。传输层换成假实现，示例照常从
``Client(...)`` 开始，不依赖一个跑着的服务端。Go 代码块是片段而非完整程序，
不做编译，改为核对它引用的导出符号是否仍在包源码里声明。
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from fakes import FakeTransport
from xw_notify._http import Request, Response

ROOT = Path(__file__).resolve().parents[3]
DOC = ROOT / "docs" / "sdk.md"
GO_PACKAGE = ROOT / "sdk" / "go" / "xwnotify"

FENCE = re.compile(r"^```(\w*)\s*$")
GO_QUALIFIED = re.compile(r"xwnotify\.([A-Z][A-Za-z0-9_]*)")
GO_METHOD_CALL = re.compile(r"\.(With[A-Z][A-Za-z0-9_]*)\(")
REPO_PATH = re.compile(r"\b((?:api|sdk|engine|dashboard|docs|infra|scripts)/[A-Za-z0-9_./-]+)")


# -- Markdown 代码块 ------------------------------------------------------


def code_blocks(language: str) -> list[str]:
    """取出指定语言的围栏代码块；未标注语言或语言不符的块跳过。"""
    if not DOC.is_file():
        raise AssertionError(f"文档不存在：{DOC}")
    blocks: list[str] = []
    current: list[str] | None = None
    current_language: str | None = None
    for line in DOC.read_text(encoding="utf-8").splitlines():
        match = FENCE.match(line)
        if match:
            if current is None:
                current = []
                current_language = (match.group(1) or "").lower()
            else:
                if current_language == language:
                    blocks.append("\n".join(current))
                current = None
                current_language = None
            continue
        if current is not None:
            current.append(line)
    return blocks


# -- 假服务端：按路径给出文档示例需要的响应 -------------------------------


def _json(status: int, payload: object) -> Response:
    return Response(
        status=status,
        headers={"Content-Type": "application/json"},
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def _subscriber() -> dict[str, object]:
    return {
        "id": "creator-1001",
        "email": "creator@example.com",
        "phone": None,
        "device_token": None,
        "channels": ["in_app", "email"],
        "bindings": {"in_app": "creator-1001", "email": "creator@example.com"},
        "environment": "development",
        "created_at": "2026-10-01T09:00:00+00:00",
        "updated_at": "2026-10-01T09:00:00+00:00",
    }


def _preference(scope: str, target: str | None, enabled: bool) -> dict[str, object]:
    return {"scope": scope, "target": target, "enabled": enabled, "updated_at": "2026-10-01T09:00:00+00:00"}


def respond(request: Request) -> Response:
    """按「方法 + 路径」给出响应。

    任何文档示例请求了这里没有约定的路径，都会让用例失败——那说明文档在教
    读者调用一个不存在的接口。
    """
    path = urlsplit(request.url).path
    method = request.method.upper()

    if path == "/v1/events/trigger":
        return _json(
            200,
            {
                "acknowledged": True,
                "triggerId": "trig-1",
                "transactionId": "txn-1",
                "acknowledgmentId": "trig-1",
            },
        )
    if path == "/v1/events/trigger/batch":
        count = len(json.loads(request.body or b"{}").get("events") or [])
        results = [
            {
                "index": index,
                "name": "post_mentioned",
                "subscriberId": "creator-1001",
                "status": "accepted",
                "triggerId": f"trig-{index}",
                "transactionId": f"txn-{index}",
                "acknowledgmentId": f"trig-{index}",
            }
            for index in range(count)
        ]
        return _json(
            200,
            {"acknowledged": True, "accepted": count, "rejected": 0, "results": results},
        )
    if path == "/v1/subscribers" and method == "GET":
        return _json(200, {"items": [_subscriber()], "nextCursor": None})
    if path == "/v1/subscribers":
        return _json(200, _subscriber())
    if path.endswith("/preferences/resolve"):
        return _json(
            200,
            {
                "allowed": False,
                "matched": True,
                "scope": "channel",
                "target": "push",
                "reason": "渠道 push 已关闭",
            },
        )
    if path.endswith("/preferences"):
        if method == "GET":
            return _json(
                200,
                {
                    "subscriber_id": "creator-1001",
                    "items": [
                        _preference("global", None, False),
                        _preference("workflow", "post_commented", True),
                    ],
                    "counts": {"global": 1, "workflow": 1, "channel": 0},
                    "default_allowed": True,
                },
            )
        if method == "PUT":
            return _json(
                200,
                {
                    "created": True,
                    "scope": "channel",
                    "target": "push",
                    "enabled": False,
                    "updated_at": "2026-10-01T09:00:00+00:00",
                },
            )
        if method == "DELETE":
            return _json(200, {"removed": 1})
    if path.startswith("/v1/subscribers"):
        if method == "DELETE":
            return Response(status=204, headers={}, body=b"")
        return _json(200, _subscriber())
    if path == "/v1/inbox" and method == "GET":
        return _json(
            200,
            {
                "items": [
                    {
                        "message_id": "msg-1",
                        "trigger_id": "trig-1",
                        "workflow": "post_liked",
                        "title": "小林 等 3 人赞了你的帖子",
                        "body": "《我的第一支 Vlog》",
                        "created_at": "2026-10-01T09:00:00+00:00",
                        "read": False,
                    }
                ],
                "unread": 1,
                "total": 1,
                "matched": 1,
                "hasMore": False,
                "nextCursor": None,
            },
        )
    if path == "/v1/inbox/read":
        return _json(200, {"updated": 1, "unread": 0})

    raise AssertionError(f"文档示例请求了未约定的路径：{method} {path}")


# -- 用例 -----------------------------------------------------------------


def test_doc_has_expected_sections() -> None:
    text = DOC.read_text(encoding="utf-8")
    for heading in ("## 安装", "## 快速开始", "## 触发互动事件", "## 错误与重试", "## 排查表"):
        assert heading in text, f"文档缺少章节：{heading}"


def test_doc_is_linked_from_readme() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "docs/sdk.md" in readme, "README 没有指向 SDK 指南"


def test_python_blocks_are_self_contained_and_runnable(monkeypatch: pytest.MonkeyPatch) -> None:
    blocks = code_blocks("python")
    # 示例数量是文档结构的一部分：删到只剩一两段说明有人把内容搬走了。
    assert len(blocks) >= 8, f"Python 示例只有 {len(blocks)} 段"

    transport = FakeTransport(respond)
    # 把默认传输层换成假实现：示例照常 new Client(...)，但没有一个请求出网。
    monkeypatch.setattr("xw_notify.client.UrllibTransport", lambda **_: transport)

    for index, block in enumerate(blocks):
        namespace: dict[str, object] = {"__name__": "__docs__"}
        try:
            exec(compile(block, f"<docs/sdk.md python #{index + 1}>", "exec"), namespace)
        except Exception as exc:  # pragma: no cover - 失败时给出可用定位
            raise AssertionError(
                f"第 {index + 1} 段 Python 示例无法独立运行：{type(exc).__name__}: {exc}\n"
                f"---\n{block}\n---"
            ) from exc

    assert transport.requests, "示例一段请求都没发出，假传输层可能没生效"


def test_go_blocks_reference_declared_symbols() -> None:
    blocks = code_blocks("go")
    assert len(blocks) >= 2, f"Go 示例只有 {len(blocks)} 段"

    source = "\n".join(path.read_text(encoding="utf-8") for path in sorted(GO_PACKAGE.glob("*.go")))
    assert source, "没读到 Go 包源码"

    for index, block in enumerate(blocks):
        names = set(GO_QUALIFIED.findall(block)) | set(GO_METHOD_CALL.findall(block))
        assert names, f"第 {index + 1} 段 Go 示例没有引用任何包符号"
        for name in sorted(names):
            patterns = (
                rf"\bfunc\s+(\([^)]*\)\s*)?{name}\(",
                rf"\btype\s+{name}\b",
                rf"\b{name}\s*=",
            )
            if not any(re.search(pattern, source) for pattern in patterns):
                raise AssertionError(f"第 {index + 1} 段 Go 示例引用了不存在的符号：xwnotify.{name}")


def test_referenced_repository_paths_exist() -> None:
    text = DOC.read_text(encoding="utf-8")
    missing = sorted(
        {
            match.group(1).rstrip(".,;:）)、")
            for match in REPO_PATH.finditer(text)
            if not (ROOT / match.group(1).rstrip(".,;:）)、")).exists()
        }
    )
    assert not missing, f"文档引用了不存在的路径：{missing}"
