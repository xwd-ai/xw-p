"""OpenAPI 文档装配。

FastAPI 自动生成的 schema 只覆盖「参数类型 + 响应模型」这一层，离一份能给
别人看的接口文档还差几件事：分组的说明、鉴权方式、统一的错误响应、稳定
的操作标识，以及一个可以纳入版本管理的导出文件。这些都在这一处收口，
避免每个路由文件各写一套。

设计取舍：

- **操作标识由「方法 + 路径」推导**，而不是 FastAPI 默认的
  ``函数名_路径_方法``：后者把函数名泄露给调用方，改名一个内部函数就会让
  生成的 SDK 方法名变化；路径是接口契约的一部分，按它推导更稳。路径参数
  写成 ``By<Name>``，例如 ``GET /v1/admin/activity/{trigger_id}`` 得到
  ``getV1AdminActivityByTriggerId``。
- **API Key 是可选的**，所以 ``security`` 用「带 Key」与「空对象」两种备选
  并列（OpenAPI 里空对象表示匿名可用）。只写前者会让文档读者以为不带 Key
  一定失败，而实际上只有配置了 Key 的环境才校验。
- **统一错误响应只加在确实会返回它的接口上**：所有 ``/v1`` 接口都解析
  ``X-Environment`` 与 ``X-Tenant-Id``，环境头或租户写错是 400、Key 不匹配是
  401、拿别的租户的 Key 越权是 403；健康检查、渠道配置与投递汇总不看租户，
  给它们挂 401 只会误导。
- **429 按路径前缀声明**：限流中间件拦的是 ``/v1`` 下的所有接口（包括上面那些
  部署级接口），因此它跟着路径走、跟着环境头走的规则无关。是否真的会被拦取决于
  部署期配置，这里无条件写进契约——按当前配置决定要不要声明，会让同一份代码在
  不同环境里导出两份契约，diff 与 SDK 生成都没法用。
- 导出结果按路径排序、JSON 键排序，同一份代码重复导出的字节一致，diff 里
  才看得出真实改动。
"""
from __future__ import annotations

import os
import re
from copy import deepcopy
from typing import Any, Iterable

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

#: 文档标题与版本。版本跟随 API 契约，而不是代码发布节奏。
API_TITLE = "xw-notify API"
API_VERSION = "0.1.0"

#: 部署地址覆盖：同源相对路径适用于经反向代理暴露的部署，独立域名部署时
#: 用这个环境变量给出完整基地址，Swagger UI 的「Try it out」才不会打到本地。
SERVER_URL_ENV = "XW_OPENAPI_SERVER_URL"

#: 接口分组的展示顺序与说明。顺序按「先读、再写、最后运维」排，与管理后台
#: 的信息层次一致；未在此声明的分组会在装配时被指出来（见测试）。
TAGS: list[dict[str, str]] = [
    {
        "name": "auth",
        "description": (
            "后台登录：用管理员账号口令换取 JWT，以及查询当前令牌的身份与有效期。"
            "令牌绑定一个租户与一组可操作的环境，之后每次请求的 X-Environment 必须在其中。"
        ),
    },
    {
        "name": "system",
        "description": "健康检查与服务状态。供负载均衡与部署脚本探测，不读环境头。",
    },
    {
        "name": "trigger",
        "description": (
            "互动事件触发。把点赞、评论、@提及等事件按名称匹配工作流模板，"
            "分发到订阅者所在的多个渠道。单条与批量两个入口共用同一套校验与幂等语义。"
        ),
    },
    {
        "name": "workflows",
        "description": (
            "工作流模板：步骤序列、聚合窗口、渠道与编排步骤。"
            "模板支持启停与复制，启停不改变版本号，副本默认不启用。"
        ),
    },
    {
        "name": "activity",
        "description": "活动日志：每次触发一条记录，含声明的渠道与受理状态，按时间倒序翻页。",
    },
    {
        "name": "execution",
        "description": (
            "执行明细：一次触发 × 一个渠道一条记录，含实际投递的实现、尝试链路、"
            "耗时与失败/跳过原因。"
        ),
    },
    {
        "name": "inbox",
        "description": (
            "站内信收件箱：列表、未读聚合、标记已读与 SSE 实时推送。"
            "同一张聚合卡片无论被多少次互动改写都只算一条消息。"
        ),
    },
    {
        "name": "subscribers",
        "description": (
            "订阅者档案：渠道绑定与收件地址。邮箱按规范存小写、手机号存 E.164，"
            "地址在环境内唯一。"
        ),
    },
    {
        "name": "preferences",
        "description": (
            "发送偏好：全局 / 工作流 / 渠道三级，解析时从最具体的一层往上找、"
            "命中即止，三层都没有记录按接收处理。"
        ),
    },
    {
        "name": "topics",
        "description": "分组与广播：一次事件展开成每位成员一条消息，逐人回执按成员名单顺序对齐。",
    },
    {
        "name": "providers",
        "description": "渠道实现：注册表、健康状态与运行期启停，支持写回配置文件。",
    },
    {
        "name": "dashboard",
        "description": "仪表盘统计：按消息记录聚合的发送量、成功率、渠道分布与七日趋势。",
    },
    {
        "name": "admin",
        "description": (
            "管理视图：按执行明细聚合的统计、活动检索与单次触发下钻。"
            "与仪表盘的区别是数据源换成逐渠道明细，能分清「邮件失败、站内信成功」。"
        ),
    },
]

API_DESCRIPTION = """\
内容社区互动触达服务：点赞、评论、@提及等互动事件触发后，按工作流模板把通知
分发到创作者所在的多个渠道（站内信 / 推送 / 邮件 / 短信）。

## 请求约定

**租户与环境**：两者都从请求头解析——`X-Tenant-Id`（默认 `default`）决定数据
分区，`X-Environment`（默认 `development` / 可选 `production`）决定环境。头写错
或租户不在 `XW_TENANTS` 清单内直接 400，而不是退回默认值——静默落到另一套数据
比报错更难查。所有 `/v1` 接口都解析这两个头；例外是部署级的 `GET /v1/dashboard/stats`
与 `GET /v1/providers`（渠道配置与投递汇总属于整套部署）以及不区分租户的
`GET /health`。

**鉴权**：两种凭据，都解析出「租户 + 环境」，业务接口不区分它们从哪来。

- `X-Api-Key`：服务端调用的长期凭据。配置了 Key 时，请求必须带匹配的 Key；
  未配置任何 Key 的环境放行，便于本地与演示环境直接调用。配置后按 Key 的作用域
  收口：Key 不匹配或不可用于当前环境返回 401，用某个租户的 Key 去访问另一个租户
  返回 403。Key 与作用域用 `XW_API_KEYS` 声明，形如
  `[{"key": "...", "tenant": "acme", "environments": ["production"]}]`。
- `Authorization: Bearer <JWT>`：运营后台的会话凭据。`POST /v1/auth/token` 用
  管理员账号口令换取，有效期由 `XW_JWT_TTL` 决定。两种凭据同时出现时以令牌为准，
  避免令牌过期后请求悄悄降级成 Key 的身份。

运维接口（`/v1/admin/*`）只接受后台令牌或显式声明了 `roles: ["admin"]` 的 Key；
未配置 `XW_JWT_SECRET` 的部署保持 demo 语义，与业务接口同一套判定。

**幂等**：触发类接口接受 `Idempotency-Key` 头。相同键重复提交返回首次回执、
不重复入队；不带该头则每次都是新的一次触发。键的取值由调用方决定，通常用
业务侧的事件标识。

**分页**：列表接口用游标而不是页码。响应里的 `nextCursor` 为空表示没有更多，
把它原样回传即可取下一页；游标编码了上一页最后一条的排序键，翻页期间有新
数据写入也不会重复或漏掉。

**时间**：请求与响应里的时间均为 ISO 8601，带时区；不带时区的入参按 UTC 解释。

## 数据模型里的几个概念

- **渠道**：`in_app` / `email` / `sms` / `push` / `chat` 是可投递渠道；
  `delay` 与 `digest` 是编排步骤，不能作为投递渠道写入偏好或步骤类型。
- **聚合（digest）**：按 `payload.post_id` 一类的聚合键 + 时间窗口把同一篇帖子的
  互动合并成一条卡片，计数在原地累加。聚合由引擎执行，走内存总线时每条互动
  是独立消息。
- **跳过（skipped）**：被订阅者偏好拦下的渠道既没成功也没失败，不计入成功率
  分母；整条消息的渠道全被拦下时消息状态是 `skipped` 而不是 `failed`。

## 文档

`GET /openapi.json` 返回本页对应的 schema，`GET /docs` 是 Swagger UI，
`GET /redoc` 是 ReDoc。仓库里的 `docs/openapi.json` 是同一份内容的导出，
可用 `scripts/export_openapi.py` 重新生成。
"""

#: 鉴权方式。Key 在请求头里，不放在查询串——查询串会进代理日志与浏览器历史。
SECURITY_SCHEMES: dict[str, dict[str, Any]] = {
    "ApiKeyAuth": {
        "type": "apiKey",
        "in": "header",
        "name": "X-Api-Key",
        "description": "环境级 API Key；目标环境未配置 Key 时可省略。",
    },
    "BearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": (
            "后台会话令牌，由 `POST /v1/auth/token` 签发（HS256）。"
            "只在配置了后台密钥的部署里校验；与 `X-Api-Key` 二选一，同时出现时以令牌为准。"
        ),
    },
}

#: 匿名、带 Key、带后台令牌三种备选并列，表示「凭据可选」。OpenAPI 用一个空要求
#: 对象表达「也可以不带凭据」，这是标准写法，不是遗漏。
SECURITY_REQUIREMENTS: list[dict[str, list[str]]] = [
    {"ApiKeyAuth": []},
    {"BearerAuth": []},
    {},
]

#: 运维接口专用：只接受后台令牌或带 admin 角色的 Key，匿名不成立，因此不给
#: 空要求对象。
ADMIN_SECURITY_REQUIREMENTS: list[dict[str, list[str]]] = [
    {"BearerAuth": []},
    {"ApiKeyAuth": []},
]

#: 统一错误响应体。FastAPI 的校验错误是 `detail` 为对象数组，业务错误是字符串，
#: 两种形态都保留，调用方不必按状态码分两套解析。
ERROR_SCHEMA: dict[str, Any] = {
    "type": "object",
    "title": "Error",
    "description": "错误响应。`detail` 为可直接展示的说明；参数校验失败时是逐字段的问题列表。",
    "properties": {
        "detail": {
            "oneOf": [
                {"type": "string"},
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "loc": {"type": "array", "items": {"anyOf": [{"type": "string"}, {"type": "integer"}]}},
                            "msg": {"type": "string"},
                            "type": {"type": "string"},
                        },
                    },
                },
            ]
        }
    },
    "required": ["detail"],
    "examples": [
        {"detail": "未知环境：staging（支持：development, production）"},
        {"detail": [{"loc": ["body", "to", "subscriberId"], "msg": "Field required", "type": "missing"}]},
    ],
}

#: 环境头写错、参数互相冲突时返回 400，是所有 `/v1` 接口的公共失败面。
_BAD_REQUEST = {
    "description": "请求参数不合法或互相冲突（例如未知环境、时间区间倒置）。",
    "content": {
        "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"},
            "example": {"detail": "未知环境：staging（支持：development, production）"},
        }
    },
}

#: 目标环境配置了 Key 而请求没带或带错时返回 401。
_UNAUTHORIZED = {
    "description": "API Key 缺失或与该环境的配置不匹配。",
    "content": {
        "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"},
            "example": {"detail": "API Key 缺失或不匹配"},
        }
    },
}

#: 凭据有效但越权时返回 403：拿 A 租户的 Key 去访问 B 租户。
_FORBIDDEN = {
    "description": "凭据有效，但没有访问目标租户的权限。",
    "content": {
        "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"},
            "example": {"detail": "API Key 属于租户 acme，不能访问租户 other"},
        }
    },
}

#: 触发限流时返回 429（``XW_RATE_LIMIT_*_RPM`` 配置了额度才可能出现）。
#: 这里无条件声明：限流是部署期开关，如果按当前配置决定要不要写进 schema，
#: 同一份代码在不同环境里会导出两份契约，diff 与 SDK 生成都没法用。
_TOO_MANY_REQUESTS = {
    "description": "请求超出该凭据或来源地址的额度（响应头给出剩余额度与建议等待秒数）。",
    "content": {
        "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"},
            "example": {"detail": "请求过于频繁：来源 203.0.113.7 每分钟最多 120 次，请在 1 秒后重试"},
        }
    },
}

#: SSE 端点的响应体是事件流而不是一次性 JSON，示例给一帧让读者知道长什么样。
_SSE_EXAMPLE = (
    "event: ready\n"
    'data: {"unread": 2, "total": 7, "items": [{"message_id": "trg-1:in_app", "title": "小林赞了你的帖子"}]}\n'
    "\n"
    "event: inbox\n"
    'data: {"unread": 3, "total": 8, "items": []}\n'
    "\n"
)

_METHOD_ORDER = ("get", "post", "put", "patch", "delete", "head", "options")

_WORD_SPLIT = re.compile(r"[^0-9A-Za-z]+")


def _pascal(text: str) -> str:
    """把路径片段转成标识里的一段：``workflow_id`` → ``WorkflowId``。"""
    words = [w for w in _WORD_SPLIT.split(text) if w]
    return "".join(w[:1].upper() + w[1:] for w in words)


def operation_id_for(method: str, path: str) -> str:
    """由方法与路径推导操作标识。

    路径是契约的一部分，函数名不是：重构内部实现时不应该让调用方生成的 SDK
    方法名跟着变。路径参数写成 ``By<Name>``，读起来接近自然语序。
    """
    parts: list[str] = []
    for segment in path.strip("/").split("/"):
        if not segment:
            continue
        if segment.startswith("{") and segment.endswith("}"):
            parts.append("By" + _pascal(segment[1:-1]))
        else:
            parts.append(_pascal(segment))
    return method.strip().lower() + "".join(parts)


def servers() -> list[dict[str, str]]:
    """部署地址：默认同源相对路径，独立域名部署时读环境变量。"""
    raw = os.environ.get(SERVER_URL_ENV, "").strip()
    if raw:
        return [{"url": raw.rstrip("/"), "description": "部署地址"}]
    return [{"url": "/", "description": "当前部署（同源相对路径）"}]


def _has_header(operation: dict[str, Any], name: str) -> bool:
    for param in operation.get("parameters", []):
        if param.get("in") == "header" and str(param.get("name", "")).lower() == name.lower():
            return True
    return False


def _path_params(path: str) -> list[str]:
    return re.findall(r"\{([^{}]+)\}", path)


def _fill_error_body(responses: dict[str, Any], code: str, template: dict[str, Any]) -> None:
    """给已声明的错误响应补上统一响应体，已有的描述保持不动。

    路由里写 ``responses={400: {"description": "配置不合法"}}`` 是常见的做法：
    说明比通用文案准确，但只写描述会让生成出来的文档少一个可解析的响应形状。
    """
    existing = responses.get(code)
    if not isinstance(existing, dict):
        return
    existing.setdefault("content", deepcopy(template["content"]))


def _decorate_operations(schema: dict[str, Any]) -> None:
    """补齐操作标识、公共错误响应与事件流响应类型。"""
    paths: dict[str, Any] = schema.setdefault("paths", {})
    declared_tags = {tag["name"] for tag in TAGS}
    for path in list(paths):
        item = paths[path]
        for method in list(item):
            if method not in _METHOD_ORDER:
                continue
            operation = item[method]
            operation["operationId"] = operation_id_for(method, path)

            # 分组缺失时归到 system，并让测试把这种疏漏暴露出来；这里只兜底，
            # 不作为「可以不打标签」的依据。
            tags = [t for t in operation.get("tags", []) if t in declared_tags]
            operation["tags"] = tags or ["system"]

            responses = operation.setdefault("responses", {})
            if _has_header(operation, "X-Environment"):
                responses.setdefault("400", deepcopy(_BAD_REQUEST))
                responses.setdefault("401", deepcopy(_UNAUTHORIZED))
            if _has_header(operation, "X-Tenant-Id"):
                responses.setdefault("403", deepcopy(_FORBIDDEN))
            # 限流中间件按路径前缀生效，与是否读环境头无关：/v1 下的接口都可能被拦。
            if path.startswith("/v1"):
                responses.setdefault("429", deepcopy(_TOO_MANY_REQUESTS))

            # 运维接口不给匿名备选：它们要么要后台令牌，要么要带 admin 角色的 Key。
            # 登录接口反过来，明确写出「不需要凭据」——否则读者会以为要先有令牌
            # 才能换令牌。
            if path.startswith("/v1/admin"):
                operation["security"] = deepcopy(ADMIN_SECURITY_REQUIREMENTS)
            elif path == "/v1/auth/token":
                operation["security"] = []
            # 路由自己声明过 400 / 401 / 403 时保留它的具体说明，只把缺的响应体补上：
            # 「入参校验失败」与「环境头写错」都是 400，但统一的错误形状一样。
            _fill_error_body(responses, "400", _BAD_REQUEST)
            _fill_error_body(responses, "401", _UNAUTHORIZED)
            _fill_error_body(responses, "403", _FORBIDDEN)

            # 路径参数必须声明为 required，否则生成的 SDK 会把它当成可选。
            declared = {
                p.get("name")
                for p in operation.get("parameters", [])
                if p.get("in") == "path"
            }
            for name in _path_params(path):
                if name not in declared:
                    operation.setdefault("parameters", []).append(
                        {
                            "name": name,
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    )

            # SSE：声明真实的内容类型，Swagger 才不会按 JSON 渲染一个空响应。
            if path.endswith("/stream"):
                ok = operation.get("responses", {}).get("200")
                if isinstance(ok, dict) and "text/event-stream" not in ok.get("content", {}):
                    ok.pop("content", None)
                    ok["content"] = {
                        "text/event-stream": {
                            "schema": {"type": "string"},
                            "example": _SSE_EXAMPLE,
                        }
                    }


def _sort_paths(schema: dict[str, Any]) -> None:
    """按路径排序、按方法固定顺序，导出的 diff 只反映真实改动。"""
    paths = schema.get("paths")
    if not isinstance(paths, dict):
        return
    ordered: dict[str, Any] = {}
    for path in sorted(paths):
        item = paths[path]
        ordered_item: dict[str, Any] = {}
        for method in _METHOD_ORDER:
            if method in item:
                ordered_item[method] = item[method]
        for key, value in item.items():
            if key not in ordered_item:
                ordered_item[key] = value
        ordered[path] = ordered_item
    schema["paths"] = ordered


def build_schema(app: FastAPI) -> dict[str, Any]:
    """生成对外发布的 OpenAPI schema。

    只依赖应用已注册的路由，不读运行时状态，因此同一份代码在任何机器上导出
    的结果一致（除了显式配置的部署地址）。
    """
    schema = get_openapi(
        title=API_TITLE,
        version=API_VERSION,
        description=API_DESCRIPTION,
        routes=app.routes,
        tags=TAGS,
        servers=servers(),
        license_info={"name": "MIT", "identifier": "MIT"},
        external_docs={
            "description": "仓库文档：README 与 SDK 使用指南",
            "url": "https://github.com/xwd-ai/xw-p#readme",
        },
    )

    components = schema.setdefault("components", {})
    components.setdefault("securitySchemes", {}).update(SECURITY_SCHEMES)
    components.setdefault("schemas", {})["Error"] = ERROR_SCHEMA
    schema["security"] = SECURITY_REQUIREMENTS
    schema["tags"] = TAGS

    _decorate_operations(schema)
    _sort_paths(schema)
    return schema


def install(app: FastAPI) -> None:
    """用装配后的 schema 替换 FastAPI 的默认实现。

    保留 ``app.openapi_schema`` 缓存：schema 由路由表决定，进程内不会变，
    每次请求重新生成 44 个操作只是浪费 CPU。
    """

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = build_schema(app)
        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]


def iter_operations(schema: dict[str, Any]) -> Iterable[tuple[str, str, dict[str, Any]]]:
    """遍历 schema 里的全部操作，产出 ``(方法, 路径, 操作对象)``。"""
    for path, item in schema.get("paths", {}).items():
        for method, operation in item.items():
            if method in _METHOD_ORDER:
                yield method, path, operation


__all__ = [
    "API_DESCRIPTION",
    "API_TITLE",
    "API_VERSION",
    "ERROR_SCHEMA",
    "SECURITY_REQUIREMENTS",
    "SECURITY_SCHEMES",
    "SERVER_URL_ENV",
    "TAGS",
    "build_schema",
    "install",
    "iter_operations",
    "operation_id_for",
    "servers",
]
