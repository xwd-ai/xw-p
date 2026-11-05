"""OpenAPI schema 的契约测试。

schema 是给别人看的接口契约：分组、鉴权、错误响应、操作标识缺一项，生成的
SDK 或对接文档就会缺一块。这里把它当作代码来断言——新增接口忘了写摘要、
换了路径却让操作标识漂移、改了模型却忘了重新导出，都会在这里失败。

测试不启动服务：``build_schema`` 只读路由表，不碰存储与队列。
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from app import openapi as docs
from app.main import app
from app.openapi import build_schema, iter_operations, operation_id_for

ROOT = Path(__file__).resolve().parents[2]
EXPORT = ROOT / "docs" / "openapi.json"
SCRIPT = ROOT / "scripts" / "export_openapi.py"

_OPERATION_ID = re.compile(r"^[a-z][A-Za-z0-9]*$")
_METHODS = ("get", "post", "put", "patch", "delete", "head", "options")


@pytest.fixture()
def schema():
    """当前代码生成的 schema，不读导出文件。"""
    return build_schema(app)


def _ops(schema):
    return list(iter_operations(schema))


def _refs(node, found=None):
    """收集 schema 里所有 ``$ref``，用于检查悬空引用。"""
    if found is None:
        found = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                found.append(value)
            else:
                _refs(value, found)
    elif isinstance(node, list):
        for item in node:
            _refs(item, found)
    return found


def _resolve(schema, ref):
    assert ref.startswith("#/"), f"只支持文档内引用：{ref}"
    node = schema
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        assert part in node, f"引用无法解析：{ref}"
        node = node[part]
    return node


def _examples(schema, model):
    return schema["components"]["schemas"][model].get("examples", [])


# --- 文档级信息 ---------------------------------------------------------------


def test_info_is_complete(schema):
    info = schema["info"]
    assert info["title"] == docs.API_TITLE
    assert info["version"] == docs.API_VERSION
    # 描述要能回答「怎么鉴权、怎么翻页、怎么保证不重复」，而不是一句口号。
    for keyword in ("X-Environment", "X-Api-Key", "Idempotency-Key", "nextCursor"):
        assert keyword in info["description"], keyword
    assert schema["openapi"].startswith("3.")


def test_license_declared(schema):
    assert schema["info"]["license"]["identifier"] == "MIT"


def test_external_docs_present(schema):
    external = schema["externalDocs"]
    assert external["url"].startswith("https://")


def test_default_server_is_same_origin(schema):
    assert schema["servers"] == [{"url": "/", "description": "当前部署（同源相对路径）"}]


def test_server_url_env_override(monkeypatch):
    monkeypatch.setenv(docs.SERVER_URL_ENV, "https://notify.example.com/")
    built = build_schema(app)
    assert built["servers"] == [
        {"url": "https://notify.example.com", "description": "部署地址"}
    ]


def test_server_url_blank_falls_back(monkeypatch):
    monkeypatch.setenv(docs.SERVER_URL_ENV, "   ")
    assert build_schema(app)["servers"][0]["url"] == "/"


# --- 分组 ---------------------------------------------------------------------


def test_every_operation_has_declared_tag(schema):
    declared = {tag["name"] for tag in schema["tags"]}
    assert declared == {tag["name"] for tag in docs.TAGS}
    for method, path, operation in _ops(schema):
        assert operation["tags"], f"{method} {path} 没有分组"
        assert set(operation["tags"]) <= declared, f"{method} {path} 用了未声明的分组"


def test_no_unused_tag(schema):
    used = {tag for _, _, operation in _ops(schema) for tag in operation["tags"]}
    declared = {tag["name"] for tag in schema["tags"]}
    assert declared - used == set()


def test_tag_descriptions_are_substantial(schema):
    for tag in schema["tags"]:
        assert tag["name"]
        assert len(tag["description"]) >= 20, tag["name"]


# --- 操作 ---------------------------------------------------------------------


def test_every_operation_documents_itself(schema):
    missing = []
    for method, path, operation in _ops(schema):
        summary = (operation.get("summary") or "").strip()
        description = (operation.get("description") or "").strip()
        if len(summary) < 3 or len(description) < 20:
            missing.append(f"{method.upper()} {path} summary={summary!r} desc={len(description)}")
    assert missing == []


def test_operation_ids_are_unique(schema):
    ids = [operation["operationId"] for _, _, operation in _ops(schema)]
    assert len(ids) == len(set(ids))


def test_operation_ids_are_camel_case(schema):
    for _, _, operation in _ops(schema):
        assert _OPERATION_ID.match(operation["operationId"]), operation["operationId"]


def test_operation_ids_derive_from_path(schema):
    for method, path, operation in _ops(schema):
        assert operation["operationId"] == operation_id_for(method, path)


def test_operation_id_contract(schema):
    """钉住几个会被 SDK 生成器读到的方法名，改名要显式改这里。"""
    assert operation_id_for("post", "/v1/events/trigger") == "postV1EventsTrigger"
    assert operation_id_for("post", "/v1/events/trigger/batch") == "postV1EventsTriggerBatch"
    assert (
        operation_id_for("get", "/v1/admin/activity/{trigger_id}")
        == "getV1AdminActivityByTriggerId"
    )
    assert operation_id_for("get", "/health") == "getHealth"
    assert schema["paths"]["/v1/inbox/stream"]["get"]["operationId"] == "getV1InboxStream"


def test_operation_ids_do_not_leak_function_names(schema):
    """函数名是实现细节，重构时不该让调用方的方法名跟着变。"""
    ids = " ".join(operation["operationId"] for _, _, operation in _ops(schema))
    for leaked in ("trigger_event", "list_workflows", "get_inbox", "mark_read"):
        assert leaked not in ids


def test_paths_are_sorted(schema):
    paths = list(schema["paths"])
    assert paths == sorted(paths)


def test_methods_are_ordered_within_path(schema):
    for path, item in schema["paths"].items():
        methods = [m for m in item if m in _METHODS]
        assert methods == [m for m in _METHODS if m in methods], path


def test_path_parameters_are_required(schema):
    for method, path, operation in _ops(schema):
        names = set(re.findall(r"\{([^{}]+)\}", path))
        declared = {
            param["name"]
            for param in operation.get("parameters", [])
            if param.get("in") == "path"
        }
        assert names == declared, f"{method} {path}"
        for param in operation.get("parameters", []):
            if param.get("in") == "path":
                assert param["required"] is True
                assert param["schema"]["type"] == "string"


def test_every_response_has_description(schema):
    for method, path, operation in _ops(schema):
        for code, response in operation["responses"].items():
            assert response.get("description"), f"{method} {path} {code}"


# --- 鉴权与公共错误面 ---------------------------------------------------------


def test_api_key_scheme_declared(schema):
    scheme = schema["components"]["securitySchemes"]["ApiKeyAuth"]
    assert scheme == {
        "type": "apiKey",
        "in": "header",
        "name": "X-Api-Key",
        "description": scheme["description"],
    }
    assert scheme["description"]


def test_security_allows_anonymous(schema):
    """两种凭据都可选：空要求对象就是「也可以不带」的标准写法。"""
    assert schema["security"] == [{"ApiKeyAuth": []}, {"BearerAuth": []}, {}]


def test_bearer_scheme_declared(schema):
    scheme = schema["components"]["securitySchemes"]["BearerAuth"]
    assert scheme["type"] == "http"
    assert scheme["scheme"] == "bearer"
    assert scheme["bearerFormat"] == "JWT"
    assert scheme["description"]


def test_admin_operations_require_a_credential(schema):
    """运维接口不给匿名备选：要么后台令牌，要么带 admin 角色的 Key。"""
    for method, path, operation in _ops(schema):
        if not path.startswith("/v1/admin"):
            continue
        assert operation["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}], path


def test_login_endpoint_is_marked_as_anonymous(schema):
    """换令牌的接口自己不需要令牌，否则读者会以为要先有令牌才能登录。"""
    assert schema["paths"]["/v1/auth/token"]["post"]["security"] == []
    token = schema["paths"]["/v1/auth/token"]["post"]["responses"]
    assert {"200", "401", "403", "503"} <= set(token)
    me = schema["paths"]["/v1/auth/me"]["get"]["responses"]
    assert {"200", "401", "503"} <= set(me)


def test_environment_header_is_documented(schema):
    """读环境头的接口要把这个头写清楚；不读的接口不该凭空多一个头。"""
    env_ops = []
    for method, path, operation in _ops(schema):
        params = {p["name"]: p for p in operation.get("parameters", [])}
        if "X-Environment" not in params:
            continue
        env_ops.append(path)
        header = params["X-Environment"]
        assert header["in"] == "header"
        assert header["required"] is False
        assert "development" in header["description"]
        # 环境与租户是同一次解析出来的两个维度，声明了环境就必须声明租户。
        tenant = params.get("X-Tenant-Id")
        assert tenant is not None, f"{method} {path}"
        assert tenant["in"] == "header"
        assert tenant["required"] is False

    # 代表性接口：触发、广播、偏好、订阅者与查询侧都靠这个头定位环境与租户。
    for path in (
        "/v1/events/trigger",
        "/v1/topics/{topic_id}/trigger",
        "/v1/subscribers/{subscriber_id}/preferences",
        "/v1/subscribers",
        "/v1/providers/{identifier}",
        "/v1/activity",
        "/v1/executions",
        "/v1/inbox",
        "/v1/workflows",
    ):
        assert path in env_ops, path

    # 部署级接口不读这两个头：健康检查没有租户概念，渠道配置与投递汇总
    # 属于整套部署而非某个租户。写进文档而不是让读者猜。
    without = {
        (method, path)
        for method, path, operation in _ops(schema)
        if "X-Environment" not in {p["name"] for p in operation.get("parameters", [])}
    }
    assert without == {
        ("get", "/health"),
        ("get", "/v1/dashboard/stats"),
        ("get", "/v1/providers"),
        ("get", "/v1/providers/{identifier}"),
        # 登录发生在「进入某个环境」之前：环境写在令牌里，之后每次请求再带。
        ("post", "/v1/auth/token"),
        ("get", "/v1/auth/me"),
    }


#: 登录相关接口自己声明错误面：它们不读环境头，但确实会因为凭据与服务端配置失败。
AUTH_PATHS = ("/v1/auth/token", "/v1/auth/me")


def test_common_error_responses_only_where_reachable(schema):
    for method, path, operation in _ops(schema):
        if path in AUTH_PATHS:
            continue
        params = {p["name"] for p in operation.get("parameters", [])}
        responses = operation["responses"]
        if "X-Environment" in params:
            assert "400" in responses and "401" in responses, f"{method} {path}"
        else:
            # 不校验 Key 的接口不该声明 401：文档里写了，调用方就会以为需要配 Key。
            assert "401" not in responses, f"{method} {path}"

        # 403 只可能来自跨租户访问，因此跟着租户头走。
        if "X-Tenant-Id" in params:
            assert "403" in responses, f"{method} {path}"
        else:
            assert "403" not in responses, f"{method} {path}"

        # 409 在这里特指「幂等键已占用但回执读不出来」，跟着幂等键头走；
        # 别的接口也可能因为资源冲突返回 409（例如渠道配置重名），那条由路由自己声明。
        if "Idempotency-Key" in params:
            conflict = responses.get("409")
            assert conflict is not None, f"{method} {path}"
            assert conflict["content"]["application/json"]["example"]["detail"], f"{method} {path}"

    # 声明了 400 / 401 的接口都要给出统一的错误体，调用方不必按状态码分两套解析。
    for method, path, operation in _ops(schema):
        for code in ("400", "401"):
            response = operation["responses"].get(code)
            if response is None:
                continue
            content = response["content"]["application/json"]
            assert content["example"]["detail"], f"{method} {path} {code}"
            assert _resolve(schema, content["schema"]["$ref"])["required"] == ["detail"]

    # 健康检查不读环境、不校验 Key。
    assert "401" not in schema["paths"]["/health"]["get"]["responses"]


def test_rate_limit_response_declared_on_v1(schema):
    """429 跟着路径前缀走：限流中间件拦的是 ``/v1`` 下的所有接口。

    包括不读环境头的部署级接口（渠道列表、仪表盘汇总）——它们同样会被拦，
    文档里漏掉会让调用方把 429 当成异常而不是需要退避的信号。
    """
    for method, path, operation in _ops(schema):
        responses = operation["responses"]
        if path.startswith("/v1"):
            assert "429" in responses, f"{method} {path}"
            body = responses["429"]["content"]["application/json"]
            assert body["example"]["detail"]
        else:
            assert "429" not in responses, f"{method} {path}"

    # 健康探测被限流是运维事故，它不在 /v1 下，也就不该声明这条响应。
    assert "429" not in schema["paths"]["/health"]["get"]["responses"]


def test_trigger_documents_idempotency_header(schema):
    params = {
        p["name"]: p
        for p in schema["paths"]["/v1/events/trigger"]["post"]["parameters"]
    }
    header = params["Idempotency-Key"]
    assert header["in"] == "header"
    assert header["required"] is False
    assert "幂等" in header["description"]


def test_stream_response_is_text_event_stream(schema):
    ok = schema["paths"]["/v1/inbox/stream"]["get"]["responses"]["200"]
    content = ok["content"]
    assert list(content) == ["text/event-stream"]
    assert content["text/event-stream"]["example"].startswith("event: ready")


def test_delete_endpoints_have_no_body(schema):
    """204 不能带响应体；仍返回被删记录（200）的接口除外。"""
    seen_204 = 0
    for method, path, operation in _ops(schema):
        if operation["responses"].get("204") is None:
            continue
        seen_204 += 1
        assert "content" not in operation["responses"]["204"], f"{method} {path}"
    assert seen_204 >= 3


# --- 引用与模型 ---------------------------------------------------------------


def test_all_refs_resolve(schema):
    refs = set(_refs(schema))
    assert refs
    for ref in refs:
        _resolve(schema, ref)


def test_error_schema_is_defined(schema):
    error = schema["components"]["schemas"]["Error"]
    assert error["required"] == ["detail"]
    assert "oneOf" in error["properties"]["detail"]


def test_trigger_example_is_valid():
    from app.schemas import TriggerEvent

    example = _examples(build_schema(app), "TriggerEvent")[0]
    event = TriggerEvent.model_validate(example)
    assert event.name == "post_commented"
    assert event.to.subscriber_id == "creator-1001"


def test_subscriber_example_is_valid_and_normalized():
    from app.subscriber_schemas import SubscriberCreate

    example = _examples(build_schema(app), "SubscriberCreate")[0]
    created = SubscriberCreate.model_validate(example)
    # 示例里故意写了大小写混合的邮箱与带空格的手机号：文档要展示规范化后的形态。
    assert created.email == "creator@example.com"
    assert created.phone == "+8613800000000"


def test_preference_examples_are_valid():
    from app.preference_routes import PreferenceUpsert

    examples = _examples(build_schema(app), "PreferenceUpsert")
    assert len(examples) >= 3
    parsed = [PreferenceUpsert.model_validate(item) for item in examples]
    assert {item.scope.value for item in parsed} == {"global", "workflow", "channel"}
    assert next(item for item in parsed if item.scope.value == "global").target is None


def test_topic_example_is_valid():
    from app.topic_schemas import TopicCreate

    example = _examples(build_schema(app), "TopicCreate")[0]
    topic = TopicCreate.model_validate(example)
    assert topic.id and topic.name


def test_workflow_example_can_be_created():
    """示例要能真的建出来：文档里复制一段就报 422，比没有示例更糟。"""
    from app.models.entities import Step, Workflow
    from app.workflow_schemas import WorkflowCreate
    from app.workflow_store import validate_workflow

    example = _examples(build_schema(app), "WorkflowCreate")[0]
    body = WorkflowCreate.model_validate(example)
    workflow = Workflow(
        id="doc-example",
        name=body.name,
        description=body.description,
        steps=[
            Step(id=s.id, type=s.type, name=s.name, config=s.config) for s in body.steps
        ],
        version=1,
        environment=body.environment,
        active=True,
    )
    validate_workflow(workflow)


# --- 稳定性与导出 -------------------------------------------------------------


def test_build_is_deterministic():
    first = json.dumps(build_schema(app), ensure_ascii=False, sort_keys=True)
    second = json.dumps(build_schema(app), ensure_ascii=False, sort_keys=True)
    assert first == second


def test_installed_openapi_uses_enriched_schema():
    """服务端 ``GET /openapi.json`` 用的必须是装配后的 schema。"""
    app.openapi_schema = None
    served = app.openapi()
    assert served["info"]["title"] == docs.API_TITLE
    assert served["tags"] == docs.TAGS
    assert served["paths"]["/v1/events/trigger"]["post"]["operationId"] == "postV1EventsTrigger"


def test_exported_file_matches_code(monkeypatch):
    """导出文件是提交物：代码改了却忘了导出，这里失败。"""
    monkeypatch.delenv(docs.SERVER_URL_ENV, raising=False)
    assert EXPORT.exists(), "缺少 docs/openapi.json"
    exported = json.loads(EXPORT.read_text(encoding="utf-8"))
    assert exported == json.loads(json.dumps(build_schema(app)))


def test_export_script_writes_and_checks(tmp_path):
    target = tmp_path / "openapi.json"

    written = subprocess.run(
        [sys.executable, str(SCRIPT), "--output", str(target)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert written.returncode == 0, written.stderr
    # 只断言输出里给出了路径与操作数：具体数字随接口增长，钉死它只会让每次
    # 新增接口都要回来改一行测试。
    assert "条路径" in written.stdout and "个操作" in written.stdout
    assert target.exists()

    checked = subprocess.run(
        [sys.executable, str(SCRIPT), "--output", str(target), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert checked.returncode == 0, checked.stderr

    # 手工改一行即视为过期：CI 要能拦住「改了实现忘了导出」。
    target.write_text(target.read_text(encoding="utf-8").replace("xw-notify API", "xw API"), encoding="utf-8")
    stale = subprocess.run(
        [sys.executable, str(SCRIPT), "--output", str(target), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert stale.returncode == 1
    assert "已过期" in stale.stderr


def test_export_script_reports_missing_file(tmp_path):
    missing = subprocess.run(
        [sys.executable, str(SCRIPT), "--output", str(tmp_path / "nope.json"), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert missing.returncode == 1
    assert "缺少导出文件" in missing.stderr
