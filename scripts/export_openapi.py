#!/usr/bin/env python3
"""把 API 的 OpenAPI schema 导出成一个可提交的文件。

为什么要有导出这一步：``GET /openapi.json`` 只在服务跑起来之后才存在，
而评审接口变更、给 SDK 生成器喂输入、在合并请求里看「这次动了哪些契约」，
都需要一份能在版本管理里 diff 的文件。导出结果按路径排序、JSON 键排序，
同一份代码重复导出的字节一致，diff 里只会有真实改动。

用法::

    api/.venv/bin/python scripts/export_openapi.py            # 写入 docs/openapi.json
    api/.venv/bin/python scripts/export_openapi.py --check     # 只比对，过期时退出码 1
    api/.venv/bin/python scripts/export_openapi.py --server-url https://notify.example.com

``--check`` 适合放进 CI 或提交前检查：schema 改了却忘了重新导出，流水线会失败，
而不是等到别人发现文档与实现不一致。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API_DIR = ROOT / "api"
DEFAULT_OUTPUT = ROOT / "docs" / "openapi.json"


def _ensure_path() -> None:
    """按仓库布局把 ``api/`` 放进导入路径，不要求已安装成包。"""
    api_dir = str(API_DIR)
    if api_dir not in sys.path:
        sys.path.insert(0, api_dir)


def _load_app():
    _ensure_path()
    from app.main import app  # 延迟导入：同时把环境变量覆盖放在前面

    return app


def render(schema: dict) -> str:
    """统一序列化方式，保证重复导出字节一致。"""
    return json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _stats(schema: dict) -> dict:
    operations = sum(
        1
        for item in schema.get("paths", {}).values()
        for method in item
        if method in ("get", "post", "put", "patch", "delete", "head", "options")
    )
    return {
        "paths": len(schema.get("paths", {})),
        "operations": operations,
        "tags": len(schema.get("tags", [])),
        "schemas": len(schema.get("components", {}).get("schemas", {})),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="导出 xw-notify 的 OpenAPI schema")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="输出文件路径（默认 docs/openapi.json）",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="不写文件，仅校验现有文件与当前代码一致；不一致时退出码 1",
    )
    parser.add_argument(
        "--server-url",
        default=None,
        help="写入 schema 的部署地址；留空使用同源相对路径",
    )
    args = parser.parse_args(argv)

    if args.server_url:
        # 与 openapi.servers() 读的是同一个变量，这里代用户设置。
        os.environ["XW_OPENAPI_SERVER_URL"] = args.server_url

    _ensure_path()
    from app import openapi as openapi_docs

    schema = openapi_docs.build_schema(_load_app())
    text = render(schema)
    target = Path(args.output)
    stats = _stats(schema)

    if args.check:
        if not target.exists():
            print(f"缺少导出文件：{target}", file=sys.stderr)
            print("运行 scripts/export_openapi.py 重新生成。", file=sys.stderr)
            return 1
        existing = target.read_text(encoding="utf-8")
        if existing != text:
            print(f"导出文件已过期：{target}", file=sys.stderr)
            print("运行 scripts/export_openapi.py 重新生成后再提交。", file=sys.stderr)
            return 1
        print(f"导出文件与当前代码一致：{target}")
        return 0

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    print(
        "已导出 {path}：{paths} 条路径 / {operations} 个操作 / {tags} 个分组 / {schemas} 个模型".format(
            path=target.relative_to(ROOT) if target.is_relative_to(ROOT) else target,
            **stats,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
