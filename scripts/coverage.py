#!/usr/bin/env python3
"""跑 API 单元测试并给出覆盖率，低于门槛时以非零码退出。

为什么单独一个脚本，而不是把 ``--cov`` 写进 pytest 的 addopts：

- 单文件调试时（``pytest tests/test_health.py``）覆盖率天然只有几个百分点，
  门槛会把它判成失败，人就会开始习惯加 ``--no-cov``，门槛也就形同虚设；
- 覆盖率只在「整套跑完」时才有意义，那正是这个脚本负责的场景。

``--floor`` 是下限而不是目标：它挡住的是「顺手删掉一批测试」这类退化，
督促写出 90% 的覆盖率不是它的职责。当前缺口与原因写在 docs/testing.md。
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API_DIR = ROOT / "api"

#: 默认下限。比当前实测值低几个点，给重构留出余量，又不至于形同虚设。
DEFAULT_FLOOR = 85


def _python() -> Path:
    """优先用项目虚拟环境，其次当前解释器。"""
    candidate = API_DIR / ".venv" / "bin" / "python"
    return candidate if candidate.exists() else Path(sys.executable)


def _clean_env() -> dict[str, str]:
    """去掉代理变量。

    本机的 HTTP 代理会把 ``127.0.0.1`` 的请求也送去代理，表现为「测试里的
    服务连不上」——这类环境问题与代码无关，脚本里统一屏蔽。
    """
    env = dict(os.environ)
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(key, None)
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description="运行 API 测试并检查覆盖率下限")
    parser.add_argument("--floor", type=float, default=DEFAULT_FLOOR, help="覆盖率下限（百分比）")
    parser.add_argument("--xml", metavar="PATH", help="额外输出 Cobertura XML（给 CI 用）")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="透传给 pytest 的参数")
    opts = parser.parse_args()

    python = _python()
    probes = subprocess.run(
        [str(python), "-c", "import pytest, pytest_cov"],
        cwd=API_DIR,
        env=_clean_env(),
        capture_output=True,
        text=True,
    )
    if probes.returncode != 0:
        print(
            "缺少 pytest 或 pytest-cov。安装：\n"
            f"  uv pip install --python {python} pytest pytest-cov\n"
            "或按 api/requirements.txt 里的测试依赖安装。",
            file=sys.stderr,
        )
        return 2

    command = [
        str(python),
        "-m",
        "pytest",
        "-q",
        "--cov=app",
        "--cov-report=term-missing:skip-covered",
        f"--cov-fail-under={opts.floor:g}",
        # 默认临时目录在部分环境里不可写，统一放到系统临时目录下。
        f"--basetemp={tempfile.mkdtemp(prefix='xw-cov-')}",
    ]
    if opts.xml:
        command.append(f"--cov-report=xml:{opts.xml}")
    command.extend(opts.args)

    print("运行：" + " ".join(command[:6]) + " …")
    result = subprocess.run(command, cwd=API_DIR, env=_clean_env())
    if result.returncode == 0:
        print(f"\n覆盖率不低于 {opts.floor:g}%。")
    elif result.returncode == 1:
        print(f"\n测试失败，或覆盖率低于 {opts.floor:g}%。", file=sys.stderr)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
