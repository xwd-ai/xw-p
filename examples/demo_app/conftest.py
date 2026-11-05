"""测试收集前装配 SDK 路径：演示项目不强制安装 xw-notify。

与 :mod:`examples.demo_app.community` 里的装配逻辑一致，放在 conftest
是因为 pytest 先加载它、再导入测试模块，测试才能直接 ``import xw_notify``。
"""
from __future__ import annotations

import sys
from pathlib import Path

_SDK = Path(__file__).resolve().parents[2] / "sdk" / "python"
if str(_SDK) not in sys.path:
    sys.path.insert(0, str(_SDK))
