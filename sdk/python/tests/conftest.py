"""让测试可以直接 ``import xw_notify``，不必先安装包。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
