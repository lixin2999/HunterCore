"""pytest 配置：把工具包目录加入 sys.path，并定位仓库 contracts/kafka 供 schema 自校验。"""

from __future__ import annotations

import json
import os
import sys
from functools import cache

import pytest

_PKG_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)


def _find_contracts_dir() -> str:
    here = _PKG_ROOT
    for _ in range(6):
        candidate = os.path.join(here, "contracts", "kafka")
        if os.path.isdir(candidate):
            return candidate
        here = os.path.dirname(here)
    raise RuntimeError("未找到 contracts/kafka（用 SIM_CONTRACT_DIR 指定）")


CONTRACTS_DIR = os.environ.get("SIM_CONTRACT_DIR") or _find_contracts_dir()


@cache
def load_schema(name: str) -> dict:
    with open(os.path.join(CONTRACTS_DIR, "schemas", f"{name}.schema.json"), encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture
def schema_loader():
    """返回按名加载契约 schema 的函数（tests/ 用 jsonschema 自校验生产消息）。"""
    return load_schema
