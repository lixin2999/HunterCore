"""宿主机可直跑脚本的 Python 3.10 兼容性静态校验（V1.24.9 补）。

为什么有这条用例
----------------
部署机（Ubuntu 22.04）系统 Python 是 **3.10**，且**没有项目依赖**；`scripts/collect_carla_*.py` 这类采集脚本
的唯一第三方依赖是 `carla` wheel，因此运维按文档直接在宿主机执行 `python3 scripts/...`。
V1.24.8 的出生点采集脚本写了 `from datetime import UTC`（**3.11+ 才有的别名**）：在 3.11 的 CI / 项目 venv 里全绿，
到实机第一步就 `ImportError: cannot import name 'UTC' from 'datetime'` 崩在 import 行（现场首发）。
本用例把「宿主机可直跑 ⇒ 禁用 3.11+ 标准库写法」钉成静态门禁，**不需要真的安装 Python 3.10**。

判定规则
--------
- **宿主机可直跑** = 顶层第三方依赖只允许 `carla`（其余按 ``sys.stdlib_module_names`` 判定为标准库）；
  需要 `hunter_common` / `yaml` / `fastapi` 等的脚本一律跳过（那些跑在项目 venv 或容器里）。
- 禁用项 = 3.11+ 新增的标准库模块、`from X import 新成员`、`模块.新成员`、3.11+ 新内置名。
- 已知**覆盖不到**的形态（脚本里没有用到，扩展时再补）：实例变量上的新方法（如 `某Path对象.walk()`）、
  关键字参数新增（如 `dataclass(kw_only=)`）。
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"

#: 宿主机直跑脚本允许的唯一第三方依赖
HOST_ALLOWED_THIRD_PARTY = frozenset({"carla"})

#: 必须落在覆盖集合里的脚本（改名/加依赖会静默削弱门禁，故显式钉住）
#: V1.25.0：``collect_carla_spawn_points.py`` 已随“平台不再维护地图可用点”一起删除
MUST_BE_COVERED = (
    "collect_carla_vehicle_geometry.py",
)

#: 3.11+ 才存在的标准库模块
NEW_MODULES: dict[str, str] = {
    "tomllib": "3.11",
    "compression": "3.14",
}

#: 「标准库模块 → 3.11+ 新增成员」：`from X import 成员` 与 `X.成员` 两种写法都据此判定
NEW_MODULE_MEMBERS: dict[str, dict[str, str]] = {
    "datetime": {"UTC": "3.11"},
    "enum": {"StrEnum": "3.11", "ReprEnum": "3.11", "verify": "3.11"},
    "asyncio": {"TaskGroup": "3.11", "timeout": "3.11"},
    "contextlib": {"aclosing": "3.11", "chdir": "3.11"},
    "functools": {"CachingIterator": "3.12"},
    "itertools": {"batched": "3.12"},
    "os": {"getterminalsize": "3.13"},
    "shutil": {"Dirs": "3.12"},
    "sys": {"ExceptionStack": "3.11", "monitoring": "3.12"},
    "typing": {
        "Self": "3.11",
        "LiteralString": "3.11",
        "Never": "3.11",
        "NotRequired": "3.11",
        "Required": "3.11",
        "TypeVarTuple": "3.11",
        "Unpack": "3.11",
        "assert_never": "3.11",
        "assert_type": "3.11",
        "clear_overloads": "3.11",
        "dataclass_transform": "3.11",
        "get_overloads": "3.11",
        "is_deprecated": "3.11",
        "reveal_type": "3.11",
        "TypeAliasType": "3.12",
        "TypeIs": "3.13",
        "deprecated": "3.13",
    },
}

#: 以类型/类名为根的新属性写法（实例上的同类方法无法静态判定，见模块 docstring 的覆盖边界）
NEW_CLASS_ATTRS: dict[str, dict[str, str]] = {
    "Path": {"walk": "3.12"},
}

#: 3.11+ 新增内置名（except / raise / isinstance 引用即失败）
NEW_BUILTINS: dict[str, str] = {
    "ExceptionGroup": "3.11",
    "BaseExceptionGroup": "3.11",
}


def _attribute_chain(node: ast.Attribute) -> list[str] | None:
    """把 `a.b.c` 拆为名字链（仅处理纯 Name/Attribute 组合，含 Call 则返回 None）。"""
    parts: list[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return list(reversed(parts))


def _imports(tree: ast.AST) -> set[str]:
    """绝对导入的顶层模块名（相对导入与 ``__future__`` 不参与第三方判定）。"""
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            found.add(node.module.split(".", 1)[0])
    return found


def _host_runnable_scripts() -> list[tuple[str, Path]]:
    """筛出宿主机可直跑（只依赖标准库 + carla）的脚本，即必须兼容系统 Python 的那批。

    下划线开头的文件（`_templates_*.py` 等）是代码模板而非可执行脚本，不纳入判定。
    """
    out: list[tuple[str, Path]] = []
    for path in sorted(SCRIPTS_DIR.glob("*.py")):
        if path.name.startswith("_"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        third_party = _imports(tree) - set(sys.stdlib_module_names) - {"__future__"}
        if third_party - HOST_ALLOWED_THIRD_PARTY:
            continue
        out.append((path.name, path))
    return out


HOST_SCRIPTS = _host_runnable_scripts()
HOST_IDS = [name for name, _ in HOST_SCRIPTS]


def _violations_from_source(source: str, *, filename: str = "<host-script>") -> list[str]:
    """返回该源码中「3.10 宿主机上会失败」的写法清单。"""
    tree = ast.parse(source, filename=filename)
    offenders: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                version = NEW_MODULES.get(alias.name.split(".", 1)[0])
                if version:
                    offenders.append(f"import {alias.name}（{version}+）")
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            members = NEW_MODULE_MEMBERS.get(node.module.split(".", 1)[0], {})
            for alias in node.names:
                version = members.get(alias.name)
                if version:
                    offenders.append(f"from {node.module} import {alias.name}（{version}+）")
        elif isinstance(node, ast.Attribute):
            chain = _attribute_chain(node)
            if not chain or len(chain) < 2:
                continue
            root, attr = chain[0], chain[-1]
            version = NEW_MODULE_MEMBERS.get(root, {}).get(attr)
            if version:
                offenders.append(f"{ast.unparse(node)}（{version}+）")
            version = NEW_CLASS_ATTRS.get(root, {}).get(attr)
            if version:
                offenders.append(f"{ast.unparse(node)}（{version}+）")
        elif isinstance(node, ast.Name) and node.id in NEW_BUILTINS:
            offenders.append(f"{node.id}（{NEW_BUILTINS[node.id]}+ 内置名）")

    return offenders


def test_collectors_are_covered_by_this_guard() -> None:
    """发现规则本身有效：两个 CARLA 采集脚本必须落在被覆盖集合里（否则门禁形同虚设）。"""
    covered = set(HOST_IDS)
    missing = [name for name in MUST_BE_COVERED if name not in covered]
    assert not missing, f"采集脚本未被宿主兼容性门禁覆盖：{missing}（发现规则被削弱，检查其第三方依赖判定）"


@pytest.mark.parametrize("_name, path", HOST_SCRIPTS, ids=HOST_IDS)
def test_future_annotations_enabled(_name: str, path: Path) -> None:
    """宿主 3.10 下 PEP 604/585 注解须靠 `from __future__ import annotations` 才安全。"""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    futures = [
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "__future__"
        for alias in node.names
    ]
    assert "annotations" in futures, f"{path.name} 缺少 from __future__ import annotations"


@pytest.mark.parametrize("_name, path", HOST_SCRIPTS, ids=HOST_IDS)
def test_no_post_310_stdlib_usage(_name: str, path: Path) -> None:
    """禁止 3.11+ 的标准库模块/成员/内置名——部署机系统 Python 是 3.10。"""
    offenders = _violations_from_source(
        path.read_text(encoding="utf-8"), filename=str(path)
    )
    assert not offenders, (
        f"{path.name} 在宿主机 Python 3.10 上会失败：{offenders}；"
        "改用法：datetime.UTC → datetime.timezone.utc，其余同理（或改为需项目 venv 运行并更新文档）"
    )


def test_guard_actually_detects_the_shipped_defect() -> None:
    """自检（防空跑）：把 V1.24.8 真实踩过的 `from datetime import UTC` 喂进校验必须命中。"""
    snippet = "from datetime import UTC, datetime\n\nprint(datetime.UTC)\n"
    hits = _violations_from_source(snippet)
    assert any("from datetime import UTC" in h for h in hits), hits
    assert any("datetime.UTC" in h for h in hits), hits

    # 反向自检：修好后的写法不得再报（否则门禁会把正确代码也拦下）
    fixed = "from datetime import datetime, timezone\n\nprint(datetime.now(timezone.utc))\n"
    assert _violations_from_source(fixed) == []
