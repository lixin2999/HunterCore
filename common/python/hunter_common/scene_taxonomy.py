"""场景受控字典运行时加载（单一事实来源：``contracts/scene-taxonomy.yaml``）。

取代原先「``SceneType`` 18 值固定枚举」的硬编码：新增场景类型 / 行为 / 动作 / 触发条件
只需编辑 YAML，无需改动 Python 逻辑即可入库。本模块把字典解析为只读视图供三方共享：

- scene-service ``app.schemas.scene``：入库校验 ``scene_type`` / ``behavior_type`` /
  ``action_type`` / ``trigger.condition_type`` 与 ``trigger``/``action`` 参数键；
- ``scripts/batch_add_scenes.py``：类型 → 分类 / 标签 / 缩写的派生；
- ``scripts/gen_scene_taxonomy_ts.py``：前端常量生成。

文件定位顺序（均指向 ``scene-taxonomy.yaml``）：
1. 环境变量 ``SCENE_TAXONOMY_FILE``（K8s ConfigMap / bind mount 可显式指定）；
2. 从当前工作目录向上查找 ``contracts/scene-taxonomy.yaml``；
3. 从本模块所在位置向上查找（本地 editable 安装 / 仓库内运行 / 镜像内置）。

字典不可用时**报错而非降级**（避免"实现漂移悄悄上线"），抛 :class:`SceneTaxonomyError`（5000）。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

import yaml

from hunter_common.exceptions import HunterBaseException
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.scene_taxonomy")

#: 环境变量名（显式指向 scene-taxonomy.yaml 文件或其所在 contracts 目录）
SCENE_TAXONOMY_FILE_ENV: Final[str] = "SCENE_TAXONOMY_FILE"

#: 契约文件名
_TAXONOMY_FILENAME: Final[str] = "scene-taxonomy.yaml"


class SceneTaxonomyError(HunterBaseException):
    """场景受控字典不可用（文件缺失/结构非法）：属部署配置错误，返回 5000。"""

    code = 5000
    message = "场景受控字典不可用"


@dataclass(frozen=True, slots=True)
class SceneTypeSpec:
    """场景类型条目（value → label / category）。"""

    value: str
    label: str
    category: str


def _looks_like_taxonomy(path: Path) -> bool:
    return path.is_file()


def _search_upward(start: Path) -> Path | None:
    """从 ``start`` 向上查找 ``contracts/scene-taxonomy.yaml``。"""
    current = start if start.is_dir() else start.parent
    for candidate in (current, *current.parents):
        direct = candidate / "contracts" / _TAXONOMY_FILENAME
        if _looks_like_taxonomy(direct):
            return direct
        sibling = candidate / _TAXONOMY_FILENAME
        if _looks_like_taxonomy(sibling):
            return sibling
    return None


def locate_taxonomy_file() -> Path | None:
    """定位字典文件（见模块 docstring 三级顺序）；找不到返回 None。"""
    env_val = os.environ.get(SCENE_TAXONOMY_FILE_ENV, "").strip()
    if env_val:
        path = Path(env_val)
        if path.is_dir():
            path = path / _TAXONOMY_FILENAME
        if _looks_like_taxonomy(path):
            return path
        logger.warning("scene_taxonomy_env_invalid", path=env_val)
    for start in (Path.cwd(), Path(__file__).resolve()):
        found = _search_upward(start)
        if found is not None:
            return found
    return None


@dataclass(frozen=True, slots=True)
class SceneTaxonomy:
    """场景受控字典只读视图。"""

    source_path: Path
    version: str
    categories: dict[str, str]  # category value → label
    scene_types: dict[str, SceneTypeSpec]  # type value → spec
    behavior_types: dict[str, str]  # value → label
    action_types: dict[str, str]  # value → label
    condition_types: dict[str, str]  # value → label
    trigger_parameter_keys: frozenset[str]
    action_parameter_keys: frozenset[str]
    enumerable_parameter_values: dict[str, frozenset[str]]  # 受控枚举键 → 允许取值

    # ---------- 派生只读集合 ----------
    @property
    def scene_type_values(self) -> frozenset[str]:
        return frozenset(self.scene_types)

    @property
    def scene_type_labels(self) -> dict[str, str]:
        return {value: spec.label for value, spec in self.scene_types.items()}

    @property
    def scene_type_to_category(self) -> dict[str, str]:
        return {value: spec.category for value, spec in self.scene_types.items()}

    @property
    def behavior_type_values(self) -> frozenset[str]:
        return frozenset(self.behavior_types)

    @property
    def action_type_values(self) -> frozenset[str]:
        return frozenset(self.action_types)

    @property
    def condition_type_values(self) -> frozenset[str]:
        return frozenset(self.condition_types)

    def types_by_category(self) -> dict[str, tuple[str, ...]]:
        """分类 → 类型有序元组（保持 YAML 声明顺序）。"""
        grouped: dict[str, list[str]] = {value: [] for value in self.categories}
        for type_value, spec in self.scene_types.items():
            grouped.setdefault(spec.category, []).append(type_value)
        return {category: tuple(values) for category, values in grouped.items()}

    # ---------- 加载 ----------
    @classmethod
    def load(cls, path: Path) -> SceneTaxonomy:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise SceneTaxonomyError(f"无法读取场景字典 {path}: {exc}") from exc
        try:
            doc: dict[str, Any] = yaml.safe_load(raw) or {}
        except yaml.YAMLError as exc:
            raise SceneTaxonomyError(f"场景字典 YAML 解析失败 {path}: {exc}") from exc
        return cls._from_doc(path, doc)

    @classmethod
    def _from_doc(cls, path: Path, doc: dict[str, Any]) -> SceneTaxonomy:
        categories_raw = doc.get("categories") or []
        scene_raw = doc.get("scene_types") or []
        if not categories_raw or not scene_raw:
            raise SceneTaxonomyError("场景字典缺少 categories 或 scene_types")

        categories = {str(c["value"]): str(c.get("label", c["value"])) for c in categories_raw}
        scene_types: dict[str, SceneTypeSpec] = {}
        for item in scene_raw:
            value = str(item["value"])
            category = str(item["category"])
            if category not in categories:
                raise SceneTaxonomyError(f"场景类型 {value} 引用未定义分类 {category}")
            if value in scene_types:
                raise SceneTaxonomyError(f"场景类型重复: {value}")
            scene_types[value] = SceneTypeSpec(
                value=value, label=str(item.get("label", value)), category=category
            )

        behavior_types = {
            str(b["value"]): str(b.get("label", b["value"])) for b in (doc.get("behavior_types") or [])
        }
        action_types = {
            str(a["value"]): str(a.get("label", a["value"])) for a in (doc.get("action_types") or [])
        }
        condition_types = {
            str(c["value"]): str(c.get("label", c["value"])) for c in (doc.get("trigger_condition_types") or [])
        }
        trigger_keys = frozenset(str(k) for k in (doc.get("trigger_parameter_keys") or []))
        action_keys = frozenset(str(k) for k in (doc.get("action_parameter_keys") or []))
        enumerable_raw = doc.get("enumerable_parameter_values") or {}
        enumerable = {
            str(key): frozenset(str(v) for v in values)
            for key, values in enumerable_raw.items()
        }

        return cls(
            source_path=path,
            version=str(doc.get("version", "0")),
            categories=categories,
            scene_types=scene_types,
            behavior_types=behavior_types,
            action_types=action_types,
            condition_types=condition_types,
            trigger_parameter_keys=trigger_keys,
            action_parameter_keys=action_keys,
            enumerable_parameter_values=enumerable,
        )


def _load_from_explicit(base_dir_or_file: str | Path | None = None) -> SceneTaxonomy:
    explicit = str(base_dir_or_file).strip() if base_dir_or_file is not None else ""
    if explicit:
        path = Path(explicit)
        if path.is_dir():
            path = path / _TAXONOMY_FILENAME
        if _looks_like_taxonomy(path):
            return SceneTaxonomy.load(path)
        raise SceneTaxonomyError(f"显式指定的场景字典不可用: {path}")
    found = locate_taxonomy_file()
    if found is None:
        raise SceneTaxonomyError(
            "未找到 contracts/scene-taxonomy.yaml（设置 SCENE_TAXONOMY_FILE 或挂载 contracts/）"
        )
    return SceneTaxonomy.load(found)


@lru_cache(maxsize=8)
def _load_cached(path_str: str) -> SceneTaxonomy:
    return SceneTaxonomy.load(Path(path_str))


def get_scene_taxonomy(base_dir_or_file: str | Path | None = None) -> SceneTaxonomy:
    """获取场景字典只读视图（按解析路径缓存，进程内只解析一次）。"""
    explicit = str(base_dir_or_file).strip() if base_dir_or_file is not None else ""
    if explicit:
        path = Path(explicit)
        if path.is_dir():
            path = path / _TAXONOMY_FILENAME
        if not _looks_like_taxonomy(path):
            raise SceneTaxonomyError(f"显式指定的场景字典不可用: {path}")
        return _load_cached(str(path))
    found = locate_taxonomy_file()
    if found is None:
        raise SceneTaxonomyError(
            "未找到 contracts/scene-taxonomy.yaml（设置 SCENE_TAXONOMY_FILE 或挂载 contracts/）"
        )
    return _load_cached(str(found))


__all__ = [
    "SCENE_TAXONOMY_FILE_ENV",
    "SceneTaxonomy",
    "SceneTaxonomyError",
    "SceneTypeSpec",
    "get_scene_taxonomy",
    "locate_taxonomy_file",
]
