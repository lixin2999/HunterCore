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

import ast
import os
import re
from dataclasses import dataclass, field
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


# =====================================================================
# 场景语义契约（scene_semantics）数据结构
# =====================================================================
#: 天气 7 字段（与契约 Weather 一致）
WEATHER_FIELDS: Final[frozenset[str]] = frozenset(
    {"cloudiness", "rain", "wetness", "fog", "wind", "sun_azimuth", "sun_altitude"}
)
#: 参与者类型受控值（与契约 ActorType 一致）
ROLE_TYPE_VALUES: Final[frozenset[str]] = frozenset({"vehicle", "pedestrian", "other"})
#: 可行性约束允许的运算符
CONSTRAINT_OPS: Final[frozenset[str]] = frozenset({"<=", "<", ">=", ">", "=="})
#: 语义表达式允许的实例序号变量（角色多实例时按下标展开）
EXPRESSION_INDEX_VARIABLE: Final[str] = "i"

_PARAM_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_ORACLE_REF_RE: Final[re.Pattern[str]] = re.compile(r"^oracle\.[a-z0-9_.]+/v\d+$")
_EXPRESSION_HINTS: Final[tuple[str, ...]] = ("+", "-", "*", "/", "(", ")")


@dataclass(frozen=True, slots=True)
class ParamSemantics:
    """参数分布声明（mean/std/low/high 为截断正态口径，risk 控制长尾定向）。"""

    name: str
    mean: float
    std: float
    low: float
    high: float
    risk: int = 0
    discrete: bool = False
    unit: str = ""
    kind: str = "normal"


@dataclass(frozen=True, slots=True)
class RoleSemantics:
    """参与者角色声明（count/spawn/parameters 为参数表达式）。"""

    role: str
    actor_type: str
    count: str
    behavior_type: str
    parameters: dict[str, Any] = field(default_factory=dict)
    spawn: dict[str, str] = field(default_factory=dict)
    required: bool = False
    min_count: int = 1


@dataclass(frozen=True, slots=True)
class EventSemantics:
    """事件模板声明（触发条件与动作均取自受控字典）。"""

    type: str
    condition_type: str
    condition: str
    parameters: dict[str, Any] = field(default_factory=dict)
    action_type: str = ""
    action_parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConstraintSemantics:
    """采样可行性约束（left 为参数名，right 为表达式）。"""

    left: str
    op: str
    right: str
    message: str = ""


@dataclass(frozen=True, slots=True)
class VariantSemantics:
    """子场景（variant）声明：force 固定参数、overrides 覆盖分布、weather 覆盖天气。"""

    key: str
    label: str
    sub: str
    force: dict[str, float] = field(default_factory=dict)
    overrides: dict[str, ParamSemantics] = field(default_factory=dict)
    weather: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SceneSemantics:
    """单个场景类型的语义契约（生成器/模板/目录/用例共用）。"""

    scene_type: str
    objective: str
    oracle_ref: str
    required_events: bool
    params: dict[str, ParamSemantics]
    variants: tuple[VariantSemantics, ...]
    roles: tuple[RoleSemantics, ...]
    events: tuple[EventSemantics, ...]
    weather: dict[str, str]
    duration: str
    ego_steer: str
    constraints: tuple[ConstraintSemantics, ...] = ()

    def required_roles(self) -> tuple[RoleSemantics, ...]:
        """必需角色（语义闸门 G9 的校验清单）。"""
        return tuple(role for role in self.roles if role.required)

    def variant(self, key: str) -> VariantSemantics | None:
        """按子场景编码取变体（未知编码返回 None）。"""
        for item in self.variants:
            if item.key == key:
                return item
        return None


def _expression_names(expr: str) -> frozenset[str]:
    """提取表达式引用的名称（非法语法/属性访问均报错）。"""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise SceneTaxonomyError(f"语义表达式语法错误: {expr}") from exc
    functions = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in functions:
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            raise SceneTaxonomyError(f"语义表达式禁止属性访问: {expr}")
    return frozenset(names)


def _as_expr(value: Any) -> str:
    """把 YAML 标量统一为表达式字符串（数字直接转文本）。"""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(float(value)) if isinstance(value, float) else str(value)
    return str(value).strip()


def _looks_like_expression(text: str, allowed: frozenset[str]) -> bool:
    """判断字符串应按表达式校验（含运算符/数字/已知参数名），否则视为字面量。"""
    if not text:
        return False
    if any(hint in text for hint in _EXPRESSION_HINTS):
        return True
    try:
        float(text)
    except ValueError:
        return text in allowed
    return True


def _validate_expr(text: str, allowed: frozenset[str], where: str) -> None:
    """校验表达式引用的名称均在允许集合内。"""
    unknown = _expression_names(text) - allowed
    if unknown:
        raise SceneTaxonomyError(f"{where} 引用了未知参数: {sorted(unknown)}")


def _validate_value(
    value: Any, allowed: frozenset[str], where: str, *, enumerable: dict[str, frozenset[str]]
) -> None:
    """校验角色/事件参数值：表达式校验名称，select 校验列表非空与下标表达式。"""
    if isinstance(value, dict):
        choices = value.get("select")
        if not isinstance(choices, list) or not choices:
            raise SceneTaxonomyError(f"{where} 的 select 必须为非空列表")
        index = value.get("index")
        if index is not None:
            _validate_expr(_as_expr(index), allowed, f"{where}.index")
        return
    if not isinstance(value, str):
        return
    text = value.strip()
    if _looks_like_expression(text, allowed):
        _validate_expr(text, allowed, where)


def _parse_param(name: str, raw: Any, where: str) -> ParamSemantics:
    """解析单条参数分布声明。"""
    if not _PARAM_NAME_RE.match(name):
        raise SceneTaxonomyError(f"{where} 参数名不合法: {name}")
    if not isinstance(raw, dict):
        raise SceneTaxonomyError(f"{where}.{name} 必须为分布映射")
    try:
        mean = float(raw.get("mean", 0.0))
        std = float(raw.get("std", 0.0))
        low = float(raw.get("low", mean))
        high = float(raw.get("high", mean))
        risk = int(raw.get("risk", 0))
    except (TypeError, ValueError) as exc:
        raise SceneTaxonomyError(f"{where}.{name} 分布参数非法: {exc}") from exc
    if std < 0:
        raise SceneTaxonomyError(f"{where}.{name} 的 std 不能为负")
    if low > high:
        raise SceneTaxonomyError(f"{where}.{name} 的 low 不能大于 high")
    if risk not in (-1, 0, 1):
        raise SceneTaxonomyError(f"{where}.{name} 的 risk 必须为 -1/0/1")
    return ParamSemantics(
        name=name,
        mean=mean,
        std=std,
        low=low,
        high=high,
        risk=risk,
        discrete=bool(raw.get("discrete", False)),
        unit=str(raw.get("unit", "")),
        kind=str(raw.get("kind", "normal")),
    )


def _parse_role(raw: Any, where: str, allowed: frozenset[str], behavior_values: frozenset[str],
                enumerable: dict[str, frozenset[str]]) -> RoleSemantics:
    if not isinstance(raw, dict):
        raise SceneTaxonomyError(f"{where} 必须为映射")
    role = str(raw.get("role", "")).strip()
    if not role or not re.match(r"^[a-z][a-z0-9_-]{0,63}$", role):
        raise SceneTaxonomyError(f"{where} 的 role 不合法: {role!r}")
    actor_type = str(raw.get("type", "")).strip()
    if actor_type not in ROLE_TYPE_VALUES:
        raise SceneTaxonomyError(f"{where} 的 type 不合法: {actor_type}")
    behavior_type = str(raw.get("behavior_type", "")).strip()
    if behavior_type not in behavior_values:
        raise SceneTaxonomyError(f"{where} 的 behavior_type 不在受控字典: {behavior_type}")
    count = _as_expr(raw.get("count", 1))
    _validate_expr(count, allowed, f"{where}.count")
    spawn_raw = raw.get("spawn") or {}
    if not isinstance(spawn_raw, dict):
        raise SceneTaxonomyError(f"{where}.spawn 必须为映射")
    spawn = {str(k): _as_expr(v) for k, v in spawn_raw.items()}
    unknown_spawn = set(spawn) - {"x", "y", "z", "yaw", "lane_id"}
    if unknown_spawn:
        raise SceneTaxonomyError(f"{where}.spawn 含未知字段: {sorted(unknown_spawn)}")
    for key, expr in spawn.items():
        if key != "lane_id":
            _validate_expr(expr, allowed, f"{where}.spawn.{key}")
    parameters = raw.get("parameters") or {}
    if not isinstance(parameters, dict):
        raise SceneTaxonomyError(f"{where}.parameters 必须为映射")
    for key, value in parameters.items():
        _validate_value(value, allowed, f"{where}.parameters.{key}", enumerable=enumerable)
    min_count = int(raw.get("min_count", 1))
    if not 0 <= min_count <= 100:
        raise SceneTaxonomyError(f"{where}.min_count 越界(0,100): {min_count}")
    return RoleSemantics(
        role=role,
        actor_type=actor_type,
        count=count,
        behavior_type=behavior_type,
        parameters=dict(parameters),
        spawn=spawn,
        required=bool(raw.get("required", False)),
        min_count=min_count,
    )


def _parse_event(raw: Any, where: str, allowed: frozenset[str], *, event_types: frozenset[str],
                 action_values: frozenset[str], condition_values: frozenset[str],
                 trigger_keys: frozenset[str], action_keys: frozenset[str],
                 enumerable: dict[str, frozenset[str]]) -> EventSemantics:
    if not isinstance(raw, dict):
        raise SceneTaxonomyError(f"{where} 必须为映射")
    event_type = str(raw.get("type", "")).strip()
    if event_type not in event_types:
        raise SceneTaxonomyError(f"{where} 的 type 不在 scene_event_types: {event_type}")
    condition_type = str(raw.get("condition_type", "")).strip()
    if condition_type not in condition_values:
        raise SceneTaxonomyError(f"{where} 的 condition_type 不在受控字典: {condition_type}")
    action_type = str(raw.get("action_type", "")).strip()
    if action_type not in action_values:
        raise SceneTaxonomyError(f"{where} 的 action_type 不在受控字典: {action_type}")
    condition = str(raw.get("condition", "")).strip()
    if not condition or len(condition) > 256:
        raise SceneTaxonomyError(f"{where} 的 condition 长度非法（1~256）")
    parameters = raw.get("parameters") or {}
    action_parameters = raw.get("action_parameters") or {}
    if not isinstance(parameters, dict) or not isinstance(action_parameters, dict):
        raise SceneTaxonomyError(f"{where} 的 parameters/action_parameters 必须为映射")
    unknown = {str(k) for k in parameters} - trigger_keys
    if unknown:
        raise SceneTaxonomyError(f"{where}.parameters 含非白名单键: {sorted(unknown)}")
    unknown_action = {str(k) for k in action_parameters} - action_keys
    if unknown_action:
        raise SceneTaxonomyError(f"{where}.action_parameters 含非白名单键: {sorted(unknown_action)}")
    for key, value in parameters.items():
        _validate_value(value, allowed, f"{where}.parameters.{key}", enumerable=enumerable)
    for key, value in action_parameters.items():
        _validate_value(value, allowed, f"{where}.action_parameters.{key}", enumerable=enumerable)
    return EventSemantics(
        type=event_type,
        condition_type=condition_type,
        condition=condition,
        parameters=dict(parameters),
        action_type=action_type,
        action_parameters=dict(action_parameters),
    )


def _check_weather(weather: dict[str, str], allowed: frozenset[str], where: str) -> None:
    """校验天气表达式：键必须在 Weather 7 字段内，表达式名称必须可解析。"""
    unknown = set(weather) - WEATHER_FIELDS
    if unknown:
        raise SceneTaxonomyError(f"{where} 含未知天气字段: {sorted(unknown)}")
    for key, expr in weather.items():
        _validate_expr(expr, allowed, f"{where}.{key}")


def _parse_variant(
    raw: Any,
    where: str,
    allowed: frozenset[str],
    params: dict[str, ParamSemantics],
    enumerable: dict[str, frozenset[str]],
) -> VariantSemantics:
    """解析单个子场景（force 固定参数 / overrides 覆盖分布 / weather 覆盖天气）。"""
    if not isinstance(raw, dict):
        raise SceneTaxonomyError(f"{where} 必须为映射")
    key = str(raw.get("key", "")).strip()
    if not _PARAM_NAME_RE.match(key):
        raise SceneTaxonomyError(f"{where}.key 不合法: {key!r}")
    label = str(raw.get("label", "")).strip()
    if not label:
        raise SceneTaxonomyError(f"{where}.label 不能为空")
    sub = str(raw.get("sub", key[:3].upper())).strip()
    if not re.match(r"^[A-Z0-9]{1,6}$", sub):
        raise SceneTaxonomyError(f"{where}.sub 不合法（1~6 位大写字母/数字）: {sub!r}")
    force: dict[str, float] = {}
    for name, value in (raw.get("force") or {}).items():
        name = str(name)
        if name not in params:
            raise SceneTaxonomyError(f"{where}.force 引用了未知参数: {name}")
        try:
            force[name] = float(value)
        except (TypeError, ValueError) as exc:
            raise SceneTaxonomyError(f"{where}.force.{name} 必须为数字") from exc
    overrides: dict[str, ParamSemantics] = {}
    for name, spec in (raw.get("overrides") or {}).items():
        name = str(name)
        if name not in params:
            raise SceneTaxonomyError(f"{where}.overrides 引用了未知参数: {name}")
        overrides[name] = _parse_param(name, spec, f"{where}.overrides")
    weather = {str(k): _as_expr(v) for k, v in (raw.get("weather") or {}).items()}
    _check_weather(weather, allowed, f"{where}.weather")
    return VariantSemantics(
        key=key, label=label, sub=sub, force=force, overrides=overrides, weather=weather
    )


def _parse_constraint(
    raw: Any, where: str, allowed: frozenset[str], params: dict[str, ParamSemantics]
) -> ConstraintSemantics:
    """解析采样可行性约束（left 为参数名，right 为表达式）。"""
    if not isinstance(raw, dict):
        raise SceneTaxonomyError(f"{where} 必须为映射")
    left = str(raw.get("left", "")).strip()
    if left not in params:
        raise SceneTaxonomyError(f"{where}.left 引用了未知参数: {left}")
    op = str(raw.get("op", "")).strip()
    if op not in CONSTRAINT_OPS:
        raise SceneTaxonomyError(f"{where}.op 不在允许集合 {sorted(CONSTRAINT_OPS)}: {op!r}")
    right = _as_expr(raw.get("right"))
    _validate_expr(right, allowed, f"{where}.right")
    return ConstraintSemantics(left=left, op=op, right=right, message=str(raw.get("message", "")))


def _parse_scene_semantics(
    doc: dict[str, Any],
    *,
    scene_types: dict[str, SceneTypeSpec],
    behavior_values: frozenset[str],
    action_values: frozenset[str],
    condition_values: frozenset[str],
    trigger_keys: frozenset[str],
    action_keys: frozenset[str],
    event_types: frozenset[str],
    enumerable: dict[str, frozenset[str]],
) -> dict[str, SceneSemantics]:
    """解析并强校验 scene_semantics（结构/受控值/表达式引用），递规不降级。"""
    raw_all = doc.get("scene_semantics")
    if not raw_all:
        logger.warning("scene_semantics_absent", note="场景语义契约未声明，生成器/闸门只能降级到类型级")
        return {}
    if not isinstance(raw_all, dict):
        raise SceneTaxonomyError("scene_semantics 必须为映射")

    defaults = doc.get("scene_semantics_defaults") or {}
    default_params = {
        str(name): _parse_param(str(name), raw, "scene_semantics_defaults.params")
        for name, raw in (defaults.get("params") or {}).items()
    }
    default_weather = {
        str(key): _as_expr(value) for key, value in (defaults.get("weather") or {}).items()
    }
    default_allowed = frozenset(default_params) | {EXPRESSION_INDEX_VARIABLE}
    _check_weather(default_weather, default_allowed, "scene_semantics_defaults.weather")
    default_duration = _as_expr(defaults.get("duration", "duration"))
    default_steer = _as_expr(defaults.get("ego_steer", "0.0"))

    semantics: dict[str, SceneSemantics] = {}
    for scene_type, raw in raw_all.items():
        scene_type = str(scene_type)
        where = f"scene_semantics.{scene_type}"
        if scene_type not in scene_types:
            raise SceneTaxonomyError(f"{where} 不是受控 scene_types 成员")
        if not isinstance(raw, dict):
            raise SceneTaxonomyError(f"{where} 必须为映射")

        objective = str(raw.get("objective", "")).strip()
        if not objective:
            raise SceneTaxonomyError(f"{where}.objective 不能为空")
        oracle_ref = str(raw.get("oracle_ref", "")).strip()
        if not _ORACLE_REF_RE.match(oracle_ref):
            raise SceneTaxonomyError(f"{where}.oracle_ref 格式非法: {oracle_ref!r}")

        params = dict(default_params)
        for name, spec in (raw.get("params") or {}).items():
            params[str(name)] = _parse_param(str(name), spec, f"{where}.params")
        allowed = frozenset(params) | {EXPRESSION_INDEX_VARIABLE}

        duration = _as_expr(raw.get("duration", default_duration))
        _validate_expr(duration, allowed, f"{where}.duration")
        ego_steer = _as_expr(raw.get("ego_steer", default_steer))
        _validate_expr(ego_steer, allowed, f"{where}.ego_steer")

        weather = dict(default_weather)
        for key, value in (raw.get("weather") or {}).items():
            weather[str(key)] = _as_expr(value)
        _check_weather(weather, allowed, f"{where}.weather")

        variants: list[VariantSemantics] = []
        for index, item in enumerate(raw.get("variants") or []):
            variants.append(
                _parse_variant(item, f"{where}.variants[{index}]", allowed, params, enumerable)
            )
        if len(variants) < 2:
            raise SceneTaxonomyError(f"{where}.variants 至少需要 2 个可辨识子场景")
        keys = [variant.key for variant in variants]
        if len(set(keys)) != len(keys):
            raise SceneTaxonomyError(f"{where}.variants 存在重复 key: {keys}")

        roles: list[RoleSemantics] = []
        for index, item in enumerate(raw.get("roles") or []):
            roles.append(
                _parse_role(
                    item,
                    f"{where}.roles[{index}]",
                    allowed,
                    behavior_values,
                    enumerable,
                )
            )
        role_names = [role.role for role in roles]
        if len(set(role_names)) != len(role_names):
            raise SceneTaxonomyError(f"{where}.roles 存在重复 role: {role_names}")

        events: list[EventSemantics] = []
        for index, item in enumerate(raw.get("events") or []):
            events.append(
                _parse_event(
                    item,
                    f"{where}.events[{index}]",
                    allowed,
                    event_types=event_types,
                    action_values=action_values,
                    condition_values=condition_values,
                    trigger_keys=trigger_keys,
                    action_keys=action_keys,
                    enumerable=enumerable,
                )
            )
        required_events = bool(raw.get("required_events", True))
        if required_events and not events:
            raise SceneTaxonomyError(f"{where} required_events=true 但未声明 events")

        constraints: list[ConstraintSemantics] = []
        for index, item in enumerate(raw.get("constraints") or []):
            constraints.append(
                _parse_constraint(item, f"{where}.constraints[{index}]", allowed, params)
            )

        semantics[scene_type] = SceneSemantics(
            scene_type=scene_type,
            objective=objective,
            oracle_ref=oracle_ref,
            required_events=required_events,
            params=params,
            variants=tuple(variants),
            roles=tuple(roles),
            events=tuple(events),
            weather=weather,
            duration=duration,
            ego_steer=ego_steer,
            constraints=tuple(constraints),
        )
    return semantics


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
    scene_event_types: frozenset[str] = frozenset()  # 4.2.2 events[].type 受控值
    semantics: dict[str, SceneSemantics] = field(default_factory=dict)  # 类型 → 语义契约

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

    def semantics_for(self, scene_type: str) -> SceneSemantics:
        """取场景类型的语义契约（缺失即报错，不允许静默降级）。"""
        try:
            return self.semantics[scene_type]
        except KeyError as exc:
            raise SceneTaxonomyError(f"场景类型 {scene_type} 未声明 scene_semantics") from exc

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
        event_types = frozenset(str(v) for v in (doc.get("scene_event_types") or []))
        semantics = _parse_scene_semantics(
            doc,
            scene_types=scene_types,
            behavior_values=frozenset(behavior_types),
            action_values=frozenset(action_types),
            condition_values=frozenset(condition_types),
            trigger_keys=trigger_keys,
            action_keys=action_keys,
            event_types=event_types,
            enumerable=enumerable,
        )

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
            scene_event_types=event_types,
            semantics=semantics,
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
    "CONSTRAINT_OPS",
    "EXPRESSION_INDEX_VARIABLE",
    "ROLE_TYPE_VALUES",
    "SCENE_TAXONOMY_FILE_ENV",
    "WEATHER_FIELDS",
    "ConstraintSemantics",
    "EventSemantics",
    "ParamSemantics",
    "RoleSemantics",
    "SceneSemantics",
    "SceneTaxonomy",
    "SceneTaxonomyError",
    "SceneTypeSpec",
    "VariantSemantics",
    "get_scene_taxonomy",
    "locate_taxonomy_file",
]
