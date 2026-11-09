"""场景语义层：``contracts/scene-taxonomy.yaml::scene_semantics`` 的执行侧实现。

三个职责：

1. **构造**：把语义契约实例化为 4.2.2 片段（actors / events / weather / duration / ego），
   供批量生成器（``scripts/batch_add_scenes.py``）、模板服务（``services/templates.py``）与
   场景引擎目录（``catalog_semantics.py``）共用。此前三处各写一份装配逻辑，正是"每种类型
   只有 1 辆前车"的来源；
2. **自检**：:func:`check_scene_semantics` 对照语义逐条检查 config 的必需角色、角色类型/行为、
   数量下界与必需事件，作为 G9 闸门、入库/发布校验与下发前复检的统一判据；
3. **指纹**：:func:`semantic_fingerprint` 输出语义指纹（角色集合 + 行为 + 事件类型 + 天气档位），
   供批级去重与多样性报告区分"同类型不同测试目的"的场景。

表达式口径与 ``scene_engine.ir.evaluate_expression`` 一致（只允许数字、参数名与
``+ - * / ** ()``、``min/max/abs/round``）；角色多实例时变量 ``i`` 表示 0 基实例序号。
"""
from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from hunter_common.scene_taxonomy import (
    EventSemantics,
    RoleSemantics,
    SceneSemantics,
    SceneTaxonomy,
    VariantSemantics,
    get_scene_taxonomy,
)

from app.scene_engine.ir import ExpressionError, evaluate_expression, expression_names

#: 单角色实例上限与整场景参与者/事件上限（与 ``app.schemas.common`` 一致）
MAX_ROLE_ACTORS: Final[int] = 100
MAX_ACTORS: Final[int] = 100
MAX_EVENTS: Final[int] = 100
#: 天气字段取值范围（与契约 ``Weather`` 一致）
WEATHER_LIMITS: Final[dict[str, tuple[float, float]]] = {
    "cloudiness": (0.0, 100.0),
    "rain": (0.0, 100.0),
    "wetness": (0.0, 100.0),
    "fog": (0.0, 100.0),
    "wind": (0.0, 100.0),
    "sun_azimuth": (0.0, 360.0),
    "sun_altitude": (-90.0, 90.0),
}
#: 语义缺失天气字段时的兜底表达式（防御性：正常契约 7 字段齐全）
WEATHER_FALLBACK: Final[dict[str, str]] = {
    "cloudiness": "10",
    "rain": "0",
    "wetness": "0",
    "fog": "0",
    "wind": "0",
    "sun_azimuth": "120",
    "sun_altitude": "40",
}
#: 表达式的运算符提示（与 ``hunter_common.scene_taxonomy`` 的启发式保持一致）
_EXPRESSION_HINTS: Final[tuple[str, ...]] = ("+", "-", "*", "/", "(", ")")
#: 角向归一化常量
_TWO_PI: Final[float] = 2.0 * math.pi
_PI: Final[float] = math.pi


class SemanticsError(ValueError):
    """语义片段构造失败（表达式求值/取值越界）。"""


# =====================================================================
# 契约访问
# =====================================================================


def taxonomy() -> SceneTaxonomy:
    """场景受控字典（进程内缓存）。"""
    return get_scene_taxonomy()


def semantics_for(scene_type: str) -> SceneSemantics | None:
    """取场景类型的语义契约（``custom``/实车回放等无合成语义的类型返回 None）。"""
    return taxonomy().semantics.get(scene_type)


def require_semantics(scene_type: str) -> SceneSemantics:
    """取语义契约（缺失即报错，用于生成器/模板的启动期断言）。"""
    semantics = semantics_for(scene_type)
    if semantics is None:
        raise SemanticsError(f"场景类型 {scene_type} 未声明 scene_semantics")
    return semantics


# =====================================================================
# 表达式与取值解析
# =====================================================================


def _is_expression(text: str) -> bool:
    """判断字符串应按表达式求值（含运算符/数字/已知参数名），否则视为字面量。"""
    if not text:
        return False
    if any(hint in text for hint in _EXPRESSION_HINTS):
        return True
    try:
        float(text)
    except ValueError:
        return False
    return True


def evaluate(expr: str, variables: Mapping[str, float], *, where: str = "") -> float:
    """在语义变量上求解表达式（未知名称/除零转为 :class:`SemanticsError`）。"""
    try:
        return float(evaluate_expression(expr, dict(variables)))
    except ExpressionError as exc:
        raise SemanticsError(f"语义表达式求值失败 {where or expr}: {exc}") from exc
    except ZeroDivisionError as exc:
        raise SemanticsError(f"语义表达式除零 {where or expr}") from exc


def resolve_parameters(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
) -> dict[str, float]:
    """合并参数向量：分布均值打底 → 采样值 → 变体强制值（后者覆盖前者）。"""
    resolved: dict[str, float] = {
        name: float(spec.mean) for name, spec in semantics.params.items()
    }
    for name, value in (values or {}).items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if name in resolved:
            resolved[name] = float(value)
    if variant is not None:
        for name, value in variant.force.items():
            if name in resolved:
                resolved[name] = float(value)
    return resolved


def resolve_value(value: Any, variables: Mapping[str, float], *, where: str = "") -> Any:
    """解析角色/事件参数值：``{select/index}`` → 受控取值，表达式 → 数值，其余保持字面量。"""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        choices = value.get("select") or []
        if not choices:
            raise SemanticsError(f"{where} 的 select 为空")
        index_expr = value.get("index")
        index = 0
        if isinstance(index_expr, str) and index_expr.strip():
            index = round(evaluate(index_expr, variables, where=f"{where}.index"))
        elif isinstance(index_expr, (int, float)) and not isinstance(index_expr, bool):
            index = round(float(index_expr))
        return str(choices[index % len(choices)])
    text = str(value).strip()
    if _is_expression(text):
        return round(evaluate(text, variables, where=where), 3)
    return text


def _resolve_parameters_map(
    parameters: Mapping[str, Any], variables: Mapping[str, float], *, where: str
) -> dict[str, Any]:
    """解析一张参数表（角色行为参数 / 事件触发参数 / 动作参数）。"""
    return {
        name: resolve_value(value, variables, where=f"{where}.{name}")
        for name, value in parameters.items()
    }


def _resolve_count(role: RoleSemantics, variables: Mapping[str, float]) -> int:
    """解析角色实例数（四舍五入后裁剪到 [0, MAX_ROLE_ACTORS]）。"""
    count = evaluate(role.count, variables, where=f"roles.{role.role}.count")
    return max(0, min(MAX_ROLE_ACTORS, round(count)))


def _wrap_angle(value: float) -> float:
    """角度归一化到 [-π, π]（契约 SpawnPoint.yaw 取值域）。"""
    wrapped = math.remainder(value, _TWO_PI)
    return max(-_PI, min(_PI, wrapped))


def _round_angle(value: float) -> float:
    """四舍五入后再裁剪，避免 -3.1416 越出 ``ge=-π``。"""
    return max(-_PI, min(_PI, round(_wrap_angle(value), 4)))


# =====================================================================
# 语义 → 4.2.2 片段
# =====================================================================


def build_actors(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
    *,
    counts: Mapping[str, int] | None = None,
) -> list[dict[str, Any]]:
    """按 ``roles`` 生成参与者清单（actor_id = ``{role}-{序号}``，供角色回溯）。

    ``counts`` 用于按角色名覆盖实例数（目录合成时取可采样上界，保证任意采样值下
    语义检查不会出现「角色实例少于 count 表达式」的假阴性）。
    """
    resolved = resolve_parameters(semantics, values, variant)
    actors: list[dict[str, Any]] = []
    for role in semantics.roles:
        count = (
            max(0, min(MAX_ROLE_ACTORS, int(counts[role.role])))
            if counts is not None and role.role in counts
            else _resolve_count(role, resolved)
        )
        for index in range(count):
            if len(actors) >= MAX_ACTORS:
                return actors
            actors.append(_actor_entry(role, index, {**resolved, "i": float(index)}))
    return actors


def _actor_entry(
    role: RoleSemantics, index: int, variables: Mapping[str, float]
) -> dict[str, Any]:
    """单个参与者条目（生成点相对自车出生点，平台不折算）。"""
    spawn: dict[str, Any] = {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0}
    for key in ("x", "y", "z"):
        expr = role.spawn.get(key)
        if expr:
            spawn[key] = round(evaluate(expr, variables, where=f"roles.{role.role}.spawn.{key}"), 2)
    yaw_expr = role.spawn.get("yaw")
    if yaw_expr:
        spawn["yaw"] = _round_angle(
            evaluate(yaw_expr, variables, where=f"roles.{role.role}.spawn.yaw")
        )
    lane_id = role.spawn.get("lane_id")
    if lane_id and not _is_expression(lane_id):
        spawn["lane_id"] = lane_id
    return {
        "actor_id": f"{role.role}-{index + 1}",
        "type": role.actor_type,
        "spawn_point": spawn,
        "behavior": {
            "behavior_type": role.behavior_type,
            "parameters": _resolve_parameters_map(
                role.parameters, variables, where=f"roles.{role.role}.parameters"
            ),
        },
    }


def build_events(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
) -> list[dict[str, Any]]:
    """按 ``events`` 生成事件清单（触发条件/动作均取自受控字典）。"""
    resolved = resolve_parameters(semantics, values, variant)
    events: list[dict[str, Any]] = []
    for index, event in enumerate(semantics.events[:MAX_EVENTS]):
        events.append(_event_entry(event, index, resolved))
    return events


def _event_entry(
    event: EventSemantics, index: int, variables: Mapping[str, float]
) -> dict[str, Any]:
    trigger: dict[str, Any] = {"condition": event.condition}
    if event.condition_type:
        trigger["condition_type"] = event.condition_type
    trigger["parameters"] = _resolve_parameters_map(
        event.parameters, variables, where=f"events[{index}].parameters"
    )
    return {
        "event_id": f"ev-{index + 1}",
        "type": event.type,
        "trigger": trigger,
        "action": {
            "action_type": event.action_type,
            "parameters": _resolve_parameters_map(
                event.action_parameters, variables, where=f"events[{index}].action_parameters"
            ),
        },
    }


def build_weather(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
) -> dict[str, float]:
    """按 ``weather`` 表达式生成 7 字段天气（裁剪到契约取值域）。"""
    resolved = resolve_parameters(semantics, values, variant)
    expressions = dict(WEATHER_FALLBACK)
    expressions.update(semantics.weather)
    if variant is not None:
        expressions.update(variant.weather)
    weather: dict[str, float] = {}
    for field, expr in expressions.items():
        low, high = WEATHER_LIMITS[field]
        raw = evaluate(expr, resolved, where=f"weather.{field}")
        weather[field] = round(min(high, max(low, raw)), 3)
    return weather


def build_duration(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
) -> float:
    """按 ``duration`` 表达式生成场景时长（裁剪到 [5, 300] s）。"""
    resolved = resolve_parameters(semantics, values, variant)
    raw = evaluate(semantics.duration, resolved, where="duration")
    return round(min(300.0, max(5.0, raw)), 1)


def build_ego(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
    *,
    model: str = "HUNTER SE",
) -> dict[str, Any]:
    """按语义生成自车初始状态（速度取 ``ego_speed``/``v0``，转向取 ``ego_steer``）。"""
    resolved = resolve_parameters(semantics, values, variant)
    speed = resolved.get("ego_speed", resolved.get("v0", 8.0))
    steer = evaluate(semantics.ego_steer, resolved, where="ego_steer")
    return {
        "model": model,
        "initial_speed": round(max(0.0, float(speed)), 2),
        "initial_steer": round(max(-1.0, min(1.0, steer)), 3),
    }


def build_semantic_config(
    semantics: SceneSemantics,
    values: Mapping[str, float] | None = None,
    variant: VariantSemantics | None = None,
) -> dict[str, Any]:
    """生成与 semantics 相关的配置片段（不含 map / success_criteria）。"""
    return {
        "ego_vehicle": build_ego(semantics, values, variant),
        "weather": build_weather(semantics, values, variant),
        "actors": build_actors(semantics, values, variant),
        "events": build_events(semantics, values, variant),
        "duration": build_duration(semantics, values, variant),
    }


# =====================================================================
# 语义完整性自检（G9）
# =====================================================================


@dataclass(frozen=True, slots=True)
class SemanticsProblem:
    """单条语义违例（G9 / 入库校验 / 下发复检共用）。"""

    code: str
    message: str
    role: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "role": self.role}


@dataclass(frozen=True, slots=True)
class RoleAudit:
    """角色覆盖审计行（``scripts/verify_scene_semantics.py`` 输出表）。"""

    role: str
    actor_type: str
    behavior_type: str
    required: bool
    min_count: int
    actual: int
    actor_ids: tuple[str, ...]

    @property
    def status(self) -> str:
        if self.actual >= max(self.min_count, 1 if self.required else 0):
            return "ok"
        return "missing" if self.actual == 0 else "underflow"

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "type": self.actor_type,
            "behavior": self.behavior_type,
            "required": self.required,
            "min_count": self.min_count,
            "actual": self.actual,
            "actor_ids": list(self.actor_ids),
            "status": self.status,
        }


def _role_of_actor_id(actor_id: str, roles: Sequence[RoleSemantics]) -> str | None:
    """按 actor_id 命名回溯角色（``{role}`` / ``{role}-N`` / ``{role}_N``）。"""
    best: str | None = None
    for role in roles:
        if actor_id == role.role:
            return role.role
        for sep in ("-", "_"):
            if actor_id.startswith(role.role + sep) and (best is None or len(role.role) > len(best)):
                best = role.role
    return best


def _unique_role_pairs(semantics: SceneSemantics) -> set[tuple[str, str]]:
    """(类型, 行为) 组合中只出现一次的那些（兜底绑定必须是唯一候选）。"""
    counts: dict[tuple[str, str], int] = {}
    for role in semantics.roles:
        key = (role.actor_type, role.behavior_type)
        counts[key] = counts.get(key, 0) + 1
    return {key for key, count in counts.items() if count == 1}


def group_actors(
    semantics: SceneSemantics, actors: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, list[Mapping[str, Any]]], list[Mapping[str, Any]]]:
    """把 config.actors 归并到角色上（返回 角色→参与者 与 未匹配参与者）。"""
    groups: dict[str, list[Mapping[str, Any]]] = {role.role: [] for role in semantics.roles}
    unmatched: list[Mapping[str, Any]] = []
    for actor in actors:
        role_name = _role_of_actor_id(str(actor.get("actor_id") or ""), semantics.roles)
        if role_name is None:
            unmatched.append(actor)
        else:
            groups[role_name].append(actor)
    unique_pairs = _unique_role_pairs(semantics)
    for role in semantics.roles:
        if groups[role.role]:
            continue
        pair = (role.actor_type, role.behavior_type)
        if pair not in unique_pairs:
            continue
        limit = max(role.min_count, 1)
        for actor in list(unmatched):
            if len(groups[role.role]) >= limit:
                break
            behavior = actor.get("behavior") or {}
            if str(actor.get("type")) == role.actor_type and str(
                behavior.get("behavior_type")
            ) == role.behavior_type:
                groups[role.role].append(actor)
                unmatched.remove(actor)
    return groups, unmatched


def audit_roles(
    scene_type: str,
    config: Mapping[str, Any],
    *,
    values: Mapping[str, float] | None = None,
) -> list[RoleAudit]:
    """逐角色审计参与者覆盖（必需角色/类型/行为/数量）。"""
    semantics = semantics_for(scene_type)
    if semantics is None:
        return []
    groups, _ = group_actors(semantics, config.get("actors") or [])
    rows: list[RoleAudit] = []
    for role in semantics.roles:
        matched = groups.get(role.role, [])
        rows.append(
            RoleAudit(
                role=role.role,
                actor_type=role.actor_type,
                behavior_type=role.behavior_type,
                required=role.required,
                min_count=max(role.min_count, _expected_count(role, semantics, values)),
                actual=len(matched),
                actor_ids=tuple(str(actor.get("actor_id") or "") for actor in matched),
            )
        )
    return rows


def _expected_count(
    role: RoleSemantics,
    semantics: SceneSemantics,
    values: Mapping[str, float] | None,
) -> int:
    """已知语义参数向量时按 count 表达式推算期望实例数。

    只在该表达式的变量名全部可从 ``values`` 解析时才有意义：引擎闸门传入的是 IR 参数
    向量（如 ``v0/gap``），与语义参数名可能不同，否则会把常量角色误判为「实例不足」。
    """
    if values is None:
        return 0
    used = expression_names(role.count)
    if not used or not used <= set(values):
        return 0
    return _resolve_count(role, resolve_parameters(semantics, values))


def check_scene_semantics(
    scene_type: str,
    config: Mapping[str, Any],
    *,
    values: Mapping[str, float] | None = None,
) -> list[SemanticsProblem]:
    """语义完整性检查（无契约的类型返回空列表，由调用方标注跳过）。

    检查项：必需角色缺失、角色类型/行为不匹配、参与者数少于 ``min_count``、必需事件为空。
    """
    semantics = semantics_for(scene_type)
    if semantics is None:
        return []
    groups, _ = group_actors(semantics, config.get("actors") or [])
    problems: list[SemanticsProblem] = []
    for role in semantics.roles:
        matched = groups.get(role.role, [])
        expected = _expected_count(role, semantics, values)
        if role.required and not matched:
            problems.append(
                SemanticsProblem(
                    code="role_missing",
                    message=(
                        f"缺少必需参与者角色 {role.role}"
                        f"（{role.actor_type}/{role.behavior_type}）"
                    ),
                    role=role.role,
                )
            )
            continue
        threshold = max(role.min_count if role.required else 0, expected)
        if len(matched) < threshold:
            problems.append(
                SemanticsProblem(
                    code="role_underflow",
                    message=(
                        f"角色 {role.role} 参与者数 {len(matched)} 少于语义要求 {threshold}"
                    ),
                    role=role.role,
                )
            )
        for actor in matched:
            actor_id = str(actor.get("actor_id") or role.role)
            actor_type = str(actor.get("type") or "")
            if actor_type != role.actor_type:
                problems.append(
                    SemanticsProblem(
                        code="role_type_mismatch",
                        message=(
                            f"参与者 {actor_id} 类型为 {actor_type}，"
                            f"角色 {role.role} 要求 {role.actor_type}"
                        ),
                        role=role.role,
                    )
                )
            behavior = str((actor.get("behavior") or {}).get("behavior_type") or "")
            if behavior != role.behavior_type:
                problems.append(
                    SemanticsProblem(
                        code="role_behavior_mismatch",
                        message=(
                            f"参与者 {actor_id} 行为为 {behavior}，"
                            f"角色 {role.role} 要求 {role.behavior_type}"
                        ),
                        role=role.role,
                    )
                )
    if semantics.required_events and not (config.get("events") or []):
        problems.append(
            SemanticsProblem(
                code="events_missing",
                message=f"场景类型 {scene_type} 要求至少 1 个事件，当前 events 为空",
            )
        )
    return problems


def missing_roles(
    scene_type: str,
    config: Mapping[str, Any],
    *,
    values: Mapping[str, float] | None = None,
) -> list[str]:
    """缺失/不足的必需角色名（供 2001 响应的 ``details.missing_roles``）。"""
    return [
        problem.role
        for problem in check_scene_semantics(scene_type, config, values=values)
        if problem.role is not None
    ]


# =====================================================================
# 语义指纹 / 摘要
# =====================================================================


def _weather_bucket(weather: Mapping[str, Any]) -> str:
    """天气档位（降水/雾/光照三档量化，用于指纹与多样性统计）。"""

    def _value(name: str) -> float:
        try:
            return float(weather.get(name) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    rain = _value("rain")
    fog = _value("fog")
    altitude = _value("sun_altitude")
    rain_band = "rain0" if rain < 1 else ("rain1" if rain < 40 else "rain2")
    fog_band = "fog0" if fog < 5 else ("fog1" if fog < 40 else "fog2")
    light_band = "day" if altitude >= 15 else ("dusk" if altitude >= -6 else "night")
    return f"{rain_band}/{fog_band}/{light_band}"


def semantic_fingerprint(scene_type: str, config: Mapping[str, Any]) -> str:
    """语义指纹：角色×行为 + 事件类型 + 天气档位（同类型内必须唯一）。"""
    semantics = semantics_for(scene_type)
    roles = semantics.roles if semantics is not None else ()
    role_parts: list[str] = []
    for actor in config.get("actors") or []:
        behavior = str((actor.get("behavior") or {}).get("behavior_type") or "")
        role = _role_of_actor_id(str(actor.get("actor_id") or ""), roles)
        role_parts.append(f"{role or actor.get('type')}@{behavior}")
    event_parts = sorted(str(event.get("type") or "") for event in config.get("events") or [])
    payload = "|".join(
        [
            scene_type,
            "+".join(sorted(role_parts)),
            "+".join(event_parts),
            _weather_bucket(config.get("weather") or {}),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _role_summary(role: RoleSemantics) -> dict[str, Any]:
    return {
        "role": role.role,
        "type": role.actor_type,
        "count": role.count,
        "behavior_type": role.behavior_type,
        "parameters": dict(role.parameters),
        "min_count": role.min_count,
        "required": role.required,
    }


def semantics_summary(scene_type: str) -> dict[str, Any] | None:
    """单类型语义摘要（前端语义自检区块 / 校验脚本共用）。"""
    semantics = semantics_for(scene_type)
    if semantics is None:
        return None
    return {
        "scene_type": scene_type,
        "objective": semantics.objective,
        "oracle_ref": semantics.oracle_ref,
        "required_events": semantics.required_events,
        "roles": [_role_summary(role) for role in semantics.roles],
        "required_roles": [role.role for role in semantics.required_roles()],
        "events": [
            {
                "type": event.type,
                "condition_type": event.condition_type,
                "action_type": event.action_type,
            }
            for event in semantics.events
        ],
        "variants": [
            {"key": variant.key, "label": variant.label, "sub": variant.sub}
            for variant in semantics.variants
        ],
        "params": [
            {
                "name": name,
                "mean": spec.mean,
                "std": spec.std,
                "low": spec.low,
                "high": spec.high,
                "unit": spec.unit,
                "discrete": spec.discrete,
                "risk": spec.risk,
            }
            for name, spec in semantics.params.items()
        ],
        "duration": semantics.duration,
        "weather": dict(semantics.weather),
    }


def list_semantics() -> list[dict[str, Any]]:
    """全部类型语义摘要（保持 YAML 声明顺序）。"""
    summaries = [semantics_summary(scene_type) for scene_type in taxonomy().semantics]
    return [item for item in summaries if item is not None]


__all__ = [
    "MAX_ACTORS",
    "MAX_EVENTS",
    "MAX_ROLE_ACTORS",
    "WEATHER_LIMITS",
    "RoleAudit",
    "SemanticsError",
    "SemanticsProblem",
    "audit_roles",
    "build_actors",
    "build_duration",
    "build_ego",
    "build_events",
    "build_semantic_config",
    "build_weather",
    "check_scene_semantics",
    "evaluate",
    "group_actors",
    "list_semantics",
    "missing_roles",
    "require_semantics",
    "resolve_parameters",
    "resolve_value",
    "semantic_fingerprint",
    "semantics_for",
    "semantics_summary",
    "taxonomy",
]
