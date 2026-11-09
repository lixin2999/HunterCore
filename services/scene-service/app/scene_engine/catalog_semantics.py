"""语义驱动的合成逻辑场景目录（``scene_semantics`` → ``SceneIR``）。

分工
----
- ``catalog.py`` 保留 17 个手写 IR（``logical_id`` 稳定，避免已发布版本漂移）；
- 本模块用 ``contracts/scene-taxonomy.yaml::scene_semantics`` 合成其余 29 个类型的 IR，
  与批量生成器 ``scripts/batch_add_scenes.py``、模板服务、用例派生共用同一份语义事实来源。

合成规则
--------
- ``params`` → ``ParameterIR``（分布类型/均值/方差/上下界与语义声明一致；行人步速参数
  上界收敛到 3 m/s，与 G7 真实性口径一致）；
- ``bindings`` → 自车初速/转向、各角色生成点与行为参数、天气 7 字段、时长（仅绑定
  语义参数可表达的表达式，含实例序号 ``i`` 的表达式按实例代入字面量）；
- ``constraints`` → 语义声明的「初始不碰撞（纵向距离 ≥ 简化制动距离）」线性化，
  外加行人步速上限；
- ``CriticalityIR`` → ``speed_param`` 取自车速度参数、``gap_param`` 取语义冲突距离参数；
- ``oracle_ref`` 取语义声明（须在 ``app/scene_engine/oracles.py`` 中存在）。

目录版本 ``CATALOG_VERSION`` 1.1.0 起含本模块合成部分；``sync_catalog`` 按内容哈希幂等入库。
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Final

from hunter_common.scene_taxonomy import (
    ConstraintSemantics,
    RoleSemantics,
    SceneSemantics,
    get_scene_taxonomy,
)

from app.scene_engine.ir import (
    BindingIR,
    ConstraintIR,
    CriticalityIR,
    DistributionIR,
    ParameterIR,
    SceneIR,
    evaluate_expression,
    expression_names,
)
from app.scene_engine.semantics import (
    SemanticsError,
    build_actors,
    build_duration,
    build_ego,
    build_events,
    build_weather,
    evaluate,
    require_semantics,
    resolve_parameters,
)
from app.scene_engine.vehicle_params import get_vehicle_params

#: 已由 ``catalog.py`` 手写的类型（本模块只补缺，避免 logical_id / 已发布版本漂移）
HANDWRITTEN_TYPES: Final[frozenset[str]] = frozenset(
    {
        "straight_cruise",
        "curve_driving",
        "car_following",
        "lane_change",
        "narrow_road_meeting",
        "cut_in",
        "pedestrian_crossing",
        "occluded_pedestrian",
        "intersection_meeting",
        "emergency_braking",
        "obstacle_appearance",
        "child_chasing_ball",
        "stationary_obstacle",
        "rainy",
        "night",
        "minimal_risk_manoeuvre",
        "uturn",
    }
)

#: 分类 → logical_id 前缀（与手写目录的 LS-BAS/INT/ENV/CC 编号体系一致）
_ID_PREFIX: Final[dict[str, str]] = {
    "basic": "BAS",
    "interactive": "INT",
    "environment": "ENV",
    "corner_case": "CC",
}
#: 前缀 → 合成起始序号（手写目录已占用 BAS 001-005 / INT 001-004 / ENV 001-002 / CC 001-004）
_ID_START: Final[dict[str, int]] = {"BAS": 6, "INT": 5, "ENV": 3, "CC": 5}
#: 行人步速上限（m/s，与 G7 真实性检查同口径）
PEDESTRIAN_SPEED_CAP: Final[float] = 3.0
#: 单条 IR 的绑定条数上限（SceneIR.bindings max_length=64，留余量）
_BINDINGS_MAX: Final[int] = 60
#: 默认地图（与手写目录一致，占位原点由仿真端替换）
_DEFAULT_MAP_ID: Final[str] = "Town04"
#: 默认成功判据（与手写目录默认值一致）
_DEFAULT_CRITERIA: Final[dict[str, Any]] = {
    "max_speed_deviation": 2.0,
    "no_collision": True,
    "min_safe_distance": 2.0,
}


# =====================================================================
# 语义 → base_config / bindings / constraints
# =====================================================================


def ego_speed_cap() -> float:
    """自车速度上限（m/s）：语义契约按乘用车口径声明，合成 IR 需按整车能力收敛，否则 G7 判不真实。"""
    return float(get_vehicle_params().max_speed)


def steer_cap_gain() -> float:
    """转向裁剪增益 ``k``：保证 ``v²·tan(|δ|·δ_max)/L ≤ a_max`` 恒成立（线性近似取 0.9 余量）。"""
    vehicle = get_vehicle_params()
    return 0.9 * vehicle.max_lateral_accel * vehicle.wheelbase / vehicle.max_steer_rad


def semantic_defaults(semantics: SceneSemantics) -> dict[str, float]:
    """语义参数默认取值（分布均值裁剪到上下界），用于构造可编辑的 base_config。"""
    return {
        name: min(max(float(spec.mean), float(spec.low)), float(spec.high))
        for name, spec in semantics.params.items()
    }


def pedestrian_speed_params(semantics: SceneSemantics) -> set[str]:
    """行人角色速度参数名（用于把上界收敛到 3 m/s 并加约束）。"""
    names: set[str] = set()
    for role in semantics.roles:
        if role.actor_type != "pedestrian":
            continue
        for key, expr in role.parameters.items():
            if not key.endswith("speed") or not isinstance(expr, str):
                continue
            used = expression_names(expr)
            if len(used) == 1 and next(iter(used)) in semantics.params:
                names |= used
    return names


def _count_expression(role: RoleSemantics, variables: Mapping[str, float]) -> int:
    """求角色 count 表达式（异常时退化为 1，保证目录可构造）。"""
    try:
        return max(0, round(evaluate(role.count, variables, where=f"roles.{role.role}.count")))
    except SemanticsError:
        return 1


def role_counts(
    semantics: SceneSemantics, resolved: Mapping[str, float], *, maximize: bool = True
) -> dict[str, int]:
    """角色实例数：``max`` 取「默认值 / 参数上界」两处的较大值，确保任意采样值都不缺实例。"""
    highs = {name: float(spec.high) for name, spec in semantics.params.items()}
    counts: dict[str, int] = {}
    for role in semantics.roles:
        count = max(int(role.min_count), _count_expression(role, resolved))
        if maximize:
            count = max(count, _count_expression(role, {**resolved, **highs}))
        counts[role.role] = max(1, count)
    return counts


def semantic_base_config(
    scene_type: str,
    *,
    ego_speed: float | None = None,
    ego_steer: float | None = None,
    duration: float | None = None,
    criteria: dict[str, Any] | None = None,
    map_id: str = _DEFAULT_MAP_ID,
    values: Mapping[str, float] | None = None,
    actor_extra: Mapping[int, Mapping[str, Any]] | None = None,
    event_extra: Mapping[int, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """语义契约 → 4.2.2 base_config（参与者/事件/天气/时长/自车同源）。

    ``actor_extra`` / ``event_extra`` 仅供手写 IR 追加语义未声明的自由参数键
    （如 ``actors[0].behavior.parameters.width``），避免为一两个旧参数另写一份平行事实源。
    """
    semantics = require_semantics(scene_type)
    resolved = resolve_parameters(semantics, values)
    counts = role_counts(semantics, resolved)
    ego = build_ego(semantics, resolved, model="HUNTER_SE")
    cap = ego_speed_cap()
    ego["initial_speed"] = round(min(max(0.0, float(ego["initial_speed"])), cap), 3)
    if ego_speed is not None:
        ego["initial_speed"] = round(min(max(0.0, float(ego_speed)), cap), 3)
    if ego_steer is not None:
        ego["initial_steer"] = round(max(-1.0, min(1.0, float(ego_steer))), 3)
    actors = build_actors(semantics, resolved, counts=counts)
    for index, extra in (actor_extra or {}).items():
        if 0 <= index < len(actors):
            actors[index]["behavior"]["parameters"].update(dict(extra))
    events = build_events(semantics, resolved)
    for index, extra in (event_extra or {}).items():
        if 0 <= index < len(events):
            events[index]["action"]["parameters"].update(dict(extra))
    return {
        "map": {
            "map_id": map_id,
            "map_type": "carla_town",
            "spawn_point": {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0, "lane_id": None},
        },
        "ego_vehicle": ego,
        "weather": build_weather(semantics, resolved),
        "actors": actors,
        "events": events,
        "success_criteria": dict(criteria or _DEFAULT_CRITERIA),
        "duration": round(
            float(duration) if duration is not None else build_duration(semantics, resolved), 1
        ),
    }


def _role_for_actor(actor_id: str, roles: Mapping[str, RoleSemantics]) -> RoleSemantics | None:
    """actor_id（``{role}-{序号}``）→ 语义角色（角色名可含 ``-``，取最长匹配）。"""
    matched: str | None = None
    for name in roles:
        if (actor_id == name or actor_id.startswith(f"{name}-")) and (
            matched is None or len(name) > len(matched)
        ):
            matched = name
    return roles.get(matched) if matched is not None else None


def _with_index(expr: str, index: int) -> str:
    """把表达式中的实例序号变量 ``i`` 替换为字面量（IR 绑定只认参数名）。"""
    return re.sub(r"\bi\b", str(index), expr)


def _ego_speed_param(semantics: SceneSemantics) -> str | None:
    """自车速度参数名（语义契约用 ``ego_speed``，兼容旧口径 ``v0``）。"""
    for candidate in ("ego_speed", "v0"):
        if candidate in semantics.params:
            return candidate
    return None


def _bindings(
    semantics: SceneSemantics, resolved: Mapping[str, float], actors: list[dict[str, Any]]
) -> list[BindingIR]:
    """语义 → IR 绑定（只绑定语义参数可表达的表达式，避免引用未知参数）。"""
    names = set(semantics.params)
    bindings: list[BindingIR] = []

    def add(path: str, expr: Any) -> None:
        if not isinstance(expr, str) or len(bindings) >= _BINDINGS_MAX:
            return
        text = expr.strip()
        if not text or len(text) > 128 or not expression_names(text) <= names:
            return
        if any(binding.path == path for binding in bindings):
            return
        bindings.append(BindingIR(path=path, expr=text))

    speed_param = _ego_speed_param(semantics)
    if speed_param:
        add("ego_vehicle.initial_speed", speed_param)
    steer_expr = semantics.ego_steer.strip()
    if speed_param and expression_names(steer_expr) & names:
        # 转向与速度同采样时按车端横向加速度能力裁剪，否则 G4 阿克曼检查必败
        limit = f"{steer_cap_gain():.4f} / max({speed_param} * {speed_param}, 1)"
        steer_expr = f"max(min({steer_expr}, {limit}), -{limit})"
    add("ego_vehicle.initial_steer", steer_expr)

    roles = {role.role: role for role in semantics.roles}
    instances: dict[str, int] = {}
    for index, actor in enumerate(actors):
        role = _role_for_actor(str(actor.get("actor_id") or ""), roles)
        if role is None:
            continue
        instance = instances.get(role.role, 0)
        instances[role.role] = instance + 1
        if instance == 0:  # 首个实例绑定生成点；后续实例随自身 i 变化，不参与绑定
            for axis in ("x", "y", "yaw"):
                expr = role.spawn.get(axis)
                if expr:
                    add(f"actors[{index}].spawn_point.{axis}", _with_index(str(expr), instance))
        for key, expr in role.parameters.items():
            if isinstance(expr, str):
                add(f"actors[{index}].behavior.parameters.{key}", _with_index(expr, instance))

    for field, expr in semantics.weather.items():
        add(f"weather.{field}", expr)
    add("duration", semantics.duration)
    return bindings


def _linear_terms(
    constraint: ConstraintSemantics, semantics: SceneSemantics
) -> tuple[dict[str, float], float] | None:
    """把 ``left op right`` 线性化为 ``Σ(coef·param) op value``（仅支持线性右端）。"""
    left = constraint.left.strip()
    if left not in semantics.params:
        return None
    right = str(constraint.right).strip()
    names = sorted(expression_names(right) & set(semantics.params))
    if not names:  # 常数右端：left op c
        try:
            return {left: 1.0}, -float(evaluate_expression(right, {}))
        except Exception:  # noqa: BLE001 - 无法求值的右端直接放弃约束
            return None
    try:
        constant = float(evaluate_expression(right, {name: 0.0 for name in semantics.params}))
        coefficients: dict[str, float] = {}
        for name in names:
            probe = {candidate: 0.0 for candidate in semantics.params}
            probe[name] = 1.0
            coefficients[name] = float(evaluate_expression(right, probe)) - constant
    except Exception:  # noqa: BLE001 - 非线性右端不做线性化（生成器采样阶段逐点校验）
        return None
    terms: dict[str, float] = {left: 1.0}
    for name, coefficient in coefficients.items():
        terms[name] = terms.get(name, 0.0) - coefficient
    trimmed = {name: round(value, 6) for name, value in terms.items() if abs(value) > 1e-9}
    if not trimmed:
        return None
    return trimmed, round(-constant, 6)


def _constraints(semantics: SceneSemantics) -> list[ConstraintIR]:
    """语义约束 → IR 约束（初始不碰撞 + 行人步速上限）。"""
    constraints: list[ConstraintIR] = []
    for index, constraint in enumerate(semantics.constraints, start=1):
        linear = _linear_terms(constraint, semantics)
        if linear is None:
            continue
        terms, value = linear
        constraints.append(
            ConstraintIR(
                name=f"semantics_{index}",
                terms=terms,
                op=constraint.op if constraint.op in {"<=", "<", ">=", ">"} else ">=",
                value=value,
                message=(constraint.message or "")[:160] or "语义约束",
            )
        )
    for name in sorted(pedestrian_speed_params(semantics)):
        constraints.append(
            ConstraintIR(
                name=f"pedestrian_speed_cap_{name}",
                terms={name: 1.0},
                op="<=",
                value=PEDESTRIAN_SPEED_CAP,
                message=f"行人步速 {name} ≤ {PEDESTRIAN_SPEED_CAP:.0f} m/s（G7 真实性）",
            )
        )
    return constraints


def _parameters(semantics: SceneSemantics) -> list[ParameterIR]:
    """语义参数分布 → ParameterIR（行人步速 ≤3 m/s、自车速度 ≤ 车端 v_max）。"""
    capped = pedestrian_speed_params(semantics)
    ego_param = _ego_speed_param(semantics)
    vehicle_cap = ego_speed_cap()
    parameters: list[ParameterIR] = []
    for name, spec in semantics.params.items():
        low, high = float(spec.low), float(spec.high)
        mean, std = float(spec.mean), float(spec.std)
        if name in capped:
            high = min(high, PEDESTRIAN_SPEED_CAP)
            mean = min(max(mean, low), high)
        if name == ego_param:
            # 车端 v_max 低于语义口径（乘用车尺度）时按整车能力收敛，保留可采样区间
            high = min(high, vehicle_cap)
            if low >= high:
                low = round(high * 0.5, 3)
            mean = min(max(mean, low), high)
            std = min(std, max((high - low) / 3.0, 1e-3))
        if spec.discrete or std <= 0 or high <= low:
            parameters.append(
                ParameterIR(
                    name=name,
                    distribution=DistributionIR(kind="uniform", params={"low": low, "high": max(low, high)}),
                    unit=spec.unit,
                    description=f"{name}（离散参数，语义契约声明）",
                )
            )
            continue
        parameters.append(
            ParameterIR(
                name=name,
                distribution=DistributionIR(
                    kind="truncated_normal",
                    params={"mean": mean, "std": std, "low": low, "high": high},
                ),
                unit=spec.unit,
                description=f"{name}（语义契约声明，risk={spec.risk}）",
            )
        )
    return parameters


def _conflict_gap_param(semantics: SceneSemantics) -> str | None:
    """临界度净距参数：优先取语义约束左端（即冲突纵向距离），否则取首个 gap/dist 类参数。"""
    for constraint in semantics.constraints:
        if constraint.left in semantics.params:
            return constraint.left
    for name in semantics.params:
        lowered = name.lower()
        if "gap" in lowered or lowered.startswith(("d_", "dist", "vis")):
            return name
    return None


def synthesize_ir(scene_type: str, logical_id: str) -> SceneIR:
    """按语义契约合成单条 SceneIR（参数分布 + 绑定 + 约束 + 判据引用）。"""
    semantics = require_semantics(scene_type)
    values = semantic_defaults(semantics)
    resolved = resolve_parameters(semantics, values)
    base_config = semantic_base_config(scene_type, values=resolved)
    actors = list(base_config["actors"])
    spec = get_scene_taxonomy().scene_types[scene_type]
    gap_param = _conflict_gap_param(semantics)
    return SceneIR(
        logical_id=logical_id,
        name=f"{spec.label}（语义合成）",
        category=spec.category,
        scene_type=scene_type,
        description=semantics.objective[:500],
        parameters=_parameters(semantics),
        constraints=_constraints(semantics),
        bindings=_bindings(semantics, resolved, actors),
        base_config=base_config,
        oracle_ref=semantics.oracle_ref,
        allowed_modes=["sil", "hil"],
        criticality=CriticalityIR(
            speed_param=_ego_speed_param(semantics),
            gap_param=gap_param,
            d_margin=0.5 if gap_param else 1.0,
        ),
        tags=[
            spec.category,
            scene_type,
            "semantics",
            *(role.role for role in semantics.roles[:4]),
        ],
    )


def _remaining_types() -> list[tuple[str, str]]:
    """待合成类型与其 logical_id（按受控字典顺序，分类内顺序编号）。"""
    taxonomy = get_scene_taxonomy()
    counters = dict(_ID_START)
    planned: list[tuple[str, str]] = []
    for scene_type, spec in taxonomy.scene_types.items():
        if scene_type in HANDWRITTEN_TYPES or spec.category not in _ID_PREFIX:
            continue
        prefix = _ID_PREFIX[spec.category]
        planned.append((scene_type, f"LS-{prefix}-{counters[prefix]:03d}"))
        counters[prefix] += 1
    return planned


#: 语义合成目录（手写目录之外的全部可合成类型）
CATALOG_SEMANTICS: tuple[SceneIR, ...] = tuple(
    synthesize_ir(scene_type, logical_id) for scene_type, logical_id in _remaining_types()
)


__all__ = [
    "CATALOG_SEMANTICS",
    "HANDWRITTEN_TYPES",
    "PEDESTRIAN_SPEED_CAP",
    "pedestrian_speed_params",
    "role_counts",
    "semantic_base_config",
    "semantic_defaults",
    "synthesize_ir",
]
