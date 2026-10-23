"""八道校验门（G1-G8）与双轴分级（稀有度 × 临界度）。

G1 语法/结构 · G2 参数 · G3 地图 · G4 阿克曼运动学 · G5 干跑 · G6 可避免性 ·
G7 真实性 · G8 去重（批级，由生成器在入批前执行并回填结果）。

分级口径（§2.3 / §2.8.1）：
- 临界度 ``CI = a_req / a_max``，``a_req = v² / (2·(d - d_m))``；
  C0 < 0.3 ≤ C1 < 0.7 ≤ C2 < 1.0 ≤ C3（``a_max`` 来自车端 vehicle_params）；
- 可避免性：CI < 0.1 → trivial_safe；< 0.7 → avoidable；≤ 1.0 → marginal；
  > 1.0 → unavoidable（§2.6：CI>1 表示仅靠制动不可避免）；
  **标注而非剔除**：unavoidable 样本进入待评审标记，不直接丢弃；
- 风险矩阵：稀有度 Z1-Z4 × 临界度 C0-C3 → L1-L4（见 ``RISK_MATRIX``）。
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from app.models.scene_engine import Avoidability, CriticalityLevel, RarityZone, RiskLevel
from app.scene_engine.esmini import EsminiRunner
from app.scene_engine.ir import SceneIR, is_finite_number
from app.scene_engine.stats import chi2_sf, mahalanobis_d2, rarity_zone
from app.scene_engine.vehicle_params import VehicleParams, get_vehicle_params
from app.schemas.scene import SceneConfig, ensure_scene_type

#: 风险矩阵：Z1-Z4 × C0-C3（行 = 稀有度，列 = 临界度；设计 §2.8.1）
RISK_MATRIX: dict[str, dict[str, str]] = {
    RarityZone.Z1.value: {"C0": "L1", "C1": "L1", "C2": "L2", "C3": "L3"},
    RarityZone.Z2.value: {"C0": "L1", "C1": "L2", "C2": "L3", "C3": "L3"},
    RarityZone.Z3.value: {"C0": "L2", "C1": "L3", "C2": "L3", "C3": "L4"},
    RarityZone.Z4.value: {"C0": "L2", "C1": "L3", "C2": "L4", "C3": "L4"},
}

#: 各参数类型的合理性上限（G7；自车速度上限另取 vehicle_params.max_speed）
SPEED_LIMIT = 60.0
DECEL_LIMIT = 10.0
#: 行人/VRU 步速上限（G7，§2.3.4 ODD）
PEDESTRIAN_SPEED_LIMIT = 3.0


@dataclass(frozen=True, slots=True)
class GateResult:
    """单道门的结论。"""

    gate: str
    passed: bool
    message: str
    skipped: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate,
            "passed": self.passed,
            "message": self.message,
            "skipped": self.skipped,
        }


@dataclass(slots=True)
class ValidationReport:
    """G1-G7 的汇总（G8 由生成器追加）。"""

    gates: list[GateResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(gate.passed for gate in self.gates if not gate.skipped)

    @property
    def failures(self) -> list[str]:
        return [f"{gate.gate}: {gate.message}" for gate in self.gates if not gate.passed]

    def add(self, gate: str, passed: bool, message: str, *, skipped: bool = False) -> None:
        self.gates.append(GateResult(gate=gate, passed=passed, message=message, skipped=skipped))

    def to_dict(self) -> dict[str, Any]:
        return {
            "gates": [gate.to_dict() for gate in self.gates],
            "passed": self.passed,
            "failures": self.failures,
        }


# =====================================================================
# 临界度 / 可避免性 / 风险
# =====================================================================


def criticality_index(
    ir: SceneIR,
    values: dict[str, float | str],
    vehicle: VehicleParams | None = None,
) -> float:
    """CI = a_req / a_max（缺速度/间距参数时为 0；a_max 取车端 vehicle_params）。"""
    speed_param = ir.criticality.speed_param
    gap_param = ir.criticality.gap_param
    if not speed_param or not gap_param:
        return 0.0
    speed = values.get(speed_param)
    gap = values.get(gap_param)
    if not isinstance(speed, (int, float)) or not isinstance(gap, (int, float)):
        return 0.0
    clearance = float(gap) - float(ir.criticality.d_margin)
    if clearance <= 1e-3:
        return float("inf")
    required = float(speed) ** 2 / (2.0 * clearance)
    max_decel = (vehicle or get_vehicle_params()).max_decel
    return max(required / max_decel, 0.0)


def criticality_level(ci: float) -> str:
    """CI → C0-C3（inf 归入 C3；设计 §2.8.1）。"""
    if ci < 0.3:
        return CriticalityLevel.C0.value
    if ci < 0.7:
        return CriticalityLevel.C1.value
    if ci < 1.0:
        return CriticalityLevel.C2.value
    return CriticalityLevel.C3.value


def avoidability_label(ci: float) -> str:
    """可避免性标签（§2.4）；CI>1 表示仅靠制动不可避免。"""
    if ci < 0.1:
        return Avoidability.TRIVIAL_SAFE.value
    if ci < 0.7:
        return Avoidability.AVOIDABLE.value
    if ci <= 1.0:
        return Avoidability.MARGINAL.value
    return Avoidability.UNAVOIDABLE.value


def risk_level(zone: str, level: str) -> str:
    """稀有度 × 临界度 → L1-L4。"""
    return RISK_MATRIX.get(zone, {}).get(level, RiskLevel.L1.value)


def assess_rarity(
    ir: SceneIR, values: dict[str, float | str]
) -> tuple[float, str, float]:
    """返回 (p_rar, zone, d_M²)。仅数值参数参与马氏距离。"""
    numeric = ir.numeric_parameters
    names = [parameter.name for parameter in numeric]
    vector = [float(values[name]) for name in names]
    means = [_moments(ir, name)[0] for name in names]
    stds = [_moments(ir, name)[1] for name in names]
    d2, _ = mahalanobis_d2(vector, means, stds, ir.correlation_pairs, names)
    p_value = chi2_sf(d2, len(names)) if names else 1.0
    return p_value, rarity_zone(p_value), d2


def _moments(ir: SceneIR, name: str) -> tuple[float, float]:
    from app.scene_engine.stats import distribution_moments

    parameter = ir.parameter_map[name]
    return distribution_moments(parameter.distribution.kind, parameter.distribution.params)


# =====================================================================
# 校验门
# =====================================================================


def _check_constraints(ir: SceneIR, values: dict[str, float | str]) -> str | None:
    for constraint in ir.constraints:
        total = 0.0
        for name, coefficient in constraint.terms.items():
            value = values.get(name)
            if not isinstance(value, (int, float)):
                return f"约束 {constraint.name} 引用的参数 {name} 非数值"
            total += coefficient * float(value)
        satisfied = {
            "<=": total <= constraint.value + 1e-9,
            "<": total < constraint.value,
            ">=": total >= constraint.value - 1e-9,
            ">": total > constraint.value,
        }[constraint.op]
        if not satisfied:
            return constraint.message or f"违反约束 {constraint.name}（{total:.3f} {constraint.op} {constraint.value}）"
    return None


def _check_parameter_values(ir: SceneIR, values: dict[str, float | str]) -> str | None:
    for parameter in ir.parameters:
        value = values.get(parameter.name)
        distribution = parameter.distribution
        if distribution.kind == "categorical":
            choices = {str(item["value"]) for item in distribution.params.get("choices", [])}
            if str(value) not in choices:
                return f"参数 {parameter.name}={value!r} 不在分类取值内"
            continue
        if not isinstance(value, (int, float)) or not is_finite_number(float(value)):
            return f"参数 {parameter.name} 非有限数值"
        low = distribution.params.get("low", distribution.params.get("min"))
        high = distribution.params.get("high", distribution.params.get("max"))
        if low is not None and float(value) < float(low) - 1e-9:
            return f"参数 {parameter.name}={value:.3f} 低于下界 {low}"
        if high is not None and float(value) > float(high) + 1e-9:
            return f"参数 {parameter.name}={value:.3f} 高于上界 {high}"
    return None


def _out_of_bounds(
    config: dict[str, Any], bounds: tuple[float, float, float, float] | None
) -> str | None:
    """G3 几何围栏：自车与全部参与者生成点必须在部署地图范围内。"""
    if bounds is None:
        return None
    xmin, ymin, xmax, ymax = bounds
    map_section = config.get("map") or {}
    base = map_section.get("spawn_point") or {}
    points: list[tuple[str, dict[str, Any]]] = [("ego", base)]
    for index, actor in enumerate(config.get("actors") or []):
        points.append((str(actor.get("actor_id") or f"actor_{index}"), actor.get("spawn_point") or {}))
    for name, point in points:
        x = _number_or_none(point.get("x"))
        y = _number_or_none(point.get("y"))
        if x is None or y is None:
            return f"G3: {name} 生成点非数值"
        if name != "ego":
            x += _number_or_none(base.get("x")) or 0.0
            y += _number_or_none(base.get("y")) or 0.0
        if not (xmin <= x <= xmax and ymin <= y <= ymax):
            return (
                f"G3: {name} 生成点 ({x:.1f}, {y:.1f}) 超出地图围栏 "
                f"[{xmin:.1f}, {ymin:.1f}, {xmax:.1f}, {ymax:.1f}]"
            )
    return None


def _number_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if is_finite_number(number) else None


def _check_kinematics(
    ir: SceneIR,
    values: dict[str, float | str],
    config: dict[str, Any],
    vehicle: VehicleParams,
) -> str | None:
    ego = config.get("ego_vehicle") or {}
    speed = float(ego.get("initial_speed", 0.0))
    steer = float(ego.get("initial_steer", 0.0))
    angle = abs(steer) * vehicle.max_steer_rad
    yaw_rate = speed / vehicle.wheelbase * math.tan(angle)
    lateral_accel = speed * yaw_rate
    if lateral_accel > vehicle.max_lateral_accel + 1e-9:
        return (
            f"横向加速度 {lateral_accel:.2f} m/s² 超限 "
            f"{vehicle.max_lateral_accel:.2f}（v={speed:.2f}, δ={angle:.3f} rad）"
        )
    return None


def run_gates(
    ir: SceneIR,
    values: dict[str, float | str],
    config: dict[str, Any],
    *,
    max_duration: float,
    allowed_maps: frozenset[str] | None = None,
    vehicle: VehicleParams | None = None,
    bounds: tuple[float, float, float, float] | None = None,
    esmini: EsminiRunner | None = None,
) -> ValidationReport:
    """执行 G1-G7（G8 由生成器在批级追加；物理能力取 vehicle_params）。"""
    resolved_vehicle = vehicle or get_vehicle_params()
    report = ValidationReport()

    # G1 语法/结构：IR 词表 + SceneConfig 严格解析
    try:
        ensure_scene_type(ir.scene_type)
        SceneConfig.model_validate(config)
        report.add("G1", True, "IR/SceneConfig 结构与受控词表校验通过")
    except (ValidationError, ValueError) as exc:
        report.add("G1", False, f"结构/词表校验失败: {exc}")
        return report

    # G2 参数合法性：取值范围 + 线性约束
    problem = _check_parameter_values(ir, values) or _check_constraints(ir, values)
    report.add("G2", problem is None, problem or "参数范围与约束满足")

    # G3 地图：非空、白名单（配置）、生成点有限 + 几何围栏
    map_section = config.get("map") or {}
    map_id = str(map_section.get("map_id", ""))
    spawn = map_section.get("spawn_point") or {}
    spawn_ok = all(
        _number_or_none(spawn.get(key)) is not None for key in ("x", "y", "z", "yaw")
    )
    out_of_bounds = _out_of_bounds(config, bounds)
    map_ok = bool(map_id) and spawn_ok and out_of_bounds is None and (
        allowed_maps is None or map_id in allowed_maps
    )
    if map_ok:
        map_message = f"地图 {map_id!r} 有效"
    elif out_of_bounds is not None:
        map_message = out_of_bounds
    else:
        map_message = f"地图 {map_id!r} 不在允许集合或生成点非法"
    report.add("G3", map_ok, map_message)

    # G4 阿克曼运动学（轴距/最大转角取车端 vehicle_params）
    kinematic_problem = _check_kinematics(ir, values, config, resolved_vehicle)
    report.add(
        "G4",
        kinematic_problem is None,
        kinematic_problem
        or f"阿克曼运动学可行（vehicle={resolved_vehicle.profile}）",
    )

    # G5 干跑：静态边界 + 可选 esmini 实机加载推进
    duration = float(config.get("duration", 0.0))
    actor_ids = [actor.get("actor_id") for actor in config.get("actors") or []]
    event_ids = [event.get("event_id") for event in config.get("events") or []]
    dry_run_problem = None
    if not 0 < duration <= max_duration:
        dry_run_problem = f"时长 {duration:.1f}s 超出 (0, {max_duration:.0f}]"
    elif len(set(actor_ids)) != len(actor_ids) or len(set(event_ids)) != len(event_ids):
        dry_run_problem = "actor_id / event_id 重复"
    if dry_run_problem is not None:
        report.add("G5", False, dry_run_problem)
    elif esmini is None:
        report.add("G5", True, f"静态干跑通过（duration={duration:.1f}s）")
    else:
        outcome = esmini.run(config)
        report.add(
            "G5",
            outcome.passed,
            outcome.message,
            skipped=outcome.skipped,
        )

    # G6 可避免性（标注而非剔除）：只在标签为 unavoidable 时标记待评审
    ci = criticality_index(ir, values, resolved_vehicle)
    label = avoidability_label(ci)
    report.add(
        "G6",
        True,
        f"CI={ci:.3f}，可避免性={label}" + ("（待评审）" if label == Avoidability.UNAVOIDABLE.value else ""),
    )

    # G7 真实性：自车速度取 vehicle_params.max_speed；行人 ≤3m/s；减速度上限
    speed = float((config.get("ego_vehicle") or {}).get("initial_speed", 0.0))
    realism_problem = None
    if speed < 0 or speed > resolved_vehicle.max_speed + 1e-9:
        realism_problem = (
            f"自车速度 {speed:.2f} m/s 超出 [0, {resolved_vehicle.max_speed:.2f}]"
        )
    for actor in config.get("actors") or []:
        behavior = actor.get("behavior") or {}
        parameters = behavior.get("parameters") or {}
        target_speed = parameters.get("target_speed")
        # SceneConfig 的参与者类型字段名为 type（4.2.2 节），兼容旧写法 actor_type
        actor_type = str(actor.get("type") or actor.get("actor_type") or "").lower()
        limit = (
            PEDESTRIAN_SPEED_LIMIT
            if "pedestrian" in actor_type or "vru" in actor_type
            else SPEED_LIMIT
        )
        if isinstance(target_speed, (int, float)) and not 0 <= float(target_speed) <= limit:
            realism_problem = (
                f"参与者 {actor.get('actor_id')} 目标速度 {target_speed} 超出 [0, {limit:.0f}]"
            )
            break
        deceleration = parameters.get("deceleration")
        if isinstance(deceleration, (int, float)) and not 0 <= float(deceleration) <= DECEL_LIMIT:
            realism_problem = f"参与者 {actor.get('actor_id')} 减速度 {deceleration} 超限"
            break
    report.add("G7", realism_problem is None, realism_problem or "参数真实性检查通过")

    return report


# =====================================================================
# 内容哈希 / 命名 / 有效期标记
# =====================================================================


def content_hash(logical_id: str, logical_version: str, values: dict[str, float | str]) -> str:
    """具体场景内容哈希（参数四舍五入到 6 位，保证跨进程稳定去重）。"""
    canonical = {
        "logical_id": logical_id,
        "logical_version": logical_version,
        "params": {
            name: round(float(value), 6) if isinstance(value, (int, float)) else str(value)
            for name, value in sorted(values.items())
        },
    }
    payload = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ir_content_hash(ir: SceneIR, version: str | None = None) -> str:
    """IR 内容哈希（目录同步幂等键；``version`` 参与哈希以区分同一 IR 的不同版本）。"""
    payload_dict: dict[str, Any] = {"ir": ir.model_dump(mode="json")}
    if version is not None:
        payload_dict["version"] = version
    payload = json.dumps(
        payload_dict, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "RISK_MATRIX",
    "GateResult",
    "ValidationReport",
    "assess_rarity",
    "avoidability_label",
    "content_hash",
    "criticality_index",
    "criticality_level",
    "ir_content_hash",
    "risk_level",
    "run_gates",
    "utcnow_iso",
]
