"""CARLA / OpenSCENARIO 执行映射（场景内容 → 可执行语义）。

设计约束：**禁止静默降级**。控制字典里的 ``behavior_type`` / ``action_type`` /
``condition_type`` 凡是无法映射为 OpenSCENARIO 1.2 标准元素或 CARLA ScenarioRunner
行为的组合，必须通过 :func:`check_executable` 显式报告，并在导出产物里以注释保留
原始信息（不伪造语义、不默认当直行）。

- ``BEHAVIOR_MAP``：参与者行为 → (CARLA 控制器/ScenarioRunner 行为, OSC 元素)
- ``ACTION_MAP``：事件动作 → OSC 动作元素（``None`` 表示暂无等价物）
- ``CONDITION_MAP``：触发条件 → OSC 条件元素（``None`` 表示暂无等价物）

XML 片段构造函数（``condition_lines`` / ``action_lines``）供 G5 干跑（``esmini.py``）
与导出（``serializers.py``）共用，保证「校验说的」与「导出的」是同一份事实。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

__all__ = [
    "ACTION_MAP",
    "BEHAVIOR_MAP",
    "CONDITION_MAP",
    "BehaviorMapping",
    "ExecutionReport",
    "action_lines",
    "check_executable",
    "condition_lines",
]


@dataclass(frozen=True, slots=True)
class BehaviorMapping:
    """单个行为的执行映射。"""

    carla_behavior: str
    """CARLA ScenarioRunner 行为/控制器名（HunterSim 侧对照用）。"""

    osc_action: str
    """OpenSCENARIO 1.2 顶层动作元素名。"""


#: 参与者行为映射（与 ``scene-taxonomy.yaml::behavior_types`` 一一对应）。
BEHAVIOR_MAP: Final[dict[str, BehaviorMapping]] = {
    "accelerate": BehaviorMapping("Accelerate", "SpeedAction"),
    "aggressive_merge": BehaviorMapping("AggressiveMerge", "LaneChangeAction"),
    "backtrack": BehaviorMapping("Backtrack", "FollowTrajectoryAction"),
    "boarding": BehaviorMapping("Boarding", "PositionAction"),
    "borrow_oncoming": BehaviorMapping("BorrowOncoming", "LaneOffsetAction"),
    "breakdown_stop": BehaviorMapping("BreakdownStop", "SpeedAction"),
    "car_following": BehaviorMapping("CarFollowing", "SpeedAction"),
    "child_chase_ball": BehaviorMapping("ChildChaseBall", "WalkAction"),
    "constant_velocity": BehaviorMapping("ConstantVelocity", "SpeedAction"),
    "continuous_lane_change": BehaviorMapping("ContinuousLaneChange", "LaneChangeAction"),
    "creep": BehaviorMapping("Creep", "SpeedAction"),
    "cross_road": BehaviorMapping("CrossRoad", "WalkAction"),
    "cut_in": BehaviorMapping("CutIn", "LaneChangeAction"),
    "cut_out": BehaviorMapping("CutOut", "LaneChangeAction"),
    "decelerate": BehaviorMapping("Decelerate", "SpeedAction"),
    "decelerate_detour": BehaviorMapping("DecelerateDetour", "LaneOffsetAction"),
    "detour": BehaviorMapping("Detour", "LaneOffsetAction"),
    "door_open": BehaviorMapping("DoorOpen", "PositionAction"),
    "double_park": BehaviorMapping("DoublePark", "PositionAction"),
    "drift_off_lane": BehaviorMapping("DriftOffLane", "LaneOffsetAction"),
    "emergency_brake": BehaviorMapping("EmergencyBrake", "SpeedAction"),
    "forward": BehaviorMapping("Forward", "SpeedAction"),
    "hesitation_stop": BehaviorMapping("HesitationStop", "SpeedAction"),
    "lane_change_accelerate": BehaviorMapping("LaneChangeAccelerate", "LaneChangeAction"),
    "lane_change_left": BehaviorMapping("LaneChangeLeft", "LaneChangeAction"),
    "lane_change_right": BehaviorMapping("LaneChangeRight", "LaneChangeAction"),
    "lane_follow": BehaviorMapping("LaneFollow", "SpeedAction"),
    "lane_keep": BehaviorMapping("LaneKeep", "LaneOffsetAction"),
    "occluded_dash": BehaviorMapping("OccludedDash", "WalkAction"),
    "pushing": BehaviorMapping("Pushing", "PositionAction"),
    "reverse": BehaviorMapping("Reverse", "SpeedAction"),
    "start_up": BehaviorMapping("StartUp", "SpeedAction"),
    "static": BehaviorMapping("Static", "PositionAction"),
    "stop": BehaviorMapping("Stop", "SpeedAction"),
    "sudden_cross": BehaviorMapping("SuddenCross", "WalkAction"),
    "trajectory_replay": BehaviorMapping("TrajectoryReplay", "FollowTrajectoryAction"),
    "turn_left": BehaviorMapping("TurnLeft", "LaneChangeAction"),
    "turn_right": BehaviorMapping("TurnRight", "LaneChangeAction"),
    "turn_yield": BehaviorMapping("TurnYield", "SpeedAction"),
    "uturn": BehaviorMapping("UTurn", "LaneChangeAction"),
    "yield": BehaviorMapping("Yield", "SpeedAction"),
}

#: 事件动作 → OpenSCENARIO 动作元素（``None`` = 无标准等价物，需显式报告）。
ACTION_MAP: Final[dict[str, str | None]] = {
    "acceleration": "AccelerationAction",
    "activate_controller": "ActivateControllerAction",
    "assign_controller": "ActivateControllerAction",
    "brake": "SpeedAction",
    "degrade_control": None,
    "emergency_stop": "SpeedAction",
    "environment": "EnvironmentAction",
    "follow_trajectory": None,
    "hold_speed": "SpeedAction",
    "infrastructure": "InfrastructureAction",
    "lane_change": "LaneChangeAction",
    "lane_offset": "LaneOffsetAction",
    "longitudinal_distance": None,
    "manual_takeover": None,
    "parameter_assignment": "ParameterSetAction",
    "position": "TeleportAction",
    "speed": "SpeedAction",
    "speed_change": "SpeedAction",
    "speed_limit": None,
    "stop": "SpeedAction",
    "stop_trigger": None,
    "teleport": "TeleportAction",
    "traffic_light": "TrafficSignalStateAction",
    "user_defined": None,
    "visibility": "EnvironmentAction",
    "weather": "EnvironmentAction",
    "yield": "SpeedAction",
}

#: 触发条件 → OpenSCENARIO 条件元素（``None`` = 无标准等价物，需显式报告）。
CONDITION_MAP: Final[dict[str, str | None]] = {
    "acceleration": "AccelerationCondition",
    "collision": "CollisionCondition",
    "distance": "TraveledDistanceCondition",
    "end_of_road": "EndOfRoadCondition",
    "offroad": "OffroadCondition",
    "parameter_value": "ParameterCondition",
    "reach_position": "ReachPositionCondition",
    "relative_distance": "RelativeDistanceCondition",
    "simulation_time": "SimulationTimeCondition",
    "speed": "SpeedCondition",
    "standstill": "StandStillCondition",
    "storyboard_element_state": None,
    "time_headway": "TimeHeadwayCondition",
    "time_to_collision": "TimeToCollisionCondition",
    "traveled_distance": "TraveledDistanceCondition",
    "user_defined_value": None,
}

#: 规则符号 → OpenSCENARIO ``rule`` 枚举
_RULE_MAP: Final[dict[str, str]] = {
    "<": "lessThan",
    "<=": "lessOrEqual",
    ">": "greaterThan",
    ">=": "greaterOrEqual",
    "==": "equalTo",
    "=": "equalTo",
    "lessThan": "lessThan",
    "lessOrEqual": "lessOrEqual",
    "greaterThan": "greaterThan",
    "greaterOrEqual": "greaterOrEqual",
    "equalTo": "equalTo",
}

#: 事件里可省略的「起始速度」参数名（按优先级）
_SPEED_KEYS: Final[tuple[str, ...]] = ("target_speed", "speed", "value", "crossing_speed")

#: 动物类 kind 取值：与仿真端 ``scene_mapper._ANIMAL_KINDS``（animal/dog/cat/deer）对齐，
#: 并补上本仓 taxonomy 实际使用的 cattle / livestock（``animal_crossing.animal_kind`` 的
#: select 取值）。``parameters.animal_kind`` 非空即视为动物（仿真端口径一致）。
_ANIMAL_KINDS: Final[frozenset[str]] = frozenset(
    {"animal", "dog", "cat", "deer", "cattle", "livestock"}
)


def _animal_kind(parameters: Mapping[str, Any]) -> str | None:
    """参与者的动物 kind（非动物返回 None；口径与仿真端 animal_asset_missing 判定一致）。"""
    animal_kind = str(parameters.get("animal_kind") or "").strip().lower()
    if animal_kind:
        return animal_kind
    object_kind = str(parameters.get("object_kind") or "").strip().lower()
    return object_kind if object_kind in _ANIMAL_KINDS else None


@dataclass(frozen=True, slots=True)
class ExecutionReport:
    """可执行性自检结论（``unsupported`` 非空即视为该场景不可完整执行）。"""

    unsupported: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def executable(self) -> bool:
        return not self.unsupported

    def to_dict(self) -> dict[str, list[str]]:
        return {"unsupported": list(self.unsupported), "warnings": list(self.warnings)}


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt(value: Any, default: float = 0.0) -> str:
    return f"{_number(value, default):.3f}"


def _rule(parameters: Mapping[str, Any], default: str = "lessThan") -> str:
    raw = str(parameters.get("rule", "")).strip()
    return _RULE_MAP.get(raw, default)


def _first(parameters: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in parameters and parameters[key] is not None:
            return parameters[key]
    return None


def check_executable(config: Mapping[str, Any]) -> ExecutionReport:
    """扫描 SceneConfig，返回不可执行/需注意的组合（不修改配置）。"""
    unsupported: list[str] = []
    warnings: list[str] = []
    actors = list(config.get("actors") or [])
    events = list(config.get("events") or [])

    for index, actor in enumerate(actors):
        behavior = actor.get("behavior") or {}
        behavior_type = str(behavior.get("behavior_type") or "")
        actor_id = str(actor.get("actor_id") or f"actor_{index}")
        mapping = BEHAVIOR_MAP.get(behavior_type)
        if mapping is None:
            unsupported.append(f"参与者 {actor_id} 的行为 {behavior_type!r} 未定义执行映射")
            continue
        actor_type = str(actor.get("type") or "").strip().lower()
        # V1.27.0：WalkAction 告警只对「车辆」发——type=other（动物/锥桶/碎片等）按行人/
        # 步行动作执行属正常（动物穿行 sudden_cross、碎皮遗洒等），旧口径把 animal-1
        # 误报成「参与者为车辆但行为按行人执行」。
        if mapping.osc_action == "WalkAction" and actor_type == "vehicle":
            warnings.append(f"参与者 {actor_id} 为车辆但行为 {behavior_type!r} 按行人执行")
        if mapping.osc_action == "FollowTrajectoryAction":
            unsupported.append(
                f"参与者 {actor_id} 的行为 {behavior_type!r} 需要显式轨迹，当前配置未提供"
            )
        # V1.27.0：动物类参与者显式报告资产降级（与仿真端 animal_asset_missing 同口径）——
        # CARLA 0.9.16 无动物资产，仿真端会降级为行人执行；平台侧同步留痕，避免「下发成功
        # 但画面是行人」被当成静默降级。
        kind = _animal_kind(behavior.get("parameters") or {})
        if kind is not None and actor_type == "other":
            warnings.append(
                f"参与者 {actor_id} 为动物（kind={kind}）：CARLA 0.9.16 无动物资产，"
                f"仿真端将降级为行人执行（animal_asset_missing）"
            )

    for index, event in enumerate(events):
        action = event.get("action") or {}
        action_type = str(action.get("action_type") or "")
        event_id = str(event.get("event_id") or f"event_{index}")
        if action_type not in ACTION_MAP:
            unsupported.append(f"事件 {event_id} 的动作 {action_type!r} 不在受控字典内")
        elif ACTION_MAP[action_type] is None:
            unsupported.append(f"事件 {event_id} 的动作 {action_type!r} 无 OpenSCENARIO 等价物")
        trigger = event.get("trigger") or {}
        condition_type = trigger.get("condition_type")
        if condition_type is None:
            warnings.append(f"事件 {event_id} 未声明 condition_type，按仿真时间 0s 触发")
        elif str(condition_type) not in CONDITION_MAP:
            unsupported.append(f"事件 {event_id} 的触发 {condition_type!r} 不在受控字典内")
        elif CONDITION_MAP[str(condition_type)] is None:
            unsupported.append(
                f"事件 {event_id} 的触发 {condition_type!r} 无 OpenSCENARIO 等价物"
            )

    if not events:
        warnings.append("场景无事件：仅验证初始位姿与巡航，无法触发交互动作")
    return ExecutionReport(tuple(unsupported), tuple(warnings))


def _condition_headers(name: str) -> list[str]:
    return [f'<Condition name="{name}" delay="0" conditionEdge="none">']


def _by_entity(entity_ref: str, condition_xml: list[str], *, name: str) -> list[str]:
    lines = _condition_headers(name)
    lines.append("<ByEntityCondition>")
    lines.append('<TriggeringEntities triggeringEntitiesRule="any">')
    lines.append(f'<EntityRef entityRef="{entity_ref}"/>')
    lines.append("</TriggeringEntities>")
    lines.append("<EntityCondition>")
    lines.extend(condition_xml)
    lines.append("</EntityCondition>")
    lines.append("</ByEntityCondition>")
    lines.append("</Condition>")
    return lines


def _by_value(condition_xml: list[str], *, name: str) -> list[str]:
    lines = _condition_headers(name)
    lines.append("<ByValueCondition>")
    lines.extend(condition_xml)
    lines.append("</ByValueCondition>")
    lines.append("</Condition>")
    return lines


def condition_lines(
    *,
    condition_type: str | None,
    parameters: Mapping[str, Any],
    name: str,
    ego_ref: str = "ego",
    actor_refs: Sequence[str] = (),
) -> list[str]:
    """触发条件 → ``<Condition>`` 片段（无法映射时返回注释行，保持文档合法）。"""
    kind = CONDITION_MAP.get(str(condition_type)) if condition_type is not None else None
    value = _first(parameters, ("value", "threshold", "distance", "duration"))
    rule = _rule(parameters)
    target = str(parameters.get("target") or (actor_refs[0] if actor_refs else "")) or None

    if kind is None:
        reason = "未声明 condition_type" if not condition_type else f"{condition_type} 无等价物"
        return [f"<!-- x-hunter-condition: {reason} -->"]

    if kind == "SimulationTimeCondition":
        return _by_value(
            [f'<SimulationTimeCondition value="{_fmt(value)}" rule="{rule}"/>'],
            name=name,
        )
    if kind in {"SpeedCondition", "AccelerationCondition"}:
        return _by_entity(
            ego_ref,
            [f'<{kind} value="{_fmt(value)}" rule="{rule}"/>'],
            name=name,
        )
    if kind == "RelativeDistanceCondition":
        if target is None:
            return [f"<!-- x-hunter-condition: relative_distance 缺少目标实体（{name}） -->"]
        distance_type = str(parameters.get("relative_distance_type") or "longitudinal")
        return _by_entity(
            ego_ref,
            [
                (
                    f'<RelativeDistanceCondition entityRef="{target}" '
                    f'relativeDistanceType="{distance_type}" value="{_fmt(value)}" '
                    f'freespace="false" rule="{rule}"/>'
                )
            ],
            name=name,
        )
    if kind == "TimeToCollisionCondition":
        if target is None:
            return [f"<!-- x-hunter-condition: time_to_collision 缺少目标实体（{name}） -->"]
        return _by_entity(
            ego_ref,
            [
                f'<TimeToCollisionCondition value="{_fmt(value)}" freespace="false" rule="{rule}">',
                "<TimeToCollisionConditionTarget>",
                f'<EntityRef entityRef="{target}"/>',
                "</TimeToCollisionConditionTarget>",
                "</TimeToCollisionCondition>",
            ],
            name=name,
        )
    if kind == "TimeHeadwayCondition":
        if target is None:
            return [f"<!-- x-hunter-condition: time_headway 缺少目标实体（{name}） -->"]
        return _by_entity(
            ego_ref,
            [
                (
                    f'<TimeHeadwayCondition entityRef="{target}" value="{_fmt(value)}" '
                    f'freespace="false" alongRoute="true" rule="{rule}"/>'
                )
            ],
            name=name,
        )
    if kind == "CollisionCondition":
        if target is None:
            return [f"<!-- x-hunter-condition: collision 缺少目标实体（{name}） -->"]
        return _by_entity(
            ego_ref,
            [f'<CollisionCondition><EntityRef entityRef="{target}"/></CollisionCondition>'],
            name=name,
        )
    if kind == "StandStillCondition":
        return _by_entity(
            ego_ref, [f'<StandStillCondition duration="{_fmt(value)}"/>'], name=name
        )
    if kind in {"TraveledDistanceCondition", "EndOfRoadCondition", "OffroadCondition"}:
        attribute = "value" if kind == "TraveledDistanceCondition" else "duration"
        return _by_entity(
            ego_ref, [f'<{kind} {attribute}="{_fmt(value)}"/>'], name=name
        )
    if kind == "ParameterCondition":
        parameter_ref = str(parameters.get("parameter_ref") or "duration")
        return _by_value(
            [
                (
                    f'<ParameterCondition parameterRef="{parameter_ref}" '
                    f'value="{_fmt(value)}" rule="{rule}"/>'
                )
            ],
            name=name,
        )
    if kind == "ReachPositionCondition":
        tolerance = _fmt(_first(parameters, ("tolerance", "value")), 0.5)
        x = _fmt(_first(parameters, ("x",)), 0.0)
        y = _fmt(_first(parameters, ("y",)), 0.0)
        return _by_entity(
            ego_ref,
            [
                f'<ReachPositionCondition tolerance="{tolerance}">',
                f'<Position><WorldPosition x="{x}" y="{y}" z="0.000" h="0.000"/></Position>',
                "</ReachPositionCondition>",
            ],
            name=name,
        )
    return [f"<!-- x-hunter-condition: {condition_type} 暂无片段构造器 -->"]


def _speed_action(target: float, *, deceleration: float | None = None) -> list[str]:
    dynamics = (
        f'<SpeedActionDynamics dynamicsShape="linear" value="{_fmt(deceleration)}" '
        'dynamicsDimension="rate"/>'
        if deceleration is not None
        else '<SpeedActionDynamics dynamicsShape="step" value="0" dynamicsDimension="time"/>'
    )
    return [
        "<LongitudinalAction>",
        "<SpeedAction>",
        dynamics,
        "<SpeedActionTarget>",
        f'<AbsoluteTargetSpeed value="{_fmt(target)}"/>',
        "</SpeedActionTarget>",
        "</SpeedAction>",
        "</LongitudinalAction>",
    ]


def _lane_change_action(parameters: Mapping[str, Any]) -> list[str]:
    direction = str(parameters.get("direction") or "left").lower()
    lane = 1 if direction == "left" else -1
    duration = _fmt(_first(parameters, ("duration", "value")), 1.0)
    return [
        "<LateralAction>",
        "<LaneChangeAction>",
        (
            f'<LaneChangeActionDynamics dynamicsShape="linear" value="{duration}" '
            'dynamicsDimension="time"/>'
        ),
        "<LaneChangeTarget>",
        f'<RelativeTargetLane entityRef="ego" value="{lane}"/>',
        "</LaneChangeTarget>",
        "</LaneChangeAction>",
        "</LateralAction>",
    ]


def action_lines(action_type: str, parameters: Mapping[str, Any]) -> list[str]:
    """事件动作 → ``<PrivateAction>`` 内部片段（无法映射时返回注释行）。"""
    kind = ACTION_MAP.get(action_type)
    target = _first(parameters, _SPEED_KEYS)
    deceleration = _first(parameters, ("deceleration", "rate"))

    if kind is None:
        return [f"<!-- x-hunter-action: {action_type} 无 OpenSCENARIO 等价物 -->"]
    if kind == "SpeedAction":
        target_speed = _number(target, 0.0) if target is not None else 0.0
        if action_type in {"speed", "hold_speed", "speed_change", "acceleration", "yield"}:
            if action_type == "yield" and target is None:
                target_speed = 0.0
            return _speed_action(target_speed, deceleration=deceleration)
        return _speed_action(0.0, deceleration=deceleration)
    if kind == "AccelerationAction":
        shape = str(parameters.get("shape") or "linear")
        return [
            "<LongitudinalAction>",
            f'<AccelerationAction value="{_fmt(target)}" shape="{shape}"/>',
            "</LongitudinalAction>",
        ]
    if kind == "LaneChangeAction":
        return _lane_change_action(parameters)
    if kind == "LaneOffsetAction":
        offset = _number(_first(parameters, ("lane_offset", "offset", "value")), 0.0)
        return [
            "<LateralAction>",
            '<LaneOffsetAction continuous="true">',
            '<LaneOffsetActionDynamics dynamicsShape="sinusoidal" maxLateralAcc="2.0"/>',
            "<LaneOffsetTarget>",
            f'<AbsoluteTargetLaneOffset value="{_fmt(offset)}"/>',
            "</LaneOffsetTarget>",
            "</LaneOffsetAction>",
            "</LateralAction>",
        ]
    if kind == "TeleportAction":
        x = _fmt(_first(parameters, ("x",)), 0.0)
        y = _fmt(_first(parameters, ("y",)), 0.0)
        yaw = _fmt(_first(parameters, ("yaw", "h")), 0.0)
        return [
            "<TeleportAction>",
            "<Position>",
            f'<WorldPosition x="{x}" y="{y}" z="0.000" h="{yaw}"/>',
            "</Position>",
            "</TeleportAction>",
        ]
    if kind == "ActivateControllerAction":
        lateral = str(bool(parameters.get("lateral", True))).lower()
        longitudinal = str(bool(parameters.get("longitudinal", True))).lower()
        return [
            "<ControllerAction>",
            f'<ActivateControllerAction lateral="{lateral}" longitudinal="{longitudinal}"/>',
            "</ControllerAction>",
        ]
    if kind == "TrafficSignalStateAction":
        signal = str(parameters.get("signal_id") or parameters.get("name") or "signal-1")
        state = str(parameters.get("state") or parameters.get("phase") or "green")
        return [
            "<InfrastructureAction>",
            "<TrafficSignalAction>",
            f'<TrafficSignalStateAction name="{signal}" state="{state}"/>',
            "</TrafficSignalAction>",
            "</InfrastructureAction>",
        ]
    if kind == "InfrastructureAction":
        return [
            "<InfrastructureAction>",
            "<TrafficSignalAction>",
            '<TrafficSignalStateAction name="signal-1" state="green"/>',
            "</TrafficSignalAction>",
            "</InfrastructureAction>",
        ]
    if kind == "EnvironmentAction":
        weather = parameters.get("weather") if isinstance(parameters.get("weather"), Mapping) else {}
        rain = _fmt(weather.get("rain", parameters.get("rain", 0.0)))
        fog = _fmt(weather.get("fog", parameters.get("fog", 0.0)))
        return [
            "<EnvironmentAction>",
            "<Environment>",
            '<TimeOfDay dateTime="2026-01-01T12:00:00" animation="false"/>',
            (
                f'<Weather cloudiness="0.0" rainfall="{rain}" fog="{fog}" '
                'snowfall="0.0" sunAzimuth="0.0" sunElevation="0.0" temperature="20.0"/>'
            ),
            "</Environment>",
            "</EnvironmentAction>",
        ]
    if kind == "ParameterSetAction":
        parameter_ref = str(parameters.get("parameter_ref") or "duration")
        return [
            "<ParameterAction>",
            f'<ParameterSetAction parameterRef="{parameter_ref}" value="{_fmt(target)}"/>',
            "</ParameterAction>",
        ]
    return [f"<!-- x-hunter-action: {action_type} 暂无片段构造器 -->"]
