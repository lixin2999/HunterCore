"""内置逻辑场景目录（IR 目录，HUNTER SE 专项包 + 通用场景族）。

目录是代码资产（与 47 个场景模板、``contracts/scene-taxonomy.yaml`` 对齐）：
- 每个逻辑场景 = IR（参数分布 + 相关 + 约束 + 绑定 + 判据引用），不含具体参数值；
- 参数取值由生成器按策略采样，编译时经 ``bindings`` 落到 ``SceneConfig`` 字段；
- ``service.sync_catalog`` 按 ``content_hash`` 幂等入库，IR 变更自动升版本。

覆盖范围（V1.23.0，17 个）：
- BASIC 5：直线巡航 / 弯道 / 跟车 / 变道 / 窄路会车；
- INTERACTIVE 4：前车切入 / 行人横穿 / 鬼探头 / 路口会车；
- CORNER_CASE 4：紧急制动 / 障碍物突现 / 儿童追球 / 窄通道脱困（SP-GEO-01）；
- ENVIRONMENT 2：雨天 / 夜间；
- HUNTER SE 专项 2：低电量安全停车（SP-PWR-01）/ 窄通道转弯（SP-GEO-02）。

⚠ 新增/修改逻辑场景只改本文件：DDL 无变更、无需迁移；发布需 ``POST /api/v1/scene/logical/sync``。
"""
from __future__ import annotations

from typing import Any

from app.scene_engine.ir import (
    BindingIR,
    ConstraintIR,
    CorrelationIR,
    CriticalityIR,
    DistributionIR,
    ParameterIR,
    SceneIR,
)

# =====================================================================
# 构造助手（保证 base_config 满足 SceneConfig 全字段约束）
# =====================================================================


def _spawn(x: float = 0.0, y: float = 0.0, z: float = 0.0, yaw: float = 0.0) -> dict[str, Any]:
    return {"x": x, "y": y, "z": z, "yaw": yaw, "lane_id": None}


def _weather(
    *,
    cloudiness: float = 20.0,
    rain: float = 0.0,
    wetness: float = 0.0,
    fog: float = 0.0,
    wind: float = 5.0,
    sun_azimuth: float = 180.0,
    sun_altitude: float = 45.0,
) -> dict[str, Any]:
    return {
        "cloudiness": cloudiness,
        "rain": rain,
        "wetness": wetness,
        "fog": fog,
        "wind": wind,
        "sun_azimuth": sun_azimuth,
        "sun_altitude": sun_altitude,
    }


def _actor(
    actor_id: str,
    actor_type: str,
    *,
    x: float,
    y: float = 0.0,
    yaw: float = 0.0,
    behavior_type: str,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "actor_id": actor_id,
        "type": actor_type,
        "spawn_point": _spawn(x, y, 0.0, yaw),
        "behavior": {"behavior_type": behavior_type, "parameters": parameters or {}},
    }


def _event(
    event_id: str,
    event_type: str,
    *,
    condition: str,
    condition_type: str,
    trigger_parameters: dict[str, Any] | None = None,
    action_type: str,
    action_parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "type": event_type,
        "trigger": {
            "condition": condition,
            "condition_type": condition_type,
            "parameters": trigger_parameters or {},
        },
        "action": {"action_type": action_type, "parameters": action_parameters or {}},
    }


def _criteria(
    *,
    max_speed_deviation: float = 2.0,
    no_collision: bool = True,
    min_safe_distance: float = 2.0,
) -> dict[str, Any]:
    return {
        "max_speed_deviation": max_speed_deviation,
        "no_collision": no_collision,
        "min_safe_distance": min_safe_distance,
    }


def _config(
    *,
    ego_speed: float,
    ego_model: str = "HUNTER_SE",
    ego_steer: float = 0.0,
    actors: list[dict[str, Any]] | None = None,
    events: list[dict[str, Any]] | None = None,
    weather: dict[str, Any] | None = None,
    criteria: dict[str, Any] | None = None,
    duration: float = 30.0,
    map_id: str = "Town04",
) -> dict[str, Any]:
    return {
        "map": {"map_id": map_id, "map_type": "carla_town", "spawn_point": _spawn()},
        "ego_vehicle": {
            "model": ego_model,
            "initial_speed": ego_speed,
            "initial_steer": ego_steer,
        },
        "weather": weather or _weather(),
        "actors": actors or [],
        "events": events or [],
        "success_criteria": criteria or _criteria(),
        "duration": duration,
    }


def _parameter(name: str, kind: str, params: dict[str, Any], *, unit: str = "", description: str = "") -> ParameterIR:
    return ParameterIR(
        name=name,
        distribution=DistributionIR(kind=kind, params=params),
        unit=unit,
        description=description,
    )


def _ir(
    *,
    logical_id: str,
    name: str,
    category: str,
    scene_type: str,
    description: str,
    parameters: list[ParameterIR],
    bindings: list[BindingIR],
    base_config: dict[str, Any],
    oracle_ref: str,
    criticality: CriticalityIR,
    correlations: list[CorrelationIR] | None = None,
    constraints: list[ConstraintIR] | None = None,
    allowed_modes: list[str] | None = None,
    tags: list[str] | None = None,
) -> SceneIR:
    return SceneIR(
        logical_id=logical_id,
        name=name,
        category=category,
        scene_type=scene_type,
        description=description,
        parameters=parameters,
        correlations=correlations or [],
        constraints=constraints or [],
        bindings=bindings,
        base_config=base_config,
        oracle_ref=oracle_ref,
        allowed_modes=allowed_modes or ["sil", "hil"],
        criticality=criticality,
        tags=tags or [],
    )


def _v0(low: float = 6.0, high: float = 22.0, mean: float = 13.0, sigma: float = 3.0) -> ParameterIR:
    return _parameter(
        "v0",
        "truncated_normal",
        {"mean": mean, "std": sigma, "low": low, "high": high},
        unit="m/s",
        description="自车初始速度",
    )


# =====================================================================
# BASIC（5）
# =====================================================================


def _straight_cruise() -> SceneIR:
    return _ir(
        logical_id="LS-BAS-001",
        name="直线巡航（速度跟踪基线）",
        category="basic",
        scene_type="straight_cruise",
        description="空旷直线道路，验证纵向速度跟踪与车道保持基线。",
        parameters=[
            _v0(8.0, 25.0, 15.0, 3.0),
            _parameter("d0", "lognormal", {"mu": 5.2, "sigma": 0.4, "low": 80.0, "high": 400.0}, unit="m", description="前向无威胁距离"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="duration", expr="max(20, min(60, d0 / max(v0, 1)))"),
        ],
        base_config=_config(ego_speed=15.0, duration=40.0),
        oracle_ref="oracle.task.speed_tracking/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d0", d_margin=1.0),
        tags=["baseline", "longitudinal"],
    )


def _curve_driving() -> SceneIR:
    return _ir(
        logical_id="LS-BAS-002",
        name="弯道行驶（横向控制）",
        category="basic",
        scene_type="curve_driving",
        description="定曲率弯道，验证横向控制与转向平顺性。",
        parameters=[
            _v0(5.0, 12.0, 8.0, 1.5),
            _parameter(
                "steer",
                "uniform",
                {"low": 0.04, "high": 0.12},
                description="归一化初始转角（× 最大前轮转角 0.6rad，约 R≥37m）",
            ),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="ego_vehicle.initial_steer", expr="steer"),
        ],
        base_config=_config(ego_speed=9.0, ego_steer=0.08, duration=30.0),
        oracle_ref="oracle.comfort.accel/v1",
        criticality=CriticalityIR(speed_param="v0"),
        tags=["lateral", "comfort"],
    )


def _car_following() -> SceneIR:
    return _ir(
        logical_id="LS-BAS-003",
        name="跟车行驶（时距保持）",
        category="basic",
        scene_type="car_following",
        description="前车匀速，验证自适应巡航的稳态时距与速度跟踪。",
        parameters=[
            _v0(6.0, 20.0, 12.0, 2.5),
            _parameter("gap", "truncated_normal", {"mean": 18.0, "std": 5.0, "low": 6.0, "high": 45.0}, unit="m", description="初始净距"),
            _parameter("v1", "truncated_normal", {"mean": 10.0, "std": 2.5, "low": 4.0, "high": 18.0}, unit="m/s", description="前车速度"),
        ],
        correlations=[CorrelationIR(left="v0", right="v1", rho=0.6)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 4.5"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v1"),
            BindingIR(path="duration", expr="max(20, min(90, gap * 3))"),
        ],
        base_config=_config(
            ego_speed=12.0,
            actors=[_actor("lead", "vehicle", x=22.5, behavior_type="constant_velocity", parameters={"target_speed": 10.0})],
            duration=60.0,
            criteria=_criteria(min_safe_distance=5.0),
        ),
        oracle_ref="oracle.task.speed_tracking/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap", d_margin=1.0),
        constraints=[ConstraintIR(name="lead_slower", terms={"v0": 1.0, "v1": -1.0}, op="<=", value=8.0, message="前车不应远快于自车")],
        tags=["acc", "time_gap"],
    )


def _lane_change() -> SceneIR:
    return _ir(
        logical_id="LS-BAS-004",
        name="变道行驶（邻道交互）",
        category="basic",
        scene_type="lane_change",
        description="邻道有车，验证变道决策与横向轨迹规划。",
        parameters=[
            _v0(6.0, 20.0, 12.0, 2.5),
            _parameter("v_adj", "truncated_normal", {"mean": 13.0, "std": 3.0, "low": 5.0, "high": 22.0}, unit="m/s", description="邻道车辆速度"),
            _parameter("gap_adj", "uniform", {"low": 8.0, "high": 35.0}, unit="m", description="邻道车辆纵向间距"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap_adj"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_adj"),
        ],
        base_config=_config(
            ego_speed=12.0,
            actors=[_actor("adjacent", "vehicle", x=20.0, y=3.5, behavior_type="constant_velocity", parameters={"target_speed": 13.0})],
            duration=30.0,
        ),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap_adj", d_margin=1.5),
        correlations=[CorrelationIR(left="v0", right="v_adj", rho=0.4)],
        tags=["lane_change", "interaction"],
    )


def _narrow_road_meeting() -> SceneIR:
    return _ir(
        logical_id="LS-BAS-005",
        name="窄路会车（通行间隙）",
        category="basic",
        scene_type="narrow_road_meeting",
        description="窄路对向会车，验证横向间隙评估与减速让行策略。",
        parameters=[
            _parameter("w", "uniform", {"low": 3.0, "high": 5.5}, unit="m", description="可通行宽度"),
            _v0(4.0, 16.0, 8.0, 2.0),
            _parameter("v_on", "truncated_normal", {"mean": 7.0, "std": 2.0, "low": 3.0, "high": 14.0}, unit="m/s", description="对向车速"),
        ],
        correlations=[CorrelationIR(left="v0", right="v_on", rho=0.5)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.y", expr="max(0, w / 2 - 0.9)"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_on"),
            BindingIR(path="success_criteria.min_safe_distance", expr="max(0.3, w - 2.4)"),
        ],
        base_config=_config(
            ego_speed=8.0,
            actors=[_actor("oncoming", "vehicle", x=60.0, y=0.6, yaw=3.141592653589793, behavior_type="constant_velocity", parameters={"target_speed": 7.0})],
            duration=40.0,
            criteria=_criteria(min_safe_distance=1.0),
        ),
        oracle_ref="oracle.geometry.clearance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="w", max_decel=6.0, d_margin=1.8),
        tags=["narrow", "HUNTER_SE", "SP-GEO"],
    )


# =====================================================================
# INTERACTIVE（4）
# =====================================================================


def _cut_in() -> SceneIR:
    return _ir(
        logical_id="LS-INT-001",
        name="前车切入（距离触发）",
        category="interactive",
        scene_type="cut_in",
        description="邻道前车在设定距离处切入本车道，验证紧急减速与避让。",
        parameters=[
            _v0(8.0, 20.0, 13.0, 2.5),
            _parameter("gap", "lognormal", {"mu": 2.6, "sigma": 0.45, "low": 6.0, "high": 60.0}, unit="m", description="切入时纵向净距"),
            _parameter("v_lead", "truncated_normal", {"mean": 9.0, "std": 2.5, "low": 3.0, "high": 16.0}, unit="m/s", description="切入车速度"),
        ],
        correlations=[
            CorrelationIR(left="v0", right="v_lead", rho=0.5),
            CorrelationIR(left="v0", right="gap", rho=-0.35),
        ],
        constraints=[
            ConstraintIR(name="cut_in_gap", terms={"gap": 1.0, "v0": -0.6}, op=">=", value=1.5, message="切入距离需大于 0.6s 行驶距离 + 1.5m")
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 8.0"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_lead"),
            BindingIR(path="events[0].trigger.parameters.value", expr="gap"),
        ],
        base_config=_config(
            ego_speed=13.0,
            actors=[_actor("cutter", "vehicle", x=24.0, y=3.5, behavior_type="cut_in", parameters={"target_speed": 9.0})],
            events=[
                _event(
                    "cut-in",
                    "cut_in",
                    condition="relative_distance <= gap",
                    condition_type="relative_distance",
                    trigger_parameters={"value": 14.0, "rule": "<="},
                    action_type="lane_change",
                    action_parameters={"target_lane": "current"},
                )
            ],
            duration=30.0,
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap", max_decel=8.0, d_margin=1.0),
        tags=["cut_in", "HUNTER_SE"],
    )


def _pedestrian_crossing() -> SceneIR:
    return _ir(
        logical_id="LS-INT-002",
        name="行人横穿（行人保护）",
        category="interactive",
        scene_type="pedestrian_crossing",
        description="行人从路侧横穿，验证行人检测、减速与停车让行。",
        parameters=[
            _v0(6.0, 18.0, 11.0, 2.5),
            _parameter("d_ped", "uniform", {"low": 10.0, "high": 40.0}, unit="m", description="行人横向起点距离"),
            _parameter("v_ped", "truncated_normal", {"mean": 1.4, "std": 0.3, "low": 0.8, "high": 2.2}, unit="m/s", description="行人步速"),
        ],
        correlations=[CorrelationIR(left="v0", right="d_ped", rho=0.3)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_ped"),
            BindingIR(path="actors[0].behavior.parameters.crossing_speed", expr="v_ped"),
            BindingIR(path="events[0].trigger.parameters.value", expr="d_ped"),
        ],
        base_config=_config(
            ego_speed=11.0,
            actors=[_actor("ped", "pedestrian", x=20.0, y=6.0, behavior_type="cross_road", parameters={"crossing_speed": 1.4})],
            events=[
                _event(
                    "ped-cross",
                    "pedestrian_crossing",
                    condition="distance to crossing <= d_ped",
                    condition_type="distance",
                    trigger_parameters={"value": 20.0, "rule": "<="},
                    action_type="emergency_stop",
                    action_parameters={"deceleration": 6.0},
                )
            ],
            duration=30.0,
            criteria=_criteria(min_safe_distance=1.5),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_ped", max_decel=7.0, d_margin=1.5),
        tags=["vru", "pedestrian"],
    )


def _occluded_pedestrian() -> SceneIR:
    return _ir(
        logical_id="LS-INT-003",
        name="鬼探头（遮挡行人）",
        category="interactive",
        scene_type="occluded_pedestrian",
        description="视线受遮挡后行人突然窜出，验证感知盲区与极限减速。",
        parameters=[
            _v0(6.0, 16.0, 9.0, 2.0),
            _parameter("d_occ", "uniform", {"low": 8.0, "high": 30.0}, unit="m", description="遮挡物距自车距离"),
            _parameter("v_ped", "uniform", {"low": 1.0, "high": 2.4}, unit="m/s", description="行人窜出速度"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_occ"),
            BindingIR(path="actors[0].behavior.parameters.crossing_speed", expr="v_ped"),
        ],
        base_config=_config(
            ego_speed=9.0,
            actors=[_actor("occluded-ped", "pedestrian", x=16.0, y=5.0, behavior_type="occluded_dash", parameters={"crossing_speed": 1.6})],
            duration=25.0,
            criteria=_criteria(min_safe_distance=1.2),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_occ", max_decel=8.0, d_margin=1.2),
        tags=["occlusion", "vru", "corner"],
    )


def _intersection_meeting() -> SceneIR:
    return _ir(
        logical_id="LS-INT-004",
        name="路口会车（交叉冲突）",
        category="interactive",
        scene_type="intersection_meeting",
        description="无信号路口侧向来车，验证路权判断与冲突消解。",
        parameters=[
            _v0(5.0, 16.0, 9.0, 2.0),
            _parameter("d_meet", "uniform", {"low": 12.0, "high": 45.0}, unit="m", description="到冲突点距离"),
            _parameter("v_other", "truncated_normal", {"mean": 8.0, "std": 2.0, "low": 3.0, "high": 15.0}, unit="m/s", description="侧向来车速度"),
        ],
        correlations=[CorrelationIR(left="v0", right="v_other", rho=0.45)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_meet"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_other"),
        ],
        base_config=_config(
            ego_speed=9.0,
            actors=[_actor("crosser", "vehicle", x=25.0, y=8.0, yaw=-1.5707963267948966, behavior_type="forward", parameters={"target_speed": 8.0})],
            duration=30.0,
        ),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_meet", max_decel=6.0, d_margin=2.0),
        tags=["intersection", "right_of_way"],
    )


# =====================================================================
# CORNER CASE（4）
# =====================================================================


def _emergency_braking() -> SceneIR:
    return _ir(
        logical_id="LS-CC-001",
        name="紧急制动（前车急刹）",
        category="corner_case",
        scene_type="emergency_braking",
        description="前车最大减速度制动，验证 AEB 触发与最小安全距离。",
        parameters=[
            _v0(8.0, 22.0, 14.0, 3.0),
            _parameter("gap", "lognormal", {"mu": 2.3, "sigma": 0.4, "low": 5.0, "high": 35.0}, unit="m", description="初始净距"),
            _parameter("a_lead", "uniform", {"low": 3.0, "high": 8.0}, unit="m/s²", description="前车制动减速度"),
        ],
        correlations=[CorrelationIR(left="gap", right="v0", rho=0.35)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 4.5"),
            BindingIR(path="actors[0].behavior.parameters.deceleration", expr="a_lead"),
        ],
        base_config=_config(
            ego_speed=14.0,
            actors=[_actor("braking-lead", "vehicle", x=20.5, behavior_type="emergency_brake", parameters={"deceleration": 6.0})],
            duration=25.0,
            criteria=_criteria(min_safe_distance=1.5),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap", max_decel=8.0, d_margin=1.0),
        tags=["aeb", "longitudinal"],
    )


def _obstacle_appearance() -> SceneIR:
    return _ir(
        logical_id="LS-CC-002",
        name="障碍物突现（静止障碍）",
        category="corner_case",
        scene_type="obstacle_appearance",
        description="车道内静止障碍物突然进入感知范围，验证避障与制动。",
        parameters=[
            _v0(6.0, 20.0, 12.0, 3.0),
            _parameter("d_obs", "lognormal", {"mu": 3.2, "sigma": 0.5, "low": 12.0, "high": 90.0}, unit="m", description="障碍物距离"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_obs"),
            BindingIR(path="events[0].trigger.parameters.value", expr="d_obs"),
        ],
        base_config=_config(
            ego_speed=12.0,
            actors=[_actor("obstacle", "other", x=30.0, behavior_type="static", parameters={"object_kind": "box"})],
            events=[
                _event(
                    "obstacle-appear",
                    "obstacle_appearance",
                    condition="distance to obstacle <= d_obs",
                    condition_type="distance",
                    trigger_parameters={"value": 30.0, "rule": "<="},
                    action_type="visibility",
                    action_parameters={"parameters": "visible"},
                )
            ],
            duration=25.0,
            criteria=_criteria(min_safe_distance=1.0),
        ),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_obs", max_decel=7.5, d_margin=1.0),
        tags=["obstacle", "perception"],
    )


def _child_chasing_ball() -> SceneIR:
    return _ir(
        logical_id="LS-CC-003",
        name="儿童追球（不可预测目标）",
        category="corner_case",
        scene_type="child_chasing_ball",
        description="儿童追逐皮球进入车道，验证弱势交通参与者极限场景。",
        parameters=[
            _v0(5.0, 15.0, 8.0, 2.5),
            _parameter("d_child", "uniform", {"low": 7.0, "high": 28.0}, unit="m", description="儿童起点距离"),
            _parameter("v_child", "uniform", {"low": 1.5, "high": 3.5}, unit="m/s", description="儿童奔跑速度"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_child"),
            BindingIR(path="actors[0].behavior.parameters.crossing_speed", expr="v_child"),
        ],
        base_config=_config(
            ego_speed=8.0,
            actors=[_actor("child", "pedestrian", x=15.0, y=5.0, behavior_type="child_chase_ball", parameters={"crossing_speed": 2.5})],
            duration=25.0,
            criteria=_criteria(min_safe_distance=1.0),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_child", max_decel=8.0, d_margin=1.0),
        tags=["vru", "unpredictable"],
    )


def _narrow_corridor_escape() -> SceneIR:
    return _ir(
        logical_id="LS-CC-004",
        name="窄通道障碍脱困（SP-GEO-01）",
        category="corner_case",
        scene_type="stationary_obstacle",
        description="HUNTER SE 窄通道内静止障碍占道，验证几何可行性与脱困轨迹。",
        parameters=[
            _parameter("w", "uniform", {"low": 2.6, "high": 4.2}, unit="m", description="通道宽度"),
            _v0(2.0, 8.0, 4.5, 1.5),
            _parameter("offset", "uniform", {"low": -0.4, "high": 0.4}, unit="m", description="障碍物横向偏移"),
        ],
        correlations=[CorrelationIR(left="w", right="v0", rho=0.3)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="18.0"),
            BindingIR(path="actors[0].spawn_point.y", expr="offset"),
            BindingIR(path="success_criteria.min_safe_distance", expr="max(0.2, w - 2.4)"),
        ],
        base_config=_config(
            ego_speed=4.5,
            actors=[_actor("block", "other", x=18.0, behavior_type="static", parameters={"object_kind": "barrier"})],
            duration=40.0,
            criteria=_criteria(min_safe_distance=0.8),
        ),
        oracle_ref="oracle.geometry.clearance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="w", max_decel=4.0, d_margin=2.0),
        tags=["HUNTER_SE", "SP-GEO-01", "geometry", "deadlock"],
    )


# =====================================================================
# ENVIRONMENT（2）
# =====================================================================


def _rainy_cruise() -> SceneIR:
    return _ir(
        logical_id="LS-ENV-001",
        name="雨天巡航（湿滑路面）",
        category="environment",
        scene_type="rainy",
        description="中到大雨、路面积水，验证湿滑路面速度跟踪与制动距离退化。",
        parameters=[
            _v0(6.0, 18.0, 10.0, 2.0),
            _parameter("rain", "uniform", {"low": 30.0, "high": 100.0}, unit="%", description="降雨强度"),
            _parameter("wetness", "uniform", {"low": 20.0, "high": 100.0}, unit="%", description="地面积水"),
            _parameter("fog", "uniform", {"low": 0.0, "high": 30.0}, unit="%", description="雾浓度"),
        ],
        correlations=[
            CorrelationIR(left="rain", right="wetness", rho=0.8),
            CorrelationIR(left="rain", right="fog", rho=0.35),
            CorrelationIR(left="v0", right="rain", rho=-0.3),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="weather.rain", expr="rain"),
            BindingIR(path="weather.wetness", expr="wetness"),
            BindingIR(path="weather.fog", expr="fog"),
            BindingIR(path="weather.cloudiness", expr="max(60, rain)"),
        ],
        base_config=_config(ego_speed=10.0, duration=40.0, weather=_weather(cloudiness=80.0, rain=60.0, wetness=60.0, fog=10.0)),
        oracle_ref="oracle.task.speed_tracking/v1",
        criticality=CriticalityIR(speed_param="v0"),
        tags=["weather", "rain", "low_friction"],
    )


def _night_driving() -> SceneIR:
    return _ir(
        logical_id="LS-ENV-002",
        name="夜间行驶（低照度）",
        category="environment",
        scene_type="night",
        description="夜间低照度，验证感知降级下的车道保持与速度控制。",
        parameters=[
            _v0(5.0, 16.0, 9.0, 2.0),
            _parameter("sun_altitude", "uniform", {"low": -30.0, "high": -5.0}, unit="°", description="太阳高度角（负值夜间）"),
            _parameter("cloudiness", "uniform", {"low": 0.0, "high": 90.0}, unit="%", description="云量"),
        ],
        correlations=[CorrelationIR(left="sun_altitude", right="cloudiness", rho=-0.4)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="weather.sun_altitude", expr="sun_altitude"),
            BindingIR(path="weather.cloudiness", expr="cloudiness"),
        ],
        base_config=_config(ego_speed=9.0, duration=40.0, weather=_weather(cloudiness=40.0, sun_altitude=-15.0)),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0"),
        tags=["weather", "night", "illumination"],
    )


# =====================================================================
# HUNTER SE 专项（2）
# =====================================================================


def _low_battery_stop() -> SceneIR:
    return _ir(
        logical_id="LS-HSE-001",
        name="低电量安全停车（SP-PWR-01）",
        category="corner_case",
        scene_type="minimal_risk_manoeuvre",
        description="SOC 跌入告警区间，验证最小风险策略（靠边停车/返航决策）。",
        parameters=[
            _parameter("soc", "uniform", {"low": 0.08, "high": 0.25}, description="电池 SOC（0-1）"),
            _parameter("d_station", "uniform", {"low": 300.0, "high": 5000.0}, unit="m", description="到充电点距离"),
            _v0(4.0, 12.0, 7.0, 2.0),
        ],
        correlations=[CorrelationIR(left="soc", right="d_station", rho=-0.55)],
        constraints=[
            ConstraintIR(name="critical_soc", terms={"soc": 1.0, "d_station": -0.00003}, op=">=", value=0.02, message="SOC 与返航距离需满足可达性下限")
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="events[0].action.parameters.soc", expr="soc"),
            BindingIR(path="events[0].trigger.parameters.value", expr="d_station"),
            BindingIR(path="duration", expr="max(30, min(180, d_station / max(v0, 1)))"),
        ],
        base_config=_config(
            ego_speed=7.0,
            events=[
                _event(
                    "soc-guard",
                    "emergency_brake",
                    condition="remaining range < d_station",
                    condition_type="parameter_value",
                    trigger_parameters={"parameter_name": "soc", "value": 0.15, "rule": "<="},
                    action_type="degrade_control",
                    action_parameters={"soc": 0.15, "fallback": "minimal_risk_stop"},
                )
            ],
            duration=90.0,
            criteria=_criteria(max_speed_deviation=1.5, min_safe_distance=0.5),
        ),
        oracle_ref="oracle.energy.soc_reserve/v1",
        criticality=CriticalityIR(speed_param="v0"),
        tags=["HUNTER_SE", "SP-PWR-01", "energy", "MRM"],
    )


def _narrow_turn() -> SceneIR:
    return _ir(
        logical_id="LS-HSE-002",
        name="窄通道转弯（SP-GEO-02）",
        category="corner_case",
        scene_type="uturn",
        description="HUNTER SE 在窄通道内大转角掉头，验证阿克曼几何可行性（G4）。",
        parameters=[
            _parameter("w", "uniform", {"low": 4.0, "high": 8.0}, unit="m", description="通道宽度"),
            _v0(2.0, 7.0, 4.0, 1.2),
            _parameter("steer", "uniform", {"low": 0.25, "high": 0.55}, description="归一化转角"),
        ],
        correlations=[CorrelationIR(left="v0", right="steer", rho=-0.45)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="ego_vehicle.initial_steer", expr="steer"),
            BindingIR(path="success_criteria.min_safe_distance", expr="max(0.2, w - 3.6)"),
        ],
        base_config=_config(
            ego_speed=4.0,
            ego_steer=0.4,
            duration=60.0,
            criteria=_criteria(min_safe_distance=0.8),
        ),
        oracle_ref="oracle.geometry.clearance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="w", max_decel=4.0, d_margin=3.6),
        tags=["HUNTER_SE", "SP-GEO-02", "kinematics"],
    )


#: 内置目录（顺序稳定：同步/列表按此顺序可读）
CATALOG: tuple[SceneIR, ...] = (
    _straight_cruise(),
    _curve_driving(),
    _car_following(),
    _lane_change(),
    _narrow_road_meeting(),
    _cut_in(),
    _pedestrian_crossing(),
    _occluded_pedestrian(),
    _intersection_meeting(),
    _emergency_braking(),
    _obstacle_appearance(),
    _child_chasing_ball(),
    _narrow_corridor_escape(),
    _rainy_cruise(),
    _night_driving(),
    _low_battery_stop(),
    _narrow_turn(),
)

#: logical_id → IR（生成接口按 ID 检索）
CATALOG_BY_ID: dict[str, SceneIR] = {ir.logical_id: ir for ir in CATALOG}

#: 目录版本（IR 语义变更时递增；用于同步日志与文档追溯）
CATALOG_VERSION = "1.0.0"


def get_ir(logical_id: str, version: str | None = None) -> SceneIR | None:
    """按 ID 取 IR（``version`` 仅做兼容参数：代码目录只保留最新 IR）。"""
    del version
    return CATALOG_BY_ID.get(logical_id)


__all__ = [
    "CATALOG",
    "CATALOG_BY_ID",
    "CATALOG_VERSION",
    "get_ir",
]
