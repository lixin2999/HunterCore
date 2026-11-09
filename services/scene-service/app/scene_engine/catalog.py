"""内置逻辑场景目录（IR 目录，HUNTER SE 专项包 + 通用场景族）。

目录是代码资产（与 46 个可合成场景类型、``contracts/scene-taxonomy.yaml`` 对齐）：
- 每个逻辑场景 = IR（参数分布 + 相关 + 约束 + 绑定 + 判据引用），不含具体参数值；
- 参数取值由生成器按策略采样，编译时经 ``bindings`` 落到 ``SceneConfig`` 字段；
- ``service.sync_catalog`` 按 ``content_hash`` 幂等入库，IR 变更自动升版本。

来源分工（单一事实来源）：
- 本文件保留 17 个手写 IR（``logical_id`` 稳定，避免已发布版本漂移）；
  其 ``base_config`` 的参与者/事件/天气/时长同样由 ``catalog_semantics`` 从语义契约实例化，
  不再手写一份平行事实源；
- 其余 29 个类型由 ``app/scene_engine/catalog_semantics.py`` 按语义契约合成（V1.1.0 起）。

覆盖范围（17 个手写）：
- BASIC 5：直线巡航 / 弯道 / 跟车 / 变道 / 窄路会车；
- INTERACTIVE 4：前车切入 / 行人横穿 / 鬼探头 / 路口会车；
- CORNER_CASE 4：紧急制动 / 障碍物突现 / 儿童追球 / 窄通道脱困（SP-GEO-01）；
- ENVIRONMENT 2：雨天 / 夜间；
- HUNTER SE 专项 2：低电量安全停车（SP-PWR-01）/ 窄通道转弯（SP-GEO-02）。

⚠ 新增/修改逻辑场景只改本文件与语义契约：DDL 无变更、无需迁移；发布需 ``POST /api/v1/scene/logical/sync``。
"""
from __future__ import annotations

from typing import Any

from app.scene_engine.catalog_semantics import CATALOG_SEMANTICS, semantic_base_config
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
# 构造助手（语义契约实例化 + IR 字段约束）
# =====================================================================


def _semantic_config(scene_type: str, **overrides: Any) -> dict[str, Any]:
    """base_config 由语义契约合成（角色/事件/天气/时长同源，可覆盖 ego/时长/判据）。"""
    return semantic_base_config(scene_type, **overrides)


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


def _v0(low: float = 0.0, high: float = 0.5, mean: float = 0.3, sigma: float = 0.12) -> ParameterIR:
    """自车初速（§2.3.4 HUNTER SE ODD 默认 [0, 0.5] m/s）。"""
    return _parameter(
        "v0",
        "truncated_normal",
        {"mean": mean, "std": sigma, "low": low, "high": high},
        unit="m/s",
        description="自车初始速度",
    )


def _d0(
    name: str = "d0",
    *,
    description: str = "障碍物初始距离",
    low: float = 0.6,
    high: float = 15.0,
    mu: float = 0.35,
    sigma: float = 0.55,
) -> ParameterIR:
    """障碍/冲突初始距离（§2.3.4 ODD [0.6, 15] m，对数正态截断）。"""
    return _parameter(
        name,
        "lognormal",
        {"mu": mu, "sigma": sigma, "low": low, "high": high},
        unit="m",
        description=description,
    )


def _npc_speed(
    name: str, *, description: str, mean: float = 0.8, sigma: float = 0.45
) -> ParameterIR:
    """NPC 速度（§2.3.4 ODD [0, 2.0] m/s）。"""
    return _parameter(
        name,
        "truncated_normal",
        {"mean": mean, "std": sigma, "low": 0.0, "high": 2.0},
        unit="m/s",
        description=description,
    )


def _ped_speed(name: str = "v_ped", *, description: str = "行人步速") -> ParameterIR:
    """行人步速（§2.3.4 ODD [0.5, 2.0] m/s，GMM 双峰：步行/奔跑）。"""
    return _parameter(
        name,
        "gmm",
        {
            "components": [
                {"weight": 0.6, "mean": 0.9, "std": 0.18, "low": 0.5, "high": 1.4},
                {"weight": 0.4, "mean": 1.6, "std": 0.22, "low": 1.2, "high": 2.0},
            ]
        },
        unit="m/s",
        description=description,
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
            _v0(),
            _parameter(
                "d_free",
                "uniform",
                {"low": 8.0, "high": 60.0},
                unit="m",
                description="前向自由距离（无威胁）",
            ),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="duration", expr="max(20, min(60, d_free / max(v0, 1)))"),
        ],
        base_config=_semantic_config("straight_cruise", ego_speed=0.3, duration=40.0),
        oracle_ref="oracle.task.speed_tracking/v1",
        criticality=CriticalityIR(speed_param="v0"),
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
            _v0(),
            _parameter(
                "steer",
                "uniform",
                {"low": 0.04, "high": 0.12},
                description="归一化初始转角（× 车端最大前轮转角，约 R≥10m）",
            ),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="ego_vehicle.initial_steer", expr="steer"),
        ],
        base_config=_semantic_config("curve_driving", ego_speed=0.3, ego_steer=0.08, duration=30.0),
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
            _v0(),
            _d0("gap", description="初始净距"),
            _npc_speed("v1", description="前车速度"),
        ],
        correlations=[CorrelationIR(left="v0", right="v1", rho=0.6)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 1.0"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v1"),
            BindingIR(path="duration", expr="max(20, min(90, gap * 3))"),
        ],
        base_config=_semantic_config(
            "car_following",
            ego_speed=0.3,
            duration=60.0,
            criteria=_criteria(min_safe_distance=0.5),
        ),
        oracle_ref="oracle.task.speed_tracking/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap", d_margin=0.3),
        constraints=[ConstraintIR(name="lead_not_far_faster", terms={"v0": 1.0, "v1": -1.0}, op="<=", value=1.0, message="前车不应远快于自车")],
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
            _v0(),
            _npc_speed("v_adj", description="邻道车辆速度"),
            _d0("gap_adj", description="邻道车辆纵向间距"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap_adj"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_adj"),
        ],
        base_config=_semantic_config(
            "lane_change",
            ego_speed=0.3,
            duration=30.0,
        ),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap_adj", d_margin=0.5),
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
            _parameter("w", "truncated_normal", {"mean": 3.2, "std": 0.6, "low": 2.0, "high": 5.0}, unit="m", description="可通行宽度"),
            _v0(),
            _npc_speed("v_on", description="对向车速"),
        ],
        correlations=[CorrelationIR(left="v0", right="v_on", rho=0.5)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.y", expr="max(0, w / 2 - 0.9)"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_on"),
            BindingIR(path="success_criteria.min_safe_distance", expr="max(0.2, w - 2.4)"),
        ],
        base_config=_semantic_config(
            "narrow_road_meeting",
            ego_speed=0.3,
            duration=40.0,
            criteria=_criteria(min_safe_distance=0.5),
        ),
        oracle_ref="oracle.geometry.clearance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="w", d_margin=1.0),
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
            _v0(),
            _d0("gap", description="切入时纵向净距", mu=0.25, sigma=0.5),
            _npc_speed("v_lead", description="切入车速度"),
        ],
        correlations=[
            CorrelationIR(left="v0", right="v_lead", rho=0.5),
            CorrelationIR(left="v0", right="gap", rho=-0.35),
        ],
        constraints=[
            ConstraintIR(name="cut_in_gap", terms={"gap": 1.0, "v0": -0.6}, op=">=", value=0.3, message="切入距离需大于 0.6s 行驶距离 + 0.3m")
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 1.0"),
            BindingIR(path="actors[0].behavior.parameters.cut_in_speed", expr="v_lead"),
            BindingIR(path="events[0].trigger.parameters.value", expr="gap"),
        ],
        base_config=_semantic_config(
            "cut_in",
            ego_speed=0.3,
            duration=30.0,
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap", d_margin=0.1),
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
            _v0(),
            _parameter("d_ped", "uniform", {"low": 1.0, "high": 6.0}, unit="m", description="行人触发距离"),
            _ped_speed(),
        ],
        correlations=[CorrelationIR(left="v0", right="d_ped", rho=0.3)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_ped"),
            BindingIR(path="actors[0].behavior.parameters.crossing_speed", expr="v_ped"),
            BindingIR(path="events[0].trigger.parameters.value", expr="d_ped"),
        ],
        base_config=_semantic_config(
            "pedestrian_crossing",
            ego_speed=0.3,
            duration=30.0,
            criteria=_criteria(min_safe_distance=0.5),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_ped", d_margin=0.4),
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
            _v0(),
            _parameter("d_occ", "uniform", {"low": 1.0, "high": 6.0}, unit="m", description="遮挡物/行人触发距离"),
            _ped_speed(description="行人窜出速度"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_occ"),
            BindingIR(path="actors[0].behavior.parameters.crossing_speed", expr="v_ped"),
        ],
        base_config=_semantic_config(
            "occluded_pedestrian",
            ego_speed=0.3,
            duration=25.0,
            criteria=_criteria(min_safe_distance=0.4),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_occ", d_margin=0.4),
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
            _v0(),
            _parameter("d_meet", "uniform", {"low": 0.6, "high": 15.0}, unit="m", description="到冲突点距离"),
            _npc_speed("v_other", description="侧向来车速度"),
        ],
        correlations=[CorrelationIR(left="v0", right="v_other", rho=0.45)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_meet"),
            BindingIR(path="actors[0].behavior.parameters.target_speed", expr="v_other"),
        ],
        base_config=_semantic_config(
            "intersection_meeting",
            ego_speed=0.3,
            duration=30.0,
        ),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_meet", d_margin=0.5),
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
            _v0(),
            _d0("gap", description="初始净距", mu=0.25, sigma=0.5),
            _parameter("a_lead", "uniform", {"low": 0.3, "high": 1.5}, unit="m/s²", description="前车制动减速度"),
        ],
        correlations=[CorrelationIR(left="gap", right="v0", rho=0.35)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="gap + 1.0"),
            BindingIR(path="events[0].action.parameters.deceleration", expr="a_lead"),
        ],
        base_config=_semantic_config(
            "emergency_braking",
            ego_speed=0.3,
            duration=25.0,
            criteria=_criteria(min_safe_distance=0.4),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="gap", d_margin=0.2),
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
            _v0(),
            _d0("d_obs", description="障碍物距离", mu=0.5, sigma=0.5),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_obs"),
            BindingIR(path="events[0].trigger.parameters.value", expr="d_obs"),
        ],
        base_config=_semantic_config(
            "obstacle_appearance",
            ego_speed=0.3,
            duration=25.0,
            criteria=_criteria(min_safe_distance=0.3),
        ),
        oracle_ref="oracle.safety.no_collision/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_obs", d_margin=0.2),
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
            _v0(),
            _parameter("d_child", "uniform", {"low": 1.0, "high": 6.0}, unit="m", description="儿童起点/触发距离"),
            _ped_speed("v_child", description="儿童奔跑速度"),
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d_child"),
            BindingIR(path="actors[0].behavior.parameters.crossing_speed", expr="v_child"),
        ],
        base_config=_semantic_config(
            "child_chasing_ball",
            ego_speed=0.3,
            duration=25.0,
            criteria=_criteria(min_safe_distance=0.3),
        ),
        oracle_ref="oracle.safety.min_distance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d_child", d_margin=0.3),
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
            _d0("d0", description="障碍物初始距离（SP-GEO-01 重点 0.9~1.8m）", low=0.6, high=2.5, mu=0.29, sigma=0.35),
            _parameter("offset", "truncated_normal", {"mean": 0.0, "std": 0.45, "low": -1.0, "high": 1.0}, unit="m", description="障碍物横向偏移"),
            _parameter("w_obs", "lognormal", {"mu": -0.5, "sigma": 0.4, "low": 0.2, "high": 1.5}, unit="m", description="障碍物宽度"),
            _parameter("w", "truncated_normal", {"mean": 3.0, "std": 0.6, "low": 2.0, "high": 5.0}, unit="m", description="通道可通行宽度"),
            _v0(),
        ],
        correlations=[CorrelationIR(left="w", right="v0", rho=0.2)],
        constraints=[
            ConstraintIR(name="free_width", terms={"w": 1.0, "w_obs": -1.0}, op=">=", value=0.3, message="通道宽度需大于障碍物宽度 + 0.3m")
        ],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="actors[0].spawn_point.x", expr="d0"),
            BindingIR(path="actors[0].spawn_point.y", expr="offset"),
            BindingIR(path="actors[0].behavior.parameters.width", expr="w_obs"),
            BindingIR(path="success_criteria.min_safe_distance", expr="max(0.2, w - w_obs)"),
        ],
        base_config=_semantic_config(
            "stationary_obstacle",
            ego_speed=0.3,
            duration=40.0,
            criteria=_criteria(min_safe_distance=0.2),
            actor_extra={0: {"width": 0.6}},
        ),
        oracle_ref="oracle.geometry.clearance/v1",
        criticality=CriticalityIR(speed_param="v0", gap_param="d0", d_margin=0.2),
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
            _v0(),
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
        base_config=_semantic_config("rainy", ego_speed=10.0, duration=40.0),
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
            # 车端 v_max=4.8 m/s（vehicle_params），夜间场景速度区间必须落在整车能力内，否则 G7 判不真实
            _v0(1.0, 4.5, 2.8, 0.8),
            _parameter("sun_altitude", "uniform", {"low": -30.0, "high": -5.0}, unit="°", description="太阳高度角（负值夜间）"),
            _parameter("cloudiness", "uniform", {"low": 0.0, "high": 90.0}, unit="%", description="云量"),
        ],
        correlations=[CorrelationIR(left="sun_altitude", right="cloudiness", rho=-0.4)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="weather.sun_altitude", expr="sun_altitude"),
            BindingIR(path="weather.cloudiness", expr="cloudiness"),
        ],
        base_config=_semantic_config("night", ego_speed=2.8, duration=40.0),
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
            _v0(1.0, 4.5, 2.5, 0.8),
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
        base_config=_semantic_config(
            "minimal_risk_manoeuvre",
            ego_speed=2.5,
            duration=90.0,
            criteria=_criteria(max_speed_deviation=1.5, min_safe_distance=0.5),
            event_extra={0: {"soc": 0.15}},
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
            # 设计 §2.9 SP-GEO-02：W∈[2.0,3.2]，W≥R_min+b+δ≈2.5 时单次转弯可行
            _parameter("w", "uniform", {"low": 2.0, "high": 3.2}, unit="m", description="通道宽度"),
            _v0(),
            _parameter("steer", "uniform", {"low": 0.25, "high": 0.55}, description="归一化转角"),
        ],
        correlations=[CorrelationIR(left="v0", right="steer", rho=-0.45)],
        bindings=[
            BindingIR(path="ego_vehicle.initial_speed", expr="v0"),
            BindingIR(path="ego_vehicle.initial_steer", expr="steer"),
            BindingIR(path="success_criteria.min_safe_distance", expr="max(0.2, w - 1.2)"),
        ],
        base_config=_semantic_config(
            "uturn",
            ego_speed=0.4,
            ego_steer=0.4,
            duration=60.0,
            criteria=_criteria(min_safe_distance=0.2),
        ),
        oracle_ref="oracle.geometry.clearance/v1",
        # 几何专项以 oracle 判据为准；通道宽度不构成制动临界度，避免误标 C3
        criticality=CriticalityIR(speed_param="v0"),
        tags=["HUNTER_SE", "SP-GEO-02", "kinematics"],
    )


#: 内置目录（17 个手写 IR + 29 个语义合成 IR；顺序稳定：同步/列表按此顺序可读）
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
) + CATALOG_SEMANTICS

#: logical_id → IR（生成接口按 ID 检索）
CATALOG_BY_ID: dict[str, SceneIR] = {ir.logical_id: ir for ir in CATALOG}

#: 目录版本（IR 语义变更时递增；用于同步日志与文档追溯）
CATALOG_VERSION = "1.1.0"


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
