"""场景模板服务（4.2.1 分类体系预置模板，供场景编辑器一键创建）。

⚠ 契约待确认项 #11：模板数据来源（内置静态清单 / DB 表 / MinIO 资源）设计文档未定义；
本实现采用「内置静态清单」——不新增 DB 表、不引入运维依赖，模板 config 即 4.2.2 结构，
可直接作为 ``POST /api/v1/scene`` 的 config 提交。人工确认来源后仅需替换本模块数据源。
``real_vehicle_replay``（4.5 节自动提取产物）无预置模板，故不在模板清单与分类筛选枚举中。
"""
from __future__ import annotations

from typing import Any

from hunter_common.logging import get_logger

from app.config import Settings
from app.schemas.scene import (
    SCENE_TYPE_LABELS,
    SCENE_TYPES_BY_TEMPLATE_CATEGORY,
    Actor,
    ActorBehavior,
    ActorType,
    EgoVehicle,
    MapType,
    SceneConfig,
    SceneEvent,
    SceneEventAction,
    SceneEventTrigger,
    SceneEventType,
    SceneMap,
    SceneTemplate,
    SceneTemplateCategory,
    SceneTemplateListData,
    SceneType,
    SpawnPoint,
    SuccessCriteria,
    Weather,
)

logger = get_logger("app.services.templates")

#: 默认地图（Carla 内置城镇；⚠ 地图资源清单待确认）
_DEFAULT_MAP_ID = "Town01"
#: 车端基线车型（HUNTER SE）
_DEFAULT_EGO_MODEL = "HUNTER_SE"
#: 晴天基准天气（Carla WeatherParameters 语义）
_CLEAR_WEATHER: dict[str, float] = {
    "cloudiness": 0.0,
    "rain": 0.0,
    "wetness": 0.0,
    "fog": 0.0,
    "wind": 0.0,
    "sun_azimuth": 0.0,
    "sun_altitude": 45.0,
}


def _actor(
    actor_id: str,
    actor_type: ActorType,
    x: float,
    y: float,
    yaw: float,
    behavior_type: str,
    **params: Any,
) -> Actor:
    """构造模板参与者（behavior.parameters 为自由结构，与 4.2.2 一致）。"""
    return Actor(
        actor_id=actor_id,
        type=actor_type,
        spawn_point=SpawnPoint(x=x, y=y, yaw=yaw),
        behavior=ActorBehavior(behavior_type=behavior_type, parameters=dict(params)),
    )


def _event(
    event_id: str,
    event_type: SceneEventType,
    condition: str,
    action_type: str,
    **action_params: Any,
) -> SceneEvent:
    """构造模板事件（trigger.condition 为条件标识，action.parameters 为自由结构）。"""
    return SceneEvent(
        event_id=event_id,
        type=event_type,
        trigger=SceneEventTrigger(condition=condition, parameters={"source": "template"}),
        action=SceneEventAction(action_type=action_type, parameters=dict(action_params)),
    )


def _weather(**overrides: float) -> Weather:
    """按覆盖值构造天气段（未覆盖字段取晴天基准，7 字段全部必填）。"""
    return Weather(**{**_CLEAR_WEATHER, **overrides})


def _config(
    *,
    speed: float,
    steer: float,
    actors: list[Actor],
    events: list[SceneEvent],
    duration: float,
    weather: Weather | None = None,
) -> SceneConfig:
    """构造模板配置（成功判据取保守默认值，供场景编辑器二次调整）。"""
    return SceneConfig(
        map=SceneMap(
            map_id=_DEFAULT_MAP_ID,
            map_type=MapType.CARLA_TOWN,
            spawn_point=SpawnPoint(x=0.0, y=0.0, yaw=0.0, lane_id="lane-1"),
        ),
        ego_vehicle=EgoVehicle(
            model=_DEFAULT_EGO_MODEL, initial_speed=speed, initial_steer=steer
        ),
        weather=weather or _weather(),
        actors=actors,
        events=events,
        success_criteria=SuccessCriteria(
            max_speed_deviation=0.5, no_collision=True, min_safe_distance=3.0
        ),
        duration=duration,
    )


def _lead_vehicle(x: float, yaw: float = 0.0, speed: float = 1.0) -> Actor:
    """前车（跟车/切入/变道模板复用）。"""
    return _actor(
        "lead-vehicle", ActorType.VEHICLE, x, 0.0, yaw, "lane_follow", target_speed=speed
    )


def _build_config(scene_type: SceneType) -> SceneConfig:
    """按场景类型构造模板配置（穷举 SceneType，禁止静默回落）。"""
    builder = _CONFIG_BUILDERS.get(scene_type)
    if builder is None:  # pragma: no cover - 枚举穷举后不可达
        raise KeyError(f"未定义模板的场景类型: {scene_type}")
    return builder()


#: 类型 → 模板配置构造器（4.2.1 分类体系的叶子场景；不含 real_vehicle_replay，4.5 节无预置模板）
_CONFIG_BUILDERS: dict[SceneType, Any] = {
    SceneType.STRAIGHT_CRUISE: lambda: _config(
        speed=1.5, steer=0.0, actors=[], events=[], duration=30.0
    ),
    SceneType.CURVE_DRIVING: lambda: _config(
        speed=1.2, steer=0.2, actors=[], events=[], duration=30.0
    ),
    SceneType.CAR_FOLLOWING: lambda: _config(
        speed=1.5, steer=0.0, actors=[_lead_vehicle(10.0)], events=[], duration=30.0
    ),
    SceneType.LANE_CHANGE: lambda: _config(
        speed=1.5,
        steer=0.0,
        actors=[_lead_vehicle(15.0)],
        events=[
            _event(
                "ev-lane-change", SceneEventType.CUT_IN, "ego.position.x > 10", "lane_change",
                target_lane="right",
            )
        ],
        duration=25.0,
    ),
    SceneType.CUT_IN: lambda: _config(
        speed=1.5,
        steer=0.0,
        actors=[
            _actor(
                "cut-in-vehicle", ActorType.VEHICLE, 20.0, 3.5, 0.0, "cut_in", target_lane="ego_lane"
            )
        ],
        events=[
            _event(
                "ev-cut-in", SceneEventType.CUT_IN, "ego.position.x > 25", "speed_change",
                target_speed=1.0,
            )
        ],
        duration=30.0,
    ),
    SceneType.PEDESTRIAN_CROSSING: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[
            _actor(
                "crossing-pedestrian", ActorType.PEDESTRIAN, 25.0, -4.0, 1.5708, "cross_road",
                crossing_speed=1.2,
            )
        ],
        events=[
            _event("ev-ped-crossing", SceneEventType.PEDESTRIAN_CROSSING, "ego.position.x > 20", "stop")
        ],
        duration=30.0,
    ),
    SceneType.INTERSECTION_MEETING: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[
            _lead_vehicle(30.0),
            _actor(
                "cross-vehicle", ActorType.VEHICLE, 35.0, -8.0, 1.5708, "lane_follow",
                target_speed=1.0,
            ),
        ],
        events=[],
        duration=30.0,
    ),
    SceneType.CONSTRUCTION_DETOUR: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[_actor("construction-barrier", ActorType.OTHER, 15.0, 2.5, 0.0, "static")],
        events=[
            _event(
                "ev-construction-detour", SceneEventType.OBSTACLE_APPEARANCE,
                "ego.position.x > 10", "lane_change", target_lane="left",
            )
        ],
        duration=30.0,
    ),
    SceneType.RAINY: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[],
        events=[],
        duration=30.0,
        weather=_weather(cloudiness=90.0, rain=80.0, wetness=70.0, fog=10.0, wind=20.0),
    ),
    SceneType.NIGHT: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[],
        events=[],
        duration=30.0,
        weather=_weather(sun_altitude=-10.0, sun_azimuth=180.0),
    ),
    SceneType.FOGGY: lambda: _config(
        speed=0.8,
        steer=0.0,
        actors=[],
        events=[],
        duration=30.0,
        weather=_weather(cloudiness=60.0, fog=60.0, wetness=30.0),
    ),
    SceneType.BACKLIGHT: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[],
        events=[],
        duration=30.0,
        weather=_weather(sun_azimuth=90.0, sun_altitude=10.0),
    ),
    SceneType.EMERGENCY_BRAKING: lambda: _config(
        speed=1.5,
        steer=0.0,
        actors=[_lead_vehicle(15.0)],
        events=[
            _event("ev-emergency-brake", SceneEventType.EMERGENCY_BRAKE, "ttc < 1.5", "emergency_stop")
        ],
        duration=20.0,
    ),
    SceneType.OBSTACLE_APPEARANCE: lambda: _config(
        speed=1.5,
        steer=0.0,
        actors=[_actor("sudden-obstacle", ActorType.OTHER, 12.0, 0.5, 0.0, "static")],
        events=[
            _event(
                "ev-obstacle-appearance", SceneEventType.OBSTACLE_APPEARANCE,
                "ego.position.x > 8", "emergency_stop",
            )
        ],
        duration=20.0,
    ),
    SceneType.SENSOR_FAILURE: lambda: _config(
        speed=1.0,
        steer=0.0,
        actors=[],
        events=[_event("ev-sensor-failure", SceneEventType.SENSOR_FAILURE, "ego.position.x > 10", "stop")],
        duration=20.0,
    ),
    SceneType.MULTI_VEHICLE_MIXED: lambda: _config(
        speed=1.2,
        steer=0.0,
        actors=[
            _lead_vehicle(10.0),
            _actor("adjacent-vehicle", ActorType.VEHICLE, 18.0, 3.5, 0.0, "lane_follow", target_speed=1.0),
            _actor("crossing-pedestrian", ActorType.PEDESTRIAN, 22.0, -4.0, 1.5708, "cross_road"),
            _actor("roadside-object", ActorType.OTHER, 28.0, 4.5, 0.0, "static"),
        ],
        events=[
            _event(
                "ev-mixed-cut-in", SceneEventType.CUT_IN, "ego.position.x > 20", "speed_change",
                target_speed=0.5,
            )
        ],
        duration=40.0,
    ),
    SceneType.CUSTOM: lambda: _config(speed=1.0, steer=0.0, actors=[], events=[], duration=30.0),
}

#: 模板清单顺序（4.2.1 分类顺序；实车回放无预置模板故排除）
_TEMPLATE_TYPES: tuple[SceneType, ...] = tuple(
    scene_type
    for types in SCENE_TYPES_BY_TEMPLATE_CATEGORY.values()
    for scene_type in types
)


class SceneTemplateService:
    """场景模板服务（内置静态清单；按分类/场景类型过滤，不分页）。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._templates: tuple[SceneTemplate, ...] = tuple(
            self._build_template(scene_type) for scene_type in _TEMPLATE_TYPES
        )

    def list_templates(
        self, *, category: SceneTemplateCategory | None, scene_type: SceneType | None
    ) -> SceneTemplateListData:
        """查询模板列表（category / scene_type 过滤；实车回放无模板故不出现）。"""
        items = list(self._templates)
        if category is not None:
            allowed = set(SCENE_TYPES_BY_TEMPLATE_CATEGORY.get(category, ()))
            items = [item for item in items if item.scene_type in allowed]
        if scene_type is not None:
            items = [item for item in items if item.scene_type is scene_type]
        return SceneTemplateListData(items=items, total=len(items))

    def get_template(self, scene_type: SceneType) -> SceneTemplate | None:
        """按场景类型取模板（不存在返回 None）。"""
        return next((item for item in self._templates if item.scene_type is scene_type), None)

    @staticmethod
    def _build_template(scene_type: SceneType) -> SceneTemplate:
        """构造单个模板（template_id 命名 tpl-{scene_type}，config 可直提创建接口）。"""
        label = SCENE_TYPE_LABELS[scene_type]
        return SceneTemplate(
            template_id=f"tpl-{scene_type.value}",
            template_name=f"{label}模板",
            scene_type=scene_type,
            description=f"4.2.1 节预置{label}场景模板（config 可直接作为创建场景请求的 config）",
            config=_build_config(scene_type),
        )


__all__ = ["SceneTemplateService"]