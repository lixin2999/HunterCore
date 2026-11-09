"""场景模板服务（4.2.1 分类体系预置模板，供场景编辑器一键创建）。

⚠ 契约待确认项 #11：模板数据来源（内置静态清单 / DB 表 / MinIO 资源）设计文档未定义；
本实现采用「内置静态清单」——不新增 DB 表、不引入运维依赖，模板 config 即 4.2.2 结构，
可直接作为 ``POST /api/v1/scene`` 的 config 提交。人工确认来源后仅需替换本模块数据源。

参与者/事件/天气/时长**不再逐类型手写**，而是由 ``contracts/scene-taxonomy.yaml::scene_semantics``
语义契约实例化（与 ``scripts/batch_add_scenes.py``、场景引擎目录、用例派生共用同一份事实来源），
因此「新增受控类型只需补语义」即可一键创建模板，且不会退化成「1 辆前车」的分类兜底：
鬼探头场景天然含行人 + 遮挡物、儿童追球场景天然含儿童 + 球，模板同样受 G9 语义完整性闸门约束。
``custom``（人工自定义）无语义契约，保留最小可编辑骨架。
``real_vehicle_replay``（4.5 节自动提取产物）无预置模板，故不在模板清单与分类筛选枚举中。
"""
from __future__ import annotations

from hunter_common.logging import get_logger

from app.config import Settings
from app.scene_engine.semantics import (
    build_actors,
    build_duration,
    build_ego,
    build_events,
    build_weather,
    require_semantics,
    semantics_for,
)
from app.schemas.scene import (
    SCENE_TYPE_LABELS,
    SCENE_TYPES_BY_TEMPLATE_CATEGORY,
    Actor,
    EgoVehicle,
    MapType,
    SceneConfig,
    SceneEvent,
    SceneMap,
    SceneTemplate,
    SceneTemplateCategory,
    SceneTemplateListData,
    SpawnPoint,
    SuccessCriteria,
    Weather,
)

logger = get_logger("app.services.templates")

#: 默认地图（Carla 内置城镇；⚠ 地图资源清单待确认）
_DEFAULT_MAP_ID = "Town01"
#: 车端基线车型（HUNTER SE）
_DEFAULT_EGO_MODEL = "HUNTER_SE"
#: 晴天基准天气（Carla WeatherParameters 语义；仅 custom 骨架使用）
_CLEAR_WEATHER: dict[str, float] = {
    "cloudiness": 0.0,
    "rain": 0.0,
    "wetness": 0.0,
    "fog": 0.0,
    "wind": 0.0,
    "sun_azimuth": 0.0,
    "sun_altitude": 45.0,
}


def _default_map() -> SceneMap:
    """模板地图段（出生点为目录占位基准 (0,0,0)，有效坐标由仿真端选取）。"""
    return SceneMap(
        map_id=_DEFAULT_MAP_ID,
        map_type=MapType.CARLA_TOWN,
        spawn_point=SpawnPoint(x=0.0, y=0.0, yaw=0.0, lane_id="lane-1"),
    )


def _criteria() -> SuccessCriteria:
    """成功判据保守默认值（供场景编辑器二次调整）。"""
    return SuccessCriteria(max_speed_deviation=0.5, no_collision=True, min_safe_distance=3.0)


def _semantics_config(scene_type: str) -> SceneConfig:
    """按语义契约实例化模板配置（参与者角色×数量 / 事件 / 天气 / 时长 / 自车）。"""
    semantics = require_semantics(scene_type)
    return SceneConfig(
        map=_default_map(),
        ego_vehicle=EgoVehicle(**build_ego(semantics, model=_DEFAULT_EGO_MODEL)),
        weather=Weather(**build_weather(semantics)),
        actors=[Actor.model_validate(item) for item in build_actors(semantics)],
        events=[SceneEvent.model_validate(item) for item in build_events(semantics)],
        success_criteria=_criteria(),
        duration=build_duration(semantics),
    )


def _custom_config() -> SceneConfig:
    """人工自定义模板：无语义契约，保留最小可编辑骨架（不代猜参与者）。"""
    return SceneConfig(
        map=_default_map(),
        ego_vehicle=EgoVehicle(model=_DEFAULT_EGO_MODEL, initial_speed=8.0, initial_steer=0.0),
        weather=Weather(**_CLEAR_WEATHER),
        actors=[],
        events=[],
        success_criteria=_criteria(),
        duration=30.0,
    )


def _build_config(scene_type: str) -> SceneConfig:
    """按场景类型构造模板配置：全部由语义契约驱动，仅 ``custom`` 走空骨架。"""
    if scene_type == "custom":
        return _custom_config()
    return _semantics_config(scene_type)


#: 模板清单顺序（受控字典分类顺序；实车回放无预置模板故排除）
_TEMPLATE_TYPES: tuple[str, ...] = tuple(
    scene_type
    for types in SCENE_TYPES_BY_TEMPLATE_CATEGORY.values()
    for scene_type in types
)

#: 需语义契约的模板类型（custom 为人工骨架，不受契约约束）
_SEMANTIC_TEMPLATE_TYPES: frozenset[str] = frozenset(_TEMPLATE_TYPES) - {"custom"}
_MISSING_SEMANTICS: list[str] = sorted(
    scene_type for scene_type in _SEMANTIC_TEMPLATE_TYPES if semantics_for(scene_type) is None
)
if _MISSING_SEMANTICS:  # 新增类型忘写语义时启动即失败，而非静默退化
    raise RuntimeError(
        "以下模板类型缺少 scene_semantics 语义契约（请在 contracts/scene-taxonomy.yaml 补齐）："
        + ", ".join(_MISSING_SEMANTICS)
    )


class SceneTemplateService:
    """场景模板服务（内置静态清单；按分类/场景类型过滤，不分页）。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._templates: tuple[SceneTemplate, ...] = tuple(
            self._build_template(scene_type) for scene_type in _TEMPLATE_TYPES
        )

    def list_templates(
        self, *, category: SceneTemplateCategory | None, scene_type: str | None
    ) -> SceneTemplateListData:
        """查询模板列表（category / scene_type 过滤；实车回放无模板故不出现）。"""
        items = list(self._templates)
        if category is not None:
            allowed = set(SCENE_TYPES_BY_TEMPLATE_CATEGORY.get(category, ()))
            items = [item for item in items if item.scene_type in allowed]
        if scene_type is not None:
            items = [item for item in items if item.scene_type == scene_type]
        return SceneTemplateListData(items=items, total=len(items))

    def get_template(self, scene_type: str) -> SceneTemplate | None:
        """按场景类型取模板（不存在返回 None）。"""
        return next((item for item in self._templates if item.scene_type == scene_type), None)

    @staticmethod
    def _build_template(scene_type: str) -> SceneTemplate:
        """构造单个模板（template_id 命名 tpl-{scene_type}，config 可直提创建接口）。"""
        label = SCENE_TYPE_LABELS[scene_type]
        semantics = semantics_for(scene_type)
        objective = semantics.objective if semantics is not None else "人工自定义场景"
        return SceneTemplate(
            template_id=f"tpl-{scene_type}",
            template_name=f"{label}模板",
            scene_type=scene_type,
            description=(
                f"4.2.1 节预置{label}场景模板：{objective}"
                "（config 可直接作为创建场景请求的 config；参与者/事件由语义契约生成）"
            ),
            config=_build_config(scene_type),
        )


__all__ = ["SceneTemplateService"]
