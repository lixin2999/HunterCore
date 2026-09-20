"""场景 Schema（严格对齐 contracts/openapi/scene-service.yaml components.schemas）。

- 受控词表：SceneType（4.2.1 分类体系 18 个叶子场景）/ SceneStatus（hunter_common 单一事实来源）/
  SimulationStatus（4.4 节）/ MapType / ActorType / SceneEventType / SceneExportFormat；
- 场景配置结构（4.2.2 节，字段名不可更改）：SceneMap / EgoVehicle / Weather / Actor / SceneEvent /
  SuccessCriteria → 合成 SceneConfig（落库 scenes.config_json）；元信息 → SceneMeta（对应 scenes 列）；
- 请求体一律 ``extra="forbid"``（契约 additionalProperties: false，禁止未声明字段）。
"""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from hunter_common.database.enums import SceneStatus
from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import (
    ACTORS_MAX_ITEMS,
    EVENTS_MAX_ITEMS,
    SCENE_DESCRIPTION_MAX_LENGTH,
    SCENE_NAME_MAX_LENGTH,
    SCENE_NAME_MIN_LENGTH,
    SHA256_PATTERN,
    TAG_MAX_ITEMS,
    TAG_MAX_LENGTH,
    TAG_MIN_LENGTH,
    VERSION_PATTERN,
)


# =====================================================================
# 一、受控词表（取值不可新增/更改）
# =====================================================================
class SceneType(StrEnum):
    """场景类型（4.2.1 分类体系的叶子场景；「分类 → 类型」映射见 SCENE_TYPES_BY_CATEGORY）。"""

    STRAIGHT_CRUISE = "straight_cruise"
    CURVE_DRIVING = "curve_driving"
    CAR_FOLLOWING = "car_following"
    LANE_CHANGE = "lane_change"
    CUT_IN = "cut_in"
    PEDESTRIAN_CROSSING = "pedestrian_crossing"
    INTERSECTION_MEETING = "intersection_meeting"
    CONSTRUCTION_DETOUR = "construction_detour"
    RAINY = "rainy"
    NIGHT = "night"
    FOGGY = "foggy"
    BACKLIGHT = "backlight"
    EMERGENCY_BRAKING = "emergency_braking"
    OBSTACLE_APPEARANCE = "obstacle_appearance"
    SENSOR_FAILURE = "sensor_failure"
    MULTI_VEHICLE_MIXED = "multi_vehicle_mixed"
    CUSTOM = "custom"
    REAL_VEHICLE_REPLAY = "real_vehicle_replay"


class SceneCategory(StrEnum):
    """场景分类（4.2.1 节两级结构的第一级；模板筛选参数取值）。"""

    BASIC = "basic"
    INTERACTIVE = "interactive"
    ENVIRONMENT = "environment"
    CORNER_CASE = "corner_case"
    CUSTOM = "custom"
    REAL_VEHICLE = "real_vehicle"


class SceneTemplateCategory(StrEnum):
    """模板筛选分类（契约 templates 端点 category enum）。

    ⚠ 不含 ``real_vehicle``：4.5 节实车回放为自动提取产物，无预置模板（契约 category enum）。
    """

    BASIC = "basic"
    INTERACTIVE = "interactive"
    ENVIRONMENT = "environment"
    CORNER_CASE = "corner_case"
    CUSTOM = "custom"


class SimulationStatus(StrEnum):
    """Carla 仿真实例运行状态（4.4 节；⚠ 除 running 外取值由契约补充，待确认 #2/#9）。"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class MapType(StrEnum):
    """地图类型（契约 MapType；⚠ 编码值为契约提出）。"""

    CARLA_TOWN = "carla_town"
    HD_MAP = "hd_map"
    SITE_MAP = "site_map"


class ActorType(StrEnum):
    """参与者类型（与遥测 perception.object_types 域一致）。"""

    VEHICLE = "vehicle"
    PEDESTRIAN = "pedestrian"
    OTHER = "other"


class SceneEventType(StrEnum):
    """场景事件类型（契约 SceneEventType）。"""

    COLLISION = "collision"
    CUT_IN = "cut_in"
    PEDESTRIAN_CROSSING = "pedestrian_crossing"
    EMERGENCY_BRAKE = "emergency_brake"
    MANUAL_TAKEOVER = "manual_takeover"
    SENSOR_FAILURE = "sensor_failure"
    OBSTACLE_APPEARANCE = "obstacle_appearance"


class SceneExportFormat(StrEnum):
    """导出格式（4.3 节：Carla ScenarioRunner XML + OpenSCENARIO 1.2）。"""

    CARLA_SCENARIORUNNER_XML = "carla_scenariorunner_xml"
    OPENSCENARIO_1_2 = "openscenario_1_2"


class SceneSortField(StrEnum):
    """列表排序字段白名单（契约排序参数 enum；禁止任意字段排序以防注入）。"""

    CREATE_TIME = "create_time"
    UPDATE_TIME = "update_time"
    SCENE_NAME = "scene_name"
    VERSION = "version"


class SortOrder(StrEnum):
    """排序方向（契约 order 参数 enum）。"""

    ASC = "asc"
    DESC = "desc"


#: 分类 → 类型映射（契约 x-hunter-scene-types，并集必须等于 SceneType 全集）
SCENE_TYPES_BY_CATEGORY: dict[SceneCategory, tuple[SceneType, ...]] = {
    SceneCategory.BASIC: (
        SceneType.STRAIGHT_CRUISE,
        SceneType.CURVE_DRIVING,
        SceneType.CAR_FOLLOWING,
        SceneType.LANE_CHANGE,
    ),
    SceneCategory.INTERACTIVE: (
        SceneType.CUT_IN,
        SceneType.PEDESTRIAN_CROSSING,
        SceneType.INTERSECTION_MEETING,
        SceneType.CONSTRUCTION_DETOUR,
    ),
    SceneCategory.ENVIRONMENT: (
        SceneType.RAINY,
        SceneType.NIGHT,
        SceneType.FOGGY,
        SceneType.BACKLIGHT,
    ),
    SceneCategory.CORNER_CASE: (
        SceneType.EMERGENCY_BRAKING,
        SceneType.OBSTACLE_APPEARANCE,
        SceneType.SENSOR_FAILURE,
        SceneType.MULTI_VEHICLE_MIXED,
    ),
    SceneCategory.CUSTOM: (SceneType.CUSTOM,),
    SceneCategory.REAL_VEHICLE: (SceneType.REAL_VEHICLE_REPLAY,),
}

#: 类型 → 中文标签（契约 x-hunter-scene-types.categories[].type_labels）
SCENE_TYPE_LABELS: dict[SceneType, str] = {
    SceneType.STRAIGHT_CRUISE: "直线巡航",
    SceneType.CURVE_DRIVING: "弯道行驶",
    SceneType.CAR_FOLLOWING: "跟车行驶",
    SceneType.LANE_CHANGE: "变道行驶",
    SceneType.CUT_IN: "前车切入",
    SceneType.PEDESTRIAN_CROSSING: "行人横穿",
    SceneType.INTERSECTION_MEETING: "路口会车",
    SceneType.CONSTRUCTION_DETOUR: "施工绕行",
    SceneType.RAINY: "雨天",
    SceneType.NIGHT: "夜间",
    SceneType.FOGGY: "雾天",
    SceneType.BACKLIGHT: "强光逆光",
    SceneType.EMERGENCY_BRAKING: "紧急制动",
    SceneType.OBSTACLE_APPEARANCE: "障碍物突现",
    SceneType.SENSOR_FAILURE: "传感器失效",
    SceneType.MULTI_VEHICLE_MIXED: "多车混行",
    SceneType.CUSTOM: "自定义",
    SceneType.REAL_VEHICLE_REPLAY: "实车回放",
}

#: 类型 → 分类（由 SCENE_TYPES_BY_CATEGORY 派生，避免两处维护）
SCENE_CATEGORY_BY_TYPE: dict[SceneType, SceneCategory] = {
    scene_type: category
    for category, types in SCENE_TYPES_BY_CATEGORY.items()
    for scene_type in types
}

#: 模板筛选分类 → 类型（与契约 templates 端点 category enum 同源，排除实车回放）
SCENE_TYPES_BY_TEMPLATE_CATEGORY: dict[SceneTemplateCategory, tuple[SceneType, ...]] = {
    category: SCENE_TYPES_BY_CATEGORY[SceneCategory(category.value)]
    for category in SceneTemplateCategory
}

#: 导出格式 → 文件扩展名（契约 x-hunter-export.formats）
EXPORT_EXTENSION_BY_FORMAT: dict[SceneExportFormat, str] = {
    SceneExportFormat.CARLA_SCENARIORUNNER_XML: ".xml",
    SceneExportFormat.OPENSCENARIO_1_2: ".xosc",
}


def scene_category_for(scene_type: SceneType) -> SceneCategory:
    """返回场景类型所属分类（未知类型抛 KeyError，禁止静默降级）。"""
    return SCENE_CATEGORY_BY_TYPE[SceneType(scene_type)]


# =====================================================================
# 二、4.2.2 场景配置结构（字段名不可更改）
# =====================================================================
class SpawnPoint(BaseModel):
    """生成点（⚠ 4.2.2 节未定义内部结构，契约按 Carla transform 语义定义，待确认 #3）。"""

    x: float = Field(description="X 坐标（m）")
    y: float = Field(description="Y 坐标（m）")
    z: float = Field(default=0.0, description="Z 坐标（m）")
    yaw: float = Field(ge=-3.141592653589793, le=3.141592653589793, description="朝向（rad）")
    lane_id: str | None = Field(default=None, description="车道 ID（可选）")


class SceneMap(BaseModel):
    """场景地图段（4.2.2 节 map）。"""

    map_id: str = Field(min_length=1, max_length=128, description="地图标识")
    map_type: MapType = Field(description="地图类型")
    spawn_point: SpawnPoint = Field(description="自车生成点")


class EgoVehicle(BaseModel):
    """自车初始状态段（4.2.2 节 ego_vehicle）。"""

    model: str = Field(min_length=1, max_length=64, description="车型（车端基线 HUNTER SE）")
    initial_speed: float = Field(ge=0, description="初始速度（m/s）")
    initial_steer: float = Field(ge=-1, le=1, description="初始转向（归一化 -1..1）")


class Weather(BaseModel):
    """环境条件段（4.2.2 节 weather；7 字段全部必填，取值按 Carla WeatherParameters 语义）。"""

    cloudiness: float = Field(ge=0, le=100, description="云量 %")
    rain: float = Field(ge=0, le=100, description="降雨强度 %")
    wetness: float = Field(ge=0, le=100, description="地面积水 %")
    fog: float = Field(ge=0, le=100, description="雾浓度 %")
    wind: float = Field(ge=0, le=100, description="风速强度 %")
    sun_azimuth: float = Field(ge=0, le=360, description="太阳方位角（度）")
    sun_altitude: float = Field(ge=-90, le=90, description="太阳高度角（度，负值为夜间）")


class ActorBehavior(BaseModel):
    """参与者行为编排（4.2.2 节 actors[].behavior；⚠ 取值域待确认 #3）。"""

    behavior_type: str = Field(min_length=1, max_length=64, description="行为类型")
    parameters: dict[str, Any] = Field(default_factory=dict, description="行为参数（自由结构）")


class Actor(BaseModel):
    """场景参与者（4.2.2 节 actors[]）。"""

    actor_id: str = Field(min_length=1, max_length=64, description="场景内参与者唯一标识")
    type: ActorType = Field(description="参与者类型")
    spawn_point: SpawnPoint = Field(description="参与者生成点")
    behavior: ActorBehavior = Field(description="行为编排")


class SceneEventTrigger(BaseModel):
    """事件触发条件（4.2.2 节 events[].trigger；⚠ 内部结构待确认 #3）。"""

    condition: str = Field(min_length=1, max_length=256, description="触发条件标识/表达式")
    parameters: dict[str, Any] = Field(default_factory=dict, description="条件参数（自由结构）")


class SceneEventAction(BaseModel):
    """事件动作（4.2.2 节 events[].action；⚠ 内部结构待确认 #3）。"""

    action_type: str = Field(min_length=1, max_length=64, description="动作类型")
    parameters: dict[str, Any] = Field(default_factory=dict, description="动作参数（自由结构）")


class SceneEvent(BaseModel):
    """场景事件（4.2.2 节 events[]）。"""

    event_id: str = Field(min_length=1, max_length=64, description="场景内事件唯一标识")
    type: SceneEventType = Field(description="事件类型")
    trigger: SceneEventTrigger = Field(description="触发条件")
    action: SceneEventAction = Field(description="触发动作")


class SuccessCriteria(BaseModel):
    """场景成功判据（4.2.2 节 success_criteria）。"""

    max_speed_deviation: float = Field(ge=0, description="最大速度偏差（m/s）")
    no_collision: bool = Field(default=True, description="是否要求全程无碰撞")
    min_safe_distance: float = Field(ge=0, description="最小安全距离（m）")


class SceneConfig(BaseModel):
    """场景仿真配置（4.2.2 节仿真段，落库 scenes.config_json，7 字段全部必填）。"""

    map: SceneMap = Field(description="地图段")
    ego_vehicle: EgoVehicle = Field(description="自车段")
    weather: Weather = Field(description="环境条件段")
    actors: list[Actor] = Field(max_length=ACTORS_MAX_ITEMS, description="参与者列表（可为空）")
    events: list[SceneEvent] = Field(max_length=EVENTS_MAX_ITEMS, description="事件列表（可为空）")
    success_criteria: SuccessCriteria = Field(description="成功判据")
    duration: float = Field(gt=0, description="场景时长（s）；上限由部署配置校验（2001）")


#: 单个标签（契约 tags items：minLength 1 / maxLength 32）
TagStr = Annotated[str, Field(min_length=TAG_MIN_LENGTH, max_length=TAG_MAX_LENGTH)]


# =====================================================================
# 三、场景元信息 / 完整对象 / 模板（4.2.2 元信息段 + 业务数据段）
# =====================================================================
class SceneMeta(BaseModel):
    """场景元信息（4.2.2 元信息段，一一对应 scene_svc.scenes 列）。"""

    scene_id: UUID = Field(description="场景 ID（scenes.scene_id）")
    scene_name: str = Field(
        min_length=SCENE_NAME_MIN_LENGTH,
        max_length=SCENE_NAME_MAX_LENGTH,
        description="场景名称（存活场景内唯一）",
    )
    scene_type: SceneType = Field(description="场景类型")
    description: str | None = Field(
        default=None, max_length=SCENE_DESCRIPTION_MAX_LENGTH, description="场景描述"
    )
    version: str = Field(pattern=VERSION_PATTERN, description="语义化版本（导出写入 x-scenario-version）")
    creator: UUID = Field(description="创建者 user_id（逻辑外键 → user_svc.users）")
    tags: list[TagStr] = Field(default_factory=list, max_length=TAG_MAX_ITEMS, description="标签")
    status: SceneStatus = Field(description="场景状态（受控词表 draft/published/archived）")
    create_time: datetime = Field(description="创建时间（UTC）")
    update_time: datetime = Field(description="最后更新时间（UTC）")


class Scene(SceneMeta):
    """场景完整对象 = SceneMeta（元信息）+ config（4.2.2 仿真配置段）。"""

    config: SceneConfig = Field(description="场景配置（config_json）")


class SceneTemplate(BaseModel):
    """场景模板（4.2.1 分类体系预置；config 可直接作为创建请求的 config 提交）。"""

    template_id: str = Field(min_length=1, max_length=64, description="模板 ID")
    template_name: str = Field(min_length=1, max_length=128, description="模板名称")
    scene_type: SceneType = Field(description="模板场景类型")
    description: str | None = Field(default=None, description="模板描述")
    config: SceneConfig = Field(description="模板场景配置（4.2.2 结构）")


class SceneListData(BaseModel):
    """场景列表数据（GET /api/v1/scene）。"""

    items: list[Scene] = Field(description="当前页场景列表")
    total: int = Field(ge=0, description="满足筛选条件的总数（软删除数据不计入）")
    page: int = Field(ge=1, description="当前页码")
    page_size: int = Field(ge=1, description="每页条数（≤200）")


class SceneTemplateListData(BaseModel):
    """模板列表数据（GET /api/v1/scene/templates，不分页）。"""

    items: list[SceneTemplate] = Field(description="模板列表")
    total: int = Field(ge=0, description="模板总数（过滤后）")


class SceneDeleteData(BaseModel):
    """删除结果数据（DELETE /api/v1/scene/{scene_id}，软删除）。"""

    scene_id: UUID = Field(description="场景 ID")
    deleted: bool = Field(description="软删除结果（true=deleted_at 已写入）")


class SceneExportData(BaseModel):
    """导出结果数据（POST /api/v1/scene/export，含预签名下载地址）。"""

    format: SceneExportFormat = Field(description="导出格式")
    file_name: str = Field(description="导出文件名（含扩展名）")
    object_key: str = Field(description="MinIO hunter-scene-assets 对象键")
    size_bytes: int = Field(ge=0, description="文件字节数")
    sha256: str | None = Field(default=None, pattern=SHA256_PATTERN, description="文件 SHA-256")
    download_url: str = Field(description="预签名下载地址（900s，支持 Range 分片下载）")
    expires_in: int = Field(description="预签名过期秒数（契约固定 900）")


class SceneRunData(BaseModel):
    """下发结果数据（POST /api/v1/scene/{scene_id}/run，4.4 节第 5 步）。"""

    sim_instance_id: str = Field(description="Carla 仿真实例 ID")
    scene_id: UUID = Field(description="场景 ID")
    status: SimulationStatus = Field(description="仿真实例状态")
    started_at: datetime = Field(description="下发成功时间（UTC）")
    param_overrides: dict[str, Any] | None = Field(
        default=None, description="实际生效的参数覆盖（回显，便于追溯）"
    )


class SimulationProgress(BaseModel):
    """仿真实例进度快照（GET /api/v1/scene/simulations/{sim_instance_id}，G-20①）。

    ⚠ 进度字段 Carla 管理 API 未定义（待确认 #9），服务端宽松透传，缺失置 null。
    """

    sim_instance_id: str = Field(description="Carla 仿真实例 ID")
    scene_id: UUID | None = Field(default=None, description="关联场景 ID（Carla 携带时透传）")
    status: SimulationStatus = Field(description="仿真实例状态")
    progress_percent: float | None = Field(
        default=None, ge=0, le=100, description="完成百分比（Carla 未提供时 null）"
    )
    current_time_s: float | None = Field(default=None, ge=0, description="已仿真时长（秒）")
    total_time_s: float | None = Field(default=None, ge=0, description="场景总时长（秒）")
    message: str | None = Field(default=None, max_length=512, description="状态补充说明（失败原因等）")
    updated_at: datetime | None = Field(default=None, description="Carla 侧最后更新时间（UTC）")


class SimulationArtifact(BaseModel):
    """仿真产物（落 hunter-scene-assets 时换发预签名 URL，15 分钟）。"""

    name: str = Field(max_length=256, description="产物名称/文件名")
    object_key: str | None = Field(default=None, description="MinIO 对象键（非对象存储产物为 null）")
    size_bytes: int | None = Field(default=None, ge=0)
    download_url: str | None = Field(default=None, description="预签名下载地址（900s）")
    expires_in: int | None = Field(default=None, description="预签名过期秒数（仅 download_url 非空时）")


class SimulationResult(BaseModel):
    """仿真实例结果（GET …/simulations/{sim_instance_id}/result，G-20①；仅终态可查）。

    ⚠ 结果结构 Carla 侧未定义（待确认 #9），success_criteria_result 为宽松对象透传。
    """

    sim_instance_id: str = Field(description="Carla 仿真实例 ID")
    scene_id: UUID | None = Field(default=None, description="关联场景 ID（Carla 携带时透传）")
    status: SimulationStatus = Field(description="仿真实例状态（终态）")
    success: bool | None = Field(default=None, description="是否达成 success_criteria（未判定时 null）")
    success_criteria_result: dict[str, Any] | None = Field(
        default=None, description="成功判据逐项结果（透传，结构待确认 #9）"
    )
    message: str | None = Field(default=None, max_length=512, description="失败/取消原因等补充说明")
    artifacts: list[SimulationArtifact] = Field(default_factory=list, description="产物清单")
    started_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None, description="仿真结束时间（UTC）")


# =====================================================================
# 四、请求体（契约 additionalProperties: false → extra="forbid"）
# =====================================================================
class SceneCreateRequest(BaseModel):
    """创建场景请求（status/creator/version/create_time 由服务端生成，不接收客户端传值）。"""

    model_config = ConfigDict(extra="forbid")

    scene_name: str = Field(min_length=SCENE_NAME_MIN_LENGTH, max_length=SCENE_NAME_MAX_LENGTH)
    scene_type: SceneType
    description: str | None = Field(default=None, max_length=SCENE_DESCRIPTION_MAX_LENGTH)
    tags: list[TagStr] = Field(default_factory=list, max_length=TAG_MAX_ITEMS)
    config: SceneConfig


class SceneUpdateRequest(BaseModel):
    """更新场景请求（PUT 全量语义；status/version/creator 不可改）。"""

    model_config = ConfigDict(extra="forbid")

    scene_name: str = Field(min_length=SCENE_NAME_MIN_LENGTH, max_length=SCENE_NAME_MAX_LENGTH)
    scene_type: SceneType
    description: str | None = Field(default=None, max_length=SCENE_DESCRIPTION_MAX_LENGTH)
    tags: list[TagStr] = Field(default_factory=list, max_length=TAG_MAX_ITEMS)
    config: SceneConfig


class SceneDuplicateRequest(BaseModel):
    """复制场景请求（可省略请求体；new_scene_name 缺省按 <源名称>-copy 生成）。"""

    model_config = ConfigDict(extra="forbid")

    new_scene_name: str | None = Field(
        default=None,
        min_length=SCENE_NAME_MIN_LENGTH,
        max_length=SCENE_NAME_MAX_LENGTH,
        description="新场景名称（省略时按 <源名称>-copy 生成，冲突追加序号）",
    )


class ScenePublishRequest(BaseModel):
    """发布场景请求（可省略请求体；仅在显式传 version 时更新 scenes.version）。"""

    model_config = ConfigDict(extra="forbid")

    version: str | None = Field(default=None, pattern=VERSION_PATTERN, description="发布版本（需 ≥ 当前版本）")


class SceneExportRequest(BaseModel):
    """导出请求（契约 maxItems 50；批量导出为单文件集）。"""

    model_config = ConfigDict(extra="forbid")

    scene_ids: list[UUID] = Field(min_length=1, max_length=50, description="待导出场景 ID 列表")
    format: SceneExportFormat = Field(description="导出格式（4.3 节）")


class SceneSimConfig(BaseModel):
    """仿真运行配置（⚠ 设计文档未定义配置项，待确认 #2/#9）。"""

    model_config = ConfigDict(extra="forbid")

    allow_parallel: bool = Field(default=False, description="允许同一场景并行运行多个实例")


class SceneRunRequest(BaseModel):
    """场景下发请求（可省略请求体）；param_overrides 仅允许覆盖白名单路径。"""

    model_config = ConfigDict(extra="forbid")

    param_overrides: dict[str, Any] = Field(
        default_factory=dict, description="参数化覆盖（白名单路径深合并；键为点分路径）"
    )
    sim_config: SceneSimConfig = Field(default_factory=SceneSimConfig, description="仿真运行配置")


# =====================================================================
# 五、成功响应（data 已定型，便于前端 TS 类型生成）
# =====================================================================
class SceneResponse(ApiResponse[Scene]):
    """单场景响应（GET/POST/PUT/duplicate/publish）。"""


class SceneListResponse(ApiResponse[SceneListData]):
    """场景列表响应（GET /api/v1/scene）。"""


class SceneTemplateListResponse(ApiResponse[SceneTemplateListData]):
    """模板列表响应（GET /api/v1/scene/templates）。"""


class SceneDeleteResponse(ApiResponse[SceneDeleteData]):
    """删除响应（DELETE /api/v1/scene/{scene_id}）。"""


class SceneExportResponse(ApiResponse[SceneExportData]):
    """导出响应（POST /api/v1/scene/export）。"""


class SceneRunResponse(ApiResponse[SceneRunData]):
    """下发响应（POST /api/v1/scene/{scene_id}/run）。"""


class SimulationProgressResponse(ApiResponse[SimulationProgress]):
    """仿真进度查询响应（GET /api/v1/scene/simulations/{sim_instance_id}，G-20①）。"""


class SimulationResultResponse(ApiResponse[SimulationResult]):
    """仿真结果查询响应（GET …/simulations/{sim_instance_id}/result，G-20①）。"""


__all__ = [
    "EXPORT_EXTENSION_BY_FORMAT",
    "SCENE_CATEGORY_BY_TYPE",
    "SCENE_TYPES_BY_CATEGORY",
    "SCENE_TYPES_BY_TEMPLATE_CATEGORY",
    "SCENE_TYPE_LABELS",
    "Actor",
    "ActorBehavior",
    "ActorType",
    "EgoVehicle",
    "MapType",
    "Scene",
    "SceneCategory",
    "SceneConfig",
    "SceneCreateRequest",
    "SceneDeleteData",
    "SceneDeleteResponse",
    "SceneDuplicateRequest",
    "SceneEvent",
    "SceneEventAction",
    "SceneEventTrigger",
    "SceneEventType",
    "SceneExportData",
    "SceneExportFormat",
    "SceneExportRequest",
    "SceneExportResponse",
    "SceneListData",
    "SceneListResponse",
    "SceneMap",
    "SceneMeta",
    "ScenePublishRequest",
    "SceneResponse",
    "SceneRunData",
    "SceneRunRequest",
    "SceneRunResponse",
    "SceneSimConfig",
    "SceneSortField",
    "SceneStatus",
    "SceneTemplate",
    "SceneTemplateCategory",
    "SceneTemplateListData",
    "SceneTemplateListResponse",
    "SceneType",
    "SceneUpdateRequest",
    "SimulationArtifact",
    "SimulationProgress",
    "SimulationProgressResponse",
    "SimulationResult",
    "SimulationResultResponse",
    "SimulationStatus",
    "SortOrder",
    "SpawnPoint",
    "SuccessCriteria",
    "TagStr",
    "Weather",
    "scene_category_for",
]