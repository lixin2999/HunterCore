"""场景 Schema（严格对齐 contracts/openapi/scene-service.yaml components.schemas）。

- 受控词表：SceneType（数据驱动，取自 contracts/scene-taxonomy.yaml，不再固定 18 值）/
  SceneStatus（hunter_common 单一事实来源）/ SimulationStatus（4.4 节）/ MapType / ActorType /
  SceneEventType / SceneExportFormat；behavior_type / action_type / trigger.condition_type 与
  受控枚举参数值亦按同一字典入库校验；
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
from hunter_common.scene_taxonomy import get_scene_taxonomy
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

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
# 一、受控词表（数据驱动：契约 contracts/scene-taxonomy.yaml 单一事实来源）
# =====================================================================
#: 场景受控字典（进程内加载一次；文件缺失即报错，禁止降级）
_TAXONOMY = get_scene_taxonomy()


def ensure_scene_type(value: str) -> str:
    """校验 scene_type 是否在受控字典内；越界抛 ValueError（Pydantic 转 422/2001）。"""
    if value not in _TAXONOMY.scene_type_values:
        raise ValueError(f"非法 scene_type: {value}")
    return value


def ensure_behavior_type(value: str) -> str:
    """校验参与者 behavior_type 是否受控。"""
    if value not in _TAXONOMY.behavior_type_values:
        raise ValueError(f"非法 behavior_type: {value}")
    return value


def ensure_action_type(value: str) -> str:
    """校验事件 action_type 是否受控。"""
    if value not in _TAXONOMY.action_type_values:
        raise ValueError(f"非法 action_type: {value}")
    return value


def ensure_condition_type(value: str) -> str:
    """校验触发器 condition_type 是否受控。"""
    if value not in _TAXONOMY.condition_type_values:
        raise ValueError(f"非法 trigger.condition_type: {value}")
    return value


def validate_enumerable_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    """校验受控枚举参数键的取值（rule/edge/coordinate_system/... 出现即拦截越界值）。

    自由结构键（result_id/window_start/extracted_from 等）不拒绝，兼容实车回放自动提取。
    """
    for key, allowed in _TAXONOMY.enumerable_parameter_values.items():
        if key in parameters and parameters[key] not in allowed:
            raise ValueError(f"参数 {key}={parameters[key]!r} 越界，允许: {sorted(allowed)}")
    return parameters


#: SceneType：受控字典校验的字符串（不再固定 18 值，编辑 YAML 即可扩充）
SceneType = Annotated[str, AfterValidator(ensure_scene_type)]
#: behavior_type / action_type / condition_type 受控字符串
BehaviorType = Annotated[str, AfterValidator(ensure_behavior_type)]
ActionType = Annotated[str, AfterValidator(ensure_action_type)]
ConditionType = Annotated[str, AfterValidator(ensure_condition_type)]


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
    """Carla 仿真实例运行状态（4.4 节；平台状态枚举不扩充）。"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class SimulationMode(StrEnum):
    """仿真模式（数据采集与分析系统 ↔ CARLA 详细设计 6.1.1）。"""

    SIL = "sil"
    VIL = "vil"
    REPLAY = "replay"


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


#: 分类 → 类型（由受控字典派生，保持 YAML 声明顺序；并集等于 scene_type 全集）
SCENE_TYPES_BY_CATEGORY: dict[SceneCategory, tuple[str, ...]] = {
    SceneCategory(category): tuple(types)
    for category, types in _TAXONOMY.types_by_category().items()
    if category in {c.value for c in SceneCategory}
}

#: 类型 → 中文标签（契约 x-hunter-scene-types；由字典派生）
SCENE_TYPE_LABELS: dict[str, str] = dict(_TAXONOMY.scene_type_labels)

#: 类型 → 分类（由字典派生，避免两处维护）
SCENE_CATEGORY_BY_TYPE: dict[str, SceneCategory] = {
    type_value: SceneCategory(category)
    for type_value, category in _TAXONOMY.scene_type_to_category.items()
}

#: 模板筛选分类 → 类型（与契约 templates 端点 category enum 同源，排除实车回放）
SCENE_TYPES_BY_TEMPLATE_CATEGORY: dict[SceneTemplateCategory, tuple[str, ...]] = {
    category: SCENE_TYPES_BY_CATEGORY[SceneCategory(category.value)]
    for category in SceneTemplateCategory
}

#: 导出格式 → 文件扩展名（契约 x-hunter-export.formats）
EXPORT_EXTENSION_BY_FORMAT: dict[SceneExportFormat, str] = {
    SceneExportFormat.CARLA_SCENARIORUNNER_XML: ".xml",
    SceneExportFormat.OPENSCENARIO_1_2: ".xosc",
}


def scene_category_for(scene_type: str) -> SceneCategory:
    """返回场景类型所属分类（先校验受控，未知类型抛 ValueError，禁止静默降级）。"""
    return SCENE_CATEGORY_BY_TYPE[ensure_scene_type(scene_type)]


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
    """参与者行为编排（4.2.2 节 actors[].behavior；behavior_type 受控字典校验）。"""

    behavior_type: BehaviorType = Field(description="行为类型（受控字典）")
    parameters: dict[str, Any] = Field(default_factory=dict, description="行为参数（自由结构）")


class Actor(BaseModel):
    """场景参与者（4.2.2 节 actors[]）。"""

    actor_id: str = Field(min_length=1, max_length=64, description="场景内参与者唯一标识")
    type: ActorType = Field(description="参与者类型")
    spawn_point: SpawnPoint = Field(description="参与者生成点")
    behavior: ActorBehavior = Field(description="行为编排")


class SceneEventTrigger(BaseModel):
    """事件触发条件（4.2.2 节 events[].trigger；condition_type 受控，parameters 自由结构）。"""

    condition: str = Field(min_length=1, max_length=256, description="触发条件标识/表达式")
    condition_type: ConditionType | None = Field(
        default=None, description="触发条件类型（受控字典，可选）"
    )
    parameters: dict[str, Any] = Field(default_factory=dict, description="条件参数（自由结构）")

    @model_validator(mode="after")
    def _check_enumerable(self) -> SceneEventTrigger:
        validate_enumerable_parameters(self.parameters)
        return self


class SceneEventAction(BaseModel):
    """事件动作（4.2.2 节 events[].action；action_type 受控，parameters 自由结构）。"""

    action_type: ActionType = Field(description="动作类型（受控字典）")
    parameters: dict[str, Any] = Field(default_factory=dict, description="动作参数（自由结构）")

    @model_validator(mode="after")
    def _check_enumerable(self) -> SceneEventAction:
        validate_enumerable_parameters(self.parameters)
        return self


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
    sim_id: str | None = Field(default=None, description="平台仿真会话 ID（动态 Topic 前缀）")
    scene_id: UUID = Field(description="场景 ID")
    mode: SimulationMode = Field(default=SimulationMode.SIL, description="仿真模式")
    vehicle_id: str | None = Field(default=None, description="VIL/回放关联车辆 ID")
    status: SimulationStatus = Field(description="仿真实例状态")
    carla_status: str | None = Field(default=None, description="CARLA 原始状态（排障用）")
    vil_state_topic: str | None = Field(default=None, description="VIL 状态 Topic（mode=vil）")
    sensor_data_topic: str | None = Field(default=None, description="仿真回传 Topic")
    stream_url: str | None = Field(default=None, description="CARLA 视频流地址（服务端返回时透传）")
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
    mode: SimulationMode | None = Field(default=None, description="仿真模式（会话登记时回填）")
    vehicle_id: str | None = Field(default=None, description="VIL/回放关联车辆 ID")
    status: SimulationStatus = Field(description="平台状态")
    carla_status: str | None = Field(default=None, description="CARLA 原始状态（排障用）")
    progress_percent: float | None = Field(
        default=None, ge=0, le=100, description="完成百分比（Carla 未提供时 null）"
    )
    current_time_s: float | None = Field(default=None, ge=0, description="已仿真时长（秒）")
    total_time_s: float | None = Field(default=None, ge=0, description="场景总时长（秒）")
    actors_count: int | None = Field(default=None, ge=0, description="参与者数量")
    collisions: int | None = Field(default=None, ge=0, description="碰撞次数")
    lane_invasions: int | None = Field(default=None, ge=0, description="车道入侵次数")
    ego_state: dict[str, Any] | None = Field(default=None, description="VIL 自车位姿/速度/转角")
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
    mode: SimulationMode | None = Field(default=None, description="仿真模式")
    vehicle_id: str | None = Field(default=None, description="VIL/回放关联车辆 ID")
    status: SimulationStatus = Field(description="仿真实例状态（终态）")
    carla_status: str | None = Field(default=None, description="CARLA 原始状态")
    success: bool | None = Field(default=None, description="是否达成 success_criteria（未判定时 null）")
    success_criteria_result: dict[str, Any] | None = Field(
        default=None, description="成功判据逐项结果（透传，结构待确认 #9）"
    )
    evaluation: dict[str, Any] | None = Field(default=None, description="安全/效率/舒适/任务评估")
    message: str | None = Field(default=None, max_length=512, description="失败/取消原因等补充说明")
    artifacts: list[SimulationArtifact] = Field(default_factory=list, description="产物清单")
    started_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None, description="仿真结束时间（UTC）")


class SimulationControlData(BaseModel):
    """仿真控制结果（pause / resume / cancel）。"""

    sim_instance_id: str = Field(description="Carla 仿真实例 ID")
    status: SimulationStatus = Field(description="控制后的平台状态")
    carla_status: str | None = Field(default=None, description="控制后的 CARLA 原始状态")
    message: str | None = Field(default=None, max_length=512, description="补充说明（如 paused）")


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


class SimulationQuality(StrEnum):
    """CARLA 渲染/物理质量档位（CARLA 管理 API 创建参数）。"""

    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


class VilCalibration(BaseModel):
    """VIL 会话坐标标定（平台只做结构校验，坐标语义由 CARLA 校验）。"""

    model_config = ConfigDict(extra="forbid")

    calibration_id: str = Field(min_length=1, max_length=128, description="标定 ID（追溯用）")
    map_x: float = Field(description="标定参考点地图 X / m")
    map_y: float = Field(description="标定参考点地图 Y / m")
    map_heading: float = Field(description="标定参考点地图航向 rad")
    odom_x: float = Field(description="对应 odom X / m")
    odom_y: float = Field(description="对应 odom Y / m")
    odom_heading: float = Field(description="对应 odom 航向 rad")


class VilConfig(BaseModel):
    """VIL 数据面参数（CARLA vil-mapper 使用）。"""

    model_config = ConfigDict(extra="forbid")

    calibration: VilCalibration = Field(description="坐标标定（VIL 必填）")
    target_hz: int = Field(default=10, ge=1, le=30, description="VIL 状态目标频率 Hz")
    data_timeout_ms: int = Field(default=500, ge=50, le=5000, description="数据超时阈值 ms")
    extrapolation_ms: int = Field(default=150, ge=0, le=2000, description="丢帧外推上限 ms")


class ReplayConfig(BaseModel):
    """实车数据回放配置（replay 模式）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_id: str = Field(
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$", description="回放数据来源车辆 ID"
    )
    start_time: float = Field(ge=0, description="回放起始时间 Unix epoch 秒")
    end_time: float = Field(ge=0, description="回放结束时间 Unix epoch 秒")
    include_sensors: bool = Field(default=True, description="是否回放传感器数据")

    @model_validator(mode="after")
    def _validate_window(self) -> ReplayConfig:
        if self.end_time <= self.start_time:
            raise ValueError("replay_config.end_time 必须大于 start_time")
        return self


class SceneSimConfig(BaseModel):
    """仿真运行配置（CARLA 质量档位 + VIL 参数）。"""

    model_config = ConfigDict(extra="forbid")

    allow_parallel: bool = Field(default=False, description="允许同一场景并行运行多个实例")
    quality: SimulationQuality = Field(default=SimulationQuality.MEDIUM, description="渲染/物理质量档位")
    vil: VilConfig | None = Field(default=None, description="VIL 数据面参数（mode=vil 时必填）")


class SceneRunRequest(BaseModel):
    """场景下发请求（可省略请求体）；param_overrides 仅允许覆盖白名单路径。

    - ``mode=sil``：默认，vehicle_id 可为空；
    - ``mode=vil``：vehicle_id + sim_config.vil 必填，同一车辆同时仅允许一个活动会话；
    - ``mode=replay``：vehicle_id + replay_config 必填。
    """

    model_config = ConfigDict(extra="forbid")

    param_overrides: dict[str, Any] = Field(
        default_factory=dict, description="参数化覆盖（白名单路径深合并；键为点分路径）"
    )
    sim_config: SceneSimConfig = Field(default_factory=SceneSimConfig, description="仿真运行配置")
    mode: SimulationMode = Field(default=SimulationMode.SIL, description="仿真模式 sil/vil/replay")
    vehicle_id: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
        description="VIL/回放关联车辆 ID；SIL 可为空",
    )
    replay_config: ReplayConfig | None = Field(default=None, description="回放配置（mode=replay 必填）")

    @model_validator(mode="after")
    def _validate_mode(self) -> SceneRunRequest:
        if self.mode is SimulationMode.VIL:
            if not self.vehicle_id:
                raise ValueError("mode=vil 时必须提供 vehicle_id")
            if self.sim_config.vil is None:
                raise ValueError("mode=vil 时必须提供 sim_config.vil")
        if self.mode is SimulationMode.REPLAY:
            if not self.vehicle_id:
                raise ValueError("mode=replay 时必须提供 vehicle_id")
            if self.replay_config is None:
                raise ValueError("mode=replay 时必须提供 replay_config")
        return self


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


class SimulationControlResponse(ApiResponse[SimulationControlData]):
    """仿真控制响应（POST …/simulations/{sim_instance_id}/pause|resume|cancel）。"""


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
    "ReplayConfig",
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
    "SimulationControlData",
    "SimulationControlResponse",
    "SimulationMode",
    "SimulationProgress",
    "SimulationProgressResponse",
    "SimulationQuality",
    "SimulationResult",
    "SimulationResultResponse",
    "SimulationStatus",
    "SortOrder",
    "SpawnPoint",
    "SuccessCriteria",
    "TagStr",
    "VilCalibration",
    "VilConfig",
    "Weather",
    "scene_category_for",
]