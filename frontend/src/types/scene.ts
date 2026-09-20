/**
 * 场景生成（scene-service）类型
 * 来源：contracts/openapi/scene-service.yaml —— 字段名与契约完全一致（snake_case）。
 * 规则：枚举取值域严格对齐契约 enum，禁止新增/更改；behavior_type/trigger.condition/
 * action_type 契约未定义取值域，前端按字符串透传（不硬编码分支）。
 */
import type { PageData } from './common'

/** 场景状态（draft 可编辑 / published 可下发导出 / archived 只读可删） */
export type SceneStatus = 'draft' | 'published' | 'archived'

/** 场景类型（4.2.1 分类体系叶子场景，18 种，不可新增） */
export type SceneType =
  | 'straight_cruise'
  | 'curve_driving'
  | 'car_following'
  | 'lane_change'
  | 'cut_in'
  | 'pedestrian_crossing'
  | 'intersection_meeting'
  | 'construction_detour'
  | 'rainy'
  | 'night'
  | 'foggy'
  | 'backlight'
  | 'emergency_braking'
  | 'obstacle_appearance'
  | 'sensor_failure'
  | 'multi_vehicle_mixed'
  | 'custom'
  | 'real_vehicle_replay'

/** Carla 仿真实例状态 */
export type SimulationStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'canceled'

/** 地图类型 */
export type MapType = 'carla_town' | 'hd_map' | 'site_map'

/** 参与者类型（与遥测 perception.object_types 域一致） */
export type ActorType = 'vehicle' | 'pedestrian' | 'other'

/** 场景事件类型（契约 enum，不可新增） */
export type SceneEventType =
  | 'collision'
  | 'cut_in'
  | 'pedestrian_crossing'
  | 'emergency_brake'
  | 'manual_takeover'
  | 'sensor_failure'
  | 'obstacle_appearance'

/** 导出格式（Carla ScenarioRunner XML / OpenSCENARIO 1.2） */
export type SceneExportFormat = 'carla_scenariorunner_xml' | 'openscenario_1_2'

/** 场景模板分类 */
export type SceneTemplateCategory = 'basic' | 'interactive' | 'environment' | 'corner_case' | 'custom'

/** 生成点（Carla transform 语义：位置 m + 朝向 rad） */
export interface SpawnPoint {
  x: number
  y: number
  /** 默认 0.0 */
  z?: number
  /** 朝向，取值 [-π, π] */
  yaw: number
  lane_id?: string | null
}

/** 地图段 */
export interface SceneMap {
  /** Carla 地图名 / 高精地图 ID */
  map_id: string
  map_type: MapType
  spawn_point: SpawnPoint
}

/** 自车初始状态 */
export interface EgoVehicle {
  model: string
  initial_speed: number
  initial_steer: number
}

/** 天气（Carla 天气参数，0–1 归一化，sun_* 为角度） */
export interface Weather {
  cloudiness: number
  rain: number
  wetness: number
  fog: number
  wind: number
  sun_azimuth: number
  sun_altitude: number
}

/** 参与者行为编排（behavior_type 取值域待人工确认，按字符串透传） */
export interface ActorBehavior {
  behavior_type: string
  parameters?: Record<string, unknown>
}

/** 参与者 */
export interface Actor {
  actor_id: string
  type: ActorType
  spawn_point: SpawnPoint
  behavior: ActorBehavior
}

/** 事件触发条件（condition 取值域待人工确认） */
export interface SceneEventTrigger {
  condition: string
  parameters?: Record<string, unknown>
}

/** 事件动作（action_type 取值域待人工确认） */
export interface SceneEventAction {
  action_type: string
  parameters?: Record<string, unknown>
}

/** 场景事件 */
export interface SceneEvent {
  event_id: string
  type: SceneEventType
  trigger: SceneEventTrigger
  action: SceneEventAction
}

/** 成功判据 */
export interface SuccessCriteria {
  /** 速度偏差上限（m/s） */
  max_speed_deviation: number
  no_collision: boolean
  /** 最小安全距离（m） */
  min_safe_distance: number
}

/** 场景配置（4.2.2 节，字段名不可更改） */
export interface SceneConfig {
  map: SceneMap
  ego_vehicle: EgoVehicle
  weather: Weather
  actors: Actor[]
  events: SceneEvent[]
  success_criteria: SuccessCriteria
  /** 场景时长（秒） */
  duration: number
}

/** 场景元数据 */
export interface SceneMeta {
  scene_id: string
  scene_name: string
  scene_type: SceneType
  description: string | null
  version: string
  creator: string
  tags: string[]
  status: SceneStatus
  /** RFC3339 date-time 字符串 */
  create_time: string
  update_time: string
}

/** 场景实体（元数据 + 配置） */
export interface Scene extends SceneMeta {
  config?: SceneConfig
}

/** 场景模板 */
export interface SceneTemplate {
  template_id: string
  template_name: string
  scene_type: SceneType
  description: string | null
  config: SceneConfig
}

/** 列表响应数据 */
export type SceneListData = PageData<Scene>

/** 模板列表响应数据（无分页） */
export interface SceneTemplateListData {
  items: SceneTemplate[]
  total: number
}

/** 删除结果 */
export interface SceneDeleteData {
  scene_id: string
  deleted: boolean
}

/** 导出结果（download_url 有效期由 expires_in 指定） */
export interface SceneExportData {
  format: SceneExportFormat
  file_name: string
  object_key: string
  size_bytes: number
  sha256?: string | null
  download_url: string
  expires_in: number
}

/** 下发 Carla 仿真结果 */
export interface SceneRunData {
  sim_instance_id: string
  scene_id: string
  status: SimulationStatus
  /** RFC3339 date-time 字符串 */
  started_at: string
  param_overrides?: Record<string, unknown>
}

/** 仿真实例进度快照（GET /api/v1/scene/simulations/{sim_instance_id}，决策 G-20①） */
export interface SimulationProgress {
  sim_instance_id: string
  scene_id?: string | null
  status: SimulationStatus
  /** 完成百分比（Carla 未提供时 null，按 status 降级展示） */
  progress_percent?: number | null
  current_time_s?: number | null
  total_time_s?: number | null
  message?: string | null
  updated_at?: string | null
}

/** 仿真产物（object_key 落 hunter-scene-assets 时已换发预签名 URL，15 分钟） */
export interface SimulationArtifact {
  name: string
  object_key?: string | null
  size_bytes?: number | null
  download_url?: string | null
  expires_in?: number | null
}

/** 仿真实例结果（GET …/simulations/{sim_instance_id}/result，G-20①；仅终态可查） */
export interface SimulationResult {
  sim_instance_id: string
  scene_id?: string | null
  status: SimulationStatus
  success?: boolean | null
  success_criteria_result?: Record<string, unknown> | null
  message?: string | null
  artifacts: SimulationArtifact[]
  started_at?: string | null
  finished_at?: string | null
}

/** 创建/更新场景请求（同一结构） */
export interface SceneUpsertRequest {
  scene_name: string
  scene_type: SceneType
  description?: string | null
  tags?: string[]
  config: SceneConfig
}

/** 复制请求 */
export interface SceneDuplicateRequest {
  new_scene_name?: string | null
}

/** 发布请求 */
export interface ScenePublishRequest {
  version?: string | null
}

/** 导出请求（支持批量） */
export interface SceneExportRequest {
  scene_ids: string[]
  format: SceneExportFormat
}

/** 下发仿真请求 */
export interface SceneRunRequest {
  param_overrides?: Record<string, unknown>
  sim_config?: Record<string, unknown>
}

/** 场景列表查询参数（GET /api/v1/scene） */
export interface SceneListQuery {
  page?: number
  page_size?: number
  scene_type?: SceneType
  status?: SceneStatus
  tags?: string[]
  keyword?: string
  creator?: string
  sort?: 'create_time' | 'update_time' | 'scene_name' | 'version'
  order?: 'asc' | 'desc'
}
