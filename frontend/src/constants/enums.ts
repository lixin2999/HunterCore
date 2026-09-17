/**
 * 受控词表展示映射（车辆 / 事件 / 场景 / 仿真）
 * 取值域来源：系统约束第 12、13 条 + contracts/openapi/scene-service.yaml、data-collector.yaml 的 enum。
 * ⚠ 仅用于 UI 展示，禁止据此做业务分支判断（业务以契约 code/enum 为准）。
 */

/** 车辆状态（系统约束第 12 条：8 态，不可新增/更改） */
export const VEHICLE_STATUS_LABELS: Record<string, string> = {
  offline: '离线',
  online_idle: '在线空闲',
  auto_driving: '自动驾驶中',
  remote_controlled: '远程操控中',
  upgrading: 'OTA 升级中',
  charging: '充电中',
  fault: '故障',
  emergency: '紧急状态',
}

/** 车辆状态 → Element Plus 标签类型 */
export const VEHICLE_STATUS_TAG_TYPES: Record<string, string> = {
  offline: 'info',
  online_idle: 'success',
  auto_driving: 'primary',
  remote_controlled: 'warning',
  upgrading: 'warning',
  charging: 'info',
  fault: 'danger',
  emergency: 'danger',
}

/** 在线判定用的状态集合（offline 之外均视为在线） */
export const VEHICLE_ONLINE_STATUSES: readonly string[] = [
  'online_idle',
  'auto_driving',
  'remote_controlled',
  'upgrading',
  'charging',
  'fault',
  'emergency',
]

/** 事件等级（系统约束第 13 条：info 蓝 / warning 黄 / critical 红） */
export const EVENT_LEVEL_LABELS: Record<string, string> = {
  info: '提示',
  warning: '警告',
  critical: '严重',
}

/** 事件等级 → 颜色（专题色，与 global.css 变量同名含义） */
export const EVENT_LEVEL_COLORS: Record<string, string> = {
  info: '#409eff',
  warning: '#e6a23c',
  critical: '#f56c6c',
}

/** 事件类型（系统约束第 13 条：18 种，不可新增/更改） */
export const EVENT_TYPE_LABELS: Record<string, string> = {
  harsh_acceleration: '急加速',
  harsh_braking: '急刹车',
  harsh_turning: '急转弯',
  over_speed: '超速',
  collision_warning: '碰撞预警',
  manual_takeover: '人工接管',
  emergency_stop: '紧急停车',
  battery_low: '电量低',
  battery_critical: '电量严重不足',
  communication_loss: '通信中断',
  sensor_fault: '传感器故障',
  perception_fault: '感知故障',
  planning_fault: '规划故障',
  control_fault: '控制故障',
  ota_start: 'OTA 开始',
  ota_success: 'OTA 成功',
  ota_failed: 'OTA 失败',
  ota_rollback: 'OTA 回滚',
}

/** 事件类型 → 触发阈值（系统约束第 13 条，仅用于界面提示文案，不参与判定） */
export const EVENT_TYPE_THRESHOLDS: Record<string, string> = {
  harsh_acceleration: '纵向加速度 a > 3 m/s²',
  harsh_braking: '减速度 > 3 m/s²',
  harsh_turning: '横摆角速度 > 0.8 rad/s',
  over_speed: '超速 > 10%',
  collision_warning: 'TTC < 1.5 s',
  battery_low: 'SOC < 20%',
  battery_critical: 'SOC < 10%',
  communication_loss: '遥测中断 > 10 s',
}

/** 场景状态（scene-service 契约 scenes.status） */
export const SCENE_STATUS_LABELS: Record<string, string> = {
  draft: '草稿',
  published: '已发布',
  archived: '已归档',
}

/** 场景状态 → 标签类型 */
export const SCENE_STATUS_TAG_TYPES: Record<string, string> = {
  draft: 'info',
  published: 'success',
  archived: 'warning',
}

/** 场景类型（scene-service 契约 SceneType：18 种，不可新增） */
export const SCENE_TYPE_LABELS: Record<string, string> = {
  straight_cruise: '直线巡航',
  curve_driving: '弯道行驶',
  car_following: '跟车',
  lane_change: '变道',
  cut_in: '加塞切入',
  pedestrian_crossing: '行人横穿',
  intersection_meeting: '交叉口会车',
  construction_detour: '施工绕行',
  rainy: '雨天',
  night: '夜间',
  foggy: '雾天',
  backlight: '逆光',
  emergency_braking: '紧急制动',
  obstacle_appearance: '障碍物突现',
  sensor_failure: '传感器失效',
  multi_vehicle_mixed: '多车混行',
  custom: '自定义',
  real_vehicle_replay: '实车回放',
}

/** 场景模板分类（GET /api/v1/scene/templates?category=） */
export const SCENE_TEMPLATE_CATEGORY_LABELS: Record<string, string> = {
  basic: '基础场景',
  interactive: '交互场景',
  environment: '环境场景',
  corner_case: '极端场景',
  custom: '自定义',
}

/** 仿真状态（SceneRunData.status → SimulationStatus） */
export const SIMULATION_STATUS_LABELS: Record<string, string> = {
  pending: '排队中',
  running: '运行中',
  succeeded: '成功',
  failed: '失败',
  canceled: '已取消',
}

/** 仿真状态 → 标签类型 */
export const SIMULATION_STATUS_TAG_TYPES: Record<string, string> = {
  pending: 'info',
  running: 'primary',
  succeeded: 'success',
  failed: 'danger',
  canceled: 'info',
}

/** 仿真参与者类型（ActorType） */
export const ACTOR_TYPE_LABELS: Record<string, string> = {
  vehicle: '车辆',
  pedestrian: '行人',
  other: '其他',
}

/** 地图类型（MapType） */
export const MAP_TYPE_LABELS: Record<string, string> = {
  carla_town: 'Carla 城镇地图',
  hd_map: '高精地图',
  site_map: '园区地图',
}

/**
 * 场景导出格式（SceneExportFormat 契约 enum，不可更改）
 * carla_scenariorunner_xml = Carla ScenarioRunner XML；openscenario_1_2 = OpenSCENARIO 1.2
 */
export const SCENE_EXPORT_FORMAT_LABELS: Record<string, string> = {
  carla_scenariorunner_xml: 'Carla ScenarioRunner XML',
  openscenario_1_2: 'OpenSCENARIO 1.2',
}

/** 场景事件类型（SceneEventType） */
export const SCENE_EVENT_TYPE_LABELS: Record<string, string> = {
  collision: '碰撞',
  cut_in: '加塞',
  pedestrian_crossing: '行人横穿',
  emergency_brake: '紧急制动',
  manual_takeover: '人工接管',
  sensor_failure: '传感器失效',
  obstacle_appearance: '障碍物出现',
}
