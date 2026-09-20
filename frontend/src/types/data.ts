/**
 * 数据采集（data-collector）类型
 * 来源：contracts/openapi/data-collector.yaml —— 字段名与契约完全一致（snake_case）。
 * 遥测嵌套结构对齐系统约束第 5 节（字段名、类型、嵌套结构不可更改）。
 */

/** 遥测-底盘 */
export interface TelemetryChassis {
  velocity?: number | null
  steering_angle?: number | null
  battery_voltage?: number | null
  battery_soc?: number | null
  battery_current?: number | null
  battery_temp?: number | null
  /** 控制模式（CAN / RC / AUTO） */
  control_mode?: string | null
  /** 车辆运行状态 */
  vehicle_state?: string | null
  fault_code?: number | null
  /** 三电机转速 [rpm] */
  motor_rpm?: number[]
  motor_current?: number[]
  motor_temp?: number[]
}

/** 遥测-定位 */
export interface TelemetryLocalization {
  x?: number | null
  y?: number | null
  z?: number | null
  roll?: number | null
  pitch?: number | null
  heading?: number | null
  linear_velocity?: number[]
  angular_velocity?: number[]
  position_std?: number | null
  heading_std?: number | null
}

/** 遥测-感知 */
export interface TelemetryPerception {
  detected_objects?: number | null
  fps?: number | null
  latency_ms?: number | null
  /** { vehicle, pedestrian, other } 计数 */
  object_types?: Record<string, number> | null
}

/** 遥测-规划 */
export interface TelemetryPlanning {
  trajectory_length?: number | null
  trajectory_points?: number | null
  planning_latency_ms?: number | null
  current_behavior?: string | null
}

/** 遥测-控制 */
export interface TelemetryControl {
  target_velocity?: number | null
  target_steer?: number | null
  velocity_error?: number | null
  steer_error?: number | null
  control_latency_ms?: number | null
}

/** 遥测-系统 */
export interface TelemetrySystem {
  cpu_usage?: number | null
  gpu_usage?: number | null
  memory_usage_mb?: number | null
  gpu_temp?: number | null
  cpu_temp?: number | null
  network_rssi?: number | null
  network_latency_ms?: number | null
}

/** 遥测采样点（TimescaleDB vehicle_telemetry 投影） */
export interface TelemetrySample {
  /** Unix epoch 秒（含小数） */
  time: number
  vehicle_id: string
  seq?: number | null
  chassis?: TelemetryChassis
  localization?: TelemetryLocalization
  perception?: TelemetryPerception
  planning?: TelemetryPlanning
  control?: TelemetryControl
  system?: TelemetrySystem
}

/** 遥测查询响应数据（retention_days = 90，系统约束第 6 条） */
export interface TelemetryQueryData {
  items: TelemetrySample[]
  total: number
  page: number
  page_size: number
  retention_days: number
}

/** 遥测查询参数（GET /api/v1/data/telemetry） */
export interface TelemetryQuery {
  vehicle_id?: string
  start_time?: number
  end_time?: number
  page?: number
  page_size?: number
  order?: 'asc' | 'desc'
}

/** 事件类型（19 种受控词表，系统约束第 13 条；G-22② 新增 collision_pre_warning） */
export type EventType =
  | 'harsh_acceleration'
  | 'harsh_braking'
  | 'harsh_turning'
  | 'over_speed'
  | 'collision_pre_warning'
  | 'collision_warning'
  | 'manual_takeover'
  | 'emergency_stop'
  | 'battery_low'
  | 'battery_critical'
  | 'communication_loss'
  | 'sensor_fault'
  | 'perception_fault'
  | 'planning_fault'
  | 'control_fault'
  | 'ota_start'
  | 'ota_success'
  | 'ota_failed'
  | 'ota_rollback'

/** 事件等级（3 级，由事件类型决定，前端不得放宽映射） */
export type EventLevel = 'info' | 'warning' | 'critical'

/** 事件记录（data_collector.events） */
export interface EventItem {
  /** 自增主键（int64，服务端保证在业务可用范围内） */
  event_id: number
  vehicle_id: string
  event_type: EventType
  event_level: EventLevel
  /** Unix epoch 秒 */
  event_time: number
  description?: string | null
  /** 事件附加数据（JSONB） */
  data_json: Record<string, unknown>
  /** MinIO object_key（如 hunter-rosbag/…） */
  data_file_url?: string | null
  /** 预签名下载 URL（有效期由服务端决定） */
  data_file_download_url?: string | null
  acknowledged: boolean
  acknowledged_by?: string | null
  acknowledge_time?: number | null
}

/** 事件列表响应数据 */
export interface EventListData {
  items: EventItem[]
  total: number
  page: number
  page_size: number
}

/** 事件列表查询参数（GET /api/v1/data/events） */
export interface EventListQuery {
  vehicle_id?: string
  event_type?: EventType
  event_level?: EventLevel
  acknowledged?: boolean
  start_time?: number
  end_time?: number
  page?: number
  page_size?: number
  sort?: 'event_time' | 'event_level' | 'event_type'
  order?: 'asc' | 'desc'
}

/** 允许车端/前端上传的 Bucket（仅 3 个，系统约束第 7 条） */
export type UploadBucket = 'hunter-raw-data' | 'hunter-rosbag' | 'hunter-video'

/** 文件数据类型 */
export type FileDataType = 'point_cloud' | 'camera_image' | 'rosbag' | 'video' | 'other'

/** 上传方式（put 单次直传 / multipart 分片） */
export type UploadMethod = 'put' | 'multipart'

/** 预签名请求（POST /api/v1/data/files/presign） */
export interface FilePresignRequest {
  vehicle_id: string
  bucket: UploadBucket
  data_type: FileDataType
  file_name: string
  file_size: number
  /** 分片上传时提供 */
  part_count?: number
  content_type?: string | null
  sha256?: string | null
}

/** 预签名分片（part_number 从 1 开始） */
export interface FilePresignPart {
  part_number: number
  upload_url: string
}

/** 预签名结果（上传有效期 1 小时，系统约束第 7 条） */
export interface FilePresignData {
  bucket: UploadBucket
  object_key: string
  method: UploadMethod
  upload_url?: string | null
  upload_id?: string | null
  parts?: FilePresignPart[]
  expires_in: number
}

/** 已上传分片（complete 时回传 ETag） */
export interface CompletedPart {
  part_number: number
  etag: string
}

/** 上传完成确认请求（POST /api/v1/data/files/complete） */
export interface FileCompleteRequest {
  vehicle_id: string
  bucket: UploadBucket
  object_key: string
  size_bytes: number
  md5: string
  sha256: string
  data_type?: FileDataType
  upload_id?: string | null
  parts?: CompletedPart[]
  timestamp: number
}

/** 上传完成确认结果（verified=false 时服务端返回 6001） */
export interface FileCompleteData {
  bucket: UploadBucket
  object_key: string
  size_bytes: number
  md5: string
  sha256: string
  data_type?: FileDataType
  verified: boolean
  notify_topic: 'sensor_file'
  timestamp: number
}

/** 文件对象（下载 URL 有效期 15 分钟，支持 Range 分片下载） */
export interface FileObjectItem {
  bucket: UploadBucket
  object_key: string
  size_bytes: number
  etag?: string | null
  last_modified: number
  download_url: string
  expires_in: number
}

/** 文件列表响应数据（游标分页） */
export interface FileListData {
  items: FileObjectItem[]
  next_marker: string | null
  truncated: boolean
}

/** 文件列表查询参数（GET /api/v1/data/files） */
export interface FileListQuery {
  bucket: UploadBucket
  vehicle_id?: string
  /** YYYY-MM-DD */
  date?: string
  data_type?: FileDataType
  marker?: string
  limit?: number
}

