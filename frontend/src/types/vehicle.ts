/**
 * 车辆台账与接入 provisioning 类型（vehicle-service）
 *
 * 契约：contracts/openapi/vehicle-service.yaml
 * 字段命名与契约完全一致（snake_case），禁止在 API 层做驼峰转换。
 * 车辆状态词表（8 态）复用 constants/enums.ts 中的 VEHICLE_STATUS_LABELS。
 */
import type { PageQuery } from './common'

/** Provisioning 步骤名（4 步固定顺序：db → scram → topics → cert） */
export type ProvisionStepName = 'db' | 'scram' | 'topics' | 'cert'

/** 单步状态：pending / in_progress / ok / failed / skipped（takeover_existing 时为 skipped） */
export type ProvisionStepState = 'pending' | 'in_progress' | 'ok' | 'failed' | 'skipped'

/** 汇总态：由 4 步状态归并（任一 failed → failed；全 ok/skipped → ready） */
export type ProvisionAggregateState = 'pending' | 'in_progress' | 'ready' | 'failed'

/** 车辆状态（受控词表：系统约束第 12 条，8 态） */
export type VehicleStatusValue =
  | 'offline'
  | 'online_idle'
  | 'auto_driving'
  | 'remote_controlled'
  | 'upgrading'
  | 'charging'
  | 'fault'
  | 'emergency'

/** 单个 provisioning 步骤（ProvisionStep） */
export interface ProvisionStep {
  name: ProvisionStepName
  state: ProvisionStepState
  /** 步骤失败时的错误摘要（≤ 500 字符，服务端已脱敏） */
  error?: string | null
  /** 步骤完成 Unix 秒 */
  ts?: number | null
}

/** 车辆列表行（VehicleRow） */
export interface VehicleRow {
  vehicle_id: string
  vehicle_name: string
  model: string
  firmware_version?: string | null
  software_version?: string | null
  status: VehicleStatusValue
  /** ISO-8601 注册时间 */
  register_time: string
  last_online_time?: string | null
  /** 客户端证书序列号（大写十六进制；未开通或证书缺失为空） */
  device_cert_sn?: string | null
  provision_state: ProvisionAggregateState
  provision_steps: ProvisionStep[]
}

/** 车辆详情（VehicleDetail = VehicleRow + fence/desc/kafka/scram/topics_preview） */
export interface VehicleDetail extends VehicleRow {
  fence_json?: Record<string, unknown> | null
  description?: string | null
  /** 车端接入 bootstrap（SERVER_IP:9093；由 settings 生成，非查询） */
  kafka_bootstrap: string
  /** 车端 SCRAM 用户名（= vehicle_id） */
  scram_username: string
  /** 8 个 Topic 名称预览（不查 Kafka，纯字符串拼接） */
  topics_preview: string[]
}

/** 分页响应（VehiclePage） */
export interface VehiclePage {
  items: VehicleRow[]
  total: number
  page: number
  page_size: number
}

/** 列表查询（GET /api/v1/vehicle/list） */
export interface VehicleListQuery extends PageQuery {
  status?: VehicleStatusValue
  provision_state?: ProvisionAggregateState
  /** 模糊匹配 vehicle_id / vehicle_name（≤ 64 字符） */
  keyword?: string
}

/** 一键开通请求（POST /api/v1/vehicle） */
export interface VehicleCreateRequest {
  vehicle_id: string
  vehicle_name: string
  model?: string
  firmware_version?: string | null
  software_version?: string | null
  fence_json?: Record<string, unknown> | null
  description?: string | null
  /** 若 SCRAM/Topic/证书已存在（历史遗留），是否接管复用（不重复创建） */
  takeover_existing?: boolean
}

/** 台账修改请求（PATCH /api/v1/vehicle/{vehicle_id}；不涉及 provisioning 资源） */
export interface VehicleUpdateRequest {
  vehicle_name?: string
  model?: string
  firmware_version?: string | null
  software_version?: string | null
  fence_json?: Record<string, unknown> | null
  description?: string | null
}

/** 一键开通响应（ProvisionResult；scram_password 一次性，服务端不落 DB/日志） */
export interface ProvisionResult {
  vehicle: VehicleDetail
  scram_password: string
  /** '/api/v1/vehicle/{id}/bundle' */
  bundle_download_url: string
}

/** SCRAM 口令重置响应（RotateScramResult） */
export interface RotateScramResult {
  vehicle_id: string
  scram_password: string
}

/** 证书重签响应（ReissueCertResult） */
export interface ReissueCertResult {
  vehicle_id: string
  device_cert_sn: string
  issued_at: string
}
