/**
 * OTA 管理（ota-service）类型（一）：版本仓库 / 上传 / 发布
 * 来源：contracts/openapi/ota-service.yaml —— 字段名与契约完全一致（snake_case）。
 * 安全：OTA 包必须 SHA-256 + RSA-2048 签名校验，版本号单调递增（防回滚）。
 */

/** 版本状态（DDL CHECK 约束，不可更改） */
export type OtaVersionStatus = 'draft' | 'published' | 'deprecated' | 'disabled'

/**
 * 版本发布类型（⚠ 契约未定义 enum，DDL 注释为「正式/灰度/补丁」，example=formal）
 * 前端仅原样展示，禁止硬编码值分支。
 */
export type OtaReleaseType = string

/** 升级任务状态（DDL CHECK 约束，不可更改） */
export type OtaTaskStatus =
  | 'created'
  | 'pending_approval'
  | 'running'
  | 'paused'
  | 'succeeded'
  | 'failed'
  | 'canceled'

/** 车端升级状态/阶段（状态机不可更改：IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS） */
export type OtaUpgradeStatus =
  | 'IDLE'
  | 'PENDING'
  | 'DOWNLOAD'
  | 'INSTALL'
  | 'TEST'
  | 'SUCCESS'
  | 'ROLLBACK'
  | 'ROLLED_BACK'
  | 'FAILED'

/** 灰度批次状态（平台派生，非 DDL 列） */
export type OtaBatchStatus = 'pending' | 'in_progress' | 'observing' | 'passed' | 'halted'

/** 批次推进判定（服务端派生，供人工决策） */
export type OtaNextAction = 'advance' | 'observing' | 'halt'

/** 任务动作类型 */
export type OtaTaskAction = 'start' | 'pause' | 'resume' | 'cancel'

/** 回滚目标（当前仅支持 A/B 分区回退到上一分区） */
export type OtaRollbackTarget = 'previous_slot'

/** 升级前置条件门禁名（系统约束第 14 条：SOC≥50%、P 档静止、网络稳定、存储≥2GB） */
export type OtaPreconditionName = 'battery_soc' | 'vehicle_parked' | 'network_stable' | 'storage'

/** 版本条目 */
export interface OtaVersionItem {
  version_id: string
  version_name: string
  version_code: number
  release_type: OtaReleaseType
  /** MinIO object_key（hunter-ota-packages） */
  package_url: string
  package_size: number
  package_md5: string
  package_sha256: string
  /** RSA-2048 数字签名（Base64） */
  signature: string
  changelog: Record<string, unknown>
  applicable_models: string[]
  status: OtaVersionStatus
  release_time?: number | null
  /** 预签名下载 URL（按需下发） */
  package_download_url?: string | null
}

/** 版本列表数据 */
export interface OtaVersionList {
  items: OtaVersionItem[]
  total: number
  page: number
  page_size: number
}

/** 版本创建请求（包已由发布工具计算完整性信息并离线签名） */
export interface OtaVersionCreateRequest {
  version_name: string
  version_code: number
  release_type: OtaReleaseType
  package_size: number
  package_md5: string
  package_sha256: string
  signature: string
  changelog?: Record<string, unknown>
  applicable_models: string[]
  /** 分片数（大包分片上传）；不传为单次 PUT */
  part_count?: number
}

/** 上传分片预签名信息 */
export interface OtaVersionUploadPart {
  part_number: number
  upload_url: string
}

/** 上传信息（有效期固定 3600s，系统约束第 7 条） */
export interface OtaVersionUploadInfo {
  object_key: string
  upload_url: string
  expires_in: 3600
  part_count: number
  parts?: OtaVersionUploadPart[]
}

/** 版本创建响应数据（含上传凭据） */
export interface OtaVersionCreateData {
  version: OtaVersionItem
  upload: OtaVersionUploadInfo
}

/** 发布请求 */
export interface OtaVersionPublishRequest {
  note?: string
}

/** 发布前置校验项（全部 true 才允许上线） */
export interface OtaVersionPublishChecks {
  package_size_verified: boolean
  md5_verified: boolean
  sha256_verified: boolean
  signature_verified: boolean
  version_code_monotonic: boolean
}

/** 发布结果数据 */
export interface OtaVersionPublishData {
  version_id: string
  version_code: number
  status: OtaVersionStatus
  release_time: number
  checks: OtaVersionPublishChecks
}

/** 版本弃用/下线请求（status 仅可取 deprecated / disabled） */
export interface OtaVersionDeprecateRequest {
  status: 'deprecated' | 'disabled'
  reason: string
}

/** 版本列表查询参数 */
export interface OtaVersionListQuery {
  status?: OtaVersionStatus
  release_type?: OtaReleaseType
  version_code?: number
  applicable_model?: string
  version_name?: string
  page?: number
  page_size?: number
}

/**
 * 灰度批次（比例/观察时长/成功率阈值取自契约 enum：5/20/50/100、24h、0.95，不可自定义）
 */
export interface OtaCanaryBatch {
  batch_no: number
  percent: 5 | 20 | 50 | 100
  observe_hours: 24
  success_rate_threshold: 0.95
}

/** 升级策略（stage_gate 固定 true：分批门禁，系统约束第 14 条） */
export interface OtaUpgradeStrategy {
  batches: OtaCanaryBatch[]
  stage_gate: true
}

/** 升级排期 */
export interface OtaTaskSchedule {
  mode: 'immediate' | 'scheduled'
  start_time?: number | null
  window_end?: number | null
}

/** 升级门禁（前置条件，系统约束第 14 条） */
export interface OtaTaskPreconditions {
  soc_min: number
  must_be_parked: boolean
  network_stable: boolean
  min_storage_mb: number
}

/** 任务创建请求 */
export interface OtaTaskCreateRequest {
  task_name: string
  target_version_id: string
  target_vehicles: string[]
  upgrade_strategy?: OtaUpgradeStrategy
  schedule?: OtaTaskSchedule
  preconditions?: OtaTaskPreconditions
}

/** 任务整体进度 */
export interface OtaTaskProgress {
  total: number
  pending: number
  in_progress: number
  succeeded: number
  failed: number
  rolled_back: number
  success_rate?: number | null
  current_batch?: number
}

/** 任务条目 */
export interface OtaTaskItem {
  task_id: string
  task_name: string
  target_version_id: string
  target_vehicles?: string[]
  vehicle_count?: number
  upgrade_strategy?: OtaUpgradeStrategy
  schedule?: OtaTaskSchedule
  preconditions?: OtaTaskPreconditions
  status: OtaTaskStatus
  progress: OtaTaskProgress
  creator: string
  create_time: number
}

/** 任务详情（契约 allOf: OtaTaskItem + 灰度视图/门禁失败明细等扩展字段） */
export interface OtaTaskDetail extends OtaTaskItem {
  rollout?: OtaRolloutView
  blocked_vehicles?: OtaPreconditionFailure[]
  current_version_id?: string | null
  pause_reason?: string | null
  started_at?: number | null
  finished_at?: number | null
}

/** 灰度批次进度 */
export interface OtaBatchProgress {
  batch_no: number
  percent: 5 | 20 | 50 | 100
  status: OtaBatchStatus
  target_count: number
  success_count: number
  failed_count: number
  rolled_back_count: number
  in_progress_count: number
  success_rate?: number | null
  observe_until?: number | null
  started_at?: number | null
  finished_at?: number | null
}

/** 灰度推进视图（批次 4 批固定，next_action 供人工决策） */
export interface OtaRolloutView {
  current_batch: number
  total_batches: 4
  observe_until?: number | null
  next_action: OtaNextAction
  halt_reason?: string | null
  batches: OtaBatchProgress[]
}

/** 启动/继续请求（batch_no 用于指定重试批次） */
export interface OtaTaskStartRequest {
  note?: string
  batch_no?: number | null
}

/** 暂停请求 */
export interface OtaTaskPauseRequest {
  reason?: string
}

/** 取消请求 */
export interface OtaTaskCancelRequest {
  reason?: string
}

/** 门禁未通过明细（单车） */
export interface OtaPreconditionFailure {
  vehicle_id: string
  failed_conditions: OtaPreconditionName[]
  actual?: Record<string, unknown>
}

/** 任务动作结果（start/pause/resume/cancel 共用） */
export interface OtaTaskActionData {
  task_id: string
  action: OtaTaskAction
  status: OtaTaskStatus
  batch_no?: number | null
  released_vehicles?: string[]
  blocked_vehicles?: OtaPreconditionFailure[]
  released_count: number
  blocked_count: number
  executed_at: number
  operator_id?: string | null
  reason?: string | null
}

/** 回滚请求（未指定 vehicle_ids 时按门禁范围回滚整任务） */
export interface OtaTaskRollbackRequest {
  vehicle_ids?: string[]
  target?: OtaRollbackTarget
  reason: string
}

/** 回滚单车结果 */
export interface OtaTaskRollbackResultItem {
  vehicle_id: string
  accepted: boolean
  command_id?: string | null
  topic?: string | null
  reason?: string | null
  current_status?: OtaUpgradeStatus
}

/** 回滚结果 */
export interface OtaTaskRollbackData {
  task_id: string
  target?: OtaRollbackTarget
  requested_count: number
  accepted_count: number
  rejected_count: number
  results: OtaTaskRollbackResultItem[]
  executed_at?: number
  operator_id?: string | null
}

/** 任务列表数据 */
export interface OtaTaskList {
  items: OtaTaskItem[]
  total: number
  page: number
  page_size: number
}

/** 单车升级记录 */
export interface OtaRecordItem {
  record_id: number
  task_id: string
  vehicle_id: string
  from_version?: string | null
  to_version: string
  /** 当前状态（含失败/回滚终态） */
  status: OtaUpgradeStatus
  /** 当前阶段（状态机阶段，与 status 同域） */
  phase: OtaUpgradeStatus
  /** 0–100 */
  progress: number
  error_code?: string | null
  error_message?: string | null
  start_time: number
  end_time?: number | null
  duration_seconds?: number | null
}

/** 升级记录列表数据（summary 为任务级汇总） */
export interface OtaRecordList {
  items: OtaRecordItem[]
  total: number
  page: number
  page_size: number
  summary?: OtaTaskProgress
}

/** 任务列表查询参数 */
export interface OtaTaskListQuery {
  status?: OtaTaskStatus
  target_version_id?: string
  vehicle_id?: string
  creator?: string
  page?: number
  page_size?: number
}

/** 升级记录查询参数（任务维度 / 车辆维度共用） */
export interface OtaRecordListQuery {
  vehicle_id?: string
  status?: OtaUpgradeStatus
  page?: number
  page_size?: number
}

