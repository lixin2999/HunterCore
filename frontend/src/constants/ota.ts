/**
 * OTA 管理受控词表
 * 来源：contracts/openapi/ota-service.yaml（OtaVersionStatus / OtaTaskStatus / OtaUpgradeStatus /
 * OtaBatchStatus / OtaNextAction / OtaPreconditionName）+ 系统约束第 14 条状态机与灰度流程。
 */

/**
 * ⚠ OtaReleaseType（版本发布类型）契约**未定义 enum**（DDL 注释「正式/灰度/补丁」，
 * 见 x-hunter-pending-confirmation #2），因此前端不得硬编码分支，仅原样展示字符串。
 */

/** 版本状态 */
export const OTA_VERSION_STATUS_LABELS: Record<string, string> = {
  draft: '草稿',
  published: '已发布',
  deprecated: '已弃用',
  disabled: '已下线',
}

/** 版本状态 → 标签类型 */
export const OTA_VERSION_STATUS_TAG_TYPES: Record<string, string> = {
  draft: 'info',
  published: 'success',
  deprecated: 'warning',
  disabled: 'danger',
}

/** 任务状态（7 态） */
export const OTA_TASK_STATUS_LABELS: Record<string, string> = {
  created: '已创建',
  pending_approval: '待审批',
  running: '执行中',
  paused: '已暂停',
  succeeded: '成功',
  failed: '失败',
  canceled: '已取消',
}

/** 任务状态 → 标签类型 */
export const OTA_TASK_STATUS_TAG_TYPES: Record<string, string> = {
  created: 'info',
  pending_approval: 'warning',
  running: 'primary',
  paused: 'warning',
  succeeded: 'success',
  failed: 'danger',
  canceled: 'info',
}

/** 单车升级状态机（系统约束第 14 条：9 态，流转不可更改） */
export const OTA_UPGRADE_STATUS_LABELS: Record<string, string> = {
  IDLE: '空闲',
  PENDING: '待升级',
  DOWNLOAD: '下载中',
  INSTALL: '安装中',
  TEST: '自检中',
  SUCCESS: '成功',
  ROLLBACK: '回滚中',
  ROLLED_BACK: '已回滚',
  FAILED: '失败',
}

/** 单车升级状态机主流程顺序（升级记录时间轴展示用） */
export const OTA_UPGRADE_FLOW: readonly string[] = [
  'IDLE',
  'PENDING',
  'DOWNLOAD',
  'INSTALL',
  'TEST',
  'SUCCESS',
]

/** 单车升级状态 → 标签类型 */
export const OTA_UPGRADE_STATUS_TAG_TYPES: Record<string, string> = {
  IDLE: 'info',
  PENDING: 'info',
  DOWNLOAD: 'primary',
  INSTALL: 'primary',
  TEST: 'warning',
  SUCCESS: 'success',
  ROLLBACK: 'warning',
  ROLLED_BACK: 'warning',
  FAILED: 'danger',
}

/** 灰度批次状态 */
export const OTA_BATCH_STATUS_LABELS: Record<string, string> = {
  pending: '待下发',
  in_progress: '下发中',
  observing: '观察期',
  passed: '已通过',
  halted: '已暂停',
}

/** 灰度批次状态 → 标签类型 */
export const OTA_BATCH_STATUS_TAG_TYPES: Record<string, string> = {
  pending: 'info',
  in_progress: 'primary',
  observing: 'warning',
  passed: 'success',
  halted: 'danger',
}

/** 灰度推进动作 */
export const OTA_NEXT_ACTION_LABELS: Record<string, string> = {
  advance: '可推进下一批',
  observing: '观察中',
  halt: '已暂停待人工介入',
}

/** 升级前置条件门禁（系统约束第 14 条） */
export const OTA_PRECONDITION_LABELS: Record<string, string> = {
  battery_soc: '电量（SOC ≥ 50%）',
  vehicle_parked: '车辆静止（P 档）',
  network_stable: '网络稳定',
  storage: '存储空间（≥ 2GB）',
}

/**
 * 默认灰度策略（系统约束第 14 条：5% → 20% → 50% → 100%，每批观察 24h，成功率阈值 95%）
 * 前端展示与任务创建表单默认值，不可由用户改动比例/阈值（契约 OtaCanaryBatch enum 固定）。
 */
export const OTA_DEFAULT_CANARY_BATCHES = [
  { batch_no: 1, percent: 5, observe_hours: 24, success_rate_threshold: 0.95 },
  { batch_no: 2, percent: 20, observe_hours: 24, success_rate_threshold: 0.95 },
  { batch_no: 3, percent: 50, observe_hours: 24, success_rate_threshold: 0.95 },
  { batch_no: 4, percent: 100, observe_hours: 24, success_rate_threshold: 0.95 },
] as const

/** 默认升级门禁（系统约束第 14 条：SOC ≥ 50%、静止、网络稳定、存储 ≥ 2GB） */
export const OTA_DEFAULT_PRECONDITIONS = {
  soc_min: 50,
  must_be_parked: true,
  network_stable: true,
  min_storage_mb: 2048,
} as const

/** 版本上传分片大小（MinIO 分片 ≥ 5MiB；此处保守取 16MiB） */
export const OTA_UPLOAD_PART_SIZE = 16 * 1024 * 1024

/** 版本上线前必须通过的校验项（OtaVersionPublishChecks，系统约束第 9 条 OTA 安全） */
export const OTA_PUBLISH_CHECK_LABELS: Record<string, string> = {
  package_size_verified: '包大小校验',
  md5_verified: 'MD5 完整性校验',
  sha256_verified: 'SHA-256 完整性校验',
  signature_verified: 'RSA-2048 数字签名验签',
  version_code_monotonic: '版本号单调递增（防回滚）',
}
