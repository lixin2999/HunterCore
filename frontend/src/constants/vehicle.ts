/**
 * 车辆管理与接入 provisioning 词表
 *
 * 来源：contracts/openapi/vehicle-service.yaml（ProvisionStep.name/state、
 * VehicleRow.provision_state）；车辆状态（8 态）在 constants/enums.ts 中集中定义。
 *
 * ⚠ 仅用于 UI 展示；业务分支必须以契约 code/enum 为准，禁止基于本表字符串匹配。
 */

/** Provisioning 汇总态展示标签（VehicleRow.provision_state） */
export const PROVISION_STATE_LABELS: Record<string, string> = {
  pending: '待开通',
  in_progress: '开通中',
  ready: '就绪',
  failed: '失败',
}

/** Provisioning 汇总态 → Element Plus 标签类型 */
export const PROVISION_STATE_TAG_TYPES: Record<string, string> = {
  pending: 'info',
  in_progress: 'warning',
  ready: 'success',
  failed: 'danger',
}

/** 单步状态标签（ProvisionStep.state） */
export const PROVISION_STEP_STATE_LABELS: Record<string, string> = {
  pending: '待执行',
  in_progress: '执行中',
  ok: '成功',
  failed: '失败',
  skipped: '已跳过（接管）',
}

/** 单步状态 → Element Plus 标签类型 */
export const PROVISION_STEP_STATE_TAG_TYPES: Record<string, string> = {
  pending: 'info',
  in_progress: 'warning',
  ok: 'success',
  failed: 'danger',
  skipped: 'info',
}

/** 步骤名展示标签（ProvisionStep.name，按契约固定顺序 db → scram → topics → cert） */
export const PROVISION_STEP_LABELS: Record<string, string> = {
  db: 'DB 台账',
  scram: 'SCRAM 账号',
  topics: 'Kafka Topic',
  cert: '客户端证书',
}

/** 步骤固定顺序（列表/时间线渲染依据；契约 x-hunter-provisioning.steps） */
export const PROVISION_STEP_ORDER = ['db', 'scram', 'topics', 'cert'] as const

/** 车辆型号（vehicle_svc.vehicles.model；契约未定义 enum，此处仅按当前产品线提供候选） */
export const VEHICLE_MODEL_OPTIONS: ReadonlyArray<{ value: string; label: string }> = [
  { value: 'HUNTER_SE', label: 'HUNTER_SE' },
  { value: 'HUNTER_PRO', label: 'HUNTER_PRO' },
  { value: 'HUNTER_MAX', label: 'HUNTER_MAX' },
]

/** vehicle_id 校验（契约 VehicleIdPath.pattern：^[A-Za-z0-9_-]{1,32}$） */
export const VEHICLE_ID_STRICT_PATTERN = /^[A-Za-z0-9_-]{1,32}$/

/** 每车 Topic 数量（契约 x-hunter-kafka.per_vehicle_topics；用于文案提示，不参与业务判定） */
export const PER_VEHICLE_TOPIC_COUNT = 8
