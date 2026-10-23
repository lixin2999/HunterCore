/**
 * 场景引擎展示映射（逻辑/具体场景、生成任务、用例、测试集）
 *
 * 取值域来源：contracts/openapi/scene-engine.yaml 与 07_scene_engine.sql 的 CHECK；
 * ⚠ 仅用于 UI 展示，禁止据此做业务分支判断（业务以契约 code/enum 为准）。
 */

/** Element Plus 标签类型（与 el-tag 的 type 取值一致） */
type TagType = 'primary' | 'success' | 'info' | 'warning' | 'danger'

/** 生成策略 */
export const GENERATION_STRATEGY_LABELS: Record<string, string> = {
  coverage_driven: '覆盖驱动',
  distribution: '分布采样',
  replay_mutation: '回放变异',
  search_based: '搜索式',
  ai_generated: 'AI 参数导入（受护栏）',
}

/** 生成剖面（Z1-Z4 配额） */
export const GENERATION_PROFILE_LABELS: Record<string, string> = {
  regression: '回归（70:25:5:0）',
  robustness: '鲁棒（40:40:20:0）',
  exploration: '探索（10:30:50:10）',
}

/** 生成任务状态 */
export const GENERATION_TASK_STATUS_LABELS: Record<string, string> = {
  pending: '排队中',
  running: '生成中',
  succeeded: '已完成',
  failed: '失败',
  canceled: '已取消',
}

/** 生成任务状态 → 标签类型 */
export const GENERATION_TASK_STATUS_TAG_TYPES: Record<string, TagType> = {
  pending: 'info',
  running: 'primary',
  succeeded: 'success',
  failed: 'danger',
  canceled: 'info',
}

/** 逻辑场景状态 */
export const LOGICAL_STATUS_LABELS: Record<string, string> = {
  draft: '草稿',
  validating: '校验中',
  pending_review: '待审核',
  published: '已发布',
  quarantined: '已隔离',
  deprecated: '已弃用',
  archived: '已归档',
  rejected: '已驳回',
}

/** 逻辑场景状态 → 标签类型 */
export const LOGICAL_STATUS_TAG_TYPES: Record<string, TagType> = {
  draft: 'info',
  validating: 'warning',
  pending_review: 'warning',
  published: 'success',
  quarantined: 'danger',
  deprecated: 'warning',
  archived: 'info',
  rejected: 'danger',
}

/** 具体场景状态 */
export const CONCRETE_STATUS_LABELS: Record<string, string> = {
  generated: '已生成',
  validated: '已校验',
  published: '已发布',
  quarantined: '已隔离',
  archived: '已归档',
}

/** 具体场景状态 → 标签类型 */
export const CONCRETE_STATUS_TAG_TYPES: Record<string, TagType> = {
  generated: 'info',
  validated: 'success',
  published: 'success',
  quarantined: 'danger',
  archived: 'warning',
}

/** 稀有度分区 */
export const RARITY_ZONE_LABELS: Record<string, string> = {
  Z1: 'Z1 常见',
  Z2: 'Z2 少见',
  Z3: 'Z3 稀有',
  Z4: 'Z4 极稀有',
}

/** 稀有度分区 → 标签类型 */
export const RARITY_ZONE_TAG_TYPES: Record<string, TagType> = {
  Z1: 'info',
  Z2: 'success',
  Z3: 'warning',
  Z4: 'danger',
}

/** 临界度分级 */
export const CRITICALITY_LEVEL_LABELS: Record<string, string> = {
  C0: 'C0 无风险',
  C1: 'C1 低',
  C2: 'C2 高',
  C3: 'C3 极限',
}

/** 风险等级 */
export const RISK_LEVEL_LABELS: Record<string, string> = {
  L1: 'L1 低',
  L2: 'L2 中',
  L3: 'L3 高',
  L4: 'L4 极高',
}

/** 风险等级 → 标签类型 */
export const RISK_LEVEL_TAG_TYPES: Record<string, TagType> = {
  L1: 'info',
  L2: 'primary',
  L3: 'warning',
  L4: 'danger',
}

/** 可避免性（标注不剔除） */
export const AVOIDABILITY_LABELS: Record<string, string> = {
  avoidable: '可避免',
  marginal: '临界',
  unavoidable: '不可避免',
  trivial_safe: '安全冗余',
}

/** 测试用例状态 */
export const TEST_CASE_STATUS_LABELS: Record<string, string> = {
  draft: '草稿',
  published: '已发布',
  deprecated: '已弃用',
  archived: '已归档',
}

/** 测试用例状态 → 标签类型 */
export const TEST_CASE_STATUS_TAG_TYPES: Record<string, TagType> = {
  draft: 'info',
  published: 'success',
  deprecated: 'warning',
  archived: 'info',
}

/** 测试集状态 */
export const SUITE_STATUS_LABELS: Record<string, string> = {
  draft: '草稿',
  locked: '已锁定',
  archived: '已归档',
}

/** 测试集状态 → 标签类型 */
export const SUITE_STATUS_TAG_TYPES: Record<string, TagType> = {
  draft: 'info',
  locked: 'success',
  archived: 'warning',
}

/** 测试集条目类型 */
export const SUITE_ITEM_TYPE_LABELS: Record<string, string> = {
  test_case: '测试用例',
  concrete_scenario: '具体场景',
}
