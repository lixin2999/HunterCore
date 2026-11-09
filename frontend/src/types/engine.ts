/**
 * 场景引擎类型（逻辑场景 / 具体场景 / 生成任务 / 用例 / 测试集 / 覆盖）
 *
 * 契约依据：《场景生成器与场景管理详细设计文档》V2.0 §4.1 与
 * contracts/openapi/scene-engine.yaml；字段命名保持契约 snake_case，不做驼峰转换。
 */
import type { PageData } from './common'
import type { SceneConfig } from './scene'

/* ---------------------------- 受控词表 ---------------------------- */

/** 逻辑场景生命周期 */
export type LogicalScenarioStatus =
  | 'draft'
  | 'validating'
  | 'pending_review'
  | 'published'
  | 'quarantined'
  | 'deprecated'
  | 'archived'
  | 'rejected'

/** 具体场景状态 */
export type ConcreteScenarioStatus =
  | 'generated'
  | 'validated'
  | 'published'
  | 'quarantined'
  | 'archived'

/** 稀有度分区（Z1 常见 → Z4 极稀有） */
export type RarityZone = 'Z1' | 'Z2' | 'Z3' | 'Z4'

/** 临界度分级（CI = a_req / a_max） */
export type CriticalityLevel = 'C0' | 'C1' | 'C2' | 'C3'

/** 风险等级（稀有度 × 临界度） */
export type RiskLevel = 'L1' | 'L2' | 'L3' | 'L4'

/** 可避免性标签（标注不剔除） */
export type Avoidability = 'avoidable' | 'marginal' | 'unavoidable' | 'trivial_safe'

/** 生成策略插件 */
export type GenerationStrategy =
  | 'coverage_driven'
  | 'distribution'
  | 'replay_mutation'
  | 'search_based'
  | 'ai_generated'

/** 生成剖面（Z1-Z4 分层配额） */
export type GenerationProfile = 'regression' | 'robustness' | 'exploration'

/** 生成任务状态 */
export type GenerationTaskStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'canceled'

/** 测试用例状态 */
export type TestCaseStatus = 'draft' | 'published' | 'deprecated' | 'archived'

/** 测试集状态 */
export type SuiteStatus = 'draft' | 'locked' | 'archived'

/** 测试集条目类型 */
export type SuiteItemType = 'test_case' | 'concrete_scenario'

/* ---------------------------- 逻辑场景 ---------------------------- */

export interface LogicalScenarioSummary {
  logical_id: string
  version: string
  name: string
  category: string
  scene_type: string
  description: string
  status: string
  ir_version: string
  oracle_ref: string
  parameter_count: number
  tags: string[]
  content_hash: string
  origin: 'catalog' | 'database'
  update_time?: string | null
}

export interface LogicalParameter {
  name: string
  distribution: { kind: string; params: Record<string, number | string> }
  unit?: string | null
  description?: string | null
}

export interface LogicalScenarioDetail extends LogicalScenarioSummary {
  ir: Record<string, unknown>
  parameters: LogicalParameter[]
  bindings: Array<{ path: string; expr: string }>
  constraints: Array<Record<string, unknown>>
  correlations: Array<{ left: string; right: string; rho: number }>
  allowed_modes: string[]
}

export interface LogicalScenarioListData {
  items: LogicalScenarioSummary[]
  total: number
}

export interface LogicalVersionSummary {
  version: string
  status: string
  content_hash: string
  origin: 'catalog' | 'database'
  update_time?: string | null
}

export interface LogicalVersionListData {
  logical_id: string
  items: LogicalVersionSummary[]
}

export interface GateItem {
  gate: string
  passed: boolean
  message: string
}

export interface ValidationReport {
  logical_id: string
  version: string
  passed: boolean
  gates: GateItem[]
  failures: string[]
  oracle_ref: string
  oracle_found: boolean
  parameter_defaults: Record<string, number | string>
}

export interface DryRunData {
  logical_id: string
  version: string
  passed: boolean
  values: Record<string, number | string>
  scene_config: SceneConfig
  gates: GateItem[]
  failures: string[]
}

export interface PublishData {
  logical_id: string
  version: string
  status: string
}

export interface CatalogSyncData {
  version: string
  synced: number
  logical_ids: string[]
}

/* ---------------------------- 生成任务 ---------------------------- */

export interface GenerateRequest {
  logical_id: string
  logical_version?: string | null
  count: number
  strategy: GenerationStrategy
  profile: GenerationProfile
  seed: number
  overrides: Record<string, unknown>
}

export interface GenerationTask {
  task_id: string
  logical_id: string
  logical_version: string
  strategy: string
  profile: string
  count: number
  seed: number
  status: GenerationTaskStatus
  progress: number
  produced: number
  duplicates: number
  rejected: number
  zone_quota: Record<string, number>
  zone_counts: Record<string, number>
  error_message?: string | null
  created_by: string
  create_time?: string | null
  update_time?: string | null
}

export interface GenerationTaskListData {
  items: GenerationTask[]
  total: number
}

export interface GenerationTaskListQuery {
  page?: number
  page_size?: number
  status?: GenerationTaskStatus
}

/* ---------------------------- 具体场景 ---------------------------- */

export interface ConcreteScenarioSummary {
  scenario_id: string
  logical_id: string
  logical_version: string
  name: string
  rarity_p: number
  rarity_zone: RarityZone
  criticality_ci: number
  criticality_level: CriticalityLevel
  risk_level: RiskLevel
  avoidability: Avoidability
  vil_safe: boolean
  status: ConcreteScenarioStatus
  content_hash: string
  seed: number
  create_time?: string | null
}

export interface ConcreteScenarioDetail extends ConcreteScenarioSummary {
  param_values: Record<string, number | string>
  param_vec: number[]
  allowed_modes: string[]
  generator: Record<string, unknown>
  validity: Record<string, unknown>
  scene_config: SceneConfig
}

export interface ConcreteListQuery {
  page?: number
  page_size?: number
  logical_id?: string
  rarity_zone?: RarityZone
  risk_level?: RiskLevel
  status?: ConcreteScenarioStatus
}

export type ConcreteListData = PageData<ConcreteScenarioSummary>

export interface ExecutionReport {
  /** 无标准 OpenSCENARIO/CARLA 等价物的行为/动作/触发 */
  unsupported: string[]
  /** 可执行但不完整的组合 */
  warnings: string[]
}

export interface RenderData {
  scenario_id: string
  logical_id: string
  content_hash: string
  scene_config: SceneConfig
  gates: GateItem[]
  passed: boolean
  execution: ExecutionReport
}

export interface BatchValidateItem {
  scenario_id: string
  passed: boolean
  failures: string[]
  status: ConcreteScenarioStatus
}

export interface BatchValidateData {
  validated: number
  quarantined: number
  items: BatchValidateItem[]
}

/* ---------------------------- 判据 / 测试用例 ---------------------------- */

export interface OracleData {
  oracle_ref: string
  name: string
  kind: string
  description: string
  metric: string
  operator: string
  threshold: number
  unit: string
  platforms: string[]
  timeout_seconds: number
  extra: Record<string, unknown>
}

export interface OracleListData {
  items: OracleData[]
  total: number
}

export interface TestCaseCreateRequest {
  case_key: string
  name: string
  logical_id: string
  logical_version?: string | null
  scenario_id?: string | null
  objective: string
  preconditions: Record<string, unknown>
  steps: Array<Record<string, unknown>>
  platforms: string[]
  oracle_ref: string
  timeout_seconds: number
  tags: string[]
}

export interface TestCase {
  case_key: string
  name: string
  logical_id: string
  logical_version: string
  scenario_id?: string | null
  objective: string
  preconditions: Record<string, unknown>
  steps: Array<Record<string, unknown>>
  platforms: string[]
  oracle_ref: string
  timeout_seconds: number
  tags: string[]
  status: TestCaseStatus
  test_case_id: string
  create_time?: string | null
}

export interface TestCaseListQuery {
  page?: number
  page_size?: number
  logical_id?: string
  status?: TestCaseStatus
}

export type TestCaseListData = PageData<TestCase>

/* ---------------------------- 测试集 / 基线 ---------------------------- */

export interface SuiteSummary {
  suite_id: string
  name: string
  description?: string | null
  status: SuiteStatus
  item_count: number
  lock_hash?: string | null
  locked_at?: string | null
  locked_by?: string | null
  create_time?: string | null
  update_time?: string | null
}

export interface SuiteItem {
  item_id: string
  item_type: SuiteItemType
  ref_id: string
  weight: number
}

export interface SuiteDetail extends SuiteSummary {
  items: SuiteItem[]
}

export interface SuiteCreateRequest {
  name: string
  description?: string | null
}

export interface SuiteItemInput {
  item_id: string
  item_type: SuiteItemType
  ref_id: string
  weight: number
}

export interface SuiteLockData {
  suite_id: string
  status: string
  lock_hash: string
  item_count: number
  baseline_name?: string | null
}

export interface SuiteDiffData {
  suite_id: string
  current_hash: string
  baseline_hash?: string | null
  identical: boolean
  added: string[]
  removed: string[]
  changed: string[]
}

export interface Baseline {
  baseline_id: string
  suite_id: string
  name: string
  suite_hash: string
  metrics: Record<string, unknown>
  locked_by: string
  create_time?: string | null
}

export type SuiteListData = PageData<SuiteSummary>

/* ---------------------------- 用例 / 测试集派生 ---------------------------- */

export interface CaseDerivationRequest {
  /** 场景类型；省略（空数组）覆盖目录内全部可合成类型 */
  scene_types?: string[]
  /** 稀有度分区，默认 Z1-Z4 */
  zones?: RarityZone[]
  include_concrete?: boolean
  dry_run?: boolean
}

export interface DerivedCase {
  case_key: string
  logical_id: string
  scene_type: string
  zone: RarityZone
  name: string
  status: 'created' | 'skipped' | 'planned'
  test_case_id?: string | null
  reason?: string | null
}

export interface CaseDerivationData {
  created: number
  skipped: number
  planned: number
  cases: DerivedCase[]
}

export interface DerivedSuite {
  suite_id?: string | null
  name: string
  scene_type: string
  item_count: number
  status: 'created' | 'updated' | 'skipped' | 'planned'
  reason?: string | null
}

export interface SuiteDerivationData {
  created: number
  updated: number
  skipped: number
  planned: number
  suites: DerivedSuite[]
}

/* ---------------------------- 覆盖分析 ---------------------------- */

export interface CoverageOverview {
  logical_id?: string | null
  total: number
  zone_counts: Record<string, number>
  zone_ratio: Record<string, number>
  risk_counts: Record<string, number>
  risk_matrix_applied: boolean
  /** 按场景类型（logical_id → scene_type 聚合）的具体场景数 */
  type_counts: Record<string, number>
  /** 场景类型 × 稀有分区矩阵（类型 → 分区 → 数量） */
  type_zone_counts: Record<string, Record<string, number>>
  /** 按语义契约声明的角色统计具体场景数 */
  role_counts: Record<string, number>
}

export interface CoverageGap {
  logical_id: string
  name: string
  zone: RarityZone
  count: number
  target: number
  deficit: number
  /** zero_scenes = 该逻辑场景尚无具体场景；zone_deficit = 稀有分区配额缺口 */
  kind: 'zero_scenes' | 'zone_deficit'
  recommendation: string
}

export interface CoverageGapListData {
  profile: string
  items: CoverageGap[]
}

/* ---------------------------- 场景语义契约（只读） ---------------------------- */

/** 参与者角色语义（count 为参数表达式原文） */
export interface SceneRoleSemantics {
  role: string
  type: string
  count: string
  behavior_type: string
  parameters: Record<string, unknown>
  min_count: number
  required: boolean
}

/** 事件语义 */
export interface SceneEventSemantics {
  type: string
  condition_type: string
  action_type: string
}

/** 子场景（variant）语义 */
export interface SceneVariantSemantics {
  key: string
  label: string
  sub: string
}

/** 参数分布语义 */
export interface SceneParamSemantics {
  name: string
  mean: number
  std: number
  low: number
  high: number
  unit: string
  discrete: boolean
  risk: number
}

/** 单类型语义契约摘要 */
export interface SceneSemantics {
  scene_type: string
  objective: string
  oracle_ref: string
  required_events: boolean
  roles: SceneRoleSemantics[]
  required_roles: string[]
  events: SceneEventSemantics[]
  variants: SceneVariantSemantics[]
  params: SceneParamSemantics[]
  duration: string
  weather: Record<string, string>
}

export interface SceneSemanticsListData {
  items: SceneSemantics[]
  total: number
  /** contracts/scene-taxonomy.yaml::version */
  taxonomy_version: string
}
