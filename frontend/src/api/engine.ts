/**
 * 场景引擎 API（scene-service 场景生成器与场景管理）
 *
 * 契约：contracts/openapi/scene-engine.yaml（设计文档 §4.1）
 * 约定：耗时生成返回 202 + task_id，前端轮询 GET /generate/{task_id}；
 *      写操作走 scene:create|update|delete，生成/下发走 scene:execute。
 */
import { request } from './request'
import type {
  Baseline,
  BatchValidateData,
  CatalogSyncData,
  ConcreteListData,
  ConcreteListQuery,
  ConcreteScenarioDetail,
  DryRunData,
  GenerateRequest,
  GenerationTask,
  GenerationTaskListData,
  GenerationTaskListQuery,
  LogicalScenarioDetail,
  LogicalScenarioListData,
  LogicalVersionListData,
  OracleListData,
  PublishData,
  RenderData,
  SuiteCreateRequest,
  SuiteDetail,
  SuiteDiffData,
  SuiteItemInput,
  SuiteListData,
  SuiteLockData,
  TestCase,
  TestCaseCreateRequest,
  TestCaseListData,
  TestCaseListQuery,
  ValidationReport,
  CoverageGapListData,
  CoverageOverview,
  GenerationProfile,
} from '@/types/engine'

/* ------------------------------ 逻辑场景 ------------------------------ */

/** 逻辑场景列表（GET /api/v1/scene/logical；DB 快照 + 内置目录） */
export function listLogicalScenarios(params: {
  category?: string
  keyword?: string
  include_catalog?: boolean
  limit?: number
} = {}): Promise<LogicalScenarioListData> {
  return request<LogicalScenarioListData>({ url: '/scene/logical', method: 'get', params })
}

/** 同步内置逻辑场景目录（POST /api/v1/scene/logical/sync，幂等） */
export function syncLogicalCatalog(): Promise<CatalogSyncData> {
  return request<CatalogSyncData>({ url: '/scene/logical/sync', method: 'post' })
}

/** 逻辑场景详情（GET /api/v1/scene/logical/{logical_id}） */
export function getLogicalScenario(
  logicalId: string,
  params: { version?: string } = {},
): Promise<LogicalScenarioDetail> {
  return request<LogicalScenarioDetail>({
    url: `/scene/logical/${logicalId}`,
    method: 'get',
    params,
  })
}

/** 版本列表（GET /api/v1/scene/logical/{logical_id}/versions） */
export function listLogicalVersions(logicalId: string): Promise<LogicalVersionListData> {
  return request<LogicalVersionListData>({
    url: `/scene/logical/${logicalId}/versions`,
    method: 'get',
  })
}

/** 静态校验 G1-G7（POST /api/v1/scene/logical/{logical_id}/validate） */
export function validateLogicalScenario(
  logicalId: string,
  version?: string,
): Promise<ValidationReport> {
  return request<ValidationReport>({
    url: `/scene/logical/${logicalId}/validate`,
    method: 'post',
    params: { version },
  })
}

/** 干跑（POST /api/v1/scene/logical/{logical_id}/dryrun，返回 SceneConfig 预览） */
export function dryRunLogicalScenario(
  logicalId: string,
  payload: { version?: string | null; values: Record<string, number | string> },
): Promise<DryRunData> {
  return request<DryRunData>({
    url: `/scene/logical/${logicalId}/dryrun`,
    method: 'post',
    data: payload,
  })
}

/** 发布逻辑场景（POST /api/v1/scene/logical/{logical_id}/publish） */
export function publishLogicalScenario(logicalId: string, version?: string): Promise<PublishData> {
  return request<PublishData>({
    url: `/scene/logical/${logicalId}/publish`,
    method: 'post',
    params: { version },
  })
}

/* ------------------------------ 生成任务 ------------------------------ */

/** 提交异步生成任务（POST /api/v1/scene/generate，202 + task_id） */
export function createGenerationTask(payload: GenerateRequest): Promise<GenerationTask> {
  return request<GenerationTask>({ url: '/scene/generate', method: 'post', data: payload })
}

/** 生成任务列表（GET /api/v1/scene/generate） */
export function listGenerationTasks(
  params: GenerationTaskListQuery = {},
): Promise<GenerationTaskListData> {
  return request<GenerationTaskListData>({ url: '/scene/generate', method: 'get', params })
}

/** 生成任务详情（GET /api/v1/scene/generate/{task_id}） */
export function getGenerationTask(taskId: string): Promise<GenerationTask> {
  return request<GenerationTask>({ url: `/scene/generate/${taskId}`, method: 'get' })
}

/** 取消生成任务（POST /api/v1/scene/generate/{task_id}/cancel；仅 pending） */
export function cancelGenerationTask(taskId: string): Promise<GenerationTask> {
  return request<GenerationTask>({
    url: `/scene/generate/${taskId}/cancel`,
    method: 'post',
  })
}

/* ------------------------------ 具体场景 ------------------------------ */

/** 具体场景列表（GET /api/v1/scene/concrete） */
export function listConcreteScenarios(params: ConcreteListQuery = {}): Promise<ConcreteListData> {
  return request<ConcreteListData>({ url: '/scene/concrete', method: 'get', params })
}

/** 具体场景详情（GET /api/v1/scene/concrete/{scenario_id}） */
export function getConcreteScenario(scenarioId: string): Promise<ConcreteScenarioDetail> {
  return request<ConcreteScenarioDetail>({
    url: `/scene/concrete/${scenarioId}`,
    method: 'get',
  })
}

/** 按需渲染（POST /api/v1/scene/concrete/{scenario_id}/render） */
export function renderConcreteScenario(scenarioId: string): Promise<RenderData> {
  return request<RenderData>({
    url: `/scene/concrete/${scenarioId}/render`,
    method: 'post',
  })
}

/** 批量复检（POST /api/v1/scene/concrete/batch-validate；失败进 quarantined） */
export function batchValidateConcreteScenarios(scenarioIds: string[]): Promise<BatchValidateData> {
  return request<BatchValidateData>({
    url: '/scene/concrete/batch-validate',
    method: 'post',
    data: { scenario_ids: scenarioIds },
  })
}

/* ------------------------------ 判据 / 测试用例 ------------------------------ */

/** 判据目录（GET /api/v1/scene/oracles） */
export function listOracles(): Promise<OracleListData> {
  return request<OracleListData>({ url: '/scene/oracles', method: 'get' })
}

/** 测试用例列表（GET /api/v1/scene/testcases） */
export function listTestCases(params: TestCaseListQuery = {}): Promise<TestCaseListData> {
  return request<TestCaseListData>({ url: '/scene/testcases', method: 'get', params })
}

/** 创建测试用例（POST /api/v1/scene/testcases，201；编号冲突 3002） */
export function createTestCase(payload: TestCaseCreateRequest): Promise<TestCase> {
  return request<TestCase>({ url: '/scene/testcases', method: 'post', data: payload })
}

/** 测试用例详情（GET /api/v1/scene/testcases/{test_case_id}） */
export function getTestCase(testCaseId: string): Promise<TestCase> {
  return request<TestCase>({ url: `/scene/testcases/${testCaseId}`, method: 'get' })
}

/* ------------------------------ 测试集 / 基线 ------------------------------ */

/** 测试集列表（GET /api/v1/scene/suites） */
export function listSuites(
  params: { page?: number; page_size?: number } = {},
): Promise<SuiteListData> {
  return request<SuiteListData>({ url: '/scene/suites', method: 'get', params })
}

/** 创建测试集（POST /api/v1/scene/suites，201；名称冲突 3002） */
export function createSuite(payload: SuiteCreateRequest): Promise<SuiteDetail> {
  return request<SuiteDetail>({ url: '/scene/suites', method: 'post', data: payload })
}

/** 测试集详情（GET /api/v1/scene/suites/{suite_id}，含条目） */
export function getSuite(suiteId: string): Promise<SuiteDetail> {
  return request<SuiteDetail>({ url: `/scene/suites/${suiteId}`, method: 'get' })
}

/** 加条目（POST /api/v1/scene/suites/{suite_id}/items；锁定后 3003） */
export function addSuiteItems(suiteId: string, items: SuiteItemInput[]): Promise<SuiteDetail> {
  return request<SuiteDetail>({
    url: `/scene/suites/${suiteId}/items`,
    method: 'post',
    data: { items },
  })
}

/** 锁定测试集（POST /api/v1/scene/suites/{suite_id}/lock，可建基线） */
export function lockSuite(
  suiteId: string,
  payload: { baseline_name?: string | null; metrics: Record<string, unknown> },
): Promise<SuiteLockData> {
  return request<SuiteLockData>({
    url: `/scene/suites/${suiteId}/lock`,
    method: 'post',
    data: payload,
  })
}

/** 测试集差异（GET /api/v1/scene/suites/{suite_id}/diff） */
export function diffSuite(suiteId: string, baselineId?: string): Promise<SuiteDiffData> {
  return request<SuiteDiffData>({
    url: `/scene/suites/${suiteId}/diff`,
    method: 'get',
    params: { baseline_id: baselineId },
  })
}

/** 基线列表（GET /api/v1/scene/suites/{suite_id}/baselines） */
export function listSuiteBaselines(suiteId: string): Promise<{ items: Baseline[] }> {
  return request<{ items: Baseline[] }>({
    url: `/scene/suites/${suiteId}/baselines`,
    method: 'get',
  })
}

/* ------------------------------ 覆盖分析 ------------------------------ */

/** 覆盖总览（GET /api/v1/scene/coverage/overview） */
export function getCoverageOverview(params: { logical_id?: string } = {}): Promise<CoverageOverview> {
  return request<CoverageOverview>({ url: '/scene/coverage/overview', method: 'get', params })
}

/** 覆盖盲区（GET /api/v1/scene/coverage/gaps） */
export function listCoverageGaps(profile: GenerationProfile): Promise<CoverageGapListData> {
  return request<CoverageGapListData>({
    url: '/scene/coverage/gaps',
    method: 'get',
    params: { profile },
  })
}
