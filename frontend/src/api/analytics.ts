/**
 * 数据分析 API（data-analytics）
 * 契约：contracts/openapi/data-analytics.yaml
 * 说明：报告生成为异步流程 —— 先 POST /reports/generate（202），再轮询 GET /reports/{report_id}。
 */
import { request } from './request'
import type {
  ControlEvalData,
  CornerCaseListData,
  CornerCaseQuery,
  DashboardData,
  EvalQuery,
  PerceptionEvalData,
  ReportGenerateData,
  ReportGenerateRequest,
  ReportListData,
  ReportListQuery,
  ReportMeta,
  SceneCoverageData,
  SceneCoverageQuery,
} from '@/types/analytics'

/** 看板聚合数据（GET /api/v1/analytics/dashboard） */
export function fetchDashboard(params: {
  time_range: '1h' | '24h' | '7d' | '30d'
  vehicle_id?: string
}): Promise<DashboardData> {
  return request<DashboardData>({ url: '/analytics/dashboard', method: 'get', params })
}

/** 感知评估（GET /api/v1/analytics/perception/eval） */
export function fetchPerceptionEval(params: EvalQuery): Promise<PerceptionEvalData> {
  return request<PerceptionEvalData>({ url: '/analytics/perception/eval', method: 'get', params })
}

/** 控制评估（GET /api/v1/analytics/control/eval） */
export function fetchControlEval(params: EvalQuery): Promise<ControlEvalData> {
  return request<ControlEvalData>({ url: '/analytics/control/eval', method: 'get', params })
}

/** 场景覆盖率（GET /api/v1/analytics/scene/coverage） */
export function fetchSceneCoverage(params: SceneCoverageQuery): Promise<SceneCoverageData> {
  return request<SceneCoverageData>({ url: '/analytics/scene/coverage', method: 'get', params })
}

/** Corner Case 挖掘结果（GET /api/v1/analytics/corner-cases） */
export function fetchCornerCases(params: CornerCaseQuery): Promise<CornerCaseListData> {
  return request<CornerCaseListData>({ url: '/analytics/corner-cases', method: 'get', params })
}

/** 报告列表（GET /api/v1/analytics/reports） */
export function listReports(params: ReportListQuery): Promise<ReportListData> {
  return request<ReportListData>({ url: '/analytics/reports', method: 'get', params })
}

/** 报告详情（GET /api/v1/analytics/reports/{report_id}，前端轮询用） */
export function getReport(reportId: string): Promise<ReportMeta> {
  return request<ReportMeta>({ url: `/analytics/reports/${reportId}`, method: 'get' })
}

/** 触发报告生成（POST /api/v1/analytics/reports/generate，202） */
export function generateReport(payload: ReportGenerateRequest): Promise<ReportGenerateData> {
  return request<ReportGenerateData>({
    url: '/analytics/reports/generate',
    method: 'post',
    data: payload,
  })
}
