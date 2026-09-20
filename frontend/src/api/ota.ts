/**
 * OTA 管理 API（ota-service）
 * 契约：contracts/openapi/ota-service.yaml
 * 限流（附录 D）：POST /ota/versions 单用户 5 QPS。
 */
import { request } from './request'
import type {
  OtaRecordList,
  OtaRecordListQuery,
  OtaTaskActionData,
  OtaTaskCancelRequest,
  OtaTaskCreateRequest,
  OtaTaskDetail,
  OtaTaskList,
  OtaTaskListQuery,
  OtaTaskPauseRequest,
  OtaTaskRollbackData,
  OtaTaskRollbackRequest,
  OtaTaskStartRequest,
  OtaVersionCreateData,
  OtaVersionCreateRequest,
  OtaVersionDeprecateRequest,
  OtaVersionItem,
  OtaVersionList,
  OtaVersionListQuery,
  OtaVersionPublishData,
  OtaVersionPublishRequest,
  OtaVersionRejectReviewRequest,
  OtaVersionSubmitReviewRequest,
} from '@/types/ota'

/* ------------------------------- 版本仓库 ------------------------------- */

/** 版本列表（GET /api/v1/ota/versions） */
export function listOtaVersions(params: OtaVersionListQuery): Promise<OtaVersionList> {
  return request<OtaVersionList>({ url: '/ota/versions', method: 'get', params })
}

/** 版本详情（GET /api/v1/ota/versions/{version_id}） */
export function getOtaVersion(versionId: string): Promise<OtaVersionItem> {
  return request<OtaVersionItem>({ url: `/ota/versions/${versionId}`, method: 'get' })
}

/** 创建版本并获取包上传凭据（POST /api/v1/ota/versions，201） */
export function createOtaVersion(payload: OtaVersionCreateRequest): Promise<OtaVersionCreateData> {
  return request<OtaVersionCreateData>({ url: '/ota/versions', method: 'post', data: payload })
}

/** 提交测试（G-18②：POST /api/v1/ota/versions/{version_id}/submit-testing，包未直传 → 6001） */
export function submitOtaVersionTesting(versionId: string): Promise<OtaVersionItem> {
  return request<OtaVersionItem>({
    url: `/ota/versions/${versionId}/submit-testing`,
    method: 'post',
  })
}

/** 提交审核（G-18②：POST /api/v1/ota/versions/{version_id}/submit-review） */
export function submitOtaVersionReview(
  versionId: string,
  payload: OtaVersionSubmitReviewRequest = {},
): Promise<OtaVersionItem> {
  return request<OtaVersionItem>({
    url: `/ota/versions/${versionId}/submit-review`,
    method: 'post',
    data: payload,
  })
}

/** 审核驳回（G-18②：POST /api/v1/ota/versions/{version_id}/reject-review，reason 必填） */
export function rejectOtaVersionReview(
  versionId: string,
  payload: OtaVersionRejectReviewRequest,
): Promise<OtaVersionItem> {
  return request<OtaVersionItem>({
    url: `/ota/versions/${versionId}/reject-review`,
    method: 'post',
    data: payload,
  })
}

/** 发布版本（前置仅 reviewing，G-18② 审核批准；完整性/签名校验失败返回 6001/6002） */
export function publishOtaVersion(
  versionId: string,
  payload: OtaVersionPublishRequest = {},
): Promise<OtaVersionPublishData> {
  return request<OtaVersionPublishData>({
    url: `/ota/versions/${versionId}/publish`,
    method: 'post',
    data: payload,
  })
}

/** 弃用/下线版本（POST /api/v1/ota/versions/{version_id}/deprecate） */
export function deprecateOtaVersion(
  versionId: string,
  payload: OtaVersionDeprecateRequest,
): Promise<OtaVersionItem> {
  return request<OtaVersionItem>({
    url: `/ota/versions/${versionId}/deprecate`,
    method: 'post',
    data: payload,
  })
}

/* ------------------------------- 升级任务 ------------------------------- */

/** 任务列表（GET /api/v1/ota/tasks） */
export function listOtaTasks(params: OtaTaskListQuery): Promise<OtaTaskList> {
  return request<OtaTaskList>({ url: '/ota/tasks', method: 'get', params })
}

/** 任务详情（GET /api/v1/ota/tasks/{task_id}，含灰度批次视图） */
export function getOtaTask(taskId: string): Promise<OtaTaskDetail> {
  return request<OtaTaskDetail>({ url: `/ota/tasks/${taskId}`, method: 'get' })
}

/** 创建任务（POST /api/v1/ota/tasks，201） */
export function createOtaTask(payload: OtaTaskCreateRequest): Promise<OtaTaskDetail> {
  return request<OtaTaskDetail>({ url: '/ota/tasks', method: 'post', data: payload })
}

/** 启动/继续（POST /api/v1/ota/tasks/{task_id}/start，门禁不满足返回 6003） */
export function startOtaTask(taskId: string, payload: OtaTaskStartRequest = {}): Promise<OtaTaskActionData> {
  return request<OtaTaskActionData>({ url: `/ota/tasks/${taskId}/start`, method: 'post', data: payload })
}

/** 暂停（POST /api/v1/ota/tasks/{task_id}/pause） */
export function pauseOtaTask(taskId: string, payload: OtaTaskPauseRequest = {}): Promise<OtaTaskActionData> {
  return request<OtaTaskActionData>({ url: `/ota/tasks/${taskId}/pause`, method: 'post', data: payload })
}

/** 继续（POST /api/v1/ota/tasks/{task_id}/resume） */
export function resumeOtaTask(taskId: string, payload: OtaTaskStartRequest = {}): Promise<OtaTaskActionData> {
  return request<OtaTaskActionData>({ url: `/ota/tasks/${taskId}/resume`, method: 'post', data: payload })
}

/** 取消（POST /api/v1/ota/tasks/{task_id}/cancel） */
export function cancelOtaTask(taskId: string, payload: OtaTaskCancelRequest = {}): Promise<OtaTaskActionData> {
  return request<OtaTaskActionData>({ url: `/ota/tasks/${taskId}/cancel`, method: 'post', data: payload })
}

/** 回滚（POST /api/v1/ota/tasks/{task_id}/rollback，A/B 分区回退） */
export function rollbackOtaTask(
  taskId: string,
  payload: OtaTaskRollbackRequest,
): Promise<OtaTaskRollbackData> {
  return request<OtaTaskRollbackData>({
    url: `/ota/tasks/${taskId}/rollback`,
    method: 'post',
    data: payload,
  })
}

/** 任务升级记录（GET /api/v1/ota/tasks/{task_id}/records） */
export function listOtaTaskRecords(
  taskId: string,
  params: OtaRecordListQuery = {},
): Promise<OtaRecordList> {
  return request<OtaRecordList>({ url: `/ota/tasks/${taskId}/records`, method: 'get', params })
}

/** 单车升级记录（GET /api/v1/ota/vehicles/{vehicle_id}/records） */
export function listVehicleOtaRecords(
  vehicleId: string,
  params: OtaRecordListQuery = {},
): Promise<OtaRecordList> {
  return request<OtaRecordList>({ url: `/ota/vehicles/${vehicleId}/records`, method: 'get', params })
}
