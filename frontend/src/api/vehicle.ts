/**
 * 车辆管理与车端接入 API（vehicle-service）
 *
 * 契约：contracts/openapi/vehicle-service.yaml
 * RBAC：vehicle:read / create / update / delete / execute
 * 关键约束：SCRAM 口令仅在 create/rotate 响应中一次性返回，前端不缓存、不写日志。
 */
import { downloadBlob, request } from './request'
import type {
  ProvisionResult,
  ReissueCertResult,
  RotateScramResult,
  VehicleCreateRequest,
  VehicleDetail,
  VehicleListQuery,
  VehiclePage,
  VehicleUpdateRequest,
} from '@/types/vehicle'

/* ------------------------------ 台账查询 ------------------------------ */

/** 列表（GET /api/v1/vehicle/list；分页 + status/provision_state/keyword 过滤） */
export function listVehicles(params: VehicleListQuery): Promise<VehiclePage> {
  return request<VehiclePage>({ url: '/vehicle/list', method: 'get', params })
}

/** 详情（GET /api/v1/vehicle/{vehicle_id}） */
export function getVehicle(vehicleId: string): Promise<VehicleDetail> {
  return request<VehicleDetail>({ url: `/vehicle/${encodeURIComponent(vehicleId)}`, method: 'get' })
}

/* ------------------------------ Provisioning ------------------------------ */

/**
 * 一键开通（POST /api/v1/vehicle；4 步编排：DB → SCRAM → Topics → Cert）
 * 响应一次性返回 scram_password（写时展示，不可再次拉取）与 bundle_download_url。
 */
export function createVehicle(payload: VehicleCreateRequest): Promise<ProvisionResult> {
  return request<ProvisionResult>({ url: '/vehicle', method: 'post', data: payload })
}

/** 修改台账基础字段（PATCH /api/v1/vehicle/{vehicle_id}；不触发 provisioning） */
export function patchVehicle(vehicleId: string, payload: VehicleUpdateRequest): Promise<VehicleDetail> {
  return request<VehicleDetail>({
    url: `/vehicle/${encodeURIComponent(vehicleId)}`,
    method: 'patch',
    data: payload,
  })
}

/** 下线车辆（DELETE /api/v1/vehicle/{vehicle_id}?purge_topics=true；逆序回收 SCRAM/Topic/证书） */
export function deleteVehicle(vehicleId: string, purgeTopics = true): Promise<null> {
  return request<null>({
    url: `/vehicle/${encodeURIComponent(vehicleId)}`,
    method: 'delete',
    params: { purge_topics: purgeTopics },
  })
}

/** 重置 SCRAM 口令（POST /api/v1/vehicle/{vehicle_id}/rotate-scram；一次性响应） */
export function rotateScram(vehicleId: string): Promise<RotateScramResult> {
  return request<RotateScramResult>({
    url: `/vehicle/${encodeURIComponent(vehicleId)}/rotate-scram`,
    method: 'post',
  })
}

/** 重签客户端证书（POST /api/v1/vehicle/{vehicle_id}/reissue-cert） */
export function reissueCert(vehicleId: string): Promise<ReissueCertResult> {
  return request<ReissueCertResult>({
    url: `/vehicle/${encodeURIComponent(vehicleId)}/reissue-cert`,
    method: 'post',
  })
}

/**
 * 下载接入包 ZIP（GET /api/v1/vehicle/{vehicle_id}/bundle）
 * 契约规定该端点非统一 JSON 响应 → 走 downloadBlob；文件名从 Content-Disposition 提取。
 */
export function downloadBundle(vehicleId: string): Promise<{ blob: Blob; filename: string }> {
  return downloadBlob(
    `/vehicle/${encodeURIComponent(vehicleId)}/bundle`,
    `${vehicleId}-bundle.zip`,
  )
}
