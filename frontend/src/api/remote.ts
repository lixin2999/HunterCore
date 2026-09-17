/**
 * 远程操控 API（remote-control）
 * 契约：contracts/openapi/remote-control.yaml
 * 限流（附录 D）：POST /remote/session 单用户 1 QPS。
 * 注意：控制指令走 WebSocket（20Hz），HTTP 仅用于会话/车辆/历史管理。
 */
import { request } from './request'
import type {
  ControllableVehicleList,
  ControllableVehicleQuery,
  ControlHistoryDetail,
  ControlHistoryList,
  ControlHistoryQuery,
  ControlVideoAccess,
  CreateRemoteSessionRequest,
  RemoteSessionDetail,
  RemoteSessionList,
  RemoteSessionListQuery,
  RemoteSessionResult,
} from '@/types/remote'

/** 可操控车辆列表（GET /api/v1/remote/vehicles，读模型 Redis vehicle:status:*） */
export function listControllableVehicles(
  params: ControllableVehicleQuery = {},
): Promise<ControllableVehicleList> {
  return request<ControllableVehicleList>({ url: '/remote/vehicles', method: 'get', params })
}

/** 会话列表（GET /api/v1/remote/sessions） */
export function listRemoteSessions(params: RemoteSessionListQuery = {}): Promise<RemoteSessionList> {
  return request<RemoteSessionList>({ url: '/remote/sessions', method: 'get', params })
}

/**
 * 建立会话（POST /api/v1/remote/session，201）
 * 冲突（车辆已被占用）返回 7001；车辆离线 4001；车辆忙 4002；视频建立失败 7002。
 */
export function createRemoteSession(payload: CreateRemoteSessionRequest): Promise<RemoteSessionDetail> {
  return request<RemoteSessionDetail>({ url: '/remote/session', method: 'post', data: payload })
}

/** 会话详情（GET /api/v1/remote/session/{session_id}，include_stats=true 带实时统计） */
export function getRemoteSession(
  sessionId: string,
  includeStats = true,
): Promise<RemoteSessionDetail> {
  return request<RemoteSessionDetail>({
    url: `/remote/session/${sessionId}`,
    method: 'get',
    params: { include_stats: includeStats },
  })
}

/** 结束会话（DELETE /api/v1/remote/session/{session_id}） */
export function endRemoteSession(sessionId: string): Promise<RemoteSessionResult> {
  return request<RemoteSessionResult>({
    url: `/remote/session/${sessionId}`,
    method: 'delete',
  })
}

/** 操控历史列表（GET /api/v1/remote/history，来源 MinIO sidecar，保留 90 天） */
export function listControlHistory(params: ControlHistoryQuery = {}): Promise<ControlHistoryList> {
  return request<ControlHistoryList>({ url: '/remote/history', method: 'get', params })
}

/** 操控历史详情（GET /api/v1/remote/history/{session_id}） */
export function getControlHistory(sessionId: string): Promise<ControlHistoryDetail> {
  return request<ControlHistoryDetail>({ url: `/remote/history/${sessionId}`, method: 'get' })
}

/** 录像访问凭据（GET /api/v1/remote/history/{session_id}/video，预签名 15 分钟 + Range） */
export function getControlVideoAccess(sessionId: string): Promise<ControlVideoAccess> {
  return request<ControlVideoAccess>({ url: `/remote/history/${sessionId}/video`, method: 'get' })
}
