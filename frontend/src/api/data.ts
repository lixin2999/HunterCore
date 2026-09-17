/**
 * 数据采集 API（data-collector）
 * 契约：contracts/openapi/data-collector.yaml
 * 限流（附录 D）：GET /data/telemetry 单用户 20 QPS → 前端遥测轮询间隔不得低于 2000ms。
 */
import { request } from './request'
import type {
  EventItem,
  EventListData,
  EventListQuery,
  FileCompleteData,
  FileCompleteRequest,
  FileListData,
  FileListQuery,
  FilePresignData,
  FilePresignRequest,
  TelemetryQuery,
  TelemetryQueryData,
} from '@/types/data'

/** 遥测数据查询（GET /api/v1/data/telemetry，保留期 90 天） */
export function queryTelemetry(params: TelemetryQuery): Promise<TelemetryQueryData> {
  return request<TelemetryQueryData>({ url: '/data/telemetry', method: 'get', params })
}

/** 事件列表（GET /api/v1/data/events） */
export function listEvents(params: EventListQuery): Promise<EventListData> {
  return request<EventListData>({ url: '/data/events', method: 'get', params })
}

/** 事件确认（POST /api/v1/data/events/{event_id}/acknowledge） */
export function acknowledgeEvent(eventId: number): Promise<Record<string, never>> {
  return request<Record<string, never>>({
    url: `/data/events/${eventId}/acknowledge`,
    method: 'post',
  })
}

/** 事件详情（GET /api/v1/data/events/{event_id}） */
export function getEvent(eventId: number): Promise<EventItem> {
  return request<EventItem>({ url: `/data/events/${eventId}`, method: 'get' })
}

/** 文件预签名（POST /api/v1/data/files/presign，上传有效期 1 小时） */
export function presignUpload(payload: FilePresignRequest): Promise<FilePresignData> {
  return request<FilePresignData>({ url: '/data/files/presign', method: 'post', data: payload })
}

/** 上传完成确认（POST /api/v1/data/files/complete，校验失败返回 6001） */
export function completeUpload(payload: FileCompleteRequest): Promise<FileCompleteData> {
  return request<FileCompleteData>({ url: '/data/files/complete', method: 'post', data: payload })
}

/** 文件列表（GET /api/v1/data/files，游标分页） */
export function listFiles(params: FileListQuery): Promise<FileListData> {
  return request<FileListData>({ url: '/data/files', method: 'get', params })
}
