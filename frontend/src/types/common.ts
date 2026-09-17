/**
 * 通用类型（统一响应 / 分页 / 错误码）
 *
 * 字段命名约定：**与 OpenAPI 契约保持完全一致的 snake_case**，
 * 不做 camelCase 转换，避免前后端字段映射错误与"硬编码字段"风险。
 * 来源：contracts/openapi/README.md「统一约定」+ api-gateway.yaml 的 ApiResponse/ErrorCode。
 */

/**
 * 预定义错误码（附录 A，不可新增/更改含义）
 * 0 成功 / 1001 未认证 / 1002 无权限 / 1003 Token 过期 /
 * 2001 参数错误 / 2002 参数缺失 / 3001 资源不存在 / 3002 资源已存在 / 3003 资源状态冲突 /
 * 4001 车辆不在线 / 4002 车辆忙 / 5000 服务器内部错误 / 5001 服务不可用 /
 * 6001 OTA 包校验失败 / 6002 OTA 签名验证失败 / 6003 OTA 前置条件不满足 /
 * 7001 远程操控会话冲突 / 7002 远程操控视频建立失败
 */
export type ErrorCode =
  | 0
  | 1001
  | 1002
  | 1003
  | 2001
  | 2002
  | 3001
  | 3002
  | 3003
  | 4001
  | 4002
  | 5000
  | 5001
  | 6001
  | 6002
  | 6003
  | 7001
  | 7002

/** 统一响应体（所有服务强制，字段不可更改） */
export interface ApiResponse<T> {
  code: ErrorCode
  message: string
  data: T
  /** 链路追踪 ID（响应头 X-Request-ID 同值） */
  request_id: string
  /** Unix epoch 秒 */
  timestamp: number
}

/** 列表分页响应数据（page_size ≤ 200，与 BaseRepository.paginate 一致） */
export interface PageData<T> {
  items: T[]
  total: number
  page: number
  page_size: number
}

/** 分页查询参数 */
export interface PageQuery {
  page?: number
  page_size?: number
}

/** 时间范围查询参数（Unix epoch 秒） */
export interface TimeRangeQuery {
  start_time?: number
  end_time?: number
}

/** 健康探针响应数据（/healthz） */
export interface HealthStatus {
  status: 'ok'
  service?: string
  version?: string
}

/** 就绪探针响应数据（/readyz；依赖异常时 HTTP 503 + code=5001） */
export interface ReadyChecks {
  database: boolean
  redis: boolean
  minio?: boolean
}

/** HTTP 状态与业务码映射（服务端 error_handlers 的 HTTP_STATUS_BY_CODE 子集） */
export const ERROR_CODE_HTTP_STATUS: Readonly<Record<number, number>> = {
  1001: 401,
  1002: 403,
  1003: 401,
  2001: 422,
  2002: 422,
  3001: 404,
  3002: 409,
  3003: 409,
  4001: 409,
  4002: 409,
  5000: 500,
  5001: 503,
  6001: 422,
  6002: 422,
  6003: 422,
  7001: 409,
  7002: 503,
}
