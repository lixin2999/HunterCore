/**
 * 错误码处理（附录 A 预定义错误码，禁止自定义）
 * 说明：HTTP 状态码 → 业务码映射见 types/common.ts ERROR_CODE_HTTP_STATUS；
 * 前端仅做提示文案与跳转决策，业务语义以服务端 message 为准。
 */
import type { ErrorCode } from '@/types/common'

/** 错误码文案（附录 A） */
export const ERROR_CODE_MESSAGES: Readonly<Record<number, string>> = {
  1001: '未认证，请重新登录',
  1002: '无权限，请联系管理员分配角色',
  1003: '登录状态已过期，请重新登录',
  2001: '请求参数错误',
  2002: '必填参数缺失',
  3001: '资源不存在',
  3002: '资源已存在',
  3003: '资源当前状态不允许该操作',
  4001: '车辆不在线',
  4002: '车辆忙（可能正在执行升级等任务）',
  5000: '服务器内部错误',
  5001: '依赖服务不可用，请稍后重试',
  6001: 'OTA 包校验失败（完整性不匹配）',
  6002: 'OTA 签名验证失败',
  6003: 'OTA 前置条件不满足（电量/停车/存储/网络）',
  7001: '远程操控会话冲突（车辆已被其他操作员操控）',
  7002: '远程操控视频建立失败',
}

/** 会话失效类错误码（需清理本地会话并跳转登录） */
export const AUTH_ERROR_CODES: readonly ErrorCode[] = [1001, 1003]

/** Token 过期（可尝试静默刷新） */
export const TOKEN_EXPIRED_CODE: ErrorCode = 1003

/** 取错误码文案（未知码回退服务端 message） */
export function describeErrorCode(code: number, fallback = '请求失败'): string {
  return ERROR_CODE_MESSAGES[code] ?? fallback
}

/** 业务/网络异常统一封装（便于组件层 catch 后展示 message） */
export class HunterApiError extends Error {
  readonly code: number
  readonly requestId?: string
  readonly httpStatus?: number

  constructor(code: number, message: string, requestId?: string, httpStatus?: number) {
    super(message)
    this.name = 'HunterApiError'
    this.code = code
    this.requestId = requestId
    this.httpStatus = httpStatus
  }
}
