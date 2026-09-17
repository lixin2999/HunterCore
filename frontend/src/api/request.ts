/**
 * Axios 实例与统一请求封装
 *
 * 约定：
 * 1. 统一响应体 ApiResponse{code,message,data,request_id,timestamp}：code≠0 一律抛 HunterApiError；
 *    业务错误 HTTP 状态映射见 types/common.ts ERROR_CODE_HTTP_STATUS。
 * 2. 请求自动附加 Authorization: Bearer <access_token> 与 X-Request-ID（trace_id，贯穿全链路日志）。
 * 3. 1003（Token 过期）→ 单飞静默刷新后重放一次；1001（未认证）→ 清理会话并广播失效事件。
 * 4. 预签名直传 MinIO 的 PUT 请求不经本实例（无 JWT、无 ApiResponse 包装）。
 */
import axios, {
  type AxiosError,
  type AxiosInstance,
  type AxiosRequestConfig,
  type AxiosResponse,
  type InternalAxiosRequestConfig,
} from 'axios'

import { API_BASE_URL } from '@/constants'
import type { ApiResponse } from '@/types/common'
import type { RefreshTokenRequest, TokenPair } from '@/types/user'
import { HunterApiError, TOKEN_EXPIRED_CODE, describeErrorCode } from '@/utils/error-code'
import {
  clearStoredSession,
  getRefreshToken,
  getStoredSession,
  setStoredSession,
  toExpiresAt,
} from '@/utils/storage'

/** 会话失效广播事件（由布局/路由守卫监听后跳转登录页，避免模块循环依赖） */
export const SESSION_EXPIRED_EVENT = 'hunter:session-expired'

/** 请求默认超时（服务端 P95 ≤ 200ms，前端留足余量） */
const REQUEST_TIMEOUT_MS = 20_000

/** 带重试标记的请求配置 */
type RetryableConfig = AxiosRequestConfig & { __retried?: boolean }

/** 底层实例（不导出，业务统一走 request()） */
const http: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: REQUEST_TIMEOUT_MS,
  headers: { Accept: 'application/json' },
})

http.interceptors.request.use((config: InternalAxiosRequestConfig) => {
  const session = getStoredSession()
  if (session?.access_token) {
    config.headers.Authorization = `Bearer ${session.access_token}`
  }
  // trace_id：服务端契约 TraceRequestId 头，便于 Jaeger/日志关联
  if (!config.headers['X-Request-ID']) {
    config.headers['X-Request-ID'] = globalThis.crypto?.randomUUID?.() ?? `${Date.now()}`
  }
  return config
})

/** 清理本地会话并广播失效（不在此处跳路由，避免与 router 循环依赖） */
function expireSession(message: string): void {
  clearStoredSession()
  globalThis.dispatchEvent(new CustomEvent<string>(SESSION_EXPIRED_EVENT, { detail: message }))
}

/** 单飞刷新令牌（并发请求共享同一次刷新） */
let refreshPromise: Promise<string> | null = null

async function refreshAccessToken(): Promise<string> {
  if (refreshPromise) {
    return refreshPromise
  }
  const session = getStoredSession()
  const refreshToken = getRefreshToken()
  if (!session || !refreshToken || session.refresh_expires_at <= Date.now()) {
    expireSession('登录状态已过期，请重新登录')
    throw new HunterApiError(TOKEN_EXPIRED_CODE, '登录状态已过期，请重新登录')
  }
  const body: RefreshTokenRequest = { refresh_token: refreshToken }
  refreshPromise = axios
    .post<ApiResponse<TokenPair>>(`${API_BASE_URL}/user/refresh`, body, {
      timeout: 10_000,
      headers: { 'Content-Type': 'application/json' },
    })
    .then((response) => {
      const payload = response.data
      if (payload.code !== 0 || !payload.data) {
        throw new HunterApiError(payload.code, payload.message || '刷新令牌失败', payload.request_id)
      }
      const pair = payload.data
      setStoredSession({
        access_token: pair.access_token,
        refresh_token: pair.refresh_token,
        access_expires_at: toExpiresAt(pair.expires_in),
        refresh_expires_at: toExpiresAt(pair.refresh_expires_in),
        user: pair.user,
      })
      return pair.access_token
    })
    .catch((error: unknown) => {
      const message = error instanceof HunterApiError ? error.message : '登录状态已过期，请重新登录'
      expireSession(message)
      throw error instanceof HunterApiError ? error : new HunterApiError(TOKEN_EXPIRED_CODE, message)
    })
    .finally(() => {
      refreshPromise = null
    })
  return refreshPromise
}

http.interceptors.response.use(
  (response: AxiosResponse) => response,
  async (error: AxiosError<ApiResponse<unknown>>) => {
    const response = error.response
    const payload = response?.data
    const status = response?.status
    const config = error.config as RetryableConfig | undefined

    // 统一响应体存在时按业务码处理
    if (payload && typeof payload.code === 'number') {
      if (payload.code === TOKEN_EXPIRED_CODE && config && !config.__retried) {
        const token = await refreshAccessToken()
        config.__retried = true
        config.headers = { ...(config.headers ?? {}), Authorization: `Bearer ${token}` }
        return http.request(config)
      }
      if (payload.code === 1001) {
        expireSession(payload.message || describeErrorCode(1001))
      }
      return Promise.reject(
        new HunterApiError(
          payload.code,
          payload.message || describeErrorCode(payload.code),
          payload.request_id,
          status,
        ),
      )
    }

    // 无统一响应体：网络/网关异常
    if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT') {
      return Promise.reject(new HunterApiError(5000, '请求超时，请检查网络或稍后重试', undefined, status))
    }
    return Promise.reject(
      new HunterApiError(status ?? 5000, error.message || describeErrorCode(status ?? 5000), undefined, status),
    )
  },
)

/**
 * 统一请求入口：返回 ApiResponse.data（已做业务码校验）
 * 用法：`const data = await request<DashboardData>({ url: '/analytics/dashboard', method: 'get', params })`
 */
export async function request<T>(config: RetryableConfig): Promise<T> {
  const response = await http.request<ApiResponse<T>>(config as AxiosRequestConfig)
  const payload = response.data
  if (payload && typeof payload.code === 'number') {
    if (payload.code !== 0) {
      throw new HunterApiError(
        payload.code,
        payload.message || describeErrorCode(payload.code),
        payload.request_id,
        response.status,
      )
    }
    return payload.data
  }
  // 非统一响应体（例如网关直出静态内容）→ 原样返回
  return response.data as unknown as T
}

/**
 * 预签名直传（MinIO PUT / 分片 PUT）
 * - 不携带 JWT（凭据在 URL 中）、不解析统一响应体、支持上传进度回调
 * - 返回响应头 ETag（分片上传 complete 时回传）
 */
export async function putToPresignedUrl(
  url: string,
  body: Blob,
  onProgress?: (percent: number) => void,
): Promise<string> {
  const response = await axios.put(url, body, {
    timeout: 0,
    onUploadProgress: (event) => {
      if (onProgress && event.total) {
        onProgress(Math.round((event.loaded / event.total) * 100))
      }
    },
  })
  const etag = response.headers.etag
  return typeof etag === 'string' ? etag.replaceAll('"', '') : ''
}

