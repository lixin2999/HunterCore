/**
 * 用户与认证 API
 * 契约：contracts/openapi/api-gateway.yaml（网关统一入口，JWT + RBAC）
 * 限流（附录 D）：暴力破解防护由服务端登录失败限制实现，前端仅做交互提示。
 */
import { request } from './request'
import type { ChangePasswordRequest, LogoutRequest, LoginRequest, RefreshTokenRequest, TokenPair, UserProfile } from '@/types/user'

/** 登录（POST /api/v1/user/login） */
export function login(payload: LoginRequest): Promise<TokenPair> {
  return request<TokenPair>({ url: '/user/login', method: 'post', data: payload })
}

/** 刷新令牌（POST /api/v1/user/refresh） */
export function refreshToken(payload: RefreshTokenRequest): Promise<TokenPair> {
  return request<TokenPair>({ url: '/user/refresh', method: 'post', data: payload })
}

/** 登出（POST /api/v1/user/logout） */
export function logout(payload: LogoutRequest = {}): Promise<Record<string, never>> {
  return request<Record<string, never>>({ url: '/user/logout', method: 'post', data: payload })
}

/** 当前用户信息（GET /api/v1/user/me） */
export function fetchCurrentUser(): Promise<UserProfile> {
  return request<UserProfile>({ url: '/user/me', method: 'get' })
}

/** 修改密码（POST /api/v1/user/change-password；G-06 首登强制改密） */
export function changePassword(payload: ChangePasswordRequest): Promise<null> {
  return request<null>({ url: '/user/change-password', method: 'post', data: payload })
}
