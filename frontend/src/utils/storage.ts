/**
 * 本地会话持久化（纯函数模块，不依赖 Pinia/axios，避免循环依赖）
 *
 * 说明：服务端会话态为 Redis `session:{user_id}`（2 小时），前端本地仅保存 Token 副本。
 * 安全：Refresh Token 存 localStorage 属已知折衷（无 HttpOnly Cookie 端点契约），
 * 通过 CSP + 前端转义降低 XSS 风险；禁止在日志中输出 Token。
 */
import type { AuthSession } from '@/types/user'

const SESSION_KEY = 'hunter.session'

/** 读取本地会话（解析失败自动清理） */
export function getStoredSession(): AuthSession | null {
  const raw = localStorage.getItem(SESSION_KEY)
  if (!raw) {
    return null
  }
  try {
    const parsed = JSON.parse(raw) as AuthSession
    if (!parsed.access_token || !parsed.user) {
      localStorage.removeItem(SESSION_KEY)
      return null
    }
    return parsed
  } catch {
    localStorage.removeItem(SESSION_KEY)
    return null
  }
}

/** 写入会话 */
export function setStoredSession(session: AuthSession): void {
  localStorage.setItem(SESSION_KEY, JSON.stringify(session))
}

/** 清理会话 */
export function clearStoredSession(): void {
  localStorage.removeItem(SESSION_KEY)
}

/** 读取 Access Token */
export function getAccessToken(): string {
  return getStoredSession()?.access_token ?? ''
}

/** 读取 Refresh Token */
export function getRefreshToken(): string {
  return getStoredSession()?.refresh_token ?? ''
}

/** 本地时间戳（毫秒）判断 Access Token 是否即将过期（默认提前 30s 视为过期） */
export function isAccessTokenExpiring(thresholdMs = 30_000): boolean {
  const session = getStoredSession()
  if (!session) {
    return true
  }
  return session.access_expires_at - Date.now() <= thresholdMs
}

/** 由 expires_in（秒）计算绝对过期时间（毫秒） */
export function toExpiresAt(expiresInSeconds: number): number {
  return Date.now() + expiresInSeconds * 1000
}
