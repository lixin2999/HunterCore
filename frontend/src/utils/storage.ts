/**
 * 本地会话持久化（纯函数模块，不依赖 Pinia/axios，避免循环依赖）
 *
 * 说明：服务端会话态为 Redis `session:{user_id}`（30 分钟，G-04 决策①收紧），前端本地仅保存 Token 副本。
 * 安全（G-04①：localStorage + 短 TTL + 严格 CSP）：
 * - Access Token 短 TTL（30min）由服务端签发控制，到期经请求层 401 静默刷新；
 * - 本地副本硬 TTL：Refresh Token 已过期时读取即清理（不留不可用的 Token 残影）；
 * - CSP 由 Nginx 响应头（infra/deploy/config/nginx.conf）+ index.html meta 双层施加；
 * - 禁止在日志中输出 Token。
 */
import type { AuthSession } from '@/types/user'

const SESSION_KEY = 'hunter.session'

/** 读取本地会话（解析失败 / 字段缺失 / Refresh 已过期 → 自动清理，G-04① 本地硬 TTL） */
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
    // 本地硬 TTL：Refresh Token 已过期则副本不可用，即刻清理（缩小 XSS 暴露窗口）
    if (parsed.refresh_expires_at && parsed.refresh_expires_at <= Date.now()) {
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
