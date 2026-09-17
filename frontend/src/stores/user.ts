/**
 * 用户会话状态（Pinia）
 *
 * 契约：api-gateway.yaml POST /user/login|refresh|logout、GET /user/me
 * 安全：Access Token 2h / Refresh Token 7d；仅本地保存 Token 副本，服务端会话态为 Redis `session:{user_id}`；
 *      日志与界面均不展示 Token 内容。
 */
import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

import * as userApi from '@/api/user'
import { SESSION_EXPIRED_EVENT } from '@/api/request'
import { ADMIN_ROLE } from '@/constants'
import type { LoginRequest, UserProfile } from '@/types/user'
import {
  clearStoredSession,
  getStoredSession,
  setStoredSession,
  toExpiresAt,
} from '@/utils/storage'

export const useUserStore = defineStore('user', () => {
  /** 用户档案（含 roles / permissions，用于菜单与 v-permission 指令） */
  const profile = ref<UserProfile | null>(getStoredSession()?.user ?? null)
  /** 会话失效提示（由 SESSION_EXPIRED_EVENT 写入，登录页/布局展示一次后清空） */
  const sessionExpiredMessage = ref('')
  const loading = ref(false)

  const roles = computed<string[]>(() => profile.value?.roles ?? [])
  const permissions = computed<string[]>(() => profile.value?.permissions ?? [])
  const isAuthenticated = computed<boolean>(
    () => Boolean(profile.value) && Boolean(getStoredSession()?.access_token),
  )
  const isAdmin = computed<boolean>(() => roles.value.includes(ADMIN_ROLE))
  const displayName = computed<string>(
    () => profile.value?.real_name || profile.value?.username || '未登录',
  )

  /** 登录（MFA 启用时须携带 totp_code，6 位数字） */
  async function login(payload: LoginRequest): Promise<void> {
    loading.value = true
    try {
      const pair = await userApi.login(payload)
      setStoredSession({
        access_token: pair.access_token,
        refresh_token: pair.refresh_token,
        access_expires_at: toExpiresAt(pair.expires_in),
        refresh_expires_at: toExpiresAt(pair.refresh_expires_in),
        user: pair.user,
      })
      profile.value = pair.user
      sessionExpiredMessage.value = ''
    } finally {
      loading.value = false
    }
  }

  /** 拉取当前用户（刷新页面后同步权限变更） */
  async function fetchProfile(): Promise<void> {
    const user = await userApi.fetchCurrentUser()
    profile.value = user
    const session = getStoredSession()
    if (session) {
      setStoredSession({ ...session, user })
    }
  }

  /** 登出（幂等：服务端 Token 已失效仍返回 code=0） */
  async function logout(): Promise<void> {
    const session = getStoredSession()
    try {
      await userApi.logout({ refresh_token: session?.refresh_token ?? null })
    } finally {
      reset()
    }
  }

  /**
   * 权限判定（管理员直通）
   * @param required 单个或多个权限编码（如 'ota:execute'）
   * @param mode some = 满足其一；every = 全部满足
   */
  function hasPermission(required: string | string[], mode: 'some' | 'every' = 'some'): boolean {
    if (isAdmin.value) {
      return true
    }
    const needed = Array.isArray(required) ? required : [required]
    if (needed.length === 0) {
      return true
    }
    return mode === 'every'
      ? needed.every((code) => permissions.value.includes(code))
      : needed.some((code) => permissions.value.includes(code))
  }

  /** 清理会话状态 */
  function reset(message = ''): void {
    clearStoredSession()
    profile.value = null
    sessionExpiredMessage.value = message
  }

  /** 消费会话失效提示（展示后清空，避免重复弹窗） */
  function consumeExpiredMessage(): string {
    const message = sessionExpiredMessage.value
    sessionExpiredMessage.value = ''
    return message
  }

  /** 监听请求层广播的会话失效事件（在 main.ts 中注册一次） */
  function bindSessionExpiredListener(): void {
    globalThis.addEventListener(SESSION_EXPIRED_EVENT, (event) => {
      const detail = (event as CustomEvent<string>).detail
      reset(detail || '登录状态已失效，请重新登录')
    })
  }

  return {
    profile,
    roles,
    permissions,
    isAuthenticated,
    isAdmin,
    displayName,
    sessionExpiredMessage,
    loading,
    login,
    fetchProfile,
    logout,
    hasPermission,
    reset,
    consumeExpiredMessage,
    bindSessionExpiredListener,
  }
})
