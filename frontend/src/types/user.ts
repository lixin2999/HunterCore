/**
 * 用户与认证类型
 * 来源：contracts/openapi/api-gateway.yaml（POST /api/v1/user/login|refresh|logout、GET /api/v1/user/me）
 * 安全约定：Access Token 2h / Refresh Token 7d；支持 MFA(TOTP)；日志禁止输出密码与 Token。
 */

/** 登录请求（POST /api/v1/user/login） */
export interface LoginRequest {
  username: string
  /** 8–128 字符；仅提交不落盘（writeOnly） */
  password: string
  /** MFA 动态码：6 位数字，启用 TOTP 时必填 */
  totp_code?: string | null
}

/** 刷新令牌请求（POST /api/v1/user/refresh） */
export interface RefreshTokenRequest {
  refresh_token: string
}

/** 登出请求（POST /api/v1/user/logout） */
export interface LogoutRequest {
  refresh_token?: string | null
}

/** 改密请求（POST /api/v1/user/change-password；G-06 首登强制改密） */
export interface ChangePasswordRequest {
  /** 当前口令（writeOnly，仅提交不落盘） */
  old_password: string
  /** 新口令：≥8 位且含大写、小写、数字（契约 pattern） */
  new_password: string
}

/** 用户档案（TokenPair.user 与 GET /api/v1/user/me 共用） */
export interface UserProfile {
  user_id: string
  username: string
  real_name?: string | null
  /** RBAC 角色（如 admin / operator / viewer） */
  roles: string[]
  /** 资源动作权限串（如 scene:write、remote:execute） */
  permissions?: string[]
  /** G-06 首登强制改密标志；true 时布局层弹改密对话框引导改密 */
  must_change_password?: boolean
}

/** 令牌对（POST /api/v1/user/login|refresh 响应 data） */
export interface TokenPair {
  access_token: string
  refresh_token: string
  token_type: 'Bearer'
  /** Access Token 有效期（秒，2h） */
  expires_in: number
  /** Refresh Token 有效期（秒，7d） */
  refresh_expires_in: number
  user: UserProfile
}

/** 前端会话模型（本地持久化，非契约实体） */
export interface AuthSession {
  access_token: string
  refresh_token: string
  /** Access Token 过期时间（Unix epoch 毫秒，由 expires_in 推算） */
  access_expires_at: number
  /** Refresh Token 过期时间（Unix epoch 毫秒） */
  refresh_expires_at: number
  user: UserProfile
}
