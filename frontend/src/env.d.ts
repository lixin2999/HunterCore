/// <reference types="vite/client" />

/**
 * Vite 环境变量类型声明（与 .env.development / .env.production 一一对应）
 * 禁止在业务代码中使用未声明的环境变量。
 */
interface ImportMetaEnv {
  /** 应用标题 */
  readonly VITE_APP_TITLE: string
  /** REST 基础路径（默认 /api/v1） */
  readonly VITE_API_BASE_URL: string
  /** WebSocket 基础路径（默认 /ws） */
  readonly VITE_WS_BASE_URL: string
  /** 开发代理目标（仅开发环境） */
  readonly VITE_PROXY_TARGET?: string
  /** 开发端口（仅开发环境） */
  readonly VITE_DEV_PORT?: string
  /** 看板轮询间隔（ms） */
  readonly VITE_POLL_DASHBOARD_MS?: string
  /** 实时遥测轮询间隔（ms） */
  readonly VITE_POLL_TELEMETRY_MS?: string
  /** 会话状态轮询间隔（ms） */
  readonly VITE_POLL_SESSION_MS?: string
  /** 远程操控 UI 限速提示（m/s） */
  readonly VITE_RC_MAX_SPEED_MPS?: string
  /** 远程操控 UI 转角提示（rad） */
  readonly VITE_RC_MAX_STEER_RAD?: string
  /** 操控历史最大查询跨度（天） */
  readonly VITE_RC_HISTORY_MAX_RANGE_DAYS?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

/** 允许导入 .vue 单文件组件（vue-tsc 由 @vitejs/plugin-vue 提供类型） */
declare module '*.vue' {
  import type { DefineComponent } from 'vue'

  const component: DefineComponent<Record<string, unknown>, Record<string, unknown>, unknown>
  export default component
}
