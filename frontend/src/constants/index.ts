/**
 * 前端运行时配置（环境变量 → 类型化常量）
 *
 * 约束：
 * 1. 所有可调参数从环境变量读取（系统约束第 16 条），禁止在业务代码中硬编码 URL/端口/阈值/频率。
 * 2. 此处仅做默认值兜底与类型收敛；展示标签见 constants/enums.ts 等文件（取值域来自
 *    contracts/openapi/*.yaml 的 enum 与系统约束第 12/13/14/15 条，禁止自行新增）。
 */
export * from './enums'
export * from './analysis'
export * from './data'
export * from './ota'
export * from './permissions'
export * from './remote'

/** 应用标题 */
export const APP_TITLE: string = import.meta.env.VITE_APP_TITLE ?? 'HunterCore 运营管理后台'

/** REST 基础路径（网关统一入口；相对路径，禁止硬编码后端主机） */
export const API_BASE_URL: string = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'

/** WebSocket 基础路径（网关路由 /ws/remote/**） */
export const WS_BASE_URL: string = import.meta.env.VITE_WS_BASE_URL ?? '/ws'

function readNumber(raw: string | undefined, fallback: number): number {
  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

/**
 * 轮询间隔（毫秒）
 * 依据附录 D 限流：单用户 100 QPS、单 IP 200 QPS、GET /api/v1/data/telemetry 单用户 20 QPS。
 * 遥测最快 2000ms/车辆（0.5 QPS/车辆），10 辆车并行仍低于 20 QPS 上限。
 */
export const POLL_INTERVALS = {
  /** 运营看板聚合数据（analytics/dashboard + remote/vehicles + data/events） */
  dashboard: readNumber(import.meta.env.VITE_POLL_DASHBOARD_MS, 5000),
  /** 单车实时遥测（实时监控 / 地图最新定位） */
  telemetry: readNumber(import.meta.env.VITE_POLL_TELEMETRY_MS, 2000),
  /** 远程操控会话状态（GET /api/v1/remote/session/{session_id}） */
  session: readNumber(import.meta.env.VITE_POLL_SESSION_MS, 1000),
} as const

/**
 * 远程操控 UI 限幅（仅用于控件归一化与提示；真实限幅由服务端执行，
 * 速度以会话响应 control_channel.max_speed_mps 为准 —— x-hunter-control-safety）
 */
export const RC_LIMITS = {
  /** 契约 RC_MAX_SPEED_MPS 默认 2.0 m/s（系统约束第 15 条，不可更改） */
  maxSpeedMps: readNumber(import.meta.env.VITE_RC_MAX_SPEED_MPS, 2.0),
  /** ⚠ 待契约 pending #18 确认（底盘转角量程），此处仅为 UI 提示值 */
  maxSteerRad: readNumber(import.meta.env.VITE_RC_MAX_STEER_RAD, 0.6),
  /** 契约 RC_HISTORY_QUERY_MAX_RANGE_DAYS 默认 31 天（超限后端返回 2001） */
  historyMaxRangeDays: readNumber(import.meta.env.VITE_RC_HISTORY_MAX_RANGE_DAYS, 31),
  /** 控制指令频率 20Hz（50ms，系统约束第 15 条） */
  commandIntervalMs: 50,
  /** 前端心跳 10s（x-hunter-websocket-contract frames.control_uplink.heartbeat） */
  heartbeatIntervalMs: 10_000,
} as const

/**
 * 车辆标识校验
 * 来源：各契约 VehicleId.pattern = ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$，maxLength=64
 * （= 设备证书 CommonName = Kafka 消息 key），示例 HUNTER-001
 */
export const VEHICLE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/
