/**
 * 通用 WebSocket 封装（自动重连 + 心跳保活）
 *
 * 契约依据：
 * - x-hunter-websocket-contract：`/ws/remote/{session_id}/control`（20Hz 控制上行 + 10s 心跳）、
 *   `/ws/remote/{session_id}/signal`（SDP/ICE 中继）；网关 WSS + JWT 握手。
 * - 关闭码语义见 types/remote.ts WS_CLOSE_CODES：1008/4001/4003/4010 属终态不重连；
 *   4008（新连接替代旧连接）不自动重连，避免多标签页抢连接。
 * - 上行超频（>25Hz）会被服务端丢弃，控制指令必须由调用方按 50ms 节流（constants RC_LIMITS）。
 */
import { WS_CLOSE_CODES } from '@/types/remote'
import { WS_JWT_SUBPROTOCOL } from '@/constants/remote'
import { getAccessToken } from '@/utils/storage'

/**
 * WS 握手子协议 offer（契约 remote-control pending #20 决策①：hunter-jwt 子协议承载 Token）
 * 双值形态 `hunter-jwt, <token>`，服务端验签后回显 `hunter-jwt`；
 * 非浏览器客户端可用 Authorization 头，浏览器禁止查询串明文携带 Token。
 */
export function hunterJwtProtocols(): string[] {
  const token = getAccessToken()
  return token ? [WS_JWT_SUBPROTOCOL, token] : [WS_JWT_SUBPROTOCOL]
}

/** 不重连的关闭码（正常关闭 / 认证失败 / 终态 / 被替代） */
const TERMINAL_CLOSE_CODES: readonly number[] = [1000, 1008, 4001, 4003, 4008, 4010]

export interface ReconnectingSocketOptions {
  /** 完整 ws(s):// URL（由 buildWsUrl 生成） */
  url: string
  /** 子协议（握手携带 JWT，见 constants WS_JWT_SUBPROTOCOL） */
  protocols?: string[]
  /** 心跳间隔（毫秒）；与 createHeartbeatFrame 同时提供才启用 */
  heartbeatIntervalMs?: number
  /** 心跳帧工厂（每 tick 调用以刷新 timestamp） */
  createHeartbeatFrame?: () => unknown
  /** 指数退避基准延迟（毫秒，默认 1000） */
  baseDelayMs?: number
  /** 指数退避上限（毫秒，默认 30000） */
  maxDelayMs?: number
  /** 最大重连次数（0 = 无限） */
  maxRetries?: number
  onOpen?: () => void
  onMessage?: (payload: unknown) => void
  /** willReconnect=false 表示连接已终止（终态关闭码或超出重试上限） */
  onClose?: (event: CloseEvent, willReconnect: boolean) => void
  onError?: (event: Event) => void
}

/** 套接字状态（对外暴露，供 UI 展示） */
export type SocketState = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed'

/** 自动重连 WebSocket 客户端：new → connect() → send/onMessage → close() */
export class ReconnectingSocket {
  private readonly options: ReconnectingSocketOptions
  private socket: WebSocket | null = null
  private heartbeatTimer: number | null = null
  private reconnectTimer: number | null = null
  private retries = 0
  private manualClosed = false
  private state: SocketState = 'idle'
  private lastCloseCode = 0

  constructor(options: ReconnectingSocketOptions) {
    this.options = { baseDelayMs: 1000, maxDelayMs: 30_000, maxRetries: 0, ...options }
  }

  /** 当前状态 */
  get currentState(): SocketState {
    return this.state
  }

  /** 最近一次关闭码对应文案（UI 提示用） */
  get closeReason(): string {
    return WS_CLOSE_CODES[this.lastCloseCode] ?? ''
  }

  /** 建立连接（可重复调用以手动重连） */
  connect(): void {
    this.clearReconnectTimer()
    this.state = 'connecting'
    this.socket = new WebSocket(this.options.url, this.options.protocols)
    this.socket.onopen = () => {
      this.retries = 0
      this.state = 'open'
      this.startHeartbeat()
      this.options.onOpen?.()
    }
    this.socket.onmessage = (event: MessageEvent<string>) => {
      this.options.onMessage?.(this.parsePayload(event.data))
    }
    this.socket.onerror = (event: Event) => {
      this.options.onError?.(event)
    }
    this.socket.onclose = (event: CloseEvent) => {
      this.lastCloseCode = event.code
      this.stopHeartbeat()
      const terminal = this.manualClosed || TERMINAL_CLOSE_CODES.includes(event.code)
      const maxRetries = this.options.maxRetries ?? 0
      const exhausted = maxRetries > 0 && this.retries >= maxRetries
      if (terminal || exhausted) {
        this.state = 'closed'
        this.options.onClose?.(event, false)
        return
      }
      this.state = 'reconnecting'
      this.options.onClose?.(event, true)
      this.scheduleReconnect()
    }
  }

  /** 发送 JSON 帧（未连接时静默丢弃；控制帧语义为最新值优先） */
  send(payload: unknown): boolean {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      return false
    }
    this.socket.send(JSON.stringify(payload))
    return true
  }

  /** 主动关闭（不再重连） */
  close(code = 1000, reason = 'client_close'): void {
    this.manualClosed = true
    this.stopHeartbeat()
    this.clearReconnectTimer()
    this.socket?.close(code, reason)
    this.state = 'closed'
  }

  /** 释放资源（组件 unmount 时调用） */
  dispose(): void {
    this.close()
    this.socket = null
  }

  /** 指数退避 + 抖动重连 */
  private scheduleReconnect(): void {
    this.retries += 1
    const base = this.options.baseDelayMs ?? 1000
    const max = this.options.maxDelayMs ?? 30_000
    const delay = Math.min(base * 2 ** (this.retries - 1), max)
    const jitter = Math.round(delay * 0.2 * Math.random())
    this.reconnectTimer = window.setTimeout(() => {
      if (!this.manualClosed) {
        this.connect()
      }
    }, delay + jitter)
  }

  /** 心跳保活（服务端：连续缺失 3 次降级，60s 结束会话） */
  private startHeartbeat(): void {
    const interval = this.options.heartbeatIntervalMs
    const factory = this.options.createHeartbeatFrame
    if (!interval || !factory) {
      return
    }
    this.stopHeartbeat()
    this.heartbeatTimer = window.setInterval(() => {
      this.send(factory())
    }, interval)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      window.clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  private parsePayload(raw: string): unknown {
    try {
      return JSON.parse(raw) as unknown
    } catch {
      return raw
    }
  }
}

/**
 * 构造 WebSocket URL（网关路由 /ws/remote/**）
 * @param path 形如 `/remote/{session_id}/control`（不含 WS 前缀）
 */
export function buildWsUrl(path: string): string {
  const base = import.meta.env.VITE_WS_BASE_URL ?? '/ws'
  if (base.startsWith('ws://') || base.startsWith('wss://')) {
    return `${base}${path}`
  }
  const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${scheme}//${window.location.host}${base}${path}`
}
