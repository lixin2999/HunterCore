/**
 * 远程操控（remote-control）类型（一）：车辆可操控视图 / 视频 / 控制通道 / WebRTC
 * 来源：contracts/openapi/remote-control.yaml —— 字段名与契约完全一致（snake_case）。
 */

/** 车辆状态（8 态受控词表，系统约束第 12 条） */
export type VehicleStatus =
  | 'offline'
  | 'online_idle'
  | 'auto_driving'
  | 'remote_controlled'
  | 'upgrading'
  | 'charging'
  | 'fault'
  | 'emergency'

/** 不可操控原因（controllable=false 时给出）；错误码映射 offline→4001、其他→4002、already_controlled→7001 */
export type BlockReason =
  | 'offline'
  | 'upgrading'
  | 'charging'
  | 'fault'
  | 'emergency'
  | 'already_controlled'

/** 会话状态：connecting → active → degraded → ended */
export type SessionStatus = 'connecting' | 'active' | 'degraded' | 'ended'

/** 会话结束原因（写入归档 sidecar + 审计日志） */
export type SessionEndReason =
  | 'operator_end'
  | 'admin_terminate'
  | 'heartbeat_timeout'
  | 'command_timeout'
  | 'vehicle_offline'
  | 'vehicle_fault'
  | 'video_lost'
  | 'server_shutdown'

/** 视频配置（系统约束第 15 条：H.264 / 720p@30fps / 2–4Mbps / 关键帧 1s） */
export interface VideoConfig {
  codec: 'h264'
  width: 1280
  height: 720
  fps: number
  min_bitrate_kbps: number
  max_bitrate_kbps: number
  keyframe_interval_s: 1
}

/** 控制通道信息（20Hz / 50ms；车端 >500ms 未收到指令自动减速停车） */
export interface ControlChannelInfo {
  hz: 20
  interval_ms: 50
  ack_timeout_ms: number
  stop_on_timeout_ms: 500
  max_speed_mps: number
}

/** 会话心跳（10s 间隔；连续缺失降级 / 超时结束会话） */
export interface SessionHeartbeat {
  interval_s: 10
  degraded_after_misses: number
  end_after_s: number
}

/** ICE 服务器（含 TURN 临时凭据与其有效期） */
export interface IceServer {
  urls: string[]
  username?: string
  credential?: string
  credential_ttl_s?: number
}

/** SRS 媒体信息（WHIP 推流 / WHEP 拉流） */
export interface SrsMediaInfo {
  app: string
  stream: string
  whip_url?: string
  whep_url?: string
  rtmp_url?: string
}

/** WebRTC 连接信息（信令 WS + 控制 WS + SRTP） */
export interface WebRtcConnectionInfo {
  signal_ws_url: string
  control_ws_url: string
  publisher?: 'vehicle'
  subscriber?: 'browser'
  transport: 'srtp'
  ice_servers: IceServer[]
  media_server: SrsMediaInfo
}

/** 操控录像归档信息（hunter-video，保留 90 天） */
export interface RecordArchiveInfo {
  bucket: 'hunter-video'
  object_key: string
  sidecar_object_key: string
  retention_days: 90
  container: 'mp4'
  codec: 'h264'
}

/** 会话简要信息（车辆列表内联展示） */
export interface ActiveSessionBrief {
  session_id: string
  operator_id: string
  operator_name?: string | null
  started_at: number
}

/** 可操控车辆视图（读模型：Redis vehicle:status:* + 车辆表） */
export interface ControllableVehicle {
  vehicle_id: string
  vehicle_name?: string | null
  model?: 'HUNTER_SE'
  status: VehicleStatus
  controllable: boolean
  block_reason?: BlockReason
  battery_soc: number
  velocity?: number
  last_online_time: number
  active_session?: ActiveSessionBrief
}

/** 可操控车辆列表（source=redis_read_model） */
export interface ControllableVehicleList {
  items: ControllableVehicle[]
  total: number
  page: number
  page_size: number
  source?: 'redis_read_model'
  generated_at?: number
}

/** 视频偏好（建会话时可选，服务端可拒绝超限值） */
export interface VideoPreference {
  fps?: number
  target_bitrate_kbps?: number
}

/** 创建会话请求（POST /api/v1/remote/session） */
export interface CreateRemoteSessionRequest {
  vehicle_id: string
  video?: VideoPreference
  note?: string
}

/** 车辆列表查询参数 */
export interface ControllableVehicleQuery {
  status?: VehicleStatus
  controllable_only?: boolean
  vehicle_id?: string
  page?: number
  page_size?: number
}

/** 控制通道统计（含指令往返时延） */
export interface ControlStats {
  commands_sent: number
  commands_acked: number
  ack_latency_ms_avg?: number
  ack_latency_ms_p95?: number
  timeout_events: number
  /** 会话内单调递增的指令序号（服务端 INCR，不复用前端 seq） */
  last_seq: number
  last_ack_at?: number | null
}

/** 视频链路统计（E2E 延迟预算 ≤200ms） */
export interface VideoStats {
  bitrate_kbps: number
  fps: number
  rtt_ms: number
  /** 0–1 */
  packet_loss_rate: number
  frames_dropped?: number
  e2e_latency_ms?: number
}

/** 降级原因（status 帧 degraded_reasons） */
export type DegradedReason = 'command_timeout' | 'video_latency' | 'packet_loss' | 'heartbeat_miss'

/** 远程会话主体信息（建会话 / 会话详情 / 会话列表共用） */
export interface RemoteSessionInfo {
  session_id: string
  vehicle_id: string
  operator_id: string
  operator_name?: string | null
  status: SessionStatus
  started_at: number
  video: VideoConfig
  control_channel: ControlChannelInfo
  heartbeat: SessionHeartbeat
  webrtc: WebRtcConnectionInfo
  record: RecordArchiveInfo
}

/** 会话详情（include_stats=true 时携带实时统计与降级告警） */
export interface RemoteSessionDetail extends RemoteSessionInfo {
  last_heartbeat_at?: number | null
  control_stats?: ControlStats
  video_stats?: VideoStats
  degraded?: boolean
  degraded_reasons?: DegradedReason[]
}

/** 会话结束结果（DELETE /session/{session_id}） */
export interface RemoteSessionResult {
  session_id: string
  vehicle_id: string
  operator_id: string
  status: 'ended'
  end_reason: SessionEndReason
  started_at: number
  ended_at: number
  duration_s: number
  control_stats?: ControlStats
  video_stats?: VideoStats
  record: RecordArchiveInfo
  sidecar_written?: boolean
}

/** 会话列表 */
export interface RemoteSessionList {
  items: RemoteSessionInfo[]
  total: number
  page: number
  page_size: number
}

/** 会话列表查询参数 */
export interface RemoteSessionListQuery {
  vehicle_id?: string
  operator_id?: string
  page?: number
  page_size?: number
}

/** 操控历史条目（由 MinIO sidecar 归档生成） */
export interface ControlHistoryItem {
  session_id: string
  vehicle_id: string
  operator_id: string
  operator_name?: string | null
  started_at: number
  ended_at: number
  duration_s: number
  end_reason: SessionEndReason
  end_reason_source?: 'platform' | 'vehicle' | 'timeout'
  commands_sent?: number
  commands_acked?: number
  ack_latency_ms_avg?: number
  ack_latency_ms_p95?: number
  timeout_events?: number
  video_e2e_latency_ms_p95?: number
  video_config?: VideoConfig
  video_object_key: string
  video_size_bytes?: number
  video_sha256?: string
  sidecar_object_key: string
  note?: string | null
}

/** 操控历史详情（附原始 sidecar 与录像可用性） */
export interface ControlHistoryDetail extends ControlHistoryItem {
  /** sidecar JSON 原文（schema_version / degraded_events / environment 等） */
  sidecar?: Record<string, unknown>
  /** false = 已过 90 天保留期或未封存 */
  video_available?: boolean
}

/** 操控历史列表（retention_days 固定 90） */
export interface ControlHistoryList {
  items: ControlHistoryItem[]
  total: number
  page: number
  page_size: number
  source?: 'minio_sidecar'
  retention_days?: 90
}

/** 操控历史查询参数（started_from/started_to 语义为会话开始时间） */
export interface ControlHistoryQuery {
  vehicle_id?: string
  operator_id?: string
  started_from?: number
  started_to?: number
  end_reason?: SessionEndReason
  page?: number
  page_size?: number
}

/** 录像访问凭据（预签名 URL 15 分钟 + HTTP Range 支持；URL 禁止落日志/缓存） */
export interface ControlVideoAccess {
  video_url: string
  expires_in: 900
  content_type: 'video/mp4'
  range_supported: true
  size_bytes?: number
  duration_s?: number
  etag?: string
}

/* ---------------------------------------------------------------------------
 * WebSocket 帧类型
 * 来源：x-hunter-websocket-contract.frames（字段名不可新增/更改）
 * 通道：
 *   /ws/remote/{session_id}/control —— 控制指令上行（20Hz）+ 回执/状态下行 + 10s 心跳
 *   /ws/remote/{session_id}/signal  —— WebRTC 信令中继（SDP/ICE）
 * ------------------------------------------------------------------------- */

/** 控制指令帧（浏览器 → 平台，20Hz） */
export interface WsControlFrame {
  type: 'control'
  session_id: string
  /** 前端顺序号（服务端不复用，仅用于本地诊断） */
  seq: number
  /** Unix epoch 秒（含毫秒） */
  timestamp: number
  control: {
    /** 限幅 [-max_speed_mps, +max_speed_mps]（服务端截断） */
    target_velocity: number
    /** 限幅 ±RC_MAX_STEER_RAD（服务端截断，pending #18） */
    target_steer: number
    /** 挡位（D/N/R） */
    gear: 'D' | 'N' | 'R'
  }
}

/** 心跳帧（浏览器 → 平台，10s 间隔；连续缺失 3 次降级，60s 结束会话） */
export interface WsHeartbeatFrame {
  type: 'heartbeat'
  session_id: string
  timestamp: number
}

/** 紧急停车帧（浏览器 → 平台；立即下发 target_velocity=0 并收敛会话） */
export interface WsEstopFrame {
  type: 'estop'
  session_id: string
  reason: string
}

/** 控制指令回执（平台 → 浏览器；来源 Kafka command_result） */
export interface WsAckFrame {
  type: 'ack'
  session_id: string
  /** 与服务端 last_seq 对应 */
  seq: number
  ack_latency_ms: number
  vehicle_state?: string
}

/** 会话状态推送（平台 → 浏览器，1Hz 或状态变更时） */
export interface WsStatusFrame {
  type: 'status'
  status: SessionStatus
  degraded_reasons: DegradedReason[]
  control_stats?: ControlStats
  video_stats?: VideoStats
}

/** 错误帧（平台 → 浏览器；code 取预定义错误码，禁止自定义） */
export interface WsErrorFrame {
  type: 'error'
  code: number
  message: string
  request_id?: string
}

/** 控制通道下行帧联合类型 */
export type WsControlDownlinkFrame = WsAckFrame | WsStatusFrame | WsErrorFrame

/** 信令帧：SDP 交换 */
export interface WsSdpFrame {
  type: 'sdp'
  sdp_type: 'offer' | 'answer'
  sdp: string
}

/** 信令帧：ICE candidate */
export interface WsIceFrame {
  type: 'ice'
  candidate: string
  sdpMid: string | null
  sdpMLineIndex: number | null
}

/** 信令通道帧联合类型（双向） */
export type WsSignalFrame = WsSdpFrame | WsIceFrame

/**
 * WebSocket 关闭码（x-hunter-websocket-contract.frames.close_codes）
 * 1008 认证/权限失效、4001 会话已结束、4003 心跳超时、4010 被管理员结束、4008 新连接替代旧连接
 */
export const WS_CLOSE_CODES: Readonly<Record<number, string>> = {
  1008: '认证/权限失效（Token 过期或角色被回收）',
  4001: '会话已结束或不存在',
  4003: '心跳超时（会话已结束）',
  4010: '会话被管理员强制结束',
  4008: '同一会话建立了新连接（旧连接已被替代）',
}


