/**
 * 远程操控受控词表
 * 来源：contracts/openapi/remote-control.yaml（VehicleStatus / BlockReason / SessionStatus /
 * SessionEndReason / VideoConfig / ControlChannelInfo / SessionHeartbeat）+ 系统约束第 15 条。
 */

/** 会话状态 */
export const SESSION_STATUS_LABELS: Record<string, string> = {
  connecting: '连接中',
  active: '操控中',
  degraded: '链路降级',
  ended: '已结束',
}

/** 会话状态 → 标签类型 */
export const SESSION_STATUS_TAG_TYPES: Record<string, string> = {
  connecting: 'info',
  active: 'success',
  degraded: 'warning',
  ended: 'info',
}

/** 会话结束原因 */
export const SESSION_END_REASON_LABELS: Record<string, string> = {
  operator_end: '操作员结束',
  admin_terminate: '管理员强制结束',
  heartbeat_timeout: '心跳超时',
  command_timeout: '指令超时',
  vehicle_offline: '车辆离线',
  vehicle_fault: '车辆故障',
  video_lost: '视频中断',
  server_shutdown: '服务重启',
}

/** 不可操控原因 */
export const BLOCK_REASON_LABELS: Record<string, string> = {
  offline: '车辆离线',
  upgrading: 'OTA 升级中',
  charging: '充电中',
  fault: '车辆故障',
  emergency: '紧急状态',
  already_controlled: '已被其他操作员操控',
}

/** 视频配置展示（x-hunter-video-contract：H.264 / 720p@30fps / 2–4Mbps / 关键帧 1s） */
export const VIDEO_CODEC_LABELS: Record<string, string> = {
  h264: 'H.264（AGX Orin NVENC 硬编码）',
}

/** 控制通道指标文案（x-hunter-control-channel / x-hunter-control-safety） */
export const CONTROL_CHANNEL_HINTS = {
  /** 指令频率 20Hz（50ms 间隔） */
  hz: 20,
  intervalMs: 50,
  /** 车端 >500ms 未收到指令自动减速停车 */
  stopOnTimeoutMs: 500,
  /** 指令端到端延迟 SLO ≤ 100ms */
  ackSloMs: 100,
  /** 视频端到端延迟 SLO ≤ 200ms（采集编码 50 + 网络 100 + 解码渲染 30） */
  videoSloMs: 200,
  /** 心跳 10s，连续缺失 3 次降级，60s 结束会话 */
  heartbeatIntervalS: 10,
  endAfterS: 60,
} as const

/** WebSocket 下行帧类型（x-hunter-websocket-contract.frames.control_downlink） */
export const WS_DOWNLINK_TYPES = {
  ack: 'ack',
  status: 'status',
  error: 'error',
} as const

/** WebSocket 上行帧类型（frames.control_uplink） */
export const WS_UPLINK_TYPES = {
  control: 'control',
  heartbeat: 'heartbeat',
  estop: 'estop',
} as const

/** 信令通道帧类型（frames.signal） */
export const WS_SIGNAL_TYPES = {
  sdp: 'sdp',
  ice: 'ice',
} as const

/**
 * WebSocket 子协议名（握手阶段以 Sec-WebSocket-Protocol 承载 JWT）
 * 契约 remote-control pending #20 已定稿（决策①）：浏览器以子协议承载——
 * 双值形态 `hunter-jwt, <token>`（服务端回显 hunter-jwt），拼装见 utils/websocket
 * hunterJwtProtocols()；非浏览器客户端可用 Authorization 头；禁止查询串明文携带 Token。
 */
export const WS_JWT_SUBPROTOCOL = 'hunter-jwt'
