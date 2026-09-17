<script setup lang="ts">
/**
 * 远程操控台（remote-control.yaml）
 *
 * 链路（x-hunter-control-channel）：
 *   浏览器 --WS 20Hz--> remote-control --Kafka hunter.{vehicle_id}.remote_control--> 车端
 *   Kafka hunter.{vehicle_id}.command_result --> 平台 --> WS ack 帧 --> 浏览器
 *
 * 安全约束（系统约束第 15 条，前端必须遵守）：
 * - 指令频率 20Hz（50ms）；服务端仅保留最近 1 帧（最新值优先），>25Hz 会被丢弃；
 * - 指令 >500ms 无回执 → 会话降级并提示；车端 >500ms 未收到指令自动减速停车；
 * - 速度上限取会话响应 control_channel.max_speed_mps（默认 2.0 m/s，不可越过）；
 * - 会话互斥：同一车辆同时仅允许一名操作员（冲突返回 7001）；
 * - 心跳 10s（缺失 3 次降级、60s 结束会话）；
 * - 紧急停车：立即下发 estop 帧（服务端转 target_velocity=0 并收敛会话）。
 * - 视频：原生 WebRTC（RTCPeerConnection），禁止第三方云视频服务。
 */
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'

import { createRemoteSession, endRemoteSession, getRemoteSession } from '@/api/remote'
import { queryTelemetry } from '@/api/data'
import StatusTag from '@/components/common/StatusTag.vue'
import {
  POLL_INTERVALS,
  RC_LIMITS,
  SESSION_END_REASON_LABELS,
  WS_JWT_SUBPROTOCOL,
  WS_UPLINK_TYPES,
} from '@/constants'
import { PERMISSIONS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'
import { useVehicleStore } from '@/stores/vehicle'
import type { TelemetrySample } from '@/types/data'
import type {
  ControllableVehicle,
  DegradedReason,
  RemoteSessionDetail,
  WsAckFrame,
  WsControlDownlinkFrame,
  WsEstopFrame,
  WsHeartbeatFrame,
  WsStatusFrame,
} from '@/types/remote'
import { HunterApiError } from '@/utils/error-code'
import { formatNumber, formatTime } from '@/utils/format'
import { WebRtcClient } from '@/utils/webrtc'
import { ReconnectingSocket, buildWsUrl } from '@/utils/websocket'

const route = useRoute()
const userStore = useUserStore()
const vehicleStore = useVehicleStore()

/* ------------------------------ 车辆与会话 ------------------------------ */
const selectedVehicleId = ref<string>(
  typeof route.query.vehicle_id === 'string' ? route.query.vehicle_id : '',
)
const session = ref<RemoteSessionDetail | null>(null)
const creating = ref(false)
const ending = ref(false)

const vehicles = computed<ControllableVehicle[]>(() => vehicleStore.vehicles)
const selectedVehicle = computed<ControllableVehicle | undefined>(() =>
  vehicles.value.find((item) => item.vehicle_id === selectedVehicleId.value),
)

/** 是否可建立会话（离线 4001 / 忙 4002 / 已被占用 7001 由服务端最终判定） */
const canCreateSession = computed<boolean>(() => Boolean(selectedVehicle.value?.controllable))

async function loadVehicles(): Promise<void> {
  await vehicleStore.fetchVehicles({ controllable_only: false })
}

async function handleCreateSession(): Promise<void> {
  if (!selectedVehicleId.value) {
    ElMessage.warning('请先选择车辆')
    return
  }
  creating.value = true
  try {
    const data = await createRemoteSession({ vehicle_id: selectedVehicleId.value })
    session.value = data
    ElMessage.success(`会话已建立（${data.session_id}）`)
    // 视频元素由 v-if="session" 控制渲染，需等待 DOM 更新后再绑定 srcObject
    await nextTick()
    startVideo()
    startControlLoop()
    startSessionPolling()
  } catch (error) {
    // 4001 车辆不在线 / 4002 车辆忙 / 7001 会话冲突 / 7002 视频建立失败
    ElMessage.error(error instanceof HunterApiError ? error.message : '建立会话失败')
  } finally {
    creating.value = false
  }
}

/* ------------------------------ 视频（WebRTC） ------------------------------ */
const videoRef = ref<HTMLVideoElement | null>(null)
const webrtc = ref<WebRtcClient | null>(null)
const connectionState = ref<string>('closed')
const localVideoStats = ref<{ bitrateKbps: number; fps: number; rttMs: number; lossRate: number; latencyMs: number | null }>({
  bitrateKbps: 0,
  fps: 0,
  rttMs: 0,
  lossRate: 0,
  latencyMs: null,
})
let videoStatsTimer: number | null = null

function startVideo(): void {
  const current = session.value
  if (!current || !videoRef.value) {
    return
  }
  const client = new WebRtcClient({
    session: current,
    protocols: [WS_JWT_SUBPROTOCOL],
    onStateChange: (state) => {
      connectionState.value = state
    },
    onError: (message) => {
      ElMessage.warning(message)
    },
  })
  webrtc.value = client
  void client.start(videoRef.value).catch((error: unknown) => {
    ElMessage.error(error instanceof Error ? error.message : '视频连接建立失败')
  })
  videoStatsTimer = window.setInterval(async () => {
    if (!webrtc.value) {
      return
    }
    const stats = await webrtc.value.collectStats()
    localVideoStats.value = {
      bitrateKbps: stats.bitrate_kbps,
      fps: stats.fps,
      rttMs: stats.rtt_ms,
      lossRate: stats.packet_loss_rate,
      latencyMs: stats.e2e_latency_ms ?? null,
    }
  }, 2000)
}

/* ------------------------------ 控制通道（20Hz） ------------------------------ */
const controlSocket = ref<ReconnectingSocket | null>(null)
const targetVelocity = ref(0)
const targetSteer = ref(0)
const gear = ref<'D' | 'N' | 'R'>('N')
const seq = ref(0)
const lastAckSeq = ref(0)
const lastAckLatencyMs = ref<number | null>(null)
const lastSentAt = ref(0)
const socketState = ref<string>('idle')
let controlTimer: number | null = null

/** 速度上限（会话响应 control_channel.max_speed_mps 优先，缺省用环境变量限幅） */
const maxSpeed = computed<number>(() => session.value?.control_channel.max_speed_mps ?? RC_LIMITS.maxSpeedMps)

const currentSpeed = computed<number>(() =>
  Math.max(-maxSpeed.value, Math.min(maxSpeed.value, targetVelocity.value)),
)

function startControlLoop(): void {
  const current = session.value
  if (!current) {
    return
  }
  const socket = new ReconnectingSocket({
    url: buildWsUrl(
      current.webrtc.control_ws_url.startsWith('/')
        ? current.webrtc.control_ws_url
        : `/remote/${current.session_id}/control`,
    ),
    protocols: [WS_JWT_SUBPROTOCOL],
    heartbeatIntervalMs: RC_LIMITS.heartbeatIntervalMs,
    createHeartbeatFrame: (): WsHeartbeatFrame => ({
      type: WS_UPLINK_TYPES.heartbeat,
      session_id: current.session_id,
      timestamp: Date.now() / 1000,
    }),
    onMessage: (payload) => handleDownlink(payload as WsControlDownlinkFrame),
    onClose: (event, willReconnect) => {
      socketState.value = willReconnect ? 'reconnecting' : 'closed'
      if (!willReconnect) {
        ElMessage.error(`控制通道已关闭（code=${event.code}）：${socket.closeReason || '会话已结束'}`)
      }
    },
  })
  socket.connect()
  controlSocket.value = socket

  // 20Hz 固定节拍：即便值未变化也持续下发（车端 >500ms 无指令会自动停车）
  controlTimer = window.setInterval(() => {
    sendControlFrame()
  }, RC_LIMITS.commandIntervalMs)
}

/** 上行控制帧（限幅在这里做一次，服务端会再次截断） */
function sendControlFrame(): void {
  const current = session.value
  const socket = controlSocket.value
  if (!current || !socket) {
    return
  }
  seq.value += 1
  const payload = {
    type: WS_UPLINK_TYPES.control,
    session_id: current.session_id,
    seq: seq.value,
    timestamp: Date.now() / 1000,
    control: {
      target_velocity: currentSpeed.value,
      target_steer: Math.max(-RC_LIMITS.maxSteerRad, Math.min(RC_LIMITS.maxSteerRad, targetSteer.value)),
      gear: gear.value,
    },
  }
  if (socket.send(payload)) {
    lastSentAt.value = Date.now()
  }
}

/** 下行帧：ack（回执）/ status（1Hz 状态）/ error（预定义错误码） */
function handleDownlink(frame: WsControlDownlinkFrame): void {
  if (frame.type === 'ack') {
    const ack = frame as WsAckFrame
    lastAckSeq.value = ack.seq
    lastAckLatencyMs.value = ack.ack_latency_ms
    return
  }
  if (frame.type === 'status') {
    const status = frame as WsStatusFrame
    if (session.value) {
      session.value.status = status.status
      session.value.degraded_reasons = status.degraded_reasons
      session.value.control_stats = status.control_stats ?? session.value.control_stats
      session.value.video_stats = status.video_stats ?? session.value.video_stats
    }
    return
  }
  ElMessage.error(`控制通道错误（code=${frame.code}）：${frame.message}`)
}

/** 紧急停车（estop：立即下发 target_velocity=0 并触发服务端收敛会话） */
async function handleEstop(): Promise<void> {
  const current = session.value
  targetVelocity.value = 0
  gear.value = 'N'
  sendControlFrame()
  if (!current) {
    return
  }
  const frame: WsEstopFrame = {
    type: WS_UPLINK_TYPES.estop,
    session_id: current.session_id,
    reason: 'operator_emergency_stop',
  }
  controlSocket.value?.send(frame)
  ElMessage.warning('已发送紧急停车指令')
}

/** 降级原因文案（WS status 帧 degraded_reasons[]） */
const degradedTexts = computed<string[]>(() => {
  const reasons: DegradedReason[] = session.value?.degraded_reasons ?? []
  const map: Record<DegradedReason, string> = {
    command_timeout: '控制指令回执超时（>500ms）',
    video_latency: '视频端到端延迟超过 200ms 预算',
    packet_loss: '视频丢包率偏高',
    heartbeat_miss: '心跳缺失（连续 3 次）',
  }
  return reasons.map((reason) => map[reason] ?? reason)
})

/** 指令回执是否超时（>500ms 提示；车端会自动减速停车） */
const ackStale = computed<boolean>(
  () => lastSentAt.value > 0 && Date.now() - lastSentAt.value > 500,
)

/* --------------------- 会话状态轮询 + 实时遥测（限流区） --------------------- */
const telemetry = ref<TelemetrySample | null>(null)
let sessionTimer: number | null = null
let telemetryTimer: number | null = null

/** 会话状态兜底轮询（WS status 帧为主，HTTP 为兜底，间隔来自环境变量） */
function startSessionPolling(): void {
  sessionTimer = window.setInterval(async () => {
    const current = session.value
    if (!current) {
      return
    }
    try {
      const detail = await getRemoteSession(current.session_id, true)
      session.value = detail
    } catch (error) {
      if (error instanceof HunterApiError && error.code === 3001) {
        ElMessage.warning('会话已结束（可能因心跳超时或管理员操作）')
        await cleanupSession(false)
      }
    }
  }, POLL_INTERVALS.session)

  // 实时遥测：单车最快 2000ms（附录 D 限流 GET /data/telemetry 单用户 20 QPS）
  telemetryTimer = window.setInterval(async () => {
    const vehicleId = session.value?.vehicle_id
    if (!vehicleId) {
      return
    }
    try {
      const data = await queryTelemetry({
        vehicle_id: vehicleId,
        page: 1,
        page_size: 1,
        order: 'desc',
      })
      telemetry.value = data.items[0] ?? null
    } catch {
      // 遥测失败不打断操控（视频/控制通道独立）
    }
  }, POLL_INTERVALS.telemetry)
}

/** 键盘控车（W/S 加减速，A/D 转向，空格紧急停车；松键回落至 0） */
const KEYS = { forward: 'w', backward: 's', left: 'a', right: 'd', estop: ' ' } as const
/** 单次按键的线速度增量（m/s）按最大速度比例，避免硬编码绝对阈值 */
const VELOCITY_STEP_RATIO = 0.2

function handleKeyDown(event: KeyboardEvent): void {
  if (!session.value) {
    return
  }
  const step = maxSpeed.value * VELOCITY_STEP_RATIO
  const steerStep = RC_LIMITS.maxSteerRad * VELOCITY_STEP_RATIO
  const key = event.key.toLowerCase()
  if (key === KEYS.forward) {
    targetVelocity.value = Math.min(maxSpeed.value, targetVelocity.value + step)
    gear.value = 'D'
  } else if (key === KEYS.backward) {
    targetVelocity.value = Math.max(-maxSpeed.value, targetVelocity.value - step)
    gear.value = targetVelocity.value < 0 ? 'R' : 'D'
  } else if (key === KEYS.left) {
    targetSteer.value = Math.max(-RC_LIMITS.maxSteerRad, targetSteer.value + steerStep)
  } else if (key === KEYS.right) {
    targetSteer.value = Math.min(RC_LIMITS.maxSteerRad, targetSteer.value - steerStep)
  } else if (event.key === KEYS.estop) {
    void handleEstop()
  } else {
    return
  }
  event.preventDefault()
}

/** 释放控件后回到零位（安全默认：转向回中、速度归零） */
function releaseControls(): void {
  targetSteer.value = 0
  targetVelocity.value = 0
}

/* ------------------------------ 会话收敛与清理 ------------------------------ */
async function cleanupSession(callApi = true): Promise<void> {
  if (controlTimer !== null) {
    window.clearInterval(controlTimer)
    controlTimer = null
  }
  if (sessionTimer !== null) {
    window.clearInterval(sessionTimer)
    sessionTimer = null
  }
  if (telemetryTimer !== null) {
    window.clearInterval(telemetryTimer)
    telemetryTimer = null
  }
  if (videoStatsTimer !== null) {
    window.clearInterval(videoStatsTimer)
    videoStatsTimer = null
  }
  controlSocket.value?.dispose()
  controlSocket.value = null
  webrtc.value?.stop()
  webrtc.value = null
  telemetry.value = null
  releaseControls()

  const current = session.value
  session.value = null
  if (callApi && current) {
    try {
      const result = await endRemoteSession(current.session_id)
      ElMessage.info(
        `会话已结束（${SESSION_END_REASON_LABELS[result.end_reason] ?? result.end_reason}，时长 ${formatNumber(result.duration_s, 0)} s）`,
      )
    } catch (error) {
      ElMessage.error(error instanceof HunterApiError ? error.message : '结束会话失败，请重试')
    }
  }
}

/** 主动结束（需二次确认：结束后车辆恢复待命，录像归档保留 90 天） */
async function handleEndSession(): Promise<void> {
  try {
    await ElMessageBox.confirm('结束会话后车辆将退出远程操控模式（录像与记录会归档保留）。确认结束？', '结束会话', {
      type: 'warning',
    })
  } catch {
    return
  }
  ending.value = true
  try {
    await cleanupSession(true)
  } finally {
    ending.value = false
  }
}

function handleBeforeUnload(event: BeforeUnloadEvent): void {
  if (session.value) {
    // 浏览器无法在卸载时可靠发送请求；此处仅提示，会话由心跳超时（60s）自动收敛
    event.preventDefault()
    event.returnValue = ''
  }
}

onMounted(async () => {
  await loadVehicles()
  window.addEventListener('keydown', handleKeyDown)
  window.addEventListener('beforeunload', handleBeforeUnload)
})

onBeforeUnmount(() => {
  window.removeEventListener('keydown', handleKeyDown)
  window.removeEventListener('beforeunload', handleBeforeUnload)
  void cleanupSession(true)
})
</script>

<template>
  <div class="console">
    <el-card shadow="never" class="console__toolbar">
      <div class="toolbar">
        <el-select
          v-model="selectedVehicleId"
          filterable
          placeholder="选择目标车辆"
          style="width: 260px"
          :disabled="Boolean(session)"
        >
          <el-option
            v-for="vehicle in vehicles"
            :key="vehicle.vehicle_id"
            :label="`${vehicle.vehicle_name || vehicle.vehicle_id}（SOC ${formatNumber(vehicle.battery_soc, 0)}%）`"
            :value="vehicle.vehicle_id"
            :disabled="!vehicle.controllable"
          />
        </el-select>
        <StatusTag v-if="selectedVehicle" kind="vehicle" :value="selectedVehicle.status" />
        <el-button :loading="vehicleStore.loading" @click="loadVehicles">刷新车辆</el-button>
        <el-button
          v-if="!session"
          v-permission="PERMISSIONS.remoteCreate"
          type="primary"
          :loading="creating"
          :disabled="!canCreateSession"
          @click="handleCreateSession"
        >
          建立操控会话
        </el-button>
        <el-tag v-if="!session && selectedVehicle && !selectedVehicle.controllable" type="warning" size="small">
          不可操控：{{ selectedVehicle.block_reason ?? '车辆状态不允许' }}
        </el-tag>
        <el-button v-if="session" v-permission="PERMISSIONS.remoteExecute" type="danger" :loading="ending" @click="handleEndSession">
          结束会话
        </el-button>
      </div>
    </el-card>

    <el-row v-if="session" :gutter="12">
      <el-col :span="16">
        <el-card shadow="never" class="console__video-card">
          <template #header>
            <div class="toolbar">
              <span>实时视频（WebRTC / SRTP）</span>
              <div class="toolbar">
                <el-tag size="small" :type="connectionState === 'connected' ? 'success' : 'warning'">
                  连接状态：{{ connectionState }}
                </el-tag>
                <StatusTag kind="session" :value="session.status" />
              </div>
            </div>
          </template>

          <el-alert
            v-if="degradedTexts.length"
            class="alert"
            type="warning"
            :closable="false"
            show-icon
            title="链路降级提示"
          >
            <ul class="alert__list">
              <li v-for="text in degradedTexts" :key="text">{{ text }}</li>
            </ul>
          </el-alert>

          <div class="video">
            <video ref="videoRef" class="video__player" playsinline muted autoplay />
            <div class="video__overlay">
              <div class="video__metric">
                <span>速度指令</span>
                <strong>{{ formatNumber(currentSpeed, 2) }} m/s</strong>
                <span class="video__sub">上限 {{ formatNumber(maxSpeed, 2) }} m/s</span>
              </div>
              <div class="video__metric">
                <span>视频码率</span>
                <strong>{{ formatNumber(localVideoStats.bitrateKbps / 1000, 2) }} Mbps</strong>
                <span class="video__sub">{{ formatNumber(localVideoStats.fps, 1) }} fps</span>
              </div>
              <div class="video__metric">
                <span>端到端延迟</span>
                <strong>{{ localVideoStats.latencyMs === null ? '-' : formatNumber(localVideoStats.latencyMs, 0) }} ms</strong>
                <span class="video__sub">预算 ≤ 200 ms</span>
              </div>
              <div class="video__metric">
                <span>指令回执</span>
                <strong>{{ lastAckLatencyMs === null ? '-' : formatNumber(lastAckLatencyMs, 0) }} ms</strong>
                <span class="video__sub">SLO ≤ 100 ms</span>
              </div>
            </div>
          </div>

          <p class="hint">
            视频编码 H.264 720p@30fps（AGX Orin NVENC），码率 2–4 Mbps，关键帧间隔 1s；
            浏览器侧仅可请求降档（契约 quality_guard）。
          </p>
        </el-card>
      </el-col>

      <el-col :span="8">
        <el-card shadow="never" header="控制面板（20Hz / 50ms）">
          <el-form label-width="90px">
            <el-form-item label="目标速度">
              <el-slider
                v-model="targetVelocity"
                :min="-maxSpeed"
                :max="maxSpeed"
                :step="0.1"
                show-input
                :show-input-controls="false"
              />
            </el-form-item>
            <el-form-item label="目标转角">
              <el-slider
                v-model="targetSteer"
                :min="-RC_LIMITS.maxSteerRad"
                :max="RC_LIMITS.maxSteerRad"
                :step="0.01"
                show-input
                :show-input-controls="false"
              />
            </el-form-item>
            <el-form-item label="挡位">
              <el-radio-group v-model="gear">
                <el-radio-button value="D">D</el-radio-button>
                <el-radio-button value="N">N</el-radio-button>
                <el-radio-button value="R">R</el-radio-button>
              </el-radio-group>
            </el-form-item>
            <el-form-item>
              <el-button type="info" @click="releaseControls">回中 / 停车</el-button>
              <el-button v-permission="PERMISSIONS.remoteExecute" type="danger" @click="handleEstop">紧急停车</el-button>
            </el-form-item>
          </el-form>
          <el-alert
            v-if="ackStale"
            type="warning"
            :closable="false"
            show-icon
            title="指令回执超时（>500ms）：车端将自动减速停车，请检查网络后重试"
          />
          <p class="hint">
            键盘：W/S 加减速、A/D 转向、空格紧急停车；控制指令服务端限幅并写入
            Kafka <code>hunter.&#123;vehicle_id&#125;.remote_control</code>（acks=all）。
          </p>
        </el-card>
      </el-col>
    </el-row>

    <el-row v-if="session" :gutter="12" class="console__row">
      <el-col :span="8">
        <el-card shadow="never" header="会话信息">
          <el-descriptions :column="1" size="small" border>
            <el-descriptions-item label="会话 ID">{{ session.session_id }}</el-descriptions-item>
            <el-descriptions-item label="车辆">{{ vehicleStore.resolveName(session.vehicle_id) }}</el-descriptions-item>
            <el-descriptions-item label="操作员">
              {{ session.operator_name || session.operator_id }}
              <el-tag v-if="session.operator_id === userStore.profile?.user_id" size="small" type="success">我</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="开始时间">{{ formatTime(session.started_at) }}</el-descriptions-item>
            <el-descriptions-item label="视频编码">
              {{ session.video.codec }} {{ session.video.width }}×{{ session.video.height }}@{{ session.video.fps }}fps
              （{{ session.video.min_bitrate_kbps / 1000 }}–{{ session.video.max_bitrate_kbps / 1000 }} Mbps）
            </el-descriptions-item>
            <el-descriptions-item label="控制通道">
              {{ session.control_channel.hz }}Hz / {{ session.control_channel.interval_ms }}ms ·
              停车超时 {{ session.control_channel.stop_on_timeout_ms }}ms
            </el-descriptions-item>
            <el-descriptions-item label="心跳">
              {{ session.heartbeat.interval_s }}s（缺 {{ session.heartbeat.degraded_after_misses }} 次降级 /
              {{ session.heartbeat.end_after_s }}s 结束）
            </el-descriptions-item>
            <el-descriptions-item label="录像归档">
              {{ session.record.bucket }} · {{ session.record.object_key }}（保留 {{ session.record.retention_days }} 天）
            </el-descriptions-item>
            <el-descriptions-item label="最近心跳">{{ formatTime(session.last_heartbeat_at ?? null) }}</el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

      <el-col :span="8">
        <el-card shadow="never" header="实时遥测（2000ms 轮询）">
          <el-descriptions :column="1" size="small" border>
            <el-descriptions-item label="采样时间">
              {{ formatTime(telemetry ? Math.floor(telemetry.time) : null) }}
            </el-descriptions-item>
            <el-descriptions-item label="速度 / 转角">
              {{ formatNumber(telemetry?.chassis?.velocity ?? null, 2) }} m/s ·
              {{ formatNumber(telemetry?.chassis?.steering_angle ?? null, 3) }} rad
            </el-descriptions-item>
            <el-descriptions-item label="电量">
              {{ formatNumber(telemetry?.chassis?.battery_soc ?? null, 0) }}% ·
              {{ formatNumber(telemetry?.chassis?.battery_voltage ?? null, 1) }} V
            </el-descriptions-item>
            <el-descriptions-item label="定位">
              x {{ formatNumber(telemetry?.localization?.x ?? null, 2) }} ·
              y {{ formatNumber(telemetry?.localization?.y ?? null, 2) }} ·
              heading {{ formatNumber(telemetry?.localization?.heading ?? null, 3) }}
            </el-descriptions-item>
            <el-descriptions-item label="感知">
              {{ formatNumber(telemetry?.perception?.detected_objects ?? null, 0) }} 目标 ·
              {{ formatNumber(telemetry?.perception?.fps ?? null, 1) }} fps ·
              {{ formatNumber(telemetry?.perception?.latency_ms ?? null, 0) }} ms
            </el-descriptions-item>
            <el-descriptions-item label="规划 / 控制时延">
              {{ formatNumber(telemetry?.planning?.planning_latency_ms ?? null, 0) }} ms /
              {{ formatNumber(telemetry?.control?.control_latency_ms ?? null, 0) }} ms
            </el-descriptions-item>
            <el-descriptions-item label="系统负载">
              CPU {{ formatNumber(telemetry?.system?.cpu_usage ?? null, 1) }}% ·
              GPU {{ formatNumber(telemetry?.system?.gpu_usage ?? null, 1) }}% ·
              RSSI {{ formatNumber(telemetry?.system?.network_rssi ?? null, 0) }} dBm
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

      <el-col :span="8">
        <el-card shadow="never" header="链路统计">
          <el-descriptions :column="1" size="small" border>
            <el-descriptions-item label="WS 状态">{{ socketState }}</el-descriptions-item>
            <el-descriptions-item label="指令序号">已发 {{ seq }} · 已回执 {{ lastAckSeq }}</el-descriptions-item>
            <el-descriptions-item label="指令统计（服务端）">
              发送 {{ session.control_stats?.commands_sent ?? 0 }} ·
              回执 {{ session.control_stats?.commands_acked ?? 0 }} ·
              超时 {{ session.control_stats?.timeout_events ?? 0 }}
            </el-descriptions-item>
            <el-descriptions-item label="回执延迟">
              均值 {{ formatNumber(session.control_stats?.ack_latency_ms_avg ?? null, 0) }} ms ·
              P95 {{ formatNumber(session.control_stats?.ack_latency_ms_p95 ?? null, 0) }} ms
            </el-descriptions-item>
            <el-descriptions-item label="视频统计（服务端）">
              码率 {{ formatNumber(session.video_stats?.bitrate_kbps ?? null, 0) }} kbps ·
              RTT {{ formatNumber(session.video_stats?.rtt_ms ?? null, 0) }} ms
            </el-descriptions-item>
            <el-descriptions-item label="丢包率">
              {{ formatNumber((session.video_stats?.packet_loss_rate ?? localVideoStats.lossRate) * 100, 2) }}%
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <el-empty v-else-if="!creating" description="请选择车辆并建立操控会话（同一车辆同一时间仅允许一名操作员）" />
  </div>
</template>

<style scoped>
.console__toolbar {
  margin-bottom: 12px;
}

.console__row {
  margin-top: 12px;
}

.toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.alert {
  margin-bottom: 12px;
}

.alert__list {
  margin: 4px 0 0;
  padding-left: 18px;
}

.video {
  position: relative;
  width: 100%;
  aspect-ratio: 16 / 9;
  background: #0f1419;
  border-radius: 4px;
  overflow: hidden;
}

.video__player {
  width: 100%;
  height: 100%;
  object-fit: contain;
  background: #0f1419;
}

.video__overlay {
  position: absolute;
  left: 8px;
  top: 8px;
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.video__metric {
  display: flex;
  flex-direction: column;
  min-width: 108px;
  padding: 6px 8px;
  border-radius: 4px;
  background: rgba(15, 20, 25, 0.66);
  color: #e6edf3;
  font-size: 12px;
}

.video__metric strong {
  font-size: 15px;
}

.video__sub {
  color: #9fb0c0;
}

.hint {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
