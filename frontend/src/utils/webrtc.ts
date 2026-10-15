/**
 * 原生 WebRTC 客户端（浏览器侧 WHEP 拉流 · recvonly）
 *
 * 架构（V1.21.0 收口 pending #10/#12）：车端 RTMP 推流 → SRS → 浏览器 WHEP 拉流。
 * 视频不再经由 signal WS 中继（旧「recvonly offer → 车端 answer」回程在平台侧从未落地，
 * 导致「信令连接异常，正在重试」死链）。改为标准 WHEP 握手：
 *   1. 读取会话响应 webrtc.media_server.whep_url（由后端按会话 stream 拼装下发）；
 *   2. 本地 recvonly offer → 等待 ICE 收集完成（offer 内联 candidate，非 trickle）；
 *   3. POST(whep_url, Content-Type: application/sdp, body=offer.sdp) → 取回 answer SDP；
 *   4. setRemoteDescription(answer)，ontrack 绑定到 video；stop 时对 WHEP 资源发 DELETE 释放。
 *
 * 契约依据（remote-control.yaml x-hunter-control-channel）：
 * - 媒体面：WebRTC / SRTP（DTLS-SRTP 加密），不经 api-gateway；
 * - ICE：服务器与 TURN 短期凭据由会话响应 webrtc.ice_servers 下发（禁止前端硬编码 STUN/TURN）；
 * - 视频：H.264 720p@30fps，2–4 Mbps，关键帧 1s（会话响应 video 字段），浏览器只能请求降档；
 * - 延迟预算：采集编码 50 + 网络 100 + 解码渲染 30 = 200ms，前端以 getStats() 估测并上报。
 * 禁止使用任何第三方云视频服务（契约 ui 约束）。
 */
import type { IceServer, RemoteSessionInfo, VideoStats } from '@/types/remote'

export interface WebRtcClientOptions {
  /** 会话信息（提供 ice_servers / media_server.whep_url / video 配置） */
  session: RemoteSessionInfo
  /** 保留字段（WHEP 走 HTTP，不再需要 WS 子协议；控制通道另有其实例） */
  protocols?: string[]
  onStateChange?: (state: RTCPeerConnectionState) => void
  onIceCandidateStateChange?: (state: RTCIceConnectionState) => void
  onError?: (message: string) => void
}

/**
 * getStats() 返回的统计对象在 TS DOM 类型中不完整，按 RTCStats 规范补充所需可选字段
 * （remote-inbound-rtp.roundTripTime / candidate-pair.nominated / currentRoundTripTime 等）
 */
interface RTCRemoteInboundRtpStreamStatsLike {
  roundTripTime?: number
}

interface RTCCandidatePairStatsLike {
  nominated?: boolean
  currentRoundTripTime?: number
}

/** toIceServers：契约 IceServer → 浏览器 RTCIceServer（credential_ttl_s 仅用于展示） */
function toIceServers(servers: IceServer[]): RTCIceServer[] {
  return servers.map((item) => ({
    urls: item.urls,
    username: item.username,
    credential: item.credential,
  }))
}

/** ICE 收集等待上限（超时则以当前 candidate 继续，避免个别环境无 candidate 时永久挂起） */
const ICE_GATHERING_TIMEOUT_MS = 2000

/**
 * WHEP 重试策略（V1.21.9）：车端 RTMP 推流到 SRS 有启动延迟（pipeline preroll ~1-2s），
 * 首次 WHEP 可能因流尚无数据而被 SRS 关连（502），需退避重试。
 */
const WHEP_MAX_RETRIES = 3
const WHEP_RETRY_DELAYS_MS = [3000, 5000, 8000] // 第 1/2/3 次重试前等待时间

/**
 * 远程操控视频客户端（WHEP 拉流）
 * 用法：new WebRtcClient(options) → await start(videoEl) → stats() → stop()
 */
export class WebRtcClient {
  private readonly options: WebRtcClientOptions
  private readonly pc: RTCPeerConnection
  /** WHEP 资源 URL（answer 响应 Link 头 rel="resource"）；stop 时 DELETE 释放会话 */
  private whepResourceUrl: string | null = null
  private lastStatsSnapshot: { bytes: number; at: number } | null = null

  constructor(options: WebRtcClientOptions) {
    this.options = options
    this.pc = new RTCPeerConnection({
      iceServers: toIceServers(options.session.webrtc.ice_servers),
    })
    this.pc.onconnectionstatechange = () => {
      this.options.onStateChange?.(this.pc.connectionState)
    }
    this.pc.oniceconnectionstatechange = () => {
      this.options.onIceCandidateStateChange?.(this.pc.iceConnectionState)
    }
    // 仅接收（recvonly）：媒体由车端经 SRS 推送
    this.pc.addTransceiver('video', { direction: 'recvonly' })
    this.pc.addTransceiver('audio', { direction: 'recvonly' })
  }

  /** 经 WHEP 完成 SDP 协商，远端轨道绑定到 video 元素（含退避重试） */
  async start(videoElement: HTMLVideoElement): Promise<void> {
    const whepUrl = this.options.session.webrtc.media_server.whep_url
    if (!whepUrl) {
      // 媒体地址缺失（后端未配置 SRS 基址）：显式提示，不再无限重连
      this.options.onError?.('媒体服务未就绪（缺少 SRS WHEP 拉流地址）')
      return
    }

    this.pc.ontrack = (event: RTCTrackEvent) => {
      const [stream] = event.streams
      if (stream) {
        videoElement.srcObject = stream
        void videoElement.play().catch(() => {
          // 自动播放可能被浏览器策略阻止：由 UI 提示用户点击播放
          this.options.onError?.('浏览器阻止了自动播放，请点击视频区域以开始播放')
        })
      }
    }

    // 构建 offer（只做一次，重试复用同一 PC 与 offer）
    try {
      const offer = await this.pc.createOffer({ offerToReceiveVideo: true, offerToReceiveAudio: true })
      await this.pc.setLocalDescription(offer)
      // 非 trickle：等待 ICE 收集完成，把 candidate 内联进 offer 后一次性提交给 SRS
      await this.waitForIceGathering()
    } catch (error) {
      const message = error instanceof Error ? error.message : '未知错误'
      this.options.onError?.(`视频 offer 构建失败：${message}`)
      return
    }

    // WHEP POST + 退避重试
    const sdp = this.pc.localDescription?.sdp ?? ''
    let lastError = ''
    for (let attempt = 0; attempt <= WHEP_MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        const delay = WHEP_RETRY_DELAYS_MS[attempt - 1] ?? 8000
        this.options.onError?.(`视频拉流未就绪，${Math.round(delay / 1000)}s 后重试（第 ${attempt} 次）…`)
        await new Promise((r) => setTimeout(r, delay))
      }
      try {
        const response = await fetch(whepUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/sdp' },
          body: sdp,
        })
        if (!response.ok) {
          throw new Error(`WHEP ${response.status} ${response.statusText}`)
        }
        this.captureResourceUrl(response, whepUrl)
        const answer = await response.text()
        await this.pc.setRemoteDescription({ type: 'answer', sdp: answer })
        return // 成功
      } catch (error) {
        lastError = error instanceof Error ? error.message : '未知错误'
        // 网络错误（TypeError: Failed to fetch）不重试（可能已离线）
        if (error instanceof TypeError) break
      }
    }
    this.options.onError?.(`视频拉流失败：${lastError}`)
  }

  /** 等待 ICE gathering 完成（或超时兜底） */
  private waitForIceGathering(): Promise<void> {
    if (this.pc.iceGatheringState === 'complete') {
      return Promise.resolve()
    }
    return new Promise((resolve) => {
      const finish = () => {
        if (this.pc.iceGatheringState !== 'complete') {
          return
        }
        window.clearTimeout(timer)
        this.pc.removeEventListener('icegatheringstatechange', finish)
        resolve()
      }
      const timer = window.setTimeout(() => {
        this.pc.removeEventListener('icegatheringstatechange', finish)
        resolve()
      }, ICE_GATHERING_TIMEOUT_MS)
      this.pc.addEventListener('icegatheringstatechange', finish)
    })
  }

  /** 解析 answer 响应 Link 头中的 WHEP 资源地址（用于 stop 时 DELETE 释放） */
  private captureResourceUrl(response: Response, whepUrl: string): void {
    const link = response.headers.get('Link') ?? ''
    const match = link.match(/<([^>]+)>;\s*rel="resource"/)
    if (!match?.[1]) {
      this.whepResourceUrl = null
      return
    }
    try {
      this.whepResourceUrl = new URL(match[1], whepUrl).toString()
    } catch {
      this.whepResourceUrl = match[1]
    }
  }

  /**
   * 采集视频链路统计（对照 200ms 延迟预算与 2–4 Mbps 码率区间）
   * 说明：e2e_latency_ms 由 jitterBufferDelay/framesDecoded 换算（含抖动缓冲排队时间），
   *       为近似值；精确 P95 由服务端按 RTP 时间戳换算并写入归档 sidecar。
   */
  async collectStats(): Promise<VideoStats> {
    const report = await this.pc.getStats()
    let bitrateKbps = 0
    let fps = 0
    let lossRate = 0
    let rttMs = 0
    let e2eLatencyMs: number | undefined

    report.forEach((stat: RTCStats) => {
      const item = stat as RTCInboundRtpStreamStats &
        RTCRemoteInboundRtpStreamStatsLike &
        RTCCandidatePairStatsLike
      if (stat.type === 'inbound-rtp' && item.kind === 'video') {
        fps = Number(item.framesPerSecond ?? 0)
        const received = Number(item.packetsReceived ?? 0)
        const lost = Number(item.packetsLost ?? 0)
        lossRate = received + lost > 0 ? lost / (received + lost) : 0
        const bytes = Number(item.bytesReceived ?? 0)
        const now = Date.now()
        if (this.lastStatsSnapshot && now > this.lastStatsSnapshot.at) {
          bitrateKbps = Math.round(
            ((bytes - this.lastStatsSnapshot.bytes) * 8) / (now - this.lastStatsSnapshot.at),
          )
        }
        this.lastStatsSnapshot = { bytes, at: now }
        const jitterBufferDelay = Number(item.jitterBufferDelay ?? 0)
        const emitted = Number(item.jitterBufferEmittedCount ?? 0)
        if (emitted > 0) {
          e2eLatencyMs = Math.round((jitterBufferDelay / emitted) * 1000)
        }
      }
      if (stat.type === 'remote-inbound-rtp' && item.kind === 'video') {
        rttMs = Math.round(Number(item.roundTripTime ?? 0) * 1000)
      }
      if (stat.type === 'candidate-pair' && item.nominated === true) {
        rttMs = rttMs || Math.round(Number(item.currentRoundTripTime ?? 0) * 1000)
      }
    })

    return {
      bitrate_kbps: bitrateKbps,
      fps,
      rtt_ms: rttMs,
      packet_loss_rate: Number(lossRate.toFixed(4)),
      e2e_latency_ms: e2eLatencyMs,
    }
  }

  /** 关闭拉流会话（DELETE WHEP 资源 + 关闭 PeerConnection，触发 SRS 停止向本订阅推送） */
  stop(): void {
    if (this.whepResourceUrl) {
      // fire-and-forget：释放服务端 WHEP 会话，失败不影响本地关闭
      void fetch(this.whepResourceUrl, { method: 'DELETE' }).catch(() => undefined)
      this.whepResourceUrl = null
    }
    try {
      this.pc.getSenders().forEach((sender) => this.pc.removeTrack(sender))
    } catch {
      // 忽略：recvonly 无发送轨道
    }
    this.pc.close()
  }
}
