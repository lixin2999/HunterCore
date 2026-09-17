/**
 * 原生 WebRTC 客户端（浏览器侧 recvonly 拉流 + 信令中继）
 *
 * 契约依据（remote-control.yaml x-hunter-control-channel）：
 * - 媒体面：WebRTC / SRTP（DTLS-SRTP 加密），不经 api-gateway；
 * - 信令面：WSS `/ws/remote/{session_id}/signal` 中继 SDP/ICE（帧格式见 types/remote.ts WsSignalFrame）；
 * - ICE：ICE 服务器与 TURN 短期凭据由会话响应 webrtc.ice_servers 下发（禁止前端硬编码 STUN/TURN）；
 * - 视频：H.264 720p@30fps，2–4 Mbps，关键帧 1s（会话响应 video 字段），浏览器只能请求降档；
 * - 延迟预算：采集编码 50 + 网络 100 + 解码渲染 30 = 200ms，前端以 getStats() 估测并上报。
 * 禁止使用任何第三方云视频服务（契约 ui 约束）。
 */
import type { IceServer, RemoteSessionInfo, VideoStats, WsIceFrame, WsSignalFrame, WsSdpFrame } from '@/types/remote'
import { ReconnectingSocket, buildWsUrl } from '@/utils/websocket'

export interface WebRtcClientOptions {
  /** 会话信息（提供 ice_servers / signal_ws_url / video 配置） */
  session: RemoteSessionInfo
  /** WS 握手子协议（默认 hunter-jwt，见 constants WS_JWT_SUBPROTOCOL） */
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

/**
 * 远程操控视频客户端
 * 用法：new WebRtcClient(options) → await start(videoEl) → stats() → stop()
 */
export class WebRtcClient {
  private readonly options: WebRtcClientOptions
  private readonly pc: RTCPeerConnection
  private signal: ReconnectingSocket | null = null
  /** 远端在收到 offer 前可能先发 ICE，需缓存后统一 addIceCandidate */
  private pendingCandidates: RTCIceCandidateInit[] = []
  private remoteDescriptionSet = false
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
    // 仅接收（recvonly）：视频轨道由车端/SRS 推送
    this.pc.addTransceiver('video', { direction: 'recvonly' })
    this.pc.addTransceiver('audio', { direction: 'recvonly' })
  }

  /** 建立信令连接并完成 SDP/ICE 交换，远端轨道绑定到 video 元素 */
  async start(videoElement: HTMLVideoElement): Promise<void> {
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

    this.pc.onicecandidate = (event: RTCPeerConnectionIceEvent) => {
      if (!event.candidate) {
        return
      }
      const frame: WsIceFrame = {
        type: 'ice',
        candidate: event.candidate.candidate,
        sdpMid: event.candidate.sdpMid,
        sdpMLineIndex: event.candidate.sdpMLineIndex,
      }
      this.signal?.send(frame)
    }

    const signalUrl = this.resolveSignalUrl()
    this.signal = new ReconnectingSocket({
      url: signalUrl,
      protocols: this.options.protocols,
      onMessage: (payload) => {
        void this.handleSignalFrame(payload as WsSignalFrame)
      },
      onError: () => {
        this.options.onError?.('信令连接异常，正在重试')
      },
    })
    this.signal.connect()

    const offer = await this.pc.createOffer({ offerToReceiveVideo: true, offerToReceiveAudio: true })
    await this.pc.setLocalDescription(offer)
    const frame: WsSdpFrame = {
      type: 'sdp',
      sdp_type: 'offer',
      sdp: offer.sdp ?? '',
    }
    // 信令通道可能尚未 open：等待 open 后重发一次（20Hz 控制通道不受影响）
    const resend = window.setInterval(() => {
      if (this.signal?.send(frame)) {
        window.clearInterval(resend)
      }
    }, 300)
    window.setTimeout(() => window.clearInterval(resend), 10_000)
  }

  /** 处理信令下行帧（SDP answer / 远端 ICE） */
  private async handleSignalFrame(frame: WsSignalFrame): Promise<void> {
    try {
      if (frame.type === 'sdp' && frame.sdp_type === 'answer') {
        await this.pc.setRemoteDescription({ type: 'answer', sdp: frame.sdp })
        this.remoteDescriptionSet = true
        for (const candidate of this.pendingCandidates) {
          await this.pc.addIceCandidate(candidate)
        }
        this.pendingCandidates = []
        return
      }
      if (frame.type === 'ice') {
        const candidate: RTCIceCandidateInit = {
          candidate: frame.candidate,
          sdpMid: frame.sdpMid,
          sdpMLineIndex: frame.sdpMLineIndex,
        }
        if (!this.remoteDescriptionSet) {
          this.pendingCandidates.push(candidate)
          return
        }
        await this.pc.addIceCandidate(candidate)
      }
    } catch (error) {
      this.options.onError?.(`信令处理失败：${error instanceof Error ? error.message : '未知错误'}`)
    }
  }

  /** 信令 WS 地址（会话响应 webrtc.signal_ws_url 优先；否则按网关 WS 前缀拼装） */
  private resolveSignalUrl(): string {
    const fromSession = this.options.session.webrtc.signal_ws_url
    if (fromSession.startsWith('ws')) {
      return fromSession
    }
    return buildWsUrl(
      fromSession.startsWith('/') ? fromSession : `/remote/${this.options.session.session_id}/signal`,
    )
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

  /** 关闭信令与 PeerConnection（会话结束时必须调用以触发车端停止推流） */
  stop(): void {
    this.signal?.dispose()
    this.signal = null
    try {
      this.pc.getSenders().forEach((sender) => this.pc.removeTrack(sender))
    } catch {
      // 忽略：无发送轨道时无需移除
    }
    this.pc.close()
  }
}
