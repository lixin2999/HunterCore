"""操控会话模型（契约 components.schemas：VideoConfig / WebRtc / RemoteSession* 系列）。

字段名、类型、必填项与 contracts/openapi/remote-control.yaml 逐字对齐；
VideoConfig / ControlChannelInfo 的枚举字面量（1280/720/h264/20Hz/50ms/500ms）
为契约固定值，不可放宽。
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from app.schemas.common import (
    UUID_PATTERN,
    VEHICLE_ID_PATTERN,
    DegradedReason,
    RemoteApiResponse,
    SessionEndReason,
    SessionStatus,
)


# ---------- 视频链路 ----------
class VideoConfig(BaseModel):
    """协商后的视频配置（契约 VideoConfig；枚举字段 = 系统约束第 15 条固定值）。"""

    width: Literal[1280] = 1280
    height: Literal[720] = 720
    codec: Literal["h264"] = "h264"
    fps: int = Field(le=30, description="帧率（上限 30，不可提高）")
    min_bitrate_kbps: int = Field(description="码率下限（kbps；来自 RC_VIDEO_MIN_BITRATE_KBPS）")
    max_bitrate_kbps: int = Field(description="码率上限（kbps；来自 RC_VIDEO_MAX_BITRATE_KBPS）")
    keyframe_interval_s: Literal[1] = Field(default=1, description="关键帧间隔（固定 1s）")


class VideoPreference(BaseModel):
    """操作员视频偏好（创建会话请求；越界 → 422/2001，data 携带 field/min/max/actual）。"""

    fps: int | None = Field(default=None, ge=1, le=30, description="期望帧率（1-30）")
    target_bitrate_kbps: int | None = Field(
        default=None, ge=2048, le=4096, description="期望目标码率（2048-4096 kbps）"
    )


class ControlChannelInfo(BaseModel):
    """控制通道参数（契约 ControlChannelInfo；20Hz/50ms/500ms 为固定约束）。"""

    hz: Literal[20] = Field(default=20, description="指令频率（固定 20Hz）")
    interval_ms: Literal[50] = Field(default=50, description="指令间隔（固定 50ms）")
    ack_timeout_ms: int = Field(le=100, description="指令端到端延迟指标上限（≤100ms）")
    stop_on_timeout_ms: Literal[500] = Field(default=500, description="超时停车阈值（固定 500ms）")
    max_speed_mps: float = Field(le=2.0, description="远程操控限速（≤2.0 m/s；RC_MAX_SPEED_MPS 不可上调）")


class SessionHeartbeat(BaseModel):
    """会话心跳策略（契约 SessionHeartbeat；interval 10s 不可放宽）。"""

    interval_s: Literal[10] = Field(default=10, description="车端心跳周期（固定 10s）")
    degraded_after_misses: int = Field(ge=1, description="连续丢失 N 次判定降级")
    end_after_s: int = Field(ge=1, description="无心跳持续 N 秒平台侧结束会话")


# ---------- WebRTC 接入 ----------
class IceServer(BaseModel):
    """ICE 服务器（契约 IceServer；TURN 凭据为短期 REST API 凭据）。"""

    urls: list[str] = Field(min_length=1, description="stun:/turn: 地址列表")
    username: str | None = Field(default=None, description="短期 TURN 凭证用户名（禁止写入日志/审计）")
    credential: str | None = Field(default=None, description="短期 TURN 凭证（禁止写入日志/审计）")
    credential_ttl_s: int | None = Field(
        default=None, ge=1, description="凭证有效期（秒），应覆盖会话时长"
    )


class SrsMediaInfo(BaseModel):
    """SRS 媒体服务器信息（契约 SrsMediaInfo；pending #10：流命名规则待定稿）。"""

    app: str = Field(description="SRS 应用名")
    stream: str = Field(description="SRS 流名")
    whip_url: str | None = Field(default=None, description="车端推流入口（WHIP）")
    whep_url: str | None = Field(default=None, description="浏览器拉流入口（WHEP）")
    rtmp_url: str | None = Field(default=None, description="车端 RTMP 推流地址")


class WebRtcConnectionInfo(BaseModel):
    """WebRTC 接入上下文（契约 WebRtcConnectionInfo）。"""

    signal_ws_url: str = Field(description="WebRTC 信令通道（WSS；JWT 经握手 Authorization 头携带）")
    control_ws_url: str = Field(description="控制指令通道（WSS；20Hz 指令 + 回执 + 10s 心跳）")
    publisher: Literal["vehicle"] | None = Field(default=None, description="车端为唯一发布方")
    subscriber: Literal["browser"] | None = Field(default=None, description="浏览器为订阅方（不得推流）")
    transport: Literal["srtp"] = Field(default="srtp", description="媒体传输（SRTP 加密）")
    ice_servers: list[IceServer] = Field(min_length=1, description="ICE 服务器列表")
    media_server: SrsMediaInfo = Field(description="SRS 媒体服务器")


class RecordArchiveInfo(BaseModel):
    """录像归档对象（契约 RecordArchiveInfo；hunter-video 桶 + 同名 sidecar JSON）。"""

    object_key: str = Field(description="录像对象键（remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4）")
    sidecar_object_key: str = Field(description="归档元信息对象键（同目录同名 .json）")
    bucket: Literal["hunter-video"] = Field(default="hunter-video", description="录像 Bucket（不可更改）")
    retention_days: Literal[90] = Field(default=90, description="录像保留 90 天（生命周期自动清理）")
    container: Literal["mp4"] = Field(default="mp4", description="封装格式（支持 HTTP Range seek）")
    codec: Literal["h264"] = Field(default="h264", description="编码格式（与直播一致）")


# ---------- 会话统计 ----------
class ControlStats(BaseModel):
    """控制链路统计（契约 ControlStats；多副本下由 rc:session Hash 汇总，秒级延迟）。"""

    commands_sent: int = Field(ge=0, description="已下发指令数（含心跳帧）")
    commands_acked: int = Field(ge=0, description="收到 command_result 回执数（幂等键 command_id）")
    ack_latency_ms_avg: float | None = Field(default=None, ge=0, description="回执延迟均值（ms；目标 ≤100ms）")
    ack_latency_ms_p95: float | None = Field(default=None, ge=0, description="回执延迟 P95（ms）")
    timeout_events: int = Field(ge=0, description=">500ms 未收到回执的次数（超时保护触发计数）")
    last_seq: int = Field(ge=0, description="最近一次指令序号（车端据此判重/丢包）")
    last_ack_at: float | None = Field(default=None, description="最近回执时间（Unix epoch 秒）")


class VideoStats(BaseModel):
    """视频链路统计（契约 VideoStats；端到端延迟 ≤200ms 性能指标守护）。"""

    bitrate_kbps: int = Field(ge=0, description="当前码率（kbps；稳态区间 2048-4096）")
    fps: float = Field(ge=0, description="当前实际帧率（目标 30）")
    rtt_ms: float = Field(ge=0, description="网络往返时延（ms）")
    packet_loss_rate: float = Field(ge=0, le=1, description="丢包率（0-1）")
    frames_dropped: int | None = Field(default=None, ge=0, description="累计丢帧数")
    e2e_latency_ms: float | None = Field(
        default=None, ge=0, description="端到端延迟估算（ms；目标 ≤200ms，超阈置会话 degraded）"
    )


# ---------- 会话请求/响应体 ----------
class CreateRemoteSessionRequest(BaseModel):
    """POST /session 请求体（契约 CreateRemoteSessionRequest）。"""

    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="目标车辆 ID")
    video: VideoPreference = Field(default_factory=VideoPreference, description="视频偏好（可选）")
    note: str | None = Field(
        default=None,
        max_length=200,
        description="操作备注（写入归档 sidecar 与审计日志；禁止写入敏感信息）",
    )


class RemoteSessionInfo(BaseModel):
    """操控会话上下文（契约 RemoteSessionInfo；创建成功响应）。"""

    session_id: str = Field(pattern=UUID_PATTERN, description="会话唯一 ID")
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆 ID")
    operator_id: str = Field(pattern=UUID_PATTERN, description="操作员用户 ID")
    operator_name: str | None = Field(default=None, description="操作员显示名（user-service 读模型）")
    started_at: float = Field(description="会话开始时间（Unix epoch 秒）")
    status: SessionStatus = Field(description="会话状态（创建后为 connecting）")
    video: VideoConfig = Field(description="协商后视频配置")
    control_channel: ControlChannelInfo = Field(description="控制通道参数")
    heartbeat: SessionHeartbeat = Field(description="心跳策略")
    webrtc: WebRtcConnectionInfo = Field(description="WebRTC 接入上下文")
    record: RecordArchiveInfo = Field(description="录像归档对象")


class RemoteSessionDetail(RemoteSessionInfo):
    """会话详情（契约 RemoteSessionDetail = RemoteSessionInfo + 实时统计）。"""

    last_heartbeat_at: float | None = Field(default=None, description="最近心跳时间（Unix epoch 秒）")
    control_stats: ControlStats | None = Field(default=None, description="控制链路统计")
    video_stats: VideoStats | None = Field(default=None, description="视频链路统计")
    degraded: bool = Field(default=False, description="链路是否降级")
    degraded_reasons: list[DegradedReason] = Field(default_factory=list, description="降级原因列表")


class RemoteSessionResult(BaseModel):
    """会话结束结果（契约 RemoteSessionResult：RemoteSessionInfo 全量快照 + 终态字段）。"""

    session_id: str = Field(pattern=UUID_PATTERN)
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN)
    operator_id: str = Field(pattern=UUID_PATTERN)
    operator_name: str | None = Field(default=None, description="操作员显示名")
    status: Literal[SessionStatus.ENDED] = Field(default=SessionStatus.ENDED, description="固定为 ended")
    end_reason: SessionEndReason = Field(description="结束原因")
    started_at: float = Field(description="开始时间（Unix epoch 秒）")
    ended_at: float = Field(description="结束时间（Unix epoch 秒）")
    duration_s: int = Field(ge=0, description="持续时长（秒）")
    control_stats: ControlStats | None = Field(default=None, description="控制链路统计（快照）")
    video_stats: VideoStats | None = Field(default=None, description="视频链路统计（快照）")
    record: RecordArchiveInfo = Field(description="录像归档对象（sidecar 未写入也返回计划位置）")
    sidecar_written: bool | None = Field(
        default=None, description="sidecar JSON 是否写入成功（false → 需人工核查录像与元信息）"
    )


class RemoteSessionList(BaseModel):
    """活跃会话列表数据体（契约 RemoteSessionList）。"""

    items: list[RemoteSessionInfo] = Field(description="会话条目（started_at 降序）")
    total: int = Field(ge=0, description="过滤后总数")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数（≤200）")


# ---------- 统一响应体 ----------
class RemoteSessionInfoResponse(RemoteApiResponse):
    """创建/查询会话统一响应（data: RemoteSessionInfo）。"""

    data: RemoteSessionInfo | None = None


class RemoteSessionDetailResponse(RemoteApiResponse):
    """会话详情统一响应（data: RemoteSessionDetail）。"""

    data: RemoteSessionDetail | None = None


class RemoteSessionResultResponse(RemoteApiResponse):
    """结束会话统一响应（data: RemoteSessionResult）。"""

    data: RemoteSessionResult | None = None


class RemoteSessionListResponse(RemoteApiResponse):
    """会话列表统一响应（data: RemoteSessionList）。"""

    data: RemoteSessionList | None = None


__all__ = [
    "ControlChannelInfo",
    "ControlStats",
    "CreateRemoteSessionRequest",
    "IceServer",
    "RecordArchiveInfo",
    "RemoteSessionDetail",
    "RemoteSessionDetailResponse",
    "RemoteSessionInfo",
    "RemoteSessionInfoResponse",
    "RemoteSessionList",
    "RemoteSessionListResponse",
    "RemoteSessionResult",
    "RemoteSessionResultResponse",
    "SessionHeartbeat",
    "SrsMediaInfo",
    "VideoConfig",
    "VideoPreference",
    "VideoStats",
    "WebRtcConnectionInfo",
]
