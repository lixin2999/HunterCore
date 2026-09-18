"""操控历史与录像访问模型（契约 ControlHistory* / ControlVideoAccess + sidecar 写侧模型）。

sidecar JSON（契约 sidecar_schema）随录像一同归档到 hunter-video 桶，键名固定、
实现侧 Pydantic 模型与之 1:1（写入前经模型自校验）。
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.schemas.common import (
    SHA256_PATTERN,
    UUID_PATTERN,
    VEHICLE_ID_PATTERN,
    DegradedReason,
    EndReasonSource,
    RemoteApiResponse,
    SessionEndReason,
)
from app.schemas.sessions import VideoConfig

HISTORY_SOURCE: Literal["minio_sidecar"] = "minio_sidecar"
HISTORY_RETENTION_DAYS: Literal[90] = 90


# ---------- sidecar 写侧模型（契约 sidecar_schema 键名固定） ----------
class SidecarVideo(BaseModel):
    """录像归档信息（sidecar.video）。"""

    object_key: str = Field(description="录像对象键（.mp4）")
    size_bytes: int = Field(ge=0, description="录像文件大小（字节；未落盘为 0）")
    sha256: str = Field(default="", description="录像 SHA-256（完整性/取证；录像管线未回写时为空串）")
    container: Literal["mp4"] = Field(default="mp4", description="封装格式")
    codec: Literal["h264"] = Field(default="h264", description="编码格式")
    width: Literal[1280] = Field(default=1280, description="分辨率宽")
    height: Literal[720] = Field(default=720, description="分辨率高")
    fps: int = Field(ge=1, le=30, description="帧率")
    bitrate_kbps_avg: int = Field(ge=0, description="会话平均码率（kbps）")
    keyframe_interval_s: Literal[1] = Field(default=1, description="关键帧间隔（固定 1s）")
    e2e_latency_ms_p95: float = Field(ge=0, description="端到端时延 P95（ms；目标 ≤200ms）")


class SidecarControl(BaseModel):
    """控制链路信息（sidecar.control）。"""

    commands_sent: int = Field(ge=0, description="已下发指令帧数")
    commands_acked: int = Field(ge=0, description="车端已确认帧数")
    ack_latency_ms_avg: float = Field(ge=0, description="ACK 平均时延（ms）")
    ack_latency_ms_p95: float = Field(ge=0, description="ACK P95 时延（ms）")
    timeout_events: int = Field(ge=0, description="超时事件数")
    max_speed_mps: float = Field(gt=0, le=2.0, description="会话限速（≤2.0 m/s）")


class SidecarEnvironment(BaseModel):
    """归档环境（sidecar.environment）。"""

    service_version: str = Field(description="remote-control 服务版本")
    config_digest: str = Field(description="会话创建时服务配置摘要（SHA-256）")


class SidecarDegradedEvent(BaseModel):
    """降级事件（sidecar.degraded_events 元素；契约 DegradedEvent 键 {at, reason, detail}）。"""

    at: float = Field(description="事件发生时间（Unix epoch 秒）")
    reason: DegradedReason = Field(
        description="降级原因（command_timeout/video_latency/packet_loss/heartbeat_miss）"
    )
    detail: str = Field(description="事件详情（时延/丢包率等量化上下文）")


class SessionSidecar(BaseModel):
    """会话归档元信息（sidecar JSON 根对象；键名与契约 1:1，禁止增删）。"""

    schema_version: Literal["1"] = Field(default="1", description="sidecar 结构版本")
    session_id: str = Field(pattern=UUID_PATTERN, description="会话 ID")
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆 ID")
    operator_id: str = Field(pattern=UUID_PATTERN, description="操作员用户 ID")
    operator_name: str | None = Field(default=None, description="操作员显示名")
    started_at: float = Field(description="会话开始时间（Unix epoch 秒）")
    ended_at: float = Field(description="会话结束时间（Unix epoch 秒）")
    duration_s: int = Field(ge=0, description="持续时长（秒）")
    end_reason: SessionEndReason = Field(description="结束原因")
    end_reason_source: EndReasonSource = Field(description="结束触发方")
    degraded_events: list[SidecarDegradedEvent] = Field(default_factory=list, description="降级事件")
    video: SidecarVideo = Field(description="录像归档信息")
    control: SidecarControl = Field(description="控制链路信息")
    note: str | None = Field(default=None, description="备注（如人工核查提示）")
    environment: SidecarEnvironment = Field(description="归档环境")


# ---------- 历史查询/响应体（读侧投影） ----------
class ControlHistoryItem(BaseModel):
    """操控记录摘要（契约 ControlHistoryItem = sidecar 的列表投影，字段即投影键）。"""

    session_id: str = Field(pattern=UUID_PATTERN, description="会话 ID")
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆 ID")
    operator_id: str = Field(pattern=UUID_PATTERN, description="操作员用户 ID")
    operator_name: str | None = Field(default=None, description="操作员显示名（user-service 读模型）")
    started_at: float = Field(description="开始时间（Unix epoch 秒）")
    ended_at: float = Field(description="结束时间（Unix epoch 秒）")
    duration_s: int = Field(ge=0, description="会话时长（秒）")
    end_reason: SessionEndReason = Field(description="结束原因")
    end_reason_source: EndReasonSource | None = Field(default=None, description="结束触发方")
    commands_sent: int | None = Field(default=None, ge=0, description="指令总数（含心跳帧）")
    commands_acked: int | None = Field(default=None, ge=0, description="回执总数")
    ack_latency_ms_avg: float | None = Field(default=None, ge=0, description="回执延迟均值（ms）")
    ack_latency_ms_p95: float | None = Field(
        default=None, ge=0, description="回执延迟 P95（ms；目标 ≤100ms）"
    )
    timeout_events: int | None = Field(default=None, ge=0, description="超时保护触发次数（>500ms 无回执）")
    video_e2e_latency_ms_p95: float | None = Field(
        default=None, ge=0, description="视频端到端延迟 P95（ms；目标 ≤200ms）"
    )
    video_config: VideoConfig | None = Field(default=None, description="录像配置")
    video_object_key: str = Field(description="录像对象键（hunter-video）")
    video_size_bytes: int | None = Field(default=None, ge=0, description="录像文件大小（字节）")
    video_sha256: str | None = Field(
        default=None, pattern=SHA256_PATTERN, description="录像 SHA-256（完整性/取证）"
    )
    sidecar_object_key: str = Field(description="归档元信息对象键（同名 .json）")
    note: str | None = Field(default=None, description="操作员创建会话时的备注")


class ControlHistoryList(BaseModel):
    """历史列表数据体（契约 ControlHistoryList）。"""

    items: list[ControlHistoryItem] = Field(description="历史条目（started_at 降序）")
    total: int = Field(ge=0, description="列举并解析出的 sidecar 数量（非 DB COUNT）")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数（≤200）")
    source: Literal["minio_sidecar"] = Field(default="minio_sidecar", description="数据来源标注")
    retention_days: Literal[90] = Field(default=90, description="录像保留期（超出即被生命周期回收）")


class ControlHistoryDetail(ControlHistoryItem):
    """历史详情数据体（契约 ControlHistoryDetail = ControlHistoryItem 平铺 + sidecar 原文）。"""

    sidecar: dict[str, Any] = Field(description="原始 sidecar JSON（键集见契约 sidecar_schema）")
    video_available: bool = Field(description="录像对象当前是否可下载（false = 已过 90 天或未封存）")


class ControlVideoAccess(BaseModel):
    """录像访问信息（契约 ControlVideoAccess；预签名 GET 900s，支持 Range 分片下载）。"""

    video_url: str = Field(description="预签名下载 URL（15 分钟有效）")
    expires_in: Literal[900] = Field(default=900, description="预签名有效期（秒，固定 900）")
    content_type: Literal["video/mp4"] = Field(default="video/mp4", description="MIME 类型")
    range_supported: Literal[True] = Field(default=True, description="支持 Range 分片下载")
    size_bytes: int = Field(ge=0, description="录像大小（字节）")
    duration_s: int | None = Field(default=None, description="录像时长（秒；未知为 null）")
    etag: str | None = Field(default=None, description="对象 ETag（断点续传一致性校验）")


# ---------- 统一响应体 ----------
class ControlHistoryListResponse(RemoteApiResponse):
    """历史列表统一响应（data: ControlHistoryList）。"""

    data: ControlHistoryList | None = None


class ControlHistoryDetailResponse(RemoteApiResponse):
    """历史详情统一响应（data: ControlHistoryDetail）。"""

    data: ControlHistoryDetail | None = None


class ControlVideoAccessResponse(RemoteApiResponse):
    """录像访问统一响应（data: ControlVideoAccess）。"""

    data: ControlVideoAccess | None = None


__all__ = [
    "HISTORY_RETENTION_DAYS",
    "HISTORY_SOURCE",
    "ControlHistoryDetail",
    "ControlHistoryDetailResponse",
    "ControlHistoryItem",
    "ControlHistoryList",
    "ControlHistoryListResponse",
    "ControlVideoAccess",
    "ControlVideoAccessResponse",
    "SessionSidecar",
    "SidecarControl",
    "SidecarDegradedEvent",
    "SidecarEnvironment",
    "SidecarVideo",
]
