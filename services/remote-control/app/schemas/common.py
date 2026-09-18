"""remote-control 共享常量、受控枚举与统一响应基类（契约 components.schemas）。

- 枚举取值域严格对齐 contracts/openapi/remote-control.yaml（禁止新增取值）；
- pattern 常量与契约 VehicleId / session_id（uuid）/ sha256 定义逐字对齐。
"""
from __future__ import annotations

import time
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

# ---------- 契约 pattern 常量 ----------
VEHICLE_ID_PATTERN = r"^HUNTER-[0-9]{3}$"  # 契约 VehicleId（= 证书 CN = Kafka key）
UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"  # 契约 session_id/operator_id
SHA256_PATTERN = r"^[0-9a-f]{64}$"  # 契约 sha256（小写十六进制）

# ---------- 统一分页（与 ota-service / hunter_common 约定一致） ----------
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200


class VehicleStatus(StrEnum):
    """车辆状态（契约 VehicleStatus = 系统状态定义 8 态，禁止新增）。"""

    OFFLINE = "offline"
    ONLINE_IDLE = "online_idle"
    AUTO_DRIVING = "auto_driving"
    REMOTE_CONTROLLED = "remote_controlled"
    UPGRADING = "upgrading"
    CHARGING = "charging"
    FAULT = "fault"
    EMERGENCY = "emergency"


class BlockReason(StrEnum):
    """不可操控原因（契约 BlockReason；映射 4001/4002/7001，见 services.vehicle_view）。"""

    OFFLINE = "offline"
    UPGRADING = "upgrading"
    CHARGING = "charging"
    FAULT = "fault"
    EMERGENCY = "emergency"
    ALREADY_CONTROLLED = "already_controlled"


class SessionStatus(StrEnum):
    """会话状态（契约 SessionStatus；connecting/active/degraded 为活跃态，ended 为终态）。"""

    CONNECTING = "connecting"
    ACTIVE = "active"
    DEGRADED = "degraded"
    ENDED = "ended"


class SessionEndReason(StrEnum):
    """会话结束原因（契约 SessionEndReason）。"""

    OPERATOR_END = "operator_end"
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"
    COMMAND_TIMEOUT = "command_timeout"
    VIDEO_LOST = "video_lost"
    VEHICLE_OFFLINE = "vehicle_offline"
    VEHICLE_FAULT = "vehicle_fault"
    ADMIN_TERMINATE = "admin_terminate"
    SERVER_SHUTDOWN = "server_shutdown"


class EndReasonSource(StrEnum):
    """结束触发方（契约 EndReasonSource：平台/车端/超时）。"""

    PLATFORM = "platform"
    VEHICLE = "vehicle"
    TIMEOUT = "timeout"


class DegradedReason(StrEnum):
    """链路降级原因（契约 RemoteSessionDetail.degraded_reasons 内联 enum，取值不可增减）。"""

    COMMAND_TIMEOUT = "command_timeout"
    VIDEO_LATENCY = "video_latency"
    PACKET_LOSS = "packet_loss"
    HEARTBEAT_MISS = "heartbeat_miss"


class RemoteApiResponse(BaseModel):
    """统一响应基类（契约 ApiResponse 五字段，字段名不可更改）。

    code=0 成功；非 0 使用预定义错误码（1001-7002，见 core.error_handlers 映射）；
    request_id/timestamp 缺省自动填充（路由层显式传入链路 trace_id）。
    """

    code: int = 0
    message: str = "success"
    data: object | None = None
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: int = Field(default_factory=lambda: int(time.time()))


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "SHA256_PATTERN",
    "UUID_PATTERN",
    "VEHICLE_ID_PATTERN",
    "BlockReason",
    "DegradedReason",
    "EndReasonSource",
    "RemoteApiResponse",
    "SessionEndReason",
    "SessionStatus",
    "VehicleStatus",
    "time",
    "uuid4",
]