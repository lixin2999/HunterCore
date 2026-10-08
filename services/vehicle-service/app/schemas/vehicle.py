"""vehicle-service Pydantic 请求/响应模型（对齐 contracts/openapi/vehicle-service.yaml）。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ProvisionStepName = Literal["db", "scram", "topics", "cert"]
ProvisionStepState = Literal["pending", "in_progress", "ok", "failed", "skipped"]
ProvisionState = Literal["pending", "in_progress", "ready", "failed"]
VehicleStatusLiteral = Literal[
    "offline",
    "online_idle",
    "auto_driving",
    "remote_controlled",
    "upgrading",
    "charging",
    "fault",
    "emergency",
]


class ProvisionStep(BaseModel):
    """provisioning 4 步的单步状态（写入 vehicle_svc.vehicles.provision_status.steps）。"""

    name: ProvisionStepName
    state: ProvisionStepState
    error: str | None = Field(default=None, max_length=500)
    ts: int | None = Field(default=None, description="步骤完成 Unix 秒")


# ---------- 请求 ----------
class VehicleCreateRequest(BaseModel):
    """一键开通请求（POST /api/v1/vehicle）。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_id: str = Field(
        ...,
        pattern=r"^[A-Za-z0-9_-]{1,32}$",
        description="车辆唯一标识（= SCRAM username = 证书 CN = Topic {vehicle_id}）",
    )
    vehicle_name: str = Field(..., max_length=64)
    model: str = Field(default="HUNTER_SE", max_length=32)
    firmware_version: str | None = Field(default=None, max_length=32)
    software_version: str | None = Field(default=None, max_length=32)
    fence_json: dict[str, Any] | None = None
    description: str | None = Field(default=None, max_length=500)
    takeover_existing: bool = Field(
        default=False,
        description="若 SCRAM/Topic/证书已存在（历史遗留），是否接管复用（跳过创建）",
    )


class VehicleUpdateRequest(BaseModel):
    """修改台账基础字段（PATCH）；不含 provisioning 资源相关字段。"""

    model_config = ConfigDict(extra="forbid")

    vehicle_name: str | None = Field(default=None, max_length=64)
    model: str | None = Field(default=None, max_length=32)
    firmware_version: str | None = Field(default=None, max_length=32)
    software_version: str | None = Field(default=None, max_length=32)
    fence_json: dict[str, Any] | None = None
    description: str | None = Field(default=None, max_length=500)


# ---------- 响应 ----------
class VehicleRow(BaseModel):
    """列表行。"""

    vehicle_id: str
    vehicle_name: str
    model: str
    firmware_version: str | None = None
    software_version: str | None = None
    status: VehicleStatusLiteral
    register_time: datetime
    last_online_time: datetime | None = None
    device_cert_sn: str | None = None
    provision_state: ProvisionState
    provision_steps: list[ProvisionStep] = Field(default_factory=list)


class VehicleDetail(VehicleRow):
    """详情：含接入配置摘要与 8 个 Topic 预览。"""

    fence_json: dict[str, Any] | None = None
    description: str | None = None
    kafka_bootstrap: str = Field(
        ..., description="车端接入 bootstrap（SERVER_IP:9093）"
    )
    scram_username: str = Field(..., description="车端 SCRAM 用户名（= vehicle_id）")
    topics_preview: list[str] = Field(default_factory=list)


class ProvisionResult(BaseModel):
    """一键开通成功响应。"""

    vehicle: VehicleDetail
    scram_password: str = Field(
        ...,
        description="一次性 SCRAM 口令（24 字节 url-safe）；不落 DB/日志；丢失需重新轮换",
    )
    bundle_download_url: str


class RotateScramResult(BaseModel):
    vehicle_id: str
    scram_password: str


class ReissueCertResult(BaseModel):
    vehicle_id: str
    device_cert_sn: str = Field(..., description="新证书序列号（大写十六进制）")
    issued_at: datetime


class VehiclePage(BaseModel):
    items: list[VehicleRow]
    total: int
    page: int
    page_size: int


__all__ = [
    "ProvisionResult",
    "ProvisionState",
    "ProvisionStep",
    "ProvisionStepName",
    "ProvisionStepState",
    "ReissueCertResult",
    "RotateScramResult",
    "VehicleCreateRequest",
    "VehicleDetail",
    "VehiclePage",
    "VehicleRow",
    "VehicleUpdateRequest",
]
