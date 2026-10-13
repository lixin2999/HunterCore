"""可操控车辆视图模型（契约 components.schemas：ControllableVehicle* 系列）。

数据源为 Redis 读模型（vehicle:online:set + vehicle:status:{vehicle_id}），权威值属
vehicle-service，本服务只读（契约 ControllableVehicle.description；pending #2）。
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from app.schemas.common import (
    UUID_PATTERN,
    VEHICLE_ID_PATTERN,
    BlockReason,
    RemoteApiResponse,
    VehicleStatus,
)


class ActiveSessionBrief(BaseModel):
    """车辆活跃会话摘要（契约 ActiveSessionBrief；status=remote_controlled 时给出）。"""

    session_id: str = Field(pattern=UUID_PATTERN, description="活跃会话 ID")
    operator_id: str = Field(pattern=UUID_PATTERN, description="占用者（user_id）")
    operator_name: str | None = Field(default=None, description="占用者显示名（缺失为 null）")
    started_at: float = Field(description="会话开始时间（Unix epoch 秒）")


class ControllableVehicle(BaseModel):
    """可操控车辆条目（契约 ControllableVehicle，字段集合不可增减）。

    不可操控时 controllable=False 且 block_reason 指明原因：
    offline → 4001；upgrading/charging/fault/emergency → 4002；already_controlled → 7001。
    last_online_time 为契约必填：读模型缺失时以 0 兜底（时间戳量，前端按 `>0` 判「尚未上线」）。
    battery_soc / velocity **缺失即 null**（V1.19.21）：旧写法兜 0/0.0 把「车端没上报」
    说成「电量 0%」（与写侧 G-08/G-11「未上报不写字段」纪律冲突）；0 仍如实是 0。
    """

    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆 ID（= 证书 CN = Kafka key）")
    vehicle_name: str | None = Field(default=None, description="车辆别名（缺失为 null）")
    model: Literal["HUNTER_SE"] = Field(default="HUNTER_SE", description="车型（硬件基线固定）")
    status: VehicleStatus = Field(description="当前状态（Redis vehicle:status 读模型）")
    controllable: bool = Field(description="当前是否允许创建操控会话（false 时看 block_reason）")
    block_reason: BlockReason | None = Field(default=None, description="不可操控原因（可操控时为 null）")
    battery_soc: int | None = Field(
        default=None, ge=0, le=100, description="电量百分比（接管前提示）；null = 读模型无该字段（车端未上报/遥测未到达），不等于 0"
    )
    velocity: float | None = Field(
        default=None, ge=0, description="当前速度 m/s（远程操控上限 2.0 m/s）；null = 无该字段，不等于静止"
    )
    last_online_time: float = Field(default=0.0, description="最近在线时间（Unix epoch 秒）")
    active_session: ActiveSessionBrief | None = Field(
        default=None, description="当前占用的操控会话摘要（未被占用为 null）"
    )


class ControllableVehicleList(BaseModel):
    """车辆列表数据体（契约 ControllableVehicleList；source/generated_at 供前端展示实时视图）。"""

    items: list[ControllableVehicle] = Field(description="车辆条目（vehicle_id 升序）")
    total: int = Field(ge=0, description="Redis 读模型匹配数量（非 DB COUNT；最终一致）")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数（≤200）")
    source: Literal["redis_read_model"] = Field(
        default="redis_read_model", description="数据来源标注（便于前端展示「实时视图」）"
    )
    generated_at: float = Field(description="视图生成时间（Unix epoch 秒）")


class ControllableVehicleListResponse(RemoteApiResponse):
    """GET /vehicles 统一响应体（data: ControllableVehicleList）。"""

    data: ControllableVehicleList | None = None


__all__ = [
    "ActiveSessionBrief",
    "ControllableVehicle",
    "ControllableVehicleList",
    "ControllableVehicleListResponse",
]