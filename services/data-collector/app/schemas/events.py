"""事件查询/确认模型（契约 components.schemas：EventItem → EventListResponse）。

一一对应 data_collector.events 列；data_file_download_url 为服务端即时签发的
15 分钟预签名 URL（不落库）。事件确认使用服务端身份（X-User-Id），
禁止请求体指定确认人，且幂等（x-hunter-event-contract.acknowledge）。
"""
from __future__ import annotations

from hunter_common.database.enums import EventLevel, EventType
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import VEHICLE_ID_PATTERN


class EventItem(BaseModel):
    """事件记录（一一对应 data_collector.events 列）。"""

    model_config = ConfigDict(extra="forbid")

    event_id: int = Field(description="events.event_id（BIGSERIAL）")
    vehicle_id: str = Field(description="events.vehicle_id（逻辑外键 → vehicle_svc.vehicles）")
    event_type: EventType = Field(description="事件类型（19 种受控词表）")
    event_level: EventLevel = Field(description="事件等级（由事件类型决定）")
    event_time: float = Field(
        description="事件发生时间（Unix epoch 秒，车端时间；非入库时间）"
    )
    description: str | None = Field(default=None, description="事件描述")
    data_json: dict[str, object] = Field(
        default_factory=dict, description="事件上下文 JSONB（阈值、实测值、轨迹片段摘要等）"
    )
    data_file_url: str | None = Field(
        default=None, description="关联文件对象地址（MinIO bucket/key）"
    )
    data_file_download_url: str | None = Field(
        default=None,
        description="关联文件下载地址（服务端即时签发的 15 分钟预签名 URL，不落库；无关联文件为 null）",
    )
    acknowledged: bool = Field(description="是否已确认")
    acknowledged_by: str | None = Field(default=None, description="确认人（users.user_id UUID）")
    acknowledge_time: float | None = Field(
        default=None, description="确认时间（Unix epoch 秒）；未确认为 null"
    )


class EventListData(BaseModel):
    """事件列表数据体（按 event_time 倒序为默认）。"""

    model_config = ConfigDict(extra="forbid")

    items: list[EventItem] = Field(default_factory=list, description="事件列表")
    total: int = Field(ge=0, description="筛选条件下事件总数")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数")


class EventListResponse(BaseModel):
    """GET /events 统一响应。"""

    code: int = 0
    message: str = "success"
    data: EventListData | None = None
    request_id: str
    timestamp: int


class EventResponse(BaseModel):
    """事件详情 / 确认结果统一响应。"""

    code: int = 0
    message: str = "success"
    data: EventItem | None = None
    request_id: str
    timestamp: int


__all__ = [
    "VEHICLE_ID_PATTERN",
    "EventItem",
    "EventLevel",
    "EventListData",
    "EventListResponse",
    "EventResponse",
    "EventType",
]
