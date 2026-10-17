"""事件查询/确认模型（契约 components.schemas：EventItem → EventListResponse）。

一一对应 data_collector.events 列；data_file_download_url 为服务端即时签发的
15 分钟预签名 URL（不落库）。事件确认使用服务端身份（X-User-Id），
禁止请求体指定确认人，且幂等（x-hunter-event-contract.acknowledge）。
"""
from __future__ import annotations

import time

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
    timestamp: int = Field(default_factory=lambda: int(time.time()))


class EventResponse(BaseModel):
    """事件详情 / 确认结果统一响应。"""

    code: int = 0
    message: str = "success"
    data: EventItem | None = None
    request_id: str
    timestamp: int = Field(default_factory=lambda: int(time.time()))


class EventStatsData(BaseModel):
    """事件精确聚合（窗口内 GROUP BY；供看板 Top N / 未确认分级直接使用）。

    V1.21.16：看板此前的 by_type 是「最近 200 条近似采样」，与事件列表精确值不一致，
    现在改由本端点提供精确计数（SQL GROUP BY），前端/看板无需再做近似。
    """

    model_config = ConfigDict(extra="forbid")

    total: int = Field(ge=0, description="窗口内事件总数（精确计数）")
    unacknowledged: int = Field(ge=0, description="窗口内未确认事件数（精确计数）")
    unacknowledged_by_level: dict[str, int] = Field(
        default_factory=dict,
        description="未确认事件按等级（info/warning/critical）精确计数；三级之和 = unacknowledged",
    )
    by_type: dict[str, int] = Field(
        default_factory=dict, description="按事件类型精确计数（19 种受控词表）"
    )


class EventStatsResponse(BaseModel):
    """GET /events/stats 统一响应。"""

    code: int = 0
    message: str = "success"
    data: EventStatsData | None = None
    request_id: str
    timestamp: int = Field(default_factory=lambda: int(time.time()))


__all__ = [
    "VEHICLE_ID_PATTERN",
    "EventItem",
    "EventLevel",
    "EventListData",
    "EventListResponse",
    "EventResponse",
    "EventStatsData",
    "EventStatsResponse",
    "EventType",
]
