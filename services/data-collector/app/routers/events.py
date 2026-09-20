"""/api/v1/data/events 路由（事件列表 / 详情 / 确认）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request
from hunter_common.database.enums import EventLevel, EventType
from hunter_common.database.repository import MAX_PAGE_SIZE

from app.core.dependencies import (
    require_execute_permission,
    require_read_permission,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.events import EventListResponse, EventResponse
from app.services.events import EventService

router = APIRouter(prefix="/api/v1/data", tags=["events"])


def get_event_service(request: Request) -> EventService:
    """从 app.state 获取事件服务（lifespan 装配，测试可替换）。"""
    return request.app.state.event_service  # type: ignore[no-any-return]


@router.get(
    "/events",
    response_model=EventListResponse,
    summary="查询事件列表",
)
async def list_events(
    service: Annotated[EventService, Depends(get_event_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    vehicle_id: Annotated[str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（可选）")] = None,
    event_type: Annotated[EventType | None, Query(description="事件类型（19 种受控词表）")] = None,
    event_level: Annotated[EventLevel | None, Query(description="事件等级")] = None,
    acknowledged: Annotated[bool | None, Query(description="确认状态过滤（可选）")] = None,
    start_time: Annotated[float | None, Query(ge=0, description="时间区间起点（成对出现，可选）")] = None,
    end_time: Annotated[float | None, Query(ge=0, description="时间区间终点（成对出现，可选）")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = 20,
) -> EventListResponse:
    """事件列表（event_time DESC；total 为筛选条件下总数）。"""
    data = await service.list_events(
        vehicle_id=vehicle_id,
        event_type=event_type,
        event_level=event_level,
        acknowledged=acknowledged,
        start_time=start_time,
        end_time=end_time,
        page=page,
        page_size=page_size,
    )
    return EventListResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.get(
    "/events/{event_id}",
    response_model=EventResponse,
    summary="查询事件详情",
)
async def get_event(
    event_id: Annotated[int, Path(ge=1, description="事件 ID（events.event_id）")],
    service: Annotated[EventService, Depends(get_event_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> EventResponse:
    """事件详情（3001：不存在；ready 时关联文件附 15 分钟预签名下载 URL）。"""
    data = await service.get_event(event_id)
    return EventResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.post(
    "/events/{event_id}/acknowledge",
    response_model=EventResponse,
    summary="确认事件（幂等）",
)
async def acknowledge_event(
    event_id: Annotated[int, Path(ge=1, description="事件 ID（events.event_id）")],
    service: Annotated[EventService, Depends(get_event_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
) -> EventResponse:
    """事件确认（幂等：重复确认原样返回；确认人取服务端身份，拒绝客户端指定）。"""
    data = await service.acknowledge_event(event_id, user_id=user_id)
    return EventResponse(code=0, message="success", data=data, request_id=trace_request_id())
