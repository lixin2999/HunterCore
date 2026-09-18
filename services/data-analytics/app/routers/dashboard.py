"""GET /api/v1/analytics/dashboard 路由（运营看板聚合）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import (
    get_dashboard_service,
    require_read_permission,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.dashboard import DashboardResponse, DashboardTimeRange
from app.services.dashboard import DashboardService

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics-dashboard"])


@router.get(
    "/dashboard",
    response_model=DashboardResponse,
    summary="运营看板聚合数据",
    response_model_exclude_none=True,
)
async def get_dashboard(
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[DashboardService, Depends(get_dashboard_service)],
    time_range: Annotated[DashboardTimeRange, Query(description="统计时间窗口（1h/24h/7d/30d）")] = "24h",
    vehicle_id: Annotated[
        str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆过滤（可选）")
    ] = None,
) -> DashboardResponse:
    """四数据块并发聚合，独立降级（degraded 字段标识）；全部降级 → 503/5001。"""
    data = await service.aggregate(time_range=time_range, vehicle_id=vehicle_id)
    return DashboardResponse(code=0, message="success", data=data, request_id=trace_request_id())