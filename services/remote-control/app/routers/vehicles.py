"""/api/v1/remote/vehicles 路由（可操控车辆列表；契约 listControllableVehicles）。

数据源为 Redis 读模型（vehicle:online:set + vehicle:status），权威值属 vehicle-service，
本服务只读（契约 71 行）；status/controllable_only/vehicle_id 过滤与分页在此组合
（VehicleViewReader.list_online_views 返回全量在线视图，契约 130 行）。
"""

from __future__ import annotations

import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import ReadOperatorDep, RequestIdDep, get_vehicle_view
from app.schemas.common import VehicleStatus
from app.schemas.vehicles import (
    ControllableVehicle,
    ControllableVehicleList,
    ControllableVehicleListResponse,
)
from app.services.vehicle_view import VehicleViewReader

router = APIRouter(prefix="/api/v1/remote", tags=["vehicles"])

#: 业务端点统一错误响应声明（错误码 → HTTP 状态映射见 core.error_handlers）
_COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    422: {"description": "2001/2002（参数错误/缺失）"},
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    503: {"description": "5001 服务不可用"},
    500: {"description": "5000 服务器内部错误"},
}

#: 在线车辆集合规模上限（VehicleViewReader.list_online_views 默认值；防 SMEMBERS 放大）
_ONLINE_VIEW_LIMIT = 500


@router.get(
    "/vehicles",
    operation_id="listControllableVehicles",
    response_model=ControllableVehicleListResponse,
    summary="查询可操控车辆列表（分页）",
    responses=_COMMON_ERRORS,
)
async def list_controllable_vehicles(
    _operator: ReadOperatorDep,
    request_id: RequestIdDep,
    view_reader: Annotated[VehicleViewReader, Depends(get_vehicle_view)],
    status: Annotated[VehicleStatus | None, Query(description="按车辆状态过滤")] = None,
    controllable_only: Annotated[
        bool, Query(description="仅返回当前可被操控的车辆")
    ] = True,
    vehicle_id: Annotated[str | None, Query(description="车辆标识精确匹配")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=200, description="每页条数（≤200）")] = 20,
) -> ControllableVehicleListResponse:
    """可操控车辆列表（vehicle_id 升序；total 为 Redis 读模型匹配数量，非 DB COUNT）。"""
    views: list[ControllableVehicle] = await view_reader.list_online_views(
        limit=_ONLINE_VIEW_LIMIT
    )
    if status is not None:
        views = [view for view in views if view.status == status]
    if controllable_only:
        views = [view for view in views if view.controllable]
    if vehicle_id is not None:
        views = [view for view in views if view.vehicle_id == vehicle_id]
    total = len(views)
    start = (page - 1) * page_size
    data = ControllableVehicleList(
        items=views[start : start + page_size],
        total=total,
        page=page,
        page_size=page_size,
        generated_at=time.time(),  # 契约必填：视图生成时间（前端展示「实时视图」）
    )
    return ControllableVehicleListResponse(data=data, request_id=request_id)


__all__ = ["router"]
