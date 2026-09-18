"""GET /api/v1/analytics/corner-cases 路由（Corner Case 检索）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import (
    get_corner_case_service,
    require_read_permission,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.corner_cases import (
    CornerCaseAlgorithm,
    CornerCaseCategory,
    CornerCaseListResponse,
)
from app.services.corner_cases import CornerCaseService

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics-corner-cases"])


@router.get(
    "/corner-cases",
    response_model=CornerCaseListResponse,
    summary="Corner Case 检索",
    response_model_exclude_none=True,
)
async def list_corner_cases(
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[CornerCaseService, Depends(get_corner_case_service)],
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页条数（≤100）")] = 20,
    category: Annotated[CornerCaseCategory | None, Query(description="案例类别过滤")] = None,
    vehicle_id: Annotated[
        str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识过滤")
    ] = None,
    algorithm: Annotated[CornerCaseAlgorithm | None, Query(description="挖掘算法过滤")] = None,
    min_anomaly_score: Annotated[float | None, Query(ge=0.0, le=1.0, description="异常分下界")] = None,
    start_time: Annotated[
        float | None, Query(ge=0, description="事件时间窗起点（成对出现，可选）")
    ] = None,
    end_time: Annotated[
        float | None, Query(ge=0, description="事件时间窗终点（成对出现，可选）")
    ] = None,
) -> CornerCaseListResponse:
    """Corner Case 检索（anomaly_score DESC 默认排序 + 分类计数 + 挖掘元信息）。"""
    data = await service.list_cases(
        page=page,
        page_size=page_size,
        category=category,
        vehicle_id=vehicle_id,
        algorithm=algorithm,
        min_anomaly_score=min_anomaly_score,
        start_time=start_time,
        end_time=end_time,
    )
    return CornerCaseListResponse(code=0, message="success", data=data, request_id=trace_request_id())