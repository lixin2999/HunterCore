"""GET /api/v1/analytics/scene/coverage 路由（场景覆盖率）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import (
    get_coverage_service,
    require_read_permission,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.coverage import SceneCoverageResponse
from app.services.coverage import CoverageService

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics-coverage"])


@router.get(
    "/scene/coverage",
    response_model=SceneCoverageResponse,
    summary="场景覆盖率查询",
    response_model_exclude_none=True,
)
async def get_scene_coverage(
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[CoverageService, Depends(get_coverage_service)],
    start_time: Annotated[
        float | None, Query(ge=0, description="窗口起点（成对出现，缺省取最近文档）")
    ] = None,
    end_time: Annotated[
        float | None, Query(ge=0, description="窗口终点（成对出现，缺省取最近文档）")
    ] = None,
    vehicle_id: Annotated[
        str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（缺省为车队级）")
    ] = None,
    max_cells: Annotated[
        int, Query(ge=1, le=50000, description="heatmap 截断上限（契约默认 5000，最大 50000）")
    ] = 5000,
) -> SceneCoverageResponse:
    """覆盖率查询（网格热度图支持 max_cells 截断，截断置 heatmap_truncated=true）。"""
    data = await service.coverage(
        start_time=start_time, end_time=end_time, vehicle_id=vehicle_id, max_cells=max_cells
    )
    return SceneCoverageResponse(code=0, message="success", data=data, request_id=trace_request_id())