"""/api/v1/data/telemetry 路由（附录 D 限流：单用户 20 QPS）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Response
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

from app.core.dependencies import telemetry_query_user, trace_request_id
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.telemetry import TelemetryQueryResponse
from app.services.telemetry import TelemetryService

router = APIRouter(prefix="/api/v1/data", tags=["telemetry"])


def get_telemetry_service(request: Request) -> TelemetryService:
    """从 app.state 获取遥测服务（lifespan 装配，测试可替换）。"""
    return request.app.state.telemetry_service  # type: ignore[no-any-return]


@router.get(
    "/telemetry",
    response_model=TelemetryQueryResponse,
    summary="查询车辆遥测数据（TimescaleDB）",
    response_model_exclude_none=False,
    responses={
        200: {"description": "查询成功"},
        206: {"description": "时间区间被截断（X-Truncated-Range: seconds=<截去秒数>）"},
    },
)
async def query_telemetry(
    response: Response,
    service: Annotated[TelemetryService, Depends(get_telemetry_service)],
    _user_id: Annotated[str, Depends(telemetry_query_user)],
    vehicle_id: Annotated[str, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（必选）")],
    start_time: Annotated[float, Query(ge=0, description="区间起点（Unix epoch 秒，必选）")],
    end_time: Annotated[float, Query(ge=0, description="区间终点（Unix epoch 秒，必选）")],
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = DEFAULT_PAGE_SIZE,
) -> TelemetryQueryResponse:
    """遥测查询（time DESC 分页；跨度超上限时截断并返回 206）。"""
    data, truncated_seconds = await service.query_telemetry(
        vehicle_id=vehicle_id,
        start_time=start_time,
        end_time=end_time,
        page=page,
        page_size=page_size,
    )
    if truncated_seconds > 0:
        response.status_code = 206
        response.headers["X-Truncated-Range"] = f"seconds={truncated_seconds}"
    return TelemetryQueryResponse(code=0, message="success", data=data, request_id=trace_request_id())
