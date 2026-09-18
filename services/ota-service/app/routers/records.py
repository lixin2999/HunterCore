"""/api/v1/ota 记录查询路由（任务监控明细 / 单车升级历史时间线）。"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from hunter_common.database.enums import OtaStatus
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

from app.core.dependencies import require_read_permission, trace_request_id
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.records import OtaRecordListResponse
from app.services.records import RecordService

router = APIRouter(prefix="/api/v1/ota", tags=["records"])

_ERRORS = {
    422: {"description": "2001 参数错误"},
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    503: {"description": "5001 服务不可用"},
    500: {"description": "5000 服务器内部错误"},
}


def get_record_service(request: Request) -> RecordService:
    """从 app.state 获取记录服务（lifespan 装配，测试可替换）。"""
    return request.app.state.record_service  # type: ignore[no-any-return]


@router.get(
    "/tasks/{task_id}/records",
    operation_id="listOtaTaskRecords",
    response_model=OtaRecordListResponse,
    summary="查询任务下车辆升级记录（升级监控明细）",
    responses={
        404: {"description": "3001 任务不存在"},
        **_ERRORS,
    },
)
async def list_ota_task_records(
    task_id: Annotated[UUID, Path(description="任务 ID")],
    service: Annotated[RecordService, Depends(get_record_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    vehicle_id: Annotated[str | None, Query(description="按车辆过滤")] = None,
    status: Annotated[OtaStatus | None, Query(description="按升级状态过滤（车端 9 态）")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = DEFAULT_PAGE_SIZE,
) -> OtaRecordListResponse:
    """任务升级记录（start_time DESC NULLS LAST, record_id DESC；含任务级 summary）。"""
    data = await service.list_task_records(
        task_id, vehicle_id=vehicle_id, status=status, page=page, page_size=page_size
    )
    return OtaRecordListResponse(data=data, request_id=trace_request_id())


@router.get(
    "/vehicles/{vehicle_id}/records",
    operation_id="listVehicleOtaRecords",
    response_model=OtaRecordListResponse,
    summary="查询单车 OTA 升级历史（时间线）",
    responses={**_ERRORS},
)
async def list_vehicle_ota_records(
    vehicle_id: Annotated[str, Path(pattern=VEHICLE_ID_PATTERN, description="车辆标识")],
    service: Annotated[RecordService, Depends(get_record_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    status: Annotated[OtaStatus | None, Query(description="按升级状态过滤")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = DEFAULT_PAGE_SIZE,
) -> OtaRecordListResponse:
    """单车升级历史（跨任务时间线；只读，不回写 vehicles.software_version）。"""
    data = await service.list_vehicle_records(
        vehicle_id, status=status, page=page, page_size=page_size
    )
    return OtaRecordListResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]
