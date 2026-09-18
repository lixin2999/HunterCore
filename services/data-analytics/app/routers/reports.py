"""/api/v1/analytics/reports 路由（报告列表 / 详情 / 生成提交）。"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from hunter_common.database import MAX_PAGE_SIZE

from app.core.dependencies import (
    get_report_service,
    require_execute_permission,
    require_read_permission,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.reports import (
    ReportDetailResponse,
    ReportGenerateRequest,
    ReportGenerateResponse,
    ReportListResponse,
    ReportStatus,
    ReportType,
)
from app.services.reports import ReportService

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics-reports"])


@router.get("/reports", response_model=ReportListResponse, summary="查询分析报告列表", response_model_exclude_none=True)
async def list_reports(
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[ReportService, Depends(get_report_service)],
    report_type: Annotated[ReportType | None, Query(description="报告模板类型")] = None,
    vehicle_id: Annotated[str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（可选）")] = None,
    status: Annotated[ReportStatus | None, Query(description="报告状态（可选）")] = None,
    start_time: Annotated[float | None, Query(ge=0, description="窗口起点（成对出现，可选）")] = None,
    end_time: Annotated[float | None, Query(ge=0, description="窗口终点（成对出现，可选）")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = 20,
) -> ReportListResponse:
    """报告列表（MinIO sidecar 聚合；created_at DESC 排序）。"""
    data = await service.list_reports(
        report_type=report_type,
        vehicle_id=vehicle_id,
        status=status,
        start_time=start_time,
        end_time=end_time,
        page=page,
        page_size=page_size,
    )
    return ReportListResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.get(
    "/reports/{report_id}",
    response_model=ReportDetailResponse,
    summary="查询报告详情",
    response_model_exclude_none=True,
)
async def get_report(
    report_id: Annotated[uuid.UUID, Path(description="报告 ID（UUID）")],
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[ReportService, Depends(get_report_service)],
) -> ReportDetailResponse:
    """报告详情（ready 时产物附下载预签名 URL，15 分钟有效，支持 Range）。"""
    data = await service.get_report(str(report_id))
    return ReportDetailResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.post(
    "/reports/generate",
    response_model=ReportGenerateResponse,
    status_code=202,
    summary="提交报告生成任务（异步）",
    response_model_exclude_none=True,
)
async def generate_report(
    payload: ReportGenerateRequest,
    user_id: Annotated[str, Depends(require_execute_permission)],
    service: Annotated[ReportService, Depends(get_report_service)],
) -> ReportGenerateResponse:
    """202 受理：校验→车辆校验→去重(409/3003)→并发守卫→写 sidecar 入队（pending）。"""
    data = await service.submit(payload, user_id=user_id)
    return ReportGenerateResponse(code=0, message="success", data=data, request_id=trace_request_id())