"""/api/v1/analytics/{perception,control}/eval 路由（算法评估）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import (
    get_evaluation_service,
    require_read_permission,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN
from app.schemas.evaluation import ControlEvalResponse, PerceptionEvalResponse
from app.services.evaluation import EvaluationService

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics-evaluation"])


@router.get(
    "/perception/eval",
    response_model=PerceptionEvalResponse,
    summary="感知精度评估",
    response_model_exclude_none=True,
)
async def get_perception_eval(
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    start_time: Annotated[
        float | None, Query(ge=0, description="窗口起点（成对出现，缺省取最近文档）")
    ] = None,
    end_time: Annotated[
        float | None, Query(ge=0, description="窗口终点（成对出现，缺省取最近文档）")
    ] = None,
    vehicle_id: Annotated[
        str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（缺省为车队级）")
    ] = None,
) -> PerceptionEvalResponse:
    """感知精度评估（mAP/IoU/召回/精确率/定位误差；文档缺失 → 503/5001）。"""
    data = await service.perception(start_time=start_time, end_time=end_time, vehicle_id=vehicle_id)
    return PerceptionEvalResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.get(
    "/control/eval",
    response_model=ControlEvalResponse,
    summary="控制性能评估",
    response_model_exclude_none=True,
)
async def get_control_eval(
    _user_id: Annotated[str, Depends(require_read_permission)],
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    start_time: Annotated[
        float | None, Query(ge=0, description="窗口起点（成对出现，缺省取最近文档）")
    ] = None,
    end_time: Annotated[
        float | None, Query(ge=0, description="窗口终点（成对出现，缺省取最近文档）")
    ] = None,
    vehicle_id: Annotated[
        str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（缺省为车队级）")
    ] = None,
) -> ControlEvalResponse:
    """控制性能评估（RMSE/超调/调节时间 + 6.3.3 节阈值判定；文档缺失 → 503/5001）。"""
    data = await service.control(start_time=start_time, end_time=end_time, vehicle_id=vehicle_id)
    return ControlEvalResponse(code=0, message="success", data=data, request_id=trace_request_id())