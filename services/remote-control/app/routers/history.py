"""/api/v1/remote/history 路由（操控历史与录像访问；契约 3 端点）。

数据源为 MinIO hunter-video sidecar（不新增数据库表，契约 x-hunter-history-archive）；
数据权限：普通用户仅可查询自身 operator_id 记录（契约 324-325 行），管理员可按
车辆/操作员过滤 —— 在路由层收敛 effective_operator_id 后传入 service。
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Path, Query

from app.config import settings
from app.core.dependencies import (
    OperatorContext,
    ReadOperatorDep,
    RequestIdDep,
    get_history_service,
)
from app.schemas.common import SessionEndReason
from app.schemas.history import (
    ControlHistoryDetailResponse,
    ControlHistoryListResponse,
    ControlVideoAccessResponse,
)
from app.services.history_service import HistoryService

router = APIRouter(prefix="/api/v1/remote", tags=["history"])

#: 业务端点统一错误响应声明（错误码 → HTTP 状态映射见 core.error_handlers）
_COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    422: {"description": "2001/2002（参数错误/缺失；含时间范围超限）"},
    404: {"description": "3001 资源不存在（记录/录像缺失或已过保留期）"},
    409: {"description": "3003 资源状态冲突（录像仍在写入未封存）"},
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    503: {"description": "5001 服务不可用"},
    500: {"description": "5000 服务器内部错误"},
}


def _effective_operator_id(
    operator_id: str | None, operator: OperatorContext
) -> str | None:
    """数据权限收敛：普通用户强制 operator_id=自身；admin 可按参数过滤（契约 324 行）。"""
    if operator.is_admin(settings):
        return operator_id
    return operator.user_id


@router.get(
    "/history",
    operation_id="listControlHistory",
    response_model=ControlHistoryListResponse,
    summary="查询操控记录列表（分页）",
    responses=_COMMON_ERRORS,
)
async def list_control_history(
    operator: ReadOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[HistoryService, Depends(get_history_service)],
    vehicle_id: Annotated[
        str | None, Query(description="按车辆过滤（对象键前缀）")
    ] = None,
    operator_id: Annotated[
        str | None, Query(description="按操作员过滤（普通用户忽略，强制为自身）")
    ] = None,
    started_from: Annotated[
        float | None, Query(description="会话开始时间下界（Unix epoch 秒，含）")
    ] = None,
    started_to: Annotated[
        float | None, Query(description="会话开始时间上界（Unix epoch 秒，含）")
    ] = None,
    end_reason: Annotated[
        SessionEndReason | None, Query(description="按结束原因过滤")
    ] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=200, description="每页条数（≤200）")] = 20,
) -> ControlHistoryListResponse:
    """操控记录列表（sidecar 投影；started_at 降序；时间跨度 ≤31 天，契约 322 行）。"""
    data = await service.list_history(
        vehicle_id=vehicle_id,
        operator_id=_effective_operator_id(operator_id, operator),
        started_from=started_from,
        started_to=started_to,
        end_reason=end_reason,
        page=page,
        page_size=page_size,
    )
    return ControlHistoryListResponse(data=data, request_id=request_id)


@router.get(
    "/history/{session_id}",
    operation_id="getControlHistoryDetail",
    response_model=ControlHistoryDetailResponse,
    summary="查询单场操控记录详情",
    responses=_COMMON_ERRORS,
)
async def get_control_history_detail(
    session_id: Annotated[str, Path(description="会话 ID（UUID）")],
    _operator: ReadOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[HistoryService, Depends(get_history_service)],
) -> ControlHistoryDetailResponse:
    """记录详情（sidecar 原文 + video_available；记录不存在 → 3001）。"""
    data = await service.get_history_detail(session_id)
    return ControlHistoryDetailResponse(data=data, request_id=request_id)


@router.get(
    "/history/{session_id}/video",
    operation_id="getControlVideo",
    response_model=ControlVideoAccessResponse,
    summary="获取操控录像下载/播放地址（15 分钟预签名）",
    responses=_COMMON_ERRORS,
)
async def get_control_video(
    session_id: Annotated[str, Path(description="会话 ID（UUID）")],
    _operator: ReadOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[HistoryService, Depends(get_history_service)],
) -> ControlVideoAccessResponse:
    """录像访问（预签名 URL 不落日志；对象缺失 → 3001，契约 412-414 行）。"""
    data = await service.get_video_access(session_id)
    return ControlVideoAccessResponse(data=data, request_id=request_id)


__all__ = ["router"]
