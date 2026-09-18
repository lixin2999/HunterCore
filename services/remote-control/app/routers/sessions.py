"""/api/v1/remote/session(s) 路由（会话生命周期；契约 4 端点）。

- POST /session（createRemoteSession）：remote:create + 附录 D 单用户 1 QPS 限流
  （CreateOperatorDep 内置）；创建成功 200 + RemoteSessionInfo；4001/4002/7001 → 409；
- GET /sessions（listRemoteSessions）：remote:read；普通用户仅自身会话（service 收敛）；
- GET /session/{session_id}（getRemoteSession）：remote:read + include_stats；
- DELETE /session/{session_id}（endRemoteSession）：remote:execute；reason 经 query
  （契约 285-289 行）；越权 → 1002；幂等：已结束 → 3001（契约 280 行）。
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Path, Query

from app.core.dependencies import (
    CreateOperatorDep,
    ExecuteOperatorDep,
    ReadOperatorDep,
    RequestIdDep,
    get_session_service,
)
from app.schemas.common import SessionEndReason
from app.schemas.sessions import (
    CreateRemoteSessionRequest,
    RemoteSessionDetailResponse,
    RemoteSessionInfoResponse,
    RemoteSessionListResponse,
    RemoteSessionResultResponse,
)
from app.services.session_service import SessionService

router = APIRouter(prefix="/api/v1/remote", tags=["sessions"])

#: 业务端点统一错误响应声明（错误码 → HTTP 状态映射见 core.error_handlers）
_COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    429: {"description": "附录 D 限流 + Retry-After"},
    422: {"description": "2001/2002（参数错误/缺失）"},
    404: {"description": "3001 资源不存在"},
    409: {"description": "4001/4002/7001（车辆不在线/车辆忙/会话冲突）"},
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    503: {"description": "5001 服务不可用"},
    500: {"description": "5000 服务器内部错误"},
}


@router.post(
    "/session",
    operation_id="createRemoteSession",
    response_model=RemoteSessionInfoResponse,
    summary="创建远程操控会话（互斥校验 + 下发车端信令）",
    responses=_COMMON_ERRORS,
)
async def create_remote_session(
    payload: CreateRemoteSessionRequest,
    operator: CreateOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[SessionService, Depends(get_session_service)],
) -> RemoteSessionInfoResponse:
    """创建会话（可控性判定 → 分布式锁 → rc:session Hash → 车端 session_start + boot）。"""
    data = await service.create_session(payload.vehicle_id, operator, payload)
    return RemoteSessionInfoResponse(data=data, request_id=request_id)


@router.get(
    "/sessions",
    operation_id="listRemoteSessions",
    response_model=RemoteSessionListResponse,
    summary="查询当前进行中的操控会话（分页）",
    responses=_COMMON_ERRORS,
)
async def list_remote_sessions(
    operator: ReadOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[SessionService, Depends(get_session_service)],
    vehicle_id: Annotated[str | None, Query(description="按车辆过滤")] = None,
    operator_id: Annotated[
        str | None, Query(description="按操作员过滤（普通用户强制为自身）")
    ] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=200, description="每页条数（≤200）")] = 20,
) -> RemoteSessionListResponse:
    """活跃会话列表（SCAN rc:session:*；started_at 降序；数据权限在 service 收敛）。"""
    data = await service.list_active_sessions(
        operator,
        vehicle_id=vehicle_id,
        operator_id=operator_id,
        page=page,
        page_size=page_size,
    )
    return RemoteSessionListResponse(data=data, request_id=request_id)


@router.get(
    "/session/{session_id}",
    operation_id="getRemoteSession",
    response_model=RemoteSessionDetailResponse,
    summary="查询操控会话详情（实时统计）",
    responses=_COMMON_ERRORS,
)
async def get_remote_session(
    session_id: Annotated[str, Path(description="会话 ID（UUID）")],
    operator: ReadOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[SessionService, Depends(get_session_service)],
    include_stats: Annotated[bool, Query(description="是否返回实时统计块")] = True,
) -> RemoteSessionDetailResponse:
    """会话详情（会话已结束 → 3001，改查 /history/{session_id}，契约 238 行）。"""
    data = await service.get_session(session_id, operator, include_stats=include_stats)
    return RemoteSessionDetailResponse(data=data, request_id=request_id)


@router.delete(
    "/session/{session_id}",
    operation_id="endRemoteSession",
    response_model=RemoteSessionResultResponse,
    summary="结束操控会话（释放车辆占用并封存录像）",
    responses=_COMMON_ERRORS,
)
async def end_remote_session(
    session_id: Annotated[str, Path(description="会话 ID（UUID）")],
    operator: ExecuteOperatorDep,
    request_id: RequestIdDep,
    service: Annotated[SessionService, Depends(get_session_service)],
    reason: Annotated[
        SessionEndReason, Query(description="结束原因（默认 operator_end）")
    ] = SessionEndReason.OPERATOR_END,
) -> RemoteSessionResultResponse:
    """结束会话（先安全后清理：stop 帧 + session_end → 删 Hash → sidecar 归档）。"""
    data = await service.end_session(session_id, operator, reason)
    return RemoteSessionResultResponse(data=data, request_id=request_id)


__all__ = ["router"]
