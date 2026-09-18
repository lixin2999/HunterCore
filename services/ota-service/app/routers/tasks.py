"""/api/v1/ota/tasks 路由（创建 / 查询 / 启动 / 暂停 / 恢复 / 终止 / A/B 回滚）。"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from hunter_common.database.enums import OtaTaskStatus
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

from app.core.dependencies import (
    require_create_permission,
    require_execute_permission,
    require_read_permission,
    trace_request_id,
)
from app.schemas.tasks import (
    OtaTaskActionResponse,
    OtaTaskCancelRequest,
    OtaTaskCreateRequest,
    OtaTaskDetailResponse,
    OtaTaskListResponse,
    OtaTaskPauseRequest,
    OtaTaskResponse,
    OtaTaskRollbackRequest,
    OtaTaskRollbackResponse,
    OtaTaskStartRequest,
)
from app.services.tasks import TaskService

router = APIRouter(prefix="/api/v1/ota", tags=["tasks"])

#: 业务端点统一错误响应声明（附录 A 错误码 → HTTP 状态映射见 core.error_handlers）
_COMMON_ERRORS = {
    429: {"description": "附录 D 限流 + Retry-After"},
    422: {"description": "2001/2002/6003（参数/门禁）"},
    404: {"description": "3001 资源不存在"},
    409: {"description": "3003/4001/4002（状态冲突/车辆不在线/车辆忙）"},
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    503: {"description": "5001 服务不可用"},
    500: {"description": "5000 服务器内部错误"},
}


def get_task_service(request: Request) -> TaskService:
    """从 app.state 获取任务服务（lifespan 装配，测试可替换）。"""
    return request.app.state.task_service  # type: ignore[no-any-return]


@router.get(
    "/tasks",
    operation_id="listOtaTasks",
    response_model=OtaTaskListResponse,
    summary="查询升级任务列表（分页）",
    responses={**_COMMON_ERRORS},
)
async def list_ota_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    status: Annotated[OtaTaskStatus | None, Query(description="按任务状态过滤")] = None,
    target_version_id: Annotated[UUID | None, Query(description="按目标版本过滤")] = None,
    vehicle_id: Annotated[str | None, Query(description="按目标车辆过滤（GIN 包含匹配）")] = None,
    creator: Annotated[UUID | None, Query(description="按创建人过滤")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = DEFAULT_PAGE_SIZE,
) -> OtaTaskListResponse:
    """任务列表（create_time DESC；target_vehicles 仅返回计数，名单走详情端点）。"""
    data = await service.list_tasks(
        status=status,
        target_version_id=target_version_id,
        vehicle_id=vehicle_id,
        creator=creator,
        page=page,
        page_size=page_size,
    )
    return OtaTaskListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/tasks",
    operation_id="createOtaTask",
    response_model=OtaTaskResponse,
    summary="创建升级任务（灰度批次规划，不下发）",
    responses={**_COMMON_ERRORS},
)
async def create_ota_task(
    payload: OtaTaskCreateRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    user_id: Annotated[str, Depends(require_create_permission)],
) -> OtaTaskResponse:
    """创建任务并冻结灰度策略（status=created；下发统一由 start 触发）。"""
    data = await service.create_task(payload, user_id)
    return OtaTaskResponse(data=data, request_id=trace_request_id())


@router.get(
    "/tasks/{task_id}",
    operation_id="getOtaTask",
    response_model=OtaTaskDetailResponse,
    summary="查询升级任务详情（含灰度进度、批次统计与推进判定）",
    responses={**_COMMON_ERRORS},
)
async def get_ota_task(
    task_id: Annotated[UUID, Path(description="任务 ID（ota_svc.ota_tasks.task_id）")],
    service: Annotated[TaskService, Depends(get_task_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> OtaTaskDetailResponse:
    """任务详情（目标版本快照 + 冻结策略 + 实时 rollout 视图）。"""
    data = await service.get_task(task_id)
    return OtaTaskDetailResponse(data=data, request_id=trace_request_id())


@router.post(
    "/tasks/{task_id}/start",
    operation_id="startOtaTask",
    response_model=OtaTaskActionResponse,
    summary="启动任务（首轮门禁校验 + 下发第 1 批 5% 升级通知）",
    responses={**_COMMON_ERRORS},
)
async def start_ota_task(
    task_id: Annotated[UUID, Path(description="任务 ID")],
    service: Annotated[TaskService, Depends(get_task_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
    payload: OtaTaskStartRequest | None = None,
) -> OtaTaskActionResponse:
    """启动（逐车门禁：放行清单 released[] / 拦截明细 blocked[]；重复调用幂等）。"""
    data = await service.start_task(task_id, payload, user_id)
    return OtaTaskActionResponse(data=data, request_id=trace_request_id())


@router.post(
    "/tasks/{task_id}/pause",
    operation_id="pauseOtaTask",
    response_model=OtaTaskActionResponse,
    summary="暂停升级任务（冻结批次推进，不影响已下发车辆）",
    responses={**_COMMON_ERRORS},
)
async def pause_ota_task(
    task_id: Annotated[UUID, Path(description="任务 ID")],
    service: Annotated[TaskService, Depends(get_task_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
    payload: OtaTaskPauseRequest | None = None,
) -> OtaTaskActionResponse:
    """暂停（running → paused；非 running → 3003）。"""
    data = await service.pause_task(task_id, payload, user_id)
    return OtaTaskActionResponse(data=data, request_id=trace_request_id())


@router.post(
    "/tasks/{task_id}/resume",
    operation_id="resumeOtaTask",
    response_model=OtaTaskActionResponse,
    summary="恢复升级任务（重新评估批次推进条件）",
    responses={**_COMMON_ERRORS},
)
async def resume_ota_task(
    task_id: Annotated[UUID, Path(description="任务 ID")],
    service: Annotated[TaskService, Depends(get_task_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
    payload: OtaTaskStartRequest | None = None,
) -> OtaTaskActionResponse:
    """恢复（paused → running；成功率未达门禁 → 3003 + data.halt_reason）。"""
    data = await service.resume_task(task_id, payload, user_id)
    return OtaTaskActionResponse(data=data, request_id=trace_request_id())


@router.post(
    "/tasks/{task_id}/cancel",
    operation_id="cancelOtaTask",
    response_model=OtaTaskActionResponse,
    summary="终止升级任务（不再推进任何批次）",
    responses={**_COMMON_ERRORS},
)
async def cancel_ota_task(
    task_id: Annotated[UUID, Path(description="任务 ID")],
    service: Annotated[TaskService, Depends(get_task_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
    payload: OtaTaskCancelRequest | None = None,
) -> OtaTaskActionResponse:
    """终止（终态：canceled；已下发车辆不强制中断，进度冻结）。"""
    data = await service.cancel_task(task_id, payload, user_id)
    return OtaTaskActionResponse(data=data, request_id=trace_request_id())


@router.post(
    "/tasks/{task_id}/rollback",
    operation_id="rollbackOtaTask",
    response_model=OtaTaskRollbackResponse,
    summary="触发 A/B 分区回滚（人工兜底，按车辆）",
    responses={**_COMMON_ERRORS},
)
async def rollback_ota_task(
    task_id: Annotated[UUID, Path(description="任务 ID")],
    payload: OtaTaskRollbackRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
) -> OtaTaskRollbackResponse:
    """回滚（仅 SUCCESS 记录；逐车下发 hunter.{vehicle_id}.command ota_rollback 指令）。"""
    data = await service.rollback_task(task_id, payload, user_id)
    return OtaTaskRollbackResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]
