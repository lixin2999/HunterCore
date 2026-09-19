"""场景下发路由（契约 POST /api/v1/scene/{scene_id}/run，operationId: runScene，4.4 节）。"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path

from app.core.dependencies import (
    get_simulation_service,
    require_execute_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.scene import SceneRunRequest, SceneRunResponse
from app.services.simulation import SceneSimulationService

router = APIRouter(prefix="/api/v1/scene", tags=["simulation"])


@router.post(
    "/{scene_id}/run",
    operation_id="runScene",
    response_model=SceneRunResponse,
    summary="下发场景到 Carla 仿真（4.4 节流程）",
    responses={**COMMON_ERRORS},
)
async def run_scene(
    scene_id: Annotated[UUID, Path(description="场景 ID（仅 published 可下发）")],
    service: Annotated[SceneSimulationService, Depends(get_simulation_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
    payload: SceneRunRequest | None = None,
) -> SceneRunResponse:
    """下发场景（① 校验配置/状态 → ② 创建仿真实例 → ③④ 下发配置 → ⑤ 返回实例信息）。"""
    data = await service.run_scene(scene_id, payload)
    return SceneRunResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]