"""场景下发路由（契约 POST /api/v1/scene/{scene_id}/run，operationId: runScene，4.4 节；
以及 G-20① 新增的仿真进度/结果查询 GET 端点，无状态代理 Carla 管理 API）。"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path

from app.core.dependencies import (
    get_simulation_service,
    require_execute_permission,
    require_read_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.scene import (
    SceneRunRequest,
    SceneRunResponse,
    SimulationProgressResponse,
    SimulationResultResponse,
)
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


@router.get(
    "/simulations/{sim_instance_id}",
    operation_id="getSimulationProgress",
    response_model=SimulationProgressResponse,
    summary="查询仿真实例进度（4.4 节第 6 步，决策 G-20①）",
    responses={**COMMON_ERRORS},
)
async def get_simulation_progress(
    sim_instance_id: Annotated[str, Path(max_length=128, description="Carla 仿真实例 ID")],
    service: Annotated[SceneSimulationService, Depends(get_simulation_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> SimulationProgressResponse:
    """无状态代理 Carla 实例查询接口（404 → 3001；进度字段宽松透传，缺失置 null）。"""
    data = await service.get_progress(sim_instance_id)
    return SimulationProgressResponse(data=data, request_id=trace_request_id())


@router.get(
    "/simulations/{sim_instance_id}/result",
    operation_id="getSimulationResult",
    response_model=SimulationResultResponse,
    summary="查询仿真实例结果（4.4 节第 7 步，决策 G-20①）",
    responses={**COMMON_ERRORS},
)
async def get_simulation_result(
    sim_instance_id: Annotated[str, Path(max_length=128, description="Carla 仿真实例 ID")],
    service: Annotated[SceneSimulationService, Depends(get_simulation_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> SimulationResultResponse:
    """仅终态实例可查（非终态 → 3003）；产物落 hunter-scene-assets 时换发预签名 URL。"""
    data = await service.get_result(sim_instance_id)
    return SimulationResultResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]