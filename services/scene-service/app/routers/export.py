"""场景导出路由（契约 POST /api/v1/scene/export，operationId: exportScenes，4.3 节）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from app.core.dependencies import (
    get_export_service,
    require_read_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.scene import SceneExportRequest, SceneExportResponse
from app.services.export import SceneExportService

router = APIRouter(prefix="/api/v1/scene", tags=["export"])


@router.post(
    "/export",
    operation_id="exportScenes",
    response_model=SceneExportResponse,
    summary="导出场景（Carla ScenarioRunner XML / OpenSCENARIO 1.2）",
    responses={**COMMON_ERRORS},
)
async def export_scenes(
    payload: SceneExportRequest,
    service: Annotated[SceneExportService, Depends(get_export_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> SceneExportResponse:
    """导出场景产物到 MinIO hunter-scene-assets 并返回 15 分钟预签名下载地址。"""
    data = await service.export_scenes(payload)
    return SceneExportResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]