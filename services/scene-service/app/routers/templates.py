"""场景模板路由（契约 GET /api/v1/scene/templates，operationId: listSceneTemplates）。

⚠ 注册顺序：本路由必须在 scenes 路由（含 ``/{scene_id}``）之前 include，
否则 ``/api/v1/scene/templates`` 会被路径参数路由优先匹配。
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import (
    get_template_service,
    require_read_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.scene import (
    SceneTemplateCategory,
    SceneTemplateListResponse,
    SceneType,
)
from app.services.templates import SceneTemplateService

router = APIRouter(prefix="/api/v1/scene", tags=["templates"])


@router.get(
    "/templates",
    operation_id="listSceneTemplates",
    response_model=SceneTemplateListResponse,
    summary="查询场景模板列表",
    responses={**COMMON_ERRORS},
)
async def list_scene_templates(
    service: Annotated[SceneTemplateService, Depends(get_template_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    category: Annotated[
        SceneTemplateCategory | None,
        Query(description="模板分类（4.2.1 节分类；实车回放无预置模板故不在枚举中）"),
    ] = None,
    scene_type: Annotated[SceneType | None, Query(description="模板场景类型过滤")] = None,
) -> SceneTemplateListResponse:
    """预置模板列表（不分页；config 可直接作为创建场景请求的 config 提交）。"""
    data = service.list_templates(category=category, scene_type=scene_type)
    return SceneTemplateListResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]