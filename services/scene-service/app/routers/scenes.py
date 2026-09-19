"""场景库路由（契约 paths：/api/v1/scene 与 /api/v1/scene/{scene_id} 及其子动作）。

operationId 必须与契约一致（listScenes/createScene/getScene/updateScene/deleteScene/
duplicateScene/publishScene），路径按全路径注册（网关不剥离前缀，见 x-hunter-service）。
"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from hunter_common.database.enums import SceneStatus
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

from app.core.dependencies import (
    get_scene_service,
    require_execute_permission,
    require_read_permission,
    require_read_principals,
    require_write_permission,
    trace_request_id,
)
from app.routers.errors import COMMON_ERRORS, CREATE_ERRORS
from app.schemas.common import TAG_MAX_ITEMS
from app.schemas.scene import (
    SceneCreateRequest,
    SceneDeleteResponse,
    SceneDuplicateRequest,
    SceneListResponse,
    ScenePublishRequest,
    SceneResponse,
    SceneSortField,
    SceneType,
    SceneUpdateRequest,
    SortOrder,
)
from app.services.scenes import SceneService

router = APIRouter(prefix="/api/v1/scene", tags=["scenes"])


@router.get(
    "",
    operation_id="listScenes",
    response_model=SceneListResponse,
    summary="查询场景列表（分页 + 分类/状态/标签/关键字筛选）",
    responses={**COMMON_ERRORS},
)
async def list_scenes(
    principals: Annotated[tuple[str, frozenset[str]], Depends(require_read_principals)],
    service: Annotated[SceneService, Depends(get_scene_service)],
    page: Annotated[int, Query(ge=1, description="页码（≥1）")] = 1,
    page_size: Annotated[
        int, Query(ge=1, le=MAX_PAGE_SIZE, description=f"每页条数（1-{MAX_PAGE_SIZE}）")
    ] = DEFAULT_PAGE_SIZE,
    scene_type: Annotated[SceneType | None, Query(description="场景类型过滤")] = None,
    status: Annotated[SceneStatus | None, Query(description="场景状态过滤")] = None,
    tags: Annotated[
        list[str] | None,
        Query(
            max_length=TAG_MAX_ITEMS,
            description="标签过滤（可重复传参，多标签为 AND 关系）",
        ),
    ] = None,
    keyword: Annotated[
        str | None, Query(min_length=1, max_length=64, description="名称/描述模糊搜索关键字")
    ] = None,
    creator: Annotated[
        UUID | None, Query(description="创建者 user_id（数据权限隔离时由服务端强制注入）")
    ] = None,
    sort: Annotated[SceneSortField, Query(description="排序字段（白名单）")] = SceneSortField.CREATE_TIME,
    order: Annotated[SortOrder, Query(description="排序方向")] = SortOrder.DESC,
) -> SceneListResponse:
    """场景列表（默认 create_time DESC；tags 为包含查询；软删除场景不出现）。"""
    user_id, roles = principals
    data = await service.list_scenes(
        user_id=user_id,
        roles=roles,
        page=page,
        page_size=page_size,
        scene_type=scene_type.value if scene_type is not None else None,
        status=status,
        tags=tags,
        keyword=keyword,
        creator=creator,
        sort=sort.value,
        order=order.value,
    )
    return SceneListResponse(data=data, request_id=trace_request_id())


@router.post(
    "",
    operation_id="createScene",
    response_model=SceneResponse,
    status_code=201,
    summary="创建场景（场景编辑器保存）",
    responses={**CREATE_ERRORS},
)
async def create_scene(
    payload: SceneCreateRequest,
    service: Annotated[SceneService, Depends(get_scene_service)],
    user_id: Annotated[str, Depends(require_write_permission)],
) -> SceneResponse:
    """创建场景（status 固定 draft、creator 取网关注入用户、version 默认 1.0.0）。"""
    scene = await service.create_scene(payload, user_id=user_id)
    return SceneResponse(data=scene, request_id=trace_request_id())


@router.get(
    "/{scene_id}",
    operation_id="getScene",
    response_model=SceneResponse,
    summary="查询场景详情",
    responses={**COMMON_ERRORS},
)
async def get_scene(
    scene_id: Annotated[UUID, Path(description="场景 ID（scene_svc.scenes.scene_id）")],
    service: Annotated[SceneService, Depends(get_scene_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> SceneResponse:
    """场景详情（含 4.2.2 结构 config；按 Redis cache:scene:{scene_id} 缓存，TTL 1 小时）。"""
    scene = await service.get_scene(scene_id)
    return SceneResponse(data=scene, request_id=trace_request_id())


@router.put(
    "/{scene_id}",
    operation_id="updateScene",
    response_model=SceneResponse,
    summary="更新场景（仅 draft 可编辑）",
    responses={**COMMON_ERRORS},
)
async def update_scene(
    scene_id: Annotated[UUID, Path(description="场景 ID")],
    payload: SceneUpdateRequest,
    service: Annotated[SceneService, Depends(get_scene_service)],
    user_id: Annotated[str, Depends(require_write_permission)],
) -> SceneResponse:
    """全量更新可编辑字段（非 draft → 3003；重名 → 3002；写后失效缓存）。"""
    scene = await service.update_scene(scene_id, payload, user_id=user_id)
    return SceneResponse(data=scene, request_id=trace_request_id())


@router.delete(
    "/{scene_id}",
    operation_id="deleteScene",
    response_model=SceneDeleteResponse,
    summary="删除场景（软删除）",
    responses={**COMMON_ERRORS},
)
async def delete_scene(
    scene_id: Annotated[UUID, Path(description="场景 ID")],
    service: Annotated[SceneService, Depends(get_scene_service)],
    _user_id: Annotated[str, Depends(require_write_permission)],
) -> SceneDeleteResponse:
    """软删除场景（仅 draft/archived 可删；published → 3003 需先归档）。"""
    data = await service.delete_scene(scene_id)
    return SceneDeleteResponse(data=data, request_id=trace_request_id())


@router.post(
    "/{scene_id}/duplicate",
    operation_id="duplicateScene",
    response_model=SceneResponse,
    status_code=201,
    summary="复制场景（生成新草稿）",
    responses={**CREATE_ERRORS},
)
async def duplicate_scene(
    scene_id: Annotated[UUID, Path(description="源场景 ID")],
    service: Annotated[SceneService, Depends(get_scene_service)],
    user_id: Annotated[str, Depends(require_write_permission)],
    payload: SceneDuplicateRequest | None = None,
) -> SceneResponse:
    """复制为新草稿（未指定名称时按 <源名称>-copy 生成，冲突追加序号）。"""
    scene = await service.duplicate_scene(scene_id, payload, user_id=user_id)
    return SceneResponse(data=scene, request_id=trace_request_id())


@router.post(
    "/{scene_id}/publish",
    operation_id="publishScene",
    response_model=SceneResponse,
    summary="发布场景（draft → published）",
    responses={**COMMON_ERRORS},
)
async def publish_scene(
    scene_id: Annotated[UUID, Path(description="场景 ID")],
    service: Annotated[SceneService, Depends(get_scene_service)],
    _user_id: Annotated[str, Depends(require_execute_permission)],
    payload: ScenePublishRequest | None = None,
) -> SceneResponse:
    """发布场景（仅 draft 可发布；发布前再次校验 4.2.2 结构完整性）。"""
    scene = await service.publish_scene(scene_id, payload)
    return SceneResponse(data=scene, request_id=trace_request_id())


__all__ = ["router"]