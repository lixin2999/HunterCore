"""/api/v1/ota/versions 路由（附录 D 限流：POST 单用户 5 QPS；publish 建议值 2 QPS）。"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from hunter_common.database.enums import OtaVersionStatus
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

from app.core.dependencies import (
    require_execute_permission,
    require_read_permission,
    trace_request_id,
    version_create_user,
    version_publish_user,
)
from app.schemas.common import MODEL_NAME_PATTERN
from app.schemas.versions import (
    OtaVersionCreateRequest,
    OtaVersionCreateResponse,
    OtaVersionDeprecateRequest,
    OtaVersionDetailResponse,
    OtaVersionListResponse,
    OtaVersionPublishRequest,
    OtaVersionPublishResponse,
)
from app.services.versions import VersionService

router = APIRouter(prefix="/api/v1/ota", tags=["versions"])

#: 业务端点统一错误响应声明（附录 A 错误码 → HTTP 状态映射见 core.error_handlers）
_COMMON_ERRORS = {
    422: {"description": "2001/2002/6001/6002/6003（参数/校验/门禁）"},
    404: {"description": "3001 资源不存在"},
    409: {"description": "3002/3003/4001/4002（冲突/状态不允许）"},
    401: {"description": "1001/1003 未认证"},
    403: {"description": "1002 无权限"},
    503: {"description": "5001 服务不可用（PostgreSQL/Redis/MinIO/Kafka）"},
    500: {"description": "5000 服务器内部错误"},
}


def get_version_service(request: Request) -> VersionService:
    """从 app.state 获取版本服务（lifespan 装配，测试可替换）。"""
    return request.app.state.version_service  # type: ignore[no-any-return]


@router.get(
    "/versions",
    operation_id="listOtaVersions",
    response_model=OtaVersionListResponse,
    summary="查询 OTA 版本仓库列表（分页）",
    responses={**_COMMON_ERRORS},
)
async def list_ota_versions(
    service: Annotated[VersionService, Depends(get_version_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    status: Annotated[OtaVersionStatus | None, Query(description="按版本状态过滤")] = None,
    release_type: Annotated[str | None, Query(max_length=32, description="按发布类型过滤")] = None,
    version_code: Annotated[int | None, Query(ge=1, description="精确查询版本编码")] = None,
    applicable_model: Annotated[
        str | None, Query(pattern=MODEL_NAME_PATTERN, description="适用车型过滤（GIN 包含匹配）")
    ] = None,
    version_name: Annotated[str | None, Query(max_length=64, description="版本名模糊匹配（ILIKE）")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE, description="每页条数（≤200）")] = DEFAULT_PAGE_SIZE,
) -> OtaVersionListResponse:
    """版本列表（release_time DESC NULLS LAST, version_code DESC；即时签发 15 分钟下载地址）。"""
    data = await service.list_versions(
        status=status,
        release_type=release_type,
        version_code=version_code,
        applicable_model=applicable_model,
        version_name=version_name,
        page=page,
        page_size=page_size,
    )
    return OtaVersionListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/versions",
    operation_id="createOtaVersion",
    response_model=OtaVersionCreateResponse,
    summary="创建 OTA 版本（草稿）+ 申请升级包上传预签名地址",
    responses={
        429: {"description": "附录 D 限流（单用户 5 QPS）+ Retry-After"},
        **_COMMON_ERRORS,
    },
)
async def create_ota_version(
    payload: OtaVersionCreateRequest,
    service: Annotated[VersionService, Depends(get_version_service)],
    user_id: Annotated[str, Depends(version_create_user)],
) -> OtaVersionCreateResponse:
    """创建版本草稿（唯一性 3002 / 防回滚 6003）并返回 1 小时上传预签名地址。"""
    data = await service.create_version(payload, user_id)
    return OtaVersionCreateResponse(data=data, request_id=trace_request_id())


@router.get(
    "/versions/{version_id}",
    operation_id="getOtaVersion",
    response_model=OtaVersionDetailResponse,
    summary="查询 OTA 版本详情",
    responses={**_COMMON_ERRORS},
)
async def get_ota_version(
    version_id: Annotated[UUID, Path(description="版本 ID（ota_svc.ota_versions.version_id）")],
    service: Annotated[VersionService, Depends(get_version_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> OtaVersionDetailResponse:
    """版本详情（含 task_count 引用评估与即时签发的 15 分钟下载预签名地址）。"""
    data = await service.get_version(version_id)
    return OtaVersionDetailResponse(data=data, request_id=trace_request_id())


@router.post(
    "/versions/{version_id}/publish",
    operation_id="publishOtaVersion",
    response_model=OtaVersionPublishResponse,
    summary="发布 OTA 版本（SHA-256 校验 + RSA-2048 验签 + version_code 单调性门禁）",
    responses={
        429: {"description": "建议限流 2 QPS（x-hunter-rate-limits.recommended）+ Retry-After"},
        **_COMMON_ERRORS,
    },
)
async def publish_ota_version(
    version_id: Annotated[UUID, Path(description="版本 ID")],
    service: Annotated[VersionService, Depends(get_version_service)],
    user_id: Annotated[str, Depends(version_publish_user)],
    payload: OtaVersionPublishRequest | None = None,
) -> OtaVersionPublishResponse:
    """发布（长耗时：单次流式 size/MD5/SHA-256 + RSA-2048 验签 + 防回滚门禁 → published）。"""
    note = payload.note if payload is not None else None
    data = await service.publish_version(version_id, note, user_id)
    return OtaVersionPublishResponse(data=data, request_id=trace_request_id())


@router.post(
    "/versions/{version_id}/deprecate",
    operation_id="deprecateOtaVersion",
    response_model=OtaVersionDetailResponse,
    summary="废弃 / 停用版本（published → deprecated / disabled）",
    responses={**_COMMON_ERRORS},
)
async def deprecate_ota_version(
    version_id: Annotated[UUID, Path(description="版本 ID")],
    payload: OtaVersionDeprecateRequest,
    service: Annotated[VersionService, Depends(get_version_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
) -> OtaVersionDetailResponse:
    """退役（无 DELETE 端点：DDL ON DELETE RESTRICT + OTA 可追溯要求）。"""
    data = await service.deprecate_version(version_id, payload, user_id)
    return OtaVersionDetailResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]
