"""/api/v1/data/files 路由（5.5 节：预签名直传 + 完整性校验 + 清单）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from hunter_common.exceptions import InvalidParameterError

from app.core.dependencies import require_read_permission, trace_request_id
from app.schemas.common import VEHICLE_ID_PATTERN, UploadBucket
from app.schemas.files import (
    FileCompleteRequest,
    FileCompleteResponse,
    FileListResponse,
    FilePresignRequest,
    FilePresignResponse,
)
from app.services.files import FileService

router = APIRouter(prefix="/api/v1/data", tags=["files"])


def get_file_service(request: Request) -> FileService:
    """从 app.state 获取文件服务（lifespan 装配，测试可替换）。"""
    return request.app.state.file_service  # type: ignore[no-any-return]


@router.get(
    "/files",
    response_model=FileListResponse,
    summary="查询已上传文件清单",
    response_model_exclude_none=False,
)
async def list_files(
    service: Annotated[FileService, Depends(get_file_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    bucket: Annotated[UploadBucket, Query(description="对象存储桶（必选，仅上传类 Bucket）")],
    vehicle_id: Annotated[str | None, Query(pattern=VEHICLE_ID_PATTERN, description="车辆标识（可选过滤）")] = None,
    prefix: Annotated[str | None, Query(max_length=256, description="对象路径前缀（可选）")] = None,
    marker: Annotated[str | None, Query(max_length=256, description="分页游标（上一页 next_marker）")] = None,
    limit: Annotated[int, Query(ge=1, le=1000, description="单页上限（≤1000，默认 100）")] = 100,
) -> FileListResponse:
    """文件清单（prefix 列举 + marker 游标；每项即时签发 15 分钟下载 URL）。"""
    data = await service.list_files(
        vehicle_id=vehicle_id,
        bucket=bucket.value,
        prefix=prefix,
        marker=marker,
        limit=limit,
    )
    return FileListResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.post(
    "/files/presign",
    response_model=FilePresignResponse,
    summary="请求上传预签名（车端）",
    response_model_exclude_none=False,
)
async def presign_upload(
    payload: FilePresignRequest,
    service: Annotated[FileService, Depends(get_file_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> FilePresignResponse:
    """预签名直传（method=put 单地址 / multipart 分片地址；有效期 1 小时）。"""
    data = await service.presign(payload)
    return FilePresignResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.post(
    "/files/complete",
    response_model=FileCompleteResponse,
    summary="通知上传完成（车端）",
    response_model_exclude_none=False,
    responses={
        422: {"description": "完整性校验失败（6001）"},
    },
)
async def complete_upload(
    payload: FileCompleteRequest,
    service: Annotated[FileService, Depends(get_file_service)],
    user_id: Annotated[str, Depends(require_read_permission)],
) -> FileCompleteResponse:
    """上传完成通知（size/md5/sha256 一致性校验通过后发布 sensor_file 通知）。"""
    if payload.sha256 and len(payload.sha256) != 64:
        # Pydantic pattern 已兜底；此处防御式校验避免绕过（不可达分支，双保险）
        raise InvalidParameterError(message="sha256 必须为 64 位小写十六进制")
    data = await service.complete(payload, user_id=user_id)
    return FileCompleteResponse(code=0, message="success", data=data, request_id=trace_request_id())
