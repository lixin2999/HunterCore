"""/api/v1/data/files 路由（5.5 节：预签名直传 + 完整性校验 + 清单）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from hunter_common.exceptions import InvalidParameterError

from app.core.dependencies import (
    UploaderPrincipal,
    require_execute_permission,
    require_read_permission,
    require_uploader_identity,
    trace_request_id,
)
from app.schemas.common import VEHICLE_ID_PATTERN, FileDataType, UploadBucket
from app.schemas.files import (
    FileCompleteRequest,
    FileCompleteResponse,
    FileDeleteRequest,
    FileDeleteResponse,
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
    date: Annotated[
        str | None,
        Query(pattern=r"^\d{4}-\d{2}-\d{2}$", description="上传日期（YYYY-MM-DD，UTC；可选过滤）"),
    ] = None,
    data_type: Annotated[FileDataType | None, Query(description="数据类型（可选过滤）")] = None,
    marker: Annotated[str | None, Query(max_length=1024, description="分页游标（上一页 next_marker）")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="单页上限（≤200，默认 50）")] = 50,
) -> FileListResponse:
    """文件清单（对象键前缀列举 + marker 游标；每项即时签发 15 分钟下载 URL）。

    R1（V1.21.16）：此前实现私增 ``prefix`` 且漏了契约要求的 ``date``/``data_type``，
    前端按契约发 date/data_type 被 FastAPI 静默忽略 ⇒ 筛查形同虚设；本版与契约对齐。
    """
    data = await service.list_files(
        vehicle_id=vehicle_id,
        bucket=bucket.value,
        date=date,
        data_type=data_type.value if data_type else None,
        marker=marker,
        limit=limit,
    )
    return FileListResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.post(
    "/files/delete",
    response_model=FileDeleteResponse,
    summary="批量删除对象（数据清理）",
    response_model_exclude_none=False,
)
async def delete_files(
    payload: FileDeleteRequest,
    service: Annotated[FileService, Depends(get_file_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
) -> FileDeleteResponse:
    """批量删除 MinIO 对象（清理无效/过期记录，如 DVR 修复前的 0 B 孤儿 sidecar）。

    权限 data:execute（与事件确认同档）；逐次审计（operator + 对象键）。幂等：
    不存在的对象计入 deleted（DeleteObjects 语义）。

    **hunter-video 桶（V1.21.22）**：按会话配对删除（`.json`+`.mp4` 同删）且默认启用
    有效录像护栏——对应视频 >0 B 的会话不删（回带于 ``skipped``），除非 ``include_valid=true``；
    避免误删有效录像、不留失联对象。其余桶保持裸对象删除语义。
    """
    data = await service.delete_files(
        bucket=payload.bucket.value,
        object_keys=payload.object_keys,
        user_id=user_id,
        include_valid=payload.include_valid,
    )
    return FileDeleteResponse(code=0, message="success", data=data, request_id=trace_request_id())


@router.post(
    "/files/presign",
    response_model=FilePresignResponse,
    summary="请求上传预签名（车端）",
    response_model_exclude_none=False,
)
async def presign_upload(
    payload: FilePresignRequest,
    service: Annotated[FileService, Depends(get_file_service)],
    principal: Annotated[UploaderPrincipal, Depends(require_uploader_identity)],
) -> FilePresignResponse:
    """预签名直传（method=put 单地址 / multipart 分片地址；有效期 1 小时）。

    双主体（契约 x-hunter-device-ingress）：设备主体经证书 CN 一致性校验后仅能为本车申请；
    控制台主体为代传路线（data:read）。
    """
    data = await service.presign(payload, device_vehicle_id=principal.device_vehicle_id)
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
    principal: Annotated[UploaderPrincipal, Depends(require_uploader_identity)],
) -> FileCompleteResponse:
    """上传完成通知（size/md5/sha256 一致性校验通过后发布 sensor_file 通知）。

    设备主体额外强校 vehicle_id == 证书 CN（不符 1002）；审计日志 operator 记录主体 user_id。
    """
    if payload.sha256 and len(payload.sha256) != 64:
        # Pydantic pattern 已兜底；此处防御式校验避免绕过（不可达分支，双保险）
        raise InvalidParameterError(message="sha256 必须为 64 位小写十六进制")
    data = await service.complete(
        payload, user_id=principal.user_id, device_vehicle_id=principal.device_vehicle_id
    )
    return FileCompleteResponse(code=0, message="success", data=data, request_id=trace_request_id())
