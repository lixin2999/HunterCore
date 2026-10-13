"""产物预签名路由（§8.1 GET /api/v1/artifacts）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from hunter_common.responses import success_response

from app.core.dependencies import ReadUser, SessionDep, get_storage
from app.repositories.artifacts import ArtifactRepository
from app.storage.minio_client import LakeStorage

router = APIRouter(prefix="/api/v1/artifacts", tags=["artifacts"])


@router.get("/{bag_id}")
async def list_artifacts(
    bag_id: str,
    session: SessionDep,
    _user: ReadUser,
) -> dict:
    """列举 bag 所有已注册产物（§8.1）。"""
    repo = ArtifactRepository(session)
    artifacts = await repo.list_by_bag(bag_id)
    return success_response(data={
        "bag_id": bag_id,
        "items": [
            {
                "object_key": a.object_key,
                "artifact_class": a.artifact_class.value,
                "upload_state": a.upload_state.value,
                "size_bytes": a.size_bytes,
                "sha256": a.sha256,
            }
            for a in artifacts
        ]
    }).model_dump()


@router.get("/presign/{bag_id}")
async def presign_artifact(
    bag_id: str,
    session: SessionDep,
    _user: ReadUser,
    object_key: Annotated[str, Query(description="对象完整 key（来自 /artifacts/{bag_id} 列表）")],
    storage: LakeStorage = Depends(get_storage),
) -> dict:
    """生成产物预签名下载 URL（§8.1 时效 10 min）。"""
    from fastapi import HTTPException
    repo = ArtifactRepository(session)
    artifact = await repo.get_by_key(object_key)
    if artifact is None or artifact.bag_id != bag_id:
        raise HTTPException(status_code=404, detail="产物不存在或不属于该 bag")

    # 根据 artifact_class 选择 bucket
    from app.config import settings
    from app.models.ingest import ArtifactClass
    if artifact.artifact_class in (ArtifactClass.TABLE,):
        bucket = settings.minio_bucket_parsed
    else:
        bucket = settings.minio_bucket_parsed

    url = storage.presigned_url(bucket, object_key)
    return success_response(data={
        "object_key": object_key,
        "url": url,
        "expires_in": settings.presigned_download_expire_seconds,
    }).model_dump()
