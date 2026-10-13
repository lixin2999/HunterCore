"""数据集路由（§8.2 POST/GET /api/v1/datasets）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from hunter_common.responses import success_response

from app.core.dependencies import ExecuteUser, ReadUser, SessionDep, get_dataset_service
from app.schemas.schemas import DatasetCreateReq
from app.services.dataset_service import DatasetService

router = APIRouter(prefix="/api/v1/datasets", tags=["datasets"])


@router.post("")
async def create_dataset(
    body: DatasetCreateReq,
    _user: ExecuteUser,
    svc: Annotated[DatasetService, Depends(get_dataset_service)],
) -> dict:
    """创建数据集定义并异步触发导出（§8.2）。"""
    result = await svc.create_dataset(
        dataset_id=body.dataset_id,
        version=body.version,
        selector=body.selector,
        fmt=body.format,
        shuffle_seed=body.shuffle_seed,
    )
    return success_response(data=result).model_dump()


@router.get("")
async def list_datasets(
    _user: ReadUser,
    svc: Annotated[DatasetService, Depends(get_dataset_service)],
) -> dict:
    """列举数据集（§8.1 GET /api/v1/datasets）。"""
    items = await svc.list_datasets()
    return success_response(data={"items": items}).model_dump()


@router.get("/{dataset_id}/{version}")
async def get_dataset(
    dataset_id: str,
    version: str,
    _user: ReadUser,
    session: SessionDep,
) -> dict:
    """查询数据集导出状态（§8.1）。"""
    from sqlalchemy import select
    from app.models.ingest import Dataset

    result = await session.execute(
        select(Dataset).where(
            Dataset.dataset_id == dataset_id,
            Dataset.version == version,
        )
    )
    ds = result.scalars().first()

    if ds is None:
        from hunter_common.exceptions import ResourceNotFoundError
        raise ResourceNotFoundError(message=f"dataset 不存在: {dataset_id}/{version}")

    return success_response(data={
        "dataset_id": ds.dataset_id,
        "version": ds.version,
        "state": ds.state,
        "clip_count": ds.clip_count,
        "manifest_key": ds.manifest_key,
    }).model_dump()


@router.post("/{dataset_id}/{version}/export")
async def trigger_export(
    dataset_id: str,
    version: str,
    _user: ExecuteUser,
    svc: Annotated[DatasetService, Depends(get_dataset_service)],
) -> dict:
    """手动触发数据集导出（§8.2）。"""
    result = await svc.export(dataset_id, version)
    return success_response(data=result).model_dump()
