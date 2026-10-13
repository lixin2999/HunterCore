"""Bag 查询路由（§8.1 GET /api/v1/ingest/bags）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from hunter_common.responses import success_response

from app.core.dependencies import ReadUser, SessionDep
from app.models.ingest import BagState
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository, BagTopicRepository

router = APIRouter(prefix="/api/v1/ingest/bags", tags=["bags"])


@router.get("")
async def list_bags(
    session: SessionDep,
    _user: ReadUser,
    vehicle_id: Annotated[str | None, Query(description="车辆 ID 过滤")] = None,
    state: Annotated[str | None, Query(description="bag 状态过滤（如 done/parsed）")] = None,
    min_qc_score: Annotated[float | None, Query(ge=0, le=1, description="最低 QC 分数")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
) -> dict:
    """分页查询 bag 列表（§8.1）。"""
    bag_state: BagState | None = None
    if state:
        try:
            bag_state = BagState(state)
        except ValueError:
            pass  # 非法 state 忽略过滤

    repo = BagRepository(session)
    items, total = await repo.list_bags(
        vehicle_id=vehicle_id,
        state=bag_state,
        min_qc_score=min_qc_score,
        page=page,
        page_size=page_size,
    )
    bag_items = [
        {
            "bag_id": b.bag_id,
            "vehicle_id": b.vehicle_id,
            "state": b.state.value,
            "start_ns": b.start_ns,
            "end_ns": b.end_ns,
            "duration_s": float(b.duration_s) if b.duration_s else None,
            "size_bytes": b.size_bytes,
            "message_count": b.message_count,
            "qc_score": float(b.qc_score) if b.qc_score else None,
            "parse_status": b.parse_status,
            "progress": b.progress,
            "updated_at": b.updated_at.isoformat() if b.updated_at else None,
        }
        for b in items
    ]
    has_next = (page * page_size) < total
    return success_response(data={
        "meta": {"page": page, "page_size": page_size, "total": total, "has_next": has_next},
        "items": bag_items,
    }).model_dump()


@router.get("/{bag_id}")
async def get_bag(
    bag_id: str,
    session: SessionDep,
    _user: ReadUser,
) -> dict:
    """获取 bag 详情（含 topics + artifacts，§8.1）。"""
    bag_repo = BagRepository(session)
    topic_repo = BagTopicRepository(session)
    artifact_repo = ArtifactRepository(session)

    bag = await bag_repo.get_by_id(bag_id)
    if bag is None:
        from hunter_common.exceptions import ResourceNotFoundError
        raise ResourceNotFoundError(message=f"bag 不存在: {bag_id}")

    topics = await topic_repo.list_by_bag(bag_id)
    artifacts = await artifact_repo.list_by_bag(bag_id)

    return success_response(data={
        "bag_id": bag.bag_id,
        "vehicle_id": bag.vehicle_id,
        "state": bag.state.value,
        "start_ns": bag.start_ns,
        "end_ns": bag.end_ns,
        "duration_s": float(bag.duration_s) if bag.duration_s else None,
        "size_bytes": bag.size_bytes,
        "message_count": bag.message_count,
        "qc_score": float(bag.qc_score) if bag.qc_score else None,
        "parse_status": bag.parse_status,
        "progress": bag.progress,
        "updated_at": bag.updated_at.isoformat() if bag.updated_at else None,
        "manifest": bag.manifest,
        "topics": [
            {
                "topic": t.topic,
                "msg_type": t.msg_type,
                "message_count": t.message_count,
                "hz": float(t.hz) if t.hz is not None else None,
            }
            for t in topics
        ],
        "artifacts": [
            {
                "object_key": a.object_key,
                "artifact_class": a.artifact_class.value,
                "size_bytes": a.size_bytes,
                "upload_state": a.upload_state.value,
            }
            for a in artifacts
        ],
    }).model_dump()


@router.get("/{bag_id}/timeline")
async def get_bag_timeline(
    bag_id: str,
    session: SessionDep,
    _user: ReadUser,
) -> dict:
    """获取 bag topic 时间线（§8.1 GET /bags/{bag_id}/timeline）。"""
    topic_repo = BagTopicRepository(session)
    topics = await topic_repo.list_by_bag(bag_id)
    return success_response(data={
        "bag_id": bag_id,
        "topics": [
            {
                "topic": t.topic,
                "msg_type": t.msg_type,
                "message_count": t.message_count,
                "start_ns": t.start_ns,
                "end_ns": t.end_ns,
                "hz": float(t.hz) if t.hz is not None else None,
            }
            for t in topics
        ],
    }).model_dump()
