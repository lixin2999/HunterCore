"""Clip 检索路由（§8.1 GET /api/v1/clips）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from hunter_common.responses import success_response

from app.core.dependencies import ReadUser, SessionDep
from app.repositories.clips import ClipRepository

router = APIRouter(prefix="/api/v1/clips", tags=["clips"])


@router.get("")
async def list_clips(
    session: SessionDep,
    _user: ReadUser,
    bag_id: Annotated[str, Query(description="bag ID（必选）")],
    tags: Annotated[str | None, Query(description="逗号分隔标签过滤")] = None,
    min_quality: Annotated[float | None, Query(ge=0, le=1)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
) -> dict:
    """检索 bag 场景切片（§7.3 + §8.1）。"""
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
    repo = ClipRepository(session)
    clips = await repo.list_by_bag(bag_id, tags=tag_list, min_quality=min_quality, limit=limit)
    return success_response(data={
        "items": [
            {
                "clip_id": c.clip_id,
                "bag_id": c.bag_id,
                "input_rev": c.input_rev,
                "start_ns": c.start_ns,
                "end_ns": c.end_ns,
                "source": c.source.value if hasattr(c.source, "value") else str(c.source),
                "tags": c.tags or [],
                "scene_label": c.scene_label,
                "quality": float(c.quality) if c.quality else None,
                "manifest_key": c.manifest_key,
            }
            for c in clips
        ]
    }).model_dump()
