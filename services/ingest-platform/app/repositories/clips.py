"""场景切片与标签 Repository（§6.2 + §7.3）。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import Clip, ClipSource, Label


class ClipRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert(self, clip_data: dict[str, Any]) -> Clip:
        """幂等创建/更新 clip（§7.3 确定性 object key 保证重复计算幂等）。"""
        stmt = pg_insert(Clip).values(**clip_data)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_clips_bag_rev_span",
            set_={k: v for k, v in clip_data.items()
                  if k not in ("bag_id", "input_rev", "start_ns", "end_ns", "source")},
        ).returning(Clip)
        result = await self._session.execute(stmt)
        clip: Clip = result.scalars().one()
        return clip

    async def list_by_bag(
        self,
        bag_id: str,
        *,
        tags: list[str] | None = None,
        min_quality: float | None = None,
        limit: int = 200,
    ) -> list[Clip]:
        """检索 bag 切片（§8.1 GET /api/v1/clips）。"""
        stmt = select(Clip).where(Clip.bag_id == bag_id)
        if tags:
            stmt = stmt.where(Clip.tags.overlap(tags))
        if min_quality is not None:
            stmt = stmt.where(Clip.quality >= min_quality)
        stmt = stmt.order_by(Clip.start_ns.asc()).limit(limit)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, clip_id: int) -> Clip | None:
        stmt = select(Clip).where(Clip.clip_id == clip_id)
        result = await self._session.execute(stmt)
        c: Clip | None = result.scalars().first()
        return c

    async def list_by_ids(self, clip_ids: list[int]) -> list[Clip]:
        stmt = select(Clip).where(Clip.clip_id.in_(clip_ids))
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


class LabelRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert(
        self,
        bag_id: str,
        key: str,
        value: dict[str, Any],
        *,
        clip_id: int = 0,
        origin: str = "manual",
    ) -> None:
        stmt = pg_insert(Label).values(
            bag_id=bag_id, clip_id=clip_id, key=key, value=value, origin=origin
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["bag_id", "clip_id", "key", "origin"],
            set_={"value": value},
        )
        await self._session.execute(stmt)

    async def list_by_bag(self, bag_id: str) -> list[Label]:
        stmt = select(Label).where(Label.bag_id == bag_id)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


__all__ = ["ClipRepository", "LabelRepository"]
