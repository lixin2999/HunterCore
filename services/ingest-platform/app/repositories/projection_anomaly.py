"""投影异常记录 Repository（§5.2 层2 乱序兜底）。"""
from __future__ import annotations

from typing import Any

from app.models.ingest import ProjectionAnomaly


class ProjectionAnomalyRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def record(self, data: dict[str, Any]) -> None:
        """记录乱序投影异常（rank 落后 2 级以上）。"""
        obj = ProjectionAnomaly(**data)
        self._session.add(obj)

    async def list_by_bag(self, bag_id: str, *, limit: int = 50) -> list[ProjectionAnomaly]:
        from sqlalchemy import select
        stmt = (
            select(ProjectionAnomaly)
            .where(ProjectionAnomaly.bag_id == bag_id)
            .order_by(ProjectionAnomaly.logged_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


__all__ = ["ProjectionAnomalyRepository"]
