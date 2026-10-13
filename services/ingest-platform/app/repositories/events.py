"""事件仓 Repository（§5.2 层1：幂等去重）。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import IngestEvent


class IngestEventRepository:
    """事件仓专用 Repository（不继承 BaseRepository，因分区表复合主键特殊性）。

    核心方法：
    - :meth:`insert_dedup`: 幂等插入（ON CONFLICT DO NOTHING），返回 True = 新事件应处理。
    """

    def __init__(self, session: Any) -> None:
        self._session = session

    async def insert_dedup(self, event: dict[str, Any]) -> bool:
        """幂等插入事件（§5.2 层1）。

        Returns:
            True = 新事件（rowcount=1），应继续下游处理；
            False = 重复事件（rowcount=0），仅 commit offset 结束。
        """
        stmt = (
            pg_insert(IngestEvent)
            .values(**event)
            .on_conflict_do_nothing(index_elements=["event_id", "received_at"])
        )
        result = await self._session.execute(stmt)
        return bool(result.rowcount == 1)

    async def list_by_bag(self, bag_id: str, *, limit: int = 200) -> list[IngestEvent]:
        stmt = (
            select(IngestEvent)
            .where(IngestEvent.bag_id == bag_id)
            .order_by(IngestEvent.received_at.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_max_seq(self, bag_id: str) -> int:
        """返回 bag 当前已知最大 seq（投影守卫用）。"""
        stmt = select(IngestEvent.seq).where(IngestEvent.bag_id == bag_id).order_by(
            IngestEvent.seq.desc()
        ).limit(1)
        result = await self._session.execute(stmt)
        row = result.first()
        return int(row[0]) if row and row[0] is not None else 0

    async def count_by_type(self, event_type: str, *, since: str) -> int:
        stmt = select(text("COUNT(*)")).select_from(IngestEvent.__table__).where(
            IngestEvent.event_type == event_type,
            IngestEvent.received_at >= text(":since"),
        ).params(since=since)
        result = await self._session.execute(stmt)
        return int(result.scalar() or 0)


__all__ = ["IngestEventRepository"]
