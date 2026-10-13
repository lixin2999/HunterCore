"""派生资产 Repository（§6.2 derived_assets 表）。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import DerivedAsset, DerivedKind


class DerivedAssetRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert(
        self,
        bag_id: str,
        input_rev: str,
        kind: DerivedKind,
        object_key: str,
        meta: dict[str, Any],
    ) -> None:
        stmt = pg_insert(DerivedAsset).values(
            bag_id=bag_id, input_rev=input_rev, kind=kind,
            object_key=object_key, meta=meta,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["bag_id", "input_rev", "kind", "object_key"],
            set_={"meta": meta},
        )
        await self._session.execute(stmt)

    async def list_by_bag_rev(
        self, bag_id: str, input_rev: str, kind: DerivedKind | None = None
    ) -> list[DerivedAsset]:
        stmt = select(DerivedAsset).where(
            DerivedAsset.bag_id == bag_id,
            DerivedAsset.input_rev == input_rev,
        )
        if kind:
            stmt = stmt.where(DerivedAsset.kind == kind)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


__all__ = ["DerivedAssetRepository"]
