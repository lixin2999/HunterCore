"""产物目录 Repository（§6.2 artifacts 表）。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import Artifact, ArtifactClass, UploadState


class ArtifactRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert(self, object_key: str, fields: dict[str, Any]) -> None:
        """幂等注册产物（§6.2 object_key 为主键，重复上传只更新时间戳）。"""
        stmt = pg_insert(Artifact).values(object_key=object_key, **fields)
        update_set = {k: v for k, v in fields.items() if k != "object_key"}
        update_set["upload_state"] = UploadState.UPLOADED
        stmt = stmt.on_conflict_do_update(index_elements=["object_key"], set_=update_set)
        await self._session.execute(stmt)

    async def set_verified(self, object_key: str, *, sha256: str | None = None) -> None:
        sets: dict[str, Any] = {"upload_state": UploadState.VERIFIED}
        if sha256:
            sets["sha256"] = sha256
        stmt = update(Artifact).where(Artifact.object_key == object_key).values(**sets)
        await self._session.execute(stmt)

    async def set_missing(self, object_key: str) -> None:
        stmt = update(Artifact).where(Artifact.object_key == object_key).values(
            upload_state=UploadState.MISSING
        )
        await self._session.execute(stmt)

    async def set_corrupt(self, object_key: str) -> None:
        """核验不通过（如声明 size 与实际不符）→ 置 corrupt（§7.1 verify）。"""
        stmt = update(Artifact).where(Artifact.object_key == object_key).values(
            upload_state=UploadState.CORRUPT
        )
        await self._session.execute(stmt)

    async def get_by_key(self, object_key: str) -> Artifact | None:
        result = await self._session.execute(
            select(Artifact).where(Artifact.object_key == object_key)
        )
        a: Artifact | None = result.scalar_one_or_none()
        return a

    async def list_by_bag(self, bag_id: str, artifact_class: ArtifactClass | None = None) -> list[Artifact]:
        stmt = select(Artifact).where(Artifact.bag_id == bag_id)
        if artifact_class:
            stmt = stmt.where(Artifact.artifact_class == artifact_class)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def count_by_bag_class(self, bag_id: str) -> dict[ArtifactClass, int]:
        """按类型统计 bag 的产物数量（§7.1 就绪判定用）。"""
        stmt = (
            select(Artifact.artifact_class, func.count(Artifact.object_key))
            .where(Artifact.bag_id == bag_id)
            .group_by(Artifact.artifact_class)
        )
        result = await self._session.execute(stmt)
        return {row[0]: int(row[1]) for row in result.all()}

    async def has_class(self, bag_id: str, artifact_class: ArtifactClass) -> bool:
        stmt = select(text("1")).select_from(Artifact).where(
            Artifact.bag_id == bag_id,
            Artifact.artifact_class == artifact_class,
        ).limit(1)
        result = await self._session.execute(stmt)
        return result.first() is not None


__all__ = ["ArtifactRepository"]
