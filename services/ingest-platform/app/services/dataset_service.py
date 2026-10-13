"""数据集导出服务（§8.2）。"""
from __future__ import annotations

from typing import Any

from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import Dataset
from app.producers.platform_events import PlatformEventProducer
from app.repositories.clips import ClipRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage, dataset_key
from app.storage.parquet_writer import build_snapshot_line

logger = get_logger("app.services.dataset_service")


class DatasetService:
    """训练数据集导出（§8.2）。"""

    def __init__(
        self,
        settings: Settings,
        storage: LakeStorage,
        db_mgr: DatabaseSessionManager,
        producer: PlatformEventProducer,
    ) -> None:
        self._settings = settings
        self._storage = storage
        self._db_mgr = db_mgr
        self._producer = producer

    async def create_dataset(
        self,
        dataset_id: str,
        version: str,
        selector: dict[str, Any],
        fmt: str = "webdataset",
        shuffle_seed: int | None = None,
    ) -> dict[str, Any]:
        """创建数据集定义并返回 pending 状态（§8.2 步骤1）。"""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        async with self._db_mgr.session() as session:
            stmt = pg_insert(Dataset).values(
                dataset_id=dataset_id, version=version,
                selector=selector, format=fmt, shuffle_seed=shuffle_seed,
            ).on_conflict_do_nothing(index_elements=["dataset_id", "version"])
            await session.execute(stmt)
        return {"dataset_id": dataset_id, "version": version, "state": "pending"}

    async def export(self, dataset_id: str, version: str) -> dict[str, Any]:
        """执行数据集导出（§8.2 步骤1-4）。"""
        from sqlalchemy import select, update
        async with self._db_mgr.session() as session:
            result = await session.execute(
                select(Dataset).where(
                    Dataset.dataset_id == dataset_id, Dataset.version == version
                )
            )
            ds: Dataset | None = result.scalars().first()
            if ds is None:
                raise ValueError(f"dataset not found: {dataset_id}/{version}")
            selector = ds.selector or {}

        clip_ids: list[int] = list(selector.get("clip_ids", []))
        if not clip_ids:
            query = selector.get("query", {})
            bag_id = query.get("bag_id")
            if bag_id:
                async with self._db_mgr.session() as session:
                    clip_repo = ClipRepository(session)
                    clips = await clip_repo.list_by_bag(
                        bag_id,
                        tags=query.get("tags") or None,
                        min_quality=query.get("min_qc", self._settings.qc_score_threshold),
                    )
                    clip_ids = [c.clip_id for c in clips]

        async with self._db_mgr.session() as session:
            clip_repo = ClipRepository(session)
            clips_data = await clip_repo.list_by_ids(clip_ids)

        # 2. 生成 snapshot.manifest.jsonl（步骤2）
        lines: list[str] = []
        for clip in clips_data:
            line = build_snapshot_line(
                clip.clip_id, clip.bag_id, clip.input_rev,
                [clip.manifest_key] if clip.manifest_key else [],
            )
            lines.append(line)

        # 3. 写入 manifest（步骤3）
        manifest_content = "\n".join(lines)
        manifest_key = dataset_key(dataset_id, version, "snapshot.manifest.jsonl")
        self._storage._put_object(
            self._settings.minio_bucket_dataset,
            manifest_key,
            manifest_content.encode("utf-8"),
            content_type="application/x-ndjson",
        )

        # 4. 更新 datasets 表 + 发事件（步骤4）
        async with self._db_mgr.session() as session:
            stmt = update(Dataset).where(
                Dataset.dataset_id == dataset_id, Dataset.version == version
            ).values(state="done", manifest_key=manifest_key, clip_count=len(clips_data))
            await session.execute(stmt)

        await self._producer.publish_dataset_ready(dataset_id, version, manifest_key, len(clips_data))
        logger.info("dataset_exported", dataset_id=dataset_id, clips=len(clips_data))
        return {"dataset_id": dataset_id, "version": version, "clip_count": len(clips_data)}

    async def list_datasets(self) -> list[dict[str, Any]]:
        """列举所有数据集（GET /api/v1/datasets）。"""
        from sqlalchemy import select
        async with self._db_mgr.session() as session:
            result = await session.execute(
                select(Dataset).order_by(Dataset.created_at.desc()).limit(100)
            )
            items = list(result.scalars().all())
        return [
            {
                "dataset_id": d.dataset_id,
                "version": d.version,
                "state": d.state,
                "clip_count": d.clip_count,
                "manifest_key": d.manifest_key,
            }
            for d in items
        ]


__all__ = ["DatasetService"]
