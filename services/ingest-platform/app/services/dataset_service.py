"""数据集导出服务（§8.2）。

导出流程（§8.2 步骤 1-4）：
1. 解析 selector → clip 集合
2. 生成 snapshot.manifest.jsonl（索引，媒体以引用计）
3. 按 format 打包分片：
   - webdataset → stdlib tarfile 分片（每片 ≤ dataset_shard_max_clips 个 clip 的
     clip.manifest.json + 该片 snapshot 索引）；真实按样本嵌入媒体字节属 M5 待办；
   - lance → snapshot.parquet（列式快照；真 Lance 格式需 pylance，M5 待办）；
4. 写 datasets.state 与发 dataset.ready 事件

任务化（§9 / 审查 #9）：create_dataset 新增导出任务（type=EXPORT）交 orchestrator
推进，进程重启后由 lease 队列续跑；export 本体可被 ExportWorker 或手动端点调用。
"""
from __future__ import annotations

import io
import json
import tarfile
from typing import Any

import pyarrow as pa
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import Dataset, TaskType
from app.producers.platform_events import PlatformEventProducer
from app.repositories.clips import ClipRepository
from app.repositories.tasks import TaskRepository
from app.storage.minio_client import LakeStorage, dataset_key
from app.storage.parquet_writer import build_snapshot_line, write_records_parquet

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
        """创建数据集定义并注册 EXPORT 任务（§8.2 步骤1 + §9 任务化）。"""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        async with self._db_mgr.session() as session:
            stmt = (
                pg_insert(Dataset)
                .values(
                    dataset_id=dataset_id, version=version,
                    selector=selector, format=fmt, shuffle_seed=shuffle_seed,
                    state="pending",
                )
                .on_conflict_do_nothing(index_elements=["dataset_id", "version"])
            )
            result = await session.execute(stmt)
            is_new = bool(result.rowcount == 1)
            if is_new:
                # 新数据集 → 注册 EXPORT 任务（payload 携 dataset_id/version，§9）
                task_repo = TaskRepository(session)
                await task_repo.create(
                    None, None, TaskType.EXPORT,
                    payload={"dataset_id": dataset_id, "version": version, "format": fmt},
                )
        return {
            "dataset_id": dataset_id,
            "version": version,
            "state": "pending",
            "export_task_enqueued": is_new,
        }

    async def _set_state(self, dataset_id: str, version: str, state: str) -> None:
        from sqlalchemy import update
        async with self._db_mgr.session() as session:
            await session.execute(
                update(Dataset)
                .where(Dataset.dataset_id == dataset_id, Dataset.version == version)
                .values(state=state, updated_at=sa_now())
            )

    async def export(self, dataset_id: str, version: str) -> dict[str, Any]:
        """执行数据集导出（§8.2 步骤1-4）。"""
        from sqlalchemy import select
        # 步骤 0：置 running（幂等推进，崩溃后可重入）
        await self._set_state(dataset_id, version, "running")

        try:
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
                fmt = ds.format or "webdataset"

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

            # 步骤 2：snapshot.manifest.jsonl（索引）
            lines: list[str] = []
            rows: list[dict[str, Any]] = []
            for clip in clips_data:
                keys = [clip.manifest_key] if clip.manifest_key else []
                line = build_snapshot_line(
                    clip.clip_id, clip.bag_id, clip.input_rev, keys,
                )
                lines.append(line)
                rows.append({
                    "clip_id": clip.clip_id,
                    "bag_id": clip.bag_id,
                    "input_rev": clip.input_rev,
                    "start_ns": clip.start_ns,
                    "end_ns": clip.end_ns,
                    "manifest_key": clip.manifest_key or "",
                })

            manifest_content = "\n".join(lines)
            manifest_key = dataset_key(dataset_id, version, "snapshot.manifest.jsonl")
            await self._storage.put_dataset_object(
                manifest_key, manifest_content.encode("utf-8"),
                content_type="application/x-ndjson",
            )

            # 步骤 3：按 format 打包分片
            shard_keys = await self._pack(dataset_id, version, fmt, clips_data, rows)

            # 步骤 4：置 done + 发事件
            from sqlalchemy import update
            async with self._db_mgr.session() as session:
                await session.execute(
                    update(Dataset)
                    .where(Dataset.dataset_id == dataset_id, Dataset.version == version)
                    .values(
                        state="done", manifest_key=manifest_key,
                        clip_count=len(clips_data), updated_at=sa_now(),
                    )
                )

            await self._producer.publish_dataset_ready(
                dataset_id, version, manifest_key, len(clips_data)
            )
            logger.info("dataset_exported", dataset_id=dataset_id, clips=len(clips_data), shards=len(shard_keys))
            return {
                "dataset_id": dataset_id, "version": version,
                "clip_count": len(clips_data),
                "manifest_key": manifest_key,
                "shard_keys": shard_keys,
            }
        except Exception:
            await self._set_state(dataset_id, version, "failed")
            raise

    async def _pack(
        self,
        dataset_id: str,
        version: str,
        fmt: str,
        clips_data: list[Any],
        rows: list[dict[str, Any]],
    ) -> list[str]:
        """按 format 打包分片，返回上传后的 shard object keys（§8.2 步骤3）。"""
        shard_keys: list[str] = []
        max_clips = max(1, self._settings.dataset_shard_max_clips)

        if fmt == "lance":
            # Lance 替代：列式 snapshot.parquet（真 Lance 需 pylance，M5 待办）
            if rows:
                columns = [
                    ("clip_id", pa.int64()), ("bag_id", pa.string()),
                    ("input_rev", pa.string()), ("start_ns", pa.int64()),
                    ("end_ns", pa.int64()), ("manifest_key", pa.string()),
                ]
                buf = io.BytesIO()
                write_records_parquet(rows, columns, buf)
                key = dataset_key(dataset_id, version, "snapshot.parquet")
                await self._storage.put_dataset_object(
                    key, buf.getvalue(), content_type="application/octet-stream",
                )
                shard_keys.append(key)
            return shard_keys

        # webdataset：stdlib tar 分片，成员 = 各 clip.manifest.json + 该片 snapshot.jsonl
        for shard_idx in range(max(1, (len(clips_data) + max_clips - 1) // max_clips)):
            chunk = clips_data[shard_idx * max_clips:(shard_idx + 1) * max_clips]
            chunk_rows = rows[shard_idx * max_clips:(shard_idx + 1) * max_clips]
            if not chunk:
                continue
            tar_buf = io.BytesIO()
            with tarfile.open(fileobj=tar_buf, mode="w") as tar:
                for clip, row in zip(chunk, chunk_rows, strict=True):
                    member_body = await self._read_clip_manifest(clip)
                    info = tarfile.TarInfo(name=f"{row['clip_id']}.json")
                    info.size = len(member_body)
                    tar.addfile(info, io.BytesIO(member_body))
                index_body = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in chunk_rows).encode("utf-8")
                info = tarfile.TarInfo(name="snapshot.jsonl")
                info.size = len(index_body)
                tar.addfile(info, io.BytesIO(index_body))
            key = dataset_key(dataset_id, version, f"shard-{shard_idx:05d}.tar")
            await self._storage.put_dataset_object(
                key, tar_buf.getvalue(), content_type="application/x-tar",
            )
            shard_keys.append(key)
        return shard_keys

    async def _read_clip_manifest(self, clip: Any) -> bytes:
        """读取 clip.manifest.json 内容（缺失时回退最小 manifest）。"""
        if clip.manifest_key:
            try:
                return await self._storage.get_derived_object(clip.manifest_key)
            except Exception as exc:  # noqa: BLE001
                logger.debug("clip_manifest_read_failed", key=clip.manifest_key, error=str(exc))
        fallback = {
            "clip_id": clip.clip_id, "bag_id": clip.bag_id, "input_rev": clip.input_rev,
            "start_ns": clip.start_ns, "end_ns": clip.end_ns,
            "source": str(clip.source), "tags": list(clip.tags or []),
        }
        return json.dumps(fallback, ensure_ascii=False, default=str).encode("utf-8")

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


def sa_now() -> Any:
    from sqlalchemy import func
    return func.now()


__all__ = ["DatasetService"]
