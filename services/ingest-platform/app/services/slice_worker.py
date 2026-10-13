"""slice-worker：场景切片提取（§7.3）。

三种来源：事件驱动、固定窗口、挖掘式（插件接口）。
输出：derived/v1/{bag_id}/{input_rev}/clips/ + clips 表 + clips.index.parquet

会话管理：execute() 每次调用独立开启 session。
"""
from __future__ import annotations

import io
from typing import Any

import numpy as np
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import ClipSource, DerivedKind, Task
from app.producers.platform_events import PlatformEventProducer
from app.repositories.bags import BagRepository
from app.repositories.clips import ClipRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage
from app.storage.parquet_writer import build_clips_index_schema, write_clips_index, write_records_parquet

logger = get_logger("app.services.slice_worker")


class SliceWorker:
    """场景切片 Worker（§7.3）。"""

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

    async def execute(self, task: Task) -> dict[str, Any]:
        bag_id = task.bag_id or ""
        input_rev = task.input_rev or ""

        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            clip_repo = ClipRepository(session)
            derived_repo = DerivedAssetRepository(session)

            bag = await bag_repo.get_by_id(bag_id)
            if bag is None:
                raise ValueError(f"bag not found: {bag_id}")

            # 读取 frame_index 时间轴
            frame_index_key = f"derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet"
            try:
                fi_bytes = self._storage._get_object_bytes(
                    self._settings.minio_bucket_derived, frame_index_key
                )
                import pyarrow.parquet as pq
                fi_table = pq.read_table(io.BytesIO(fi_bytes))
                anchor_ts: np.ndarray = fi_table.column("ts_ns").to_numpy(zero_copy_only=False)
            except Exception as exc:
                raise RuntimeError(f"frame_index not available for {bag_id}: {exc}") from exc

            if anchor_ts.size == 0:
                return {"clip_count": 0, "reason": "no_frames"}

            clips: list[dict[str, Any]] = []

            # 1. 固定窗口切片（§7.3 来源2）
            window_clips = self._slice_by_window(anchor_ts, bag_id, input_rev)
            clips.extend(window_clips)

            # 2. 事件驱动切片（§7.3 来源1，从 bag manifest 中的事件标记提取）
            event_clips = self._slice_by_events(anchor_ts, bag, bag_id, input_rev)
            clips.extend(event_clips)

            # 写 clips.index.parquet
            if clips:
                buf = io.BytesIO()
                write_clips_index(clips, buf)
                clips_index_key = self._storage.put_derived_bytes(
                    bag_id, input_rev, "clips/clips.index.parquet",
                    buf.getvalue(), content_type="application/octet-stream",
                )
                await derived_repo.upsert(bag_id, input_rev, DerivedKind.CLIP, clips_index_key, {
                    "clip_count": len(clips),
                })

            # 为每个 clip 写 clip.manifest.json + 入库 clips 表
            for clip_data in clips:
                manifest_key = self._write_clip_manifest(bag_id, input_rev, clip_data)
                clip_data["manifest_key"] = manifest_key
                saved_clip = await clip_repo.upsert(clip_data)
                # 回填 clip_id（upsert 返回带 ID 的对象）
                clip_data["clip_id"] = saved_clip.clip_id
                await self._producer.publish_clip_created(
                    bag_id, saved_clip.clip_id, clip_data.get("tags", []),
                    clip_data["start_ns"], clip_data["end_ns"],
                )

        logger.info("slice_done", bag_id=bag_id, clip_count=len(clips))
        return {"clip_count": len(clips)}

    def _slice_by_window(
        self, anchor_ts: np.ndarray, bag_id: str, input_rev: str
    ) -> list[dict[str, Any]]:
        """固定窗口切片（§7.3 来源2：window_s=10, overlap_s=2）。"""
        window_ns = self._settings.slice_window_s * 1_000_000_000
        step_ns = (self._settings.slice_window_s - self._settings.slice_overlap_s) * 1_000_000_000
        clips: list[dict[str, Any]] = []
        start = int(anchor_ts[0])
        end_all = int(anchor_ts[-1])

        while start + window_ns <= end_all + step_ns:
            end = start + window_ns
            mask = (anchor_ts >= start) & (anchor_ts < end)
            if mask.sum() >= 2:
                clips.append({
                    "bag_id": bag_id,
                    "input_rev": input_rev,
                    "start_ns": start,
                    "end_ns": end,
                    "source": ClipSource.WINDOW,
                    "tags": [],
                    "scene_label": None,
                    "quality": None,
                })
            start += step_ns
        return clips

    def _slice_by_events(
        self, anchor_ts: np.ndarray, bag: Any, bag_id: str, input_rev: str
    ) -> list[dict[str, Any]]:
        """事件驱动切片（§7.3 来源1：急刹/告警等）。"""
        before_ns = self._settings.slice_event_before_s * 1_000_000_000
        after_ns = self._settings.slice_event_after_s * 1_000_000_000
        clips: list[dict[str, Any]] = []

        manifest = bag.manifest or {}
        event_marks: list[dict[str, Any]] = list(manifest.get("event_marks", []))
        for em in event_marks:
            t_ns = int(em.get("ts_ns", 0))
            if not t_ns:
                continue
            start = max(int(anchor_ts[0]), t_ns - before_ns)
            end = min(int(anchor_ts[-1]), t_ns + after_ns)
            if end > start:
                clips.append({
                    "bag_id": bag_id,
                    "input_rev": input_rev,
                    "start_ns": start,
                    "end_ns": end,
                    "source": ClipSource.EVENT,
                    "tags": list(em.get("tags", [])),
                    "scene_label": em.get("label"),
                    "quality": None,
                })
        return clips

    def _write_clip_manifest(
        self, bag_id: str, input_rev: str, clip_data: dict[str, Any]
    ) -> str:
        """写 clip.manifest.json（§7.3），返回 object key。"""
        clip_id = clip_data.get("clip_id", 0)
        start = clip_data["start_ns"]
        end = clip_data["end_ns"]
        suffix = f"clips/clip_{start}_{end}/clip.manifest.json"
        manifest_body = {
            "clip_id": clip_id,
            "bag_id": bag_id,
            "input_rev": input_rev,
            "start_ns": start,
            "end_ns": end,
            "anchor_camera": self._settings.anchor_camera,
            "source": str(clip_data.get("source", "")),
            "tags": clip_data.get("tags", []),
        }
        return self._storage.put_derived_json(bag_id, input_rev, suffix, manifest_body)


__all__ = ["SliceWorker"]
