"""slice-worker：场景切片提取（§7.3）。

三种来源：
- 固定窗口（来源2）
- 事件驱动（来源1 车端 manifest.event_marks）
- 服务端规则引擎挖掘（来源1 mined，§7.3：从 IMU 加速度序列挖掘急刹/碰撞，source='mined'）
输出：derived/v1/{bag_id}/{input_rev}/clips/ + clips 表 + clips.index.parquet

会话管理：execute() 每次调用独立开启 session。
存储 IO：全 async（不阻塞事件循环）。
"""
from __future__ import annotations

import io
from typing import Any

import numpy as np
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import ArtifactClass, ClipSource, DerivedKind, Task
from app.producers.platform_events import PlatformEventProducer
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository
from app.repositories.clips import ClipRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage
from app.storage.parquet_writer import write_clips_index

logger = get_logger("app.services.slice_worker")

_GNSS_GRAVITY = 9.8  # m/s²，去重力后的线加速度


def _mine_event_timestamps(
    stamp_ns: np.ndarray,
    ax: np.ndarray,
    ay: np.ndarray,
    az: np.ndarray,
    brake_th: float,
    collision_th: float,
) -> list[dict[str, Any]]:
    """服务端规则引擎（§7.3 来源1）：从 IMU 加速度序列挖掘急刹/碰撞事件。

    去重力合加速度 a_mag = sqrt(ax² + ay² + (az-9.8)²)；超阈连续段聚为一个事件，
    取峰值时刻；峰值 ≥collision_th 判 collision，否则 ≥brake_th 判 hard_brake。

    Returns:
        [{"ts_ns": int, "kind": "collision"|"hard_brake", "peak_mag": float}]
    """
    n = int(min(len(stamp_ns), len(ax), len(ay), len(az)))
    if n == 0:
        return []
    ax = np.asarray(ax[:n], dtype=np.float64)
    ay = np.asarray(ay[:n], dtype=np.float64)
    az = np.asarray(az[:n], dtype=np.float64)
    a_mag = np.sqrt(ax ** 2 + ay ** 2 + (az - _GNSS_GRAVITY) ** 2)
    a_mag = np.nan_to_num(a_mag, nan=0.0)

    events: list[dict[str, Any]] = []
    over = a_mag >= brake_th
    i = 0
    while i < n:
        if not over[i]:
            i += 1
            continue
        j = i
        while j < n and over[j]:
            j += 1
        seg = slice(i, j)
        peak_rel = int(np.argmax(a_mag[seg]))
        peak_mag = float(a_mag[seg][peak_rel])
        peak_idx = i + peak_rel
        kind = "collision" if peak_mag >= collision_th else "hard_brake"
        events.append({
            "ts_ns": int(stamp_ns[peak_idx]),
            "kind": kind,
            "peak_mag": round(peak_mag, 3),
        })
        i = j
    return events


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
            artifact_repo = ArtifactRepository(session)

            bag = await bag_repo.get_by_id(bag_id)
            if bag is None:
                raise ValueError(f"bag not found: {bag_id}")
            table_artifacts = await artifact_repo.list_by_bag(bag_id, ArtifactClass.TABLE)

        # 读取 frame_index 时间轴（derived 层，async）
        frame_index_key = f"derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet"
        try:
            fi_bytes = await self._storage.get_derived_object(frame_index_key)
            import pyarrow.parquet as pq
            fi_table = pq.read_table(io.BytesIO(fi_bytes))
            anchor_ts: np.ndarray = fi_table.column("ts_ns").to_numpy(zero_copy_only=False)
        except Exception as exc:
            raise RuntimeError(f"frame_index not available for {bag_id}: {exc}") from exc

        if anchor_ts.size == 0:
            return {"clip_count": 0, "reason": "no_frames"}

        clips: list[dict[str, Any]] = []

        # 1. 固定窗口切片（§7.3 来源2）
        clips.extend(self._slice_by_window(anchor_ts, bag_id, input_rev))

        # 2. 事件驱动切片（§7.3 来源1，车端 manifest 事件标记）
        clips.extend(self._slice_by_events(anchor_ts, bag, bag_id, input_rev))

        # 3. 服务端规则引擎挖掘（§7.3 来源1 mined，急刹/碰撞）
        if self._settings.rule_mining_enabled:
            mined = await self._mine_events(anchor_ts, table_artifacts, bag_id, input_rev)
            clips.extend(mined)

        async with self._db_mgr.session() as session:
            clip_repo = ClipRepository(session)
            derived_repo = DerivedAssetRepository(session)

            # 写 clips.index.parquet
            if clips:
                buf = io.BytesIO()
                write_clips_index(clips, buf)
                clips_index_key = await self._storage.put_derived_bytes(
                    bag_id, input_rev, "clips/clips.index.parquet",
                    buf.getvalue(), content_type="application/octet-stream",
                )
                await derived_repo.upsert(bag_id, input_rev, DerivedKind.CLIP, clips_index_key, {
                    "clip_count": len(clips),
                })

            # 为每个 clip 写 clip.manifest.json + 入库 clips 表
            for clip_data in clips:
                manifest_key = await self._write_clip_manifest(bag_id, input_rev, clip_data)
                clip_data["manifest_key"] = manifest_key
                saved_clip = await clip_repo.upsert(clip_data)
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

    async def _mine_events(
        self, anchor_ts: np.ndarray, table_artifacts: list[Any], bag_id: str, input_rev: str
    ) -> list[dict[str, Any]]:
        """服务端规则引擎挖掘切片（§7.3 来源1，source=MINED）。"""
        imu_keys = [a.object_key for a in table_artifacts if "imu" in a.object_key.lower()]
        if not imu_keys:
            return []
        import pyarrow.parquet as pq
        try:
            raw = await self._storage.get_parsed_object(imu_keys[0])
            tbl = pq.read_table(io.BytesIO(raw))
        except Exception as exc:  # noqa: BLE001 - IMU 表不可读则不挖掘
            logger.warning("mine_imu_read_failed", bag_id=bag_id, error=str(exc))
            return []
        needed = ("stamp_ns", "ax", "ay", "az")
        if not all(c in tbl.column_names for c in needed):
            return []
        stamp = tbl.column("stamp_ns").to_numpy(zero_copy_only=False).astype(np.int64)
        ax = tbl.column("ax").to_numpy(zero_copy_only=False)
        ay = tbl.column("ay").to_numpy(zero_copy_only=False)
        az = tbl.column("az").to_numpy(zero_copy_only=False)
        events = _mine_event_timestamps(
            stamp, ax, ay, az,
            brake_th=self._settings.rule_hard_brake_mps2,
            collision_th=self._settings.rule_collision_mps2,
        )
        return self._slice_by_mined(anchor_ts, events, bag_id, input_rev)

    def _slice_by_mined(
        self, anchor_ts: np.ndarray, events: list[dict[str, Any]], bag_id: str, input_rev: str
    ) -> list[dict[str, Any]]:
        """将挖掘事件映射为 clip（前后窗口同事件驱动，source=MINED）。"""
        before_ns = self._settings.slice_event_before_s * 1_000_000_000
        after_ns = self._settings.slice_event_after_s * 1_000_000_000
        clips: list[dict[str, Any]] = []
        lo, hi = int(anchor_ts[0]), int(anchor_ts[-1])
        for ev in events:
            t_ns = int(ev["ts_ns"])
            start = max(lo, t_ns - before_ns)
            end = min(hi, t_ns + after_ns)
            if end > start:
                clips.append({
                    "bag_id": bag_id,
                    "input_rev": input_rev,
                    "start_ns": start,
                    "end_ns": end,
                    "source": ClipSource.MINED,
                    "tags": [ev["kind"]],
                    "scene_label": None,
                    "quality": None,
                })
        return clips

    async def _write_clip_manifest(
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
        return await self._storage.put_derived_json(bag_id, input_rev, suffix, manifest_body)


__all__ = ["SliceWorker", "_mine_event_timestamps"]
