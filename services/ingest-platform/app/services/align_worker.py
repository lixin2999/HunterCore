"""align-worker：跨模态时间对齐（§7.2 核心算法）。

输入：parsed 层 images/ / pointclouds/ / tables/*.parquet / metadata/tf_static.yaml
输出：derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet

会话管理：execute() 每次调用独立开启 session，适配 orchestrator 长时运行。
"""
from __future__ import annotations

import io
from typing import Any

import numpy as np
import pyarrow as pa
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import Bag, DerivedKind, Task
from app.producers.platform_events import PlatformEventProducer
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage, parsed_key
from app.storage.parquet_writer import build_frame_index_schema, write_frame_index
from app.time_sync import NanoSec, align_batch_nearest, stamp_to_ns

logger = get_logger("app.services.align_worker")


class AlignWorker:
    """跨模态对齐 Worker（§7.2）。

    算法步骤：
    1. 读各模态时间列（图像文件名 ts_ns、pcd start/end_ns、表格 stamp_ns）
    2. 锚相机帧序列 T_img（升序去重）
    3. 点云：每帧取包含该帧时间的切片（最近邻）
    4. 表格：[t-50ms, t+50ms] 内取最近行（可选线性插值）
    5. 输出 frame_index.parquet（只存引用，不复制媒体）
    """

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
        """执行对齐任务（由 orchestrator 调用）。"""
        bag_id = task.bag_id or ""
        input_rev = task.input_rev or ""

        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            artifact_repo = ArtifactRepository(session)
            derived_repo = DerivedAssetRepository(session)

            bag = await bag_repo.get_by_id(bag_id)
            if bag is None:
                raise ValueError(f"bag not found: {bag_id}")

            # 读取 parsed 层产物清单
            manifest = await self._read_manifest(artifact_repo, bag)
            image_keys: list[str] = list(manifest.get("image_keys", []))
            pcd_keys: list[str] = list(manifest.get("pcd_keys", []))
            table_keys: dict[str, str] = dict(manifest.get("table_keys", {}))

        # 解析时间戳
        image_ts = self._extract_ts_from_keys(image_keys)
        pcd_ts = self._extract_ts_from_keys(pcd_keys)

        if not image_ts:
            raise ValueError(f"no image frames for bag {bag_id}")

        anchor_ts = np.sort(np.unique(np.array(image_ts, dtype=np.int64)))

        # 对齐点云（最近邻）
        pcd_arr = np.sort(np.array(pcd_ts, dtype=np.int64)) if pcd_ts else np.array([], dtype=np.int64)
        max_lidar_dt_ns = self._settings.max_lidar_dt_ms * 1_000_000
        if pcd_arr.size:
            lidar_idx, lidar_dt = align_batch_nearest(anchor_ts, pcd_arr, max_dt_ns=max_lidar_dt_ns)
        else:
            lidar_idx = np.full(anchor_ts.size, -1, dtype=np.int64)
            lidar_dt = np.full(anchor_ts.size, np.iinfo(np.int64).max, dtype=np.int64)

        # 对齐表格（IMU / odom / gnss / can）
        table_results: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        for sensor_type, parquet_key in table_keys.items():
            try:
                stamp_ns = await self._read_parquet_stamps(parquet_key)
                if stamp_ns.size:
                    idx, dt = align_batch_nearest(
                        anchor_ts, stamp_ns,
                        max_dt_ns=self._settings.align_table_window_ms * 1_000_000,
                    )
                    table_results[sensor_type] = (idx, dt)
                else:
                    table_results[sensor_type] = (
                        np.full(anchor_ts.size, -1, dtype=np.int64),
                        np.full(anchor_ts.size, np.iinfo(np.int64).max, dtype=np.int64),
                    )
            except Exception as exc:
                logger.warning("align_table_skip", sensor=sensor_type, error=str(exc))
                table_results[sensor_type] = (
                    np.full(anchor_ts.size, -1, dtype=np.int64),
                    np.full(anchor_ts.size, np.iinfo(np.int64).max, dtype=np.int64),
                )

        # 构建 frame_index 行
        rows = self._build_frame_index_rows(
            anchor_ts, image_keys, pcd_keys, lidar_idx, lidar_dt, table_results
        )

        # 写出 Parquet
        schema = build_frame_index_schema(
            camera_ids=[self._settings.anchor_camera],
            lidar_ids=["lidar_top"],
            imu_keys=[""],
            odom_keys=[""],
            gnss_keys=[""],
            can_signals=[],
        )
        buf = io.BytesIO()
        write_frame_index(rows, schema, buf)
        parquet_bytes = buf.getvalue()

        frame_index_key = self._storage.put_derived_bytes(
            bag_id, input_rev, "aligned/frame_index.parquet",
            parquet_bytes, content_type="application/octet-stream",
        )

        # 写 derived_assets（新 session）
        async with self._db_mgr.session() as session2:
            derived_repo2 = DerivedAssetRepository(session2)
            await derived_repo2.upsert(bag_id, input_rev, DerivedKind.ALIGNED, frame_index_key, {
                "frame_count": len(rows),
                "align_version": "v1.0",
                "anchor_camera": self._settings.anchor_camera,
            })

        # 发 derived.ready 事件
        await self._producer.publish_derived_ready(
            bag_id, input_rev, kind="aligned", object_keys=[frame_index_key],
            count=len(rows),
        )

        hit_rate = float((lidar_idx >= 0).mean()) if len(rows) else 0.0
        logger.info("align_done", bag_id=bag_id, frames=len(rows), lidar_hit_rate=hit_rate)

        return {
            "frame_index_key": frame_index_key,
            "frame_count": len(rows),
            "lidar_hit_rate": hit_rate,
        }

    # ========== 内部工具 ==========

    async def _read_manifest(self, artifact_repo: ArtifactRepository, bag: Bag) -> dict[str, Any]:
        """从 artifacts 表读取产物清单。"""
        from app.models.ingest import ArtifactClass
        result: dict[str, Any] = {"image_keys": [], "pcd_keys": [], "table_keys": {}}
        for a_cls, key_list in [
            (ArtifactClass.IMAGE, "image_keys"),
            (ArtifactClass.PCD, "pcd_keys"),
        ]:
            artifacts = await artifact_repo.list_by_bag(bag.bag_id, a_cls)
            result[key_list] = [a.object_key for a in artifacts]

        table_artifacts = await artifact_repo.list_by_bag(bag.bag_id, ArtifactClass.TABLE)
        for a in table_artifacts:
            if a.object_key.endswith(".parquet"):
                sensor_hint = a.object_key.split("/")[-1].replace(".parquet", "")
                result["table_keys"][sensor_hint] = a.object_key

        return result

    def _extract_ts_from_keys(self, keys: list[str]) -> list[int]:
        """从 object key 文件名中提取纳秒时间戳。"""
        ts_list: list[int] = []
        for key in keys:
            filename = key.split("/")[-1]
            stem = filename.split(".")[0]
            try:
                # 图像: {ts_ns}.jpg; 点云: {start_ns}_{end_ns}.pcd
                parts = stem.split("_")
                ts_list.append(int(parts[0]))
            except (ValueError, IndexError):
                pass
        return ts_list

    async def _read_parquet_stamps(self, object_key: str) -> np.ndarray:
        """从 Parquet 文件读取 stamp_ns 列（升序数组）。"""
        try:
            raw = self._storage.get_parsed_object(object_key)
            table = pa.read_table(io.BytesIO(raw))
            if "stamp_ns" in table.column_names:
                col = table.column("stamp_ns").to_numpy(zero_copy_only=False)
                return np.sort(np.asarray(col, dtype=np.int64))
            return np.array([], dtype=np.int64)
        except Exception as exc:
            logger.warning("parquet_read_failed", key=object_key, error=str(exc))
            return np.array([], dtype=np.int64)

    def _build_frame_index_rows(
        self,
        anchor_ts: np.ndarray,
        image_keys: list[str],
        pcd_keys: list[str],
        lidar_idx: np.ndarray,
        lidar_dt: np.ndarray,
        table_results: dict[str, tuple[np.ndarray, np.ndarray]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        ts_to_img_key = {
            int(k.split("/")[-1].split(".")[0].split("_")[0]): k
            for k in image_keys if k.split("/")[-1].split(".")[0].split("_")[0].isdigit()
        }
        anchor_camera = self._settings.anchor_camera

        for i, t_ns in enumerate(anchor_ts):
            row: dict[str, Any] = {"ts_ns": int(t_ns)}
            # 相机
            row[f"camera_{anchor_camera}_path"] = ts_to_img_key.get(int(t_ns), "")
            # 点云
            li = int(lidar_idx[i])
            row["lidar_lidar_top_pcd_key"] = pcd_keys[li] if 0 <= li < len(pcd_keys) else None
            row["lidar_lidar_top_dt_ms"] = int(lidar_dt[i] // 1_000_000) if lidar_dt[i] != np.iinfo(np.int64).max else None
            # 表格
            for sensor, (idx_arr, dt_arr) in table_results.items():
                row[f"{sensor}_dt_ms"] = int(dt_arr[i] // 1_000_000) if dt_arr[i] != np.iinfo(np.int64).max else None
            # 对齐状态位图
            status = 0
            if row.get(f"camera_{anchor_camera}_path"):
                status |= 0x01
            if row.get("lidar_lidar_top_pcd_key"):
                status |= 0x02
            row["align_status"] = status
            rows.append(row)
        return rows


__all__ = ["AlignWorker"]
