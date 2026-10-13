"""align-worker：跨模态时间对齐（§7.2 核心算法）。

输入：parsed 层 images/ / pointclouds/ / tables/*.parquet / metadata/tf_static.yaml
输出：derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet

§7.2 步骤 5（外参缺失处理）：某传感器在 manifest topics 声明但 tf_static 无对应
child_frame_id → 该传感器整列帧置 null（不参与对齐），并在结果 detail.tf_missing 记录。

传感器全集（#6）：按 object key 目录分组展开多相机/多雷达，不再写死单点；
schema 采用真实 camera_ids / lidar_ids 列表。

GNSS 脱敏（§13.4）：读 gnss parquet 的 lat/lon 经 mask_coordinates 截断后写入
frame_index（derived 读路径即脱敏），gnss_mask_enabled 可关。

会话管理：execute() 每次调用独立开启 session，适配 orchestrator 长时运行。
存储 IO：全 async（LakeStorage 公开方法已 executor 化，不阻塞事件循环）。
"""
from __future__ import annotations

import io
from typing import Any

import numpy as np
import pyarrow.parquet as pq
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import Bag, DerivedKind, Task
from app.parsers.gnss_parser import mask_coordinates
from app.producers.platform_events import PlatformEventProducer
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage
from app.storage.parquet_writer import build_frame_index_schema, write_frame_index
from app.time_sync import align_batch_nearest

logger = get_logger("app.services.align_worker")


def _sensor_from_key(key: str, generic_parents: tuple[str, ...], default_sensor: str) -> str:
    """从 object key 提取传感器 id（父目录名），generic/无层时回退 default_sensor。

    示例：.../images/camera_front/123.jpg → camera_front；
          .../images/123.jpg（父为 images）→ default_sensor。
    """
    parts = [p for p in (key or "").split("/") if p]
    if len(parts) < 2:
        return default_sensor
    parent = parts[-2]
    if parent in generic_parents:
        return default_sensor
    return parent


def _kind(sensor: str) -> str:
    """表格传感器分类：imu/odom/gnss/can/other。"""
    s = sensor.lower()
    if "imu" in s:
        return "imu"
    if "odom" in s:
        return "odom"
    if "gnss" in s or "gps" in s or "navsat" in s:
        return "gnss"
    if "can" in s:
        return "can"
    return "other"


def group_by_sensor(keys: list[str], generic_parents: tuple[str, ...], default_sensor: str) -> dict[str, list[str]]:
    """按传感器 id 分组 object keys（保序）。"""
    groups: dict[str, list[str]] = {}
    for k in keys:
        groups.setdefault(_sensor_from_key(k, generic_parents, default_sensor), []).append(k)
    return groups


class AlignWorker:
    """跨模态对齐 Worker（§7.2）。

    算法步骤：
    1. 读各模态时间列（图像文件名 ts_ns、pcd start/end_ns、表格 stamp_ns）
    2. 锚相机帧序列 T_img（升序去重）
    3. 点云：每个雷达独立最近邻切片（多雷达展开）
    4. 表格：[t-window, t+window] 内取最近行；外参缺失传感器置 null
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

            bag = await bag_repo.get_by_id(bag_id)
            if bag is None:
                raise ValueError(f"bag not found: {bag_id}")

            manifest = await self._read_manifest(artifact_repo, bag)
            image_keys: list[str] = list(manifest.get("image_keys", []))
            pcd_keys: list[str] = list(manifest.get("pcd_keys", []))
            table_keys: dict[str, str] = dict(manifest.get("table_keys", {}))
            tf_children: set[str] = self._tf_child_set(bag.manifest or {})

        # ---- 传感器全集分组（#6）----
        cam_groups = group_by_sensor(image_keys, ("images",), self._settings.anchor_camera)
        lidar_groups = group_by_sensor(pcd_keys, ("pointclouds",), "lidar_top")
        camera_ids = sorted(cam_groups) or [self._settings.anchor_camera]
        lidar_ids = sorted(lidar_groups)

        # 外参缺失传感器（tf_children 非空时才判定，空集视为无 tf 约束全部保留）
        tf_missing = {
            "camera": [c for c in camera_ids if tf_children and c.lstrip("/") not in tf_children],
            "lidar": [l for l in lidar_ids if tf_children and l.lstrip("/") not in tf_children],
        }

        # ---- 锚相机帧序列 ----
        anchor_ts = self._anchor_timestamps(cam_groups)
        if anchor_ts.size == 0:
            raise ValueError(f"no image frames for bag {bag_id}")

        # ---- 点云对齐（每雷达独立）----
        max_lidar_dt_ns = self._settings.max_lidar_dt_ms * 1_000_000
        lidar_align: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        for lid in lidar_ids:
            if lid in tf_missing["camera"] or lid in tf_missing["lidar"]:
                lidar_align[lid] = (
                    np.full(anchor_ts.size, -1, dtype=np.int64),
                    np.full(anchor_ts.size, np.iinfo(np.int64).max, dtype=np.int64),
                )
                continue
            lts = self._extract_ts_from_keys(lidar_groups[lid])
            arr = np.sort(np.array(lts, dtype=np.int64)) if lts else np.array([], dtype=np.int64)
            if arr.size:
                lidar_align[lid] = align_batch_nearest(anchor_ts, arr, max_dt_ns=max_lidar_dt_ns)
            else:
                lidar_align[lid] = (
                    np.full(anchor_ts.size, -1, dtype=np.int64),
                    np.full(anchor_ts.size, np.iinfo(np.int64).max, dtype=np.int64),
                )

        # ---- 表格对齐（IMU/odom/gnss/can）----
        table_data = await self._align_tables(anchor_ts, table_keys, tf_children)

        # ---- 构建 frame_index 行 ----
        rows = self._build_rows(
            anchor_ts, cam_groups, lidar_groups, lidar_align, table_data, tf_missing
        )

        # ---- schema（真实传感器全集）----
        imu_keys = [s for s in table_keys if _kind(s) == "imu"]
        odom_keys = [s for s in table_keys if _kind(s) == "odom"]
        gnss_keys = [s for s in table_keys if _kind(s) == "gnss"]
        can_keys = [s for s in table_keys if _kind(s) == "can"]
        schema = build_frame_index_schema(
            camera_ids=camera_ids,
            lidar_ids=lidar_ids,
            imu_keys=imu_keys,
            odom_keys=odom_keys,
            gnss_keys=gnss_keys,
            can_signals=can_keys,
        )
        buf = io.BytesIO()
        write_frame_index(rows, schema, buf)
        parquet_bytes = buf.getvalue()

        frame_index_key = await self._storage.put_derived_bytes(
            bag_id, input_rev, "aligned/frame_index.parquet",
            parquet_bytes, content_type="application/octet-stream",
        )

        # ---- 写 derived_assets（新 session）----
        async with self._db_mgr.session() as session2:
            derived_repo2 = DerivedAssetRepository(session2)
            await derived_repo2.upsert(bag_id, input_rev, DerivedKind.ALIGNED, frame_index_key, {
                "frame_count": len(rows),
                "align_version": "v1.0",
                "anchor_camera": self._settings.anchor_camera,
                "camera_ids": camera_ids,
                "lidar_ids": lidar_ids,
                "tf_missing": tf_missing,
            })

        await self._producer.publish_derived_ready(
            bag_id, input_rev, kind="aligned", object_keys=[frame_index_key],
            count=len(rows),
        )

        first_lidar = lidar_ids[0] if lidar_ids else None
        hit_rate = 0.0
        if first_lidar is not None and len(rows):
            hit_rate = float((lidar_align[first_lidar][0] >= 0).mean())
        logger.info(
            "align_done", bag_id=bag_id, frames=len(rows),
            cameras=camera_ids, lidars=lidar_ids, tf_missing=tf_missing,
        )
        return {
            "frame_index_key": frame_index_key,
            "frame_count": len(rows),
            "lidar_hit_rate": hit_rate,
            "tf_missing": tf_missing,
        }

    # ========== 内部工具 ==========

    @staticmethod
    def _tf_child_set(bag_manifest: dict[str, Any]) -> set[str]:
        transforms = (bag_manifest.get("tf_static") or {}).get("transforms") or []
        return {str(t.get("child_frame_id", "")).lstrip("/") for t in transforms if t.get("child_frame_id")}

    def _anchor_timestamps(self, cam_groups: dict[str, list[str]]) -> np.ndarray:
        """锚相机帧时间升序去重；锚组缺失时退回全相机并集。"""
        anchor_keys = cam_groups.get(self._settings.anchor_camera)
        if not anchor_keys:
            anchor_keys = [k for ks in cam_groups.values() for k in ks]
        ts = self._extract_ts_from_keys(anchor_keys)
        if not ts:
            return np.array([], dtype=np.int64)
        return np.sort(np.unique(np.array(ts, dtype=np.int64)))

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
                parts = stem.split("_")
                ts_list.append(int(parts[0]))
            except (ValueError, IndexError):
                pass
        return ts_list

    async def _read_parquet_table(self, object_key: str) -> dict[str, np.ndarray]:
        """读取 parquet 全表（按 stamp_ns 升序），返回列名→数组映射。"""
        try:
            raw = await self._storage.get_parsed_object(object_key)
            table = pq.read_table(io.BytesIO(raw))
            if "stamp_ns" not in table.column_names:
                return {}
            out: dict[str, np.ndarray] = {}
            columns: dict[str, Any] = {str(f.name): f for f in table.schema}
            order = np.argsort(table.column("stamp_ns").to_numpy(zero_copy_only=False).astype(np.int64))
            for name in table.column_names:
                col = table.column(name)
                if name == "stamp_ns":
                    out[name] = col.to_numpy(zero_copy_only=False).astype(np.int64)[order]
                elif columns[name].type.is_floating:
                    out[name] = col.to_numpy(zero_copy_only=False, zeroable=True).astype(np.float64)[order]
                else:
                    out[name] = col.to_numpy(zero_copy_only=False)[order]
            return out
        except Exception as exc:  # noqa: BLE001 - 单表读取失败降级为空
            logger.warning("parquet_read_failed", key=object_key, error=str(exc))
            return {}

    async def _align_tables(
        self,
        anchor_ts: np.ndarray,
        table_keys: dict[str, str],
        tf_children: set[str],
    ) -> dict[str, dict[str, Any]]:
        """对齐各表格传感器，返回 sensor→{idx, dt, values}。

        values：gnss 类型携带脱敏后的 lat/lon/alt 数组（对齐行取样）。
        """
        results: dict[str, dict[str, Any]] = {}
        max_dt_ns = self._settings.align_table_window_ms * 1_000_000
        for sensor_type, parquet_key in table_keys.items():
            tbl = await self._read_parquet_table(parquet_key)
            stamps = tbl.get("stamp_ns", np.array([], dtype=np.int64))
            if stamps.size == 0:
                results[sensor_type] = {
                    "idx": np.full(anchor_ts.size, -1, dtype=np.int64),
                    "dt": np.full(anchor_ts.size, np.iinfo(np.int64).max, dtype=np.int64),
                    "values": {},
                }
                continue
            idx, dt = align_batch_nearest(anchor_ts, stamps, max_dt_ns=max_dt_ns)
            values: dict[str, np.ndarray] = {}
            if _kind(sensor_type) == "gnss":
                # §13.4 GNSS 脱敏：读路径经 mask_coordinates 截断坐标精度（审查 #17）
                for col in ("lat", "lon", "alt"):
                    if col in tbl:
                        values[col] = self._pick_by_idx(tbl[col], idx)
                if self._settings.gnss_mask_enabled and "lat" in values and "lon" in values:
                    places = self._settings.gnss_descimal_places
                    lat_out: list[float] = []
                    lon_out: list[float] = []
                    for lat_v, lon_v in zip(values["lat"], values["lon"]):
                        if np.isfinite(lat_v) and np.isfinite(lon_v):
                            m_lat, m_lon = mask_coordinates(float(lat_v), float(lon_v), places)
                        else:
                            m_lat, m_lon = float(lat_v), float(lon_v)
                        lat_out.append(m_lat)
                        lon_out.append(m_lon)
                    values["lat"] = np.array(lat_out, dtype=np.float64)
                    values["lon"] = np.array(lon_out, dtype=np.float64)
            results[sensor_type] = {"idx": idx, "dt": dt, "values": values, "table": tbl}
        return results

    @staticmethod
    def _pick_by_idx(col: np.ndarray, idx: np.ndarray) -> np.ndarray:
        """按对齐索引取样列值；idx<0（未命中）→ NaN。"""
        out = np.full(idx.size, np.nan, dtype=np.float64)
        valid = idx >= 0
        vi = idx[valid]
        vi = np.clip(vi, 0, col.size - 1)
        out[valid] = col[vi].astype(np.float64)
        return out

    def _build_rows(
        self,
        anchor_ts: np.ndarray,
        cam_groups: dict[str, list[str]],
        lidar_groups: dict[str, list[str]],
        lidar_align: dict[str, tuple[np.ndarray, np.ndarray]],
        table_data: dict[str, dict[str, Any]],
        tf_missing: dict[str, list[str]],
    ) -> list[dict[str, Any]]:
        anchor_camera = self._settings.anchor_camera
        # 每相机 ts→key 映射
        cam_ts_key: dict[str, dict[int, str]] = {}
        for cam, keys in cam_groups.items():
            m: dict[int, str] = {}
            for k in keys:
                stem = k.split("/")[-1].split(".")[0].split("_")[0]
                if stem.isdigit():
                    m[int(stem)] = k
            cam_ts_key[cam] = m

        miss_cam = set(tf_missing["camera"])
        miss_lid = set(tf_missing["lidar"])

        rows: list[dict[str, Any]] = []
        for i, t_ns in enumerate(anchor_ts):
            row: dict[str, Any] = {"ts_ns": int(t_ns)}
            # 相机列（外参缺失 → 空）
            for cam in cam_groups:
                if cam in miss_cam:
                    row[f"camera_{cam}_path"] = None
                else:
                    row[f"camera_{cam}_path"] = cam_ts_key[cam].get(int(t_ns), "")
            # 点云列
            for lid, keys in lidar_groups.items():
                if lid in miss_lid:
                    row[f"lidar_{lid}_pcd_key"] = None
                    row[f"lidar_{lid}_dt_ms"] = None
                    continue
                li = int(lidar_align[lid][0][i])
                row[f"lidar_{lid}_pcd_key"] = keys[li] if 0 <= li < len(keys) else None
                dt = lidar_align[lid][1][i]
                row[f"lidar_{lid}_dt_ms"] = int(dt // 1_000_000) if dt != np.iinfo(np.int64).max else None
            # 表格列（dt 与值列按类型命名，对齐 schema）
            for sensor, data in table_data.items():
                kind = _kind(sensor)
                dt_val = data["dt"][i]
                dt_ms = int(dt_val // 1_000_000) if dt_val != np.iinfo(np.int64).max else None
                if kind == "imu":
                    row[f"imu_{sensor}_dt_ms"] = dt_ms
                elif kind == "odom":
                    row[f"odom_{sensor}_dt_ms"] = dt_ms
                elif kind == "gnss":
                    row[f"gnss_{sensor}_dt_ms"] = dt_ms
                    for col, arr in data.get("values", {}).items():
                        row[f"gnss_{sensor}_{col}"] = None if not np.isfinite(arr[i]) else float(arr[i])
                elif kind == "can":
                    row[f"can_{sensor}"] = dt_ms  # can 无独立数值列时占位（保持 schema 可写）
            # 状态位图
            status = 0
            if row.get(f"camera_{anchor_camera}_path"):
                status |= 0x01
            if any(row.get(f"lidar_{lid}_pcd_key") for lid in lidar_groups):
                status |= 0x02
            row["align_status"] = status
            rows.append(row)
        return rows

    @staticmethod
    def _table_col_prefix(sensor: str) -> str:
        """传感器 key → frame_index 值列前缀（仅 gnss 有值列）。"""
        if _kind(sensor) == "gnss":
            return f"gnss_{sensor}"
        return sensor


__all__ = ["AlignWorker", "_kind", "_sensor_from_key", "group_by_sensor"]
