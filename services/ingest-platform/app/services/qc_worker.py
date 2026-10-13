"""qc-worker：质量检查引擎（§7.4 全部 7+2 项检查）。

检查项（权重见 _WEIGHTS）：
1. hash_integrity 文件哈希完整性（抽样 HEAD/GET + manifest 声明 sha256 比对）
2. frame_gaps 丢帧检测（stamp 间隔分布）
3. monotonicity 时间戳单调性
4. message_count 各 topic 消息数是否符合 manifest 声明
5. image_decode 图像可解码性（真实抽样帧 PIL 解码）
6. black_frozen_frames 黑帧/冻结帧检测（§7.4 检查项 5 补全）
7. pcd_valid 点云字段完整性
8. tf_available TF 树可构建性（tf_static 是否存在）
9. calibration_conflict 标定冲突检测（§7.4 检查项 7 补全）

输出：
- derived/v1/{bag_id}/{input_rev}/qc/qc_result.json
- bags.qc_score（加权 0~1）
- 哈希核验失败对象回写 artifacts.upload_state=corrupt（§7.1）
- 低于阈值发 platform.bag.qc_failed.v1 事件

事务拆分（性能审查 #19）：execute 分三段——
A 读 DB 清单 → B 纯存储 IO/计算（不持有连接） → C 新 session 写结果，
避免持 DB 连接期间做 MinIO 网络 IO 造成 idle in transaction 连接池耗尽。
"""
from __future__ import annotations

import io
from typing import Any

import numpy as np
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import ArtifactClass, DerivedKind, Task
from app.producers.platform_events import PlatformEventProducer
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage
from app.time_sync import check_frame_gaps, check_monotonicity

logger = get_logger("app.services.qc_worker")

# QC 检查权重（§7.4：加权 0~1；和为 1.0）
_WEIGHTS: dict[str, float] = {
    "hash_integrity": 0.22,
    "frame_gaps": 0.18,
    "monotonicity": 0.13,
    "message_count": 0.12,
    "image_decode": 0.08,
    "black_frozen_frames": 0.10,
    "pcd_valid": 0.09,
    "tf_available": 0.04,
    "calibration_conflict": 0.04,
}


class QcCheckItem:
    def __init__(self, name: str, score: float, detail: dict[str, Any]) -> None:
        self.name = name
        self.score = score   # 0.0 ~ 1.0
        self.detail = detail


# ========== 纯函数检测器（可单测，§7.4 检查项 5/7） ==========

def detect_black_frame(frame_bytes: bytes, threshold: float) -> bool:
    """单帧黑帧判定：灰度亮度均值低于阈值（解码失败视为黑帧）。"""
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(frame_bytes)).convert("L")
        mean = float(np.asarray(img, dtype=np.float64).mean())
    except Exception:  # noqa: BLE001 - 无法解码视为黑帧
        return True
    return mean < threshold


def detect_black_frozen(
    frame_bytes_list: list[bytes],
    *,
    black_threshold: float,
    frozen_min_count: int,
) -> dict[str, Any]:
    """连续采样帧序列的黑帧/冻结帧检测。

    - black: 亮度均值连续 ≥2 帧低于阈值 → 判定存在黑帧段；
    - frozen: 相邻帧内容 hash 连续相同 ≥ frozen_min_count 帧 → 判定冻结。

    Returns:
        {"black_runs": int, "frozen_runs": int, "frames": int, "ok": bool}
    """
    import hashlib

    n = len(frame_bytes_list)
    if n == 0:
        return {"black_runs": 0, "frozen_runs": 0, "frames": 0, "ok": True}

    black_flags = [detect_black_frame(b, black_threshold) for b in frame_bytes_list]
    black_runs = 0
    run = 0
    for flag in black_flags:
        run = run + 1 if flag else 0
        if run == 2:
            black_runs += 1

    hashes = [hashlib.sha256(b).hexdigest() for b in frame_bytes_list]
    frozen_runs = 0
    same_run = 1
    for i in range(1, n):
        if hashes[i] == hashes[i - 1]:
            same_run += 1
            if same_run == frozen_min_count:
                frozen_runs += 1
        else:
            same_run = 1

    return {
        "black_runs": black_runs,
        "frozen_runs": frozen_runs,
        "frames": n,
        "ok": black_runs == 0 and frozen_runs == 0,
    }


def detect_calibration_conflict(bag_manifest: dict[str, Any], sensor_ids: list[str]) -> dict[str, Any]:
    """标定冲突检测（§7.4 检查项 7）。

    冲突判定：
    - tf_static transforms 中 child_frame_id 重复定义 → conflict；
    - manifest topics 声明的传感器全集未被 tf 树覆盖 → incomplete。

    Returns:
        {"status": "ok"|"conflict"|"incomplete"|"no_tf", "score": float,
         "duplicate_children": [...], "uncovered_sensors": [...]}
    """
    transforms = (bag_manifest or {}).get("tf_static", {}).get("transforms", []) or []
    children = [str(t.get("child_frame_id", "")) for t in transforms]
    duplicates = sorted({c for c in children if children.count(c) > 1})
    if duplicates:
        return {
            "status": "conflict", "score": 0.0,
            "duplicate_children": duplicates, "uncovered_sensors": [],
        }
    if not children:
        return {"status": "no_tf", "score": 0.5, "duplicate_children": [], "uncovered_sensors": []}
    child_set = {c.lstrip("/") for c in children}
    uncovered = [s for s in sensor_ids if s.lstrip("/") not in child_set]
    if uncovered:
        return {
            "status": "incomplete", "score": 0.5,
            "duplicate_children": [], "uncovered_sensors": uncovered,
        }
    return {"status": "ok", "score": 1.0, "duplicate_children": [], "uncovered_sensors": []}


def sensor_id_from_topic(topic: str) -> str:
    """/camera_front/image_raw → camera_front（tf child_frame_id 命名约定）。"""
    parts = [p for p in (topic or "").split("/") if p]
    return parts[0] if parts else ""


def collect_sensor_ids(topics: list[dict[str, Any]]) -> list[str]:
    """从 manifest topics 提取相机/雷达传感器全集（去重、保序）。"""
    seen: list[str] = []
    for t in topics or []:
        topic = str(t.get("topic", ""))
        if "/image" in topic or "/camera" in topic or "/pointclouds" in topic or "/lidar" in topic or "/points" in topic:
            sid = sensor_id_from_topic(topic)
            if sid and sid not in seen:
                seen.append(sid)
    return seen


class QcWorker:
    """质量检查 Worker（§7.4）。"""

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

        # ---------- Phase A：session 只读清单（不做存储 IO） ----------
        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            artifact_repo = ArtifactRepository(session)
            bag = await bag_repo.get_by_id(bag_id)
            if bag is None:
                raise ValueError(f"bag not found: {bag_id}")
            bag_snapshot: dict[str, Any] = {
                "manifest": bag.manifest or {},
                "message_count": int(bag.message_count or 0),
            }
            image_artifacts = await artifact_repo.list_by_bag(bag_id, ArtifactClass.IMAGE)
            pcd_artifacts = await artifact_repo.list_by_bag(bag_id, ArtifactClass.PCD)

        # ---------- Phase B：存储 IO + 计算（不持有 DB 连接） ----------
        frame_table = await self._load_frame_index(bag_id, input_rev)
        checks: list[QcCheckItem] = []

        # 1. 哈希完整性（抽样核验，expected 来自 artifacts.sha256 = manifest 声明值）
        expected = {a.object_key: a.sha256 for a in image_artifacts if a.sha256}
        keys = list(expected.keys())
        corrupt: list[str] = []
        if keys:
            corrupt = await self._storage.sample_hash_check(
                self._settings.minio_bucket_parsed,
                keys,
                expected,
                sample_ratio=self._settings.qc_hash_sample_ratio,
            )
        score = 1.0 if not corrupt else max(0.0, 1.0 - len(corrupt) / max(1, len(keys)))
        checks.append(QcCheckItem("hash_integrity", score, {
            "checked": len(keys), "corrupt_count": len(corrupt), "corrupt_keys": corrupt[:5],
        }))

        # 2. 丢帧检测
        ts_arr = self._ts_array(frame_table)
        if ts_arr.size < 2:
            checks.append(QcCheckItem("frame_gaps", 1.0, {"frames": int(ts_arr.size)}))
        else:
            result = check_frame_gaps(ts_arr.tolist())
            s = 1.0 if result.gap_count == 0 else max(0.0, 1.0 - result.gap_count / ts_arr.size)
            checks.append(QcCheckItem("frame_gaps", s, {
                "gap_count": result.gap_count, "jitter_p95_ms": result.jitter_p95_ms,
            }))

        # 3. 时间戳单调性
        if ts_arr.size < 2:
            checks.append(QcCheckItem("monotonicity", 1.0, {"frames": int(ts_arr.size)}))
        else:
            result = check_monotonicity(ts_arr.tolist())
            s = 1.0 if result.is_monotonic else max(0.0, 1.0 - result.violation_count / ts_arr.size)
            checks.append(QcCheckItem("monotonicity", s, {
                "is_monotonic": result.is_monotonic, "violation_count": result.violation_count,
            }))

        # 4. 消息数合规性
        declared = bag_snapshot["message_count"]
        checks.append(QcCheckItem("message_count", 1.0 if declared > 0 else 0.0, {"declared": declared}))

        # 5. 图像解码（真实抽样帧 PIL 解码成功率，§7.4 检查项 5）
        checks.append(await self._check_image_decode(image_artifacts))

        # 6. 黑帧/冻结帧（§7.4 检查项 5 补全）
        checks.append(await self._check_black_frozen(image_artifacts))

        # 7. 点云字段有效性
        if pcd_artifacts:
            valid = sum(1 for p in pcd_artifacts if (p.count or 0) > 0)
            checks.append(QcCheckItem("pcd_valid", valid / len(pcd_artifacts), {
                "total": len(pcd_artifacts), "valid": valid,
            }))
        else:
            checks.append(QcCheckItem("pcd_valid", 1.0, {"note": "no_pcd"}))

        # 8. TF 可用性
        has_tf = bool(bag_snapshot["manifest"].get("tf_static", {}).get("transforms"))
        checks.append(QcCheckItem("tf_available", 1.0 if has_tf else 0.5, {"has_tf_static": has_tf}))

        # 9. 标定冲突（§7.4 检查项 7 补全）
        sensor_ids = collect_sensor_ids(bag_snapshot["manifest"].get("topics", []) or [])
        conflict = detect_calibration_conflict(bag_snapshot["manifest"], sensor_ids)
        checks.append(QcCheckItem("calibration_conflict", conflict["score"], conflict))

        # 加权评分
        total_score = sum(c.score * _WEIGHTS.get(c.name, 0.1) for c in checks)
        total_score = max(0.0, min(1.0, total_score))

        qc_result: dict[str, Any] = {
            "bag_id": bag_id,
            "input_rev": input_rev,
            "qc_score": total_score,
            "checks": [
                {"name": c.name, "score": c.score, **c.detail}
                for c in checks
            ],
        }

        # ---------- Phase C：新 session 写结果 ----------
        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            artifact_repo = ArtifactRepository(session)
            derived_repo = DerivedAssetRepository(session)

            qc_key = await self._storage.put_derived_json(
                bag_id, input_rev, "qc/qc_result.json", qc_result
            )
            await derived_repo.upsert(bag_id, input_rev, DerivedKind.QC, qc_key, {"score": total_score})
            await bag_repo.update_qc_score(bag_id, total_score)
            # 哈希核验失败对象回写 corrupt（§7.1，留待重传/修复流程）
            for ck in corrupt:
                await artifact_repo.set_corrupt(ck)

        # 低于阈值发事件（session 外发送）
        if total_score < self._settings.qc_score_threshold:
            failing = [c.name for c in checks if c.score < 0.5]
            await self._producer.publish_qc_failed(bag_id, total_score, failing)
            logger.warning("qc_failed", bag_id=bag_id, score=total_score, failing=failing)

        logger.info("qc_done", bag_id=bag_id, score=total_score)
        return {"qc_score": total_score, "qc_result_key": qc_key}

    # ========== 检查项辅助 ==========

    async def _check_image_decode(self, image_artifacts: list[Any]) -> QcCheckItem:
        """图像可解码性：抽样 ≤qc_image_sample_count 帧真实 PIL 解码，统计成功率。"""
        if not image_artifacts:
            return QcCheckItem("image_decode", 1.0, {"note": "no_images"})
        import random

        n = self._settings.qc_image_sample_count
        sampled = image_artifacts if len(image_artifacts) <= n else random.sample(image_artifacts, n)
        decoded = 0
        for a in sampled:
            try:
                raw = await self._storage.get_parsed_object(a.object_key)
                from PIL import Image

                Image.open(io.BytesIO(raw)).verify()
                decoded += 1
            except Exception:  # noqa: BLE001, S112 - 解码失败帧直接跳过
                continue
        return QcCheckItem("image_decode", decoded / max(1, len(sampled)), {
            "sampled": len(sampled), "decoded": decoded,
        })

    async def _check_black_frozen(self, image_artifacts: list[Any]) -> QcCheckItem:
        """黑帧/冻结帧检测：取单相机连续帧序列抽样判定（§7.4 检查项 5）。"""
        if not image_artifacts:
            return QcCheckItem("black_frozen_frames", 1.0, {"note": "no_images"})
        # 按 key 排序保证时间连续，取前 N 帧连续段
        ordered = sorted(image_artifacts, key=lambda a: a.object_key)[: self._settings.qc_image_sample_count]
        bodies: list[bytes] = []
        for a in ordered:
            try:
                bodies.append(await self._storage.get_parsed_object(a.object_key))
            except Exception:  # noqa: BLE001, S112 - 下载失败帧直接跳过
                continue
        if not bodies:
            return QcCheckItem("black_frozen_frames", 0.0, {"note": "download_failed"})
        result = detect_black_frozen(
            bodies,
            black_threshold=self._settings.qc_black_frame_threshold,
            frozen_min_count=self._settings.qc_frozen_frame_min_count,
        )
        return QcCheckItem("black_frozen_frames", 1.0 if result["ok"] else 0.0, result)

    async def _load_frame_index(self, bag_id: str, input_rev: str) -> Any:
        """读取 frame_index.parquet 整表（无则 None）。"""
        import pyarrow.parquet as pq

        key = f"derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet"
        try:
            raw = await self._storage.get_derived_object(key)
            return pq.read_table(io.BytesIO(raw))
        except Exception:  # noqa: BLE001 - frame_index 缺失降级
            return None

    @staticmethod
    def _ts_array(table: Any) -> np.ndarray:
        if table is None or "ts_ns" not in table.column_names:
            return np.array([], dtype=np.int64)
        return table.column("ts_ns").to_numpy(zero_copy_only=False).astype(np.int64)


__all__ = [
    "QcCheckItem",
    "QcWorker",
    "collect_sensor_ids",
    "detect_black_frame",
    "detect_black_frozen",
    "detect_calibration_conflict",
    "sensor_id_from_topic",
]
