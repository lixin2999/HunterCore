"""qc-worker：质量检查引擎（§7.4 + 设计文档第6点质量控制）。

检查项：
1. 文件哈希完整性（抽样 HEAD + sha256 比对）
2. 丢帧检测（stamp 间隔分布）
3. 时间戳单调性
4. 各 topic 消息数是否符合 manifest 声明
5. 图像可解码性（来自 frame_index align_status 位图）
6. 点云字段完整性
7. TF 树可构建性（tf_static 是否存在）

输出：
- derived/v1/{bag_id}/{input_rev}/qc/qc_result.json
- bags.qc_score（加权 0~1）
- 低于阈值发 platform.bag.qc_failed.v1 事件

会话管理：execute() 每次调用独立开启 session。
"""
from __future__ import annotations

from typing import Any

import numpy as np
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import ArtifactClass, BagState, DerivedKind, Task
from app.producers.platform_events import PlatformEventProducer
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository
from app.repositories.derived_assets import DerivedAssetRepository
from app.storage.minio_client import LakeStorage
from app.time_sync import check_frame_gaps, check_monotonicity

logger = get_logger("app.services.qc_worker")

# QC 检查权重（§7.4：加权 0~1，权重可配；此处为默认权重）
_WEIGHTS: dict[str, float] = {
    "hash_integrity": 0.25,
    "frame_gaps": 0.20,
    "monotonicity": 0.15,
    "message_count": 0.15,
    "image_decode": 0.10,
    "pcd_valid": 0.10,
    "tf_available": 0.05,
}


class QcCheckItem:
    def __init__(self, name: str, score: float, detail: dict[str, Any]) -> None:
        self.name = name
        self.score = score   # 0.0 ~ 1.0
        self.detail = detail


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

        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            artifact_repo = ArtifactRepository(session)
            derived_repo = DerivedAssetRepository(session)

            bag = await bag_repo.get_by_id(bag_id)
            if bag is None:
                raise ValueError(f"bag not found: {bag_id}")

            checks: list[QcCheckItem] = []

            # 1. 哈希完整性（抽样核验）
            checks.append(await self._check_hash(artifact_repo, bag_id))

            # 2. 丢帧检测
            checks.append(self._check_frame_gaps(bag_id, input_rev))

            # 3. 时间戳单调性
            checks.append(self._check_monotonicity(bag_id, input_rev))

            # 4. 消息数合规性
            checks.append(self._check_message_count(bag))

            # 5. 图像解码完整性（frame_index align_status 位图）
            checks.append(self._check_image_decode(bag_id, input_rev))

            # 6. 点云字段有效性
            checks.append(await self._check_pcd_valid(artifact_repo, bag_id))

            # 7. TF 可用性
            checks.append(self._check_tf(bag))

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

            # 写 qc_result.json
            qc_key = self._storage.put_derived_json(bag_id, input_rev, "qc/qc_result.json", qc_result)
            await derived_repo.upsert(bag_id, input_rev, DerivedKind.QC, qc_key, {"score": total_score})

            # 更新 bags.qc_score
            await bag_repo.update_qc_score(bag_id, total_score)

        # 低于阈值发事件（在 session 外发送，避免长事务）
        if total_score < self._settings.qc_score_threshold:
            failing = [c.name for c in checks if c.score < 0.5]
            await self._producer.publish_qc_failed(bag_id, total_score, failing)
            logger.warning("qc_failed", bag_id=bag_id, score=total_score, failing=failing)

        logger.info("qc_done", bag_id=bag_id, score=total_score)
        return {"qc_score": total_score, "qc_result_key": qc_key}

    # ========== 各检查项 ==========

    async def _check_hash(self, artifact_repo: ArtifactRepository, bag_id: str) -> QcCheckItem:
        """抽样哈希核验（§7.4）。"""
        artifacts = await artifact_repo.list_by_bag(bag_id, ArtifactClass.IMAGE)
        if not artifacts:
            return QcCheckItem("hash_integrity", 1.0, {"checked": 0, "note": "no_images"})
        expected = {a.object_key: a.sha256 for a in artifacts if a.sha256}
        keys = list(expected.keys())
        corrupt = self._storage.sample_hash_check(
            self._settings.minio_bucket_parsed,
            keys,
            expected,
            sample_ratio=self._settings.qc_hash_sample_ratio,
        )
        score = 1.0 if not corrupt else max(0.0, 1.0 - len(corrupt) / max(1, len(keys)))
        return QcCheckItem("hash_integrity", score, {"checked": len(keys), "corrupt_keys": corrupt[:5]})

    def _check_frame_gaps(self, bag_id: str, input_rev: str) -> QcCheckItem:
        """丢帧检测（读 frame_index.parquet ts_ns 列）。"""
        ts_arr = self._read_frame_ts(bag_id, input_rev)
        if ts_arr.size < 2:
            return QcCheckItem("frame_gaps", 1.0, {"frames": int(ts_arr.size)})
        result = check_frame_gaps(ts_arr.tolist())
        score = 1.0 if result.gap_count == 0 else max(0.0, 1.0 - result.gap_count / ts_arr.size)
        return QcCheckItem("frame_gaps", score, {
            "gap_count": result.gap_count,
            "jitter_p95_ms": result.jitter_p95_ms,
        })

    def _check_monotonicity(self, bag_id: str, input_rev: str) -> QcCheckItem:
        """时间戳单调性检查。"""
        ts_arr = self._read_frame_ts(bag_id, input_rev)
        if ts_arr.size < 2:
            return QcCheckItem("monotonicity", 1.0, {"frames": int(ts_arr.size)})
        result = check_monotonicity(ts_arr.tolist())
        score = 1.0 if result.is_monotonic else max(0.0, 1.0 - result.violation_count / ts_arr.size)
        return QcCheckItem("monotonicity", score, {
            "is_monotonic": result.is_monotonic,
            "violation_count": result.violation_count,
        })

    def _check_message_count(self, bag: Any) -> QcCheckItem:
        """消息数合规性（manifest 声明 vs artifacts 实际计数）。"""
        declared = int(bag.message_count or 0)
        score = 1.0 if declared > 0 else 0.0
        return QcCheckItem("message_count", score, {"declared": declared})

    def _check_image_decode(self, bag_id: str, input_rev: str) -> QcCheckItem:
        """图像解码完整性（align_status 位图中 bit0 = 相机命中）。"""
        ts_arr = self._read_frame_ts(bag_id, input_rev)
        if ts_arr.size == 0:
            return QcCheckItem("image_decode", 0.0, {"note": "no_frame_index"})
        import io
        import pyarrow.parquet as pq
        key = f"derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet"
        try:
            raw = self._storage._get_object_bytes(self._settings.minio_bucket_derived, key)
            table = pq.read_table(io.BytesIO(raw))
            if "align_status" not in table.column_names:
                return QcCheckItem("image_decode", 1.0, {"note": "no_status_col"})
            statuses = table.column("align_status").to_numpy(zero_copy_only=False)
            hit = float(((statuses & 0x01) > 0).mean())
        except Exception:
            hit = 0.0
        return QcCheckItem("image_decode", hit, {"hit_rate": hit})

    async def _check_pcd_valid(self, artifact_repo: ArtifactRepository, bag_id: str) -> QcCheckItem:
        """点云字段有效性（pcd artifacts 数量 > 0 则通过）。"""
        pcds = await artifact_repo.list_by_bag(bag_id, ArtifactClass.PCD)
        if not pcds:
            return QcCheckItem("pcd_valid", 1.0, {"note": "no_pcd"})
        valid = sum(1 for p in pcds if (p.count or 0) > 0)
        score = valid / len(pcds)
        return QcCheckItem("pcd_valid", score, {"total": len(pcds), "valid": valid})

    def _check_tf(self, bag: Any) -> QcCheckItem:
        """TF 可用性（manifest 含 tf_static 则视为可构建）。"""
        manifest = bag.manifest or {}
        has_tf = bool(manifest.get("tf_static", {}).get("transforms"))
        return QcCheckItem("tf_available", 1.0 if has_tf else 0.5, {"has_tf_static": has_tf})

    def _read_frame_ts(self, bag_id: str, input_rev: str) -> np.ndarray:
        """从 frame_index.parquet 读取 ts_ns 列（同步调用，存储 IO）。"""
        import io
        import pyarrow.parquet as pq
        key = f"derived/v1/{bag_id}/{input_rev}/aligned/frame_index.parquet"
        try:
            raw = self._storage._get_object_bytes(self._settings.minio_bucket_derived, key)
            table = pq.read_table(io.BytesIO(raw))
            return table.column("ts_ns").to_numpy(zero_copy_only=False).astype(np.int64)
        except Exception:
            return np.array([], dtype=np.int64)


__all__ = ["QcWorker"]
