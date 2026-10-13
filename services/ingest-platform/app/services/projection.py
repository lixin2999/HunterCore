"""状态投影引擎（§5.2 层2：投影单调守卫 + §附录B 伪码实现）。

负责将原始事件映射为 bag/agent 状态，并在满足就绪条件时触发任务图创建。

会话管理：每次 handle_xxx 调用独立开启 session（适配长时运行消费者）。
"""
from __future__ import annotations

import hashlib
from typing import Any

from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import ArtifactClass, BagState, TaskType
from app.repositories.artifacts import ArtifactRepository
from app.repositories.bags import BagRepository, BagTopicRepository
from app.repositories.projection_anomaly import ProjectionAnomalyRepository
from app.repositories.tasks import TaskRepository

logger = get_logger("app.services.projection")


def compute_input_rev(manifest_sha256: str, template_version: str) -> str:
    """计算 input_rev（§5.2：sha256(manifest sha256 + template version)）。"""
    raw = f"{manifest_sha256}:{template_version}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


class ProjectionService:
    """事件 → bag 状态投影 + 就绪判定（§附录B on_event 流程）。

    构造时注入 DatabaseSessionManager，每次方法调用独立获取 session。
    """

    def __init__(
        self,
        settings: Settings,
        db_mgr: DatabaseSessionManager,
    ) -> None:
        self._settings = settings
        self._db_mgr = db_mgr

    async def handle_manifest(self, event: dict[str, Any]) -> None:
        """处理 bag.manifest.v1 事件（§附录A：写 bags / bag_topics，触发就绪判定）。"""
        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            topic_repo = BagTopicRepository(session)
            artifact_repo = ArtifactRepository(session)
            task_repo = TaskRepository(session)
            anomaly_repo = ProjectionAnomalyRepository(session)

            payload = event.get("payload", {})
            bag_id: str = event["bag_id"]
            vehicle_id: str = event["vehicle_id"]
            seq: int = int(event.get("seq", 0))

            manifest_sha = payload.get("sha256", "")
            input_rev = compute_input_rev(manifest_sha, self._settings.parse_template_version)

            # 1. UPSERT bags
            bag_fields: dict[str, Any] = {
                "vehicle_id": vehicle_id,
                "ros_version": payload.get("ros_version"),
                "start_ns": payload.get("start_ns"),
                "end_ns": payload.get("end_ns"),
                "duration_s": payload.get("duration_s"),
                "size_bytes": payload.get("size_bytes"),
                "sha256": manifest_sha,
                "message_count": payload.get("message_count"),
                "manifest_rev": input_rev,
                "manifest": payload,
            }
            await bag_repo.upsert_from_manifest(bag_id, bag_fields)

            # 2. UPSERT bag_topics
            topics_raw: list[dict[str, Any]] = list(payload.get("topics", []))
            if topics_raw:
                await topic_repo.upsert_batch(bag_id, topics_raw)

            # 3. 投影 discovered → parsed（manifest 事件意味着车端已解析完成）
            await self._project_state(bag_repo, anomaly_repo, bag_id, BagState.PARSED, seq)

            # 4. 就绪判定
            await self._check_and_trigger(bag_repo, artifact_repo, task_repo, bag_id, input_rev)

    async def handle_artifact_uploaded(self, event: dict[str, Any]) -> None:
        """处理 bag.artifact.uploaded.v1 事件（§附录A：写 artifacts，触发就绪判定）。"""
        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            artifact_repo = ArtifactRepository(session)
            task_repo = TaskRepository(session)

            payload = event.get("payload", {})
            bag_id: str = event["bag_id"]
            object_key: str = payload.get("object_key", "")
            artifact_class_str: str = payload.get("artifact_class", "raw")

            try:
                a_cls = ArtifactClass(artifact_class_str)
            except ValueError:
                a_cls = ArtifactClass.RAW

            await artifact_repo.upsert(object_key, {
                "bag_id": bag_id,
                "artifact_class": a_cls,
                "size_bytes": payload.get("size_bytes"),
                "sha256": payload.get("sha256"),
                "etag": payload.get("etag"),
                "rows": payload.get("rows"),
                "count": payload.get("count"),
            })

            # 就绪判定
            bag = await bag_repo.get_by_id(bag_id)
            if bag and bag.manifest_rev:
                await self._check_and_trigger(bag_repo, artifact_repo, task_repo, bag_id, bag.manifest_rev)

    async def handle_task_status(self, event: dict[str, Any]) -> None:
        """处理 bag.task.status.v1 事件（§附录A：纯投影更新）。"""
        async with self._db_mgr.session() as session:
            bag_repo = BagRepository(session)
            anomaly_repo = ProjectionAnomalyRepository(session)

            payload = event.get("payload", {})
            bag_id: str = event["bag_id"]
            seq: int = int(event.get("seq", 0))
            to_state_str: str = payload.get("state", "uploaded")

            state_map = {
                "uploaded": BagState.UPLOADED,
                "done": BagState.DONE,
                "verified": BagState.VERIFIED,
            }
            to_state = state_map.get(to_state_str, BagState.UPLOADED)
            await self._project_state(bag_repo, anomaly_repo, bag_id, to_state, seq)

    async def _project_state(
        self,
        bag_repo: BagRepository,
        anomaly_repo: ProjectionAnomalyRepository,
        bag_id: str,
        to: BagState,
        seq: int,
    ) -> None:
        """投影状态，记录异常。"""
        updated = await bag_repo.project_state(bag_id, to, seq)
        if not updated:
            bag = await bag_repo.get_by_id(bag_id)
            if bag:
                rank_gap = bag.state.rank - to.rank
                if rank_gap >= 2:
                    await anomaly_repo.record({
                        "bag_id": bag_id,
                        "received_seq": seq,
                        "current_seq": bag.seq,
                        "received_state": to.value,
                        "event_state": bag.state.value,
                        "reason": f"rank_gap={rank_gap}",
                    })
                    logger.warning("projection_anomaly", bag_id=bag_id, rank_gap=rank_gap)

    async def _check_and_trigger(
        self,
        bag_repo: BagRepository,
        artifact_repo: ArtifactRepository,
        task_repo: TaskRepository,
        bag_id: str,
        input_rev: str,
    ) -> None:
        """就绪判定：检查必需产物，满足则创建任务图（§7.1 + §5.2 层3）。"""
        counts = await artifact_repo.count_by_bag_class(bag_id)
        required = self._settings.ready_required_class_list

        for cls_str in required:
            try:
                cls = ArtifactClass(cls_str)
            except ValueError:
                continue
            if counts.get(cls, 0) == 0:
                return  # 必需产物不足，不触发

        bag = await bag_repo.get_by_id(bag_id)
        if not bag or not bag.duration_s:
            return
        if float(bag.duration_s) < self._settings.ready_min_duration_s:
            return

        # 创建任务图首节点（§7.1：verify_assets after ready）；
        # 后继 align/qc/slice 由 orchestrator 在前置任务 done 后按依赖序推进（app.services.task_graph），
        # ENRICH 为 optional 项（M4/M5），worker 未实现前不创建。
        created = await task_repo.create(bag_id, input_rev, TaskType.VERIFY)
        if created:
            logger.info("task_created", bag_id=bag_id, task_type=TaskType.VERIFY.value, input_rev=input_rev)


__all__ = ["ProjectionService", "compute_input_rev"]
