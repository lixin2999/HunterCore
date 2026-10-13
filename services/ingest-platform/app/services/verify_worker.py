"""产物核验 Worker（§7.1 verify_assets：抽样 HEAD + hash 核验）。

对齐设计文档 §7.1：就绪后首先执行 verify，核验车端 parsed 层产物
在对象存储中真实存在且与 manifest 声明一致；核验结果写回 artifacts.upload_state
（verified / missing / corrupt）。必需产物缺失时任务失败退避重试，
超限转 dead 由运维介入（§7.1 bag.incomplete → reupload 闭环留待命令侧专项）。

说明：车端上传对象不携带 x-amz-meta-sha256，hash 逐字节核验由 QC 抽样
（§7.4 hash_integrity）承担；本 Worker 以 HEAD 存在性 + 声明 size 比对为主。
"""
from __future__ import annotations

from typing import Any

from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import ArtifactClass, Task
from app.repositories.artifacts import ArtifactRepository
from app.storage.minio_client import LakeStorage

logger = get_logger("app.services.verify_worker")


class VerifyWorker:
    """parsed 层产物核验（§7.1 task_graph 首节点）。"""

    def __init__(
        self,
        settings: Settings,
        storage: LakeStorage,
        db_mgr: DatabaseSessionManager,
    ) -> None:
        self._settings = settings
        self._storage = storage
        self._db_mgr = db_mgr

    async def execute(self, task: Task) -> dict[str, Any]:
        bag_id = task.bag_id or ""
        input_rev = task.input_rev or ""

        required_missing = False
        async with self._db_mgr.session() as session:
            repo = ArtifactRepository(session)
            artifacts = await repo.list_by_bag(bag_id)
            if not artifacts:
                raise ValueError(f"no artifacts registered for bag: {bag_id}")

            required_classes = self._required_classes()
            checked = verified = missing = corrupt = 0
            for artifact in artifacts:
                checked += 1
                meta = await self._storage.ahead_parsed(artifact.object_key)
                if meta is None:
                    await repo.set_missing(artifact.object_key)
                    missing += 1
                    if artifact.artifact_class in required_classes:
                        required_missing = True
                elif artifact.size_bytes is not None and meta.size != int(artifact.size_bytes):
                    await repo.set_corrupt(artifact.object_key)
                    corrupt += 1
                    if artifact.artifact_class in required_classes:
                        required_missing = True
                else:
                    # 对象元数据携带 sha256（平台侧写入）时顺带比对；车端对象无此元数据
                    if artifact.sha256 and meta.sha256 and meta.sha256 != artifact.sha256:
                        await repo.set_corrupt(artifact.object_key)
                        corrupt += 1
                        if artifact.artifact_class in required_classes:
                            required_missing = True
                        continue
                    await repo.set_verified(artifact.object_key)
                    verified += 1

            result = {
                "bag_id": bag_id,
                "input_rev": input_rev,
                "checked": checked,
                "verified": verified,
                "missing": missing,
                "corrupt": corrupt,
            }
            logger.info("verify_done", **result)

        # 必需产物缺失/损坏：核验事务照常提交（状态留痕），随后抛错触发任务退避重试
        if required_missing:
            raise ValueError(f"required artifacts missing or corrupt: bag={bag_id}")
        return result

    def _required_classes(self) -> set[ArtifactClass]:
        classes: set[ArtifactClass] = set()
        for cls_str in self._settings.ready_required_class_list:
            try:
                classes.add(ArtifactClass(cls_str))
            except ValueError:
                continue
        return classes


__all__ = ["VerifyWorker"]
