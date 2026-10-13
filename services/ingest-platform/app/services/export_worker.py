"""export-worker：数据集导出任务执行器（§8.2/§9 任务化，审查 #9）。

从 tasks.payload 取 dataset_id/version，调用 DatasetService.export 推进；
崩溃后可由 orchestrator lease 队列重新领取（export 本体幂等：state 由 running→done）。
"""
from __future__ import annotations

from typing import Any

from hunter_common.logging import get_logger

from app.models.ingest import Task
from app.services.dataset_service import DatasetService

logger = get_logger("app.services.export_worker")


class ExportWorker:
    """EXPORT 任务 Worker（§9）。"""

    def __init__(self, dataset_service: DatasetService) -> None:
        self._dataset_service = dataset_service

    async def execute(self, task: Task) -> dict[str, Any]:
        payload = task.payload or {}
        dataset_id = str(payload.get("dataset_id", ""))
        version = str(payload.get("version", ""))
        if not dataset_id or not version:
            raise ValueError(f"EXPORT 任务缺少 dataset_id/version: payload={payload}")
        logger.info("export_start", dataset_id=dataset_id, version=version)
        return await self._dataset_service.export(dataset_id, version)


__all__ = ["ExportWorker"]
