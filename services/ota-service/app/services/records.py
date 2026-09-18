"""升级记录业务服务（任务监控明细 / 单车历史时间线）。

契约：GET /tasks/{task_id}/records（含任务级 summary，404：任务不存在）、
GET /vehicles/{vehicle_id}/records（无 404，空列表合法；只读，不回写车辆版本）。
``duration_seconds`` = end_time - start_time（服务端派生，未结束为 null）。
"""
from __future__ import annotations

from uuid import UUID

from hunter_common.database.enums import OtaStatus
from hunter_common.database.models import OtaRecord
from hunter_common.exceptions import ResourceNotFoundError

from app.repositories.records import OtaRecordRepository
from app.repositories.tasks import OtaTaskRepository
from app.schemas.records import OtaRecordItem, OtaRecordList
from app.schemas.tasks import OtaTaskProgress
from app.services.rollout import build_task_progress


class RecordService:
    """升级记录业务逻辑（repository → 契约响应组装）。"""

    def __init__(
        self,
        record_repository: OtaRecordRepository,
        task_repository: OtaTaskRepository,
    ) -> None:
        self._records = record_repository
        self._tasks = task_repository

    async def list_task_records(
        self,
        task_id: UUID,
        *,
        vehicle_id: str | None = None,
        status: OtaStatus | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> OtaRecordList:
        """任务升级记录（升级监控明细；summary 供前端渲染批次卡片）。"""
        task = await self._tasks.get(task_id)
        if task is None:
            raise ResourceNotFoundError(details={"task_id": str(task_id)})
        rows, total = await self._records.list_by_task(
            task_id=task_id, vehicle_id=vehicle_id, status=status, page=page, page_size=page_size
        )
        snapshots = await self._records.snapshot_by_task(task_id)
        current_batch = self._current_batch_of(task.progress)
        summary = build_task_progress(
            total_vehicles=len(task.target_vehicles or []),
            snapshots=snapshots,
            current_batch=current_batch,
        )
        return OtaRecordList(
            items=[self._to_item(row) for row in rows],
            total=total,
            page=page,
            page_size=page_size,
            summary=summary,
        )

    async def list_vehicle_records(
        self,
        vehicle_id: str,
        *,
        status: OtaStatus | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> OtaRecordList:
        """单车升级历史时间线（跨任务；只读，不做版本推断回写）。"""
        rows, total = await self._records.list_by_vehicle(
            vehicle_id=vehicle_id, status=status, page=page, page_size=page_size
        )
        return OtaRecordList(
            items=[self._to_item(row) for row in rows],
            total=total,
            page=page,
            page_size=page_size,
            summary=None,  # 契约：summary 仅 /tasks/{task_id}/records 返回
        )

    @staticmethod
    def _current_batch_of(progress: dict[str, object] | None) -> int:
        """从 progress JSONB 快照读取当前批次（缺省 0=尚未开始）。"""
        if not progress:
            return 0
        value = progress.get("current_batch", 0)
        try:
            return max(0, min(4, int(value)))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _to_item(row: OtaRecord) -> OtaRecordItem:
        """ORM 行 → OtaRecordItem（TIMESTAMPTZ → epoch；duration 服务端派生）。"""
        start_time = row.start_time.timestamp()
        end_time = row.end_time.timestamp() if row.end_time else None
        return OtaRecordItem(
            record_id=row.record_id,
            task_id=row.task_id,
            vehicle_id=row.vehicle_id,
            from_version=row.from_version,
            to_version=row.to_version,
            status=row.status,
            phase=row.phase,
            progress=row.progress,
            error_code=row.error_code,
            error_message=row.error_message,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time) if end_time is not None else None,
        )


__all__ = ["OtaTaskProgress", "RecordService"]
