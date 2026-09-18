"""单车辆升级记录数据访问（ota_svc.ota_records）。

- 唯一索引 uq_ota_records_task_vehicle (task_id, vehicle_id)：同任务同车仅一条记录，
  下发用 ``ON CONFLICT DO NOTHING`` 幂等插入；
- 排序 start_time DESC NULLS LAST, record_id DESC（命中 idx_ota_records_vehicle_start_time）；
- status/phase 取值 = 车端 9 态状态机（hunter_common.database.enums.OtaStatus）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from hunter_common.database import DatabaseSessionManager
from hunter_common.database.enums import OTA_TERMINAL_STATUSES, OtaStatus
from hunter_common.database.models import OtaRecord


@dataclass(frozen=True, slots=True)
class RecordSnapshot:
    """单车记录快照（灰度视图/进度聚合输入；epoch 秒）。"""

    vehicle_id: str
    status: OtaStatus
    progress: int
    start_time: float | None
    end_time: float | None


def _to_epoch(value: datetime | None) -> float | None:
    """TIMESTAMPTZ → Unix epoch 秒（None 透传）。"""
    return value.timestamp() if value is not None else None


class OtaRecordRepository:
    """ota_svc.ota_records 读写。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    def transaction(self) -> AsyncIterator[AsyncSession]:
        """事务性会话上下文（下发/状态推进组合操作共用一个事务）。"""
        return self._db.session()

    async def insert_released(self, session: AsyncSession, rows: list[dict[str, object]]) -> int:
        """批量插入下发记录（ON CONFLICT DO NOTHING 幂等；返回实际插入行数）。"""
        if not rows:
            return 0
        result = await session.execute(
            pg_insert(OtaRecord)
            .values(rows)
            .on_conflict_do_nothing(index_elements=["task_id", "vehicle_id"])
        )
        return int(result.rowcount or 0)

    async def map_by_vehicles(
        self, session: AsyncSession, task_id: UUID, vehicle_ids: list[str]
    ) -> dict[str, OtaRecord]:
        """按 (task_id, vehicle_id) 批量查询记录（命中唯一索引）。"""
        if not vehicle_ids:
            return {}
        rows = (
            (
                await session.execute(
                    select(OtaRecord).where(
                        OtaRecord.task_id == task_id,
                        OtaRecord.vehicle_id.in_(vehicle_ids),
                    )
                )
            )
            .scalars()
            .all()
        )
        return {row.vehicle_id: row for row in rows}

    async def get(
        self, session: AsyncSession, task_id: UUID, vehicle_id: str
    ) -> OtaRecord | None:
        """单条记录查询（回滚门禁判定用）。"""
        return (
            await session.execute(
                select(OtaRecord).where(
                    OtaRecord.task_id == task_id, OtaRecord.vehicle_id == vehicle_id
                )
            )
        ).scalar_one_or_none()


    async def list_by_task(
        self,
        *,
        task_id: UUID,
        vehicle_id: str | None = None,
        status: OtaStatus | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[OtaRecord], int]:
        """任务下记录分页（升级监控明细；排序 start_time DESC NULLS LAST, record_id DESC）。"""
        conditions = [OtaRecord.task_id == task_id]
        if vehicle_id is not None:
            conditions.append(OtaRecord.vehicle_id == vehicle_id)
        if status is not None:
            conditions.append(OtaRecord.status == status)
        return await self._list_page(conditions, page=page, page_size=page_size)

    async def list_by_vehicle(
        self,
        *,
        vehicle_id: str,
        status: OtaStatus | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[OtaRecord], int]:
        """单车升级历史分页（时间线；命中 idx_ota_records_vehicle_start_time）。"""
        conditions = [OtaRecord.vehicle_id == vehicle_id]
        if status is not None:
            conditions.append(OtaRecord.status == status)
        return await self._list_page(conditions, page=page, page_size=page_size)

    async def snapshot_by_task(self, task_id: UUID) -> dict[str, RecordSnapshot]:
        """任务全量记录快照（vehicle_id → RecordSnapshot；灰度视图/进度聚合输入）。"""
        async with self._db.session() as session:
            rows = (
                (await session.execute(select(OtaRecord).where(OtaRecord.task_id == task_id)))
                .scalars()
                .all()
            )
            return {
                row.vehicle_id: RecordSnapshot(
                    vehicle_id=row.vehicle_id,
                    status=row.status,
                    progress=row.progress,
                    start_time=_to_epoch(row.start_time),
                    end_time=_to_epoch(row.end_time),
                )
                for row in rows
            }

    async def apply_status(
        self,
        session: AsyncSession,
        *,
        task_id: UUID,
        vehicle_id: str,
        status: OtaStatus,
        phase: OtaStatus | None,
        progress: int,
        error_code: str | None,
        error_message: str | None,
        event_time: float,
    ) -> bool:
        """消费 ota_status 推进记录（幂等：同状态同进度跳过）；返回是否发生变更。

        终态（SUCCESS/ROLLED_BACK/FAILED）写入 end_time；error_* 原样落库（DDL TEXT）。
        """
        record = await self.get(session, task_id, vehicle_id)
        if record is None:
            return False
        unchanged = (
            record.status == status
            and record.progress == progress
            and (phase is None or record.phase == phase)
        )
        if unchanged:
            return False
        record.status = status
        if phase is not None:
            record.phase = phase
        record.progress = progress
        record.error_code = error_code
        record.error_message = error_message
        if status in OTA_TERMINAL_STATUSES:
            record.end_time = datetime.fromtimestamp(event_time, tz=timezone.utc)
        await session.flush()
        return True

    async def _list_page(
        self, conditions: list[object], *, page: int, page_size: int
    ) -> tuple[list[OtaRecord], int]:
        """公共分页查询（调用方组装过滤条件，参数化查询禁止拼接 SQL）。"""
        order_by = (OtaRecord.start_time.desc().nulls_last(), OtaRecord.record_id.desc())
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(OtaRecord).where(*conditions)
                    )
                ).scalar_one()
            )
            rows = (
                (
                    await session.execute(
                        select(OtaRecord)
                        .where(*conditions)
                        .order_by(*order_by)
                        .offset((page - 1) * page_size)
                        .limit(page_size)
                    )
                )
                .scalars()
                .all()
            )
            return list(rows), total


__all__ = ["OtaRecordRepository", "RecordSnapshot"]
