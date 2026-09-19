"""事件查询/确认数据访问（data_collector.events）。

- 查询按筛选条件分页，event_time DESC（契约排序约定）；
- 确认仅 UPDATE acknowledged/acknowledged_by/acknowledge_time 三列，幂等；
- 事务边界由 DatabaseSessionManager.session() 统一管理（出栈提交/回滚）；
- ORM 时间列为 TIMESTAMPTZ（datetime），服务层转换为 Unix epoch 秒（契约）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from hunter_common.database import EVENT_IDEMPOTENCY_COLUMNS, DatabaseSessionManager
from hunter_common.database.enums import EventLevel, EventType
from hunter_common.database.models import Event
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession


class EventRepository:
    """data_collector.events 读写。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    def transaction(self) -> AsyncIterator[AsyncSession]:
        """事务性会话上下文（事件入库与 event_raw 投递编排共用）。"""
        return self._db.session()

    async def insert_events(self, session: AsyncSession, rows: Sequence[Mapping[str, Any]]) -> int:
        """批量幂等写入事件，返回「尝试写入行数」。

        幂等键 = DDL 唯一索引 ``uq_events_vehicle_type_time``
        （``ON CONFLICT (vehicle_id, event_type, event_time) DO NOTHING``）：
        Kafka at-least-once 重放不会产生重复事件（消费组 data-collector-events）。
        """
        if not rows:
            return 0
        await session.execute(
            pg_insert(Event)
            .values([dict(row) for row in rows])
            .on_conflict_do_nothing(index_elements=list(EVENT_IDEMPOTENCY_COLUMNS))
        )
        return len(rows)

    async def list_events(
        self,
        vehicle_id: str | None = None,
        event_type: EventType | None = None,
        event_level: EventLevel | None = None,
        acknowledged: bool | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Event], int]:
        """筛选事件列表（event_time DESC + 分页）；返回 (事件列表, total)。"""
        filters = self._build_filters(
            vehicle_id=vehicle_id,
            event_type=event_type,
            event_level=event_level,
            acknowledged=acknowledged,
            start_time=start_time,
            end_time=end_time,
        )
        async with self._db.session() as session:
            stmt = (
                select(Event)
                .where(*filters)
                .order_by(Event.event_time.desc(), Event.event_id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            items = list((await session.execute(stmt)).scalars().all())
            count_stmt = select(func.count()).select_from(Event).where(*filters)
            total = int((await session.execute(count_stmt)).scalar_one())
            return items, total

    async def get_by_id(self, event_id: int) -> Event | None:
        """按主键读取事件（不存在返回 None）。"""
        async with self._db.session() as session:
            stmt = select(Event).where(Event.event_id == event_id)
            return (await session.execute(stmt)).scalars().first()

    async def acknowledge(self, event_id: int, user_id: str) -> Event | None:
        """确认事件（幂等；仅更新确认三列，返回更新后实体）。

        若已确认则跳过 UPDATE（保留首次确认人与时间，审计优先）；
        acknowledged_by 列为 UUID 类型（users.user_id）。
        """
        async with self._db.session() as session:
            stmt = select(Event).where(Event.event_id == event_id)
            event = (await session.execute(stmt)).scalars().first()
            if event is None:
                return None
            if not event.acknowledged:
                event.acknowledged = True
                event.acknowledged_by = UUID(user_id)
                event.acknowledge_time = datetime.now(UTC)
                session.add(event)
            return event

    @staticmethod
    def _build_filters(
        vehicle_id: str | None,
        event_type: EventType | None,
        event_level: EventLevel | None,
        acknowledged: bool | None,
        start_time: float | None,
        end_time: float | None,
    ) -> list[Any]:
        """构建筛选条件（参数化，禁止字符串拼接 SQL；时间入参 epoch 秒 → UTC datetime）。"""
        filters: list[Any] = []
        if vehicle_id:
            filters.append(Event.vehicle_id == vehicle_id)
        if event_type is not None:
            filters.append(Event.event_type == event_type)
        if event_level is not None:
            filters.append(Event.event_level == event_level)
        if acknowledged is not None:
            filters.append(Event.acknowledged == acknowledged)
        if start_time is not None:
            filters.append(
                Event.event_time >= datetime.fromtimestamp(start_time, tz=UTC)
            )
        if end_time is not None:
            filters.append(Event.event_time <= datetime.fromtimestamp(end_time, tz=UTC))
        return filters


__all__ = ["EventRepository"]
