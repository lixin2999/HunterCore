"""采集接入数据访问层（data_collector：events + vehicle_telemetry）。

契约：contracts/database/ddl/04_events.sql、ddl/05_timeseries.sql + orm-mapping.md 第 3 节。
- ``events``：唯一索引 (vehicle_id, event_type, event_time) 保证 Kafka at-least-once 重放幂等
  （``insert_events`` 批量幂等写入 / ``get_by_vehicle_type_time`` 落库前预检）；
- ``vehicle_telemetry``：hypertable，批量写入（≥10000 点/秒）+ ``ON CONFLICT (time, vehicle_id)``
  幂等跳过；90 天保留由 TimescaleDB 保留策略负责（``purge_before`` 分批删除，仅供测试/应急）；
- ``event_level`` 必须由 event_type 推导（``enums.event_level_for``），本层不推断等级（禁止放宽阈值）；
- 复合主键表（``vehicle_telemetry``）禁用基类的 ``get`` / ``get_or_raise`` / ``hard_delete``
  （首列主键 ``time`` 会跨车辆误命中），改用 ``get_point`` / ``list_points`` / ``latest_point``。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, delete, literal_column, select, update

from hunter_common.database.enums import EventLevel, EventType
from hunter_common.database.models import Event, VehicleTelemetry
from hunter_common.database.repository import (
    DEFAULT_PURGE_BATCH_SIZE,
    MAX_SERIES_POINTS,
    BaseRepository,
)
from hunter_common.exceptions import InvalidParameterError

#: 事件幂等冲突列 = DDL 唯一索引 uq_events_vehicle_type_time
EVENT_IDEMPOTENCY_COLUMNS: tuple[str, ...] = ("vehicle_id", "event_type", "event_time")
#: 遥测幂等冲突列 = DDL 主键 PRIMARY KEY (time, vehicle_id)
TELEMETRY_CONFLICT_COLUMNS: tuple[str, ...] = ("time", "vehicle_id")


class EventRepository(BaseRepository[Event]):
    """车辆事件读写（18 种事件类型 / 3 级等级）。"""

    model = Event
    #: 事件列表默认排序：事件时间倒序（车端时间，非入库时间）
    default_order_by = ("-event_time", "-event_id")

    async def get_by_vehicle_type_time(
        self, vehicle_id: str, event_type: EventType, event_time: datetime
    ) -> Event | None:
        """按幂等唯一键查询事件（唯一索引 uq_events_vehicle_type_time）。

        落库前用它判断消息是否已处理（Kafka at-least-once 重放）。
        """
        return await self.find_one(
            Event.vehicle_id == vehicle_id,
            Event.event_type == event_type,
            Event.event_time == event_time,
        )

    async def list_by_vehicle(
        self,
        vehicle_id: str,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        """车辆事件时间线（命中 idx_events_vehicle_time：vehicle_id + event_time DESC）。"""
        conditions: list[Any] = [Event.vehicle_id == vehicle_id]
        if start_time is not None:
            conditions.append(Event.event_time >= start_time)
        if end_time is not None:
            conditions.append(Event.event_time <= end_time)
        return await self.find_all(*conditions, limit=limit)

    async def list_unacknowledged(
        self, *, event_level: EventLevel | None = None, limit: int | None = None
    ) -> list[Event]:
        """未确认事件（命中部分索引 idx_events_unacknowledged：level + event_time DESC）。"""
        conditions: list[Any] = [Event.acknowledged.is_(False)]
        if event_level is not None:
            conditions.append(Event.event_level == event_level)
        return await self.find_all(*conditions, limit=limit)

    async def insert_events(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """批量幂等写入事件（executemany + ``ON CONFLICT (vehicle_id, event_type, event_time) DO NOTHING``）。

        幂等键 = DDL 唯一索引 ``uq_events_vehicle_type_time``：Kafka at-least-once 重放的
        重复事件不会重复入库（消费者无需先查后写，避免"先查后写"的竞态与额外往返）。
        返回**提交（attempted）行数**（被跳过的重复行仍计入，见
        :meth:`BaseRepository.bulk_create_ignore_conflicts` 的返回值说明）；
        ``event_level`` 由调用方按 ``enums.event_level_for`` 推导后传入（本层不推断等级）。
        """
        return await self.bulk_create_ignore_conflicts(
            rows, conflict_columns=EVENT_IDEMPOTENCY_COLUMNS
        )

    async def acknowledge(
        self, event_id: int, *, acknowledged_by: UUID, at: datetime | None = None
    ) -> bool:
        """确认事件（单条条件 UPDATE：原子、1 次往返）；事件不存在返回 ``False``。

        - WHERE 带 ``acknowledged IS FALSE``：并发确认时不会互相覆盖，
          「首次确认人与时间」始终保持（审计优先，与旧实现的语义一致）；
        - 未命中时（已确认 / 不存在）再走一次轻量 EXISTS 查询区分"不存在"，返回 ``False``。
        """
        stmt = (
            update(Event)
            .where(Event.event_id == event_id, Event.acknowledged.is_(False))
            .values(
                acknowledged=True,
                acknowledged_by=acknowledged_by,
                acknowledge_time=at or datetime.now(UTC),
            )
        )
        result = await self.session.execute(stmt)
        if int(getattr(result, "rowcount", 0) or 0):
            return True
        return await self.exists(filters={"event_id": event_id})


class VehicleTelemetryRepository(BaseRepository[VehicleTelemetry]):
    """车辆遥测时序读写（hypertable，复合主键 (time, vehicle_id)）。

    注意：复合主键无单一「主键值」，基类 ``get`` / ``get_or_raise`` / ``hard_delete``
    会被显式拒绝（``NotImplementedError``），请使用 ``get_point`` / ``list_points`` / ``latest_point``。
    ``max_query_limit`` 放宽到 ``MAX_SERIES_POINTS``（轨迹回放 / 指标计算序列读取），
    调用方必须给出时间窗（``start_time``/``end_time``），否则单次拉取会拖垮 P95。
    """

    model = VehicleTelemetry
    #: 轨迹回放/查询默认排序（命中 idx_vehicle_telemetry_vehicle_time：time DESC；time 为 NOT NULL）
    default_order_by = ("-time",)
    #: 时序序列读取上限（契约 orm-mapping 第 3.3 节）
    max_query_limit = MAX_SERIES_POINTS

    async def insert_points(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """批量写入遥测点（executemany + ``ON CONFLICT (time, vehicle_id) DO NOTHING``）。

        性能契约：≥ 10000 点/秒（设计文档性能指标），禁止逐条 INSERT + 逐条 commit。
        """
        return await self.bulk_create_ignore_conflicts(
            rows, conflict_columns=TELEMETRY_CONFLICT_COLUMNS
        )

    async def get_point(self, vehicle_id: str, at: datetime) -> VehicleTelemetry | None:
        """按复合主键 (time, vehicle_id) 查询单个遥测点。"""
        return await self.find_one(
            VehicleTelemetry.vehicle_id == vehicle_id, VehicleTelemetry.time == at
        )

    async def list_points(
        self,
        vehicle_id: str,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
    ) -> list[VehicleTelemetry]:
        """车辆时间区间遥测（轨迹回放 / 指标计算，时间倒序）。"""
        conditions: list[Any] = [VehicleTelemetry.vehicle_id == vehicle_id]
        if start_time is not None:
            conditions.append(VehicleTelemetry.time >= start_time)
        if end_time is not None:
            conditions.append(VehicleTelemetry.time <= end_time)
        return await self.find_all(*conditions, limit=limit)

    async def latest_point(self, vehicle_id: str) -> VehicleTelemetry | None:
        """车辆最近一个遥测点（实时看板兜底数据源；实时态优先读 Redis）。"""
        rows = await self.find_all(
            VehicleTelemetry.vehicle_id == vehicle_id, order_by=("-time",), limit=1
        )
        return rows[0] if rows else None

    async def purge_before(
        self,
        cutoff: datetime,
        *,
        batch_size: int = DEFAULT_PURGE_BATCH_SIZE,
        max_rows: int | None = None,
    ) -> int:
        """分批删除 ``time < cutoff`` 的遥测点，返回删除行数。

        ⚠ 90 天保留策略由 TimescaleDB ``add_retention_policy`` 以 chunk 级 drop 执行（契约），
        本方法仅用于测试数据清理与应急场景，禁止在生产定时任务中替代保留策略。

        实现：``WHERE ctid IN (SELECT ctid ... LIMIT n)`` 分批 DELETE ——
        每批一个短事务，避免单条大 DELETE 造成长事务锁等待、WAL 膨胀与 chunk 膨胀；
        ``max_rows`` 可设总量上限（达到即提前返回）。

        Raises:
            InvalidParameterError: ``batch_size < 1`` 或 ``max_rows < 1``。
        """
        if batch_size < 1:
            raise InvalidParameterError("batch_size 必须 ≥ 1", details={"batch_size": batch_size})
        if max_rows is not None and max_rows < 1:
            raise InvalidParameterError("max_rows 必须 ≥ 1", details={"max_rows": max_rows})
        removed_total = 0
        while True:
            remaining = None if max_rows is None else max_rows - removed_total
            if remaining is not None and remaining < 1:
                break
            batch = batch_size if remaining is None else min(batch_size, remaining)
            target: Select[Any] = (
                select(literal_column("ctid")).where(VehicleTelemetry.time < cutoff).limit(batch)
            )
            stmt = (
                delete(VehicleTelemetry)
                .where(literal_column("ctid").in_(target))
                # 批量物理删除不参与会话同步（避免同步开销与隐式预取 SELECT）
                .execution_options(synchronize_session=False)
            )
            result = await self.session.execute(stmt)
            removed = int(getattr(result, "rowcount", 0) or 0)
            removed_total += removed
            if removed < batch:
                break
        return removed_total


__all__ = [
    "EVENT_IDEMPOTENCY_COLUMNS",
    "TELEMETRY_CONFLICT_COLUMNS",
    "EventRepository",
    "VehicleTelemetryRepository",
]
