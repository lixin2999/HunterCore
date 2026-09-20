"""ORM 模型：采集接入（data_collector）。

- ``Event``：车辆事件（19 种类型 / 3 级等级，阈值见 contracts/database/enums.md）
- ``VehicleTelemetry``：车辆遥测时序（hypertable，按天分块、保留 90 天）

对应契约：contracts/database/ddl/04_events.sql、ddl/05_timeseries.sql。
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Double,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hunter_common.database.base import Base, StrEnumType
from hunter_common.database.enums import EventLevel, EventType
from hunter_common.database.schema_names import COLLECTOR_SCHEMA

_EVENT_TYPE_VALUES = ", ".join(f"'{t.value}'" for t in EventType)
_EVENT_LEVEL_VALUES = ", ".join(f"'{level.value}'" for level in EventLevel)


class Event(Base):
    """车辆事件（data_collector.events）。

    幂等：唯一索引 (vehicle_id, event_type, event_time) 防止 Kafka at-least-once 重放重复入库。
    """

    __tablename__ = "events"
    __table_args__ = (
        CheckConstraint(f"event_type IN ({_EVENT_TYPE_VALUES})", name="events_event_type_check"),
        CheckConstraint(f"event_level IN ({_EVENT_LEVEL_VALUES})", name="events_event_level_check"),
        Index("uq_events_vehicle_type_time", "vehicle_id", "event_type", "event_time", unique=True),
        Index("idx_events_vehicle_time", "vehicle_id", text("event_time DESC")),
        Index("idx_events_type_time", "event_type", text("event_time DESC")),
        Index(
            "idx_events_unacknowledged",
            "event_level",
            text("event_time DESC"),
            postgresql_where=text("acknowledged = FALSE"),
        ),
        Index("idx_events_data_json", "data_json", postgresql_using="gin"),
        {"schema": COLLECTOR_SCHEMA},
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[EventType] = mapped_column(StrEnumType(EventType), nullable=False)
    event_level: Mapped[EventLevel] = mapped_column(StrEnumType(EventLevel), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    data_json: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    data_file_url: Mapped[str | None] = mapped_column(Text)
    acknowledged: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
    acknowledged_by: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    acknowledge_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # 无 relationship：``vehicle_id`` / ``acknowledged_by`` 均为逻辑外键，
    # 跨服务补全一律走 REST（contracts/database/orm-mapping.md 第 2.1 节）


class VehicleTelemetry(Base):
    """车辆遥测时序（data_collector.vehicle_telemetry，hypertable，保留 90 天）。

    字段 = Kafka 遥测消息的扁平化映射（contracts/kafka/schemas/telemetry.schema.json）：
    chassis / localization / perception / planning / control / system 六段，段内字段不加前缀。
    主键 (time, vehicle_id) 必须包含分区键 ``time``（TimescaleDB 约束）。
    写入性能：批量 executemany / COPY（≥10000 点/秒），禁止逐条 commit。
    """

    __tablename__ = "vehicle_telemetry"
    __table_args__ = (
        PrimaryKeyConstraint("time", "vehicle_id", name="pk_vehicle_telemetry"),
        CheckConstraint(
            "battery_soc IS NULL OR (battery_soc BETWEEN 0 AND 100)",
            name="vehicle_telemetry_battery_soc_check",
        ),
        Index("idx_vehicle_telemetry_vehicle_time", "vehicle_id", text("time DESC")),
        {"schema": COLLECTOR_SCHEMA},
    )

    # ---- 标识与分区键 ----
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    seq: Mapped[int | None] = mapped_column(BigInteger)

    # ---- chassis（底盘） ----
    velocity: Mapped[float | None] = mapped_column(Double)
    steering_angle: Mapped[float | None] = mapped_column(Double)
    battery_voltage: Mapped[float | None] = mapped_column(Double)
    battery_soc: Mapped[int | None] = mapped_column(SmallInteger)
    battery_current: Mapped[float | None] = mapped_column(Double)
    battery_temp: Mapped[float | None] = mapped_column(Double)
    control_mode: Mapped[str | None] = mapped_column(Text)
    vehicle_state: Mapped[str | None] = mapped_column(Text)
    fault_code: Mapped[int | None] = mapped_column(Integer)
    motor_rpm: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))
    motor_current: Mapped[list[float] | None] = mapped_column(ARRAY(Double))
    motor_temp: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))

    # ---- localization（定位） ----
    x: Mapped[float | None] = mapped_column(Double)
    y: Mapped[float | None] = mapped_column(Double)
    z: Mapped[float | None] = mapped_column(Double)
    roll: Mapped[float | None] = mapped_column(Double)
    pitch: Mapped[float | None] = mapped_column(Double)
    heading: Mapped[float | None] = mapped_column(Double)
    linear_velocity: Mapped[list[float] | None] = mapped_column(ARRAY(Double))
    angular_velocity: Mapped[list[float] | None] = mapped_column(ARRAY(Double))
    position_std: Mapped[float | None] = mapped_column(Double)
    heading_std: Mapped[float | None] = mapped_column(Double)

    # ---- perception（感知） ----
    detected_objects: Mapped[int | None] = mapped_column(Integer)
    fps: Mapped[float | None] = mapped_column(Double)
    latency_ms: Mapped[float | None] = mapped_column(Double)
    object_types: Mapped[dict[str, int] | None] = mapped_column(JSONB)

    # ---- planning（规划） ----
    trajectory_length: Mapped[float | None] = mapped_column(Double)
    trajectory_points: Mapped[int | None] = mapped_column(Integer)
    planning_latency_ms: Mapped[float | None] = mapped_column(Double)
    current_behavior: Mapped[str | None] = mapped_column(Text)

    # ---- control（控制） ----
    target_velocity: Mapped[float | None] = mapped_column(Double)
    target_steer: Mapped[float | None] = mapped_column(Double)
    velocity_error: Mapped[float | None] = mapped_column(Double)
    steer_error: Mapped[float | None] = mapped_column(Double)
    control_latency_ms: Mapped[float | None] = mapped_column(Double)

    # ---- system（车载计算平台） ----
    cpu_usage: Mapped[float | None] = mapped_column(Double)
    gpu_usage: Mapped[float | None] = mapped_column(Double)
    memory_usage_mb: Mapped[int | None] = mapped_column(BigInteger)
    gpu_temp: Mapped[float | None] = mapped_column(Double)
    cpu_temp: Mapped[float | None] = mapped_column(Double)
    network_rssi: Mapped[int | None] = mapped_column(Integer)
    network_latency_ms: Mapped[float | None] = mapped_column(Double)

    # 无 relationship：时序表不建外键（避免写入放大），``vehicle_id`` 为逻辑外键，
    # 跨服务补全一律走 REST（contracts/database/orm-mapping.md 第 2.1 节）


__all__ = ["Event", "VehicleTelemetry"]

