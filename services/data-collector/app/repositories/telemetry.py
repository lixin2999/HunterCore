"""车辆遥测查询（TimescaleDB hypertable，只读）。

契约（x-hunter-telemetry-query-contract）：
- time 区间过滤走 hypertable 分区裁剪；排序 time DESC；
- 分页用 LIMIT/OFFSET；total 为区间 COUNT（开销大，文档标注待确认）；
- 只读查询；事务边界由 DatabaseSessionManager.session() 统一管理。

行 → 契约模型映射（telemetry_row_to_dict）：
- ORM ``time``（TIMESTAMPTZ）→ 契约 ``time``（Unix epoch 秒 float）；
- perception 段 ORM 列名为 ``fps`` / ``latency_ms``（无前缀，与消息结构一致）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from hunter_common.database import TELEMETRY_CONFLICT_COLUMNS, DatabaseSessionManager
from hunter_common.database.models import VehicleTelemetry
from sqlalchemy import Row, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession


class TelemetryRepository:
    """data_collector.vehicle_telemetry 只读查询 + 批量幂等写入。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    def transaction(self) -> AsyncIterator[AsyncSession]:
        """事务性会话上下文（批量写入与投递编排共用；提交/回滚由会话上下文负责）。"""
        return self._db.session()

    async def insert_points(
        self, session: AsyncSession, rows: Sequence[Mapping[str, Any]]
    ) -> int:
        """批量幂等写入遥测点，返回「尝试写入行数」。

        - 单条 ``INSERT ... VALUES (多行) ON CONFLICT (time, vehicle_id) DO NOTHING``：
          批量插入满足契约「≥ 10000 点/秒」，禁止逐条 commit；
        - 冲突静默跳过 → Kafka at-least-once 重放幂等（消费组 data-collector-telemetry）；
        - 返回值为「尝试写入行数」而非实际新增行数（asyncpg 驱动不支持 sane multi-rowcount，
          与 ``BaseRepository.bulk_create_*`` 语义一致；重复率由消费指标/日志观察）。
        """
        if not rows:
            return 0
        await session.execute(
            pg_insert(VehicleTelemetry)
            .values([dict(row) for row in rows])
            .on_conflict_do_nothing(index_elements=list(TELEMETRY_CONFLICT_COLUMNS))
        )
        return len(rows)

    async def query_page(
        self,
        vehicle_id: str,
        start_time: float,
        end_time: float,
        page: int,
        page_size: int,
    ) -> tuple[list[Row], int]:
        """时间区间分页查询；返回 (行列表, 区间 total)。

        行含 vehicle_telemetry 全部扁平列；页序 time DESC。
        """
        start_dt = datetime.fromtimestamp(start_time, tz=UTC)
        end_dt = datetime.fromtimestamp(end_time, tz=UTC)
        async with self._db.session() as session:
            stmt = (
                select(VehicleTelemetry)
                .where(
                    VehicleTelemetry.vehicle_id == vehicle_id,
                    VehicleTelemetry.time >= start_dt,
                    VehicleTelemetry.time <= end_dt,
                )
                .order_by(VehicleTelemetry.time.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            rows = (await session.execute(stmt)).scalars().all()
            count_stmt = select(func.count()).select_from(VehicleTelemetry).where(
                VehicleTelemetry.vehicle_id == vehicle_id,
                VehicleTelemetry.time >= start_dt,
                VehicleTelemetry.time <= end_dt,
            )
            total = int((await session.execute(count_stmt)).scalar_one())
            return list(rows), total


def telemetry_row_to_dict(row: Row) -> dict[str, object]:
    """ORM 行 → 六段消息结构（5.3.3 节；段内字段按列名严格映射）。

    列 → 段的映射与 data-collector.yaml TelemetrySample 完全一致；
    嵌套对象列（object_types）为 JSONB dict，直接透传；
    全 None 的段输出 None（契约：未采集段返回 null）。
    """
    mapped: dict[str, object] = {
        "time": row.time.timestamp() if row.time is not None else 0.0,
        "vehicle_id": row.vehicle_id,
        "seq": row.seq,
        "chassis": {
            "velocity": row.velocity,
            "steering_angle": row.steering_angle,
            "battery_voltage": row.battery_voltage,
            "battery_soc": row.battery_soc,
            "battery_current": row.battery_current,
            "battery_temp": row.battery_temp,
            "control_mode": row.control_mode,
            "vehicle_state": row.vehicle_state,
            "fault_code": row.fault_code,
            "motor_rpm": row.motor_rpm,
            "motor_current": row.motor_current,
            "motor_temp": row.motor_temp,
        },
        "localization": {
            "x": row.x,
            "y": row.y,
            "z": row.z,
            "roll": row.roll,
            "pitch": row.pitch,
            "heading": row.heading,
            "linear_velocity": row.linear_velocity,
            "angular_velocity": row.angular_velocity,
            "position_std": row.position_std,
            "heading_std": row.heading_std,
        },
        "perception": {
            "detected_objects": row.detected_objects,
            "fps": row.fps,
            "latency_ms": row.latency_ms,
            "object_types": row.object_types,
        },
        "planning": {
            "trajectory_length": row.trajectory_length,
            "trajectory_points": row.trajectory_points,
            "planning_latency_ms": row.planning_latency_ms,
            "current_behavior": row.current_behavior,
        },
        "control": {
            "target_velocity": row.target_velocity,
            "target_steer": row.target_steer,
            "velocity_error": row.velocity_error,
            "steer_error": row.steer_error,
            "control_latency_ms": row.control_latency_ms,
        },
        "system": {
            "cpu_usage": row.cpu_usage,
            "gpu_usage": row.gpu_usage,
            "memory_usage_mb": row.memory_usage_mb,
            "gpu_temp": row.gpu_temp,
            "cpu_temp": row.cpu_temp,
            "network_rssi": row.network_rssi,
            "network_latency_ms": row.network_latency_ms,
        },
    }
    # 剔除全 None 的段（未采集段输出 null）；段内 None 字段剔除（字段 nullable）
    cleaned: dict[str, object] = {}
    for segment, payload in mapped.items():
        if isinstance(payload, dict):
            filtered = {k: v for k, v in payload.items() if v is not None}
            cleaned[segment] = filtered if filtered else None
        else:
            cleaned[segment] = payload
    return cleaned


__all__ = ["TelemetryRepository", "telemetry_row_to_dict"]
