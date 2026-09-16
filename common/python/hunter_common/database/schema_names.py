"""数据库 schema 名称常量（与 contracts/database/ddl/*.sql 完全一致）。"""

from __future__ import annotations

#: 车辆主数据（vehicles）
VEHICLE_SCHEMA = "vehicle_svc"
#: 用户与 RBAC（users/roles/user_roles/permissions/role_permissions）
USER_SCHEMA = "user_svc"
#: 场景库（scenes）
SCENE_SCHEMA = "scene_svc"
#: 采集接入（events/vehicle_telemetry）
COLLECTOR_SCHEMA = "data_collector"
#: 分析结果（algorithm_metrics）
ANALYTICS_SCHEMA = "data_analytics"
#: OTA（ota_versions/ota_tasks/ota_records）
OTA_SCHEMA = "ota_svc"
#: 预留：操控会话归档
REMOTE_CONTROL_SCHEMA = "remote_control"
#: 预留：网关审计
GATEWAY_SCHEMA = "gateway"

#: 全部 schema（供 Alembic 建/删 schema 使用）
ALL_SCHEMAS: tuple[str, ...] = (
    VEHICLE_SCHEMA,
    USER_SCHEMA,
    SCENE_SCHEMA,
    COLLECTOR_SCHEMA,
    ANALYTICS_SCHEMA,
    OTA_SCHEMA,
    REMOTE_CONTROL_SCHEMA,
    GATEWAY_SCHEMA,
)

#: 需要 TimescaleDB hypertable 化的表（schema 限定名）
HYPERTABLES: tuple[str, ...] = (
    f"{COLLECTOR_SCHEMA}.vehicle_telemetry",
    f"{ANALYTICS_SCHEMA}.algorithm_metrics",
)

#: hypertable 分区间隔与保留策略（契约：1 day / 90 days）
CHUNK_TIME_INTERVAL = "1 day"
RETENTION_INTERVAL = "90 days"

__all__ = [
    "ALL_SCHEMAS",
    "ANALYTICS_SCHEMA",
    "CHUNK_TIME_INTERVAL",
    "COLLECTOR_SCHEMA",
    "GATEWAY_SCHEMA",
    "HYPERTABLES",
    "OTA_SCHEMA",
    "REMOTE_CONTROL_SCHEMA",
    "RETENTION_INTERVAL",
    "SCENE_SCHEMA",
    "USER_SCHEMA",
    "VEHICLE_SCHEMA",
]
