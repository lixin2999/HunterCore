"""HunterCore 数据访问层（SQLAlchemy 2.0 异步 + TimescaleDB）。

模块结构：
- ``session``        异步引擎/会话管理（DatabaseSessionManager，FastAPI 依赖注入）
- ``base``           声明式 Base、StrEnumType、混入类（创建时间/更新时间/软删除）、列工厂
- ``enums``          受控词表 StrEnum（车辆状态/事件类型与等级/OTA 状态机/算法模块）
- ``schema_names``   schema 名称常量、hypertable 清单与分区间隔/保留策略
- ``repository``     通用 Repository 基类（CRUD + 分页 + 软删除 + 批量写入 + 条件查询）
- ``models``         全部 ORM 模型（与 contracts/database/ddl/*.sql 一一对应）
- ``repositories``   每个模型对应的具体 Repository（映射见 orm-mapping.md 第 3 节）
- ``migrations``     Alembic 配置与版本（初始迁移 0001 建表 + hypertable + 保留策略）

契约：``contracts/database/``（DDL / ER / 受控词表）。结构变更必须同步 DDL → ORM → Alembic。
"""
from __future__ import annotations

from hunter_common.database.base import (
    Base,
    CreateTimeMixin,
    SoftDeleteMixin,
    StrEnumType,
    UpdateTimeMixin,
    create_time_column,
    supports_soft_delete,
    uuid_primary_key_column,
)
from hunter_common.database.enums import (
    EVENT_LEVEL_BY_TYPE,
    OTA_ACTIVE_STATUSES,
    OTA_TERMINAL_STATUSES,
    EventLevel,
    EventType,
    MetricModule,
    OtaStatus,
    OtaTaskStatus,
    OtaVersionStatus,
    PermissionAction,
    PermissionResource,
    RoleStatus,
    SceneStatus,
    UserStatus,
    VehicleStatus,
    event_level_for,
)
from hunter_common.database.repositories import (
    EVENT_IDEMPOTENCY_COLUMNS,
    METRIC_CONFLICT_COLUMNS,
    REPOSITORY_BY_MODEL,
    TELEMETRY_CONFLICT_COLUMNS,
    AlgorithmMetricRepository,
    EventRepository,
    OtaRecordRepository,
    OtaTaskRepository,
    OtaVersionRepository,
    PermissionRepository,
    RolePermissionRepository,
    RoleRepository,
    SceneRepository,
    UserRepository,
    UserRoleRepository,
    VehicleRepository,
    VehicleTelemetryRepository,
)
from hunter_common.database.repository import (
    BULK_CHUNK_SIZE,
    DEFAULT_PAGE_SIZE,
    DEFAULT_PURGE_BATCH_SIZE,
    MAX_PAGE_SIZE,
    MAX_SERIES_POINTS,
    BaseRepository,
    PageResult,
)
from hunter_common.database.schema_names import (
    ALL_SCHEMAS,
    ANALYTICS_SCHEMA,
    CHUNK_TIME_INTERVAL,
    COLLECTOR_SCHEMA,
    GATEWAY_SCHEMA,
    HYPERTABLES,
    OTA_SCHEMA,
    REMOTE_CONTROL_SCHEMA,
    RETENTION_INTERVAL,
    SCENE_SCHEMA,
    USER_SCHEMA,
    VEHICLE_SCHEMA,
)
from hunter_common.database.session import DatabaseSessionManager

__all__ = [
    "ALL_SCHEMAS",
    "ANALYTICS_SCHEMA",
    "BULK_CHUNK_SIZE",
    "CHUNK_TIME_INTERVAL",
    "COLLECTOR_SCHEMA",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_PURGE_BATCH_SIZE",
    "EVENT_IDEMPOTENCY_COLUMNS",
    "EVENT_LEVEL_BY_TYPE",
    "GATEWAY_SCHEMA",
    "HYPERTABLES",
    "MAX_PAGE_SIZE",
    "MAX_SERIES_POINTS",
    "METRIC_CONFLICT_COLUMNS",
    "OTA_ACTIVE_STATUSES",
    "OTA_SCHEMA",
    "OTA_TERMINAL_STATUSES",
    "REMOTE_CONTROL_SCHEMA",
    "REPOSITORY_BY_MODEL",
    "RETENTION_INTERVAL",
    "SCENE_SCHEMA",
    "TELEMETRY_CONFLICT_COLUMNS",
    "USER_SCHEMA",
    "VEHICLE_SCHEMA",
    "AlgorithmMetricRepository",
    "Base",
    "BaseRepository",
    "CreateTimeMixin",
    "DatabaseSessionManager",
    "EventLevel",
    "EventRepository",
    "EventType",
    "MetricModule",
    "OtaRecordRepository",
    "OtaStatus",
    "OtaTaskRepository",
    "OtaTaskStatus",
    "OtaVersionRepository",
    "OtaVersionStatus",
    "PageResult",
    "PermissionAction",
    "PermissionRepository",
    "PermissionResource",
    "RolePermissionRepository",
    "RoleRepository",
    "RoleStatus",
    "SceneRepository",
    "SceneStatus",
    "SoftDeleteMixin",
    "StrEnumType",
    "UpdateTimeMixin",
    "UserRepository",
    "UserRoleRepository",
    "UserStatus",
    "VehicleRepository",
    "VehicleStatus",
    "VehicleTelemetryRepository",
    "create_time_column",
    "event_level_for",
    "supports_soft_delete",
    "uuid_primary_key_column",
]
