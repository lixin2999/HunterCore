"""HunterEdge 数据访问层（SQLAlchemy 2.0 异步 + TimescaleDB）。

模块结构：
- ``session``        异步引擎/会话管理（DatabaseSessionManager，FastAPI 依赖注入）
- ``base``           声明式 Base、StrEnumType、混入类（创建时间/更新时间/软删除）、列工厂
- ``enums``          受控词表 StrEnum（车辆状态/事件类型与等级/OTA 状态机/算法模块）
- ``schema_names``   schema 名称常量、hypertable 清单与分区间隔/保留策略
- ``repository``     通用 Repository 基类（CRUD + 分页 + 软删除 + 批量写入）
- ``models``         全部 ORM 模型（与 contracts/database/ddl/*.sql 一一对应）
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
from hunter_common.database.repository import (
    BULK_CHUNK_SIZE,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
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
    "EVENT_LEVEL_BY_TYPE",
    "GATEWAY_SCHEMA",
    "HYPERTABLES",
    "MAX_PAGE_SIZE",
    "OTA_ACTIVE_STATUSES",
    "OTA_SCHEMA",
    "OTA_TERMINAL_STATUSES",
    "REMOTE_CONTROL_SCHEMA",
    "RETENTION_INTERVAL",
    "SCENE_SCHEMA",
    "USER_SCHEMA",
    "VEHICLE_SCHEMA",
    "Base",
    "BaseRepository",
    "CreateTimeMixin",
    "DatabaseSessionManager",
    "EventLevel",
    "EventType",
    "MetricModule",
    "OtaStatus",
    "OtaTaskStatus",
    "OtaVersionStatus",
    "PageResult",
    "PermissionAction",
    "PermissionResource",
    "RoleStatus",
    "SceneStatus",
    "SoftDeleteMixin",
    "StrEnumType",
    "UpdateTimeMixin",
    "UserStatus",
    "VehicleStatus",
    "create_time_column",
    "event_level_for",
    "supports_soft_delete",
    "uuid_primary_key_column",
]
