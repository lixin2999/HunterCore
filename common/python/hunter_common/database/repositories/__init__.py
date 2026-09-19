"""Repository 集合（模型 ↔ 仓库一一对应，契约见 contracts/database/orm-mapping.md 第 3 节）。

- 每个 ORM 模型恰好一个 Repository；``REPOSITORY_BY_MODEL`` 是映射的单一登记点
  （契约校验与依赖注入均以此为准，缺项/多项由测试与 ``scripts/verify_data_layer.py`` 拦截）；
- 所有 Repository 继承 ``BaseRepository``，统一 CRUD / 分页 / 软删除 / 批量写入 / 条件查询语义；
- 写入只 ``flush``（SAVEPOINT 局部回滚约束冲突），事务边界由调用方
  （``DatabaseSessionManager.session()`` / ``Depends(get_session)``）控制；
- 默认排序显式声明空值位次（``-col:nl`` = NULLS LAST），与 DDL 索引保持一致。

⚠ 迁移状态（契约 orm-mapping.md 第 5 节）：本包为**唯一数据访问实现**，但各服务
``services/*/app/repositories/*.py`` 仍存在同名同表的过渡实现；收敛顺序为
scene-service → data-collector → api-gateway → ota-service，收敛前禁止在同一个模块内混用两套。
"""
from __future__ import annotations

from typing import Any, Final

from hunter_common.database.base import Base
from hunter_common.database.models import (
    AlgorithmMetric,
    Event,
    OtaRecord,
    OtaTask,
    OtaVersion,
    Permission,
    Role,
    RolePermission,
    Scene,
    User,
    UserRole,
    Vehicle,
    VehicleTelemetry,
)
from hunter_common.database.repositories.analytics import (
    METRIC_CONFLICT_COLUMNS,
    AlgorithmMetricRepository,
)
from hunter_common.database.repositories.collector import (
    EVENT_IDEMPOTENCY_COLUMNS,
    TELEMETRY_CONFLICT_COLUMNS,
    EventRepository,
    VehicleTelemetryRepository,
)
from hunter_common.database.repositories.core import (
    PermissionRepository,
    RolePermissionRepository,
    RoleRepository,
    UserRepository,
    UserRoleRepository,
    VehicleRepository,
)
from hunter_common.database.repositories.ota import (
    OtaRecordRepository,
    OtaTaskRepository,
    OtaVersionRepository,
)
from hunter_common.database.repositories.scene import SceneRepository
from hunter_common.database.repository import BaseRepository

#: 模型 → Repository 类（一一对应；新增表必须同步 DDL → ORM → 本表 → 契约文档）
REPOSITORY_BY_MODEL: Final[dict[type[Base], type[BaseRepository[Any]]]] = {
    Vehicle: VehicleRepository,
    User: UserRepository,
    Role: RoleRepository,
    Permission: PermissionRepository,
    UserRole: UserRoleRepository,
    RolePermission: RolePermissionRepository,
    Scene: SceneRepository,
    OtaVersion: OtaVersionRepository,
    OtaTask: OtaTaskRepository,
    OtaRecord: OtaRecordRepository,
    Event: EventRepository,
    VehicleTelemetry: VehicleTelemetryRepository,
    AlgorithmMetric: AlgorithmMetricRepository,
}

__all__ = [
    "EVENT_IDEMPOTENCY_COLUMNS",
    "METRIC_CONFLICT_COLUMNS",
    "REPOSITORY_BY_MODEL",
    "TELEMETRY_CONFLICT_COLUMNS",
    "AlgorithmMetricRepository",
    "EventRepository",
    "OtaRecordRepository",
    "OtaTaskRepository",
    "OtaVersionRepository",
    "PermissionRepository",
    "RolePermissionRepository",
    "RoleRepository",
    "SceneRepository",
    "UserRepository",
    "UserRoleRepository",
    "VehicleRepository",
    "VehicleTelemetryRepository",
]
