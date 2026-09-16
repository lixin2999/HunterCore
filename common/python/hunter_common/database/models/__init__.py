"""ORM 模型集合（单点导入，供 Alembic 与业务层使用）。

导入本模块即完成全部模型的注册（SQLAlchemy metadata 就绪），
Alembic ``env.py`` 与契约一致性校验脚本均依赖此处。
"""
from __future__ import annotations

from typing import Any

from hunter_common.database.models.analytics import AlgorithmMetric
from hunter_common.database.models.collector import Event, VehicleTelemetry
from hunter_common.database.models.core import (
    Permission,
    Role,
    RolePermission,
    User,
    UserRole,
    Vehicle,
)
from hunter_common.database.models.ota import OtaRecord, OtaTask, OtaVersion
from hunter_common.database.models.scene import Scene

#: 全部 ORM 模型（与 contracts/database/ddl/*.sql 中的表一一对应）
ALL_MODELS: tuple[type[Any], ...] = (
    Vehicle,
    User,
    Role,
    Permission,
    UserRole,
    RolePermission,
    Scene,
    OtaVersion,
    OtaTask,
    OtaRecord,
    Event,
    VehicleTelemetry,
    AlgorithmMetric,
)

__all__ = [
    "ALL_MODELS",
    "AlgorithmMetric",
    "Event",
    "OtaRecord",
    "OtaTask",
    "OtaVersion",
    "Permission",
    "Role",
    "RolePermission",
    "Scene",
    "User",
    "UserRole",
    "Vehicle",
    "VehicleTelemetry",
]
