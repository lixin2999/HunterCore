"""ORM 模型：OTA 版本/任务/记录（ota_svc）。

对应契约：contracts/database/ddl/03_ota.sql。
安全约束：``version_code`` 单调递增（防回滚）；``package_sha256``/``signature`` 为验签必需字段。
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hunter_common.database.base import Base, StrEnumType, uuid_primary_key_column
from hunter_common.database.enums import OtaStatus, OtaTaskStatus, OtaVersionStatus
from hunter_common.database.schema_names import OTA_SCHEMA

_OTA_VERSION_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in OtaVersionStatus)
_OTA_TASK_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in OtaTaskStatus)
_OTA_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in OtaStatus)


class OtaVersion(Base):
    """OTA 版本仓库（ota_svc.ota_versions）；发布前必须完成校验/验签/版本号单调性检查。"""

    __tablename__ = "ota_versions"
    __table_args__ = (
        CheckConstraint("version_code > 0", name="ota_versions_version_code_check"),
        CheckConstraint("package_size > 0", name="ota_versions_package_size_check"),
        CheckConstraint(
            f"status IN ({_OTA_VERSION_STATUS_VALUES})", name="ota_versions_status_check"
        ),
        Index("uq_ota_versions_version_code", "version_code", unique=True),
        Index("uq_ota_versions_version_name", "version_name", unique=True),
        Index(
            "idx_ota_versions_status_release_time",
            "status",
            text("release_time DESC NULLS LAST"),
        ),
        Index("idx_ota_versions_applicable_models", "applicable_models", postgresql_using="gin"),
        {"schema": OTA_SCHEMA},
    )

    version_id: Mapped[UUID] = uuid_primary_key_column()
    version_name: Mapped[str] = mapped_column(Text, nullable=False)
    version_code: Mapped[int] = mapped_column(Integer, nullable=False)
    release_type: Mapped[str] = mapped_column(Text, nullable=False)
    package_url: Mapped[str] = mapped_column(Text, nullable=False)
    package_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    package_md5: Mapped[str] = mapped_column(CHAR(32), nullable=False)
    package_sha256: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    signature: Mapped[str] = mapped_column(Text, nullable=False)
    changelog: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    applicable_models: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("ARRAY['HUNTER_SE']::TEXT[]")
    )
    status: Mapped[OtaVersionStatus] = mapped_column(
        StrEnumType(OtaVersionStatus), nullable=False, server_default=text("'draft'")
    )
    release_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OtaTask(Base):
    """OTA 升级任务（ota_svc.ota_tasks）；灰度任一批次成功率 < 95% 时暂停并告警。"""

    __tablename__ = "ota_tasks"
    __table_args__ = (
        CheckConstraint(f"status IN ({_OTA_TASK_STATUS_VALUES})", name="ota_tasks_status_check"),
        Index("idx_ota_tasks_status_create_time", "status", text("create_time DESC")),
        Index("idx_ota_tasks_target_vehicles", "target_vehicles", postgresql_using="gin"),
        Index("idx_ota_tasks_target_version_id", "target_version_id"),
        {"schema": OTA_SCHEMA},
    )

    task_id: Mapped[UUID] = uuid_primary_key_column()
    task_name: Mapped[str] = mapped_column(Text, nullable=False)
    target_version_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{OTA_SCHEMA}.ota_versions.version_id", ondelete="RESTRICT"),
        nullable=False,
    )
    target_vehicles: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    upgrade_strategy: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    schedule: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    preconditions: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    status: Mapped[OtaTaskStatus] = mapped_column(
        StrEnumType(OtaTaskStatus), nullable=False, server_default=text("'created'")
    )
    progress: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    creator: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class OtaRecord(Base):
    """单车辆升级执行记录（ota_svc.ota_records）。

    ``status`` / ``phase`` 取值 = 车端 OTA 状态机（IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS，
    自检失败 → ROLLBACK → ROLLED_BACK/FAILED）。同一 (task_id, vehicle_id) 唯一，重试在原记录上推进。
    """

    __tablename__ = "ota_records"
    __table_args__ = (
        CheckConstraint(f"status IN ({_OTA_STATUS_VALUES})", name="ota_records_status_check"),
        CheckConstraint(f"phase IN ({_OTA_STATUS_VALUES})", name="ota_records_phase_check"),
        CheckConstraint("progress BETWEEN 0 AND 100", name="ota_records_progress_check"),
        Index("uq_ota_records_task_vehicle", "task_id", "vehicle_id", unique=True),
        Index("idx_ota_records_vehicle_start_time", "vehicle_id", text("start_time DESC")),
        Index(
            "idx_ota_records_inflight",
            "status",
            "start_time",
            postgresql_where=text(
                "status IN ('PENDING', 'DOWNLOAD', 'INSTALL', 'TEST', 'ROLLBACK')"
            ),
        ),
        Index(
            "idx_ota_records_error_code",
            "error_code",
            text("start_time DESC"),
            postgresql_where=text("error_code IS NOT NULL"),
        ),
        {"schema": OTA_SCHEMA},
    )

    record_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    task_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{OTA_SCHEMA}.ota_tasks.task_id", ondelete="CASCADE"),
        nullable=False,
    )
    vehicle_id: Mapped[str] = mapped_column(Text, nullable=False)
    from_version: Mapped[str | None] = mapped_column(Text)
    to_version: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[OtaStatus] = mapped_column(
        StrEnumType(OtaStatus), nullable=False, server_default=text("'PENDING'")
    )
    phase: Mapped[OtaStatus] = mapped_column(
        StrEnumType(OtaStatus), nullable=False, server_default=text("'PENDING'")
    )
    progress: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


__all__ = ["OtaRecord", "OtaTask", "OtaVersion"]
