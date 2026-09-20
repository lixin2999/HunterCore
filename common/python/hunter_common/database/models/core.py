"""ORM 模型：车辆主数据（vehicle_svc）+ 用户与 RBAC（user_svc）。

对应契约：contracts/database/ddl/01_core.sql（字段名/类型/约束不可更改）。
关系契约：contracts/database/orm-mapping.md 第 2 节（每条关系必须显式 lazy 策略，异步安全）。
跨 schema 引用（如 scenes.creator → users.user_id）为逻辑外键，不建物理外键与 relationship（服务解耦）。
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    PrimaryKeyConstraint,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hunter_common.database.base import Base, StrEnumType, uuid_primary_key_column
from hunter_common.database.enums import (
    PermissionAction,
    PermissionResource,
    RoleStatus,
    UserStatus,
    VehicleStatus,
)
from hunter_common.database.schema_names import USER_SCHEMA, VEHICLE_SCHEMA

_VEHICLE_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in VehicleStatus)
_USER_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in UserStatus)
_ROLE_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in RoleStatus)


class Vehicle(Base):
    """车辆台账（vehicle_svc.vehicles）。

    ``vehicle_id`` = X.509 设备证书 CommonName = Kafka 消息 key（单车辆有序）。
    """

    __tablename__ = "vehicles"
    __table_args__ = (
        CheckConstraint(f"status IN ({_VEHICLE_STATUS_VALUES})", name="vehicles_status_check"),
        Index(
            "uq_vehicles_device_cert_sn",
            "device_cert_sn",
            unique=True,
            postgresql_where=text("device_cert_sn IS NOT NULL"),
        ),
        Index("idx_vehicles_status_last_online", "status", text("last_online_time DESC NULLS LAST")),
        {"schema": VEHICLE_SCHEMA},
    )

    vehicle_id: Mapped[str] = mapped_column(Text, primary_key=True)
    vehicle_name: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'HUNTER_SE'"))
    firmware_version: Mapped[str | None] = mapped_column(Text)
    software_version: Mapped[str | None] = mapped_column(Text)
    status: Mapped[VehicleStatus] = mapped_column(
        StrEnumType(VehicleStatus), nullable=False, server_default=text("'offline'")
    )
    last_online_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    register_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    device_cert_sn: Mapped[str | None] = mapped_column(Text)
    # G-11（设计文档 8.4.2 地理围栏）：circle/polygon 围栏定义 + 可选限速；NULL = 不校验
    fence_json: Mapped[dict | None] = mapped_column(JSONB)
    description: Mapped[str | None] = mapped_column(Text)

    # 无 relationship：events / vehicle_telemetry / algorithm_metrics / ota_records 的 vehicle_id
    # 均为**跨 schema 逻辑外键**，跨服务补全一律走 REST（contracts/database/orm-mapping.md 第 2.1 节）


class User(Base):
    """平台用户（user_svc.users）；``password_hash`` 为 bcrypt 哈希，禁止明文/日志输出。"""

    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(f"status IN ({_USER_STATUS_VALUES})", name="users_status_check"),
        Index("uq_users_username", "username", unique=True),
        Index(
            "uq_users_email",
            text("lower(email)"),
            unique=True,
            postgresql_where=text("email IS NOT NULL"),
        ),
        Index("idx_users_status", "status"),
        {"schema": USER_SCHEMA},
    )

    user_id: Mapped[UUID] = uuid_primary_key_column()
    username: Mapped[str] = mapped_column(Text, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    real_name: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text)
    phone: Mapped[str | None] = mapped_column(Text)
    status: Mapped[UserStatus] = mapped_column(
        StrEnumType(UserStatus), nullable=False, server_default=text("'enabled'")
    )
    # G-06（设计文档 14.1）：首登/重置后强制改密；默认管理员初始化置 true
    must_change_password: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    # G-23（设计文档 3.2.2/14.1 MFA）：密钥须应用层加密后存 totp_secret，两字段禁止接口回传明文
    mfa_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    totp_secret: Mapped[str | None] = mapped_column(Text)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_login_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # 关系契约：contracts/database/orm-mapping.md 第 2 节（lazy 策略不可改为隐式加载）
    user_roles: Mapped[list[UserRole]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        # 删除用户时交由 DB `ON DELETE CASCADE` 处理，避免 ORM 逐行 DELETE（N 次往返）
        passive_deletes=True,
        lazy="raise_on_sql",
    )
    roles: Mapped[list[Role]] = relationship(
        secondary=f"{USER_SCHEMA}.user_roles",
        viewonly=True,  # 写入走关联对象 UserRole（避免与 user_roles 双写冲突）
        order_by="Role.role_code",
        lazy="selectin",  # 鉴权热路径：一条 IN 查询批量预取，异步安全
        overlaps="user_roles",
    )


class Role(Base):
    """角色（user_svc.roles）；``role_code`` 为对外稳定标识，业务判断禁止使用 role_id。"""

    __tablename__ = "roles"
    __table_args__ = (
        CheckConstraint(f"status IN ({_ROLE_STATUS_VALUES})", name="roles_status_check"),
        Index("uq_roles_role_code", "role_code", unique=True),
        {"schema": USER_SCHEMA},
    )

    role_id: Mapped[UUID] = uuid_primary_key_column()
    role_code: Mapped[str] = mapped_column(Text, nullable=False)
    role_name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[RoleStatus] = mapped_column(
        StrEnumType(RoleStatus), nullable=False, server_default=text("'enabled'")
    )
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    user_roles: Mapped[list[UserRole]] = relationship(
        back_populates="role",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="raise_on_sql",
    )
    users: Mapped[list[User]] = relationship(
        secondary=f"{USER_SCHEMA}.user_roles",
        viewonly=True,
        lazy="selectin",
        overlaps="user_roles",
    )
    role_permissions: Mapped[list[RolePermission]] = relationship(
        back_populates="role",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="raise_on_sql",
    )
    permissions: Mapped[list[Permission]] = relationship(
        secondary=f"{USER_SCHEMA}.role_permissions",
        viewonly=True,
        order_by="Permission.permission_code",
        lazy="selectin",
        overlaps="role_permissions",
    )


class Permission(Base):
    """权限点（user_svc.permissions）；``resource:action`` 对应前端 v-permission 指令。"""

    __tablename__ = "permissions"
    __table_args__ = (
        Index("uq_permissions_permission_code", "permission_code", unique=True),
        Index("idx_permissions_resource_action", "resource", "action"),
        {"schema": USER_SCHEMA},
    )

    permission_id: Mapped[UUID] = uuid_primary_key_column()
    permission_code: Mapped[str] = mapped_column(Text, nullable=False)
    permission_name: Mapped[str] = mapped_column(Text, nullable=False)
    resource: Mapped[PermissionResource] = mapped_column(
        StrEnumType(PermissionResource), nullable=False
    )
    action: Mapped[PermissionAction] = mapped_column(StrEnumType(PermissionAction), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    role_permissions: Mapped[list[RolePermission]] = relationship(
        back_populates="permission",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="raise_on_sql",
    )
    roles: Mapped[list[Role]] = relationship(
        secondary=f"{USER_SCHEMA}.role_permissions",
        viewonly=True,
        lazy="selectin",
        overlaps="role_permissions",
    )


class UserRole(Base):
    """用户-角色关联（user_svc.user_roles，复合主键，级联删除）。"""

    __tablename__ = "user_roles"
    __table_args__ = (
        PrimaryKeyConstraint("user_id", "role_id", name="pk_user_roles"),
        Index("idx_user_roles_role_id", "role_id"),
        {"schema": USER_SCHEMA},
    )

    user_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{USER_SCHEMA}.users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    role_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{USER_SCHEMA}.roles.role_id", ondelete="CASCADE"),
        nullable=False,
    )
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # 多对一：单对象按需显式加载（raise_on_sql 禁止隐式 IO，异步安全）
    user: Mapped[User] = relationship(back_populates="user_roles", lazy="raise_on_sql")
    role: Mapped[Role] = relationship(back_populates="user_roles", lazy="raise_on_sql")


class RolePermission(Base):
    """角色-权限关联（user_svc.role_permissions，复合主键，级联删除）。"""

    __tablename__ = "role_permissions"
    __table_args__ = (
        PrimaryKeyConstraint("role_id", "permission_id", name="pk_role_permissions"),
        Index("idx_role_permissions_permission_id", "permission_id"),
        {"schema": USER_SCHEMA},
    )

    role_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{USER_SCHEMA}.roles.role_id", ondelete="CASCADE"),
        nullable=False,
    )
    permission_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{USER_SCHEMA}.permissions.permission_id", ondelete="CASCADE"),
        nullable=False,
    )
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    role: Mapped[Role] = relationship(
        back_populates="role_permissions", lazy="raise_on_sql"
    )
    permission: Mapped[Permission] = relationship(
        back_populates="role_permissions", lazy="raise_on_sql"
    )


__all__ = ["Permission", "Role", "RolePermission", "User", "UserRole", "Vehicle"]

