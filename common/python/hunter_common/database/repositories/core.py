"""车辆主数据（vehicle_svc）与用户/RBAC（user_svc）数据访问层。

契约：contracts/database/ddl/01_core.sql（表结构）+ orm-mapping.md 第 3 节（Repository 契约）。
规则：
- 每个模型恰好一个 Repository；只 ``flush`` 不 ``commit``（事务边界由调用方控制）；
- 查询条件一律参数绑定（禁止字符串拼接 SQL）；按 DDL 唯一索引/索引组织专属查询方法；
- RBAC 五表同属 user_svc（同一服务 schema），可内部 JOIN；跨服务引用（如 scenes.creator）禁止 JOIN。
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hunter_common.database.enums import PermissionResource, RoleStatus, VehicleStatus
from hunter_common.database.models import (
    Permission,
    Role,
    RolePermission,
    User,
    UserRole,
    Vehicle,
)
from hunter_common.database.repository import BaseRepository


class VehicleRepository(BaseRepository[Vehicle]):
    """车辆台账读写（vehicle_svc.vehicles）。"""

    model = Vehicle
    #: 在线看板与车辆列表默认排序（对齐 idx_vehicles_status_last_online）
    default_order_by = ("-last_online_time", "vehicle_id")

    async def get_by_device_cert_sn(self, device_cert_sn: str) -> Vehicle | None:
        """按 X.509 设备证书序列号查询（部分唯一索引 uq_vehicles_device_cert_sn）。"""
        return await self.find_one(Vehicle.device_cert_sn == device_cert_sn)

    async def list_by_status(
        self, status: VehicleStatus, *, limit: int | None = None
    ) -> list[Vehicle]:
        """按车辆状态查询（命中 idx_vehicles_status_last_online 前缀列）。"""
        return await self.find_all(Vehicle.status == status, limit=limit)

    async def update_status(
        self,
        vehicle_id: str,
        status: VehicleStatus,
        *,
        last_online_time: datetime | None = None,
    ) -> Vehicle | None:
        """更新车辆状态，可选同步最近在线时间；车辆不存在返回 ``None``。"""
        vehicle = await self.get(vehicle_id)
        if vehicle is None:
            return None
        values: dict[str, Any] = {"status": status}
        if last_online_time is not None:
            values["last_online_time"] = last_online_time
        return await self.update(vehicle, **values)


class UserRepository(BaseRepository[User]):
    """用户读写（user_svc.users）+ RBAC 展开查询。"""

    model = User
    default_order_by = ("-create_time", "username")

    async def get_by_username(self, username: str) -> User | None:
        """按用户名查询（唯一索引 uq_users_username）。"""
        return await self.find_one(User.username == username)

    async def get_by_email(self, email: str) -> User | None:
        """按邮箱查询（大小写不敏感，命中表达式唯一索引 uq_users_email）。"""
        return await self.find_one(func.lower(User.email) == email.lower())

    async def list_role_codes(self, user_id: UUID) -> list[str]:
        """用户已启用角色的编码列表。

        契约依据：ddl/01_core.sql 对 user_roles 的注释「鉴权时按 user → roles → permissions 展开」。
        仅返回 ``status = enabled`` 的角色（禁用角色不参与鉴权）。
        """
        stmt = (
            select(Role.role_code)
            .join(UserRole, UserRole.role_id == Role.role_id)
            .where(UserRole.user_id == user_id, Role.status == RoleStatus.ENABLED)
            .order_by(Role.role_code.asc())
        )
        return list((await self.session.execute(stmt)).scalars().all())

    async def list_permission_codes(self, user_id: UUID) -> list[str]:
        """用户通过角色汇总的权限编码（去重；供 JWT 权限声明与前端 v-permission 校验）。"""
        stmt = (
            select(Permission.permission_code)
            .join(RolePermission, RolePermission.permission_id == Permission.permission_id)
            .join(Role, Role.role_id == RolePermission.role_id)
            .join(UserRole, UserRole.role_id == Role.role_id)
            .where(UserRole.user_id == user_id, Role.status == RoleStatus.ENABLED)
            .distinct()
            .order_by(Permission.permission_code.asc())
        )
        return list((await self.session.execute(stmt)).scalars().all())

    async def touch_last_login(self, user_id: UUID, *, at: datetime | None = None) -> bool:
        """记录最近登录时间；用户不存在返回 ``False``。"""
        user = await self.get(user_id)
        if user is None:
            return False
        await self.update(user, last_login_time=at or datetime.now(UTC))
        return True


class RoleRepository(BaseRepository[Role]):
    """角色读写（user_svc.roles）。"""

    model = Role
    default_order_by = ("role_code",)

    async def get_by_role_code(self, role_code: str) -> Role | None:
        """按角色编码查询（唯一索引 uq_roles_role_code；role_code 为对外稳定标识）。"""
        return await self.find_one(Role.role_code == role_code)

    async def list_enabled(self) -> list[Role]:
        """全部启用角色（角色下拉与鉴权缓存预热）。"""
        return await self.find_all(Role.status == RoleStatus.ENABLED)


class PermissionRepository(BaseRepository[Permission]):
    """权限点读写（user_svc.permissions）。"""

    model = Permission
    default_order_by = ("resource", "action")

    async def get_by_permission_code(self, permission_code: str) -> Permission | None:
        """按权限编码查询（唯一索引 uq_permissions_permission_code）。"""
        return await self.find_one(Permission.permission_code == permission_code)

    async def list_by_resource(self, resource: PermissionResource) -> list[Permission]:
        """按资源域查询权限点（命中 idx_permissions_resource_action 前缀列）。"""
        return await self.find_all(Permission.resource == resource)


class UserRoleRepository(BaseRepository[UserRole]):
    """用户-角色关联读写（user_svc.user_roles，复合主键）。"""

    model = UserRole

    async def get_pair(self, user_id: UUID, role_id: UUID) -> UserRole | None:
        """按复合主键查询关联行。"""
        return await self.find_one(UserRole.user_id == user_id, UserRole.role_id == role_id)

    async def list_role_ids(self, user_id: UUID) -> list[UUID]:
        """用户已绑定的角色 ID 列表（命中复合主键前缀 (user_id, role_id)）。"""
        stmt = (
            select(UserRole.role_id)
            .where(UserRole.user_id == user_id)
            .order_by(UserRole.role_id.asc())
        )
        return list((await self.session.execute(stmt)).scalars().all())

    async def link(self, user_id: UUID, role_id: UUID) -> bool:
        """绑定用户与角色（``ON CONFLICT DO NOTHING`` 幂等）；新增返回 ``True``。"""
        stmt = (
            pg_insert(UserRole)
            .values(user_id=user_id, role_id=role_id)
            .on_conflict_do_nothing(index_elements=["user_id", "role_id"])
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return bool(getattr(result, "rowcount", 0) or 0)

    async def unlink(self, user_id: UUID, role_id: UUID) -> int:
        """解绑用户与角色，返回删除行数（0 表示原本未绑定）。"""
        return await self.delete_where(UserRole.user_id == user_id, UserRole.role_id == role_id)


class RolePermissionRepository(BaseRepository[RolePermission]):
    """角色-权限关联读写（user_svc.role_permissions，复合主键）。"""

    model = RolePermission

    async def get_pair(self, role_id: UUID, permission_id: UUID) -> RolePermission | None:
        """按复合主键查询关联行。"""
        return await self.find_one(
            RolePermission.role_id == role_id,
            RolePermission.permission_id == permission_id,
        )

    async def list_permission_ids(self, role_id: UUID) -> list[UUID]:
        """角色已绑定的权限 ID 列表（命中复合主键前缀 (role_id, permission_id)）。"""
        stmt = (
            select(RolePermission.permission_id)
            .where(RolePermission.role_id == role_id)
            .order_by(RolePermission.permission_id.asc())
        )
        return list((await self.session.execute(stmt)).scalars().all())

    async def link(self, role_id: UUID, permission_id: UUID) -> bool:
        """绑定角色与权限（``ON CONFLICT DO NOTHING`` 幂等）；新增返回 ``True``。"""
        stmt = (
            pg_insert(RolePermission)
            .values(role_id=role_id, permission_id=permission_id)
            .on_conflict_do_nothing(index_elements=["role_id", "permission_id"])
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return bool(getattr(result, "rowcount", 0) or 0)

    async def unlink(self, role_id: UUID, permission_id: UUID) -> int:
        """解绑角色与权限，返回删除行数。"""
        return await self.delete_where(
            RolePermission.role_id == role_id,
            RolePermission.permission_id == permission_id,
        )


__all__ = [
    "PermissionRepository",
    "RolePermissionRepository",
    "RoleRepository",
    "UserRepository",
    "UserRoleRepository",
    "VehicleRepository",
]
