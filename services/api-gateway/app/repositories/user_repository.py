"""用户与 RBAC 数据访问（user_svc schema，认证专用读路径）。

归属说明：user-service 不在本项目模块表（6 个微服务）内，契约
``contracts/openapi/api-gateway.yaml`` 将统一认证（登录/刷新/登出/当前用户）
落在网关自持，登录流程明确要求「验证用户名密码 → 更新 user_svc.users.
last_login_time」（契约 login description），因此 user_svc 的认证专用读写
由网关承担；本仓储禁止用于业务数据读写（跨服务数据隔离原则）。
"""
from __future__ import annotations

from uuid import UUID

from hunter_common.database import DatabaseSessionManager
from hunter_common.database.enums import RoleStatus
from hunter_common.database.models import Permission, Role, RolePermission, User, UserRole
from sqlalchemy import func, select, update

__all__ = ["UserRepository"]


class UserRepository:
    """user_svc 认证专用仓储（全部参数化查询，禁止字符串拼接 SQL）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def get_by_username(self, username: str) -> User | None:
        """按用户名查询（唯一索引 uq_users_username）。"""
        async with self._db.session() as session:
            result = await session.execute(select(User).where(User.username == username))
            return result.scalar_one_or_none()

    async def get_by_id(self, user_id: UUID) -> User | None:
        """按主键查询（刷新流程校验用户仍有效）。"""
        async with self._db.session() as session:
            result = await session.execute(select(User).where(User.user_id == user_id))
            return result.scalar_one_or_none()

    async def get_enabled_role_codes(self, user_id: UUID) -> list[str]:
        """用户角色编码（仅启用角色：RBAC 展开 user→roles）。"""
        async with self._db.session() as session:
            result = await session.execute(
                select(Role.role_code)
                .join(UserRole, UserRole.role_id == Role.role_id)
                .where(UserRole.user_id == user_id, Role.status == RoleStatus.ENABLED)
            )
            return list(result.scalars().all())

    async def get_permission_codes(self, user_id: UUID) -> list[str]:
        """用户权限编码（RBAC 展开 user→roles→permissions，仅启用角色，去重）。"""
        async with self._db.session() as session:
            result = await session.execute(
                select(Permission.permission_code)
                .distinct()
                .join(RolePermission, RolePermission.permission_id == Permission.permission_id)
                .join(UserRole, UserRole.role_id == RolePermission.role_id)
                .join(Role, Role.role_id == UserRole.role_id)
                .where(UserRole.user_id == user_id, Role.status == RoleStatus.ENABLED)
            )
            return list(result.scalars().all())

    async def touch_last_login(self, user_id: UUID) -> None:
        """更新 users.last_login_time（契约 login 描述：登录成功后调用）。"""
        async with self._db.session() as session:
            await session.execute(
                update(User).where(User.user_id == user_id).values(last_login_time=func.now())
            )

    async def get_real_name(self, user_id: UUID) -> str | None:
        """查询用户姓名（GET /user/me 用户资料；用户不存在返回 None）。"""
        async with self._db.session() as session:
            result = await session.execute(
                select(User.real_name).where(User.user_id == user_id)
            )
            return result.scalar_one_or_none()