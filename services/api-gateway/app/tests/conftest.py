"""api-gateway 认证/限流/转发测试夹具（无外部基础设施依赖）。

- FakeRedis：RedisManager 兼容最小实现（鸭子类型注入 AuthService / 限流器）
- FakeUserRepository：UserRepository 兼容实现（内存用户，bcrypt 轮次=4 提速）
- 认证服务经 FastAPI dependency_overrides 替换；会话强依赖走 app.state.redis
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from hunter_common.database.enums import UserStatus

from app.config import settings
from app.core.auth import session_key
from app.core.dependencies import get_auth_service
from app.core.security import create_access_token, hash_password
from app.main import app
from app.services.auth_service import AuthService

__all__ = [
    "FakeRedis",
    "FakeUserRepository",
    "client",
    "fake_redis",
    "fake_users",
    "make_access_token",
    "seed_session",
]


class FakeRedis:
    """RedisManager 兼容最小实现（get/set/delete/exists/incr_with_expire/ping）。

    ``incr_with_expire`` 对齐 RedisManager 语义：仅首次创建时设置 TTL。
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, *, expire_seconds: int | None = None) -> None:
        self.store[key] = value
        if expire_seconds is not None:
            self.ttls[key] = expire_seconds

    async def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                deleted += 1
        return deleted

    async def exists(self, key: str) -> bool:
        return key in self.store

    async def incr_with_expire(self, key: str, expire_seconds: int = 60) -> int:
        count = int(self.store.get(key, "0")) + 1
        if key not in self.store:
            self.ttls[key] = expire_seconds
        self.store[key] = str(count)
        return count

    async def ping(self) -> bool:
        return True


class FakeUserRepository:
    """UserRepository 兼容实现（内存用户表；仅覆盖网关认证读路径）。"""

    def __init__(self) -> None:
        self.users: dict[str, dict[str, Any]] = {}  # username -> 记录
        self.last_login_touched: list[str] = []

    def add_user(
        self,
        *,
        username: str,
        password: str,
        status: UserStatus = UserStatus.ENABLED,
        roles: list[str] | None = None,
        permissions: list[str] | None = None,
        real_name: str | None = None,
    ) -> UUID:
        user_id = uuid4()
        self.users[username] = {
            "user_id": user_id,
            "username": username,
            # bcrypt 轮次 4（测试提速；生产为 settings.password_bcrypt_rounds=12）
            "password_hash": hash_password(password, rounds=4),
            "status": status,
            "roles": list(roles or []),
            "permissions": list(permissions or []),
            "real_name": real_name,
        }
        return user_id

    async def get_by_username(self, username: str) -> SimpleNamespace | None:
        record = self.users.get(username)
        return self._as_model(record) if record else None

    async def get_by_id(self, user_id: UUID) -> SimpleNamespace | None:
        record = self._find(user_id)
        return self._as_model(record) if record else None

    async def get_enabled_role_codes(self, user_id: UUID) -> list[str]:
        record = self._find(user_id)
        return list(record["roles"]) if record else []

    async def get_permission_codes(self, user_id: UUID) -> list[str]:
        record = self._find(user_id)
        return list(record["permissions"]) if record else []

    async def touch_last_login(self, user_id: UUID) -> None:
        self.last_login_touched.append(str(user_id))

    async def get_real_name(self, user_id: UUID) -> str | None:
        record = self._find(user_id)
        return record["real_name"] if record else None

    def _find(self, user_id: UUID) -> dict[str, Any] | None:
        for record in self.users.values():
            if record["user_id"] == user_id:
                return record
        return None

    @staticmethod
    def _as_model(record: dict[str, Any]) -> SimpleNamespace:
        return SimpleNamespace(**record)


def make_access_token(
    *,
    user_id: str = "11111111-1111-4111-8111-111111111111",
    username: str = "tester",
    roles: list[str] | None = None,
    permissions: list[str] | None = None,
) -> str:
    """按契约载荷签发 Access Token（签名有效；供限流/会话测试使用）。"""
    token, _ = create_access_token(
        user_id=user_id,
        username=username,
        roles=list(roles or []),
        permissions=list(permissions or []),
        settings=settings,
    )
    return token


async def seed_session(fake_redis: FakeRedis, token: str, user_id: str) -> None:
    """写入会话（登录等价物），供 get_current_user 严格校验使用。"""
    await fake_redis.set(session_key(user_id), token, expire_seconds=7200)


@pytest.fixture()
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture()
def fake_users() -> FakeUserRepository:
    repo = FakeUserRepository()
    repo.add_user(
        username="admin",
        password="Admin@12345",
        roles=["admin"],
        permissions=["scene:read", "ota:release"],
        real_name="管理员",
    )
    repo.add_user(
        username="locked_user",
        password="Locked@12345",
        status=UserStatus.LOCKED,
        roles=["viewer"],
    )
    repo.add_user(
        username="disabled_user",
        password="Disable@12345",
        status=UserStatus.DISABLED,
    )
    return repo


@pytest.fixture()
async def client(
    fake_redis: FakeRedis,
    fake_users: FakeUserRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[AsyncClient]:
    """ASGI 测试客户端：注入伪 Redis/伪用户仓储（dependency_overrides）。

    ``jwt_secret_key`` 替换为 ≥32 字节测试密钥（消除 PyJWT InsecureKeyLengthWarning；
    生产密钥经 K8s Secret 注入）。
    """
    monkeypatch.setattr(settings, "jwt_secret_key", "test-only-secret-0123456789abcdef0123456789abcdef")
    app.state.redis = fake_redis
    auth_service = AuthService(users=fake_users, redis=fake_redis, settings=settings)
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
        ) as async_client:
            yield async_client
    finally:
        app.dependency_overrides.pop(get_auth_service, None)
        app.state.redis = None