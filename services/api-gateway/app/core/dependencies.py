"""FastAPI 依赖注入提供者（DB 会话 / 认证服务 / 代理服务）。

约定：所有提供者从 ``request.app.state`` 取进程级管理器（lifespan 初始化），
未初始化即返回 503（code=5001 服务不可用），禁止惰性隐式创建。
"""
from __future__ import annotations

from typing import Any

from fastapi import Depends, Request
from hunter_common.database import DatabaseSessionManager
from hunter_common.exceptions import ServiceUnavailableError

from app.config import settings
from app.core.auth import get_redis_manager
from app.repositories.user_repository import UserRepository
from app.services.auth_service import AuthService
from app.services.proxy_service import ProxyService

__all__ = ["get_auth_service", "get_db_manager", "get_proxy_service", "get_user_repository"]


def get_db_manager(request: Request) -> DatabaseSessionManager:
    """数据库会话管理器（lifespan 初始化；未就绪 → 503）。"""
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise ServiceUnavailableError("服务不可用")
    return db


def get_user_repository(db: DatabaseSessionManager = Depends(get_db_manager)) -> UserRepository:
    """用户/RBAC 认证专用仓储（user_svc；归属说明见类 docstring）。"""
    return UserRepository(db)


def get_auth_service(
    users: UserRepository = Depends(get_user_repository),
    redis: Any = Depends(get_redis_manager),
) -> AuthService:
    """认证服务（单测经 dependency_overrides 替换）。"""
    return AuthService(users=users, redis=redis, settings=settings)


def get_proxy_service(request: Request) -> ProxyService:
    """反向代理服务（共享 lifespan 创建的 httpx 客户端与进程级熔断器）。"""
    client = getattr(request.app.state, "http_client", None)
    if client is None:
        raise ServiceUnavailableError("服务不可用")
    return ProxyService(client=client, settings=settings)