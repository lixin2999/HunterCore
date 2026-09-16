"""DatabaseSessionManager 测试：引擎初始化/释放/健康检查（不依赖真实数据库）。"""
from __future__ import annotations

import asyncio

import pytest

from hunter_common.database import DatabaseSessionManager


def demoSettings():
    from hunter_common.config import HunterBaseConfig

    class _Settings(HunterBaseConfig):
        pass

    # 不可达端口（1）：用于验证健康检查快速失败；显式入参优先级高于环境变量/.env
    return _Settings(
        service_name="test-service",
        postgres_host="localhost",
        postgres_port=1,
        postgres_user="u",
        postgres_password="p",
        postgres_db="d",
    )


def test_engine_and_factory_unavailable_before_init() -> None:
    manager = DatabaseSessionManager(demoSettings())
    with pytest.raises(RuntimeError, match="未初始化"):
        _ = manager.engine
    with pytest.raises(RuntimeError, match="未初始化"):
        _ = manager.session_factory


async def test_session_context_requires_init() -> None:
    manager = DatabaseSessionManager(demoSettings())
    with pytest.raises(RuntimeError, match="未初始化"):
        async with manager.session():
            pass


async def test_init_is_idempotent_and_close_releases_engine() -> None:
    manager = DatabaseSessionManager(demoSettings())
    manager.init()
    engine = manager.engine
    assert str(engine.url).startswith("postgresql+asyncpg://u:***@localhost:1/d")
    manager.init()  # 幂等：重复调用复用同一引擎
    assert manager.engine is engine
    await manager.close()
    with pytest.raises(RuntimeError, match="未初始化"):
        _ = manager.engine


async def test_check_connection_returns_false_when_unreachable() -> None:
    manager = DatabaseSessionManager(demoSettings())
    manager.init()
    try:
        assert await asyncio.wait_for(manager.check_connection(), timeout=15) is False
    finally:
        await manager.close()
