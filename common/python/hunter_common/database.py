"""SQLAlchemy 2.0 异步数据库访问（asyncpg）。

- 异步引擎 + async_sessionmaker（异步优先，禁止同步阻塞 IO）
- 连接池大小 = CPU 核数 × 2 + 1（可在配置中覆盖）
- 提供 FastAPI Depends 会话依赖；事务边界：请求成功提交 / 异常回滚
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.database")


class DatabaseSessionManager:
    """进程级数据库会话管理器（init 一次，全局复用）。

    用法::

        db_manager = DatabaseSessionManager(config)
        db_manager.init()
        # FastAPI 依赖注入：db: AsyncSession = Depends(db_manager.get_session)
    """

    def __init__(self, config: HunterBaseConfig) -> None:
        self._config = config
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("DatabaseSessionManager 未初始化，请先调用 init()")
        return self._engine

    def init(self) -> None:
        """创建异步引擎与会话工厂（幂等）。"""
        if self._engine is not None:
            return
        self._engine = create_async_engine(
            self._config.database_url,
            echo=self._config.db_echo,
            pool_size=self._config.db_pool_size,
            max_overflow=self._config.db_max_overflow,
            pool_timeout=self._config.db_pool_timeout,
            pool_pre_ping=True,
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
        logger.info("database_engine_initialized", pool_size=self._config.db_pool_size)

    async def close(self) -> None:
        """释放连接池（应用关闭时调用）。"""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("database_engine_disposed")

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """事务性会话上下文：正常退出提交，异常回滚。"""
        factory = self._session_factory
        if factory is None:
            raise RuntimeError("DatabaseSessionManager 未初始化，请先调用 init()")
        async with factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """FastAPI 依赖注入入口：``Depends(db_manager.get_session)``。"""
        async with self.session() as session:
            yield session

    async def check_connection(self) -> bool:
        """就绪探针用：SELECT 1 验证数据库连通性。"""
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.exception("database_health_check_failed")
            return False
