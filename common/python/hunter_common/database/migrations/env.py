"""Alembic 迁移环境（asyncpg 异步引擎；支持离线 --sql 生成）。

连接串来源优先级：
1. 环境变量 ``DATABASE_URL``（完整 SQLAlchemy URL）
2. ``hunter_common.config.HunterBaseConfig``（由 POSTGRES_HOST/PORT/USER/PASSWORD/DB 组装）
3. ``alembic.ini`` 中 ``sqlalchemy.url``（仅本地开发兜底）

注意：应用运行期使用 asyncpg 异步引擎，本环境同样以异步引擎执行 DDL
（``connection.run_sync``），因此不引入同步驱动依赖。
"""
from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig
from typing import Any

from alembic import context
from pydantic import ValidationError
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

# 导入模型包以完成全部表注册（metadata 就绪）；禁止移除
import hunter_common.database.models  # noqa: F401  (side effect: register models)
from hunter_common.config import HunterBaseConfig
from hunter_common.database.base import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_database_url() -> str:
    """解析数据库连接串（保持 asyncpg 驱动，离线模式仅用于方言解析）。"""
    override = os.getenv("DATABASE_URL")
    if override:
        return override
    configured = config.get_main_option("sqlalchemy.url")
    try:
        return HunterBaseConfig().database_url
    except (ValidationError, ValueError):
        # 环境变量缺失/非法：退回 alembic.ini 中的本地开发连接串
        return str(configured)


def run_migrations_offline() -> None:
    """离线模式：只生成 SQL（不连接数据库），用于 CI 校验与 DBA 评审。"""
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True,
        compare_type=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Any) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """在线模式：异步引擎执行 DDL（NullPool，迁移结束即释放）。"""
    section = config.get_section(config.config_ini_section, {}) or {}
    section["sqlalchemy.url"] = get_database_url()
    connectable = async_engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
