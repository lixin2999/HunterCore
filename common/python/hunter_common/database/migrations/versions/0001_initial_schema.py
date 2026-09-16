"""0001 初始数据库结构：8 个服务 schema + 13 张业务/时序表 + hypertable + 保留策略

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-09-16

对应契约（单一事实来源）：``contracts/database/ddl/*.sql``
- 00_schemas.sql  扩展 / schema / 公共触发器函数
- 01_core.sql     vehicles + RBAC 五表
- 02_scene.sql    scenes（含 soft delete 与 update_time 触发器）
- 03_ota.sql      ota_versions / ota_tasks / ota_records
- 04_events.sql   events
- 05_timeseries.sql vehicle_telemetry / algorithm_metrics（hypertable，按天分块，保留 90 天）

⚠ 实现决策（需人工确认）：建表部分调用 ``Base.metadata.create_all``，以保证
「DDL 契约 = ORM 模型 = 迁移」三者不会出现人工抄写偏差（DDL 与 ORM 的一致性由
``scripts/verify_data_layer.py`` 逐列逐类型校验）。由此带来的约束：
**本文件为初始基线，禁止就地修改；后续任何结构变更必须新增迁移并使用显式 op.* 操作**。
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# 导入模型包注册全部表（否则 metadata 为空）
import hunter_common.database.models  # noqa: F401  (side effect: register models)
from hunter_common.database.base import Base
from hunter_common.database.schema_names import (
    ALL_SCHEMAS,
    CHUNK_TIME_INTERVAL,
    HYPERTABLES,
    RETENTION_INTERVAL,
)

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """创建扩展/schema → 建表 → 触发器 → hypertable 与保留策略。"""
    bind = op.get_bind()

    # 1) 扩展与 schema（对齐 ddl/00_schemas.sql）
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    for schema in ALL_SCHEMAS:
        op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    # 2) 全部业务/时序表（结构见 contracts/database/ddl 与 ORM 模型）
    Base.metadata.create_all(bind=bind)

    # 3) 公共触发器函数 + scenes.update_time 触发器（对齐 ddl/00_schemas.sql、ddl/02_scene.sql）
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.common_set_update_time()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            NEW.update_time := now();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute("DROP TRIGGER IF EXISTS trg_scenes_set_update_time ON scene_svc.scenes")
    op.execute(
        """
        CREATE TRIGGER trg_scenes_set_update_time
            BEFORE UPDATE ON scene_svc.scenes
            FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time()
        """
    )

    # 4) TimescaleDB hypertable + 保留策略（契约：chunk_time_interval=1 day，保留 90 天）
    for table in HYPERTABLES:
        op.execute(
            f"SELECT create_hypertable('{table}', 'time', "
            f"chunk_time_interval => INTERVAL '{CHUNK_TIME_INTERVAL}', if_not_exists => TRUE)"
        )
        op.execute(
            f"SELECT add_retention_policy('{table}', INTERVAL '{RETENTION_INTERVAL}', "
            f"if_not_exists => TRUE)"
        )


def downgrade() -> None:
    """回滚：删除触发器/函数 → 删表 → 删 schema（含 CASCADE）。

    注意：会丢弃全部数据，仅用于开发/测试环境的完整回滚。
    """
    bind = op.get_bind()

    op.execute("DROP TRIGGER IF EXISTS trg_scenes_set_update_time ON scene_svc.scenes")
    # hypertable 的保留策略随表删除自动移除（TimescaleDB 后台作业清理）
    Base.metadata.drop_all(bind=bind)
    op.execute("DROP FUNCTION IF EXISTS public.common_set_update_time()")
    for schema in ALL_SCHEMAS:
        op.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
