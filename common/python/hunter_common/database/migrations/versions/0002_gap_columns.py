"""0002 差距整改列：users 强制改密/MFA（G-06/G-23）+ vehicles 地理围栏（G-11）

Revision ID: 0002_gap_columns
Revises: 0001_initial_schema
Create Date: 2026-09-20

对应设计文档：
- G-06 §14.1 弱口令防护：``user_svc.users.must_change_password``（首登/重置强制改密）
- G-23 §3.2.2/14.1 MFA：``mfa_enabled`` + ``totp_secret``（密钥应用层加密后存储）
- G-11 §8.4.2 地理围栏：``vehicle_svc.vehicles.fence_json``（circle/polygon + 可选限速）

契约同步：contracts/database/ddl/01_core.sql + infra/deploy/sql/schema.sql（列一致）。
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0002_gap_columns"
down_revision: str | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # G-06：默认 false；部署脚本对初始 admin 显式置 true（首登强制改密）
    op.add_column(
        "users",
        sa.Column(
            "must_change_password",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="user_svc",
    )
    # G-23：MFA 双因素（TOTP）；totp_secret 仅存应用层加密后的密文
    op.add_column(
        "users",
        sa.Column(
            "mfa_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="user_svc",
    )
    op.add_column(
        "users",
        sa.Column("totp_secret", sa.Text(), nullable=True),
        schema="user_svc",
    )
    # G-11：地理围栏定义（NULL = 未配置，不参与围栏校验）
    op.add_column(
        "vehicles",
        sa.Column("fence_json", JSONB(), nullable=True),
        schema="vehicle_svc",
    )


def downgrade() -> None:
    op.drop_column("vehicles", "fence_json", schema="vehicle_svc")
    op.drop_column("users", "totp_secret", schema="user_svc")
    op.drop_column("users", "mfa_enabled", schema="user_svc")
    op.drop_column("users", "must_change_password", schema="user_svc")
