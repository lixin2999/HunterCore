"""0005 车辆台账 IP 字段：vehicles.ip_address（V1.28.0）

Revision ID: 0005_vehicle_ip_address
Revises: 0004_event_pre_warning
Create Date: 2026-10-08

背景：车辆管理列表需展示车端当前网络地址。双写入链路：
- data-collector：health 心跳顶层可选 ``ip`` 字段自动回写（车端上报值为权威，
  ``VehicleLedgerWriter.record_ip`` 仅变化时落库）；
- vehicle-service：建账 / 台账修改人工录入（PATCH 可覆盖，下次心跳再校正）。
未上报且未录入时为 NULL，前端显示 "-"。

契约同步：contracts/database/ddl/01_core.sql + infra/deploy/sql/schema.sql（建表段 + 幂等 ALTER）
+ hunter_common ORM Vehicle + contracts/kafka/schemas/health.schema.json +
contracts/openapi/vehicle-service.yaml。
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_vehicle_ip_address"
down_revision: str | None = "0004_event_pre_warning"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "vehicles",
        sa.Column("ip_address", sa.Text(), nullable=True),
        schema="vehicle_svc",
    )


def downgrade() -> None:
    op.drop_column("vehicles", "ip_address", schema="vehicle_svc")
