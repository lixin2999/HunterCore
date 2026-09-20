"""0004 事件类型扩展（G-22 决策②：新增 collision_pre_warning，18 种 → 19 种）

Revision ID: 0004_event_pre_warning
Revises: 0003_ota_review_status
Create Date: 2026-09-20

背景：设计文档 6.2.3 节「TTC < 3.0s 碰撞预警（warning）」与受控词表
``collision_warning``（critical，TTC < 1.5s）等级冲突（data-analytics 契约 pending #3）。
决策 G-22② 通过新增独立事件类型消解冲突：
``collision_pre_warning``（warning，TTC < 3.0s）与 ``collision_warning``（critical，TTC < 1.5s）分级。

变更内容：重建 ``data_collector.events.event_type`` CHECK 约束（18 值 → 19 值，新增
``collision_pre_warning``）。放宽约束无需数据回填：存量行取值均为旧 18 值子集。

契约同步：hunter_common.enums.EventType / EVENT_LEVEL_BY_TYPE
+ contracts/database/ddl/04_events.sql + infra/deploy/sql/schema.sql
+ contracts/database/enums.md 第 3 节 + contracts/kafka/schemas/{event,alert_event,analytics_result}.schema.json
+ contracts/openapi/{data-collector,data-analytics}.yaml。
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_event_pre_warning"
down_revision: str | None = "0003_ota_review_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: 旧 18 值词表（downgrade 用；G-22② 之前的历史形态，无 collision_pre_warning）
_LEGACY_EVENT_TYPE_VALUES = (
    "'harsh_acceleration', 'harsh_braking', 'harsh_turning', 'over_speed', "
    "'collision_warning', 'manual_takeover', 'emergency_stop', 'battery_low', "
    "'battery_critical', 'communication_loss', 'sensor_fault', 'perception_fault', "
    "'planning_fault', 'control_fault', 'ota_start', 'ota_success', "
    "'ota_failed', 'ota_rollback'"
)

#: 新 19 值词表（与 EventType 枚举一致）
_PRE_WARNING_EVENT_TYPE_VALUES = (
    "'harsh_acceleration', 'harsh_braking', 'harsh_turning', 'over_speed', "
    "'collision_pre_warning', 'collision_warning', 'manual_takeover', 'emergency_stop', "
    "'battery_low', 'battery_critical', 'communication_loss', 'sensor_fault', "
    "'perception_fault', 'planning_fault', 'control_fault', 'ota_start', 'ota_success', "
    "'ota_failed', 'ota_rollback'"
)


def upgrade() -> None:
    op.drop_constraint(
        "events_event_type_check", "events", type_="check", schema="data_collector"
    )
    op.create_check_constraint(
        "events_event_type_check",
        "events",
        f"event_type IN ({_PRE_WARNING_EVENT_TYPE_VALUES})",
        schema="data_collector",
    )


def downgrade() -> None:
    # 收窄回退前须确保无 collision_pre_warning 存量行，否则 PostgreSQL 将拒绝重建约束
    op.drop_constraint(
        "events_event_type_check", "events", type_="check", schema="data_collector"
    )
    op.create_check_constraint(
        "events_event_type_check",
        "events",
        f"event_type IN ({_LEGACY_EVENT_TYPE_VALUES})",
        schema="data_collector",
    )
