"""0003 OTA 版本审核流状态（G-18 决策②：实现设计文档 §7.2.2 testing/reviewing 审核流）

Revision ID: 0003_ota_review_status
Revises: 0002_gap_columns
Create Date: 2026-09-20

状态机由四态扩展为六态：
``draft → testing → reviewing → published → deprecated / disabled``（reviewing 驳回回退 draft）。

变更内容：重建 ``ota_svc.ota_versions.status`` CHECK 约束（四值 → 六值，新增
``testing``/``reviewing``）。放宽约束无需数据回填：存量行取值均为旧四值子集。

契约同步：hunter_common.enums.OtaVersionStatus + contracts/database/ddl/03_ota.sql
+ infra/deploy/sql/schema.sql + contracts/openapi/ota-service.yaml。
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_ota_review_status"
down_revision: str | None = "0002_gap_columns"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: 旧四值词表（downgrade 用；G-18② 之前的历史形态）
_LEGACY_STATUS_VALUES = "'draft', 'published', 'deprecated', 'disabled'"

#: 新六值词表（与 OtaVersionStatus 枚举一致）
_REVIEW_FLOW_STATUS_VALUES = "'draft', 'testing', 'reviewing', 'published', 'deprecated', 'disabled'"


def upgrade() -> None:
    op.drop_constraint(
        "ota_versions_status_check", "ota_versions", type_="check", schema="ota_svc"
    )
    op.create_check_constraint(
        "ota_versions_status_check",
        "ota_versions",
        f"status IN ({_REVIEW_FLOW_STATUS_VALUES})",
        schema="ota_svc",
    )


def downgrade() -> None:
    # 收窄回退前须确保无 testing/reviewing 存量行，否则 PostgreSQL 将拒绝重建约束
    op.drop_constraint(
        "ota_versions_status_check", "ota_versions", type_="check", schema="ota_svc"
    )
    op.create_check_constraint(
        "ota_versions_status_check",
        "ota_versions",
        f"status IN ({_LEGACY_STATUS_VALUES})",
        schema="ota_svc",
    )
