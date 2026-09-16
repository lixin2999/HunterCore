"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

说明：结构变更必须同步更新 contracts/database/ddl/*.sql 与 ORM 模型
（``hunter_common/database/models/``），三方一致性由 scripts/verify_data_layer.py 校验。
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: str | None = ${repr(down_revision)}
branch_labels: str | Sequence[str] | None = ${repr(branch_labels)}
depends_on: str | Sequence[str] | None = ${repr(depends_on)}


def upgrade() -> None:
    """升级：新增结构（禁止就地修改历史迁移）。"""
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    """回滚：必须实现（upgrade/downgrade 成对，评审时同时审查）。"""
    ${downgrades if downgrades else "pass"}
