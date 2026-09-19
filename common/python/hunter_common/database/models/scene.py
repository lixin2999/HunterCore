"""ORM 模型：场景库（scene_svc.scenes）。

对应契约：contracts/database/ddl/02_scene.sql。
软删除：`deleted_at` 非 NULL 视为已删除（BaseRepository 默认过滤）。
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hunter_common.database.base import (
    Base,
    SoftDeleteMixin,
    StrEnumType,
    UpdateTimeMixin,
    uuid_primary_key_column,
)
from hunter_common.database.enums import SceneStatus
from hunter_common.database.schema_names import SCENE_SCHEMA

_SCENE_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in SceneStatus)


class Scene(Base, UpdateTimeMixin, SoftDeleteMixin):
    """场景（scene_svc.scenes）；仅 ``draft`` 状态可编辑。

    ``creator`` 为逻辑外键 → user_svc.users.user_id（跨服务不建物理外键）。
    """

    __tablename__ = "scenes"
    __table_args__ = (
        CheckConstraint(f"status IN ({_SCENE_STATUS_VALUES})", name="scenes_status_check"),
        Index(
            "idx_scenes_status_type_create_time",
            "status",
            "scene_type",
            text("create_time DESC"),
            postgresql_where=text("deleted_at IS NULL"),
        ),
        Index("idx_scenes_scene_type", "scene_type"),
        Index("idx_scenes_tags", "tags", postgresql_using="gin"),
        Index("idx_scenes_config_json", "config_json", postgresql_using="gin"),
        Index(
            "uq_scenes_scene_name",
            "scene_name",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        {"schema": SCENE_SCHEMA},
    )

    scene_id: Mapped[UUID] = uuid_primary_key_column()
    scene_name: Mapped[str] = mapped_column(Text, nullable=False)
    scene_type: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    config_json: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    version: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'1.0.0'"))
    creator: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    status: Mapped[SceneStatus] = mapped_column(
        StrEnumType(SceneStatus), nullable=False, server_default=text("'draft'")
    )

    # 无 relationship：``creator`` 为逻辑外键 → user_svc.users.user_id，
    # 跨服务补全一律走 REST（contracts/database/orm-mapping.md 第 2.1 节）


__all__ = ["Scene"]
