"""SQLAlchemy 2.0 声明式基类与列类型工具。

- :class:`Base`：DeclarativeBase + 统一约束命名规范（便于 Alembic 稳定 diff）
- :class:`StrEnumType`：TEXT 列 ↔ Python StrEnum 双向映射（DDL 保持 TEXT + CHECK）
- 混入类：CreateTimeMixin / UpdateTimeMixin / SoftDeleteMixin（软删除，配合 BaseRepository）
- 列工厂：uuid_primary_key_column / create_time_column，保证 DDL 与契约一致
"""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, TypeVar
from uuid import UUID

from sqlalchemy import DateTime, MetaData, Text, func, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

#: 约束/索引命名规范：与 contracts/database/ddl 中的显式命名保持可读一致
NAMING_CONVENTION: dict[str, str] = {
    "ix": "idx_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

EnumT = TypeVar("EnumT", bound=StrEnum)


class StrEnumType(TypeDecorator[EnumT]):
    """StrEnum ↔ 字符串列映射（``impl = Text``，DDL 为 TEXT + CHECK）。

    写入：值必须是枚举成员或其等价字符串，非法值抛 ``ValueError``（拒绝脏数据）；
    读取：返回枚举成员。
    """

    impl = Text
    cache_ok = True

    def __init__(self, enum_class: type[EnumT], length: int | None = None, **kwargs: Any) -> None:
        super().__init__(length=length, **kwargs)
        self._enum_class: type[EnumT] = enum_class

    @property
    def enum_class(self) -> type[EnumT]:
        return self._enum_class

    def process_bind_param(self, value: Any, dialect: Any) -> str | None:
        if value is None:
            return None
        try:
            return self._enum_class(value).value
        except ValueError as exc:
            raise ValueError(
                f"{self._enum_class.__name__} 不接受取值 {value!r}"
                f"（受控词表见 contracts/database/enums.md）"
            ) from exc

    def process_result_value(self, value: Any, dialect: Any) -> EnumT | None:
        if value is None:
            return None
        return self._enum_class(value)


class Base(DeclarativeBase):
    """所有 ORM 模型的声明式基类（metadata 统一命名规范）。"""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class CreateTimeMixin:
    """``create_time``（TIMESTAMPTZ，默认 now()）。"""

    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class UpdateTimeMixin:
    """``update_time``（TIMESTAMPTZ）：ORM 侧 onupdate + DDL 触发器双保险。"""

    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class SoftDeleteMixin:
    """软删除标记（``deleted_at``）：非 NULL 视为已删除。

    仅当模型继承本混入类时 :class:`~hunter_common.database.repository.BaseRepository`
    才允许调用 ``soft_delete()``，否则抛 ``NotImplementedError``。
    """

    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None


def uuid_primary_key_column() -> Mapped[UUID]:
    """UUID 主键列（``gen_random_uuid()``，PG13+ 内置，无需 pgcrypto）。"""
    return mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )


def create_time_column() -> Mapped[datetime]:
    """独立 ``create_time`` 列工厂（无法使用混入类时使用）。"""
    return mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


def supports_soft_delete(model: type[Any]) -> bool:
    """模型是否支持软删除（是否具备 ``deleted_at`` 列）。"""
    return hasattr(model, "deleted_at")


__all__ = [
    "NAMING_CONVENTION",
    "Base",
    "CreateTimeMixin",
    "SoftDeleteMixin",
    "StrEnumType",
    "UpdateTimeMixin",
    "create_time_column",
    "supports_soft_delete",
    "uuid_primary_key_column",
]
