"""通用 Repository 基类：CRUD + 分页查询 + 软删除 + 批量写入。

设计要点（对齐开发规则与性能指标）：
- 只负责数据访问，**不提交事务**（事务边界由调用方或 DatabaseSessionManager 控制）
- 默认过滤软删除记录（模型继承 ``SoftDeleteMixin`` 时），``include_deleted=True`` 可显式包含
- ``filters`` 的键必须是模型真实列名（非法键抛 2001 参数错误），值走参数绑定（禁止拼接 SQL）
- 分页：``page ≥ 1``、``1 ≤ page_size ≤ MAX_PAGE_SIZE``（保护 P95 ≤ 200ms）
- 批量写入：``bulk_create`` / ``bulk_create_ignore_conflicts`` 走 executemany 与
  ``ON CONFLICT DO NOTHING``（时序写入 ≥ 10000 点/秒、消费幂等）
- 条件查询：``find_one`` / ``find_all`` 接受 ORM 列表达式（唯一索引 / 复合主键查询），
  ``delete_where`` 提供条件删除（关联表解绑等）
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar, cast

from sqlalchemy import ColumnElement, Select, delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hunter_common.database.base import Base, supports_soft_delete
from hunter_common.exceptions import (
    InvalidParameterError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.database.repository")

ModelT = TypeVar("ModelT", bound=Base)

#: 分页大小上限（保护接口 P95 ≤ 200ms，防止一次拉取过多数据）
MAX_PAGE_SIZE = 200
#: 默认分页大小
DEFAULT_PAGE_SIZE = 20
#: 批量写入分片大小
BULK_CHUNK_SIZE = 1000
#: PostgreSQL 唯一约束冲突错误码
_PG_UNIQUE_VIOLATION = "23505"


@dataclass(frozen=True, slots=True)
class PageResult(Generic[ModelT]):
    """分页结果（items/total/page/page_size，pages/has_next 为派生属性）。"""

    items: list[ModelT]
    total: int
    page: int
    page_size: int

    @property
    def pages(self) -> int:
        """总页数（向上取整；page_size 非法时返回 0）。"""
        if self.page_size <= 0:
            return 0
        return (self.total + self.page_size - 1) // self.page_size

    @property
    def has_next(self) -> bool:
        return self.page < self.pages


class BaseRepository(Generic[ModelT]):
    """通用数据访问层基类。

    子类只需声明 ``model``（可选声明 ``default_order_by``）::

        class SceneRepository(BaseRepository[Scene]):
            model = Scene
            default_order_by = ("-create_time",)
    """

    model: type[ModelT]
    #: 默认排序（列名前缀 ``-`` 表示 DESC）；缺省按主键升序
    default_order_by: Sequence[str] = ()

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # ---------- 内部工具 ----------

    @property
    def table(self) -> Any:
        return self.model.__table__

    @property
    def pk_name(self) -> str:
        """主键列名（复合主键时取第一列）。"""
        columns = list(self.table.primary_key.columns)
        if not columns:
            raise RuntimeError(f"{self.model.__name__} 未定义主键")
        return str(columns[0].name)

    @property
    def pk_column(self) -> Any:
        return self.table.columns[self.pk_name]

    def column(self, name: str) -> Any:
        """校验并返回列对象；非法列名抛 2001（防止任意属性访问与注入）。"""
        if name not in self.table.columns:
            raise InvalidParameterError(
                f"非法的查询字段: {name}",
                details={"model": self.model.__name__, "field": name},
            )
        return self.table.columns[name]

    def build_conditions(
        self,
        filters: Mapping[str, Any] | None = None,
        *,
        include_deleted: bool = False,
    ) -> list[ColumnElement[bool]]:
        """构造 WHERE 条件：软删除过滤 + 等值 / IN / IS NULL 过滤。"""
        conditions: list[ColumnElement[bool]] = []
        if supports_soft_delete(self.model) and not include_deleted:
            # 软删除过滤：模型具备 deleted_at 列时默认排除已删除记录
            conditions.append(cast(Any, self.model).deleted_at.is_(None))
        for name, value in (filters or {}).items():
            column = self.column(name)
            if value is None:
                conditions.append(column.is_(None))
            elif isinstance(value, (list, tuple, set, frozenset)):
                conditions.append(column.in_(list(value)))
            else:
                conditions.append(column == value)
        return conditions

    def apply_order(self, stmt: Select[Any], order_by: Sequence[str] | None = None) -> Select[Any]:
        """应用排序（``-`` 前缀 = DESC）；未指定时用 ``default_order_by`` 或主键升序。"""
        specs = tuple(order_by) if order_by else tuple(self.default_order_by)
        if not specs:
            return stmt.order_by(self.pk_column.asc())
        order_columns: list[ColumnElement[Any]] = []
        for spec in specs:
            descending = spec.startswith("-")
            column = self.column(spec.lstrip("-+"))
            order_columns.append(column.desc() if descending else column.asc())
        return stmt.order_by(*order_columns)

    @staticmethod
    def translate_integrity_error(exc: IntegrityError, model_name: str) -> Exception:
        """将 IntegrityError 翻译为平台预定义错误码异常（3002 唯一冲突 / 2001 约束失败）。"""
        pgcode = getattr(getattr(exc, "orig", None), "pgcode", None)
        if pgcode == _PG_UNIQUE_VIOLATION:
            return ResourceAlreadyExistsError(
                details={"model": model_name, "pgcode": pgcode, "error": str(exc.orig)}
            )
        return InvalidParameterError(
            f"{model_name} 数据约束校验失败",
            details={"model": model_name, "pgcode": pgcode, "error": str(exc.orig)},
        )

    @staticmethod
    def validate_pagination(page: int, page_size: int) -> None:
        """分页参数校验（page ≥ 1；1 ≤ page_size ≤ MAX_PAGE_SIZE）。"""
        if page < 1:
            raise InvalidParameterError("page 必须 ≥ 1", details={"page": page})
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise InvalidParameterError(
                f"page_size 必须在 1..{MAX_PAGE_SIZE} 之间",
                details={"page_size": page_size, "max": MAX_PAGE_SIZE},
            )

    # ---------- 写操作 ----------

    async def create(self, **values: Any) -> ModelT:
        """新增记录（flush 但不 commit）；唯一键冲突 → 3002。"""
        instance = self.model(**values)
        self.session.add(instance)
        try:
            await self.session.flush()
        except IntegrityError as exc:
            await self.session.rollback()
            raise self.translate_integrity_error(exc, self.model.__name__) from exc
        return instance

    async def bulk_create(
        self, rows: Sequence[Mapping[str, Any]], *, chunk_size: int = BULK_CHUNK_SIZE
    ) -> int:
        """批量插入（executemany 分片），返回插入行数。

        用于时序/事件高吞吐写入路径（目标 ≥ 10000 点/秒），禁止循环单条 INSERT + commit。
        """
        if not rows:
            return 0
        if chunk_size < 1:
            raise InvalidParameterError("chunk_size 必须 ≥ 1", details={"chunk_size": chunk_size})
        inserted = 0
        for start in range(0, len(rows), chunk_size):
            chunk = [dict(row) for row in rows[start : start + chunk_size]]
            await self.session.execute(insert(self.model), chunk)
            inserted += len(chunk)
        return inserted

    async def bulk_create_ignore_conflicts(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        conflict_columns: Sequence[str],
        chunk_size: int = BULK_CHUNK_SIZE,
    ) -> int:
        """批量插入 + ``ON CONFLICT (…) DO NOTHING``（消费幂等 / 重复时间点跳过）。

        Raises:
            InvalidParameterError: ``conflict_columns`` 为空或含非法列名。
        """
        if not conflict_columns:
            raise InvalidParameterError("conflict_columns 不能为空")
        for name in conflict_columns:
            self.column(name)
        if not rows:
            return 0
        inserted = 0
        for start in range(0, len(rows), chunk_size):
            chunk = [dict(row) for row in rows[start : start + chunk_size]]
            stmt = pg_insert(self.model).on_conflict_do_nothing(
                index_elements=list(conflict_columns)
            )
            await self.session.execute(stmt, chunk)
            inserted += len(chunk)
        return inserted

    async def update(self, instance: ModelT, **values: Any) -> ModelT:
        """按字段更新（非法字段 → 2001；flush 但不 commit）。"""
        for name, value in values.items():
            self.column(name)
            setattr(instance, name, value)
        await self.session.flush()
        return instance

    async def soft_delete(self, instance: ModelT) -> ModelT:
        """软删除（写 ``deleted_at``）；模型未继承 SoftDeleteMixin 时抛 NotImplementedError。"""
        if not supports_soft_delete(self.model):
            raise NotImplementedError(f"{self.model.__name__} 未定义 deleted_at，不支持软删除")
        instance.deleted_at = datetime.now(UTC)  # type: ignore[attr-defined]
        await self.session.flush()
        return instance

    async def hard_delete(self, instance: ModelT) -> None:
        """物理删除（仅限确认无需留痕的数据，如超期时序数据清理）。"""
        stmt = delete(self.model).where(self.pk_column == getattr(instance, self.pk_name))
        await self.session.execute(stmt)
        await self.session.flush()

    async def delete_where(self, *conditions: ColumnElement[bool]) -> int:
        """按条件物理删除，返回删除行数（用于关联表解绑等场景）。

        Raises:
            InvalidParameterError: 未提供条件（防止误删整表）。
        """
        if not conditions:
            raise InvalidParameterError("delete_where 必须提供至少一个条件")
        stmt = delete(self.model).where(*conditions)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return int(getattr(result, "rowcount", 0) or 0)

    # ---------- 读操作 ----------

    async def get(
        self, pk_value: Any, *, include_deleted: bool = False
    ) -> ModelT | None:
        """按主键查询（默认排除软删除记录）。"""
        stmt = select(self.model).where(
            *self.build_conditions(None, include_deleted=include_deleted)
        )
        stmt = stmt.where(self.pk_column == pk_value)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get_or_raise(
        self, pk_value: Any, *, include_deleted: bool = False
    ) -> ModelT:
        """按主键查询，缺失时抛 3001 资源不存在。"""
        instance = await self.get(pk_value, include_deleted=include_deleted)
        if instance is None:
            raise ResourceNotFoundError(
                details={"model": self.model.__name__, self.pk_name: str(pk_value)}
            )
        return instance

    async def find_one(
        self, *conditions: ColumnElement[bool], include_deleted: bool = False
    ) -> ModelT | None:
        """按自定义条件查询单条记录（条件为 ORM 列表达式，一律参数绑定）。

        用于按唯一索引/复合主键查询（如 ``User.username == name``）；
        模型具备 ``deleted_at`` 时自动附加软删除过滤。
        """
        stmt = select(self.model).where(
            *self.build_conditions(None, include_deleted=include_deleted), *conditions
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def find_all(
        self,
        *conditions: ColumnElement[bool],
        order_by: Sequence[str] | None = None,
        limit: int | None = None,
        include_deleted: bool = False,
    ) -> list[ModelT]:
        """按自定义条件查询列表（可选排序；limit 上限 MAX_PAGE_SIZE）。"""
        stmt = select(self.model).where(
            *self.build_conditions(None, include_deleted=include_deleted), *conditions
        )
        stmt = self.apply_order(stmt, order_by)
        if limit is not None:
            if not 1 <= limit <= MAX_PAGE_SIZE:
                raise InvalidParameterError(
                    f"limit 必须在 1..{MAX_PAGE_SIZE} 之间",
                    details={"limit": limit, "max": MAX_PAGE_SIZE},
                )
            stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        order_by: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        include_deleted: bool = False,
    ) -> list[ModelT]:
        """条件查询列表（可带 limit/offset；limit 上限 MAX_PAGE_SIZE）。"""
        stmt = select(self.model).where(
            *self.build_conditions(filters, include_deleted=include_deleted)
        )
        stmt = self.apply_order(stmt, order_by)
        if offset:
            if offset < 0:
                raise InvalidParameterError("offset 不能为负数", details={"offset": offset})
            stmt = stmt.offset(offset)
        if limit is not None:
            if not 1 <= limit <= MAX_PAGE_SIZE:
                raise InvalidParameterError(
                    f"limit 必须在 1..{MAX_PAGE_SIZE} 之间",
                    details={"limit": limit, "max": MAX_PAGE_SIZE},
                )
            stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def count(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        include_deleted: bool = False,
    ) -> int:
        """统计记录数（软删除过滤规则同 list）。"""
        stmt = (
            select(func.count())
            .select_from(self.model)
            .where(*self.build_conditions(filters, include_deleted=include_deleted))
        )
        result = await self.session.execute(stmt)
        return int(result.scalar_one())

    async def exists(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        include_deleted: bool = False,
    ) -> bool:
        """是否存在满足条件的记录（比 count 更轻量：LIMIT 1）。"""
        stmt = (
            select(self.pk_column)
            .where(*self.build_conditions(filters, include_deleted=include_deleted))
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.first() is not None

    async def paginate(
        self,
        *,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        filters: Mapping[str, Any] | None = None,
        order_by: Sequence[str] | None = None,
        include_deleted: bool = False,
    ) -> PageResult[ModelT]:
        """分页查询（先 count 后取页；page/page_size 校验失败抛 2001）。"""
        self.validate_pagination(page, page_size)
        total = await self.count(filters=filters, include_deleted=include_deleted)
        items: list[ModelT] = []
        if total:
            items = await self.list(
                filters=filters,
                order_by=order_by,
                limit=page_size,
                offset=(page - 1) * page_size,
                include_deleted=include_deleted,
            )
        return PageResult(items=items, total=total, page=page, page_size=page_size)


__all__ = [
    "BULK_CHUNK_SIZE",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "BaseRepository",
    "PageResult",
]
