"""通用 Repository 基类：CRUD + 分页查询 + 软删除 + 批量写入。

设计要点（对齐开发规则与性能指标）：
- 只负责数据访问，**不提交事务**（事务边界由调用方或 DatabaseSessionManager 控制）；
  ``create`` / ``update`` 用 SAVEPOINT 局部回滚约束冲突，失败后会话仍可用（外事务不受影响）
- 默认过滤软删除记录（模型继承 ``SoftDeleteMixin`` 时），``include_deleted=True`` 可显式包含
- ``filters`` 的键必须是模型真实列名（非法键抛 2001 参数错误），值走参数绑定（禁止拼接 SQL）；
  写路径（``create`` / ``bulk_create`` / ``bulk_create_ignore_conflicts``）同样做列名白名单校验
- 排序 spec：``"col"`` = ASC、``"-col"`` = DESC、``"-col:nl"`` = DESC NULLS LAST、
  ``"col:nf"`` = ASC NULLS FIRST（与 DDL 索引的 ``NULLS LAST`` 语义保持一致，见契约 orm-mapping 第 3.2 节）
- 分页：``page ≥ 1``、``1 ≤ page_size ≤ MAX_PAGE_SIZE``（保护 P95 ≤ 200ms）；
  读方法 ``limit`` 上限取 ``max_query_limit``（默认 ``MAX_PAGE_SIZE``，时序表可放宽到 ``MAX_SERIES_POINTS``）
- 加载策略：读方法提供 ``options=``（如 ``selectinload(Model.rel)``），
  用于显式加载契约要求的 ``raise_on_sql`` 关系（禁止隐式 IO）
- 复合主键模型（如 ``vehicle_telemetry`` / ``algorithm_metrics``）禁用继承来的
  ``get`` / ``get_or_raise`` / ``hard_delete``（单列主键语义会跨车辆误命中），改用子类专属方法
- 批量写入：``bulk_create`` / ``bulk_create_ignore_conflicts`` 走 executemany 与
  ``ON CONFLICT DO NOTHING``（时序写入 ≥ 10000 点/秒、消费幂等）；返回**提交（attempted）行数**
  （异步驱动 ``supports_sane_multi_rowcount = False``，无法提供 ``ON CONFLICT`` 跳过后的精确行数）
- 条件查询：``find_one`` / ``find_all`` 接受 ORM 列表达式（唯一索引 / 复合主键查询），
  ``delete_where`` 提供条件删除（关联表解绑等）
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar, cast

from sqlalchemy import ColumnElement, Select, delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.interfaces import ORMOption

from hunter_common.database.base import Base, supports_soft_delete
from hunter_common.exceptions import (
    InvalidParameterError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)

ModelT = TypeVar("ModelT", bound=Base)

#: 分页大小上限（保护接口 P95 ≤ 200ms，防止一次拉取过多数据）
MAX_PAGE_SIZE = 200
#: 默认分页大小
DEFAULT_PAGE_SIZE = 20
#: 批量写入分片大小
BULK_CHUNK_SIZE = 1000
#: 时序序列读取上限（轨迹回放 / 指标趋势；仅时序 Repository 放宽 ``max_query_limit``）
MAX_SERIES_POINTS = 10000
#: 物理清理默认批大小（``purge_before``：限制单事务规模，避免长事务与 WAL 放大）
DEFAULT_PURGE_BATCH_SIZE = 5000
#: PostgreSQL 唯一约束冲突错误码（23505 unique_violation）
_PG_UNIQUE_VIOLATION = "23505"
#: 排序 spec 的空值位次后缀（与 DDL 索引 ``NULLS LAST`` 对齐）
_NULLS_LAST_SUFFIX = ":nl"
_NULLS_FIRST_SUFFIX = ":nf"
#: 模型 → 合法列名集合（写路径白名单校验用；按模型缓存，避免逐行重复构造）
_COLUMN_NAMES_CACHE: dict[type[Base], frozenset[str]] = {}
#: SQL 报错文本中的**约束名**提取（仅保留标识符；行值一律不进入 details/日志）
_CONSTRAINT_NAME_RE = re.compile(r'(?:constraint|index|relation) "([^"]+)"')


def _column_names(model: type[Base]) -> frozenset[str]:
    """返回模型的合法列名集合（进程内缓存，O(1) 复用）。"""
    cached = _COLUMN_NAMES_CACHE.get(model)
    if cached is None:
        cached = frozenset(str(column.name) for column in model.__table__.columns)
        _COLUMN_NAMES_CACHE[model] = cached
    return cached


def _constraint_name(exc: IntegrityError) -> str | None:
    """从驱动报错文本中提取约束名（只取标识符，不保留行值，避免敏感信息进入日志/详情）。"""
    match = _CONSTRAINT_NAME_RE.search(str(exc.orig))
    return match.group(1) if match else None


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
    #: 默认排序（列名前缀 ``-`` 表示 DESC；后缀 ``:nl`` / ``:nf`` 指定空值位次，缺省按主键升序）
    default_order_by: Sequence[str] = ()
    #: 读方法 limit 上限（默认 MAX_PAGE_SIZE；时序 Repository 可放宽到 MAX_SERIES_POINTS）
    max_query_limit: int = MAX_PAGE_SIZE

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # ---------- 内部工具 ----------

    @property
    def table(self) -> Any:
        return self.model.__table__

    @property
    def pk_names(self) -> tuple[str, ...]:
        """主键列名（按 DDL 主键顺序；复合主键返回多列）。"""
        columns = list(self.table.primary_key.columns)
        if not columns:
            raise RuntimeError(f"{self.model.__name__} 未定义主键")
        return tuple(str(column.name) for column in columns)

    @property
    def pk_name(self) -> str:
        """主键列名（复合主键时取第一列；写路径请先确认 :attr:`is_composite_pk`）。"""
        return self.pk_names[0]

    @property
    def pk_column(self) -> Any:
        return self.table.columns[self.pk_name]

    @property
    def is_composite_pk(self) -> bool:
        """是否复合主键（如 ``vehicle_telemetry`` / ``algorithm_metrics``）。"""
        return len(self.pk_names) > 1

    @property
    def column_names(self) -> frozenset[str]:
        """模型合法列名集合（按模型缓存；写路径白名单校验用）。"""
        return _column_names(self.model)

    def column(self, name: str) -> Any:
        """校验并返回列对象；非法列名抛 2001（防止任意属性访问与注入）。"""
        if name not in self.table.columns:
            raise InvalidParameterError(
                f"非法的查询字段: {name}",
                details={"model": self.model.__name__, "field": name},
            )
        return self.table.columns[name]

    def ensure_known_columns(self, names: Iterable[str]) -> None:
        """写路径列名白名单校验（非法列名抛 2001，替代 SQLAlchemy ``TypeError`` → 5000）。"""
        unknown = sorted(set(names) - self.column_names)
        if unknown:
            raise InvalidParameterError(
                f"{self.model.__name__} 不接受字段: {', '.join(unknown)}",
                details={"model": self.model.__name__, "unknown_fields": unknown},
            )

    def require_single_primary_key(self, operation: str) -> None:
        """复合主键模型禁用单列主键语义的操作（基类 ``get`` / ``hard_delete`` 等）。

        复合主键表按首列匹配会跨车辆误命中（读错数据 / 删错数据），
        调用方必须使用子类专属方法（如 ``VehicleTelemetryRepository.get_point``）。
        """
        if self.is_composite_pk:
            raise NotImplementedError(
                f"{self.model.__name__} 为复合主键 {self.pk_names}，"
                f"不支持 {operation}；请使用子类专属方法（如 get_point / list_points）"
            )

    def apply_options(
        self, stmt: Select[Any], options: Sequence[ORMOption] | None = None
    ) -> Select[Any]:
        """挂载显式加载策略（契约 orm-mapping 第 1 节：``raise_on_sql`` 关系必须显式加载）。"""
        return stmt.options(*options) if options else stmt

    def check_query_limit(self, limit: int, *, field: str = "limit") -> None:
        """读方法 limit 校验（1 ≤ limit ≤ ``max_query_limit``）。"""
        if not 1 <= limit <= self.max_query_limit:
            raise InvalidParameterError(
                f"{field} 必须在 1..{self.max_query_limit} 之间",
                details={"model": self.model.__name__, field: limit, "max": self.max_query_limit},
            )

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

    def order_clause(self, spec: str) -> ColumnElement[Any]:
        """把排序 spec 解析为排序表达式（契约 orm-mapping 第 3.2 节）。

        - ``"col"`` → ASC；``"-col"`` → DESC
        - ``"col:nl"`` / ``"-col:nl"`` → 追加 ``NULLS LAST``（与 DDL 索引 ``... DESC NULLS LAST`` 对齐）
        - ``"col:nf"`` → 追加 ``NULLS FIRST``（显式声明）

        说明：PostgreSQL 中 ``DESC`` 默认等价 ``NULLS FIRST``，若 DDL 索引声明 ``NULLS LAST``，
        不加后缀会让执行计划多一个 Sort 并改变业务语义（空值排最前），故默认排序必须显式声明。
        """
        raw, separator, nulls = spec.partition(":")
        nulls_token = nulls.lower()
        if separator and nulls_token not in ("nl", "nf"):
            raise InvalidParameterError(
                f"非法的排序空值位次: {nulls}（仅支持 nl = NULLS LAST / nf = NULLS FIRST）",
                details={"model": self.model.__name__, "order_by": spec},
            )
        column = self.column(raw.lstrip("-+"))
        clause: ColumnElement[Any] = column.desc() if raw.startswith("-") else column.asc()
        if nulls_token == "nl":
            return clause.nulls_last()
        if nulls_token == "nf":
            return clause.nulls_first()
        return clause

    def apply_order(self, stmt: Select[Any], order_by: Sequence[str] | None = None) -> Select[Any]:
        """应用排序（``-`` 前缀 = DESC、``:nl``/``:nf`` = 空值位次）；未指定时用主键升序。"""
        specs = tuple(order_by) if order_by else tuple(self.default_order_by)
        if not specs:
            return stmt.order_by(self.pk_column.asc())
        return stmt.order_by(*(self.order_clause(spec) for spec in specs))

    @staticmethod
    def translate_integrity_error(exc: IntegrityError, model_name: str) -> Exception:
        """将 IntegrityError 翻译为平台预定义错误码异常（3002 唯一冲突 / 2001 约束失败）。

        安全：``details`` 只保留模型名、SQLSTATE 与**约束名**（标识符）。
        禁止把驱动原始报错文本放进 details —— asyncpg 的报错文本包含冲突行的键值
        （如 ``Key (username)=(admin) already exists``），会泄露可被枚举的业务数据。
        """
        pgcode = getattr(getattr(exc, "orig", None), "pgcode", None)
        constraint = _constraint_name(exc)
        if pgcode == _PG_UNIQUE_VIOLATION:
            return ResourceAlreadyExistsError(
                details={"model": model_name, "pgcode": pgcode, "constraint": constraint}
            )
        return InvalidParameterError(
            f"{model_name} 数据约束校验失败",
            details={"model": model_name, "pgcode": pgcode, "constraint": constraint},
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
        """新增记录（flush 但不 commit）；非法字段 → 2001、唯一键冲突 → 3002。

        事务：flush 包在 SAVEPOINT（``begin_nested``）中 —— 约束冲突只回滚该 SAVEPOINT，
        调用方的外事务与已 flush 的其他写入不受影响，失败后会话仍可继续使用；
        失败行同时从会话中移除，避免调用方继续提交时重放同一冲突行。
        """
        self.ensure_known_columns(values)
        instance = self.model(**values)
        self.session.add(instance)
        try:
            async with self.session.begin_nested():
                await self.session.flush()
        except IntegrityError as exc:
            self.session.expunge(instance)
            raise self.translate_integrity_error(exc, self.model.__name__) from exc
        return instance

    def _validate_bulk(self, rows: Sequence[Mapping[str, Any]], chunk_size: int) -> None:
        """批量写入前置校验：分片大小 ≥ 1、每行键均为模型真实列名（非法 → 2001）。"""
        if chunk_size < 1:
            raise InvalidParameterError("chunk_size 必须 ≥ 1", details={"chunk_size": chunk_size})
        for row in rows:
            self.ensure_known_columns(row.keys())

    async def _execute_bulk_chunk(self, stmt: Any, chunk: list[dict[str, Any]]) -> None:
        """执行一个分片（SAVEPOINT 保护，契约冲突 → 3002/2001；已成功分片留在调用方事务中）。"""
        try:
            async with self.session.begin_nested():
                await self.session.execute(stmt, chunk)
        except IntegrityError as exc:
            raise self.translate_integrity_error(exc, self.model.__name__) from exc

    async def bulk_create(
        self, rows: Sequence[Mapping[str, Any]], *, chunk_size: int = BULK_CHUNK_SIZE
    ) -> int:
        """批量插入（executemany 分片），返回**提交行数**。

        用于时序/事件高吞吐写入路径（目标 ≥ 10000 点/秒），禁止循环单条 INSERT + commit。
        任一分片失败即抛 2001/3002（该分片经 SAVEPOINT 回滚，先前分片留在调用方事务中）。
        """
        if not rows:
            return 0
        self._validate_bulk(rows, chunk_size)
        submitted = 0
        for start in range(0, len(rows), chunk_size):
            chunk = [dict(row) for row in rows[start : start + chunk_size]]
            await self._execute_bulk_chunk(insert(self.model), chunk)
            submitted += len(chunk)
        return submitted

    async def bulk_create_ignore_conflicts(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        conflict_columns: Sequence[str],
        chunk_size: int = BULK_CHUNK_SIZE,
    ) -> int:
        """批量插入 + ``ON CONFLICT (…) DO NOTHING``（消费幂等 / 重复时间点跳过），返回**提交行数**。

        ⚠ 返回值语义：返回的是"提交（attempted）行数"，被 ``DO NOTHING`` 跳过的重复行**仍计入** ——
        PostgreSQL 异步驱动声明 ``supports_sane_multi_rowcount = False``，executemany 无法提供
        精确的插入行数（``CursorResult.rowcount`` 不可用）。需要精确去重统计时，
        由调用方基于输入批次与唯一键口径自行统计（如 K8s 消费位点差值）。

        Raises:
            InvalidParameterError: ``conflict_columns`` 为空或含非法列名、行键非法、``chunk_size < 1``。
        """
        if not conflict_columns:
            raise InvalidParameterError("conflict_columns 不能为空")
        for name in conflict_columns:
            self.column(name)
        if not rows:
            return 0
        self._validate_bulk(rows, chunk_size)
        submitted = 0
        for start in range(0, len(rows), chunk_size):
            chunk = [dict(row) for row in rows[start : start + chunk_size]]
            stmt = pg_insert(self.model).on_conflict_do_nothing(
                index_elements=list(conflict_columns)
            )
            await self._execute_bulk_chunk(stmt, chunk)
            submitted += len(chunk)
        return submitted

    async def update(self, instance: ModelT, **values: Any) -> ModelT:
        """按字段更新（非法字段 → 2001；flush 但不 commit；约束冲突 → 3002/2001）。

        事务：与 ``create`` 一致，flush 包在 SAVEPOINT 中，冲突只回滚该 SAVEPOINT。
        """
        for name, value in values.items():
            self.column(name)
            setattr(instance, name, value)
        try:
            async with self.session.begin_nested():
                await self.session.flush()
        except IntegrityError as exc:
            raise self.translate_integrity_error(exc, self.model.__name__) from exc
        return instance

    async def soft_delete(self, instance: ModelT) -> ModelT:
        """软删除（写 ``deleted_at``）；模型未继承 SoftDeleteMixin 时抛 NotImplementedError。"""
        if not supports_soft_delete(self.model):
            raise NotImplementedError(f"{self.model.__name__} 未定义 deleted_at，不支持软删除")
        instance.deleted_at = datetime.now(UTC)  # type: ignore[attr-defined]
        await self.session.flush()
        return instance

    async def hard_delete(self, instance: ModelT) -> None:
        """物理删除（仅限确认无需留痕的数据，如超期时序数据清理）。

        ⚠ 复合主键模型禁用：基类按首列主键匹配会删除**其他车辆**的同时间点数据，
        时序表清理请使用 ``purge_before``（分批）或 TimescaleDB 保留策略。
        """
        self.require_single_primary_key("hard_delete")
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
        self,
        pk_value: Any,
        *,
        include_deleted: bool = False,
        options: Sequence[ORMOption] | None = None,
    ) -> ModelT | None:
        """按主键查询（默认排除软删除记录）；``options`` 用于显式加载关系。

        ⚠ 复合主键模型禁用（首列主键语义会跨车辆误命中），请使用子类专属方法。
        """
        self.require_single_primary_key("get")
        stmt = select(self.model).where(
            *self.build_conditions(None, include_deleted=include_deleted)
        )
        stmt = stmt.where(self.pk_column == pk_value)
        result = await self.session.execute(self.apply_options(stmt, options))
        return result.scalars().first()

    async def get_or_raise(
        self,
        pk_value: Any,
        *,
        include_deleted: bool = False,
        options: Sequence[ORMOption] | None = None,
    ) -> ModelT:
        """按主键查询，缺失时抛 3001 资源不存在（复合主键模型禁用，同 :meth:`get`）。"""
        instance = await self.get(pk_value, include_deleted=include_deleted, options=options)
        if instance is None:
            raise ResourceNotFoundError(
                details={"model": self.model.__name__, self.pk_name: str(pk_value)}
            )
        return instance

    async def find_one(
        self,
        *conditions: ColumnElement[bool],
        include_deleted: bool = False,
        options: Sequence[ORMOption] | None = None,
    ) -> ModelT | None:
        """按自定义条件查询单条记录（条件为 ORM 列表达式，一律参数绑定）。

        用于按唯一索引/复合主键查询（如 ``User.username == name``）；
        模型具备 ``deleted_at`` 时自动附加软删除过滤；
        ``options`` 用于显式加载契约要求的 ``raise_on_sql`` 关系。
        """
        stmt = select(self.model).where(
            *self.build_conditions(None, include_deleted=include_deleted), *conditions
        )
        result = await self.session.execute(self.apply_options(stmt, options))
        return result.scalars().first()

    async def find_all(
        self,
        *conditions: ColumnElement[bool],
        order_by: Sequence[str] | None = None,
        limit: int | None = None,
        include_deleted: bool = False,
        options: Sequence[ORMOption] | None = None,
    ) -> list[ModelT]:
        """按自定义条件查询列表（可选排序 / ``options``；limit 上限 ``max_query_limit``）。"""
        stmt = select(self.model).where(
            *self.build_conditions(None, include_deleted=include_deleted), *conditions
        )
        stmt = self.apply_options(self.apply_order(stmt, order_by), options)
        if limit is not None:
            self.check_query_limit(limit)
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
        options: Sequence[ORMOption] | None = None,
    ) -> list[ModelT]:
        """条件查询列表（可带 limit/offset；limit 上限 ``max_query_limit``）。"""
        stmt = select(self.model).where(
            *self.build_conditions(filters, include_deleted=include_deleted)
        )
        stmt = self.apply_options(self.apply_order(stmt, order_by), options)
        if offset:
            if offset < 0:
                raise InvalidParameterError("offset 不能为负数", details={"offset": offset})
            stmt = stmt.offset(offset)
        if limit is not None:
            self.check_query_limit(limit)
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
        options: Sequence[ORMOption] | None = None,
    ) -> PageResult[ModelT]:
        """分页查询（先 count 后取页；page/page_size 校验失败抛 2001）。

        ``page_size`` 上限固定为 ``MAX_PAGE_SIZE``（对外分页契约），
        与读方法的 ``max_query_limit`` 解耦，避免分页接口绕过 API 分页上限。
        """
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
                options=options,
            )
        return PageResult(items=items, total=total, page=page, page_size=page_size)


__all__ = [
    "BULK_CHUNK_SIZE",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_PURGE_BATCH_SIZE",
    "MAX_PAGE_SIZE",
    "MAX_SERIES_POINTS",
    "BaseRepository",
    "PageResult",
]
