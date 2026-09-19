"""BaseRepository 通用数据访问层测试。

使用记录型 Stub Session（不依赖数据库）：验证 CRUD、分页、软删除、批量写入的行为与生成 SQL，
并通过 PostgreSQL 方言编译语句断言「软删除过滤 / schema 限定表名 / ON CONFLICT」等关键语义。
"""
from __future__ import annotations

from types import TracebackType
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError

from hunter_common.database import MAX_PAGE_SIZE, BaseRepository, PageResult
from hunter_common.database.enums import SceneStatus
from hunter_common.database.models import Scene, Vehicle
from hunter_common.exceptions import (
    ErrorCode,
    InvalidParameterError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)


class StubResult:
    """模拟 SQLAlchemy Result：支持 scalars()/all()/first()/scalar_one()。"""

    def __init__(self, rows: list[Any] | None = None, scalar: Any = None, rowcount: int = 0) -> None:
        self._rows = list(rows or [])
        self._scalar = scalar
        self.rowcount = rowcount

    def scalars(self) -> StubResult:
        return self

    def all(self) -> list[Any]:
        return list(self._rows)

    def first(self) -> Any:
        return self._rows[0] if self._rows else None

    def scalar_one(self) -> Any:
        return self._scalar


class StubSavepoint:
    """模拟 ``AsyncSession.begin_nested()`` 返回的 SAVEPOINT 上下文。

    语义与 SQLAlchemy 一致：块内异常只回滚 SAVEPOINT（``savepoint_rollbacks`` +1），
    异常继续向上抛出，外层事务不受影响。
    """

    def __init__(self, session: StubSession) -> None:
        self._session = session

    async def __aenter__(self) -> StubSession:
        self._session.savepoints += 1
        return self._session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if exc_type is not None:
            self._session.savepoint_rollbacks += 1
        return False


class StubSession:
    """记录 execute/add/flush 调用的最小 AsyncSession 替身。"""

    def __init__(self, *, flush_error: Exception | None = None) -> None:
        self.executed: list[tuple[Any, Any]] = []
        self.added: list[Any] = []
        self.flushed = 0
        self.rolled_back = 0
        self.savepoints = 0
        self.savepoint_rollbacks = 0
        self.expunged: list[Any] = []
        self.deleted: list[Any] = []
        self._queue: list[StubResult] = []
        self._flush_error = flush_error

    def queue(self, *results: StubResult | Exception) -> None:
        """排队下一次 execute 的返回值；元素为异常时该次 execute 直接抛出。"""
        self._queue.extend(results)

    async def execute(self, statement: Any, params: Any = None) -> StubResult:
        self.executed.append((statement, params))
        item: StubResult | Exception = self._queue.pop(0) if self._queue else StubResult()
        if isinstance(item, Exception):
            raise item
        return item

    def add(self, instance: Any) -> None:
        self.added.append(instance)

    def begin_nested(self) -> StubSavepoint:
        return StubSavepoint(self)

    def expunge(self, instance: Any) -> None:
        self.expunged.append(instance)

    async def flush(self) -> None:
        self.flushed += 1
        if self._flush_error is not None:
            raise self._flush_error

    async def rollback(self) -> None:
        self.rolled_back += 1

    async def delete(self, instance: Any) -> None:
        self.deleted.append(instance)


class SceneRepository(BaseRepository[Scene]):
    model = Scene
    default_order_by = ("-create_time",)


class VehicleRepository(BaseRepository[Vehicle]):
    model = Vehicle


def compiled(statement: Any, *, literal: bool = False) -> str:
    """将语句编译为 PostgreSQL SQL 文本（单行，便于断言；literal=True 内联字面量）。"""
    compile_kwargs = {"literal_binds": True} if literal else {}
    sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs=compile_kwargs))
    return " ".join(sql.split())


def scene_repo(session: StubSession | None = None) -> tuple[SceneRepository, StubSession]:
    stub = session or StubSession()
    return SceneRepository(stub), stub  # type: ignore[arg-type]


def vehicle_repo(session: StubSession | None = None) -> tuple[VehicleRepository, StubSession]:
    stub = session or StubSession()
    return VehicleRepository(stub), stub  # type: ignore[arg-type]


# ---------- 写操作 ----------


async def test_create_adds_instance_and_flushes() -> None:
    repo, stub = scene_repo()
    scene = await repo.create(scene_name="crossing", scene_type="urban", creator=uuid4())
    assert isinstance(scene, Scene)
    assert stub.added == [scene]
    assert stub.flushed == 1


async def test_create_translates_unique_violation_to_3002() -> None:
    orig = type("FakeOrig", (Exception,), {"pgcode": "23505"})(
        'duplicate key value violates unique constraint "uq_scenes_scene_name"'
    )
    repo, stub = scene_repo(StubSession(flush_error=IntegrityError("insert", {}, orig)))
    with pytest.raises(ResourceAlreadyExistsError) as excinfo:
        await repo.create(scene_name="dup", scene_type="urban", creator=uuid4())
    assert excinfo.value.code == ErrorCode.RESOURCE_ALREADY_EXISTS
    # SAVEPOINT 局部回滚：外事务不被回滚、失败行被移出会话、约束名进入 details（不含行值）
    assert stub.savepoint_rollbacks == 1
    assert stub.rolled_back == 0, "禁止回滚调用方外事务（事务边界属调用方）"
    assert len(stub.expunged) == 1
    assert excinfo.value.details["constraint"] == "uq_scenes_scene_name"
    assert "dup" not in str(excinfo.value.details), "details 禁止携带冲突行的键值"


async def test_create_translates_check_violation_to_2001() -> None:
    orig = type("FakeOrig", (Exception,), {"pgcode": "23514"})(
        'new row violates check constraint "scenes_status_check"'
    )
    session = StubSession(flush_error=IntegrityError("insert", {}, orig))
    repo, stub = scene_repo(session)
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.create(scene_name="bad", scene_type="urban", creator=uuid4())
    assert excinfo.value.code == ErrorCode.INVALID_PARAM
    assert stub.savepoint_rollbacks == 1
    assert stub.rolled_back == 0
    assert excinfo.value.details["constraint"] == "scenes_status_check"


async def test_create_rejects_unknown_field_with_2001() -> None:
    """契约 orm-mapping 3.1：非法字段必须为 2001（而非 SQLAlchemy TypeError → 5000）。"""
    repo, stub = scene_repo()
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.create(scene_name="s", scene_type="urban", creator=uuid4(), bogus_field=1)
    assert excinfo.value.code == ErrorCode.INVALID_PARAM
    assert stub.added == [] and stub.flushed == 0, "非法入参不得进入会话"


async def test_update_sets_allowed_columns_and_rejects_unknown() -> None:
    repo, stub = scene_repo()
    scene = Scene(scene_name="s", scene_type="urban", creator=uuid4())
    updated = await repo.update(scene, scene_name="renamed")
    assert updated.scene_name == "renamed"
    assert stub.flushed == 1
    with pytest.raises(InvalidParameterError):
        await repo.update(scene, not_a_column=1)


async def test_soft_delete_sets_timestamp_and_requires_capability() -> None:
    repo, stub = scene_repo()
    scene = Scene(scene_name="s", scene_type="urban", creator=uuid4())
    assert scene.deleted_at is None

    await repo.soft_delete(scene)
    assert scene.is_deleted is True
    assert stub.flushed == 1

    repo_vehicle, _ = vehicle_repo()
    with pytest.raises(NotImplementedError):
        await repo_vehicle.soft_delete(Vehicle(vehicle_id="HUNTER-001", vehicle_name="h1"))


async def test_hard_delete_issues_delete_statement() -> None:
    repo, stub = scene_repo()
    scene = Scene(scene_name="s", scene_type="urban", creator=uuid4())
    await repo.hard_delete(scene)
    assert compiled(stub.executed[0][0]).startswith("DELETE FROM scene_svc.scenes")


async def test_bulk_create_uses_executemany_with_chunking() -> None:
    repo, stub = scene_repo()
    rows = [{"scene_name": f"s{i}", "scene_type": "urban", "creator": uuid4()} for i in range(5)]
    inserted = await repo.bulk_create(rows, chunk_size=2)
    assert inserted == 5
    assert len(stub.executed) == 3, "应分 3 片写入（2+2+1）"
    assert all(len(params) <= 2 for _stmt, params in stub.executed)
    assert stub.savepoints == 3, "每个分片一个 SAVEPOINT，限制失败影响范围"
    assert compiled(stub.executed[0][0]).startswith("INSERT INTO scene_svc.scenes")


async def test_bulk_create_translates_chunk_violation_and_reports_savepoint() -> None:
    """批量分片冲突 → 3002（与 create 一致），且只回滚该分片。"""
    repo, stub = scene_repo()
    stub.queue(
        StubResult(),  # 第一片成功
        IntegrityError(
            "insert",
            {},
            type("FakeOrig", (Exception,), {"pgcode": "23505"})(
                'duplicate key value violates unique constraint "uq_scenes_scene_name"'
            ),
        ),
    )
    with pytest.raises(ResourceAlreadyExistsError) as excinfo:
        await repo.bulk_create(
            [{"scene_name": f"s{i}", "scene_type": "urban", "creator": uuid4()} for i in range(3)],
            chunk_size=2,
        )
    assert excinfo.value.code == ErrorCode.RESOURCE_ALREADY_EXISTS
    assert stub.savepoint_rollbacks == 1
    assert stub.rolled_back == 0


async def test_bulk_create_rejects_unknown_row_field_with_2001() -> None:
    repo, stub = scene_repo()
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.bulk_create([{"scene_name": "s", "bogus_field": 1}])
    assert excinfo.value.code == ErrorCode.INVALID_PARAM
    assert stub.executed == [], "非法行键不得发出 SQL"


async def test_bulk_create_ignore_conflicts_uses_on_conflict_do_nothing() -> None:
    repo, stub = scene_repo()
    rows = [{"scene_name": "s", "scene_type": "urban", "creator": uuid4()}]
    await repo.bulk_create_ignore_conflicts(rows, conflict_columns=["scene_name"])
    assert "ON CONFLICT (scene_name) DO NOTHING" in compiled(stub.executed[0][0])


async def test_bulk_create_ignore_conflicts_returns_submitted_rows() -> None:
    """返回值 = 提交（attempted）行数：被 DO NOTHING 跳过的行仍计入（异步驱动无精确 rowcount）。"""
    repo, stub = scene_repo()
    rows = [{"scene_name": f"s{i}", "scene_type": "urban", "creator": uuid4()} for i in range(3)]
    stub.queue(StubResult(rowcount=1))  # 驱动只报告跳过后影响行数，返回值不依赖它

    assert await repo.bulk_create_ignore_conflicts(rows, conflict_columns=["scene_name"]) == 3


async def test_bulk_create_ignore_conflicts_rejects_bad_columns() -> None:
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError):
        await repo.bulk_create_ignore_conflicts([], conflict_columns=[])
    with pytest.raises(InvalidParameterError):
        await repo.bulk_create_ignore_conflicts([{"x": 1}], conflict_columns=["nope"])


async def test_bulk_create_returns_zero_for_empty_rows() -> None:
    repo, stub = scene_repo()
    assert await repo.bulk_create([]) == 0
    assert stub.executed == []


async def test_bulk_create_rejects_non_positive_chunk_size() -> None:
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError):
        await repo.bulk_create([{"scene_name": "s"}], chunk_size=0)


# ---------- 排序 spec（空值位次）----------


async def test_order_clause_supports_nulls_last_and_nulls_first() -> None:
    """排序 spec ``:nl`` / ``:nf`` 必须落到 SQL 的 NULLS LAST / NULLS FIRST（对齐 DDL 索引）。"""
    repo, _ = scene_repo()
    assert str(repo.order_clause("-create_time:nl")).endswith("DESC NULLS LAST")
    assert str(repo.order_clause("create_time:nl")).endswith("ASC NULLS LAST")
    assert str(repo.order_clause("-create_time:nf")).endswith("DESC NULLS FIRST")
    assert str(repo.order_clause("-create_time")).endswith("DESC")


async def test_order_clause_rejects_unknown_null_position_and_column() -> None:
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError):
        repo.order_clause("-create_time:xx")
    with pytest.raises(InvalidParameterError):
        repo.order_clause("-not_a_column:nl")


async def test_apply_order_renders_default_spec_with_nulls_last() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))
    await repo.list(order_by=("-create_time:nl", "scene_id"))
    sql = compiled(stub.executed[0][0], literal=True)
    assert "ORDER BY scene_svc.scenes.create_time DESC NULLS LAST" in sql
    assert "scene_svc.scenes.scene_id ASC" in sql


# ---------- 读操作 ----------


async def test_get_builds_schema_qualified_query_with_soft_delete_filter() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([Scene(scene_name="s", scene_type="urban", creator=uuid4())]))
    found = await repo.get(uuid4())
    assert found is not None
    sql = compiled(stub.executed[0][0])
    assert "FROM scene_svc.scenes" in sql
    assert "scene_svc.scenes.deleted_at IS NULL" in sql
    assert "scene_svc.scenes.scene_id = " in sql


async def test_get_includes_deleted_when_requested() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))
    await repo.get(uuid4(), include_deleted=True)
    assert "deleted_at IS NULL" not in compiled(stub.executed[0][0])


async def test_get_or_raise_raises_3001_when_missing() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))
    with pytest.raises(ResourceNotFoundError) as excinfo:
        await repo.get_or_raise(uuid4())
    assert excinfo.value.code == ErrorCode.RESOURCE_NOT_FOUND


async def test_vehicle_repository_has_no_soft_delete_filter() -> None:
    repo, stub = vehicle_repo()
    stub.queue(StubResult([]))
    await repo.list()
    sql = compiled(stub.executed[0][0])
    assert "FROM vehicle_svc.vehicles" in sql
    assert "deleted_at" not in sql


async def test_list_applies_filters_default_order_and_pagination() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))
    await repo.list(
        filters={"scene_type": "urban", "status": SceneStatus.PUBLISHED},
        limit=10,
        offset=20,
    )
    sql = compiled(stub.executed[0][0], literal=True)
    assert "scene_svc.scenes.scene_type = 'urban'" in sql
    assert "scene_svc.scenes.status = 'published'" in sql
    assert "ORDER BY scene_svc.scenes.create_time DESC" in sql
    assert "LIMIT 10 OFFSET 20" in sql


async def test_list_expands_iterable_filter_to_in_clause() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))
    await repo.list(filters={"status": [SceneStatus.DRAFT, SceneStatus.PUBLISHED]})
    sql = compiled(stub.executed[0][0], literal=True)
    assert "IN ('draft', 'published')" in sql


async def test_list_rejects_unknown_filter_field() -> None:
    """非法列名（疑似注入）必须被拒绝为 2001，而不是拼进 SQL。"""
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.list(filters={"scene_name; DROP TABLE scene_svc.scenes --": "x"})
    assert excinfo.value.code == ErrorCode.INVALID_PARAM


@pytest.mark.parametrize("limit", [0, MAX_PAGE_SIZE + 1])
async def test_list_rejects_out_of_range_limit(limit: int) -> None:
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError):
        await repo.list(limit=limit)


async def test_count_and_exists_queries() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult(scalar=7), StubResult([uuid4()]))

    assert await repo.count(filters={"status": SceneStatus.DRAFT}) == 7
    assert await repo.exists(filters={"status": SceneStatus.DRAFT}) is True

    assert "count(*)" in compiled(stub.executed[0][0]).lower()
    assert "LIMIT 1" in compiled(stub.executed[1][0], literal=True)


async def test_paginate_computes_total_offset_and_pages() -> None:
    repo, stub = scene_repo()
    scenes = [Scene(scene_name=f"s{i}", scene_type="urban", creator=uuid4()) for i in range(2)]
    stub.queue(StubResult(scalar=45), StubResult(scenes))

    page = await repo.paginate(page=3, page_size=20, filters={"status": SceneStatus.PUBLISHED})

    assert isinstance(page, PageResult)
    assert page.total == 45
    assert page.items == scenes
    assert page.pages == 3
    assert page.has_next is False
    list_sql = compiled(stub.executed[1][0], literal=True)
    assert "LIMIT 20 OFFSET 40" in list_sql


async def test_paginate_skips_item_query_when_no_records() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult(scalar=0))

    page = await repo.paginate(page=1, page_size=20)

    assert page.items == []
    assert page.pages == 0
    assert page.has_next is False
    assert len(stub.executed) == 1, "无数据时应只执行 count 查询"


@pytest.mark.parametrize(("page", "page_size"), [(0, 20), (1, 0), (1, MAX_PAGE_SIZE + 1)])
async def test_paginate_validates_parameters(page: int, page_size: int) -> None:
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.paginate(page=page, page_size=page_size)
    assert excinfo.value.code == ErrorCode.INVALID_PARAM


def test_page_result_pages_calculation() -> None:
    """PageResult.pages 向上取整；page_size<=0 时返回 0（防御非法构造）。"""
    assert PageResult(items=[], total=0, page=1, page_size=20).pages == 0
    assert PageResult(items=[], total=1, page=1, page_size=20).pages == 1
    assert PageResult(items=[], total=20, page=1, page_size=20).pages == 1
    assert PageResult(items=[], total=21, page=2, page_size=20).pages == 2
    assert PageResult(items=[], total=21, page=2, page_size=20).has_next is False
    assert PageResult(items=[], total=41, page=2, page_size=20).has_next is True
    assert PageResult(items=[], total=5, page=1, page_size=0).pages == 0


# ---------- find_one / find_all / delete_where（Repository 契约 3.1） ----------


async def test_find_one_applies_soft_delete_filter_and_condition() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([Scene(scene_name="crossing", scene_type="urban", creator=uuid4())]))

    scene = await repo.find_one(Scene.scene_name == "crossing")

    assert scene is not None
    sql = compiled(stub.executed[0][0])
    assert "scene_svc.scenes.deleted_at IS NULL" in sql
    assert "scene_svc.scenes.scene_name = " in sql


async def test_find_one_can_include_deleted_explicitly() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))

    assert await repo.find_one(include_deleted=True) is None
    assert "deleted_at IS NULL" not in compiled(stub.executed[0][0])


async def test_find_all_applies_order_by_and_limit() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([Scene(scene_name="s1", scene_type="urban", creator=uuid4())]))

    rows = await repo.find_all(Scene.scene_type == "urban", order_by=("-update_time",), limit=5)

    assert len(rows) == 1
    sql = compiled(stub.executed[0][0], literal=True)
    assert "scene_svc.scenes.scene_type = 'urban'" in sql
    assert "ORDER BY scene_svc.scenes.update_time DESC" in sql
    assert "LIMIT 5" in sql


async def test_find_all_falls_back_to_default_order() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult([]))

    await repo.find_all()

    assert "ORDER BY scene_svc.scenes.create_time DESC" in compiled(stub.executed[0][0])


async def test_find_all_rejects_limit_above_page_size_cap() -> None:
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.find_all(limit=MAX_PAGE_SIZE + 1)
    assert excinfo.value.code == ErrorCode.INVALID_PARAM


async def test_find_all_rejects_unknown_order_field() -> None:
    """排序字段非法（疑似注入）必须拒绝为 2001。"""
    repo, _ = scene_repo()
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.find_all(order_by=("scene_name; DROP TABLE scene_svc.scenes --",))
    assert excinfo.value.code == ErrorCode.INVALID_PARAM


async def test_delete_where_returns_rowcount_and_flushes() -> None:
    repo, stub = scene_repo()
    stub.queue(StubResult(rowcount=2))

    deleted = await repo.delete_where(Scene.scene_id == uuid4())

    assert deleted == 2
    assert stub.flushed == 1
    assert compiled(stub.executed[0][0]).startswith("DELETE FROM scene_svc.scenes")


async def test_delete_where_requires_condition() -> None:
    repo, stub = scene_repo()

    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.delete_where()

    assert excinfo.value.code == ErrorCode.INVALID_PARAM
    assert stub.executed == [], "无条件删除必须被拒绝，禁止误删整表"



