"""BaseRepository 通用数据访问层测试。

使用记录型 Stub Session（不依赖数据库）：验证 CRUD、分页、软删除、批量写入的行为与生成 SQL，
并通过 PostgreSQL 方言编译语句断言「软删除过滤 / schema 限定表名 / ON CONFLICT」等关键语义。
"""
from __future__ import annotations

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

    def __init__(self, rows: list[Any] | None = None, scalar: Any = None) -> None:
        self._rows = list(rows or [])
        self._scalar = scalar

    def scalars(self) -> StubResult:
        return self

    def all(self) -> list[Any]:
        return list(self._rows)

    def first(self) -> Any:
        return self._rows[0] if self._rows else None

    def scalar_one(self) -> Any:
        return self._scalar


class StubSession:
    """记录 execute/add/flush 调用的最小 AsyncSession 替身。"""

    def __init__(self, *, flush_error: Exception | None = None) -> None:
        self.executed: list[tuple[Any, Any]] = []
        self.added: list[Any] = []
        self.flushed = 0
        self.rolled_back = 0
        self.deleted: list[Any] = []
        self._queue: list[StubResult] = []
        self._flush_error = flush_error

    def queue(self, *results: StubResult) -> None:
        self._queue.extend(results)

    async def execute(self, statement: Any, params: Any = None) -> StubResult:
        self.executed.append((statement, params))
        return self._queue.pop(0) if self._queue else StubResult()

    def add(self, instance: Any) -> None:
        self.added.append(instance)

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
    orig = type("FakeOrig", (Exception,), {"pgcode": "23505"})("duplicate key value")
    repo, _ = scene_repo(StubSession(flush_error=IntegrityError("insert", {}, orig)))
    with pytest.raises(ResourceAlreadyExistsError) as excinfo:
        await repo.create(scene_name="dup", scene_type="urban", creator=uuid4())
    assert excinfo.value.code == ErrorCode.RESOURCE_ALREADY_EXISTS


async def test_create_translates_check_violation_to_2001() -> None:
    orig = type("FakeOrig", (Exception,), {"pgcode": "23514"})("check constraint violated")
    session = StubSession(flush_error=IntegrityError("insert", {}, orig))
    repo, stub = scene_repo(session)
    with pytest.raises(InvalidParameterError) as excinfo:
        await repo.create(scene_name="bad", scene_type="urban", creator=uuid4())
    assert excinfo.value.code == ErrorCode.INVALID_PARAM
    assert stub.rolled_back == 1


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
    assert compiled(stub.executed[0][0]).startswith("INSERT INTO scene_svc.scenes")


async def test_bulk_create_ignore_conflicts_uses_on_conflict_do_nothing() -> None:
    repo, stub = scene_repo()
    rows = [{"scene_name": "s", "scene_type": "urban", "creator": uuid4()}]
    await repo.bulk_create_ignore_conflicts(rows, conflict_columns=["scene_name"])
    assert "ON CONFLICT (scene_name) DO NOTHING" in compiled(stub.executed[0][0])


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


