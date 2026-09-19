"""Repository 层单元测试（契约：contracts/database/orm-mapping.md 第 3 节）。

不依赖数据库：使用记录型 Stub Session，断言每个 Repository **每个方法**的行为与 SQL 语义
（参数绑定、索引列序、幂等冲突、软删除过滤），并用 PostgreSQL 方言编译语句核验。
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import TracebackType
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import selectinload

from hunter_common.database import REPOSITORY_BY_MODEL
from hunter_common.database.enums import (
    EventLevel,
    EventType,
    MetricModule,
    OtaStatus,
    OtaTaskStatus,
    OtaVersionStatus,
    PermissionAction,
    PermissionResource,
    SceneStatus,
    VehicleStatus,
)
from hunter_common.database.models import (
    AlgorithmMetric,
    Event,
    OtaRecord,
    OtaTask,
    OtaVersion,
    Permission,
    Role,
    RolePermission,
    Scene,
    User,
    UserRole,
    Vehicle,
    VehicleTelemetry,
)
from hunter_common.database.repositories import (
    EVENT_IDEMPOTENCY_COLUMNS,
    METRIC_CONFLICT_COLUMNS,
    TELEMETRY_CONFLICT_COLUMNS,
)
from hunter_common.database.repositories.analytics import AlgorithmMetricRepository
from hunter_common.database.repositories.collector import (
    EventRepository,
    VehicleTelemetryRepository,
)
from hunter_common.database.repositories.core import (
    PermissionRepository,
    RolePermissionRepository,
    RoleRepository,
    UserRepository,
    UserRoleRepository,
    VehicleRepository,
)
from hunter_common.database.repositories.ota import (
    OtaRecordRepository,
    OtaTaskRepository,
    OtaVersionRepository,
)
from hunter_common.database.repositories.scene import SceneRepository
from hunter_common.database.repository import BaseRepository
from hunter_common.exceptions import InvalidParameterError


class StubResult:
    """模拟 SQLAlchemy Result：scalars()/all()/first()/scalar_one()/rowcount。"""

    def __init__(
        self, rows: list[Any] | None = None, scalar: Any = None, rowcount: int = 0
    ) -> None:
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
    """模拟 ``AsyncSession.begin_nested()``（SAVEPOINT）：块内异常只回滚该 SAVEPOINT。"""

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

    def __init__(self) -> None:
        self.executed: list[tuple[Any, Any]] = []
        self.added: list[Any] = []
        self.flushed = 0
        self.savepoints = 0
        self.savepoint_rollbacks = 0
        self.expunged: list[Any] = []
        self._queue: list[StubResult | Exception] = []

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

    async def rollback(self) -> None:
        pass


def compiled(statement: Any, *, literal: bool = False) -> str:
    """编译为单行 PostgreSQL SQL 文本（literal=True 内联可渲染字面量）。"""
    compile_kwargs = {"literal_binds": True} if literal else {}
    sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs=compile_kwargs))
    return " ".join(sql.split())


def repo_pair(repository_cls: type[Any]) -> tuple[Any, StubSession]:
    """构造 (repository, stub_session)。"""
    stub = StubSession()
    return repository_cls(stub), stub


def last_sql(stub: StubSession, index: int = -1) -> str:
    return compiled(stub.executed[index][0])


# ---------- 注册表与幂等列契约 ----------


@pytest.mark.parametrize("model", list(REPOSITORY_BY_MODEL), ids=lambda m: m.__name__)
def test_repository_registry_binds_model_and_primary_key(model: type[Any]) -> None:
    """每个模型对应一个 Repository，且其 model/pk_name 与 ORM 定义一致。"""
    repository_cls = REPOSITORY_BY_MODEL[model]
    repo, _stub = repo_pair(repository_cls)
    assert isinstance(repo, BaseRepository)
    assert repo.model is model
    primary_keys = {column.name for column in model.__table__.primary_key.columns}
    assert repo.pk_name in primary_keys


def test_conflict_and_idempotency_columns_match_ddl() -> None:
    """幂等冲突列必须等于 DDL 主键 / 唯一索引列（参数绑定列名，禁止臆造）。"""
    assert TELEMETRY_CONFLICT_COLUMNS == ("time", "vehicle_id")
    assert METRIC_CONFLICT_COLUMNS == ("time", "vehicle_id", "module", "metric_name")
    assert EVENT_IDEMPOTENCY_COLUMNS == ("vehicle_id", "event_type", "event_time")
    assert TELEMETRY_CONFLICT_COLUMNS == tuple(
        column.name for column in VehicleTelemetry.__table__.primary_key.columns
    )
    assert METRIC_CONFLICT_COLUMNS == tuple(
        column.name for column in AlgorithmMetric.__table__.primary_key.columns
    )
    unique_index = next(
        index
        for index in Event.__table__.indexes
        if index.name == "uq_events_vehicle_type_time"
    )
    assert EVENT_IDEMPOTENCY_COLUMNS == tuple(column.name for column in unique_index.columns)


# ---------- VehicleRepository ----------


async def test_vehicle_get_by_device_cert_sn_uses_index_column() -> None:
    repo, stub = repo_pair(VehicleRepository)
    stub.queue(StubResult([Vehicle(vehicle_id="HUNTER-001", vehicle_name="一号车")]))

    vehicle = await repo.get_by_device_cert_sn("SN-0001")

    assert vehicle is not None and vehicle.vehicle_id == "HUNTER-001"
    sql = last_sql(stub)
    assert "vehicle_svc.vehicles.device_cert_sn = " in sql
    assert "SN-0001" not in sql, "查询值必须参数绑定，禁止拼接进 SQL"


async def test_vehicle_list_by_status_uses_default_ordering_and_limit() -> None:
    repo, stub = repo_pair(VehicleRepository)
    stub.queue(StubResult([]))

    assert await repo.list_by_status(VehicleStatus.AUTO_DRIVING, limit=50) == []

    sql = compiled(stub.executed[0][0], literal=True)
    assert "vehicle_svc.vehicles.status = 'auto_driving'" in sql
    assert "ORDER BY vehicle_svc.vehicles.last_online_time DESC NULLS LAST" in sql
    assert "vehicle_svc.vehicles.vehicle_id ASC" in sql
    assert "LIMIT 50" in sql


async def test_vehicle_list_by_status_rejects_over_max_limit() -> None:
    repo, _stub = repo_pair(VehicleRepository)
    with pytest.raises(InvalidParameterError):
        await repo.list_by_status(VehicleStatus.FAULT, limit=201)


async def test_vehicle_update_status_writes_status_and_last_online() -> None:
    """单条 UPDATE ... RETURNING：原子更新、1 次往返（无"先查后写"竞态）。"""
    repo, stub = repo_pair(VehicleRepository)
    vehicle = Vehicle(vehicle_id="HUNTER-001", vehicle_name="一号车")
    seen = datetime(2026, 9, 19, 8, 0, tzinfo=UTC)
    stub.queue(StubResult([vehicle]))

    updated = await repo.update_status(
        "HUNTER-001", VehicleStatus.ONLINE_IDLE, last_online_time=seen
    )

    assert updated is vehicle
    assert len(stub.executed) == 1, "必须是单条 UPDATE ... RETURNING"
    sql = compiled(stub.executed[0][0], literal=True)
    assert sql.startswith("UPDATE vehicle_svc.vehicles SET status='online_idle'")
    assert "last_online_time" in sql
    assert "vehicle_svc.vehicles.vehicle_id = " in sql
    assert "RETURNING" in sql
    assert stub.flushed == 0, "Core UPDATE 不触发 ORM flush"


async def test_vehicle_update_status_omits_last_online_when_not_provided() -> None:
    repo, stub = repo_pair(VehicleRepository)
    stub.queue(StubResult([]))

    assert await repo.update_status("HUNTER-001", VehicleStatus.OFFLINE) is None

    sql = compiled(stub.executed[0][0], literal=True)
    assert "last_online_time" not in sql.split("WHERE")[0], "未提供时不得写入该列"


async def test_vehicle_update_status_returns_none_for_missing_vehicle() -> None:
    repo, stub = repo_pair(VehicleRepository)
    stub.queue(StubResult([]))

    assert await repo.update_status("HUNTER-404", VehicleStatus.OFFLINE) is None
    assert len(stub.executed) == 1


# ---------- UserRepository ----------


async def test_user_get_by_username_and_email_use_unique_indexes() -> None:
    repo, stub = repo_pair(UserRepository)
    stub.queue(StubResult([User(username="admin", password_hash="hash")]))
    assert await repo.get_by_username("admin") is not None
    assert "user_svc.users.username = " in last_sql(stub)

    stub.queue(StubResult([User(username="admin", password_hash="hash")]))
    assert await repo.get_by_email("ADMIN@HUNTER.DEV") is not None
    sql = last_sql(stub)
    assert "lower(user_svc.users.email) = " in sql
    assert "ADMIN@HUNTER.DEV" not in sql


async def test_user_list_role_codes_joins_user_roles_and_filters_enabled() -> None:
    repo, stub = repo_pair(UserRepository)
    stub.queue(StubResult(rows=["admin", "operator"]))

    codes = await repo.list_role_codes(uuid4())

    assert codes == ["admin", "operator"]
    sql = last_sql(stub)
    assert "JOIN user_svc.user_roles" in sql
    assert "user_svc.roles.status = " in sql
    assert "ORDER BY user_svc.roles.role_code ASC" in sql


async def test_user_list_permission_codes_joins_full_rbac_chain() -> None:
    repo, stub = repo_pair(UserRepository)
    stub.queue(StubResult(rows=["scene:create"]))

    codes = await repo.list_permission_codes(uuid4())

    assert codes == ["scene:create"]
    sql = last_sql(stub)
    assert "JOIN user_svc.role_permissions" in sql
    assert "JOIN user_svc.roles" in sql
    assert "JOIN user_svc.user_roles" in sql
    assert "DISTINCT" in sql


async def test_user_touch_last_login_sets_timestamp() -> None:
    """单条 UPDATE + rowcount 判定（登录热路径 1 次往返）。"""
    repo, stub = repo_pair(UserRepository)
    at = datetime(2026, 9, 19, 9, 30, tzinfo=UTC)
    stub.queue(StubResult(rowcount=1))

    assert await repo.touch_last_login(user_id=uuid4(), at=at) is True

    assert len(stub.executed) == 1
    sql = compiled(stub.executed[0][0], literal=True)
    assert sql.startswith("UPDATE user_svc.users SET last_login_time=")


async def test_user_touch_last_login_returns_false_for_missing_user() -> None:
    repo, stub = repo_pair(UserRepository)
    stub.queue(StubResult(rowcount=0))

    assert await repo.touch_last_login(user_id=uuid4()) is False
    assert len(stub.executed) == 1


# ---------- RoleRepository / PermissionRepository ----------


async def test_role_get_by_role_code_uses_unique_index() -> None:
    repo, stub = repo_pair(RoleRepository)
    stub.queue(StubResult([Role(role_code="admin", role_name="管理员")]))

    role = await repo.get_by_role_code("admin")

    assert role is not None and role.role_code == "admin"
    sql = last_sql(stub)
    assert "user_svc.roles.role_code = " in sql
    assert "admin" not in sql


async def test_role_list_enabled_filters_status_and_orders_by_code() -> None:
    repo, stub = repo_pair(RoleRepository)
    stub.queue(StubResult([]))

    assert await repo.list_enabled() == []

    sql = compiled(stub.executed[0][0], literal=True)
    assert "user_svc.roles.status = 'enabled'" in sql
    assert "ORDER BY user_svc.roles.role_code ASC" in sql


async def test_permission_get_by_permission_code_uses_unique_index() -> None:
    repo, stub = repo_pair(PermissionRepository)
    stub.queue(
        StubResult(
            [
                Permission(
                    permission_code="scene:create",
                    permission_name="创建场景",
                    resource=PermissionResource.SCENE,
                    action=PermissionAction.CREATE,
                )
            ]
        )
    )

    permission = await repo.get_by_permission_code("scene:create")

    assert permission is not None and permission.action is PermissionAction.CREATE
    assert "user_svc.permissions.permission_code = " in last_sql(stub)


async def test_permission_list_by_resource_orders_by_resource_action() -> None:
    repo, stub = repo_pair(PermissionRepository)
    stub.queue(StubResult([]))

    assert await repo.list_by_resource(PermissionResource.OTA) == []

    sql = compiled(stub.executed[0][0], literal=True)
    assert "user_svc.permissions.resource = 'ota'" in sql
    assert "ORDER BY user_svc.permissions.resource ASC, user_svc.permissions.action ASC" in sql


# ---------- UserRoleRepository ----------


async def test_user_role_get_pair_queries_composite_key() -> None:
    repo, stub = repo_pair(UserRoleRepository)
    user_id, role_id = uuid4(), uuid4()
    stub.queue(StubResult([UserRole(user_id=user_id, role_id=role_id)]))

    row = await repo.get_pair(user_id, role_id)

    assert row is not None and row.user_id == user_id
    sql = last_sql(stub)
    assert "user_svc.user_roles.user_id = " in sql
    assert "user_svc.user_roles.role_id = " in sql


async def test_user_role_list_role_ids_selects_single_column() -> None:
    repo, stub = repo_pair(UserRoleRepository)
    role_id = uuid4()
    stub.queue(StubResult(rows=[role_id]))

    assert await repo.list_role_ids(uuid4()) == [role_id]
    sql = last_sql(stub)
    assert "SELECT user_svc.user_roles.role_id" in sql
    assert "ORDER BY user_svc.user_roles.role_id ASC" in sql


async def test_user_role_link_is_idempotent_and_reports_insertion() -> None:
    repo, stub = repo_pair(UserRoleRepository)
    user_id, role_id = uuid4(), uuid4()
    stub.queue(StubResult(rowcount=1))

    assert await repo.link(user_id, role_id) is True
    sql = last_sql(stub)
    assert "INSERT INTO user_svc.user_roles" in sql
    assert "ON CONFLICT (user_id, role_id) DO NOTHING" in sql

    stub.queue(StubResult(rowcount=0))
    assert await repo.link(user_id, role_id) is False, "重复绑定不应报错（幂等）"


async def test_user_role_unlink_deletes_pair_and_returns_rowcount() -> None:
    repo, stub = repo_pair(UserRoleRepository)
    stub.queue(StubResult(rowcount=1))

    assert await repo.unlink(uuid4(), uuid4()) == 1
    sql = last_sql(stub)
    assert sql.startswith("DELETE FROM user_svc.user_roles")
    assert "user_svc.user_roles.role_id = " in sql
    assert stub.flushed == 1


# ---------- RolePermissionRepository ----------


async def test_role_permission_get_pair_queries_composite_key() -> None:
    repo, stub = repo_pair(RolePermissionRepository)
    role_id, permission_id = uuid4(), uuid4()
    stub.queue(StubResult([RolePermission(role_id=role_id, permission_id=permission_id)]))

    row = await repo.get_pair(role_id, permission_id)

    assert row is not None and row.permission_id == permission_id
    sql = last_sql(stub)
    assert "user_svc.role_permissions.role_id = " in sql
    assert "user_svc.role_permissions.permission_id = " in sql


async def test_role_permission_list_permission_ids_selects_single_column() -> None:
    repo, stub = repo_pair(RolePermissionRepository)
    permission_id = uuid4()
    stub.queue(StubResult(rows=[permission_id]))

    assert await repo.list_permission_ids(uuid4()) == [permission_id]
    assert "SELECT user_svc.role_permissions.permission_id" in last_sql(stub)


async def test_role_permission_link_is_idempotent_and_reports_insertion() -> None:
    repo, stub = repo_pair(RolePermissionRepository)
    role_id, permission_id = uuid4(), uuid4()
    stub.queue(StubResult(rowcount=1))

    assert await repo.link(role_id, permission_id) is True
    assert (
        "ON CONFLICT (role_id, permission_id) DO NOTHING" in last_sql(stub)
    )

    stub.queue(StubResult(rowcount=0))
    assert await repo.link(role_id, permission_id) is False


async def test_role_permission_unlink_deletes_pair_and_returns_rowcount() -> None:
    repo, stub = repo_pair(RolePermissionRepository)
    stub.queue(StubResult(rowcount=2))

    assert await repo.unlink(uuid4(), uuid4()) == 2
    assert last_sql(stub).startswith("DELETE FROM user_svc.role_permissions")


# ---------- SceneRepository ----------


async def test_scene_get_by_scene_name_filters_soft_deleted() -> None:
    repo, stub = repo_pair(SceneRepository)
    stub.queue(StubResult([Scene(scene_name="crossing-01", scene_type="urban", creator=uuid4())]))

    scene = await repo.get_by_scene_name("crossing-01")

    assert scene is not None and scene.scene_name == "crossing-01"
    sql = last_sql(stub)
    assert "scene_svc.scenes.scene_name = " in sql
    assert "scene_svc.scenes.deleted_at IS NULL" in sql


async def test_scene_paginate_excludes_deleted_and_orders_by_create_time() -> None:
    repo, stub = repo_pair(SceneRepository)
    stub.queue(StubResult(scalar=3), StubResult([]))

    page = await repo.paginate(page=1, page_size=20, filters={"status": SceneStatus.PUBLISHED})

    assert page.total == 3
    assert page.items == []
    count_sql = compiled(stub.executed[0][0], literal=True)
    assert "scene_svc.scenes.deleted_at IS NULL" in count_sql
    assert "scene_svc.scenes.status = 'published'" in count_sql
    list_sql_text = compiled(stub.executed[1][0], literal=True)
    assert "ORDER BY scene_svc.scenes.create_time DESC" in list_sql_text
    assert "LIMIT 20" in list_sql_text


async def test_scene_soft_delete_writes_deleted_at_only() -> None:
    repo, stub = repo_pair(SceneRepository)
    scene = Scene(scene_name="crossing-01", scene_type="urban", creator=uuid4())

    await repo.soft_delete(scene)

    assert scene.deleted_at is not None
    assert stub.flushed == 1
    assert stub.executed == [], "软删除不应发出 DELETE 语句"


# ---------- OtaVersionRepository ----------


async def test_ota_version_lookup_by_code_and_name() -> None:
    repo, stub = repo_pair(OtaVersionRepository)
    stub.queue(StubResult([OtaVersion(version_code=12)]))
    assert await repo.get_by_version_code(12) is not None
    assert "ota_svc.ota_versions.version_code = " in last_sql(stub)

    stub.queue(StubResult([OtaVersion(version_name="V1.2.0")]))
    assert await repo.get_by_version_name("V1.2.0") is not None
    sql = last_sql(stub)
    assert "ota_svc.ota_versions.version_name = " in sql
    assert "V1.2.0" not in sql


async def test_ota_version_max_version_code_applies_status_and_model_filters() -> None:
    repo, stub = repo_pair(OtaVersionRepository)
    stub.queue(StubResult(scalar=42))

    value = await repo.max_version_code(
        status=OtaVersionStatus.PUBLISHED, applicable_models=["HUNTER_SE"]
    )

    assert value == 42
    sql = compiled(stub.executed[0][0], literal=True)
    assert "max(ota_svc.ota_versions.version_code)" in sql
    assert "ota_svc.ota_versions.status = 'published'" in sql
    assert "&&" in sql, "适用车型过滤必须走 TEXT[] 重叠匹配（GIN 索引）"


async def test_ota_version_max_version_code_returns_none_when_table_empty() -> None:
    repo, stub = repo_pair(OtaVersionRepository)
    stub.queue(StubResult(scalar=None))

    assert await repo.max_version_code() is None
    assert "WHERE" not in last_sql(stub)


async def test_ota_version_default_order_matches_contract_nulls_last() -> None:
    """契约 openapi/ota-service.yaml：``release_time DESC NULLS LAST, version_code DESC``。

    若退化为 ``DESC``（= NULLS FIRST）会使未发布草稿排在最前，且失去
    idx_ota_versions_status_release_time 的索引排序能力（P95 ≤ 200ms）。
    """
    repo, stub = repo_pair(OtaVersionRepository)
    stub.queue(StubResult([]))

    await repo.list(limit=20)

    sql = compiled(stub.executed[0][0], literal=True)
    assert "ORDER BY ota_svc.ota_versions.release_time DESC NULLS LAST" in sql
    assert "ota_svc.ota_versions.version_code DESC" in sql


# ---------- OtaTaskRepository ----------


async def test_ota_task_list_by_status_filters_and_orders() -> None:
    repo, stub = repo_pair(OtaTaskRepository)
    stub.queue(StubResult([]))

    assert await repo.list_by_status(OtaTaskStatus.RUNNING, limit=10) == []

    sql = compiled(stub.executed[0][0], literal=True)
    assert "ota_svc.ota_tasks.status = 'running'" in sql
    assert "ORDER BY ota_svc.ota_tasks.create_time DESC" in sql
    assert "LIMIT 10" in sql


async def test_ota_task_list_by_target_version_uses_index_column() -> None:
    repo, stub = repo_pair(OtaTaskRepository)
    stub.queue(StubResult([]))

    assert await repo.list_by_target_version(uuid4()) == []

    sql = last_sql(stub)
    assert "ota_svc.ota_tasks.target_version_id = " in sql
    assert "ORDER BY ota_svc.ota_tasks.create_time DESC" in sql


# ---------- OtaRecordRepository ----------


async def test_ota_record_get_by_task_vehicle_uses_unique_index() -> None:
    repo, stub = repo_pair(OtaRecordRepository)
    task_id = uuid4()
    stub.queue(StubResult([OtaRecord(task_id=task_id, vehicle_id="HUNTER-001")]))

    record = await repo.get_by_task_vehicle(task_id, "HUNTER-001")

    assert record is not None and record.vehicle_id == "HUNTER-001"
    sql = last_sql(stub)
    assert "ota_svc.ota_records.task_id = " in sql
    assert "ota_svc.ota_records.vehicle_id = " in sql


async def test_ota_record_list_inflight_defaults_to_active_statuses() -> None:
    repo, stub = repo_pair(OtaRecordRepository)
    stub.queue(StubResult([]))

    assert await repo.list_inflight(limit=5) == []

    sql = compiled(stub.executed[0][0], literal=True)
    assert "IN ('DOWNLOAD', 'INSTALL', 'PENDING', 'ROLLBACK', 'TEST')" in sql
    for terminal in ("'IDLE'", "'SUCCESS'", "'ROLLED_BACK'", "'FAILED'"):
        assert terminal not in sql, "终态不得进入进行中查询"
    assert "ORDER BY ota_svc.ota_records.status ASC" in sql
    assert "ota_svc.ota_records.start_time DESC NULLS LAST" in sql
    assert "ota_svc.ota_records.record_id DESC" in sql
    assert "LIMIT 5" in sql


async def test_ota_record_list_inflight_accepts_explicit_statuses() -> None:
    repo, stub = repo_pair(OtaRecordRepository)
    stub.queue(StubResult([]))

    await repo.list_inflight(statuses=[OtaStatus.DOWNLOAD])

    assert "IN ('DOWNLOAD')" in compiled(stub.executed[0][0], literal=True)


async def test_ota_record_list_by_vehicle_orders_by_start_time_desc() -> None:
    repo, stub = repo_pair(OtaRecordRepository)
    stub.queue(StubResult([]))

    await repo.list_by_vehicle("HUNTER-001", limit=3)

    sql = compiled(stub.executed[0][0], literal=True)
    assert "ota_svc.ota_records.vehicle_id = " in sql
    assert "ORDER BY ota_svc.ota_records.start_time DESC NULLS LAST" in sql
    assert "ota_svc.ota_records.record_id DESC" in sql, "次级键保证分页稳定（契约）"
    assert "LIMIT 3" in sql


async def test_ota_record_status_counts_groups_by_status() -> None:
    repo, stub = repo_pair(OtaRecordRepository)
    stub.queue(StubResult(rows=[(OtaStatus.SUCCESS, 9), (OtaStatus.FAILED, 1)]))

    counts = await repo.status_counts(uuid4())

    assert counts == {OtaStatus.SUCCESS: 9, OtaStatus.FAILED: 1}
    sql = last_sql(stub)
    assert "count(*)" in sql.lower()
    assert "GROUP BY ota_svc.ota_records.status" in sql


# ---------- EventRepository ----------


async def test_event_get_by_vehicle_type_time_uses_idempotency_unique_key() -> None:
    repo, stub = repo_pair(EventRepository)
    event_time = datetime(2026, 9, 19, 10, 0, tzinfo=UTC)
    stub.queue(
        StubResult(
            [
                Event(
                    vehicle_id="HUNTER-001",
                    event_type=EventType.OVER_SPEED,
                    event_level=EventLevel.CRITICAL,
                    event_time=event_time,
                )
            ]
        )
    )

    event = await repo.get_by_vehicle_type_time("HUNTER-001", EventType.OVER_SPEED, event_time)

    assert event is not None
    sql = last_sql(stub)
    assert "data_collector.events.vehicle_id = " in sql
    assert "data_collector.events.event_type = " in sql
    assert "data_collector.events.event_time = " in sql


async def test_event_list_by_vehicle_applies_time_window_and_default_order() -> None:
    repo, stub = repo_pair(EventRepository)
    stub.queue(StubResult([]))

    await repo.list_by_vehicle(
        "HUNTER-001",
        start_time=datetime(2026, 9, 19, 0, 0, tzinfo=UTC),
        end_time=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        limit=50,
    )

    sql = compiled(stub.executed[0][0], literal=True)
    assert "data_collector.events.vehicle_id = " in sql
    assert "data_collector.events.event_time >= " in sql
    assert "data_collector.events.event_time <= " in sql
    assert "ORDER BY data_collector.events.event_time DESC" in sql
    assert "data_collector.events.event_id DESC" in sql
    assert "LIMIT 50" in sql


async def test_event_list_unacknowledged_filters_flag_and_level() -> None:
    repo, stub = repo_pair(EventRepository)
    stub.queue(StubResult([]))

    await repo.list_unacknowledged(event_level=EventLevel.CRITICAL, limit=20)

    sql = compiled(stub.executed[0][0], literal=True)
    assert "data_collector.events.acknowledged IS false" in sql
    assert "data_collector.events.event_level = 'critical'" in sql
    assert "LIMIT 20" in sql


async def test_event_acknowledge_issues_single_conditional_update() -> None:
    """原子确认：单条 UPDATE（WHERE 带 acknowledged IS FALSE），1 次往返。"""
    repo, stub = repo_pair(EventRepository)
    user_id = uuid4()
    at = datetime(2026, 9, 19, 11, 0, tzinfo=UTC)
    stub.queue(StubResult(rowcount=1))

    assert await repo.acknowledge(1024, acknowledged_by=user_id, at=at) is True

    assert len(stub.executed) == 1
    sql = compiled(stub.executed[0][0], literal=True)
    assert sql.startswith("UPDATE data_collector.events SET")
    assert "data_collector.events.event_id = " in sql
    assert "data_collector.events.acknowledged IS false" in sql
    assert "acknowledged_by" in sql and "acknowledge_time" in sql


async def test_event_acknowledge_is_idempotent_and_keeps_first_audit() -> None:
    """已确认事件不覆盖首次审计：条件 UPDATE 未命中 → 轻量 EXISTS 判定存在性。"""
    repo, stub = repo_pair(EventRepository)
    stub.queue(StubResult(rowcount=0), StubResult([1]))

    assert await repo.acknowledge(1024, acknowledged_by=uuid4()) is True

    assert len(stub.executed) == 2
    update_sql = compiled(stub.executed[0][0], literal=True)
    assert "acknowledged IS false" in update_sql, "不满足首确认条件的行不会被 UPDATE"
    exists_sql = compiled(stub.executed[1][0]).lower()
    assert exists_sql.startswith("select") and "limit" in exists_sql


async def test_event_acknowledge_returns_false_for_missing_event() -> None:
    repo, stub = repo_pair(EventRepository)
    stub.queue(StubResult(rowcount=0), StubResult([]))

    assert await repo.acknowledge(404, acknowledged_by=uuid4()) is False
    assert len(stub.executed) == 2


async def test_event_insert_events_is_idempotent_on_unique_key() -> None:
    """events 写入幂等：ON CONFLICT (vehicle_id, event_type, event_time) DO NOTHING。"""
    repo, stub = repo_pair(EventRepository)
    rows = [
        {
            "vehicle_id": "HUNTER-001",
            "event_type": EventType.OVER_SPEED,
            "event_level": EventLevel.CRITICAL,
            "event_time": datetime(2026, 9, 19, 10, 0, tzinfo=UTC),
            "data_json": {},
        }
    ]

    assert await repo.insert_events(rows) == 1

    sql = last_sql(stub)
    assert "INSERT INTO data_collector.events" in sql
    assert "ON CONFLICT (vehicle_id, event_type, event_time) DO NOTHING" in sql
    assert stub.executed[0][1] == [dict(row) for row in rows], "executemany 传行参数（禁止拼接）"


# ---------- VehicleTelemetryRepository ----------


async def test_telemetry_insert_points_uses_executemany_and_on_conflict() -> None:
    repo, stub = repo_pair(VehicleTelemetryRepository)
    rows = [
        {"time": datetime(2026, 9, 19, 0, 0, tzinfo=UTC), "vehicle_id": "HUNTER-001", "seq": 1},
        {"time": datetime(2026, 9, 19, 0, 0, 1, tzinfo=UTC), "vehicle_id": "HUNTER-001", "seq": 2},
    ]

    assert await repo.insert_points(rows) == 2

    sql = last_sql(stub)
    assert "INSERT INTO data_collector.vehicle_telemetry" in sql
    assert "ON CONFLICT (time, vehicle_id) DO NOTHING" in sql
    assert stub.executed[0][1] == [dict(row) for row in rows], (
        "批量写入必须一次 execute 传多行参数（executemany），且不与入参共享对象"
    )
    assert stub.savepoints == 1, "每个分片一个 SAVEPOINT"


async def test_telemetry_insert_points_skips_empty_input() -> None:
    repo, stub = repo_pair(VehicleTelemetryRepository)

    assert await repo.insert_points([]) == 0
    assert stub.executed == [], "空批次不得发出 SQL"


async def test_telemetry_get_point_queries_composite_primary_key() -> None:
    repo, stub = repo_pair(VehicleTelemetryRepository)
    at = datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
    stub.queue(StubResult([VehicleTelemetry(vehicle_id="HUNTER-001", time=at)]))

    point = await repo.get_point("HUNTER-001", at)

    assert point is not None
    sql = last_sql(stub)
    assert "data_collector.vehicle_telemetry.vehicle_id = " in sql
    assert "data_collector.vehicle_telemetry.time = " in sql


async def test_telemetry_list_points_applies_time_window_and_order() -> None:
    repo, stub = repo_pair(VehicleTelemetryRepository)
    stub.queue(StubResult([]))

    await repo.list_points(
        "HUNTER-001",
        start_time=datetime(2026, 9, 19, 0, 0, tzinfo=UTC),
        end_time=datetime(2026, 9, 19, 1, 0, tzinfo=UTC),
        limit=100,
    )

    sql = compiled(stub.executed[0][0], literal=True)
    assert "data_collector.vehicle_telemetry.time >= " in sql
    assert "data_collector.vehicle_telemetry.time <= " in sql
    assert "ORDER BY data_collector.vehicle_telemetry.time DESC" in sql
    assert "LIMIT 100" in sql


async def test_telemetry_latest_point_orders_desc_and_returns_first() -> None:
    repo, stub = repo_pair(VehicleTelemetryRepository)
    point = VehicleTelemetry(vehicle_id="HUNTER-001")
    stub.queue(StubResult([point]))

    assert await repo.latest_point("HUNTER-001") is point
    sql = compiled(stub.executed[0][0], literal=True)
    assert "ORDER BY data_collector.vehicle_telemetry.time DESC" in sql
    assert "LIMIT 1" in sql

    stub.queue(StubResult([]))
    assert await repo.latest_point("HUNTER-404") is None


async def test_telemetry_purge_before_deletes_in_batches() -> None:
    """物理清理分批执行：每批一个短事务（避免长事务/WAL 膨胀），末批不足即停止。"""
    repo, stub = repo_pair(VehicleTelemetryRepository)
    stub.queue(StubResult(rowcount=5000), StubResult(rowcount=1200))

    removed = await repo.purge_before(datetime(2026, 6, 1, tzinfo=UTC), batch_size=5000)

    assert removed == 6200
    assert len(stub.executed) == 2, "5000 满批后继续，第二批 1200 < 5000 即结束"
    for stmt, _params in stub.executed:
        sql = compiled(stmt, literal=True)
        assert sql.startswith("DELETE FROM data_collector.vehicle_telemetry")
        assert "ctid IN (SELECT ctid" in sql
        assert "LIMIT 5000" in sql


async def test_telemetry_purge_before_respects_max_rows() -> None:
    repo, stub = repo_pair(VehicleTelemetryRepository)
    stub.queue(StubResult(rowcount=100))

    assert await repo.purge_before(
        datetime(2026, 6, 1, tzinfo=UTC), batch_size=5000, max_rows=100
    ) == 100
    assert len(stub.executed) == 1
    assert "LIMIT 100" in compiled(stub.executed[0][0], literal=True)


async def test_telemetry_purge_before_rejects_bad_batch_params() -> None:
    repo, _stub = repo_pair(VehicleTelemetryRepository)
    with pytest.raises(InvalidParameterError):
        await repo.purge_before(datetime(2026, 6, 1, tzinfo=UTC), batch_size=0)
    with pytest.raises(InvalidParameterError):
        await repo.purge_before(datetime(2026, 6, 1, tzinfo=UTC), max_rows=0)


async def test_telemetry_series_read_allows_points_above_page_limit() -> None:
    """时序序列读取上限放宽到 MAX_SERIES_POINTS（轨迹回放），但仍有硬上限。"""
    repo, stub = repo_pair(VehicleTelemetryRepository)
    stub.queue(StubResult([]))

    await repo.list_points(
        "HUNTER-001",
        start_time=datetime(2026, 9, 19, tzinfo=UTC),
        end_time=datetime(2026, 9, 20, tzinfo=UTC),
        limit=2000,
    )
    assert "LIMIT 2000" in compiled(stub.executed[0][0], literal=True)

    with pytest.raises(InvalidParameterError):
        await repo.list_points("HUNTER-001", limit=10001)


async def test_telemetry_repository_rejects_single_column_primary_key_semantics() -> None:
    """复合主键保护：基类 get / get_or_raise / hard_delete 必须显式拒绝（防止跨车辆误命中）。"""
    repo, stub = repo_pair(VehicleTelemetryRepository)
    point = VehicleTelemetry(vehicle_id="HUNTER-001", time=datetime(2026, 9, 19, tzinfo=UTC))

    assert repo.pk_names == ("time", "vehicle_id")
    assert repo.is_composite_pk is True

    with pytest.raises(NotImplementedError):
        await repo.get(datetime(2026, 9, 19, tzinfo=UTC))
    with pytest.raises(NotImplementedError):
        await repo.get_or_raise(datetime(2026, 9, 19, tzinfo=UTC))
    with pytest.raises(NotImplementedError):
        await repo.hard_delete(point)
    assert stub.executed == [], "被拒绝的操作不得发出任何 SQL"


# ---------- AlgorithmMetricRepository ----------


async def test_metric_insert_metrics_is_idempotent_on_primary_key() -> None:
    repo, stub = repo_pair(AlgorithmMetricRepository)
    rows = [
        {
            "time": datetime(2026, 9, 19, tzinfo=UTC),
            "vehicle_id": "HUNTER-001",
            "module": MetricModule.PERCEPTION,
            "metric_name": "detection_latency_ms",
            "metric_value": 85.0,
        }
    ]

    assert await repo.insert_metrics(rows) == 1

    sql = last_sql(stub)
    assert "INSERT INTO data_analytics.algorithm_metrics" in sql
    assert "ON CONFLICT (time, vehicle_id, module, metric_name) DO NOTHING" in sql


async def test_metric_list_series_applies_module_metric_and_time_filters() -> None:
    repo, stub = repo_pair(AlgorithmMetricRepository)
    stub.queue(StubResult([]))

    await repo.list_series(
        "HUNTER-001",
        module=MetricModule.PLANNING,
        metric_name="planning_latency_ms",
        start_time=datetime(2026, 9, 19, tzinfo=UTC),
        end_time=datetime(2026, 9, 20, tzinfo=UTC),
        limit=30,
    )

    sql = compiled(stub.executed[0][0], literal=True)
    assert "data_analytics.algorithm_metrics.vehicle_id = " in sql
    assert "data_analytics.algorithm_metrics.module = 'planning'" in sql
    assert "data_analytics.algorithm_metrics.metric_name = " in sql
    assert "data_analytics.algorithm_metrics.time >= " in sql
    assert "data_analytics.algorithm_metrics.time <= " in sql
    assert "ORDER BY data_analytics.algorithm_metrics.time DESC" in sql
    assert "LIMIT 30" in sql


async def test_metric_latest_returns_newest_point_or_none() -> None:
    repo, stub = repo_pair(AlgorithmMetricRepository)
    metric = AlgorithmMetric(vehicle_id="HUNTER-001", metric_name="control_latency_ms")
    stub.queue(StubResult([metric]))

    latest = await repo.latest(
        "HUNTER-001", module=MetricModule.CONTROL, metric_name="control_latency_ms"
    )

    assert latest is metric
    sql = compiled(stub.executed[0][0], literal=True)
    assert "ORDER BY data_analytics.algorithm_metrics.time DESC" in sql
    assert "LIMIT 1" in sql

    stub.queue(StubResult([]))
    assert (
        await repo.latest(
            "HUNTER-404", module=MetricModule.CONTROL, metric_name="control_latency_ms"
        )
        is None
    )


async def test_metric_series_read_allows_points_above_page_limit() -> None:
    """指标趋势序列读取上限放宽到 MAX_SERIES_POINTS，超限仍抛 2001。"""
    repo, stub = repo_pair(AlgorithmMetricRepository)
    stub.queue(StubResult([]))

    await repo.list_series(
        "HUNTER-001",
        start_time=datetime(2026, 9, 19, tzinfo=UTC),
        end_time=datetime(2026, 9, 20, tzinfo=UTC),
        limit=5000,
    )
    assert "LIMIT 5000" in compiled(stub.executed[0][0], literal=True)

    with pytest.raises(InvalidParameterError):
        await repo.list_series("HUNTER-001", limit=10001)


async def test_metric_repository_rejects_single_column_primary_key_semantics() -> None:
    repo, stub = repo_pair(AlgorithmMetricRepository)
    metric = AlgorithmMetric(
        vehicle_id="HUNTER-001", module=MetricModule.PERCEPTION, metric_name="fps"
    )
    assert repo.pk_names == ("time", "vehicle_id", "module", "metric_name")

    with pytest.raises(NotImplementedError):
        await repo.get(datetime(2026, 9, 19, tzinfo=UTC))
    with pytest.raises(NotImplementedError):
        await repo.hard_delete(metric)
    assert stub.executed == []


# ---------- 显式加载选项（raise_on_sql 关系）----------


async def test_find_all_mounts_explicit_loader_options() -> None:
    """契约 orm-mapping 第 1 节：``raise_on_sql`` 关系必须由调用方显式 selectinload。"""
    repo, stub = repo_pair(OtaTaskRepository)
    stub.queue(StubResult([]))

    await repo.find_all(OtaTask.task_id == uuid4(), options=(selectinload(OtaTask.records),))

    stmt = stub.executed[0][0]
    paths = [str(getattr(option, "path", option)) for option in stmt._with_options]
    assert any("OtaTask.records" in path for path in paths), f"loader options 未挂载: {paths}"


async def test_paginate_passes_loader_options_through() -> None:
    repo, stub = repo_pair(OtaTaskRepository)
    stub.queue(StubResult(scalar=1), StubResult([]))

    await repo.paginate(page=1, page_size=10, options=(selectinload(OtaTask.records),))

    stmt = stub.executed[1][0]
    paths = [str(getattr(option, "path", option)) for option in stmt._with_options]
    assert any("OtaTask.records" in path for path in paths)






