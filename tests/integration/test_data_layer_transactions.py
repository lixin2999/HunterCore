"""数据访问层写路径事务语义集成测试（真实 PostgreSQL 15 + TimescaleDB 2.13）。

契约依据（``contracts/database/orm-mapping.md`` 第 3 节 + 错误码表）：
- 唯一冲突 → **3002**（``ResourceAlreadyExistsError``，HTTP 409）；约束冲突 → 2001；
- ``create`` / ``update`` 用 SAVEPOINT 局部回滚：**调用方外事务与已写数据保留**，会话可继续提交；
- 写路径纪律：``add`` / ``setattr`` 必须在 ``begin_nested()`` 之后；
- ``purge_before`` 的 ``ctid`` 分批 DELETE 必须能在 hypertable 上执行。

为什么单列一个文件（代码审查 R1/R2/R5）：``begin_nested()`` 会先 ``flush()`` 未决变更、
冲突回滚会把会话事务置为 pending-rollback、SAVEPOINT 回滚已把失败行移出会话（再 ``expunge`` 抛
``InvalidRequestError``）—— 这些都是真实的 ORM/驱动运行时行为，桩件无法覆盖。
无 Docker 环境由 ``docker_guard`` 夹具自动 skip（绝不静默通过）。

数据隔离：所有写入使用本次运行生成的唯一名称，用例结束前物理删除，不影响同容器内其他用例。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from hunter_common.database.models import Scene
from hunter_common.database.repositories.collector import VehicleTelemetryRepository
from hunter_common.database.repositories.scene import SceneRepository
from hunter_common.exceptions import ErrorCode, ResourceAlreadyExistsError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration

#: scene_svc.scenes.creator 承载的用户 UUID（逻辑外键指向 user_svc.users），测试用固定值
CREATOR: UUID = UUID("11111111-1111-4111-8111-111111111111")
#: 物理清理用旧时间点（早于任何真实数据；命中 0 行但验证 ctid DELETE 在 hypertable 可执行）
ANCIENT_CUTOFF = datetime(2000, 1, 1, tzinfo=UTC)


def _unique_name(prefix: str) -> str:
    """生成唯一场景名（避免与容器内其他用例/历史数据冲突）。"""
    return f"{prefix}-{uuid4().hex[:20]}"


@pytest.fixture
async def db_session(postgres_container: Any) -> AsyncIterator[AsyncSession]:
    """真实 PostgreSQL 上的独立会话（DSN 取自容器夹具，不硬编码连接参数）。"""
    engine = create_async_engine(str(postgres_container.dsn), pool_pre_ping=True)
    factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )
    async with factory() as session:
        yield session
    await engine.dispose()


async def _cleanup(repo: SceneRepository, names: list[str]) -> None:
    """物理删除本用例写入的场景（保持容器内数据面干净）。"""
    if names:
        await repo.delete_where(Scene.scene_name.in_(names))


async def test_create_unique_conflict_returns_3002_and_keeps_transaction(
    db_session: AsyncSession,
) -> None:
    """唯一冲突 → 3002，且冲突前已写入的数据保留、会话可继续提交（不再退化为 5000）。"""
    repo = SceneRepository(db_session)
    conflict_name = _unique_name("review-cf")
    kept_name = _unique_name("review-keep")
    after_name = _unique_name("review-after")
    try:
        await repo.create(scene_name=conflict_name, scene_type="urban", creator=CREATOR)
        await repo.create(scene_name=kept_name, scene_type="urban", creator=CREATOR)

        with pytest.raises(ResourceAlreadyExistsError) as excinfo:
            await repo.create(scene_name=conflict_name, scene_type="urban", creator=CREATOR)

        assert excinfo.value.code == ErrorCode.RESOURCE_ALREADY_EXISTS
        assert excinfo.value.details["model"] == "Scene"
        assert "constraint" in excinfo.value.details, "details 只允许模型名/SQLSTATE/约束名"
        assert conflict_name not in str(excinfo.value.details), "details 禁止携带冲突行键值"

        # 冲突后外事务与已写数据保留：继续写 + 提交（旧实现此处抛 PendingRollbackError）
        await repo.create(scene_name=after_name, scene_type="urban", creator=CREATOR)
        await db_session.commit()

        assert await repo.count(filters={"scene_name": kept_name}) == 1
        assert await repo.count(filters={"scene_name": after_name}) == 1
        assert await repo.count(filters={"scene_name": conflict_name}) == 1
    finally:
        await _cleanup(repo, [conflict_name, kept_name, after_name])
        await db_session.commit()


async def test_update_unique_conflict_returns_3002_and_keeps_transaction(
    db_session: AsyncSession,
) -> None:
    """``update`` 唯一冲突 → 3002；SAVEPOINT 局部回滚后同一事务仍可提交其他写入。"""
    repo = SceneRepository(db_session)
    first_name = _unique_name("review-up-a")
    second_name = _unique_name("review-up-b")
    try:
        await repo.create(scene_name=first_name, scene_type="urban", creator=CREATOR)
        second = await repo.create(scene_name=second_name, scene_type="urban", creator=CREATOR)
        await db_session.commit()

        with pytest.raises(ResourceAlreadyExistsError) as excinfo:
            await repo.update(second, scene_name=first_name)

        assert excinfo.value.code == ErrorCode.RESOURCE_ALREADY_EXISTS

        first = await repo.get_by_scene_name(first_name)
        assert first is not None
        await repo.update(first, scene_type="highway")
        await db_session.commit()

        await db_session.refresh(first)
        assert first.scene_type == "highway"
    finally:
        await _cleanup(repo, [first_name, second_name])
        await db_session.commit()


async def test_bulk_create_chunk_conflict_keeps_previous_chunks(
    db_session: AsyncSession,
) -> None:
    """批量分片冲突 → 3002，已成功分片保留（首分片数据在 commit 后仍在库）。"""
    repo = SceneRepository(db_session)
    names = [_unique_name("review-bulk-1"), _unique_name("review-bulk-2")]
    rows = [
        {"scene_name": names[0], "scene_type": "urban", "creator": CREATOR},
        {"scene_name": names[1], "scene_type": "urban", "creator": CREATOR},
        {"scene_name": names[0], "scene_type": "urban", "creator": CREATOR},  # 第 2 分片冲突
    ]
    try:
        with pytest.raises(ResourceAlreadyExistsError) as excinfo:
            await repo.bulk_create(rows, chunk_size=2)

        assert excinfo.value.code == ErrorCode.RESOURCE_ALREADY_EXISTS

        await db_session.commit()  # 首分片保留
        assert await repo.count(filters={"scene_name": names[0]}) == 1
        assert await repo.count(filters={"scene_name": names[1]}) == 1
    finally:
        await _cleanup(repo, names)
        await db_session.commit()


async def test_purge_before_executes_on_hypertable(db_session: AsyncSession) -> None:
    """``ctid`` 分批 DELETE 必须能在 TimescaleDB hypertable 上执行（无匹配行时删除 0 行）。"""
    repo = VehicleTelemetryRepository(db_session)

    removed = await repo.purge_before(ANCIENT_CUTOFF, batch_size=100)

    assert removed == 0, "早于 2000 年的数据不存在，删除行数必须为 0"
