"""BaseRepository 写路径的真实 Session 语义测试（``sqlite+aiosqlite``，无需容器）。

为什么必须有一个真库（真实 Session）用例（代码审查 R1/R2/R5）：
- ``Session.begin_nested()`` 会先 ``flush()`` 未决变更（``SessionTransaction._take_snapshot``）——
  ``add`` / ``setattr`` 若在 SAVEPOINT 之外，语句会落到 SAVEPOINT 外，冲突时**整个会话事务被回滚**
  （后续 commit 抛 ``PendingRollbackError``）；
- SAVEPOINT 回滚会把失败行移出会话，此时再 ``expunge`` 会抛 ``InvalidRequestError``（3002 → 5000）。
桩件 ``StubSession`` 只能断言 SQL 形状与调用顺序，无法表达上述运行时语义。

模型说明：本文件内的测试模型挂在**独立** metadata 上（不进入 ``hunter_common.database.base.Base``），
只用通用类型（``String`` + 唯一约束），避免 JSONB / ARRAY / UUID 等 PG 专有类型在 SQLite 上无法建表，
也不影响契约校验（模型/表清单仍为 13 张）。PostgreSQL 侧同语义验证见
``tests/integration/test_data_layer_transactions.py``（``integration`` 标记）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from hunter_common.database.repository import BaseRepository
from hunter_common.exceptions import ErrorCode, HunterBaseException

pytest.importorskip("aiosqlite", reason="真实 Session 语义测试需要 aiosqlite（dev 依赖）")

#: 平台预定义错误码：SQLite 无 pgcode（→2001），PostgreSQL 唯一冲突为 23505（→3002）
_UNIQUE_CONFLICT_CODES = (ErrorCode.RESOURCE_ALREADY_EXISTS, ErrorCode.INVALID_PARAM)


class ProbeBase(DeclarativeBase):
    """测试专用声明式基类（独立 metadata，不污染契约登记的 13 张表）。"""


class ReviewRow(ProbeBase):
    """探针表：主键 + 唯一约束 + 默认值，足以覆盖写路径语义。"""

    __tablename__ = "review_probe_rows"
    row_id: Mapped[str] = mapped_column(sa.String(64), primary_key=True)
    row_name: Mapped[str] = mapped_column(sa.String(64), unique=True)
    status: Mapped[str] = mapped_column(sa.String(16), default="new")


class ReviewRowRepository(BaseRepository[Any]):
    """探针表的 Repository（复用共享基类，验证 SAVEPOINT 写路径纪律）。"""

    model = ReviewRow


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """真实 AsyncSession（sqlite+aiosqlite），工厂参数与 ``DatabaseSessionManager.init()`` 一致。"""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as connection:
        await connection.run_sync(ProbeBase.metadata.create_all)
    factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )
    async with factory() as db:
        yield db
    await engine.dispose()


async def test_create_conflict_raises_platform_error_and_keeps_transaction(
    session: AsyncSession,
) -> None:
    """唯一冲突 → 平台预定义异常（PG=3002），且**调用方外事务与已写数据保留**、会话可继续提交。"""
    repo = ReviewRowRepository(session)
    await repo.create(row_id="r1", row_name="dup")
    await repo.create(row_id="r2", row_name="kept")

    with pytest.raises(HunterBaseException) as excinfo:
        await repo.create(row_id="r3", row_name="dup")

    assert excinfo.value.code in _UNIQUE_CONFLICT_CODES, (
        "必须是平台预定义异常；旧实现会抛 InvalidRequestError（→5000）"
    )

    # 冲突之后外事务仍可继续写并提交（旧实现此处抛 PendingRollbackError，数据全丢）
    await repo.create(row_id="r4", row_name="after-conflict")
    await session.commit()

    assert await repo.count() == 3, "r1/r2/r4 必须已提交，r3 必须被丢弃"


async def test_create_conflict_does_not_replay_failed_row_on_next_flush(
    session: AsyncSession,
) -> None:
    """失败行由 SAVEPOINT 回滚移出会话：后续 flush 不会重放冲突（无需 expunge）。"""
    repo = ReviewRowRepository(session)
    await repo.create(row_id="r1", row_name="dup")

    with pytest.raises(HunterBaseException):
        await repo.create(row_id="r2", row_name="dup")

    await session.flush()  # 若失败行仍在会话中，这里会再次抛唯一冲突
    assert await repo.count(filters={"row_name": "dup"}) == 1


async def test_update_conflict_keeps_transaction_usable(session: AsyncSession) -> None:
    """``update`` 唯一冲突 → 平台预定义异常，SAVEPOINT 局部回滚，外事务仍可提交。"""
    repo = ReviewRowRepository(session)
    first = await repo.create(row_id="u1", row_name="ua")
    second = await repo.create(row_id="u2", row_name="ub")
    await session.commit()

    with pytest.raises(HunterBaseException) as excinfo:
        await repo.update(second, row_name="ua")

    assert excinfo.value.code in _UNIQUE_CONFLICT_CODES, (
        "setattr 若发生在 SAVEPOINT 之外，此处会抛 PendingRollbackError 而非平台异常"
    )

    await repo.update(first, status="changed")
    await session.commit()
    await session.refresh(first)
    assert first.status == "changed", "冲突后的其他写入必须能提交"


async def test_bulk_create_chunk_conflict_keeps_previous_chunks(session: AsyncSession) -> None:
    """批量分片冲突：抛平台异常但**已成功分片保留**（事务边界仍归调用方）。"""
    repo = ReviewRowRepository(session)
    rows = [
        {"row_id": "b1", "row_name": "bn1"},
        {"row_id": "b2", "row_name": "bn2"},
        {"row_id": "b3", "row_name": "bn1"},  # 第 2 分片与第 1 分片冲突
        {"row_id": "b4", "row_name": "bn4"},
    ]

    with pytest.raises(HunterBaseException):
        await repo.bulk_create(rows, chunk_size=2)

    await session.commit()
    assert sorted(row.row_id for row in await repo.list(order_by=("row_id",))) == ["b1", "b2"]


async def test_delete_where_guard_applies_to_real_session(session: AsyncSession) -> None:
    """防呆守卫同样在真实会话生效：不引用模型列的恒真条件必须拒绝且不发 SQL。"""
    repo = ReviewRowRepository(session)
    await repo.create(row_id="d1", row_name="dn1")

    with pytest.raises(HunterBaseException):
        await repo.delete_where(sa.literal_column("1=1"))

    assert await repo.count() == 1
