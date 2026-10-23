"""scene_engine 数据访问层（服务私有 schema，SQLAlchemy 2.0 异步）。

- 会话边界：``DatabaseSessionManager.session()``（退出即提交），与 ``app/repositories/scenes.py`` 一致；
- 具体场景批量写入逐行 SAVEPOINT：跨任务/跨批次的同内容哈希冲突只跳过该行，不污染调用方事务；
- 测试替身（``app/tests/fakes_engine.py``）实现同名异步方法，服务层不感知存储形态。
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from hunter_common.database import DatabaseSessionManager
from hunter_common.exceptions import ResourceAlreadyExistsError
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import defer

from app.models.scene_engine import (
    AuditLog,
    BaselineLock,
    ConcreteScenario,
    GenerationTask,
    GenerationTaskStatus,
    LogicalScenario,
    ScenarioSuite,
    SuiteItem,
    SuiteStatus,
    TestCase,
)


def _is_unique_violation(exc: IntegrityError) -> bool:
    """识别唯一约束冲突（asyncpg / SQLAlchemy 包装层差异兼容）。"""
    orig = getattr(exc, "orig", None)
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    return sqlstate == "23505" or "23505" in str(orig)


#: 批量插入分片大小（单语句行数过多会放大执行计划/内存峰值）
BULK_INSERT_CHUNK = 500
#: 具体场景大字段（列表/统计/对账路径不需要，按需延迟加载）
CONCRETE_DEFERRED: tuple[str, ...] = (
    "param_values",
    "param_vec",
    "validity",
    "generator",
    "scene_config",
)


class LogicalScenarioRepository:
    """逻辑场景（IR 目录）读写。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def upsert(self, row: Mapping[str, Any]) -> None:
        """按 (logical_id, version) 幂等写入（目录同步重放安全）。

        ``status`` 不参与更新：目录同步只刷新 IR 快照，已发布版本保持 published，
        新插入行使用调用方状态（默认 draft，§9.2 发布需评审）。
        """
        values = dict(row)
        stmt = pg_insert(LogicalScenario).values(**values)
        mutable = {"logical_id", "version", "status", "created_by", "create_time"}
        update_cols = {key: getattr(stmt.excluded, key) for key in values if key not in mutable}
        stmt = stmt.on_conflict_do_update(
            index_elements=["logical_id", "version"], set_=update_cols
        )
        async with self._db.session() as session:
            await session.execute(stmt)

    async def get(self, logical_id: str, version: str | None = None) -> LogicalScenario | None:
        """按 ID 查询；``version`` 省略时取最近更新版本。"""
        stmt = select(LogicalScenario).where(LogicalScenario.logical_id == logical_id)
        if version is not None:
            stmt = stmt.where(LogicalScenario.version == version)
        else:
            stmt = stmt.order_by(LogicalScenario.update_time.desc()).limit(1)
        async with self._db.session() as session:
            return (await session.execute(stmt)).scalars().first()

    async def list_versions(self, logical_id: str) -> list[LogicalScenario]:
        """同一逻辑场景的全部版本（新→旧）。"""
        stmt = (
            select(LogicalScenario)
            .where(LogicalScenario.logical_id == logical_id)
            .order_by(LogicalScenario.update_time.desc())
        )
        async with self._db.session() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def list(
        self,
        *,
        category: str | None = None,
        keyword: str | None = None,
        limit: int = 200,
    ) -> list[LogicalScenario]:
        """目录列表（分类/关键字过滤；每 ID 取最近版本）。"""
        conditions: list[Any] = []
        if category is not None:
            conditions.append(LogicalScenario.category == category)
        if keyword:
            like = f"%{keyword}%"
            conditions.append(
                or_(LogicalScenario.name.ilike(like), LogicalScenario.logical_id.ilike(like))
            )
        stmt = (
            select(LogicalScenario)
            .where(*conditions)
            .order_by(LogicalScenario.logical_id, LogicalScenario.update_time.desc())
            .limit(max(1, min(limit, 500)))
        )
        async with self._db.session() as session:
            rows = list((await session.execute(stmt)).scalars().all())
        # 每个 logical_id 只保留首个（最近更新版本）
        seen: set[str] = set()
        result: list[LogicalScenario] = []
        for row in rows:
            if row.logical_id in seen:
                continue
            seen.add(row.logical_id)
            result.append(row)
        return result

    async def update_status(self, logical_id: str, version: str, status: str) -> bool:
        """更新状态（发布/隔离）；返回是否命中。"""
        async with self._db.session() as session:
            instance = (
                await session.execute(
                    select(LogicalScenario).where(
                        LogicalScenario.logical_id == logical_id,
                        LogicalScenario.version == version,
                    )
                )
            ).scalars().first()
            if instance is None:
                return False
            instance.status = status  # type: ignore[assignment]
            await session.flush()
            return True


class ConcreteScenarioRepository:
    """具体场景读写与统计。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def create_many(self, rows: Sequence[Mapping[str, Any]]) -> list[UUID]:
        """分片批量插入：仅内容哈希冲突（跨任务重复）行跳过，其余约束错误上抛。

        早期逐行 SAVEPOINT 在 5000 条量级下产生等量往返；改为多值 INSERT
        ``ON CONFLICT (content_hash) DO NOTHING + RETURNING``，一次一个往返。
        CHECK/非唯一约束违反不会被 DO NOTHING 吞掉，仍会抛 IntegrityError（不静默降级）。
        """
        if not rows:
            return []
        key_sets = {frozenset(row) for row in rows}
        if len(key_sets) != 1:
            # 列集不一致（异源行）回退逐行路径，交由数据库逐条校验
            return await self._create_many_rowwise(rows)
        created: list[UUID] = []
        async with self._db.session() as session:
            for offset in range(0, len(rows), BULK_INSERT_CHUNK):
                chunk = [dict(row) for row in rows[offset : offset + BULK_INSERT_CHUNK]]
                stmt = (
                    pg_insert(ConcreteScenario)
                    .values(chunk)
                    .on_conflict_do_nothing(constraint="uq_concrete_scenarios_hash")
                    .returning(ConcreteScenario.scenario_id)
                )
                created.extend(row[0] for row in (await session.execute(stmt)).all())
        return created

    async def _create_many_rowwise(self, rows: Sequence[Mapping[str, Any]]) -> list[UUID]:
        """逐行写入兼容路径（仅列集不一致时启用）。"""
        created: list[UUID] = []
        async with self._db.session() as session:
            for row in rows:
                instance = ConcreteScenario(**dict(row))
                try:
                    async with session.begin_nested():
                        session.add(instance)
                        await session.flush()
                except IntegrityError as exc:
                    if _is_unique_violation(exc):
                        continue
                    raise
                created.append(instance.scenario_id)
        return created

    async def get(self, scenario_id: UUID) -> ConcreteScenario | None:
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(ConcreteScenario).where(ConcreteScenario.scenario_id == scenario_id)
                )
            ).scalars().first()

    async def get_many(self, scenario_ids: Sequence[UUID]) -> dict[UUID, ConcreteScenario]:
        """批量读（复检路径避免 N+1）；复检需要参数与 config，不延迟加载。"""
        if not scenario_ids:
            return {}
        stmt = select(ConcreteScenario).where(ConcreteScenario.scenario_id.in_(list(scenario_ids)))
        async with self._db.session() as session:
            rows = list((await session.execute(stmt)).scalars().all())
        return {row.scenario_id: row for row in rows}

    async def list_page(
        self,
        *,
        page: int,
        page_size: int,
        logical_id: str | None = None,
        rarity_zone: str | None = None,
        risk_level: str | None = None,
        status: str | None = None,
    ) -> tuple[list[ConcreteScenario], int]:
        """分页列表（logical_id / 稀有度 / 风险 / 状态过滤，create_time DESC；不拉大列）。"""
        conditions: list[Any] = []
        if logical_id is not None:
            conditions.append(ConcreteScenario.logical_id == logical_id)
        if rarity_zone is not None:
            conditions.append(ConcreteScenario.rarity_zone == rarity_zone)
        if risk_level is not None:
            conditions.append(ConcreteScenario.risk_level == risk_level)
        if status is not None:
            conditions.append(ConcreteScenario.status == status)
        stmt = (
            select(ConcreteScenario)
            .options(defer(*[getattr(ConcreteScenario, name) for name in CONCRETE_DEFERRED]))
            .where(*conditions)
            .order_by(ConcreteScenario.create_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(ConcreteScenario).where(*conditions)
                    )
                ).scalar_one()
            )
            rows = list((await session.execute(stmt)).scalars().all())
        return rows, total

    async def list_all(self, *, limit: int = 2000) -> list[ConcreteScenario]:
        """全量读取（覆盖分析用；上限保护，不加载 JSONB/向量大列）。"""
        stmt = (
            select(ConcreteScenario)
            .options(defer(*[getattr(ConcreteScenario, name) for name in CONCRETE_DEFERRED]))
            .order_by(ConcreteScenario.create_time.desc())
            .limit(max(1, min(limit, 10000)))
        )
        async with self._db.session() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def zone_counts(self, *, logical_id: str | None = None) -> dict[str, int]:
        """稀有度分区计数（Z1-Z4）。"""
        stmt = select(ConcreteScenario.rarity_zone, func.count()).group_by(
            ConcreteScenario.rarity_zone
        )
        if logical_id is not None:
            stmt = stmt.where(ConcreteScenario.logical_id == logical_id)
        async with self._db.session() as session:
            rows = (await session.execute(stmt)).all()
        return {str(zone): int(count) for zone, count in rows}

    async def risk_counts(self, *, logical_id: str | None = None) -> dict[str, int]:
        """风险等级计数（L1-L4；可按逻辑场景过滤）。"""
        stmt = select(ConcreteScenario.risk_level, func.count()).group_by(
            ConcreteScenario.risk_level
        )
        if logical_id is not None:
            stmt = stmt.where(ConcreteScenario.logical_id == logical_id)
        async with self._db.session() as session:
            rows = (await session.execute(stmt)).all()
        return {str(level): int(count) for level, count in rows}

    async def zone_counts_by_logical(self) -> dict[tuple[str, str], int]:
        """按 (logical_id, rarity_zone) 分组计数（覆盖盲区一次查完，避免 N+1）。"""
        stmt = select(
            ConcreteScenario.logical_id,
            ConcreteScenario.rarity_zone,
            func.count(),
        ).group_by(ConcreteScenario.logical_id, ConcreteScenario.rarity_zone)
        async with self._db.session() as session:
            rows = (await session.execute(stmt)).all()
        return {
            (str(logical_id), str(zone)): int(count) for logical_id, zone, count in rows
        }

    async def latest(self, logical_id: str) -> ConcreteScenario | None:
        """某逻辑场景最新生成的具体场景。"""
        stmt = (
            select(ConcreteScenario)
            .where(ConcreteScenario.logical_id == logical_id)
            .order_by(ConcreteScenario.create_time.desc())
            .limit(1)
        )
        async with self._db.session() as session:
            return (await session.execute(stmt)).scalars().first()

    async def update_statuses(self, scenario_ids: Sequence[UUID], status: str) -> int:
        """批量更新状态（批量校验后回写 validated/quarantined）；返回命中行数。"""
        if not scenario_ids:
            return 0
        updated = 0
        async with self._db.session() as session:
            rows = list(
                (
                    await session.execute(
                        select(ConcreteScenario).where(
                            ConcreteScenario.scenario_id.in_(list(scenario_ids))
                        )
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                row.status = status  # type: ignore[assignment]
                updated += 1
            await session.flush()
        return updated


class GenerationTaskRepository:
    """生成任务账本。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def create(self, **values: Any) -> GenerationTask:
        instance = GenerationTask(**values)
        async with self._db.session() as session:
            session.add(instance)
            await session.flush()
            await session.refresh(instance)
        return instance

    async def get(self, task_id: UUID) -> GenerationTask | None:
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(GenerationTask).where(GenerationTask.task_id == task_id)
                )
            ).scalars().first()

    async def update(self, task_id: UUID, **values: Any) -> GenerationTask | None:
        async with self._db.session() as session:
            instance = (
                await session.execute(
                    select(GenerationTask).where(GenerationTask.task_id == task_id)
                )
            ).scalars().first()
            if instance is None:
                return None
            for key, value in values.items():
                setattr(instance, key, value)
            await session.flush()
            await session.refresh(instance)
            return instance

    async def list_page(
        self, *, page: int, page_size: int, status: str | None = None
    ) -> tuple[list[GenerationTask], int]:
        conditions: list[Any] = []
        if status is not None:
            conditions.append(GenerationTask.status == status)
        stmt = (
            select(GenerationTask)
            .where(*conditions)
            .order_by(GenerationTask.create_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(GenerationTask).where(*conditions)
                    )
                ).scalar_one()
            )
            rows = list((await session.execute(stmt)).scalars().all())
        return rows, total

    async def claim(self, task_id: UUID, owner_id: str) -> GenerationTask | None:
        """抢占任务：仅 pending/running 且归属为空或本实例时置 running。"""
        stmt = (
            update(GenerationTask)
            .where(
                GenerationTask.task_id == task_id,
                GenerationTask.status.in_(
                    [GenerationTaskStatus.PENDING.value, GenerationTaskStatus.RUNNING.value]
                ),
                or_(GenerationTask.owner_id.is_(None), GenerationTask.owner_id == owner_id),
            )
            .values(
                status=GenerationTaskStatus.RUNNING.value,
                owner_id=owner_id,
                progress=0.01,
            )
            .returning(GenerationTask)
        )
        async with self._db.session() as session:
            return (await session.execute(stmt)).scalars().first()

    async def request_cancel(self, task_id: UUID) -> GenerationTask | None:
        """协作式取消：pending 直接 canceled，running 置 cancel_requested。"""
        async with self._db.session() as session:
            canceled = (
                await session.execute(
                    update(GenerationTask)
                    .where(
                        GenerationTask.task_id == task_id,
                        GenerationTask.status == GenerationTaskStatus.PENDING.value,
                    )
                    .values(status=GenerationTaskStatus.CANCELED.value)
                    .returning(GenerationTask)
                )
            ).scalars().first()
            if canceled is not None:
                return canceled
            await session.execute(
                update(GenerationTask)
                .where(
                    GenerationTask.task_id == task_id,
                    GenerationTask.status == GenerationTaskStatus.RUNNING.value,
                )
                .values(cancel_requested=True)
            )
            return (
                await session.execute(
                    select(GenerationTask).where(GenerationTask.task_id == task_id)
                )
            ).scalars().first()

    async def is_cancel_requested(self, task_id: UUID) -> bool:
        """轻量查询取消标记（工作线程进度回调使用）。"""
        async with self._db.session() as session:
            value = (
                await session.execute(
                    select(GenerationTask.cancel_requested).where(
                        GenerationTask.task_id == task_id
                    )
                )
            ).scalar_one_or_none()
        return bool(value)

    async def reconcile_stale(self, *, stale_seconds: int) -> int:
        """启动对账：心跳超时的 pending/running 任务置 failed（多副本安全）。"""
        cutoff = utcnow() - timedelta(seconds=max(int(stale_seconds), 1))
        stmt = (
            update(GenerationTask)
            .where(
                GenerationTask.status.in_(
                    [GenerationTaskStatus.PENDING.value, GenerationTaskStatus.RUNNING.value]
                ),
                GenerationTask.update_time < cutoff,
            )
            .values(
                status=GenerationTaskStatus.FAILED.value,
                error_message="任务失联：实例重启或心跳超时，启动对账已置 failed（可重新提交）",
            )
        )
        async with self._db.session() as session:
            result = await session.execute(stmt)
        return int(result.rowcount or 0)


class TestCaseRepository:
    """测试用例读写。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def create(self, **values: Any) -> TestCase:
        instance = TestCase(**values)
        async with self._db.session() as session:
            session.add(instance)
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_unique_violation(exc):
                    raise ResourceAlreadyExistsError(
                        "测试用例编号已存在", details={"case_key": values.get("case_key")}
                    ) from exc
                raise
            await session.refresh(instance)
        return instance

    async def get(self, test_case_id: UUID) -> TestCase | None:
        async with self._db.session() as session:
            return (
                await session.execute(select(TestCase).where(TestCase.test_case_id == test_case_id))
            ).scalars().first()

    async def get_by_key(self, case_key: str) -> TestCase | None:
        async with self._db.session() as session:
            return (
                await session.execute(select(TestCase).where(TestCase.case_key == case_key))
            ).scalars().first()

    async def list_page(
        self,
        *,
        page: int,
        page_size: int,
        logical_id: str | None = None,
        status: str | None = None,
    ) -> tuple[list[TestCase], int]:
        conditions: list[Any] = []
        if logical_id is not None:
            conditions.append(TestCase.logical_id == logical_id)
        if status is not None:
            conditions.append(TestCase.status == status)
        stmt = (
            select(TestCase)
            .where(*conditions)
            .order_by(TestCase.create_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(TestCase).where(*conditions)
                    )
                ).scalar_one()
            )
            rows = list((await session.execute(stmt)).scalars().all())
        return rows, total


class SuiteRepository:
    """测试集 / 条目 / 基线。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def create(self, **values: Any) -> ScenarioSuite:
        instance = ScenarioSuite(**values)
        async with self._db.session() as session:
            session.add(instance)
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_unique_violation(exc):
                    raise ResourceAlreadyExistsError(
                        "测试集名称已存在", details={"name": values.get("name")}
                    ) from exc
                raise
            await session.refresh(instance)
        return instance

    async def get(self, suite_id: UUID) -> ScenarioSuite | None:
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(ScenarioSuite).where(ScenarioSuite.suite_id == suite_id)
                )
            ).scalars().first()

    async def list_page(self, *, page: int, page_size: int) -> tuple[list[ScenarioSuite], int]:
        stmt = (
            select(ScenarioSuite)
            .order_by(ScenarioSuite.create_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        async with self._db.session() as session:
            total = int(
                (await session.execute(select(func.count()).select_from(ScenarioSuite))).scalar_one()
            )
            rows = list((await session.execute(stmt)).scalars().all())
        return rows, total

    async def update(self, suite_id: UUID, **values: Any) -> ScenarioSuite | None:
        async with self._db.session() as session:
            instance = (
                await session.execute(
                    select(ScenarioSuite).where(ScenarioSuite.suite_id == suite_id)
                )
            ).scalars().first()
            if instance is None:
                return None
            for key, value in values.items():
                setattr(instance, key, value)
            await session.flush()
            await session.refresh(instance)
            return instance

    async def add_items(self, suite_id: UUID, items: Sequence[Mapping[str, Any]]) -> None:
        """批量加条目（PK 冲突则更新权重/类型，幂等）。"""
        if not items:
            return
        async with self._db.session() as session:
            for item in items:
                values = {"suite_id": suite_id, **dict(item)}
                stmt = pg_insert(SuiteItem).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["suite_id", "item_id"],
                    set_={
                        "item_type": stmt.excluded.item_type,
                        "ref_id": stmt.excluded.ref_id,
                        "weight": stmt.excluded.weight,
                    },
                )
                await session.execute(stmt)

    async def list_items(self, suite_id: UUID) -> list[SuiteItem]:
        stmt = (
            select(SuiteItem)
            .where(SuiteItem.suite_id == suite_id)
            .order_by(SuiteItem.create_time, SuiteItem.item_id)
        )
        async with self._db.session() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def create_baseline(self, **values: Any) -> BaselineLock:
        instance = BaselineLock(**values)
        async with self._db.session() as session:
            session.add(instance)
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_unique_violation(exc):
                    raise ResourceAlreadyExistsError(
                        "基线名称已存在", details={"name": values.get("name")}
                    ) from exc
                raise
            await session.refresh(instance)
        return instance

    async def list_baselines(self, suite_id: UUID) -> list[BaselineLock]:
        stmt = (
            select(BaselineLock)
            .where(BaselineLock.suite_id == suite_id)
            .order_by(BaselineLock.create_time.desc())
        )
        async with self._db.session() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def add_items_if_draft(
        self, suite_id: UUID, items: Sequence[Mapping[str, Any]]
    ) -> str:
        """同一事务内加锁校验 draft 并写入条目（防“读时 draft、写时已锁定”竞态）。"""
        async with self._db.session() as session:
            suite = (
                await session.execute(
                    select(ScenarioSuite)
                    .where(ScenarioSuite.suite_id == suite_id)
                    .with_for_update()
                )
            ).scalars().first()
            if suite is None:
                return "missing"
            status = str(getattr(suite.status, "value", suite.status))
            if status != SuiteStatus.DRAFT.value:
                return "conflict"
            for item in items:
                values = {"suite_id": suite_id, **dict(item)}
                stmt = pg_insert(SuiteItem).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["suite_id", "item_id"],
                    set_={
                        "item_type": stmt.excluded.item_type,
                        "ref_id": stmt.excluded.ref_id,
                        "weight": stmt.excluded.weight,
                    },
                )
                await session.execute(stmt)
            return "ok"

    async def lock_with_baseline(
        self,
        suite_id: UUID,
        *,
        digest_fn: Callable[[Sequence[Mapping[str, Any]]], str],
        baseline_name: str | None,
        metrics: Mapping[str, Any],
        user_id: str,
    ) -> tuple[str, str | None, int]:
        """单事务锁定：行锁校验状态 → 读条目 → 写 lock_hash → 可选基线（原子）。"""
        async with self._db.session() as session:
            suite = (
                await session.execute(
                    select(ScenarioSuite)
                    .where(ScenarioSuite.suite_id == suite_id)
                    .with_for_update()
                )
            ).scalars().first()
            if suite is None:
                return "missing", None, 0
            status = str(getattr(suite.status, "value", suite.status))
            if status == SuiteStatus.LOCKED.value:
                digest = str(suite.lock_hash).strip() if suite.lock_hash else None
                return "locked", digest, 0
            if status == SuiteStatus.ARCHIVED.value:
                return "archived", None, 0
            items = list(
                (
                    await session.execute(
                        select(SuiteItem)
                        .where(SuiteItem.suite_id == suite_id)
                        .order_by(SuiteItem.create_time, SuiteItem.item_id)
                    )
                ).scalars().all()
            )
            if not items:
                return "empty", None, 0
            fingerprints = [
                {
                    "item_id": item.item_id,
                    "item_type": str(getattr(item.item_type, "value", item.item_type)),
                    "ref_id": item.ref_id,
                    "weight": float(item.weight),
                }
                for item in items
            ]
            digest = digest_fn(fingerprints)
            suite.status = SuiteStatus.LOCKED.value  # type: ignore[assignment]
            suite.lock_hash = digest
            suite.locked_at = utcnow()
            suite.locked_by = user_id
            if baseline_name:
                session.add(
                    BaselineLock(
                        suite_id=suite_id,
                        name=baseline_name,
                        suite_hash=digest,
                        metrics={**dict(metrics), "items": fingerprints},
                        locked_by=user_id,
                    )
                )
                try:
                    await session.flush()
                except IntegrityError as exc:
                    if _is_unique_violation(exc):
                        raise ResourceAlreadyExistsError(
                            "基线名称已存在", details={"name": baseline_name}
                        ) from exc
                    raise
            return "ok", digest, len(items)


class AuditLogRepository:
    """追加式审计日志（哈希链 + 事务级 advisory lock 串行化）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def append(
        self,
        *,
        action: str,
        target_type: str,
        target_id: str,
        actor: str,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
        """写入一条审计：``entry_hash = sha256(prev_hash + canonical_payload)``。"""
        canonical = json.dumps(
            {
                "action": action,
                "target_type": target_type,
                "target_id": target_id,
                "actor": actor,
                "payload": dict(payload or {}),
            },
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        async with self._db.session() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext('scene_engine.audit_logs'))")
            )
            previous = (
                await session.execute(
                    select(AuditLog).order_by(AuditLog.audit_id.desc()).limit(1)
                )
            ).scalars().first()
            prev_hash = str(previous.entry_hash).strip() if previous else "0" * 64
            entry_hash = hashlib.sha256(f"{prev_hash}:{canonical}".encode()).hexdigest()
            session.add(
                AuditLog(
                    action=action,
                    target_type=target_type,
                    target_id=target_id,
                    actor=actor,
                    payload=dict(payload or {}),
                    prev_hash=prev_hash,
                    entry_hash=entry_hash,
                )
            )
            await session.flush()
        return entry_hash


def utcnow() -> datetime:
    """UTC now（服务层写 locked_at 等字段用）。"""
    return datetime.now(UTC)


__all__ = [
    "AuditLogRepository",
    "ConcreteScenarioRepository",
    "GenerationTaskRepository",
    "LogicalScenarioRepository",
    "SuiteRepository",
    "TestCaseRepository",
    "utcnow",
]
