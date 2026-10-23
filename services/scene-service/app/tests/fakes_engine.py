"""scene_engine 仓储的内存替身（与 ``app/repositories/engine.py`` 同方法签名）。

只实现服务层实际调用的方法；行为对齐真实 SQL 的关键语义：
- 逻辑场景按 (logical_id, version) upsert、列表每 ID 取最近更新；
- 具体场景按 content_hash 去重；
- 测试集名称/用例编号唯一冲突抛 3002；
- 测试集条目按 (suite_id, item_id) upsert。
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from hunter_common.exceptions import ResourceAlreadyExistsError

from app.models.scene_engine import (
    BaselineLock,
    ConcreteScenario,
    GenerationTask,
    LogicalScenario,
    ScenarioSuite,
    SuiteItem,
    TestCase,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _value(value: Any) -> str:
    return str(getattr(value, "value", value))


class InMemoryLogicalScenarioRepository:
    """逻辑场景内存仓储。"""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], LogicalScenario] = {}

    async def upsert(self, row: Any) -> None:
        values = dict(row)
        values.setdefault("create_time", _now())
        values["update_time"] = _now()
        self.rows[(values["logical_id"], values["version"])] = LogicalScenario(**values)

    async def get(self, logical_id: str, version: str | None = None) -> LogicalScenario | None:
        candidates = [row for (lid, _), row in self.rows.items() if lid == logical_id]
        if version is not None:
            return self.rows.get((logical_id, version))
        if not candidates:
            return None
        return max(candidates, key=lambda row: row.update_time)

    async def list_versions(self, logical_id: str) -> list[LogicalScenario]:
        rows = [row for (lid, _), row in self.rows.items() if lid == logical_id]
        return sorted(rows, key=lambda row: row.update_time, reverse=True)

    async def list(
        self,
        *,
        category: str | None = None,
        keyword: str | None = None,
        limit: int = 200,
    ) -> list[LogicalScenario]:
        rows = list(self.rows.values())
        if category is not None:
            rows = [row for row in rows if row.category == category]
        if keyword:
            needle = keyword.lower()
            rows = [
                row
                for row in rows
                if needle in row.logical_id.lower() or needle in row.name.lower()
            ]
        latest: dict[str, LogicalScenario] = {}
        for row in rows:
            current = latest.get(row.logical_id)
            if current is None or row.update_time > current.update_time:
                latest[row.logical_id] = row
        return sorted(latest.values(), key=lambda row: row.logical_id)[:limit]

    async def update_status(self, logical_id: str, version: str, status: str) -> bool:
        row = self.rows.get((logical_id, version))
        if row is None:
            return False
        row.status = status  # type: ignore[assignment]
        row.update_time = _now()
        return True


class InMemoryConcreteScenarioRepository:
    """具体场景内存仓储。"""

    def __init__(self) -> None:
        self.rows: dict[UUID, ConcreteScenario] = {}

    async def create_many(self, rows: Any) -> list[ConcreteScenario]:
        created: list[ConcreteScenario] = []
        hashes = {str(row.content_hash).strip() for row in self.rows.values()}
        for payload in rows:
            digest = str(payload["content_hash"]).strip()
            if digest in hashes:
                continue
            hashes.add(digest)
            values = dict(payload)
            values.setdefault("scenario_id", uuid4())
            values.setdefault("create_time", _now())
            values.setdefault("update_time", _now())
            row = ConcreteScenario(**values)
            self.rows[row.scenario_id] = row
            created.append(row)
        return created

    async def get(self, scenario_id: UUID) -> ConcreteScenario | None:
        return self.rows.get(scenario_id)

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
        rows = sorted(self.rows.values(), key=lambda row: row.create_time, reverse=True)
        if logical_id is not None:
            rows = [row for row in rows if row.logical_id == logical_id]
        if rarity_zone is not None:
            rows = [row for row in rows if _value(row.rarity_zone) == rarity_zone]
        if risk_level is not None:
            rows = [row for row in rows if _value(row.risk_level) == risk_level]
        if status is not None:
            rows = [row for row in rows if _value(row.status) == status]
        start = (page - 1) * page_size
        return rows[start : start + page_size], len(rows)

    async def list_all(self, *, limit: int = 2000) -> list[ConcreteScenario]:
        return list(self.rows.values())[:limit]

    async def zone_counts(self, *, logical_id: str | None = None) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.rows.values():
            if logical_id is not None and row.logical_id != logical_id:
                continue
            key = _value(row.rarity_zone)
            counts[key] = counts.get(key, 0) + 1
        return counts

    async def risk_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.rows.values():
            key = _value(row.risk_level)
            counts[key] = counts.get(key, 0) + 1
        return counts

    async def latest(self, logical_id: str) -> ConcreteScenario | None:
        rows = [row for row in self.rows.values() if row.logical_id == logical_id]
        return max(rows, key=lambda row: row.create_time) if rows else None

    async def update_statuses(self, scenario_ids: Any, status: str) -> int:
        updated = 0
        for scenario_id in scenario_ids:
            row = self.rows.get(scenario_id)
            if row is not None:
                row.status = status  # type: ignore[assignment]
                updated += 1
        return updated


class InMemoryGenerationTaskRepository:
    """生成任务内存仓储。"""

    def __init__(self) -> None:
        self.rows: dict[UUID, GenerationTask] = {}

    async def create(self, **values: Any) -> GenerationTask:
        values.setdefault("task_id", uuid4())
        values.setdefault("create_time", _now())
        values.setdefault("update_time", _now())
        row = GenerationTask(**values)
        self.rows[row.task_id] = row
        return row

    async def get(self, task_id: UUID) -> GenerationTask | None:
        return self.rows.get(task_id)

    async def update(self, task_id: UUID, **values: Any) -> GenerationTask | None:
        row = self.rows.get(task_id)
        if row is None:
            return None
        for key, value in values.items():
            setattr(row, key, value)
        row.update_time = _now()
        return row

    async def list_page(
        self, *, page: int, page_size: int, status: str | None = None
    ) -> tuple[list[GenerationTask], int]:
        rows = sorted(self.rows.values(), key=lambda row: row.create_time, reverse=True)
        if status is not None:
            rows = [row for row in rows if _value(row.status) == status]
        start = (page - 1) * page_size
        return rows[start : start + page_size], len(rows)


class InMemoryTestCaseRepository:
    """测试用例内存仓储。"""

    def __init__(self) -> None:
        self.rows: dict[UUID, TestCase] = {}

    async def create(self, **values: Any) -> TestCase:
        if any(row.case_key == values["case_key"] for row in self.rows.values()):
            raise ResourceAlreadyExistsError(
                "测试用例编号已存在", details={"case_key": values["case_key"]}
            )
        values.setdefault("test_case_id", uuid4())
        values.setdefault("create_time", _now())
        values.setdefault("update_time", _now())
        row = TestCase(**values)
        self.rows[row.test_case_id] = row
        return row

    async def get(self, test_case_id: UUID) -> TestCase | None:
        return self.rows.get(test_case_id)

    async def get_by_key(self, case_key: str) -> TestCase | None:
        return next((row for row in self.rows.values() if row.case_key == case_key), None)

    async def list_page(
        self,
        *,
        page: int,
        page_size: int,
        logical_id: str | None = None,
        status: str | None = None,
    ) -> tuple[list[TestCase], int]:
        rows = sorted(self.rows.values(), key=lambda row: row.create_time, reverse=True)
        if logical_id is not None:
            rows = [row for row in rows if row.logical_id == logical_id]
        if status is not None:
            rows = [row for row in rows if _value(row.status) == status]
        start = (page - 1) * page_size
        return rows[start : start + page_size], len(rows)


class InMemorySuiteRepository:
    """测试集 / 条目 / 基线内存仓储。"""

    def __init__(self) -> None:
        self.suites: dict[UUID, ScenarioSuite] = {}
        self.items: list[SuiteItem] = []
        self.baselines: dict[UUID, BaselineLock] = {}

    async def create(self, **values: Any) -> ScenarioSuite:
        if any(row.name == values["name"] for row in self.suites.values()):
            raise ResourceAlreadyExistsError("测试集名称已存在", details={"name": values["name"]})
        values.setdefault("suite_id", uuid4())
        values.setdefault("create_time", _now())
        values.setdefault("update_time", _now())
        row = ScenarioSuite(**values)
        self.suites[row.suite_id] = row
        return row

    async def get(self, suite_id: UUID) -> ScenarioSuite | None:
        return self.suites.get(suite_id)

    async def list_page(self, *, page: int, page_size: int) -> tuple[list[ScenarioSuite], int]:
        rows = sorted(self.suites.values(), key=lambda row: row.create_time, reverse=True)
        start = (page - 1) * page_size
        return rows[start : start + page_size], len(rows)

    async def update(self, suite_id: UUID, **values: Any) -> ScenarioSuite | None:
        row = self.suites.get(suite_id)
        if row is None:
            return None
        for key, value in values.items():
            setattr(row, key, value)
        row.update_time = _now()
        return row

    async def add_items(self, suite_id: UUID, items: Any) -> None:
        for item in items:
            existing = next(
                (
                    row
                    for row in self.items
                    if row.suite_id == suite_id and row.item_id == item["item_id"]
                ),
                None,
            )
            if existing is not None:
                existing.item_type = item["item_type"]
                existing.ref_id = item["ref_id"]
                existing.weight = float(item["weight"])
                continue
            self.items.append(
                SuiteItem(
                    suite_id=suite_id,
                    item_id=item["item_id"],
                    item_type=item["item_type"],
                    ref_id=item["ref_id"],
                    weight=float(item["weight"]),
                    create_time=_now(),
                )
            )

    async def list_items(self, suite_id: UUID) -> list[SuiteItem]:
        return [row for row in self.items if row.suite_id == suite_id]

    async def create_baseline(self, **values: Any) -> BaselineLock:
        if any(
            row.suite_id == values["suite_id"] and row.name == values["name"]
            for row in self.baselines.values()
        ):
            raise ResourceAlreadyExistsError("基线名称已存在", details={"name": values["name"]})
        values.setdefault("baseline_id", uuid4())
        values.setdefault("create_time", _now())
        row = BaselineLock(**values)
        self.baselines[row.baseline_id] = row
        return row

    async def list_baselines(self, suite_id: UUID) -> list[BaselineLock]:
        rows = [row for row in self.baselines.values() if row.suite_id == suite_id]
        return sorted(rows, key=lambda row: row.create_time, reverse=True)


__all__ = [
    "InMemoryConcreteScenarioRepository",
    "InMemoryGenerationTaskRepository",
    "InMemoryLogicalScenarioRepository",
    "InMemorySuiteRepository",
    "InMemoryTestCaseRepository",
]
