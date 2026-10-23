"""scene_engine 仓储的内存替身（与 ``app/repositories/engine.py`` 同方法签名）。

只实现服务层实际调用的方法；行为对齐真实 SQL 的关键语义：
- 逻辑场景按 (logical_id, version) upsert（status 不参与更新）、列表每 ID 取最近更新；
- 具体场景按 content_hash 去重；
- 测试集名称/用例编号唯一冲突抛 3002；
- 测试集条目按 (suite_id, item_id) upsert；draft 校验与锁定均为单步原子操作；
- 生成任务归属/协作取消/启动对账与真实 SQL 同语义。
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from hunter_common.exceptions import ResourceAlreadyExistsError

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
        existing = self.rows.get((values["logical_id"], values["version"]))
        if existing is not None:
            # 与真实 SQL 一致：status 不参与更新（已发布保持 published）
            values["status"] = existing.status
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

    async def create_many(self, rows: Any) -> list[UUID]:
        """与真实仓储同签名：仅哈希冲突行跳过，返回已入库 scenario_id 列表。"""
        created: list[UUID] = []
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
            created.append(row.scenario_id)
        return created

    async def get_many(self, scenario_ids: Any) -> dict[UUID, ConcreteScenario]:
        return {sid: self.rows[sid] for sid in scenario_ids if sid in self.rows}

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

    async def risk_counts(self, *, logical_id: str | None = None) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.rows.values():
            if logical_id is not None and row.logical_id != logical_id:
                continue
            key = _value(row.risk_level)
            counts[key] = counts.get(key, 0) + 1
        return counts

    async def zone_counts_by_logical(self) -> dict[tuple[str, str], int]:
        grouped: dict[tuple[str, str], int] = {}
        for row in self.rows.values():
            key = (row.logical_id, _value(row.rarity_zone))
            grouped[key] = grouped.get(key, 0) + 1
        return grouped

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

    async def claim(self, task_id: UUID, owner_id: str) -> GenerationTask | None:
        """抢占任务：仅 pending/running 且归属为空或本实例（与真实 SQL 同语义）。"""
        row = self.rows.get(task_id)
        if row is None:
            return None
        if _value(row.status) not in {
            GenerationTaskStatus.PENDING.value,
            GenerationTaskStatus.RUNNING.value,
        }:
            return None
        owner = getattr(row, "owner_id", None)
        if owner is not None and owner != owner_id:
            return None
        row.status = GenerationTaskStatus.RUNNING.value  # type: ignore[assignment]
        row.owner_id = owner_id
        row.progress = 0.01
        row.update_time = _now()
        return row

    async def request_cancel(self, task_id: UUID) -> GenerationTask | None:
        row = self.rows.get(task_id)
        if row is None:
            return None
        if _value(row.status) == GenerationTaskStatus.PENDING.value:
            row.status = GenerationTaskStatus.CANCELED.value  # type: ignore[assignment]
        elif _value(row.status) == GenerationTaskStatus.RUNNING.value:
            row.cancel_requested = True
        row.update_time = _now()
        return row

    async def is_cancel_requested(self, task_id: UUID) -> bool:
        row = self.rows.get(task_id)
        return bool(row is not None and getattr(row, "cancel_requested", False))

    async def reconcile_stale(self, *, stale_seconds: int) -> int:
        cutoff = _now() - timedelta(seconds=max(int(stale_seconds), 1))
        count = 0
        for row in self.rows.values():
            if _value(row.status) not in {
                GenerationTaskStatus.PENDING.value,
                GenerationTaskStatus.RUNNING.value,
            }:
                continue
            if row.update_time < cutoff:
                row.status = GenerationTaskStatus.FAILED.value  # type: ignore[assignment]
                row.error_message = "任务失联：实例重启或心跳超时，启动对账已置 failed（可重新提交）"
                count += 1
        return count


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

    async def add_items_if_draft(
        self, suite_id: UUID, items: Sequence[Mapping[str, Any]]
    ) -> str:
        row = self.suites.get(suite_id)
        if row is None:
            return "missing"
        if _value(row.status) != SuiteStatus.DRAFT.value:
            return "conflict"
        await self.add_items(suite_id, items)
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
        row = self.suites.get(suite_id)
        if row is None:
            return "missing", None, 0
        status = _value(row.status)
        if status == SuiteStatus.LOCKED.value:
            digest = str(row.lock_hash).strip() if row.lock_hash else None
            return "locked", digest, 0
        if status == SuiteStatus.ARCHIVED.value:
            return "archived", None, 0
        items = await self.list_items(suite_id)
        if not items:
            return "empty", None, 0
        fingerprints = [
            {
                "item_id": item.item_id,
                "item_type": _value(item.item_type),
                "ref_id": item.ref_id,
                "weight": float(item.weight),
            }
            for item in items
        ]
        digest = digest_fn(fingerprints)
        row.status = SuiteStatus.LOCKED.value  # type: ignore[assignment]
        row.lock_hash = digest
        row.locked_at = _now()
        row.locked_by = user_id
        row.update_time = _now()
        if baseline_name:
            await self.create_baseline(
                suite_id=suite_id,
                name=baseline_name,
                suite_hash=digest,
                metrics={**dict(metrics), "items": fingerprints},
                locked_by=user_id,
            )
        return "ok", digest, len(items)


class InMemoryAuditLogRepository:
    """审计日志内存替身（保留哈希链计算口径供断言）。"""

    def __init__(self) -> None:
        self.rows: list[AuditLog] = []

    async def append(
        self,
        *,
        action: str,
        target_type: str,
        target_id: str,
        actor: str,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
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
        prev_hash = (
            str(self.rows[-1].entry_hash).strip() if self.rows else "0" * 64
        )
        entry_hash = hashlib.sha256(f"{prev_hash}:{canonical}".encode()).hexdigest()
        self.rows.append(
            AuditLog(
                audit_id=len(self.rows) + 1,
                action=action,
                target_type=target_type,
                target_id=target_id,
                actor=actor,
                payload=dict(payload or {}),
                prev_hash=prev_hash,
                entry_hash=entry_hash,
                create_time=_now(),
            )
        )
        return entry_hash


__all__ = [
    "InMemoryAuditLogRepository",
    "InMemoryConcreteScenarioRepository",
    "InMemoryGenerationTaskRepository",
    "InMemoryLogicalScenarioRepository",
    "InMemorySuiteRepository",
    "InMemoryTestCaseRepository",
]
