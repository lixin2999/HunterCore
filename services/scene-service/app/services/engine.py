"""场景引擎服务层（逻辑/具体场景、生成任务、用例、测试集、覆盖）。

职责边界：
- **IR 事实来源**：内置目录（``scene_engine.catalog``）是代码内单一事实来源，
  数据库 ``scene_engine.logical_scenarios`` 是发布快照；读取时"DB 优先、目录兜底"，
  因此新建环境未同步目录也能校验/干跑/生成；
- **生成任务**：请求落账（pending）→ BackgroundTasks 执行（running → succeeded/failed），
  CPU 密集的采样在 ``asyncio.to_thread`` 中执行，避免阻塞事件循环；
- **具体场景**：生成产物批量入库（内容哈希唯一，跨任务重复自动跳过），
  支持按需渲染与批量复检（G1-G7 重跑，失败进 quarantined）。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import random
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from hunter_common.exceptions import (
    InvalidParameterError,
    ResourceNotFoundError,
    ResourceStateConflictError,
)
from hunter_common.logging import get_logger
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.config import Settings
from app.models.scene_engine import (
    ConcreteScenarioStatus,
    GenerationProfile,
    GenerationStrategy,
    GenerationTaskStatus,
    LogicalScenarioStatus,
    SuiteItemType,
    SuiteStatus,
    TestCaseStatus,
)
from app.repositories.engine import (
    ConcreteScenarioRepository,
    GenerationTaskRepository,
    LogicalScenarioRepository,
    SuiteRepository,
    TestCaseRepository,
    utcnow,
)
from app.scene_engine.catalog import CATALOG, CATALOG_BY_ID, CATALOG_VERSION
from app.scene_engine.compiler import compile_scene, default_values
from app.scene_engine.generator import (
    PROFILE_QUOTAS,
    GenerationOptions,
    SceneGenerator,
    ensure_overrides_shape,
)
from app.scene_engine.ir import IR_VERSION, SceneIR
from app.scene_engine.oracles import ORACLE_BY_REF, list_oracles
from app.scene_engine.validators import ir_content_hash
from app.schemas.engine import (
    BatchValidateData,
    BatchValidateItem,
    CoverageGapData,
    CoverageGapListData,
    CoverageOverviewData,
    DryRunData,
    GateItem,
    GenerateRequest,
    LogicalScenarioDetail,
    LogicalScenarioSummary,
    LogicalVersionSummary,
    PublishData,
    SuiteCreateRequest,
    SuiteData,
    SuiteDetailData,
    SuiteDiffData,
    SuiteItemData,
    SuiteItemInput,
    SuiteItemsRequest,
    SuiteLockData,
    TestCaseCreateRequest,
    TestCaseData,
    ValidationReportData,
)

logger = get_logger("app.services.engine")

#: 目录同步使用的创建者（无人类操作者）
SYSTEM_CREATOR = "system"
#: 生成任务进度回写的最小间隔（百分比）
PROGRESS_STEP = 0.1


def _enum_value(value: Any) -> str:
    """ORM StrEnum / 原生字符串统一转 str。"""
    return str(getattr(value, "value", value))


def suite_hash(items: Sequence[Mapping[str, Any]]) -> str:
    """测试集内容哈希（item_id 排序，权重保留 6 位小数）。"""
    canonical = [
        {
            "item_id": str(item["item_id"]),
            "item_type": _enum_value(item["item_type"]),
            "ref_id": str(item["ref_id"]),
            "weight": round(float(item["weight"]), 6),
        }
        for item in sorted(items, key=lambda item: str(item["item_id"]))
    ]
    payload = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class SceneEngineService:
    """场景引擎业务服务（依赖注入仓储，测试用内存替身）。"""

    def __init__(
        self,
        logical_repository: LogicalScenarioRepository,
        concrete_repository: ConcreteScenarioRepository,
        task_repository: GenerationTaskRepository,
        testcase_repository: TestCaseRepository,
        suite_repository: SuiteRepository,
        settings: Settings,
        *,
        generator: SceneGenerator | None = None,
    ) -> None:
        self._logical = logical_repository
        self._concrete = concrete_repository
        self._tasks = task_repository
        self._testcases = testcase_repository
        self._suites = suite_repository
        self._settings = settings
        allowed_maps = settings.allowed_map_set or None
        self._generator = generator or SceneGenerator(
            max_duration=settings.scene_duration_max_seconds,
            allowed_maps=allowed_maps,
        )
        self._allowed_maps = allowed_maps

    # =================================================================
    # 一、逻辑场景目录
    # =================================================================

    def catalog_summaries(
        self, *, category: str | None = None, keyword: str | None = None
    ) -> list[LogicalScenarioSummary]:
        """内置目录摘要（不依赖数据库）。"""
        items: list[LogicalScenarioSummary] = []
        for ir in CATALOG:
            if category is not None and ir.category != category:
                continue
            if keyword and keyword.lower() not in f"{ir.logical_id} {ir.name}".lower():
                continue
            items.append(self._ir_summary(ir))
        return items

    async def list_logical(
        self,
        *,
        category: str | None = None,
        keyword: str | None = None,
        include_catalog: bool = True,
        limit: int = 200,
    ) -> list[LogicalScenarioSummary]:
        """逻辑场景列表：DB 快照优先，目录补齐未同步项。"""
        items: list[LogicalScenarioSummary] = []
        seen: set[str] = set()
        try:
            rows = await self._logical.list(category=category, keyword=keyword, limit=limit)
        except (SQLAlchemyError, OSError):  # pragma: no cover - 未初始化 schema 时目录兜底
            logger.warning("logical_scenarios_unavailable_fallback_to_catalog")
            rows = []
        for row in rows:
            items.append(self._row_summary(row))
            seen.add(row.logical_id)
        if include_catalog:
            for summary in self.catalog_summaries(category=category, keyword=keyword):
                if summary.logical_id not in seen:
                    items.append(summary)
        items.sort(key=lambda item: (item.logical_id, item.version))
        return items[:limit]

    async def get_logical(self, logical_id: str, version: str | None = None) -> LogicalScenarioDetail:
        """逻辑场景详情（含 IR 与参数/绑定/约束声明）。"""
        ir, resolved_version, origin, row = await self._load_ir(logical_id, version)
        detail = self._ir_detail(ir, resolved_version, origin)
        if row is not None:
            detail.status = _enum_value(getattr(row, "status", detail.status))
            detail.update_time = getattr(row, "update_time", None)
        return detail

    async def list_versions(self, logical_id: str) -> tuple[str, list[LogicalVersionSummary]]:
        """同一逻辑场景的版本列表（DB 版本 + 目录当前版本）。"""
        items: list[LogicalVersionSummary] = []
        try:
            rows = await self._logical.list_versions(logical_id)
        except (SQLAlchemyError, OSError):  # pragma: no cover
            rows = []
        for row in rows:
            items.append(
                LogicalVersionSummary(
                    version=row.version,
                    status=_enum_value(row.status),
                    content_hash=str(row.content_hash).strip(),
                    origin="database",
                    update_time=getattr(row, "update_time", None),
                )
            )
        if not any(item.version == CATALOG_VERSION for item in items):
            ir = CATALOG_BY_ID.get(logical_id)
            if ir is not None:
                items.append(
                    LogicalVersionSummary(
                        version=CATALOG_VERSION,
                        status=LogicalScenarioStatus.PUBLISHED.value,
                        content_hash=ir_content_hash(ir),
                        origin="catalog",
                    )
                )
        if not items:
            raise ResourceNotFoundError(
                "逻辑场景不存在", details={"logical_id": logical_id}
            )
        return logical_id, items

    async def sync_catalog(self, created_by: str = SYSTEM_CREATOR) -> dict[str, Any]:
        """把内置目录幂等写入 DB（按 logical_id + version upsert）。"""
        synced: list[str] = []
        for ir in CATALOG:
            await self._logical.upsert(self._ir_row(ir, CATALOG_VERSION, created_by=created_by))
            synced.append(ir.logical_id)
        return {"version": CATALOG_VERSION, "synced": len(synced), "logical_ids": synced}

    async def validate(
        self, logical_id: str, version: str | None = None
    ) -> ValidationReportData:
        """静态校验：缺省参数编译 + G1-G7 + 判据存在性。"""
        ir, resolved_version, _, _ = await self._load_ir(logical_id, version)
        values = default_values(ir)
        try:
            compilation = compile_scene(
                ir,
                values,
                max_duration=self._settings.scene_duration_max_seconds,
                allowed_maps=self._allowed_maps,
            )
            gates = [
                GateItem(gate=item.gate, passed=item.passed, message=item.message)
                for item in compilation.report.gates
            ]
            failures = list(compilation.report.failures)
        except ValueError as exc:
            gates = [GateItem(gate="G1", passed=False, message=str(exc))]
            failures = [f"G1: {exc}"]
        oracle_found = ir.oracle_ref in ORACLE_BY_REF
        if not oracle_found:
            failures.append(f"oracle: 未知判据 {ir.oracle_ref}")
        return ValidationReportData(
            logical_id=ir.logical_id,
            version=resolved_version,
            passed=not failures,
            gates=gates,
            failures=failures,
            oracle_ref=ir.oracle_ref,
            oracle_found=oracle_found,
            parameter_defaults=values,
        )

    async def dryrun(
        self,
        logical_id: str,
        *,
        version: str | None = None,
        values: Mapping[str, float | str] | None = None,
    ) -> DryRunData:
        """干跑：合并缺省参数与显式覆盖，返回渲染后的 SceneConfig 与闸门明细。"""
        ir, resolved_version, _, _ = await self._load_ir(logical_id, version)
        merged = default_values(ir)
        for key, value in (values or {}).items():
            if key not in ir.parameter_map:
                raise InvalidParameterError(
                    f"未知参数: {key}", details={"logical_id": logical_id, "parameter": key}
                )
            merged[key] = value
        try:
            compilation = compile_scene(
                ir,
                merged,
                max_duration=self._settings.scene_duration_max_seconds,
                allowed_maps=self._allowed_maps,
            )
        except ValueError as exc:
            raise InvalidParameterError(str(exc), details={"logical_id": logical_id}) from exc
        return DryRunData(
            logical_id=ir.logical_id,
            version=resolved_version,
            passed=compilation.passed,
            values=compilation.values,
            scene_config=compilation.config,
            gates=[
                GateItem(gate=item.gate, passed=item.passed, message=item.message)
                for item in compilation.report.gates
            ],
            failures=list(compilation.report.failures),
        )

    async def publish(self, logical_id: str, version: str | None = None) -> PublishData:
        """发布逻辑场景（目录项先落库再置 published）。"""
        ir, resolved_version, _, row = await self._load_ir(logical_id, version)
        if row is None:
            await self._logical.upsert(
                self._ir_row(
                    ir,
                    resolved_version,
                    status=LogicalScenarioStatus.PUBLISHED,
                    created_by=SYSTEM_CREATOR,
                )
            )
        else:
            updated = await self._logical.update_status(
                logical_id, resolved_version, LogicalScenarioStatus.PUBLISHED.value
            )
            if not updated:
                raise ResourceNotFoundError("逻辑场景不存在", details={"logical_id": logical_id})
        return PublishData(
            logical_id=logical_id,
            version=resolved_version,
            status=LogicalScenarioStatus.PUBLISHED.value,
        )

    # =================================================================
    # 二、生成任务
    # =================================================================

    async def create_generation_task(
        self, request: GenerateRequest, user_id: str
    ) -> Any:
        """创建异步生成任务（校验请求早失败，返回 pending 任务）。"""
        ir, resolved_version, _, _ = await self._load_ir(request.logical_id, request.logical_version)
        try:
            ensure_overrides_shape(request.overrides)
        except ValueError as exc:
            raise InvalidParameterError(str(exc), details={"overrides": request.overrides}) from exc
        if request.strategy == GenerationStrategy.REPLAY_MUTATION and not request.overrides.get("base"):
            raise InvalidParameterError("replay_mutation 必须提供 overrides.base 参数基准")
        if request.strategy == GenerationStrategy.AI_GENERATED and not request.overrides.get("candidates"):
            raise InvalidParameterError("ai_generated 必须提供 overrides.candidates 候选参数集")
        if request.count > self._settings.scene_generation_max_count:
            raise InvalidParameterError(
                f"count 超过上限 {self._settings.scene_generation_max_count}",
                details={"count": request.count},
            )
        seed = request.seed or random.SystemRandom().randint(1, 2**31 - 1)
        return await self._tasks.create(
            logical_id=ir.logical_id,
            logical_version=resolved_version,
            strategy=request.strategy.value,
            profile=request.profile.value,
            count=request.count,
            seed=seed,
            overrides=dict(request.overrides),
            status=GenerationTaskStatus.PENDING.value,
            progress=0.0,
            produced=0,
            duplicates=0,
            rejected=0,
            zone_quota={},
            zone_counts={},
            created_by=user_id,
        )

    async def execute_generation_task(self, task_id: UUID) -> None:
        """执行生成任务（BackgroundTasks 调用；异常写回 failed 不抛出）。"""
        task = await self._tasks.get(task_id)
        if task is None:
            logger.warning("generation_task_missing", task_id=str(task_id))
            return
        if _enum_value(task.status) not in {
            GenerationTaskStatus.PENDING.value,
            GenerationTaskStatus.RUNNING.value,
        }:
            return
        await self._tasks.update(task_id, status=GenerationTaskStatus.RUNNING.value, progress=0.01)
        try:
            ir, resolved_version, _, _ = await self._load_ir(task.logical_id, task.logical_version)
            options = GenerationOptions(
                logical_version=resolved_version,
                count=int(task.count),
                strategy=_enum_value(task.strategy),
                profile=_enum_value(task.profile),
                seed=int(task.seed),
                overrides=dict(task.overrides or {}),
            )
            progress_state = {"last": 0.0}
            loop = asyncio.get_running_loop()

            def on_progress(done: int, total: int) -> None:
                ratio = min(max(done / max(total, 1), 0.0), 0.99)
                if ratio - progress_state["last"] < PROGRESS_STEP:
                    return
                progress_state["last"] = ratio
                try:
                    asyncio.run_coroutine_threadsafe(
                        self._tasks.update(task_id, progress=round(ratio, 3)), loop
                    )
                except RuntimeError:  # pragma: no cover - 事件循环已关闭
                    pass

            outcome = await asyncio.to_thread(
                self._generator.generate, ir, options, on_progress=on_progress
            )
            rows = [{**row, "task_id": task_id} for row in outcome.rows]
            created = await self._concrete.create_many(rows) if rows else []
            if len(created) != len(rows):
                logger.info(
                    "generation_rows_skipped",
                    task_id=str(task_id),
                    requested=len(rows),
                    inserted=len(created),
                )
            await self._tasks.update(
                task_id,
                status=GenerationTaskStatus.SUCCEEDED.value,
                progress=1.0,
                produced=len(created),
                duplicates=outcome.duplicates,
                rejected=outcome.rejected,
                zone_quota=outcome.zone_quota,
                zone_counts=outcome.zone_counts,
                error_message=None,
            )
        except Exception as exc:
            logger.exception("generation_task_failed", task_id=str(task_id))
            await self._tasks.update(
                task_id,
                status=GenerationTaskStatus.FAILED.value,
                error_message=str(exc)[:500],
            )

    async def get_task(self, task_id: UUID) -> Any:
        task = await self._tasks.get(task_id)
        if task is None:
            raise ResourceNotFoundError("生成任务不存在", details={"task_id": str(task_id)})
        return task

    async def list_tasks(
        self, *, page: int, page_size: int, status: str | None = None
    ) -> tuple[list[Any], int]:
        return await self._tasks.list_page(page=page, page_size=page_size, status=status)

    async def cancel_task(self, task_id: UUID) -> Any:
        """取消任务：仅 pending 可取消；运行中返回 3003（进程内任务无法安全中断）。"""
        task = await self._tasks.get(task_id)
        if task is None:
            raise ResourceNotFoundError("生成任务不存在", details={"task_id": str(task_id)})
        status = _enum_value(task.status)
        if status == GenerationTaskStatus.PENDING.value:
            return await self._tasks.update(task_id, status=GenerationTaskStatus.CANCELED.value)
        if status in {
            GenerationTaskStatus.SUCCEEDED.value,
            GenerationTaskStatus.FAILED.value,
            GenerationTaskStatus.CANCELED.value,
        }:
            raise ResourceStateConflictError(
                f"任务已结束（{status}），不可取消", details={"task_id": str(task_id)}
            )
        raise ResourceStateConflictError(
            "任务执行中不可取消（进程内生成不支持安全中断）", details={"task_id": str(task_id)}
        )

    # =================================================================
    # 三、具体场景
    # =================================================================

    async def list_concrete(
        self,
        *,
        page: int,
        page_size: int,
        logical_id: str | None = None,
        rarity_zone: str | None = None,
        risk_level: str | None = None,
        status: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows, total = await self._concrete.list_page(
            page=page,
            page_size=page_size,
            logical_id=logical_id,
            rarity_zone=rarity_zone,
            risk_level=risk_level,
            status=status,
        )
        return [self._concrete_dict(row) for row in rows], total

    async def get_concrete(self, scenario_id: UUID) -> dict[str, Any]:
        row = await self._concrete.get(scenario_id)
        if row is None:
            raise ResourceNotFoundError("具体场景不存在", details={"scenario_id": str(scenario_id)})
        return self._concrete_dict(row, detail=True)

    async def render_concrete(self, scenario_id: UUID) -> dict[str, Any]:
        """按需渲染：以存储参数重新编译，返回 SceneConfig 与闸门明细。"""
        row = await self._concrete.get(scenario_id)
        if row is None:
            raise ResourceNotFoundError("具体场景不存在", details={"scenario_id": str(scenario_id)})
        ir = await self._ir_or_none(row.logical_id, row.logical_version)
        if ir is None:
            return {
                "scenario_id": row.scenario_id,
                "logical_id": row.logical_id,
                "content_hash": str(row.content_hash).strip(),
                "scene_config": dict(row.scene_config or {}),
                "gates": [],
                "passed": bool(row.scene_config),
            }
        values = dict(row.param_values or {})
        compilation = compile_scene(
            ir,
            values,
            max_duration=self._settings.scene_duration_max_seconds,
            allowed_maps=self._allowed_maps,
        )
        return {
            "scenario_id": row.scenario_id,
            "logical_id": row.logical_id,
            "content_hash": str(row.content_hash).strip(),
            "scene_config": compilation.config,
            "gates": [
                GateItem(gate=item.gate, passed=item.passed, message=item.message)
                for item in compilation.report.gates
            ],
            "passed": compilation.passed,
        }

    async def batch_validate(self, scenario_ids: Sequence[UUID]) -> BatchValidateData:
        """批量复检：逐条重跑 G1-G7，通过写 validated，失败写 quarantined。"""
        items: list[BatchValidateItem] = []
        valid_ids: list[UUID] = []
        invalid_ids: list[UUID] = []
        for scenario_id in scenario_ids:
            row = await self._concrete.get(scenario_id)
            if row is None:
                items.append(
                    BatchValidateItem(
                        scenario_id=scenario_id,
                        passed=False,
                        failures=["3001: 具体场景不存在"],
                        status=ConcreteScenarioStatus.QUARANTINED.value,
                    )
                )
                continue
            passed, failures = await self._validate_concrete_row(row)
            status = (
                ConcreteScenarioStatus.VALIDATED.value
                if passed
                else ConcreteScenarioStatus.QUARANTINED.value
            )
            items.append(
                BatchValidateItem(
                    scenario_id=scenario_id, passed=passed, failures=failures, status=status
                )
            )
            (valid_ids if passed else invalid_ids).append(scenario_id)
        if valid_ids:
            await self._concrete.update_statuses(valid_ids, ConcreteScenarioStatus.VALIDATED.value)
        if invalid_ids:
            await self._concrete.update_statuses(invalid_ids, ConcreteScenarioStatus.QUARANTINED.value)
        return BatchValidateData(
            validated=len(valid_ids), quarantined=len(invalid_ids), items=items
        )

    async def _validate_concrete_row(self, row: Any) -> tuple[bool, list[str]]:
        ir = await self._ir_or_none(row.logical_id, row.logical_version)
        if ir is None:
            return False, ["G1: 逻辑场景 IR 不存在，无法复检"]
        try:
            compilation = compile_scene(
                ir,
                dict(row.param_values or {}),
                max_duration=self._settings.scene_duration_max_seconds,
                allowed_maps=self._allowed_maps,
            )
        except ValueError as exc:
            return False, [f"G1: {exc}"]
        return compilation.passed, list(compilation.report.failures)

    # =================================================================
    # 四、判据 / 测试用例
    # =================================================================

    @staticmethod
    def oracles() -> list[dict[str, Any]]:
        return list_oracles()

    async def create_testcase(self, request: TestCaseCreateRequest, user_id: str) -> TestCaseData:
        """创建测试用例（判据/逻辑场景必须存在，case_key 唯一）。"""
        del user_id  # 用例表未预留 created_by 列（审计由应用日志承载）
        if request.oracle_ref not in ORACLE_BY_REF:
            raise InvalidParameterError(
                f"未知判据: {request.oracle_ref}", details={"oracle_ref": request.oracle_ref}
            )
        _, resolved_version, _, _ = await self._load_ir(request.logical_id, request.logical_version)
        if request.scenario_id is not None and await self._concrete.get(request.scenario_id) is None:
            raise ResourceNotFoundError(
                "具体场景不存在", details={"scenario_id": str(request.scenario_id)}
            )
        try:
            row = await self._testcases.create(
                case_key=request.case_key,
                name=request.name,
                logical_id=request.logical_id,
                logical_version=resolved_version,
                scenario_id=request.scenario_id,
                objective=request.objective,
                preconditions=dict(request.preconditions),
                steps=list(request.steps),
                platforms=list(request.platforms),
                oracle_ref=request.oracle_ref,
                timeout_seconds=request.timeout_seconds,
                tags=list(request.tags),
                status=TestCaseStatus.DRAFT.value,
            )
        except IntegrityError as exc:  # pragma: no cover - 仓储已转 3002
            raise InvalidParameterError("测试用例创建失败", details={"case_key": request.case_key}) from exc
        return self._testcase_dict(row)

    async def list_testcases(
        self,
        *,
        page: int,
        page_size: int,
        logical_id: str | None = None,
        status: str | None = None,
    ) -> tuple[list[TestCaseData], int]:
        rows, total = await self._testcases.list_page(
            page=page, page_size=page_size, logical_id=logical_id, status=status
        )
        return [self._testcase_dict(row) for row in rows], total

    async def get_testcase(self, test_case_id: UUID) -> TestCaseData:
        row = await self._testcases.get(test_case_id)
        if row is None:
            raise ResourceNotFoundError(
                "测试用例不存在", details={"test_case_id": str(test_case_id)}
            )
        return self._testcase_dict(row)

    # =================================================================
    # 五、测试集 / 基线
    # =================================================================

    async def create_suite(self, request: SuiteCreateRequest, user_id: str) -> SuiteDetailData:
        del user_id  # 测试集暂不记录创建者（schema 未预留列，0007 不扩展）
        row = await self._suites.create(
            name=request.name,
            description=request.description,
            status=SuiteStatus.DRAFT.value,
        )
        return await self._suite_detail(row)

    async def list_suites(
        self, *, page: int, page_size: int
    ) -> tuple[list[SuiteData], int]:
        rows, total = await self._suites.list_page(page=page, page_size=page_size)
        return [self._suite_summary(row) for row in rows], total

    async def get_suite(self, suite_id: UUID) -> SuiteDetailData:
        row = await self._suites.get(suite_id)
        if row is None:
            raise ResourceNotFoundError("测试集不存在", details={"suite_id": str(suite_id)})
        return await self._suite_detail(row)

    async def add_suite_items(
        self, suite_id: UUID, request: SuiteItemsRequest
    ) -> SuiteDetailData:
        """加条目（仅 draft 可改；引用必须存在，item_id 冲突则覆盖）。"""
        row = await self._suites.get(suite_id)
        if row is None:
            raise ResourceNotFoundError("测试集不存在", details={"suite_id": str(suite_id)})
        if _enum_value(row.status) != SuiteStatus.DRAFT.value:
            raise ResourceStateConflictError(
                "测试集已锁定/归档，条目不可变更", details={"suite_id": str(suite_id)}
            )
        validated: list[dict[str, Any]] = []
        for item in request.items:
            await self._ensure_item_ref(item)
            validated.append(
                {
                    "item_id": item.item_id,
                    "item_type": item.item_type.value,
                    "ref_id": item.ref_id,
                    "weight": item.weight,
                }
            )
        await self._suites.add_items(
            suite_id,
            [
                {
                    "item_id": item["item_id"],
                    "item_type": item["item_type"],
                    "ref_id": item["ref_id"],
                    "weight": item["weight"],
                }
                for item in validated
            ],
        )
        return await self.get_suite(suite_id)

    async def _ensure_item_ref(self, item: SuiteItemInput) -> None:
        if item.item_type == SuiteItemType.TEST_CASE:
            found = await self._testcases.get_by_key(item.ref_id)
            if found is None:
                try:
                    found = await self._testcases.get(UUID(item.ref_id))
                except ValueError:
                    found = None
            if found is None:
                raise ResourceNotFoundError(
                    "测试用例不存在", details={"case_key": item.ref_id}
                )
            return
        try:
            scenario_id = UUID(item.ref_id)
        except ValueError as exc:
            raise InvalidParameterError(
                "concrete_scenario 的 ref_id 必须是 UUID", details={"ref_id": item.ref_id}
            ) from exc
        if await self._concrete.get(scenario_id) is None:
            raise ResourceNotFoundError(
                "具体场景不存在", details={"scenario_id": item.ref_id}
            )

    async def lock_suite(
        self, suite_id: UUID, *, baseline_name: str | None, metrics: dict[str, Any], user_id: str
    ) -> SuiteLockData:
        """锁定测试集（写 lock_hash/locked_at），可选创建基线快照。"""
        row = await self._suites.get(suite_id)
        if row is None:
            raise ResourceNotFoundError("测试集不存在", details={"suite_id": str(suite_id)})
        if _enum_value(row.status) == SuiteStatus.LOCKED.value:
            raise ResourceStateConflictError(
                "测试集已锁定", details={"suite_id": str(suite_id)}
            )
        items = await self._suites.list_items(suite_id)
        if not items:
            raise ResourceStateConflictError("测试集为空，不能锁定", details={"suite_id": str(suite_id)})
        fingerprints = [
            {
                "item_id": item.item_id,
                "item_type": _enum_value(item.item_type),
                "ref_id": item.ref_id,
                "weight": float(item.weight),
            }
            for item in items
        ]
        digest = suite_hash(fingerprints)
        locked_at = utcnow()
        await self._suites.update(
            suite_id,
            status=SuiteStatus.LOCKED.value,
            lock_hash=digest,
            locked_at=locked_at,
            locked_by=user_id,
        )
        if baseline_name:
            await self._suites.create_baseline(
                suite_id=suite_id,
                name=baseline_name,
                suite_hash=digest,
                metrics={**dict(metrics), "items": fingerprints},
                locked_by=user_id,
            )
        return SuiteLockData(
            suite_id=suite_id,
            status=SuiteStatus.LOCKED.value,
            lock_hash=digest,
            item_count=len(fingerprints),
            baseline_name=baseline_name,
        )

    async def suite_diff(
        self, suite_id: UUID, *, baseline_id: UUID | None = None
    ) -> SuiteDiffData:
        """当前条目与基线（或锁定快照）的差异。"""
        row = await self._suites.get(suite_id)
        if row is None:
            raise ResourceNotFoundError("测试集不存在", details={"suite_id": str(suite_id)})
        items = await self._suites.list_items(suite_id)
        current = {
            item.item_id: {
                "item_type": _enum_value(item.item_type),
                "ref_id": item.ref_id,
                "weight": round(float(item.weight), 6),
            }
            for item in items
        }
        current_hash = suite_hash(
            [{"item_id": key, **value} for key, value in current.items()]
        )
        baseline: Any = None
        if baseline_id is not None:
            baselines = await self._suites.list_baselines(suite_id)
            baseline = next((item for item in baselines if item.baseline_id == baseline_id), None)
            if baseline is None:
                raise ResourceNotFoundError(
                    "基线不存在", details={"baseline_id": str(baseline_id)}
                )
        else:
            baselines = await self._suites.list_baselines(suite_id)
            baseline = baselines[0] if baselines else None
        if baseline is None:
            return SuiteDiffData(
                suite_id=suite_id,
                current_hash=current_hash,
                baseline_hash=None,
                identical=False,
            )
        previous: dict[str, dict[str, Any]] = {}
        # metrics["items"] 兼容两种存储形态：列表（lock 写入）或映射（历史数据）
        raw_items = dict(baseline.metrics or {}).get("items") or {}
        if isinstance(raw_items, list):
            previous = {
                str(item.get("item_id")): {
                    "item_type": _enum_value(item.get("item_type")),
                    "ref_id": str(item.get("ref_id")),
                    "weight": round(float(item.get("weight", 0.0)), 6),
                }
                for item in raw_items
            }
        elif isinstance(raw_items, dict):
            previous = {
                str(key): {
                    "item_type": _enum_value(value.get("item_type")),
                    "ref_id": str(value.get("ref_id")),
                    "weight": round(float(value.get("weight", 0.0)), 6),
                }
                for key, value in raw_items.items()
            }
        added = sorted(set(current) - set(previous))
        removed = sorted(set(previous) - set(current))
        changed = sorted(
            key for key in set(current) & set(previous) if current[key] != previous[key]
        )
        return SuiteDiffData(
            suite_id=suite_id,
            current_hash=current_hash,
            baseline_hash=str(baseline.suite_hash).strip(),
            identical=current_hash == str(baseline.suite_hash).strip(),
            added=added,
            removed=removed,
            changed=changed,
        )

    async def list_baselines(self, suite_id: UUID) -> list[Any]:
        if await self._suites.get(suite_id) is None:
            raise ResourceNotFoundError("测试集不存在", details={"suite_id": str(suite_id)})
        return await self._suites.list_baselines(suite_id)

    # =================================================================
    # 六、覆盖分析
    # =================================================================

    async def coverage_overview(self, logical_id: str | None = None) -> CoverageOverviewData:
        zone_counts = await self._concrete.zone_counts(logical_id=logical_id)
        risk_counts = await self._concrete.risk_counts()
        total = sum(zone_counts.values())
        ratio = {
            zone: (round(count / total, 4) if total else 0.0)
            for zone, count in zone_counts.items()
        }
        return CoverageOverviewData(
            logical_id=logical_id,
            total=total,
            zone_counts=zone_counts,
            zone_ratio=ratio,
            risk_counts=risk_counts,
        )

    async def coverage_gaps(self, profile: str = GenerationProfile.EXPLORATION.value) -> CoverageGapListData:
        """覆盖盲区：按剖面目标配额与已产出的 Z3/Z4 数量计算缺口。"""
        ratios = PROFILE_QUOTAS.get(profile)
        if ratios is None:
            raise InvalidParameterError(f"未知剖面: {profile}", details={"profile": profile})
        items: list[CoverageGapData] = []
        for ir in CATALOG:
            counts = await self._concrete.zone_counts(logical_id=ir.logical_id)
            total = sum(counts.values())
            base = max(total, 50)
            for zone in ("Z3", "Z4"):
                target = round(base * ratios.get(zone, 0.0))
                current = int(counts.get(zone, 0))
                if target <= current:
                    continue
                items.append(
                    CoverageGapData(
                        logical_id=ir.logical_id,
                        name=ir.name,
                        zone=zone,
                        count=current,
                        target=target,
                        deficit=target - current,
                        recommendation=(
                            f"POST /api/v1/scene/generate strategy=distribution "
                            f"profile={profile} overrides.target_zone={zone} "
                            f"count={target - current}"
                        ),
                    )
                )
        items.sort(key=lambda item: (-item.deficit, item.logical_id))
        return CoverageGapListData(profile=profile, items=items[:100])

    # =================================================================
    # 内部：IR 读取 / 持久化 / 序列化
    # =================================================================

    async def _load_ir(
        self, logical_id: str, version: str | None = None
    ) -> tuple[SceneIR, str, str, Any | None]:
        """读取 IR（DB 优先、目录兜底）；返回 (ir, version, origin, row)。"""
        row = None
        try:
            row = await self._logical.get(logical_id, version)
        except (SQLAlchemyError, OSError):  # pragma: no cover - 未初始化 schema 时目录兜底
            logger.warning("logical_scenario_read_failed", logical_id=logical_id)
        if row is not None:
            try:
                return SceneIR.model_validate(row.ir_json), row.version, "database", row
            except ValueError:
                logger.error("logical_scenario_ir_invalid", logical_id=logical_id, version=row.version)
        ir = CATALOG_BY_ID.get(logical_id)
        if ir is None or (version is not None and version != CATALOG_VERSION):
            raise ResourceNotFoundError(
                "逻辑场景不存在", details={"logical_id": logical_id, "version": version}
            )
        resolved = version or CATALOG_VERSION
        return ir, resolved, "catalog", None

    async def _ir_or_none(self, logical_id: str, version: str | None) -> SceneIR | None:
        try:
            ir, _, _, _ = await self._load_ir(logical_id, version)
        except ResourceNotFoundError:
            return None
        return ir

    def _ir_row(
        self,
        ir: SceneIR,
        version: str,
        *,
        status: LogicalScenarioStatus = LogicalScenarioStatus.PUBLISHED,
        created_by: str = SYSTEM_CREATOR,
    ) -> dict[str, Any]:
        return {
            "logical_id": ir.logical_id,
            "version": version,
            "name": ir.name,
            "category": ir.category,
            "scene_type": ir.scene_type,
            "description": ir.description,
            "ir_version": IR_VERSION,
            "ir_json": ir.model_dump(mode="json"),
            "status": status.value,
            "content_hash": ir_content_hash(ir),
            "tags": list(ir.tags),
            "created_by": created_by,
        }

    def _ir_summary(self, ir: SceneIR) -> LogicalScenarioSummary:
        return LogicalScenarioSummary(
            logical_id=ir.logical_id,
            version=CATALOG_VERSION,
            name=ir.name,
            category=ir.category,
            scene_type=ir.scene_type,
            description=ir.description,
            status=LogicalScenarioStatus.PUBLISHED.value,
            ir_version=ir.ir_version,
            oracle_ref=ir.oracle_ref,
            parameter_count=len(ir.parameters),
            tags=list(ir.tags),
            content_hash=ir_content_hash(ir),
            origin="catalog",
        )

    def _ir_detail(self, ir: SceneIR, version: str, origin: str) -> LogicalScenarioDetail:
        return LogicalScenarioDetail(
            logical_id=ir.logical_id,
            version=version,
            name=ir.name,
            category=ir.category,
            scene_type=ir.scene_type,
            description=ir.description,
            status=LogicalScenarioStatus.PUBLISHED.value,
            ir_version=ir.ir_version,
            oracle_ref=ir.oracle_ref,
            parameter_count=len(ir.parameters),
            tags=list(ir.tags),
            content_hash=ir_content_hash(ir),
            origin=origin,  # type: ignore[arg-type]
            ir=ir.model_dump(mode="json"),
            parameters=[
                {
                    "name": parameter.name,
                    "distribution": parameter.distribution.model_dump(mode="json"),
                    "unit": parameter.unit,
                    "description": parameter.description,
                }
                for parameter in ir.parameters
            ],
            bindings=[binding.model_dump(mode="json") for binding in ir.bindings],
            constraints=[constraint.model_dump(mode="json") for constraint in ir.constraints],
            correlations=[pair.model_dump(mode="json") for pair in ir.correlations],
            allowed_modes=list(ir.allowed_modes),
        )

    def _row_summary(self, row: Any) -> LogicalScenarioSummary:
        ir_json = dict(row.ir_json or {})
        return LogicalScenarioSummary(
            logical_id=row.logical_id,
            version=row.version,
            name=row.name,
            category=row.category,
            scene_type=row.scene_type,
            description=row.description or "",
            status=_enum_value(row.status),
            ir_version=row.ir_version,
            oracle_ref=str(ir_json.get("oracle_ref") or ""),
            parameter_count=len(ir_json.get("parameters") or []),
            tags=list(row.tags or []),
            content_hash=str(row.content_hash).strip(),
            origin="database",
            update_time=getattr(row, "update_time", None),
        )

    @staticmethod
    def _concrete_dict(row: Any, *, detail: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "scenario_id": row.scenario_id,
            "logical_id": row.logical_id,
            "logical_version": row.logical_version,
            "name": row.name,
            "rarity_p": float(row.rarity_p),
            "rarity_zone": _enum_value(row.rarity_zone),
            "criticality_ci": float(row.criticality_ci),
            "criticality_level": _enum_value(row.criticality_level),
            "risk_level": _enum_value(row.risk_level),
            "avoidability": _enum_value(row.avoidability),
            "vil_safe": bool(row.vil_safe),
            "status": _enum_value(row.status),
            "content_hash": str(row.content_hash).strip(),
            "seed": int(row.seed),
            "create_time": getattr(row, "create_time", None),
        }
        if detail:
            payload.update(
                {
                    "param_values": dict(row.param_values or {}),
                    "param_vec": [float(value) for value in (row.param_vec or [])],
                    "allowed_modes": list(row.allowed_modes or []),
                    "generator": dict(row.generator or {}),
                    "validity": dict(row.validity or {}),
                    "scene_config": dict(row.scene_config or {}),
                }
            )
        return payload

    @staticmethod
    def _testcase_dict(row: Any) -> TestCaseData:
        return TestCaseData(
            case_key=row.case_key,
            name=row.name,
            logical_id=row.logical_id,
            logical_version=row.logical_version,
            scenario_id=row.scenario_id,
            objective=row.objective,
            preconditions=dict(row.preconditions or {}),
            steps=list(row.steps or []),
            platforms=list(row.platforms or []),
            oracle_ref=row.oracle_ref,
            timeout_seconds=int(row.timeout_seconds),
            tags=list(row.tags or []),
            status=_enum_value(row.status),
            test_case_id=row.test_case_id,
            create_time=getattr(row, "create_time", None),
        )

    async def _suite_detail(self, row: Any) -> SuiteDetailData:
        items: list[SuiteItemData] = []
        for item in await self._suites.list_items(row.suite_id):
            items.append(
                SuiteItemData(
                    item_id=item.item_id,
                    item_type=_enum_value(item.item_type),
                    ref_id=item.ref_id,
                    weight=float(item.weight),
                )
            )
        return SuiteDetailData(items=items, **self._suite_payload(row, item_count=len(items)))

    @staticmethod
    def _suite_summary(row: Any) -> SuiteData:
        return SuiteData(**SceneEngineService._suite_payload(row, item_count=0))

    @staticmethod
    def _suite_payload(row: Any, *, item_count: int) -> dict[str, Any]:
        return {
            "suite_id": row.suite_id,
            "name": row.name,
            "description": row.description,
            "status": _enum_value(row.status),
            "item_count": item_count,
            "lock_hash": str(row.lock_hash).strip() if row.lock_hash else None,
            "locked_at": getattr(row, "locked_at", None),
            "locked_by": row.locked_by,
            "create_time": getattr(row, "create_time", None),
            "update_time": getattr(row, "update_time", None),
        }


__all__ = ["SceneEngineService", "suite_hash"]
