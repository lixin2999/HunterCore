"""场景引擎路由（逻辑/具体场景、生成、用例、测试集、覆盖）。

路径前缀与 scene-service 共用 ``/api/v1/scene``；本路由必须在 ``scenes.router``
**之前**注册，否则 ``/oracles``、``/testcases``、``/suites`` 会被 ``/{scene_id}`` 吞掉。

通用约定（设计文档 §4.1）：耗时生成返回 ``202 + task_id``；写操作走
``scene:create|update|delete``，生成走 ``scene:execute``，读走 ``scene:read``。
"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

from app.core.dependencies import (
    get_engine_service,
    require_execute_permission,
    require_read_permission,
    require_review_permission,
    require_write_permission,
    trace_request_id,
)
from app.models.scene_engine import (
    ConcreteScenarioStatus,
    GenerationProfile,
    GenerationTaskStatus,
    RarityZone,
    RiskLevel,
)
from app.routers.errors import COMMON_ERRORS
from app.schemas.engine import (
    BaselineData,
    BaselineListData,
    BaselineListResponse,
    BatchValidateRequest,
    BatchValidateResponse,
    CatalogSyncData,
    CatalogSyncResponse,
    ConcreteRenderResponse,
    ConcreteScenarioDetail,
    ConcreteScenarioListData,
    ConcreteScenarioListResponse,
    ConcreteScenarioResponse,
    ConcreteScenarioSummary,
    CoverageGapListResponse,
    CoverageOverviewData,
    CoverageOverviewResponse,
    DryRunRequest,
    GenerateRequest,
    GenerationTaskData,
    GenerationTaskListData,
    GenerationTaskListResponse,
    GenerationTaskResponse,
    LogicalDryRunResponse,
    LogicalPublishResponse,
    LogicalScenarioListData,
    LogicalScenarioListResponse,
    LogicalScenarioResponse,
    LogicalValidationResponse,
    LogicalVersionListData,
    LogicalVersionListResponse,
    OracleData,
    OracleListData,
    OracleListResponse,
    PublishData,
    RenderData,
    SuiteCreateRequest,
    SuiteDiffResponse,
    SuiteItemsRequest,
    SuiteListData,
    SuiteListResponse,
    SuiteLockRequest,
    SuiteLockResponse,
    SuiteResponse,
    TestCaseCreateRequest,
    TestCaseData,
    TestCaseListData,
    TestCaseListResponse,
    TestCaseResponse,
)
from app.services.engine import SceneEngineService

router = APIRouter(prefix="/api/v1/scene", tags=["scene-engine"])


def _task_data(task: object) -> GenerationTaskData:
    """生成任务 ORM → 响应模型（枚举统一转 str）。"""
    zone_quota = {
        str(key): int(value) for key, value in (getattr(task, "zone_quota", None) or {}).items()
    }
    zone_counts = {
        str(key): int(value) for key, value in (getattr(task, "zone_counts", None) or {}).items()
    }
    return GenerationTaskData(
        task_id=task.task_id,
        logical_id=task.logical_id,
        logical_version=task.logical_version,
        strategy=str(getattr(getattr(task, "strategy", ""), "value", getattr(task, "strategy", ""))),
        profile=str(getattr(getattr(task, "profile", ""), "value", getattr(task, "profile", ""))),
        count=int(getattr(task, "count", 0)),
        seed=int(getattr(task, "seed", 0)),
        status=str(getattr(getattr(task, "status", ""), "value", getattr(task, "status", ""))),
        progress=float(getattr(task, "progress", 0.0)),
        produced=int(getattr(task, "produced", 0)),
        duplicates=int(getattr(task, "duplicates", 0)),
        rejected=int(getattr(task, "rejected", 0)),
        zone_quota=zone_quota,
        zone_counts=zone_counts,
        error_message=getattr(task, "error_message", None),
        owner_id=getattr(task, "owner_id", None),
        cancel_requested=bool(getattr(task, "cancel_requested", False)),
        created_by=str(getattr(task, "created_by", "")),
        create_time=getattr(task, "create_time", None),
        update_time=getattr(task, "update_time", None),
    )


# =====================================================================
# 一、逻辑场景
# =====================================================================


@router.get(
    "/logical",
    operation_id="listLogicalScenarios",
    response_model=LogicalScenarioListResponse,
    summary="查询逻辑场景（DB 快照 + 内置目录）",
    responses={**COMMON_ERRORS},
)
async def list_logical_scenarios(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    category: Annotated[str | None, Query(max_length=32, description="分类过滤")] = None,
    keyword: Annotated[str | None, Query(min_length=1, max_length=64, description="ID/名称模糊")] = None,
    include_catalog: Annotated[bool, Query(description="是否包含未同步的内置目录项")] = True,
    limit: Annotated[int, Query(ge=1, le=500, description="返回上限")] = 200,
) -> LogicalScenarioListResponse:
    """逻辑场景列表（DB 快照优先，目录补齐；按 logical_id 去重）。"""
    items = await service.list_logical(
        category=category, keyword=keyword, include_catalog=include_catalog, limit=limit
    )
    data = LogicalScenarioListData(items=items, total=len(items))
    return LogicalScenarioListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/logical/sync",
    operation_id="syncLogicalScenarioCatalog",
    response_model=CatalogSyncResponse,
    summary="同步内置逻辑场景目录到数据库（幂等）",
    responses={**COMMON_ERRORS},
)
async def sync_logical_catalog(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    user_id: Annotated[str, Depends(require_write_permission)],
) -> CatalogSyncResponse:
    """把内置 17 个逻辑场景幂等 upsert 到 scene_engine.logical_scenarios。"""
    payload = await service.sync_catalog(created_by=user_id)
    data = CatalogSyncData(**payload)
    return CatalogSyncResponse(data=data, request_id=trace_request_id())


@router.get(
    "/logical/{logical_id}",
    operation_id="getLogicalScenario",
    response_model=LogicalScenarioResponse,
    summary="逻辑场景详情（含 IR / 参数 / 绑定 / 约束）",
    responses={**COMMON_ERRORS},
)
async def get_logical_scenario(
    logical_id: str,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    version: Annotated[str | None, Query(max_length=16, description="版本；缺省取最新")] = None,
) -> LogicalScenarioResponse:
    detail = await service.get_logical(logical_id, version)
    return LogicalScenarioResponse(data=detail, request_id=trace_request_id())


@router.get(
    "/logical/{logical_id}/versions",
    operation_id="listLogicalScenarioVersions",
    response_model=LogicalVersionListResponse,
    summary="逻辑场景版本列表",
    responses={**COMMON_ERRORS},
)
async def list_logical_versions(
    logical_id: str,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> LogicalVersionListResponse:
    resolved_id, items = await service.list_versions(logical_id)
    data = LogicalVersionListData(logical_id=resolved_id, items=items)
    return LogicalVersionListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/logical/{logical_id}/validate",
    operation_id="validateLogicalScenario",
    response_model=LogicalValidationResponse,
    summary="逻辑场景静态校验（缺省参数 + G1-G7 + 判据存在性）",
    responses={**COMMON_ERRORS},
)
async def validate_logical_scenario(
    logical_id: str,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    version: Annotated[str | None, Query(max_length=16)] = None,
) -> LogicalValidationResponse:
    report = await service.validate(logical_id, version)
    return LogicalValidationResponse(data=report, request_id=trace_request_id())


@router.post(
    "/logical/{logical_id}/dryrun",
    operation_id="dryRunLogicalScenario",
    response_model=LogicalDryRunResponse,
    summary="逻辑场景干跑（返回渲染后的 SceneConfig 预览）",
    responses={**COMMON_ERRORS},
)
async def dry_run_logical_scenario(
    logical_id: str,
    request: DryRunRequest,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> LogicalDryRunResponse:
    result = await service.dryrun(logical_id, version=request.version, values=request.values)
    return LogicalDryRunResponse(data=result, request_id=trace_request_id())


@router.post(
    "/logical/{logical_id}/publish",
    operation_id="publishLogicalScenario",
    response_model=LogicalPublishResponse,
    summary="发布逻辑场景（需 reviewer/admin；写审计哈希链）",
    responses={**COMMON_ERRORS},
)
async def publish_logical_scenario(
    logical_id: str,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    user_id: Annotated[str, Depends(require_review_permission)],
    version: Annotated[str | None, Query(max_length=16)] = None,
) -> LogicalPublishResponse:
    result: PublishData = await service.publish(logical_id, version, user_id=user_id)
    return LogicalPublishResponse(data=result, request_id=trace_request_id())


# =====================================================================
# 二、生成任务
# =====================================================================


@router.post(
    "/generate",
    operation_id="createGenerationTask",
    response_model=GenerationTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="创建异步批量生成任务（202 + task_id）",
    responses={**COMMON_ERRORS},
)
async def create_generation_task(
    request: GenerateRequest,
    background_tasks: BackgroundTasks,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    user_id: Annotated[str, Depends(require_execute_permission)],
) -> GenerationTaskResponse:
    """提交生成任务；响应返回后由 BackgroundTasks 执行（CPU 密集部分在线程池）。"""
    task = await service.create_generation_task(request, user_id)
    background_tasks.add_task(service.execute_generation_task, task.task_id)
    return GenerationTaskResponse(data=_task_data(task), request_id=trace_request_id())


@router.get(
    "/generate",
    operation_id="listGenerationTasks",
    response_model=GenerationTaskListResponse,
    summary="查询生成任务列表",
    responses={**COMMON_ERRORS},
)
async def list_generation_tasks(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    task_status: Annotated[
        GenerationTaskStatus | None, Query(alias="status", description="任务状态过滤")
    ] = None,
) -> GenerationTaskListResponse:
    rows, total = await service.list_tasks(
        page=page,
        page_size=page_size,
        status=task_status.value if task_status is not None else None,
    )
    data = GenerationTaskListData(items=[_task_data(row) for row in rows], total=total)
    return GenerationTaskListResponse(data=data, request_id=trace_request_id())


@router.get(
    "/generate/{task_id}",
    operation_id="getGenerationTask",
    response_model=GenerationTaskResponse,
    summary="查询生成任务详情（进度/实产/分区配额）",
    responses={**COMMON_ERRORS},
)
async def get_generation_task(
    task_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> GenerationTaskResponse:
    task = await service.get_task(task_id)
    return GenerationTaskResponse(data=_task_data(task), request_id=trace_request_id())


@router.post(
    "/generate/{task_id}/cancel",
    operation_id="cancelGenerationTask",
    response_model=GenerationTaskResponse,
    summary="取消生成任务（pending 立即取消；running 置 cancel_requested 协作式中止）",
    responses={**COMMON_ERRORS},
)
async def cancel_generation_task(
    task_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_write_permission)],
) -> GenerationTaskResponse:
    task = await service.cancel_task(task_id)
    return GenerationTaskResponse(data=_task_data(task), request_id=trace_request_id())


# =====================================================================
# 三、具体场景
# =====================================================================


@router.get(
    "/concrete",
    operation_id="listConcreteScenarios",
    response_model=ConcreteScenarioListResponse,
    summary="查询具体场景（稀有度/风险/状态过滤）",
    responses={**COMMON_ERRORS},
)
async def list_concrete_scenarios(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    logical_id: Annotated[str | None, Query(max_length=48)] = None,
    rarity_zone: Annotated[RarityZone | None, Query(description="稀有度分区")] = None,
    risk_level: Annotated[RiskLevel | None, Query(description="风险等级")] = None,
    scenario_status: Annotated[
        ConcreteScenarioStatus | None, Query(alias="status", description="具体场景状态")
    ] = None,
) -> ConcreteScenarioListResponse:
    items, total = await service.list_concrete(
        page=page,
        page_size=page_size,
        logical_id=logical_id,
        rarity_zone=rarity_zone.value if rarity_zone is not None else None,
        risk_level=risk_level.value if risk_level is not None else None,
        status=scenario_status.value if scenario_status is not None else None,
    )
    data = ConcreteScenarioListData(
        items=[ConcreteScenarioSummary(**item) for item in items],
        total=total,
        page=page,
        page_size=page_size,
    )
    return ConcreteScenarioListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/concrete/batch-validate",
    operation_id="batchValidateConcreteScenarios",
    response_model=BatchValidateResponse,
    summary="具体场景批量复检（通过 validated / 失败 quarantined）",
    responses={**COMMON_ERRORS},
)
async def batch_validate_concrete_scenarios(
    request: BatchValidateRequest,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_write_permission)],
) -> BatchValidateResponse:
    result = await service.batch_validate(request.scenario_ids)
    return BatchValidateResponse(data=result, request_id=trace_request_id())


@router.get(
    "/concrete/{scenario_id}",
    operation_id="getConcreteScenario",
    response_model=ConcreteScenarioResponse,
    summary="具体场景详情（参数向量 + 可渲染 config）",
    responses={**COMMON_ERRORS},
)
async def get_concrete_scenario(
    scenario_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> ConcreteScenarioResponse:
    payload = await service.get_concrete(scenario_id)
    return ConcreteScenarioResponse(
        data=ConcreteScenarioDetail(**payload), request_id=trace_request_id()
    )


@router.post(
    "/concrete/{scenario_id}/render",
    operation_id="renderConcreteScenario",
    response_model=ConcreteRenderResponse,
    summary="按需渲染具体场景（返回可直接下发的 SceneConfig）",
    responses={**COMMON_ERRORS},
)
async def render_concrete_scenario(
    scenario_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> ConcreteRenderResponse:
    payload = await service.render_concrete(scenario_id)
    return ConcreteRenderResponse(data=RenderData(**payload), request_id=trace_request_id())


# =====================================================================
# 四、判据 / 测试用例
# =====================================================================


@router.get(
    "/oracles",
    operation_id="listOracles",
    response_model=OracleListResponse,
    summary="判据目录（版本化 ref，代码即事实来源）",
)
async def list_oracles_endpoint(
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> OracleListResponse:
    items = [OracleData(**item) for item in SceneEngineService.oracles()]
    return OracleListResponse(
        data=OracleListData(items=items, total=len(items)), request_id=trace_request_id()
    )


@router.get(
    "/testcases",
    operation_id="listTestCases",
    response_model=TestCaseListResponse,
    summary="查询测试用例",
    responses={**COMMON_ERRORS},
)
async def list_test_cases(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    logical_id: Annotated[str | None, Query(max_length=48)] = None,
    case_status: Annotated[str | None, Query(alias="status", max_length=16)] = None,
) -> TestCaseListResponse:
    items, total = await service.list_testcases(
        page=page, page_size=page_size, logical_id=logical_id, status=case_status
    )
    data = TestCaseListData(items=items, total=total, page=page, page_size=page_size)
    return TestCaseListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/testcases",
    operation_id="createTestCase",
    response_model=TestCaseResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建测试用例（ISO 34505：目标/前置/步骤/平台/判据/超时）",
    responses={**COMMON_ERRORS},
)
async def create_test_case(
    request: TestCaseCreateRequest,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    user_id: Annotated[str, Depends(require_write_permission)],
) -> TestCaseResponse:
    data = await service.create_testcase(request, user_id)
    return TestCaseResponse(data=data, request_id=trace_request_id())


@router.get(
    "/testcases/{test_case_id}",
    operation_id="getTestCase",
    response_model=TestCaseResponse,
    summary="测试用例详情",
    responses={**COMMON_ERRORS},
)
async def get_test_case(
    test_case_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> TestCaseResponse:
    data: TestCaseData = await service.get_testcase(test_case_id)
    return TestCaseResponse(data=data, request_id=trace_request_id())


# =====================================================================
# 五、测试集 / 基线
# =====================================================================


@router.get(
    "/suites",
    operation_id="listScenarioSuites",
    response_model=SuiteListResponse,
    summary="查询测试集",
    responses={**COMMON_ERRORS},
)
async def list_scenario_suites(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
) -> SuiteListResponse:
    items, total = await service.list_suites(page=page, page_size=page_size)
    data = SuiteListData(items=items, total=total, page=page, page_size=page_size)
    return SuiteListResponse(data=data, request_id=trace_request_id())


@router.post(
    "/suites",
    operation_id="createScenarioSuite",
    response_model=SuiteResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建测试集",
    responses={**COMMON_ERRORS},
)
async def create_scenario_suite(
    request: SuiteCreateRequest,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    user_id: Annotated[str, Depends(require_write_permission)],
) -> SuiteResponse:
    data = await service.create_suite(request, user_id)
    return SuiteResponse(data=data, request_id=trace_request_id())


@router.get(
    "/suites/{suite_id}",
    operation_id="getScenarioSuite",
    response_model=SuiteResponse,
    summary="测试集详情（含条目）",
    responses={**COMMON_ERRORS},
)
async def get_scenario_suite(
    suite_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> SuiteResponse:
    data = await service.get_suite(suite_id)
    return SuiteResponse(data=data, request_id=trace_request_id())


@router.post(
    "/suites/{suite_id}/items",
    operation_id="addScenarioSuiteItems",
    response_model=SuiteResponse,
    summary="测试集加条目（幂等：item_id 冲突覆盖；锁定后拒绝）",
    responses={**COMMON_ERRORS},
)
async def add_scenario_suite_items(
    suite_id: UUID,
    request: SuiteItemsRequest,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_write_permission)],
) -> SuiteResponse:
    data = await service.add_suite_items(suite_id, request)
    return SuiteResponse(data=data, request_id=trace_request_id())


@router.post(
    "/suites/{suite_id}/lock",
    operation_id="lockScenarioSuite",
    response_model=SuiteLockResponse,
    summary="锁定测试集（需 reviewer/admin；单事务写 lock_hash + 可选基线）",
    responses={**COMMON_ERRORS},
)
async def lock_scenario_suite(
    suite_id: UUID,
    request: SuiteLockRequest,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    user_id: Annotated[str, Depends(require_review_permission)],
) -> SuiteLockResponse:
    data = await service.lock_suite(
        suite_id,
        baseline_name=request.baseline_name,
        metrics=request.metrics,
        user_id=user_id,
    )
    return SuiteLockResponse(data=data, request_id=trace_request_id())


@router.get(
    "/suites/{suite_id}/diff",
    operation_id="diffScenarioSuite",
    response_model=SuiteDiffResponse,
    summary="测试集差异（当前条目 vs 基线/锁定快照）",
    responses={**COMMON_ERRORS},
)
async def diff_scenario_suite(
    suite_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    baseline_id: Annotated[UUID | None, Query(description="基线 ID；缺省取最新")] = None,
) -> SuiteDiffResponse:
    data = await service.suite_diff(suite_id, baseline_id=baseline_id)
    return SuiteDiffResponse(data=data, request_id=trace_request_id())


@router.get(
    "/suites/{suite_id}/baselines",
    operation_id="listScenarioSuiteBaselines",
    response_model=BaselineListResponse,
    summary="测试集基线列表",
    responses={**COMMON_ERRORS},
)
async def list_scenario_suite_baselines(
    suite_id: UUID,
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
) -> BaselineListResponse:
    rows = await service.list_baselines(suite_id)
    items = [
        BaselineData(
            baseline_id=row.baseline_id,
            suite_id=row.suite_id,
            name=row.name,
            suite_hash=str(row.suite_hash).strip(),
            metrics=dict(row.metrics or {}),
            locked_by=row.locked_by,
            create_time=getattr(row, "create_time", None),
        )
        for row in rows
    ]
    return BaselineListResponse(
        data=BaselineListData(items=items), request_id=trace_request_id()
    )


# =====================================================================
# 六、覆盖分析
# =====================================================================


@router.get(
    "/coverage/overview",
    operation_id="getCoverageOverview",
    response_model=CoverageOverviewResponse,
    summary="覆盖总览（Z1-Z4 / L1-L4 计数与占比）",
    responses={**COMMON_ERRORS},
)
async def get_coverage_overview(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    logical_id: Annotated[str | None, Query(max_length=48)] = None,
) -> CoverageOverviewResponse:
    data: CoverageOverviewData = await service.coverage_overview(logical_id)
    return CoverageOverviewResponse(data=data, request_id=trace_request_id())


@router.get(
    "/coverage/gaps",
    operation_id="listCoverageGaps",
    response_model=CoverageGapListResponse,
    summary="覆盖盲区（按剖面配额计算 Z3/Z4 缺口与补采建议）",
    responses={**COMMON_ERRORS},
)
async def list_coverage_gaps(
    service: Annotated[SceneEngineService, Depends(get_engine_service)],
    _user_id: Annotated[str, Depends(require_read_permission)],
    profile: Annotated[GenerationProfile, Query(description="生成剖面")] = GenerationProfile.EXPLORATION,
) -> CoverageGapListResponse:
    data = await service.coverage_gaps(profile.value)
    return CoverageGapListResponse(data=data, request_id=trace_request_id())


__all__ = ["router"]
