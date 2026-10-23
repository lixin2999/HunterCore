"""场景引擎 Schema（逻辑/具体场景、生成任务、用例、测试集、覆盖）。

请求体一律 ``extra="forbid"``；响应统一 ``ApiResponse``（code/message/data/request_id）。
字段命名与《场景生成器与场景管理详细设计文档》V2.0 §4.1 对齐，内部实现细节不外泄。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field

from app.models.scene_engine import (
    GenerationProfile,
    GenerationStrategy,
    LogicalScenarioStatus,
    RarityZone,
    SuiteItemType,
)

#: IR 逻辑场景 ID 规则（与 SceneIR.logical_id 一致）
LOGICAL_ID_PATTERN = r"^LS-[A-Z0-9-]{3,40}$"
#: 测试用例编号规则（ISO 34505 风格：TC-<域>-<序号>）
CASE_KEY_PATTERN = r"^TC-[A-Z0-9-]{3,48}$"


# =====================================================================
# 一、逻辑场景
# =====================================================================


class LogicalScenarioSummary(BaseModel):
    """逻辑场景摘要（列表项；不含 IR 本体）。"""

    model_config = ConfigDict(from_attributes=True)

    logical_id: str
    version: str
    name: str
    category: str
    scene_type: str
    description: str = ""
    status: str = LogicalScenarioStatus.PUBLISHED.value
    ir_version: str = "hunter.ir/1.2"
    oracle_ref: str = ""
    parameter_count: int = 0
    tags: list[str] = Field(default_factory=list)
    content_hash: str = ""
    origin: Literal["catalog", "database"] = "database"
    update_time: datetime | None = None


class LogicalScenarioDetail(LogicalScenarioSummary):
    """逻辑场景详情（含 IR 与参数声明）。"""

    ir: dict[str, Any]
    parameters: list[dict[str, Any]] = Field(default_factory=list)
    bindings: list[dict[str, Any]] = Field(default_factory=list)
    constraints: list[dict[str, Any]] = Field(default_factory=list)
    correlations: list[dict[str, Any]] = Field(default_factory=list)
    allowed_modes: list[str] = Field(default_factory=list)


class LogicalScenarioListData(BaseModel):
    items: list[LogicalScenarioSummary]
    total: int


class LogicalScenarioListResponse(ApiResponse[LogicalScenarioListData]):
    """逻辑场景列表响应。"""


class LogicalScenarioResponse(ApiResponse[LogicalScenarioDetail]):
    """逻辑场景详情响应。"""


class LogicalVersionSummary(BaseModel):
    version: str
    status: str
    content_hash: str
    origin: Literal["catalog", "database"]
    update_time: datetime | None = None


class LogicalVersionListData(BaseModel):
    logical_id: str
    items: list[LogicalVersionSummary]


class LogicalVersionListResponse(ApiResponse[LogicalVersionListData]):
    """逻辑场景版本列表响应。"""


class GateItem(BaseModel):
    gate: str = Field(description="G1-G8")
    passed: bool
    message: str
    skipped: bool = Field(
        default=False,
        description="该门未执行（如 esmini 未部署）；skipped 不计入失败，但必须在报告中显式标注",
    )


class ValidationReportData(BaseModel):
    logical_id: str
    version: str
    passed: bool
    gates: list[GateItem]
    failures: list[str] = Field(default_factory=list)
    oracle_ref: str
    oracle_found: bool
    parameter_defaults: dict[str, float | str] = Field(default_factory=dict)


class LogicalValidationResponse(ApiResponse[ValidationReportData]):
    """逻辑场景校验响应（G1-G7 静态校验）。"""


class DryRunRequest(BaseModel):
    """干跑请求：可覆盖参数（其余取中位数）。"""

    model_config = ConfigDict(extra="forbid")

    version: str | None = None
    values: dict[str, float | str] = Field(default_factory=dict)


class DryRunData(BaseModel):
    logical_id: str
    version: str
    passed: bool
    values: dict[str, float | str]
    scene_config: dict[str, Any]
    gates: list[GateItem]
    failures: list[str] = Field(default_factory=list)


class LogicalDryRunResponse(ApiResponse[DryRunData]):
    """逻辑场景干跑响应（返回渲染后的 SceneConfig 预览）。"""


class PublishData(BaseModel):
    logical_id: str
    version: str
    status: str


class LogicalPublishResponse(ApiResponse[PublishData]):
    """逻辑场景发布响应。"""


class CatalogSyncData(BaseModel):
    version: str
    synced: int
    logical_ids: list[str]


class CatalogSyncResponse(ApiResponse[CatalogSyncData]):
    """内置目录同步响应。"""


# =====================================================================
# 二、生成任务
# =====================================================================


class GenerateRequest(BaseModel):
    """批量生成请求（异步，返回 202 + task_id）。"""

    model_config = ConfigDict(extra="forbid")

    logical_id: str = Field(pattern=LOGICAL_ID_PATTERN)
    logical_version: str | None = Field(default=None, max_length=16)
    count: int = Field(default=100, ge=1, le=5000, description="目标生成数量")
    strategy: GenerationStrategy = Field(
        default=GenerationStrategy.DISTRIBUTION, description="生成策略"
    )
    profile: GenerationProfile = Field(
        default=GenerationProfile.REGRESSION, description="Z1-Z4 分层配额剖面"
    )
    seed: int = Field(default=0, ge=0, le=2**63 - 1, description="随机种子；0 表示由服务端生成")
    overrides: dict[str, Any] = Field(
        default_factory=dict,
        description="策略覆盖（fixed/ranges/candidates/base/target_zone 等白名单键）",
    )


class GenerationTaskData(BaseModel):
    task_id: UUID
    logical_id: str
    logical_version: str
    strategy: str
    profile: str
    count: int
    seed: int
    status: str
    progress: float
    produced: int
    duplicates: int
    rejected: int
    zone_quota: dict[str, int] = Field(default_factory=dict)
    zone_counts: dict[str, int] = Field(default_factory=dict)
    error_message: str | None = None
    owner_id: str | None = Field(default=None, description="执行该任务的实例标识（多副本归属/对账）")
    cancel_requested: bool = Field(default=False, description="是否已请求协作式取消")
    created_by: str
    create_time: datetime | None = None
    update_time: datetime | None = None


class GenerationTaskResponse(ApiResponse[GenerationTaskData]):
    """生成任务详情响应。"""


class GenerationTaskListData(BaseModel):
    items: list[GenerationTaskData]
    total: int


class GenerationTaskListResponse(ApiResponse[GenerationTaskListData]):
    """生成任务列表响应。"""


# =====================================================================
# 三、具体场景
# =====================================================================


class ConcreteScenarioSummary(BaseModel):
    scenario_id: UUID
    logical_id: str
    logical_version: str
    name: str
    rarity_p: float
    rarity_zone: str
    criticality_ci: float
    criticality_level: str
    risk_level: str
    avoidability: str
    vil_safe: bool
    status: str
    content_hash: str
    seed: int
    create_time: datetime | None = None


class ConcreteScenarioDetail(ConcreteScenarioSummary):
    param_values: dict[str, Any] = Field(default_factory=dict)
    param_vec: list[float] = Field(default_factory=list)
    allowed_modes: list[str] = Field(default_factory=list)
    generator: dict[str, Any] = Field(default_factory=dict)
    validity: dict[str, Any] = Field(default_factory=dict)
    scene_config: dict[str, Any] = Field(default_factory=dict)


class ConcreteScenarioListData(BaseModel):
    items: list[ConcreteScenarioSummary]
    total: int
    page: int
    page_size: int


class ConcreteScenarioListResponse(ApiResponse[ConcreteScenarioListData]):
    """具体场景列表响应。"""


class ConcreteScenarioResponse(ApiResponse[ConcreteScenarioDetail]):
    """具体场景详情响应。"""


class RenderData(BaseModel):
    scenario_id: UUID
    logical_id: str
    content_hash: str
    scene_config: dict[str, Any]
    gates: list[GateItem]
    passed: bool


class ConcreteRenderResponse(ApiResponse[RenderData]):
    """具体场景按需渲染响应（返回可直接下发的 SceneConfig）。"""


class BatchValidateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario_ids: list[UUID] = Field(min_length=1, max_length=200)


class BatchValidateItem(BaseModel):
    scenario_id: UUID
    passed: bool
    failures: list[str] = Field(default_factory=list)
    status: str


class BatchValidateData(BaseModel):
    validated: int
    quarantined: int
    items: list[BatchValidateItem]


class BatchValidateResponse(ApiResponse[BatchValidateData]):
    """具体场景批量校验响应（通过 → validated，失败 → quarantined）。"""


# =====================================================================
# 四、判据 / 测试用例
# =====================================================================


class OracleData(BaseModel):
    oracle_ref: str
    name: str
    kind: str
    description: str
    metric: str
    operator: str
    threshold: float
    unit: str = ""
    platforms: list[str] = Field(default_factory=list)
    timeout_seconds: int = 120
    extra: dict[str, Any] = Field(default_factory=dict)


class OracleListData(BaseModel):
    items: list[OracleData]
    total: int


class OracleListResponse(ApiResponse[OracleListData]):
    """判据目录响应。"""


class TestCaseCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    case_key: str = Field(pattern=CASE_KEY_PATTERN, max_length=48)
    name: str = Field(min_length=1, max_length=128)
    logical_id: str = Field(pattern=LOGICAL_ID_PATTERN)
    logical_version: str | None = Field(default=None, max_length=16, description="省略取最新版本")
    scenario_id: UUID | None = None
    objective: str = Field(min_length=1, max_length=500)
    preconditions: dict[str, Any] = Field(default_factory=dict)
    steps: list[Any] = Field(default_factory=list, max_length=50)
    platforms: list[str] = Field(
        default_factory=lambda: ["simulation"], max_length=8, description="sil/vil/replay/hil…"
    )
    oracle_ref: str = Field(min_length=1, max_length=64)
    timeout_seconds: int = Field(default=120, ge=1, le=3600)
    tags: list[str] = Field(default_factory=list, max_length=20)


class TestCaseData(BaseModel):
    case_key: str
    name: str
    logical_id: str
    logical_version: str
    scenario_id: UUID | None = None
    objective: str
    preconditions: dict[str, Any] = Field(default_factory=dict)
    steps: list[Any] = Field(default_factory=list)
    platforms: list[str] = Field(default_factory=list)
    oracle_ref: str
    timeout_seconds: int
    tags: list[str] = Field(default_factory=list)
    status: str
    test_case_id: UUID
    create_time: datetime | None = None


class TestCaseListData(BaseModel):
    items: list[TestCaseData]
    total: int
    page: int
    page_size: int


class TestCaseListResponse(ApiResponse[TestCaseListData]):
    """测试用例列表响应。"""


class TestCaseResponse(ApiResponse[TestCaseData]):
    """测试用例详情响应。"""


# =====================================================================
# 五、测试集 / 基线
# =====================================================================


class SuiteCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=500)


class SuiteItemInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1, max_length=64, description="集内唯一条目 ID")
    item_type: SuiteItemType = Field(description="test_case / concrete_scenario")
    ref_id: str = Field(min_length=1, max_length=64, description="用例 ID 或具体场景 UUID")
    weight: float = Field(default=1.0, gt=0, le=100)


class SuiteItemsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[SuiteItemInput] = Field(min_length=1, max_length=500)


class SuiteLockRequest(BaseModel):
    """锁定测试集（可选创建基线快照）。"""

    model_config = ConfigDict(extra="forbid")

    baseline_name: str | None = Field(default=None, max_length=128)
    metrics: dict[str, Any] = Field(default_factory=dict)


class SuiteData(BaseModel):
    suite_id: UUID
    name: str
    description: str | None = None
    status: str
    item_count: int = 0
    lock_hash: str | None = None
    locked_at: datetime | None = None
    locked_by: str | None = None
    create_time: datetime | None = None
    update_time: datetime | None = None


class SuiteItemData(BaseModel):
    item_id: str
    item_type: str
    ref_id: str
    weight: float


class SuiteDetailData(SuiteData):
    items: list[SuiteItemData] = Field(default_factory=list)


class SuiteListData(BaseModel):
    items: list[SuiteData]
    total: int
    page: int
    page_size: int


class SuiteListResponse(ApiResponse[SuiteListData]):
    """测试集列表响应。"""


class SuiteResponse(ApiResponse[SuiteDetailData]):
    """测试集详情响应。"""


class SuiteLockData(BaseModel):
    suite_id: UUID
    status: str
    lock_hash: str
    item_count: int
    baseline_name: str | None = None


class SuiteLockResponse(ApiResponse[SuiteLockData]):
    """测试集锁定响应。"""


class BaselineData(BaseModel):
    baseline_id: UUID
    suite_id: UUID
    name: str
    suite_hash: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    locked_by: str
    create_time: datetime | None = None


class BaselineListData(BaseModel):
    items: list[BaselineData]


class BaselineListResponse(ApiResponse[BaselineListData]):
    """测试集基线列表响应。"""


class SuiteDiffData(BaseModel):
    suite_id: UUID
    current_hash: str
    baseline_hash: str | None = None
    identical: bool
    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    changed: list[str] = Field(default_factory=list)


class SuiteDiffResponse(ApiResponse[SuiteDiffData]):
    """测试集差异响应（当前条目 vs 基线/锁定快照）。"""


# =====================================================================
# 六、覆盖分析
# =====================================================================


class CoverageOverviewData(BaseModel):
    logical_id: str | None = None
    total: int
    zone_counts: dict[str, int] = Field(default_factory=dict)
    zone_ratio: dict[str, float] = Field(default_factory=dict)
    risk_counts: dict[str, int] = Field(default_factory=dict)
    risk_matrix_applied: bool = True


class CoverageOverviewResponse(ApiResponse[CoverageOverviewData]):
    """覆盖总览响应。"""


class CoverageGapData(BaseModel):
    logical_id: str
    name: str
    zone: RarityZone
    count: int
    target: int
    deficit: int
    recommendation: str


class CoverageGapListData(BaseModel):
    profile: str
    items: list[CoverageGapData]


class CoverageGapListResponse(ApiResponse[CoverageGapListData]):
    """覆盖盲区响应（按剖面目标配额计算缺口）。"""


__all__ = [
    "BaselineData",
    "BaselineListData",
    "BaselineListResponse",
    "BatchValidateData",
    "BatchValidateItem",
    "BatchValidateRequest",
    "BatchValidateResponse",
    "CatalogSyncData",
    "CatalogSyncResponse",
    "ConcreteRenderResponse",
    "ConcreteScenarioDetail",
    "ConcreteScenarioListData",
    "ConcreteScenarioListResponse",
    "ConcreteScenarioResponse",
    "ConcreteScenarioSummary",
    "CoverageGapData",
    "CoverageGapListData",
    "CoverageGapListResponse",
    "CoverageOverviewData",
    "CoverageOverviewResponse",
    "DryRunData",
    "DryRunRequest",
    "GateItem",
    "GenerateRequest",
    "GenerationTaskData",
    "GenerationTaskListData",
    "GenerationTaskListResponse",
    "GenerationTaskResponse",
    "LogicalDryRunResponse",
    "LogicalPublishResponse",
    "LogicalScenarioDetail",
    "LogicalScenarioListData",
    "LogicalScenarioListResponse",
    "LogicalScenarioResponse",
    "LogicalScenarioSummary",
    "LogicalValidationResponse",
    "LogicalVersionListData",
    "LogicalVersionListResponse",
    "LogicalVersionSummary",
    "OracleData",
    "OracleListData",
    "OracleListResponse",
    "PublishData",
    "RenderData",
    "SuiteCreateRequest",
    "SuiteData",
    "SuiteDetailData",
    "SuiteDiffData",
    "SuiteDiffResponse",
    "SuiteItemData",
    "SuiteItemInput",
    "SuiteItemsRequest",
    "SuiteListData",
    "SuiteListResponse",
    "SuiteLockData",
    "SuiteLockRequest",
    "SuiteLockResponse",
    "SuiteResponse",
    "TestCaseCreateRequest",
    "TestCaseData",
    "TestCaseListData",
    "TestCaseListResponse",
    "TestCaseResponse",
    "ValidationReportData",
]
