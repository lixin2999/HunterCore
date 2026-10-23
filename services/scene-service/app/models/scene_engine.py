"""ORM 模型：scene_engine schema（场景引擎，对应 07_scene_engine.sql）。

设计来源：《场景生成器与场景管理详细设计文档》V2.0
（§2 三层场景抽象 + 概率分级；§5.2 场景管理数据模型）。

边界：本 schema 为 scene-service **服务私有**，模型归属服务侧、不入共享
``hunter_common.database.models.ALL_MODELS``；DDL ↔ ORM 由
``app/tests/test_scene_engine_ddl.py`` 钉住（与 ingest-platform 同一模式）。

枚举列一律 ``TEXT + CHECK``（``StrEnumType``），禁止 PG 原生枚举：
原生枚举会让 asyncpg 以 varchar 绑定比较参数并报 undefined function。
"""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from hunter_common.database.base import Base, StrEnumType
from hunter_common.database.schema_names import SCENE_ENGINE_SCHEMA
from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Double,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

# =====================================================================
# 受控词表（取值与 07_scene_engine.sql 的 CHECK 逐字一致）
# =====================================================================


class LogicalScenarioStatus(StrEnum):
    """逻辑场景生命周期（§2.7）：draft → validating → pending_review → published，
    以及 quarantined / deprecated / archived / rejected 旁路态。"""

    DRAFT = "draft"
    VALIDATING = "validating"
    PENDING_REVIEW = "pending_review"
    PUBLISHED = "published"
    QUARANTINED = "quarantined"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    REJECTED = "rejected"


class ConcreteScenarioStatus(StrEnum):
    """具体场景状态（生成 → 校验 → 发布/隔离/归档）。"""

    GENERATED = "generated"
    VALIDATED = "validated"
    PUBLISHED = "published"
    QUARANTINED = "quarantined"
    ARCHIVED = "archived"


class RarityZone(StrEnum):
    """稀有度分区（马氏距离 d_M² ~ χ²ₙ 分位）：Z1 常见 → Z4 极稀有。"""

    Z1 = "Z1"
    Z2 = "Z2"
    Z3 = "Z3"
    Z4 = "Z4"


class CriticalityLevel(StrEnum):
    """临界度分级（CI = a_req / a_max）：C0 无风险 → C3 极限。"""

    C0 = "C0"
    C1 = "C1"
    C2 = "C2"
    C3 = "C3"


class RiskLevel(StrEnum):
    """风险等级（稀有度 × 临界度 双轴矩阵，§2.8.1）。"""

    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"


class Avoidability(StrEnum):
    """可避免性标签（§2.4：标注而非剔除）。"""

    AVOIDABLE = "avoidable"
    MARGINAL = "marginal"
    UNAVOIDABLE = "unavoidable"
    TRIVIAL_SAFE = "trivial_safe"


class GenerationStrategy(StrEnum):
    """生成策略插件（§2.9）：覆盖驱动/分布/回放变异/搜索/受护栏的 AI 参数导入。"""

    COVERAGE_DRIVEN = "coverage_driven"
    DISTRIBUTION = "distribution"
    REPLAY_MUTATION = "replay_mutation"
    SEARCH_BASED = "search_based"
    AI_GENERATED = "ai_generated"


class GenerationProfile(StrEnum):
    """生成剖面的 Z1-Z4 分层配额（默认回归 70:25:5:0 / 鲁棒 40:40:20:0 / 探索 10:30:50:10）。"""

    REGRESSION = "regression"
    ROBUSTNESS = "robustness"
    EXPLORATION = "exploration"


class GenerationTaskStatus(StrEnum):
    """异步生成任务状态。"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class TestCaseStatus(StrEnum):
    """测试用例状态。"""

    __test__ = False  # pytest 收集护栏（类名以 Test 开头）

    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class SuiteStatus(StrEnum):
    """测试集状态（lock 后不可再改条目）。"""

    DRAFT = "draft"
    LOCKED = "locked"
    ARCHIVED = "archived"


class SuiteItemType(StrEnum):
    """测试集条目类型（多态引用：测试用例 / 具体场景）。"""

    TEST_CASE = "test_case"
    CONCRETE_SCENARIO = "concrete_scenario"


# =====================================================================
# 1. 逻辑场景
# =====================================================================


class LogicalScenario(Base):
    """逻辑场景（IR 单一事实来源，按语义化版本留存；PK = logical_id + version）。"""

    __tablename__ = "logical_scenarios"
    __table_args__ = (
        PrimaryKeyConstraint("logical_id", "version", name="pk_logical_scenarios"),
        UniqueConstraint("content_hash", name="uq_logical_scenarios_hash"),
        CheckConstraint(
            "status IN ('draft', 'validating', 'pending_review', 'published', "
            "'quarantined', 'deprecated', 'archived', 'rejected')",
            name="ck_logical_scenarios_status",
        ),
        CheckConstraint("ir_version = 'hunter.ir/1.2'", name="ck_logical_scenarios_ir_version"),
        CheckConstraint(
            "array_length(tags, 1) IS NULL OR array_length(tags, 1) <= 20",
            name="ck_logical_scenarios_tags",
        ),
        Index("idx_logical_scenarios_scene_type", "scene_type", text("version DESC")),
        Index("idx_logical_scenarios_status", "status", text("update_time DESC")),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    logical_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    scene_type: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    ir_version: Mapped[str] = mapped_column(Text, nullable=False)
    ir_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[LogicalScenarioStatus] = mapped_column(
        StrEnumType(LogicalScenarioStatus), nullable=False, server_default=text("'published'")
    )
    content_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


# =====================================================================
# 2. 具体场景
# =====================================================================


class ConcreteScenario(Base):
    """具体场景（参数向量 + 稀有度/临界度/风险/可避免性 + 可渲染 config）。"""

    __tablename__ = "concrete_scenarios"
    __table_args__ = (
        UniqueConstraint("content_hash", name="uq_concrete_scenarios_hash"),
        CheckConstraint("rarity_zone IN ('Z1', 'Z2', 'Z3', 'Z4')", name="ck_concrete_scenarios_rarity_zone"),
        CheckConstraint(
            "criticality_level IN ('C0', 'C1', 'C2', 'C3')",
            name="ck_concrete_scenarios_criticality_level",
        ),
        CheckConstraint("risk_level IN ('L1', 'L2', 'L3', 'L4')", name="ck_concrete_scenarios_risk_level"),
        CheckConstraint(
            "avoidability IN ('avoidable', 'marginal', 'unavoidable', 'trivial_safe')",
            name="ck_concrete_scenarios_avoidability",
        ),
        CheckConstraint(
            "status IN ('generated', 'validated', 'published', 'quarantined', 'archived')",
            name="ck_concrete_scenarios_status",
        ),
        CheckConstraint("rarity_p >= 0 AND rarity_p <= 1", name="ck_concrete_scenarios_rarity_p"),
        CheckConstraint("criticality_ci >= 0", name="ck_concrete_scenarios_ci"),
        CheckConstraint(
            "array_length(allowed_modes, 1) IS NULL OR array_length(allowed_modes, 1) <= 4",
            name="ck_concrete_scenarios_allowed_modes",
        ),
        Index("idx_concrete_scenarios_logical", "logical_id", "logical_version", text("create_time DESC")),
        Index("idx_concrete_scenarios_zone", "rarity_zone", "criticality_level"),
        Index("idx_concrete_scenarios_risk", "risk_level", text("create_time DESC")),
        Index("idx_concrete_scenarios_task", "task_id"),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    scenario_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    logical_id: Mapped[str] = mapped_column(Text, nullable=False)
    logical_version: Mapped[str] = mapped_column(Text, nullable=False)
    task_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    param_values: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    param_vec: Mapped[list[float]] = mapped_column(ARRAY(Double), nullable=False)
    rarity_p: Mapped[float] = mapped_column(Double, nullable=False)
    rarity_zone: Mapped[RarityZone] = mapped_column(StrEnumType(RarityZone), nullable=False)
    criticality_ci: Mapped[float] = mapped_column(Double, nullable=False)
    criticality_level: Mapped[CriticalityLevel] = mapped_column(
        StrEnumType(CriticalityLevel), nullable=False
    )
    risk_level: Mapped[RiskLevel] = mapped_column(StrEnumType(RiskLevel), nullable=False)
    avoidability: Mapped[Avoidability] = mapped_column(StrEnumType(Avoidability), nullable=False)
    validity: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    generator: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    allowed_modes: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    vil_safe: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    seed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    scene_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[ConcreteScenarioStatus] = mapped_column(
        StrEnumType(ConcreteScenarioStatus), nullable=False, server_default=text("'generated'")
    )
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


# =====================================================================
# 3. 测试用例
# =====================================================================


class TestCase(Base):
    """测试用例（ISO 34505：目标/前置/步骤/平台/判据/超时/标签）。"""

    __test__ = False  # pytest 收集护栏（类名以 Test 开头）

    __tablename__ = "test_cases"
    __table_args__ = (
        UniqueConstraint("case_key", name="uq_test_cases_case_key"),
        CheckConstraint(
            "status IN ('draft', 'published', 'deprecated', 'archived')", name="ck_test_cases_status"
        ),
        CheckConstraint("timeout_seconds > 0 AND timeout_seconds <= 3600", name="ck_test_cases_timeout"),
        CheckConstraint(
            "array_length(platforms, 1) IS NULL OR array_length(platforms, 1) <= 8",
            name="ck_test_cases_platforms",
        ),
        Index("idx_test_cases_logical", "logical_id", "logical_version", text("create_time DESC")),
        Index("idx_test_cases_status", "status", text("create_time DESC")),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    test_case_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    case_key: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    logical_id: Mapped[str] = mapped_column(Text, nullable=False)
    logical_version: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    objective: Mapped[str] = mapped_column(Text, nullable=False)
    preconditions: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    steps: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    platforms: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    oracle_ref: Mapped[str] = mapped_column(Text, nullable=False)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("120"))
    tags: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, server_default=text("'{}'"))
    status: Mapped[TestCaseStatus] = mapped_column(
        StrEnumType(TestCaseStatus), nullable=False, server_default=text("'draft'")
    )
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


# =====================================================================
# 4. 测试集 / 5. 条目 / 6. 基线
# =====================================================================


class ScenarioSuite(Base):
    """测试集（lock 后条目冻结，内容哈希写入 lock_hash）。"""

    __tablename__ = "scenario_suites"
    __table_args__ = (
        UniqueConstraint("name", name="uq_scenario_suites_name"),
        CheckConstraint("status IN ('draft', 'locked', 'archived')", name="ck_scenario_suites_status"),
        Index("idx_scenario_suites_status", "status", text("create_time DESC")),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    suite_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[SuiteStatus] = mapped_column(
        StrEnumType(SuiteStatus), nullable=False, server_default=text("'draft'")
    )
    lock_hash: Mapped[str | None] = mapped_column(CHAR(64), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class SuiteItem(Base):
    """测试集条目（PK = suite_id + item_id；多态引用用例/具体场景）。"""

    __tablename__ = "suite_items"
    __table_args__ = (
        PrimaryKeyConstraint("suite_id", "item_id", name="pk_suite_items"),
        CheckConstraint(
            "item_type IN ('test_case', 'concrete_scenario')", name="ck_suite_items_type"
        ),
        CheckConstraint("weight > 0 AND weight <= 100", name="ck_suite_items_weight"),
        Index("idx_suite_items_ref", "item_type", "ref_id"),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    suite_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    item_id: Mapped[str] = mapped_column(Text, nullable=False)
    item_type: Mapped[SuiteItemType] = mapped_column(StrEnumType(SuiteItemType), nullable=False)
    ref_id: Mapped[str] = mapped_column(Text, nullable=False)
    weight: Mapped[float] = mapped_column(Double, nullable=False, server_default=text("1.0"))
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BaselineLock(Base):
    """基线锁（suite 内容哈希 + 指标快照，供回归比对）。"""

    __tablename__ = "baseline_locks"
    __table_args__ = (
        UniqueConstraint("suite_id", "name", name="uq_baseline_locks_suite_name"),
        Index("idx_baseline_locks_suite", "suite_id", text("create_time DESC")),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    baseline_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    suite_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    suite_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    metrics: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    locked_by: Mapped[str] = mapped_column(Text, nullable=False)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# =====================================================================
# 7. 生成任务
# =====================================================================


class GenerationTask(Base):
    """生成任务账本（异步任务；zone_quota/zone_counts 记录分层配额与实产）。"""

    __tablename__ = "generation_tasks"
    __table_args__ = (
        CheckConstraint(
            "strategy IN ('coverage_driven', 'distribution', 'replay_mutation', "
            "'search_based', 'ai_generated')",
            name="ck_generation_tasks_strategy",
        ),
        CheckConstraint(
            "profile IN ('regression', 'robustness', 'exploration')",
            name="ck_generation_tasks_profile",
        ),
        CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')",
            name="ck_generation_tasks_status",
        ),
        CheckConstraint("count >= 1 AND count <= 5000", name="ck_generation_tasks_count"),
        CheckConstraint("progress >= 0 AND progress <= 1", name="ck_generation_tasks_progress"),
        Index("idx_generation_tasks_status", "status", text("create_time DESC")),
        Index("idx_generation_tasks_logical", "logical_id", text("create_time DESC")),
        {"schema": SCENE_ENGINE_SCHEMA},
    )

    task_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    logical_id: Mapped[str] = mapped_column(Text, nullable=False)
    logical_version: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[GenerationStrategy] = mapped_column(
        StrEnumType(GenerationStrategy), nullable=False
    )
    profile: Mapped[GenerationProfile] = mapped_column(
        StrEnumType(GenerationProfile), nullable=False
    )
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    seed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    overrides: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    status: Mapped[GenerationTaskStatus] = mapped_column(
        StrEnumType(GenerationTaskStatus), nullable=False, server_default=text("'pending'")
    )
    progress: Mapped[float] = mapped_column(Double, nullable=False, server_default=text("0"))
    produced: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    duplicates: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    rejected: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    zone_quota: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    zone_counts: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


#: 本 schema 全部模型（DDL 契约测试用）
SCENE_ENGINE_MODELS: tuple[type[Base], ...] = (
    LogicalScenario,
    ConcreteScenario,
    TestCase,
    ScenarioSuite,
    SuiteItem,
    BaselineLock,
    GenerationTask,
)

__all__ = [
    "SCENE_ENGINE_MODELS",
    "Avoidability",
    "BaselineLock",
    "ConcreteScenario",
    "ConcreteScenarioStatus",
    "CriticalityLevel",
    "GenerationProfile",
    "GenerationStrategy",
    "GenerationTask",
    "GenerationTaskStatus",
    "LogicalScenario",
    "LogicalScenarioStatus",
    "RarityZone",
    "RiskLevel",
    "ScenarioSuite",
    "SuiteItem",
    "SuiteItemType",
    "SuiteStatus",
    "TestCase",
    "TestCaseStatus",
]
