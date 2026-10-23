-- =====================================================================
-- HunterCore 场景引擎（scene-service 服务私有 schema）
-- ---------------------------------------------------------------------
-- 设计来源：《场景生成器与场景管理详细设计文档》V2.0
--   §2.1 三层场景抽象（逻辑 → 具体）、§2.2 概率建模与 Z1-Z4 稀有度、
--   §2.3 双轴分级（稀有度 × 临界度 → L1-L4）、§2.4 可避免性标签、
--   §5.2 场景管理数据模型（测试用例 / 测试集 / 基线 / 生成任务）。
--
-- 边界（契约 orm-mapping.md §3.5）：
--   * scene_engine 为 scene-service **服务私有 schema**，ORM 模型归属服务侧
--     （services/scene-service/app/models/scene_engine.py），不入共享 hunter_common 元数据；
--   * 与 scene_svc.scenes 的关系：具体场景经 content_hash 可落到场景库（发布动作由
--     scene-service 既有 /api/v1/scene 接口完成），本 schema 不做逻辑外键，避免跨 schema 耦合；
--   * DDL ↔ ORM 逐列/枚举取值由 services/scene-service/app/tests/test_scene_engine_ddl.py 钉住。
--
-- 枚举列约定：一律 TEXT + 命名 CHECK（禁止 PG 原生 CREATE TYPE ... AS ENUM），
--   与平台 01~06 schema 一致（原生枚举会导致 asyncpg 报 operator does not exist）。
-- 幂等：全部 CREATE ... IF NOT EXISTS / CREATE OR REPLACE TRIGGER，可重复重放。
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS scene_engine;

-- ========== 1. 逻辑场景（IR 单一事实来源，按语义化版本留存） ==========
CREATE TABLE IF NOT EXISTS scene_engine.logical_scenarios (
    logical_id      TEXT        NOT NULL,
    version         TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    category        TEXT        NOT NULL,
    scene_type      TEXT        NOT NULL,
    description     TEXT        NULL,
    ir_version      TEXT        NOT NULL,
    ir_json         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'published',
    content_hash    CHAR(64)    NOT NULL,
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    created_by      TEXT        NULL,
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (logical_id, version),
    CONSTRAINT ck_logical_scenarios_status CHECK (
        status IN ('draft', 'validating', 'pending_review', 'published',
                   'quarantined', 'deprecated', 'archived', 'rejected')
    ),
    CONSTRAINT ck_logical_scenarios_ir_version CHECK (ir_version = 'hunter.ir/1.2'),
    CONSTRAINT uq_logical_scenarios_hash UNIQUE (content_hash),
    CONSTRAINT ck_logical_scenarios_tags CHECK (array_length(tags, 1) IS NULL OR array_length(tags, 1) <= 20)
);

CREATE INDEX IF NOT EXISTS idx_logical_scenarios_scene_type
    ON scene_engine.logical_scenarios (scene_type, version DESC);
CREATE INDEX IF NOT EXISTS idx_logical_scenarios_status
    ON scene_engine.logical_scenarios (status, update_time DESC);
CREATE INDEX IF NOT EXISTS idx_logical_scenarios_tags
    ON scene_engine.logical_scenarios USING GIN (tags);

-- ========== 2. 具体场景（参数向量 + 稀有度/临界度/风险/可避免性） ==========
CREATE TABLE IF NOT EXISTS scene_engine.concrete_scenarios (
    scenario_id       UUID        NOT NULL DEFAULT gen_random_uuid(),
    logical_id        TEXT        NOT NULL,
    logical_version   TEXT        NOT NULL,
    task_id           UUID        NULL,
    name              TEXT        NOT NULL,
    param_values      JSONB       NOT NULL,
    param_vec         DOUBLE PRECISION[] NOT NULL,
    rarity_p          DOUBLE PRECISION NOT NULL,
    rarity_zone       TEXT        NOT NULL,
    criticality_ci    DOUBLE PRECISION NOT NULL,
    criticality_level TEXT        NOT NULL,
    risk_level        TEXT        NOT NULL,
    avoidability      TEXT        NOT NULL,
    validity          JSONB       NOT NULL DEFAULT '{}',
    generator         JSONB       NOT NULL DEFAULT '{}',
    allowed_modes     TEXT[]      NOT NULL DEFAULT '{}',
    vil_safe          BOOLEAN     NOT NULL DEFAULT false,
    seed              BIGINT      NOT NULL,
    content_hash      CHAR(64)    NOT NULL,
    scene_config      JSONB       NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'generated',
    create_time       TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scenario_id),
    CONSTRAINT ck_concrete_scenarios_rarity_zone CHECK (rarity_zone IN ('Z1', 'Z2', 'Z3', 'Z4')),
    CONSTRAINT ck_concrete_scenarios_criticality_level CHECK (criticality_level IN ('C0', 'C1', 'C2', 'C3')),
    CONSTRAINT ck_concrete_scenarios_risk_level CHECK (risk_level IN ('L1', 'L2', 'L3', 'L4')),
    CONSTRAINT ck_concrete_scenarios_avoidability CHECK (
        avoidability IN ('avoidable', 'marginal', 'unavoidable', 'trivial_safe')
    ),
    CONSTRAINT ck_concrete_scenarios_status CHECK (
        status IN ('generated', 'validated', 'published', 'quarantined', 'archived')
    ),
    CONSTRAINT ck_concrete_scenarios_rarity_p CHECK (rarity_p >= 0 AND rarity_p <= 1),
    CONSTRAINT ck_concrete_scenarios_ci CHECK (criticality_ci >= 0),
    CONSTRAINT uq_concrete_scenarios_hash UNIQUE (content_hash),
    CONSTRAINT ck_concrete_scenarios_allowed_modes CHECK (
        array_length(allowed_modes, 1) IS NULL OR array_length(allowed_modes, 1) <= 4
    )
);

CREATE INDEX IF NOT EXISTS idx_concrete_scenarios_logical
    ON scene_engine.concrete_scenarios (logical_id, logical_version, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_concrete_scenarios_zone
    ON scene_engine.concrete_scenarios (rarity_zone, criticality_level);
CREATE INDEX IF NOT EXISTS idx_concrete_scenarios_risk
    ON scene_engine.concrete_scenarios (risk_level, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_concrete_scenarios_task
    ON scene_engine.concrete_scenarios (task_id);

-- ========== 3. 测试用例（ISO 34505 结构：目标/前置/步骤/平台/判据/超时） ==========
CREATE TABLE IF NOT EXISTS scene_engine.test_cases (
    test_case_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
    case_key        TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    logical_id      TEXT        NOT NULL,
    logical_version TEXT        NOT NULL,
    scenario_id     UUID        NULL,
    objective       TEXT        NOT NULL,
    preconditions   JSONB       NOT NULL DEFAULT '{}',
    steps           JSONB       NOT NULL DEFAULT '[]',
    platforms       TEXT[]      NOT NULL DEFAULT '{}',
    oracle_ref      TEXT        NOT NULL,
    timeout_seconds INTEGER     NOT NULL DEFAULT 120,
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'draft',
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (test_case_id),
    CONSTRAINT uq_test_cases_case_key UNIQUE (case_key),
    CONSTRAINT ck_test_cases_status CHECK (status IN ('draft', 'published', 'deprecated', 'archived')),
    CONSTRAINT ck_test_cases_timeout CHECK (timeout_seconds > 0 AND timeout_seconds <= 3600),
    CONSTRAINT ck_test_cases_platforms CHECK (
        array_length(platforms, 1) IS NULL OR array_length(platforms, 1) <= 8
    )
);

CREATE INDEX IF NOT EXISTS idx_test_cases_logical
    ON scene_engine.test_cases (logical_id, logical_version, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_test_cases_status
    ON scene_engine.test_cases (status, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_test_cases_tags
    ON scene_engine.test_cases USING GIN (tags);

-- ========== 4. 测试集（suite） ==========
CREATE TABLE IF NOT EXISTS scene_engine.scenario_suites (
    suite_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    description TEXT        NULL,
    status      TEXT        NOT NULL DEFAULT 'draft',
    lock_hash   CHAR(64)    NULL,
    locked_at   TIMESTAMPTZ NULL,
    locked_by   TEXT        NULL,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (suite_id),
    CONSTRAINT uq_scenario_suites_name UNIQUE (name),
    CONSTRAINT ck_scenario_suites_status CHECK (status IN ('draft', 'locked', 'archived'))
);

CREATE INDEX IF NOT EXISTS idx_scenario_suites_status
    ON scene_engine.scenario_suites (status, create_time DESC);

-- ========== 5. 测试集条目（用例 / 具体场景，多态引用） ==========
CREATE TABLE IF NOT EXISTS scene_engine.suite_items (
    suite_id    UUID        NOT NULL,
    item_id     TEXT        NOT NULL,
    item_type   TEXT        NOT NULL,
    ref_id      TEXT        NOT NULL,
    weight      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (suite_id, item_id),
    CONSTRAINT ck_suite_items_type CHECK (item_type IN ('test_case', 'concrete_scenario')),
    CONSTRAINT ck_suite_items_weight CHECK (weight > 0 AND weight <= 100)
);

CREATE INDEX IF NOT EXISTS idx_suite_items_ref
    ON scene_engine.suite_items (item_type, ref_id);

-- ========== 6. 基线锁（suite 内容哈希 + 指标快照） ==========
CREATE TABLE IF NOT EXISTS scene_engine.baseline_locks (
    baseline_id UUID        NOT NULL DEFAULT gen_random_uuid(),
    suite_id    UUID        NOT NULL,
    name        TEXT        NOT NULL,
    suite_hash  CHAR(64)    NOT NULL,
    metrics     JSONB       NOT NULL DEFAULT '{}',
    locked_by   TEXT        NOT NULL,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (baseline_id),
    CONSTRAINT uq_baseline_locks_suite_name UNIQUE (suite_id, name)
);

CREATE INDEX IF NOT EXISTS idx_baseline_locks_suite
    ON scene_engine.baseline_locks (suite_id, create_time DESC);

-- ========== 7. 生成任务（异步任务账本） ==========
CREATE TABLE IF NOT EXISTS scene_engine.generation_tasks (
    task_id           UUID        NOT NULL DEFAULT gen_random_uuid(),
    logical_id        TEXT        NOT NULL,
    logical_version   TEXT        NOT NULL,
    strategy          TEXT        NOT NULL,
    profile           TEXT        NOT NULL,
    count             INTEGER     NOT NULL,
    seed              BIGINT      NOT NULL,
    overrides         JSONB       NOT NULL DEFAULT '{}',
    status            TEXT        NOT NULL DEFAULT 'pending',
    progress          DOUBLE PRECISION NOT NULL DEFAULT 0,
    produced          INTEGER     NOT NULL DEFAULT 0,
    duplicates        INTEGER     NOT NULL DEFAULT 0,
    rejected          INTEGER     NOT NULL DEFAULT 0,
    zone_quota        JSONB       NOT NULL DEFAULT '{}',
    zone_counts       JSONB       NOT NULL DEFAULT '{}',
    error_message     TEXT        NULL,
    created_by        TEXT        NOT NULL,
    create_time       TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id),
    CONSTRAINT ck_generation_tasks_strategy CHECK (
        strategy IN ('coverage_driven', 'distribution', 'replay_mutation', 'search_based', 'ai_generated')
    ),
    CONSTRAINT ck_generation_tasks_profile CHECK (profile IN ('regression', 'robustness', 'exploration')),
    CONSTRAINT ck_generation_tasks_status CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')
    ),
    CONSTRAINT ck_generation_tasks_count CHECK (count >= 1 AND count <= 5000),
    CONSTRAINT ck_generation_tasks_progress CHECK (progress >= 0 AND progress <= 1)
);

CREATE INDEX IF NOT EXISTS idx_generation_tasks_status
    ON scene_engine.generation_tasks (status, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_generation_tasks_logical
    ON scene_engine.generation_tasks (logical_id, create_time DESC);

-- ========== 8. update_time 触发器（复用 00_schemas.sql 公共函数） ==========
CREATE OR REPLACE TRIGGER trg_logical_scenarios_update_time
    BEFORE UPDATE ON scene_engine.logical_scenarios
FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_concrete_scenarios_update_time
    BEFORE UPDATE ON scene_engine.concrete_scenarios
FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_test_cases_update_time
    BEFORE UPDATE ON scene_engine.test_cases
FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_scenario_suites_update_time
    BEFORE UPDATE ON scene_engine.scenario_suites
FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_generation_tasks_update_time
    BEFORE UPDATE ON scene_engine.generation_tasks
FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();
