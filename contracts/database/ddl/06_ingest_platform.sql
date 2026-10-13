-- =====================================================================
-- HunterCore 数据库契约 — 06 ingest_platform（服务端数据消费/解析/处理）
-- 来源：《服务端数据消费解析与处理详细设计文档》v1.0 §6.2
-- 约束：本目录 SQL 为数据库结构的单一事实来源；ORM 模型必须与本文件保持一致
-- 幂等：全部使用 IF NOT EXISTS / CREATE OR REPLACE，可重复执行
-- =====================================================================

-- ---------- Schema ----------
CREATE SCHEMA IF NOT EXISTS ingest_platform;

-- ---------- 枚举类型（受控词表，对应 contracts/database/enums.md） ----------
-- bag 状态机 rank：discovered < verified < parsed < uploaded < done
DO $$ BEGIN
    CREATE TYPE ingest_platform.bag_state AS ENUM
        ('discovered', 'verified', 'parsed', 'uploaded', 'done', 'parse_failed');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 任务状态机：pending -> running -> done | failed | dead
DO $$ BEGIN
    CREATE TYPE ingest_platform.task_state AS ENUM
        ('pending', 'running', 'done', 'failed', 'dead');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 任务类型
DO $$ BEGIN
    CREATE TYPE ingest_platform.task_type AS ENUM
        ('verify', 'align', 'slice', 'qc', 'export', 'compact', 'enrich');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 产物类型
DO $$ BEGIN
    CREATE TYPE ingest_platform.artifact_class AS ENUM
        ('manifest', 'image', 'pcd', 'table', 'raw', 'meta', 'derived');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 产物上传状态
DO $$ BEGIN
    CREATE TYPE ingest_platform.upload_state AS ENUM
        ('uploaded', 'verified', 'missing', 'corrupt');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 派生资产类型
DO $$ BEGIN
    CREATE TYPE ingest_platform.derived_kind AS ENUM
        ('aligned', 'clip', 'qc', 'enrich');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 命令状态
DO $$ BEGIN
    CREATE TYPE ingest_platform.cmd_ack_status AS ENUM
        ('pending', 'ack', 'timeout', 'failed');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 切片来源
DO $$ BEGIN
    CREATE TYPE ingest_platform.clip_source AS ENUM
        ('event', 'window', 'mined', 'manual');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ========== 1. 事件仓（append-only，按月分区，§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.events (
    event_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_type      TEXT        NOT NULL,
    bag_id          TEXT,
    vehicle_id      TEXT        NOT NULL,
    seq             BIGINT,
    occurred_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    kafka_partition INT,
    kafka_offset    BIGINT,
    origin          TEXT        NOT NULL DEFAULT 'vehicle',  -- vehicle | platform
    CONSTRAINT pk_ingest_events PRIMARY KEY (event_id, received_at)
) PARTITION BY RANGE (received_at);

-- 默认分区（兜底，后续由 gc-svc 按月创建新分区）
CREATE TABLE IF NOT EXISTS ingest_platform.events_default
    PARTITION OF ingest_platform.events DEFAULT;

-- 按月分区示例（gc-svc 自动管理，此处仅为 2026-10 手动创建）
CREATE TABLE IF NOT EXISTS ingest_platform.events_202610
    PARTITION OF ingest_platform.events
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE INDEX IF NOT EXISTS idx_ingest_events_bag_seq  ON ingest_platform.events (bag_id, seq);
CREATE INDEX IF NOT EXISTS idx_ingest_events_vehicle  ON ingest_platform.events (vehicle_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingest_events_type     ON ingest_platform.events (event_type, received_at DESC);


-- ========== 2. bag 主档与状态投影（§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.bags (
    bag_id        TEXT        NOT NULL,
    vehicle_id    TEXT        NOT NULL,
    ros_version   TEXT,
    start_ns      BIGINT,
    end_ns        BIGINT,
    duration_s    NUMERIC(12, 3),
    size_bytes    BIGINT,
    sha256        TEXT,
    message_count BIGINT,
    manifest_rev  TEXT,                              -- input_rev 计算基础
    state         ingest_platform.bag_state NOT NULL DEFAULT 'discovered',
    seq           BIGINT      NOT NULL DEFAULT 0,
    parse_status  TEXT,
    qc_score      NUMERIC(4, 3),                     -- 0.000 ~ 1.000
    manifest      JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_bags PRIMARY KEY (bag_id)
);

CREATE INDEX IF NOT EXISTS idx_bags_vehicle   ON ingest_platform.bags (vehicle_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_bags_state     ON ingest_platform.bags (state);
CREATE INDEX IF NOT EXISTS idx_bags_qc_score  ON ingest_platform.bags (qc_score)
    WHERE qc_score IS NOT NULL;


-- ========== 3. bag topic 明细（§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.bag_topics (
    bag_id        TEXT   NOT NULL REFERENCES ingest_platform.bags (bag_id) ON DELETE CASCADE,
    topic         TEXT   NOT NULL,
    msg_type      TEXT   NOT NULL,
    message_count INT,
    start_ns      BIGINT,
    end_ns        BIGINT,
    hz            NUMERIC(10, 4),
    CONSTRAINT pk_bag_topics PRIMARY KEY (bag_id, topic)
);


-- ========== 4. 产物目录（§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.artifacts (
    object_key    TEXT        NOT NULL,
    bag_id        TEXT,
    artifact_class ingest_platform.artifact_class NOT NULL,
    size_bytes    BIGINT,
    sha256        TEXT,
    etag          TEXT,
    rows          INT,
    count         INT,
    upload_state  ingest_platform.upload_state NOT NULL DEFAULT 'uploaded',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_artifacts PRIMARY KEY (object_key)
);

CREATE INDEX IF NOT EXISTS idx_artifacts_bag_class
    ON ingest_platform.artifacts (bag_id, artifact_class);


-- ========== 5. 派生资产（§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.derived_assets (
    bag_id     TEXT   NOT NULL,
    input_rev  TEXT   NOT NULL,
    kind       ingest_platform.derived_kind NOT NULL,
    object_key TEXT   NOT NULL,
    meta       JSONB  NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_derived_assets PRIMARY KEY (bag_id, input_rev, kind, object_key)
);

CREATE INDEX IF NOT EXISTS idx_derived_bag ON ingest_platform.derived_assets (bag_id, input_rev);


-- ========== 6. 任务编排（§6.2 + §9.1）==========
CREATE TABLE IF NOT EXISTS ingest_platform.tasks (
    task_id         BIGSERIAL   NOT NULL,
    bag_id          TEXT,
    input_rev       TEXT,
    task_type       ingest_platform.task_type NOT NULL,
    state           ingest_platform.task_state NOT NULL DEFAULT 'pending',
    priority        INT         NOT NULL DEFAULT 5,
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 3,
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner     TEXT,
    lease_expires_at TIMESTAMPTZ,
    result          JSONB,
    error_code      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_tasks PRIMARY KEY (task_id),
    -- §5.2 副作用幂等键：同 (bag_id, task_type, input_rev) 只有一条记录
    CONSTRAINT uq_tasks_bag_type_rev UNIQUE (bag_id, task_type, input_rev)
);

CREATE INDEX IF NOT EXISTS idx_tasks_queue
    ON ingest_platform.tasks (state, priority, next_retry_at)
    WHERE state IN ('pending', 'running');


-- ========== 7. 场景切片（§6.2 + §7.3）==========
CREATE TABLE IF NOT EXISTS ingest_platform.clips (
    clip_id     BIGSERIAL   NOT NULL,
    bag_id      TEXT        NOT NULL,
    input_rev   TEXT        NOT NULL,
    start_ns    BIGINT      NOT NULL,
    end_ns      BIGINT      NOT NULL,
    source      ingest_platform.clip_source NOT NULL DEFAULT 'window',
    tags        TEXT[]      NOT NULL DEFAULT '{}',
    scene_label TEXT,
    quality     NUMERIC(4, 3),
    manifest_key TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_clips PRIMARY KEY (clip_id),
    CONSTRAINT uq_clips_bag_rev_span UNIQUE (bag_id, input_rev, start_ns, end_ns, source)
);

CREATE INDEX IF NOT EXISTS idx_clips_bag   ON ingest_platform.clips (bag_id);
CREATE INDEX IF NOT EXISTS idx_clips_tags  ON ingest_platform.clips USING GIN (tags);


-- ========== 8. 标签（§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.labels (
    bag_id  TEXT   NOT NULL,
    clip_id BIGINT NOT NULL DEFAULT 0,       -- 0 表示 bag 级标签
    key     TEXT   NOT NULL,
    value   JSONB  NOT NULL DEFAULT '{}'::jsonb,
    origin  TEXT   NOT NULL DEFAULT 'manual', -- manual | labeling-svc | rule-engine
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_labels PRIMARY KEY (bag_id, clip_id, key, origin)
);

CREATE INDEX IF NOT EXISTS idx_labels_bag ON ingest_platform.labels (bag_id);


-- ========== 9. 车端 Agent 视图（§6.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.agents (
    vehicle_id     TEXT        NOT NULL,
    agent_version  TEXT,
    config_rev     TEXT,
    state          TEXT,
    last_heartbeat TIMESTAMPTZ,
    metrics        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_agents PRIMARY KEY (vehicle_id)
);


-- ========== 10. 命令闭环（§6.2 + §9.4）==========
CREATE TABLE IF NOT EXISTS ingest_platform.commands (
    cmd_id      UUID        NOT NULL DEFAULT gen_random_uuid(),
    vehicle_id  TEXT        NOT NULL,
    type        TEXT        NOT NULL,        -- cleanup | reupload | reparse | seal | set_config
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    issued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ack_at      TIMESTAMPTZ,
    ack_status  ingest_platform.cmd_ack_status NOT NULL DEFAULT 'pending',
    ack_detail  JSONB,
    CONSTRAINT pk_commands PRIMARY KEY (cmd_id)
);

CREATE INDEX IF NOT EXISTS idx_commands_vehicle ON ingest_platform.commands (vehicle_id, issued_at DESC);


-- ========== 11. 投影异常记录（§5.2 层2 乱序兜底）==========
CREATE TABLE IF NOT EXISTS ingest_platform.projection_anomaly (
    id          BIGSERIAL   NOT NULL,
    bag_id      TEXT        NOT NULL,
    event_id    UUID,
    received_seq BIGINT,
    current_seq  BIGINT,
    received_state TEXT,
    event_state    TEXT,
    reason      TEXT,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_projection_anomaly PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_bag ON ingest_platform.projection_anomaly (bag_id, logged_at DESC);


-- ========== 12. 数据集快照（§8.2）==========
CREATE TABLE IF NOT EXISTS ingest_platform.datasets (
    dataset_id    TEXT        NOT NULL,
    version       TEXT        NOT NULL,
    selector      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    format        TEXT        NOT NULL DEFAULT 'webdataset',  -- webdataset | lance
    shuffle_seed  BIGINT,
    template      TEXT,
    manifest_key  TEXT,
    state         TEXT        NOT NULL DEFAULT 'pending',    -- pending | running | done | failed
    clip_count    INT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_datasets PRIMARY KEY (dataset_id, version)
);


-- ========== 13. update_time 触发器 ==========
CREATE OR REPLACE TRIGGER trg_bags_updated_at
    BEFORE UPDATE ON ingest_platform.bags
    FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_tasks_updated_at
    BEFORE UPDATE ON ingest_platform.tasks
    FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_agents_updated_at
    BEFORE UPDATE ON ingest_platform.agents
    FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

CREATE OR REPLACE TRIGGER trg_datasets_updated_at
    BEFORE UPDATE ON ingest_platform.datasets
    FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();
