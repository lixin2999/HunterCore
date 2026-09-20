-- =====================================================================
-- HunterCore 数据库契约 — 03 OTA（版本 / 任务 / 记录）
-- schema：ota_svc
-- 来源：设计文档第 9 章；字段名、类型、约束不可更改
-- 约束：
--   - version_code 单调递增（防回滚，OTA 安全）
--   - package_md5 / package_sha256 / signature 为完整性校验与数字验签必需字段（禁止置空）
--   - ota_records.status/phase 取值 = 车端 OTA 状态机（IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS，
--     自检失败 → ROLLBACK → ROLLED_BACK / FAILED），不可更改
-- ⚠ 需核对：release_type、ota_versions.status、ota_tasks.status 取值域（设计文档未提供枚举）
-- =====================================================================

-- ---------- ota_svc.ota_versions：版本仓库 ----------
CREATE TABLE IF NOT EXISTS ota_svc.ota_versions (
    version_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    version_name      TEXT        NOT NULL,                              -- 展示名，如 V1.2.0
    version_code      INTEGER     NOT NULL CHECK (version_code > 0),     -- 单调递增整数（防回滚）
    release_type      TEXT        NOT NULL,                              -- ⚠ 取值域待核对（正式/灰度/补丁）
    package_url       TEXT        NOT NULL,                              -- MinIO hunter-ota-packages 对象地址
    package_size      BIGINT      NOT NULL CHECK (package_size > 0),     -- 字节
    package_md5       CHAR(32)    NOT NULL,
    package_sha256    CHAR(64)    NOT NULL,
    signature         TEXT        NOT NULL,                              -- RSA-2048 签名（base64）
    changelog         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    applicable_models TEXT[]      NOT NULL DEFAULT ARRAY['HUNTER_SE']::TEXT[],
    status            TEXT        NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'testing', 'reviewing', 'published', 'deprecated', 'disabled')),  -- G-18② 定稿（§7.2.2 审核流六态）
    release_time      TIMESTAMPTZ
);
COMMENT ON TABLE ota_svc.ota_versions IS
    'OTA 版本仓库；状态机 draft→testing→reviewing→published→deprecated/disabled（G-18②）；发布前必须完成 SHA-256 校验 + RSA-2048 验签 + version_code 单调性检查';

CREATE UNIQUE INDEX IF NOT EXISTS uq_ota_versions_version_code
    ON ota_svc.ota_versions (version_code);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ota_versions_version_name
    ON ota_svc.ota_versions (version_name);
CREATE INDEX IF NOT EXISTS idx_ota_versions_status_release_time
    ON ota_svc.ota_versions (status, release_time DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_ota_versions_applicable_models
    ON ota_svc.ota_versions USING GIN (applicable_models);

-- ---------- ota_svc.ota_tasks：升级任务（灰度发布） ----------
CREATE TABLE IF NOT EXISTS ota_svc.ota_tasks (
    task_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_name         TEXT        NOT NULL,
    target_version_id UUID        NOT NULL
        REFERENCES ota_svc.ota_versions (version_id) ON DELETE RESTRICT,  -- 已发布版本不可被删除
    target_vehicles   TEXT[]      NOT NULL DEFAULT '{}',
    upgrade_strategy  JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 灰度批次配置（默认 5%→20%→50%→100%，每批观察 24h）
    schedule          JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 调度窗口（立即/定时）
    preconditions     JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 升级门禁：SOC≥50%、静止(P档)、网络稳定、存储≥2GB
    status            TEXT        NOT NULL DEFAULT 'created'
        CHECK (status IN ('created', 'pending_approval', 'running', 'paused',
                          'succeeded', 'failed', 'canceled')),       -- ⚠ 需核对
    progress          JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 各批次进度快照（同步写 Redis ota:progress:{task_id}）
    creator           UUID        NOT NULL,                       -- 逻辑外键 → user_svc.users.user_id
    create_time       TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE ota_svc.ota_tasks IS
    'OTA 升级任务；灰度任一批次成功率 < 95% 时立即置为 paused 并告警（人工介入）';

CREATE INDEX IF NOT EXISTS idx_ota_tasks_status_create_time
    ON ota_svc.ota_tasks (status, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_ota_tasks_target_vehicles
    ON ota_svc.ota_tasks USING GIN (target_vehicles);
CREATE INDEX IF NOT EXISTS idx_ota_tasks_target_version_id
    ON ota_svc.ota_tasks (target_version_id);

-- ---------- ota_svc.ota_records：单车辆升级执行记录 ----------
CREATE TABLE IF NOT EXISTS ota_svc.ota_records (
    record_id     BIGSERIAL   PRIMARY KEY,
    task_id       UUID        NOT NULL
        REFERENCES ota_svc.ota_tasks (task_id) ON DELETE CASCADE,
    vehicle_id    TEXT        NOT NULL,          -- 逻辑外键 → vehicle_svc.vehicles.vehicle_id
    from_version  TEXT,                          -- 升前版本（首次装机可为 NULL）
    to_version    TEXT        NOT NULL,          -- 目标版本名
    -- status / phase 取值 = 车端 OTA 状态机（不可更改）
    status        TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('IDLE', 'PENDING', 'DOWNLOAD', 'INSTALL', 'TEST',
                          'SUCCESS', 'ROLLBACK', 'ROLLED_BACK', 'FAILED')),
    phase         TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (phase IN ('IDLE', 'PENDING', 'DOWNLOAD', 'INSTALL', 'TEST',
                         'SUCCESS', 'ROLLBACK', 'ROLLED_BACK', 'FAILED')),
    progress      SMALLINT    NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    error_code    TEXT,                          -- 车端/平台错误码（如 6001/6002/6003，字符串以兼容车端自定义码）
    error_message TEXT,
    start_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time      TIMESTAMPTZ
);
COMMENT ON TABLE ota_svc.ota_records IS
    'OTA 单车执行记录；灰度成功率 = SUCCESS 数 / 本批次总数（阈值 ≥95%，低于则暂停并告警）';

-- 同一任务对同一车辆仅一条记录（升级重试在原记录上推进 phase/progress）
CREATE UNIQUE INDEX IF NOT EXISTS uq_ota_records_task_vehicle
    ON ota_svc.ota_records (task_id, vehicle_id);
-- 车辆升级历史（车辆详情页时间线）
CREATE INDEX IF NOT EXISTS idx_ota_records_vehicle_start_time
    ON ota_svc.ota_records (vehicle_id, start_time DESC);
-- 进行中记录（灰度监控 / 卡死检测）：仅非终态
CREATE INDEX IF NOT EXISTS idx_ota_records_inflight
    ON ota_svc.ota_records (status, start_time)
    WHERE status IN ('PENDING', 'DOWNLOAD', 'INSTALL', 'TEST', 'ROLLBACK');
-- 失败排查：按错误码聚合
CREATE INDEX IF NOT EXISTS idx_ota_records_error_code
    ON ota_svc.ota_records (error_code, start_time DESC) WHERE error_code IS NOT NULL;

