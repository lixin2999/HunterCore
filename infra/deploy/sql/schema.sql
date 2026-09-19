-- =====================================================================
-- HunterCore 单机部署包 — 业务库结构 schema.sql
-- ⚠ GENERATED FILE（生成产物）：请勿手工编辑；单一事实来源 = contracts/database/ddl/
-- 内容 = 00_schemas.sql + 01_core.sql + 02_scene.sql + 03_ota.sql + 04_events.sql
-- 表清单（11 张：字段名/类型/约束与设计文档第 9 章一致，不可更改）
--   vehicle_svc.vehicles
--   user_svc.users / roles / permissions / user_roles / role_permissions（RBAC 五表）
--   scene_svc.scenes
--   ota_svc.ota_versions / ota_tasks / ota_records
--   data_collector.events
-- 时序表（vehicle_telemetry / algorithm_metrics）见同目录 timescaledb.sql
-- 重新生成（仓库根目录执行）：
--   $ cat contracts/database/ddl/00_schemas.sql contracts/database/ddl/01_core.sql \
--         contracts/database/ddl/02_scene.sql contracts/database/ddl/03_ota.sql \
--         contracts/database/ddl/04_events.sql > infra/deploy/sql/schema.sql
-- 幂等：全部语句使用 IF NOT EXISTS / CREATE OR REPLACE，可重复执行
-- 执行：psql -U hunter -d hunter_core -f schema.sql
-- 注：契约要求生产/测试统一经 Alembic 执行（cd common/python && alembic upgrade head）；
--     本文件用于单机 docker compose 部署的等价初始化路径，二者结构必须保持一致
-- =====================================================================
-- =====================================================================
-- HunterCore 数据库契约 — 00 扩展 / schema / 公共函数
-- 来源：设计文档第 9 章（数据层）
-- 约束：本目录 SQL 为数据库结构的单一事实来源；ORM 模型与 Alembic migration 必须与本文件保持一致
-- 幂等：全部使用 IF NOT EXISTS / CREATE OR REPLACE，可重复执行
-- ⚠ 需核对（设计文档未随仓库提供）：
--   - vehicle_svc / user_svc 为本契约新增 schema（承载车辆与用户主数据，对应网关路由
--     /api/v1/vehicle/** 与 /api/v1/user/** 两个服务域）；gateway / remote_control 为预留 schema
-- =====================================================================

-- ---------- 扩展 ----------
-- TimescaleDB 2.13：vehicle_telemetry / algorithm_metrics 的 hypertable 与保留策略依赖
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------- 服务独立 schema（模块解耦：各服务独立 schema，禁止跨服务直查） ----------
CREATE SCHEMA IF NOT EXISTS vehicle_svc;      -- 车辆主数据（vehicles）
CREATE SCHEMA IF NOT EXISTS user_svc;         -- 用户与 RBAC（users/roles/user_roles/permissions/role_permissions）
CREATE SCHEMA IF NOT EXISTS scene_svc;        -- 场景库（scenes）
CREATE SCHEMA IF NOT EXISTS data_collector;   -- 采集接入（events + vehicle_telemetry 时序写入）
CREATE SCHEMA IF NOT EXISTS data_analytics;   -- 分析结果（algorithm_metrics 时序）
CREATE SCHEMA IF NOT EXISTS ota_svc;          -- OTA（ota_versions/ota_tasks/ota_records）
CREATE SCHEMA IF NOT EXISTS remote_control;   -- 预留：操控会话归档（录像落 MinIO hunter-video，Redis 存会话态）
CREATE SCHEMA IF NOT EXISTS gateway;          -- 预留：网关审计（当前仅 Redis/日志，后续按契约建表）

-- ---------- 公共函数：update_time 自动维护 ----------
-- 场景等含 create_time/update_time 的表通过触发器维护 update_time，
-- 保证直接 SQL 写入（运维/批处理）时 update_time 仍正确（ORM onupdate 仅覆盖 ORM 路径）
CREATE OR REPLACE FUNCTION public.common_set_update_time()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.update_time := now();
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION public.common_set_update_time() IS 'HunterCore 公共触发器函数：写入 update_time = now()';

-- =====================================================================
-- HunterCore 数据库契约 — 01 车辆主数据 + 用户与 RBAC
-- schema：vehicle_svc（车辆）/ user_svc（用户与权限五表）
-- 来源：设计文档第 9 章；字段名、类型、约束不可更改
-- ⚠ 需核对：status（users）取值域、email/phone 长度约束按最小必要原则定义
-- =====================================================================

-- ---------- vehicle_svc.vehicles：车辆台账 ----------
CREATE TABLE IF NOT EXISTS vehicle_svc.vehicles (
    vehicle_id       TEXT        PRIMARY KEY,   -- 车辆唯一标识（如 HUNTER-001）= 设备证书 CommonName = Kafka 消息 key
    vehicle_name     TEXT        NOT NULL,
    model            TEXT        NOT NULL DEFAULT 'HUNTER_SE',  -- 硬件基线：HUNTER SE 阿克曼 UGV
    firmware_version TEXT,
    software_version TEXT,
    status           TEXT        NOT NULL DEFAULT 'offline'
        CHECK (status IN ('offline', 'online_idle', 'auto_driving', 'remote_controlled',
                          'upgrading', 'charging', 'fault', 'emergency')),
    last_online_time TIMESTAMPTZ,                               -- 遥测/心跳最近上报时间
    register_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    device_cert_sn   TEXT,                                      -- X.509 设备证书序列号
    description      TEXT
);
COMMENT ON TABLE vehicle_svc.vehicles IS
    '车辆台账；status 取值见设计文档“车辆状态定义”（8 态，不可新增/更改）';

-- 设备证书序列号唯一（部分唯一索引：允许未签发证书的车辆先登记）
CREATE UNIQUE INDEX IF NOT EXISTS uq_vehicles_device_cert_sn
    ON vehicle_svc.vehicles (device_cert_sn) WHERE device_cert_sn IS NOT NULL;
-- 车队状态看板查询：按状态过滤 + 最近在线倒序
CREATE INDEX IF NOT EXISTS idx_vehicles_status_last_online
    ON vehicle_svc.vehicles (status, last_online_time DESC NULLS LAST);

-- ---------- user_svc.users：用户 ----------
CREATE TABLE IF NOT EXISTS user_svc.users (
    user_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    username        TEXT        NOT NULL,
    password_hash   TEXT        NOT NULL,       -- bcrypt 哈希，禁止明文/可逆加密（安全机制）
    real_name       TEXT,
    email           TEXT,
    phone           TEXT,
    status          TEXT        NOT NULL DEFAULT 'enabled'
        CHECK (status IN ('enabled', 'disabled', 'locked')),  -- ⚠ 需核对：locked 用于登录失败锁定
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_time TIMESTAMPTZ
);
COMMENT ON TABLE user_svc.users IS '平台用户；password_hash 使用 bcrypt，禁止在日志/接口中输出';

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_username ON user_svc.users (username);
-- 邮箱唯一（大小写不敏感，且允许为空）
CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email
    ON user_svc.users (lower(email)) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_status ON user_svc.users (status);

-- ---------- user_svc.roles：角色 ----------
CREATE TABLE IF NOT EXISTS user_svc.roles (
    role_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    role_code   TEXT        NOT NULL,        -- 角色编码（如 admin / operator / analyst / viewer）
    role_name   TEXT        NOT NULL,
    description TEXT,
    status      TEXT        NOT NULL DEFAULT 'enabled'
        CHECK (status IN ('enabled', 'disabled')),
    create_time TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE user_svc.roles IS '角色（RBAC）：role_code 为对外稳定标识，禁止用 role_id 做业务判断';
CREATE UNIQUE INDEX IF NOT EXISTS uq_roles_role_code ON user_svc.roles (role_code);

-- ---------- user_svc.permissions：权限点 ----------
CREATE TABLE IF NOT EXISTS user_svc.permissions (
    permission_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    permission_code TEXT        NOT NULL,    -- 权限编码，形如 <resource>:<action>（如 scene:create / ota:release）
    permission_name TEXT        NOT NULL,
    resource        TEXT        NOT NULL,    -- 资源域：scene / data / analytics / ota / remote / vehicle / user
    action          TEXT        NOT NULL,    -- 动作：create / read / update / delete / execute
    description     TEXT,
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE user_svc.permissions IS
    '权限点（RBAC）；resource 与网关路由表资源域一一对应，便于前端 v-permission 指令校验';
CREATE UNIQUE INDEX IF NOT EXISTS uq_permissions_permission_code
    ON user_svc.permissions (permission_code);
CREATE INDEX IF NOT EXISTS idx_permissions_resource_action
    ON user_svc.permissions (resource, action);

-- ---------- user_svc.user_roles：用户-角色关联 ----------
CREATE TABLE IF NOT EXISTS user_svc.user_roles (
    user_id     UUID        NOT NULL REFERENCES user_svc.users (user_id) ON DELETE CASCADE,
    role_id     UUID        NOT NULL REFERENCES user_svc.roles (role_id) ON DELETE CASCADE,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, role_id)
);
COMMENT ON TABLE user_svc.user_roles IS '用户-角色关联（多对多）；删除用户/角色级联清理';
CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON user_svc.user_roles (role_id);

-- ---------- user_svc.role_permissions：角色-权限关联 ----------
CREATE TABLE IF NOT EXISTS user_svc.role_permissions (
    role_id       UUID        NOT NULL REFERENCES user_svc.roles (role_id) ON DELETE CASCADE,
    permission_id UUID        NOT NULL REFERENCES user_svc.permissions (permission_id) ON DELETE CASCADE,
    create_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, permission_id)
);
COMMENT ON TABLE user_svc.role_permissions IS '角色-权限关联（多对多）；鉴权时按 user → roles → permissions 展开';
CREATE INDEX IF NOT EXISTS idx_role_permissions_permission_id
    ON user_svc.role_permissions (permission_id);


-- =====================================================================
-- HunterCore 数据库契约 — 02 场景库
-- schema：scene_svc
-- 来源：设计文档第 9 章；字段名、类型、约束不可更改
-- ⚠ 需核对：
--   - scene_type 取值域（设计文档未提供枚举，暂不设 CHECK，禁止前端硬编码分支）
--   - deleted_at 为软删除标记（通用 Repository 软删除能力所需）
--   - version 采用语义化版本字符串（导出 OpenSCENARIO 时写入 x-scenario-version）
-- =====================================================================

CREATE TABLE IF NOT EXISTS scene_svc.scenes (
    scene_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    scene_name  TEXT        NOT NULL,
    scene_type  TEXT        NOT NULL,                                    -- ⚠ 取值域待核对（如 highway/urban/...）
    description TEXT,
    config_json JSONB       NOT NULL DEFAULT '{}'::jsonb,                -- 场景参数化配置（结构见 OpenSCENARIO 导出契约）
    version     TEXT        NOT NULL DEFAULT '1.0.0',
    creator     UUID        NOT NULL,                                    -- 逻辑外键 → user_svc.users.user_id（跨服务不建物理外键）
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags        TEXT[]      NOT NULL DEFAULT '{}',
    status      TEXT        NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'published', 'archived')),
    deleted_at  TIMESTAMPTZ                                              -- 软删除：非 NULL 即视为已删除
);
COMMENT ON TABLE scene_svc.scenes IS
    '场景库（scene-service 拥有）；status 三态 draft/published/archived，仅 draft 可编辑';
COMMENT ON COLUMN scene_svc.scenes.config_json IS '场景参数化配置 JSONB；写入即校验 OpenSCENARIO 结构';
COMMENT ON COLUMN scene_svc.scenes.deleted_at IS '软删除时间；所有查询默认过滤 deleted_at IS NULL';

-- 列表检索：按状态 + 类型 + 创建时间倒序（默认列表页）
CREATE INDEX IF NOT EXISTS idx_scenes_status_type_create_time
    ON scene_svc.scenes (status, scene_type, create_time DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_scenes_scene_type ON scene_svc.scenes (scene_type);
-- 标签检索（tags TEXT[] 包含查询）
CREATE INDEX IF NOT EXISTS idx_scenes_tags ON scene_svc.scenes USING GIN (tags);
-- 参数化配置内的 JSONB 检索（如按 scenario 类型/路网筛选）
CREATE INDEX IF NOT EXISTS idx_scenes_config_json ON scene_svc.scenes USING GIN (config_json);
-- 名称唯一（同一存活场景内不可重名）
CREATE UNIQUE INDEX IF NOT EXISTS uq_scenes_scene_name
    ON scene_svc.scenes (scene_name) WHERE deleted_at IS NULL;

-- update_time 自动维护（ORM onupdate + 触发器双保险）
DROP TRIGGER IF EXISTS trg_scenes_set_update_time ON scene_svc.scenes;
CREATE TRIGGER trg_scenes_set_update_time
    BEFORE UPDATE ON scene_svc.scenes
    FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();

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
        CHECK (status IN ('draft', 'published', 'deprecated', 'disabled')),  -- ⚠ 需核对
    release_time      TIMESTAMPTZ
);
COMMENT ON TABLE ota_svc.ota_versions IS
    'OTA 版本仓库；发布前必须完成 SHA-256 校验 + RSA-2048 验签 + version_code 单调性检查';

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


-- =====================================================================
-- HunterCore 数据库契约 — 04 事件
-- schema：data_collector
-- 来源：设计文档第 9 章；字段名、类型、约束不可更改
-- 约束：event_type 取值 = 设计文档“事件类型定义”（18 种，触发阈值不可更改）；
--       event_level ∈ (info, warning, critical)
-- =====================================================================

CREATE TABLE IF NOT EXISTS data_collector.events (
    event_id         BIGSERIAL   PRIMARY KEY,
    vehicle_id       TEXT        NOT NULL,          -- 逻辑外键 → vehicle_svc.vehicles.vehicle_id
    event_type       TEXT        NOT NULL
        CHECK (event_type IN ('harsh_acceleration', 'harsh_braking', 'harsh_turning', 'over_speed',
                              'collision_warning', 'manual_takeover', 'emergency_stop', 'battery_low',
                              'battery_critical', 'communication_loss', 'sensor_fault', 'perception_fault',
                              'planning_fault', 'control_fault', 'ota_start', 'ota_success',
                              'ota_failed', 'ota_rollback')),
    event_level      TEXT        NOT NULL
        CHECK (event_level IN ('info', 'warning', 'critical')),
    event_time       TIMESTAMPTZ NOT NULL,          -- 车端事件发生时间（非入库时间）
    description      TEXT,
    data_json        JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 事件上下文（阈值、实测值、轨迹片段摘要）
    data_file_url    TEXT,                          -- 关联文件（MinIO hunter-rosbag / hunter-raw-data / hunter-video）
    acknowledged     BOOLEAN     NOT NULL DEFAULT FALSE,
    acknowledged_by  UUID,                          -- 逻辑外键 → user_svc.users.user_id
    acknowledge_time TIMESTAMPTZ
);
COMMENT ON TABLE data_collector.events IS
    '车辆事件；event_level 由 event_type 决定（阈值见设计文档“事件类型定义”，代码不得放宽）';
COMMENT ON COLUMN data_collector.events.data_file_url IS 'MinIO 对象地址（下载用 15 分钟预签名 URL，不落长期签名）';

-- 单车事件时间线（车辆详情页）
CREATE INDEX IF NOT EXISTS idx_events_vehicle_time
    ON data_collector.events (vehicle_id, event_time DESC);
-- 按类型统计（分类报表 / Corner Case 挖掘）
CREATE INDEX IF NOT EXISTS idx_events_type_time
    ON data_collector.events (event_type, event_time DESC);
-- 告警中心：仅未确认事件（部分索引，critical 优先展示）
CREATE INDEX IF NOT EXISTS idx_events_unacknowledged
    ON data_collector.events (event_level, event_time DESC) WHERE acknowledged = FALSE;
-- 事件上下文 JSONB 检索
CREATE INDEX IF NOT EXISTS idx_events_data_json
    ON data_collector.events USING GIN (data_json);
-- 消费幂等保护：Kafka at-least-once 重放时同一 (vehicle_id, event_type, event_time) 不重复入库
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_vehicle_type_time
    ON data_collector.events (vehicle_id, event_type, event_time);
