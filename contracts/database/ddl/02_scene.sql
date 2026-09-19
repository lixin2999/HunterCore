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
