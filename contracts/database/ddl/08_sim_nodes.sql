-- =====================================================================
-- HunterCore 仿真端管理登记表（scene-service 私有）
-- ---------------------------------------------------------------------
-- 设计来源：V1.24.1 仿真端管理功能（列表/详情 + 服务令牌签发 + 双类型 mTLS 证书签发）
--
-- 边界：
--   * scene_svc.sim_node_registry 为 scene-service **管理侧**持久化数据，
--     与 Redis sim:node:{node_id} 实时登记表互补（Redis 存活态，本表存凭证/标签）；
--   * 节点自注册（PUT /sim-nodes/{node_id}）只写 Redis，管理员首次操作（查看/签发）时
--     懒建本表行（upsert-on-read）；
--   * V1.24.2 一键开通：新增 provision_status / scram_username / scram_credential_updated_at /
--     cert_not_after 四列，对齐 vehicle_svc.vehicles.provision_status 编排态；平台建节点即产出
--     SCRAM 口令 + 双证书 + 接入包（详见 docs/仿真端接入平台系统方案.md）。
--     ⚠ 一次性 SCRAM 口令**不落本表**（仅经开通/轮换响应返回），此处只存口令主体名与更新时间。
--   * 枚举列约定：无枚举字段；标签/编排态存 JSONB；node_id 格式校验由应用层负责。
--
-- 幂等：全部 CREATE ... IF NOT EXISTS / CREATE OR REPLACE TRIGGER，可重复重放。
-- =====================================================================

-- 1. 登记表主结构
CREATE TABLE IF NOT EXISTS scene_svc.sim_node_registry (
    node_id              TEXT        NOT NULL,
    display_name         TEXT        NULL,
    description          TEXT        NULL,
    owner_client_id      TEXT        NULL,   -- 首次注册绑定的调用方身份（service:{client_id}）
    token_id             TEXT        NULL,   -- 已签发服务令牌的 jti；撤销后置 NULL
    token_issued_at      TIMESTAMPTZ NULL,
    kafka_cert_sn        TEXT        NULL,   -- Kafka mTLS 客户端证书序列号（大写 HEX）
    kafka_cert_issued_at TIMESTAMPTZ NULL,
    plat_cert_sn         TEXT        NULL,   -- 平台访问 mTLS 客户端证书序列号（大写 HEX）
    plat_cert_issued_at  TIMESTAMPTZ NULL,
    first_seen_at        TIMESTAMPTZ NULL,   -- 首次注册时间（来自 Redis registered_at 写入时快照）
    labels               JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 管理员自定义标签
    -- ---- V1.24.2 一键开通编排态（对齐 vehicle_svc.vehicles.provision_status）----
    provision_status     JSONB       NOT NULL DEFAULT '{"state":"pending","steps":{}}'::jsonb,
                         -- 骨架 {state,steps:{db,scram,topics,cert}}；聚合态 pending|in_progress|ready|failed
    scram_username       TEXT        NULL,   -- Kafka SCRAM 主体名（= sim-{node_id}，与 Kafka 证书 CN 一致）；口令本身不落库
    scram_credential_updated_at TIMESTAMPTZ NULL,  -- 口令生成/轮换时间（口令本身不存）
    cert_not_after       TIMESTAMPTZ NULL,   -- 双证书到期快照（取 Kafka 证书 notAfter，供到期预警）
    create_time          TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id)
);

-- 2. 按令牌/证书签发时间排序查询（运维视图）
CREATE INDEX IF NOT EXISTS idx_sim_node_registry_token_issued
    ON scene_svc.sim_node_registry (token_issued_at DESC NULLS LAST)
    WHERE token_issued_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sim_node_registry_kafka_cert_issued
    ON scene_svc.sim_node_registry (kafka_cert_issued_at DESC NULLS LAST)
    WHERE kafka_cert_issued_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sim_node_registry_owner
    ON scene_svc.sim_node_registry (owner_client_id)
    WHERE owner_client_id IS NOT NULL;

-- 3. V1.24.2 增列迁移（IF NOT EXISTS，供既有库幂等重放；新库经上方 CREATE TABLE 已含这些列）
ALTER TABLE scene_svc.sim_node_registry
    ADD COLUMN IF NOT EXISTS provision_status            JSONB       NOT NULL DEFAULT '{"state":"pending","steps":{}}'::jsonb,
    ADD COLUMN IF NOT EXISTS scram_username              TEXT        NULL,
    ADD COLUMN IF NOT EXISTS scram_credential_updated_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS cert_not_after              TIMESTAMPTZ NULL;

-- 4. 按编排态过滤（运维视图：开通失败/待处理节点定位）
CREATE INDEX IF NOT EXISTS idx_sim_node_registry_provision_state
    ON scene_svc.sim_node_registry ((provision_status->>'state'))
    WHERE provision_status->>'state' <> 'ready';

-- 5. update_time 触发器（复用 00_schemas.sql 公共函数）
CREATE OR REPLACE TRIGGER trg_sim_node_registry_update_time
    BEFORE UPDATE ON scene_svc.sim_node_registry
    FOR EACH ROW EXECUTE FUNCTION public.common_set_update_time();
