-- =====================================================================
-- HunterCore 分析服务账号（最小权限）— analytics-roles.sql
-- 目标库：hunter_core（业务库 + 时序同库形态）
-- 依赖：先执行 schema.sql + timescaledb.sql（data_collector.vehicle_telemetry /
--        data_analytics.algorithm_metrics 两张 hypertable 已创建；TimescaleDB 会把
--        父表授权自动传播到后续新建 chunk）
-- 幂等：角色用 DO 块存在性判断，口令可重复 ALTER 重置；授权可重复执行
--
-- 来源（禁止自行发明）：
--   1) 角色名与权限边界：contracts/database/er.md 第 3 节「跨 schema 访问例外」
--      · hunter_analytics_ro：data-analytics 跨 schema **只读**（SELECT vehicle_telemetry /
--        algorithm_metrics），读路径与 /readyz 探针走此账号（审查 Y10，无 DDL/DML）；
--      · hunter_analytics_rw：data-analytics 写 **自身** schema data_analytics.algorithm_metrics
--        （algorithm_metrics Topic 落库消费者），仅 INSERT + schema USAGE，
--        禁止 UPDATE/DELETE/DDL 与其他 schema（最小权限）。
--   2) 口令来源：K8s Secret（hunter-app-secrets 的 ANALYTICS_RO_DB_PASSWORD /
--      ANALYTICS_RW_DB_PASSWORD）或单机 .env；经 -v 注入，禁止明文写入本文件/日志。
--
-- 执行（口令经 psql -v 注入，单引号转义；⚠ 占位符换成真实口令，shell 单引号内 '<...>' 会被当作字面量口令；
--   路径为宿主机的仓库内位置，按实际部署目录大小写为准，如 /opt/HunterCore）：
--   docker exec -i hunter-postgres psql -U hunter -d hunter_core -v ON_ERROR_STOP=1 \
--     -v analytics_ro_password='<RO_PWD>' -v analytics_rw_password='<RW_PWD>' \
--     < /opt/HunterCore/infra/deploy/sql/analytics-roles.sql
--
-- ⚠ 单机默认形态可不创建本组角色（compose 默认复用 POSTGRES 超级用户）；
--   生产加固建议执行本脚本，并把 data-analytics 的 ANALYTICS_RO_DB_* / ANALYTICS_RW_DB_* 指向本组账号。
-- =====================================================================

-- ---------- 1) 只读账号 hunter_analytics_ro（仅 SELECT，跨 schema 例外） ----------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hunter_analytics_ro') THEN
        CREATE ROLE hunter_analytics_ro LOGIN;
    END IF;
END
$$;

-- 口令每次重跑都可重置（幂等；psql 顶层语句才对 :'var' 做客户端插值，故不放 DO 块内）
ALTER ROLE hunter_analytics_ro WITH PASSWORD :'analytics_ro_password';

GRANT USAGE ON SCHEMA data_collector, data_analytics TO hunter_analytics_ro;
GRANT SELECT ON data_collector.vehicle_telemetry TO hunter_analytics_ro;
GRANT SELECT ON data_analytics.algorithm_metrics TO hunter_analytics_ro;
-- 撤销任何历史写权限（最小权限收敛，幂等）
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON ALL TABLES IN SCHEMA data_collector, data_analytics FROM hunter_analytics_ro;

-- ---------- 2) 写账号 hunter_analytics_rw（仅写自身 schema algorithm_metrics） ----------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hunter_analytics_rw') THEN
        CREATE ROLE hunter_analytics_rw LOGIN;
    END IF;
END
$$;

ALTER ROLE hunter_analytics_rw WITH PASSWORD :'analytics_rw_password';

-- 仅 data_analytics schema USAGE + algorithm_metrics INSERT（消费者批量 executemany
-- + ON CONFLICT DO NOTHING 无需 SELECT/UPDATE/DELETE）
GRANT USAGE ON SCHEMA data_analytics TO hunter_analytics_rw;
GRANT INSERT ON data_analytics.algorithm_metrics TO hunter_analytics_rw;
-- 明确不授予：跨 schema 读、UPDATE/DELETE/DDL、其他 schema（最小权限，禁止越权写他人表）
REVOKE ALL ON SCHEMA data_collector FROM hunter_analytics_rw;
REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON data_analytics.algorithm_metrics FROM hunter_analytics_rw;

-- ---------- 3) 未来 chunk / 新建表默认权限（TimescaleDB chunk 由父表传播，本条为兜底） ----------
-- 只读账号对 data_analytics 未来表的 SELECT、写账号对 algorithm_metrics 的 INSERT 已由
-- TimescaleDB 父表授权传播保证；此处不改 ALTER DEFAULT PRIVILEGES，避免误扩权限面。
