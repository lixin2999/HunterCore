-- =====================================================================
-- HunterEdge 数据库契约 — 00 扩展 / schema / 公共函数
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

COMMENT ON FUNCTION public.common_set_update_time() IS 'HunterEdge 公共触发器函数：写入 update_time = now()';
