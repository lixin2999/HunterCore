-- =====================================================================
-- HunterEdge PostgreSQL 初始化（容器首次启动时由 docker-entrypoint-initdb.d 执行）
-- 镜像：timescale/timescaledb:2.13.1-pg15
-- =====================================================================

-- 启用 TimescaleDB 扩展（vehicle_telemetry / algorithm_metrics hypertable 依赖，设计文档第 9 章）
-- 注意：hypertable 建表与分区（chunk_time_interval=1 day，保留 90 天）由
-- contracts/database 契约确认后通过 Alembic migration 执行，此处不建表。
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 服务独立 schema（模块解耦：每个微服务独立 schema，禁止跨服务直查）
-- 与 contracts/database/ddl/00_schemas.sql 保持一致（修改需同步两处）
CREATE SCHEMA IF NOT EXISTS vehicle_svc;      -- 车辆主数据（vehicles）
CREATE SCHEMA IF NOT EXISTS user_svc;         -- 用户与 RBAC（users/roles/permissions...）
CREATE SCHEMA IF NOT EXISTS scene_svc;
CREATE SCHEMA IF NOT EXISTS data_collector;
CREATE SCHEMA IF NOT EXISTS data_analytics;
CREATE SCHEMA IF NOT EXISTS ota_svc;
CREATE SCHEMA IF NOT EXISTS remote_control;
CREATE SCHEMA IF NOT EXISTS gateway;
