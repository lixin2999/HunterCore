# contracts/database — 数据库 DDL 契约

单一事实来源：表结构先在此定义 DDL（含 Alembic migration），再写 ORM 实现。

核心表（设计文档第 9 章）：

- 业务表（PostgreSQL）：vehicles / users / roles / user_roles / permissions / role_permissions / scenes / ota_versions / ota_tasks / ota_records / events
- 时序表（TimescaleDB hypertable）：vehicle_telemetry（按天分区，保留 90 天）/ algorithm_metrics

计划文件：`ddl/01_rbac.sql`、`ddl/02_scene.sql`、`ddl/03_ota.sql`、`ddl/04_events.sql`、`ddl/05_timeseries.sql`、`er.md`（ER 关系说明）。

约束：各服务独立数据库 schema；服务间禁止跨库直查；所有变更必须生成 Alembic upgrade/downgrade 脚本。
