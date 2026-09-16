# ER 关系说明（contracts/database）

来源：设计文档第 9 章 + 系统上下文基座（表字段清单）。物理外键仅存在于**同一 schema 内**；
跨 schema 引用为**逻辑外键**（不加 FOREIGN KEY 约束，由应用层与数据校验保证），避免服务解耦被破坏。

## 1. 表清单与归属

| schema | 表 | 主键 | 归属服务 | 软删除 |
|--------|----|------|---------|--------|
| `vehicle_svc` | vehicles | vehicle_id (TEXT) | vehicle-service（`/api/v1/vehicle/**`） | 否（用 status） |
| `user_svc` | users | user_id (UUID) | user-service（`/api/v1/user/**`） | 否（用 status） |
| `user_svc` | roles | role_id (UUID) | user-service | 否 |
| `user_svc` | permissions | permission_id (UUID) | user-service | 否 |
| `user_svc` | user_roles | (user_id, role_id) | user-service | 否 |
| `user_svc` | role_permissions | (role_id, permission_id) | user-service | 否 |
| `scene_svc` | scenes | scene_id (UUID) | scene-service | 是（deleted_at） |
| `ota_svc` | ota_versions | version_id (UUID) | ota-service | 否 |
| `ota_svc` | ota_tasks | task_id (UUID) | ota-service | 否 |
| `ota_svc` | ota_records | record_id (BIGSERIAL) | ota-service | 否 |
| `data_collector` | events | event_id (BIGSERIAL) | data-collector | 否 |
| `data_collector` | vehicle_telemetry | (time, vehicle_id) | data-collector | 否（90 天保留策略） |
| `data_analytics` | algorithm_metrics | (time, vehicle_id, module, metric_name) | data-analytics | 否（90 天保留策略） |

预留 schema：`remote_control`（操控会话态在 Redis `rc:session:{vehicle_id}`，录像落 MinIO `hunter-video`）、
`gateway`（审计走结构化日志 + Loki；后续如需落库再按契约建表）。

## 2. 关系（物理外键 = FK，逻辑外键 = LOGICAL）

| 主表 | 从表 | 关系 | 键 / 约束 |
|------|------|------|-----------|
| `user_svc.users` | `user_svc.user_roles` | 1:N | FK user_id ON DELETE CASCADE |
| `user_svc.roles` | `user_svc.user_roles` | 1:N | FK role_id ON DELETE CASCADE |
| `user_svc.roles` | `user_svc.role_permissions` | 1:N | FK role_id ON DELETE CASCADE |
| `user_svc.permissions` | `user_svc.role_permissions` | 1:N | FK permission_id ON DELETE CASCADE |
| `ota_svc.ota_versions` | `ota_svc.ota_tasks` | 1:N | FK target_version_id ON DELETE RESTRICT |
| `ota_svc.ota_tasks` | `ota_svc.ota_records` | 1:N | FK task_id ON DELETE CASCADE，唯一 (task_id, vehicle_id) |
| `vehicle_svc.vehicles` | `data_collector.events` | 1:N | LOGICAL vehicle_id |
| `vehicle_svc.vehicles` | `data_collector.vehicle_telemetry` | 1:N | LOGICAL vehicle_id（时序表不建外键，避免写入放大） |
| `vehicle_svc.vehicles` | `data_analytics.algorithm_metrics` | 1:N | LOGICAL vehicle_id |
| `vehicle_svc.vehicles` | `ota_svc.ota_records` | 1:N | LOGICAL vehicle_id |
| `user_svc.users` | `scene_svc.scenes.creator` | 1:N | LOGICAL creator |
| `user_svc.users` | `ota_svc.ota_tasks.creator` | 1:N | LOGICAL creator |
| `user_svc.users` | `data_collector.events.acknowledged_by` | 1:N | LOGICAL acknowledged_by |
| `ota_svc.ota_tasks` | `ota_svc.ota_records` | 1:N | 见上（灰度批次按 task 聚合统计成功率） |
| `scene_svc.scenes` | MinIO `hunter-scene-assets` | 1:N | 资源文件（地图/模型）经对象地址引用，不入库 |

关联图示（简化）：

```
users ──< user_roles >── roles ──< role_permissions >── permissions
  │                       │
  │ (LOGICAL creator)     └── (LOGICAL 鉴权：user→roles→permissions)
  ├──< scenes
  ├──< ota_tasks ──< ota_records >── vehicles ──< events
  └──< events.acknowledged_by          │
                                       ├──< vehicle_telemetry   (hypertable, 90d)
                                       └──< algorithm_metrics   (hypertable, 90d)
ota_versions ──< ota_tasks
```

## 3. 跨 schema 访问例外（唯一）

| 场景 | 允许方 | 方式 | 约束 |
|------|--------|------|------|
| 离线/实时分析读取时序数据 | data-analytics（Spark 批处理、Flink 作业） | 直读 `data_collector.vehicle_telemetry` / `data_analytics.algorithm_metrics` | 使用**只读账号**（`hunter_analytics_ro`，仅 SELECT 授权，无 DDL/DML）；同步数据通道不影响实时链路 |
| 用户信息补全（展示创建人姓名） | 各服务 | REST 调用 user-service | 禁止直查 `user_svc` |

> 除上表外，禁止跨服务直查数据库（架构原则：模块化解耦）。新增例外必须在 `er.md` 登记并评审。

## 4. 类型约定

| 用途 | 类型 | 说明 |
|------|------|------|
| 标识（users/roles/permissions/scenes/ota_versions/ota_tasks） | `UUID` + `gen_random_uuid()` | PG15 内置函数，无需 pgcrypto |
| 标识（vehicles） | `TEXT` | 等于 X.509 证书 CommonName 与 Kafka 消息 key |
| 高写入流水（events/ota_records） | `BIGSERIAL` | 设计文档指定 |
| 时间 | `TIMESTAMPTZ` | 统一 UTC 存储，前端/网关按时区渲染 |
| 半结构化配置 | `JSONB` | scenes.config_json / upgrade_strategy / schedule / preconditions / progress / changelog / data_json / object_types / tags |
| 多值定长数组 | `TEXT[]` / `INTEGER[]` / `DOUBLE PRECISION[]` | tags / applicable_models / target_vehicles / motor_* / linear_velocity / angular_velocity |
| 百分比 | `SMALLINT` + `CHECK 0..100` | battery_soc / ota_records.progress |
| 校验哈希 | `CHAR(32)` / `CHAR(64)` | MD5 / SHA-256 定长 |

## 5. 与设计文档的差异 / 待核对清单（⚠ 人工确认）

| 项 | 本契约取值 | 依据 / 风险 |
|----|-----------|------------|
| `vehicle_svc` / `user_svc` schema | 新增 2 个 schema 承载车辆与用户主数据（对应网关路由 `/api/v1/vehicle/**`、`/api/v1/user/**`） | 设计文档未给出 schema 名称；L0 仅创建了 6 个业务 schema，故补 2 个并同步 `01-extensions.sql` 与 K8s ConfigMap |
| `scenes.version` | `TEXT` 语义化版本（默认 `1.0.0`） | 设计文档仅写 `version`；若为单调递增整数需改 `INTEGER`（影响迁移与前端展示） |
| `scenes.deleted_at` | 存在（软删除） | 任务要求 Repository 支持软删除；若设计文档采用硬删除需调整 |
| `vehicle_telemetry.seq` | 存在（`BIGINT`） | 遥测消息含 `seq`；用于幂等/丢包检测，是否入表待核对 |
| `ota_records.error_code` | `TEXT` | 兼容平台错误码（6001-6003）与车端自定义码；若设计文档为整型需改 `INTEGER` |
| `ota_records.phase` | 与 `status` 同域（OTA 状态机 9 态） | 设计文档仅给出 `phase`，未给取值域 |
| `ota_versions.create_time` | 未建列（严格按设计文档字段清单） | 若需要"创建时间"筛选需补充 |
| 压缩策略 / 连续聚合 | 未启用 | 90 天保留下压缩可降存储成本；启用时机需与容量规划一并确认 |
