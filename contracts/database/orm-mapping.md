# ORM 映射与 Repository 契约（contracts/database）

本文件是 **ORM 关系映射与数据访问层（Repository）的单一事实来源**，与 `ddl/*.sql`、`er.md` 同级：
DDL 定义表结构，本文件定义「ORM 如何映射这些结构」「每个模型对应哪个 Repository」「Repository 暴露哪些方法」。

实现位置：`common/python/hunter_common/database/`（`models/` 为 ORM 模型，`repositories/` 为数据访问层）。
三方一致性由 `common/python/tests/test_orm_relationships.py`、`common/python/tests/test_repositories.py`
与 `scripts/verify_data_layer.py`（校验 11/12/13）强制校验，**先改本契约，再改实现**。

## 1. 异步安全 lazy 策略规范（不可更改）

异步会话（asyncpg + AsyncSession）中，任何隐式惰性加载都会抛
`MissingGreenlet`（同步 IO 在事件循环内执行）；因此本契约要求 **每条 relationship 必须显式声明 `lazy`**，
且只允许下列取值：

| lazy 取值 | 适用场景 | 理由 |
|-----------|---------|------|
| `selectin` | 多对多（基数小、鉴权/展示高频）、多对一（单对象） | SQLAlchemy 以额外一条 `IN` 查询批量预取，异步安全、无 N+1 |
| `raise_on_sql` | 一对多集合（可能成千上万行，如 `ota_records`）、关联对象（`user_roles` / `role_permissions`） | 未显式 `selectinload` 即报错，强制调用方声明加载策略，避免隐式大集合加载拖垮 P95 ≤ 200ms |

**禁止**：`lazy="select"`（默认值）、`lazy="subquery"`、`lazy="write_only"` 等隐式加载策略（违例由测试拦截）。

## 2. relationship 契约表（与实现逐条对齐）

<!-- relationship-table:start -->
| 关系属性 | 两端 | 关联实现 | lazy 策略 | 级联 / 删除语义 | 说明 |
|----------|------|---------|-----------|----------------|------|
| `User.user_roles` | User → UserRole（1:N） | FK `user_svc.user_roles.user_id` | `raise_on_sql` | `all, delete-orphan` + `passive_deletes=True` | 关联对象按需显式加载；删除用户由 DB `ON DELETE CASCADE` 处理 |
| `User.roles` | User ↔ Role（M:N） | `secondary=user_svc.user_roles` | `selectin` | `viewonly`（写入走关联对象） | 鉴权热路径（user → roles） |
| `Role.user_roles` | Role → UserRole（1:N） | FK `user_svc.user_roles.role_id` | `raise_on_sql` | `all, delete-orphan` + `passive_deletes=True` | 同上 |
| `Role.users` | Role ↔ User（M:N） | `secondary=user_svc.user_roles` | `selectin` | `viewonly` | 反向查询（角色下的用户） |
| `Role.role_permissions` | Role → RolePermission（1:N） | FK `user_svc.role_permissions.role_id` | `raise_on_sql` | `all, delete-orphan` + `passive_deletes=True` | 同上 |
| `Role.permissions` | Role ↔ Permission（M:N） | `secondary=user_svc.role_permissions` | `selectin` | `viewonly` | 鉴权展开（roles → permissions） |
| `Permission.role_permissions` | Permission → RolePermission（1:N） | FK `user_svc.role_permissions.permission_id` | `raise_on_sql` | `all, delete-orphan` + `passive_deletes=True` | 同上 |
| `Permission.roles` | Permission ↔ Role（M:N） | `secondary=user_svc.role_permissions` | `selectin` | `viewonly` | 反向查询（权限点被哪些角色引用） |
| `UserRole.user` | UserRole → User（N:1） | FK `user_svc.user_roles.user_id` | `raise_on_sql` | 无 | 单对象，按需显式加载 |
| `UserRole.role` | UserRole → Role（N:1） | FK `user_svc.user_roles.role_id` | `raise_on_sql` | 无 | 同上 |
| `RolePermission.role` | RolePermission → Role（N:1） | FK `user_svc.role_permissions.role_id` | `raise_on_sql` | 无 | 同上 |
| `RolePermission.permission` | RolePermission → Permission（N:1） | FK `user_svc.role_permissions.permission_id` | `raise_on_sql` | 无 | 同上 |
| `OtaVersion.tasks` | OtaVersion → OtaTask（1:N） | FK `ota_svc.ota_tasks.target_version_id` ON DELETE **RESTRICT** | `raise_on_sql` | `passive_deletes=True`（**不** delete-orphan） | 已发布版本不可删（DDL RESTRICT）；ORM 不得把 `target_version_id` 置空 |
| `OtaTask.version` | OtaTask → OtaVersion（N:1） | FK `ota_svc.ota_tasks.target_version_id` | `selectin` | 无 | 单对象，批量 `IN` 预取 |
| `OtaTask.records` | OtaTask → OtaRecord（1:N） | FK `ota_svc.ota_records.task_id` ON DELETE **CASCADE** | `raise_on_sql` | `all, delete-orphan` + `passive_deletes=True` | 记录量级大，禁止隐式加载 |
| `OtaRecord.task` | OtaRecord → OtaTask（N:1） | FK `ota_svc.ota_records.task_id` | `selectin` | 无 | 单对象，批量 `IN` 预取 |
<!-- relationship-table:end -->

### 2.1 逻辑外键（跨服务）不建 relationship —— 不可新增

跨 schema 引用在 `er.md`「关系」表中标记为 LOGICAL，**必须通过 REST 调用补全**，禁止 ORM 关系（架构原则：模块化解耦）：

| 逻辑外键 | 归属服务 schema | 补全方式 |
|---------|----------------|---------|
| `scene_svc.scenes.creator` → `user_svc.users.user_id` | scene-service | REST 调 user-service |
| `ota_svc.ota_tasks.creator` → `user_svc.users.user_id` | ota-service | REST 调 user-service |
| `data_collector.events.acknowledged_by` → `user_svc.users.user_id` | data-collector | REST 调 user-service |
| `ota_svc.ota_records.vehicle_id` → `vehicle_svc.vehicles.vehicle_id` | ota-service | REST 调 vehicle-service |
| `events.vehicle_id` / `vehicle_telemetry.vehicle_id` / `algorithm_metrics.vehicle_id` | data-collector / data-analytics | REST 调 vehicle-service（时序表另**禁止建外键**，避免写入放大） |

## 3. Repository 契约（命名 / 归属 / 方法）

**命名规范**：`<Entity>Repository`，与被映射的 ORM 类同名对应；一个模型**恰好**一个 Repository 类。
**事务边界**：Repository 只 `flush`，**不在内部提交/回滚**（事务边界由调用方 / `DatabaseSessionManager.session()` 控制）。
**错误码**：唯一冲突 → 3002、缺失 → 3001、非法列名/参数 → 2001（均由 `BaseRepository` 统一映射）。
**查询安全**：`filters` 键必须是真实列名；条件一律参数绑定，禁止字符串拼接 SQL。

<!-- repository-table:start -->
| Repository | 模型 | 模块 | 专属方法（映射的契约索引） |
|------------|------|------|---------------------------|
| `VehicleRepository` | `Vehicle` | `repositories/core.py` | `get_by_device_cert_sn`（`uq_vehicles_device_cert_sn`）、`list_by_status`（`idx_vehicles_status_last_online`）、`update_status` |
| `UserRepository` | `User` | `repositories/core.py` | `get_by_username`（`uq_users_username`）、`get_by_email`（`uq_users_email`，小写比较）、`list_role_codes` / `list_permission_codes`（RBAC 展开）、`touch_last_login` |
| `RoleRepository` | `Role` | `repositories/core.py` | `get_by_role_code`（`uq_roles_role_code`）、`list_enabled`（`roles.status`） |
| `PermissionRepository` | `Permission` | `repositories/core.py` | `get_by_permission_code`（`uq_permissions_permission_code`）、`list_by_resource`（`idx_permissions_resource_action`） |
| `UserRoleRepository` | `UserRole` | `repositories/core.py` | `get_pair`、`list_role_ids`（复合主键前缀）、`link`（幂等绑定）、`unlink` |
| `RolePermissionRepository` | `RolePermission` | `repositories/core.py` | `get_pair`、`list_permission_ids`（复合主键前缀）、`link`（幂等绑定）、`unlink` |
| `SceneRepository` | `Scene` | `repositories/scene.py` | `get_by_scene_name`（`uq_scenes_scene_name`，仅存活场景） |
| `OtaVersionRepository` | `OtaVersion` | `repositories/ota.py` | `get_by_version_code`（`uq_ota_versions_version_code`）、`get_by_version_name`（`uq_ota_versions_version_name`）、`max_version_code`（`version_code` 单调递增基准） |
| `OtaTaskRepository` | `OtaTask` | `repositories/ota.py` | `list_by_status`（`idx_ota_tasks_status_create_time`）、`list_by_target_version`（`idx_ota_tasks_target_version_id`） |
| `OtaRecordRepository` | `OtaRecord` | `repositories/ota.py` | `get_by_task_vehicle`（`uq_ota_records_task_vehicle`）、`list_inflight`（`idx_ota_records_inflight`）、`list_by_vehicle`（`idx_ota_records_vehicle_start_time`）、`status_counts`（灰度成功率分子/分母） |
| `EventRepository` | `Event` | `repositories/collector.py` | `get_by_vehicle_type_time`（`uq_events_vehicle_type_time` 幂等）、`list_by_vehicle`（`idx_events_vehicle_time`）、`list_unacknowledged`（`idx_events_unacknowledged`）、`acknowledge` |
| `VehicleTelemetryRepository` | `VehicleTelemetry` | `repositories/collector.py` | `insert_points`（`ON CONFLICT (time, vehicle_id) DO NOTHING`）、`get_point`、`list_points`（`idx_vehicle_telemetry_vehicle_time`）、`latest_point`、`purge_before` |
| `AlgorithmMetricRepository` | `AlgorithmMetric` | `repositories/analytics.py` | `insert_metrics`（`ON CONFLICT (time, vehicle_id, module, metric_name) DO NOTHING`）、`list_series`（`idx_algorithm_metrics_module_metric_time`）、`latest` |
<!-- repository-table:end -->

### 3.1 通用方法契约（`BaseRepository`，所有 Repository 共享）

| 方法 | 语义 | 契约要点 |
|------|------|---------|
| `get` / `get_or_raise` | 主键查询 | 缺失：`get` 返回 None，`get_or_raise` 抛 3001 |
| `find_one` / `find_all` | 自定义条件查询 | 条件为 ORM 列表达式（参数绑定）；自动附加软删除过滤 |
| `list` / `paginate` | 列表 / 分页 | `page ≥ 1`、`1 ≤ page_size ≤ 200`（保护 P95 ≤ 200ms）；排序 `-` 前缀 = DESC |
| `count` / `exists` | 计数 / 存在性 | `exists` 走 `LIMIT 1`，比 `count` 轻 |
| `create` / `update` | 单条写 | 仅 `flush`，不 commit；唯一冲突 → 3002、非法字段 → 2001 |
| `bulk_create` | 批量插入 | executemany 分片（`BULK_CHUNK_SIZE`），时序写入 ≥ 10000 点/秒 |
| `bulk_create_ignore_conflicts` | 幂等批量插入 | `ON CONFLICT (…) DO NOTHING`，消费重放/重复时间点跳过 |
| `delete_where` / `hard_delete` | 条件删除 / 物理删除 | 仅用于关联表解绑与超期数据清理；`scenes` 一律走 `soft_delete` |
| `soft_delete` | 软删除 | 仅 `deleted_at` 模型（当前仅 `Scene`）；其他模型抛 `NotImplementedError` |

**默认排序**：`VehicleRepository` → `last_online_time DESC`（在线看板）、`SceneRepository` → `create_time DESC`、
`OtaVersionRepository` → `release_time DESC`、`OtaRecordRepository` → `start_time DESC`、
`EventRepository` → `event_time DESC`（均对齐 DDL 索引与契约列表排序约定）。

## 4. 校验方式

```bash
# ORM 关系契约 + Repository 契约（无需数据库）
pytest common/python/tests/test_orm_relationships.py -q
pytest common/python/tests/test_repositories.py -q

# 数据层契约校验（含校验 11 模型↔仓库、12 relationship 异步安全、13 本文件与实现同步）
python scripts/verify_data_layer.py
```

