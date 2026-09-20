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
**事务边界**：Repository 只 `flush`，**不在内部提交/回滚**（事务边界由调用方 / `DatabaseSessionManager.session()` 控制）；
`create` / `update` 与批量写入的每个分片用 **SAVEPOINT**（`begin_nested`）包裹，约束冲突只回滚该 SAVEPOINT，
**调用方外事务与已写入分片不受影响**（捕获 3002/2001 后无需丢弃整个事务）。

<!-- write-path-discipline:start -->
**写路径纪律（SAVEPOINT 顺序；实现与测试强校验，不可违反）**：

1. `create` / `update` 的 `add` / `setattr` **必须位于 `begin_nested()` 之后**：SQLAlchemy 进入
   SAVEPOINT 时会先 `flush()` 未决变更（`SessionTransaction._take_snapshot`），写在 SAVEPOINT 之前的语句
   会落到 SAVEPOINT **之外**，冲突时整个会话事务被回滚（后续 `commit()` 抛 `PendingRollbackError`，
   调用方已写数据全部丢失）。
2. 冲突后**禁止调用 `session.expunge()`**：SAVEPOINT 回滚已把失败行移出会话，再 `expunge` 会抛
   `InvalidRequestError`，使本应返回的 3002/2001 退化为 5000。
3. 同类纪律适用于批量路径：每个分片在 SAVEPOINT **内部**执行（`execute` 之前不得有未决 ORM 变更）。
4. 校验手段：`common/python/tests/test_repository_transactions.py`（真实 Session，unit 阶段）+
   `tests/integration/test_data_layer_transactions.py`（真实 PostgreSQL，integration 阶段）。
<!-- write-path-discipline:end -->
**错误码**：唯一冲突 → 3002、缺失 → 3001、非法列名/字段/参数 → 2001（均由 `BaseRepository` 统一映射，
且 `details` 只允许携带模型名 / SQLSTATE / 约束名，**禁止**携带驱动原始报错文本与行值）。
**查询安全**：`filters` 键必须是真实列名；条件一律参数绑定，禁止字符串拼接 SQL；
写路径（`create` / `bulk_create` / `bulk_create_ignore_conflicts`）同样做列名白名单校验。
**方法级同步**：本表「专属方法」列与实现由 `tests/test_orm_relationships.py::test_repository_methods_match_contract`
与 `scripts/verify_data_layer.py`（校验 13）双向强校验 —— 新增/删除/改名专属方法必须**先改本表**。

<!-- repository-table:start -->
| Repository | 模型 | 模块 | 专属方法（映射的契约索引） |
|------------|------|------|---------------------------|
| `VehicleRepository` | `Vehicle` | `repositories/core.py` | `get_by_device_cert_sn`（`uq_vehicles_device_cert_sn`）、`list_by_status`（`idx_vehicles_status_last_online`）、`update_status`（单条 `UPDATE ... RETURNING`，1 次往返） |
| `UserRepository` | `User` | `repositories/core.py` | `get_by_username`（`uq_users_username`）、`get_by_email`（`uq_users_email`，小写比较）、`list_role_codes` / `list_permission_codes`（RBAC 展开）、`touch_last_login`（单条 UPDATE + rowcount 判定） |
| `RoleRepository` | `Role` | `repositories/core.py` | `get_by_role_code`（`uq_roles_role_code`）、`list_enabled`（`roles.status`） |
| `PermissionRepository` | `Permission` | `repositories/core.py` | `get_by_permission_code`（`uq_permissions_permission_code`）、`list_by_resource`（`idx_permissions_resource_action`） |
| `UserRoleRepository` | `UserRole` | `repositories/core.py` | `get_pair`、`list_role_ids`（复合主键前缀）、`link`（幂等绑定）、`unlink` |
| `RolePermissionRepository` | `RolePermission` | `repositories/core.py` | `get_pair`、`list_permission_ids`（复合主键前缀）、`link`（幂等绑定）、`unlink` |
| `SceneRepository` | `Scene` | `repositories/scene.py` | `get_by_scene_name`（`uq_scenes_scene_name`，仅存活场景） |
| `OtaVersionRepository` | `OtaVersion` | `repositories/ota.py` | `get_by_version_code`（`uq_ota_versions_version_code`）、`get_by_version_name`（`uq_ota_versions_version_name`）、`max_version_code`（version_code 单调递增基准） |
| `OtaTaskRepository` | `OtaTask` | `repositories/ota.py` | `list_by_status`（`idx_ota_tasks_status_create_time`）、`list_by_target_version`（`idx_ota_tasks_target_version_id`） |
| `OtaRecordRepository` | `OtaRecord` | `repositories/ota.py` | `get_by_task_vehicle`（`uq_ota_records_task_vehicle`）、`list_inflight`（`idx_ota_records_inflight`）、`list_by_vehicle`（`idx_ota_records_vehicle_start_time`）、`status_counts`（灰度成功率分子/分母） |
| `EventRepository` | `Event` | `repositories/collector.py` | `get_by_vehicle_type_time`（`uq_events_vehicle_type_time` 幂等预检）、`insert_events`（批量幂等写入，同唯一键）、`list_by_vehicle`（`idx_events_vehicle_time`）、`list_unacknowledged`（`idx_events_unacknowledged`）、`acknowledge`（条件 `UPDATE`，原子且保留首次审计） |
| `VehicleTelemetryRepository` | `VehicleTelemetry` | `repositories/collector.py` | `insert_points`（`ON CONFLICT (time, vehicle_id) DO NOTHING`）、`get_point`、`list_points`（`idx_vehicle_telemetry_vehicle_time`）、`latest_point`、`purge_before`（按 ctid 分批删除） |
| `AlgorithmMetricRepository` | `AlgorithmMetric` | `repositories/analytics.py` | `insert_metrics`（`ON CONFLICT (time, vehicle_id, module, metric_name) DO NOTHING`）、`list_series`（`idx_algorithm_metrics_module_metric_time`）、`latest` |
<!-- repository-table:end -->

### 3.1 通用方法契约（`BaseRepository`，所有 Repository 共享）

<!-- base-methods-table:start -->
| 方法 | 语义 | 契约要点 |
|------|------|---------|
| `get` / `get_or_raise` | 主键查询 | 缺失：`get` 返回 None，`get_or_raise` 抛 3001；**复合主键模型抛 `NotImplementedError`** |
| `find_one` / `find_all` | 自定义条件查询 | 条件为 ORM 列表达式（参数绑定）；自动附加软删除过滤；支持 `options=`；`limit ≤ max_query_limit` |
| `list` / `paginate` | 列表 / 分页 | `page ≥ 1`、`1 ≤ page_size ≤ 200`（保护 P95 ≤ 200ms）；排序 spec 见 3.2 节 |
| `count` / `exists` | 计数 / 存在性 | `exists` 走 `LIMIT 1`，比 `count` 轻 |
| `create` / `update` | 单条写 | SAVEPOINT 内 `flush`，不 commit；唯一冲突 → 3002、非法字段 → 2001；`add` / `setattr` 必须在 SAVEPOINT 内（见本节「写路径纪律」） |
| `bulk_create` | 批量插入 | executemany 分片（`BULK_CHUNK_SIZE`），时序写入 ≥ 10000 点/秒；返回**提交行数** |
| `bulk_create_ignore_conflicts` | 幂等批量插入 | `ON CONFLICT (…) DO NOTHING`，消费重放/重复时间点跳过；返回**提交（attempted）行数**（被跳过的重复行仍计入，异步驱动无法提供精确插入行数） |
| `delete_where` / `hard_delete` | 条件删除 / 物理删除 | 仅用于关联表解绑与超期数据清理；`scenes` 一律走 `soft_delete`；**复合主键模型禁用 `hard_delete`** |
| `soft_delete` | 软删除 | 仅 `deleted_at` 模型（当前仅 `Scene`）；其他模型抛 `NotImplementedError` |
<!-- base-methods-table:end -->

### 3.2 排序 spec 与默认排序（空值位次必须显式声明）

排序 spec 由 `BaseRepository.order_clause` 解析，取值不可扩展：

| spec | 生成 SQL | 适用场景 |
|------|---------|---------|
| `col` | `col ASC` | 非空列升序 |
| `-col` | `col DESC` | 非空列降序 |
| `col:nl` / `-col:nl` | `col ASC NULLS LAST` / `col DESC NULLS LAST` | **可空列**（对齐 DDL 索引 `... DESC NULLS LAST`） |
| `col:nf` / `-col:nf` | `col ASC NULLS FIRST` / `col DESC NULLS FIRST` | 需显式把空值排前 |

⚠ PostgreSQL 中 `DESC` 默认等价 `NULLS FIRST`：可空列若漏写 `:nl`，会同时造成
**业务语义反转**（空值排最前，如未发布草稿排在版本列表首位）与**索引失效**（多出 `Sort` 节点，破坏 P95 ≤ 200ms）。

**默认排序**（列表页第一排序键；次级键用于分页稳定）：

| Repository | 默认排序 | 依据 |
|------------|---------|------|
| `VehicleRepository` | `last_online_time DESC NULLS LAST, vehicle_id ASC` | `idx_vehicles_status_last_online` |
| `SceneRepository` | `create_time DESC, scene_id ASC` | `idx_scenes_status_type_create_time` |
| `OtaVersionRepository` | `release_time DESC NULLS LAST, version_code DESC` | OpenAPI ota-service.yaml + `idx_ota_versions_status_release_time` |
| `OtaTaskRepository` | `create_time DESC` | `idx_ota_tasks_status_create_time` |
| `OtaRecordRepository` | `start_time DESC NULLS LAST, record_id DESC` | OpenAPI ota-service.yaml + `idx_ota_records_vehicle_start_time` |
| `EventRepository` | `event_time DESC, event_id DESC` | `idx_events_vehicle_time` |
| `VehicleTelemetryRepository` | `time DESC` | `idx_vehicle_telemetry_vehicle_time` |
| `AlgorithmMetricRepository` | `time DESC` | `idx_algorithm_metrics_vehicle_time` |

### 3.3 读取上限（`max_query_limit`）与序列读取窗口

<!-- series-window-rule:start -->
- 通用读方法默认 `limit ≤ 200`（`MAX_PAGE_SIZE`，对齐分页接口契约与 P95 ≤ 200ms）；
- 时序序列读取（`VehicleTelemetryRepository.list_points` / `AlgorithmMetricRepository.list_series`）
  放宽到 `MAX_SERIES_POINTS = 10000`，且**强制要求时间窗**：`start_time` / `end_time` 至少提供一个，
  否则抛 **2001** —— 无窗口查询会全量扫描该车辆的 hypertable 历史（单车辆可达百万行），
  直接违背 P95 ≤ 200ms；
- `paginate` 的 `page_size` 始终 ≤ 200（对外分页契约不因时序放宽而上浮）。
<!-- series-window-rule:end -->

### 3.4 显式加载策略（`options=`）

契约第 1 节的 `raise_on_sql` 关系必须由调用方显式加载，因此 `get` / `get_or_raise` / `find_one` /
`find_all` / `list` / `paginate` 均提供 `options: Sequence[ORMOption] | None`：

```python
tasks = await OtaTaskRepository(session).find_all(
    OtaTask.status == OtaTaskStatus.RUNNING,
    options=(selectinload(OtaTask.records),),
)
```

### 3.5 服务 → 共享 Repository 白名单（跨服务边界）

共享包 `hunter_common.database.repositories` 同时暴露 13 个模型的数据访问能力，因此**必须用白名单约束
服务边界**（禁止跨服务直接访问其他服务的数据库 schema）。白名单由 `scripts/verify_data_layer.py`
**校验 14** 强制：服务源码中出现未授权 Repository 导入即校验失败。

<!-- service-repository-table:start -->
| 服务 | 允许使用的共享 Repository | 说明 |
|------|--------------------------|------|
| `scene-service` | `SceneRepository` | `scene_svc.scenes`（软删除） |
| `data-collector` | `EventRepository`、`VehicleTelemetryRepository` | `data_collector.events` / `vehicle_telemetry`（写路径幂等） |
| `data-analytics` | `AlgorithmMetricRepository`、`EventRepository`、`VehicleTelemetryRepository` | 分析读取（时序 + 事件，只读） |
| `ota-service` | `OtaVersionRepository`、`OtaTaskRepository`、`OtaRecordRepository` | `ota_svc` 三表（灰度成功率统计等） |
| `api-gateway` | `UserRepository`、`RoleRepository`、`PermissionRepository`、`UserRoleRepository`、`RolePermissionRepository`、`VehicleRepository` | 鉴权链路（`user_svc` RBAC 五表）+ 车辆台账只读 |
| `remote-control` | `VehicleRepository` | 车辆状态读取（状态写入走 REST 调 vehicle-service） |
| `flink-jobs`（非服务，书面豁免） | 无（禁止使用任何 Repository） | G-13 写侧豁免：实时作业（`hunter_flink`）经 Kafka 写 `alert_event`/`algorithm_metrics`，不直连数据库、不经 hunter_common 数据层；不在 services/ 目录（校验 14 不扫描），本行为书面登记 |
<!-- service-repository-table:end -->

⚠ 未列入白名单的 schema（如车辆主数据写入、其他服务业务表）必须通过 REST API 访问
（例：车辆信息 `/api/v1/vehicle/**`），不得直接使用其 Repository。

## 4. 服务层 Repository 收敛路径（迁移契约）
`common/python/hunter_common/database/repositories/` 是**唯一数据访问实现**的最终形态；
迁移期各服务 `services/*/app/repositories/*.py` 仍存在**同名同表**的过渡实现（构造签名与事务粒度不同：
服务层为 `db: DatabaseSessionManager` 且每个方法独立事务，共享层为 `session: AsyncSession` 且由调用方控制事务）。

| 顺序 | 服务 | 过渡实现 | 收敛要求 |
|------|------|---------|---------|
| 1 | scene-service | `app/repositories/scenes.py::SceneRepository` | 改为注入共享 `SceneRepository`；软删除、白名单排序行为逐条回归 |
| 2 | data-collector | `app/repositories/events.py::EventRepository`、`telemetry.py::TelemetryRepository` | 写路径改用 `insert_events` / `insert_points`；查询改用共享 Repository；保留契约行转换函数 |
| 3 | api-gateway | `app/repositories/user_repository.py::UserRepository` | **鉴权链路**，须独立变更 + 全量回归；`touch_last_login` 采用共享实现 |
| 4 | ota-service | `app/repositories/versions.py` 等 | `max_published_code` → 复用 `max_version_code(status=published)` |

**迁移期纪律（不可违反）**：
1. 同一个模块内**禁止同时导入**服务层与共享层的同名 Repository（避免隐式双写/双读）；
2. 新代码一律使用共享层；服务层过渡实现只允许修 BUG，不再新增方法；
3. 收敛完成的服务必须删除过渡实现（避免"两套事实来源"长期并存）。

## 5. 校验方式

```bash
# ORM 关系契约 + Repository 契约（无需数据库）
pytest common/python/tests/test_orm_relationships.py -q
pytest common/python/tests/test_repositories.py -q
pytest common/python/tests/test_repository.py -q
# 写路径 SAVEPOINT 语义（真实 Session，sqlite+aiosqlite，无需容器）
pytest common/python/tests/test_repository_transactions.py -q

# 数据层契约校验（含校验 11 模型↔仓库、12 relationship 异步安全、13 本文件与实现同步、
# 14 服务→Repository 白名单、15 写路径纪律与序列窗口规则声明）
python scripts/verify_data_layer.py

# 真实 PostgreSQL/TimescaleDB 事务语义（docker-compose 或 testcontainers，无 Docker 自动 skip）
pytest tests/integration/test_data_layer_transactions.py -m integration -q
```

