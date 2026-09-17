# contracts/database — 数据库 DDL 与数据存储契约

单一事实来源：表结构先在此定义 DDL，再写 ORM 实现（`common/python/hunter_common/database/`），
并由 Alembic migration（`hunter_common/database/migrations/versions/0001_initial_schema.py`）执行建表。
三方（DDL / ORM / migration）一致性由 `scripts/verify_data_layer.py` 与单元测试强制校验。

同一目录下的 `redis-keys.yaml`（Redis Key 契约）与 `object-storage.yaml`（MinIO Bucket/对象键/预签名契约）
是缓存与对象存储的单一事实来源，与各服务 OpenAPI 中的 `x-hunter-service.redis_keys` /
`x-hunter-service.minio_buckets` 声明、`infra/docker/minio/init-buckets.sh`、
`infra/k8s/jobs/minio-init-job.yaml` 保持三方一致（校验见 `scripts/verify_data_layer.py`）。

## 文件清单

| 文件 | 内容 | schema |
|------|------|--------|
| `ddl/00_schemas.sql` | TimescaleDB 扩展、8 个服务 schema、公共触发器函数 `public.common_set_update_time()` | — |
| `ddl/01_core.sql` | `vehicles`（车辆台账）；`users` / `roles` / `permissions` / `user_roles` / `role_permissions`（RBAC 五表） | vehicle_svc / user_svc |
| `ddl/02_scene.sql` | `scenes`（场景库，含软删除 `deleted_at`） | scene_svc |
| `ddl/03_ota.sql` | `ota_versions` / `ota_tasks` / `ota_records` | ota_svc |
| `ddl/04_events.sql` | `events`（18 种事件类型 + 3 级事件等级） | data_collector |
| `ddl/05_timeseries.sql` | `vehicle_telemetry`、`algorithm_metrics`（hypertable：按天分块 + 90 天保留） | data_collector / data_analytics |
| `er.md` | ER 关系说明、跨 schema 访问例外、字段类型约定、与设计文档的差异清单 | — |
| `enums.md` | 受控词表（车辆状态 / 事件类型与等级 / OTA 状态机 / 权限资源域），与 ORM `StrEnum` 一一对应 | — |
| `redis-keys.yaml` | Redis Key 契约：系统约束第 8 条 7 个 Key + `rc:lock:{vehicle_id}` 实现派生键、命名规则、字段结构、TTL、读写方、跨服务引用 | — |
| `object-storage.yaml` | MinIO 契约：7 个 Bucket（生命周期 / SSE-S3 / 读写方 / 对象键约定）、预签名策略（上传 3600s、下载 900s、Range、分片）、对象键规则 | — |


## 执行顺序与方式

```bash
# 契约文件本身可直接执行（幂等，IF NOT EXISTS）；生产/测试统一经 Alembic 执行：
cd common/python && alembic upgrade head                 # 建表 + hypertable + 保留策略
alembic upgrade head --sql > /tmp/hunter_ddl.sql         # 离线生成 SQL（无需数据库，CI 校验用）
alembic downgrade -1                                     # 回滚（含 drop schema 前置校验）
```

容器首次初始化仅执行 `infra/docker/postgres/init/01-extensions.sql`（扩展 + schema，与
`infra/k8s/statefulsets/postgres.yaml` 中的 ConfigMap 内容保持一致），**建表一律走 Alembic**。

## 存储契约校验方式

```bash
# 26 项数据层契约校验（含校验 9 Redis Key、校验 10 对象存储；无需数据库/集群）
python scripts/verify_data_layer.py

# 存储契约单元测试（Redis Key / MinIO Bucket ↔ 服务声明 ↔ MinIO 初始化脚本）
pytest common/python/tests/test_storage_contracts.py -q
```

两份存储契约含 `x-hunter-pending-confirmation` 待确认项（`redis-keys.yaml` 8 项、`object-storage.yaml` 7 项，
其中标阻塞 8 项），人工决策后回填契约，再进入存储层实现（shared library MinIO 封装等）。

## 关键约束（不可更改）

- 各服务独立 schema，禁止跨服务直查（`er.md` 列出唯一例外：时序库只读分析账号）
- `vehicle_telemetry` / `algorithm_metrics`：`chunk_time_interval = 1 day`，保留 90 天
- 受控词表（车辆状态 8 态、事件类型 18 种、事件等级 3 级、OTA 状态机 9 态、算法模块 3 种）必须与
  `hunter_common/database/enums.py` 完全一致，禁止在业务代码中硬编码字符串
- Redis Key 命名模式以 `redis-keys.yaml` 为准（`session:` / `vehicle:status:` / `vehicle:online:set` /
  `rate_limit:` / `ota:progress:` / `rc:session:` / `cache:scene:` / `rc:lock:`），禁止新增命名空间
- MinIO Bucket 名称、生命周期、SSE 策略以 `object-storage.yaml` 为准；预签名有效期固定
  「上传 3600s / 下载 900s」，禁止按接口随意调整；对象键一律由服务端生成
- 所有结构变更必须同时更新：DDL → ORM 模型 → Alembic migration（upgrade + downgrade）→ 本文件清单；
  Redis / MinIO 契约变更必须同步各服务 OpenAPI 的 `x-hunter-service` 声明与
  `infra/docker/minio/init-buckets.sh`、`infra/k8s/jobs/minio-init-job.yaml`

