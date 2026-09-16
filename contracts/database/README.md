# contracts/database — 数据库 DDL 契约

单一事实来源：表结构先在此定义 DDL，再写 ORM 实现（`common/python/hunter_common/database/`），
并由 Alembic migration（`hunter_common/database/migrations/versions/0001_initial_schema.py`）执行建表。
三方（DDL / ORM / migration）一致性由 `scripts/verify_data_layer.py` 与单元测试强制校验。

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

## 执行顺序与方式

```bash
# 契约文件本身可直接执行（幂等，IF NOT EXISTS）；生产/测试统一经 Alembic 执行：
cd common/python && alembic upgrade head                 # 建表 + hypertable + 保留策略
alembic upgrade head --sql > /tmp/hunter_ddl.sql         # 离线生成 SQL（无需数据库，CI 校验用）
alembic downgrade -1                                     # 回滚（含 drop schema 前置校验）
```

容器首次初始化仅执行 `infra/docker/postgres/init/01-extensions.sql`（扩展 + schema，与
`infra/k8s/statefulsets/postgres.yaml` 中的 ConfigMap 内容保持一致），**建表一律走 Alembic**。

## 关键约束（不可更改）

- 各服务独立 schema，禁止跨服务直查（`er.md` 列出唯一例外：时序库只读分析账号）
- `vehicle_telemetry` / `algorithm_metrics`：`chunk_time_interval = 1 day`，保留 90 天
- 受控词表（车辆状态 8 态、事件类型 18 种、事件等级 3 级、OTA 状态机 9 态、算法模块 3 种）必须与
  `hunter_common/database/enums.py` 完全一致，禁止在业务代码中硬编码字符串
- 所有结构变更必须同时更新：DDL → ORM 模型 → Alembic migration（upgrade + downgrade）→ 本文件清单

