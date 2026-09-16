# Alembic 迁移使用说明（hunter_common.database.migrations）

数据库结构的**唯一执行入口**：DDL 契约（`contracts/database/ddl/*.sql`）→ ORM 模型
（`hunter_common/database/models/`）→ Alembic 迁移（本目录）。三者一致性由
`python scripts/verify_data_layer.py` 与单元测试强制校验。

## 常用命令（任意工作目录均可执行）

```bash
# 应用全部迁移（需可连通的 PostgreSQL + TimescaleDB）
alembic -c common/python/alembic.ini upgrade head

# 离线生成 SQL（无需数据库，CI/DBA 评审用）
alembic -c common/python/alembic.ini upgrade head --sql > /tmp/hunter_ddl.sql

# 当前版本 / 历史
alembic -c common/python/alembic.ini current
alembic -c common/python/alembic.ini history --verbose

# 回滚一步（dev/test 环境；会丢数据）
alembic -c common/python/alembic.ini downgrade -1

# 结构变更后生成增量迁移（再人工审查 upgrade/downgrade）
alembic -c common/python/alembic.ini revision --autogenerate -m "add scenes.source"
```

连接串优先级：环境变量 `DATABASE_URL` > `POSTGRES_*`（经 `hunter_common.config` 组装）
> `alembic.ini` 中的 `sqlalchemy.url`（仅本地开发兜底）。

## 迁移策略（重要）

| 项 | 约定 |
|----|------|
| 驱动 | 异步引擎 asyncpg + `connection.run_sync()` 执行 DDL（不引入同步驱动依赖） |
| `0001_initial_schema` | 初始基线：创建 8 个 schema、13 张表（`Base.metadata.create_all`）、`scenes.update_time` 触发器、hypertable（1 day 分块）与 90 天保留策略。**禁止就地修改**：历史迁移一旦发布即冻结 |
| 增量迁移 | 必须使用显式 `op.create_table/op.add_column/op.create_index` 等操作，并同时提交 `downgrade()` |
| 一致性 | 变更须同步三处：`contracts/database/ddl/*.sql` → ORM 模型 → 迁移；随后运行 `python scripts/verify_data_layer.py` |
| 时序表 | `create_hypertable` / `add_retention_policy` 必须放在建表之后；保留策略变更同样需要迁移 |
| 版本表 | `alembic_version` 位于 `public` schema |

⚠ 人工确认点：`0001` 采用 `metadata.create_all` 以避免人工抄写偏差，代价是该文件与 ORM 模型强绑定；
若不接受此取舍，需改为一次性生成的显式 `op.create_table` 明文迁移。

## 校验

```bash
python scripts/verify_data_layer.py     # DDL ↔ ORM ↔ 迁移 三方一致性（含离线 SQL 生成）
pytest common/python/tests/test_database_models.py -q
```
