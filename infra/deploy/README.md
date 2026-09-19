# infra/deploy — 单机 Docker Compose 部署包（→ /opt/hunter-edge）

> 目标环境：Ubuntu 22.04 LTS + Docker Engine 24.0+ / Compose v2，单机部署
> 6 业务服务 + 中间件（PostgreSQL 15 + TimescaleDB 2.13 / Kafka 3.6 / ZooKeeper 3.8 / Redis 7 /
> MinIO / SRS 5.0 / Flink 1.18）+ Vue3 前端（Nginx）。
> 部署文档：`docs/01-部署概述与环境要求.md` … `docs/07-卸载与灾难恢复.md`。

## 目录映射（仓库 → 服务器）

| 本仓库 | 服务器路径 | 说明 |
|--------|-----------|------|
| `config/daemon.json` | `/etc/docker/daemon.json` | Docker 守护进程（日志驱动 json-file 100MB×5、overlay2、live-restore） |
| `config/nginx.conf` | `/opt/hunter-edge/config/nginx.conf` | web-portal Nginx（静态站点 + `/api/` 反代网关 + `/ws/` 反代 remote-control） |
| `sql/schema.sql` | `/opt/hunter-edge/sql/schema.sql` | **生成产物**：业务库 11 张表 + 8 schema（源：`contracts/database/ddl/00-04`） |
| `sql/timescaledb.sql` | `/opt/hunter-edge/sql/timescaledb.sql` | **生成产物**：2 张 hypertable + 按天分块 + 90 天保留（源：`contracts/database/ddl/05`） |
| `sql/init-data.sql` | `/opt/hunter-edge/sql/init-data.sql` | 初始数据：4 角色 + 16 权限点 + 角色权限矩阵 + 默认 admin + 测试车辆 |
| `.env.example` | `/opt/hunter-edge/.env` | 环境变量模板（`cp` 后由 `scripts/gen-passwords.sh` 生成随机口令） |
| `certs/` | `/opt/hunter-edge/certs/` | Kafka SASL_SSL 证书（`scripts/gen-kafka-certs.sh` 生成，**不入库**） |
| `scripts/` | `/opt/hunter-edge/scripts/` | 运维脚本（口令/证书生成、DB 初始化、健康检查、巡检、备份、卸载） |

## 契约来源（单一事实来源，禁止在本目录重复定义）

| 本目录文件 | 上游契约 |
|-----------|---------|
| `sql/schema.sql`、`sql/timescaledb.sql` | `contracts/database/ddl/*.sql`（生成，勿手改） |
| `sql/init-data.sql` 角色/权限编码 | `contracts/openapi/api-gateway.yaml`、`contracts/database/enums.md` §8 |
| MinIO Bucket 变量、Kafka SASL_SSL | `contracts/database/object-storage.yaml`、`contracts/kafka/topics.yaml` |
| 端口 8080-8085、路由前缀 | 设计文档模块表 + `contracts/openapi/*.yaml` |
| Kafka 内部 Topic（分区/保留） | `contracts/kafka/topics.yaml`（`infra/docker/kafka/create-topics.sh` 同源） |

## 重新生成 SQL 部署产物

```bash
# 仓库根目录执行（内容须与 contracts/database/ddl 完全一致）
cat contracts/database/ddl/00_schemas.sql contracts/database/ddl/01_core.sql \
    contracts/database/ddl/02_scene.sql  contracts/database/ddl/03_ota.sql \
    contracts/database/ddl/04_events.sql > infra/deploy/sql/schema.sql
cat contracts/database/ddl/05_timeseries.sql > infra/deploy/sql/timescaledb.sql
```

## 批次说明（TBD）

以下文件属部署包其余批次，尚未在本目录生成：`docker-compose.yml`、`config/srs.conf`、
`config/flink-conf.yaml`、`scripts/*.sh`、`certs/README.md`。
在 `docker-compose.yml` 就位前，第一至七章中的 `docker compose` 命令无法执行。
