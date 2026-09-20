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
| `scripts/` | `/opt/hunter-edge/scripts/` | 运维脚本（12 个：公共库、一键部署、口令/证书生成、DB/Kafka/MinIO 初始化、健康检查、巡检、备份、卸载、日志收集）—— 位于仓库根 `scripts/` |

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

## 交付状态

| 本目录产物 | 状态 |
|-----------|------|
| `config/daemon.json`、`config/nginx.conf` | ✅ 已交付 |
| `sql/schema.sql`、`sql/timescaledb.sql`、`sql/init-data.sql` | ✅ 已交付（生成产物） |
| `.env.example` | ✅ 已交付（133 个变量，口令为 CHANGE_ME_*） |
| `scripts/*.sh`（12 个：common/install/gen-passwords/gen-kafka-certs/init-db/init-kafka/init-minio/health-check/daily-check/backup/uninstall/collect-logs） | ✅ 已交付（位于仓库根 `scripts/`，部署时映射至 `/opt/hunter-edge/scripts/`，见下表） |
| `docker-compose.yml` | ✅ 已交付（单机生产编排：中间件 + Flink + SRS + 6 业务服务 + web-portal；变量名对齐 pydantic-settings，健康检查用 `/healthz`，distroless 探针用 `python3 -c`；容器名与 `scripts/common.sh` 的 `C_*` 一致） |
| `docker-compose.override.yml` | ✅ 已交付（开发覆盖：源码热重载 / 降配 / 端口开放；⚠ 生产须删除或 `docker compose -f docker-compose.yml` 显式忽略） |
| `config/srs.conf` | ✅ 已交付（SRS 5.0：RTMP 1935 / WebRTC 8000udp / HTTP-API·WHEP 9090；部署前须将 `rtc_server.candidate` 改为真实 SERVER_IP） |
| `frontend/Dockerfile`、`frontend/.dockerignore` | ✅ 已交付（多阶段：node:20 构建 → nginx:1.25 托管；nginx.conf 由 compose 挂载，不烘焙进镜像） |
| `config/flink-conf.yaml`、`certs/README.md` | ⛔ **待交付**（后续批次；Flink 参数当前经 compose `FLINK_PROPERTIES` 注入） |

> 在 `docker-compose.yml` 就位前，`install.sh` 的 step_6/7/11 会在 `require_compose_file` 处给出明确指引并退出；
> step_1~5（系统初始化/目录/.env/证书）与各初始化脚本的静态校验可独立执行。
> 脚本已通过 `bash -n` 与函数级自检；`shellcheck` 需在服务器执行（`sudo apt-get install -y shellcheck && shellcheck scripts/*.sh`）。

