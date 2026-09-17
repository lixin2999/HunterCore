# HunterEdge —— HUNTER 自动驾驶数据采集与分析系统

基于 **HUNTER SE 阿克曼 UGV 底盘 + EDU Pro Kit 传感器套件 + NVIDIA AGX Orin** 车载平台构建的
自动驾驶数据采集与分析云端系统，承担五大核心职能：

| 职能 | 说明 |
|------|------|
| 场景生成 | 场景库管理、场景编辑、参数化、OpenSCENARIO 导出、Carla 仿真下发、实车场景提取 |
| 数据采集 | Kafka 遥测/事件接入、数据预处理（校验/清洗/对齐）、数据路由、文件上传管理 |
| 数据分析 | Flink 实时流处理、Spark 离线批处理、指标计算、Corner Case 挖掘、报告生成 |
| OTA 管理 | 版本仓库、升级任务调度、灰度发布（5%→20%→50%→100%）、A/B 分区回滚 |
| 远程操控 | WebRTC 视频流转发、控制指令转发（20Hz）、操作员权限与会话录像管理 |

## 技术栈

- **后端**：Python 3.11+ / FastAPI 0.100+（异步）/ SQLAlchemy 2.0（asyncpg）/ confluent-kafka 2.x
- **数据**：PostgreSQL 15 + TimescaleDB 2.13 / Redis 7 / MinIO / Apache Kafka 3.6 / Flink 1.18 / Spark 3.5
- **前端**：Vue 3 + TypeScript + Vite + Element Plus + Pinia + ECharts 5 + Three.js
- **部署**：Docker / Kubernetes 1.28 / Prometheus + Grafana + Loki + Jaeger

## 系统架构

```
                    ┌──────────────────────────────────────────────────┐
                    │              展示层：Vue 3 管理后台               │
                    │ 场景管理 / 数据采集 / 数据分析 / OTA / 远程操控    │
                    └───────────────────────┬──────────────────────────┘
                                            │ HTTPS / WebSocket / WebRTC
┌───────────────────────────────────────────▼────────────────────────────────────────────┐
│  接入层：API 网关 api-gateway(8080)        Kafka 集群(SASL_SSL)        SRS 媒体服务器   │
└───────────┬────────────────────────────┬───────────────────────────────┬───────────────┘
            │ REST(统一响应/错误码)       │ 消息(key=vehicle_id)          │ WebRTC/SRTP
┌───────────▼────────────────────────────▼───────────────────────────────▼───────────────┐
│  业务服务层：scene(8081) / data-collector(8082) / data-analytics(8083) /                │
│              ota-service(8084) / remote-control(8085)                                   │
└───────────┬────────────────────────────┬───────────────────────────────┬───────────────┘
            │                            │                               │
┌───────────▼────────────────────────────▼───────────────────────────────▼───────────────┐
│  数据层：PostgreSQL 15 + TimescaleDB 2.13 / Redis 7 / MinIO / Kafka / Flink 1.18 /      │
│          Spark 3.5                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
HunterCore/
├── contracts/                 # 接口契约（先定义后实现，单一事实来源）
│   ├── openapi/               # REST API OpenAPI 3.0 YAML
│   ├── kafka/                 # Kafka Topic 定义 YAML + JSON Schema
│   └── database/              # 数据库 DDL SQL + ER 关系说明
├── services/                  # 后端微服务（每服务独立目录/独立部署/独立数据库 schema）
│   ├── api-gateway/           # 8080 统一接入 / JWT 鉴权 / 限流熔断 / 路由转发
│   ├── scene-service/         # 8081 场景生成
│   ├── data-collector/        # 8082 数据采集
│   ├── data-analytics/        # 8083 数据分析
│   ├── ota-service/           # 8084 OTA 管理
│   └── remote-control/        # 8085 远程操控
├── common/
│   ├── python/hunter_common/  # Python 共享库（配置/日志/异常/响应/Kafka/DB/Redis）
│   └── typescript/            # TypeScript 共享类型（后续层级初始化）
├── frontend/                  # Vue 3 管理后台（后续层级初始化）
├── infra/
│   ├── k8s/                   # K8s 清单：6 微服务 Deployment/Service/ConfigMap、中间件 StatefulSet/PVC、初始化 Job、Ingress
│   ├── docker/                # 本地开发：postgres/kafka/minio 初始化脚本
│   └── monitoring/            # Prometheus 采集与告警 + Alertmanager + Grafana 看板 + exporters + 监控组件清单
├── flink-jobs/                # Flink 1.18 实时分析作业（后续层级）
├── spark-jobs/                # Spark 3.5 离线分析作业（后续层级）
├── docs/                      # 设计与开发文档
├── docker-compose.yml         # 本地开发基础设施（TimescaleDB/Kafka/Redis/MinIO）
└── release.md                 # 变更记录
```

### 微服务内部结构（固定，不可更改）

```
services/{service-name}/
├── app/
│   ├── main.py            # FastAPI 入口（统一响应/全局异常/trace_id 中间件/健康探针）
│   ├── config.py          # pydantic-settings 配置（继承 hunter_common.HunterBaseConfig）
│   ├── routers/           # API 路由层（health.py 已就绪，业务路由按契约逐层添加）
│   ├── schemas/           # Pydantic v2 请求/响应模型
│   ├── models/            # SQLAlchemy 2.0 ORM 模型
│   ├── services/          # 业务逻辑层
│   ├── repositories/      # 数据访问层（仅访问本服务数据库）
│   ├── consumers/         # Kafka 消费者（手动提交 offset，异常进 DLQ）
│   ├── producers/         # Kafka 生产者（单例，key=vehicle_id 保证单车辆有序）
│   ├── core/              # 核心组件（安全/依赖注入，后续层级实现）
│   └── tests/             # 单元测试（pytest + pytest-asyncio + httpx）
├── Dockerfile             # 多阶段构建：python:3.11-slim(builder) → distroless nonroot(runner)，上下文 = 仓库根目录
├── pyproject.toml
└── README.md
```

## 微服务清单

| 模块 | 服务 | 端口 | 网关路由前缀 |
|------|------|------|--------------|
| API 网关 | api-gateway | 8080 | 统一入口（全部 `/api/v1/**`，JWT 鉴权 + 五级限流） |
| 场景生成 | scene-service | 8081 | `/api/v1/scene/**` |
| 数据采集 | data-collector | 8082 | `/api/v1/data/**` |
| 数据分析 | data-analytics | 8083 | `/api/v1/analytics/**` |
| OTA 管理 | ota-service | 8084 | `/api/v1/ota/**` |
| 远程操控 | remote-control | 8085 | `/api/v1/remote/**`、`/ws/remote/**`（WebSocket） |

## 快速开始

### 前置要求

- Docker Desktop（含 Docker Compose v2）
- Python 3.11+
- （可选）Node.js 20+（前端，后续层级初始化）

### 1. 启动本地基础设施

```bash
# 仓库根目录执行
docker compose up -d
docker compose ps        # 等待 postgres / kafka / redis / minio 全部 healthy
                         # kafka-init / minio-init 为一次性初始化任务（正常退出）
```

组件与端口：PostgreSQL+TimescaleDB `5432`、Kafka `9092`、Redis `6379`、MinIO API `9000` / 控制台 `9001`。

### 2. 初始化 Python 环境与共享库

```powershell
# Windows PowerShell 示例（Linux/macOS: source .venv/bin/activate）
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -e "common/python[dev]"        # 共享库（editable 安装，含 pytest/ruff/mypy）
pip install -e "services/scene-service"    # 按需安装任一微服务（fastapi/uvicorn/httpx）
```

### 3. 启动单个微服务

```bash
cd services/scene-service
uvicorn app.main:app --reload --port 8081
curl http://localhost:8081/healthz
# -> {"code":0,"message":"success","data":{"status":"ok"},"request_id":"...","timestamp":...}
```

### 4. 运行单元测试与静态检查

```bash
pytest common/python/tests -q                     # 共享库测试
cd services/scene-service && pytest -q            # 服务测试（每个服务目录内执行）
ruff check common services                        # Lint
```

## 本地启动顺序（依赖链）

1. `docker compose up -d`：TimescaleDB → Kafka（kafka-init 按契约创建平台内部 Topic）→ Redis → MinIO（minio-init 创建 7 个 Bucket + 生命周期）
2. 安装共享库 `hunter_common`（editable），并执行数据库迁移：`alembic -c common/python/alembic.ini upgrade head`（建 schema/表/hypertable）
3. 启动业务微服务（依赖基础设施）：scene-service → data-collector → data-analytics → ota-service → remote-control
4. 启动 api-gateway（依赖上述服务就绪后统一对外路由，8080）
5. 前端 dev server（Vite，5173，后续层级初始化）

## Kubernetes 部署与监控（L1）

基础设施层清单位于 `infra/k8s/`（应用与中间件）与 `infra/monitoring/`（监控栈），
静态校验与部署命令：

```bash
# 静态校验（无需集群）：API 版本/命名空间/探针/资源配额/敏感字段/端口/契约一致性
python scripts/verify_infra.py

# 部署顺序（完整版见 infra/k8s/README.md）
kubectl apply -f infra/k8s/base/            # 命名空间 + 共享 ConfigMap + Secret（先复制 02-secret.example.yaml）
# 前置：创建 TLS Secret hunter-kafka-tls / hunter-minio-tls / hunter-edge-tls
kubectl apply -f infra/k8s/statefulsets/    # postgres / kafka / redis / minio
kubectl apply -f infra/k8s/jobs/            # Topic 与 Bucket 初始化（契约一致）
kubectl apply -f infra/k8s/services/        # 6 个微服务
kubectl apply -f infra/k8s/ingress.yaml     # 网关路由表 + WebSocket + TLS

# 监控栈
kubectl apply -k infra/monitoring
kubectl apply -f infra/monitoring/exporters/exporters.yaml
```

要点：

- **探针**：`startupProbe`/`livenessProbe` → `/healthz`，`readinessProbe` → `/readyz`（内部 2s 超时，依赖异常时快速 503 + `code=5001`）
- **指标**：各服务暴露 `GET /metrics`（`common/python/hunter_common/metrics.py`，统一前缀 `hunter_`），Prometheus 通过 Pod 注解自动发现
- **安全**：`hunter-edge` 命名空间 `pod-security=restricted`；密钥/证书仅经 Secret 注入；Kafka `SASL_SSL + SCRAM-SHA-512`；MinIO HTTPS + SSE-S3
- **告警**：5 组规则（服务健康 / API 性能 / 数据管道 / 中间件 / 业务约束），阈值需与设计文档 15.4.2 节核对
- **看板**：`hunter-fleet-overview`、`hunter-kafka`、`hunter-api-performance`（Git 供给，UI 只读）

## 数据层与契约（L2）

数据层契约（单一事实来源）已落地，后续服务开发必须先扩展契约再写实现：

```bash
# 1) 建表：扩展 + 8 个 schema + 13 张表 + hypertable（1 day 分块 / 90 天保留）
alembic -c common/python/alembic.ini upgrade head

# 2) 离线生成 SQL（无需数据库，CI/DBA 评审）
alembic -c common/python/alembic.ini upgrade head --sql > /tmp/hunter_ddl.sql

# 3) 契约一致性校验：DDL ↔ ORM ↔ Alembic + Kafka Topic/JSON Schema（24 项，无需数据库）
python scripts/verify_data_layer.py
```

| 契约 | 位置 | 内容 |
|------|------|------|
| 数据库 DDL | `contracts/database/ddl/*.sql` | `00_schemas` 扩展/schema/公共函数；`01_core` 车辆 + RBAC 五表；`02_scene`；`03_ota`；`04_events`；`05_timeseries`（hypertable） |
| ER / 受控词表 | `contracts/database/er.md`、`enums.md` | 关系与跨 schema 只读例外；车辆 8 态 / 事件 18 种类型 3 级等级 / OTA 9 态状态机 |
| Kafka Topic | `contracts/kafka/topics.yaml`、`consumer-groups.yaml` | 车端 9 个 + 平台内部 6 个 Topic（分区/副本/acks/保留/key）；12 个消费者组（手动提交 + DLQ + 幂等键） |
| 消息 Schema | `contracts/kafka/schemas/*.schema.json` | 11 个 draft-07 JSON Schema（telemetry/event/health/command/command_result/ota_notify/ota_status/remote_control/analytics_result/sensor_file/alert_event），自带设计文档示例 |

要点：

- **ORM 与 DDL 逐列一致**：列名、类型（TEXT/UUID/JSONB/TEXT[]/TIMESTAMPTZ/CHAR(n)…）、可空性、主键三方对齐，偏差会让校验脚本失败
- **受控词表统一**：`hunter_common.database.enums` 的 `StrEnum` 是唯一来源，DB 侧用 `TEXT + CHECK`（便于扩展），`StrEnumType` 拒绝非法取值
- **通用 Repository**：`BaseRepository`（CRUD + 分页 + 软删除 + `ON CONFLICT DO NOTHING` 批量写入），默认过滤软删除记录，非法字段抛 2001
- **写入性能路径**：时序/事件走 `bulk_create*`（executemany 分片），支撑遥测入库延迟 ≤ 1s、时序写入 ≥ 10000 点/秒
- **⚠ 待核对项**（设计文档 15.2/9/5.3 到位后回填）：`vehicle_svc`/`user_svc` schema 归属、`scenes.version` 类型、`scenes.scene_type`、`ota_versions.release_type/status`、`data_analytics` 报告/评估结果元信息缺表（落 MinIO `hunter-reports`）

## 接口契约（OpenAPI）

REST 接口契约集中在 `contracts/openapi/`，遵循「先契约、后实现」；契约 ↔ 实现 ↔ K8s 清单三方一致性由契约测试强制校验。

| 契约 | 覆盖范围 | 状态 |
|------|----------|------|
| `api-gateway.yaml` | 统一认证（登录 / 刷新 / 登录态查询 / 注销）+ 运维探针（`/healthz`、`/readyz`、`/metrics`）+ 路由表 / 五级限流 / WebSocket / 审计日志 / 错误码映射扩展字段 | ✅ |
| `scene-service.yaml` | 场景库 CRUD（10 端点，12.2 节）+ 4.2.1 分类体系 / 4.2.2 配置结构 / 4.3 导出 / 4.4 下发 / 4.5 实车提取扩展字段 | ✅ |
| `data-collector.yaml` | 数据采集（7 端点：遥测查询 / 事件查询与确认 / 文件清单与预签名上传）+ 5.3.3 遥测结构 / 5.4 预处理 / 5.5 上传流程扩展字段 | ✅ |
| `data-analytics.yaml` | 数据分析（8 端点：报告列表/详情/生成、看板、感知与控制评估、场景覆盖率、Corner Case）+ 6.2 实时作业 / 6.3 离线作业 / 6.4 挖掘 / 6.5 报告扩展字段 | ✅ |
| `ota-service.yaml` | OTA 管理（15 端点：版本仓库 CRUD/发布/废弃、升级任务 CRUD/start|pause|resume|cancel|rollback、升级记录）+ 8/9 章 DDL 与 Kafka 对齐 + 版本上传两步式 / 灰度批次 / 发布校验扩展字段 | ✅ |
| `remote-control.yaml` | 远程操控资源端点 | 待开发 |

```bash
# 契约校验（29 项，无需运行服务）
pytest services/api-gateway/app/tests/test_openapi_contract.py -q
# 场景服务契约校验（29 项，无需运行服务）
cd services/scene-service && pytest app/tests/test_scene_contract.py -q
# 数据采集契约校验（30 项，无需运行服务）
cd services/data-collector && pytest app/tests/test_data_collector_contract.py -q
# 数据分析契约校验（32 项，无需运行服务）
cd services/data-analytics && pytest app/tests/test_data_analytics_contract.py -q
```

要点：

- **统一响应**：`{code, message, data, request_id, timestamp}`；错误码仅取预定义值（附录 A），错误码 → HTTP 状态映射与
  `app/core/error_handlers.py:HTTP_STATUS_BY_CODE` 完全一致（契约内以 `x-hunter-error-status-map` 声明并可机检）
- **认证**：Bearer JWT（Access Token 2h / Refresh Token 7d）；车辆设备为 X.509 双向 TLS（`mutualTLS`，CommonName = vehicle_id），非 REST
- **转发约定**：网关按 `x-hunter-gateway-routes` 的 7 个前缀转发（不可更改），注入 `X-User-Id`、`X-Roles`、`X-Trace-Id`（覆盖客户端同名头）
- **限流**：附录 D 阈值（全局 10000 / 用户 100 / IP 200 QPS 等），超限 **HTTP 429 + `Retry-After`**（响应体 `code` 复用 5001）
- **Kafka**：网关不生产/不消费任何 Topic（`x-hunter-kafka`）；若承接告警推送须先更新 `contracts/kafka/consumer-groups.yaml`
- **场景服务**：`scene-service.yaml` 与设计文档 12.2 节逐条对齐（不得增删端点）；4.2.2 场景配置结构拆为 `SceneMeta`（→ `scenes` 列）
  + `SceneConfig`（→ `config_json`），映射关系由 `x-hunter-scene-config-contract` 声明；Kafka 仅消费 `analytics_result`
  （`analytics_result.schema.json` 已按 4.5 节补全：触发事件 + 前后各 10 秒截取窗口）
- **数据采集服务**：`data-collector.yaml` 的 7 个业务端点由「附录 D 限流端点 + 5.5 上传流程 + events 表列能力」推导
  （12.2 节未随仓库提供，推导依据在 `x-hunter-endpoints.derivation` 逐条登记）；遥测查询响应与 5.3.3 节消息结构逐字段一致
  （`x-hunter-telemetry-query-contract` 给出扁平列映射）；文件上传 6 步流程 + 命名规范 + 校验失败复用 6001（`x-hunter-file-upload-flow`）；
  Kafka 消费 4 个车端 Topic（`data-collector-*` 消费组）、生产 4 个内部 Topic（含本次补全的 `sensor_file.schema.json`）
- **⚠ 待确认**（契约内标 `pending_confirmation`）：认证端点路径与载荷、`/api/v1/vehicle|user` 归属服务（模块表仅 6 个微服务）、
  MFA / 限流错误码复用、服务发现机制与配置键名、熔断阈值；场景服务侧见其契约 `x-hunter-pending-confirmation`（12 项）；
  数据采集侧见其契约 `x-hunter-pending-confirmation`（12 项：端点清单来源 / 文件元信息缺表 / sensor_file 字段 / Carla Topic 归属等）；
  数据分析侧见其契约 `x-hunter-pending-confirmation`（12 项：报告元信息缺表 / 报告异步语义 / TTC 等级冲突 /
  alert_event 消费方落位 / 消费组契约修正等）；
  OTA 管理侧见其契约 `x-hunter-pending-confirmation`（19 项：12.5 节原文缺失 / `release_type` 取值域 / 跳批策略 /
  publish 网关超时与异步化 / DLQ Topic 登记 / command_result 与 broadcast 归属 / 审计保留期等）
- **数据分析服务**：`data-analytics.yaml` 的 8 个业务端点与设计文档 12.4 节逐条对齐（不可增删）；
  定位为「读模型 + 编排」——实时由 5 个 Flink 作业（6.2 节）、离线由 7 个 Spark 作业（6.3/6.4/6.5 节）承担，
  REST 只读预计算结果（P95 ≤ 200ms），报告生成为异步（202 + 轮询）；14 项实时阈值（3.0 m/s² / 0.5s / 0.8 rad/s /
  1.1 倍 / SOC 20%+10% / TTC 1.5s / 断联 10s 等）全部环境变量化并在 K8s ConfigMap 注入，与受控词表 `EVENT_LEVEL_BY_TYPE`
  机器可检一致；`alert_event.schema.json` 补全后 `topics.yaml` 已无 `schema: null`（Kafka 契约 11 个 Schema 全部落地）
- **OTA 管理服务**：`ota-service.yaml` 的 15 个业务端点由「8 章升级流程 + 9 章 ota_* 三表列能力 + 附录 D 限流端点」
  推导（12.5 节未随仓库提供，推导依据在 `x-hunter-endpoints.items` 逐条登记）；版本上传为**两步式**
  （建草稿 → 1 小时预签名直传 → `publish` 单次流式校验 MD5/SHA-256/RSA-2048/版本单调递增，错误码 6001/6002/6003）；
  灰度 4 批 `5%→20%→50%→100%`、每批观察 24h、成功率 ≥0.95（`OTA_CANARY_MIN_SUCCESS_RATE`）才推进，否则暂停 + 告警；
  Kafka 消费 `hunter.*.ota_status`（消费组 `ota-service-ota-status`，DLQ `{topic}.dlq`），生产 `ota_notify`（逐车 1 小时预签名）
  与 `command`（仅 `ota_rollback`）；最小权限仅写 `ota_svc` schema（禁止 DELETE），`publish` 作为唯一性能例外已登记

## 验证命令清单

| 验证点 | 命令 |
|--------|------|
| 基础设施健康 | `docker compose ps` |
| TimescaleDB 扩展 | `docker exec hunter-postgres psql -U hunter -d hunter_edge -c "SELECT extname FROM pg_extension WHERE extname='timescaledb';"` |
| Kafka 平台内部 Topic | `docker exec hunter-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| MinIO Bucket | 浏览器打开 `http://localhost:9001`（7 个 Bucket：hunter-raw-data / rosbag / video / ota-packages / reports / logs / scene-assets） |
| Redis | `docker exec hunter-redis redis-cli ping` |
| 共享库测试 | `pytest common/python/tests -q` |
| 数据层契约校验 | `python scripts/verify_data_layer.py`（DDL ↔ ORM ↔ Alembic + Kafka Topic/JSON Schema，24 项） |
| 接口契约校验 | `pytest services/api-gateway/app/tests/test_openapi_contract.py -q`（OpenAPI ↔ 实现 ↔ K8s 清单，29 项） |
| 场景契约校验 | `cd services/scene-service && pytest app/tests/test_scene_contract.py -q`（契约 ↔ 设计文档 4 章/12.2 节 ↔ DDL ↔ Kafka ↔ K8s，29 项） |
| 数据采集契约校验 | `cd services/data-collector && pytest app/tests/test_data_collector_contract.py -q`（契约 ↔ 设计文档 5 章 ↔ DDL ↔ Kafka ↔ K8s，30 项） |
| 数据分析契约校验 | `cd services/data-analytics && pytest app/tests/test_data_analytics_contract.py -q`（契约 ↔ 设计文档 6 章/12.4 节 ↔ DDL ↔ Kafka ↔ K8s，32 项） |
| 数据库迁移 | `alembic -c common/python/alembic.ini current` / `... upgrade head` / `... upgrade head --sql`（离线预览） |
| 表结构核对 | `docker exec hunter-postgres psql -U hunter -d hunter_edge -c "\\dt scene_svc.*"` |
| 服务健康探针 | `curl http://localhost:<port>/healthz` |
| 服务单元测试 | `cd services/<service> && pytest -q` |
| 指标端点 | `curl http://localhost:<port>/metrics`（Prometheus 文本格式，含 `hunter_` 前缀指标） |
| K8s/监控清单静态校验 | `python scripts/verify_infra.py`（68 个文档：API 版本/命名空间/探针/资源/敏感字段/端口/契约） |
| 容器镜像构建 | `docker build -f services/<service>/Dockerfile -t hunter/<service>:0.1.0 .` |
| 监控栈部署 | `kubectl apply -k infra/monitoring` + `kubectl apply -f infra/monitoring/exporters/exporters.yaml` |

## 开发约束（摘要）

- **契约先行**：先在 `contracts/` 定义 OpenAPI / Kafka Schema / DDL，再写实现；契约是单一事实来源。
- **统一响应**：所有接口返回 `{code, message, data, request_id, timestamp}`；错误码使用预定义值（1001–7002）。
- **模块解耦**：服务间仅通过 REST/Kafka 通信，禁止跨服务直查数据库。
- **异步优先**：所有 IO 使用 async/await，禁止在异步函数中调用同步阻塞 IO。
- **安全基线**：JWT + RBAC、车云 Kafka SASL_SSL(SCRAM-SHA-512)、OTA SHA-256 + RSA-2048 签名、日志脱敏。
- **配置管理**：所有参数从环境变量读取（pydantic-settings），禁止硬编码 URL/密钥/端口/阈值。
- 全部约束以《数据采集与分析系统详细设计文档 V4.0》为准，不可自行更改字段名、Topic 名、表名、错误码、阈值。

## 文档

- `docs/`：设计文档索引与开发文档
- `contracts/`：接口契约（数据库 DDL/ER/受控词表、Kafka Topic 清单/消费者组/11 个消息 JSON Schema 已完成；OpenAPI 已完成 api-gateway / scene-service / data-collector / data-analytics，ota-service / remote-control 随开发填充）
- `release.md`：版本变更记录

