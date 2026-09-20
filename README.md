# HunterCore —— HUNTER 自动驾驶数据采集与分析系统

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
├── frontend/                  # Vue 3 管理后台（Vite + TS + Element Plus + Pinia + ECharts + Three.js）
│   └── src/                   # api（按模块）/ types（契约类型）/ stores / router / views / utils / components
├── infra/
│   ├── k8s/                   # K8s 清单：6 微服务 Deployment/Service/ConfigMap、中间件 StatefulSet/PVC、初始化 Job、Ingress
│   ├── docker/                # 本地开发：postgres/kafka/minio 初始化脚本
│   └── monitoring/            # Prometheus 采集与告警 + Alertmanager + Grafana 看板 + exporters + 监控组件清单
├── flink-jobs/                # Flink 1.18 实时分析作业（后续层级）
├── spark-jobs/                # Spark 3.5 离线分析作业（后续层级）
├── docs/                      # 设计与开发文档
├── scripts/                   # 部署与运维脚本集（Ubuntu 22.04 单机 Docker Compose 一键部署）
├── docker-compose.yml         # 本地开发基础设施（TimescaleDB/Kafka/Redis/MinIO）
└── release.md                 # 变更记录
```

### 单机部署脚本（`scripts/`，12 个）

| 脚本 | 用途 |
|------|------|
| `common.sh` | 公共库：结构化日志（终端 + `/var/log/hunter-edge-install.log`）、检查（root/端口/磁盘/等待）、`.env` 加载（CRLF 自动归一）、容器/Kafka/mc 辅助 |
| `install.sh` | 一键部署主脚本（13 步，幂等可续跑）：系统初始化 → Docker → 目录 → .env/证书 → 镜像 → 中间件 → DB/Kafka/MinIO 初始化 → 业务服务 → 健康检查 → 摘要 |
| `gen-passwords.sh` | 生成 `.env` 强随机口令（幂等；`--force` 轮换）+ `passwords.txt`(600)，口令不入日志 |
| `gen-kafka-certs.sh` | 生成 Kafka SASL_SSL 全套证书（CA/broker/client + JKS + P12），校验 SAN 含真实 SERVER_IP |
| `init-db.sh` | 应用 `schema.sql`/`timescaledb.sql`/`init-data.sql` 并校验（8 schema / 11 表 / 2 hypertable），可选注入 admin 口令 |
| `init-kafka.sh` | 创建 6 个契约 Topic（分区/保留时间校验），可选 `--scram-users` |
| `init-minio.sh` | 创建 7 个 Bucket 与生命周期规则（30/90 天与永久） |
| `health-check.sh` | 全栈健康检查（10 类，`[PASS]/[WARN]/[FAIL]`，退出码 0/1/2），可被其他脚本 source 复用 |
| `daily-check.sh` | 日常巡检（复用健康检查 + critical 事件/DB 连接数/容器重启次数/容量），报告写入 `/var/log/hunter-edge/check/` |
| `backup.sh` | 备份 PostgreSQL 全量 + 时序热数据 + 配置 + Redis RDB，按保留天数清理，输出 MinIO `mc mirror` 指引 |
| `uninstall.sh` | 卸载（默认保留数据；`--all` 需输入 `DELETE-ALL`，含受保护路径护栏） |
| `collect-logs.sh` | 日志与系统信息收集（`.env` 脱敏 + 残留明文口令自检） |

```bash
# 部署（服务器上脚本位于 /opt/hunter-edge/scripts/）
sudo bash /opt/hunter-edge/scripts/install.sh -y --ip <服务器IP>
# 运维
bash /opt/hunter-edge/scripts/health-check.sh     # 健康检查（exit 0/1/2）
bash /opt/hunter-edge/scripts/daily-check.sh      # 日常巡检（生成报告）
bash /opt/hunter-edge/scripts/backup.sh           # 数据备份
bash /opt/hunter-edge/scripts/collect-logs.sh     # 收集日志供排障
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
- Node.js 20.19+（前端 `frontend/`）

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
# 全量单元回归（每个服务独立进程，避免顶层包名 app 冲突）
python scripts/run_unit_tests.py
pytest common/python/tests -q                     # 共享库测试（含 Kafka 契约/消息/缓冲/幂等/生产/消费）
cd services/scene-service && pytest -q            # 服务测试（每个服务目录内执行）
ruff check tests common/python services           # Lint（CI 同款命令）
```

> ⚠ 测试根目录必须逐个运行（6 个服务共用顶层包名 `app`）：CI 与本地统一走
> `python scripts/run_unit_tests.py`。

## 数据采集链路（审查 R1 后已落地）

`data-collector` 承担车端 Topic 的接入与预处理，链路与契约
（`contracts/kafka/consumer-groups.yaml` + `contracts/openapi/data-collector.yaml`
`x-hunter-ingest-pipeline`）一一对应：

| 消费组 | 订阅 | 职责 | 产出 |
|--------|------|------|------|
| `data-collector-telemetry` | `hunter.*.telemetry` | 6 步预处理（校验→时间对齐→清洗→映射）→ 批量入库 | `telemetry_raw` / `telemetry_clean` + `vehicle_telemetry` |
| `data-collector-health` | `hunter.*.health` | 写车辆实时读模型（不落库） | `vehicle:status:{id}` / `vehicle:online:set` |
| `data-collector-events` | `hunter.*.event` | 等级一致性校验 → 幂等落库 | `event_raw` + `events` |

- **批量入库**：缓冲达到 `TELEMETRY_BATCH_SIZE` 或消费者批次收尾时冲刷；
  批次钩子在**提交 offset 之前**执行，钩子失败 → 跳过提交 → 消息重投（写侧
  `ON CONFLICT (time, vehicle_id) DO NOTHING` 保证幂等）。
- **车辆状态守护**：`VehicleStatusSweeper` 周期对账，心跳超
  `VEHICLE_OFFLINE_THRESHOLD_SECONDS`（默认 10s）→ 置 `offline` 并移出在线集合
  （OTA 门禁与远程接管判定的数据来源）。
- **运行时契约**：消费侧 Schema 校验（`schema_name="auto"`）需要契约目录可访问，
  集群内由 `hunter-contracts` ConfigMap 挂载（生成/校验：
  `python scripts/generate_contracts_configmap.py [--check]`）。

## 本地启动顺序（依赖链）

1. `docker compose up -d`：TimescaleDB → Kafka（kafka-init 按契约创建平台内部 Topic）→ Redis → MinIO（minio-init 创建 7 个 Bucket + 生命周期）
2. 安装共享库 `hunter_common`（editable），并执行数据库迁移：`alembic -c common/python/alembic.ini upgrade head`（建 schema/表/hypertable）
3. 启动业务微服务（依赖基础设施）：scene-service → data-collector → data-analytics → ota-service → remote-control
4. 启动 api-gateway（依赖上述服务就绪后统一对外路由，8080）
5. 前端 dev server：`cd frontend && npm install && npm run dev`（Vite 5173，代理 `/api` 与 `/ws` → 网关 8080；详见 `frontend/README.md`）

## Kubernetes 部署与监控（L1）

基础设施层清单位于 `infra/k8s/`（应用与中间件）与 `infra/monitoring/`（监控栈），
静态校验与部署命令：

```bash
# 静态校验（无需集群）：API 版本/命名空间/探针/资源/HPA/PDB/敏感字段/端口/契约一致性
python scripts/render_k8s.py --check    # 镜像 {version} 占位符契约（6 个微服务）
python scripts/verify_infra.py

# 版本渲染（{version} 占位符 → 发布版本；清单禁止硬编码版本，产物 build/k8s/）
python scripts/render_k8s.py --version 0.1.0

# 部署顺序（完整版见 infra/k8s/README.md）
# 0) 生成运行时契约 ConfigMap（消费侧 Schema 校验依赖；契约变更后需重新生成）
python scripts/generate_contracts_configmap.py
kubectl apply -f build/k8s/base/            # 命名空间 + 共享 ConfigMap + 契约 ConfigMap + Secret（先复制 02-secret.example.yaml）
# 前置：创建 TLS Secret hunter-kafka-tls / hunter-minio-tls / hunter-core-tls
kubectl apply -f build/k8s/statefulsets/    # postgres / kafka / redis / minio
kubectl apply -f build/k8s/jobs/            # Topic 与 Bucket 初始化（契约一致）
kubectl apply -f build/k8s/services/        # 6 个微服务
kubectl apply -f build/k8s/ingress.yaml     # 网关路由表 + WebSocket + TLS
# 网络策略（默认拒绝入向 + 仅网关可达后端；审查 R5 纵深防御）
kubectl apply -f build/k8s/networkpolicies/
# 自动扩缩容（min=2/max=5/CPU 70%）与自愿中断保护（PDB）
kubectl apply -f build/k8s/autoscaling/hpa.yaml
kubectl apply -f build/k8s/disruption/poddisruptionbudgets.yaml

# 监控栈
kubectl apply -k infra/monitoring
kubectl apply -f infra/monitoring/exporters/exporters.yaml
```

要点：

- **探针**：`startupProbe`/`livenessProbe` → `/healthz`，`readinessProbe` → `/readyz`（初始延迟 10s / 周期 5s；就绪端点内部 2s 超时，依赖异常时快速 503 + `code=5001`）
- **优雅终止**：`terminationGracePeriodSeconds` 基准 30s（api-gateway / scene-service）；消费类与会话类服务 60s（data-collector 提交 offset、ota 任务交接、remote-control 会话收敛，契约 pending 已固化）
- **滚动更新**：`maxSurge: 1` / `maxUnavailable: 0`（更新期间不降级），配合 PDB 覆盖节点排空场景
- **自动扩缩容**：HPA 6 个微服务（min=2 / max=5 / CPU 70%），需集群安装 metrics-server；有状态中间件禁止 HPA
- **指标**：各服务暴露 `GET /metrics`（`common/python/hunter_common/metrics.py`，统一前缀 `hunter_`），Prometheus 通过 Pod 注解自动发现
- **安全**：`hunter-core` 命名空间 `pod-security=restricted`；密钥/证书仅经 Secret 注入；Kafka `SASL_SSL + SCRAM-SHA-512`；MinIO HTTPS + SSE-S3
- **告警**：5 组规则（服务健康 / API 性能 / 数据管道 / 中间件 / 业务约束），阈值需与设计文档 15.4.2 节核对
- **看板**：`hunter-fleet-overview`、`hunter-kafka`、`hunter-api-performance`（Git 供给，UI 只读）

## 数据层与契约（L2）

数据层契约（单一事实来源）已落地，后续服务开发必须先扩展契约再写实现：

```bash
# 1) 建表：扩展 + 8 个 schema + 13 张表 + hypertable（1 day 分块 / 90 天保留）
alembic -c common/python/alembic.ini upgrade head

# 2) 离线生成 SQL（无需数据库，CI/DBA 评审）
alembic -c common/python/alembic.ini upgrade head --sql > /tmp/hunter_ddl.sql

# 3) 契约一致性校验：DDL ↔ ORM ↔ Alembic + Kafka Topic/JSON Schema + Redis Key + MinIO（26 项，无需数据库）
python scripts/verify_data_layer.py
```

| 契约 | 位置 | 内容 |
|------|------|------|
| 数据库 DDL | `contracts/database/ddl/*.sql` | `00_schemas` 扩展/schema/公共函数；`01_core` 车辆 + RBAC 五表；`02_scene`；`03_ota`；`04_events`；`05_timeseries`（hypertable） |
| ER / 受控词表 | `contracts/database/er.md`、`enums.md` | 关系与跨 schema 只读例外；车辆 8 态 / 事件 19 种类型 3 级等级 / OTA 9 态状态机 |
| Kafka Topic | `contracts/kafka/topics.yaml`、`consumer-groups.yaml` | 车端 9 个 + 平台内部 6 个 Topic（分区/副本/acks/保留/key）；12 个消费者组（手动提交 + DLQ + 幂等键） |
| 消息 Schema | `contracts/kafka/schemas/*.schema.json` | 11 个 draft-07 JSON Schema（telemetry/event/health/command/command_result/ota_notify/ota_status/remote_control/analytics_result/sensor_file/alert_event），自带设计文档示例 |
| Redis Key | `contracts/database/redis-keys.yaml` | 缓存契约（设计文档 9.5 节 + 约束第 8 条）：8 个受控键（7 个约定键 + `rc:lock:{vehicle_id}` 派生键）的类型 / TTL / 字段结构 / 失效路径 / 读写方 / 跨服务引用 |
| 对象存储 | `contracts/database/object-storage.yaml` | MinIO 契约（设计文档 9.4.1/9.4.2 节 + 约束第 7、9 条）：7 个 Bucket 的生命周期（`hunter-rosbag` 按对象 Tag `hunter-retention`：`regular` 30 天 / `event` 永久，G-12）/ SSE-S3 / 读写方 / 对象键约定，预签名策略（上传 3600s / 下载 900s / Range / 禁止落日志） |

要点：

- **ORM 与 DDL 逐列一致**：列名、类型（TEXT/UUID/JSONB/TEXT[]/TIMESTAMPTZ/CHAR(n)…）、可空性、主键三方对齐，偏差会让校验脚本失败
- **受控词表统一**：`hunter_common.database.enums` 的 `StrEnum` 是唯一来源，DB 侧用 `TEXT + CHECK`（便于扩展），`StrEnumType` 拒绝非法取值
- **通用 Repository**：`BaseRepository`（CRUD + 分页 + 软删除 + `ON CONFLICT DO NOTHING` 批量写入），默认过滤软删除记录，非法字段抛 2001；写入用 SAVEPOINT 局部回滚（`add`/`setattr` 必须在 SAVEPOINT 内、冲突后禁止 `expunge`，见契约 §3 写路径纪律），不侵占调用方事务边界
- **排序与索引对齐**：排序 spec 支持 `-col:nl`（`DESC NULLS LAST`），默认排序与 DDL 索引一致，避免 `Sort` 破坏 P95 ≤ 200ms
- **复合主键保护**：时序表（`vehicle_telemetry` / `algorithm_metrics`）禁用基类 `get`/`hard_delete`，防止跨车辆误命中
- **显式加载与读取上限**：读方法支持 `options=(selectinload(...),)`；分页 `page_size ≤ 200`，时序序列读取放宽到 `MAX_SERIES_POINTS = 10000`（须带时间窗）
- **方法级契约校验**：`orm-mapping.md` 的 Repository 专属方法列与实现双向比对（测试 + `verify_data_layer.py` 校验 13），未登记即失败
- **写入性能路径**：时序/事件走 `bulk_create*`（executemany 分片），支撑遥测入库延迟 ≤ 1s、时序写入 ≥ 10000 点/秒
- **⚠ 待核对项**（设计文档 15.2/9/5.3 到位后回填）：`vehicle_svc`/`user_svc` schema 归属、`scenes.version` 类型、`scenes.scene_type`、`ota_versions.release_type/status`、`data_analytics` 报告/评估结果元信息缺表（落 MinIO `hunter-reports`）

存储契约（Redis / MinIO）要点：

- **单一事实来源**：Cache 与对象存储的键模式、Bucket 名称、生命周期、预签名有效期只在两份 YAML 中定义；各服务 OpenAPI 的 `x-hunter-service.redis_keys` / `minio_buckets` 是**声明**而非定义，校验器逐项比对键模式 / 类型 / TTL / 读写方 / 生命周期天数，声明未登记资源即失败（禁止实现侧发明 Redis 前缀或新 Bucket）
- **三方一致**：MinIO 契约 ↔ `infra/docker/minio/init-buckets.sh` ↔ `infra/k8s/jobs/minio-init-job.yaml`（`create_bucket` 集合、`add_expiry` 天数、`encrypt_bucket` SSE-S3 覆盖 7 个 Bucket）；Redis 契约 ↔ 6 份服务契约（兼容「对象列表」与 `{read: [...]}` 两种既有声明形态）
- **校验器条目**：`verify_data_layer.py` 校验 9（Redis Key）、校验 10（对象存储）；单测见 `common/python/tests/test_storage_contracts.py`
- **⚠ 待确认项**（`x-hunter-pending-confirmation` 共 15 项 / 标阻塞 8 项）：Redis —— `vehicle:status:{vehicle_id}` 字段清单与写入方归属、`rc:lock:{vehicle_id}` 派生键登记、**网关登出 Token 黑名单键模式缺失（安全缺陷）**；对象存储 —— 对象键是否含 Bucket 名前缀、`hunter-rosbag` 前缀语义冲突**已由 G-12 定稿**（改为对象 Tag `hunter-retention` 生命周期：regular 30 天 / event 永久，data-collector complete 阶段服务端打标，留痕于 pending #2）、分片上传 `part_size_bytes` 未定、MinIO 配置项未进入 `hunter_common.config`

Kafka 契约驱动的生产/消费（共享库 `hunter_common.kafka`）要点：

- **契约运行时加载**：`hunter_common.kafka.contracts` 读取 `topics.yaml` / `consumer-groups.yaml` / `schemas/*.schema.json`，
  解析六种 Topic 写法（契约名 / 模板 `hunter.{vehicle_id}.telemetry` / 正则 `hunter.*.telemetry` / 具体实例 / 广播 / DLQ）；
  契约目录定位顺序：显式 `KAFKA_CONTRACT_DIR` → 工作目录向上查找 → 包位置向上查找（**显式启用校验时契约缺失即 fail fast**，不静默降级）
- **生产者**：按 Topic 契约 `acks` 选择底层 Producer 实例（telemetry=1 / health=0 / 其余=all）；
  契约 `key=vehicle_id` 的 Topic 强制 key 与消息体一致；`retries=3` + 指数退避（仅可重试错误）；
  网络中断且重试耗尽时落盘缓冲（上限 1GB，契约 `producer_defaults.local_disk_buffer_bytes`），`replay_buffered()` 重投
- **消费者**：`schema_name="auto"` 时按消息实际 Topic 校验契约 Schema（非法消息直接进 DLQ，`reason=schema_invalid` 不重试）；
  handler 失败指数退避重试，耗尽后转投 `{topic}.dlq` 并保留 `dlq.original.topic/partition/offset/reason/error` 头；
  整批处理后手动提交 offset（at-least-once）；可注入 `IdempotencyGuard` 跳过重复消息；每批刷新 `hunter_kafka_consumer_lag`
- **指标**（前缀 `hunter_kafka_`，随各服务 `/metrics` 暴露）：生产计数/时延/重试、缓冲水位/丢弃/重投、
  消费计数（processed/skipped_duplicate/schema_invalid/handler_failed）、DLQ 计数、分区消费积压
- **验证**：`pytest common/python/tests -q`（契约 round-trip、Mock Producer/Consumer 行为、缓冲崩溃恢复、幂等守卫）；
  ⚠ 服务接入 `schema_name=SCHEMA_AUTO` 属显式选择，需先确认契约文件在容器内可读（K8s 挂载 ConfigMap），见 `release.md` 风险项

## 接口契约（OpenAPI）

REST 接口契约集中在 `contracts/openapi/`，遵循「先契约、后实现」；契约 ↔ 实现 ↔ K8s 清单三方一致性由契约测试强制校验。

| 契约 | 覆盖范围 | 状态 |
|------|----------|------|
| `api-gateway.yaml` | 统一认证（登录 / 刷新 / 登录态查询 / 注销）+ 运维探针（`/healthz`、`/readyz`、`/metrics`）+ 路由表 / 五级限流 / WebSocket / 审计日志 / 错误码映射扩展字段 | ✅ |
| `scene-service.yaml` | 场景库 CRUD（10 端点，12.2 节）+ 4.2.1 分类体系 / 4.2.2 配置结构 / 4.3 导出 / 4.4 下发 / 4.5 实车提取扩展字段 | ✅ 契约 + 实现（服务端口 8081；100 项测试） |
| `data-collector.yaml` | 数据采集（7 端点：遥测查询 / 事件查询与确认 / 文件清单与预签名上传）+ 5.3.3 遥测结构 / 5.4 预处理 / 5.5 上传流程扩展字段 | ✅ |
| `data-analytics.yaml` | 数据分析（8 端点：报告列表/详情/生成、看板、感知与控制评估、场景覆盖率、Corner Case）+ 6.2 实时作业 / 6.3 离线作业 / 6.4 挖掘 / 6.5 报告扩展字段 | ✅ |
| `ota-service.yaml` | OTA 管理（15 端点：版本仓库 CRUD/发布/废弃、升级任务 CRUD/start|pause|resume|cancel|rollback、升级记录）+ 8/9 章 DDL 与 Kafka 对齐 + 版本上传两步式 / 灰度批次 / 发布校验扩展字段 | ✅ |
| `remote-control.yaml` | 远程操控（8 端点：可操控车辆 / 会话列表-创建-详情-结束 / 操控记录列表-详情-录像）+ WebSocket 契约（控制 20Hz + 信令）/ 会话生命周期 / 控制通道与安全约束 / 视频 / MinIO 归档扩展字段 | ✅ |

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
  - **实现（L3 Step 2）**：10 个端点全部落地（`app/routers/{scenes,templates,export,simulation}.py`），
    4.2.1 分类体系驱动 17 个内置模板，4.3 节导出 Carla XML / OpenSCENARIO 1.2 至 `hunter-scene-assets`+900s 预签名，
    4.4 节下发走环境变量化的 Carla 管理 API 客户端（超时/重试 → 5001），4.5 节由 `analytics_result` 消费者自动建
    `real_vehicle_replay` 草稿场景（幂等键 `(vehicle_id, window_start)`，异常进 `analytics_result.dlq`）；
    状态机与错误码严格按契约（重名 3002、状态冲突 3003、软删除过滤、`cache:scene:{scene_id}` 写后失效）
- **数据采集服务**：`data-collector.yaml` 的 7 个业务端点由「附录 D 限流端点 + 5.5 上传流程 + events 表列能力」推导
  （12.2 节未随仓库提供，推导依据在 `x-hunter-endpoints.derivation` 逐条登记）；遥测查询响应与 5.3.3 节消息结构逐字段一致
  （`x-hunter-telemetry-query-contract` 给出扁平列映射）；文件上传 6 步流程 + 命名规范 + 校验失败复用 6001（`x-hunter-file-upload-flow`）；
  Kafka 消费 4 个车端 Topic（`data-collector-*` 消费组）、生产 4 个内部 Topic（含本次补全的 `sensor_file.schema.json`）
- **⚠ 待确认**（契约内标 `pending_confirmation`）：~~认证端点路径与载荷~~（**已实现**：网关自持
  login/refresh/logout/me，MFA 待数据库契约扩展；详见 `docs/api-gateway.md`）、`/api/v1/vehicle|user` 归属服务（模块表仅 6 个微服务，未配置 URL → 503）、
  MFA / 限流错误码复用、服务发现机制与配置键名、熔断阈值；场景服务侧见其契约 `x-hunter-pending-confirmation`（12 项）；
  数据采集侧见其契约 `x-hunter-pending-confirmation`（12 项：端点清单来源 / 文件元信息缺表 / sensor_file 字段 / Carla Topic 归属等）；
  数据分析侧见其契约 `x-hunter-pending-confirmation`（12 项：报告元信息缺表 / 报告异步语义 / TTC 等级冲突 /
  alert_event 消费方落位 / 消费组契约修正等）；
  OTA 管理侧见其契约 `x-hunter-pending-confirmation`（19 项：12.5 节原文缺失 / `release_type` 取值域 / 跳批策略 /
  publish 网关超时与异步化 / DLQ Topic 登记 / command_result 与 broadcast 归属 / 审计保留期等）；
  远程操控侧见其契约 `x-hunter-pending-confirmation`（20 项，其中 8 项 `blocking`：12.6 节原文缺失 /
  会话状态名与心跳阈值 / SRS 应用名与 WHIP-WHEP 端点 / WS 子路径拆分 / 操控指令 `command_id` 缺失（回执精确关联） /
  `command_type` 取值域 / WS 握手 JWT 传送方式 / 车辆状态读模型字段清单与写入方 / 多副本粘性路由等）
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
- **远程操控服务**：`remote-control.yaml` 的 8 个业务端点由「附录 D 明列端点 `POST /api/v1/remote/session` +
  网关 `x-hunter-websocket-routes`（`/ws/remote/**`）+ 第 15 条安全约束 + MinIO `hunter-video` 录像」推导
  （12.6 节未随仓库提供，推导依据在 `x-hunter-endpoints.items` 逐条登记）；不可更改阈值已在契约中固化：
  指令 20Hz（50ms）→ Kafka `hunter.{vehicle_id}.remote_control`、>500ms 无指令车端自动减速停车、服务端对
  `target_velocity` 限幅 ±2.0 m/s、同车同一时间仅一名操作员（Redis 分布式锁 + 错误码 7001）、视频 720p@30fps
  H.264（NVENC）2048–4096 kbps 关键帧 1s、视频端到端 ≤200ms、指令 ≤100ms；会话状态机
  `connecting → active → degraded → ended`，结束收敛顺序「session_end → 停录像 → 封存 sidecar → 释放锁 → 删 Redis 键」；
  **操控记录不落数据库**（方案 A）：会话态 = Redis `rc:session:{vehicle_id}`，操控记录 = MinIO `hunter-video`
  录像 `remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4` + 同目录 sidecar JSON（保留 90 天），
  代价是历史查询无 SQL 过滤/聚合能力（需按日期前缀收敛，`RC_HISTORY_QUERY_MAX_RANGE_DAYS` 默认 31）；
  Kafka 仅消费 `hunter.*.command_result`（消费组 `remote-control-command-result`），`contracts/kafka/` 相关条目
  均已登记、本契约零改动；媒体面 SRTP/UDP 不经网关，须在 K8s 单独暴露 UDP

## 前端管理后台（L4）

`frontend/` 为 Vue 3 + TypeScript 管理后台，**只经 api-gateway(8080)** 访问后端（REST `/api/v1/**` + WS `/ws/remote/**`），
媒体面 WebRTC/SRTP 不经网关；页面模块与 OpenAPI 契约一一对应：

| 页面模块 | 路由 | 主要契约端点 |
|----------|------|--------------|
| 运营看板 | `/dashboard` | `GET /analytics/dashboard`（车队/管道/算法/事件四维，含降级标记）、`GET /remote/vehicles` |
| 场景库 | `/scenes`、`/scenes/:scene_id` | `/api/v1/scene`（列表/创建/更新/复制/发布/删除/模板/导出/下发 Carla 仿真）+ Three.js 3D 预览 |
| 数据分析 | `/analytics` | `perception/eval`、`control/eval`（阈值判定）、`scene/coverage`（热力图）、`corner-cases`、`reports*`（异步生成 + 轮询） |
| OTA 管理 | `/ota/versions`、`/ota/tasks`、`/ota/tasks/:task_id` | `/api/v1/ota/versions*`（本地算 MD5/SHA-256 → 分片上传 → 服务端 6001/6002 校验）、`tasks*`（4 批灰度 5/20/50/100 + 门禁放行/阻塞明细 + A/B 回滚） |
| 远程操控 | `/remote/console`、`/remote/history` | `/api/v1/remote/*` + WS `control`（20Hz）/`signal`（WebRTC 中继）；原生 WebRTC 视频 + 急停 + 链路统计 + 录像回放 |
| 事件与文件 | `/data` | `/api/v1/data/events*`（确认留痕）、`files*`（预签名下载/游标分页） |

要点：

- **契约类型直用**：`src/types/**` 字段名与 `contracts/openapi/*.yaml` 完全一致（`snake_case`），不做驼峰转换，降低字段错配风险
- **统一响应处理**：`api/request.ts` 解析 `ApiResponse`，`code≠0` 抛业务异常；`1003` 单飞刷新后重放，`1001` 清理会话并广播失效事件
- **限流友好**：轮询间隔全部来自环境变量（遥测 ≥ 2000ms，符合附录 D 20 QPS），页面隐藏暂停、请求未完成跳过本轮
- **远程操控安全约束**：控制 20Hz 固定节拍、速度以会话响应 `max_speed_mps` 限幅（默认 2.0 m/s）、心跳 10s、
  `estop` 立即下发、离开页面自动结束会话、终态关闭码（1008/4001/4003/4010）不重连
- **RBAC 一致性**：`v-permission` 指令与路由 `meta.permission` 使用的权限编码均取自各服务契约 RBAC 说明
  （`scene:read|create|update|delete|execute`、`data:read`、`analytics:read|execute`、`ota:read|create|execute`、`remote:read|create|execute`），前端仅做展示控制，鉴权以后端为准
- **已知折衷**：Token 存 `localStorage`（无 HttpOnly Cookie 端点契约）；`OtaReleaseType` 未定义 enum（pending #2）、`RC_MAX_STEER_RAD`（pending #18）、WS JWT 传送方式（pending #20，现用子协议 `hunter-jwt`）

## L5 测试层（集成 / 端到端 / 性能）

基于 testcontainers 的三层测试体系（`tests/`），按「契约即示例即测试数据」组织，覆盖系统关键约束三条主链路与第 10 条性能指标：

| 层级 | 位置 | 内容 | 依赖 |
|------|------|------|------|
| 集成 | `tests/integration/` | 11.1 遥测链路（Topic 分区/保留契约、字节级往返、落库列覆盖、入库延迟、幂等、hypertable 配置、丢包检测）；API 契约面（统一响应/trace_id/就绪探针/契约外端点拦截，6 服务进程实测） | Docker（Postgres/Kafka/MinIO）+ uvicorn 进程 |
| 端到端 | `tests/e2e/` | 11.2 OTA（版本防回滚 6003、SHA-256/RSA 校验顺序、门禁 6003、车端状态机与回滚路径、灰度 5/20/50/100 与 95% 门槛、Kafka 下发/上报往返、Redis 进度 Hash TTL、MinIO 预签名包仓库）；11.3 远程操控（互斥 7001/4001/4002、限幅 2.0 m/s、20Hz 节奏、500ms 超时安全停车、Kafka 20Hz 指令流延迟 ≤100ms、Redis 互斥锁/会话/车辆状态） | 纯逻辑可跑；Kafka/Redis 部分需 Docker |
| 性能 | `tests/performance/` | API P95 ≤ 200ms（/healthz 全链路）、遥测入库延迟 ≤ 1s、时序写入 ≥ 10000 点/s、Kafka 吞吐回归下限（阈值 `tests/support/thresholds.py`，来源逐条登记，禁止魔法数字） | Docker + api-gateway 进程 |

```powershell
# 全量运行（无 Docker 时容器型用例自动 skip，绝不伪造结果）
python -m pytest tests -q

# 生成 L5 测试报告（docs/test-reports/ 下 Markdown + JSON，CI artifact）
python -m pytest tests -q --l5-report

# 测试层自检：pytest.ini 标记 ↔ report.MARKERS、阈值来源登记、报告模板占位符、用例号
python scripts/verify_test_layer.py
```

- **支撑模块**（`tests/support/`）：`contracts`（契约加载，所有常量唯一来源）、`messages`（消息工厂，契约示例派生 + Schema 回校）、`flow`（11.1/11.2/11.3 平台侧逻辑基准：路由/事件判定/OTA 状态机/灰度/RC 会话/P95）、`broker`（Kafka 生产消费 + Topic 管理 + S3 SigV4 纯标准库实现）、`db`（asyncpg 落库/查询）、`infra`（容器夹具 + uvicorn 服务进程）、`thresholds`（阈值常量 + 来源登记）、`report`（L5 报告聚合，模板 `tests/report-template.md` 渲染）
- **无 Docker 环境降级**：容器型夹具探测 Docker 不可用时 skip（附原因），纯逻辑用例照常运行；CI 在具备 Docker 的 runner 上跑完整套件（`.gitlab-ci.yml`：lint → unit → integration → e2e → performance → report）

## 验证命令清单

| 验证点 | 命令 |
|--------|------|
| L5 测试层全量 | `python -m pytest tests -q`（无 Docker 时容器型用例自动 skip） |
| L5 测试报告 | `python -m pytest tests -q --l5-report`（`docs/test-reports/`，Markdown + JSON） |
| 测试层自检 | `python scripts/verify_test_layer.py`（标记/阈值来源/模板占位符/用例号） |
| 基础设施健康 | `docker compose ps` |
| TimescaleDB 扩展 | `docker exec hunter-postgres psql -U hunter -d hunter_core -c "SELECT extname FROM pg_extension WHERE extname='timescaledb';"` |
| Kafka 平台内部 Topic | `docker exec hunter-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| MinIO Bucket | 浏览器打开 `http://localhost:9001`（7 个 Bucket：hunter-raw-data / rosbag / video / ota-packages / reports / logs / scene-assets） |
| Redis | `docker exec hunter-redis redis-cli ping` |
| 共享库测试 | `pytest common/python/tests -q` |
| 数据层契约校验 | `python scripts/verify_data_layer.py`（DDL ↔ ORM ↔ Alembic + Kafka Topic/JSON Schema + Redis Key + MinIO + Repository 专属方法双向一致 + 服务→Repository 白名单 + 写路径纪律声明，34 项） |
| 存储契约单元测试 | `pytest common/python/tests/test_storage_contracts.py -q`（Redis Key / MinIO Bucket ↔ 服务声明 ↔ 初始化脚本，9 项） |
| 接口契约校验 | `pytest services/api-gateway/app/tests/test_openapi_contract.py -q`（OpenAPI ↔ 实现 ↔ K8s 清单，29 项） |
| 网关认证/限流/转发测试 | `pytest services/api-gateway -q`（登录/刷新轮换防重放/注销幂等/会话强依赖/限流维度/代理转发与熔断，66 项） |
| 场景契约校验 | `cd services/scene-service && pytest app/tests/test_scene_contract.py -q`（契约 ↔ 设计文档 4 章/12.2 节 ↔ DDL ↔ Kafka ↔ K8s，29 项） |
| 场景服务实现测试 | `cd services/scene-service && pytest -q`（端点/服务层/消费者/健康探针 100 项；含错误码 1001/1002/2001/2002/3001/3002/3003/5001 分支） |
| 数据采集契约校验 | `cd services/data-collector && pytest app/tests/test_data_collector_contract.py -q`（契约 ↔ 设计文档 5 章 ↔ DDL ↔ Kafka ↔ K8s，30 项） |
| 数据采集链路测试 | `cd services/data-collector && pytest -q`（114 项：契约 + 健康探针 + FileService（含 SHA-256 内容校验回归）/ EventService / TelemetryService + 5.4 预处理流水线 + 批量入库 + 车辆读模型 + 三路消费者） |
| 契约 ConfigMap 一致性 | `python scripts/generate_contracts_configmap.py --check`（运行时 Schema 校验依赖的 `hunter-contracts` 与 `contracts/kafka` 一致） |
| 数据分析契约校验 | `cd services/data-analytics && pytest app/tests/test_data_analytics_contract.py -q`（契约 ↔ 设计文档 6 章/12.4 节 ↔ DDL ↔ Kafka ↔ K8s，32 项） |
| 远程操控契约校验 | `cd services/remote-control && pytest app/tests/test_remote_control_contract.py -q`（契约 ↔ 设计文档 15 条安全约束/12.6 节推导 ↔ Kafka ↔ MinIO 归档 ↔ K8s，契约测试随实现步骤落地） |
| 契约文件静态校验 | `python -c "import yaml; yaml.safe_load(open('contracts/openapi/remote-control.yaml', encoding='utf-8'))"`（YAML 语法 + `$ref` 解析，无需服务） |
| 数据库迁移 | `alembic -c common/python/alembic.ini current` / `... upgrade head` / `... upgrade head --sql`（离线预览） |
| 表结构核对 | `docker exec hunter-postgres psql -U hunter -d hunter_core -c "\\dt scene_svc.*"` |
| 服务健康探针 | `curl http://localhost:<port>/healthz` |
| 服务单元测试 | `cd services/<service> && pytest -q` |
| 指标端点 | `curl http://localhost:<port>/metrics`（Prometheus 文本格式，含 `hunter_` 前缀指标） |
| K8s/监控清单静态校验 | `python scripts/verify_infra.py`（85 个文档：API 版本/命名空间/探针/资源/HPA/PDB/敏感字段/端口/契约） |
| K8s 清单版本渲染 | `python scripts/render_k8s.py --check` / `--version 0.1.0`（镜像 `{version}` 占位符 → 渲染产物 `build/k8s/`） |
| 容器镜像构建 | `docker build -f services/<service>/Dockerfile -t hunter/<service>:$VERSION .`（禁止 latest，与清单同源） |
| 前端类型检查 | `cd frontend && npm run type-check`（vue-tsc 严格模式，0 error） |
| 前端生产构建 | `cd frontend && npm run build`（type-check + vite build，产物 `frontend/dist/`） |
| 前端本地联调 | `cd frontend && npm run dev` → `http://localhost:5173`（代理 `/api`、`/ws` 到网关 8080） |
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

- `docs/`：设计文档索引与开发文档（含 `deployment.md`：部署指南——镜像构建 / K8s 清单 / 版本变量 / HPA-PDB / 验证清单；`frontend-portal.md`：前端模块设计、鉴权与权限、远程操控实现要点、回归清单）
- `frontend/README.md`：前端快速开始、环境变量、页面与契约端点映射、待确认项
- `contracts/`：接口契约（数据库 DDL/ER/受控词表 + Redis Key / MinIO 对象存储契约、Kafka Topic 清单/消费者组/11 个消息 JSON Schema 已完成；OpenAPI 已完成 api-gateway / scene-service / data-collector / data-analytics / ota-service / remote-control，六个服务契约齐备）
- `release.md`：版本变更记录

