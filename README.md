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
│   ├── k8s/                   # K8s 部署清单（后续层级）
│   ├── docker/                # 本地开发：postgres/kafka/minio 初始化脚本
│   └── monitoring/            # Prometheus/Grafana 配置（后续层级）
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
├── Dockerfile             # 多阶段构建（构建上下文 = 仓库根目录）
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
2. 安装共享库 `hunter_common`（editable）
3. 启动业务微服务（依赖基础设施）：scene-service → data-collector → data-analytics → ota-service → remote-control
4. 启动 api-gateway（依赖上述服务就绪后统一对外路由，8080）
5. 前端 dev server（Vite，5173，后续层级初始化）

## 验证命令清单

| 验证点 | 命令 |
|--------|------|
| 基础设施健康 | `docker compose ps` |
| TimescaleDB 扩展 | `docker exec hunter-postgres psql -U hunter -d hunter_edge -c "SELECT extname FROM pg_extension WHERE extname='timescaledb';"` |
| Kafka 平台内部 Topic | `docker exec hunter-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| MinIO Bucket | 浏览器打开 `http://localhost:9001`（7 个 Bucket：hunter-raw-data / rosbag / video / ota-packages / reports / logs / scene-assets） |
| Redis | `docker exec hunter-redis redis-cli ping` |
| 共享库测试 | `pytest common/python/tests -q` |
| 服务健康探针 | `curl http://localhost:<port>/healthz` |
| 服务单元测试 | `cd services/<service> && pytest -q` |

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
- `contracts/`：接口契约（当前为占位，随各层级开发逐步填充）
- `release.md`：版本变更记录

