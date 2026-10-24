# HunterCore版本更新文档

## V0.1.0

Date：2026-9-15
变更类型：新增
版本主要有以下功能：
1. 初始化 HunterCore Monorepo 骨架：contracts/（openapi、kafka、database 契约占位）、services/（api-gateway、scene-service、data-collector、data-analytics、ota-service、remote-control 六个微服务 FastAPI 骨架）、common/（hunter_common 共享库 + typescript 占位）、frontend/、infra/（k8s、docker、monitoring）、flink-jobs/、spark-jobs/、docs/
2. 新增共享库 hunter_common：config（pydantic-settings 基础配置）、logging（structlog JSON 日志 + trace_id/vehicle_id 上下文注入 + 敏感字段脱敏）、exceptions（预定义错误码 1001-7002 异常体系）、responses（统一响应 ApiResponse[T]）、kafka（confluent-kafka 异步生产/消费封装，支持 SASL_SSL，手动提交 offset，异常进 DLQ）、database（SQLAlchemy 2.0 异步引擎与会话管理）、redis（异步客户端封装，限流计数/分布式锁）
3. 每个微服务骨架：app/main.py（统一响应、全局异常处理、trace_id 中间件、/healthz /readyz 探针）、app/config.py、routers/schemas/models/services/repositories/consumers/producers/core/tests 完整分层 + Dockerfile（多阶段构建）+ pyproject.toml + README.md；含健康检查接口单元测试
4. 新增本地开发环境 docker-compose.yml：PostgreSQL 15 + TimescaleDB 2.13、Kafka 3.6（KRaft 单节点 + 平台内部 Topic 初始化）、Redis 7、MinIO（7 个 Bucket + 生命周期规则初始化）；新增 infra/docker 初始化脚本（timescaledb 扩展 / create-topics / init-buckets）
5. 新增根目录 .gitignore / .dockerignore / .env.example / README.md（项目说明 + 快速开始 + 启动顺序 + 验证命令）

## V0.1.1（修复）

Date：2026-9-15
变更类型：修复
1. **修复 hunter_common.logging 固定 stdout 引用缺陷**：configure_logging 曾以固定流对象创建 PrintLoggerFactory/StreamHandler，pytest capsys 或容器日志采集器替换流后，旧流关闭导致日志写入抛 ValueError，进而使请求路径 500（实测 /healthz 挂）。现改为 _DynamicStdout 动态流代理：每次写入取当前 sys.stdout，且 IO 异常静默降级（日志故障不影响业务可用性）
2. **修复 /readyz 依赖探测挂起缺陷**：redis-py/asyncpg 对不可达依赖内部重试可达 4s+，导致就绪探针挂起 8s+（触发 K8s readinessProbe 误判）。现为每项探测加 asyncio.wait_for 2s 超时，超时/异常一律返回 503（code=5001）+ 探测明细
3. **新增根级 pytest.ini**：统一 monorepo 测试配置（asyncio_mode=auto、--import-mode=importlib 解决各服务同名 app.tests.* 模块 sys.modules 冲突、统一 testpaths）
4. **scaffold_service.py 集成自动 ruff --fix**：生成骨架后自动修复模板产出的 import 排序等机械 lint 问题，形成"生成→修复→验证"闭环
5. **修复 .gitignore 误忽略**：release.md 与 docs/ 纳入版本控制（文档更新规则要求提交）

验证：pytest 全量 **67 passed**（共享库 49 + 六服务 18）；ruff **All checks passed**；实机冒烟 /healthz=200（统一响应）、/readyz=503（code=5001 + 明细，无挂起）

## V0.2.0

Date：2026-9-15
变更类型：新增
版本主要有以下功能：
1. **Kubernetes 部署清单（L1）**：新增 `infra/k8s/`——base（namespace 启用 pod-security=restricted、共享 ConfigMap、Secret 模板）、services（6 微服务 ConfigMap + Deployment + Service，含 startup/liveness/readiness 三探针、资源 requests/limits、非 root 安全上下文、拓扑分散、maxUnavailable=0 滚动更新）、statefulsets（PostgreSQL 15+TimescaleDB 2.13 / Kafka 3.6 KRaft 三节点 SASL_SSL+SCRAM-SHA-512 / Redis 7 AOF noeviction / MinIO 4 节点纠删码 EC:2 + SSE-S3 + HTTPS，均含 headless Service 与 PVC）、jobs（Kafka 内部 Topic 与 MinIO 7 Bucket + 生命周期 + SSE-S3 初始化）
2. **Ingress 网关入口**：`infra/k8s/ingress.yaml`，按网关路由表前缀（/api/v1/scene|data|analytics|ota|remote|vehicle|user、/ws/remote）统一指向 api-gateway（禁止绕过 JWT 鉴权与限流），WebSocket 低延迟注解 + TLS
3. **镜像加固**：各服务 Dockerfile 改为 `python:3.11-slim`(builder) → `gcr.io/distroless/python3-debian12:nonroot`(runner) 多阶段构建，非 root 运行、无 shell/包管理器，配合 `.dockerignore` 裁剪构建上下文
4. **Prometheus 指标端点**：新增共享模块 `hunter_common/metrics.py`（独立 CollectorRegistry、路由模板标签防基数爆炸、时延直方图桶覆盖 P95≤200ms），并在服务模板中注册 `GET /metrics` 与 HTTP 埋点中间件（统一前缀 `hunter_`）
5. **监控栈配置（L1）**：`infra/monitoring/`——prometheus（k8s 服务发现 + 中间件 exporter 采集 + 5 组 28 条告警规则：服务健康 / API 性能 / 数据管道 / 中间件与基础设施 / 业务约束）、alertmanager（critical 立即通知 + 告警抑制）、grafana（供给配置 + 车队概览 / Kafka / API 性能 3 个看板）、exporters（node-exporter / kube-state-metrics / postgres / redis / kafka exporter，SASL_SSL）、kustomization（配置由 configMapGenerator 从仓库源文件生成）
6. **基础设施静态校验器**：新增 `scripts/verify_infra.py`——校验 API 版本白名单、命名空间、探针与资源配额、镜像标签（禁止 latest）、ConfigMap 敏感字段、服务端口一致性、共享 ConfigMap 键与 `hunter_common.config` 契约一致、Kafka Topic 契约一致性、MinIO Bucket 契约一致性、Dockerfile 多阶段与非 root
7. **`.gitignore` 补充敏感配置规则**：忽略 `infra/k8s/**/02-secret.yaml`、`secret*.yaml`、`*.key`、`*.jks`、`ca.crt` 等，仅保留 `02-secret.example.yaml` 模板
8. **文档更新**：新增 `infra/k8s/README.md`（部署顺序、TLS 证书生成、资源配额与连接池核算表、设计决策、排查手册）、`infra/monitoring/README.md`（指标来源、告警规则清单、看板说明、待办）；更新根 `README.md`（K8s 与监控章节、验证命令）与 `infra/README.md`

验证：pytest 全量 **73 passed**（新增 6 条 /metrics 端点测试）；ruff **All checks passed**；`scripts/verify_infra.py` **全部校验通过**（68 个 K8s 文档 + 6 个 Dockerfile）。`kubectl apply --dry-run` 需集群 API（当前环境无集群，已改为静态校验 + 清单契约校验）



## V0.3.0

Date：2026-9-16
变更类型：新增
版本主要有以下功能：
1. **数据库契约（L2）**：`contracts/database/` 由占位升级为完整契约——`ddl/00_schemas.sql`（TimescaleDB 扩展 + 8 个服务 schema + 公共 `common_set_update_time()` 触发器）、`ddl/01_core.sql`（vehicles 车辆台账 + RBAC 五表 users / roles / permissions / user_roles / role_permissions）、`ddl/02_scene.sql`（scenes：JSONB 配置、TEXT[] 标签、软删除 deleted_at、部分唯一索引 + update_time 触发器）、`ddl/03_ota.sql`（ota_versions / ota_tasks / ota_records：MD5+SHA-256+签名字段、灰度策略/门禁 JSONB、9 态状态机 CHECK + 进行中部分索引）、`ddl/04_events.sql`（events：18 种事件类型 + 3 级等级 CHECK、未确认告警部分索引、(vehicle_id,event_type,event_time) 幂等唯一索引）、`ddl/05_timeseries.sql`（vehicle_telemetry（遥测六段扁平化 43 列）+ algorithm_metrics，hypertable `chunk_time_interval=1 day` + 90 天保留策略，含批量写入与幂等规范）；新增 `er.md`（ER 关系、跨 schema 只读例外、类型约定、与设计文档差异清单）与 `enums.md`（受控词表）
2. **Kafka 契约（L2）**：`contracts/kafka/topics.yaml`（车端 9 个 Topic：分区/acks/频率/保留/key 策略 + 平台内部 6 个 Topic + 生产者/消费者默认参数 + Topic 级限流：单车遥测 ≤100 msg/s、文件上传 ≤10 Mbps）、`consumer-groups.yaml`（12 个消费者组：手动提交 + DLQ + 幂等键 + 延迟目标，含 Flink/Spark 作业消费组）、`schemas/*.schema.json`（8 个 draft-07 消息 Schema，`additionalProperties=false` + required + 受控词表枚举 + 设计文档原例：telemetry / event / health / command / command_result / ota_notify / ota_status / remote_control）
3. **数据访问层（L2）**：`hunter_common/database.py` 重构为 `hunter_common/database/` 包——`session.py`（DatabaseSessionManager）、`base.py`（Base + 约束命名规范 + StrEnumType（TEXT 列 ↔ StrEnum，非法值拒绝）+ CreateTime/UpdateTime/SoftDelete 混入 + UUID 主键列工厂）、`enums.py`（VehicleStatus / EventType + EVENT_LEVEL_BY_TYPE / EventLevel / OtaStatus（终态与进行中集合）/ MetricModule / SceneStatus / OtaTaskStatus / UserStatus / PermissionResource|Action）、`repository.py`（BaseRepository：CRUD + 分页（page_size ≤ 200）+ 软删除（仅声明 deleted_at 的模型）+ bulk_create / bulk_create_ignore_conflicts（executemany 分片 + `ON CONFLICT DO NOTHING`）；非法列名 → 2001、唯一冲突 → 3002、缺失 → 3001）、`models/`（13 张表 ORM，schema 限定，与 DDL 逐列对齐）
4. **Alembic 迁移**：`common/python/alembic.ini`（`%(here)s` 路径，任意工作目录可执行）+ `hunter_common/database/migrations/`（异步 env.py，支持在线执行与 `--sql` 离线生成）+ `versions/0001_initial_schema.py`（schema → 13 表 → 公共函数与触发器 → hypertable 与保留策略，downgrade 成对实现）+ `migrations/README.md`（命令、迁移策略、校验方式）
5. **数据层契约校验器**：新增 `scripts/verify_data_layer.py`——24 项检查：契约文件齐备、DDL↔ORM 逐列（列名/类型/可空性/主键）、DDL CHECK 枚举↔StrEnum、hypertable 契约三处一致、Alembic 离线 SQL 覆盖全部表与关键语句、Kafka Topic 三处同步（topics.yaml ↔ create-topics.sh ↔ kafka-init Job）、JSON Schema draft-07 合法且 examples 自校验、消费者组 Topic 登记与幂等键（全程无需数据库/集群）
6. **单元测试**：新增 54 条（共享库累计 103 条 / 全量 127 条）——ORM↔DDL 契约一致性（含独立解析器交叉校验）、受控词表与状态机常量、StrEnumType 绑定与校验、DatabaseSessionManager 初始化/释放/探活、BaseRepository 行为与 SQL 断言（软删除过滤、分页 offset、IN 过滤、ON CONFLICT、错误码映射）、Kafka Topic/Schema/消费者组契约
7. **基础设施同步**：`infra/docker/postgres/init/01-extensions.sql` 与 `infra/k8s/statefulsets/postgres.yaml` 的 ConfigMap 补齐 `vehicle_svc` / `user_svc` schema（与契约 00_schemas.sql 一致）
8. **文档更新**：根 `README.md` 新增「数据层与契约（L2）」章节、本地启动顺序补迁移步骤、验证命令清单新增数据层校验/迁移/表结构核对；`common/python/README.md` 补充数据访问层用法示例；新增 `migrations/README.md`

验证：pytest 全量 **127 passed**；ruff **All checks passed**（common/services/scripts）；`scripts/verify_data_layer.py` **24 项全通过**；`scripts/verify_infra.py` **全部校验通过**

待核对（设计文档 15.2/9/5.3 未随仓库提供，已在契约中标 ⚠）：`vehicle_svc`/`user_svc` schema 归属、`scenes.version` 类型、`scenes.scene_type`、`ota_versions.release_type`/`status`、`ota_records.phase` 取值域、`vehicle_telemetry.seq` 是否入表、时序表压缩/连续聚合策略、`sensor_file`/`analytics_result`/`alert_event` 消息结构（当前 `schema: null`）

## V0.4.0

Date：2026-9-16
变更类型：新增
版本主要有以下功能：
1. **API 网关接口契约（L3 Step 1，契约先行）**：新增 `contracts/openapi/api-gateway.yaml`（OpenAPI 3.0.3）——自持端点 `POST /api/v1/user/login|refresh|logout`、`GET /api/v1/user/me`（认证）与 `/healthz`、`/readyz`、`/metrics`（运维，`x-internal: true`，与 `app/routers/health.py` 实现一致）；components 统一复用：`bearerAuth`（JWT，载荷含 sub/username/roles/permissions/iat/exp/iss/jti）、`deviceCertificate`（mutualTLS，CommonName = vehicle_id）、`ErrorCode` 枚举、8 类共用错误响应、`ApiResponse` 五字段基座与泛型包装（TokenPair/UserProfile/HealthStatus/ReadyChecks）
2. **扩展字段承载系统契约（机器可读）**：`x-hunter-gateway-routes`（3.4 节 7 个路由前缀 → 目标服务与端口、转发注入头 `X-User-Id`/`X-Roles`/`X-Trace-Id`、服务发现与熔断标记待确认）、`x-hunter-rate-limits`（附录 D 五级限流 + 接口级 5/1/20 QPS + 车辆级 100 msg/s、10 Mbps + 429 与 `Retry-After`）、`x-hunter-websocket-routes`（`/ws/remote/**`：20Hz、>500ms 停车、视频 ≤200ms、指令 ≤100ms）、`x-hunter-audit-log`（访问日志 8 字段 + 敏感信息脱敏）、`x-hunter-kafka`（网关不生产/不消费 Topic）、`x-hunter-error-status-map`（错误码 → HTTP 状态）
3. **契约一致性测试**：新增 `services/api-gateway/app/tests/test_openapi_contract.py` **29 条**——OpenAPI 3.0.3 合法性、`$ref` 全解析、`operationId` 唯一、端点集合与契约完全一致、示例错误码 ⊆ 预定义码、错误码 → HTTP 状态与 `HTTP_STATUS_BY_CODE` 完全一致、路由前缀三方一致（契约 ↔ K8s ConfigMap `GATEWAY_ROUTE_PREFIXES` ↔ Ingress 且全部经 api-gateway:8080）、限流阈值与附录 D 一致、探针字段与实现一致、`/metrics` 文本格式例外、WebSocket 安全约束、Kafka 参与度为空且无网关消费者组、审计字段
4. **文档更新**：`contracts/openapi/README.md` 升级为契约索引（文件清单与状态、统一约定：响应 / 错误码映射 / 认证 / trace_id / 限流 / 分页 / 探针、网关契约要点、校验命令）；根 `README.md` 新增「接口契约（OpenAPI）」章节与验证命令行

验证：pytest 全量 **156 passed**（新增 29 条契约测试）；ruff **All checks passed**；`scripts/verify_data_layer.py` 24 项全通过；`scripts/verify_infra.py` 全部通过

待确认（契约内标 `pending_confirmation`，需人工决策后回填）：认证端点路径与载荷（设计文档 3.2.1 仅给出流程）、`/api/v1/vehicle|user` 归属服务（模块表仅 6 个微服务，无 vehicle-service/user-service）、认证所需账号数据来源（网关读 `user_svc` 只读 vs 调用 user-service REST）、MFA 无专用错误码（复用 1001）、限流无专用错误码（复用 5001 + 429）、服务发现机制与配置键名（Nacos/Consul vs K8s DNS）、熔断阈值与半开策略




## V0.5.0

Date：2026-9-16
变更类型：新增
版本主要有以下功能：
1. **场景生成服务接口契约（L3 Step 1，契约先行）**：新增 `contracts/openapi/scene-service.yaml`（OpenAPI 3.0.3，约 1420 行 / 42 个组件 Schema）——10 个业务端点严格对齐设计文档 12.2 节（`GET|POST /api/v1/scene`、`GET|PUT|DELETE /api/v1/scene/{scene_id}`、`GET /api/v1/scene/templates`、`POST /api/v1/scene/{scene_id}/duplicate|publish|run`、`POST /api/v1/scene/export`）+ 3 个运维端点（`/healthz`、`/readyz`、`/metrics`）；组件复用 `bearerAuth`/`deviceCertificate`/`ErrorCode`/`ApiResponse` 与 10 类共用错误响应
2. **4.2.2 场景配置结构落地为可校验 Schema**：`SceneMeta`（scene_id/scene_name/scene_type/description/version/creator/tags，一一对应 `scene_svc.scenes` 列）+ `SceneConfig`（map/ego_vehicle/weather/actors/events/success_criteria/duration，写入 `config_json`），嵌套段 `SceneMap`/`EgoVehicle`/`Weather`/`Actor(+ActorBehavior)`/`SceneEvent(+Trigger/Action)`/`SuccessCriteria`/`SpawnPoint`，合并即 4.2.2 节完整对象；顶层字段映射由 `x-hunter-scene-config-contract` 机器可读声明
3. **业务规则扩展字段**：`x-hunter-scene-types`（4.2.1 分类体系 6 类 18 个场景类型 + 中文标签，含 4.5 实车回放）、`x-hunter-lifecycle`（draft→published→archived；仅 draft 可编辑、published 可下发、published 需先归档再删除）、`x-hunter-export`（4.3 节 Carla ScenarioRunner XML / OpenSCENARIO 1.2，MinIO `hunter-scene-assets` + 预签名 900s + Range 下载）、`x-hunter-simulation-flow`（4.4 节 7 步下发流程 + running 状态 + Carla 地址环境变量 `CARLA_MANAGEMENT_ENDPOINT`）、`x-hunter-real-vehicle-extraction`（4.5 节：harsh_braking/collision_warning/manual_takeover 触发 + 前后各 10 秒截取 + 自车/目标轨迹与环境 + 输出 real_vehicle_replay/draft）、`x-hunter-rate-limits`（scene 无专属限流，继承网关五级限流）、`x-hunter-kafka`（不生产 Topic，仅消费 analytics_result）、`x-hunter-error-status-map`、`x-hunter-pending-confirmation`（12 项待人工确认）
4. **Kafka 契约补全**：新增 `contracts/kafka/schemas/analytics_result.schema.json`（draft-07，`additionalProperties: false`）——`result_type` ∈ {metric, corner_case, report}、`trigger_event_type` 取 18 种受控事件词表、`event_level` 3 级、`clip.pre_seconds/post_seconds` 固定 10（4.5 节不可配置）、`ego_trajectory`/`objects`(含 trajectory_point/object_track 定义)/`environment`(weather/road_type/speed_limit)、2 个 examples 自校验；同步 `topics.yaml`（analytics_result `schema` 由 null 改为引用 + 消费方说明）与 `consumer-groups.yaml`（scene-service-analytics-result：触发类型、10s 窗口、DLQ、幂等键 `(vehicle_id, window_start)`）
5. **契约一致性测试**：新增 `services/scene-service/app/tests/test_scene_contract.py` **29 条**（文件名按服务唯一命名，规避 monorepo 内 pytest importlib 模式同名模块冲突）——OpenAPI 3.0.3 合法性、`$ref` 全解析、`operationId` 唯一、端点集合与 12.2 节逐条一致、运维端点与 FastAPI 实际注册路由（兼容新版 Starlette `_IncludedRouter` 聚合节点）一致、4.2.2 字段 ↔ `scenes` DDL 列 ↔ config_json 映射一致、`scenes.status` 与 enums.md/ORM/DDL CHECK 四处一致、4.2.1 分类体系与 SceneType 枚举及模板分类参数一致、导出格式与格式扩展名一致、4.4 流程 7 步与关键要素、4.5 触发事件与 10/10 窗口与 Kafka Schema 交叉校验、Kafka 参与度合规（无生产方、消费组已登记）、错误码 ⊆ 预定义码、错误码→HTTP 与实现一致、统一响应五字段、分页上限与 `BaseRepository` 常量一致、限流继承网关、Redis 缓存键 `cache:scene:{scene_id}`、网关路由/Ingress/K8s 清单三方一致、待确认项结构化
6. **契约校验器同步**：`scripts/verify_data_layer.py` 将 `analytics_result.schema.json` 纳入必检 Schema 集合（含 enum 强制）并把「Schema 待定 Topic」集合收敛为 `sensor_file`/`alert_event`；`common/python/tests/test_kafka_contracts.py` 新增 2 条（analytics_result ↔ 4.5 节实车提取契约、未定稿消息结构必须保持 `schema: null`）
7. **文档更新**：`contracts/openapi/README.md`（scene-service 状态转 ✅ + 场景服务契约要点 + 校验命令）、`contracts/kafka/schemas/README.md`（新增 analytics_result 行与待核对项）、根 `README.md`（接口契约表与验证命令清单）、`services/scene-service/README.md`（契约清单与待确认项）

验证：pytest 全量 **187 passed**（scene-service 33 = 健康探针 4 + 契约 29；既有 156 保持通过）；ruff **All checks passed**（common / services / scripts）；`scripts/verify_data_layer.py` 24 项全通过（消息 JSON Schema 9 个）；`scripts/verify_infra.py` 全部通过

待确认（契约内 `x-hunter-pending-confirmation` 12 项，需人工决策后回填再进入 Step 2）：`scene_type` 编码值（4.2.1 仅给两级结构）、4.4 节第 6/7 步（仿真进度监控/结果保存）REST 端点缺口、`archived` 无归档端点、`spawn_point`/`behavior`/`trigger`/`action` 内部结构、published 是否允许直接删除、导出返回形式（MinIO 预签名 vs XML 文本）与批量语义、`version` 发布时是否递增、限流错误码复用 5001、Carla 管理 API 配置键名与仿真并发/时长上限、实车提取触发源（`analytics_result` vs `event_raw`）、场景模板数据来源、weather 取值范围


## V0.6.0

Date：2026-9-16
变更类型：新增
版本主要有以下功能：
1. **数据采集服务接口契约（L4 Step 1，契约先行）**：新增 `contracts/openapi/data-collector.yaml`（OpenAPI 3.0.3，约 1520 行 / 40+ 组件 Schema）——7 个业务端点 + 3 个运维端点（`/healthz`、`/readyz`、`/metrics`）；⚠ 设计文档 12.2 节未随仓库提供，端点清单由「附录 D 限流端点 + 5.5 上传流程 + events 表列能力」推导，`x-hunter-endpoints.derivation` 逐条登记依据与风险，整体标 `pending_confirmation` 待人工核对
2. **5.3.3 遥测结构落地为查询契约**：`TelemetrySample` + 六段嵌套（`TelemetryChassis`/`Localization`/`Perception`/`Planning`/`Control`/`System`）与 `contracts/kafka/schemas/telemetry.schema.json` 逐字段一致（值域同步：`battery_soc` 0-100、`cpu/gpu_usage` 0-100、`network_rssi` ≤ 0）；`x-hunter-telemetry-query-contract` 机器可读声明扁平列 ↔ 嵌套字段映射、90 天保留、`(vehicle_id, time DESC)` 索引与必填过滤（`vehicle_id` + `start_time`/`end_time`，保护 P95 ≤ 200ms）
3. **事件查询与告警确认契约**：`EventItem` 一一对应 `data_collector.events` 列（派生字段 `data_file_download_url` = 即时签发 15 分钟预签名下载）；`EventType`(18 种)/`EventLevel`(3 级) 取受控词表，`x-hunter-event-contract.level_by_type` 与 ORM `EVENT_LEVEL_BY_TYPE` 逐条一致；确认动作仅取网关注入的 `X-User-Id`（禁止请求体指定）且幂等（已确认重复调用返回当前记录）
4. **5.4 / 5.5 业务规则扩展字段**：`x-hunter-ingest-pipeline`（预处理 6 步：JSON 解析 → 数据校验 → 时间戳对齐 → 数据清洗 → enrichment → 序列化写入；路由 `telemetry_raw`/`telemetry_clean`/`vehicle_telemetry`；DLQ `{topic}.dlq`；≤1s 入库、≥10000 点/秒、单车 ≤100 msg/s）、`x-hunter-file-upload-flow`（6 步：请求上传 → 预签名 3600s（分片 upload_id + parts[]）→ 车辆直传 ≤10 Mbps → 完成通知 → MD5/SHA-256 校验（失败复用 6001）→ 投递 `sensor_file`；命名规范 `{bucket}/{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}.{ext}` 由服务端生成，禁止客户端指定路径）、`x-hunter-kafka`（消费 4 个车端 Topic / 4 个 `data-collector-*` 消费组；生产 `telemetry_raw`/`telemetry_clean`/`event_raw`/`sensor_file`；显式声明不消费 `alert_event`/`analytics_result`/Carla Topic）、`x-hunter-rate-limits`（继承网关五级限流 + `GET /api/v1/data/telemetry` 单用户 20 QPS + 车辆维度 100 msg/s、10 Mbps）、`x-hunter-error-status-map`、`x-hunter-pending-confirmation`（12 项）
5. **Kafka 契约补全（tbd Schema 收敛）**：新增 `contracts/kafka/schemas/sensor_file.schema.json`（draft-07，`additionalProperties: false`）——`bucket` ∈ {hunter-raw-data, hunter-rosbag, hunter-video}、`object_key` 正则强制 5.5 命名规范、`data_type` 枚举、`size_bytes`/`md5`(32 hex)/`sha256`(64 hex)、可选 `seq`/`etag`/`content_type`/`data_file_url`、2 个 examples 自校验；同步 `topics.yaml`（`sensor_file.schema` 由 `null` 改为引用 + 生产者/幂等键说明）
6. **契约一致性测试**：新增 `services/data-collector/app/tests/test_data_collector_contract.py` **30 条**（文件名按服务唯一命名，规避 importlib 模式同名模块冲突）——OpenAPI 3.0.3 合法性、`$ref` 全解析、`operationId` 唯一驼峰、业务端点集合 == 推导清单、运维端点与 FastAPI 实际注册路由一致、遥测六段字段 ↔ Kafka Schema 逐段一致、列映射 ↔ `vehicle_telemetry` DDL 一致、事件枚举/等级映射 ↔ ORM ↔ DDL 一致、`EventItem` ↔ `events` 列一致、确认动作服务端身份校验、5.5 上传流程/命名规范 ↔ `sensor_file` Schema 一致、完整性校验 6001 复用与 HTTP 映射一致、5.4 流程 6 步与 4 组幂等键 ↔ `consumer-groups.yaml` 一致、Kafka 生产/消费双向一致（含 Carla Topic 不得越界）、错误码 ⊆ 预定义集合、统一响应五字段 + 成功响应 `data` 定型、分页 ↔ `BaseRepository` 常量、限流 ↔ 网关/附录 D/`topics.yaml` 三方一致、Redis 键 ↔ 健康消费职责一致、网关路由/Ingress/K8s 清单三方一致、环境变量在 ConfigMap/Secret 中实际接线、待确认项结构化（≥10 项、ID 有序唯一）
7. **K8s 清单对齐修正**：`infra/k8s/services/data-collector.yaml` 的消费者配置由 L0 骨架的错误值（`KAFKA_CONSUMER_GROUP`/`KAFKA_CONSUMER_TOPICS: telemetry_raw,event_raw,sensor_file`）修正为按 `consumer-groups.yaml` 的 4 个消费组 + 4 个订阅模式 + 4 个生产 Topic；补充 MinIO Bucket（raw-data/rosbag/video）、`TELEMETRY_QUERY_MAX_RANGE_HOURS`、`FILE_UPLOAD_MAX_BYTES_PER_SECOND`（10 Mbps = 1310720 B/s）、`VEHICLE_TELEMETRY_MAX_MSG_PER_SEC` 等环境变量
8. **契约校验器与共享库测试同步**：`scripts/verify_data_layer.py` 将 `sensor_file.schema.json` 纳入必检 Schema 集合（含 enum 强制），「Schema 待定 Topic」集合收敛为 `alert_event`；`common/python/tests/test_kafka_contracts.py` 新增 1 条（sensor_file ↔ 5.5 节上传契约：Bucket 枚举 / 命名规范 / 校验值 / 生产者 / 消费幂等键 `object_key` 四处一致）并更新未定稿 Schema 断言
9. **文档更新**：`contracts/openapi/README.md`（data-collector 状态转 ✅ + 数据采集契约要点 + 校验命令）、`contracts/kafka/README.md`（Schema 数量 8→9）、`contracts/kafka/schemas/README.md`（新增 sensor_file 行 + 待核对项收敛）、根 `README.md`（接口契约表 / 消息 Schema 数量 10 个 / 验证命令清单）、`services/data-collector/README.md`（契约清单、关键阈值、待确认 12 项）

验证：pytest 全量 **218 passed**（data-collector 34 = 健康探针 4 + 契约 30；共享库 Kafka 契约新增 1 条；既有用例保持通过）；ruff **All checks passed**（common / services / scripts）；`scripts/verify_data_layer.py` 24 项全通过（消息 JSON Schema 10 个）；`scripts/verify_infra.py` 全部通过

待确认（契约内 `x-hunter-pending-confirmation` 12 项，需人工决策后回填再进入 Step 2）：REST 端点清单来源（12.2 节缺失）、5.5 元信息入库无对应表、`sensor_file` 字段全集、`data_type` 取值域与 Bucket 映射、完整性校验失败错误码复用 6001、车端 REST 接入方式（网关 vs 独立 Ingress mTLS）、遥测查询时间跨度上限与 `total` 计数成本、车辆实时状态端点归属、Carla Topic 未登记、`vehicle_telemetry.seq` 是否入表、`alert_event` 推送归属、上传带宽限流落实方式

## V0.7.0

Date：2026-9-16
变更类型：新增
版本主要有以下功能：
1. **数据分析服务接口契约（L4 Step 1，契约先行）**：新增 `contracts/openapi/data-analytics.yaml`（OpenAPI 3.0.3，约 2200 行 / 45+ 组件 Schema）——8 个业务端点（与设计文档 12.4 节逐条对齐：报告列表/详情/生成、看板、感知评估、控制评估、场景覆盖率、Corner Case）+ 3 个运维端点；服务定位明确为「读模型 + 编排」（实时由 Flink、离线由 Spark 承担，REST 只读预计算结果以保 P95 ≤ 200ms）
2. **6.2 节实时分析契约（`x-hunter-realtime-jobs`）**：5 个 Flink 作业（`vehicle_state_monitor` / `driving_anomaly_detection` / `algorithm_performance_monitor` / `collision_risk_assessment` / `data_quality_monitor`）的输入 Topic+消费组、判定逻辑、输出通道；14 项阈值（急加速/急减速 3.0 m/s² 持续 0.5s、急转弯 0.8 rad/s、超速 1.1 倍、SOC 20%/10%、TTC 1.5s/3.0s、断联 10s、告警延迟 2s、指标窗口 60s）全部声明 `source` 章节并环境变量化；`x-hunter-alert-contract` 给出 18 种告警类型的等级映射（= `EVENT_LEVEL_BY_TYPE`）与「3.0s TTC 预警等级冲突」的处置决策（不新增类型、不放宽等级 → 走 `analytics_result`）
3. **`alert_event` 消息 Schema 补全（Kafka 契约 tbd 归零）**：新增 `contracts/kafka/schemas/alert_event.schema.json`（draft-07，`additionalProperties: false`，3 个 examples 自校验）——`alert_type` = 18 种受控事件类型、`level` 必须 = `EVENT_LEVEL_BY_TYPE[alert_type]`、`source_job` = 5 个实时作业、`rule`（name/metric/comparator/threshold/duration_seconds）回带命中阈值、`metrics`/`location`/`window_*` 为可选观测值；同步 `topics.yaml`（`schema` 由 `null` 改为引用 + `consumed_by` + 幂等键 `(vehicle_id, alert_id)`）→ **平台内部 6 个 Topic 的 Schema 全部落地**
4. **6.3 / 6.4 / 6.5 节离线契约（`x-hunter-offline-jobs` / `corner-case-mining` / `report-templates` / `report-storage`）**：7 个 Spark 作业与 UTC cron（日报 00:30、感知评估 01:00、控制评估 01:30、覆盖率每周一 01:00、挖掘每周一 02:00、月报每月 1 日 03:00）；6.3.3 节控制性能阈值（速度 RMSE < 0.2 m/s、转向 RMSE < 0.02 rad、超调 < 10%、调节时间 < 2s）三处一致（契约/响应 Schema/作业定义）；6.4 节 5 类异常 + Isolation Forest/DBSCAN + 事件前后各 10 秒截取窗口（与 `analytics_result.schema.json` 一致）；6.5 节 5 类报告模板 × 3 格式，产物落 MinIO `hunter-reports`（永久，下载 15 分钟预签名 + Range）
5. **报告生成为异步（契约决策）**：`POST /api/v1/analytics/reports/generate` 返回 `202 + report_id + status=pending`，客户端轮询详情（`pending/generating/ready/failed`）——规避 PDF/HTML（ECharts + Puppeteer）渲染远超 API P95 约束；配套并发保护 `REPORT_GENERATE_MAX_CONCURRENT`（超限 3003）、窗口上限 `REPORT_MAX_RANGE_DAYS`（90 天）、查询跨度上限 `ANALYTICS_QUERY_MAX_RANGE_DAYS`（7 天，超出 2001）
6. **数据访问边界写成契约**：写 `data_analytics.algorithm_metrics`（hypertable 批量 + `ON CONFLICT DO NOTHING`）；只读例外严格限定 `data_collector.vehicle_telemetry`（`hunter_analytics_ro` 账号，er.md 第 3 节）；events/scenes/vehicles 一律经 data-collector / scene-service / vehicle-service REST；**不新增 DB 表、不新增 Redis 键模式**（看板只读既有 `vehicle:online:set` / `vehicle:status:{vehicle_id}`），缺口在 `contracts/database/er.md` 与契约 pending 项中登记
7. **Kafka 消费组契约修正**：`consumer-groups.yaml` 中 data-analytics 由「`telemetry_raw` 订阅 + 声明生产 `telemetry_clean`（与 topics.yaml 生产者 data-collector 冲突）+ 重复消费组 `data-analytics-telemetry-clean`」按 6.2 节作业输入归并为 4 组（`telemetry_clean` 承担 4 作业、`telemetry_raw` 仅数据质量监控、`event_raw`、`sensor_file`），并登记为契约待确认项 #12
8. **契约一致性测试**：新增 `services/data-analytics/app/tests/test_data_analytics_contract.py` **32 条**——OpenAPI 合法性/`$ref` 全解析/`operationId` 唯一、端点集合 == 12.4 节清单、唯一写操作 = 报告生成（202/409/422）、网关前缀 + Ingress + 多副本探针三方一致、统一响应与错误码映射 ↔ `HTTP_STATUS_BY_CODE` 一致、限流继承附录 D（禁止自造接口级 QPS）、5 个实时作业 ↔ 消费组一致、6.2.2 规则名称+等级 ↔ `EVENT_LEVEL_BY_TYPE` 一致、14 项阈值 ↔ ConfigMap 一致、`alert_event` Schema ↔ 告警契约 ↔ topics.yaml 一致、TTC 等级冲突已登记、7 个离线作业 cron ↔ ConfigMap 一致、6.3.3 控制阈值三处一致、感知指标 ↔ 响应 Schema 一致、Corner Case 5 类/算法/10s 窗口 ↔ `analytics_result` 一致、报告模板/异步语义/MinIO 存储与预签名一致、DB 只读例外 ↔ er.md 一致、看板四块与局部降级一致、无越界订阅、环境变量全部接线、待确认项结构化（12 项）
9. **K8s 清单与密钥模板同步**：`infra/k8s/services/data-analytics.yaml` 补齐 4 组消费组/4 个输入 Topic、报告 Bucket 与预签名/窗口/并发、`ANALYTICS_RO_DB_USER`、算法指标窗口、11 项告警阈值、6 个离线作业 cron、3 个依赖服务地址；`infra/k8s/base/02-secret.example.yaml` 新增只读账号口令 `ANALYTICS_RO_DB_PASSWORD`
10. **校验器与共享库测试同步**：`scripts/verify_data_layer.py` 将 `alert_event.schema.json` 纳入必检 Schema（含 enum 强制），「Schema 待定 Topic」集合收敛为**空集**；`common/python/tests/test_kafka_contracts.py` 用「平台内部 Topic Schema 全部已定义」替换原「alert_event 仍为 null」断言，并新增 alert_event ↔ 6.2 节实时作业契约（类型/等级/来源作业/规则结构/消费组幂等键）1 条
11. **文档更新**：`contracts/openapi/README.md`（data-analytics 状态转 ✅ + 数据分析契约要点 + 校验命令 32 项）、`contracts/kafka/README.md`（Schema 数量 9→11 + data-analytics 消费组与契约修正说明）、`contracts/kafka/schemas/README.md`（新增 alert_event 行 + 待核对项收敛）、`contracts/database/er.md`（登记 `data_analytics` 报告/评估结果缺表缺口）、根 `README.md`（接口契约表 / 消息 Schema 11 个 / 验证命令清单 / 数据分析要点）、`services/data-analytics/README.md`（契约清单与章节落位）、`flink-jobs/README.md`、`spark-jobs/README.md`（作业清单指向契约）

验证：pytest 全量 **251 passed**（data-analytics 36 = 健康探针 4 + 契约 32；共享库 Kafka 契约净增 2 条；既有用例保持通过）；ruff **All checks passed**（common / services / scripts）；`scripts/verify_data_layer.py` 24 项全通过（消息 JSON Schema 11 个，无 `schema: null`）；`scripts/verify_infra.py` 全部通过

待确认（契约内 `x-hunter-pending-confirmation` 12 项，需人工决策后回填再进入 Step 2）：报告/评估/Corner Case 元信息无 DB 表、报告异步语义与状态枚举、6.2.3 节 TTC<3.0s 预警与受控词表等级冲突、`alert_event` 消费方（告警推送）落位服务、6.2.5 节数据质量阈值与结果落位、算法指标统计窗口与感知评估真值来源、Corner Case 特征窗口与 Isolation Forest/DBSCAN 参数、看板车队状态读取（Redis）归属、12.4 节缺规划质量/数据质量端点、看板遥测入库延迟数据来源、分析查询跨度上限与看板缓存键、data-analytics 消费组契约修正

## V0.8.0

Date：2026-9-17
变更类型：新增
版本主要有以下功能：
1. **OTA 管理服务接口契约（L5 Step 1，契约先行）**：新增 `contracts/openapi/ota-service.yaml`（OpenAPI 3.0.3）——**15 个业务端点**（版本仓库：列表 / 创建草稿 / 详情 / 发布 / 废弃；升级任务：列表 / 创建 / 详情 / start / pause / resume / cancel / rollback；升级记录：任务维度 / 车辆维度）+ 3 个运维端点（`/healthz`、`/readyz`、`/metrics`）；因设计文档 §12.5 原文未随仓库提供，端点清单为**推导集合**，推导依据（8 章升级流程 + `ota_*` 三表列能力 + 附录 D 限流端点）逐条登记于 `x-hunter-endpoints.items`，并在 pending 项 #1 标记待核对
2. **版本上传两步式契约（`x-hunter-version-upload-flow`）**：`POST /versions` 建草稿并返回服务端生成的 MinIO 对象键 `hunter-core/ota/{model}/{version_name}/{version_code}/package.tar.gz` 与 **1 小时**上传预签名（客户端直传，禁止指定对象键）→ `POST /versions/{id}/publish` 由服务端**单次流式**校验：包长/MD5（→ 6001）→ SHA-256（→ 6001）→ RSA-2048 验签（`RSASSA-PKCS1-v1_5 + SHA-256`，→ 6002）→ `version_code` 同 `applicable_models` 范围单调递增（→ 6003）→ `draft → published` + 写 `release_time`；下载预签名 **15 分钟 + Range**
3. **灰度发布契约（`x-hunter-canary-rollout`）**：4 批 `5% → 20% → 50% → 100%`、每批观察 24h、成功率 ≥ `OTA_CANARY_MIN_SUCCESS_RATE`(0.95) 才推进（成功率定义 = `SUCCESS / (SUCCESS + FAILED + ROLLED_BACK)`）；< 0.95 立即 `paused` + 投递告警 `alert_event`（人工只可 rollback / cancel，禁止在未达阈值时 resume 推进）；批次推进以 PG 行锁串行化（防并发跳批，pending #3）；小车队批次取整规则（≥1 台且去重、末批补全）已登记 pending #14
4. **升级门禁与任务状态机**：门禁 `电量 ≥50% / 静止(P 档) / 网络稳定(遥测间隔 ≤ OTA_OFFLINE_THRESHOLD_SECONDS) / 存储 ≥2GB`，不满足车辆在 `start` 响应 `blocked[]` 中返回并附 `reason`；任务状态机 `draft → pending_approval → running/observing → paused → completed/rolled_back/cancelled`；车端状态机沿用 `IDLE → PENDING → DOWNLOAD → INSTALL → TEST → SUCCESS`（自检失败 → `ROLLBACK → ROLLED_BACK / FAILED`）
5. **回滚语义界定（pending #5 / #12）**：平台侧仅依据 `ota_records` 下发 `ota_rollback` 指令（车端 A/B 分区 + TEST 自检失败自动回退为兜底）；**仅允许 `previous_slot`**，不支持任意历史版本；**不消费 `command_result`**（车端通过 `ota_status` 推进记录状态）、**不生产 `broadcast.command`**（逐车下发），两项越界变更均需先改契约
6. **Kafka 参与度与消费语义**：消费 `hunter.*.ota_status`（消费组 `ota-service-ota-status`，**正则订阅**、手动提交 offset、处理成功后提交、异常进 DLQ `{original_topic}.dlq`，幂等键 `(vehicle_id, task_id, phase)`）；生产 `hunter.{vehicle_id}.ota_notify`（逐车下发，载荷含 1 小时预签名 URL 与门禁条件）与 `hunter.{vehicle_id}.command`（仅 `ota_rollback`）；车端连接强制 SASL_SSL + SCRAM-SHA-512（禁止 PLAINTEXT），消息 key = `vehicle_id`（单车有序）；`contracts/kafka/topics.yaml` 的 `hunter.broadcast.command` 上方补注释，说明 `producer` 为「设计预期生产者登记」而非实现现状（api-gateway 契约声明不生产任何 Topic，本服务待确认 #12 前不生产），避免 `topics.yaml` 与两份 OpenAPI 契约的读法歧义
7. **数据访问边界写成契约**：仅写 `ota_svc.ota_versions` / `ota_tasks` / `ota_records`（**禁止 DELETE**，废弃以状态字段表达，保证审计可追溯）；不跨 schema 直连（vehicle-service REST 亦不调用，pending #18）；不新增 Redis 键模式——写 `ota:progress:{task_id}`（任务结束 + 1 天过期），只读 `vehicle:status:{vehicle_id}` / `vehicle:online:set`；MinIO 仅 `hunter-ota-packages`（永久保留 + SSE-S3）
8. **机器可读扩展字段体系（`x-hunter-*`）**：`service`（DB schema/表、跨服务策略、MinIO Bucket 与生命周期、Redis 键、依赖服务、`config_source` + `required_env` + `env_derived` + `env_defaults` + `env_notes` + `forbidden_hardcode`、性能例外）、`kafka`（消费/生产/安全/幂等）、`version-upload-flow`、`canary-rollout`、`ota-security`（校验顺序与算法套件）、`error-status-map`（错误码 → HTTP 状态，与 `app/core/error_handlers.py:HTTP_STATUS_BY_CODE` 完全一致）、`endpoints`（15 + 3 推导清单）、`pending-confirmation`（**19 项**，结构化 `id / question / contract_decision / impact`）
9. **契约内部一致性修正**：环境变量统一为下划线命名（`OTA_CANARY_SUCCESS_RATE_THRESHOLD` → `OTA_CANARY_MIN_SUCCESS_RATE`）；`OTA_PRECONDITION_MIN_STORAGE_GB` → **`OTA_PRECONDITION_MIN_STORAGE_MB`(2048)**，消除与 Kafka `ota_notify.preconditions.min_storage_mb`、`ota_tasks.preconditions.min_storage_mb` 的单位换算歧义；`OTA_OFFLINE_FALLBACK_ENABLED` 默认值由 `true` 收紧为 **`false`（缺门禁读模型数据即拒绝，安全默认，与 K8s 生产 ConfigMap 一致）**；`env_notes` 由「键名含 `/` 的映射」重构为 `[{env, note}]` 列表（可机检、可逐条定位）；补齐缺失端点 `POST /api/v1/ota/versions/{version_id}/publish`（上传流程门禁的落点，此前只有流程描述无端点）
10. **K8s 清单与密钥模板同步**：`infra/k8s/services/ota-service.yaml` ConfigMap 由 `OTA_PRECONDITION_MIN_STORAGE_GB` 改为 `OTA_PRECONDITION_MIN_STORAGE_MB: "2048"`，并补齐 `PRESIGNED_UPLOAD_EXPIRE_SECONDS`(3600) / `PRESIGNED_DOWNLOAD_EXPIRE_SECONDS`(900) / `OTA_ROLLOUT_TICK_SECONDS`(60) / `OTA_OFFLINE_THRESHOLD_SECONDS`(10) / `OTA_OFFLINE_FALLBACK_ENABLED`（生产 **false**）/ `OTA_TASK_MAX_TARGET_VEHICLES`(1000)；`infra/k8s/base/02-secret.example.yaml` 新增 `OTA_SIGNATURE_PUBLIC_KEY`（RSA-2048 验签公钥，私钥仅存发布方 CI/KMS）
11. **文档更新**：`contracts/openapi/README.md`（ota-service 状态转 ✅ + 「OTA 管理契约要点」章节 + 校验命令）、根 `README.md`（接口契约表拆分 ota/remote 两行、OTA 管理服务要点、待确认项补充 19 项）、`services/ota-service/README.md`（15 端点清单 + 章节落位 + 数据访问边界 + 契约测试命令）

验证：契约校验脚本（YAML 解析 / `$ref` 全解析 / `operationId` 唯一 / 端点声明与实际一致 / pending 项字段结构 / 编号引用完整性 / 命名遗留扫描）全部通过；`scripts/verify_infra.py`、`scripts/verify_data_layer.py` 保持通过；见下方「待确认」。

待确认（契约内 `x-hunter-pending-confirmation` **19 项**，需人工决策后回填再进入 Step 2）：§12.5 端点清单来源、`release_type`/`status` 取值域、是否允许跳批、分片/断点续传语义、回滚目标槽位、观察窗口顺延、`ota_notify` 到达时延目标、车端是否直连 REST、`publish` 网关超时（≥300s）与异步化、`publish` 限流建议值、DLQ Topic 是否登记为正式 Topic、`command_result` 与 `broadcast.command` 归属、升级包大小上限、小车队批次取整、审计日志保留期（30 天 vs 永久）、紧急全量发布通道、安全校验开关生产禁用、vehicle-service 依赖、门禁读模型缺失时的放行策略




## V0.9.0

Date：2026-9-17
变更类型：新增
版本主要有以下功能：
1. **远程操控服务接口契约（L6 Step 1，契约先行）**：新增 `contracts/openapi/remote-control.yaml`（OpenAPI 3.0.3）——**8 个业务端点** + **2 条 WebSocket 契约** + 3 个运维端点（`/healthz`、`/readyz`、`/metrics`）；因设计文档 §12.6 / §8.3–8.5 原文未随仓库提供，端点清单为**推导集合**，推导依据逐条登记于 `x-hunter-endpoints.items[].source`（附录 D 明列 `POST /api/v1/remote/session`、网关 `x-hunter-websocket-routes` 的 `/ws/remote/**`、第 15 条安全约束、MinIO `hunter-video` 录像），并在 pending #1 标记待核对
2. **第 15 条安全约束固化为机器可读契约（`x-hunter-control-safety` / `x-hunter-video-contract`）**：指令 **20Hz（50ms）** → Kafka `hunter.{vehicle_id}.remote_control`（`acks=all` / `key=vehicle_id` / lz4）；**>500ms 未收到指令车端自动减速停车**（执行主体为车端，平台侧仅会话降级，禁止在服务端伪造停车指令）；速度上限 **2.0 m/s**（`RC_MAX_SPEED_MPS`，服务端对 `target_velocity` 限幅）；**同车同一时间仅一名操作员**（Redis 分布式锁 `rc:lock:{vehicle_id}` → 错误码 **7001**）；视频 **H.264（AGX Orin NVENC）/ 720p@30fps / 2048–4096 kbps / 关键帧 1s**；延迟预算「采集编码 ≤50ms + 网络 ≤100ms + 解码渲染 ≤30ms」= 视频 E2E ≤200ms、指令 ≤100ms；`timing_guard` 与 `quality_guard` 的降级动作（先降码率、再降帧率、最后断流）逐档登记
3. **会话生命周期与心跳契约（`x-hunter-session-lifecycle` / `x-hunter-session-heartbeat`）**：状态机 `connecting → active → degraded → ended`；心跳 10s（不可放宽）、连续缺失 3 次降级、60s 超时结束（后两项为派生阈值，pending #4）；会话结束收敛顺序 **session_end 下发 → 停录制 → 封存 sidecar → 释放锁 → 删 Redis 键**（不依赖车端回执，副本崩溃后可自愈，残留窗口见 pending #5）；RBAC 动作 `remote:read / remote:create / remote:execute` 与端点逐条绑定
4. **WebSocket 契约（`x-hunter-websocket-contract` / `x-hunter-websocket-routes`）**：`/ws/remote/{session_id}/control`（指令上行 + `ack`/`status`/`error` 下行 + 10s 心跳）与 `/ws/remote/{session_id}/signal`（SDP/ICE 中继，服务端不解析媒体）；WSS + JWT 握手校验，1001/1003/1002/3001/7001 直接以 HTTP 4xx 拒绝（不进帧循环）；帧格式、close code、上下行流控与网关 `x-hunter-websocket-routes` 镜像一致；**媒体面 SRTP/UDP 不经 api-gateway/Ingress**，须在 K8s 单独暴露 UDP（pending #10/#12）
5. **操控记录方案 A（用户已确认，`x-hunter-history-archive`）**：**不新增数据库表 / ORM / Alembic（`contracts/database/` 与 `common/python/alembic` 本次零改动）** —— 会话态唯一来源 Redis `rc:session:{vehicle_id}`；操控记录 = MinIO `hunter-video` 录像 `remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4` + 同目录 sidecar JSON（保留 90 天，与 Bucket 生命周期一致，sidecar schema 已写入契约）；代价显式登记：历史查询无 SQL 过滤/聚合能力（须按日期前缀收敛，`RC_HISTORY_QUERY_MAX_RANGE_DAYS` 默认 31，超限 2001）、`total` 非 DB COUNT、跨会话统计报表不可用、数据权限靠应用层判断
6. **Kafka 契约零改动核验**：`contracts/kafka/topics.yaml` 已登记 `hunter.{vehicle_id}.remote_control`（partitions 3 / acks all / key `vehicle_id` / 20Hz / producer `remote-control` / schema `remote_control.schema.json`）与 `hunter.{vehicle_id}.command`（producers 含 remote-control）；`consumer-groups.yaml` 已登记消费组 `remote-control-command-result`（订阅 `hunter.*.command_result`、regex、`produces: []`、幂等键 `command_id`）——本次**未改动** `contracts/kafka/` 任何文件（生产者单例、手动提交 offset、异常进 DLQ `{topic}.dlq` 约束在契约中声明）
7. **机器可读扩展字段体系（`x-hunter-*`，15 个顶层扩展）**：`service`（端口 8085 / 无业务表 / Redis 键 / 依赖与不可用语义 / `config_source` + `required_env` + `env_to_add`）、`endpoints`（8 + 2 WS + 3 运维清单，含 RBAC / 限流 / 推导来源）、`session-lifecycle`、`websocket-contract`、`session-heartbeat`、`control-channel`（topic/消息映射/ack/timing_guard/session_signalling）、`control-safety`、`video-contract`、`history-archive`、`websocket-routes`、`kafka`、`rate-limits`（全局限流 + `POST /remote/session` 1 QPS、`GET /data/telemetry` 类端点维度）、`error-status-map`（错误码 → HTTP 状态全表，含 4001/4002/7001/7002）、`observability`（structlog / 指标 / 探针 / 告警）、`pending-confirmation`（**20 项**，`blocking` **8 项**）
8. **契约内部一致性修正**：视频稳态码率区间统一为 **2048–4096 kbps**（与 `x-hunter-video-contract` 一致）；会话结束原因 `video_failed` → **`video_lost`**（对齐设计文档 15 节语义与 `SessionEndReason`）；WS `ack` 帧说明更正为「按 `(session_id, seq)` 近似关联」（因 `remote_control.schema.json` 无 `command_id`，见 pending #16）；指标名 `hunter_remote_control_command_latency_ms` → **`hunter_remote_control_command_latency_seconds`**（对齐 Prometheus Histogram 命名与共享库 `MetricsMiddleware` 的 `hunter_` 前缀约定）；`timeout_events` 描述中裸 `>` 加引号修复 YAML 解析；pending 项字段结构补齐（#5 / #6 / #7 / #8 / #19 补 `files`），全部 20 项具备 `title/status/blocking` 且逐项可定位
9. **文档更新**：`contracts/openapi/README.md`（remote-control 状态转 ✅ + 「远程操控契约要点」章节 + 契约静态校验命令 + 滚动校验命令清单）、根 `README.md`（接口契约表 remote-control 行转 ✅、新增「远程操控服务」要点、待确认项补充 20 项 / 8 blocking、验证命令清单新增 2 行）、`services/remote-control/README.md`（10 端点清单 + 章节落位 + 数据访问边界 + 待确认 20 项）

验证：契约静态校验通过 —— PyYAML 解析 OK（`openapi: 3.0.3`）、**10 条 path = 13 个 operation**（8 业务 + 2 WS + 3 运维，`operationId` 13 个全局唯一）、**内部 `$ref` 无悬空、无外部引用**、11 个 JSON Schema（含 `command` / `command_result` / `remote_control`）均可解析；Kafka 三方登记核验（`topics.yaml` 生产者/消费者列表、`consumer-groups.yaml` 消费组与幂等键）一致；`contracts/kafka/`、`contracts/database/`、`infra/` 本次零改动

待确认（契约内 `x-hunter-pending-confirmation` **20 项**，其中 `blocking=true` **8 项**，需人工决策后回填再进入 Step 2）：**#2** Redis `vehicle:status:{vehicle_id}` Hash 字段清单（须与 vehicle-service 写入方一致）、**#10** SRS 应用名/流名规范与 WHIP/WHEP 端点命名、**#12** 新引入配置键需同步 `infra/k8s`（`RC_SESSION_HEARTBEAT_INTERVAL_S` / `RC_SESSION_HEARTBEAT_TIMEOUT_S` / `RC_SESSION_DEGRADED_MISSES` / `RC_HISTORY_QUERY_MAX_RANGE_DAYS` / `RC_ACK_TIMEOUT_MS` / `RC_COMMAND_RESULT_TOPIC_CONSUMER_GROUP` / `SRS_*` / `RC_STUN_URLS` / `RC_TURN_*` / `RC_ICE_CREDENTIAL_TTL_S` / `RC_PRESIGNED_DOWNLOAD_EXPIRE_SECONDS`）、**#14** 多副本下 WS 连接故障转移与会话续期（Ingress 会话保持 / 粘性路由）、**#15** WS 子路径 `/control` 与 `/signal` 是否拆分、**#16** 操控指令 `command_id` 缺失导致回执只能近似关联（三选一：新增字段 / 接受近似 / 改用 command topic）、**#17** 会话信令 `command_type` 取值（`rc_session_start` / `rc_session_end`）待设计文档 5.3 节定稿、**#20** WS 握手 JWT 传送方式（Authorization 头 / 子协议 / 首帧）；非阻塞项 #1 方案 A 能力边界、#3 `block_reason` 值域、#4 状态名与心跳派生阈值、#5 Redis 残留窗口与硬 TTL、#6 多副本统计聚合、#7 查询跨度默认值、#8 sidecar 检索成本、#9 `/readyz` 是否保留 database 项、#11 `rc:lock:{vehicle_id}` 键命名登记、#13 车辆读模型写入方归属、#18 `target_steer` 限幅与底盘转角量程、#19 H.264 profile


## V1.0.0

Date：2026-9-17
变更类型：新增
版本主要有以下功能：
1. **Redis Key 契约（设计文档 9.5 节 + 系统关键约束第 8 条）**：新增 `contracts/database/redis-keys.yaml`——**8 个受控键**（约束第 8 条 7 个 + 实现派生键 `rc:lock:{vehicle_id}`），逐键登记 类型 / TTL / 字段结构（`fields_draft`）/ 失效路径 `lifecycle` / `writers` / `readers` / `ops` / `owner_service` / 跨服务引用；命名规则 `<domain>:<entity>[:<qualifier>]`（全小写 + 冒号分隔 + 花括号占位符，`db_index=0`）；`cross_check.contracts` 固化 **6 份服务 OpenAPI** 为声明文件清单，避免「契约未登记键被实现侧发明」
2. **对象存储契约（设计文档 9.4.1 / 9.4.2 节 + 约束第 7、9 条）**：新增 `contracts/database/object-storage.yaml`——**7 个 Bucket**（名称 / 生命周期 `mode=expire|permanent|mixed` / SSE-S3 / 读写方 / 对象键约定）：`hunter-rosbag` 前缀级规则（`regular/` 30 天、`events/` 永久）、`hunter-raw-data`/`hunter-logs` 30 天、`hunter-video` 90 天、`hunter-ota-packages`/`hunter-reports`/`hunter-scene-assets` 永久；预签名策略（上传 3600s / 下载 900s / Range 分片 / URL 禁止落日志 / 下载鉴权规则）；对象键规则（禁止 `..`、前导 `/`、控制字符等）
3. **契约校验器扩展（校验 9、10）**：`scripts/verify_data_layer.py` 由 24 项扩至 **26 项**——校验 9「Redis Key 契约」：8 键 ↔ 6 服务 `x-hunter-service.redis_keys` 声明逐项比对键模式 / 类型 / TTL / 读写方（兼容「对象列表」与 `{read: [...]}` 两种既有声明形态，见 pending #8）；校验 10「对象存储契约」：7 Bucket ↔ `infra/docker/minio/init-buckets.sh` ↔ `infra/k8s/jobs/minio-init-job.yaml` ↔ 5 份服务契约（`create_bucket` 集合 / `add_expiry` 天数 / `encrypt_bucket` SSE 覆盖 / 预签名 TTL 允许值集合）
4. **单元测试**：新增 `common/python/tests/test_storage_contracts.py`（**9 条**）——契约基准值（键 / 类型 / TTL / Bucket / 生命周期 / SSE / 预签名）与系统约束逐项对齐、键命名规范、失效路径与归属完整性（无 TTL 必须显式 `DEL`/`SREM`）、两种服务声明形态归一化与未登记键检出、MinIO 初始化脚本实测解析（`create_bucket` / `add_expiry` / `encrypt_bucket`）、各服务 Bucket 声明 ⊆ 契约且生命周期文本含契约天数、pending 项结构完整性（`{id, question, impact, contract_decision}`）
5. **文档更新**：`contracts/database/README.md`（新增校验命令与 pending 项提示）、根 `README.md`（数据层契约表新增 Redis Key / MinIO 两行、校验项 24 → 26、验证命令新增存储契约测试）、`common/python/README.md`（契约一致性测试清单）

验证：`python scripts/verify_data_layer.py` **26 项全通过**（含新增校验 9/10）；`pytest -q` 全量 **260 passed**（共享库 116，含新增 9 条）；`ruff check` **All checks passed**

待确认（两份契约 `x-hunter-pending-confirmation` 共 **15 项**，其中标注阻塞 **8 项**，需人工决策后回填再进入存储层实现）：Redis —— **#1** `vehicle:status:{vehicle_id}` Hash 字段清单（当前 `fields_draft` 12 字段）、**#2** 该 Hash 写入方归属（模块表无 vehicle-service）、**#3** `rc:lock:{vehicle_id}` 为 redis-py `client.lock` 派生键、约束第 8 条未登记、**#6** 网关登出 Token 黑名单键模式缺失（登出后旧 Token 在有效期内仍可调用业务接口，安全缺陷）；对象存储 —— **#1** 对象键是否含 Bucket 名前缀（data-collector / data-analytics 现有模板不一致）、**#2** `hunter-rosbag` `regular/` 前缀与 5.5 节命名规范冲突（**按现行命名上传的常规 rosbag 不匹配任何过期规则 → 永久占用存储**）、**#5** 分片上传 `part_size_bytes` 与单对象大小上限未定、**#6** MinIO 配置项未进入 `hunter_common.config`；非阻塞项：Redis #4（`rc:session:{vehicle_id}` 无硬 TTL）、#5（`vehicle:online:set` 进程崩溃残留）、#7（键构造常量模块位置未定）、#8（服务声明结构不统一）、对象存储 #3（`hunter-logs` 键模板为契约推导）、#4（日志归档写入方归属）、#7（共享库缺 MinIO 客户端封装）

## V1.1.0

Date：2026-9-17
变更类型：新增
版本主要有以下功能：

1. **前端工程与构建配置**：`frontend/`（Vue 3 + TypeScript + Vite 5 + Element Plus + Pinia + Vue Router 4 + ECharts 5 + Three.js）——`package.json`（dev/build/type-check/preview 脚本）、`vite.config.ts`（开发代理 `/api`→网关 8080、`/ws` 启用 `ws: true`；生产 manualChunks 拆分 vue/element-plus/echarts/three）、`tsconfig.json`（strict + noUnusedLocals + `@/*` 别名）、`index.html`、`src/env.d.ts`（VITE_* 变量类型声明）、`.env.development` / `.env.production` / `.env.example`（地址、轮询间隔、RC 限幅提示全部环境变量化）、`src/styles/global.css`
2. **契约类型层（`src/types/`）**：`common`（ApiResponse/ErrorCode 联合/PageData/HTTP↔业务码映射）、`user`（LoginRequest/TokenPair/UserProfile/AuthSession）、`scene`（Scene/SceneConfig：map·ego_vehicle·weather·actors·events·success_criteria·duration）、`data`（遥测 6 子结构 + events 18 类型 + 文件预签名）、`analytics`（看板四维可用性 + 感知/控制/覆盖/Corner Case/报告）、`ota`（版本/任务/灰度 4 批/门禁/记录状态机）、`remote`（会话/视频/控制通道/心跳/WebRTC/归档 + WS 上行下行与信令帧 + 关闭码）；字段名严格 snake_case，**不做驼峰转换**
3. **契约偏差核实与修正**：`VEHICLE_ID_PATTERN` 校正为 `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`（= 设备证书 CommonName）；补 `SCENE_EXPORT_FORMAT_LABELS`（carla_scenariorunner_xml / openscenario_1_2）；`OtaReleaseType` 契约未定义 enum（pending #2）→ 类型置 `string` 且仅原样展示；评估类数据块按契约**移除臆造的 `available/reason`**（保留 `data_source/updated_at/job_name` 溯源字段），control `metrics`/`thresholds` 收敛为 6.3.3 节固定 4 键（velocity_rmse_ms / steering_rmse_rad / overshoot_percent / settling_time_s）
4. **常量层**：受控词表与展示映射（车辆 8 态、事件 18 类型 + 阈值文案、场景 18 类型/状态、仿真状态、OTA 7 态任务 + 9 态升级状态机 + 4 批灰度（5/20/50/100、24h、95%）+ 门禁、报告/Corner Case、会话状态/结束原因/不可操控原因/降级原因）；`constants/permissions.ts` 登记权限编码（`scene:*`、`data:read`、`analytics:read|execute`、`ota:read|create|execute`、`remote:read|create|execute`，逐条溯源各服务契约 RBAC 说明）
5. **基建**：`api/request.ts`（统一响应解析、`code≠0` 抛 `HunterApiError`、`1003` **单飞静默刷新后重放一次**、`1001` 清会话并广播 `hunter:session-expired`、`X-Request-ID` trace 注入、预签名直传 `putToPresignedUrl`）；7 个模块 API（`user/scene/data/analytics/ota/remote`，端点与契约逐条对齐，含 `analytics/*/eval`、`scene/coverage`、`events/{id}/acknowledge` 等实际路径校正）；`utils`（storage 会话持久化、format 时间/字节/百分比、error-code 附录 A 文案、websocket 指数退避重连 + 心跳 + 终态关闭码不重连、webrtc 原生 recvonly + 信令中继 + getStats 统计、hash 本地 MD5/SHA-256 + 下载）；Pinia（`user` 会话/RBAC/失效监听、`vehicle` Redis 读模型 + 名称解析）；`usePolling`（页面隐藏暂停、防请求堆积）；`v-permission` 指令 + 路由守卫（登录态 + `meta.permission`）
6. **五大模块页面**：运营看板（车队/管道/算法/事件四维 + 降级提示 + 8 态分布与事件 Top10 + 车辆明细跳转操控台）；场景库（列表/筛选/分页、新建（套模板）、复制、发布、删除、批量导出、下发 Carla 仿真、详情页含 Three.js 3D 预览与 config_json 表单化编辑）；数据分析（感知评估、控制阈值判定、场景覆盖率热力图 + 未覆盖清单、Corner Case 挖掘（算法/类别/最小异常分过滤）、报告生成（202 受理 + 5s 轮询 + 产物下载））；OTA 管理（版本仓库：本地算摘要 → 分片上传 → 发布校验结果展示（6001/6002）→ 弃用；任务：4 批灰度推进（next_action/halt 提示、批次进度条）、门禁预检与放行/阻塞明细、暂停/继续/取消/回滚（A/B 分区）结果明细、单车升级记录时间轴）；远程操控（可操控车辆列表与不可操控原因、建会话、原生 WebRTC 视频 + 链路统计、20Hz 控制面板（速度/转角/挡位/键盘 W/S/A/D/空格急停）、降级告警条、实时遥测、会话信息（视频/控制/心跳/归档）、操控记录（sidecar 详情抽屉 + 录像预签名回放 + 31 天跨度守卫））；事件与文件（事件筛选/确认/数据文件下载、MinIO 文件浏览游标分页）
7. **文档更新**：根 `README.md` 新增「前端管理后台（L4）」节（页面↔契约端点映射、限流/权限/安全要点、已知折衷）并更新目录结构、前置要求、启动顺序与验证命令清单（新增 type-check/build/dev 三行）；`frontend/README.md`（快速开始、环境变量表、目录结构、页面映射、实现约定、前端待确认项）；新增 `docs/frontend-portal.md`（设计原则、目录职责、鉴权与权限模型、远程操控实现要点、回归清单、pending 对齐表）

验证：`npm run type-check`（vue-tsc 严格模式）**0 error**；`npx vite build` **构建成功**（2322 modules，产物含 `vue` 114KB / `element-plus` 1.09MB / `echarts` 1.03MB / `three` 469KB 分包，gzip 后分别 44.6/341/343/118 KB）

待确认（前端侧，均已在 `frontend/README.md` 与 `docs/frontend-portal.md` 登记）：`OtaReleaseType` 无 enum（pending #2，前端仅原样展示）、`RC_MAX_STEER_RAD` 底盘转角量程（pending #18，前端仅 UI 归一化提示，真实限幅服务端执行）、WS 握手 JWT 传送方式（pending #20，当前用子协议 `hunter-jwt`，禁止查询串明文携带 Token）、SRS 应用名/流名与 WHIP/WHEP 端点（pending #10，信令地址优先取会话响应）、OTA 分片上传 complete 端点未在契约暴露（前端仅上传分片，汇总在 publish 阶段由服务端处理）、场景事件 `trigger.condition` / `action.action_type` 取值域未定（表单字符串透传）、Token 存 `localStorage`（无 HttpOnly Cookie 端点契约的折衷，需安全评审）



## V1.2.0

Date：2026-9-17
变更类型：新增
版本主要有以下功能：

1. **L5 测试层（testcontainers 集成测试）**：`tests/integration/test_11_1_telemetry_pipeline.py` 全量重写——车端/平台 Topic 分区契约、retention.ms 回读比对、produce→consume 字节级往返（key=vehicle_id + Schema 校验）、消息→DDL 列覆盖、`test_telemetry_persisted_within_latency_budget`（入库延迟 ≤1s）、`test_telemetry_insert_is_idempotent`（PK(time,vehicle_id) 幂等）、`test_hypertable_config_matches_contract`（分块 1 day/保留 90 天）、`test_telemetry_columns_match_ddl`、`test_sequence_gap_detection_matches_reference`；`tests/support/broker.py` 新增 `topic_config`（AdminClient.describe_configs，broker 默认值不返回以避免误判契约漂移），并清理历史重复定义
2. **11.2 OTA 端到端**：`tests/e2e/test_11_2_ota_flow.py`——版本单调递增防回滚（6003）、完整性先于签名（SHA-256/MD5→6001、RSA→6002）、升级门禁逐项（电量≥50%/P 档/网络/存储≥2GB）、车端状态机合法流转与 TEST→ROLLBACK→ROLLED_BACK/FAILED 路径及非法跳跃拒绝、灰度 5/20/50/100 批次与 95% 成功率门槛（恰 19/20 通过、94/100 暂停）、进度汇总口径、ota_notify/ota_status Schema 校验；容器链路：Kafka 下发→状态上报往返、Redis `ota:progress` Hash TTL=86400、MinIO 预签名上传(1h)/下载(15min) SHA-256 一致、`hunter-ota-packages` 永久保留断言
3. **11.3 远程操控端到端**：`tests/e2e/test_11_3_remote_control_flow.py`——互斥（第二操作员 7001，Redis SET NX 实证）、离线 4001/忙 4002/同操作员重入幂等、限幅 ±2.0 m/s、20Hz 节奏（50ms 合规/30ms 超频拒绝）、500ms 超时→减速停车安全动作、会话关闭释放锁、车辆状态 remote_controlled∈契约枚举、视频参数（720p@30fps/2-4Mbps/关键帧 1s）与 `/ws/remote/**` 网关契约（含 7001/7002 错误码）；容器链路：Kafka 20Hz 指令流往返（顺序一致、延迟 ≤100ms）、Redis 互斥锁(30s TTL)/会话 Hash/状态回写
4. **性能基准套件**：`tests/performance/test_benchmarks.py`——API P95 ≤200ms（/healthz 300 次+预热，实测 8.11ms PASS 入报告）、遥测入库延迟 ≤1s、时序写入 ≥10000 点/s（executemany 批量 50000 点）、Kafka 生产吞吐回归下限（契约仅约束单车 ≤100 msg/s 上限，平台侧下限经 `HUNTER_BENCH_KAFKA_MIN_MSGS_PER_S` 可调）；指标经 `l5_report.add_perf` 登记（含阈值/方向/来源），供 CI 门槛判定
5. **测试层自检与报告闭环**：新增 `scripts/verify_test_layer.py`（pytest.ini 标记 ↔ report.MARKERS/EXTRA_MARKERS 双向一致、thresholds.SOURCES 登记完整性、report-template.md 占位符 ↔ report.py 渲染键一一对应、l5_case 用例号白名单 11.1/11.2/11.3/PERF）；`pytest.ini` 标记描述与 report.py 对齐（以 report.py 为单一事实来源）；`--l5-report` 实测渲染完整（96 用例：79 pass/17 skip + API P95 指标）
6. **GitLab CI**：新增 `.gitlab-ci.yml`（lint→unit→integration→e2e→performance→report 六阶段；integration/e2e/performance 采用 dind + testcontainers；`--junitxml` 产出 junit；report 阶段解析 `docs/test-reports/l5-results.json` 汇总性能判定；基准规模经 HUNTER_BENCH_* 环境变量调整）；lint 阶段 mypy 收窄至 L5 测试层（`--ignore-missing-imports --disable-error-code=import-untyped`，存量 stub 缺失不阻塞、真实类型错误仍拦截）
7. **存量 lint 清理**：tests/support（noqa 失配 RUF100、`lru_cache(maxsize=None)`→`functools.cache`、`items()`→`values()`、SigV4 canonical_request 改 f-string 拼接、`zip`→`itertools.pairwise`、SIM102 合并、未用导入）与 conftest.py（导入名与夹具参数遮蔽 F811，以显式引用清单消除）；`ruff check tests scripts` 全绿

验证：`python -m pytest tests -q` **79 passed / 17 skipped**（无 Docker 环境容器型用例 skip，0 fail；含 6 服务 uvicorn 进程实测 56 项契约面用例）；`python -m ruff check tests scripts` **All checks passed**；`python -m mypy --ignore-missing-imports --disable-error-code=import-untyped tests/support scripts/verify_test_layer.py` **0 error**；`python scripts/verify_test_layer.py` **4 项全 OK**；`--l5-report` 报告渲染闭环验证通过（docs/test-reports/，API P95=8.11ms PASS）

风险与待人工确认：① 本机无 Docker——容器型 17 项 skip（Kafka 往返/TimescaleDB 落库/Redis 键/MinIO 预签名/性能基准的容器分支），需在具备 Docker 的 runner 上跑完整套件确认；② `tests/support/flow.py` 的 11.1/11.2/11.3 参考实现基于系统关键约束+contracts/ 推导（详细设计文档 V4.0 原文未入库），流程断言与文档最终口径需人工核对；③ CI 的 dind runner 需预置（DOCKER_HOST=docker:2375、无需 TLS），Kafka 镜像拉取与 50000 点写入在低配 runner 上可能超时，可用 HUNTER_BENCH_* 降规模

## V1.3.0

Date：2026-9-18
变更类型：新增

1. **api-gateway 统一认证实现（契约 paths 全量落地）**：`POST /api/v1/user/login`（bcrypt 校验 + 可选 MFA(TOTP) 载荷 + 登录频率限制 + IP 失败锁定，失败统一 1001 防用户名枚举，成功写 Redis `session:{user_id}`（TTL 7200s）+ 更新 `last_login_time` + RBAC 五表展开 roles/permissions）；`POST /api/v1/user/refresh`（一次一换：Refresh Token 绑定配对 Access Token `at_jti`，刷新覆盖会话，旧 Refresh 重放 → 1003，登出后刷新 → 1003）；`POST /api/v1/user/logout`（幂等：仅校验 JWT 签名/有效期，DEL 会话使旧 Access/Refresh 即刻失效——落地 redis-keys pending #6 选项②「删除会话 + jti 校验替代黑名单」，未新增键模式）；`GET /api/v1/user/me`（JWT 解析结果 + real_name（DB 异常降级 null，待人工确认））
2. **认证安全原语**（`app/core/security.py`）：bcrypt（轮次 `PASSWORD_BCRYPT_ROUNDS=12` env 可覆盖）+ PyJWT（算法白名单 HS256、`iss=hunter-platform`、载荷 sub/username/roles/permissions/iat/exp/iss/jti，设计文档 3.2.2 节）；会话为**强依赖**（读取/写入失败 → 503 + code=5001，禁止降级，redis-keys common_rules）
3. **五级限流中间件（附录 D，阈值不可更改）**：单 IP 200 QPS（`rate_limit:{ip}:global`）/ 单用户 100 QPS（`rate_limit:{user_id}:global`，签名有效即计数）/ 接口级 user 维度（POST /api/v1/ota/versions 5、POST /api/v1/remote/session 1、GET /api/v1/data/telemetry 20）；Redis 滑动窗口（INCR+EXPIRE(NX) 复用 RedisManager.incr_with_expire）；429 + Retry-After + X-RateLimit-* 头，响应体 code 复用 5001；限流器非强依赖 → Redis 故障 fail-open 放行告警；ops/探针/CORS 预检豁免；全局 10000 QPS 由 Ingress 入口承担（进程内不重复计数）
4. **反向代理转发（x-hunter-gateway-routes）**：7 前缀最长前缀匹配 + strip_prefix=false 全路径转发 + 查询串透传 + **流式转发**（请求体 request.stream() 支持 OTA 大包；响应 aiter_raw 透传，含流已消费回退回放）；身份头注入 X-User-Id/X-Roles/X-Trace-Id（覆盖客户端同名头防伪造）并剥离客户端 Authorization；转发失败（连接/超时/熔断/后端未配置）统一 503 + 5001 禁止透传裸错误；vehicle/user 前缀（pending_confirmation）未配置 URL → 503；未知路径 → 404 + 3001；catch-all `include_in_schema=False` 保证「实现路由 ⊆ 契约」L5 校验成立
5. **三态熔断器**（契约 circuit_breaker 默认策略，阈值 env 可覆盖）：按目标服务隔离，连续 5 次失败或窗口超时率 > 50% → 熔断 30s → 半开放行单探测，半开探测失败立即重开
6. **配置扩展（config.py）**：新增 5 个转发目标 URL（K8s Service DNS 就绪）、转发超时/连接数、熔断参数、登录防护阈值（30 次/分、5 次锁定/900s——契约建议默认，待 14.5 节人工确认）、JWT_ISSUER、PASSWORD_BCRYPT_ROUNDS、TRUST_FORWARDED_FOR；`infra/k8s/services/api-gateway.yaml` ConfigMap 同步全部非敏感新键（敏感项仍走 Secret）
7. **依赖与工程**：api-gateway 新增运行时依赖 PyJWT/bcrypt/httpx（已装入 .venv），pyproject 增加 [tool.ruff] 配置（B008=FastAPI Depends 惯例豁免）；新增 `docs/api-gateway.md`（开发文档：分层/流程/设计决策/7 项待确认）；服务与根 README 更新；每层单元测试新增 62 项（auth 14 / rate_limit 8 / proxy 11 + 契约面）

验证：`pytest services/api-gateway -q` **66 passed**（health 4 + OpenAPI 契约 29 + auth/rate_limit/proxy 33）；`pytest common/python -q` **116 passed**（共享库无回归）；`pytest tests/integration/test_api_contract_surface.py` **53 passed / 1 skipped**（6 服务进程级契约面，含「实现路由 ⊆ 契约」与未知路径统一响应体）；`pytest tests -q` **79 passed / 1 skipped**（与 V1.2.0 基线 passed 数一致；5 failed + 11 errors **全部为容器型用例**——本机 Docker CLI 已安装但 daemon 未启动，testcontainers 探测语义由 skip 变为连接报错，属环境问题非代码回归，需启动 Docker Desktop 后复跑容器套件）；`ruff check services/api-gateway` **0 error**；`python scripts/verify_infra.py` **全部校验通过**（ConfigMap 新键无敏感字段）

风险与待人工确认：① MFA(TOTP)：`user_svc.users` 无 TOTP 密文字段（数据库契约未定义），当前全部账号按「未启用时忽略」处理，启用前须先扩展 DDL + OpenAPI 契约；② Refresh 防重放采用会话绑定（pending #6 选项②），如需独立黑名单键须先更新系统约束第 8 条；③ 单会话语义（二次登录覆盖前会话，多设备互踢）——多终端并存需扩展 session 键模式；④ /user/me 的 real_name 在 DB 异常时降级为 null（契约允许 503，选了部分降级保可用）；⑤ 登录防护与熔断阈值、WebSocket /ws/remote/** 转发（契约 Token 传送方式待确认）待人工确认后回填契约；⑥ catch-all 转发的处理顺序经 L5 契约面校验修正为「路由表匹配（404+3001）→ 严格认证（401/1001、1003）→ 转发能力惰性就绪（503+5001）」——认证前不得泄露 503/401 状态差异（tests/integration/test_api_contract_surface.py::test_unknown_path_unified_error_body 强制），语义边界（如 /api/v1/ 下未登记前缀返回 404 而非 401）需契约侧确认

## V1.4.0

Date：2026-9-18
变更类型：新增
版本主要有以下功能：
1. **data-analytics 分析查询 API 模块开发（契约 paths 全量落地）**：`GET /api/v1/analytics/perception/eval` 与 `GET /api/v1/analytics/control/eval`（算法评估文档查询：时间窗/车辆过滤，窗口成对校验，评估文档缺失 503+5001 降级）；`GET /api/v1/analytics/scene/coverage`（场景覆盖率统计 + heatmap，max_cells 截断 1..50000 默认 5000）；`GET /api/v1/analytics/corner-cases`（Corner Case 分页列表：分类/车辆/算法/异常分下界/时间窗过滤，按异常分排序，附分类计数，脏数据跳过）；`GET /api/v1/analytics/dashboard`（运营看板四数据块：车队概览/车辆状态/告警统计/指标趋势，offline=3 兜底，部分降级 degraded、全降级 503+5001，非法 time_range 422+2001）
2. **报告管理 API 开发**：`GET /api/v1/analytics/reports`（模板类型/车辆/状态/时间窗过滤 + 分页，page_size ≤ MAX_PAGE_SIZE）；`GET /api/v1/analytics/reports/{report_id}`（详情，ready 状态含预签名下载 URL expires_in=900）；`POST /api/v1/analytics/reports/generate`（202 异步生成流程，重复提交 409+3003，并发守卫防重复执行）
3. **权限与依赖注入**：查询类接口挂 `require_read_permission`、生成/执行类挂 `require_execute_permission`（JWT + RBAC：未认证 401+1001、角色不足 403+1002）；DB 会话/服务实例经 `Depends()` 注入，路由层零业务逻辑
4. **端点测试 24 例**：test_reports_endpoints.py（10：空列表、生成 202、重复提交 409、并发守卫、车辆校验 422/404、窗口校验、详情 404、预签名 URL 有效期、401、操作员 403）、test_dashboard_endpoints.py（5：聚合与兜底、部分/全降级、非法参数、401）、test_analysis_query_endpoints.py（9：评估文档缺失降级、窗口/车辆过滤、控制阈值 overall_pass、覆盖率截断、Corner Case 排序/分页/计数/脏数据跳过）
5. **代码质量收口（服务全绿）**：全部路由与依赖工厂签名迁移至 `Annotated[T, Depends/Query/Path]` 风格（FastAPI 官方推荐，消除 B008，OpenAPI 生成等价）；降级/清理路径宽捕获统一 `# noqa: BLE001[, S112] - <理由>` 注释（刻意吞异常 + 已记日志的降级语义）；fakes.py 可变默认参数 `set()` → `frozenset()`（B006）；`--fix` 清理导入排序（I001）、typing→collections.abc（UP035/UP037）、未用导入（F401）、无用 noqa（RUF100）；ruff 56 → 0
6. **过程中缺陷修复**：repositories/reports.py `_scan_all` 未解包 list_json 返回的 `(key, doc)` 元组导致列表接口返回二元素包装对象，现解包为 doc 后聚合
7. **契约对齐核对**：data-analytics 11 个契约路径全部落地——10 个业务/健康路径均在 app.openapi() 中且 36 项契约基线测试（operationId/参数/响应结构/错误码）全绿；`GET /metrics` 路由存在（Prometheus 文本格式，服务级与集成测试均验证），按共享库 hunter_common.metrics.register_metrics 统一约定 `include_in_schema=False`（data-collector 等服务同口径），属运维探针例外不视为缺失

验证：pytest services/data-analytics **60/60 passed**（36 契约基线 + 24 新增端点测试）；ruff check services/data-analytics **All checks passed**；pytest tests/integration/test_api_contract_surface.py data-analytics 项 **8 passed + 1 skipped**（skip=契约端点已全部实现，正向语义）

风险与待人工确认：① `GET /metrics` 不出现在 app.openapi()（schema 隐藏为共享库统一约定）——若后续引入「契约 ⊆ openapi()」强校验脚本，需团队统一决策（修改共享库影响全部 6 个服务，或契约 YAML 增加探针豁免标注）；② 新版 FastAPI 的 app.routes 含 `_IncludedRouter` 复合条目（延迟展开），路由核对不能直接遍历 `r.path`，应以 app.openapi() + 实际请求（测试）为验收口径；③ 报告生成的存储端（MinIO sidecar 文档）与评估文档窗口语义（成对出现/缺省取最近文档）基于契约推导，与详细设计文档 V4.0 原文口径需人工核对

## V1.5.0

Date：2026-9-18
变更类型：新增
版本主要有以下功能：
1. **新增 TelemetryControl 遥测控制段模型（5.3.3 节 control 段）**：`app/schemas/telemetry.py` 此前缺失 control 段定义（`from __future__ import annotations` 延迟求值掩盖引用缺失，仅在 app.openapi() 时暴露），按 5.3.3 节补全（target_velocity / target_steer / velocity_error / steer_error / control_latency_ms，段内字段全 nullable、extra=forbid），与其余六段结构对齐，OpenAPI 文档得以完整生成
2. **新增契约 206 截断响应声明**：`GET /api/v1/data/telemetry` 增补 `206 Partial Content` 响应定义（X-Request-ID + X-Truncated-Range header pattern `^seconds=[0-9]+$` + TelemetryQueryResponse schema）与 description 截断语义说明——服务端按待确认项 7（`TELEMETRY_QUERY_MAX_RANGE_HOURS` 默认 24h）超限截断并携带 `X-Truncated-Range: seconds=<n>`，契约与实现双侧对齐
3. **新增契约↔实现自动 diff 与 example 反序列化测试（+18 例）**：① 生成 OpenAPI paths（方法+路径）↔ 契约 paths 双向 diff（`/metrics` 运维端点豁免，由 ops 端点测试覆盖）；② 生成 Schema 名集合 ⊆ 契约同名集合（禁止实现私增/改名），契约 Request/Data/Item/Sample/Part 后缀实体必须全部暴露；③ 同名 Schema「有效属性集」一致（契约 allOf 组合 ↔ 实现内联展开，递归展开 $ref/allOf 比对）；④ 枚举 Schema 成员一致（ErrorCode 例外，由预定义错误码测试覆盖）；⑤ 契约 12 个 Schema 的最小合法实例（example 优先合成 + 实现必填字段叠加）必须被同名实现 Pydantic 模型接受且逐字段保值（递归子集比较）；⑥ 206 双侧声明一致性断言
4. **新增 boto3 依赖**：data-collector `pyproject.toml` 登记运行时依赖 `boto3>=1.28,<2`（MinIO S3 协议客户端，files 上传服务依赖），已装入 .venv

验证：pytest services/data-collector **52/52 passed**（34 存量 + 18 新增）；ruff check services/data-collector **All checks passed**；`import app.main` 正常（openapi() 生成成功，路径 diff 为空）

风险与待人工确认：① 契约 `FileCompleteData.data_type` 为可选、实现必填（服务端 complete 阶段必从对象路径解析回填，响应为契约的合法超集）——测试侧以「叠加实现必填字段」合成最小实例，若业务侧认为应严格对齐契约可选语义，需评估改为 `FileDataType | None`；② `ReadyChecks`/`HealthStatus` 等 ops Schema 与 `ApiResponse`/`ErrorCode` 共享组件不进入 FastAPI 生成文档（探针路由无 response_model、错误响应经 exception handler 返回），契约侧已由专项测试覆盖，若引入「契约 Schema 全量 ⊆ openapi()」强校验需统一豁免口径；③ 契约 206 声明是对既有实现行为的追认（截断决策本身已登记为待确认项 7），前端/网关消费方需按 2xx 但非 200 处理 206（axios 默认 validateStatus 覆盖 2xx，无影响）

## V1.6.0

Date：2026-9-18
变更类型：新增

**ota-service 契约实现（routers / schemas / services / tests，基于 contracts/openapi/ota-service.yaml 全量落地）**

1. **schemas/**：common（统一响应基类 OtaApiResponse 五字段 + 受控枚举 OtaBatchStatus/OtaNextAction/OtaTaskAction/OtaRollbackTarget/OtaPreconditionName/ScheduleMode + pattern 常量）、versions、tasks、records —— 与契约 components.schemas 一一对应（Pydantic v2，extra=forbid；灰度比例 Literal[5,20,50,100]、观察 Literal[24]、门禁 Literal[0.95] 锁定不可更改取值域）
2. **repositories/**：storage（MinIO 预签名 上传 1h/下载 15min+Range、head_object、单次流式 MD5+SHA-256 不整包入内存）、versions（唯一性/防回滚基准 max_published_code/task_count 引用统计）、tasks（FOR UPDATE 行锁、GIN 包含匹配、create_time DESC）、records（ON CONFLICT DO NOTHING 幂等下发、按 (task_id,vehicle_id) 状态推进终态写 end_time、快照聚合）——SQLAlchemy 2.0 异步，仅 ota_svc 三表
3. **services/**：versions（创建 3002 唯一性 + 6003 防回滚前置；发布五项校验 size/md5/sha256/signature/monotonic 全过才 published，6001/6002/6003 data 携带 expected/actual 上下文；deprecate 校验 running/paused 引用）、tasks（灰度策略冻结：显式给出必须逐字段等于规范序列否则 2001；门禁不得弱于平台门禁；start/resume 逐车门禁 released[]/blocked[] 回执、整批全失败→failed+CRITICAL 告警日志；FOR UPDATE 串行化；禁止跳批 batch_no=当前+1；幂等仅对 PENDING 重发；cancel 终态冲突 3003；rollback 仅 SUCCESS 记录逐车下发 command、离线/门禁/状态逐车 rejected）、records（任务明细含 summary、单车历史）、rollout.py（纯函数：ceil+末批补足取整、成功率=SUCCESS/(SUCCESS+FAILED+ROLLED_BACK)、无终态 null、next_action/halt_reason 派生）、gates.py（纯函数：电量≥50%/P 档/网络≤10s/存储≥2048MB，读 vehicle:status 读模型，缺数据即拒绝=安全默认 #19，回滚不查电量）
4. **producers/**：ota_notify（hunter.{vehicle_id}.ota_notify，key=vehicle_id/acks=all，package_url=发送时刻签发 1h 预签名地址，载荷对齐 ota_notify.schema.json）；rollback_command（hunter.{vehicle_id}.command，command_type=ota_rollback，params={task_id,target,reason}，对齐 command.schema.json oneOf 约束）；broadcast.command 不生产（pending #12）
5. **consumers/ota_status.py**：消费 hunter.*.ota_status（组 ota-service-ota-status，regex 订阅，手动提交+DLQ {topic}.dlq，幂等键 (task_id,vehicle_id,status)），推进 ota_records、重算 progress JSONB 快照与 ota_canary_success_rate 指标、批次成功率<0.95→任务 paused+CRITICAL 告警日志（alert_event 生产者属 data-analytics，本服务不生产）、第 4 批全终态→succeeded
6. **core/**：dependencies（RBAC：网关注入 X-User-Id/X-Roles，ota:read/create/execute 三动作，缺失→1001/不足→1002）、rate_limit（Redis 固定窗口 incr_with_expire，429+Retry-After、code 复用 5001，fail-open）、signature（RSA-2048 RSASSA-PKCS1-v1_5+SHA-256 验签，公钥 OTA_SIGNATURE_PUBLIC_KEY Secret 注入，组件缺失→5001 拒绝发布绝不跳过；签名载荷=SHA-256 hex 字符串为契约缺失项的实现约定）、metrics（ota_package_verify_failures_total{reason}/ota_canary_success_rate{task_id,batch_no}）
7. **routers/**：versions/tasks/records 共 15 业务端点（operationId 与契约一致，response_model 对应契约响应 Schema，统一错误响应声明 429/422/404/409/401/403/503/500）；routers/health.py：/readyz 增加 MinIO head_bucket 探测（契约 ReadyChecks{database,redis,minio}），/healthz data 对齐 HealthStatus；error_handlers：业务异常 details 透传响应体 data（对齐契约错误上下文示例）
8. **config.py**：OTA_* 全量环境变量（灰度/门禁/校验开关/限流/Kafka 消费组与 Topic 模板/MinIO bucket/签名公钥/RBAC 角色），OTA_CANARY_STAGES/observe_hours/min_success_rate 契约固定值启动强校验（fail-fast）
9. **tests/**：conftest 全内存替身（FakeRedis/FakePackageStorage/三仓储/生产者/读模型）+ **98 passed**（无外部基础设施依赖）：test_ota_contract.py（契约端点集==x-hunter-endpoints、生成 OpenAPI 与契约路径/方法一致、HTTP_STATUS_BY_CODE==x-hunter-error-status-map、枚举==hunter_common、附录 D 限流==settings、十类响应 Schema 最小实例合成校验、DDL 三表列覆盖、README 端点同步）、test_versions_api（auth/1002/列表预签名/创建/multipart/3002/6003/详情/发布五项/6001/3003/退役）、test_tasks_api（灰度冻结/2001 策略/3001/3003/上限/列表/详情 rollout/门禁放行与拦截/整批失败/离线/幂等/禁跳批/pause/resume halt 3003+halt_reason/cancel/回滚 command 下发与逐车拒绝）、test_records_api（404/summary/duration/过滤/空列表/pattern 422）、test_rollout_gates（取整/策略/视图/进度/门禁七例/契约固定值）、test_signature（RSA 四例，importorskip cryptography）
10. **pyproject.toml**：新增运行依赖 boto3、cryptography；dev 增加 PyYAML；README.md 实现结构与安全/灰度/限流说明同步

验证：`pytest services/ota-service/app/tests -q` **98 passed / 0 failed**（contract 27 + versions 12 + tasks 15 + records 5 + rollout_gates 20 + signature 4 + health 6 + 既有 9）；`app.openapi()` 生成 15 业务路径 + /healthz /readyz（/metrics 文本格式为仓库统一例外）与契约一致；全模块 compileall 通过

风险与待人工确认：① 签名载荷语义契约未定义，实现取「SHA-256 hex 字符串（UTF-8）」并与车端验签规则需对齐后回填契约；② alert_event 本服务不生产（topics.yaml 生产者=data-analytics），灰度 halt 仅 CRITICAL 审计日志 + 指标，告警链路接入方式待确认（同 pending #12）；③ start/resume 整批门禁全失败→任务 failed，4001/4002 以逐车 blocked[] 语义表达而非全局 409（与「单车失败不阻断」自洽），Conflict 示例触发时机需产品确认；④ publish 验签组件 cryptography 已入 .venv 与 pyproject，CI 需确认缓存；⑤ 消费者订阅 pattern 以 ^hunter\..*\.ota_status$ 正则传入 confluent-kafka（consumer-groups.yaml 记法 hunter.*.ota_status 的语义等价）

## V1.7.0

Date：2026-9-18
变更类型：新增

**remote-control 契约实现（services / routers / main / tests，基于 contracts/openapi/remote-control.yaml 全量落地）**

1. **services/session_service.py（重建与契约收口）**：配置字段全面对齐 `app/config.py` 实名（`rc_command_hz`/`rc_stop_on_timeout_ms`/`rc_heartbeat_end_after_s`/`rc_turn_shared_secret`/`rc_turn_credential_ttl_s`/`rc_presign_get_ttl_s`/`rc_sessions_scan_limit`），契约固定字面量内联（视频 1280×720@30、指令 hz=20/interval_ms=50/stop_on_timeout_ms=500、心跳 10s——config 校验器保证环境变量恒等，偏离即启动失败）；会话生命周期（创建：Redis 互斥锁 `rc:lock:{vid}` + 锁内二次判定 → 7001、车辆在线/状态/操控检查 → 4001/4002、先写 `rc:session:{vid}` Hash（11 字段唯一事实源）→ Kafka 信令 → 失败回滚；结束：单条 Lua 式条件写（仅 active 可终态）→ session_end 信令/stop 帧/封存下发，admin_terminate 强制日志）；`_negotiate_video`/`_build_control_channel`/`_build_heartbeat`/`_build_ice_servers`/`_build_webrtc`（STUN+TURN（共享密钥+TTL）+ SRS WebRTC 契约地址/SrsMediaInfo）全部按契约 Schema 装配；session_id 用 `re.match(UUID_PATTERN, ...)` 严格校验（3001）；`_config_digest` 以 `rc_command_hz` 计算一致性指纹；降级原因枚举补 `DegradedReason.HEARTBEAT_MISS`
2. **services/history_service.py（新增）**：MinIO `hunter-video` sidecar JSON（`remote-control/{vid}/{y}/{m}/{d}/{sid}.json`）为唯一持久层（无 DB 表，契约 383-385 行）——`list_history`：ListObjectsV2 读取 → SessionSidecar 模型容错解析（损坏条目跳过计数）→ 操作员（数据权限收敛）/end_reason/时间窗过滤 → started_at 降序 → 分页；`get_history_detail`：sidecar + `video_available` 派生（对象存在性判定）；`get_video_access`：presign 900s（cast `Literal[900]`，契约上限）、对象缺失 → 3001；`_validate_range`：跨度 > 31 天 → 2001
3. **routers/（新增 vehicles/sessions/history 三文件，8 业务端点）**：`GET /api/v1/remote/vehicles`（在线可操控车辆 + status 过滤 + `generated_at`，remote:read）、`GET /api/v1/remote/sessions`、`POST /api/v1/remote/session`（附录 D 1 QPS/用户，remote:create）、`GET|DELETE /api/v1/remote/session/{session_id}`（include_stats/reason，remote:read/execute）、`GET /api/v1/remote/history`、`GET /api/v1/remote/history/{session_id}`、`GET /api/v1/remote/history/{session_id}/video`——统一前缀、`responses=_COMMON_ERRORS`（429/401/403/500/503）、ota 风格 `*Dep` 别名注入、`_effective_operator_id` 将普通用户强制收敛为自身（数据权限），路由层零业务逻辑
4. **main.py（重写）**：lifespan 装配（DatabaseSessionManager/RedisManager/KafkaProducerManager → `S3VideoArchiveStorage`/`VehicleViewReader`/双 Producer（`hunter.{vid}.remote_control` boot/stop 帧 + `hunter.{vid}.command` rc_session_start/end 信令）→ `SessionService`/`HistoryService` 全量入 `app.state` DI）；`_produce` ProduceFn 适配（真实生产者内部将 Kafka 异常转 5001，service 层据此回滚）；CORS/trace_id 中间件/3 业务路由注册/`/metrics`/统一异常处理器；service 构造签名与契约对齐
5. **测试（新增 4 文件 35 例，全内存替身无外部依赖）**：conftest（FakeRedis 含 `acquire_lock`/`lock`、FakeLock、FakeVideoStorage（临时目录 + sidecar/fixtures 制造）、Fake 双 Producer 继承真实类且失败时抛 `ServiceUnavailableError`（与真实生产者 5001 契约一致）、`rc_env`/`client` fixtures、创建限流放宽 1000/min）；test_session_service ~12（创建成功/重复 7001/车辆离线 4001/忙碌 4002/信令失败回滚 Hash→5001/结束状态机/详情/降级字段）；test_history_service ~7（过滤/分页/排序/31 天 2001/详情/录像访问 3001/脏数据容错）；test_routers ~10（401/1001、403/1002、数据权限收敛、vehicles generated_at、列表/创建/详情/结束/历史/录像端到端）
6. **质量收口（服务全绿）**：mypy 37 → **0**（`Awaitable` 探针类型、`Mapping` headers、`Annotated` 路由、契约 Literal 字面量/cast 收敛）；ruff check **All checks passed** + format（重排 11 文件含既有文件，diff 较大）；`py_compile` 全过；`repositories/storage.py` 的 `_KEY_PREFIX` 公共化为 `KEY_PREFIX`；`app.openapi()` 与 `remote-control.yaml` diff：8/8 业务端点 operationId+method+path 精确一致（+ /healthz /readyz 运维探针，契约 1324-1326 行同有定义）

验证：`pytest services/remote-control/app/tests -q` **35 passed / 0 failed**；`ruff check app` **All checks passed**（35 文件）；`mypy app --ignore-missing-imports` **Success: no issues found in 35 source files**（对照 ota-service 基线 66 errors，本服务 0）

风险与待人工确认：① 契约固定值（hz=20/50ms/500ms/10s/1280×720/keyframe=1s）在代码内以字面量内联 + config 校验器强制环境变量恒等（偏离启动即失败）——如评审倾向 `cast()` 注入为等价小改；② `expires_in=cast("Literal[900]", ...)`：`RC_PRESIGN_GET_TTL_S` 实际不可调（配为非 900 时运行时模型校验 422 快速暴露），需告知运维；③ sidecar 统计字段（ACK 延迟/超时事件/视频 P95）为 0 占位（Hash 11 字段无此数据），随 WS 控制通道（pending #13-#16）接入后回填，sidecar schema 不变；④ history 详情检索为全桶前缀扫描（契约 383-385 行已知风险，pending #8），数据量大时需按日期前缀收敛；⑤ `/ws/remote/{session_id}/{signal|control}` 两个 WebSocket 端点未含（pending #13-#16 范围），REST 面已完整；⑥ 新版 FastAPI `app.routes` 含 `_IncludedRouter` 复合条目，路由核对以 `app.openapi()` + httpx `ASGITransport` 实际请求为验收口径（同 V1.4.0 结论 ②）

## V1.8.0

Date：2026-9-19
变更类型：新增

**scene-service 契约实现（L3 Step 2：routers / schemas / services / repositories / consumers / tests 全量落地，基于 `contracts/openapi/scene-service.yaml`）**

1. **schemas（契约 1:1 映射）**：`app/schemas/scene.py` —— 受控词表 `SceneType`（4.2.1 分类体系 18 个叶子场景）/`SceneTemplateCategory`（模板筛选 5 类，不含实车回放）/`SimulationStatus`/`MapType`/`ActorType`/`SceneEventType`/`SceneExportFormat`/`SceneSortField`/`SortOrder`；4.2.2 结构 `SpawnPoint`/`SceneMap`/`EgoVehicle`/`Weather`/`Actor(+Behavior)`/`SceneEvent(+Trigger/Action)`/`SuccessCriteria` → `SceneConfig`、元信息 → `SceneMeta` → `Scene`；分类↔类型双向映射（并集 == SceneType 全集）；请求体 `extra="forbid"`（契约 additionalProperties:false）；字段约束逐条对齐（scene_name 1..128、tags ≤20 项 ×1..32、actors/events ≤100、duration > 0、version `^\d+\.\d+\.\d+$`、sha256 `^[0-9a-f]{64}$`）；`app/schemas/common.py` 复用 `DEFAULT_PAGE_SIZE`/`MAX_PAGE_SIZE` 单一事实来源
2. **repositories（数据访问层解耦，禁跨服务直连）**：`scenes.py`（SQLAlchemy 2.0 异步：软删除过滤 `deleted_at IS NULL`、`tags @>` 命中 GIN、keyword 走 `ilike` + LIKE 元字符转义、排序白名单 + `scene_id` 次级排序稳定分页、`IntegrityError(23505)` → 3002）、`cache.py`（`cache:scene:{scene_id}` String(JSON) TTL 3600，读写失败降级为未命中）、`storage.py`（MinIO `hunter-scene-assets`，boto3 经 `asyncio.to_thread`，对象命名 `scenarios/scene-{scene_id}-{version}.{ext}`）、`carla.py`（httpx 客户端：未配置地址/连接失败/超时/非 2xx/响应非法一律 5001，有限重试 + 连接池惰性创建）
3. **services（业务规则，路由与数据访问解耦）**：`scenes.py`（列表筛选/分页/数据权限隔离；创建 draft+creator+version 1.0.0；仅 draft 可编辑/发布 3003；published 不可删 3003；软删除；复制 `<源名称>-copy`（冲突追加序号）；发布前复校 4.2.2 结构；version 需 ≥ 当前版本；写后缓存失效；`config_json` 脏数据 → 5000 不静默丢弃）、`templates.py`（4.2.1 分类体系驱动的 17 个内置模板，穷举 SceneType 构造器，config 即 4.2.2 结构）、`serializers.py`（4.3 节导出：Carla ScenarioRunner XML + OpenSCENARIO 1.2；确定性输出（6 位有效数字 + JSON 参数 sort_keys）；`x-hunter-trigger/action` 与 `x-scenario-version` 原样承载未定义结构，不臆造 OSC 语义；批量导出为单文件集）、`export.py`（SHA-256 + 上传 + 900s 预签名，scene_ids 上限校验）、`simulation.py`（4.4 节 ①②③④⑤：published 门禁 → 参数覆盖白名单深合并（含列表下标与 `map.*` 禁止）→ 并行实例判定（allow_parallel=false → 3003 / true → 复用）→ 创建实例 → 下发配置 → 返回 sim_instance_id）
4. **routers（10 个业务端点 operationId/method/path 与契约逐条一致）**：`scenes.py`（listScenes/createScene/getScene/updateScene/deleteScene/duplicateScene/publishScene）、`templates.py`（listSceneTemplates，**注册顺序先于 `/{scene_id}`** 避免路径参数抢占）、`export.py`（exportScenes）、`simulation.py`（runScene）、`errors.py`（统一错误响应声明 1001/1002/2001/2002/3001/3002/3003/5000/5001）、`health.py` 补 `HealthResponse`/`ReadyResponse` 响应模型
5. **core/consumers/main**：`core/dependencies.py`（网关转发头 X-User-Id/X-Roles → 1001/1002；scene:read|create|update|delete|execute 五动作依赖；服务经 app.state 注入）、`consumers/analytics_result.py`（4.5 节实车场景自动提取：必填字段/结果类型/10-10 窗口校验（违规 → ValueError → DLQ）；触发事件映射 harsh_braking→emergency_brake / collision_warning→collision / manual_takeover→manual_takeover；环境条件 → Carla 天气映射；自车轨迹/目标轨迹 → ego+actors；输出 real_vehicle_replay+draft；幂等键 `(vehicle_id, window_start)` 经 scene_name 唯一索引落地）、`main.py`（lifespan 装配 DB/Redis/MinIO/Carla/四服务 + 消费者任务；契约 `produces=[]` 故不初始化 Kafka 生产者）
6. **契约与配置同步（先契约后实现）**：`contracts/openapi/scene-service.yaml` 的 `x-hunter-service.required_env` 登记实现所用全部环境变量（MINIO_REGION / SCENE_EXPORT_* / CARLA_INSTANCE_* / CARLA_SCENARIO_SUBMIT_PATH / CARLA_REQUEST_* / SCENE_DURATION_MAX_SECONDS / SCENE_PARAM_OVERRIDE_* / SCENE_DETAIL_CACHE_TTL_SECONDS / SCENE_READ|WRITE|EXECUTE_ROLES / SCENE_SCOPE_* / ANALYTICS_RESULT_TOPIC / SCENE_EXTRACTION_*），待确认项 #9 回填实现侧落地方式；`infra/k8s/services/scene-service.yaml` ConfigMap 注入非敏感键（Carla 地址仍留环境 overlay，禁止硬编码）；`.env.example` 新增「服务私有配置：scene-service」小节；`services/scene-service/pyproject.toml` 增加运行时依赖 boto3 / httpx
7. **测试（100 项，全内存替身无外部依赖）**：`tests/fakes.py`（InMemorySceneRepository 复刻软删除/唯一/筛选/排序语义，FakeSceneCache/InMemorySceneStorage/FakeCarlaClient）、`tests/conftest.py`（app.state 全套替身装配 + ASGI 客户端 + admin/operator/viewer 三角色头）、`test_scene_endpoints.py`（42：10 端点正常流程 + 1001/1002/2001/2002/3001/3002/3003/5001 错误分支 + 缓存命中/失效断言 + 未知路径统一响应体 + DB 不可达 → 5001）、`test_scene_services.py`（15：分类并集、模板完整性、版本语义、参数覆盖白名单（含 map 禁止/未知路径/越界取值/列表下标）、导出确定性与批量容器、命名规则、MinIO 端点规范化、数据权限强制 creator）、`test_analytics_result_consumer.py`（10：示例消息 → real_vehicle_replay 草稿、三类触发映射、10/10 窗口、幂等、非触发跳过、5 组非法消息参数化 → ValueError）
8. **真机冒烟发现并修复的两个启动期缺陷**：① `repositories/storage.py` 的 MinIO 端点未带 scheme（共享配置默认 `localhost:9000`）导致 boto3 抛 `ValueError: Invalid endpoint`、应用启动挂起 —— 新增 `endpoint_url_for()` 按 `MINIO_SECURE` 补全并加单测；② 数据库连接被拒时 asyncpg 直接抛 `ConnectionRefusedError`（非 SQLAlchemy 包装异常），落到通用处理器变成 **5000** —— `core/error_handlers.py` 新增依赖不可用处理器（`OperationalError`/`InterfaceError`/`ConnectionError`/`OSError` → **503 + code=5001**，对齐契约「PostgreSQL 不可达」）

验证：`pytest services/scene-service -q` **100 passed / 0 failed**（契约 29 + 端点 42 + 服务层 15 + 消费者 10 + 健康 4）= 变更前 33 → 100；`ruff check services/scene-service` **All checks passed**；`scripts/verify_infra.py` **全部校验通过**；`scripts/verify_data_layer.py` **26 项通过**；`app.openapi()` 与 `scene-service.yaml` 比对：10/10 业务端点 operationId + method + path 完全一致（+ `/healthz` `/readyz` 探针，`/metrics` 为契约例外不入 Schema）；**uvicorn 真机冒烟**（无 DB/Redis/MinIO/Carla）：`/healthz` 200 统一响应、`/readyz` 503 + code=5001（探测明细 database/redis=false）、`/metrics` Prometheus 文本、`/api/v1/scene/templates` 200（17 模板）、`/api/v1/scene` 无 DB → 503 + code=5001、缺失身份头 → 401 + 1001、未知路径 → 404 + 3001

风险与待人工确认：① **Carla 管理 API 子路径与响应结构**属待确认 #9，实现以 `CARLA_INSTANCE_CREATE_PATH`/`CARLA_INSTANCE_QUERY_PATH`（默认 `/api/v1/instances`）与 `CARLA_SCENARIO_SUBMIT_PATH`（默认 `/api/v1/instances/{sim_instance_id}/scenario`）环境变量化，响应体实例 ID 兼容 `sim_instance_id`/`instance_id`/`id`，确认后仅调配置；② 并行实例判定依赖 Carla 查询接口返回结构，结构未知时记 warning 并放行（不误判 3003），需确认后收紧；③ 4.5 节自动提取的 `creator` 用系统 uuid（默认全 0，`SCENE_EXTRACTION_CREATOR_ID` 可配），**系统账号 uuid 待确认**（当前非真实 user_svc 用户，仅作逻辑外键）；④ 模板数据来源（待确认 #11）当前为内置静态清单，确认后替换数据源即可（响应结构不变）；⑤ 导出批量语义（待确认 #6）实现为「单文件集」：单场景输出纯 `OpenSCENARIO` 根，多场景输出 `<x-hunter-scenario-set>` 容器（Carla XML 天然支持多 `<scenario>`）；文件命名恒取首个场景（契约 naming 未定义批量规则）；⑥ 数据权限隔离默认关闭（`SCENE_SCOPE_BY_CREATOR=false`），开启后非豁免角色仅可见自有场景，需确认是否作为默认安全基线；⑦ **monorepo 级 pytest 收集冲突（既有问题，非本次引入）**：`pytest common/python services` 在多服务 `app/tests/conftest.py` 同名（插件重名 `app.tests.conftest`）时会失败，已用 `pytest services/data-analytics services/ota-service` 复现，建议后续统一测试包命名或按服务目录运行；本次验证采用 `pytest services/scene-service -q` 与 `cd services/scene-service && pytest -q` 两种口径；⑧ `/readyz` 两项探测各自 2s 超时（DB 连接被拒在 Windows 上约 4s），依赖全不可达时该端点约 4s 返回 503，逼近 K8s `timeoutSeconds: 5` 预算——如需压缩需在共享库 `DatabaseSessionManager` 增加 `connect_args={"timeout": ...}`（涉及全部服务，建议独立变更）

## V1.9.0

Date：2026-9-19
变更类型：新增

**Kafka 契约驱动生产/消费能力补全（`common/python/hunter_common/kafka`，契约 `contracts/kafka/` 为单一事实来源）**

1. **契约运行时加载（新增 `contracts.py`）**：四级目录定位（显式 `KAFKA_CONTRACT_DIR` → 工作目录向上 → 包位置向上；空白值按未提供处理并回退自动探测）；`TopicSpec` / `ConsumerGroupSpec` 视图；六种 Topic 写法解析（契约条目名 / 模板 `hunter.{vehicle_id}.telemetry` / 正则 `hunter.*.telemetry` / 具体实例 / 广播 / DLQ `{topic}.dlq` 继承源 Topic 配置）；Schema 加载时自动校验 draft-07 合法性；`validate_message()` 错误详情含字段路径（前 5 条）；新增 `KafkaContractError`（code=5000）与 `KafkaMessageSchemaError`（code=2001，复用预定义错误码）
2. **消息编解码（新增 `messages.py`）**：`KafkaRecord` + `encode_payload` / `decode_payload`（紧凑 JSON、非 ASCII 不转义、bytes 透传）；`build_record()` 生产侧强制契约约束——消息体按 Schema 校验（`additionalProperties=false`）、契约 `key: vehicle_id` 的 Topic 强制 key 与消息体 `vehicle_id` 一致、广播 Topic（`key: none`）禁止携带 key；`parse_payload()` 消费侧解码 + Schema 校验；契约可用时禁止投递契约外 Topic
3. **本地磁盘缓冲（新增 `buffer.py`）**：分段 JSONL（base64 字节保真、`fsync` 落盘）；容量超过契约 `producer_defaults.local_disk_buffer_bytes`（1GB）时整段 FIFO 淘汰最旧并累计丢弃指标 + WARNING（仅剩单段时保留并 CRITICAL 提示扩容）；崩溃安全重投（成功后原子 `os.replace` 重写剩余记录、`*.jsonl.tmp` 启动还原）；损坏行跳过不阻塞；进程重启后缓冲水位恢复
4. **消费幂等守卫（新增 `idempotency.py`）**：`IdempotencyGuard.claim()`（LRU + TTL，`compose()` 把契约幂等键元组归一为 sha256）；预留分布式存储注入点 `IdempotencyStore`（Redis Key 模式须先在 `redis-keys.yaml` 登记，故不内置键名）
5. **Kafka 指标（新增 `metrics.py`）**：12 个 `hunter_kafka_*` 指标注册到共享 `REGISTRY`（生产计数/时延/重试、缓冲水位/丢弃/重投、消费计数按 `processed/skipped_duplicate/schema_invalid/handler_failed`、DLQ 计数按 `reason`、分区消费积压），随各服务 `/metrics` 一并暴露
6. **生产者改造（`producer.py`）**：按 Topic 契约 `acks` 选择底层 Producer 实例（telemetry=1 / health=0 / 其余=all；契约外 Topic 回退配置并告警）；重试 + 指数退避（仅 broker `retriable` 错误）；重试耗尽且为可重试错误时落盘缓冲并返回 `ProduceResult(status="buffered")`；新增 `publish()` / `publish_payload()`（契约驱动严格入口）、`replay_buffered()`；`close()` 先尽力重投缓冲再 flush；低层 `produce()` 保持宽松语义（DLQ 等契约外 Topic、既有服务调用方零改动）
7. **消费者改造（`consumer.py`）**：`schema_name` 支持 `"auto"`（按消息实际 Topic 解析契约 Schema）/ 固定名 / `None`（兼容既有调用方）；非法消息直接进 DLQ（`reason=schema_invalid`，不重试）；handler 失败按配置指数退避重试，耗尽转 DLQ（`reason=handler_error`）并保留 `dlq.original.topic/partition/offset/reason/error` 头；可注入幂等守卫跳过重复消息；整批处理后手动提交 offset；每批刷新消费积压指标；新增 `stop()` 优雅停机
8. **配置（`config.py`）**：新增 `kafka_produce_max_attempts` / `kafka_produce_retry_backoff_ms` / `kafka_produce_retry_backoff_max_ms` / `kafka_local_buffer_enabled|dir|max_bytes|segment_max_records` / `kafka_consumer_max_attempts` / `kafka_consumer_retry_backoff_ms` / `kafka_consumer_idempotency_cache_size|ttl_s` / `kafka_consumer_lag_metrics_enabled` / `kafka_contract_dir`（空串归一为 None）
9. **依赖（`pyproject.toml`）**：`jsonschema` / `PyYAML` 由 dev 提升为运行时依赖（契约加载与 Schema 校验在运行时使用）
10. **测试（新增 5 个文件 46 条）**：`test_kafka_message_roundtrip.py`（契约解析六写法 / acks 与分区契约值 / 消费组契约 / telemetry+sensor_file 字节级往返 / key 强制与广播禁 key / 非法消息字段路径 / 契约缺失 fail fast / 空白契约目录回退）、`test_kafka_producer_unit.py`（Mock Producer：契约 acks 三实例 / 可重试重试后成功 / 不可重试直接抛出且不落盘 / 重试耗尽落盘 / 缓冲可关闭 / 契约入口校验 key / 重投清空 / 退避指数与封顶）、`test_kafka_consumer_unit.py`（Mock Consumer+Producer：批次提交与关闭 / handler 重试 / 重试耗尽 DLQ 与溯源头 / Schema 非法 DLQ 不进 handler / auto 按消息解析 / 幂等跳过 / 积压指标 / 未知 Schema 与 auto 缺契约 fail fast / 无校验时 bytes 回退）、`test_kafka_local_buffer.py`（落盘水位 / 重启恢复 / 超限淘汰 / 单段超限保留 / 重投清空 / 失败保留剩余 / 损坏行跳过 / max_records / tmp 还原）、`test_kafka_idempotency.py`（键归一 / 一次性认领 / LRU 淘汰 / TTL 过期 / 跨副本存储去重）
11. **文档与契约登记**：`contracts/kafka/README.md` 新增「实现侧语义登记」表（acks 选择、key 规则、重试退避、磁盘缓冲、Schema 校验、DLQ 消息头、幂等、积压指标、契约目录定位）并更新一致性清单第 5 条为模块清单；`common/python/README.md` 新增「Kafka 契约驱动生产/消费（用法）」；根 `README.md` 新增 Kafka 要点；`.env.example` 新增 Kafka 行为配置小节

验证：`pytest common/python/tests -q` **162 passed**（较变更前 116 新增 46）；服务回归 `pytest services/ota-service -q` **98 passed**、`data-collector` **52 passed**、`scene-service` **100 passed**、`data-analytics` **60 passed**、`remote-control` **35 passed**、`api-gateway` **66 passed**；`ruff check common/python` **All checks passed**；`mypy common/python/hunter_common` 仅剩 1 处**既有**告警（`logging.py:143` `_DynamicStdout`，非本次引入）；`python scripts/verify_data_layer.py` **26 项通过**；`python scripts/verify_test_layer.py` **L5 测试层自检通过**。

风险与待人工确认：
① **契约文件未随服务镜像分发**（各服务 Dockerfile 仅 `COPY app` 与 `common/python`）：容器内未挂载契约时，`schema_name="auto"` / `contract_required=True` 会启动即失败 → 需在 K8s 为相关服务增加 `contracts/kafka` ConfigMap + volumeMount（涉及多服务清单，建议独立变更）；本地/CI（仓库内运行）不受影响。
② **消费者 Schema 校验是显式开关**（默认 `None`，保持既有服务行为不变）：data-collector / ota-service / scene-service 是否启用 `schema_name="auto"` 需评审后逐服务开启——若车端消息与契约不一致会**批量进 DLQ**，上线前需确认车端字段与 `schemas/*.schema.json` 完全一致。
③ 本地磁盘缓冲为**进程内单副本**语义：K8s 需挂载 `emptyDir`（建议 `sizeLimit`）或 PVC 并纳入容量配额；多副本/多分区之间缓冲不共享，扩容策略待确认。
④ 幂等守卫默认进程内 LRU：多副本无法跨副本去重（依赖业务唯一索引兜底）；若需 Redis 严格去重，须**先**在 `contracts/database/redis-keys.yaml` 登记 Key 模式（契约先行），当前仅提供注入点。
⑤ 生产 acks 通过「按 acks 值缓存多个 Producer 实例」实现：若后续契约出现新 acks 取值（如 `-1`/数字串）需确认归一规则。
⑥ 重试仅对 broker 侧 `retriable()` 错误生效：若线上出现「消息未重试即进缓冲/DLQ」与预期不符，需按实际错误码补充白名单。
⑦ 契约外 Topic 的低层 `produce()` 走宽松路径（回退配置 acks + WARNING），不阻断链路；若需严格禁止契约外投递，需在后续变更中收紧并同步 DLQ 例外说明。
⑧ DLQ 头字段（`dlq.reason` / `dlq.error`）与本地缓冲语义当前登记在 `contracts/kafka/README.md`，未写入 `topics.yaml` 结构；是否提升为契约显式字段需人工确认（避免擅自改动契约结构）。

## V1.10.0

Date：2026-9-19
变更类型：新增

**数据库契约驱动的数据访问层补全（ORM relationship + 模型级 Repository，契约 `contracts/database/orm-mapping.md` 为单一事实来源）**

1. **新增 ORM 关系与 Repository 契约（`contracts/database/orm-mapping.md`）**：定义异步安全 lazy 策略白名单（`selectin` / `raise_on_sql`，禁止隐式 `lazy="select"`）、16 条 relationship 契约表（两端 / 关联实现 / lazy / 级联与删除语义 / 理由）、跨服务逻辑外键不建 relationship 清单、13 个 Repository 契约表（模型 / 模块 / 专属方法与对应 DDL 索引）、`BaseRepository` 通用方法契约、默认排序约定与校验命令；`contracts/database/README.md` 文件清单与「关键约束」同步（结构变更链路扩展为 DDL → ORM → Repository → Alembic → 契约文档）
2. **ORM relationship 补全（`models/core.py`、`models/ota.py`，16 条）**：RBAC 四表——`User.roles` / `Role.users`（`secondary=user_svc.user_roles`，`viewonly`，`selectin`）、`Role.permissions` / `Permission.roles`（`secondary=user_svc.role_permissions`，`viewonly`，`selectin`）、关联对象 `User.user_roles` / `Role.user_roles` / `Role.role_permissions` / `Permission.role_permissions`（`all, delete-orphan` + `passive_deletes=True` + `raise_on_sql`）、`UserRole`/`RolePermission` 的多对一（`raise_on_sql`）；OTA——`OtaVersion.tasks`（DDL FK `ON DELETE RESTRICT` → **不** delete-orphan + `passive_deletes`，避免 ORM 置空 NOT NULL 外键）、`OtaTask.records`（`ON DELETE CASCADE` → delete-orphan + `passive_deletes`）、`OtaTask.version` / `OtaRecord.task`（`selectin`）；全部关系显式声明 lazy，零 SAWarning（`overlaps` 消除关联对象与多对多的双写告警）
3. **新增模型级 Repository（`hunter_common/database/repositories/`，13 个）**：`core.py`（VehicleRepository：`get_by_device_cert_sn` / `list_by_status` / `update_status`；UserRepository：`get_by_username` / `get_by_email`（`lower(email)` 表达式唯一索引）/ `list_role_codes` / `list_permission_codes`（RBAC 展开）/ `touch_last_login`；RoleRepository、PermissionRepository、UserRoleRepository / RolePermissionRepository 的 `get_pair` / 列表 / `link`（`ON CONFLICT DO NOTHING` 幂等）/ `unlink`）、`scene.py`（`get_by_scene_name`）、`ota.py`（版本按 code/name 查询与 `max_version_code` 防回滚基准；任务按状态/目标版本查询；记录 `get_by_task_vehicle` / `list_inflight`（非终态集合）/ `list_by_vehicle` / `status_counts`）、`collector.py`（EventRepository：幂等唯一键查询 / 车辆时间线 / 未确认告警 / `acknowledge`（幂等、保留首次审计）；VehicleTelemetryRepository：`insert_points`（`ON CONFLICT (time, vehicle_id) DO NOTHING`，executemany）/ `get_point` / `list_points` / `latest_point` / `purge_before`）、`analytics.py`（`insert_metrics`（四列主键幂等）/ `list_series` / `latest`）；`REPOSITORY_BY_MODEL` 作为模型 ↔ 仓库唯一登记点并随包导出（错误码沿用 2001/3001/3002）
4. **`BaseRepository` 新增三个通用原语（`repository.py`）**：`find_one` / `find_all`（ORM 列表达式条件、参数绑定、自动附加软删除过滤、`limit ≤ 200` 校验）、`delete_where`（条件删除并返回行数；无条件调用抛 2001，防误删整表）；三者是各 Repository 专属方法复用的基础，未改动既有方法语义
5. **新增单元测试 82 条**：`test_orm_relationships.py`（10 条：契约关系表 ↔ 实现逐条一致、条数 16、lazy 白名单、mapper 零 SAWarning、RBAC 多对多 viewonly / 关联对象 cascade + passive_deletes、OTA 级联与 DDL 外键一致、逻辑外键不建关系、模型 ↔ Repository 一一对应、Repository 契约表 ↔ 注册表一致）、`test_repositories.py`（64 条：13 个 Repository 的**每个方法**行为与 SQL 语义，含参数绑定防拼接、索引列、`ON CONFLICT` 幂等、软删除过滤、默认排序与 LIMIT、`status_counts` 分组、`purge_before`）、`test_repository.py`（+8 条覆盖新原语的软删除过滤 / 排序 / 上限 / 非法字段拒绝 / 无条件删除拒绝）
6. **契约校验脚本扩展（`scripts/verify_data_layer.py`，26 → 31 项）**：新增校验 11（模型 ↔ Repository 一一对应且绑定正确）、校验 12（relationship 全部显式异步安全 lazy 策略）、校验 13（`orm-mapping.md` 关系表 / Repository 表 ↔ 实现同步）；契约文件齐备校验纳入 `orm-mapping.md`
7. **文档更新**：`common/python/README.md`（数据访问层模块说明与用法示例改为契约驱动写法：分页/软删除/车辆状态/遥测批量写入/注册表）、`contracts/database/README.md`（契约清单、约束、校验命令）、新增 `docs/data-layer.md`（数据访问层开发文档：契约驱动流程、关系与 lazy 策略、Repository 清单与事务边界、性能与安全要点、常见错误）、`docs/README.md` 登记新文档

验证：`pytest common/python -q` **244 passed**（较变更前 162 新增 82）；`python scripts/verify_data_layer.py` **31 项通过 0 项失败**（含新增 3 项）；`ruff check common/python scripts tests` **All checks passed**；服务回归（逐服务运行，**411 passed**）——api-gateway **66**、scene-service **100**、data-collector **52**、data-analytics **60**、ota-service **98**、remote-control **35**；ORM 映射 `configure_mappers()` 零告警（测试内断言）。

风险与待人工确认：
① **ORM relationship 是纯增量能力**：现有各服务 Repository（`services/*/app/repositories/*.py`）仍以显式 `select()` 实现 CRUD，未强制切换到 `hunter_common.database.repositories`；两者语义一致但存在**实现重复**，是否统一收敛（服务层 Repository 改为继承通用 Repository，或注入共享实现）需人工决策——本次不做跨服务重构以避免影响既有 3xx 条服务测试。
② **`raise_on_sql` 语义收紧**：`OtaVersion.tasks` / `OtaTask.records` / 关联对象 / 多对一使用 `raise_on_sql`，未显式 `selectinload` 即抛 `InvalidRequestError` 是**刻意**的（防异步隐式 IO 与大集合加载）；若后续业务需要「懒加载式」访问，须先在契约中评审并改为 `selectin`。
③ **`selectin` 会增加 SQL 条数**：查询 `OtaRecord` 时自动预取 `task`、查询 `Role` 时自动预取 `permissions`，对 P95 ≤ 200ms 属可接受（均为 `IN` 批量），但高基数报表查询需按需 `noload` / 显式列选择，需在性能评审中确认。
④ **`purge_before` / `delete_where` 是物理删除能力**：`vehicle_telemetry` 的 90 天保留仍由 TimescaleDB 保留策略（chunk 级 drop）负责，`purge_before` 仅供测试数据清理与应急；生产定时清理脚本若使用它需人工确认（否则造成 chunk 膨胀）。
⑤ **软删除仅 `scenes`**：`soft_delete()` 对其他模型抛 `NotImplementedError`（契约：其余表用状态字段表达生命周期）；若设计文档要求 `ota_versions` / `users` 也软删除，需先改 DDL。
⑥ `OtaVersionRepository.max_version_code` 与 `services/ota-service/app/repositories/versions.py::max_published_code` 逻辑重叠（后者带 `status=published` 语义）：发布门禁的**唯一实现**归属需人工确认（建议服务层复用通用实现）。
⑦ `UserRepository.list_role_codes` / `list_permission_codes` 为 user_svc schema 内 RBAC 五表 JOIN（符合「禁止跨服务直查」），但与 api-gateway 现有 `UserRepository` 实现重叠；网关是否切换待确认（涉及鉴权链路，建议独立变更 + 全量回归）。
⑧ **既有问题登记（非本次引入）**：`pytest services`（或任意两个服务测试目录同批收集）会因 pytest 插件重复注册报 `ValueError: Plugin already registered` 而中断收集——已在 HEAD 干净检出（`git worktree`）复现确认与本次变更无关，本次改用逐服务运行完成回归；根因（root `tests/conftest.py` 的 `pytest_addoption/pytest_configure` 在多 rootdir 收集下的注册冲突）建议独立修复。另 `ruff check services` 存在 43 项告警（全部位于 `services/ota-service`，新版 ruff 对 `# noqa: E402` 的判定变化），本次未改动 `services/`，故未处理。

## V1.10.1（修复）

Date：2026-9-19
变更类型：修复

**数据访问层审查整改：排序空值语义 / 事务边界 / 字段校验 / 复合主键保护 / 契约方法级校验（按代码审查报告优先级 1→5 逐项修复）**

1. **🔴 默认排序空值语义与契约/索引反转（R1）**：`BaseRepository.apply_order` 新增排序 spec 后缀解析 `:nl`（`NULLS LAST`）/ `:nf`（`NULLS FIRST`），并引入 `order_clause`；修正三处默认排序——`OtaVersionRepository` → `release_time DESC NULLS LAST, version_code DESC`（对齐 openapi/ota-service.yaml 与 `idx_ota_versions_status_release_time`）、`VehicleRepository` → `last_online_time DESC NULLS LAST, vehicle_id`（在线看板不再把"从未上线"车辆排最前）、`OtaRecordRepository` → `start_time DESC NULLS LAST, record_id DESC`（次级键修复同 start_time 的分页抖动）；`list_inflight` 排序补次级键并更正"与索引列序一致"的错误注释。此前 `DESC` 等价 `NULLS FIRST`，同时造成业务语义反转与索引排序失效（多出 `Sort`，威胁 P95 ≤ 200ms）。
2. **🟡 事务边界（R2）**：`create` / `update` / 批量分片改用 SAVEPOINT（`session.begin_nested()`）局部回滚，`create` 失败后 `expunge` 失败行——调用方捕获 3002/2001 后外事务与已写分片保留，不再出现"仓库内部 `rollback()` 静默丢弃调用方待提交数据"与"失败行残留导致下次 flush 重放"的缺陷。
3. **🟡 批量路径异常翻译与字段白名单（R3/R4）**：`bulk_create` / `bulk_create_ignore_conflicts` / `update` 统一走 `translate_integrity_error`（此前批量冲突直接冒泡为 5000）；新增 `ensure_known_columns` 写路径列名白名单（`create` / 批量行键），非法字段由 `TypeError → 5000` 修正为 **2001**（对齐契约 3.1），并按模型缓存列名集合（O(1) 复用）。
4. **🟡 复合主键保护（R5）**：新增 `pk_names` / `is_composite_pk` / `require_single_primary_key`；`vehicle_telemetry` / `algorithm_metrics` 调用基类 `get` / `get_or_raise` / `hard_delete` 抛 `NotImplementedError`（首列 `time` 会读取他车数据、`hard_delete` 会删除所有车辆同一时间点的数据），引导使用 `get_point` / `list_points` / `list_series`。
5. **🟡 显式加载与读取上限（A2/P2）**：`get` / `get_or_raise` / `find_one` / `find_all` / `list` / `paginate` 新增 `options: Sequence[ORMOption] | None`，使契约要求的 `raise_on_sql` 关系可经 Repository 显式 `selectinload`（此前只能绕过共享层自写查询）；新增 `max_query_limit` 与 `MAX_SERIES_POINTS = 10000`，时序序列读取（轨迹回放/指标趋势）不再被 200 上限卡死，`paginate.page_size` 仍恒 ≤ 200。
6. **🟢 安全与性能优化**：`translate_integrity_error` 的 `details` 只保留模型名 / SQLSTATE / 约束名，移除驱动原始报错文本（asyncpg 文本含 `Key (username)=(admin)` 等可枚举行值）；`EventRepository.acknowledge`、`VehicleRepository.update_status`（`UPDATE ... RETURNING`）、`UserRepository.touch_last_login` 改为单条原子语句（1 次往返，消除"先查后写"的 TOCTOU，保留首次确认审计语义）；`EventRepository` 新增 `insert_events`（`ON CONFLICT (vehicle_id, event_type, event_time) DO NOTHING`，补齐事件写入幂等路径，`EVENT_IDEMPOTENCY_COLUMNS` 不再悬空）；`purge_before` 改为按 `ctid` 分批删除（默认 5000/批 + `max_rows` 上限，短事务避免长事务锁与 WAL 膨胀）；`bulk_create_*` 返回值语义在 docstring / 契约 / 测试中统一为"提交（attempted）行数"（依据 `PGDialect_asyncpg.supports_sane_multi_rowcount = False`）；删除未使用的模块级 `logger`；修正 `AlgorithmMetricRepository.latest` 的索引说明。
7. **🟡 契约先行与校验强化（A1/F1）**：`contracts/database/orm-mapping.md` 新增 §3.1 通用方法表区块标记（`base-methods-table`）、§3.2 排序 spec 与默认排序表、§3.3 读取上限、§3.4 显式加载、§4 服务层 Repository **收敛路径与迁移期纪律**；新增**专属方法双向契约校验**（`tests/test_orm_relationships.py::test_repository_methods_match_contract` + `scripts/verify_data_layer.py` 校验 13b：契约列出的 43 个专属方法必须实现，实现侧公开方法必须登记契约）。
8. **测试与文档**：新增/改写 21 条用例（排序 spec 与 `NULLS LAST` 断言、SAVEPOINT 局部回滚与 expunge、批量冲突翻译、非法行键 2001、复合主键拒绝、`options` 透传、`ctid` 分批清理与 `max_rows`、时序读取上限、`insert_events` 幂等、原子 `acknowledge` / `update_status` / `touch_last_login`、行参数深拷贝断言）；同步 `common/python/README.md`（用法示例 + 使用要点表）、根 `README.md`（数据层要点）、`docs/data-layer.md`（规则/性能/排查/验证命令/待确认项）。

验证：`pytest common/python -q` **265 passed**（较整改前 244 新增 21）；`python scripts/verify_data_layer.py` **32 项通过 0 项失败**（新增校验 13b）；`ruff check common/python scripts tests` **All checks passed**；`mypy common/python/hunter_common` **仅 4 处既有告警**（`logging.py:143`、`kafka/contracts.py` 缺 `types-PyYAML`/`types-jsonschema` stub，均非本次引入）；服务回归逐服务运行（api-gateway / scene-service / data-collector / data-analytics / ota-service / remote-control，见下）。

风险与待人工确认：
① **真库运行时语义未验证**：本次修复的排序 `NULLS LAST` 实际行序、SAVEPOINT 回滚范围、ORM `UPDATE ... RETURNING` 的 identity-map 合并、`ctid` 分批 DELETE 在 hypertable 上的行为，均以"契约 + PG 方言编译 + Stub Session"验证；建议补 `pytest.mark.integration` + docker-compose Postgres 用例（已登记 `docs/data-layer.md` 第 7 节）。
② **服务层过渡实现仍在**：`services/*/app/repositories/*.py` 与服务层既有测试期望未改动（本次为共享层内部整改），收敛顺序与迁移期纪律已写入契约 §4，**具体排期/责任人待定**；收敛前禁止同一模块混用两套 Repository。
③ **`EventRepository.acknowledge` 语义不变但调用方需注意实体新鲜度**：改为条件 `UPDATE` 后不再返回/改写 ORM 实体（仍返回 `bool`），持有旧实体的调用方需重新查询或依赖 `synchronize_session="auto"` 的会话同步。


## V1.10.2（修复）

Date：2026-9-19
变更类型：修复

**数据访问层代码审查整改（按审查报告 R1→R11 逐项修复；含真库/真实 Session 取证复现）**

1. **🔴 R1/R2 写路径 SAVEPOINT 失效（契约违背 + 错误码退化 5000）**：`BaseRepository.create` 原实现
   `session.add()` 位于 `begin_nested()` **之前**，而 SQLAlchemy 进入 SAVEPOINT 时会先 `flush()` 未决变更
   （`SessionTransaction._take_snapshot`），INSERT 实际发在 SAVEPOINT **之外** —— 唯一冲突时整个会话事务被回滚
   （后续 `commit()` 抛 `PendingRollbackError`，调用方已写数据全部丢失），且 SAVEPOINT 回滚已把失败行移出会话，
   紧接着的 `session.expunge()` 抛 `InvalidRequestError`，使契约要求的 **3002 退化为 5000**（HTTP 409 → 500）；
   `update` 的 `setattr` 同样落在 SAVEPOINT 之外（SAVEPOINT 形同虚设）。现改为 `add` / `setattr` 与 `flush`
   全部在 `begin_nested()` 内、删除 `expunge`，并把「写路径纪律」写入模块 docstring 与契约 §3。
   取证：用真实 `BaseRepository` + async SQLite/真实 Session 复现（旧实现：`InvalidRequestError` +
   `PendingRollbackError`；新实现：平台预定义异常且冲突后仍可 `commit`）。
2. **🟡 R3 清理语义澄清与上限**：`purge_before` 改为**语句级**分批（原注释"每批一个短事务"与实现相反：
   Repository 不 commit，所有批次同处调用方一个事务）；新增 `max_batches`（默认 `DEFAULT_PURGE_BATCH_LIMIT=1000`）
   防止无上限循环长期占用连接；docstring 明确"大表清理须由调用方按批 commit，否则仍是长事务"。
3. **🟡 R4 时序读取强制时间窗**：`list_points` / `list_series` 要求 `start_time` / `end_time` 至少其一，否则 **2001**
   （原实现 `limit=None` 时不生成 LIMIT，可全量扫描 hypertable）；新增 `BaseRepository.require_time_window`。
4. **🟡 R5 测试守护升级**：新增 `common/python/tests/test_repository_transactions.py`（真实 `AsyncSession` +
   `sqlite+aiosqlite`，**无需容器**，unit 阶段守护：平台异常、冲突后外事务可继续提交、失败行不重放、
   批量分片保留、`delete_where` 防呆）与 `tests/integration/test_data_layer_transactions.py`
   （真实 PostgreSQL + TimescaleDB：3002 精确断言、`details` 不含行值、`ctid` DELETE 在 hypertable 可执行、
   数据自清理）；修正 `StubSavepoint` 的错误语义假设并新增调用顺序断言（`savepoint → add/setattr → flush`）。
5. **🟡 R6 CI 单元阶段修复**：6 个服务共用顶层包名 `app`，同进程收集会 `ModuleNotFoundError: app.schemas.events`
   或 `ValueError: Plugin already registered`（实测：原 `.gitlab-ci.yml` 的单测命令与 `pytest` 全量收集必然失败）。
   新增 `scripts/run_unit_tests.py`（每个测试根目录独立 pytest 进程；L5 层按 `unit` 套件并容忍"无用例"退出码 5），
   CI unit 阶段改为调用它；`pytest.ini` 的 `testpaths` 收敛为 `common/python/tests tests` 并注明原因。
6. **🟢 R7 服务边界可校验**：契约新增 §3.5「服务 → 共享 Repository 白名单」，`scripts/verify_data_layer.py`
   新增**校验 14**（扫描服务源码中 `hunter_common.database*` 的 Repository 导入，越界即失败）与
   **校验 15**（写路径纪律 / 序列读取窗口规则区块锚点必须存在，防契约静默丢失约束）。
7. **🟢 R8 阈值溯源**：`MAX_PAGE_SIZE` / `BULK_CHUNK_SIZE` / `MAX_SERIES_POINTS` / `DEFAULT_PURGE_BATCH_SIZE`
   补「阈值来源：系统关键约束第 10 / 6 条」注释（规范要求标注来源章节）。
8. **🟢 R9 防误删加固**：`delete_where` 拒绝不引用本模型表列的条件（`find_tables(check_columns=True)` 判定），
   `literal_column("1=1")` 之类恒真条件不再绕过"防误删整表"守卫（真实会话同样生效）。
9. **🟢 R10 类型收窄**：`table` / `column` / `pk_column` 由 `Any` 收窄为 `Table` / `Column[Any]`；
   `translate_integrity_error` 返回类型收窄为 `HunterBaseException`。
10. **🟢 R11 语义澄清**：`EventRepository.acknowledge` docstring 明确"**事件存在即返回 `True`（无论是否首次确认）**，
    仅事件不存在返回 `False`"，消除"未命中即 False"的误读（语义与旧实现一致，已核对）。
11. **文档与依赖**：更新 `contracts/database/orm-mapping.md`（§3.1 写路径纪律 / §3.3 窗口规则 / §3.5 白名单 /
    §5 校验命令）、`docs/data-layer.md`（规则 / 性能表 / 排查表 / 验证命令 / 待确认项）、根 `README.md`、
    `common/python/README.md`；`common/python/pyproject.toml` dev extras 新增**测试依赖** `aiosqlite`
    （仅用于真实 Session 回归，运行时不引入、不改动技术栈）。

验证：`pytest common/python -q` **277 passed**（较整改前 265 新增 12）；`python scripts/run_unit_tests.py`
**8 个目标全部通过**（common 277 + api-gateway 66 + scene-service 100 + data-collector 52 +
data-analytics 60 + ota-service 98 + remote-control 35 = 411 服务用例，L5 层无 unit 用例按设计跳过）；
`python scripts/verify_data_layer.py` **34 项通过 0 项失败**（新增校验 14/15）；`python scripts/verify_test_layer.py` 通过；
`ruff check tests common/python scripts` **All checks passed**；新增
`pytest common/python/tests/test_repository_transactions.py` **5 passed**（真实 Session）；
`pytest tests/integration/test_data_layer_transactions.py` **4 collected / skipped**（本机无 Docker，有 Docker 的 runner 上执行）。

风险与待人工确认：
① **真库（PostgreSQL）语义仍需在有 Docker 的 runner 复验**：本次写入路径缺陷用 async SQLite + 真实 `AsyncSession`
复现并修复（ORM 层语义与方言无关），PG 侧断言（3002 / `details` / `ctid` DELETE）已固化为 integration 用例，
但本机无 Docker 未实际执行。
② **`aiosqlite` 为新增 dev 依赖**（仅测试；`common/python[dev]`）：如需保持依赖面不变，可改为仅在 integration
阶段跑写路径语义测试（代价：无 Docker 环境下失去该回归守卫）。
③ **SAVEPOINT 往返开销未量化**：修复后单条 `create` / `update` 在 PG 上会多出 `SAVEPOINT` / `RELEASE` 往返，
   需在性能评审中确认对单条写 P95 与高频写路径吞吐的影响。
④ **`purge_before` 在压缩 chunk 上的行为**：`ctid` 分批 DELETE 的 hypertable 可执行性已加入集成用例，
但 TimescaleDB 压缩 chunk 与保留策略（`add_retention_policy`）的职责边界仍需人工确认。
⑤ **R6 的彻底修复**：当前以"分进程运行"兜底（`scripts/run_unit_tests.py`）；各服务改用唯一顶层包名
（`app` → `scene_app` 等，涉及 Dockerfile / K8s command / 服务入口）属独立变更，需排期 + 全量回归。
⑥ **服务层 Repository 收敛**：共享层现已具备真库语义与边界校验，但 `services/*/app/repositories/*.py`
过渡实现仍在（契约 §4 迁移纪律不变）；建议先修 R1/R2 后启动 scene-service 收敛。
⑦ **服务端会话超时**：`DatabaseSessionManager` 未设置 `statement_timeout`（慢查询可能拖垮 P95），
是否按服务配置待评审（已登记 `docs/data-layer.md` 第 7 节）。

## V1.11.0（修复：系统代码审查整改）

Date：2026-9-19
变更类型：新增 / 修复 / 优化

### 🔴 P0：功能与契约红线

1. **补齐数据采集管道（审查 R1，功能断点）**：`data-collector` 此前**无任何 Kafka 消费者**，
   导致 `vehicle_telemetry`/`events` 永不写入、`vehicle:status:{id}`/`vehicle:online:set`
   无写入方（OTA 门禁恒拒、远程接管恒 4001）。本次按契约落地三路消费链路：
   - `app/consumers/base.py` + `telemetry.py` + `health.py` + `events.py`：消费组/订阅模式/幂等键
     严格对齐 `consumer-groups.yaml`（`data-collector-telemetry/health/events`），
     `schema_name="auto"` 契约校验 + 手动提交 + DLQ；
   - `app/services/ingest.py`：5.4 节 6 步流水线（校验 → 时间戳对齐（毫秒归一/时钟漂移）→
     数据清洗（NaN/Inf 丢弃、值域裁剪）→ enrichment 扁平行映射（与读路径严格对称）→
     批量入库 + `telemetry_raw`/`telemetry_clean` 投递）；批缓冲由**批次收尾钩子**驱动，
     保证「写库成功才提交 offset」（失败回填缓冲并重投，写侧 `ON CONFLICT` 幂等）；
   - `app/services/vehicle_status.py`：读模型唯一写方（health 写状态/系统段；telemetry 刷新
     电量/速度/心跳）+ 车辆状态守护（心跳超 10s → 置 offline 并 SREM，
     落实 redis-keys pending #5 建议）；
   - `app/producers/pipeline.py`：内部 Topic 投递一律走契约校验入口 `publish_payload`
     （同时修复审查 Y11：既有生产者全部绕过 Schema/key 校验）；
   - 共享库 `KafkaConsumerManager` 新增 `on_batch_end` 批次收尾钩子（提交 offset 前执行，
     钩子失败跳过提交 → 消息重投）。
2. **文件上传 SHA-256 内容校验（审查 R2，契约违反）**：`/files/complete` 此前仅比对
   对象长度与（单分片对象的）ETag，**sha256 从未参与校验**却返回 `verified=true`，
   multipart 对象连 MD5 都不校验。现新增 `MinioStorage.stream_hashes`（1 MiB 分块流式计算
   size/MD5/SHA-256）并在 `_verify_integrity` 中三项逐一比对（任一不符 → 6001 + expected/actual）。
3. **CI lint 红灯修复（审查 R3）**：`ruff check tests common/python services` 原有 **43 个错误**
   （均在 ota-service，含 F821 `AsyncIterator` 未导入 ×2、F401 ×2、F841 ×1），与
   `release.md` 声称的 "ruff All checks passed" 不符。现全部修复；5 个服务补齐
   `[tool.ruff]` 配置（与 api-gateway 一致），`ruff` 锁定 `>=0.16,<0.17` 防版本漂移，
   并统一 `datetime.UTC` 写法（UP017 ×23）。
4. **Kafka 本地缓冲同步 IO 阻塞事件循环（审查 R4）**：断网落盘（`open/write/fsync` + 目录扫描）
   与重投的 `read_bytes`/`os.replace` 原在事件循环内直接执行。现全部经 `asyncio.to_thread`
   下沉线程池，并新增回归用例（慢 fsync 期间并发协程可调度、线程归属断言）。

### 🟡 P1：安全与可靠性

5. **日志递归脱敏（审查 Y1）**：原 `_mask_sensitive` 仅处理顶层 key，
   `logger.info("x", payload={"password": ...})`、`[{"token": ...}]` 会明文落盘（实测确认）。
   现递归处理 dict/list/tuple/set（深度上限 6），并掩码 `Bearer <token>` / PEM 私钥形态；
   新增 3 条嵌套/形态/深度用例。
6. **生产凭据强校验（审查 Y2）**：`staging/prod` 环境沿用开发默认凭据
   （`hunter_dev_123` / `minioadmin` / `change-me-in-production`）或 JWT 密钥 < 32 字节
   → **启动即失败**并列出未覆盖项；dev/test 保留默认值。
7. **陈旧操控会话守护 + 会话硬 TTL（审查 R7）**：`rc:session:{vehicle_id}` 原无 TTL 且唯一
   清理路径是 `DELETE /session/{id}`，副本崩溃即永久占用车辆互斥位（后续接管恒 7001）。
   现创建会话时写硬 TTL（`RC_SESSION_TTL_S` 默认 6h），并新增 `SessionReaper` 周期对账：
   心跳超 `RC_HEARTBEAT_END_AFTER_S` → 以 `heartbeat_timeout` 走完整「先安全后清理」
   （stop 帧 + session_end → 删 Hash → sidecar 归档）；会话收尾逻辑抽为
   `_finalize_session` 供操作员结束与系统强制结束复用。
8. **fire-and-forget 任务治理（审查 Y9）**：OTA 进度缓存写入改为「强引用集合 + 完成回调
   （记录异常）+ 停机 `aclose()` 等待收尾」，避免任务被 GC 回收或异常无人观察。
9. **网络策略纵深防御（审查 R5）**：新增 `infra/k8s/networkpolicies/`——默认拒绝入向 +
   仅放行网关→后端（8081-8085）与 monitoring 命名空间抓取，收敛「任意 Pod 伪造
   `X-User-Id`/`X-Roles` 绕过 JWT/RBAC」路径（彻底消除仍需网关↔后端 mTLS）。

### 🟡 P2：契约、性能与可观测

10. **OTA 验证开关如实回报（审查 Y3）**：`OTA_PACKAGE_VERIFY_SHA256/SIGNATURE=false` 时
    `checks.sha256_verified/signature_verified` 仍返回 true（假声明）。现如实返回开关状态，
    关闭验签时记 WARNING 审计日志。
11. **批次下发批量取数（审查 Y4）—— 未实施，登记为已知性能项**：
    现状仍为逐车 2 次 Redis 往返（`is_online` + `get_status`）且在 `SELECT ... FOR UPDATE`
    事务内，1000 台批次最坏数千次串行往返（秒级，占用行锁与连接池）。
    **未实施原因**：需扩展 `VehicleStateReader` 协议（批量 `online_members()` + `status_snapshot()`）
    并同步改造 ota-service 读模型实现与测试替身，属跨模块契约级改动，风险与本次整改范围不符，
    应先确认「大车队批次下发是否允许异步化（202 + 后台推进）」再落地。
    推荐路径：①读模型批量获取（`smembers` + 管道 `hgetall`）→ 门禁在内存求值
    （`evaluate_gates` 已是纯函数）；②门禁计算移出事务；③通知投递改 `asyncio.gather` + 信号量。
12. **看板事件块并发取数（审查 Y5）**：6 次 data-collector 查询由串行改 `asyncio.gather`
    并发，避免累积 6 次网络往返突破 P95 ≤ 200ms。
13. **data-analytics 就绪探针改用只读账号（审查 Y10）**：不再初始化全权限
    `DatabaseSessionManager`（本服务仅持 `hunter_analytics_ro`，契约 x-hunter-db-readonly），
    `/readyz` 复用只读池 `ping()`。
14. **契约目录运行时挂载**：新增 `scripts/generate_contracts_configmap.py`
    （生成 `infra/k8s/base/03-configmap-contracts.yaml`，键保留 `schemas/` 相对路径）+
    CI `--check` 一致性校验；`data-collector` Deployment 挂载至 `/app/contracts/kafka`
    并由 `KAFKA_CONTRACT_DIR` 指向，保证集群内 `schema_name="auto"` 可用。
15. **死信队列失败可观测性（审查 Y7）**：DLQ 关闭 / 生产者未初始化 / 转投异常三条路径
    原仅记日志（消息随 offset 提交而丢失），现统一上报
    `hunter_kafka_dlq_failed_total{service,group,topic,reason}` 指标，可告警、可人工溯源重放。
16. **死代码与冗余清理（审查 G1）**：遥测仓储重复赋值行删除。

### 验证

- `python scripts/run_unit_tests.py`：**773 passed**（common 292 / api-gateway 66 / scene 100 /
  data-collector 114 / data-analytics 60 / ota 99 / remote-control 42；8 个目标各自独立进程）
- `ruff check tests common/python services`：**All checks passed**
- `python scripts/verify_infra.py` / `verify_data_layer.py`（34 项）/ `verify_test_layer.py`：全部通过
- `python scripts/generate_contracts_configmap.py --check`：契约清单一致
- 关键回归均为「先验证测试能捕获缺陷、再修复」：R2（3 条 sha256/ETag 用例在旧逻辑下失败）、
  R4（慢 fsync 用例在旧逻辑下阻塞 3s 并断言失败）、Y1（嵌套敏感字段实测泄露）

### 本次未实施（登记备案，避免文档与实际不符）

| 项 | 审查编号 | 现状与原因 |
|----|---------|-----------|
| 批次下发批量取数 | Y4 | 见上文第 11 条：需跨模块扩展读模型协议，建议与「批次异步化」决策一并落地 |
| 消费幂等认领语义（claim → handler → commit） | Y6 | 现状：认领先于 handler；「handler 失败 + 未提交 + 崩溃」窗口内消息可能被跳过（转 DLQ 后需人工重放）。改为「成功后记账」或失败释放认领需评估跨服务写侧约束（数据库唯一索引已兜底），留待下一轮 |
| 网关与共享库 `UserRepository` 双份实现合并 | Y12 | 两处 RBAC 展开 SQL 语义一致但未合并（网关仓储有归属说明）；合并需触碰共享数据层契约（`verify_data_layer.py` 白名单），建议随数据层演进一并处理 |
| 灰度批次自动调度器 | Y8 | 配置 `OTA_ROLLOUT_TICK_SECONDS` 悬空；当前为人工 `POST /tasks/{id}/start` 推进，自动推进须先确认契约（已列入待人工确认） |
| Flink / Spark 作业 | R1 余项 | 本次仅打通「采集 → 落库/投递」；`flink-jobs/`、`spark-jobs/` 仍为占位 README（实时告警 ≤2s 与离线分析未实现） |

### 待人工确认（未决契约项，实现已按安全默认处理）

- 签名载荷语义（对 SHA-256 摘要签名）需与车端验签规则逐字节对齐；
- `vehicle:status` 字段来源：`gear`（P 档）/`free_storage_mb` 在 health/telemetry Schema 中无来源，
  OTA 门禁对应项仍无法满足 → 须先定稿契约字段（redis-keys pending #1/#2）；
- 灰度批次推进仍为人工触发（`POST /tasks/{id}/start`）：自动调度器需契约确认后落地（审查 Y8）；
- 后端身份头信任的彻底消除需 mTLS（NetworkPolicy 为第一道防线）。


## V1.12.0（新增：系统详细部署配置）

Date：2026-9-19
变更类型：新增 / 修复

1. **镜像与清单版本变量化（部署约束：镜像标签使用 `{version}` 变量）**：`infra/k8s/services/*.yaml`
   6 个微服务的 `image` 由硬编码 `hunter/<svc>:0.1.0` 改为 `hunter/<svc>:{version}`；新增渲染脚本
   `scripts/render_k8s.py`（`--version` 渲染到 `build/k8s/`、`--check` 仅校验占位符；版本白名单
   `v?X.Y.Z[-pre]` 禁止 `latest`；排除 `*.example.yaml` 避免误 apply 占位密钥；输出目录禁止落在
   `infra/k8s` 内；渲染后逐文件报告替换处数）；各服务 `Dockerfile` 构建注释同步为 `$VERSION`。
2. **新增 HPA（`infra/k8s/autoscaling/hpa.yaml`）**：6 个微服务 `autoscaling/v2`，minReplicas=2 /
   maxReplicas=5 / CPU 平均利用率 70%；scaleUp 30s 窗口（Percent 100% 或 +2 取大者）、scaleDown
   稳定窗口 300s + 每分钟至多 -1 副本（防抖）；有状态中间件禁止 HPA；清单内逐服务标注 3 副本基线
   与 HPA 取值冲突的确认点、data-collector 受分区数约束、remote-control 长连接缩容风险。
3. **新增 PDB（`infra/k8s/disruption/poddisruptionbudgets.yaml`）**：6 个微服务 `policy/v1`，
   3 副本基线 → `minAvailable: 2`、2 副本基线 → `minAvailable: 1`；覆盖 `kubectl drain` / 集群升级
   等自愿中断，与 Deployment 的 `maxUnavailable: 0` 互补，支撑可用性 ≥ 99.9%。
4. **探针参数对齐部署要求**：6 个服务 `livenessProbe` / `readinessProbe` 调整为「初始延迟 10s /
   周期 5s」（readinessProbe 补 `initialDelaySeconds`），`startupProbe` 与探针路径（`/healthz`、
   `/readyz`）保持不变（路径已在各服务 OpenAPI 契约 `ops_endpoints` 登记，不得新增 `/health`）。
5. **Dockerfile 容器级健康检查**：6 个服务新增 exec 形式 `HEALTHCHECK`（`python3 -c` 探测
   `http://127.0.0.1:<port>/healthz`，interval 30s / timeout 3s / start-period 20s），
   兼容 distroless 无 shell；K8s 侧仍由 `livenessProbe`/`readinessProbe` 承担（Dockerfile 级检查被忽略）。
6. **基础设施校验器扩展（`scripts/verify_infra.py`）**：API 版本白名单新增 `autoscaling/v2`、`policy/v1`；
   新增 3 项校验——① 自有镜像必须使用 `{version}` 占位符且 6 个微服务镜像齐备（含渲染脚本存在性与
   占位符引用）；② HPA 契约（目标为微服务 Deployment、名称一致、min/max/CPU 70%、必须配置
   `scaleDown.stabilizationWindowSeconds`、6 个服务齐备）；③ PDB 契约（selector 指向微服务、
   必须声明 `minAvailable`、`minAvailable` < 基线副本数）；Dockerfile 校验新增 HEALTHCHECK 要求。
7. **CI 流水线补全（`.gitlab-ci.yml`）**：阶段补为 lint → unit → integration → e2e → performance →
   build → deploy → report；lint 阶段新增 `render_k8s.py --check` 与 `verify_infra.py`；新增
   `build-images`（tag 流水线构建推送 6 镜像，标签 = `$CI_COMMIT_TAG`）、`render-manifests`
   （渲染 `{version}` 并以制品传递，避免“渲染与部署不一致”）、`deploy-prod`（manual + production
   环境，按 `infra/k8s/README.md` 顺序 apply 并等待 rollout，Secret 不入流水线制品）。
   同时修复存量缺陷：`report` 任务的内联 Python heredoc 直写在 `- ` 之后，导致整份 CI 文件 YAML 非法
   （`ScannerError: mapping values are not allowed here`），改为块标量（`|`）书写。
8. **文档更新**：新增 `docs/deployment.md`（部署拓扑与 K8s 对象映射 / 镜像构建 / 清单总览 /
   资源配额与连接池核算 / 版本变量链路 / 探针与优雅终止 / HPA / 安全基线 / 部署顺序 /
   验证清单 14 项 / 风险与人工确认点 11 项）；更新 `infra/k8s/README.md`（目录结构、版本渲染步骤、
   HPA/PDB 章节、设计决策与排查表）、`infra/README.md`、根 `README.md`（K8s 章节与验证命令表）、
   `docs/README.md`。

验证：`python scripts/verify_infra.py` **全部校验通过**（85 个 K8s 文档 + 6 个 Dockerfile：镜像版本变量 /
HPA 契约 / PDB 契约 / 端口 8080-8085 / 探针 / 资源配额 / 敏感字段 / Kafka Topic 与 MinIO Bucket 契约 /
多阶段-非 root-健康检查）；`python scripts/render_k8s.py --check` 占位符校验通过（6 个微服务镜像）；
`python scripts/render_k8s.py --version 0.1.0` 渲染 **19 个清单、替换 6 处**，产物目录不含示例 Secret
模板，渲染后 YAML 解析通过（`image=hunter/api-gateway:0.1.0`、liveness/readiness 延迟 10s 周期 5s、
HPA 6 份 min2/max5/CPU70%）；契约测试回归 `pytest services/scene-service/app/tests/test_scene_contract.py`
**29 passed**、`pytest services/data-collector/app/tests/test_data_collector_contract.py` **48 passed**
（两服务契约测试均解析 `infra/k8s` 清单 / Ingress / 契约 ConfigMap）；`ruff check scripts/render_k8s.py
scripts/verify_infra.py` **All checks passed**；`.gitlab-ci.yml` YAML 解析通过（8 阶段 / 9 任务）。

风险与待人工确认：① 资源配额数值仍为“按负载特征推导”，需与设计文档 15.2 节逐项核对（含 PVC 容量与
`max_connections=300`）；② HPA `minReplicas=2` 与 api-gateway / data-collector / remote-control 的
3 副本基线冲突（低负载可能缩到 2），如需“永不低于 3 副本”须改 `minReplicas`；③ HPA 与 PDB 叠加时缩容到
2 副本会让 `minAvailable: 2` 阻塞 drain，建议改用 `maxUnavailable: 1`；④ remote-control 长连接副本不迁移
（契约 pending #14），缩容中断在线操控会话，是否冻结缩容待确认；⑤ `terminationGracePeriodSeconds` 未统一为
30s（data-collector / data-analytics / ota-service / remote-control 为 60s，契约已固化消费提交与会话收敛
语义，不建议降为 30s）；⑥ readinessProbe 初始延迟 10s 在已有 startupProbe 前提下略显保守，是否回调为 0
待确认；⑦ 前端管理后台未纳入部署清单（`CORS_ORIGINS` 已引用 console 域名，容器端口需设计文档确认，
禁止自行预设端口）；⑧ HPA 依赖 metrics-server（清单未包含，需集群侧具备）；⑨ 渲染产物 `build/k8s/` 已被
`.gitignore` 忽略，直接 apply `infra/k8s/` 源清单会出现 `ImagePullBackOff`（镜像名含 `{version}`），
已在 README 与排查表显式提示；⑩ Kafka `ssl.client.auth` 仍为 `requested`（非严格 mTLS），切换 `required`
需应用侧补 `KAFKA_SSL_CERTFILE/KEYFILE`。

## V1.12.1（修改：系统更名 HunterEdge → HunterCore）

Date：2026-9-19
变更类型：修改

命名口径：**HunterCore = 云端数据采集与分析系统（本仓库）**；**HunterEdge = 车端自动驾驶系统**（本仓库
不包含车端代码，仅通过 Kafka Topic / REST 契约与其交互）。本次将仓库内所有指向“平台自身”的 HunterEdge
命名统一改为 HunterCore，消除产品名与车端系统名混淆。

1. **全量更名（大小写 / 分隔符全变体）**：`HunterEdge → HunterCore`、`hunter-edge → hunter-core`
   （kebab-case：命名空间/域名/对象键/资源名前缀）、`hunter_edge → hunter_core`（snake_case：数据库名）、
   `hunteredge → huntercore`（看板 tag）、`HUNTER_EDGE/HUNTER-EDGE → HUNTER_CORE/HUNTER-CORE`。
   共 **126 个文本文件 / 461 处命中**（其中受版本控制的 121 个文件、419 行改动；其余为 `docs/`、
   `release.md`、`.clinerules` 等本地文档）。前端构建产物 `frontend/dist/` 与 npm 日志缓存
   `frontend/.npm-cache/` 未做文本改写，由 `npm run build` 重新生成（已验证构建成功）。
2. **契约层（单一事实来源）**：`contracts/openapi/*.yaml`（`info.title` 改为 `HunterCore - <service>`、
   `info.contact.name` 改为 `HunterCore Platform Team`、示例域名 `api.hunter-core.example.com`、
   STS/TURN 示例 `stun.hunter-core.example.com`）、`contracts/kafka/topics.yaml`、`consumer-groups.yaml`、
   9+2 份 JSON Schema（`$id` 改为 `https://hunter-core.local/...`、`title` 前缀、OTA 示例对象键
   `hunter-core/ota/...`）、`contracts/database/ddl/*.sql`（文件头与函数注释）、`object-storage.yaml`、
   `redis-keys.yaml`；契约自身的注释与示例同步，字段名 / Topic 名 / 表名 / 错误码 / 阈值**零改动**。
3. **共享库与服务**：`common/python/hunter_common`（`__init__`/`config`/`exceptions`/`database` 文档字符串与
   `postgres_db` 默认值）、`common/python/alembic.ini`、`pyproject.toml` 描述；6 个微服务
   `FastAPI(title=...)`、`scene-service` OpenSCENARIO 导出注释、`ota-service` 对象键模板
   `hunter-core/ota/{model}/{version_name}/{version_code}/package.tar.gz`、`remote-control` 默认
   `RC_PUBLIC_WS_BASE_URL` / `RC_STUN_URLS`；服务内契约测试中的标题与域名断言同步更新（保持“实现 ⊆ 契约”
   校验成立）。Python 包名 `hunter_common` / 镜像名 `hunter/<svc>` / Kafka Topic 前缀 `hunter.` **未改**。
4. **前端**：`package.json` 包名 `hunter-edge-web-portal → hunter-core-web-portal`（`package-lock.json`
   同步）、`index.html` 标题、`src/constants` 的 `APP_TITLE` 默认值、路由 `document.title`、全局样式注释、
   `vite.config.ts` 注释、`.env.development/.env.production/.env.example` 的 `VITE_APP_TITLE` 与代理注释。
5. **基础设施**：`infra/k8s/**` 命名空间由 `hunter-edge → hunter-core`（00-namespace、01-configmap-common、
   02-secret.example、03-configmap-contracts、services/*、statefulsets/*、jobs/*、ingress、autoscaling/hpa、
   disruption/poddisruptionbudgets、networkpolicies），标签 `app.kubernetes.io/part-of` 与
   `hunter-core.local/*` 同步；`infra/docker/{kafka,minio,postgres,nginx}*` 初始化脚本注释；
   `infra/monitoring/**`（Grafana 目录 `HunterCore`、看板标题与 tag `huntercore`、供給器 name、
   Prometheus 抓取/规则、Alertmanager 路由与邮件示例、exporters 的 kube-state-metrics 名称
   `hunter-core-kube-state-metrics` 与 PostgreSQL DSN 库名）；`docker-compose.yml`（`POSTGRES_DB`、
   注释、MinIO/Kafka 容器说明）。
6. **脚本与测试层**：`scripts/_templates_a/b/c.py`（骨架模板标题与包说明）、`scaffold_service.py`、
   `render_k8s.py`、`verify_infra.py`（`NAMESPACE`/`ALLOWED_NAMESPACES`/提示文案）、`verify_data_layer.py`、
   `generate_contracts_configmap.py`（`NAMESPACE` 与生成头）、`tests/`（`L5` 层说明、报告模板与报告器文案、
   `support/infra.py` 测试库默认名、e2e/集成用例中的哨兵值）。
7. **文档**：根 `README.md`（标题 `# HunterCore —— HUNTER 自动驾驶数据采集与分析系统`、验证命令中的库名、
   TLS Secret 名称、命名空间说明）、`infra/README.md`、`infra/k8s/README.md`、`infra/monitoring/README.md`、
   `common/python/README.md`、`contracts/*/README.md`、`frontend/README.md`、`docs/`（`deployment.md` 等）、
   `.gitignore`、`.gitlab-ci.yml`（部署任务中的命名空间与 `url: https://hunter-core.example.com`）。

验证：替换后**受控源码 / 契约 / 配置 / 基础设施 / 测试**中残留 **0 处**（`HunterEdge`/`Hunter Edge`/
`hunter-edge`/`hunter_edge`/`hunteredge`/`HUNTER_EDGE`；仅本 `release.md` 按变更记录要求引用旧名、
`frontend/.npm-cache/` 历史 npm 日志未改写）；逐文件校验「HEAD 内容 + 更名替换 == 当前内容」对 121 个受控
改动文件 **全部成立（0 个非纯替换）**，证明本次 diff 不含任何语义改动；
`python scripts/generate_contracts_configmap.py --check` **[PASS]**（生成物与
`contracts/kafka` 一致，证明契约与 ConfigMap 双端同步改名）；`python scripts/verify_infra.py` **全部校验通过**；
`python scripts/verify_data_layer.py` **通过 34 项 / 失败 0 项**；`python scripts/verify_test_layer.py` **4 项全 OK**；
`python scripts/run_unit_tests.py` **8 个目标全部通过**（六服务 + 共享库分进程执行）；`ruff check tests
common/python services scripts` **All checks passed**；`mypy --ignore-missing-imports
--disable-error-code=import-untyped tests/support scripts/verify_test_layer.py` **0 error**；
前端 `npm run build`（`vue-tsc --noEmit` + `vite build`）**成功**（dist 已用新名重建）；
L5 层 `pytest tests -q` **57 passed / 25 skipped / 18 failed**，18 项失败全部为 data-collector 与 ota-service
启动失败（`ValueError: Invalid endpoint: localhost:9000`，boto3 要求 endpoint 带协议头 → 本机测试环境
`MINIO_ENDPOINT` 缺 scheme，与本次更名无关）；已用 `git worktree` 在 HEAD（`442aa85`）原始代码上复现同两条
用例**同样失败**确认基线一致，且排除这两个服务后契约面子集 `pytest tests/integration/test_api_contract_surface.py
-k "not data-collector and not ota-service"` **32 passed / 4 skipped**（无更名相关回归）。

风险与待人工确认：① **K8s 命名空间 `hunter-edge → hunter-core`** 属破坏性变更，存量集群需先 `kubectl` 迁移
资源或重建命名空间；② **PostgreSQL 库名 `hunter_edge → hunter_core`** 需在数据库侧 `ALTER DATABASE ... RENAME`
或重新初始化，存量 `docker-compose` 数据卷（`POSTGRES_DB` 变更）不会自动迁移，本地需 `docker compose down -v`
重建；Alembic `sqlalchemy.url` 与 `.env`（`POSTGRES_DB`）需同步；③ **TLS Secret 名 `hunter-edge-tls →
hunter-core-tls`** 与证书 CN/SAN（`hunter-core-internal-ca`、`*.hunter-core.svc.cluster.local`）需在集群侧
重新签发，否则 Ingress 挂载失败；④ **示例域名 `hunter-edge.example.com → hunter-core.example.com`** 为占位
域名，生产域名/证书须以实际 DNS 规划为准（契约与配置中的域名仅作示例）；⑤ **OTA 对象键前缀
`hunter-core/ota/...`** 改变影响存量已上传包的对象键，历史版本记录需数据迁移或保持读取兼容；
⑥ **Grafana 看板 tag/目录改名**会导致既有看板归属目录变化（以 Git 为准，UI 手动调整会被覆盖）；
⑦ 前端 npm 包名变更后建议清理 `node_modules` 重装（`package-lock.json` 已同步，一般无需）；
⑧ 本仓库**不包含车端 HunterEdge 代码**，车端侧若有引用平台域名/命名空间（如 Carla 管理 API、
OTA 回调）需在车端仓库同步调整；⑨ 本机 L5 契约面 18 项失败根因是测试环境 `MINIO_ENDPOINT` 缺协议头
（应为 `http://localhost:9000`），与本次更名无关但是在库问题，建议修复测试环境变量后复跑完整套件。

## V1.13.0（新增：单机 Docker Compose 部署文档与部署包配置）

Date：2026-9-19
变更类型：新增
1. **新增单机部署包 `infra/deploy/`**（映射服务器 `/opt/hunter-edge`）：
   `config/daemon.json`（日志驱动 json-file 100MB×5 / overlay2 / live-restore / no-new-privileges）、
   `config/nginx.conf`（web-portal：静态资源 + `/api/` 反代 api-gateway:8080 + `/ws/` 反代 remote-control:8085
   + Vue Router history fallback + gzip + `client_max_body_size 1g` + CSP 等安全头）、
   `.env.example`（133 个变量：PG/Kafka SASL_SSL/Redis/MinIO/JWT/SRS/Flink/端口/备份，全部口令为 `CHANGE_ME_*`）、
   `README.md`（目录映射与契约来源）、`certs/`（占位，真实证书不入库）
2. **新增 SQL 部署产物（由契约生成，禁止手改）**：`sql/schema.sql`（`contracts/database/ddl/00-04` 拼接，
   含 8 schema + 11 张业务表：vehicles/users/roles/permissions/user_roles/role_permissions/scenes/
   ota_versions/ota_tasks/ota_records/events）、`sql/timescaledb.sql`（`ddl/05` 拼接，
   `data_collector.vehicle_telemetry` + `data_analytics.algorithm_metrics` 两张 hypertable，
   1 day chunk、90 天保留策略）、`sql/init-data.sql`（4 角色 + 16 权限点 + 角色→权限矩阵 + 默认 admin(bcrypt)
   + HUNTER-001 验证车辆，幂等且支持 `psql -v admin_password_hash` 注入）
   —— 角色编码来自 `contracts/openapi/api-gateway.yaml`，权限编码 `<resource>:<action>` 来自
   `contracts/database/enums.md` §8，矩阵来自各服务 RBAC 配置默认值（未自行发明编码）
3. **新增部署文档第一章** `docs/01-部署概述与环境要求.md`：系统简介与端口映射总表、与任务书的 10 项契约
   对齐差异、硬件三档配置与组件资源基线（取自 `infra/k8s` 配额）、磁盘/网络要求、OS 与内核验证、
   对外/内网端口清单、ufw 配置与「Docker 绕过 ufw」注意事项、ASCII 部署架构图与 6 条数据流说明、
   `/opt/hunter-edge`、`/data`、`/var/log/hunter-edge` 目录结构、需按环境修改的 13 项配置、12 项人工确认点
4. **同步更新**：`.gitignore`（新增部署包忽略规则：`infra/deploy/.env`、`certs/*`、`data/`、`backups/`、
   `*.pem/*.crt/*.keystore/*.truststore`）、`infra/README.md`（新增 `deploy/` 行）、
   `.env.example`（指引单机部署使用 `infra/deploy/.env.example`）

关于数据库形态（**待人工确认，阻断级**）：任务书要求 `hunter`（业务）+ `hunter_ts`（时序）双实例，
而仓库契约与 6 个微服务实现为**单实例 `hunter_core`**（8 个服务 schema 隔离，单一 DSN）；
本次按契约落单实例，`/data/timescale` 与 `TIMESCALE_*` 变量作预留。若确需双实例，须先更新
`contracts/database`、6 个服务配置与 Alembic 链，再同步部署文档。

验证：`daemon.json` 经 `ConvertFrom-Json` 校验通过；`nginx.conf` 花括号配平（16/16）并对
`add_header` 继承语义做了修正（`/assets/`、`/index.html` 改用 `expires` 以保留 server 级安全头）；
`schema.sql` 经比对含契约全部 11 张业务表、`timescaledb.sql` 含 2 张 hypertable 与保留策略；
`.env.example` 键名唯一性校验通过（133 键无重复）；`init-data.sql` 权限编码 16 个且无重复。
⚠ `nginx -t` 未执行（开发机无可用 Docker 守护进程/nginx 二进制），须在服务器上执行
`docker exec hunter-web nginx -t` 复核；SQL 未在真实 PG 上执行（须在第四章初始化步骤中验证）。

风险与待人工确认：① `infra/deploy/docker-compose.yml`（及 `config/srs.conf`、`config/flink-conf.yaml`、
`scripts/*.sh`）属后续批次交付，第一~五章命令暂不可整跑；② 数据库单/双实例形态（见上）；
③ MinIO Console 按任务书对外（9001），建议收紧为内网/VPN；④ 原始数据回传策略与 `/data` 容量需按实测复核；
⑤ Kafka 部署模式（ZooKeeper vs 仓库 KRaft）需统一确认；⑥ 单机无高可用（PG/Redis/MinIO 单点）；
⑦ HTTPS/TLS 与 TURN 未纳入本批次；⑧ 文档中资源配额数值需与设计文档 15.2 节逐项核对。

## V1.14.0（新增：单机一键部署脚本集 —— 公共库与主流程）

Date：2026-9-19
变更类型：新增
1. **新增 `scripts/common.sh`（554 行，被其他脚本 source 的公共库）**：彩色日志（`log_info/success/warn/error`，格式
   `[YYYY-MM-DD HH:MM:SS] [LEVEL] msg`，终端彩色 + 落盘无色，非交互终端/`NO_COLOR=1` 自动禁用颜色，
   日志不可写时静默降级且只探测一次）；检查工具（`check_root`、`command_exists`、`version_ge`、`check_port`
   （ss→netstat→lsof 回退）、`check_disk_space`、`wait_for`、`confirm`）；环境工具（`load_env`
   支持 `参数 > HUNTER_ENV_FILE > ${APP_DIR}/.env`、`env_value`、`require_env` 拒占位值）；Docker/Kafka/探针辅助
   （`compose`、`container_health`、`kafka_topics_cli`（内部监听非 PLAINTEXT 时自动注入 `--command-config`，
   与 `infra/k8s/jobs/kafka-init-job.yaml` 同策略）、`service_health_ok`、`minio_health_url`、`srs_health_url`、
   `flink_ui_url`）；`hc_run_logged` 统一"终端 + 日志文件"双写并避免重复落盘；库文件被直接执行时仅打印说明
2. **新增 `scripts/install.sh`（1266 行，13 步一键部署主脚本）**：`--help/--skip-docker/--skip-build/--env/--ip/--step/-y`；
   预检查（root、OS 版本（非 22.04 警告继续）、对外端口占用（本项目容器占用视为通过，支持重复部署与 `--step` 续跑）、
   `/data` ≥50GB、`/data` 独立挂载与 CPU/内存/swap 档位提示）；`show_plan` 展示 13 步并确认；
   13 个步骤函数 `step_1_system_init` … `step_13_print_summary`（apt 300s 超时+3 次重试、基础工具包、时区、主机名、
   hunter(UID1001)、limits.conf、sysctl（vm.max_map_count/net.core.somaxconn/vm.swappiness）、swapoff+fstab 注释、ufw；
   Docker 官方源安装 24.0+、daemon.json（优先部署包 `config/daemon.json`，内容一致则不重启）、hunter-net 网络；
   /data 与 APP_DIR 目录与属主（PG/Timescale/Redis=999:999、Kafka/ZK/MinIO=1001:1001）；.env 生成（委托 `gen-passwords.sh`，
   缺失时内置回退）+ SERVER_IP 强校验 + passwords.txt(600) + nginx.conf；Kafka 证书（幂等 + SAN 校验）；
   镜像构建（日志 `deploy/build.log`，失败打印末 50 行）；中间件按依赖就绪；DB/Kafka/MinIO 初始化调用；
   服务启动顺序（collector→analytics→Flink→scene/ota/remote→gateway→web，各自 `/healthz` 探针）；
   健康检查退出码 0/1/2 语义；部署摘要）；`run_step` 统一失败输出（错误 + 5 条排查建议 + `--step N` 续跑命令）并退出码 1
3. **新增 `.gitattributes`**：`*.sh/*.bash/*.sql/Dockerfile/*.yml/*.yaml/*.json` 统一 `eol=lf`（修复 Windows 检出
   CRLF 导致 `set: pipefail\r: invalid option name` —— 同时消除 `infra/docker/*.sh` 被挂载进 Linux 容器的潜在执行失败）
4. **路径基准**：`/opt/hunter-edge`（`APP_DIR` = 脚本父目录）、`/data`、`/var/log/hunter-edge`；
   可用 `HUNTER_APP_DIR/HUNTER_DATA_DIR/HUNTER_LOG_DIR/HUNTER_INSTALL_LOG/HUNTER_ENV_FILE` 覆盖（便于本地验证）

契约对齐决策（与任务书文本冲突处，按已落地契约执行，需人工确认）：
   ① 健康探针用 **`/healthz`**（`contracts/openapi/*.yaml` 的 `ops_endpoints`：[GET /healthz, GET /readyz]），
      任务书写的 `/health` 在实现中不存在；② Flink Web UI 宿主机端口用 **`FLINK_UI_HOST_PORT`（默认 8088）**
      （同主机无法同时绑定 8081 于 scene-service 与 Flink，docs/01 §1.4③）；③ 部署根/日志路径按 `/opt/hunter-edge`、
      `/var/log/hunter-edge-install.log`（docs/01 §1.2/§5、infra/deploy/README.md），任务书顶部的 `/opt/HunterCore`、
      `/var/log/hunter-core-install.log` 未采用；④ 数据库名/口令等一律从 `.env` 读取（`POSTGRES_DB=hunter_core`，不硬编码 `hunter`）；
      ⑤ 默认管理员口令取自 `sql/init-data.sql` 注释（`Hunter@2025`），未新增 `.env` 变量

验证：`bash -n scripts/common.sh`、`bash -n scripts/install.sh` **语法通过**（LF 版本）；
函数级自检 **全绿**：36 个 common 函数与 34 个 install 函数 `type -t` 全部为 function、`TOTAL_STEPS=13` 与
`STEP_DESCRIPTIONS` 数量一致、13 个 `step_*` 函数引用完整性检查无缺失；参数解析（`--step 7/-y/--skip-*/--ip`、
非法 `--step 99/abc`→1、未知参数→2、`--env` 缺值→1）；`show_plan` 输出 13 步；`run_step` 成功/失败路径与"排查建议"文案；
`generate_env_inline` 幂等且 `CHANGE_ME_` 残留 0、`passwords.txt` 权限 600、`set_env_var_value` 保留行尾注释；
`ensure_app_file` 仓库回退复制（10754B，二次调用幂等）；日志落盘 640 + 无 ANSI 色码；
非 root 运行输出明确报错且**无 Permission denied 噪声**；`scripts/install.sh --help` 正常输出。
⚠ `shellcheck` 未执行（本机非 root 且无外网无法安装）：已按 SC2154/SC1091/SC2034/SC2086 规则逐项加固并在源码内标注理由，
须在服务器执行 `sudo apt-get install -y shellcheck && shellcheck scripts/*.sh` 复核。

风险与待人工确认：① `infra/deploy/docker-compose.yml`（含 srs.conf/flink-conf.yaml）尚未交付，`install.sh` 的
step_6/7/11 会在 `require_compose_file` 处带指引失败；② Kafka SCRAM 用户（`hunter-client`/`hunter-vehicle`）创建未纳入
本批次脚本（`init-kafka.sh` 批次处理），在此之前 9093 车端接入不可用；③ 证书文件名以 `gen-kafka-certs.sh` 契约为准
（`certs/kafka/kafka.keystore.jks`、`kafka.truststore.jks`、`broker-cert.pem`），与 docs/01 §5 现有清单
（`broker.keystore.jks`/`client.keystore.jks` 等）不一致，待证书脚本落地后统一更新文档；
④ 任务书 step_11 未列 data-analytics，本脚本按"6 个服务全量启动"补齐；⑤ `trap` 清理的临时目录与 `/var/log` 提权写入
仅在 root 下生效（非 root 仅用于语法/函数级验证）；⑥ 其余 10 个脚本（gen-passwords/gen-kafka-certs/init-db/init-kafka/
init-minio/health-check/backup/uninstall/daily-check/collect-logs）与 docs/02-07 文档属后续批次。

## V1.14.1（新增：单机一键部署脚本集 —— 其余 10 个脚本 + 3 处缺陷修复）

Date：2026-9-19
变更类型：新增
1. **新增 `scripts/gen-passwords.sh`（267 行）**：`.env` 生成/口令轮换（`--ip`/`--env`/`--force`/`-y`）；
   替换 `CHANGE_ME_*`（含内嵌 `CHANGE_ME_SERVER_IP` 的 `RC_PUBLIC_WS_BASE_URL`/`RC_STUN_URLS` 与 `CORS_ORIGINS`）；
   幂等（已有值不覆盖）；生成 `passwords.txt`(600)；**口令明文仅终端展示、不写日志**（实测日志无明文）
2. **新增 `scripts/gen-kafka-certs.sh`（388 行）**：openssl 生成 CA(3650d)/broker(CN=kafka，SAN=DNS:kafka/localhost/<主机名>+IP:127.0.0.1+IP:<SERVER_IP>)/client(CN=kafka-client)，
   签发→PKCS12→JKS（`kafka.keystore.jks`/`kafka.truststore.jks`，口令取 `KAFKA_SSL_PASSWORD`）+ 车端 `kafka-client.p12`；
   校验证书链与 **SAN 含真实 SERVER_IP**（不含则报错退出）；私钥/JKS/P12 权限 600、证书 644；`--force` 重签并提示重启 Kafka 与更新车端
3. **新增 `scripts/init-db.sh`（271 行）**：等 PG 就绪（60s）→ `schema.sql` → `timescaledb.sql` → `init-data.sql`（`ON_ERROR_STOP=1`）；
   检测到独立 `hunter-timescale` 容器时在该库同样初始化；结构校验（8 schema / 13 表 / 2 hypertable / timescaledb 扩展版本 / admin / 车辆数）；
   可选 `--admin-password`（本地 bcrypt(12)，需 python3-bcrypt 或 htpasswd，缺失时给出明确安装指引）与 `--admin-password-hash`
4. **新增 `scripts/init-kafka.sh`（241 行）**：6 个契约 Topic（`telemetry_raw/telemetry_clean/event_raw/sensor_file/analytics_result/alert_event`，分区 12/12/6/3/6/3、保留 7/7/30/7/30/30 天）；
   创建后用 `--describe` 校验分区数与 `retention.ms`；内部监听非 PLAINTEXT 时自动注入 `--command-config`（与 `infra/k8s/jobs/kafka-init-job.yaml` 同策略）；可选 `--scram-users`
5. **新增 `scripts/init-minio.sh`（244 行）**：7 个 Bucket + 生命周期（raw-data 30d、rosbag `regular/` 30d（`events/` 永久）、video 90d、logs 30d、ota-packages/reports/scene-assets 永久）；
   幂等（`mb --ignore-existing` + 已有规则跳过）；`minio/mc` 独立镜像经 `--network container:hunter-minio` 复用网络命名空间（不依赖 compose 网络前缀）
6. **新增 `scripts/health-check.sh`（447 行，lib+main 混合）**：10 类检查（容器与 Docker healthcheck、7 服务 `/healthz` + web + MinIO/SRS/Flink 探针、PG、TimescaleDB、Redis、Kafka Topic≥6、MinIO Bucket≥7、`/data` 磁盘（>85% WARN / >95% FAIL）、内存（>90% WARN）、`data-collector-telemetry` LAG（>10000 WARN））；
   输出 `[PASS]/[WARN]/[FAIL]` 与汇总，退出码 0/1/2；可被 source 复用（`health_init_counters`/`run_all_health_checks`/`record_result`/`print_health_report`）
7. **新增 `scripts/daily-check.sh`（233 行）**：复用健康检查 + 业务巡检（24h critical 事件、PG 连接数与上限余量、容器 RestartCount、`/data` 各子目录占用）；
   报告写入 `${LOG_DIR}/check/daily-check-YYYYMMDD.log`（docs/01 §5），退出码 0/2
8. **新增 `scripts/backup.sh`（324 行）**：pg_dump 全量（`-F c -Z 9`）+ 两张 hypertable 最近 7 天 data-only + 配置 tar（600）+ Redis RDB + MinIO `mc mirror` 指引；
   按 `--retention`/`BACKUP_RETENTION_DAYS` 清理过期备份；单次日志 `${LOG_DIR}/backups/backup-<ts>.log`；给出 TimescaleDB 恢复顺序（`timescaledb_pre/post_restore`）
9. **新增 `scripts/uninstall.sh`（271 行）**：`--keep-data`（默认，保留 `/data` 与配置）/`--all`（需输入 `DELETE-ALL` 两次确认）；
   `docker compose down -v` + `hunter/*` 镜像 + `hunter-net`；**受保护路径护栏**（`DATA_DIR` 为 `/` 等直接拒绝）；不自动 `apt purge`（避免影响同机其他负载）
10. **新增 `scripts/collect-logs.sh`（328 行）**：容器日志（`docker compose logs --no-color --tail=N`）+ 系统信息（uname/lsb_release/free/df/docker info/compose ps/stats/监听端口）
    + 配置（`.env` 脱敏为 `***`）+ `journalctl -u docker` + 部署/运维日志尾部 → 单个 tar.gz(600)，含 `MANIFEST.txt`；归档后**自检明文口令残留**
11. **`common.sh` 增强**：新增 `hc_normalize_env_file`（`.env` CRLF 自动归一为 LF）、`minio_mc`/`minio_bucket_count`（mc 容器化调用）、`kafka_consumer_lag`/`kafka_topic_count`；
    `load_env` 中 `HUNTER_LOG_DIR/HUNTER_DATA_DIR/HUNTER_INSTALL_LOG` 覆盖优先级高于 `.env` 同名项
12. **同步更新文档**：`README.md`（新增「单机部署脚本」章节）、`docs/01`（§5 目录结构：scripts 12 个脚本 + 证书实际文件名）、
    `infra/deploy/README.md`（批次说明 → 交付状态表：compose/srs.conf/flink-conf.yaml/certs README 仍待交付）

缺陷修复（本批次实测发现）：
   a. **CRLF `.env` 导致部署全面故障**：Windows 编辑/直接复制 `.env` 时，`source` 后变量值带 `\r`（如 `LOG_DIR=/var/log/hunter-edge\r`），
      表现为 "加载 .env 失败"、巡检报告写入 `hunter-edge\r/check` 等疑难问题 → `load_env` 与 `.env` 生成路径统一加入 CRLF→LF 归一（幂等，附提示）
   b. **`collect-logs.sh` 脱敏静默失效**：`sed -E 's|...(PASSWORD|PASSWD...)...|'` 用 `|` 作分隔符与正则内交替符冲突 → sed 报错且被静默吞掉，
      归档缺少 `.env.masked` → 改用 `#` 分隔符，并改为显式 `if/else`（失败可见），实测归档含 `config/.env.masked` 且各口令为 `***`
   c. **脱敏自检误报**：未锚定正则命中 `.env.example` 注释行 `# TIMESCALE_PASSWORD=CHANGE_ME_...` → 改为行首锚定并跳过 `CHANGE_ME_*` 与 `=***`

验证：12 个脚本 `bash -n` **全部通过**（LF 版本，共 4936 行）、全部 `--help` 可正常输出；
函数级/功能级自检（隔离夹具，不触碰真实系统）：gen-passwords 幂等 + `--force` 轮换 + 口令不落日志（`leaked_secret_lines=0`）+ `.env`/`passwords.txt` 600；
gen-kafka-certs 真实 openssl 全流程（CA 10 年、broker SAN 含 `IPAddress:192.168.1.10`、`openssl verify` OK、权限 600/644、幂等不重签、`--force` 重签、SERVER_IP 占位时报错退出）；
health-check 无 Docker 环境给出 **通过 2 / 警告 3 / 失败 14** 且 exit 2（汇总与退出码语义正确）；daily-check 生成报告并含汇总与业务巡检项（exit 2）；
collect-logs 归档含 `MANIFEST.txt/config/.env.masked/system/*`、口令全部 `***`、**负向用例（故意残留明文口令）被自检拦下**；
backup MinIO 指引与 `--retention/--dir/--no-minio-hint` 解析正确；uninstall 错误确认中止（无损）、保留数据模式不删数据、`DATA_DIR=/` 被护栏拒绝；
init-db/init-kafka/init-minio/backup 在无容器环境给出明确的就绪超时/依赖缺失报错（非静默）。
⚠ 仍需在服务器执行：`shellcheck scripts/*.sh`（本机非 root 无外网无法安装）、真实 `docker compose up` 全链路、`nginx -t`、SQL/Kafka/MinIO 初始化实测。

风险与待人工确认：① `infra/deploy/docker-compose.yml` 与 `config/srs.conf`/`config/flink-conf.yaml` 仍待交付，install.sh step_6/7/11 会明确失败；
② Kafka SCRAM 账号（`hunter-client`/`hunter-vehicle`）创建需 broker 已存在可用管理员凭据，`init-kafka.sh --scram-users` 为可选且需人工确认首建方式（ZooKeeper 模式下通常由 broker 配置预置）；
③ 备份未自动加密/异地（脚本仅提示），文件名与路径需按合规要求确认；
④ daily-check 报告目录按 docs/01 §5 使用 `${LOG_DIR}/check/`（任务书写 `${LOG_DIR}/` 根目录，存在轻微差异）；
⑤ TimescaleDB 双实例 vs 单实例形态仍为阻断级待确认项，脚本已按"存在 timescale 服务才初始化独立实例"兼容。

## V1.15.0（新增：单机生产部署编排 —— docker-compose.yml + SRS + 前端镜像）

Date：2026-9-20
变更类型：新增
补齐 `infra/deploy/README.md` 长期标注 ⛔待交付 的部署编排产物（此前 install.sh step_6/7/11 会在 `require_compose_file` 处失败）：
1. **新增 `infra/deploy/docker-compose.yml`（15 个服务，Compose Specification 格式、无 version 字段）**：中间件（postgres/redis/zookeeper/kafka/minio/srs）+ Flink（jm/tm）+ 6 业务服务（api-gateway/scene-service/data-collector/data-analytics/ota-service/remote-control）+ web-portal；外部网络 `hunter-net`、数据全 bind mount 到 `${DATA_DIR:-/data}`、统一 `restart: unless-stopped` + `logging` json-file 100m×5 + 全量 `healthcheck`、业务服务 `depends_on: condition: service_healthy`、`deploy.resources` 限额（含生产扩容注释）；容器名严格对齐 `scripts/common.sh` 的 `C_*`，服务名与 `install.sh` step_7/11 启动序列及 `collect-logs.sh` 服务清单一致
2. **新增 `infra/deploy/config/srs.conf`（SRS 5.0）**：RTMP 1935 / WebRTC 8000udp / HTTP-API·WHEP·HTTP-FLV 9090，低延迟远程操控 vhost（`dwq_enabled off`、关 HLS），推流/播放地址约定（app=`remote_control`、stream 前缀 `hunter`）；标注 `rtc_server.candidate` 须替换为真实 SERVER_IP
3. **新增 `frontend/Dockerfile` + `frontend/.dockerignore`（web-portal）**：`node:20` 多阶段构建（`npm ci` + `vite build`）→ `nginx:1.25-alpine` 托管 `dist`；nginx.conf 不烘焙进镜像、由 compose 挂载 `./config/nginx.conf:/etc/nginx/nginx.conf:ro`（单一事实来源），busybox `wget` 探活首页
4. **新增 `infra/deploy/docker-compose.override.yml`（开发覆盖，⚠ 生产须删除/忽略）**：6 业务服务源码热重载（`uvicorn --reload --workers 1`）+ `ENVIRONMENT=dev`/`DEBUG=true`、资源 limits 下调、中间件端口对全网卡开放、Kafka 内部监听临时放宽为 PLAINTEXT
5. **文档同步**：`infra/deploy/README.md` 交付状态表更新（compose/override/srs.conf/frontend 标记 ✅；`config/flink-conf.yaml`/`certs/README.md` 仍待交付，Flink 参数当前经 compose `FLINK_PROPERTIES` 注入）

契约对齐决策（与本次任务提示词清单冲突处，按仓库既有事实来源执行）：
   ① **环境变量名对齐 `hunter_common.config` pydantic-settings 字段**（`POSTGRES_*`/`REDIS_*`/`KAFKA_*`/`MINIO_*`/`JWT_SECRET_KEY`/`ENVIRONMENT`），
      未采用提示词的 `DB_HOST`/`TS_HOST`/`JWT_SECRET`/`HUNTER_ENV=production`（服务读不到）；`ENVIRONMENT` 取 `prod`（Literal 枚举）非 `production`；
   ② **不拆分独立 timescale 容器**（契约单实例：TimescaleDB 为 PG 扩展、业务/时序同库，服务只连 `POSTGRES_*`），
      postgres 直接用 `timescale/timescaledb:2.13.1-pg15`；install.sh 的 `compose_service_exists timescale` 分支保留兼容；
   ③ **健康检查用 `/healthz` 且探针走 `python3 -c`**（业务镜像为 distroless，无 shell/无 curl），未引入 `entrypoint.sh`+`curl`（会破坏非 root/最小攻击面），
      6 个已交付 distroless Dockerfile 未回退重写；④ Redis `maxmemory-policy=noeviction`（会话/锁不可淘汰，非 `allkeys-lru`）、
      Kafka 内部 SASL_PLAINTEXT + 关闭自动建 Topic（契约，非 PLAINTEXT+auto-create）；⑤ Flink UI 容器内 8081 与 scene-service 冲突 → 宿主 `${FLINK_UI_HOST_PORT:-8088}`；
      业务服务除网关外端口只绑 `127.0.0.1`；⑥ 复用已存在的 `config/nginx.conf`（整份挂载）与 `infra/deploy/.env.example`（133 变量），未新建重复物；
   ⑦ Kafka 移除对不存在的 `kafka_jaas.conf` 挂载（bitnami 由 `KAFKA_CLIENT_USERS/PASSWORDS` 自动装配 SASL），补全 `KAFKA_CFG_SSL_*` JKS 参数

验证：`docker-compose.yml`（15 服务）与 `docker-compose.override.yml`（13 服务）经 PyYAML `safe_load` 解析通过——合并键（`<<: *business-env`）正确展开、
`volumes` 为扁平字符串列表、`command` 无嵌套、`environment` 均为映射；`args` 与 `business-volumes` 由流式映射/序列锚点改为块式/标量锚点
（修复 `${VERSION}` 落入 flow-map 触发 YAML 解析报错、序列锚点致 `volumes` 嵌套两处缺陷）。本机无 `docker compose` 插件，未做 `compose config` 实跑。

风险与待人工确认：① `certs/kafka/*.jks` 需 `gen-kafka-certs.sh` 先于 Kafka 生成（缺则对外 SASL_SSL 监听起不来）；② `/data/*` 子目录需匹配容器 UID
（bitnami=1001、postgres/distroless 非 root）后授权；③ `SERVER_IP` 必须真实（Kafka advertised.listeners / broker 证书 SAN / SRS candidate / RC_STUN_URLS 均依赖）；
④ 生产须移除 `docker-compose.override.yml`（否则 Kafka 降级 PLAINTEXT 且端口全开）；⑤ `flink-conf.yaml`、`certs/README.md` 属后续批次。

## V1.16.0（新增/修改/修复：系统差距整改 —— 必须 / 建议 / 人工决策项）

Date：2026-9-20
变更类型：新增 / 修改 / 修复
按「契约先行 → 实现 → 测试 → 全量回归」纪律，逐项落地设计文档↔实现差距整改（G-01～G-14 必须/建议项 + 8 项人工决策）；
所有结论对齐 `contracts/` 契约与设计文档具体章节，不臆造。变更以工作树未提交集体现（143 文件 +3815/−445，含 25 个新增文件）。

### 必须修改项（安全 / 阻塞发布）
1. **G-01 Kafka 严格 mTLS**（设计文档 3.2.3/14.2）：EXTERNAL（SASL_SSL，车端 9093）broker `ssl.client.auth=required` 强制车端 X.509 客户端证书（`gen-kafka-certs.sh` 签发 `kafka-client.p12`）；`infra/k8s/base/01-configmap-common.yaml`、`infra/k8s/statefulsets/kafka.yaml`、`infra/deploy/docker-compose.yml` 对齐；仅本地联调可临时降 `requested`（需书面豁免）
2. **G-02 网关→后端身份头信任加固**（设计文档 14.2）：新增 `hunter_common/internal_auth.py`——对 `X-User-Id`/`X-Roles`/`X-Trace-Id` 做 HMAC-SHA256 签名（共享密钥 `GATEWAY_HMAC_SECRET`）+ `X-Identity-Timestamp` 新鲜度（防重放窗口 `gateway_identity_max_age_s` 默认 300s）；`hunter_common.config` staging/prod 启动强校验密钥必填；六服务 RBAC 依赖（`core/dependencies.py`）统一校验 MAC，杜绝集群内伪造身份头绕过 JWT/RBAC；新增 `test_internal_auth.py`
3. **G-03 生产 compose override 硬闸**：`scripts/install.sh`/`health-check.sh` 检测到生效的 `docker-compose.override.yml` 即拦截；开发覆盖降级为模板 `infra/deploy/docker-compose.override.yml.example`，联调须手动改名并 `export HUNTER_ALLOW_OVERRIDE=1`（书面豁免）
4. **G-05 OTA 发布校验启动硬闸**（设计文档 7.2.4/14.3）：`ota-service` `Settings` `model_validator` 于 staging/prod 下关闭 SHA-256/签名校验或缺验签公钥即 fail-fast；新增 `test_config_prod_gate.py`
5. **G-06 弱口令防护 / 首登强制改密**（设计文档 14.1）：`user_svc.users.must_change_password`（migration `0002_gap_columns` + DDL `01_core.sql` + `infra/deploy/sql/schema.sql` 三处同步）；api-gateway 登录/`/user/me` 返回改密标记并支持改密；部署 SQL 对初始 admin 置 true
6. **G-07 MinIO Console 仅绑回环**：`docker-compose.yml` Console 端口 `127.0.0.1:` 前缀（与生产 `infra/deploy` 同基线），跨机访问走 SSH 隧道
7. **G-09 远程控制 WS 控制/信令通道**（契约 `x-hunter-websocket-contract`）：`services/remote-control` 新增 `routers/websocket.py`（`/ws/remote/{session_id}/{control,signal}`）、`core/ws_auth.py`（JWT 握手鉴权）、`services/ws_hub.py`（连接注册表 + 会话粘性）、`consumers/command_result.py`（ack 下行）、`producers/session_command.py`（WebRTC 信令中继）、`schemas/websocket.py`；`metrics.py` 增帧丢弃/时延指标；`main.py` lifespan 装配
8. **G-11 车辆地理围栏**（设计文档 8.4.2）：`vehicle_svc.vehicles.fence_json`（circle/polygon + 可选限速，migration 0002）；remote-control `services/geofence.py` + `repositories/vehicle_fence.py` 越界判定；新增 `test_geofence.py`
9. **G-14 灰度自动调度器 + 批次批量取数**（契约 `x-hunter-canary-rollout.scheduler`）：新增 `services/scheduler.py`（`RolloutScheduler` 后台循环）+ `TaskService.scheduler_tick`（到点启 scheduled 首批 / 观察 24h 且成功率≥95% 推进下批 / 不达标置 paused（CRITICAL 日志）/ 末批达标置 succeeded），`rollout.derive_scheduler_action` 纯函数决策，`repositories/tasks.list_for_scheduler` 单次批量候选取数；`gates.check_batch` + `RedisVehicleStateReader.get_status_many/is_online_many` 以两次 pipeline 批量读替代逐车 2N 次往返；FOR UPDATE 行锁 + 状态机校验保证多副本单任务只推进一次、不新增 Redis 键；环境变量 `ROLLOUT_SCHEDULER_ENABLED`（默认 true）登记契约 `required_env`/`env_defaults`/`env_notes` + 服务 ConfigMap；契约 `scheduler` 段新增 `enabled_env`/`halt_signaling` 并消解旧文本「发 alert_event」与服务边界冲突（alert_event 生产者唯一为 data-analytics）
10. **G-23 MFA 双因素（数据层 + 登录入口占位，校验流程待实现）**（设计文档 3.2.2/14.1）：`users.mfa_enabled` + `totp_secret`（密钥应用层加密后存储，migration 0002）+ DDL/`schema.sql` 同步；api-gateway 登录请求预留可选 `totp_code`（6 位），但 **TOTP 签发/校验流程与前端绑定尚未实现**，当前按契约「账号未启用 MFA 时忽略」（见下方风险项）

### 建议修改项
11. **G-10 remote_control Schema 扩展**：`control.brake`（0~1，estop 恒 1.0）/`control.gear`（P/R/N/D，词表同 health）/`control.mode`（值域 pending #21）为可选字段（不入 `required`，旧消息保持兼容）；顶层 `type`/`video`/`reason` 契约化（boot/stop 帧收对，`reason` = `SessionEndReason` 受控词表）
12. **G-12 hunter-rosbag 生命周期按对象 Tag 区分**：data-collector complete 阶段服务端打 Tag `hunter-retention`（regular 30 天 / event 永久），消解前缀语义冲突（留痕 pending #2）；`object-storage.yaml`、`infra/docker/minio/init-buckets.sh`、`infra/k8s/jobs/minio-init-job.yaml`、`verify_data_layer.py`、契约/测试同步
13. **实时异常检测 Flink 作业落地**（设计文档 6.2.2 / `x-hunter-realtime-jobs`）：新增 `flink-jobs/hunter_flink`——`detection_job.py`（拓扑 `telemetry_clean`(组 data-analytics-telemetry)→keyBy(vehicle_id)→`VehicleAnomalyTracker`→`build_alert`→`alert_event`）、`rules.py`/`thresholds.py`（纯 Python 规则核心，仓库单测无需 PyFlink）+ `flink-jobs/tests`；`scripts/run_unit_tests.py` 纳入 flink-jobs 目标

### 需人工决策项（按决策落地）
14. **G-04① 前端 Token 存储**：localStorage + 短 TTL + 严格 CSP——服务端同步收紧 Access Token TTL 至 30min（原设计文档 2h，`hunter_common.config` + 契约 `api-gateway.yaml`/`test_config` 断言）；CSP 双层施加（`infra/deploy/config/nginx.conf` 响应头 + `frontend/index.html` meta）；本地 Refresh Token 硬 TTL（过期即清理，`utils/storage.ts`）；禁止日志输出 Token
15. **G-08① health Schema 增字段**：`health.schema.json` 增 `gear`/`free_storage_mb`（作 OTA 门禁 P 档 / 存储判定数据源，需车端排期）；data-collector 车辆状态读模型写入 `vehicle:status`
16. **G-13①/G-28① 部署与数据流形态**：单机 compose 定为**受控降级形态**并书面豁免 §13.3/§15.1（高可用/多副本约束在单机形态不适用；G-14 调度器等后台协程在单机同样运行）
17. **G-17① 数据查询路径**：维持现有扁平路径（不改设计文档分层路径），在 `contracts/openapi/README.md` 落地「设计↔实现路径勘误映射表」
18. **G-18② OTA 版本审核流**（设计文档 §7.2.2）：`ota_versions.status` 扩为六态含 `testing`/`reviewing`（migration `0003_ota_review_status` + DDL `03_ota.sql` + `enums.md`）；新增 `submit-testing`/`submit-review`/`reject-review` 端点，`publish` 前置状态改为 `reviewing`（审核批准，驳回回退 `draft` 留痕）；前端 `OtaVersionsView` 审核操作；`test_versions_api.py` 覆盖
19. **G-20① 仿真进度/结果查询端点**：`scene-service` 补 `GET .../simulation` 查询（进度/结果），契约 `scene-service.yaml` + `repositories/carla.py` + `services/simulation.py` + `schemas/scene.py` + 前端 scene api/types + `test_scene_endpoints.py`
20. **G-22② TTC 冲突消解**：新增事件类型 `collision_pre_warning`（warning，TTC<3.0s），与 `collision_warning`（critical，TTC<1.5s）分级——受控事件词表 18→19（全局契约变更）：`enums.py`（+`EVENT_LEVEL_BY_TYPE`）→ DDL `04_events.sql` → `schema.sql` → migration `0004_event_pre_warning` → `enums.md` → event/alert_event/analytics_result schema → data-collector/data-analytics openapi → 重生成 ConfigMap → `thresholds.py`/`flow.py` → 5 处测试 → 前端词表；scene 4.5 提取触发口径不变（仅 collision_warning）
21. **G-24①/#20 WS JWT 传送**：以 `Sec-WebSocket-Protocol` 子协议 `hunter-jwt` 承载 Access Token（前端 `utils/websocket.ts` 已实现，后端 `core/ws_auth.py` 解析），避免 Token 进入 URL/查询串与访问日志

验证：`scripts/run_unit_tests.py` **9 个目标全部通过**（common 303 / flink-jobs 15 / api-gateway 71 / scene-service 108 / data-collector 121 / data-analytics 60 / ota-service 128 / remote-control 97；tests 单元标记下无用例）；`ruff check` **All checks passed**；`scripts/verify_data_layer.py` **34 项通过 0 失败**；`scripts/verify_infra.py` **全部校验通过**；`scripts/verify_test_layer.py` **L5 自检通过**；`generate_contracts_configmap.py --check` **PASS**（ConfigMap 与 contracts/kafka 一致）；前端 `npm run type-check`（vue-tsc）**通过**。Alembic `--sql` 离线：0002/0003/0004 迁移覆盖 gap 列 / OTA 审核态 / 事件词表扩展。

风险与待人工确认：① G-02 身份头 HMAC 为 mTLS 前的**第一道替代防线**，彻底消除信任敞口仍需网关↔后端 mTLS/Istio（NetworkPolicy 为纵深防御第一层）；② G-08/G-10 车端字段扩展需车端排期后联调（`pending #21` 值域未定稿）；③ G-12 事件包必须显式声明 `retention=event`（缺省 regular）；④ G-01/G-03/G-13 的降级/豁免路径均需**书面豁免**，禁止默认放行；⑤ Flink 作业 DLQ 失败处理器由部署侧承担，作业内部禁止吞异常；⑥ **G-23 MFA 仅完成数据层字段与登录入参占位，TOTP 签发/校验与密钥加密存储、前端绑定流程待后续批次实现（当前不得对外宣称 MFA 已启用）**。

## V1.16.1（修复：部署脚本实机缺陷修复 —— 首次生产部署路径修正）

Date：2026-9-24
变更类型：修复
首次 Ubuntu 22.04 实机部署（`/opt/HunterCore` = git 仓库根目录，非专用部署目录）过程中发现并修复的七个脚本缺陷。所有变更向后兼容：纯部署包模式（APP_DIR 无 `infra/deploy/` 子目录）行为不变。

### 修复内容

1. **Step 4 .env 必需口令变量缺失/空值自动补全**（`scripts/install.sh`）：
   修复 `.env` 由旧模板或手动创建时，`KAFKA_SSL_PASSWORD` 等变量完全不存在或值为空，Step 4 因"已存在则跳过口令生成"的幂等逻辑未处理，致使 Step 5（`gen-kafka-certs.sh` `require_env KAFKA_SSL_PASSWORD`）报错退出的问题。新增 `REQUIRED_SECRET_VARS` 列表（11 个，与 `infra/deploy/.env.example` 中 `CHANGE_ME_*` 条目同步）与 `ensure_required_secret_vars()` 函数：变量缺失时追加 `CHANGE_ME_*` 占位，值为空时重置为 `CHANGE_ME_*` 占位，再调用 `gen-passwords.sh` 填充随机强口令；幂等：已有有效值的变量不受影响。

2. **Step 6 业务镜像构建使用错误编排文件**（`scripts/common.sh` / `scripts/install.sh` / `scripts/uninstall.sh`）：
   修复 git 仓库根目录直跑时，`compose()` 函数默认使用开发版 `docker-compose.yml`（仅含中间件，无业务服务 `build:` 段）而非生产版 `infra/deploy/docker-compose.yml`，导致 Step 6 输出 "No services to build"、业务镜像从未构建的问题。`common.sh` 新增 `HC_COMPOSE_ARGS`：检测到 `${APP_DIR}/infra/deploy/docker-compose.yml` 存在时自动追加 `-f infra/deploy/docker-compose.yml --project-directory .`，使 `compose()`、`compose_service_exists()` 均指向生产编排且相对路径正确解析到 APP_DIR；同步：`step_6_build_images()` 改用 `compose` 函数、`require_compose_file()` 扩展检查双路径、`uninstall.sh` `stop_and_remove_compose()` 改用 `compose down -v`。

3. **Step 2 daemon.json 多路径查找与国内镜像源缺失警告**（`scripts/install.sh`）：
   修复 `write_daemon_json()` 仅检查 `${APP_DIR}/config/daemon.json` 而忽略仓库中 `infra/deploy/config/daemon.json` 的问题。新查找优先级：`${APP_DIR}/config/daemon.json`（用户自定义）→ `${APP_DIR}/infra/deploy/config/daemon.json`（仓库部署配置）→ 内置默认。Step 2 完成后追加检测：若 `/etc/docker/daemon.json` 的 `registry-mirrors` 为空列表，输出国内部署警告，引导中国大陆服务器运维人员提前创建 `${APP_DIR}/config/daemon.json` 并填入镜像加速器地址，避免 Step 6/7 因 Docker Hub 拉取超时而中断。

4. **生产编排 postgres 服务使用 Compose 不支持的 `deploy.resources.requests` 字段**（`infra/deploy/docker-compose.yml`）：
   修复 Step 6 报 `validating infra/deploy/docker-compose.yml: services.postgres.deploy.resources additional properties 'requests' not allowed` 导致构建立即退出。`requests` 为 Kubernetes/Swarm 术语，Docker Compose v2 仅支持 `limits` 和 `reservations`，将 postgres 服务的 `requests: { cpus: "1", memory: 2G }` 改为 `reservations: { cpus: "1", memory: 2G }`（语义相同：最小保障资源，不限制上限）。

5. **Step 4 未检查必需非口令配置变量导致 Step 6 变量未设置警告**（`scripts/install.sh`）：
   修复 `.env` 由旧模板或手动创建时，`VERSION`、`KAFKA_SASL_USER`、`KAFKA_SASL_VEHICLE_USER`、`MINIO_ROOT_USER` 等必需配置变量完全不存在或值为空，`gen-passwords.sh` 仅处理 `CHANGE_ME_*` 占位无法覆盖此情况，造成 Step 6 构建时 `docker compose` 报出大量 `variable is not set` 警告，并导致镜像名称中的 `${VERSION}` 展开为空。新增 `REQUIRED_CONFIG_VARS` 列表（`VERSION`、`KAFKA_SASL_USER` 等 6 个，默认值来自 `infra/deploy/.env.example`）和 `ensure_required_config_vars()` 函数：在调用 `gen-passwords.sh` 前先行填充默认值；`CORS_ORIGINS` 缺失或为空时设为 `CHANGE_ME_FRONTEND_ORIGIN`，由 `gen-passwords.sh` 联动 `SERVER_IP` 替换为 `http://<SERVER_IP>`；幂等：已有有效值的变量不受影响。

6. **Python 服务镜像迁移 python:3.11-slim → python:3.12-slim，废弃 distroless 运行时**（6 个服务 Dockerfile、6 个 K8s services/*.yaml、common/python/pyproject.toml）：
   Step 6 实机运行发现 python:3.11-slim 和 gcr.io/distroless/python3-debian12:nonroot 在中国大陆服务器均因网络不可达拉取超时。将 6 个 Python 服务镜像构建全面迁移：builder 阶段 FROM 改为 python:3.12-slim，pip 安装路径改为 /install/lib/python3.12/site-packages；runner 阶段废弃 distroless，改为 python:3.12-slim + useradd -u 10001 非 root 用户（USER 10001，与 Dockerfile 原本注释记录的回退方案对齐），COPY 目标改为 /usr/local/lib/python3.12/site-packages（slim 镜像默认路径），删除 PYTHONPATH=/usr/lib/python3/dist-packages；K8s 清单 runAsUser/runAsGroup/fsGroup 从 65532 改为 10001；common/python/pyproject.toml ruff target-version 和 mypy python_version 同步更新为 3.12；requires-python=">=3.11" 已是下限语义，无需修改。

7. **Step 2 snap 版 Docker 导致镜像加速器配置始终未生效（误因排查）**（`scripts/install.sh`）：
   Step 6 拉取超时的真正原因：服务器 Ubuntu 安装的是 snap 版 Docker（data-root=/var/snap/docker/...）。snap 版 dockerd 读取 /var/snap/docker/*/config/daemon.json，不读 /etc/docker/daemon.json，导致 write_daemon_json() 写入的镜像加速器始终无效。`step_2_install_docker()` 新增 snap 检测逻辑：通过 `docker info --format '{{.DockerRootDir}}'` 判断路径是否含 /var/snap/docker，如是则输出警告并自动执行 `snap remove docker`（-y 模式）或提示手动确认，清空 docker_version 变量强制进入 apt 安装流程。迁移后 dockerd 将读取 /etc/docker/daemon.json，registry-mirrors 立即生效。

风险与待人工确认：① 国内 Docker Hub 公共镜像加速站可用性变动频繁，`infra/deploy/config/daemon.json` 保持空列表（面向全球部署），中国大陆用户须自行维护 `${APP_DIR}/config/daemon.json`（推荐阿里云容器镜像服务个人专属加速地址，最稳定）；② `HC_COMPOSE_ARGS` 依赖 `--project-directory .` 将编排相对路径解析到 APP_DIR，要求 APP_DIR 下已创建 `certs/`、`config/`、`sql/` 等子目录（Step 3 负责，幂等），部署包模式下无此问题；③ Step 4 口令补全调用 `gen-passwords.sh` 仅处理 `CHANGE_ME_*` 占位，现有有效口令不受影响，但若用户已有口令被意外清空，补全后会生成新口令——需同步更新依赖方（Kafka SCRAM、MinIO 服务账号、admin 登录密）；④ `reservations` 字段在旧版 Compose v2 插件（< 2.x）不支持，本部署要求 Compose >= v2.20，生产环境已验证 Compose 5.5.1 兼容；⑤ python:3.12-slim 仍为 Docker Hub 镜像，服务器须确保镜像已缓存本地或加速器可用。若拉取仍失败，可在本地机器执行 docker pull python:3.12-slim 后导出并传至服务器 docker load。

验证：以 `/opt/HunterCore` git 仓库根目录场景全量走查（Step 4 → 5 → 6 → 7 路径），确认 `compose()` 使用 `infra/deploy/docker-compose.yml`（`--project-directory .`）且相对路径正确解析；`write_daemon_json` 回退链命中 `infra/deploy/config/daemon.json`；`ensure_required_secret_vars` 对 `KAFKA_SSL_PASSWORD=` 空值与缺失两种情况均追加 `CHANGE_ME_*` 占位并由 `gen-passwords.sh` 填充；`ensure_required_config_vars` 对 `VERSION`、`KAFKA_SASL_USER` 等缺失时填充默认值，对 `CORS_ORIGINS` 空值设为占位符并联动 `SERVER_IP` 解析；`docker-compose.yml` 验证通过（`reservations` 替换 `requests`）；`compose config` 验证通过；Python 6 个服务 Dockerfile 均切换到 python:3.12-slim 双阶段，K8s securityContext UID 10001。

## V1.16.2（修复：Step 7 Kafka 启动多重阻断逐层定位与修复，直至 broker 成功就绪）

Date：2026-9-28
变更类型：修复
实机部署（`sudo bash scripts/install.sh -y --ip <公网IP>`）在 Step 7 失败：Kafka 容器反复 `Started` 但就绪探测 `kafka_broker_ready` 90s 内始终连不上 `127.0.0.1:9092`。`docker logs hunter-kafka` 显示容器每分钟重启一次，每次同一条错误：

```
==> No injected configuration files found, creating default config files
cp: cannot create regular file '/opt/bitnami/kafka/config/server.properties': Permission denied
```

### 根因
compose 中 kafka 以 `user: "1001:1001"` 运行，而新版 bitnami 镜像的容器内目录 `/opt/bitnami/kafka/config` 归 **root 且不可写**。bitnami 启动时需向该目录生成 `server.properties`，UID 1001 无权写入 → 初始化失败 → broker 从未进入监听阶段（连 INTERNAL 9092 也未绑定）。这与本编排中 **ZooKeeper 的同源缺陷完全一致**（已由 `zookeeper-init` 解决），只是 Kafka 缺了对应的 init 容器。（注：keystore 权限并非“尚未加载就无关”——在依次越过配置目录、advertised、以及下述 OS 网络层阻断之后，它确实成为最后一道阻断，详见修复项 8。）

### 修复内容

1. **为 Kafka 补 init 容器 + 可写 config 卷**（`infra/deploy/docker-compose.yml`，主修复）：新增 `kafka-config-init`（`user: root`，将镜像默认配置 `cp -a` 到 `${DATA_DIR}/kafka-config` 并 `chown -R 1001:1001`）；`kafka` 新增 `depends_on: kafka-config-init (service_completed_successfully)`，并将该卷挂回 `/opt/bitnami/kafka/config`（证书仍为 `config/certs` 子路径只读挂载，不受覆盖）。与 `zookeeper-init` 完全同构；`install.sh` 的 `compose up -d zookeeper kafka` 会自动带起该 init 容器，无需改脚本。
2. **JKS 对 broker 组可读（次要加固）**（`scripts/gen-kafka-certs.sh`）：新增 `KAFKA_CERT_GID`（默认 1001）与 `broker_readable_jks()`——keystore/truststore 改为 `chgrp 1001 + chmod 0640`（组不可用退回 `0644`）；私钥 `.pem`/`.p12` 仍 `0600`。避免 config 修复后 keystore 读取成为下一个潜在阻断点。
3. **Step 7 启动前幂等兜底**（`scripts/install.sh`）：新增 `ensure_kafka_certs_readable()`，在启动 Kafka 前修正存量部署遗留的 JKS `root:root 0600`（Step 5 命中“证书已存在”会跳过生成）。
4. **inter-broker 监听器未 advertised 导致启动即退出**（`infra/deploy/docker-compose.yml`，config 目录修复后暴露的下一层阻断）：实机日志报 `inter.broker.listener.name must be a listener name defined in advertised.listeners`。原因：`KAFKA_CFG_LISTENERS` 含 BROKER://:9094 且 `KAFKA_CFG_INTER_BROKER_LISTENER_NAME: BROKER`，但 `KAFKA_CFG_ADVERTISED_LISTENERS` 只列 INTERNAL/EXTERNAL——Kafka 要求 inter-broker 监听器必须出现在 advertised.listeners。修复：advertised 补 `BROKER://kafka:9094`（hunter-net 内服务名，单节点自连可解析）。**后续坑**：因 `/opt/bitnami/kafka/config` 已改为持久卷，上一轮旧 advertised 生成的 `server.properties` 会落在 `${DATA_DIR}/kafka-config` 里，bitnami 检测到已存在则不再按新 env 重生（即使改了 advertised 仍报同样错）；因此 kafka-config-init 改为每次启动先 `rm -rf /patch/*` + `rm -f /patch/server.properties` 再拷默认，保证按当前 env 重生。存量环保持久目录需手工 `rm -rf /data/kafka-config` 清一次。
5. **修复 `kafka_broker_ready` 端口探测在 dash 下恒失败**（`scripts/common.sh`）：原 TCP 兜底 `sh -c 'exec 3<>/dev/tcp/127.0.0.1/9092'` 在 bitnami 镜像（/bin/sh=dash，不支持 /dev/tcp）下永远返回非 0，叠加刚建 SCRAM 号时 `kafka-topics.sh --list` 可能尚未就绪，会造成 broker 已起但误判未就绪。改为优先 `nc -z localhost 9092`（与 compose healthcheck 一致），兜底显式用 `bash -c '/dev/tcp'`。
6. **kafka 容器 healthcheck 无 `nc` 导致永远 unhealthy**（`infra/deploy/docker-compose.yml`）：实机确认 bitnami/kafka:3.6 镜像不含 `nc`（`sh: 1: nc: not found`），原 `CMD-SHELL nc -z localhost 9092` 恒失败 → 即使 broker 就绪也显 unhealthy，阻断 Step 11 业务服务的 `depends_on: kafka (service_healthy)`。改为 `CMD ["bash","-c","exec 3<>/dev/tcp/127.0.0.1/9092"]`（显式走 bash 探端口，dash 不支持 /dev/tcp）。
7. **生产服务器 OS 孤儿网桥（重复子网）阻断容器互访**（环境处置、非代码；Step 7 越过 Kafka 自身配置后暴露）：`docker logs` 一度显示 kafka 连 `zookeeper:2181` 会话超时，而 ZK 实际 `0.0.0.0:2181` 正常监听。逐层实测（`ip -br addr` / `ip route` / nsenter 抓包）定位为：宿主机存在两块 docker 网桥 `br-fdb374e51f17`(DOWN) 与 `br-42bd3b701a40`(UP, hunter-net) **同占子网 `172.28.0.0/16`、同为网关 `172.28.0.1`**，内核残留一条指向 DOWN 桥的 `linkdown` 路由，导致发包命中死路由 → `EHOSTUNREACH（没有到主机的路由）`，表现为 kafka↔zk 连不通。该 DOWN 桥 `docker network ls` 查无归属（OS 层遗留），`systemctl restart docker` 也清不掉。修复：`sudo ip link set br-fdb374e51f17 down && sudo ip link delete br-fdb374e51f17`，删后仅剩 UP 桥一条路由，`HOST_OK`/跨容器 `OK` 立即恢复。（UFW `deny routed` 曾列为疑凶，经 `ufw disable` 复测排除。）**根因预防**：根目录 dev 版 `docker-compose.yml` 与 `infra/deploy` 生产版若把 `hunter-net` 钉成同一 `172.28.0.0/16`，早期以不同 project 名各建一次即会遗留重复子网/孤儿桥；建议两套编排使用不同子网，或确保同一时刻只存在一套网络。
8. **keystore JKS 对 broker（uid 1001）不可读导致 SASL_SSL 监听器加载失败**（实机确认，为最后一道阻断）：越过网络层后 `docker logs` 报 `Failed to load SSL keystore /opt/bitnami/kafka/config/certs/kafka.keystore.jks ... Caused by: java.nio.file.AccessDeniedException`。存量证书由 Step 5 早期以 root 生成（other 无读），本次又跳过 Step 5（命中“证书已存在”）→ 权限未更新。现场修复（对 `docker inspect` 取到的挂载源真实路径执行）：`chmod o+X` 证书目录链 + `chgrp -h 1001` + `chmod 0640` 两块 `.jks`，以 `--user 1001:1001` 容器实测 `READ_OK` 后 kafka 即 `[KafkaServer id=1] started`、`Up (healthy)`。根治需把修复项 2/3（`gen-kafka-certs.sh` 的 `broker_readable_jks()`、`install.sh` 的 `ensure_kafka_certs_readable()`）同步到服务器，未来全新部署自动带上组可读权限。

向后兼容：仅新增 init 容器与只读挂载调整，不改业务契约/镜像。`${DATA_DIR}/kafka-config` 首次由 Docker 自动创建、init 容器置为 1001 属主（与 `zookeeper-conf` 同机制）。

需同步到服务器后生效（/opt/HunterCore 与开发机为不同机器）：将更新后的 `infra/deploy/docker-compose.yml`（必需）与 `scripts/gen-kafka-certs.sh`、`scripts/install.sh`（可选加固）同步到服务器，然后：
`cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d zookeeper kafka`
或整体续跑：`sudo bash /opt/HunterCore/scripts/install.sh --step 7 -y`。

验证：`bash -n` 通过（scripts）；`docker compose config` 通过。实机按上述逐层修复后（config-init 重生含 BROKER advertised 的 `server.properties` → 删孤儿桥恢复容器互访 → keystore 组可读），`docker logs hunter-kafka` 出现 `Registered broker 1 ... INTERNAL/EXTERNAL/BROKER`、`Awaiting socket connections on 0.0.0.0:9092`、`[KafkaServer id=1] started`，`ps` 显示 `Up (healthy)`，`docker exec hunter-kafka bash -c '/dev/tcp/127.0.0.1/9092'` = `BROKER_READY`。Step 7 Kafka 通过，可续跑 Step 8~13。

风险与待人工确认：① `kafka-config` 内容来自镜像当前版本，镜像升级后需重新填充（删 `${DATA_DIR}/kafka-config` 重启即可重建）；② 若服务器 GID 1001 缺失，`chown`/`chgrp` 会失败（但 compose 中 zookeeper-init 早已以 1001 运行，Step 1 已创建该用户/组）；③ JKS 加固属防御性，若后续 broker 进入启动后仍报 SSL 错误，按 `docker logs hunter-kafka` 实际异常继续定位。

验证（静态）：`bash -n scripts/gen-kafka-certs.sh`、`bash -n scripts/install.sh`、`bash -n scripts/common.sh` 均通过语法校验；compose 经 YAML 解析，`kafka-config-init` 与 `zookeeper-init` 结构一致（root 初始化、主容器挂卷覆盖）；listeners 与 advertised.listeners 名称集合对齐（INTERNAL/EXTERNAL/BROKER）。实机验证见上方“验证”段（Step 7 Kafka 已成功就绪）。

## V1.16.3（修复：Step 7 SRS 容器启动失败 —— srs.conf 挂载源缺失被 Docker 误建为空目录）

Date：2026-09-28
变更类型：修复
实机续跑 Step 7（Kafka 就绪后）在启动 srs 时失败，MinIO 正常新建，srs 容器创建即报错退出：

```
Error response from daemon: failed to create task for container: ... error mounting "/opt/HunterCore/config/srs.conf" to rootfs at "/usr/local/srs/conf/srs.conf": ... not a directory: Are you trying to mount a directory onto a file (or vice-versa)?
```

### 根因
生产编排以**文件方式**挂载 `./config/srs.conf:/usr/local/srs/conf/srs.conf:ro`，但服务器上 `/opt/HunterCore/config/srs.conf` 不存在。Docker 对缺失的 bind 源路径会**自动创建同名空目录**，随后“目录挂载到容器内文件”失败。文件缺失的流程缺口：`install.sh` Step 4 仅有 `ensure_nginx_conf()` 负责将 `infra/deploy/config/` 下配置就位到 `${APP_DIR}/config/`，未覆盖 `srs.conf`（模板仅存在于仓库 `infra/deploy/config/srs.conf`，其中 `candidate $SERVER_IP` 为占位符，SRS 不会自行展开环境变量）。

### 修复内容

1. **新增 `ensure_srs_conf()` 并接入 Step 4**（`scripts/install.sh`，主修复）：与 `ensure_nginx_conf` 同构的幂等就位逻辑，置于 4.6（Nginx 配置之后）：
   - 检测 `${APP_DIR}/config/srs.conf` 为**目录**（Docker 误建）时：空目录自动 `rmdir` 清理；非空则报错终止并提示人工确认；
   - 文件缺失时从 `${APP_DIR}/infra/deploy/config/srs.conf` 复制（`install -m 0644`）；已存在则跳过（幂等）；
   - 模板中 `candidate $SERVER_IP` 占位符自动 `sed` 替换为 `.env` 的真实 `SERVER_IP`（Step 4.3 已校验非空，避免 WebRTC candidate 指向错误地址）。
   今后所有被 compose 以文件方式挂载的配置均须有对应 `ensure_*_conf` 就位步骤，且在容器首次 `up` 之前完成。

向后兼容：纯新增函数与调用点，不改编排/契约/镜像；`config/srs.conf` 已就位且 candidate 已替换的存量环境重跑 Step 4 无任何变化。

临时手工修复（服务器立即解锁，无需等待脚本同步）：
```bash
docker rm -f hunter-srs
[ -d /opt/HunterCore/config/srs.conf ] && rmdir /opt/HunterCore/config/srs.conf
cp /opt/HunterCore/infra/deploy/config/srs.conf /opt/HunterCore/config/srs.conf
IP=$(grep -E '^SERVER_IP=' /opt/HunterCore/.env | head -1 | cut -d= -f2 | tr -d '\r')
sed -i "s|\$SERVER_IP|${IP}|g" /opt/HunterCore/config/srs.conf && chmod 644 /opt/HunterCore/config/srs.conf
grep -n 'candidate' /opt/HunterCore/config/srs.conf   # 确认已替换为真实 IP
sudo bash /opt/HunterCore/scripts/install.sh --step 7 -y
```

需同步到服务器后生效：将更新后的 `scripts/install.sh` 同步到 `/opt/HunterCore`，规范做法为先补跑 `--step 4 -y`（自动完成 srs.conf 就位与占位符替换）再 `--step 7 -y`；或直接执行上方手工命令。

风险与待人工确认：① 若用户曾手工创建 `config/srs.conf` 且 candidate 为自定义值（非占位符），`ensure_srs_conf` 不会改写该文件（幂等跳过），需自行确认 candidate 为真实 IP；② WebRTC 生产建议显式 `SERVER_IP`，同机 NAT 测试可用 `auto`；③ SRS 跨主机播放依赖 8000/udp 与 1935/tcp 对外放行（Step 1 ufw 规则已覆盖）、如后续启用 HTTPS/WSS 播放需另行配置证书（运维手册第 7 节）。

验证：`bash -n scripts/install.sh` 语法校验通过；走查 Step 4 调用链（4.3 SERVER_IP 校验 → 4.5 nginx → 4.6 `ensure_srs_conf "$server_ip"`）确认占位符替换在非空 IP 前提下执行；误建空目录场景经 `rmdir` 清理后复制-替换-续跑路径与上述手工命令等价。

## V1.16.4（修复：Step 7 SRS 容器启动失败 —— objs 卷挂载覆盖镜像自带二进制）

Date：2026-09-28
变更类型：修复
V1.16.3 修复 srs.conf 挂载后实机续跑 Step 7，srs 容器创建成功但启动立即退出（MinIO 正常）：

```
Error response from daemon: failed to create task for container: ... exec: "./objs/srs": stat ./objs/srs: no such file or directory
```

### 根因
`ossrs/srs:5` 镜像将可执行文件置于 `/usr/local/srs/objs/srs`（工作目录 `/usr/local/srs`，compose `command: ["./objs/srs", "-c", "conf/srs.conf"]` 即启动它）。但 srs 服务同时挂载了 `${DATA_DIR:-/data}/srs:/usr/local/srs/objs`——Step 3 在宿主机创建的 `/data/srs` 为**空目录**，挂载后**覆盖掉了镜像内的 objs 目录（含 srs 二进制与 objs/nginx/html）**，导致容器内 `./objs/srs` 不存在。`collect-logs.sh` 实际走 `docker compose logs` 收集日志（不读宿主机 `${DATA_DIR}/srs` 文件），因此该 objs 挂载有害无益。

### 修复内容

1. **移除覆盖二进制的 objs 卷挂载**（`infra/deploy/docker-compose.yml`，主修复）：删除 `srs` 服务的 `- ${DATA_DIR:-/data}/srs:/usr/local/srs/objs` 行，保留镜像自带的 `/usr/local/srs/objs`（含二进制）；就地注释标警“禁止挂载到 objs”。
2. **SRS 日志改为 console**（`infra/deploy/config/srs.conf`）：`srs_log_tank` 由 `file`（写 `./objs/srs.log`）改为 `console`，日志输出到 stdout，与其余服务一致，由 Docker 日志驱动（`*default-logging`）采集，`docker compose logs srs` 可直接查看。

向后兼容：不改业务契约/镜像；SRS 为实时转发（hls/dvr 已关），无需持久化文件。Step 3 仍创建 `${DATA_DIR}/srs` 目录（不再被 compose 引用，无害）。全新部署从仓库模板复制的 srs.conf 自动带 console 日志；存量环境已复制的 `config/srs.conf` 为幂等跳过，需手工改 console（或删后重跑 Step 4）。

临时手工修复（服务器立即解锁）：
```bash
sudo sed -i '\|/usr/local/srs/objs|d' /opt/HunterCore/infra/deploy/docker-compose.yml
sudo sed -i 's/srs_log_tank.*/srs_log_tank            console;/' /opt/HunterCore/config/srs.conf
cd /opt/HunterCore && sudo docker rm -f hunter-srs
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
curl -sf http://127.0.0.1:9090/api/v1/versions && echo "SRS OK"
```

需同步到服务器后生效：将更新后的 `infra/deploy/docker-compose.yml` 与 `infra/deploy/config/srs.conf` 同步到 `/opt/HunterCore`；因 `config/srs.conf` 为运行时从模板复制，同步后需重跑 `--step 4 -y`（幂等覆盖就位）或直接执行上方手工命令，再 `--step 7 -y`。

风险与待人工确认：① 若后续需将 SRS 日志落盘到宿主机，应挂载**独立子目录**（如 `/usr/local/srs/data`）而非 objs，并相应修改 `srs_log_file`，切勿再覆盖二进制目录；② console 日志受 `*default-logging` 轮转策略约束，与其他服务一致；③ `ossrs/srs:5` 镜像工作目录需为 `/usr/local/srs`（官方镜像默认即是），若更换镜像需复核 `command` 路径。

验证：`docker compose config` 语法校验通过（srs 服务不再含 objs 挂载）；手工走查：删除挂载行后容器使用镜像自带 `/usr/local/srs/objs/srs` 二进制，`srs_log_tank console` 使 `docker logs hunter-srs` 可见输出。实机待按上述手工命令重建 srs 后确认 `Up (healthy)` 与 `9090/api/v1/versions` 可达。（注：本项修复后实机又暴露 `srs.conf` 指令与 SRS 5.0 不兼容的新问题，见 V1.16.5。）

## V1.16.5（修复：Step 7 SRS 容器崩溃重启 —— srs.conf 含 SRS 5.0 非法指令）

Date：2026-09-28
变更类型：修复
V1.16.4 修复 objs 挂载后，hunter-srs 容器能启动但立即以 `Restarting (255)` 循环，`docker logs hunter-srs` 反复报：

```
[ERROR] Failed, code=1023(ConfigInvalid)(Configuration is invalid) : check config : check normal : illegal directive accept_threads
thread: check_normal_config() [./src/app/srs_app_config.cpp:2335](No such file or directory)
```

### 根因
`infra/deploy/config/srs.conf` 初版写入了多个 **SRS 5.0 已移除/不识别的全局与子块指令**：`accept_threads`、`work_threads`、`goleada_enabled`、`pithy_close`、`kernel_madvise`、`stream_circuit_breaker`（全局），`rtc_server.nomatch_drop`，vhost 下 `time_interval`、`publish.enabled` 等。SRS 启动先执行 `check_config`，**遇首个非法指令即报 code=1023 退出**（因此日志只显 `accept_threads`，实际后续还有）；叠加 `restart: unless-stopped` 形成启动→校验失败→崩溃→重启循环，9090 从未监听，健康探测 30s 超时。

### 修复内容

1. **srs.conf 收敛为 SRS 5.0 最小稳定指令集**（`infra/deploy/config/srs.conf`，主修复）：删除所有 5.x 非稳定调优指令，仅保留官方可靠的全局（`listen`/`max_connections`/`pid`/`send_timeout`/`recv_timeout`/`tcp_nodelay`/`srs_log_tank console`/`srs_log_level`）+ `http_api{listen 9090}`（提供 /api/v1/versions 健康检查与 /rtc/v1/whep·whip 信令）+ `rtc_server{listen 8000; candidate $SERVER_IP}` + `vhost{rtc/play/http_remux/hls}`。其中：删除 `http_server` 块（与 http_api 同占 9090 会冲突且静态页非必要，WHEP/WHIP 信令本就由 http_api 提供）；`candidate` 仍保留 `$SERVER_IP` 占位符由 `ensure_srs_conf` 替换。

向后兼容：不改端口/服务拓扑（仍 RTMP 1935 / WebRTC 8000udp / HTTP-API 9090）；去除的均为可选调优指令，不影响远程操控核心链路（RTMP 推流→WebRTC 播放）。存量环境 `config/srs.conf` 为运行时副本且 `ensure_srs_conf` 幂等跳过，不会自动更新，需手工覆盖（或删后重跑 Step 4）。

临时手工修复（服务器立即解锁，直接 tee 覆盖运行时 srs.conf）：
```bash
sudo docker rm -f hunter-srs
IP=$(sudo grep -E '^SERVER_IP=' /opt/HunterCore/.env | head -1 | cut -d= -f2 | tr -d '\r ')
sudo tee /opt/HunterCore/config/srs.conf >/dev/null <<EOF
listen 1935;
max_connections 1000;
pid ./objs/srs.pid;
srs_log_tank console;
srs_log_level warn;
http_api { enabled on; listen 9090; }
rtc_server { enabled on; listen 8000; candidate ${IP}; }
vhost __defaultVhost__ {
  rtc { enabled on; modify_codec on; }
  play { dwq_enabled off; }
  http_remux { enabled on; mount [flv]; }
  hls { enabled off; }
}
EOF
cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
sleep 6 && sudo docker ps --filter name=hunter-srs && curl -sf http://127.0.0.1:9090/api/v1/versions && echo SRS_OK
```

需同步到服务器后生效：将更新后的 `infra/deploy/config/srs.conf` 同步到 `/opt/HunterCore`；全新部署走 Step 4 会自动就位。本修复与 V1.16.3/V1.16.4 一同使 `install.sh` 的 `ensure_srs_conf`（复制模板+替换 candidate）在 SRS 上真正可用。

风险与待人工确认：① 本版本以“能启动”为优先，未保留初版队列/反馈类低延迟调优（如 mrQ/queue_length/keepalive_timeout 等）；若实机对端到端延迟有严格指标，需逐项验证其在 SRS 5.0 的合法拼写后再回填；② WHEP 信令走 http_api 9090（与 compose `RC_SRS_WHEP_URL=http://srs:9090/rtc/v1/whep/` 一致）；若后续需静态播放器页另配 `http_server` 并改为不同端口；③ `check_config` 仅暴首个非法指令，若 tee 最小集后仍报 restart，以 `docker logs` 新提示为准继续收敛。

验证：`bash -n scripts/install.sh` 已过；`docker compose config` 已过。实机待按上述手工命令重建 srs 后确认：`docker ps` 为 `Up (healthy)` 且无 `Restarting`，`docker logs hunter-srs` 出现 `config parse complete` 与 http api listen 9090，`curl http://127.0.0.1:9090/api/v1/versions` 返回版本 JSON。（注：V1.16.5 首版仍残留 vhost 子指令问题，见 V1.16.6。）

## V1.16.6（修复：Step 7 SRS 崩溃重启续 —— vhost.rtc.modify_codec 等子指令仍非5.0合法）

Date：2026-09-28
变更类型：修复
V1.16.5 收敛全局指令后，hunter-srs 仍 `Restarting (255)`，`docker logs` 报错下沉到 vhost 子块：

```
Failed, code=1023(ConfigInvalid) : check config : check normal : illegal vhost.rtc.modify_codec of __defaultVhost__
```

### 根因
V1.16.5 保留了 vhost 下 `rtc.modify_codec`、`play.dwq_enabled`、`http_remux{...}`、`hls{...}` 等子指令，但 SRS 5.0.225 的 `check_normal_config`（srs_app_config.cpp:2730）对这些子指令部分不识别（`modify_codec` 已不在 5.x vhost.rtc 白名单）。`check_config` 仍只暴首个（modify_codec），逐个试错成本过高。

### 修复内容

1. **vhost 收敛为仅 `rtc { enabled on; }`**（`infra/deploy/config/srs.conf`，主修复）：删除 vhost 内所有调优子块（`rtc.modify_codec`、`play.dwq_enabled`、`http_remux`、`hls`），只保留官方 `docker-srs.conf` 保证存在且合法的 `rtc { enabled on; }`（即可打开 RTMP→WebRTC 转换与 WHEP 拉流）。全局保留 `listen`/`max_connections`/`pid`/`srs_log_tank console`/`srs_log_level`/`send_timeout`/`recv_timeout`/`tcp_nodelay`；`http_api{listen 9090}` + `rtc_server{listen 8000; candidate $SERVER_IP}` 不变。

向后兼容：端口/服务拓扑不变；`rtc.enabled` 仍保证远程操控核心链路（车端 RTMP 推流→浏览器 WHEP 播放）。去除的均为可选调优，不影响可用性。

临时手工修复（服务器直接 tee 覆盖为仅含官方核心指令）：
```bash
sudo docker rm -f hunter-srs
IP=$(sudo grep -E '^SERVER_IP=' /opt/HunterCore/.env | head -1 | cut -d= -f2 | tr -d '\r ')
sudo tee /opt/HunterCore/config/srs.conf >/dev/null <<EOF
listen 1935;
max_connections 1000;
pid ./objs/srs.pid;
srs_log_tank console;
srs_log_level warn;
http_api { enabled on; listen 9090; }
rtc_server { enabled on; listen 8000; candidate ${IP}; }
vhost __defaultVhost__ {
  rtc { enabled on; }
}
EOF
cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
sleep 6 && sudo docker ps --filter name=hunter-srs && curl -sf http://127.0.0.1:9090/api/v1/versions && echo SRS_OK
```

金标准兜底（若最小集仍报 illegal，不再猜）：`sudo docker run --rm --entrypoint cat ossrs/srs:5 conf/docker-srs.conf` 打印镜像自带官方配置，以其实际写法为蓝本对齐（端口改 1935/8000/9090、candidate 填真实 IP）。

需同步到服务器后生效：将更新后的 `infra/deploy/config/srs.conf` 同步到 `/opt/HunterCore`；全新部署走 Step 4 会自动就位。

风险与待人工确认：① 本版以“必能启动”为绝对优先，vhost 仅留 `rtc.enabled`；HLS/HTTP-FLV/编解码改写/队列调优均未启用，若后续需要，**逐项**先查镜像版 `docker-srs.conf`/`full.conf` 确认指令存在再回填，避免重踏 illegal directive；② WHEP 信令走 http_api 9090（与 `RC_SRS_WHEP_URL` 一致）；③ 根因教训：为特定中间件版本写配置文件时，只使用目标版本文档/镜像自带样本中实际存在的指令，不得凭旧版记忆堆砌调优项。

验证：`bash -n scripts/install.sh` 已过。实机待按上述手工命令重建后确认 `docker ps` 为 `Up (healthy)` 且无 `Restarting`，`docker logs` 出现 `config parse complete` 与 http api listen 9090，`curl` 返回版本 JSON。

## V1.16.7（修复：Step 8 数据库初始化失败 —— Step 3 对 PG 数据目录强制 chown 与容器 uid 不一致）

Date：2026-09-28
变更类型：修复

Step 7 修复后中间件全部就绪，Step 8 初始化数据库首个 `psql` 即失败：

```
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed:
FATAL:  could not open file "global/pg_filenode.map": Permission denied
```

同时 `docker logs hunter-postgres` 出现 `could not open directory "pg_logical/snapshots": Permission denied`、checkpoint `PANIC: could not open file .../pg_control: Permission denied` 并触发一次崩溃重启；重启后容器 entrypoint 将属主自愈回正确 uid，服务恢复。

### 根因

1. **容器运行 uid 与 Step 3 硬编码属主不一致**（主因，`scripts/install.sh`）：`timescale/timescaledb:2.13.1-pg15` 为 **Alpine/musl** 基础镜像（日志 `x86_64-pc-linux-musl` 佐证），其 `postgres` 用户为 **uid 70**（`docker top hunter-postgres` 显示 USER=70）；而 Step 3 的 `ensure_dir "${DATA_DIR}/postgresql" "999:999"` 依据旧注释“PG uid=999”（Debian 版才成立）。当宿主机 `stat` 检出目录属主≠999 时执行 `chown -R 999:999`，把**正在运行的**集群数据文件属主改为 999，而后端/checkpointer 仍以 uid 70 运行，于是读 `pg_filenode.map`/`pg_control`/`pg_logical/snapshots` 全部 `Permission denied`。`pg_isready`（纯客户端 ping）不读数据文件，故健康检查“就绪”但真实连接失败。

### 修复内容

1. **新增 `ensure_data_dir`，数据目录非空时绝不全量 chown**（`scripts/install.sh`，主修复）：PG/Timescale/Redis/Kafka/ZK/MinIO 六个容器自管理的数据目录改用 `ensure_data_dir`——目录为空才按给定属主初始化（PG/Timescale 权限收紧为 `0700`）；目录非空时跳过 host 侧 `chown`/`chmod`，属主交由容器 entrypoint 管理。宿主机自有目录（srs/flink/backups 及 APP_DIR 子目录）仍用 `ensure_dir`。同步更正注释与完成日志。

2. **SRS 健康检查 `localhost`→`127.0.0.1`**（`infra/deploy/docker-compose.yml`，次修复）：V1.16.6 后 hunter-srs 能启动且宿主机 `curl 127.0.0.1:9090/api/v1/versions` 正常，但 `docker compose ps` 仍标 `unhealthy`。原因是 compose 探针用 `http://localhost:9090`，容器内 `localhost` 常解析到 IPv6 `::1`，而 SRS http_api 仅 bind IPv4 → 探针失败。改为 `127.0.0.1`（与 install.sh 探测一致）。否则 remote-control `depends_on srs: service_healthy` 会卡住 Step 11。

向后兼容：端口/服务拓扑不变；仅修正 Step 3 对已存在数据目录的越权 chown 与探针地址，不改变全新环境的首次初始化行为。已部署环境需同步新脚本后生效。

临时手工修复（服务器）：
```bash
# ① 当前库已由容器 entrypoint 自愈回 uid 70（文件/进程一致），先验证可起后端
sudo docker exec hunter-postgres psql -U hunter -d hunter_core -c 'select 1'   # 期望返回 1
# ② 分段续跑（--step 8 会跳过 Step 3，不会再次 chown 改坏）
sudo bash /opt/HunterCore/scripts/install.sh --step 8 -y
# 若 select 1 仍报 Permission denied（属主与进程仍不一致）：
cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . stop postgres
sudo chown -R 70:70 /data/postgresql && sudo chmod 700 /data/postgresql   # uid 以 docker top 实际值为准
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d postgres
# 空库可重置卷（⚠ 仅无业务数据时）：down postgres → rm -rf /data/postgresql → mkdir+chown 70:70 → up -d postgres
# SRS unhealthy 修复：同步 compose 后重建探针
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
sleep 15 && sudo docker inspect --format '{{.State.Health.Status}}' hunter-srs   # 期望 healthy
```

需同步到服务器后生效：将更新后的 `scripts/install.sh` 与 `infra/deploy/docker-compose.yml` 同步到 `/opt/HunterCore`。

风险与待人工确认：① 不同基础镜像的容器 uid 不同（Alpine postgres=70、Debian postgres=999、redis=999、bitnami=1001），因此“非空目录不 host 侧 chown”是根本安全的做法；全新初始化时空目录仍会按注释值预设，容器 entrypoint 会自行校正为镜像内真实 uid；② 若日后换用 Debian 版 timescale 镜像（postgres=999），本修复无需调整；③ SRS 探针改 127.0.0.1 仅影响健康判定，不影响媒体链路。

验证：`bash -n scripts/install.sh` 已过；`docker compose config` 语法合法。实机待按上述手工命令确认：`select 1` 返回 1、Step 8 顺利（schema/timescaledb/init-data 应用完成且校验通过）、hunter-srs 为 `Up (healthy)`。

## V1.16.8（修复：Step 9 创建 Kafka Topic 超时 —— kafka CLI 未附带 SCRAM 凭据）

Date：2026-09-28
变更类型：修复

Step 8 数据库初始化顺利（schema 8/表 13/hypertable 2/TimescaleDB 2.13.1/admin+车辆均达标），Step 9 创建内部 Topic 失败：

```
Error while executing topic command : Timed out waiting for a node assignment. Call: createTopics
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment.
```

### 根因

`infra/deploy/docker-compose.yml` 将 Kafka INTERNAL（9092）监听**硬编码为 `SASL_PLAINTEXT`**（第 211 行 `INTERNAL:SASL_PLAINTEXT`，不随 .env 变化），因此容器内 `kafka-topics.sh` 必须携带 `--command-config`（SCRAM 凭据）才能建立连接。而 `common.sh` 的 `kafka_auth_args` 回退默认值却写的是 `PLAINTEXT`（与 compose 业务服务默认 `SASL_PLAINTEXT` 不一致）；加上服务器生成的 `.env` **缺失 `KAFKA_INTERNAL_SECURITY_PROTOCOL` 键**（`grep` 无输出），导致 CLI 以明文去连要求 SCRAM 握手的 broker，AdminClient 无法建立可用连接/路由到 controller → `createTopics` 报 “waiting for a node assignment”。`kafka_broker_ready` 因先试 `--list`（无凭据失败）后回退 `/dev/tcp` 端口探活而假阳性“就绪”，掩盖了问题。带 SCRAM 凭据直连实测（`--list` via `kafka:9092` 与 `localhost:9092`）均无报错且连通，证实为纯认证缺失、非主机名解析。

### 修复内容

1. **`kafka_auth_args` 回退默认对齐 broker**（`scripts/common.sh`，主修复）：`local protocol="${KAFKA_INTERNAL_SECURITY_PROTOCOL:-SASL_PLAINTEXT}"`（原 `PLAINTEXT`）。即使 .env 未显式设置该键，CLI 也会按 SASL_PLAINTEXT 附带 SCRAM 凭据，与硬编码的 INTERNAL 监听一致。
2. **错误提示默认值同步修正**（`scripts/init-kafka.sh`）：`create_topic` 失败提示中的 `${KAFKA_INTERNAL_SECURITY_PROTOCOL:-PLAINTEXT}` 改为 `:-SASL_PLAINTEXT`，并补充“broker 固定为 SASL_PLAINTEXT、需 KAFKA_SASL_USER/PASSWORD 有效”。
3. **将 `KAFKA_INTERNAL_SECURITY_PROTOCOL=SASL_PLAINTEXT` 纳入 `REQUIRED_CONFIG_VARS`**（`scripts/install.sh` Step 4）：使全新/续跑部署的 `.env` 都显式写入该键（与 `.env.example` 对齐），不再依赖默认值兑底（服务器旧 .env 缺失此键即为本病根）。
4. **修复 `verify_topic` 的 PartitionCount 解析**（`scripts/init-kafka.sh`，认证修复后暴出的第二问题）：原 `awk '{split($i,a,":"); print a[2]}'` 因 kafka `--describe` 输出为 `PartitionCount: 12`（冒号后空格/制表符再接数字，`PartitionCount:` 与 `12` 为两个独立字段），`a[2]` 取空 → `partitions=N/A` 误报“与契约不一致”导致脚本退出码 1（Topic 实际已按 --partitions 正确创建）。改用整行正则 `grep -oE 'PartitionCount:[[:space:]]+[0-9]+'` 提取，兼容 tab/空格。

向后兼容：仅将内部监听认证回退默认从明文改为 SASL_PLAINTEXT，与镜像/compose 既有强制约定对齐，不改变已显式配置 .env 的环境；业务服务侧本就默认 SASL_PLAINTEXT（compose 第 55 行）。

临时手工修复（服务器，脚本未同步前）：
```bash
# 补齐 .env 缺失键（值优先于脚本默认），后续 --step 9 会自动附带 --command-config
if grep -qE '^KAFKA_INTERNAL_SECURITY_PROTOCOL=' /opt/HunterCore/.env; then
  sudo sed -i 's/^KAFKA_INTERNAL_SECURITY_PROTOCOL=.*/KAFKA_INTERNAL_SECURITY_PROTOCOL=SASL_PLAINTEXT/' /opt/HunterCore/.env
else
  printf 'KAFKA_INTERNAL_SECURITY_PROTOCOL=SASL_PLAINTEXT\n' | sudo tee -a /opt/HunterCore/.env
fi
grep -n KAFKA_INTERNAL_SECURITY_PROTOCOL /opt/HunterCore/.env
sudo bash /opt/HunterCore/scripts/install.sh --step 9 -y
```

需同步到服务器后生效：将更新后的 `scripts/common.sh`、`scripts/init-kafka.sh` 同步到 `/opt/HunterCore`（若不同步，仅靠 .env 补齐亦可行）。

风险与待人工确认：① 本部署 INTERNAL 监听由 compose 硬编码 SASL_PLAINTEXT，因此“默认走 SASL”与实际一致；若将来改为真明文 broker，需显式在 .env 设 `KAFKA_INTERNAL_SECURITY_PROTOCOL=PLAINTEXT` 覆盖；② SCRAM 账号由 bitnami 依 `KAFKA_CLIENT_USERS/PASSWORDS` 预置，需保证 `KAFKA_SASL_USER` 已存在且密码与 broker 一致。

验证：`bash -n scripts/common.sh`、`bash -n scripts/init-kafka.sh` 均已过。实机带凭据 `--list` 已连通（无报错）；待按上述手工命令确认 `--create` 探测 topic 成功且 Step 9 六个 Topic 全部建成并通过分区/retention 校验。

## V1.16.9（修复：Step 11 业务服务启动崩溃 —— 非 root 容器无法写 root 所有的 Kafka 缓冲 bind 目录）

Date：2026-09-28
变更类型：修复

Step 9/10 顺利后，Step 11 启动 data-collector：容器 `Restarting (3)` 循环，`docker logs hunter-collector`：

```
File ".../hunter_common/kafka/buffer.py", line 74, in __init__
    self._root.mkdir(parents=True, exist_ok=True)
PermissionError: [Errno 13] Permission denied: 'data/kafka-buffer/data-collector'
```

### 根因

业务服务镜像均以**非 root `uid 10001`（appuser）**运行（`services/*/Dockerfile` 第 16/28 行）。compose 为每个服务挂载 `${DATA_DIR}/kafka-buffer/<service>:/app/data/kafka-buffer`（本地 Kafka 磁盘缓冲）。但 Step 3 只创建了 postgresql/kafka/.../backups 等目录，**未预建 `kafka-buffer/<service>`** → 容器首次挂载时 Docker 以 **root** 自动创建该宿主目录 → `uid 10001` 无权在其内 `mkdir`（`LocalDiskBuffer` 建 `data/kafka-buffer/data-collector` 子目录）→ 启动即崩溃。6 个业务服务同一坑。

### 修复内容

1. **Step 3 预建业务服务 Kafka 缓冲目录并 chown 10001**（`scripts/install.sh` `step_3_prepare_dirs`，主修复）：对 `api-gateway data-collector data-analytics ota-service remote-control scene-service` 逐个 `ensure_dir "${DATA_DIR}/kafka-buffer/${d}" "10001:10001" "0755"`（与六个 Dockerfile 统一 uid 10001 对齐）。`ensure_dir` 对已存但属 root 的旧目录会 `chown -R` 纠正。

向后兼容：仅新增宿主目录与其属主，不改端口/服务拓扑。已被 Docker 误建为 root 的环境，重跑 Step 3 会自动 chown 回 10001。

临时手工修复（服务器，脚本未同步前）：
```bash
# ① 预建并改属主为 10001（六个服务）
sudo mkdir -p /data/kafka-buffer/{api-gateway,data-collector,data-analytics,ota-service,remote-control,scene-service}
sudo chown -R 10001:10001 /data/kafka-buffer
sudo chmod 755 /data/kafka-buffer /data/kafka-buffer/*
# ② SRS 仍 unhealthy（服务器 compose 探针 localhost 未同步为 127.0.0.1），remote-control 会因 depends_on srs healthy 被卡，先修：
sudo sed -i 's#http://localhost:9090/api/v1/versions#http://127.0.0.1:9090/api/v1/versions#' /opt/HunterCore/infra/deploy/docker-compose.yml
cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
sleep 15 && sudo docker inspect --format '{{.State.Health.Status}}' hunter-srs   # 期望 healthy
# ③ 重启 collector 验证不再 PermissionDenied，再续跑 Step 11
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d data-collector
sleep 20 && sudo docker logs --tail=30 hunter-collector
sudo bash /opt/HunterCore/scripts/install.sh --step 11 -y
```

需同步到服务器后生效：将更新后的 `scripts/install.sh` 与 `infra/deploy/docker-compose.yml` 同步到 `/opt/HunterCore`。

风险与待人工确认：① 六个服务均硬编码 uid 10001，若日后某个镜像改 uid 需同步更新本目录属主；② Kafka 磁盘缓冲与业务写入同 uid，多服务共用 `${DATA_DIR}/kafka-buffer` 父目录时各自子目录隔离，权限互不影响；③ 本修复仅解决目录写入权限；若 collector 后续因 Kafka SASL/依赖报新错，需另查日志。

验证：`bash -n scripts/install.sh` 已过。实机待按手工命令确认 chown 后 hunter-collector 为 `Up (healthy)`、Step 11 八个业务服务陆续就绪。

## V1.16.10（修复：Step 11 data-collector 启动崩溃 —— MinIO endpoint 缺 scheme 直传 boto3）

Date：2026-09-28
变更类型：修复

V1.16.9 的缓冲目录权限修复后，hunter-collector 仍 `Restarting`，日志推进到下一处崩溃：

```
File "/app/app/repositories/storage.py", line 245, in _build_sync_client
    return boto3.client("s3", endpoint_url=settings.minio_endpoint, ...)
ValueError: Invalid endpoint: minio:9000
```

### 根因

compose 共享 `business-env` 下发 `MINIO_ENDPOINT: minio:9000`（**host:port，无 scheme**，k8s ConfigMap 同源）。boto3/aioboto3 的 `endpoint_url` 必须含 scheme。各服务实现不一致：
- **scene-service**：`endpoint_url_for()` 已做防护（含 `://` 原样返回，否则按 secure 补）——正确；
- **remote-control / data-analytics**：`f"{'https' if secure else 'http'}://{endpoint}"` 主动补 scheme——当前可用，但若 env 带 scheme 会变双前缀；
- **data-collector / ota-service**：`endpoint_url=settings.minio_endpoint` 裸值——直接报 `Invalid endpoint`。

### 修复内容（均位于会打进镜像的源码，需重建生效）

1. **共享库新增带防护属性**（`common/python/hunter_common/config.py`）：`HunterBaseConfig.minio_endpoint_url`，含 `://` 原样返回，否则按 `minio_secure` 补 `http/https`（与 scene-service 同源）。
2. **四服务统一改用该属性**：`services/data-collector|ota-service|remote-control|data-analytics/app/repositories/storage.py` 的 `endpoint_url` / `self._endpoint_url` 均改为 `settings.minio_endpoint_url`（data-collector/ota 修复崩溃；remote-control/data-analytics 行为等价且消除双前缀隐患）。

向后兼容：compose/k8s 的 `MINIO_ENDPOINT` 保持 host:port 不变；无论 env 是否带 scheme，属性均产出合法 URL。scene-service 未改（已正确）。

临时手工修复（服务器，不重建镜像，给两个裸值服务的 env 直接补 scheme；其余服务保持 host:port）：
```bash
# 仅 data-collector / ota-service 用裸 endpoint，给它们局部覆盖为带 scheme；
# remote-control/data-analytics 会主动补 scheme，绝不能加；否则双前缀。
sudo python3 - <<'PY'
p="/opt/HunterCore/infra/deploy/docker-compose.yml"
s=open(p).read()
s=s.replace('      API_PORT: "8082"\n','      API_PORT: "8082"\n      MINIO_ENDPOINT: http://minio:9000\n',1)  # data-collector
s=s.replace('      API_PORT: "8084"\n','      API_PORT: "8084"\n      MINIO_ENDPOINT: http://minio:9000\n',1)  # ota-service
open(p,"w").write(s)
print("patched")
PY
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d data-collector
sleep 25 && sudo docker ps --filter name=hunter-collector && sudo docker logs --tail=30 hunter-collector
sudo bash /opt/HunterCore/scripts/install.sh --step 11 -y
```

需同步到服务器后生效（长期）：将 `common/python/hunter_common/config.py` 与四个 `storage.py` 同步到 `/opt/HunterCore` 后重跑 Step 6 构建镜像；重建后上述 env 覆盖可保留（防护属性容忍带 scheme 的值）。

风险与待人工确认：① 服务器不重建时的 env 局部覆盖属过渡手段，与服务代码强耦合（若日后 data-collector/ota 改用防护属性但未同步重建，而共享 business-env 又改成带 scheme，则 remote-control/data-analytics 旧镜像会双前缀——因此长期修复应整面重建）；② MinIO 内部链路默认 http（`MINIO_SECURE=false`），k8s 侧为 https（`MINIO_SECURE=true`），属性均已兼顾。

验证：`python -m py_compile` 五个变更文件均通过。实机待确认 hunter-collector 不再报 `Invalid endpoint`、转为 `Up (healthy)`，Step 11 其余服务就绪。

## V1.16.11（修复：Step 11 data-analytics 启动崩溃 —— 运行时依赖 httpx 未声明，仅存于 dev extra）

Date：2026-09-28
变更类型：修复

hunter-collector 已 `Up`（V1.16.10 解锁生效），Step 11 推进到 data-analytics：容器 `Restarting (1)`，日志：

```
File "/app/app/repositories/events.py", line 10, in <module>
    import httpx
ModuleNotFoundError: No module named 'httpx'
```

### 根因

`services/data-analytics/pyproject.toml` 的运行时 `dependencies` 仅声明 fastapi/uvicorn/prometheus-client，**httpx 只在 `[project.optional-dependencies].dev`**。而 `app/repositories/events.py`、`vehicles.py` 顶层 `import httpx`（访问 data-collector 内部 HTTP API）。Dockerfile 构建为 `pip install ./common/python ./service`（不装 dev extra），镜像内无 httpx → 导入即崩。hunter_common 提供 SQLAlchemy/asyncpg/redis/confluent-kafka/pydantic 但不含 httpx/aioboto3/boto3。

### 全量审计（本轮一次排查，避免逐个试错）

对六个业务服务比对“运行时顶层/惰性导入”与“pyproject 声明”：
- **data-analytics**：缺 httpx（顶层→启动崩）+ aioboto3（惰性→报告/评估接口坏）——本次修复；
- **remote-control**：缺 aioboto3（惰性→录像归档接口坏）——本次修复；
- **api-gateway**：已声明 httpx（第 13 行）→ 正常；**scene-service**：已声明 boto3+httpx → 正常；**data-collector/ota-service**：已声明 boto3 → 正常。

### 修复内容（均为依赖声明，需重建镜像生效）

1. `services/data-analytics/pyproject.toml` 运行时 `dependencies` 新增：`httpx>=0.27,<1`、`aioboto3>=12,<14`。
2. `services/remote-control/pyproject.toml` 运行时 `dependencies` 新增：`aioboto3>=12,<14`。

向后兼容：仅补全已有代码实际依赖的包，不改行为/接口；httpx 区间与 scene-service/api-gateway 一致。

临时手工修复（服务器，不跑完整 builder，基于现有镜像叠一层 pip 秒级安装）：
```bash
cat > /tmp/da-deps.Dockerfile <<'EOF'
FROM hunter/data-analytics:0.1.0
USER root
RUN pip install --no-cache-dir "httpx>=0.27,<1" "aioboto3>=12,<14"
USER 10001
EOF
sudo docker build -f /tmp/da-deps.Dockerfile -t hunter/data-analytics:0.1.0 /tmp
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate data-analytics
sleep 25 && sudo docker ps --filter name=hunter-analytics && sudo docker logs --tail=30 hunter-analytics
# 长期修复同步（使后续 Step 6 重建自动含依赖）：把修复后的两个 pyproject.toml 同步到 /opt/HunterCore
sudo bash /opt/HunterCore/scripts/install.sh --step 11 -y
```

风险与待人工确认：① remote-control 的 aioboto3 为惰性导入，不阻断启动/健康，但若不补则录像归档接口首次调用 500，建议同步重建 remote-control；② 若 PyPI 不可达，叠层 pip 与完整重建均会失败，需配镜像源或导入 wheel；③ 本次仅审计到 httpx/aioboto3/boto3 类常见漏项；若还有其它未声明的顶层依赖，会在相应服务启动时同类暴露。

验证：`tomllib` 解析四个 pyproject 均正常且依赖列表正确。实机待确认 hunter-analytics 不再报 ModuleNotFoundError、转为 `Up (healthy)`，Step 11 继续到 scene/ota/remote-control/api-gateway/web-portal。

## V1.16.12（修复：Step 11 remote-control 被卡 —— SRS 镜像无 curl，健康探针永远 unhealthy）

Date：2026-09-29
变更类型：修复

Step 11 推进到 scene/ota/remote-control 时：`dependency failed to start: container hunter-srs is unhealthy`，remote-control（`depends_on srs: service_healthy`）无法启动。`docker inspect` 健康日志：

```
"Status": "unhealthy", "FailingStreak": 5459,
"Output": "OCI runtime exec failed: ... exec: \"curl\": executable file not found in $PATH"
```

而宿主 `curl http://127.0.0.1:9090/api/v1/versions` 正常返回版本 JSON → **SRS 本身健康，只是探针坏**。

### 根因

compose srs 探针为 `test: ["CMD", "curl", "-f", "http://127.0.0.1:9090/api/v1/versions"]`，**在容器内执行 curl**；而 `ossrs/srs:5` 镜像基于精简 Ubuntu，**未安装 curl**（实测 `command -v curl` 为空）→ 探针从首次起就失败，永远 unhealthy。

> 更正 V1.16.7：当时将 unhealthy 归因于 `localhost` 被解为 `::1`，实为误判（服务器 sed 改 127.0.0.1 后仍 unhealthy）；真因是缺 curl。127.0.0.1 写法仍保留（新探针继续用）。本镜像有 `bash`（`/usr/bin/bash`）、无 curl/wget。

### 修复内容

1. **SRS 探针去掉对 curl 的依赖**（`infra/deploy/docker-compose.yml` srs.healthcheck.test）：改为 `test: ["CMD-SHELL", "bash -c '</dev/tcp/127.0.0.1/9090' || exit 1"]`，用 bash 内置 `/dev/tcp` 直连探 HTTP API 端口，不依赖任何外部二进制。

向后兼容：仅改探针实现，不改端口/服务拓扑/启动命令。remote-control 无需再改。若日后镜像提供 curl，可改回真实 HTTP 校验。

临时手工修复（服务器）：
```bash
# 先确认 bash（探针依赖）；预期输出 /usr/bin/bash
sudo docker exec hunter-srs command -v bash
# 用 python3 就地改探针（避开 sed 对 /dev/tcp 斜杠与引号的转义地狱）
sudo python3 - <<'PY'
p="/opt/HunterCore/infra/deploy/docker-compose.yml"
s=open(p).read()
old='      test: ["CMD", "curl", "-f", "http://127.0.0.1:9090/api/v1/versions"]'
new='      test: ["CMD-SHELL", "bash -c \'</dev/tcp/127.0.0.1/9090\' || exit 1"]'
assert old in s, "anchor not found"
open(p,"w").write(s.replace(old,new,1)); print("patched srs healthcheck")
PY
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
sleep 20 && sudo docker inspect --format '{{.State.Health.Status}}' hunter-srs   # 期望 healthy
sudo bash /opt/HunterCore/scripts/install.sh --step 11 -y
```

需同步到服务器长期生效：将更新后的 `infra/deploy/docker-compose.yml` 同步到 `/opt/HunterCore`。

风险与待人工确认：① 新探针为 TCP 探活（端口 open 即健康），不做 HTTP 内容校验；SRS 进程存活但未监听 9090 时仍会被判健康（概率低，且容器已 `restart: unless-stopped`）；② 依赖镜像内含 bash（实测 `/usr/bin/bash` 存在）；若未来换为无 bash 的基底，需改为派生镜像装 curl 或用 `/usr/local/srs/objs/srs` 自行探活。

验证：`yaml.safe_load` 解析 compose 正常，test 解析为 `['CMD-SHELL', "bash -c '</dev/tcp/127.0.0.1/9090' || exit 1"]`。实机待确认 hunter-srs 转 `healthy`、remote-control 及后续 api-gateway/web-portal 启动。

## V1.16.13（修复：Step 12 健康检查将刚启动的 hunter-web 误判 FAIL + Kafka LAG 解析报“需要整数表达式”）

Date：2026-09-29
变更类型：修复

Step 11 全部业务服务 healthy；Step 12 健检：通过 31 / 警告 3 / **失败 1**：
- `[FAIL] 容器 hunter-web - running 但健康状态为 unhealthy`（后实测为 unhealthy，非仅 starting；同行 `HTTP web-portal :80 正常`）；
- `health-check.sh: 第 392/394 行：[: -1-1: 需要整数表达式`，`Kafka 消费积压 LAG=-1-1`。

### 根因

1. **hunter-web 持续 unhealthy（真因：探针误命中 IPv6）**：探针 `test: ["CMD","wget",...,"--spider","http://localhost/"]` 在容器内执行；`config/nginx.conf` 是**整份覆盖** `/etc/nginx/nginx.conf`，`http{}` 内**未 include `conf.d/*.conf`**，官方镜像入口脚本 `10-listen-on-ipv6-by-default.sh` 对 `default.conf` 加的 IPv6 监听**根本没被加载**，nginx 实际仅 `listen 80`（IPv4）。而 alpine 将 `localhost` 优先解为 `::1` → wget 连 `[::1]:80` **Connection refused** → 永远 unhealthy（docker inspect 实报：`wget: can't connect to remote host: Connection refused`；而容器日志里宿主经端口映射走 IPv4 得到 200）→ remote-control/web 下游与 Step 12 均判 FAIL。（另有轻微时序因素：web 是最后启动的容器，首周期内为 `starting`，但持续态是 `unhealthy`。）
2. **LAG=-1-1**：`lag="$(kafka_consumer_lag ... || printf '%s' "-1")"`——`kafka_consumer_lag` 无 offset 时**已自行打印 `-1` 且 `return 1`**，`|| printf -1` 又追加一个 `-1`，两段 stdout 合并为 `-1-1`，导致 `[ -lt 0 ]` 报非整数。

### 修复内容

1. `scripts/health-check.sh` `check_containers` 开头新增**有界等待**：只要 EXPECTED_CONTAINERS 中任一仍为 `starting`，每 5s 重试、最多等 `HEALTH_SETTLE_WAIT`（默认 60s）待其收敛为 healthy/unhealthy 再判定（真实失败仍会因 unhealthy 被判 FAIL，仅消除“刚启动未判完”的假阳性）。
2. `scripts/health-check.sh` LAG 行去冗余 fallback：`lag="$(kafka_consumer_lag ... 2>/dev/null)"; [ -z "$lag" ] && lag="-1"`（不再双拼）。
3. **web 探针根治**：`infra/deploy/docker-compose.yml` 与 `frontend/Dockerfile` 的 web 探针由 `http://localhost/` 改为 **`http://127.0.0.1/healthz`**（显式 IPv4 避免解析到 ::1；`/healthz` 为 `nginx.conf` 第 104 行与门设置的存活探针，返回 200、`access_log off`）。

向后兼容：不改端口/拓扑；等待上限可用 `HEALTH_SETTLE_WAIT` 调整；lag 无 offset 仍为 `-1`→归 WARN（语义不变，仅不再报错）。

临时手工修复（服务器）：
```bash
# hunter-web 很可能已转 healthy（首个探测周期过后）；先确认：
sudo docker inspect --format '{{.State.Health.Status}}' hunter-web   # 期望 healthy
# 拉回修复后的 health-check.sh（含 starting 有界等待 + lag 修正）后重跑 Step 12
sudo bash /opt/HunterCore/scripts/health-check.sh
sudo bash /opt/HunterCore/scripts/install.sh --step 12 -y
```

需同步到服务器长期生效：`scripts/health-check.sh`（必，否则仍会报 -1-1 且可能误判 starting）、`infra/deploy/docker-compose.yml` 与 `frontend/Dockerfile`（web 探针 127.0.0.1，重建后生效）。

风险与待人工确认：① hunter-web 已实测为 `unhealthy` 且 output 为 `Connection refused`，坐实为探针走 ::1 而 nginx 只听 IPv4——改 `127.0.0.1/healthz` 后应转 healthy；若仍失败则需查 nginx 首页/healthz 是否非 2xx；② 有界等待使健检在存在未就绪容器时最多多耗时 60s；③ WARN 项（flink-tm 无 healthcheck、timescale 未单独部署）为契约既定，非缺陷。

验证：`bash -n scripts/health-check.sh` 已过。实机待确认：web `healthy` 后 Step 12 不再因 starting 误报，lag 不再输出 -1-1。


## V1.16.14（修复：Step 7 Kafka 崩溃循环 —— 幂等跳过分支不修 JKS 权限致 broker 读不到 keystore）

Date：2026-10-08
变更类型：修复

重跑部署时 Step 7：`hunter-kafka Restarting(1)`，`kafka_broker_ready` 90s 未就绪。`docker logs` 报：
```
Fatal error during KafkaServer startup
Caused by: Failed to load SSL keystore /opt/bitnami/kafka/config/certs/kafka.keystore.jks of type JKS
Caused by: java.nio.file.AccessDeniedException: /opt/bitnami/kafka/config/certs/kafka.keystore.jks
```

### 根因

broker 以 `user: "1001:1001"` 运行，证书从 `./certs/kafka`（宿主 `${APP_DIR}/certs/kafka`）以 `:ro` 挂入 `config/certs`。`gen-kafka-certs.sh` 本与 `broker_readable_jks`（chgrp 1001 + 0640，组不可用退 0644）保障可读，**但该函数只在“生成”分支调用**；第 217-222 行的**幂等跳过分支**（keystore/truststore 已存在则 `return 0`）**既不 `chmod 0755` 目录、也不重申 JKS 权限**。当证书由更早一轮（旧版脚本或异常中断遗留 `0600 root:root`）生成时，本轮 Step 5 命中跳过→权限永不修复→broker 加载 keystore 报 AccessDenied → SASL_SSL 监听初始化失败 → 整个 broker 崩溃循环（9092 从未监听）。

### 修复内容

`scripts/gen-kafka-certs.sh` 幂等跳过分支新增**权限自愈**：`return 0` 前先 `chmod 0755 "$cert_dir"` 并对 `kafka.keystore.jks` / `kafka.truststore.jks` 各调一次 `broker_readable_jks`，使复用旧证书时也能自我修正目录可穿透与 JKS 对 GID 1001 可读。

向后兼容：不改变证书内容与 SAN；仅对已存在的 JKS 重申目录 0755 与文件组可读权限（无副作用）；`--force` 全量重签路径不变。

临时手工修复（服务器，不依赖同步）：
```bash
# ① 现场确认（看文件与目录实际属主/权限）
sudo ls -ldn /opt/HunterCore/certs/kafka
sudo ls -ln  /opt/HunterCore/certs/kafka/kafka.keystore.jks /opt/HunterCore/certs/kafka/kafka.truststore.jks
getent group 1001 || echo "无命名组 1001（chgrp 用数字 gid 仍可行）"
# ② 就地修复权限（chgrp 失败则改 0644）
sudo chmod 0755 /opt/HunterCore/certs/kafka
if sudo chgrp 1001 /opt/HunterCore/certs/kafka/kafka.keystore.jks /opt/HunterCore/certs/kafka/kafka.truststore.jks 2>/dev/null; then
  sudo chmod 0640 /opt/HunterCore/certs/kafka/kafka.*.jks
else
  sudo chmod 0644 /opt/HunterCore/certs/kafka/kafka.*.jks
fi
# ③ 重启 kafka 并等健康
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d kafka
sleep 25 && sudo docker inspect --format '{{.State.Health.Status}}' hunter-kafka   # 期望 healthy
# ④ 从 Step 7 续跑
sudo bash /opt/HunterCore/scripts/install.sh --step 7 -y
```

需同步到服务器长期生效：`scripts/gen-kafka-certs.sh`（否则下次命中跳过仍可能复发）。

风险与待人工确认：① 若修权限后仍 AccessDenied，则可能是容器内 uid 与宿主 gid 映射差异，需 `sudo ls -ln` 交叉核对（预期文件属组为 1001、组可读）；② 若 `--force` 重签会改变证书，需重启 Kafka 并同步更新车端证书；③ 本条与 V1.16.13 属不同步骤（Step 7 vs Step 12）。

验证：`bash -n scripts/gen-kafka-certs.sh` 已过。实机待确认：kafka `healthy` 后 Step 7 可续跑至 Step 11。


## V1.16.15（修复：Step 7 Kafka 崩溃循环 —— `ensure_kafka_certs_readable` 只按权限位判可读、漏校验属组，`root:root 0640` 证书仍读不到）

Date：2026-10-08
变更类型：修复

续跑部署时 Step 7：`hunter-kafka Restarting(1)`（`restarts=32 exit=1`），`kafka_broker_ready` 90s 未就绪；`docker logs hunter-kafka` 报：
```
Fatal error during KafkaServer startup
Caused by: Failed to load SSL keystore /opt/bitnami/kafka/config/certs/kafka.keystore.jks of type JKS
Caused by: java.nio.file.AccessDeniedException: /opt/bitnami/kafka/config/certs/kafka.keystore.jks
```
现场 `stat` 实测：`kafka.keystore.jks` / `kafka.truststore.jks` 均为 **`root:root 640`**（9 月旧证书；本次 Step 5「证书已存在，跳过生成」，未触发 `gen-kafka-certs.sh`）。

### 根因

broker 以 `user: "1001:1001"` 运行，证书从 `./certs/kafka`（宿主 `${APP_DIR}/certs/kafka`）以 `:ro` 挂入 `config/certs`。Step 7 前置的 `install.sh::ensure_kafka_certs_readable` 本应兜底修复 JKS 可读性，但其**跳过判断只看权限位、不看属组**：
```bash
if [ -r "$f" ] && stat -c '%a' "$f" | grep -qE '^0?[0-7][4-7][0-7]$'; then continue; fi
```
`root:root 640` 的权限位 `640` 命中「组可读」正则（`[4-7]`）→ **直接跳过 chgrp**。但属组是 `root`（gid 0）而非 broker gid `1001`，容器用户 `1001` 既非属主也不在 root 组，落到 "other"（无权限）→ 加载 keystore `AccessDenied` → SASL_SSL(EXTERNAL 9093) 监听初始化失败 → 整个 broker 崩溃循环（INTERNAL 9092 从未监听）。此路径与 V1.16.14 不同：V1.16.14 修的是 `gen-kafka-certs.sh` 生成/跳过分支，而本场景 Step 5 完全未调用该脚本，唯一能兜底的正是 `ensure_kafka_certs_readable`，却因漏校验属组而失效。

### 修复内容

`scripts/install.sh` `ensure_kafka_certs_readable`：跳过条件由「仅权限位」改为**同时校验属组 gid 与组可读位**——新增 `cur_group="$(stat -c '%g' "$f")"` / `cur_mode="$(stat -c '%a' "$f")"`，仅当 `cur_group == $gid`（默认 1001）**且** `cur_mode` 组可读时才 `continue`；否则执行 `chgrp "$gid"` + `chmod 640`（组不存在退回 `0644`）。使 `root:root 0640` 这类「组是 root」的历史证书被正确识别并自愈。

向后兼容：不改证书内容与 SAN；仅对已存在的 JKS 重申属组/组可读权限（无副作用）；已满足「属组=1001 且组可读」的证书仍幂等跳过。

临时手工修复（服务器，不依赖脚本同步）：
```bash
# ① 就地修权限：把 JKS 属组改为 broker gid 1001（保持 0640）
sudo chgrp 1001 /opt/HunterCore/certs/kafka/kafka.keystore.jks \
                /opt/HunterCore/certs/kafka/kafka.truststore.jks
sudo chmod 640  /opt/HunterCore/certs/kafka/kafka.keystore.jks \
                /opt/HunterCore/certs/kafka/kafka.truststore.jks
# ② 证书为 :ro 挂载，改宿主文件即时生效；重启崩溃循环中的容器
sudo docker restart hunter-kafka
# ③ 确认进入监听（约 30~60s），期望 9092-LISTENING
sudo docker exec hunter-kafka bash -c 'exec 3<>/dev/tcp/127.0.0.1/9092 && echo 9092-LISTENING'
# ④ 续跑（幂等，从 Step 7 继续）
sudo bash /opt/HunterCore/scripts/install.sh -y --ip 192.168.31.35
```

需同步到服务器长期生效：`scripts/install.sh`（否则下次命中 `root:root 0640` 仍会复发）。

风险与待人工确认：① 若 `chgrp 1001` 报「组不存在」，脚本会自动退回 `0644`（仅单机可接受，多租户需建命名组 1001）；② 若改组后仍 `AccessDenied`，需 `sudo ls -ln` 交叉核对容器内 uid/gid 映射；③ 本条与 V1.16.14 属同一 Step 7 的不同兜底点（`gen-kafka-certs.sh` 分支 vs `install.sh::ensure_kafka_certs_readable`），建议两处一并同步。

验证：`bash -n scripts/install.sh` 已过；跳过判断对 `root:root 0640`（`cur_group=0≠1001`）不再命中 `continue`，会走 `chgrp 1001 + chmod 640`。实机待确认：kafka `healthy`（9092 监听）后 Step 7 续跑至 Step 11。


## V1.16.16（修复：首登强制改密弹窗校验失败时静默无反馈，用户误以为“确认修改”无效/弹窗关不掉）

Date：2026-10-08
变更类型：修复

首次登录后弹出「首次登录须修改口令」（G-06），输入新口令点「确认修改」后弹窗仍停留、无法关闭。现场截图中 `新口令` 报红「需包含大写字母、小写字母与数字」，且三个输入框内容均未清空。

### 根因

弹窗可见性由 `userStore.mustChangePassword`（computed，读 `profile.must_change_password`）单向驱动（`:model-value`），按 G-06 设计**不可关闭**（无关闭按钮、遮罩/ESC 均不关），唯一出口是改密成功（store 本地置 `must_change_password=false`）或退出登录。`frontend/src/layouts/MainLayout.vue` 的 `handleSubmitChangePassword` 中 `await form.validate()` 失败时**直接 `return`，无任何提示**；当新口令不满足强度规则（长度 8–128 且同时含大写/小写/数字）时，校验拦截→静默返回→弹窗不关也不报错，用户误以为主按钮失效/弹窗卡死。（表单未清空也佐证提交从未真正发出：成功分支会 `Object.assign(pwdForm, { oldPassword: '', newPassword: '', confirmPassword: '' })` 重置。）

### 修复内容

`frontend/src/layouts/MainLayout.vue` `handleSubmitChangePassword`：`form.validate()` 的 catch 分支新增 `ElMessage.warning('口令不符合要求：长度需 8–128 位，且同时包含大写字母、小写字母与数字')`，将静默拦截变为显式反馈。未改弹窗不可关闭的 G-06 语义（仍仅“改密成功/退出登录”可关）。

向后兼容：不改接口/字段/校验规则（`validateStrength` 与契约 `ChangePasswordRequest.new_password` pattern 一致）；仅新增一条失败提示，无副作用。

临时规避（无需改码）：将新口令改为满足强度规则（≥ 8 位、同时含大写+小写+数字，如 `Hunter@2026abc`）再点「确认修改」即可正常提交并自动关闭；当前口令输错会弹「当前口令不正确」。

需同步到服务器长期生效：`frontend/src/layouts/MainLayout.vue`，并重建 `web-portal` 镜像：
```bash
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build web-portal
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d web-portal
```

风险与待人工确认：① 本条为体验修复，若用户实际已输入合法口令仍不关，则需排查 `change-password` 接口是否返回 `code=0`（失败会走 catch 弹错，弹窗不关属预期）；② 弹窗按 G-06 故意不可关闭，若业务需要“未改密也能浏览”需另行评审（会削弱首登强制改密安全约束）。

验证：`ElMessage` 已在组件内导入，无新增依赖；GetProblems 无错。实机待确认：弱口令点「确认修改」会弹出口令要求提示；合法口令提交后弹窗自动关闭。


## V1.17.0（新增：场景库批量添加脚本 + 300 个自动驾驶训练高频场景数据集，覆盖全功能与极端环境，满足 Carla 仿真）

Date：2026-10-08
变更类型：新增

背景：自动驾驶算法训练需要成规模、覆盖全功能与极端环境的高频场景数据集。此前场景库仅有 17 个内置模板（4.2.1 叶子场景），缺少「一键批量灌库」能力，故新增按系统场景库规范批量创建 300 个场景的脚本。

### 新增内容

1. `scripts/batch_add_scenes.py`（新增）——按场景库规范（设计文档 4.2.1 分类体系 + 4.2.2 场景配置结构，落库 `scene_svc.scenes.config_json`）确定性生成 300 个高频训练场景，并经 scene-service `POST /api/v1/scene` 批量入库：
   - 配额矩阵 `SCENE_QUOTAS` 覆盖 4 大类 16 个叶子场景类型（basic 71 / interactive 89 / environment 66 / corner_case 74，和恒=300，模块级 `assert` 守卫，配额/生成器缺项在导入期即报错）；
   - 满足 Carla 仿真训练：`map_type=carla_town`（Town01~07 轮换）、自车基线车型 `HUNTER_SE`、7 字段天气段对齐 `carla.WeatherParameters` 语义、参与者行为采用 ScenarioRunner 词汇（lane_follow / cut_in / cross_road / static / constant_velocity / forward）；
   - 覆盖全部功能与极端环境：感知预测（切入/行人/多车混行/障碍突现）、决策规划（跟车/换道/路口会车/紧急制动/施工绕行）、控制（直线/弯道）、鲁棒（雨/雾/夜/逆光）、失效安全（传感器失效）；16 套天气预设含暴雨 / 浓雾 / 黑夜 / 冬季暴雪 / 大风 / 强逆光等；
   - 严守场景库规范：每条提交前以真实 `SceneCreateRequest`（`extra="forbid"`）自校验，Schema 不可导入（如旧版 Python 无 `StrEnum`）时降级轻量校验；命名全局唯一 `SCN-001…SCN-300`（重名 → 3002 幂等跳过）、时长 ≤ 600s；
   - 双模式认证：`--mode direct`（`X-User-Id`/`X-Roles`，可选 `GATEWAY_HMAC_SECRET` 经 `hunter_common.internal_auth` 验签头）/ `--mode gateway`（登录换 JWT Bearer）；异步并发 + 429/5xx 指数退避重试 + `--publish`（draft→published）+ `--dry-run`/`--limit`/`--start`（断点续跑）/`--only-type` 等；仅复用 `app.schemas.scene` 与 `hunter_common`，不改任何服务/契约代码。
2. `scripts/output/scene_batch_300.json`（新增产物）——300 条场景清单（UTF-8），每条即一个 `POST /api/v1/scene` 请求体，可离线审阅或作为数据集归档；入库后经 `POST /api/v1/scene/export` 导出 ScenarioRunner XML / OpenSCENARIO 1.2。

向后兼容：纯新增脚本与产物，不改任何服务/契约/接口/DDL；场景仍走既有 `createScene`/`publishScene` 端点（status=draft、creator=网关注入用户、version=1.0.0），无 schema 变更。

风险与待人工确认：① 场景命名/配额/难度参数若与内部命名规范不符，可在对应生成器（`_b_*`）或 `SCENE_QUOTAS` 调整；② `real_vehicle_replay`（4.5 节自动提取产物、无预置模板）与 `custom` 不在批量数据集内；③ Carla 地图资源清单与管理 API 键名为契约待确认项，脚本已环境变量化，确认后可仅调配置不改代码；④ 需 Python 3.11+（项目 venv 3.12）方可启用真实契约 Schema 校验。

验证：Python 3.12 venv `python scripts/batch_add_scenes.py --dry-run` 输出 `[validate] 通过 300/300（Schema=True）`，分布统计覆盖 16/16 类型、极端/安全长尾 122 条；数据集共 300 条、场景名全唯一、`max_name_len=39`、`max_duration=120s`、全部 `carla_town`；`ruff check scripts/batch_add_scenes.py` All checks passed。实机入库待确认：服务/网关可达时执行 `--mode gateway ... --publish` 全量提交并核对 `hunter-scene-assets` 导出产物。


## V1.17.1（修复：`batch_add_scenes.py` 在生产服务器（非 root + 系统 python3.10）上 dry-run 因 `PermissionError` 直接崩溃）

Date：2026-10-08
变更类型：修复

服务器 `dongyan@hunter-core-server:/opt/HunterCore` 执 `python3 scripts/batch_add_scenes.py --dry-run`：场景正常生成 300/300（但降级轻量校验），随后写盘失败报错退出：
```
PermissionError: [Errno 13] Permission denied: '/opt/HunterCore/scripts/output/scene_batch_300.json'
  File "/opt/HunterCore/scripts/batch_add_scenes.py", line 986, in main
```

### 根因

两个独立问题（同一场景暴露）：
1. **写盘未做容错（导致崩溃的主因）**：`main()` 的 dry-run 分支直接 `out.write_text(...)`，无任何异常处理。部署目录 `/opt/HunterCore` 为 root 属主，普通用户 `dongyan` 对 `scripts/output/` 无写权限 → `PermissionError` 直接抛到顶层，整个运行非零退出。
2. **解释器降级（不致命，但校验强度下降）**：系统 `python3` 为 3.10，`from app.schemas.scene import ...` 报 `cannot import name 'StrEnum' from 'enum'`（`StrEnum` 仅 3.11+ 有），脚本自动降级轻量校验（数据集仍 300/300）；且 `load_schema_validator()` 未缓存，导致告警刷屏两次。

### 修复内容

`scripts/batch_add_scenes.py`：
- **新增 `_dump_scenes()` 进行写盘容错**：写目标不可写（`OSError`/`PermissionError`）时自动回退到系统临时目录（`tempfile.gettempdir()/文件名`）并打印回退提示与修复建议（`sudo chown` / `--output ~/...` / `--output -`）；若临时目录仍不可写则输出到 **stdout**——全程**不再崩溃**，退出码 0；支持 `--output -` 显式输出 stdout。
- **`load_schema_validator()` 结果缓存**（`_SCHEMA_VALIDATOR_LOADED` 哨兵），同一进程只尝试导入一次、只告警一次；降级告警补充“需 Python 3.11+，请改用项目 venv 或 3.11+ 解释器”提示。
- 顶部新增 `import tempfile`；文档字符串用法示例改为 `python3` 并注释不可写时的回退与替代路径。

向后兼容：不改场景生成/校验/提交逻辑与任何服务/契约代码；仅增强 dry-run 写盘健壮性与告警降噪；默认行为（目标可写时写入原路径）不变。

临时规避（服务器，不依赖代码同步）：
```bash
# ① 写到用户可写目录（避开 root 属主的 scripts/output）
python3 scripts/batch_add_scenes.py --dry-run --output ~/scene_batch_300.json
# 或直接输出到 stdout 重定向
python3 scripts/batch_add_scenes.py --dry-run --output - > ~/scene_batch_300.json
# ② 若要启用真实契约 Schema 校验，用 3.11+ 解释器（如项目 venv）
ls -d .venv 2>/dev/null && .venv/bin/python scripts/batch_add_scenes.py --dry-run
# 或 sudo apt-get install -y python3.11 && python3.11 scripts/batch_add_scenes.py --dry-run
```

需同步到服务器长期生效：`scripts/batch_add_scenes.py`。

风险与待人工确认：① 回退写入的临时文件为当前用户临时目录，多用户共享主机下需注意文件名冲突（固定为 `scene_batch_300.json`）；② 若要真实契约级校验仍必须 3.11+ 解释器（3.10 只能降级轻量校验）；③ 实际入库（非 dry-run）若 `--user-id` 无写权限（需 admin/operator）会返回 403。

验证：`ruff check scripts/batch_add_scenes.py` All checks passed；Python 3.12 venv 正常路径仍写入原目录并 `[validate] 通过 300/300（Schema=True）`；模拟不可写目标（`--output C:\Windows\_forbidden_dir_test\out.json`）退出码 0、自动回退临时目录并打印修复建议，不再崩溃；`--output -` 正常输出 stdout。


## V1.17.2（修复：`batch_add_scenes.py` gateway 模式批量入库遭网关限流，300 条仅建 100、publish 全部 429 失败）

Date：2026-10-08
变更类型：修复

服务器容器内执行 `--mode gateway ... --publish`（默认 `--concurrency 5`）：`[auth] 登录成功` 后，前 100 条 create 成功，随后 create 与全部 publish 持续返回 `HTTP 429 code=5001`；最终 `新建:100 发布:0 失败:300`（200 create 失败 + 100 publish 失败）。前端「场景库」同时显示「服务不可用 + 共 0 条」（同一波 429 打到列表 GET）。

### 根因

`code=5001 + HTTP 429` 是 **api-gateway 限流**（本项目限流无专用码，复用 5001 + 429，见 `x-hunter-rate-limits` / `RATE_LIMIT_PER_USER_QPS`）。脚本侧三个缺陷叠加：
1. **无全局请求限速**：`concurrency=5` 下 300 create + 300 publish = 600 请求秒级打满每用户 QPS；
2. **`_publish_scene` 完全无重试**：一次 429 即永久失败（本次「发布:0」直接原因）；
3. **create 重试退避过短**（固定 `0.6*(attempt+1)`、`max_retries=2`）且未读 `Retry-After`，持续高压下重试耗尽。

### 修复内容

`scripts/batch_add_scenes.py`：
- 新增 `_RateLimiter`（全局令牌式节流）与 CLI **`--qps`（默认 8.0，0=不限速）**，将 create+publish 总发起速率压到网关阈值之下；`submit_scenes` 构造 `limiter` 并贯穿 `_post_scene`/`_publish_scene`（每次请求前 `await limiter.acquire()`）。
- **`_publish_scene` 改为带重试循环**：对 429/5xx/超时按退避重试，不再一次失败即弃。
- 新增 `_retry_delay()`：**429 优先遵循响应头 `Retry-After`**，否则指数退避（封顶 `--retry-max`）；`_post_scene` 统一改用。新增 CLI `--backoff`（默认 1.0）/`--retry-max`（默认 30.0）；`--max-retries` 默认 2→4；提交日志补充 `qps=`。

向后兼容：不改场景生成/校验/入库语义与任何服务/契约代码；仅新增限速与重试参数（均有默认值）；默认 `--qps 8` 低于网关 100 QPS，不影新导入；幂等（重名 3002 跳过）不变。

服务器操作（需先 `docker cp` 新脚本进容器）：
```bash
cd /opt/HunterCore
docker cp scripts/batch_add_scenes.py hunter-scene:/tmp/
# 限速重跑（已建 ~100 走 3002 跳过；create+publish 一次完成，publish 现会重试 429）
docker exec hunter-scene python3 /tmp/batch_add_scenes.py \
  --mode gateway --base-url http://api-gateway:8080 \
  --username admin --password '<admin口令>' \
  --publish --qps 6 --concurrency 3 --max-retries 6
```

需同步到服务器长期生效：`scripts/batch_add_scenes.py`。

风险与待人工确认：① **幂等跳过分支不补发布**——首轮已建但未发布的 ~100 条，重跑会命中 3002 而停在 draft，需前端批量发布或后续新增 `--publish-only` 分支按名补发；② `--qps` 需根据实际 `RATE_LIMIT_PER_USER_QPS` 调整（若调低网关阈值需同步下调）；③ 前端「服务不可用/共 0 条」为限流瞬时现象，请求高峰过后刷新即可见（非数据丢失）。

验证：`ruff check scripts/batch_add_scenes.py` All checks passed；`py_compile` 通过。实机待确认：限速重跑后 `新建+幂等跳过=300`、`发布=300`、`失败=0`；前端「场景库」刷新后可见全量场景。



## V1.18.0

Date：2026-10-08
变更类型：新增
版本主要有以下功能：

1. **新增 vehicle-service（端口 8086）与契约 `contracts/openapi/vehicle-service.yaml`**（OpenAPI 3.0.3）：面向运营/操控员的“车辆台账 + 车端接入一键开通”服务，将历史上需手工 SQL 插行 + `kafka-configs.sh` 建 SCRAM + `kafka-topics.sh` 建 8 个 Topic + `openssl req/x509/pkcs12` 一车一证的 4 步手动流程收敛为单页向导；本服务不参与业务 Topic 生产/消费，仅在 provisioning 时执行 Kafka 控制面操作
2. **一键开通 4 步编排与失败逆序回滚**（`app/services/provisioner.py`）：DB 台账 INSERT → SCRAM 用户 UPSERT（`secrets.token_urlsafe(24)` 一次性）→ 8 个车端 Topic 创建（从 `contracts/kafka/topics.yaml` 动态加载，与 `init-kafka.sh` 语义一致）→ openssl 签发独立客户端证书（CN=vehicle_id、SAN=DNS:vehicle_id、3650 天、PKCS12）；任一步失败逆序回滚（cert → topics → scram），DB 台账行保留并标 `provision_status.state=failed` 供审计；`takeover_existing=true` 时已存在外部资源不回收
3. **Kafka 控制面封装**（`app/services/kafka_admin.py`）：采用 `kafka-python-ng>=2.2`（KIP-95 分支，社区 kafka-python 2.0.x 无 `AlterUserScramCredentials`），AdminClient 走 INTERNAL 9092 SASL_PLAINTEXT，避免 `docker exec` 挂载宿主 socket；principal_type=2（KAFKA_PRINCIPAL_TYPE）、scram_mechanism_id=1（SCRAM-SHA-512、iterations=8192）；`asyncio.to_thread` 封装 openssl/AdminClient 阻塞调用 + `threading.Lock` 串行化
4. **openssl 子进程签发与文件权限矩阵**（`app/services/cert_signer.py`）：5 步 openssl（genrsa → SAN ext → req → x509 -CA -set_serial → pkcs12 -export）；采用 `-set_serial 0x<hex>` 而非 `-CAcreateserial`，适配 CA 目录 `:ro` 挂载；目录 0750、key/p12/p12-password 0600、cert 0640；失败清理本次产物；p12 口令 `secrets.token_urlsafe(16)` 每车随机，落 `p12-password` 文件不入日志
5. **接入包 ZIP 与 kafka.properties 模板**（`app/services/bundle.py`）：6 文件（ca-cert.pem / client-cert.pem / client-key.pem / kafka-client.p12 / kafka.properties / README.md）；**SCRAM 口令不在 bundle 中**（只在 create/rotate 响应一次性下发），`kafka.properties` 中 `password="<SCRAM_PASSWORD>"` 为占位符，p12 口令写入头部注释；`/bundle` 返回 `application/zip`（契约明确例外，不走统一 JSON 信封）
6. **`vehicle_svc.vehicles` 新增 `provision_status` JSONB 列**（已在 V0.3.0 DDL 基础上下文列定义）：`{state, steps: {db|scram|topics|cert → {name, state, error, ts}}}`；列表接口按 `provision_status->>'state'` 表达式过滤，无需新表；`init-data.sql` 新增 5 个权限码（`vehicle:create/read/update/delete/execute`）+ 绑定 admin/operator（operator 无 delete）
7. **api-gateway 路由回填**：`contracts/openapi/api-gateway.yaml` 中 `/api/v1/vehicle` `target_port: 8086` 且去除 `pending_confirmation`；`services/api-gateway/app/{config, services/proxy_service}.py` 中 `VEHICLE_SERVICE_URL` 默认 `http://localhost:8086`（K8s/compose 内为 Service DNS）；相关契约测试 `test_openapi_contract.py` 中 pending 集从 `{vehicle, user}` 收敛为 `{user}`；`test_proxy.py::test_pending_prefix_returns_503` 改以 `/api/v1/user/profile` 为待确认前缀样本
8. **前端新增“车辆管理”入口**：`/vehicle/list` 与 `/vehicle/:vehicle_id` 两个顶层路由（侧边菜单 `Van` 图标，需 `vehicle:read`）；`views/vehicle/` 4 个组件：`VehicleListView`（表格 + 4 步徒标 + 行内“下载/重发口令/重发证书/下线”）、`VehicleDetailView`（基本信息 + Kafka 接入摘要 + 时间线 + 台账修改）、`VehicleCreateDialog`（三步向导）、`BundleDownloadNotice`（一次性 SCRAM 口令 + 确认关闭 + kafka.properties 预览）；`api/request.ts` 新增 `downloadBlob`（responseType='blob' + 失败时从 Blob 解出统一信封抛 HunterApiError + Content-Disposition 提取文件名）
9. **docker-compose + install.sh 集成 + 权限自愈**：compose 新增 vehicle-service 块（`user: 10001:10001`，`./certs/kafka:/srv/kafka-ca:ro` + `${DATA_DIR}/vehicle-certs:/app/data/vehicle-certs:rw` + KAFKA_BOOTSTRAP_SERVERS=kafka:9092）；`install.sh` 预建 `${DATA_DIR}/vehicle-certs` 0750 gid 10001；`ensure_kafka_certs_readable` 扩展：`setfacl -m g:10001:r-- ca-key.pem`，无 setfacl 时退回 `chgrp 10001 + 0640`；step_11 启动列表 + 健康等待加入 vehicle-service；`.env.example` 新增 §9b `VEHICLE_SERVICE_URL / VEHICLE_SERVICE_PORT / VEHICLE_CERTS_DIR / VEHICLE_CERT_RETENTION_DAYS`
10. **后端测试 4 个文件 + 契约自检**（`services/vehicle-service/app/tests/`）：`test_vehicle_contract.py`（176 行：契约 paths ⊆ FastAPI 实际路由 + RBAC 词表 + x-hunter-kafka Topic 数量=8 + api-gateway 路由 target_port=8086 非 pending）；`test_provisioner.py`（319 行：happy path + DB 短接冲突 + SCRAM/Topics/Cert 3 类失败回滚 + takeover_existing 不删 SCRAM + deprovision 顺序）；`test_cert_signer.py`（205 行：openssl 5 步阶段覆盖 + `-set_serial 0x<hex>` 参数断言 + 失败清理 + 超时映射 5001 + CA 缺失 fail-fast）；`test_kafka_admin.py`（216 行：_FakeAdmin 验证 telemetry 6 分区 + retention.ms=604800000 + SCRAM 请求体 principal_type/mechanism_id/password bytes 化）
11. **文档更新**：`docs/frontend-portal.md` 新增 §5 “车辆管理与接入向导”，目录职责表 `views/api/types/constants` 补 vehicle；`docs/运维操作手册.md` §7.4 重构为“前端方式（推荐）+ 命令行兜底”（前端作为主入口，手工创建的用户/Topic/证书不会自动写 vehicle_svc.vehicles，可`takeover_existing=true` 接管）；`docs/api-gateway.md` pending #7 从“vehicle/user 归属”拆为“user 归属” + 新增 #8 “provisioning 接口级限流”；`services/api-gateway/README.md` 与 `services/vehicle-service/README.md` 同步路由表与配置项

验证：
- `pytest services/vehicle-service -q` 4 个测试文件新增（具体 passed 数需服务目录内 venv 就位后确认）
- `pytest services/api-gateway/app/tests/test_openapi_contract.py -q` pending 集修正；`test_proxy.py::test_pending_prefix_returns_503` 改用 `/api/v1/user/profile` 断言仍 503
- `ruff check services/vehicle-service services/api-gateway` 全部通过
- `bash scripts/install.sh --from 11 --to 11`（已部署环境）：vehicle-service 健康检查就继；`setfacl -g ca-key.pem` 验证 gid 10001 可写
- 手工冒烟（待完成）：前端创建 HUNTER-002 → 下载 bundle → `kafka-console-producer.sh --bootstrap-server <SERVER_IP>:9093 --producer-property security.protocol=SASL_SSL --consumer.config bundle/kafka.properties` 发送一条 telemetry → data-collector 日志确认消费

风险与待人工确认：
1. **CA 私钥暴露给业务容器**：ca-key.pem 通过 setfacl ACL 让 vehicle-service gid 可读（不入镜像层/日志）——本 MVP 适合单机部署形态；生产环境应将根 CA 下沉到外部 KMS/HSM（例如 Vault PKI 或云 KMS）并由 vehicle-service 提交 CSR 接取签名，需确认安全基线后扩展
2. **kafka-python-ng fork 选型**：与社区 kafka-python API 同命名空间，支持 KIP-95；若团队不接受 fork 依赖，可改 `docker exec hunter-kafka kafka-configs.sh` 子进程方案（需挂载 docker socket，安全面更差）；需确认
3. **每车 Topic 数量 = 8**：与当前 `topics.yaml` 车辆 Topic 条目一致；未来新增须同步 `contracts/kafka/topics.yaml` + `contracts/openapi/vehicle-service.yaml:x-hunter-kafka.per_vehicle_topics` + `constants/vehicle.ts:PER_VEHICLE_TOPIC_COUNT` 三处
4. **删除车辆不删时序表数据**：`vehicle_telemetry`/`events`/`ota_records` 按 vehicle_id 逻辑外键保留审计；如需清空由 DBA 走 `backup.sh` 前的手动步骤
5. **回滚边界**：provisioner 中途失败时，若 SCRAM 用户已存在（历史遗留）则跳过创建并继续；证书签发失败仅回收本次产物，不删除 CA
6. **provisioning 接口级限流**：契约建议 1 QPS/用户，附录 D 未定义，本 MVP 沿用全局 QPS（单用户 100）；附录 D 确定后回填契约
7. **CRL 未实现**：MVP 重发证书不会自动吊销旧证书（旧证书仍在 CA 有效期内），需确认安全基线后扩展 CRL/OCSP
8. **前端 type-check 未跑**：`npx vue-tsc --noEmit` 需 `frontend/` 目录内 `npm ci` 后确认；本次交付已保证契约→类型→API 三层命名一致（snake_case），新增 4 个 Vue 组件未引入新的依赖

需同步到服务器长期生效：`contracts/openapi/{vehicle-service.yaml,api-gateway.yaml}`、`services/vehicle-service/**`、`services/api-gateway/app/{config.py, services/proxy_service.py, tests/*}`、`infra/deploy/{docker-compose.yml,.env.example,sql/init-data.sql,sql/schema.sql}`、`scripts/install.sh`、`frontend/src/**`（types/constants/api/views/router/layouts）、`docs/{api-gateway,frontend-portal,运维操作手册}.md`。


## V1.18.1（修复：`install.sh` Step 6 构建镜像时 pip 读超时致 api-gateway 构建失败、`--parallel` 连锁取消其余服务）

Date：2026-10-08
变更类型：修复

服务器 `dongyan@hunter-core-server:/opt/HunterCore` 执行 `sudo bash scripts/install.sh -y --ip 192.168.31.35`：Step 1–5 幂等通过，Step 6（`docker compose build --parallel`）中 web-portal 前端构建成功（`#49 ✓ built in 7.71s`），但 api-gateway 在 `RUN pip install --prefix=/install ./common/python ./service` 安装构建依赖（hatchling）时报错退出，部署在 Step 6 终止（退出码 1）：
```
Collecting hatchling
  Downloading hatchling-1.32.4-py3-none-any.whl (78 kB)
ERROR: Exception:
TimeoutError: The read operation timed out
pip._vendor.urllib3.exceptions.ReadTimeoutError:
  HTTPSConnectionPool(host='files.pythonhosted.org', port=443): Read timed out.
target api-gateway: failed to solve: process "/bin/sh -c pip install ..." did not complete successfully: exit code: 1
```

### 根因

纯网络问题，与项目代码无关：
1. **pip 走官方源读超时（主因）**：builder 阶段 `pip install` 从 `files.pythonhosted.org` 下载构建依赖时 `ReadTimeoutError`——国内访问 PyPI 官方源不稳定，pip 默认读超时仅 15s、重试 5 次不足以覆盖抖动。
2. **`--parallel` 连锁取消（放大器）**：api-gateway 失败后 Compose 取消其余并发构建任务（ota/data-collector/scene/vehicle/remote-control/data-analytics 全部 `CANCELED`），故日志中只有 api-gateway 报到真正的错误，其余服务未必自身有问题。

### 修复内容

按项目惯例（pip `ReadTimeoutError` 优先切国内源 + 加大超时/重试，不排查代码逻辑），为 7 个 Python 服务 Dockerfile 的 builder 阶段 `ENV` 追加国内 PyPI 源与超时/重试（`python:3.11/3.12-slim` 的 pip 均识别这些 `PIP_*` 环境变量）：

- `services/api-gateway/Dockerfile`
- `services/scene-service/Dockerfile`
- `services/vehicle-service/Dockerfile`
- `services/data-analytics/Dockerfile`
- `services/data-collector/Dockerfile`
- `services/remote-control/Dockerfile`
- `services/ota-service/Dockerfile`

新增变量（原 `PYTHONDONTWRITEBYTECODE/PIP_NO_CACHE_DIR/PIP_DISABLE_PIP_VERSION_CHECK` 保留）：
```dockerfile
PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=10
```

向后兼容：不改任何安装逻辑、镜像分层结构、运行阶段与端口；仅在 builder 的 `ENV` 增加镜像源与超时/重试参数；默认行为不变（源可达时结果与官方源一致）；runner 阶段（`COPY --from=builder`）无变化。

服务器操作（需先将 Dockerfile 同步到服务器，再断点续跑 Step 6）：
```bash
# ① 同步改动（本地 Windows → 服务器）
rsync -av --relative services/*/Dockerfile dongyan@192.168.31.35:/opt/HunterCore/
# ② 从失败步骤续跑（Step 1–5 幂等，无需重跑）
cd /opt/HunterCore && sudo bash scripts/install.sh --step 6 -y
# ③ 可先单独验证一个服务的镜像效果
docker compose -f infra/deploy/docker-compose.yml --project-directory . build api-gateway
```

需同步到服务器长期生效：`services/{api-gateway,scene-service,vehicle-service,data-analytics,data-collector,remote-control,ota-service}/Dockerfile`。

风险与待人工确认：① 若服务器访问清华源同样受限，需将 `PIP_INDEX_URL`/`PIP_TRUSTED_HOST` 两处同步换成阿里云源（`https://mirrors.aliyun.com/pypi/simple/`）；② 硬编码公网源，若后续需要内网私有 PyPI 或按环境切换，建议改造为 `ARG PIP_INDEX_URL`（build-arg）由 `.env` 注入，本次未做（待确认）；③ 本次仅覆盖 pip 超时；日志末尾排查建议提到的基础镜像拉取超时（`registry-mirrors`）是独立问题，未在本次变更内；④ `--parallel` 下一处失败仍会取消其余构建（Docker 默认行为），续跑即可恢复，未改为串行。

验证：`ruff`/`docker compose config` 无语法错误；GetProblems 无错；Dockerfile `ENV` 多行续行与原有变量合并正确。实机已验证（2026-10-08）：重跑 Step 6 `pip install` 走清华源耗时 663s 成功完成，7 个 hunter/* 镜像构建成功并顺利进入后续步骤。


## V1.18.2（修复：vehicle-service 因 `kafka_admin.py` 导入 kafka-python 旧版拼错异常名 `AleadyHasPartitionException`，在 kafka-python-ng 环下 import 崩溃、容器重启循环致 Step 11 卡住）

Date：2026-10-08
变更类型：修复

接 V1.18.1 pip 修复后重跑，Step 11 其余服务（scene/ota/remote/collector/analytics/flink）均秒级就绪，唯 vehicle-service（8086） 120s 内 `/healthz` 探测不过。`docker compose ps` 显示容器 `Restarting (1)`（非“慢”，而是启动即崩溃）；`docker compose logs` 真实堆栈：
```
File "/app/app/services/kafka_admin.py", line 30, in <module>
    from kafka.errors import ( ... )
ImportError: cannot import name 'AleadyHasPartitionException' from 'kafka.errors'
```

### 根因

`AleadyHasPartitionException` 是 kafka-python 旧仓库的**拼写错误类名**（"Al**ea**dy"，应为 Already）；本项目运行时依赖已改用 **kafka-python-ng** fork（`pyproject.toml: kafka-python-ng>=2.2,<3`），该 fork 不再导出此旧错名 → `from kafka.errors import AleadyHasPartitionException` 在 **import 期**直接抛 `ImportError` → uvicorn 加载 `app.main` 失败退出（码 1）→ `restart: unless-stopped` 陷入重启循环。因 `/healthz` 是存活探针（仅进程在即可返回 200），进程从未绑定 8086，所以探测永远失败（与 Kafka/DB/CA 等依赖无关）。

该异常仅用于 `_create_vehicle_topics_sync` 的“已存在”兑底捕获（第 253 行，与 `TopicAlreadyExistsError` 同义），而 `create_topics` 对已存在 Topic 实际抛的就是 `TopicAlreadyExistsError`，故该类名本就非必需。

### 修复内容

`services/vehicle-service/app/services/kafka_admin.py`：
- 将 `AleadyHasPartitionException` 从顶层 `from kafka.errors import (...)` 中移出，改为**可选导入 + 回退**：`try: from kafka.errors import AleadyHasPartitionException / except ImportError: AleadyHasPartitionException = TopicAlreadyExistsError`；保留模块级名字，**不改第 253 行 `except (TopicAlreadyExistsError, AleadyHasPartitionException)` 与既有测试**（`test_kafka_admin.py` 引用名仍可解）。
- `KafkaError`/`TopicAlreadyExistsError` 作为稳定名保留在顶层导入。

向后兼容：不改任何创建/回滚/SCRAM 业务逻辑与对外接口；仅在 kafka-python-ng 无此旧名时回退到 `TopicAlreadyExistsError`（同义）；旧 kafka-python 环境行为不变。单元素重复元组 `(X, X)` 在 Python 中合法。

服务器操作（需先同步代码并重建镜像，`--step` 不重建不会生效）：
```bash
# ① 同步修改（本地 → 服务器）
rsync -av services/vehicle-service/app/services/kafka_admin.py dongyan@192.168.31.35:/opt/HunterCore/services/vehicle-service/app/services/
# ② 重建 vehicle-service 镜像并重启该容器
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
# ③ 从 Step 11 续跑（幂等，其余服务已在运行）
sudo bash scripts/install.sh --step 11 -y
```

需同步到服务器长期生效：`services/vehicle-service/app/services/kafka_admin.py`。

风险与待人工确认：① 本修复依赖“kafka-python-ng 对已存在 Topic 抛 `TopicAlreadyExistsError`”这一前提，首次 provisioning 真实建 Topic 时需实机验证“重跑不报 TopicExists”幂等行为（回滚/接管语义）；② 若后续升级 kafka-python-ng 重新导出正确拼写 `AlreadyHasPartitionException`，可改为按新名导入（当前不动）；③ 本次仅修 import 崩溃；vehicle-service 真正就绪（/readyz）仍CA 私钥可读与 Kafka AdminClient 建连，属业务依赖，与本次无关。

验证：GetProblems 无错；本地 `python -c "import app.services.kafka_admin"` 在 kafka-python-ng 环下不再报 ImportError（回退生效）；`ruff check services/vehicle-service` 通过。实机已验证（2026-10-08）：重建镜像后 `docker compose ps` 显示 hunter-vehicle-service `Up (healthy)`，Step 11 `service_health_ok 8086` 耗时 0s 通过，Step 12 全栈健康检查 vehicle-service HTTP /healthz [PASS]，13 步全部完成。


## V1.18.3（修复：vehicle-service 一键开通写 DB 台账报 `UndefinedColumnError: column "provision_status" ... does not exist`）

Date：2026-10-08
变更类型：修复

接 V1.18.2 后 vehicle-service 启动正常，但前端发起一键开通（provisioning）到步骤 1（db INSERT）即失败：
```
DB 台账写入失败: (sqlalchemy.dialects.postgresql.asyncpg.ProgrammingError)
<class 'asyncpg.exceptions.UndefinedColumnError'>:
column "provision_status" of relation "vehicles" does not exist
[SQL: INSERT INTO vehicle_svc.vehicles (..., provision_status, description) VALUES (...)]
```

### 根因

`schema.sql` / `contracts/database/ddl/01_core.sql` 对 `vehicle_svc.vehicles` 采用 `CREATE TABLE IF NOT EXISTS`。目标库中 `vehicles` 表在 **V1.18.0 之前的部署就已存**（Step 8 日志可见 `NOTICE: relation "vehicles" already exists, skipping`），表存在 → 整段 CREATE 被跳过 → V1.18.0 新增的 `provision_status JSONB` 列**从未被补上**（全新库会包含该列，故仅升级环触发）。ORM 与 INSERT 都引用该列 → `UndefinedColumnError`。本质是“新增列缺少幂等升级补列语句”，非 vehicle-service 代码缺陷。

### 修复内容

- `infra/deploy/sql/schema.sql`：`vehicle_svc.vehicles` 索引块后新增幂等补列：`ALTER TABLE vehicle_svc.vehicles ADD COLUMN IF NOT EXISTS provision_status JSONB;`
- `contracts/database/ddl/01_core.sql`：同步新增同一 ALTER（契约与部署执行体保持一致；CREATE 内已声明该列，ALTER 仅覆盖升级路径）。

向后兼容：`ADD COLUMN IF NOT EXISTS`（Postgres 9.6+）幂等安全，新库/旧库重复执行均无错；可为 NULL，与“历史内置台账可为 NULL”契约一致；不改任何服务代码/ORM/接口。同一机制也修复了早期旧表可能缺 `fence_json`/`device_cert_sn` 等列的同类隐患（本次仅按需补 `provision_status`，其余列如实际缺失可照此模式补充）。

服务器操作（两种方式任选其一，均需先同步 SQL）：
```bash
# 方式 A（推荐）：同步 schema.sql 后重跑幂等初始化
rsync -av infra/deploy/sql/schema.sql dongyan@192.168.31.35:/opt/HunterCore/infra/deploy/sql/
cd /opt/HunterCore && sudo bash scripts/init-db.sh    # 或 sudo bash scripts/install.sh --step 8 -y
# 方式 B（立即解锁，不依赖同步）：直接在库上补列
sudo docker exec -i hunter-postgres psql -U hunter -d hunter_core -c \
  'ALTER TABLE vehicle_svc.vehicles ADD COLUMN IF NOT EXISTS provision_status JSONB;'
```
补列后重试一键开通即可（无需重启 vehicle-service）。

需同步到服务器长期生效：`infra/deploy/sql/schema.sql`、`contracts/database/ddl/01_core.sql`。

风险与待人工确认：① 本项目的“CREATE TABLE IF NOT EXISTS + 无集中迁移工具”模式对**跨版本升级**天然存在“新表/新列/新索引不自动补”的隐患，长期建议引入 Alembic 版本化迁移（common/python 已有 alembic.ini）以根除这类问题；② 若目标库 `vehicles` 表极旧可能还缺 `fence_json`/`device_cert_sn`，本次未一并补 ALTER（待确认实际表结构后按需补）；③ `provision_status` 写入需列存在才生效，首次开通前建议先 `\d vehicle_svc.vehicles` 确认列已就位。

验证：GetProblems 无错；Postgres 语法合法（`ADD COLUMN IF NOT EXISTS`）。实机待确认：补列后重试一键开通 → 步骤 1 db 写入成功 → provision_status 逐步回写 scram/topics/cert → 台账 `state=ready`。


## V1.18.4（修复：对已存在的 vehicle_id 发起一键开通时返回 5001“DB 台账写入失败”，应按契约返回 409/3002“vehicle_id 已存在”）

Date：2026-10-08
变更类型：修复

接 V1.18.3 补列后，对预置验证车 HUNTER-001（已在台账）发起一键开通（即使前端“接管已有资源”已勾选），步骤 1 db INSERT 撞主键，返回的是模糊的 5001：
```
DB 台账写入失败: (sqlalchemy.dialects.postgresql.asyncpg.IntegrityError)
<class 'asyncpg.exceptions.UniqueViolationError'>: duplicate key value violates unique constraint "vehicles_pkey"
DETAIL: Key (vehicle_id)=(HUNTER-001) already exists.
```

### 根因

`provisioner.provision` 步骤 1 的异常处理与契约不符：
1. `insert_ledger` 对重复 `vehicle_id` 抛的是 SQLAlchemy `IntegrityError`（非 `HunterBaseException`），但步骤 1 的“3002 分支”只 `except HunterBaseException`，`IntegrityError` 落到兜底 `except Exception` 被包成 `ServiceUnavailableError`（5001）。注释声称“unique 冲突映射 3002”从未生效。
2. 契约明确（vehicle-service.yaml）：`409 → 3002 vehicle_id 已存在`；`takeover_existing` 仅作用于 **SCRAM/Topic/证书等外部资源**，“每步入库前检查 provision_status…takeover_existing=true 允许接管外部资源”，**不接管已存在的台账行**。因此重复 vehicle_id 应干净地返回 3002，而非 5001。

### 修复内容

`services/vehicle-service/app/services/provisioner.py`：
- 步骤 1 新增 `except IntegrityError` 分支（置于 `except Exception` 之前）：映射为 `ResourceAlreadyExistsError("vehicle_id 已存在", details={vehicle_id})`（→ 409/3002）；**不调 `_persist_status`**（既有行非本次所建，禁止覆写其 provision_status）；无外部资源无需回滚；补 `provision_vehicle_exists` 告警日志。
- 顶部新增 `from sqlalchemy.exc import IntegrityError`；`except HunterBaseException` 注释收敛为“其它业务异常”。

向后兼容：不改 4 步编排/回滚/SCRAM/Topic/证书逻辑与对外契约；仅修正重复 vehicle_id 的错误码（5001→3002，与契约既有 409 定义一致）；`error_handlers.HTTP_STATUS_BY_CODE[3002]=409` 已就位，无需改。`takeover_existing` 语义不变（仍仅接管外部资源）。

服务器操作（代码变更，需重建镜像）：
```bash
rsync -av services/vehicle-service/app/services/provisioner.py \
  dongyan@192.168.31.35:/opt/HunterCore/services/vehicle-service/app/services/
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
```

需同步到服务器长期生效：`services/vehicle-service/app/services/provisioner.py`。

风险与待人工确认：① 本修复不改变“不能对已存在 vehicle_id 开通”这一契约约束——若需为预置的 HUNTER-001 真实接入，应先在前端“下线”删除内置台账行再重新开通，或直接用一个新 vehicle_id（如 HUNTER-002）；若产品希望“接管未开通的内置台账行”，属契约变更（需扩展 takeover_existing 到 db 步），本次未做，待确认；② 仅拦截主键/唯一冲突；若 IntegrityError 由其他约束（如 CHECK）触发也会被归为 3002，概率极低（vehicles 表仅主键 + 部分唯一索引）。

验证：GetProblems 无错；`ruff check services/vehicle-service` 通过。实机待确认：对已存在的 HUNTER-001 发起开通 → 返回 HTTP 409 body `code=3002 message=vehicle_id 已存在`（不再 5001）；对新 vehicle_id 开通仍正常走完 4 步。


## V1.18.5（修复：SCRAM 口令创建/重发始终报“kafka-python-ng 版本不支持 KIP-95”——kafka-python-ng 无任一版本提供该能力，改用 confluent-kafka）

Date：2026-10-08
变更类型：修复（依赖层）

接 V1.18.3/V1.18.4 后，对未存在的 vehicle_id（HUNTER-002）开通时步骤 2 SCRAM 失败，页面顶部报：
```
kafka-python-ng 版本不支持 KIP-95（AlterUserScramCredentials）；请升级至 kafka-python-ng>=2.2
```
“重发 SCRAM 口令”（rotate-scram）调同一函数，同样失败。

### 根因（实机探测确认，非配置问题）

在 vehicle-service 容器内实测：
- 已装 kafka-python-ng **2.2.3**；`KafkaAdminClient.alter_user_scram_credentials` → **不存在**；
- `kafka.protocol.scram`、`kafka.scram_types` 模块 → **不存在**；`kafka.admin`/`kafka.protocol.admin` 无任何 Scram/Upsert/Deletion 类；
- kafka-python-ng 全部已发布版本（2.0.3~2.2.3）**无一含 KIP-95 SCRAM 管理**，描述也无 SCRAM。

因此：`kafka_admin.py` 顶部从 `kafka.protocol.scram` 导入 `AlterUserScramCredentialsRequest`/`ScramCredentialUpdate`/`ScramMechanism` 必然 `ImportError` → 三符号置 None → SCRAM 方法直接抛“版本不支持”。即使导入路径改对，旧实现用 `self._admin.client.send_request(手搋协议对象)` 在 kafka-python 严格 schema 框架下也无法序列化。单测 `test_kafka_admin.py` 把整套 API 用 `MagicMock` 全 mock，所以“测试绿”从未覆盖真机接口（已记入教训）。

### 修复内容（方案 A：SCRAM 改走 confluent-kafka；Topic 仍用 kafka-python-ng）

confluent-kafka 是项目**既有依赖**（根 README 技术栈、`common/python/pyproject.toml` 已声 `confluent-kafka>=2.3,<3`、ota/data-collector 等均已用），本变更非引入新三方库，而是让 vehicle-service 对齐项目现有 Kafka 客户端。

- `services/vehicle-service/app/services/kafka_admin.py`：
  - 删除错误的 `kafka.protocol.scram`/`kafka.scram_types` 可选导入与 `name_principal`/`delete_principal`/`SCRAM_MECHANISM_ID` 辅助（及 `__all__` 中的 `SCRAM_MECHANISM_ID`）；
  - 新增 `from confluent_kafka import KafkaException` 与 `from confluent_kafka.admin import AdminClient as ConfluentAdminClient, ScramCredentialInfo, ScramMechanism, UserScramCredentialDeletion, UserScramCredentialUpsertion`；
  - `KafkaAdminOps.__init__` 新增 `scram_admin: ConfluentAdminClient | None = None`（存为 `self._scram`）；
  - `_upsert_scram_user_sync`/`_delete_scram_user_sync` 重写为 `self._scram.alter_user_scram_credentials([...])` + `future.result()` 逐个阻塞；`self._scram is None` → 5001；捕获 `KafkaException`（删除保留 not-found 幂等启发）；
  - 新增 `_build_scram_admin()`（bootstrap/security/SASL 配置，与 Topic 控制面用同一 hunter-client 凭据）；`build_admin_client()` 额外构造 confluent admin 并传入（构造失败不阻断 Topic 能力）。
- `services/vehicle-service/app/tests/test_kafka_admin.py`：SCRAM 3 个用例改为注入 `_FakeScramAdmin`（mock `alter_user_scram_credentials`），新增未就绪/错误两例；删除顶部门面的死导入 `import yaml`。
- `services/vehicle-service/pyproject.toml`：显式声明 `confluent-kafka>=2.3,<3`；修正 `kafka-python-ng` 注释（仅用于 Topic）。

向后兼容：对外契约（错误码/HTTP 映射/provisioning 4 步语义）不变；`upsert_scram_user`/`delete_scram_user` 对外签名不变；Topic 侧 kafka-python-ng 完全不动。`KafkaAdminOps` 第二参默认 None，旧构造方式（单参）仍可用（仅 Topic）。镜像无需改 Dockerfile（confluent-kafka 随 `pip install ./common/python ./service` 已装，manylinux wheel 走清华源）。

服务器操作（代码变更，需重建镜像）：
```bash
rsync -av --relative services/vehicle-service/app/services/kafka_admin.py \
  services/vehicle-service/app/tests/test_kafka_admin.py \
  services/vehicle-service/pyproject.toml \
  dongyan@192.168.31.35:/opt/HunterCore/
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
```

需同步到服务器长期生效：`services/vehicle-service/app/services/kafka_admin.py`、`services/vehicle-service/app/tests/test_kafka_admin.py`、`services/vehicle-service/pyproject.toml`。

风险与待人工确认：① **broker 侧 ACL**：执行 `AlterUserScramCredentials` 需连接账号（hunter-client）拥有 Cluster 资源的 `ALTER`（或目标 User 的 ALTER）权限；若未授予会报 `CLUSTER_AUTHORIZATION_FAILED`（与本库选择无关，旧方案同样需）。需实机确认 `.env` 中 hunter-client 已授权；② 控制面 `sasl.mechanism` 取 `settings.kafka_sasl_mechanism`（应为 SCRAM-SHA-512），与 Topic 侧一致；③ 删除 not-found 幂等仍依赖异常文本启发（confluent 具体错码未逐一枚举），回滚/下线路径下可接受；④ 新增镜像体积略增（librdkafka 已随 hunter_common 存在，无额外增量）。

验证：GetProblems 无错；`ruff check --select E4,E7,E9,F`（CI 默认规则）全部通过。实机已验证（2026-10-09）：对 HUNTER-002 开通已跨过 scram/topics 步（不再报“版本不支持”），推进至证书步（暴露 V1.18.6）。


## V1.18.6（修复：“重发证书”/开通第 4 步报 `openssl x509: Extra (unknown) options: "ca" ...`）

Date：2026-10-09
变更类型：修复

接 V1.18.5 后，provisioning 推进到第 4 步 cert（及“重发证书”reissue），页面报：
```
openssl x509 失败: x509: Extra (unknown) options: "ca" "/srv/kafka-ca/ca-cert.pem" x509: Use -help for summary.
```

### 根因

`cert_signer.issue_client_cert_sync` 步骤 4 的 `openssl x509` 命令参数错误：
1. **大小写错**：用了小写 `-ca` / `-cakey`，但 `openssl x509` 的 CA 签发选项是大写 `-CA` / `-CAkey`（区分大小写）。OpenSSL 将未知的小写 `-ca` 及其参数当作多余项 → “Extra (unknown) options”。
2. **多余参数**：同时传了 `-CAcreateserial` 与 `-set_serial`。二者语义冲突；且 `-CAcreateserial` 会向只读挂载（:ro）的 CA 目录写 `.srl` 文件必然失败。本模块设计（文件头注释）本就用 `-set_serial` 免写 serial 文件，`-CAcreateserial` 属冗余。

单测 `test_cert_signer.py` 只断言了 `-set_serial` 存在，未断言 `-CA` 大小写、也未断言不含 `-CAcreateserial`，故 mock 环境下未暴露（与 V1.18.5 同类“Mock 掩盖真实 API”问题）；另发现该测试有一行自相矛盾的死断言（`assert "-days" not in x509_cmd` 与下行 `assert "-days" in x509_cmd` 冲突）使其长期必红。

### 修复内容

`services/vehicle-service/app/services/cert_signer.py`：步骤 4 命令 `-ca`→`-CA`、`-cakey`→`-CAkey`、删除 `-CAcreateserial`（保留 `-set_serial`）。

`services/vehicle-service/app/tests/test_cert_signer.py`：新增断言锁定 `-CA`/`-CAkey` 存在、`-ca`/`-cakey` 与 `-CAcreateserial` 不存在、CA 路径紧随 `-CA`/`-CAkey`；删除自相矛盾的死断言。

向后兼容：仅修正 openssl 子进程参数，不改签发流程/产物文件/权限/序列号与 p12 逻辑；对外契约不变。需 CA 私钥/证书仍按现有 :ro 挂载且对 uid 10001 可读（本步不写 CA 目录，删 `-CAcreateserial` 后不再尝试写 .srl）。

服务器操作（代码变更，需重建镜像）：
```bash
rsync -av --relative services/vehicle-service/app/services/cert_signer.py \
  services/vehicle-service/app/tests/test_cert_signer.py \
  dongyan@192.168.31.35:/opt/HunterCore/
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
```

需同步到服务器长期生效：`services/vehicle-service/app/services/cert_signer.py`、`services/vehicle-service/app/tests/test_cert_signer.py`。

风险与待人工确认：① 签发仍需 CA 私钥 `settings.kafka_ca_key_path`（默认 /srv/kafka-ca/ca-key.pem）与证书 `ca-cert.pem` 存在且对运行用户可读（否则报 `_assert_ca_available` 或 openssl 读密钥失败）；② `-set_serial 0x<hex>` 与 `-extfile`（无 `-extensions` 段，顶层直挂 SAN）在 OpenSSL 3.0.x 行为正确，若后续升级 OpenSSL 主版本需复核；③ 本修复不改变 SAN=DNS:vehicle_id / CN=vehicle_id 证书内容策略。

验证：GetProblems 无错；`pytest app/tests/test_cert_signer.py` 本地 9 passed（含新增断言）；`ruff check --select E4,E7,E9,F` 通过。实机待确认：对新 vehicle_id 开通→步骤 4 cert 成功（不再报 Extra options）；“重发证书”返回新 device_cert_sn；“下载接入包”可取到 p12。


## V1.18.7（修复：车辆“下线”报 `Kafka SCRAM 用户删除失败: KafkaError{code=RESOURCE_NOT_FOUND ... does not exist}`）

Date：2026-10-09
变更类型：修复

对接 V1.18.5 后，对开通失败（SCRAM 从未创建成功）的车辆点“下线”，报：
```
Kafka SCRAM 用户删除失败: KafkaError{code=RESOURCE_NOT_FOUND,val=91,str="Attempt to delete a user credential that does not exist"}
```

### 根因

`kafka_admin._delete_scram_user_sync` 对“用户不存在”的幂等判定仅基于异常文本启发（`not found` / `nosuchuser` / `no such`）。但 confluent-kafka 删除不存在的 SCRAM 主体时，真实返回是 `KafkaError.code()==RESOURCE_NOT_FOUND`（val=91）、文本为“...does not exist”——不含任何上述关键词 → 未命中幂等分支 → 抛 5001。`provisioner.deprovision` 第 2 步 `delete_scram_user` 未包 try/except，异常直接中断整个下线。

与 `deprovision` docstring 承诺矛盾：“下线允许外部资源不存在视为幂等成功（kafka_admin 已内建此语义）”——实际 SCRAM 删除未内建。单测 `test_delete_scram_user_not_found_idempotent` 旧用例用了含“not found”字样的假消息（`SASL_PRINCIPAL_NOT_FOUND: principal not found`），恰好命中旧启发，未暴露真实错误串（与 V1.18.5/6 同类“Mock 掩盖真实 API”）。

### 修复内容

`services/vehicle-service/app/services/kafka_admin.py`：
- 新增模块级 `_is_scram_principal_missing(exc)`：**优先按错误码** `exc.args[0].code() == ConfluentKafkaError.RESOURCE_NOT_FOUND` 判定，文本关键词（新增 `resource_not_found` / `does not exist`）仅作回退兼容；
- `_delete_scram_user_sync` 的 except 分支改调该函数；
- confluent 的 `KafkaError` 以别名 `ConfluentKafkaError` 导入，避免遮蔽第 32 行 kafka-python-ng 的 `KafkaError`（Topic 操作 `except KafkaError` 仍用原库异常）；修正一处注释错字。

`services/vehicle-service/app/tests/test_kafka_admin.py`：删除用例改用真实错误串（“does not exist”无“not found”）；新增 `_is_scram_principal_missing` 三条直接断言（错误码命中 / 文本回退命中 / 其它码不命中）。

向后兼容：仅收紧 SCRAM 删除的幂等判定（RESOURCE_NOT_FOUND → 成功），不改 upsert/Topic 逻辑与对外契约；下线行为回归至文档既定语义（外部资源不存在=幂等）。不影响其它错误码（如权限/超时仍抛 5001）。

服务器操作（代码变更，需重建镜像）：
```bash
rsync -av --relative services/vehicle-service/app/services/kafka_admin.py \
  services/vehicle-service/app/tests/test_kafka_admin.py \
  dongyan@192.168.31.35:/opt/HunterCore/
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
```

需同步到服务器长期生效：`services/vehicle-service/app/services/kafka_admin.py`、`services/vehicle-service/app/tests/test_kafka_admin.py`。

风险与待人工确认：① 下线时若 SCRAM 用户确实存在，删除需 hunter-client 有 Cluster ALTER 权限（同 V1.18.5 ACL 前提），否则报 AUTHORIZATION 类错误（非 RESOURCE_NOT_FOUND，仍会 5001，属真实权限问题）；② 本修复使 SCRAM 删除幂等，但 `deprovision` 后续步骤（Topic/DB 行）失败仍会中断，需分别验证；③ 错误码枚举依赖 confluent `KafkaError.RESOURCE_NOT_FOUND` 存在（已导入验证）。

验证：`ruff check --select E4,E7,E9,F`（CI 默认规则）通过；本地 `.venv` 未装 confluent，test_kafka_admin 需 CI/镜像环境跑。实机已验证（2026-10-09）：对开通失败（无 SCRAM）的车点“下线”→ SCRAM 步幂等通过（不再报 RESOURCE_NOT_FOUND），推进至 Topic 步（暴露 V1.18.8）。


## V1.18.8（修复：“下线”/开通第 3 步报 `topics.yaml 未包含任何车辆 Topic 条目（模式 hunter.{vehicle_id}.）`）

Date：2026-10-09
变更类型：修复

接 V1.18.7 后，下线推进到第 3 步（删 Topic），开通也会到同一步，报：
```
topics.yaml 未包含任何车辆 Topic 条目（模式 hunter.{vehicle_id}.）
```

### 根因

`kafka_admin.load_vehicle_topics` 筛选每车 Topic 的条件错用 `.format(vehicle_id="")`：
```python
if VEHICLE_TOPIC_PATTERN.format(vehicle_id="") not in name:  # 得到 "hunter.."
    continue
```
`VEHICLE_TOPIC_PATTERN` = `"hunter.{vehicle_id}."`，`.format(vehicle_id="")` 将其变为 `"hunter.."`；而 topics.yaml 里 `vehicle_topics[*].name` 是字面模板 `hunter.{vehicle_id}.telemetry`（占位符未替换，无连续两点）→ `"hunter.."` 永远不命中 → 8 条全被 `continue` 跳过 → `entries` 为空→抛错。实测验证：旧写法 0 条、新写法 8 条（正确排除广播）。

该 bug 与 SCRAM 无关，任何走到 Topic 步（开通创建/下删删除）的操作都会触发；之前因开通先卡 SCRAM 而未暴露。单测 `test_load_vehicle_topics_returns_8` 用真实 topics.yaml 断言 8 条，旧代码下会因抛异常而失败（属未跑/CI 未拦截）。

### 修复内容

`services/vehicle-service/app/services/kafka_admin.py`：筛选条件改为直接拿字面模板匹配 `if VEHICLE_TOPIC_PATTERN not in name: continue`（不再 `.format`），并补注释说明不可预 format。

向后兼容：仅修正契约筛选逻辑，不改 Topic 命名/分区/保留期/replication 等任何字段解析与对外契约；修复后正确返回 8 条每车 Topic（与契约一致）。

服务器操作（代码变更，需重建镜像）：
```bash
rsync -av --relative services/vehicle-service/app/services/kafka_admin.py \
  dongyan@192.168.31.35:/opt/HunterCore/
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
```

需同步到服务器长期生效：`services/vehicle-service/app/services/kafka_admin.py`。

风险与待人工确认：① 需确保容器内挂载的 `contracts/kafka/topics.yaml` 与仓库一致（若服务器用了旧版/空契约，仍会 0 条，但那是部署挂载问题而非代码）；② 修复后开通第 3 步会真正创建 8 Topic，需 broker 正常且 hunter-client 有建 Topic 权限；下线第 3 步 `delete_topics` 对不存在的 Topic 应幂等（需回归验证，若报 UNKNOWN_TOPIC_OR_PARTITION 需另处理）。

验证：本地纯 yaml 复现筛选：旧写法 0 条、新写法 8 条且排除 `hunter.broadcast.command`；`ruff check --select E4,E7,E9,F` 通过。实机已验证（2026-10-09）：“下线”→ `vehicle_topics_loaded count=8` + `kafka_topics_deleted count=8`，不再报“未包含任何车辆 Topic 条目”，下线能走完（ledger_removed=True）；开通→Topic 步仍失败（但为 NewTopic 参数问题，见 V1.18.9）。


## V1.18.9（修复：开通第 3 步建 Topic 报 5001（NewTopic 参数名 `topic_config` 应为 `topic_configs`））

Date：2026-10-09
变更类型：修复

V1.18.8 后，开通走到步骤 3（建 Topic）在 8ms 内失败并回滚（日志：`kafka_scram_upsert_ok` 紧接 `kafka_scram_delete_ok` → `5001`，无任何 `kafka_topics_created`/`kafka_topic_*` 日志）。因回滚时 `created_topics` 为空，说明异常发生在建 Topic 循环**之前**的 `NewTopic(...)` 构造阶段。

### 根因

`kafka_admin._create_vehicle_topics_sync` 构造 `kafka.admin.NewTopic` 时用了错误的关键字参数名 `topic_config`（单数）。kafka-python-ng 的真实签名为（源码核实）：
```python
NewTopic(name, num_partitions, replication_factor, replica_assignments=None, topic_configs=None)
```
属性为 `.name/.num_partitions/.replication_factor/.topic_configs`。`name=`/`num_partitions=`/`replication_factor=` 均正确，**只有 `topic_config` 应为 `topic_configs`（复数）** → `TypeError: __init__() got an unexpected keyword argument 'topic_config'`（非 KafkaError，不被内层 except 捕获，直接冒泡到步骤 3 包成 5001）。删除路径（`render_vehicle_topics`→名字→`delete_topics`）不构造 NewTopic，故下线正常而开通失败。单测 `test_create_vehicle_topics_skip_existing` 走真实构造路径，本应因 TypeError 而失败（属未拦截/未跑）。

### 修复内容

`services/vehicle-service/app/services/kafka_admin.py`：`NewTopic(..., topic_config={...})` → `topic_configs={...}`。

`services/vehicle-service/app/tests/test_kafka_admin.py`：断言属性 `t.topic_config[...]` → `t.topic_configs[...]`（4 处）。

向后兼容：仅修正 NewTopic 关键字参数名，不改 Topic 名/分区/保留期/cleanup.policy 的值与对外契约。

服务器操作（代码变更，需重建镜像）：
```bash
rsync -av --relative services/vehicle-service/app/services/kafka_admin.py \
  services/vehicle-service/app/tests/test_kafka_admin.py \
  dongyan@192.168.31.35:/opt/HunterCore/
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build vehicle-service
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d vehicle-service
```

需同步到服务器长期生效：`services/vehicle-service/app/services/kafka_admin.py`、`services/vehicle-service/app/tests/test_kafka_admin.py`。

重要（需先清理残留行）：本次 5001 失败时步骤 1 的 DB 行已按设计保留（`provision_status=failed`），导致后续重开同一 vehicle_id 均撞主键→3002。重建后需先删除残留行（或改用新 vehicle_id）：
```bash
sudo docker exec hunter-postgres bash -lc \
 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "DELETE FROM vehicle_svc.vehicles WHERE vehicle_id='HUNTER-001';"'
```

风险与待人工确认：① kafka-python-ng `create_topics` 返回 futures、部分错误（如 Topic 已存在）是异步的，当前代码同步 `except TopicAlreadyExistsError` 可能捕不到（不阻塞本场景：残留 Topic 已在下线时删除，重开时 Topic 不存在→创建成功）；若后续出现“已存在却未跳过”需改为等待 future 并判错码；② 本次修复不解决“开通失败残留 failed 行阻断同 ID 重开”的设计缺口（takeover_existing 不接管 DB 行），建议后续单独优化（如开通前先探测同 ID 的 failed 行并允许重置）。

验证：`ruff check --select E4,E7,E9,F` 通过；本地 `.venv` 未装 kafka，test_kafka_admin 需 CI/镜像环境跑（修复后 NewTopic 构造不再 TypeError）。实机待确认：先删残留行→重新开通 HUNTER-001 → 步骤 3 Topic 成功创建 8 个（`kafka_topics_created count=8`）→步骤 4 cert（需 V1.18.6已部署）。

## V1.18.10（修复：车端接入 9093 失败 —— Broker 证书 SAN 不含车端实际访问地址，且旧 `--force` 修复路径会连带破坏 CA 信任链）

Date：2026-10-09
变更类型：修复

现场检查结果（车端侧取证，确认不是车端问题）：

| 检查项 | 结果 | 判定 |
|--------|------|------|
| 信任链 | OK（CA 正确） | ✓ |
| 有效期 | 2026-09-19 ~ 2036-09-21 | 未过期 ✓ |
| EKU | serverAuth | 合法 ✓ |
| SAN | `DNS:kafka, DNS:hunter-core-server, IP:127.0.0.1, IP:101.201.150.237` | **不含 `192.168.31.35` ✗** |

### 根因

1. **直接原因（Broker 侧证书，车端无法自行修正）**：车端 `ssl.endpoint.identification.algorithm=https` 要求“它实际连接的目标地址”逐字命中证书 SAN；**证书含 SAN 时 CN 不再作回退**（`CN=kafka` 无效）。旧证书只签了公网 IP，车端改走内网 `192.168.31.35:9093` 后主机名校验必然失败（典型报错 `No subject alternative names matching IP address 192.168.31.35 found`）。内网 IP / 公网 IP / 域名属不同 SAN 条目，互不命中。

2. **流程缺口（为何能静默上线）**：`scripts/install.sh` Step 5 原为“看到 JKS 就跳过”，不校验 SAN；即使不跳过，交付前的 SAN 校验也只 `grep -qF "IP Address:${SERVER_IP}"` + 失败仅 warn。该 grep 模式在 openssl 3.x 下永不命中（3.x 输出 `IP:`/`IPAddress:`），校验事实上长期为空转 → 带旧 SAN 进入 Step 7，车端运行时才暴露。

3. **修复手段本身有坑**：旧脚本只支持单个 `--ip`，唯一重签入口 `--force` **会连 CA 一起重签** —— 等于为了改一个 SAN 字段，令全部已接入车端的 `ca-cert.pem` / 客户端证书失效、需逐车重新分发；运营因此没有“低风险修复手段”，这也是本故障长期未修的实际原因。

### 修复内容

1. **`scripts/gen-kafka-certs.sh`（能力新增 + 全量重构为分段函数）**：
   - 新增 `--san-ip <ip>` / `--san-dns <dns>`（可重复）与 `--broker-only`；`--broker-only` **复用现有 CA 与客户端证书，仅重签 broker 证书 + keystore**，车端信任链不变、无需重发证书；`--broker-only` 与 `--force` 同时给出时按 `--broker-only` 处理并 warn。
   - `.env` 支持 `KAFKA_CERT_SAN_IPS` / `KAFKA_CERT_SAN_DNS`（逗号分隔）以固化多接入地址；逐项做 IPv4/IPv6/域名合法性校验并去重。
   - 幂等路径新增“**SAN 漂移自愈**”：JKS 齐备时不再直接 `return 0`，而是比对现有 `broker-cert.pem` 的 SAN 与本次要求的地址集合，缺则提示并（非 `-y` 时询问后）自动走 `reissue_broker`，避免“每次重签”与“永不重签”两个极端。三条主路径：A `--broker-only`、B 幂等复用（含权限自愈一 + SAN 漂移自愈二）、C 全新签发（broker 证书仅在 SAN 完整时才复用）。
   - `usage`/头注释明确区分“地址变化用 `--broker-only`”与“`--force` 会换 CA、须同步全部车端”。

2. **`scripts/common.sh`（新增 `hc_cert_san_entries <pem>`，全仓单一口径）**：统一归一 openssl 各版本 SAN 输出（1.x `IP Address:x`、部分构建 `IPAddress:x`、3.x `IP:x`）为 `IP:x` / `DNS:x` 每行一条，供 gen-kafka-certs / install / health-check 共用。

3. **`scripts/install.sh` Step 5**：删除“见 JKS 就跳过”，改为始终调用证书脚本（脚本自身幂等）；`ASSUME_YES=1` 时传递 `-y`；交付前用 `hc_cert_san_entries` 硬校验 SAN 含 `IP:${SERVER_IP}`，**缺失即 `return 1`**（不再只 warn）并打印 `--broker-only` / `--san-ip` / `KAFKA_CERT_SAN_IPS` 修复指引。

4. **`scripts/health-check.sh` 新增第 ⑫ 项 `check_kafka_broker_cert`**（已注册进 `run_all_health_checks`，检查项 10→12）：SAN 未覆盖 `SERVER_IP` → **FAIL**（同行给出可直接执行的 `--broker-only` 修复命令）；broker/CA 有效期 → 已过期 FAIL / ≤ 30 天 WARN / 其余 PASS。同时扩展 ⑪ `check_compose_override`：按容器 label `com.docker.compose.project.config_files` 回看实际生效的编排文件，非 `infra/deploy/docker-compose.yml` 即 **FAIL**（见下方“实机新增发现”）。该类问题从此在日常巡检与 `daily-check.sh` 中提前暴露。

5. **`.env.example` 与文档**：`infra/deploy/.env.example` 新增 `KAFKA_CERT_SAN_IPS=` / `KAFKA_CERT_SAN_DNS=` 及注释；`docs/故障排查指南.md`（场景4 新增 SAN 不匹配的服务端修复块、§6.6 Kafka 表、§14.3 异地重建、§14.4 安全事件）、`docs/运维操作手册.md`（每日/每周巡检、§7.4 车端接入配置、§7.6 拆为“地址变更 `--broker-only`”与“到期/CA 轮换 `--force`”两条路径）、`docs/01-部署概述与环境要求.md`（证书目录树 ⚠ 说明、脚本清单与 12 类）、`README.md` 同步。

本次修复过程中还发现并修掉了两个会直接影响正确性的实现陷阱（已随本条落盘）：
- `.env` CSV 解析原用 `tr ',' '\n' | tr -d '[:space:]'`，后者把刚换出来的分隔换行一并删除，末项无换行→`read` 不执行→**静默丢地址**；改为 `IFS=',' read -r -a` + 逐项去空白。
- openssl 3.x 输出 `IPAddress:127.0.0.1` 既非 `IP Address:` 也非 `IP:`，原有 sed 不匹配→所有 IP 条目误判缺失→**每次重签且 verify 直接失败**；改为 `sed -E 's/^IP[[:space:]]*Address:/IP:/I'` 并 `tr -d ' \r'`。

6. **health-check 整轮容错（实机反馈后补）**：原 `run_all_health_checks` 顺序直接调用各检查函数，而脚本开了 `set -euo pipefail`——**任一检查内部出现失败命令（如 `x=$(...)` 赋值型失败、子命令非 0）会直接打断整轮**，后面的项（含本次新增的“车端接入证书 SAN”硬闸）静默不执行，汇总也不输出（实测已复现：旧写法中途退出、无任何输出）。现改为 `for check in …; do if ! "$check"; then record_result WARN …; fi; done`（`if !` 上下文会关闭 errexit，单项内部报错不再杀掉整轮，未完成项记为 WARN），并给 ⑫ 增加 `declare -F hc_cert_san_entries` 存在性检查（`common.sh` 未同步时给出可读 FAIL 而不是静默消失）。

验证补充：临时夹具实测 `LOOP_CONTINUATION_OK`（中间项故意失败，最后一项仍执行）与旧写法无输出退出（确认成因），临时脚本已删。

7. **⑫ 改为“全部接入地址”集合校验（实机反馈后补）**：原 ⑫ 只比对 `IP:${SERVER_IP}` 一项，导致“`.env` 声明了多接入地址但证书只签了主地址”这一类故障**在巡检里隐身**（而走公网/域名的那批车端依旧主机名校验失败）。现要求集合 = `SERVER_IP` + `KAFKA_CERT_SAN_IPS` + `KAFKA_CERT_SAN_DNS`（与 `gen-kafka-certs.sh` 完全同口径，逗号分隔、逐项去空白、IPv6 因 openssl 输出写法差异跳过严格比对），FAIL 文案精确列出**缺哪几条**；集合为空（占位 SERVER_IP 且未声明附加地址）仍记 WARN 而不猜。配套要求：**多接入地址必须写进 `.env`**，否则下次重签只签主地址且巡检检不出（已写入 `docs/运维操作手册.md` 每周巡检 ② 备注与 §7.6 尾注）。

向后兼容：不改变已有证书的格式与文件名，不改 compose 编排与对外契约；新增参数均为可选，不带时行为与旧版一致（仅额外做 SAN 比对）。**复用 CA 的 `--broker-only` 路径下，车端 ca-cert.pem / kafka-client.p12 / SCRAM 账号全部无需变动**。已持有旧证书的环境首次跑 `install.sh --step 5` 会触发一次仅 broker 重签（属预期自愈，需随后 `restart kafka`）。

服务器操作（本次现场故障的推荐做法：CA 不变，车端不换证书）：
```bash
# 1) 固化双接入地址（内网主地址 + 公网次地址），避免下次重签回到单地址
sudo vim /opt/HunterCore/.env
#   SERVER_IP=192.168.31.35
#   KAFKA_CERT_SAN_IPS=101.201.150.237

# 2) 同步脚本（仅脚本，无需重建镜像）
rsync -av --relative scripts/gen-kafka-certs.sh scripts/common.sh scripts/install.sh \
  scripts/health-check.sh infra/deploy/.env.example dongyan@192.168.31.35:/opt/HunterCore/

# 3) 仅重签 broker 证书 + keystore（保留 CA）
cd /opt/HunterCore
sudo bash scripts/gen-kafka-certs.sh --ip 192.168.31.35 --san-ip 101.201.150.237 --broker-only

# 4) 让 Kafka 加载新 keystore；本次改了 SERVER_IP，影响 9093 端口绑定与 advertised.listeners，
#    因此必须重建容器（`restart` 不重读 compose 变量）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate kafka

# 5) 验证：SAN 含两个地址 + 健康检查该项 PASS
openssl x509 -in certs/kafka/broker-cert.pem -noout -ext subjectAltName
sudo bash scripts/health-check.sh | grep 车端接入证书
```
车端无需任何改动（沿用原 `ca-cert.pem` 与 `bootstrap 192.168.31.35:9093`）；若车端同时存在内网/公网两种接入场景，上一步的 `--san-ip` 已是必须而非可选。

实机新增发现（执行本条时踩到，已一并修复文档与检查）：`cd /opt/HunterCore && sudo docker compose up -d --force-recreate kafka` 报
`Image bitnami/kafka:3.6.2 Error … registry-1.docker.io … Client.Timeout`。**这不是网络问题，而是拿错了编排文件**：仓库根 `docker-compose.yml` 是本地开发版
（KRaft 单节点、**PLAINTEXT 无认证**、镜像 `bitnami/kafka:3.6.2`、`container_name` 与生产完全相同），生产编排 `infra/deploy/docker-compose.yml` 用的是 `bitnami/kafka:3.6`。
裸跑 `docker compose` 因项目名相同（均取自目录名）会把生产 broker **换成无证书挂载、无 9093 的开发版 → 车端全断**；本次仅因镜像拉不到而未真正执行到重建那一步。已修正：
- 上述第 4 项给 health-check 增加“编排来源（G-03）”硬校验（非生产编排创建 hunter-kafka 即 FAIL）；
- `docs/运维操作手册.md` §1 新增 ⚠ 块与 `dcompose()` 封装（手册内所有 compose 命令改用封装），§7.6 命令同步；`docs/故障排查指南.md`（§5 修复块与 §6.6 Kafka 表新增本故障行）、`docs/01-部署概述与环境要求.md` 同步；
- 服务器侧执行任何 compose 命令必须带 `-f infra/deploy/docker-compose.yml --project-directory .`（与 release.md 历史条目一致），或直接走 `scripts/install.sh --step N -y`（`common.sh` 的 `HC_COMPOSE_ARGS` 自带正确参数）。

风险与待人工确认：① 重签后不重启/不重建 Kafka 则新 keystore 不生效，表现为“证书已正确但车端仍失败”，验证时应先确认容器启动时间；② 若车端访问的是 NAT/弹性 IP 或域名，`SERVER_IP`/`KAFKA_CERT_SAN_IPS` 必须按**车端实际填的 bootstrap 地址**配置，不能只填网卡地址；③ `advertised.listeners` 与 compose 的 `9093` 端口绑定依赖 `SERVER_IP`，改 `SERVER_IP` 后需 `up -d --force-recreate kafka`（仅 `restart` 不重读 compose 变量）；若车端以公网地址接入而 advertised 返回内网地址，会拿到不可达的元数据地址；④ 本条不处理“SAN 正确但车端 truststore 里 CA 过期/不匹配”的另一类故障；⑤ 证书内网 IP 写进公网可达的证书，会泄露部署拓扑（可接受，如需收敛可改用域名接入 + `--san-dns`）。

验证：`bash -n` 对 `gen-kafka-certs.sh`/`common.sh`/`install.sh`/`health-check.sh` 全部通过；在 Git Bash（openssl 3.x）下用真实 openssl 证书对纯函数与新增检查做隔离自检（临时脚本已删除）：`build_san_entries`/`san_add_csv`/`missing_sans`/`hc_cert_san_entries` 对 `IP Address:` / `IPAddress:` / `IP:` 三种写法均归一为 `IP:x`，去重与顺序符合预期，全命中时 `missing_sans` 返回 0（不反复重签）；**现场故障可精确复现与检出**：旧证书 SAN 仅 `DNS:kafka DNS:localhost IP:127.0.0.1 IP:101.201.150.237` 时，health-check ⑫ 输出 `[FAIL] 车端接入证书 SAN - 不含 192.168.31.35（…）→ … 修复：bash …/gen-kafka-certs.sh --ip 192.168.31.35 --broker-only …`，有效期项 PASS（剩余 3649 天）。实机已确认（2026-10-09）：`--broker-only` 重签后 `openssl x509 -ext subjectAltName` 与**运行时 `openssl s_client -connect 192.168.31.35:9093` 递出的证书**均含 `IP:192.168.31.35` 与 `IP:101.201.150.237`；CA notAfter 仍为 2036-09-21（**证明 CA 未变、车端无需换证书**），broker 证书 notAfter 刷新为 2036-10-06；`health-check.sh` 全轮 41 项跑齐（37 PASS / 4 WARN / 0 FAIL，退出码 1 仅因 WARN），含“编排来源（G-03）PASS”与“车端接入证书 SAN PASS”；`advertised.listeners=…,EXTERNAL://192.168.31.35:9093,…` 与 `ss -ltnp` 显示 9093 仅绑在 192.168.31.35。**车端已闭环（2026-10-09，修复生效）**：车端侧接入自检（`hunter_kafka.diagnose`，参数与证书在 `/etc/hunter/kafka/`）在 `ssl.endpoint.identification.algorithm = https`（严格主机名校验未放宽）下全部通过：Broker TCP 连通 → Broker 握手（SASL_SSL + mTLS，共见 15 个 Topic）→ Topic 清单 8/8 → 端到端投递 `hunter.HUNTER-001.telemetry` 已收到 broker ack，结论“接入链路可用（警告 0 项）、exit=0”——即信任链、SAN 主机名校验、SCRAM 凭证、客户端证书四者同时成立（旧证书下“Broker 握手”那步就会报 `No subject alternative names matching IP address 192.168.31.35 found`）。

**仍需确认**：消费组 `data-collector-telemetry` 出现 offset（上次巡检仍报“尚无 offset 提交”，属车辆未上报的 WARN，非 TLS 问题）；以及 `.env` 补上 `KAFKA_CERT_SAN_IPS=101.201.150.237`（本次用命令行 `--san-ip` 传入，未固化则有下次重签回到单地址与巡检检不出的风险）。另：因 9093 只绑内网地址，以公网 IP 接入的车端仍需云侧 EIP/NAT 映射或改走域名接入（见“风险”②）。

## V1.18.11（修复：接入链路全绿但“车辆管理”页恒显离线 —— 台账列 `status/last_online_time` 无任何写入方）

Date：2026-10-09
变更类型：修复（功能缺口补齐）

现场：运营侧 `health-check.sh` “车端接入证书”三项 PASS，车端 `hunter_kafka.diagnose` 全绿（exit=0，含 SASL_SSL+mTLS 握手、Topic 8/8、端到端投递已收到 broker ack），但“车辆管理”页仍显示 `状态=离线`、`最近在线=-`。

### 根因（不是证书/网络残留问题）

1. **平台有两套互不相通的在线读模型**：Redis `vehicle:status:{id}` / `vehicle:online:set`（data-collector 是唯一写方，供实时看板/OTA 门禁/远程接管）与 DB `vehicle_svc.vehicles.status` / `last_online_time`（“车辆管理”页的唯一数据源）。vehicle-service **不读 Redis**（其 `app/main.py` 开头即写明“Redis/MinIO 本服务不使用”），列表页直接取 ORM 行字段。
2. **台账两列此前没有任何写入方**：`VehicleRepository.update_status()` 在 common 里已实现且有单测，但**全仓无生产调用点**；`contracts/database/ddl/01_core.sql` 无 trigger，`consumer-groups.yaml` 对 health 组的 notes 也只写“写 Redis…不在 DB 落明细”。因此即使遥测/心跳链路完全正常，这一页也必然恒显“离线 / -”（属设计缺口，非运行时故障）。

### 修复内容

1. **`common/python/hunter_common/database/repositories/core.py`**：`VehicleRepository` 新增 `touch_last_online_time(vehicle_id, *, seen_at) -> bool`：单条 `UPDATE` + rowcount 判定，WHERE 附加 `last_online_time IS NULL OR last_online_time < :seen_at` → **重放的旧消息不会把时间往回拨也不产生有效写入**；只刷时间、**不改写 status**（业务状态由 health 与状态机决定）。
2. **`services/data-collector/app/services/vehicle_ledger.py`（新建）** `VehicleLedgerWriter`：三个写入点共用、内部节流。health=状态权威（跃迁立即写、稳态按 `VEHICLE_LEDGER_WRITE_INTERVAL_SECONDS` 合并）；telemetry=在线证据（只 `touch_last_online_time`）；Sweeper 判离线=立即写但去重（否则每轮对陈旧车持续无效 UPDATE）。三条硬约束：① DB 异常只记 WARN **绝不上抛**（消费失败会重试/进 DLQ，把“页面数字旧一点”放大成管道中断）；② **失败也计入节流窗口**（否则 DB 故障期间每条消息都打一次库）；③ `vehicle_id` 取自消息体（外部可控）→ 跟踪表按 FIFO 淘汰至 `MAX_TRACKED_VEHICLES=10000`，内存有界。未知状态取值只记 WARN、不猜测映射。
3. **`services/data-collector/app/repositories/vehicle_ledger.py`（新建，关键适配层）**：`BaseRepository` 只接受 `AsyncSession` 且只 `flush` 不 `commit`，而本服务的会话一律由 `DatabaseSessionManager.session()` 按操作临时借出；直接 `VehicleRepository(app.state.db)` 会把会话管理器当会话用 → **每次回写抛 `AttributeError`、又被异常隔离吞成 WARN**，正好把本条要修的问题伪装成“数据本来就旧”。适配层固定“一次回写 = 一个短事务”。`VehicleLedgerWriter._run` 的 WARN 从此带 `error_type`/`error_message`（日志是回写唯一的可观测面）。
4. **挂钩与装配**：`app/services/vehicle_status.py` 的 `update_from_health` / `update_from_telemetry` / `mark_offline` 三处调用回写器（`ledger=None` 时行为与旧版完全一致）；`app/main.py` lifespan 装配 `VehicleLedgerWriter(VehicleLedgerRepositoryAdapter(app.state.db), settings)`；`app/config.py` 新增 `vehicle_ledger_write_enabled=True` / `vehicle_ledger_write_interval_seconds=30`。
5. **契约与部署接线**：`contracts/database/orm-mapping.md` ① Repository 表登记 `touch_last_online_time`（否则双向强校验失败）；② “服务 → Repository 白名单”为 `data-collector` 授权 `VehicleRepository` 并**显式限定只能写这两列**（其余列/主数据仍归 vehicle-service）；③ 补登 `vehicle-service` 行（它不引用共享 Repository，消除长期存在的“未登记白名单”假失败）。`contracts/kafka/consumer-groups.yaml` 的 health/telemetry `notes` 补上回写职责（→ 已重生 `infra/k8s/base/03-configmap-contracts.yaml`，否则 `generate_contracts_configmap.py --check` 拦截）；`infra/k8s/services/data-collector.yaml` ConfigMap 与 `infra/deploy/docker-compose.yml` 新增两个环境变量（默认开，不依赖部署方补配置）。
6. **文档**：`README.md`（消费组表 + “台账回写”条）、`services/data-collector/README.md`（组件表 + 关键配置）、`docs/运维操作手册.md` §7.4.3（那两列就是页面数据源 + ≤30s 判据 + 三个区分点）、`docs/故障排查指南.md` 场景4 新增一行“接入全通但页面离线”。
7. **附带修复（本次跟出来的 L2 闸假阳性）**：`scripts/verify_data_layer.py` 的 DDL 列解析先按顶层逗号切分再剔注释，而 `provision_status` 上方的 JSON 结构示例注释里含逗号 → 从注释中间切断，残段（`"ts":1700000000` 等）被当作列名，报“ORM 缺少列/多出列 provision_status”。现改为**先剔行注释再切分**（`parse_ddl`）。本条之前 `verify_data_layer.py` 长期为红（4 项失败），修复后退出码 0。

向后兼容：未注入 ledger 时（旧调用方与现有单测）`VehicleStatusWriter` 行为逐字不变；默认开关开、阈值 30s，无需修改 `.env`；不改对外 API/DB schema（无新表新列，两列早已存在于 DDL）；不改消息契约（`consumer-groups.notes` 为描述性变更，契约测试仅做子串断言）。旧版本环境升级后无需数据回填：车端下一次上报即刷新。

风险：① **写放大**（已缓解：health/telemetry 各 ≤ 1 次/车/30s；`INTERVAL=0` 仅供本地调试，生产勿置 0）；② **DB 故障静默**：回写失败不影响采集，只留 WARN，所以上线后需按 §7.4.3 主动核对一次（已把“只看 Redis 正常”当作接入健康的误判路径写进排障表）；③ 台账无该车行（手工建用户/Topic 未走“新增车辆”）时回写找不到主键，页面仍看不到该车——属预期（列表数据源就是台账）；④ 直写 `vehicle_svc` 跨过了 vehicle-service 的 API，靠白名单“仅限两列”约束，后续若引入车辆状态机（如 `upgrading` 由 OTA 写）需回到单一写入方否则互相覆盖；⑤ 时间基准点是 collector 接收时刻（`time.time()`）而非车端 `timestamp`，时钟漂移下“最近在线”可能有秒级偏差；⑥ **状态列仍只以 health 为权威**：若车端只发 telemetry 不发 `hunter.{vehicle_id}.health`（契约 1Hz），升级后会表现为“最近在线有值、状态仍为离线”——属契约语义而非回写缺陷，按运维手册 §7.4.3 区分点④ 核对 `data-collector-health` 消费组。

验证：`python scripts/run_unit_tests.py` 全通（9 个目标，data-collector 143 项）；新增 `services/data-collector/app/tests/test_vehicle_ledger.py` 17 项（跃迁立即写/稳态合并/超窗重写/遥测只 touch/离线去重/DB 异常被吞且计入窗口/开关短路/interval=0 不节流/未知值不写/空 id 拒绝/跟踪表有界/适配层会话粒度）+ `test_ingest_pipeline.py` 新增 4 项挂钩用例 + `common/python/tests/test_repositories.py` 新增 2 项（只写一列、时间单调不回退）+ `test_data_collector_contract.py` 新增 1 项（ConfigMap 与契约 notes 双向接线）；`ruff check app` 与 `verify_data_layer.py` / `verify_infra.py` / `verify_test_layer.py` / `generate_contracts_configmap.py --check` 均退出码 0。**实机待确认**：升级到本版 data-collector 后，车端持续上报 ≤30s 内“车辆管理”页转在线（`docker exec hunter-postgres psql … SELECT status,last_online_time FROM vehicle_svc.vehicles WHERE vehicle_id='HUNTER-001'` 与页面一致）。

另需处理（本次发现但未修，不属本条范围）：`services/vehicle-service/app/tests/test_vehicle_contract.py` 的 `ROOT = parents[3]` 算错一层（指向 `services/contracts/...`，全部用例 setup 失败），且本地 `.venv` 未装 `kafka-python-ng` 导致 `test_kafka_admin.py`/`test_provisioner.py` 采集期 ImportError——该服务因此未进入 `run_unit_tests.py` 目标列表，其实单测处于长期未运行状态。

## V1.18.12（修复：车端全绿但平台仍从未消费——契约通配订阅未转 librdkafka 正则）

Date：2026-10-09
变更类型：修复（致命功能缺陷）

现场：V1.18.10 后车端接入闭环（`hunter_kafka.diagnose` exit=0、端到端投递收到 broker ack）、V1.18.11 台账回写代码已入镜像（容器内导入自检 `LEDGER_CODE_OK`），但“车辆管理”页仍离线。逐项取证均为空：`SELECT count(*) FROM data_collector.vehicle_telemetry WHERE vehicle_id='HUNTER-001'` → **0 行**；`redis-cli KEYS 'vehicle*'` → **空**；`health-check.sh` 报 `消费组 data-collector-telemetry 尚无 offset 提交`（而 Topic 数 15、Redis PONG 均 PASS）；`docker logs hunter-collector` 三条 ERROR：`KafkaError{code=UNKNOWN_TOPIC_OR_PART, str="Subscribed topic not available: hunter.*.health: Broker: Unknown topic or partition"}`（telemetry/health/event 各一条）+ 三条 `_NO_OFFSET: Commit failed: Local: No offset stored`。

### 根因（比 V1.18.11 更靠前一层）

1. **librdkafka 的订阅语义**：`Consumer.subscribe()` **只把以 `^` 开头的订阅项当正则**，其余一律视为**字面 Topic 名**。`KafkaConsumerManager.run()` 直接 `subscribe(self._topics)`，而 `services/data-collector/app/consumers/{telemetry,health,events}.py` 传的是契约写法 `hunter.*.telemetry` —— 实际订阅了一个“名字里真带星号”的 Topic。
2. **broker 关闭自动建 Topic**（契约：`auto.create.topics.enable=false`，`infra/deploy/docker-compose.yml`、`infra/k8s/statefulsets/kafka.yaml`、infra README 三处一致）→ 该字面名永不出现，订阅项永远 `UNKNOWN_TOPIC_OR_PART`，**平台自部署以来从未消费过任何一条车端消息**。
3. **为何长期未被发现**：消费者进程活着、`/healthz` 200、容器 healthy、Topic 数正常，而消费组因未注册根本不进 broker 的组列表——**当时 41 项巡检全部“正常”**。只有服务运行期 error 日志里有痕迹（属探针看不见的故障类）。
4. 与 V1.18.11 的关系：**两层叠加**。本条（从未消费）在上、V1.18.11（台账列无写入方）在下；消费不通则回写永远不会被触发，所以上一版升级后页面依旧离线（看到本条不代表 V1.18.11 无效）。

### 修复内容

1. **`common/python/hunter_common/kafka/contracts.py`**：新增 `subscription_term(topic)` 将契约订阅写法转 librdkafka 订阅项：`*` / `{vehicle_id}` → `[^.]+`（**单层**通配，不跨越 `.` 分隔的 Topic 类型段，避免 `hunter.*.telemetry` 误命中 `hunter.a.b.telemetry` 之类分层名）并加 `^…$` 锚点；具体名原样返回。**关键守卫**：入参已以 `^` 开头时**逐字放行** —— `ota-service` 与 `remote-control` 的配置早已是正确正则（`^hunter\..*\.ota_status$` / `^hunter\..*\.command_result$`），若不放行会把 `.*` 里的 `*` 再转一道，生成**永不匹配**的正则，把两个当前正常工作的服务打挂。
2. **`common/python/hunter_common/kafka/consumer.py`**：`__init__` 预算 `self._subscription_terms` 并暴露 `subscription_terms` 属性；`run()` 改为 `subscribe(self._subscription_terms)`；`kafka_consumer_started` 日志新增 `subscription=[…]`（部署后一眼可判镜像新旧）；`_build_conf` 新增两个键：`allow.auto.create.topics=False`（写错的订阅名不在 broker 上落一个同名空 Topic，排障时“Topic 存在”会误导）与 `topic.metadata.refresh.interval.ms=30_000`（正则订阅下新开通车辆的 Topic 靠全量元数据刷新进入订阅集合，默认 300s 会让“已开通但无数据”持续 5 分钟被当成接入故障）。
3. **单测 10 项**：`common/python/tests/test_kafka_message_roundtrip.py` 新增 `SUBSCRIPTION_TERM_CASES`（7 例，含“历史 `^…$` 写法不得二次转义”）与 `test_subscription_regex_covers_contract_topics_without_overmatch`（`fullmatch hunter.HUNTER-001.telemetry` 为真，对 `.health`/`.telemetry.dlq`/`hunter.A.B.telemetry` 为假，并遍历契约全部 vehicle Topic 验各自正则命中实例名）；`test_kafka_consumer_unit.py` 新增订阅项暴露与两个配置键用例，并将原断言 `subscribed == ["hunter.*.telemetry"]`（**该断言把缺陷当预期钉住了**）改为 `r"^hunter\.[^.]+\.telemetry$"`。
4. **契约登记**：`contracts/kafka/README.md` “实现侧语义登记表”新增两行（订阅写法→正则与转换规则、历史例外；消费者不建 Topic + 30s 元数据刷新），明确**服务不得自行拼正则**。
5. **`scripts/health-check.sh` 第 ⑬ 项“服务运行期错误日志”**：扫 7 个服务容器近 `ERROR_LOG_WINDOW`（默认 15m）的 `[error]/[critical]/Traceback`；命中 `Subscribed topic not available|UNKNOWN_TOPIC_OR_PART` 直接 **FAIL** 并给出“该服务从未消费到任何消息（看板与车辆台账会恒显离线）”与 V1.18.12 升级指引，其余 error 记 WARN，无错则 PASS（带容器数与窗口）。头部注释/`usage()`/检查项清单与 `run_all_health_checks` 同步为 **13 类**。
6. **文档**：`README.md`（脚本表 13 类 + Kafka 要点新增“通配订阅转正则”块）、`common/python/README.md`（示例注释不再把 `hunter.*.telemetry` 直称“正则订阅”，而是说明转换机制）、`docs/故障排查指南.md` 场景4（新增排查第 10 步与两行原因表：订阅未命中、只发遥测不发 health）、`docs/运维操作手册.md`（每日巡检表新增“服务运行期错误日志”行、§2.2 与 §7.4.3 补“平台从未消费”块与区分点⓪）、`docs/01-部署概述与环境要求.md`（13 类）、`scripts/daily-check.sh`（头部与 usage 的 10 类→13 类）。

向后兼容：**不改服务侧任何订阅配置**（`subscribe_pattern` 仍写契约通配写法，转换集中在共享库）；`^` 开头的历史写法经守卫逐字生效，`ota-service`/`remote-control`/`scene-service`（订具体名 `analytics_result`）行为不变；不改对外 API/DB/消息契约；第 ⑬ 项仅新增巡检信号，退出码语义（0/1/2）不变。

风险：① **已上线环境必须重建镜像**：修复在共享库，仅改代码不 `--build` 则无效（实机已踩到：`--build` 全 CACHED + 容器 `Running 0.0s` 说明未变镜像）；所有含通配订阅的服务均需重建（至少 data-collector）；② 新开通车辆的 Topic 最长 **30s** 后才进入订阅集合（元数据刷新周期），首次接入“短暂无数据”属预期；③ `[^.]+` 为单层匹配，若未来引入多级命名（如 `hunter.region1.v001.telemetry`）将不命中，命名规则变更需同步本节与单测；④ 第 ⑬ 项只扫 15m 窗口且依赖容器未重启丢日志，长期潜伏故障仍需人工 `--since 12h` 回看；error 日志多的服务会持续记 WARN（可 `ERROR_LOG_WINDOW=1m` 收敛）；⑤ `allow.auto.create.topics=false` 使写错的 Topic 名**不再被静默创建**，测试环境依赖自动建 Topic 的脚本（如 console-producer 探路）需先显式创建。

验证：`pytest common/python/tests -q` **315 passed**（+10）；`python scripts/run_unit_tests.py` 全通（9 个目标：data-collector 143、ota 128、scene 108、api-gateway 71、remote-control 97、analytics 60、flink 15 等）；本条改动的 4 个 `.py` 文件 `ruff check` 全通过；`bash -n scripts/health-check.sh` / `scripts/daily-check.sh` 通过；第 ⑬ 项用临时夹具实测三态（伪造 `Subscribed topic not available` → FAIL 且退出码 2；普通 error → WARN/退出码 1；干净 → PASS/退出码 0），夹具已删除；`verify_data_layer.py` / `verify_infra.py` / `verify_test_layer.py` / `generate_contracts_configmap.py --check` / `render_k8s.py --check` 均退出码 0。

**实机待确认**（本条尚需服务器执行）：同步 `common/python/` 后 `sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector`，依次应看到：启动日志 `subscription=['^hunter\.[^.]+\.telemetry$']` 且无 `UNKNOWN_TOPIC_OR_PART` → 消费组出现 offset → `vehicle_telemetry` 开始落行 → Redis 出现 `vehicle:status:HUNTER-001`（含 `status`/`updated_at`）→ 台账两列 ≤30s 刷新 → 页面转在线；`bash scripts/health-check.sh | grep 服务错误日志` 应 PASS。

**分发路径注意**（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` **只更新、不进入版本库**，因此本条的文档改动无法靠服务器 `git pull` 同步（需要时单独 rsync）；运行不依赖它们。需入库并走 git 同步的只有 9 个受追踪文件：`common/python/hunter_common/kafka/{contracts,consumer}.py`、`common/python/tests/test_kafka_{consumer_unit,message_roundtrip}.py`、`common/python/README.md`、`contracts/kafka/README.md`、`scripts/{health-check,daily-check}.sh`、`README.md`——V1.18.12 尚未提交，未 push 前 `git pull` 仍拿到旧镜像源码（V1.18.11 已入库，当前缺口仅在本条）。

## V1.18.13（修复：死信 Topic 从未被创建 —— 不符契约的消息被静默丢弃，且 `[2001]` 无法归因）

Date：2026-10-09
变更类型：修复（数据丢失类缺陷 + 可观测性补齐）

现场（V1.18.12 生效、平台确实开始消费之后的下一层）：`docker logs --since 20m hunter-collector` 每条车端报文都是同一段三连 ERROR + 两段 Traceback：

```
kafka_message_schema_invalid  topic=hunter.HUNTER-001.telemetry  error='[2001] 消息不符合契约 Schema telemetry'
kafka_produce_failed          topic=hunter.HUNTER-001.telemetry.dlq  attempts=1  error='KafkaError{code=_UNKNOWN_TOPIC,val=-188}'
kafka_dlq_produce_failed      reason=schema_invalid  topic=hunter.HUNTER-001.telemetry
```

同轮取证还有两条干扰项：`sudo docker exec hunter-kafka bash -lc 'kafka-topics.sh … --list'` 与 `kafka-consumer-groups.sh …` 均报 `command not found`（一度被读成“Kafka 有问题”）；`vehicle_telemetry` 仍 0 行、页面仍离线。

### 根因（第 ⑤ 层：兜底本身就是丢消息的地方）

1. **死信 Topic 从未被任何入口创建**：`{topic}.dlq` 是消费侧运行期派生的名字（`consumer._send_to_dlq`），而 `infra/docker/kafka/create-topics.sh`、`infra/k8s/jobs/kafka-init-job.yaml`、`scripts/init-kafka.sh`、车辆 provisioning `kafka_admin` 四处都**只创建源 Topic**。broker 关 `auto.create.topics.enable`（契约安全基线）→ 派生死信名永远不存在，“DLQ 开关开着但死信无处可送”。
2. **投递失败被当成“已处理”**：`_UNKNOWN_TOPIC`（librdkafka `-188`）的 `retriable=False`，`produce()` 的落盘条件是 `retriable and buffering` → 该路径 `attempts=1` 直接抛出；`_send_to_dlq` 捕获 `KafkaException` 后只记 WARN/指标，`_handle_message` 正常返回、批次照常提交 offset → **非法消息既没入库也没进死信，永久丢失且无法取证**。（旧落盘条件在此场景下等价于“报错但继续”，即静默丢弃被默认行为掩盖。）
3. **Schema 不符无字段级明细**：`KafkaMessageSchemaError.details["errors"]` 里有 jsonschema 前 5 条不符项，但 `HunterBaseException.__str__` 只输出 `[code] message`，日志又只打 `str(exc)` → 运维只拿到 `[2001] 消息不符合契约 Schema telemetry`，**无法判断是车端字段错还是契约过严**，只能猜、或干脆放宽契约（把数据质量问题变成静默丢失）。
4. **巡检 ⑬ 指错修复方向**（V1.18.12 留下的误判）：该项把 `UNKNOWN_TOPIC_OR_PART` 一律归因“订阅未命中 → 重建镜像”，而本条的 `_UNKNOWN_TOPIC` 来自**生产侧死信投递** —— 同一条 grep 命中给出完全错误的修复指引。
5. **`command not found` 与 Kafka 无关**：bitnami/kafka:3.6 把 CLI 放在 `/opt/bitnami/kafka/bin/`，该 PATH 只在 entrypoint 环境生效，`docker exec` 起的裸 shell 拿不到。仓库脚本（`scripts/common.sh` 的 `kafka_tool_cli`/`kafka_topics_cli`）早已内置 `command -v` 失败即回退绝对路径，所以“脚本能跑、手工命令失败”。

### 修复内容（契约先行 → 创建落地 → 不丢消息 → 可归因 → 巡检纠偏）

1. **`contracts/kafka/topics.yaml`（naming 段新增三项 + 责任登记）**：`dlq_pattern: "{original_topic}.dlq"`、`dlq_retention_ms: 2592000000`（30 天，与 `event_raw` 对齐）、`dlq_partitions_inherit_source: true`，并在注释里写明“死信名不会自动存在 + 创建责任三方（平台 Topic → 初始化脚本；每车 Topic → provisioning）”。`contracts/openapi/vehicle-service.yaml`：`x-hunter-kafka.per_vehicle_topics` 注明实际创建集合 = 8 源 + 8 派生死信，新增 `x-hunter-kafka.dlq_creation`（命名/保留期不属本服务决策，一律读 `topics.yaml#naming`），`x-hunter-provisioning.topics.action/rollback` 同步为 16 个、回滚含死信。
2. **创建责任四处落地（互不遗漏）**：`infra/docker/kafka/create-topics.sh` 与 `infra/k8s/jobs/kafka-init-job.yaml` 各追加 6 个 `{平台 Topic}.dlq`（分区继承源 Topic、`DLQ_RETENTION_MS=2592000000`）；`scripts/init-kafka.sh` 以 `TOPIC_SPECS` 为单一来源派生 `all_specs`（创建与校验都跑满 12 项，成功文案取 `${#all_specs[@]}`）；`services/vehicle-service/app/services/kafka_admin.py` 新增 `_parse_dlq_naming()`（契约缺字段时 warning + 兜底常量，不硬编码）、`_derive_dlq_entry()`、`render_vehicle_topics(vehicle_id, *, include_dlq=True)`，开通即建 16 个、回滚一并删除，`vehicle_topics_loaded` 日志带 `dlq_pattern`。车端不需要知道死信：`routers/vehicles.py` 的 `_topics_preview` 与 `services/bundle.py` 的 `_topic_lines` 传 `include_dlq=False`（接入包仍只列车端生产/消费的 8 个）。
3. **`common/python/hunter_common/kafka/producer.py`（“不得丢消息”路径可强制落盘）**：`produce(..., buffer_on_error: bool = False)`；落盘条件放宽为 `(retriable or force_buffer)`；`_UNRECOVERABLE_CODES = frozenset({KafkaError.MSG_SIZE_TOO_LARGE, KafkaError._INVALID_ARG})` —— **消息体自身非法/超长不落盘**（补投必然再失败，堆进缓冲只会挤掉正常样本）；日志新增 `forced` 与分场景 hint（区分“目标 Topic 不可用但已落盘”与“链路中断”）；缓冲不可用时记 `kafka_produce_buffer_unavailable` 后**仍抛异常**（不静默降级）。⚠ 补投只在 `close()` 里发生（无定时轮询），已把这点写进模块注释与 hint。
4. **`common/python/hunter_common/kafka/consumer.py`（不丢 + 可归因）**：`_schema_error_details(exc)` 取 `details["errors"]` 前 5 条 `字段路径: 说明`；`kafka_message_schema_invalid` 日志带 `schema_errors=[…]` + hint；`_send_to_dlq` 改传 `buffer_on_error=True`，`result.status == "buffered"` 时记 **`kafka_dlq_message_buffered`**（ERROR 级、可告警）后 return —— **不计** `hunter_kafka_dlq_failed_total`（消息没丢），仅缓冲也不可用时才走原失败路径；`dlq.error` 消息头追加 `schema_errors` 明细再截断 500 字符（从死信取报文即可直接看出哪个字段不符）。
5. **`scripts/health-check.sh` 第 ⑬ 项拆成三类信号**（不再“一个 fatal_re 打天下”，头注释明确承认此前指错过方向）：A `Subscribed topic not available` → **FAIL·订阅未命中**（V1.18.12，重建镜像）；B `kafka_dlq_produce_failed|kafka_dlq_message_buffered|kafka_produce_failed|code=_UNKNOWN_TOPIC|code=UNKNOWN_TOPIC_OR_PART` → **FAIL·目标 Topic 缺失**（给出 `init-kafka.sh` + 每车补建指引，并说明出现 `kafka_dlq_message_buffered` 即消息已落盘、建齐后自动补投）；C `kafka_message_schema_invalid` → **WARN·报文不符契约**（提示看 `schema_errors`、这类消息不入库故页面仍离线）；三者全未命中才落回通用 WARN。A/B/C 可同时记录（一次巡检看到多层问题）。
6. **`scripts/verify_data_layer.py`：死信创建由“三方”扩为五方逐项比对** —— `topics.yaml#naming` ↔ `create-topics.sh` ↔ `kafka-init-job.yaml` ↔ `init-kafka.sh` ↔ provisioning 派生（死信名以 `.dlq` 结尾、分区继承、保留期取契约）。校验器需先展开 shell 变量才能匹配 `"$DLQ_RETENTION_MS"`（历史坑：正则匹配不到变量写法 → 假通过）。数据层校验项数 **34**（新增死信比对，其余项计数不变），已重生 `infra/k8s/base/03-configmap-contracts.yaml`（`generate_contracts_configmap.py --check` 会拦截 naming 段变更）。
7. **单测**：`common/python/tests/test_kafka_producer_unit.py`（`buffer_on_error` 下 `_UNKNOWN_TOPIC` 仍落盘、`attempts=1` 不重试、超长/非法参数不落盘、缓冲不可用仍抛、`forced` 日志字段）；`test_kafka_consumer_unit.py`（`schema_errors` 进日志、`dlq.error` 头含明细、`status=buffered` 不计失败指标）；`services/vehicle-service/app/tests/test_kafka_admin.py`（DLQ 派生 16 项、naming 缺字段兜底并告警、`include_dlq=False` 只出 8 项）与 `test_provisioner.py`（回滚含死信）；`test_vehicle_contract.py` 新增 4 条 DLQ 契约断言（`dlq_pattern` 含 `{original_topic}`、`dlq_retention_ms > 0`、`dlq_partitions_inherit_source` 为真、openapi `x-hunter-kafka.dlq_creation` 非空）。教训类用例同步修正：写错的 `bool(x, )`（尾逗号使参数变成 tuple → 断言永真）已改回单参数形式。
8. **顺带清掉 V1.18.11 末尾“另需处理”的两个 vehicle-service 缺陷**：① `test_vehicle_contract.py` 的 `ROOT = Path(__file__).resolve().parents[3]` 算错一层（指向 `services/contracts/...`，全仓 8 处同类只有它错）→ 改 `parents[4]`；② `test_fastapi_routes_registered` 在 Starlette 1.6 / FastAPI 0.141 下**漏判**：`include_router()` 不再拍平，`app.routes` 里是 `_IncludedRouter(original_router=…)` 聚合节点，直接遍历会认为“业务路由一条都没注册”（实际 `main.py` 已注册）→ 引入其余 3 个服务契约测试早已使用的 `iter_registered_routes()` 递归展开，断言改为集合差 `missing`。该服务现 **45 passed**（仍未进 `run_unit_tests.py` 目标：CI unit 只装 `common/python[dev]`，vehicle-service 还需 `kafka-python-ng`，扩 CI 范围属独立决策，见“待办”）。
9. **文档**：`contracts/kafka/README.md`（“DLQ Topic 必须显式创建”“DLQ 转投失败不丢消息”“Schema 不符必须留字段明细”三行语义 + 五方比对与补投触发时机）；`docs/运维操作手册.md` 新增 **§7.7 死信 Topic 的创建、补建与落盘缓冲确认**（三方职责表 / 齐全性核对命令 / 逐车补建脚本段 / 落盘缓冲确认与补投时机 / `schema_errors` 归因表）、每日巡检表新增“死信 Topic 齐全”行与 ⑬ 三类信号判读、§3.4/§7.4.2 全部裸 `kafka-*.sh` 改绝对路径并说明成因；`docs/故障排查指南.md`（§0 新增“Kafka CLI 必须写绝对路径”、6 处取证命令修正、场景4 步骤 4/5/6 增补死信检查与 `schema_errors` 判读、原因表新增“死信 Topic 缺失导致静默丢弃”“取证命令 command not found”并重写“消息格式不符”行）；`README.md` / `common/python/README.md`（生产者 `buffer_on_error` 语义、消费者 `schema_errors`、“死信 Topic 必须显式创建”块、数据层校验项数 32→34）；`scripts/init-kafka.sh` usage 的校验命令改绝对路径。

向后兼容：不改任何对外 API、DB schema、消息 Schema 与 Topic 契约名；`produce()` 新参数默认 `False`，现有全部调用方行为逐字不变（仅 DLQ 转投显式开启）；`render_vehicle_topics()` 新参数默认 `include_dlq=True`，两处“给人看”的调用点主动传 `False` 保持接入包/详情页输出不变；命名/保留期规则读契约、缺字段才兜底，**不新增任何可由实现自行决定的 Topic 名**；巡检 ⑬ 只改变判读文案与定级分布，退出码语义（0/1/2）不变。旧环境升级后无需数据回填（丢失的报文无法追回，只能等车端重发）。

风险：① **补投只在生产者 `close()` 触发（无定时轮询）** —— 建齐 Topic 后必须 `restart` 该服务才会把缓冲补投出去；异常退出（OOM / `kill -9`）不补投但缓冲保留，下次优雅停止时继续；② 缓冲上限 1GB、整段 FIFO 淘汰最旧并记 `kafka_buffer_dropped`：死信 Topic 长期缺失 + 大量非法报文会挤掉最老样本，**发现即补建**；③ 每车 Topic 从 8 变 16，已开通车辆不会被 `init-kafka.sh` 自动补建（脚本只管平台内部），必须手工或走前端“接管已有”，否则本条对存量车辆不生效；④ 分区数继承源 Topic 且**只能增不能减**，源 Topic 后续扩分区不会带动既有死信（不报错，仅影响并行度）；⑤ 死信不在 Topic 契约内，生产 acks 回退 `KAFKA_PRODUCER_ACKS` 并告警（属预期噪声，见 `contracts/kafka/README.md`）；⑥ `buffer_on_error` 只保护死信路径，普通业务投递仍只对可重试错误落盘（避免把 `_INVALID_ARG` 这类编程错误堆进缓冲）；⑦ 车端 `telemetry` 究竟哪个字段不符契约**本条不替用户决定**（不擅自放宽 Schema），须按 `schema_errors` + 死信原始报文归因后再改车端或改契约。

验证：`python scripts/run_unit_tests.py` 9 个目标全通（common/python **323**、data-collector 143、scene-service 108、ota-service 128、remote-control 97、api-gateway 71、data-analytics 60、flink-jobs 15、tests 100 deselected）；`pytest services/vehicle-service` **45 passed**；`verify_data_layer.py` **34 项全通过**（含死信五方比对）、`verify_infra.py` / `verify_test_layer.py` / `render_k8s.py --check` / `generate_contracts_configmap.py --check` 均退出码 0；`mypy --ignore-missing-imports tests/support scripts/verify_test_layer.py` 无问题；6 个 shell 脚本 `bash -n`（CRLF→LF 副本，`core.autocrlf` 环境下必需）通过；`ruff check tests common/python services` 与 **HEAD 基线逐项一致（50 项存量债，本次新增的 7 项已清零）** —— 用 `git worktree` 拉 HEAD 基线做集合差确认，未把无关清理混进本条。

**实机待执行**（顺序不可颠倒）：
```bash
cd /opt/HunterCore && git pull                                   # 受追踪 22 个文件（见下“分发”）
sudo bash scripts/init-kafka.sh                                 # 平台内部 6 + 死信 6（幂等，并校验配置）
# 取证一律写绝对路径（`docker exec` 的裸 shell 里 kafka-topics.sh 不在 PATH，会报 command not found）
sudo docker exec hunter-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config /tmp/hunter-client.properties --list | grep -c '\.dlq$'      # 期望 6 + 车数×8
# 已开通车辆（HUNTER-001）补建 8 个每车死信：运维手册 §7.7 第 3 步（或前端重跑“新增车辆 + 接管已有”）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . restart data-collector   # 触发 close() 补投已缓冲死信
sudo bash scripts/health-check.sh | grep 服务错误日志                       # 应只剩 WARN·报文不符契约（待车端/契约归因）
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 仍“只更新、不入库”。需入库并走 git 同步的是 **22 个受追踪文件**：`README.md`、`common/python/README.md`、`common/python/hunter_common/kafka/{producer,consumer}.py`、`common/python/tests/test_kafka_{producer_unit,consumer_unit}.py`、`contracts/kafka/{topics.yaml,README.md}`、`contracts/openapi/vehicle-service.yaml`、`infra/docker/kafka/create-topics.sh`、`infra/k8s/{base/03-configmap-contracts.yaml,jobs/kafka-init-job.yaml}`、`scripts/{init-kafka.sh,health-check.sh,verify_data_layer.py}`、`services/vehicle-service/app/routers/vehicles.py`、`services/vehicle-service/app/services/{bundle,kafka_admin,provisioner}.py`、`services/vehicle-service/app/tests/{test_kafka_admin,test_provisioner,test_vehicle_contract}.py`（V1.18.12 与本条均未 push 前，服务器 `git pull` 仍拿不到死信创建与落盘兜底逻辑）。

**待办（不属本条范围）**：① 车端 `telemetry` 报文的字段级归因与修复（看升级后的 `schema_errors` 与死信头 `dlq.error`）；② `kafka_consumer_stopped` 本轮退出原因未定性（日志末尾 `group_id=data-collector-telemetry topics=['hunter.*.telemetry']` 仍是旧字段文案，需确认镜像版本）；③ 同车消息落在 partition 2/3 与“`key=vehicle_id` 分区内有序”期望不符，需核实车端是否真的带了 key；④ `.env` 补 `KAFKA_CERT_SAN_IPS=101.201.150.237`（V1.18.10 遗留）；⑤ 给 CI 加 `services/vehicle-service` 目标（需补 `kafka-python-ng` 依赖），否则该服务单测仍靠手工跑；⑥ **存量车辆补建死信 Topic 无脚本入口**（实机已碰）：`init-kafka.sh` 只管平台内部 Topic，已开通车辆必须手工循环建 8 个 `.dlq`（手册 §7.7 第 3 步）或重跑前端“接管已有”——后续版本应提供 `init-kafka.sh --vehicle-dlq <id>` 或独立补建脚本（分区从源 Topic 读、命名仍取契约，不得硬编码）；⑦ 巡检 ⑬ 默认窗口 15m 偏短（实机对比：15m 全 PASS，`ERROR_LOG_WINDOW=6h` 立即揪出 hunter-analytics 346 条、hunter-remote 4 条 error）——`daily-check.sh` 应用更长窗口而非沿用默认值。

## V1.18.14（修复：接入全绿、曾正常后 `last_online` 突然冻结 —— 观测代码 `_refresh_lag` 抛 `TypeError` 冲垮消费主循环且无监督重启）

Date：2026-10-10
变更类型：修复（致命功能缺陷 + 健壮性补齐）；关闭 V1.18.13 待办②

现场（V1.18.12/13 生效、平台确实开始消费、页面曾转在线之后）：车辆管理页“最近在线”从“尚未上线”跳到某一刻后**长时间不再刷新**（实测冻结在 11:47:52，页面显示“16 分钟前”且持续增大）、运行状态显示“离线”，而车端 `hunter_kafka` 自检消费器 15s 内从 broker 读到 130 条新鲜 health/telemetry（证明车端在发、Topic 有数据）。逐项取证：
- `kafka-consumer-groups.sh --describe --group data-collector-health` 与 `…-telemetry` 均报 **`has no active members`**，`LAG` 分别 **2165 / 21692** 且持续增长（health CURRENT-OFFSET 停在 2、telemetry 停在 1）；
- Redis `vehicle:status:HUNTER-001` 的 `last_seen_at`/`updated_at` 冻结在 `1791604072.612`（=11:47:52），`status=offline`；
- `docker logs hunter-collector | grep -i consumer` 只有一条：`2026-10-10T03:47:52Z [info] group_id=data-collector-health message=kafka_consumer_stopped`，**其后再无 `kafka_consumer_started`**（进程未重启、`/healthz` 200、容器 healthy）。

### 根因（第 ⑥ 层：探针看不见的“静默停摆”，且崩溃点在共享库）

1. **`_refresh_lag` 把 `TopicPartition` 当 `int` 取整**：`common/python/hunter_common/kafka/consumer.py` 的 `_refresh_lag` 里 `Consumer.position([tp])` 运行时返回的是 **`list[TopicPartition]`**（偏移在 `.offset` 上），旧代码却按 `list[int]` 处理并 `cast("list[int]", …)` 抹掉类型桩、直接 `int(positions[0])` → 抛 `TypeError: int() argument … not 'cimpl.TopicPartition'`。
2. **观测异常冲垮了消费主循环**：`_refresh_lag` 的 docstring 明明写“失败不影响消费”，但只 `except KafkaException`，`TypeError` 不属于它 → 冒泡到 `run()` 的 `while` 外 → `finally` 关闭底层 Consumer 并打 `kafka_consumer_stopped` → **消费任务终止**。
3. **为何恰在“开始有数据”那一刻崩、且只更新一次**：`_refresh_lag` 只在**消费到非空批次后**才执行（`run()` 里空批 `continue`）。11:47 前车端在 communication_loss/崩溃风暴里，平台收不到消息 → 永远走不到 `_refresh_lag`，bug 不触发；11:47:52 车端恢复、第一批 health 到达 → 正常处理并回写 Redis/DB（`last_online` 更新一次）→ 紧接着 `_refresh_lag` 抛错 → 消费任务死亡。**表现为“消费到第一批即崩溃”。**
4. **无监督 → 静默永久停摆**：`app/main.py` 的 `consumer_tasks` 只在 lifespan 启动时 `create_task`、停机时才 `await`，无 done-callback/存活监控/重启。任务崩溃后异常被吞在 task 里（运行期不打印），进程与探针全绿，故障被完全掩盖 37 分钟。`docker restart` 只是临时续命：重启后消费一批（`batch_size=100`，CURRENT-OFFSET 2→102）即再次崩溃（同一 bug 仍在），故重启后 `--describe` 仍是 `no active members`。
5. **单测为何没抓到**：`common/python/tests/test_kafka_consumer_unit.py` 的 `FakeConsumer.position()` 旧实现返回 `list[int]`——**与生产代码犯了同一个错**，`test_consumer_lag_metric_refreshed` 因此恒绿，Mock 把缺陷当预期钉住了（对应“Mock 测试真实性”红线）。

### 修复内容

1. **`common/python/hunter_common/kafka/consumer.py`（致命 bug）**：`_refresh_lag` 改为取 `positions[0].offset`（与 confluent-kafka 真实返回类型一致，删除错误的 `cast` 与 `from typing import cast`）；兜底从 `except KafkaException` 放宽为 `except Exception`（观测/指标旁路**绝不得冒泡杀死消费主循环**，docstring 补记该硬约束与历史故障）。
2. **`services/data-collector/app/main.py`（纵深防御：崩溃自愈）**：新增 `_SupervisedConsumer`（factory 重建 + 退避重启，常量 `CONSUMER_RESTART_BACKOFF_S=5.0`）——因 `KafkaConsumerManager.run()` 在 `finally` 关闭底层 `Consumer`，崩溃后旧实例不可复用（再 `subscribe` 抛 `RuntimeError`），故必须用 factory 重建而非重跑 `run()`；崩溃时打 `consumer_crashed_restarting`（带 `group_id`/`backoff_seconds`）并 `logger.exception` 立即暴露堆栈（不再等停机）。`_build_consumers` 返回监督器列表；lifespan 主体不变（`consumer.run()/stop()/group_id` 接口对监督器同样成立）。
3. **`common/python/tests/test_kafka_consumer_unit.py`（还原测试真实性）**：`FakeConsumer.position()` 改为返回 `.offset` 已填充的 `list[TopicPartition]`（附历史教训注释）。改后旧实现会立即 `TypeError`，该用例从此成为真正的回归护栏（高水位 10 − 位点 4 = 6 断言不变、通过）。

向后兼容：不改任何对外 API、DB schema、消息契约、Topic/消费组名与订阅写法；`_refresh_lag` 仅在开启积压指标（默认）时执行，行为收敛为“算不出就跳过、绝不影响消费”；`_SupervisedConsumer` 对 lifespan 透明（`app.state.ingest_consumers` 仍为可 `run()/stop()` 的对象列表、`group_id` 可枚举），旧单测与调用方不受影响；重启退避为固定 5s（依赖未恢复时避免重启风暴），无需配置。

风险：① **修复在共享库 `hunter_common`（以 site-packages 装入镜像），必须 `--build` 重建 data-collector 镜像才生效**，否则线上仍旧包、重启后依旧“吞一批→再崩”（与 V1.18.12 同类踩坑）；② 监督器只处理“崩溃退出”，若消费卡在无超时的阻塞调用（Redis/DB 无 timeout）仍会 hang 而非重启——本次未一并加 handler 侧超时（列为待办）；③ 崩溃后重建会新建 `IdempotencyGuard`（内存幂等窗口清空），退化为 at-least-once 重投，依赖写入侧唯一约束去重（telemetry `ON CONFLICT`、台账两列幂等），不产生重复数据；④ 停摆期间（本次 37 分钟）已提交 offset 之后、未处理的积压会在恢复后一次性回补，`last_online`/看板时间戳出现跳变属预期；⑤ 监督重启会打 `consumer_crashed_restarting` ERROR 级日志，若某服务长期反复崩溃需据此定位根因而非仅依赖自愈。

验证：`pytest common/python/tests/test_kafka_consumer_unit.py -q` **20 passed**（含 `test_consumer_lag_metric_refreshed` 在真实返回类型下通过）；`pytest services/data-collector -q` **143 passed**；`ruff check` 三处改动文件 **All checks passed**（`_refresh_lag` 兜底 `# noqa: BLE001, S112`、监督器 `except Exception` 因 `logger.exception` 记堆栈不触发 BLE001）。**实机待确认**：同步 `common/python/` + `services/data-collector/` 后重建镜像，依次应看到——`docker logs hunter-collector` 停机时打印 `consumer_stop_failed` 的完整 `TypeError` 堆栈（旧任务 re-raise）→ 启动 `kafka_consumer_started`（三组）→ 页面“最近在线”≤30s 恢复刷新、`kafka-consumer-groups --describe` 出现成员且 LAG 回落、Redis `last_seen_at` 跟随当前时间。

**实机执行（顺序不可颠倒，根因在共享库必须重建）**：
```bash
cd /opt/HunterCore
# 同步 common/python 与 services/data-collector 后重建（仅 restart 无效）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector
# 确认崩溃堆栈（重启旧实例停机时 re-raise）与三组重新消费
sudo docker logs hunter-collector 2>&1 | grep -A20 -iE 'consumer_stop_failed|_refresh_lag|TypeError' | tail -40
sudo bash -c 'set -a && . ./.env && set +a && source scripts/common.sh && \
  kafka_tool_cli kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group data-collector-health'
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **3 个**：`common/python/hunter_common/kafka/consumer.py`、`common/python/tests/test_kafka_consumer_unit.py`、`services/data-collector/app/main.py`（`docs/` 下运维手册/排障指南/部署文档的同步改动需单独 rsync 到服务器）。

**待办（不属本条范围）**：① 给消费 handler 路径的 Redis/DB 调用加超时（防“卡死”型停摆，本条只覆盖“崩溃退出”型）；② 把“消费任务存活 / 最后 poll 时间”纳入 `/readyz` 或暴露 `consumer_last_poll_age_seconds` 指标，让探针能主动抓到停摆（当前仅靠 `health-check.sh` ⑬ 事后扫 error 日志）；③ 车端 `telemetry` 报文字段级归因（V1.18.13 待办① 延续）。

## V1.18.15（修复：运营看板“全部数据源均不可用”全 0 —— 单机 compose 未给 data-analytics 注入看板依赖环境变量）

Date：2026-10-10
变更类型：修复（部署接线缺失，非代码缺陷）

现场（V1.18.14 生效、车辆详情页已转在线（状态=故障）之后）：运营看板顶栏报 `看板全部数据源均不可用`，车辆总数 / 在线车辆 / 遥测入库点 / 感知帧率 / 未确认事件全为 0 或 “-”。而 `SELECT count(*), max(time) FROM data_collector.vehicle_telemetry` 已有数据（telemetry 已在落库）、Redis `vehicle:online:set` 非空。

### 根因（与 `_refresh_lag` 无关，是两套编排不对称的部署缺口）

1. **看板四块“任一抛异常/返回 None 即该块降级”，四块全降级才 503**（`services/data-analytics/app/services/dashboard.py`：`len(degraded)==len(_BLOCKS)` → `ServiceUnavailableError("看板全部数据源均不可用")`）。四块各自的依赖：fleet → Redis + vehicle-service HTTP（`fleet_total`）；pipeline → 只读库（`telemetry_points`）+ Kafka；algorithm → 只读库（`algorithm_averages`）；events → data-collector HTTP（`/api/v1/data/events`）。“不可用”是连接失败，不是“无数据”（无数据仍 available=True + 值 0）。
2. **compose 单机编排漏配 data-analytics 的依赖 env**：`infra/deploy/docker-compose.yml` 的 data-analytics 段只继承 `*business-env` + 一个 `FLINK_JM_URL`，**未注入 `DATA_COLLECTOR_BASE_URL` / `VEHICLE_SERVICE_BASE_URL` / `SCENE_SERVICE_BASE_URL` / `ANALYTICS_RO_DB_USER` / `ANALYTICS_RO_DB_PASSWORD`**；而 K8s 清单 `infra/k8s/services/data-analytics.yaml` 这几项全有——**两套编排不对称**。空值下：`vehicle_service_base_url=""` → `fleet_total()` 返回 None（fleet 降级）；`data_collector_base_url=""` → `_get` 直接 `data_collector_base_url_missing`（events 降级）；`analytics_ro_db_user/password=""` → 只读库连不上（pipeline + algorithm 降级）→ 四块同时挂。
3. **只读角色 `hunter_analytics_ro` 在单机部署从未创建**：`infra/deploy/sql`（init-data.sql / schema.sql / timescaledb.sql）无任何 `CREATE ROLE`/`GRANT`，即使配了 `ANALYTICS_RO_DB_USER` 也找不到该角色。
4. **为何此前未暴露**：本条是既有缺口，但 V1.18.12/13/14 期间消费链路整体停摆、看板本就无数据且用户进不了车辆页；V1.18.14 修好车辆页后，才第一次能进看板看到这一层。
5. **附带发现**：`FLINK_JM_URL` 键名与 config 字段 `flink_jobmanager_url`（对应 env `FLINK_JOBMANAGER_URL`）不符 → `flink_jobmanager_url` 一直为 ""（非看板依赖，属 Flink 提交路径的潜在缺陷，本条一并补正确键名）。

### 修复内容

1. **`infra/deploy/docker-compose.yml`（data-analytics 段补 5 个 env + 修正 Flink 键名，与 K8s 清单对齐）**：`DATA_COLLECTOR_BASE_URL: http://data-collector:8082`、`VEHICLE_SERVICE_BASE_URL: http://vehicle-service:8086`、`SCENE_SERVICE_BASE_URL: http://scene-service:8081`（hunter-net 内服务名）；`ANALYTICS_RO_DB_USER`/`ANALYTICS_RO_DB_PASSWORD` 默认复用 `POSTGRES_USER`/`POSTGRES_PASSWORD`（compose 不建专用角色），`.env` 可覆盖为只读专用账号；新增 `FLINK_JOBMANAGER_URL`。**纯环境变量变更，无需 `--build`**。

向后兼容：不改任何代码/对外 API/契约/K8s 清单；仅补 compose 环境变量，全部带默认值（`:-`），未设 `.env` 也能起；对已按 K8s 部署的环境无影响。

风险：① **嵌套默认值 `${A:-${B}}` 需 Docker Compose v2**，旧版解析为空时需在 `.env` 显式写 `ANALYTICS_RO_DB_USER`/`ANALYTICS_RO_DB_PASSWORD`；② **默认复用超级用户读库违背最小权限**（契约 er.md 第 3 节要求 `hunter_analytics_ro` 只读），生产应建专用角色 + `GRANT SELECT ON data_collector.vehicle_telemetry, data_analytics.algorithm_metrics` 再用 `.env` 覆盖（列为待办）；③ events 块依赖 data-collector `:8082` 服务间直连可达（不经网关），若后续对内部端点加鉴权需同步；④ 本条只修看板**读路径**（直连 DB/Redis/HTTP，不依赖 data-analytics 自身 Kafka 消费者），但 `algorithm_metrics` 需 Flink/写入方产出，缺则算法块 available=True 但值为空（非降级）。

验证：`yaml.safe_load` 解析 compose 通过、data-analytics env 含 7 个新键；`docker compose -f infra/deploy/docker-compose.yml --project-directory . config data-analytics` 应输出非空的三个 BASE_URL 与 RO 账号（实机待确认）。**实机待确认**：`up -d data-analytics`（无 `--build`）后刷新看板——横幅消失、车辆总数（vehicle-service）/在线车辆（Redis）/遥测入库点（DB）出数；若仍全 0 按分块定位（`docker logs hunter-analytics | grep -iE 'base_url_missing\|fleet_total_failed\|query_failed'`）。

**实机执行**：
```bash
cd /opt/HunterCore
# 同步 compose 后先看解析结果（尤其 ANALYTICS_RO 不能为空）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . config data-analytics | grep -iE 'BASE_URL|ANALYTICS_RO|FLINK_JOBMANAGER'
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d data-analytics   # 纯 env 变更，无需 --build
sudo docker exec hunter-analytics env | grep -iE 'DATA_COLLECTOR_BASE_URL|VEHICLE_SERVICE_BASE_URL|ANALYTICS_RO'
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **1 个**：`infra/deploy/docker-compose.yml`（`docs/` 下运维手册/排障指南/部署文档的同步改动需单独 rsync 到服务器）。

**待办（不属本条范围）**：① `infra/deploy/sql` 增补 `hunter_analytics_ro` 角色 + `GRANT SELECT`（最小权限），并在 `.env.example` 补 `ANALYTICS_RO_DB_USER`/`ANALYTICS_RO_DB_PASSWORD` 说明；② 前端“最近在线”将浮点秒数原样渲染（`15.783999919891357 秒前`）的显示缺陷；③ 核对 data-analytics 自身 Kafka 消费者是否也需随 V1.18.14 重建镜像（同一 `_refresh_lag` bug 影响所有共享 `hunter_common` 的消费者服务）。

## V1.18.16（修复：补全 env 后运营看板仍 503/全 0 —— data-analytics 与下游三处代码级集成缺陷）

Date：2026-10-10
变更类型：修复（代码级，需重建镜像）

现场（V1.18.15 补全 data-analytics 依赖 env 并 `up -d` 后）：看板仍报 `看板全部数据源均不可用`、全 0；`docker logs hunter-analytics` 不再报 `base_url_missing`，而是三块各自的具体错误：`vehicle_fleet_total_failed` 伴 `405 Method Not Allowed`、`data_collector_event_query_failed` 伴 `422 Unprocessable Entity`（连不带过滤的最简查询也 422）、`telemetry_points_query_failed`/`algorithm_averages_query_failed` 伴 `Traceback`（而 `SELECT count(*) FROM data_collector.vehicle_telemetry` 有 63513 行、RO 账号为超级用户）。

### 根因（三个独立的既有集成 bug，此前被“消费链路全停摆”掩盖，V1.18.14/15 逐层修好才暴露）

1. **fleet 块路径错**：`VehicleDirectoryClient.fleet_total()` 请求 `GET /api/v1/vehicle`，但 vehicle-service 列表端点实为 `GET /api/v1/vehicle/list`（`/api/v1/vehicle` 仅允许 `POST` provisioning）→ **405**。（`vehicle_exists()` 用 `/api/v1/vehicle/{id}` 是对的，仅 `fleet_total` 路径错。）
2. **服务间直连缺身份头**：data-collector / vehicle-service 的 `_require_forwarded_auth` 将 `X-User-Id`/`X-Roles` 声明为**无默认值的必填 Header**（`str | None` 但未 `= None`，FastAPI 仍视为必填）；这些头原本由 api-gateway 代理时注入，data-analytics **直连下游不带** → FastAPI 在进入函数体前即返回 **422**（故 6 个事件查询全 422，fleet 修好路径后也会撞上同一问题）。
3. **pipeline/algorithm 块时间类型绑定错**：`MetricsReadOnlyRepository.telemetry_points/algorithm_averages` 将 **float epoch 秒**直接作为参数绑给 asyncpg 去比较 `TIMESTAMPTZ` 列（`WHERE time >= $1`）→ asyncpg 要求 `datetime` 实例，抛类型异常 → 整块降级。

### 修复内容（均在 `services/data-analytics/app/`）

1. **`config.py`**：新增服务间直连身份头配置 `internal_call_user_id`（默认 `data-analytics`）/ `internal_call_roles`（默认 `analyst`，同时落在 data-collector `data:read(admin,analyst,operator)` 与 vehicle-service `vehicle:read(admin,operator,analyst,viewer)` 交集内，最小权限）+ `internal_call_headers()`（配 `GATEWAY_HMAC_SECRET` 时用 `hunter_common.internal_auth.build_identity_headers` 签名，否则明文 `X-User-Id`/`X-Roles`/`X-Trace-Id`）。
2. **`repositories/vehicles.py`**：`fleet_total()` 改请求 `/api/v1/vehicle/list`；新增 `headers_provider` 逐请求注入身份头（HMAC 时间戳须新鲜，不可在构造期固化）。
3. **`repositories/events.py`**：`_get()` 同样逐请求注入身份头。
4. **`repositories/pipeline.py`**：`telemetry_points`/`algorithm_averages` 先 `datetime.fromtimestamp(ts, tz=UTC)` 再绑定 asyncpg。
5. **`core/dependencies.py`**：`get_event_client`/`get_vehicle_client` 传 `headers_provider=settings.internal_call_headers`。

向后兼容：不改对外 API/契约/K8s 清单；仅新增带默认值的配置项与修正内部调用。若 `.env` 未设 `GATEWAY_HMAC_SECRET`（与下游一致为空），走明文头分支，行为与下游“未配置密钥则跳过验签”一致，仍可通。

风险：① **本条是代码变更，必须 `--build` 重建 data-analytics 镜像**（`restart` 无效）；② `GATEWAY_HMAC_SECRET` 必须与网关/下游**同一值**，否则下游验签失败（401）；若已配置但 data-analytics 拿到不同值会仍不通；③ 服务身份 `roles=analyst` 基于当前两下游 read 角色集；若 vehicle-service `vehicle:read` 调整不致 analyst 不再可用，需 `.env` 覆盖 `INTERNAL_CALL_ROLES` 为 `admin`；④ 本条只修看板**读路径**（直连 DB/Redis/HTTP）；若 `algorithm_metrics` 无上游产出，algorithm 块 available=True 但值为空（非降级）；⑤ pipeline 的 float→datetime、fleet 的 `/list`、身份头注入三处暂无直接单测（看板端点测试走 fakes，不触真实客户端）。

验证：`ruff` clean；data-analytics 单测 **60 passed**。**实机待确认**：`up -d --build data-analytics` 后看板横幅消失、车辆总数/在线车辆/遥测入库点/未确认事件出数；`docker logs --since 2m hunter-analytics | grep -iE 'fleet_total_failed|event_query_failed|query_failed|405|422'` 应为空。

**实机执行**：
```bash
cd /opt/HunterCore
sudo git pull   # 或 rsync 同步下列 5 个受追踪文件
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics   # 代码变更，必须 --build
sudo docker logs --since 2m hunter-analytics 2>&1 | grep -iE 'fleet_total_failed|event_query_failed|query_failed|405|422'   # 应无输出
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **5 个**：`services/data-analytics/app/config.py`、`.../repositories/vehicles.py`、`.../repositories/events.py`、`.../repositories/pipeline.py`、`.../core/dependencies.py`（`docs/` 下同步改动需单独 rsync 到服务器）。

**待办（不属本条范围）**：① 补 `httpx.MockTransport` 针对性单测锁住 fleet `/list`、身份头注入、pipeline float→datetime 三处回归；② 若希望服务间调用统一经 api-gateway（带服务 JWT）而非直连，需重构依赖注入；③ 同 V1.18.15 待办（RO 专用角色、前端浮点显示、data-analytics 自身消费者镜像核对）。

## V1.18.17（修复：看板 pipeline 块 Kafka 探测逐字符拆分 broker 地址；看板 events 块 data-collector 响应信封缺 `timestamp` 致 500；车辆详情“最近在线”浮点秒显示）

Date：2026-10-10
变更类型：修复（pipeline / events / 前端 三处均已落地并验证）

现场（V1.18.16 重建后）：看板横幅消失，车辆总数=1、在线车辆=1、状态分布、感知/控制时延均出数；但仍报 `部分维度降级：pipeline, events`——遥测入库点/入库延迟 P95 为“kafka unavailable”，未确认事件为“data-collector unavailable”。另：车辆详情页“最近在线”显示为 `0.22699999809265137 秒前`。

### 根因

1. **pipeline 块（已修）**：`KafkaLagProbe.__init__` 写 `"bootstrap.servers": ",".join(settings.kafka_bootstrap_servers)`，但 `kafka_bootstrap_servers` 是**字符串**（默认 `"localhost:9092"`，另有 `.kafka_bootstrap_servers_list` 属性）——对字符串做 `join` 会**逐字符拆分**（`l,o,c,a,l,h,o,s,t,:,9,0,9,2`），rdkafka 日志实锤：`sasl_plaintext://f:9092`、`a:9092`、`0:9092` 等碎片 host → 探测连不上 → `consumer_lag()`/`dlq_depth()` 返回 None → pipeline 块 `kafka unavailable`（DB 侧因 V1.18.16 已修，故仅剩 kafka 降级）。共享库 consumer/producer 一直用 `_list` 属性，本处为 data-analytics 漏用。
2. **前端“秒前”浮点（已修）**：`frontend/src/utils/format.ts` 的 `formatRelativeTime` 中 `Math.floor(Date.now()/1000) - epochSeconds`，`epochSeconds` 来自 `Date.parse(...)/1000` 为浮点，相减仍为浮点，`< 60` 分支 `${diffSeconds} 秒前` 原样渲染。
3. **events 块（已修）**：data-analytics 侧已正常（不再 422），但 `data-collector` 对 `GET /api/v1/data/events`（含只带时间窗、page_size=1 的最简查询）**稳定返回 500**。堆栈实锤：`app/routers/events.py:55` 手工构造 `EventListResponse(...)` 时只传了 `request_id`、**未传 `timestamp`** → `pydantic_core.ValidationError: 1 validation error for EventListResponse ... missing`。根因在 schema 层：`EventListResponse`/`EventResponse`（以及 `FileListResponse`/`FilePresignResponse`/`FileCompleteResponse`/`TelemetryQueryResponse`）将 `request_id: str`、`timestamp: int` 声明为**必填无默认**，而路由统一不传 `timestamp`（files/telemetry 属同类潜在 500，仅因看板未命中而未暴露）。先前两个假设（枚举 `.value`、MinIO 预签名）均经代码排除。

### 修复内容

1. **`services/data-analytics/app/repositories/pipeline.py`**：`",".join(settings.kafka_bootstrap_servers)` → `",".join(settings.kafka_bootstrap_servers_list)`（与共享库 consumer/producer 口径一致）。
2. **`frontend/src/utils/format.ts`**：`Math.floor(Date.now() / 1000 - epochSeconds)`（先整体取整再分支，消除浮点尾巴）。
3. **data-collector 响应信封（events + files + telemetry 共 6 处）**：将各 `timestamp: int` 改为 `timestamp: int = Field(default_factory=lambda: int(time.time()))`（与 vehicle-service `schemas/response.py` 写法完全一致）。一处根治：路由无需逐个补传 `timestamp`，新增端点也不会再犯；同时顺带修好同因潜在的 `/files`、`/telemetry` 查询 500。改动文件：`services/data-collector/app/schemas/events.py`、`.../files.py`、`.../telemetry.py`（各加 `import time`）。

向后兼容：三处均为行为修正，不改对外 API/契约；pipeline 修正后探测能正常连上内部 broker；前端仅影响相对时间显示精度；data-collector 信封只是为 `timestamp` 补默认（放宽校验、不改变输出字段，响应仍带真实 epoch 时间）。

风险：① **pipeline 是代码变更，需 `--build` 重建 data-analytics**；若修正后仍 `kafka unavailable`，则转查 SASL/网络（而非地址拆分）；② **前端变更需重新构建 web-portal 镜像**（`--build web-portal` 或前端 CI），非 compose env；③ **events 修复是 data-collector 代码变更，需 `--build data-collector`**（堆栈路径 `/app/app/...` 表明 app 代码烘入镜像，`restart` 无效）。

验证：data-analytics `ruff` clean、单测 **60 passed**（pipeline 修正不触现有 fakes 路径）；data-collector `ruff` clean、单测 **143 passed**。**实机待确认**：重建后 pipeline 块不再报 `kafka unavailable`、遥测入库点出数；events 块不再 500、未确认事件出数；前端重建后“最近在线”显示整数秒。

**实机执行**：
```bash
cd /opt/HunterCore
sudo git pull
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics   # pipeline 修复
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector    # events 信封 timestamp 修复
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build web-portal        # 前端“秒前”修复
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **5 个**：`services/data-analytics/app/repositories/pipeline.py`、`frontend/src/utils/format.ts`、`services/data-collector/app/schemas/events.py`、`.../schemas/files.py`、`.../schemas/telemetry.py`（`docs/` 下同步改动需单独 rsync 到服务器）。

**待办（不属本条范围）**：① 补 data-collector 响应信封回归单测（断言不传 `timestamp` 也能构造成功，防再退回必填无默认）；② 补 pipeline `bootstrap.servers` 回归单测（防再退回字符串 join）；③ 同 V1.18.16 待办。

## V1.18.18（修复：看板 pipeline 块 `kafka_lag_probe_failed`——`list_consumer_group_offsets` 单次只支持一个消费组，旧代码一次传 6 组即时抛 ValueError）

Date：2026-10-10
变更类型：修复（V1.18.17 pipeline 修复的后续：同一探测函数内的第二个独立缺陷）

现场（V1.18.17 已重建 data-analytics 后）：events 块已恢复（`/api/v1/data/events` 全部 200），但看板仍报 `部分维度降级：pipeline`，遥测入库点/入库延迟 P95 为“kafka unavailable”。容器日志每 5s 一条 `kafka_lag_probe_failed`，且无 broker 碎片、无 SASL/超时错误。

### 根因

**非地址拆分、非 SASL、非超时**（V1.18.17 已修好 broker 地址）。真因：`services/data-analytics/app/repositories/pipeline.py` 的 `KafkaLagProbe._lag_sync` 一次性把 6 个消费组传入 `admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g) for g in groups])`，而 **confluent-kafka 2.16.0 的 `list_consumer_group_offsets` 单次仅支持一个消费组** → **即时抛** `ValueError: Currently we support listing offsets for a single consumer group only` → 被 `consumer_lag()` 的 `except` 吞成 None → dashboard 判定 `lag is None` → “kafka unavailable”。（`_dlq_sync` 正常返回 `{}`，不受影响。）因异常在客户端本地抛出、未走网络，故失败是毫秒级。`logger.warning("kafka_lag_probe_failed")` 不带异常详情，一度难以定位。

### 修复内容

**`services/data-analytics/app/repositories/pipeline.py` `_lag_sync`**：改为**逐消费组各调一次** `list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])`，取 `next(iter(futmap.values())).result(...)`；单组异常仍走原有 `consumer_group_offset_fetch_failed` 逐组 `continue`（不阻塞其余组）。所有组都失败/为空时返回 `{}`（非 None），pipeline 块不再误判为不可用。

向后兼容：仅修内部探测实现，不改 `PipelineSource` 协议/对外 API/契约；`consumer_lag()` 返回类型不变为 `dict[str, int] | None`。

风险：① **代码变更，需 `--build` 重建 data-analytics**（`restart` 无效）；② 若列出的消费组均无提交位点/不存在，`consumer_lag` 会返回 `{}`（看板不再报 kafka unavailable、但 lag 可能为空），属正常（需真实消费者产生位点才有 lag 数值）；③ 依赖 confluent-kafka 版本行为（2.16.0 实测），升级该库时需重测 API 签名。

验证：data-analytics `ruff` clean、单测 **60 passed**（看板测试走 fakes，不触真实 Kafka 探测）。**实机已确认**（2026-10-10）：`up -d --build data-analytics` 后，`KafkaLagProbe(settings)._lag_sync()` 正常返回 6 组 lag（不再抛 `ValueError`）、`kafka_lag_probe_failed` 消失、看板降级横幅消失、pipeline 块不再降级、遥测入库点出数（实机 100070）。注：“入库延迟 P95”恒为 `-` 属契约 pending 项（`dashboard.py` 写死 `ingest_latency_ms_p95=None`，待 Flink 统计作业）；“感知/控制时延均值”为 `-` 是 `data_analytics.algorithm_metrics` 窗口内无数据（数据源缺口，非本缺陷）。探测用 Consumer 可毕见一条 `GroupCoordinator: SASL authentication error ... (after 0ms in state DOWN)` 瞬时噪声，不影响 `_lag_sync` 返回结果。

**实机执行**：
```bash
cd /opt/HunterCore
sudo git pull
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics
# 验证：看板 pipeline 块不再降级；如需直接复现探测：
sudo docker exec -i hunter-analytics python - <<'PY'
from app.repositories.pipeline import KafkaLagProbe
from app.config import settings
print(KafkaLagProbe(settings)._lag_sync())
PY
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **1 个**：`services/data-analytics/app/repositories/pipeline.py`（`docs/` 下同步改动需单独 rsync 到服务器）。

**待办（不属本条范围）**：① 建议将 `consumer_lag`/`dlq_depth` 的 `logger.warning(...)` 改为带异常上下文（`logger.exception` 或 `exc_info=True`），避免同类“吞异常”难以定位；② 补 `_lag_sync` 逐组调用的回归单测（mock `list_consumer_group_offsets` 断言每组单次）；③ 同 V1.18.17 待办。

## V1.18.19（改进：看板算法块回退聚合原始遥测列（algorithm_metrics 无生产者）；Kafka 探测异常带堆栈）

Date：2026-10-10
变更类型：改进（可观测性 + 看板数据源回退；均为 data-analytics 代码变更）

背景（V1.18.18 后看板仍有的 `-`）：横幅已消失、四块均 available，但“感知帧率/感知时延/控制时延”恒为 `-`、“入库延迟 P95”恒为 `-`。排查数据链路：

### 根因（数据侧，非接线）

1. **感知/控制指标 `-`**：看板 algorithm 块读 `data_analytics.algorithm_metrics`（长表），设想的写入方是 Flink `algorithm_performance_monitor` 作业（契约/configmap 已声明）——但该作业**从未实现**（`flink-jobs/hunter_flink/` 仅有 `detection_job`），且 `AlgorithmMetricRepository.insert_metrics` **全仓无调用者** → 表恒空 → 均值全 None。而原始指标（`fps`/`latency_ms`/`planning_latency_ms`/`control_latency_ms` 等）**已在 `data_collector.vehicle_telemetry` 宽列**（实机 100070 点）。
2. **入库延迟 P95 `-`**：`dashboard.py` 写死 `ingest_latency_ms_p95=None`（契约 pending 项）。`vehicle_telemetry.time` 为车端上报时间（DDL 明确禁止用入库时间替代），库内**无入库时间戳列** → 无法从现有数据计算（需 Flink 延迟统计作业或新增 receive_ts 列）。本条**不伪造**，保持 None。

### 修复内容

1. **`services/data-analytics/app/repositories/pipeline.py` 探测异常带堆栈**：`consumer_lag`/`dlq_depth`/`_lag_sync` 逐组的 `logger.warning(...)` 改为 `logger.exception(...)`（带堆栈，下次同类“吞异常”一眼定位）；因 `logger.exception` 已满足 BLE001，相应 `# noqa: BLE001` 同步移除。
2. **`algorithm_averages` 回退聚合**：拆为 `_algorithm_averages_from_metrics`（契约源）+ `_algorithm_averages_from_telemetry`（回退源）；先查 `algorithm_metrics`，**为空时回退**到从 `vehicle_telemetry` 即时 `avg()` 聚合（列别名 `{module}__{metric}` → `module.metric_name`，与看板 `_CORE_ALGORITHM_METRIC_KEYS` 一致：perception.fps / perception.latency_ms / planning.planning_latency_ms / control.control_latency_ms，另附 detected_objects/velocity_error/steer_error 进 metrics）。将来 Flink 作业产出后自动优先用聚合表。任一查询失败仍返回 None（上层降级）。

向后兼容：`algorithm_averages` 返回类型与键格式不变（`dict[str,float] | None`），仅新增“空表回退”行为；不改对外 API/契约。探测日志级别从 warning→exception（仅日志，不改控制流）。

风险：① **代码变更，需 `--build data-analytics`**；② 回退查询与 `telemetry_points` 同为 hypertable 带时间窗聚合（已有 `idx_vehicle_telemetry_vehicle_time`），但多一次 `avg` 扫描；若窗口大、数据量大需关注 P95（看板预算 200ms）；③ 回退口径与 Flink 作业口径均为“同窗口列均值”，但 `algorithm_metrics` 可能含 `tags` 维度拆分（当前无数据、看板也不按 tags 拆），无实质差异；④ 入库延迟 P95 仍为 `-`（属预期，需单独作业）。

验证：data-analytics `ruff` clean、单测 **60 passed**（看板测试走 fakes，不触真实 SQL）。**实机待确认**：`up -d --build data-analytics` 后，“感知帧率/感知时延/控制时延”三卡出数（vehicle_telemetry 有对应列值时）；入库延迟 P95 仍 `-`。

**实机执行**：
```bash
cd /opt/HunterCore
sudo git pull
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics
# 验证回退聚合（应看到 perception.fps / perception.latency_ms / control.control_latency_ms 等非空）：
sudo docker exec -i hunter-analytics python - <<'PY'
import asyncio, time
from app.repositories.pipeline import MetricsReadOnlyRepository
from app.config import settings
r = MetricsReadOnlyRepository(settings)
end = time.time(); start = end - 86400
print(asyncio.run(r.algorithm_averages(start, end)))
asyncio.run(r.close())
PY
```

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **1 个**：`services/data-analytics/app/repositories/pipeline.py`（`docs/` 下同步改动需单独 rsync 到服务器）。

**待办（不属本条范围）**：① 实现 Flink `algorithm_performance_monitor` 作业（telemetry_clean → algorithm_metrics，含 tags 维度）后，看板自动回到契约设计源（回退分支退化为兼容）；② 入库延迟 P95：需新增入库时间戳列或 Flink 事件时间 vs 处理时间延迟统计作业；③ 补 `algorithm_averages` 回退分支单测（mock 空 metrics 表→断言走 telemetry 聚合）。

## V1.19.0（新增：两个 Flink 实时分析作业正解 —— algorithm_performance_monitor + data_quality_monitor 入库延迟统计）

Date：2026-10-10
变更类型：新增（完结 V1.18.19 待办①②：看板算法块与 pipeline 入库延迟块的真实数据源；契约先行 + 实现 + 落库/读回链路 + 单测）

本条落地契约 `x-hunter-realtime-jobs` 中此前**从未实现**的两个 Flink 作业（`detection_job` 为既有），并打通端到端数据链路：算法性能指标经 Kafka → data-analytics 消费者落库；入库延迟 P95 经 Flink → Redis 指标键 → 看板只读。纪律统一：三个作业均为「纯 Python 核心 + `main()` 延迟 import pyflink」范式，仓库单测**无需 PyFlink 运行时**。

1. **Flink `algorithm_performance_monitor` 作业（`flink-jobs/hunter_flink/algorithm_performance_job.py`，契约 6.2.4）**：消费 `telemetry_clean`，按 `ALGORITHM_METRICS_WINDOW_SECONDS`(60s) **事件时间**滚动窗口聚合感知帧率/时延、规划时延、控制时延/误差（`percentile_cont` 线性插值，与 PostgreSQL 口径一致），投递 Kafka Topic `algorithm_metrics`（载荷严格对齐 `algorithm_metrics.schema.json`，字段 = 表列）。核心函数 `percentile_cont` / `aggregate_metrics` / `expand_metrics`（按车分桶、跨窗口边界才冲刷 tumbling 语义）。G-13：Flink 不直连业务库，仅投 Kafka。
2. **data-analytics `algorithm_metrics` 落库消费者（契约 consumer-groups `data-analytics-algorithm-metrics`）**：新增 `app/consumers/algorithm_metrics.py`（`KafkaConsumerManager`，组 `data-analytics-algorithm-metrics`，幂等键 `(vehicle_id,time,module,metric_name)`，`schema_name=auto` 契约校验，`on_batch_end` 批量落库并「抛错→跳过 commit→重投」）、`app/services/algorithm_metrics_ingest.py`（`to_row` 严格校验：module 受控枚举 / metric_value 有限浮点排除 bool / time epoch 秒↔13 位毫秒归一；缓冲达阈值触发 flush，异常回填 buffer 保消息不丢）、`app/repositories/algorithm_metrics.py`（`AlgorithmMetricsWriter` 复用 `AlgorithmMetricRepository.insert_metrics` → `ON CONFLICT DO NOTHING` 写 `data_analytics.algorithm_metrics`，Y10 只约束跨 schema 读/探针，写自身 schema 合法）。`main.py` lifespan 初始化 `DatabaseSessionManager`(RW) + 装配消费者（`_SupervisedConsumer` 崩溃退避重建 + 优雅停机，停机顺序先停消费者再关资源）；`config.py` 新增 6 个消费者开关/阈值项。看板 algorithm 块自此优先读契约设计源（V1.18.19 的 telemetry 回退分支退化为兼容）。
3. **Flink `data_quality_monitor` 入库延迟统计作业（`flink-jobs/hunter_flink/ingest_latency_job.py`，契约 6.2.5「延迟」子项）**：消费 `telemetry_raw`（组 `data-analytics-telemetry-raw`），按车端事件时间 60s 滚动窗口统计「Kafka 到达时间 − 车端 timestamp」车队级 P95（`percentile_cont`），写 Redis 指标键 `analytics:ingest_latency`（`SET ... EX 300`，作业停摆即过期）。核心 `latency_ms`（负延迟=时钟超前**丢弃不伪造**、非有限丢弃）/ `aggregate_latency` / `expand_latency`（车队全局按 window_id 缓冲）/ `encode_redis_value`；窗口用独立环境变量 `INGEST_LATENCY_WINDOW_SECONDS`（默认 60，与告警阈值解耦）。**契约 pending #10 结案路线**：不新增 DB 列/表（回退此前 Part B 的 `ingested_at` 列方案，迁移 0005 已删除）。
4. **看板 pipeline 块读 Redis（`app/repositories/pipeline.py` + `app/services/dashboard.py` + `app/core/dependencies.py`）**：新增 `IngestLatencyRedisReader`（只读 `analytics:ingest_latency`，键缺失/Redis 不可用/JSON 不可解析/p95_ms 非数值 → None，绝不展示陈旧值）；`PipelineRepository(settings, *, redis=None)` 暴露 `ingest_latency_p95()`；`DashboardService._pipeline_block` 并收发 4 项（latency 为**可选指标**，不计入块降级理由）；依赖工厂从 `app.state.redis.client` 注入（Redis 未就绪降级 None）。
5. **契约同步（先契约后实现）**：`contracts/database/redis-keys.yaml` 新增第 **9** 个受控键 `analytics:ingest_latency`（String(JSON)/TTL 300/owner+writers+readers=data-analytics，登记 pending #9：派生指标键待正式纳入约束第 8 条）；`contracts/openapi/data-analytics.yaml`：`x-hunter-service.redis_keys.read` 加入该键、`data_quality_monitor` 增 `side_output`（Redis 指标键）、`dashboard-contract.pipeline.ingest_latency` 与 `sources` 结案 pending #10、`response_semantics.cache` 澄清「响应缓存 vs 数据源指标键」、pending #10 标记【已结案】；`scripts/verify_data_layer.py` 与 `common/python/tests/test_storage_contracts.py` 受控键集 8→9（校验 9 现比对 **9 键 ↔ 6 服务**，全绿）。
6. **测试**：新增 `flink-jobs/tests/test_ingest_latency.py`（延迟口径/负值丢弃/车队 P95 线性插值/tumbling 冲刷/毫秒归一/编码往返）与 `app/tests/test_ingest_latency_reader.py`（Redis 只读 6 路径降级）；算法作业单测 `test_algorithm_performance.py` 与消费者单测 `test_algorithm_metrics_consumer.py`（a2/a3）；`test_dashboard_endpoints.py` 增入库延迟透出断言；契约测试 `test_data_analytics_contract.py` 更新 redis_keys 断言。回退 V1.18.19 待办③：`algorithm_averages` 回退分支此前已具备（本条未改其行为）。

验证（全量、无外部基础设施）：`python scripts/run_unit_tests.py` **9 个目标全部通过**（common/python **323**、flink-jobs **34**、api-gateway **71**、scene-service **108**、data-collector **143**、data-analytics **89**、ota-service **128**、remote-control **97**，L5 单元层无容器用例 skip）；`python scripts/verify_data_layer.py` **34 项全通过**（含 Redis **9 受控键** 交叉校验、data-analytics 消费者组契约）；`common/python/tests/test_storage_contracts.py` **9 passed**；`ruff check`（本次改动文件：`scripts/verify_data_layer.py` + `common/python/tests/*` + `services/data-analytics/app`）**All checks passed**，本会话**零新增** ruff 告警（仓库既有 50 项位于未改动的 `hunter_common` 泛型/vehicle-service 测试文件，属版本判定漂移、非本次引入，未处理）。

风险与待人工确认：① 两个作业为纯核心 + 延迟 import 范式，**PyFlink 提交入口 `main()` 不在仓库单测覆盖**（`# pragma: no cover`），入库延迟的 Kafka 记录到达时间解码（`_arrival_watermark_strategy`）为部署侧占位，真实实现需自定义 `KafkaRecordDeserializationSchema`，上线前须随 flink-jobs 部署链路（当前 `flink-jobs` 未入 CI lint/未编目部署）单独验证；② `algorithm_metrics` 落库需为 data-analytics 初始化 **RW** `DatabaseSessionManager`（此前只读账号 `hunter_analytics_ro` 无写权限），生产需确认写账号最小权限（仅 `data_analytics.algorithm_metrics` INSERT，对齐 Y10/consumer-groups `writes` 声明）；③ Redis 指标键 `analytics:ingest_latency` 属约束第 8 条之外的派生键（redis-keys pending #9，与 `rc:lock` 同类），键模式一经冻结改名会造成 Flink 写方与看板读方拼写漂移（该字段恒降级 null），正式纳入约束第 8 条前不进「键模式冻结」；④ 车队级 P95 采用 keyBy 常量全局缓冲，超大车队规模下需改近似分位聚合并回改契约；⑤ 契约固定「Kafka 时间戳差」估算口径与 Prometheus 查询路线二选一的取舍已按 pending #10 结案，若运维更倾向 Prometheus 需回退本路线并改契约。

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件：`flink-jobs/hunter_flink/algorithm_performance_job.py`、`flink-jobs/hunter_flink/ingest_latency_job.py`、`flink-jobs/tests/*`、`contracts/database/redis-keys.yaml`、`contracts/openapi/data-analytics.yaml`、`scripts/verify_data_layer.py`、`common/python/tests/test_storage_contracts.py`、`services/data-analytics/app/**`（config/main/consumers/repositories/services/schemas/core/tests）。`docs/` 下同步改动（运维手册/故障排查/部署文档/data-layer）需单独 rsync 到服务器。

## V1.19.1（改进：flink-jobs 纳入 CI lint 与 infra/k8s 部署清单；data-analytics RW 最小权限写账号；受控键集第 8 条统一单一常量）

Date：2026-10-10
变更类型：改进（**逐项完结 V1.19.0「风险与待人工确认」①②③**：flink-jobs 部署编目 + CI lint / RW 写账号最小权限 / 受控键单一事实来源；不改业务写读逻辑，向后兼容）

背景：V1.19.0 交付两个 Flink 作业后留下三项「待人工确认」——① flink-jobs 未入 CI lint、无 k8s 部署清单（仅 compose 受控降级）；② `algorithm_metrics` 落库消费者此前复用共享超级用户写、无最小权限 RW 账号；③ 派生键 `analytics:ingest_latency` 未正式纳入约束第 8 条、Flink 写端与看板读端各写各的字面量、拼写漂移会导致看板该字段恒降级 null。本条把三项一次性收敛为受契约与 CI 校验的结构化实现。

### 修复内容

1. **任务 1a · flink-jobs 纳入 CI lint（`.gitlab-ci.yml` lint 阶段）**：`ruff check` 路径由 `tests common/python services` 扩为 `… services flink-jobs`。本会话仅有一处真实违规需修——`flink-jobs/hunter_flink/ingest_latency_job.py` 引入共享常量后把第三方 `hunter_common` 与第一方 `hunter_flink` 挤进同一 import 组，破坏仓库既有 ruff isort 分组约定（对齐同目录 `algorithm_performance_job.py` 的分组写法）→ 恢复二组间空行。修后 `ruff check flink-jobs`（默认与 `--isolated`）均 **All checks passed**；`flink-jobs/README.md` 纪律段补「已纳入 CI lint」。注：本地 ruff 0.16.7 二进制对既有 services 代码报 UP/SIM/RUF100 等聚合告警属该二进制在本机的配置解析漂移（`--isolated` 不复现、且命中大量未改动文件），非本次引入、CI 默认规则集不含之，未处理。
2. **任务 1b · flink-jobs infra/k8s 部署清单（Option C：代码内置镜像，单一事实来源、杜绝 ConfigMap 内联源码漂移）**：
   - `flink-jobs/Dockerfile`：`FROM flink:1.18.1-scala_2.12-java17`（与 compose 单机同版本）+ 补装 python3 与 `apache-flink==1.18.1`、`pip install` 共享库 `hunter_common`（作业经 `hunter_common/__init__` 传递依赖 sqlalchemy/redis/confluent-kafka）、把 `hunter_flink` 内置到 `PYTHONPATH`。**镜像前缀必须为 `hunter-flink/`、禁止 `hunter/`**：`verify_infra.check_version_placeholder` 与 `render_k8s`（OWN_IMAGE_PATTERN/EXPECTED_IMAGES）的「6 微服务模块表不可新增」只放行 `hunter/<svc>`，`hunter/flink-jobs` 会判「未登记的微服务镜像」致 lint 失败；独立前缀不参与 `{version}` 渲染，标签随 Flink 运行时版本管理。
   - `infra/k8s/flink/`：`01-configmap.yaml`（`flink-common-config`：FLINK_PROPERTIES + 作业连接项 `KAFKA_BOOTSTRAP_SERVERS`/`REDIS_URL` + 窗口/TTL，无任何敏感键）· `02-jobmanager.yaml`（JM **Deployment** + **Service `flink-jobmanager`**，补齐 `data-analytics` ConfigMap 中此前**悬空**的 `FLINK_JOBMANAGER_URL: flink-jobmanager:8081`）· `03-taskmanager.yaml`（TM Deployment，2 副本）· `04-submit-job.yaml`（**batch/v1 Job**，等待 JM REST 后 `flink run -d -m flink-jobmanager:8081 -pym hunter_flink.{detection,algorithm_performance,ingest_latency}_job` 提交三个已交付流作业）。JM/TM 为 Deployment → 严格满足 `check_workloads`（显式版本 / requests+limits / liveness+readiness / 容器 drop ALL + allowPrivilegeEscalation=false / Pod runAsNonRoot+runAsUser 9999）；提交 Job 为 batch/v1 → 免探针/资源强校验，仍按最小权限书写。
   - `infra/k8s/networkpolicies/flink.yaml`：集群有 `hunter-default-deny-ingress`，显式放行 `component=flink` 内部互连（rpc 6122/6123、blob 6124、data 6121、rest 8081）与 `data-analytics`→JM:8081；作业出向访问 Kafka/Redis 不受 Ingress 限制、无需 egress 规则。
   - CI 接线：`build-images` 增 flink 构建（推 `hunter-flink/jobs:1.18.1`）；`deploy-prod` 在 `kafka-init` 之后、`services/` 之前 apply flink 集群 → `rollout status` JM/TM → 提交 Job → `wait job/flink-job-submitter` → 再拉起依赖 JM 的 data-analytics。
3. **任务 2 · data-analytics RW 最小权限写账号**：`app/config.py` 新增 `analytics_rw_db_user/password` 并重写 `database_url`（配置了 `ANALYTICS_RW_DB_USER` 走最小权限写账号、否则回落基类共享 `POSTGRES_USER`；`read_only_database_url` 不受影响）；`infra/deploy/sql/analytics-roles.sql`（幂等 `DO` 建 `hunter_analytics_ro/rw` + **顶层** `ALTER ROLE … PASSWORD :'var'`（psql 仅在 dollar-quote 外才对 `:'var'` 插值）+ 最小权限：`rw` 仅 `USAGE ON SCHEMA data_analytics` + `INSERT ON algorithm_metrics`，`REVOKE` 其余，禁 UPDATE/DELETE/DDL/跨 schema）；`scripts/init-db.sh` 步骤 4b 门禁（`.env` 配齐 RO+RW 两口令才 apply，单机默认不配则跳过、复用超级用户）；k8s `data-analytics.yaml` ConfigMap `ANALYTICS_RW_DB_USER` + `02-secret.example.yaml` `ANALYTICS_RW_DB_PASSWORD` + compose env（默认回落）+ `.env.example` 可选加固说明；契约 `db_access.write` 登记 `hunter_analytics_rw`（**写自身 schema、非跨库例外**）、`er.md §3` 附注；测试 `test_config_rw_database.py`（回落/优先 RW/RO 独立）+ `test_data_analytics_contract.py` rw 断言。
4. **任务 3 · 受控键单一常量、正式纳入约束第 8 条**：新增 `common/python/hunter_common/redis_keys.py`——9 个键模式常量（逐字镜像 `redis-keys.yaml pattern`）+ `CONTROLLED_PATTERNS: frozenset` + 占位符构造函数（`session_key`/`vehicle_status_key`/…），纯字符串、不依赖 redis 客户端/DB/config，可被无运行时的 Flink 安全 import。`services/data-analytics/app/repositories/pipeline.py` 的 `IngestLatencyRedisReader.KEY` 与 `flink-jobs/hunter_flink/ingest_latency_job.py` 的 `REDIS_KEY` 默认值均改为引用同一 `redis_keys.ANALYTICS_INGEST_LATENCY`，**结构性杜绝跨端拼写漂移**（漂移后果即看板 `ingest_latency_ms_p95` 恒 null）。`contracts/database/redis-keys.yaml`：`key_builder.constants_module` 落 `hunter_common/redis_keys.py` 并 `status: resolved`、`analytics:ingest_latency` 删 `pending_confirmation`、`cross_check.runtime` 增该模块、pending **#7/#9 标【已结案】**（保留 4 字段结构，不破 `test_storage_contracts` 的 track_open_conflicts 校验）；`test_storage_contracts.py` 断言 `CONTROLLED_PATTERNS == 契约 keys[].pattern 集合` + 各构造函数产键逐条相等，防契约与代码漂移。

向后兼容：任务 1a 仅加宽 CI lint 路径 + 修一处 import 分组（无行为变更）；任务 1b 为**新增** k8s 形态，compose 单机受控降级形态保留不变（README/deployment.md 两形态并存）；任务 2 `database_url` 仅在显式配置 `ANALYTICS_RW_DB_USER` 时切换，单机默认仍回落超级用户、SQL 门禁未配口令即跳过（不改现有部署行为）；任务 3 键字面值不变（`analytics:ingest_latency`），仅把两端硬编码收敛为共享常量引用，读写行为零变化。

风险与待人工确认：① **本环境无法 `docker build` PyFlink 镜像、无 k8s 集群 apply**——flink k8s 清单仅经静态校验（`verify_infra` + `render_k8s --check` 全绿），真实上线须确认：`hunter-flink/jobs` 镜像仓库推送凭据、`flink run -pym` 在共享镜像下的模块可见性、JM 高可用与 checkpoint 持久卷（现为 `emptyDir`）；② 作业侧 Kafka **SASL 凭据注入路径未打通**——flink-jobs 代码当前仅读 `KAFKA_BOOTSTRAP_SERVERS`/`REDIS_URL` 走 broker 内部监听，k8s 若强制 SASL_SSL 需先给作业加凭据读取路径再改 `flink-common-config`（本条未静默改数据面安全口径）；③ `analytics-roles.sql` 需在生产启用 RW 前先跑、口令经 Secret 注入；单机 compose 默认仍复用超级用户；④ 本地 ruff 二进制对既有未改动 services 代码的 UP/SIM/RUF100 聚合告警为该机配置解析漂移（`--isolated` 不复现），不属本条、未处理，若后续统一治理另开条目。

验证（全量、无外部基础设施）：`python scripts/run_unit_tests.py` **9 个目标全部通过**（common/python **324**、flink-jobs **35**、api-gateway **71**、scene-service **108**、data-collector **143**、data-analytics **93**、ota-service **128**、remote-control **97**）；`scripts/verify_infra.py` **全部通过**（91 个 K8s 文档，含新增 flink JM/TM Deployment 探针/资源/安全、flink Service 名 `flink-jobmanager`、flink 提交 Job、flink 网络策略；6 微服务模块表不变量仍绿）；`scripts/render_k8s.py --check` **通过**（6 `hunter/*` 镜像 = `{version}`，flink 独立前缀不参与渲染）；`scripts/verify_data_layer.py` **34 项全通过**（含 **9 受控键**交叉校验）；`scripts/verify_test_layer.py` + `generate_contracts_configmap.py --check` 通过；`.gitlab-ci.yml` YAML 合法且 flink lint/build/deploy 接线断言齐备；定向 `ruff check`（本条全部改动文件：flink-jobs/ingest_latency_job.py、redis_keys.py、data-analytics config.py/pipeline.py）**All checks passed**。

分发路径注意（仓库规则，`.gitignore` 尾段）：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件：**任务1** `.gitlab-ci.yml`、`flink-jobs/Dockerfile`、`flink-jobs/hunter_flink/ingest_latency_job.py`、`flink-jobs/README.md`、`infra/k8s/flink/*.yaml`、`infra/k8s/networkpolicies/flink.yaml`；**任务2** `services/data-analytics/app/config.py`、`services/data-analytics/app/tests/test_config_rw_database.py`、`services/data-analytics/app/tests/test_data_analytics_contract.py`、`infra/deploy/sql/analytics-roles.sql`、`infra/deploy/docker-compose.yml`、`infra/deploy/.env.example`、`scripts/init-db.sh`、`infra/k8s/services/data-analytics.yaml`、`infra/k8s/base/02-secret.example.yaml`、`contracts/openapi/data-analytics.yaml`、`contracts/database/er.md`；**任务3** `common/python/hunter_common/redis_keys.py`、`services/data-analytics/app/repositories/pipeline.py`、`flink-jobs/hunter_flink/ingest_latency_job.py`、`contracts/database/redis-keys.yaml`、`common/python/tests/test_storage_contracts.py`、`flink-jobs/tests/test_ingest_latency.py`、`services/data-analytics/app/tests/test_ingest_latency_reader.py`。`docs/` 下同步改动（deployment.md / 01-部署概述 / 运维操作手册 / 故障排查指南）需单独 rsync 到服务器。

**待办（不属本条范围）**：① flink k8s 上线实机验证（镜像推送凭据、`-pym` 模块可见性、JM HA / checkpoint PVC 持久化）；② flink-jobs 作业侧 Kafka SASL 凭据读取路径（打通后方可在 k8s 强制 SASL_SSL）；③ 若引入 Flink Application Mode / Flink Operator（`flink.apache.org` CRD），需扩 `verify_infra` apiVersion 白名单后另开条目；④ 本地 ruff 二进制聚合告警（UP/SIM/RUF100）如需统一治理应锁定 CI 与本地同一 ruff 版本并显式声明 `[tool.ruff.lint] select`。

## V1.19.2（修复：compose 形态 Flink 容器无 Python，PyFlink 作业无法提交；附实机部署路径纠偏）

Date：2026-10-10
变更类型：修复

现场（服务器 `/opt/HunterCore` 执行 V1.19.1 部署实录）：① `docker exec hunter-flink-jm flink run -d -m localhost:8081 -pym hunter_flink.<job>` 三作业全部报 `ProgramAbortException: Cannot run program "python": No such file or directory`；② 按上轮指引在容器内 `pip install` 报 `pip: command not found`（`docker cp common/python` 成功但无从安装）；③ `analytics-roles.sql` 重定向报「没有那个文件或目录」；④ 非 sudo `docker compose` 报 `.env: permission denied`（符合预期，sudo 后 data-analytics 重建启动成功）；⑤ 单机服务器上误执行 K8s 形态命令（`render_k8s --version $CI_COMMIT_TAG` 空参、`kubectl` 无集群报 `could not find the requested resource`）——属操作面错位，无代码改动。

根因：
1. **compose 形态 flink-jm/flink-tm 仍用官方 `flink:1.18.1-scala_2.12-java17` 镜像（纯 JVM，无任何 Python）**，而三个流作业（含 V1.19.0/V1.19.1 交付的 `algorithm_performance_job`/`ingest_latency_job`）是 PyFlink 且依赖 `hunter_common`——`-pym` 提交必挂。V1.19.1 的「代码内置镜像」只接了 K8s 清单与 CI，compose 形态漏切（上轮部署指引中「容器内 pip 补环境」的方案也未经验证，实际连 pip 都没有）。
2. V1.19.1 的 `flink-jobs/Dockerfile` 装了 `python3` 但未建 `python` 软链——`flink run -pym` 客户端执行的解释器名取自 config `python.client.executable`（默认字面量 `python`），Debian 仅提供 `python3`，即使换用内置镜像提交仍会撞同一错误（预防性修复）。
3. 路径笔误：`analytics-roles.sql` 头注释与上轮指引写 `/opt/hunter-core/sql/...`，实际部署目录为 `/opt/HunterCore`（大小写）且 sql 在仓库内 `infra/deploy/sql/` 下（`<` 重定向由宿主 shell 打开，需宿主机真实路径）。

修复内容：
1. `infra/deploy/docker-compose.yml`：flink-jm/flink-tm `image` 由官方镜像切为 **`hunter-flink/jobs:1.18.1`** 并各加 `build: {context: ., dockerfile: flink-jobs/Dockerfile}`——与 K8s 形态同一镜像、单一事实来源；`./flink-jobs:/opt/flink/jobs:ro` 挂载保留作应急热替换；前缀 `hunter-flink/` 不触 6 微服务模块表（compose 文件不在 render/verify 扫描范围，镜像命名规范与 K8s 侧一致）。
2. `flink-jobs/Dockerfile`：apt 段同层补 `ln -sf /usr/bin/python3 /usr/local/bin/python` 并以 `python --version` 自验——同时修正本条现场问题②与 K8s 形态 `04-submit-job` 的同类隐患（`-pym` 默认找 `python`）。
3. `infra/deploy/sql/analytics-roles.sql` 头注释：执行示例路径改为 `/opt/HunterCore/infra/deploy/sql/...`，并警示「占位符需换真实口令，shell 单引号内 `<...>` 会被当作字面量」。
4. 文档同步：`flink-jobs/README.md`（提交命令改 `-pym` + 镜像构建步骤，废弃 zip 内联提交写法；部署形态②标注两形态同镜像）、`docs/运维操作手册.md` §3.6 compose 段重写（sudo + `up -d --build --force-recreate flink-jm flink-tm` + `-pym` 三连提交）、`docs/deployment.md` §12 豁免表现状行与 §13.1 尾注、`docs/故障排查指南.md` 场景 13 新增「Cannot run program "python"/pip not found」陷阱段（含 SASL_PLAINTEXT 拒绝的区分判据：作业反复 FAILED/broker `Authentication failed` 属 V1.19.1 风险②待打通项，非镜像问题）。

向后兼容：compose 仅改 flink 两服务的镜像来源（同版本 Flink 运行时、同 entrypoint/端口/healthcheck/UI 8088）；未提交作业时 flink 容器照常运行不影响其它服务；旧官方镜像可回（改回 image 行即兑）。`analytics-roles.sql` 仅改注释，执行语义零变化。

风险与待人工确认：① 本环境无 Docker，**未能在服务器外先行验证 `hunter-flink/jobs:1.18.1` 实际构建**——服务器首次 `build` 需拉 `flink:1.18.1` 基础镜像（本机已有，命中缓存）+ pip 装 apache-flink（百 MB 级，已配清华源；超时按 `pip 构建超时处理惯例` 重试）；② 提交后作业连 Kafka 若被拒（broker 内部监听 SASL_PLAINTEXT + SCRAM，作业无凭据路径，V1.19.1 风险②），属已知待打通项，处置路线见故障排查指南场景 13 新增段；③ 单机服务器不是 K8s 集群，`infra/k8s/flink/` 与 `render_k8s`/`kubectl` 相关步骤不应在该机执行（snap 装的 kubectl 可留可卸）。

验证（本环境可验证部分）：`pyyaml` 解析 compose 断言 flink-jm/tm 的 `image`/`build`/healthcheck 均正确（临时脚本 [ALL PASS] 后已删）；`scripts/verify_infra.py` 重跑全部通过（flink 镜像前缀/清单不受影响）；本条无 Python 代码变更（仅 Dockerfile/compose/SQL 注释/文档）。

分发路径注意：`docs/` 与 `release.md` 只更新、不入库。需入库并走 git 同步的受追踪文件为 **4 个**：`infra/deploy/docker-compose.yml`、`flink-jobs/Dockerfile`、`infra/deploy/sql/analytics-roles.sql`、`flink-jobs/README.md`（`docs/` 下同步改动需单独 rsync 到服务器）。

**服务器续跑步骤（本条落地后）**：`git pull` → `sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build --force-recreate flink-jm flink-tm`（等 healthy）→ 逐个 `sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec flink-jm flink run -d -m localhost:8081 -pym hunter_flink.{detection,algorithm_performance,ingest_latency}_job` → `flink list` 确认 RUNNING → 若 RW 加固：`docker exec -i hunter-postgres psql -U hunter -d hunter_core -v ON_ERROR_STOP=1 -v analytics_ro_password='<真实RO口令>' -v analytics_rw_password='<真实RW口令>' < /opt/HunterCore/infra/deploy/sql/analytics-roles.sql` 后 `.env` 注入口令再 `up -d data-analytics`。验证矩阵沿用前条（health-check / 看板出数 / Redis 键 TTL / 角色负向测试）。

**待办（不属本条范围）**：① 作业侧 Kafka SASL 凭据注入路径（同 V1.19.1 待办②，本条现场未触达即被镜像问题拦截）；② flink-jobs 镜像在服务器上的首次实机构建与三作业 RUNNING/出数的实机确认（按本条续跑步骤执行后回填）。

## V1.19.3（修复：.dockerignore 残留 L1 裁剪排除 flink-jobs/，作业镜像首次实机构建 COPY 失败）

Date：2026-10-10
变更类型：修复

现场（服务器续跑 V1.19.2 步骤①）：`sudo docker compose ... up -d --build --force-recreate flink-jm flink-tm` 失败于 `ERROR [flink-tm 5/6] COPY flink-jobs/hunter_flink /opt/hunter/jobs/hunter_flink → "/flink-jobs/hunter_flink": not found`（failed to compute cache key）。基础镜像拉取与 apt/pip 层（CACHED 2-4/6）均正常，build context 仅传 4.87kB。

根因：根目录 `.dockerignore` 仍保留 L1 时期的裁剪条目 `flink-jobs`（当时决策「服务镜像仅需 common/python + services/<service>」，且无任何镜像需要从仓库根上下文 COPY flink-jobs/）。V1.19.1 新增的 `flink-jobs/Dockerfile` 需要 `COPY flink-jobs/hunter_flink`，但其时仅经静态校验（release V1.19.1 风险①已如实登记「无法 docker build、需上线实机确认」），首次实机构建即暴露。

修复内容：`.dockerignore` 摘除 `flink-jobs` 条目，原位留「禁止再排除」注（compose 与 K8s 共用本镜像、需 COPY 作业代码）；其余裁剪（contracts/spark-jobs/infra/scripts 等）不变；`**/__pycache__` 已全局排除，入上下文无噪声。服务镜像仅多携带几百 KB flink-jobs 源文件（不 COPY 不进镜像，无行为影响）。同步 `docs/deployment.md` §2 PyFlink 镜像段补上下文约束注。

向后兼容：仅放宽构建上下文裁剪，不改任何镜像内容与运行行为。

风险与待人工确认：本环境无 Docker，修复本身需在服务器实机验证（本条预期同 V1.19.2 续跑步骤一并闭环）。

验证：`grep` 确认 `.dockerignore` 不再命中 `flink-jobs`（仅余注释中的禁改警示）；`spark-jobs/README.md` 等其余裁剪目标不受影响；deployment.md 同步段落位。实机验证点：重新 `up -d --build --force-recreate flink-jm flink-tm` 应构建成功且两容器 healthy。

分发路径注意：受追踪文件 **2 个**：`.dockerignore`、`docs/deployment.md`（docs 不入库，需单独 rsync；本条运行依赖仅 `.dockerignore`）。

**待办（不属本条范围）**：同 V1.19.2（实机三作业 RUNNING/出数确认与 Kafka SASL 路径）。

## V1.19.4（修复：作业镜像基座 apt python3=3.10 与 hunter_common requires-python>=3.11 冲突，PyFlink 运行期 ModuleNotFoundError）

Date：2026-10-10
变更类型：修复

现场（服务器续跑 V1.19.3 后首次 `-pym` 提交）：`flink run -d -m localhost:8081 -pym hunter_flink.<job>` 三作业（detection / algorithm_performance / ingest_latency）齐报 `ModuleNotFoundError: No module named 'hunter_common'`，traceback 顶帧为 `File "/usr/lib/python3.10/runpy.py"`——即容器内实际解释器是 **apt 安装的 Python 3.10**（`/usr/lib/python3.10`），`hunter_flink` 自身经 PYTHONPATH 可见、唯独 `hunter_common` 缺失；`flink list` 显示 `No running jobs`（提交即失败，无残留）。

根因：`common/python/pyproject.toml` 声明 `requires-python = ">=3.11"`，且 `hunter_common/database/enums.py` 使用 `enum.StrEnum`（Python 3.11+ 才有的标准库成员）。V1.19.2/V1.19.3 的 `flink-jobs/Dockerfile` 走 `apt-get install python3` 取解释器，而 flink 基座镜像（`flink:1.18.1-scala_2.12-java17`，Ubuntu 22.04 jammy）的 apt `python3` = **3.10**：pip 受 Requires-Python 门禁拒装 hunter_common（未落入 3.10 的 site-packages），运行期 import 即缺模块。属镜像内容缺陷，与 compose/K8s 编排、`.dockerignore` 上下文裁剪（V1.19.3）无关——V1.19.3 打通构建后才暴露此下一层问题。

修复内容：`flink-jobs/Dockerfile` 弃用 apt python，改**多阶段自 `python:3.11-slim-bullseye` 拷入自包含 CPython 3.11**（`FROM python:3.11-slim-bullseye AS pyrel` → `COPY --from=pyrel /usr/local /usr/local`，整棵带入 bin/python3.11、pip、libpython3.11.so 与编译版 OpenSSL，不依赖宿主 libssl；bullseye glibc 2.31 < 基座 jammy 2.35 前向兼容）；`ln -sf python3 /usr/local/bin/python` 保证 `-pym` 客户端（config 键 `python.client.executable`/`python.executable` 默认字面量 `python`）解析到 3.11。apt 层仅保留 `ca-certificates`。**新增两道构建期自检**（把运行期故障前移为构建失败）：apt 层尾 `python --version && pip --version && python -c "import ssl"`；pip 层尾 `python -c "import pyflink, hunter_common.database.enums, hunter_common.redis_keys"`。apache-flink 1.18.1 提供 cp311 轮子，与 3.11 兼容。同步 `infra/deploy/docker-compose.yml` flink 段注、`docs/deployment.md` §2 PyFlink 镜像段与「镜像 Python 固定 3.11」硬约束注。

向后兼容：compose 与 K8s 共用同一镜像标签 `hunter-flink/jobs:1.18.1`，仅镜像内 Python 由 3.10（apt）换 3.11（自包含），不改编排、作业代码与任何对外契约。K8s `04-submit-job` 同镜像同受益（此前亦有同类 3.10 隐患）。

风险与待人工确认：① 本环境无 Docker，多阶段拷入的 CPython 3.11 与 jammy 基座的动态库兼容需在服务器实机 build 确认（`import ssl` 自检若失败即 build 红、不会产出坏镜像）；② 首次重建会**失效旧 apt/pip 缓存层**、重新拉取 `python:3.11-slim-bullseye` 阶段并向 3.11 重装 apache-flink（百 MB 级），构建耗时数分钟，属正常；③ 越过本坎后预期触达 V1.19.1/V1.19.2 遗留的 Kafka SASL_PLAINTEXT 内部监听 vs 作业明文连接问题（另判据见故障排查指南场景13），非本条范围。

验证：本环境静态核对——Dockerfile 两段式结构、软链、两道自检 import 语句齐备；`requires-python`/`StrEnum` 经 read/grep 确认为 3.11+；compose/deployment.md V1.19.4 注落位。实机验证点（服务器）：`up -d --build --force-recreate flink-jm flink-tm` 后 `sudo docker exec hunter-flink-jm python --version` 应输出 `Python 3.11.x`、`python -c "import hunter_common.database.enums"` 无错；三作业 `-pym` 提交后 `flink list` 见 RUNNING。

分发路径注意：受追踪文件 **3 个**：`flink-jobs/Dockerfile`、`infra/deploy/docker-compose.yml`、`docs/deployment.md`（docs 不入库、需单独 rsync；运行依赖仅前二者）。`release.md`、`docs/故障排查指南.md` 仅更新不入库。

服务器续跑步骤（本条修复依赖重新构建镜像，缓存层必须失效）：
```bash
# 本地：提交推送受追踪改动
git add flink-jobs/Dockerfile infra/deploy/docker-compose.yml && git commit -m "V1.19.4: 作业镜像改自包含 CPython 3.11，修复 hunter_common 运行期 ModuleNotFoundError" && git push

# 服务器：拉取后强制重建（--no-cache 确保旧 3.10 apt/pip 层不被复用）
cd /opt/HunterCore && git pull
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build --no-cache flink-jm
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate flink-jm flink-tm
# 先验解释器再提交（关键判定：必须是 3.11）
sudo docker exec hunter-flink-jm python --version
sudo docker exec hunter-flink-jm python -c "import hunter_common.database.enums, hunter_common.redis_keys; print('hunter_common OK')"
# 三连提交
for j in detection algorithm_performance ingest_latency; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec \
    flink-jm flink run -d -m localhost:8081 -pym hunter_flink.${j}_job
done
sudo docker exec hunter-flink-jm flink list
```

**待办（不属本条范围）**：① 作业侧 Kafka SASL 凭据注入路径（V1.19.1/V1.19.2 遗留，越过本坎后的下一道判据）；② 三作业实机 RUNNING/出数与看板 `ingest_latency_ms_p95`、Redis 键 `analytics:ingest_latency`（GET/TTL≤300s）核验后回填本条。

**勘误（由 V1.19.5 更正本条两处想当然，保留原文以备溯源）**：

- 本条「apache-flink 1.18.1 提供 cp311 轮子，与 3.11 兼容」**有误**：实查 PyPI Linux wheel，1.18.1 只发 cp37/cp38/cp39/**cp310**（无 cp311；1.20.x 才到 cp311，2.1.x 才到 cp312）。即使过了 OpenSSL 这一关，3.11 路线也会在 pip 层无匹配轮子。
- 本条「整棵带入……编译版 OpenSSL，不依赖宿主 libssl」**有误**：CPython 的 `_ssl` 扩展仍**动态链接构建时所在发行版**的 OpenSSL——bullseye 链接 `libssl.so.1.1`，而 flink 基座（jammy）只有 `libssl.so.3`；服务器 build 实录 `ImportError: libssl.so.1.1: cannot open shared object file`（Dockerfile 自检层失败）。本条风险段只写到“需实机 build 确认”，未识别出必然失败。
- 结论：本条的 **3.11 方案整体由 V1.19.5 取代**（Flink 2.1.1 + CPython 3.12 + `python:3.12-slim-bookworm` pyrel）。本条仍然成立的只有“必须弃用 apt python3=3.10、改自包含 CPython 并加构建期自检”这一条。

## V1.19.5（修复：作业运行时升 Flink 2.1.1 + CPython 3.12 与服务基线对齐；内置 DataStream Kafka 连接器 jar；2.x 配置键迁移；契约引擎版本连带）

Date：2026-10-10
变更类型：修复（含运行时大版本升级与契约文案连带，**有破坏性变更**）

现场：服务器执行 V1.19.4 指引 `build --no-cache` —— pyrel 拷入后 apt 层自检前半段正常（`Python 3.11.13`、`pip 24.0 ... /usr/local/lib/python3.11/site-packages`），紧随其后的 `python -c "import ssl"` 报 `ImportError: libssl.so.1.1: cannot open shared object file: No such file or directory`（顶帧 `File "/usr/local/lib/python3.11/ssl.py", line 100`），构建在 `Dockerfile:25` 失败。同时用户指出“系统是基于 python3.12 构建的”（已核：六个服务 Dockerfile 均 `FROM python:3.12-slim`，V1.16.1 迁移；`docs/deployment.md` §2 旧表的 3.11-slim + distroless 为过期记录）。

根因（三层叠加）：① V1.19.4 的 pyrel 取 `python:3.11-slim-bullseye`，其 `_ssl` 扩展链接 bullseye 的 `libssl.so.1.1`，jammy 基座只有 `libssl.so.3` → 扩展加载即失败（新增自检把它拦在 build 阶段，未产出坏镜像）；② 更根本的是**三元互斥**（查 PyPI 而非凭印象）：`apache-flink 1.18.1` 只发 cp37–cp310、`1.20.x` 到 cp311、**只有 2.1.x 发 cp312**；`hunter_common` 下限 3.11（`database/enums.py` 用 `enum.StrEnum`）；服务基线 3.12 ——所以下限 3.11 与上限 cp310 **无交集**，同时满足三者只能靠 Flink 2.1.x + 3.12；③ 升 2.x 后两处必然踩点（本轮一并预消除，见修复内容 A）：DataStream Kafka 连接器不在官方 flink 镜像也不在 apache-flink wheel（缺则提交后 `ClassNotFoundException: org.apache.flink.connector.kafka.source.KafkaSource`）；Flink 2.0 已**删除**旧 `state.*` 配置键（`state.checkpoints.dir` 不是告警而是启动即失败）。

修复内容（受追踪 21 个文件，三组）：

- **A 镜像与运行时**（`flink-jobs/Dockerfile` + 标签引用面）：pyrel → `python:3.12-slim-bookworm`（与基座同代 OpenSSL 3）；基座 `flink:1.18.1-scala_2.12-java17` → `flink:2.1.1-scala_2.12-java17`（已核 Docker Hub 标签存在）；`pip install "apache-flink==2.1.1"`；apt 补 `libsasl2-2` + `ldconfig`（confluent-kafka 自带 librdkafka 的 SASL 运行时依赖）；新增 `conn` 阶段用标准库 `urllib` 从 Maven Central 拉 **`flink-sql-connector-kafka-5.0.0-2.1.jar`**（已核 Maven Central：连接器自 Flink 1.15 起独立版本线，命名 `<连接器版本>-<Flink 版本>`，Flink 2.1 对应 `5.0.0-2.1`，**没有** 2.1.1 版本号也**没有** `_2.12` 后缀）并 `COPY --chown=flink:flink` 入 `/opt/flink/lib/`（JM/TM/提交 Job 共用同一镜像 → 三侧同时生效；ARG `KAFKA_CONNECTOR_URL`/`KAFKA_CONNECTOR_JAR` 可换镜像源）；构建期自检由两道扩为**三道**（① `python --version`/`pip --version`/`import ssl`；② 开 jar 断言 `KafkaSource`/`KafkaSink` 类存在；③ `import pyflink/hunter_common.database.enums/hunter_common.redis_keys/confluent_kafka/redis/sqlalchemy/pydantic` + 连接器封装）。标签全量对齐 `2.1.1`：`infra/deploy/docker-compose.yml`（flink-jm/flink-tm）、`infra/k8s/flink/02-jobmanager.yaml`、`03-taskmanager.yaml`、`04-submit-job.yaml`、`.gitlab-ci.yml` build-images。
- **B 2.x 配置键迁移**：compose 两处 `FLINK_PROPERTIES` 与 `infra/k8s/flink/01-configmap.yaml` 的 `state.checkpoints.dir` → `execution.checkpointing.dir`（并注 `state.savepoints.dir` → `execution.checkpointing.savepoint-dir`）。
- **C 契约与断言连带**（因 `"Flink 1.18"` 被写进契约并有单测冻结）：`contracts/kafka/consumer-groups.yaml` 3 处 `engine`、`contracts/kafka/schemas/alert_event.schema.json` 与 `algorithm_metrics.schema.json` 的 description、`contracts/openapi/data-analytics.yaml` 3 处（info.description / x-hunter-dependencies 的 flink usage / `x-hunter-realtime-jobs.engine`）均 Flink 1.18 → **Flink 2.1**；重生成 `infra/k8s/base/03-configmap-contracts.yaml`；`services/data-analytics/app/tests/test_data_analytics_contract.py::test_realtime_jobs_match_6_2` 断言改判 `"Flink 2.1"`。
- **文档/注释同步**：`README.md`（技术栈与架构图、目录树）、`flink-jobs/README.md`、`infra/deploy/README.md`、`infra/k8s/services/data-analytics.yaml` 注释、`scripts/install.sh` Step 11.2、三个作业入口 docstring（`detection_job.py` 两处 / `algorithm_performance_job.py` / `ingest_latency_job.py`）、`docs/deployment.md`（§2 服务表改 python:3.12-slim/UID 10001 + PyFlink 段重写 wheel ABI 矩阵、libssl 教训、连接器 jar、三道自检、RUN 续行禁写 `#`；§3/§12/§13.1 标签）、`docs/01-部署概述与环境要求.md`（容器表/分析流/目录树）、`docs/运维操作手册.md` §3.6、`docs/故障排查指南.md` 场景13。

向后兼容：**有破坏性变更**（运行时 1.18 → 2.1）。① 必须 `build --no-cache` 重建 `hunter-flink/jobs` 并 `--force-recreate` flink-jm/flink-tm；旧镜像 + 新配置键（或反之）会启动失败；② K8s 形态需重新 apply `infra/k8s/flink/01-configmap.yaml`（键名已变）；③ 旧 1.x checkpoint/savepoint 能否被 2.x 恢复**未实机验证**——单机形态建议直接新提交而不回恢旧状态；④ **作业代码零改动**（grep 核实三作业仅用 `KafkaSource/KafkaSink` builder、`key_by`/`flat_map`/`assign_timestamps_and_watermarks`，未动 1.20/2.x 改过的 `Time`/`Windows` API，窗口在 FlatMap 内 Python 自管）；⑤ 对外行为不变：Topic/Schema 字段、REST 端点、错误码、DDL、Redis 键均未动，契约仅引擎版本文案变化，`x-hunter-realtime-jobs.jobs` 结构不变。K8s `runAsUser: 9999` 未改（待核，见风险②）。

风险与待人工确认：① 本环境无 Docker，2.1.1 镜像与 `conn` 阶段的 jar 下载**未经实机 build**；服务器首次 build 需能访问 Maven Central（不可达则 `--build-arg KAFKA_CONNECTOR_URL=https://maven.aliyun.com/repository/central/...` 同路径镜像源）；② `flink:2.1.1` 镜像内 `flink` 用户 UID 是否仍为 9999 未核（影响 K8s `runAsUser`/`fsGroup`；compose 形态不受影响）；③ FLINK_PROPERTIES 其余键在 2.x 的存留（`classloader.check-leaked-classloader`、`rest.address`/`rest.port`、`blob.server.port`、`jobmanager.rpc.port`）未逐项核验，若 JM 报“配置项已移除”按日志提示替换即可（同类问题、非代码缺陷）；④ Kafka SASL_PLAINTEXT 内部监听 vs 作业明文连接（V1.19.1/V1.19.2 遗留）仍是越过本坎后的下一道判据。

验证（本环境，均实跑）：`generate_contracts_configmap.py` 重生成后 `--check` **PASS**；`verify_infra.py` 全 16 项 **PASS**；`render_k8s.py --check` **PASS**；`verify_test_layer.py` **PASS**；`run_unit_tests.py` 全量 **9 目标全绿**（common/python 326、flink-jobs 35、api-gateway 71、scene-service 108、data-collector 143、data-analytics 93、ota-service 128、remote-control 97）；`ruff check flink-jobs services/data-analytics` 无告警；临时脚本核对 10 个 YAML + 2 个 JSON 可解析、`x-hunter-realtime-jobs.engine` 与 consumer-groups 三处 engine 均为 Flink 2.1、镜像标签引用 5 处一致为 2.1.1（脚本已删）。仓内尚存的 4 处 “1.18” 均为历史/根因说明文字（deployment.md wheel ABI 矩阵、故障排查 V1.19.2 段、compose V1.19.2 注），故意保留。实机验证点（服务器）：build 日志应出现 `ssl OK OpenSSL 3.x`、`kafka connector jar OK: /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.1.jar`、`job deps OK on 3.12.x` 三行；`python --version` = 3.12.x；`flink list` 见三作业 RUNNING。

分发路径注意：受追踪 **21 个** —— A 组 `flink-jobs/Dockerfile`、`infra/deploy/docker-compose.yml`、`infra/k8s/flink/02-jobmanager.yaml`、`infra/k8s/flink/03-taskmanager.yaml`、`infra/k8s/flink/04-submit-job.yaml`、`.gitlab-ci.yml`；B 组 `infra/k8s/flink/01-configmap.yaml`；C 组 `contracts/kafka/consumer-groups.yaml`、`contracts/kafka/schemas/alert_event.schema.json`、`contracts/kafka/schemas/algorithm_metrics.schema.json`、`contracts/openapi/data-analytics.yaml`、`infra/k8s/base/03-configmap-contracts.yaml`、`services/data-analytics/app/tests/test_data_analytics_contract.py`；注释/文档类 `flink-jobs/hunter_flink/detection_job.py`、`algorithm_performance_job.py`、`ingest_latency_job.py`、`README.md`、`flink-jobs/README.md`、`infra/deploy/README.md`、`infra/k8s/services/data-analytics.yaml`、`scripts/install.sh`。运行必需为 A/B/C 三组；`docs/` 与 `release.md` 被 `.gitignore` 忽略（需单独 rsync）。

服务器续跑步骤：
```bash
# 本地：提交推送（受追踪 21 个文件）
git add -A && git commit -m "V1.19.5: 作业运行时升 Flink 2.1.1 + CPython 3.12，内置 Kafka 连接器 jar，契约引擎同步" && git push

# 服务器：必须 --no-cache（基座/pyrel/apt/pip 四层全换）
cd /opt/HunterCore && git pull
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build --no-cache flink-jm
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate flink-jm flink-tm
# 三道验：解释器 / 连接器 jar / 作业依赖
sudo docker exec hunter-flink-jm python --version
sudo docker exec hunter-flink-jm ls /opt/flink/lib | grep kafka
sudo docker exec hunter-flink-jm python -c "import hunter_common.database.enums, confluent_kafka; print('deps OK')"
for j in detection algorithm_performance ingest_latency; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec \
    flink-jm flink run -d -m localhost:8081 -pym hunter_flink.${j}_job
done
sudo docker exec hunter-flink-jm flink list
```

**待办（不属本条范围）**：① `flink:2.1.1` 内 `flink` 用户 UID 与 K8s `runAsUser: 9999` 复核；② FLINK_PROPERTIES 其余键在 2.x 的存留核验；③ 作业侧 Kafka SASL 凭据注入路径（V1.19.1/V1.19.2 遗留）；④ 三作业实机 RUNNING/出数与看板 `ingest_latency_ms_p95`、Redis 键 `analytics:ingest_latency` 核验后回填本条及 V1.19.1/V1.19.2。

**实机验证回填（服务器 V1.19.5 实录，V1.19.7 时点）**：`build --no-cache` **成功**（总 749s，其中 `pip install apache-flink==2.1.1` 层 339.7s）；`up -d --force-recreate` → `hunter-flink-jm Healthy`、`hunter-flink-tm Started` ⇒ **风险③解除**（2.1.1 接受现有全部 `FLINK_PROPERTIES` 键，未出现「配置项已被移除」）、**风险①解除**（bookworm pyrel 的 `_ssl` 在基座 OpenSSL 3 下加载正常，第一道自检通过）；容器内 `python --version` = `Python 3.12.15`；`ls /opt/flink/lib | grep kafka` = `flink-sql-connector-kafka-5.0.0-2.1.jar`（第二道自检通过）；`python -c "import hunter_common.database.enums, confluent_kafka"` → `deps OK`（第三道自检通过）。
⚠ 本条的实机验证点「`flink list` 见三作业 RUNNING」**未达成**：三连 `-pym` 提交齐报 `AttributeError: type object 'KafkaOffsetsInitializer' has no attribute 'group_offsets'`、`flink list` → `No running jobs.`。与本条的镜像/配置/连接器层无关（该层已全部转正），根因与修复见 **V1.19.7**；风险②（`flink` 用户 UID）与待办③（Kafka SASL 凭据）仍未验证。

## V1.19.6（修复：Kafka 本地磁盘缓冲同毫秒多段的 FIFO 重投顺序不保（段名字典序由随机后缀决定））

Date：2026-10-10
变更类型：修复

现场：V1.19.5 落盘后跑单元全量，`common/python/tests/test_kafka_local_buffer.py::test_replay_stops_on_failure_and_keeps_remainder` **概率性失败**：第二次 replay 得到 `[b'{"seq":2}', b'{"seq":1}']`，期望 `[b'{"seq":1}', b'{"seq":2}']`；单跑同一条用例又能通过（非确定性）。与 V1.19.5 无关（`common/python/**` 本轮未被触及），属存量缺陷被全量跑抽中。

根因：段名 `seg-<毫秒时间戳:013d>-<uuid4 前 8 位>.jsonl`，而 `_segments()` 直接对文件名取 `sorted()`，注释断言“前缀为毫秒时间戳，字典序即时间序”。该断言仅在**每段落在不同毫秒**时成立：`segment_max_records=1`（或断网期间快速批量落盘）时多个段落进同一毫秒，先后完全由**随机后缀**决定 → `replay()` 乱序投递。契约侧后果：`producer.replay_buffered` 承诺“以最旧优先重投……保证分区内顺序”，实际高频落盘下会把同一 key 的消息乱序写回 Kafka；消费侧幂等键 `(vehicle_id, seq)` 能去重但**不能纠正顺序**（`vehicle_telemetry` 的 seq 单调性与“最近状态”类读模型可能被旧值覆盖）。

修复内容：`common/python/hunter_common/kafka/buffer.py` 段名加入**定宽段序号** —— `seg-<毫秒:013d>-<序号:06d>-<随机后缀:8>.jsonl`（两级定宽数字前缀使字典序 == 写入顺序，随机后缀只用于避免进程间文件名碰撞）；`_current_segment()` 在 `self._lock` 下自增 `self._segment_seq`；`_scan_existing()` 用新增静态方法 `_max_segment_seq()` 从既有段名续接序号（重启不回退；旧命名段解析计为 0，向后兼容）；`_segments()` 注释改写为真实不变量。

向后兼容：段文件命名变化**不影响任何对外契约**：缓冲目录（`data/kafka-buffer/<service>`）是服务私有本地状态，无跨版本/跨服务读取方；旧格式既存段仍能被 `seg-*.jsonl` glob 读到并正常重投/淘汰/崩溃恢复（`_recover_incomplete_segments` 的 `seg-*.jsonl.tmp` 模式与 `_finalize_segment` 原子重写路径未改）；Kafka/DB/REST/Redis 契约零变更。共享库改动需 `--build` 重建业务镜像才生效。

风险与待人工确认：① 单进程内 FIFO 已由构造保证；**跨进程**同毫秒写入仍由后缀定序（不同进程本无先后关系，且缓冲目录按 service 隔离，不构成实际风险）；② 本轮仅单元级回归，**未做实机断网-恢复演练**；③ 旧格式段与新段混存时（升级窗口内）同毫秒并列不保证有序（可接受：升级重启本身隔毫秒）。

验证（本环境，均实跑）：新增两条回归用例——`test_replay_keeps_fifo_for_segments_within_same_millisecond`（200 条连续写入落在同一毫秒、滚 200 段，断言重投序列恰为 `range(200)`；修复前该场景即本次概率失败的放大）与 `test_segment_sequence_does_not_regress_after_restart`（重启后段序号严格递增）；`run_unit_tests.py` 全量 **9 目标全绿**（common/python **326 passed**，原失败用例转绿），缓冲/生产者用例连续 3 次运行全通过；`ruff check common/python/hunter_common/kafka/buffer.py common/python/tests/test_kafka_local_buffer.py` 无告警；`mypy` 该模块无新增错误（仅报仓库存量的 `logging.py` arg-type 与 yaml stub 缺失）。

分发路径注意：受追踪 **2 个**：`common/python/hunter_common/kafka/buffer.py`、`common/python/tests/test_kafka_local_buffer.py`。共享库随 6 个服务镜像打包 → 生产必须 `--build` 重建业务服务镜像（仅重启容器无效）；`release.md` 不入库。

## V1.19.7（修复：作业 `main()` 的 PyFlink API 名在 2.1 不存在，三作业提交齐报 AttributeError；新增两道 API 面门禁）

Date：2026-10-10
变更类型：修复（含 `ingest_latency` 运行形态调整）

现场（V1.19.5 镜像层已全绿后）：build 749s 成功、`Python 3.12.15`、`flink-sql-connector-kafka-5.0.0-2.1.jar` 在位、`deps OK`、`hunter-flink-jm Healthy`/`flink-tm Started`；但三连 `-pym` 提交齐报同一行错误：
> `File "/opt/hunter/jobs/hunter_flink/detection_job.py", line 96, in main` → `.set_starting_offsets(KafkaOffsetsInitializer.group_offsets())` → `AttributeError: type object 'KafkaOffsetsInitializer' has no attribute 'group_offsets'`（`algorithm_performance_job.py:216`、`ingest_latency_job.py:207` 同因同错），外层 `ProgramAbortException: Python process exits with code: 1`，`flink list` → `No running jobs.`。每次提交前伴随 5 行 `WARNING: Unknown module: jdk.compiler specified to --add-exports`——JDK17 基座下 flink 启动脚本导出该模块名，**与本故障无关、无害**。

根因：三个作业的 `main()` 均标 `# pragma: no cover`（仓库内无 PyFlink 运行时），**这些行从不被执行**，于是「凭印象写的 PyFlink API 名」只能在提交期暴露。以 release-2.1 源码逐条核实后确认，`group_offsets` 只是同批缺陷里**最先抛错的一个**，共 **5 类**在 2.x 不成立的写法（均已修）：
① `KafkaOffsetsInitializer.group_offsets()` / `specific_offsets()` —— 1.19 起 offsets 初始化器移交 Java `OffsetsInitializer`，2.1 仅有 `committed_offsets`/`timestamp`/`earliest`/`latest`/`offsets`；
② `KafkaSourceBuilder.value_only_decoder(Types.PRIMITIVE_STRING(Types.InformationType()))` —— 该 setter 正名 `set_value_only_deserializer`，参数位是 **DeserializationSchema**（`SimpleStringSchema()`）而非 TypeInformation；
③ `env.add_source(KafkaSource...)` —— `add_source` 只接受 `SourceFunction`，FLIP-27 Source 的唯一入口是 `from_source(source, watermark_strategy, source_name, type_info=None)`；
④ `DataStream.sink(kafkaSink)` —— 2.1 无 `.sink`，新 Source Sink 走 `sink_to(sink: Sink)`；
⑤ Python 自定义 `RedisSink(SinkFunction)` —— `SinkFunction`/`SourceFunction` 现均继承 `JavaFunctionWrapper`（构造必传 Java 对象或 Java 类名，`get_java_function()` 直接返回它），**Python 子类不再受支持**（不调 `super().__init__` 即 AttributeError）。

修复内容（受追踪 8 个文件，四组）：

- **A 作业 `main()` 对齐 2.1 API**（`detection_job.py`、`algorithm_performance_job.py`、`ingest_latency_job.py`）：起始位点改 `set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))`（对齐 `contracts/kafka/consumer-groups.yaml` 的 `defaults.auto_offset_reset: earliest`，**未改契约**）；反序列化改 `set_value_only_deserializer(SimpleStringSchema())`；入口改 `env.from_source(source, WatermarkStrategy.no_watermarks(), INPUT_TOPIC, type_info=Types.STRING())`；出口改 `.sink_to(KafkaSink...)`，sink 侧 key/value 序列化 schema 同改 `SimpleStringSchema()`；算子参数名统一为 `output_type` / `key_type`；每处改名旁附行内注释说明 1.x→2.x 差异。
- **B `ingest_latency_job.py` 运行形态调整**（因 ⑤）：删除占位 `_arrival_watermark_strategy()`（原 `raise NotImplementedError`，即 **V1.19.0 风险①登记项**）与 `RedisSink(SinkFunction)`，改为 `LatencyProcessor(KeyedProcessFunction)` —— `open()` 建 Redis 客户端、`close()` 释放、`process_element` 读 **`ctx.timestamp()`**（`KafkaSource` 默认把 `ConsumerRecord.timestamp()` 作为记录内部事件时间，即契约 pending #10 结案的「Kafka 到达时间」口径，**不改口径、不新增列/表**）；时间戳缺失或非正 → **丢弃样本**（禁止用处理时间伪造）；拓扑 `from_source → key_by("fleet") → process(...) → .print("ingest_latency")`。Redis 写在算子内完成，异常不吞（失败可见）。
- **C 新增两道 API 面门禁（堆叠）**：新建 `flink-jobs/hunter_flink/api_guard.py`（名字集合的**单一事实来源**）—— `API_WHITELIST`（模块→类→成员）、`FLUENT_SUCCESSORS` + `allowed_members()`（链式调用后继类闭包展开）、`REMOVED_IN_FLINK_21`（断言 4 个 2.x 已删名字**确已消失**）、`FORBIDDEN_IN_JOBS`（作业内禁用的 7 个名字）、`check()`/`main()`（纯属性内省，**不启 JVM**、不建 gateway，`python -m hunter_flink.api_guard`）。镜像侧：`flink-jobs/Dockerfile` 新增**第四道**构建期自检 `RUN python -m hunter_flink.api_guard`（位于 `ENV PYTHONPATH` 之后、`USER flink` 之前），并把自检编号在注释与 compose 注记里统一为①解释器/ssl ②连接器 jar 类 ③依赖 import ④API 成员名。仓库侧：新建 `flink-jobs/tests/test_pyflink_api_usage.py`（`ast` 静态校验三个 `main()`：所用 pyflink 模块/类/成员必在白名单内、禁用名字不出现、`from_source`/`set_value_only_deserializer`/`set_starting_offsets` 齐备、三作业拓扑同形，并校验白名单自身自洽：后继类必须指向白名单类；无 PyFlink 环境也能跑，直连 `check()` 那条用 `importorskip`）。
  ⚠ **本轮自查当场发现并修掉一个“自检本身失效”的缺陷**：`api_guard.py` 初版**忘了写 `if __name__ == "__main__"` 入口块**，`python -m hunter_flink.api_guard` 只导入模块就退出（返回码 0）—— Dockerfile 第四道自检会**静默变为空跑**（恰好是本模块要防的那类“自检消失”）。已补入口块（`raise SystemExit(main())`），并新增两条元用例锁死：`test_api_guard_is_executable_as_module`（断言 `api_guard.py` 存在模块级 `__main__` 分支且调 `main()`，并断言 Dockerfile 含 `RUN python -m hunter_flink.api_guard`）与 `test_api_guard_does_not_pass_silently_without_pyflink`（未装 pyflink 时 `check()` 必返回非空，绝不“空列表 = 通过”）。修复后本地实跑已能真实输出 6 行 `pyflink api guard FAIL: import ... failed` 并退出码 1。
- **D 文档与注释**：`docs/deployment.md`（四道自检 + 「作业 `main()` 必过 API 面守卫」条）、`docs/故障排查指南.md`（验证顺序加守卫一步、三道→四道、新增「环境与依赖都绿但提交齐报 AttributeError」段含 4 条改名映射）、`docs/运维操作手册.md` §3.6（提交前预检三项、命令块加守卫、「镜像内第三道」改「第二道」、新增 API 面守卫 bullet）、`flink-jobs/README.md`（四道自检、API 面纪律条、本地守卫命令）、`infra/deploy/docker-compose.yml` flink-jm 注释（编号纠正 + 守卫提示）。

向后兼容：① **作业代码变更 → 必须 `--build` 重建 `hunter-flink/jobs` 镜像**（`flink-jobs/**` 打进镜像），但**本轮不需要 `--no-cache`**（基座/pyrel/apt/pip/jar 层全命中缓存，只有 `COPY flink-jobs/hunter_flink` 与其后的第四道自检重跑）；② `ingest_latency` 的 Redis 写入落点由「Sink」改为「算子内副作用」，终端多一路 `print`（作业日志可见每条指标 JSON）——写的是**同一个键 `analytics:ingest_latency`、同一 TTL 300、同一 JSON 结构**，看板 `IngestLatencyRedisReader` 读侧零改动；③ 起始位点行为**不变**（仍为“有组偏移则续读、无则 earliest”），`committed_offsets(EARLIEST)` 与契约 `auto_offset_reset: earliest` 语义等价，只是 API 名字换了；④ 对外契约零变更：Topic/Schema 字段、REST 端点、错误码、DDL、Redis 键、`consumer-groups.yaml` 均未动；⑤ `alert_event` / `algorithm_metrics` 载荷与投递 Topic 不变（sink 侧序列化 schema 的实现对象换为 `SimpleStringSchema`，输出仍是 JSON 字符串）。

风险与待人工确认：① **`ctx.timestamp()` 若实机取不到**（Kafka 记录时间戳为 null 时返回 `-1`）→ 入库延迟指标恒不出数（看板该字段按设计降级 null，不影响其他块）——备选路线：改用 PyFlink Table API 声明 Kafka metadata `timestamp`，需回改实现并复核口径；② 本轮**未经实机验证**（本环境无 Docker、无 PyFlink wheel），第四道自检将在服务器 build 首次真跑，`ast` 单测已在本地真跑；③ Kafka SASL_PLAINTEXT 内部监听 vs 作业明文连接（V1.19.1/V1.19.2 遗留待办）很可能是越过本坎后的**下一道判据**（表现为作业反复重启/FAILED、broker 日志 `Authentication failed`）；④ sink **刻意未加** `set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)` —— 保持原投递语义、不在本轮夹带行为变更（列为待办）；⑤ `print` 输出会进 JM/TM 日志，高频窗口下有一定噪声（每 60s 每车队一条，可接受）；⑥ `flink:2.1.1` 内 `flink` 用户 UID 是否仍 9999（V1.19.5 风险②）仍未核。

验证（本环境，均实跑）：`ruff check flink-jobs` **All checks passed**；`run_unit_tests.py flink-jobs` **46 passed, 1 skipped**（新增 `test_pyflink_api_usage.py` 共 **12 个用例**（含参数化展开）；skip 的是需已装 apache-flink 的 `check()` 直连用例）；本地实跑 `python -m hunter_flink.api_guard`：无 pyflink 时输出 6 行 FAIL 且退出码 1（证明门禁真在执行）；`run_unit_tests.py` 全量 **9 目标全绿**（common/python **326**、flink-jobs **46**、api-gateway **71**、scene-service **108**、data-collector **143**、data-analytics **93**、ota-service **128**、remote-control **97**）；`mypy flink-jobs/hunter_flink --ignore-missing-imports` 无新增错误（仅存量 `rules.py:167 dict-item`，本条未触碰该文件，列入待办）；`verify_infra.py` **16 项全 PASS**、`render_k8s.py --check` PASS、`verify_test_layer.py` PASS（本轮未改 contracts，无漂移）。权威依据：release-2.1 的 `datastream/connectors/kafka.py`、`datastream/data_stream.py`、`datastream/stream_execution_environment.py`、`datastream/functions.py`、`fn_execution/datastream/process/process_function.py`、`common/watermark_strategy.py`、`common/serialization.py` 逐文件核对（`KeyedProcessFunction.Context.timestamp()` 在 runner 侧确有实现，故可用）。

分发路径注意：受追踪 **8 个** —— 代码 `flink-jobs/hunter_flink/{detection_job.py, algorithm_performance_job.py, ingest_latency_job.py, api_guard.py}`、`flink-jobs/tests/test_pyflink_api_usage.py`、`flink-jobs/Dockerfile`、`flink-jobs/README.md`、`infra/deploy/docker-compose.yml`（仅注释）。`docs/` 三份与 `release.md` 被 `.gitignore` 忽略，需单独 rsync；运行必需为前 6 个。

服务器续跑步骤：
```bash
# 本地：提交推送（受追踪 8 个文件）
git add -A && git commit -m "V1.19.7: 作业 PyFlink API 对齐 2.1，新增 api_guard 双门禁" && git push

cd /opt/HunterCore && git pull
# 本轮不需要 --no-cache（只有作业 COPY 层与第四道自检重跑）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build --force-recreate flink-jm flink-tm
sudo docker exec hunter-flink-jm python -m hunter_flink.api_guard      # 必须输出 pyflink api guard OK
for j in detection algorithm_performance ingest_latency; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec \
    flink-jm flink run -d -m localhost:8081 -pym hunter_flink.${j}_job
done
sudo docker exec hunter-flink-jm flink list                            # 期望三作业 RUNNING
# ⚠ Redis 开了 requirepass，不带口令的裸 redis-cli 只会回 NOAUTH（与作业是否出数无关）
sudo sh -c 'RP=$(grep "^REDIS_PASSWORD=" .env | cut -d= -f2-); docker exec hunter-redis redis-cli --no-auth-warning -a "$RP" GET analytics:ingest_latency'
```

判据预告：若错误变为 broker 侧 `Authentication failed` / 作业反复重启，那是 V1.19.1/V1.19.2 已登记的 SASL 凭据注入待办（**非本条回归**）；若 `ingest_latency` RUNNING 但 Redis 键始终缺失，即风险①命中（`ctx.timestamp()` 取不到），改走 Table API metadata 路线。

实机验证回填（2026-10-12 服务器实录，V1.19.7 主体**已结案**）：
- `up -d --build --force-recreate flink-jm flink-tm` 仅 **10.6s**，build 日志可见 pyrel/`apt`/`conn` jar/`pip install apache-flink` **四层全 CACHED**，只有 `COPY flink-jobs/hunter_flink`（0.8s）与 `RUN python -m hunter_flink.api_guard`（1.6s）重跑 ⇒ 向后兼容①「改作业 API 只需 `--build`、不需 `--no-cache`」**得到实证**；
- `sudo docker exec hunter-flink-jm python -m hunter_flink.api_guard` → **`pyflink api guard OK (18 classes)`** ⇒ 第四道自检在镜像内**确实在执行且有输出**（本地抓到的是“缺 `__main__` 则静默空跑”，实机正面印证），风险②解除；
- 三连 `-pym` 提交各拿到 JobID，`flink list` → `driving_anomaly_detection` / `algorithm_performance_monitor` / `data_quality_monitor_ingest_latency` **三者全部 RUNNING**、`No scheduled jobs.` ⇒ 根因 ①~⑤ 五类 API 改名**全部生效**，`WARNING: Unknown module: jdk.compiler` 如预告确认无害；
- ⚠ **但本条不能算已全部结案**：出数核验 `redis-cli GET analytics:ingest_latency` 返回 **`NOAUTH Authentication required.`** —— 该命令本身缺口令（上一条的缺陷，已在上方命令块改正），而更实质的是：作业虽 RUNNING，`alert_event`/`algorithm_metrics`/Redis 键**尚未证实有数据**。“RUNNING 而不出数”的根因是**容器里根本没有 Kafka/Redis 凭据**（正是本条风险③预告的那道坡，也是 V1.19.1/V1.19.2/V1.19.7 三条连续登记的待办）——风险③**已命中**，但表现形态与预告不同：**不是**“反复重启/FAILED + broker `Authentication failed`”，而是**静默空转仍 RUNNING**；据此单独修复于 **V1.19.8**。风险①（`ctx.timestamp()` 取不到）**仍未判定**——要等数据真的流动起来才能判。

**待办（不属本条范围）**：① sink 投递语义是否升 `AT_LEAST_ONCE`（需与下游幂等键策略一并定）；② `flink-jobs/hunter_flink/rules.py:167` 的 mypy 存量 `dict-item`；③ `flink:2.1.1` 镜像 `flink` 用户 UID 与 K8s `runAsUser: 9999` 复核（V1.19.5 风险②）；④ 三作业实机 RUNNING/出数后回填 V1.19.0/V1.19.1/V1.19.2/V1.19.5 四条的服务器验证项。


## V1.19.8（修复：作业凭据未下发导致的“RUNNING 假绿”——新增 `job_conn.py` 连接装配 + compose/K8s 两侧注入 + 无凭据则提交期失败）

Date：2026-10-12
变更类型：修复（新增模块 + 编排注入 + 文档）

现场（接 V1.19.7 同轮实录）：三作业全绿 RUNNING 后，`sudo docker exec hunter-redis redis-cli GET analytics:ingest_latency` 返回 **`NOAUTH Authentication required.`**；`alert_event` / `algorithm_metrics` 也未证实有数据。关键：**没有任何作业报错**，`flink list` 永远是三条 RUNNING。

根因（两层，均为“静默失败”而非崩溃）：
① **Kafka**：broker 的内部监听 `kafka:9092` 硬定为 **SASL_PLAINTEXT + SCRAM-SHA-512**（compose 的 `KAFKA_INTERNAL_SECURITY_PROTOCOL` 与 `scripts/common.sh` 的 `kafka_auth_args` 同源），而 `flink-jm`/`flink-tm` 的 `environment` 里**只有 `FLINK_PROPERTIES` + `TZ`**（已逐字核对），作业侧也只读 `KAFKA_BOOTSTRAP_SERVERS`、**无任何 SASL 读取路径**——该缺口在 `infra/k8s/flink/01-configmap.yaml` 头注第 10-11 行早已书面登记为待办。⚠ 重点：**明文连 SCRAM 监听不会把作业打坏**，只是 fetch 不到数据，所以仍是 RUNNING（本仓库已**第三次**踩同类坑：前两次在 CLI，表现为 `Timed out waiting for a node assignment`）。
② **Redis**：`ingest_latency` 的 `LatencyProcessor.open()` 跑在 **TaskManager** 进程里，旧版仅 `os.environ.get("REDIS_URL", "redis://redis:6379/0")` —— 无口令，而 compose 的 redis 带 `--requirepass ${REDIS_PASSWORD}`（实录的 NOAUTH 即此，与该命令自身缺 `-a` 叠加）。

修复内容（受追踪 **13 个**，五组）：

- **A 新建 `flink-jobs/hunter_flink/job_conn.py`（连接参数装配的唯一事实来源）**：纯 Python、可在无 PyFlink 环境下真实单测（不同于 `main()` 的 `pragma: no cover`）。`kafka_sasl_properties()` —— 非 SASL 协议返 `{}`；声明 SASL 而缺用户/口令 ⇒ **直接 `ValueError`**（宁可提交期失败，不可运行期假绿）；JAAS 串对口令转义 `\` 与 `"`；`KAFKA_SASL_LOGIN_MODULE` 可覆盖且显式空值报错。`apply_kafka_properties(builder, props)` —— 逐个 `set_property`，签名用 `Any`（用 `object` 会逼调用方写 `type: ignore`）。`redis_url()` —— `REDIS_HOST` 分段组装（口令 `quote(..., safe='')` URL 编码）> `REDIS_URL` > 默认；⚠ 只要设了分段变量就**忽略** `REDIS_URL`，以防无口令的遗留串悄悄抢位真凭据。
- **B 三作业接入**（`detection_job.py`/`algorithm_performance_job.py`/`ingest_latency_job.py`）：`sasl_props = kafka_sasl_properties()` 后，**source 与 sink 两侧 builder** 均经 `apply_kafka_properties(...)` 再 `.build()`（只改 source 会写不进）；`ingest_latency_job.py` 的 `open()` 改 `redis.Redis.from_url(redis_url(), decode_responses=True)`。
- **C `api_guard.py` 白名单新增 `set_property`**（`KafkaSourceBuilder` 与 `KafkaSinkBuilder` 各一处）——它不在原白名单内，不登记则第四道自检会把正确写法拦死。已核 release-2.1 `kafka.py`：`KafkaSourceBuilder.set_property(key, value)`（L624）、`KafkaSinkBuilder.set_property`（L976）均存在 ⇒ **可纯 Python 下发 SASL，无需 Java 侧配置**。
- **D 编排两侧注入**：`infra/deploy/docker-compose.yml` 的 flink-jm/flink-tm **各新增 9 个 env**（`KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_MECHANISM` / `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` + `REDIS_HOST|PORT|PASSWORD|DB`），取值全用 `.env` **现有键**、拼写与 `x-business-env` **完全一致**（不造第二套名字）。K8s：`01-configmap.yaml` **删除 `REDIS_URL`**、新增 `KAFKA_SECURITY_PROTOCOL|SASL_MECHANISM|REDIS_HOST|REDIS_PORT|REDIS_DB`（非敏感）；`02-jobmanager.yaml`/`03-taskmanager.yaml`/`04-submit-job.yaml` **各加 `- secretRef: {name: hunter-app-secrets}`**（该 Secret 已含 `REDIS_PASSWORD`/`KAFKA_SASL_USERNAME`/`KAFKA_SASL_PASSWORD`）。⚠ 进程语义：**提交在哪跑就在哪里得凭据**（作业 `main()` 在提交进程求值后写进 JobGraph）——compose 是 `flink-jm`、K8s 是 `04-submit-job` Pod；而 TM 也得挂（`open()` 在 TM 里读 Redis）。
- **E 文档四处**：`docs/故障排查指南.md` 新增整段“三作业已 RUNNING 但看板无数据”+ **四步逐层定位命令**（容器 env 键检查 → TM 日志 grep SASL → jar 内省 `ScramLoginModule` 类名 → 带口令 `redis-cli`），并改写 V1.19.2 段内“作业暂无凭据注入路径”的旧述；`docs/运维操作手册.md` §3.6（命令块改带口令 redis-cli + “作业连接凭据” bullet）；`docs/deployment.md`（“作业连接参数只能来自 `job_conn.py`” bullet，含两个易踩点）；`flink-jobs/README.md` 环境变量段重写。

向后兼容：① 作业代码变更 ⇒ 仍只需 `up -d --build --force-recreate flink-jm flink-tm`（无需 `--no-cache`）；② **无环境变量时的默认行为不变**（`KAFKA_SECURITY_PROTOCOL` 未设 ⇒ 默认 `PLAINTEXT` ⇒ 返回 `{}` ⇒  builder 与原写法完全一致），本地/单测/无认证环境不受影响；③ 对外契约零变更：Topic/Schema/REST/DDL/Redis 键均未动；④ ⚠ **行为收紧一项**：若容器配了 `KAFKA_SECURITY_PROTOCOL=SASL_*` 而未下发凭据，以前是静默空转、现在是 **`flink run` 直接失败**（故意的：假绿比红更坏）；已按 K8s 旧方式仅在 ConfigMap 里放 `REDIS_URL` 的集群，需同时删掉该行并挂 secret（分段变量会抢位它）。K8s 侧坐标不需重制：`render_k8s.py --check` 仍 PASS。

风险与待人工确认：① ⚠ **shaded login module 类名未经实机核实**——`flink-sql-connector-kafka-5.0.0-2.1.jar` 是 fat jar、Kafka 客户端被重定位，默认值按此给出；若实机报 `class not found`，用故障排查指南第 3 步的 jar 内省命令拿到真类名后用 `KAFKA_SASL_LOGIN_MODULE` 覆盖，**不必改代码**；② `KAFKA_SASL_USER` 对 `data-analytics-telemetry*` 消费组与相关 Topic 的 **ACL 授权**未核（SCRAM 认证通过 ≠ 有权 describe/read/write；表现为 `GroupAuthorizationFailed` / `TopicAuthorizationFailed`）；③ **口令随 JobGraph 下发**，在 Flink REST 的 plan / checkpoint 元数据中可见——属单节点内网形态的已知折衷，上 K8s 多租户环境需改为 TM 侧配置；④ **本条仍不能单独确认“已出数”**——真实数据流动需上游采集在跑；⑤ V1.19.7 风险①（`ctx.timestamp()` 取不到）与本条相交：若凭据通了、但 Redis 键仍缺，则转回风险①、改走 Table API metadata；⑥ 本条修复不改变“作业从不报错而只是没数据”这类故障的**可观测手段**，判出数一律看 `numRecordsIn/Out` 与产物键，**不看 `RUNNING`**。

验证（本环境，均实跑）：`ruff check flink-jobs` **All checks passed**（`--fix` 自动修掉 I001 导入排序与 UP037 引号注解各一处）；`run_unit_tests.py flink-jobs` **58 passed, 1 skipped**（V1.19.7 的 46 → **58**：新增 `tests/test_job_conn.py` **12 个用例**——明文无属性 / SASL 三属性与 JAAS 形态 / 缺凭据 3 种参数化 `ValueError` / login module 覆盖与空值 / 口令引号转义 / `_FakeBuilder` 链式 / Redis 分段编码・无口令・分段抢位・回退；`test_pyflink_api_usage.py` 的形状用例新增三条断言：三作业均调 `kafka_sasl_properties`、均用 `apply_kafka_properties`、非 ingest 作业至少对各一个 builder 应用（防只改 source））；`run_unit_tests.py` 全量 **9 目标全绿**（common/python **326**、flink-jobs **58**、api-gateway **71**、scene-service **108**、data-collector **143**、data-analytics **93**、ota-service **128**、remote-control **97**）；`verify_infra.py` **16 项全 PASS**（含 ConfigMap 敏感字段强制拦截）；`render_k8s.py --check` PASS；`mypy flink-jobs/hunter_flink --ignore-missing-imports` **无新增错误**（仅存量 `rules.py:167`）；临时脚本校验 compose + 4 份 flink YAML 可解析、flink-jm/flink-tm env 各 9 键（脚本用完已删）。

分发路径注意：受追踪 **13 个** —— 新增 `flink-jobs/hunter_flink/job_conn.py`、`flink-jobs/tests/test_job_conn.py`；代码 `flink-jobs/hunter_flink/{detection_job.py, algorithm_performance_job.py, ingest_latency_job.py, api_guard.py}`、`flink-jobs/tests/test_pyflink_api_usage.py`；编排 `infra/deploy/docker-compose.yml`、`infra/k8s/flink/{01-configmap.yaml, 02-jobmanager.yaml, 03-taskmanager.yaml, 04-submit-job.yaml}`；文档 `flink-jobs/README.md`。⚠ **本条不只是作业代码**：只 `git pull` 不重建容器 env（compose 未改到）**仍然会空转**；`docs/` 三份与 `release.md` 被 `.gitignore` 忽略，需单独 rsync。

服务器续跑步骤：
```bash
# 本地：提交推送（受追踪 13 个）
git add -A && git commit -m "V1.19.8: 作业连接凭据下发与 job_conn 装配" && git push

cd /opt/HunterCore && git pull
# 1) 先看容器确实拿到凭据（只看键名，不要粘出值）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build --force-recreate flink-jm flink-tm
sudo docker exec hunter-flink-jm sh -c 'env | grep -oE "^(KAFKA|REDIS)_[A-Z_]+" | sort'   # 期望 9 个键
# 2) 先停掉上一轮的三个假绿作业（旧 JobGraph 里没有 SASL 属性）
for id in 3fd04afe69e58c418e68ab7703e0f15c 706e9ce1dadad321d23ab39443e30623 dbf9f0594e21e95605005ed37338de5e; do
  sudo docker exec hunter-flink-jm flink cancel "$id"
done
# 3) （可选但推荐）先验 JAAS login module 类名与 jar 一致，避开风险①
sudo docker exec hunter-flink-jm python -c "import glob,zipfile; n=zipfile.ZipFile(glob.glob('/opt/flink/lib/flink-sql-connector-kafka-*.jar')[0]).namelist(); print([x for x in n if 'ScramLoginModule' in x])"
# 4) 重三连提交（变量已在容器 env 里，不需在命令行传密钥）
for j in detection algorithm_performance ingest_latency; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec \
    flink-jm flink run -d -m localhost:8081 -pym hunter_flink.${j}_job
done
sudo docker exec hunter-flink-jm flink list
# 5) 真判据：TM 日志无 SASL 报错 + 产物确实出现（二至三个 60s 窗口后）
sudo docker logs --since 5m hunter-flink-tm 2>&1 | grep -iE "sasl|authenticat|principal|authorization" | head -30
sudo sh -c 'RP=$(grep "^REDIS_PASSWORD=" .env | cut -d= -f2-); docker exec hunter-redis redis-cli --no-auth-warning -a "$RP" GET analytics:ingest_latency'
```

判据预告：① 提交即失败并回 `ValueError: KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT 需要 SASL 凭据…` ⇒ compose 的 `.env` 缺 `KAFKA_SASL_USER`/`KAFKA_SASL_PASSWORD`（新行为，按设计工作）；② `Authentication failed` / `Illegal SaslMechanism` ⇒ 机制或口令不对；③ `class not found` 含 `ScramLoginModule` ⇒ 风险①命中，按第 3 步结果设 `KAFKA_SASL_LOGIN_MODULE`；④ `GroupAuthorizationFailed` / `TopicAuthorizationFailed` ⇒ 风险②（ACL）；⑤ 以上皆无、TM 日志有 `Consumer ... assigned partitions` 且 Redis 键出值 ⇒ 全链路闭环，回填 V1.19.0/V1.19.1/V1.19.2/V1.19.5/V1.19.7 五条的服务器验证项；⑥ 键仍缺但消费已到 ⇒ 转回 V1.19.7 风险①（`ctx.timestamp()`）。

实机验证回填（2026-10-12 第二轮实录，**V1.19.8 主体已生效**）：
- `up -d --build --force-recreate` 仍 10.2s（四层 CACHED、仅 `COPY flink-jobs/hunter_flink` 0.9s + 第四道自检 1.7s 重跑，build context 由 43.82kB 增至 **51.96kB** = `job_conn.py`/`test_job_conn.py` 已入镜像）；
- `env | grep -oE "^(KAFKA|REDIS)_[A-Z_]+"` → **9 个键齐备** ⇒ D 组（compose 注入）落地；
- jar 内省输出 `['org/apache/flink/kafka/shaded/org/apache/kafka/common/security/scram/ScramLoginModule.class']` —— 与 `DEFAULT_SCRAM_LOGIN_MODULE` **逐字一致** ⇒ **风险①（shaded 类名未核实）正式解除**，无需 `KAFKA_SASL_LOGIN_MODULE` 覆盖；
- TaskManager 日志的 KafkaConsumer 配置转储里出现 **`sasl.jaas.config = [hidden]`** 与 **`sasl.mechanism = SCRAM-SHA-512`** ⇒ 两条硬结论：① `set_property` 链确实把 SASL 属性随 JobGraph 带到了 **TM 侧的客户端**（B/C 组生效；`[hidden]` 是 Kafka 客户端对敏感项的掩码，它**出现即代表已配置**），② 本轮实录未含任何 `SaslAuthenticationException`/`Authentication failed`；
- 重提交后三作业拿到新 JobID（`a651af31…` / `d114e97a…` / `f47b8f1e…`）并全部 RUNNING；
- ⚠ **但“已出数”仍未证实**，且末条命令暴露了一个新问题：`AUTH failed: WRONGPASS invalid username-password pair or user is disabled.` —— 这与上一轮的 `NOAUTH` 不同：命令**已带口令却被拒**，⇒ `.env` 的 `REDIS_PASSWORD` 与**运行中 redis 容器的 `requirepass` 不同代**（最可能原因：只 `--force-recreate` 了 flink-jm/flink-tm，而 redis/六个业务容器仍是创建时渲染的旧口令），或者我给的取值命令 `grep .env | cut -d= -f2-` 被引号/空格/CRLF/重复键污染。两种成因尚未分流（靠 `bash scripts/health-check.sh` 的 Redis 行一键可分），风险②（ACL）与⑤（`ctx.timestamp()`）**均未判定**。
- ⚠ **本条命令块里两条命令自身写错了**（属本条登记范畴的缺陷，不是环境问题）：① “先 cancel 再重建”的顺序不对——已 `--force-recreate flink-jm` 后旧作业随 JM 内存消失，三条 cancel 各刷约 100 行 `FlinkJobNotFoundException` 栈；② “TM 日志 grep 看是否无 SASL 报错”不可靠——`grep -iE "sasl|…" | head -30` 的名额被配置转储占满（每个 key 都叫 `sasl.*`），**真错误行反而落在名额之外**，所以上面第 4 点的“未见认证错误”证据强度有限。两者均在 **V1.19.9** 修正，后续请按修正后的命令执行。

**待办（不属本条范围）**：① Kafka SASL 用户的 ACL 授权清单需与 `contracts/kafka/consumer-groups.yaml` 逐项对账（现在只保认证、未保授权）；② sink 投递语义是否升 `AT_LEAST_ONCE`（V1.19.7 待办①遗留）；③ `ctx.timestamp()` 仍未判（V1.19.7 风险①）；④ K8s 形态下口令进 JobGraph 的隔离方案（专业集群应用 TM 侧 `flink-conf` 而非属性下发）；⑤ `rules.py:167` mypy 存量；⑥ V1.19.0～V1.19.2 与 V1.19.5/V1.19.7 的服务器验证项，需待本条实机出数后统一回填归档。


## V1.19.9（修复：V1.19.8 给出的三条核验命令自身不可用——取消时机、日志名额、口令取值；并登记一个新发现的契约级隐患）

Date：2026-10-12
变更类型：修复（**纯文档与运维命令**，无运行时代码/编排/契约变更）

现场（V1.19.8 实机回填那一轮的同一份实录）：修复本身已验证生效（9 个凭据键齐备、shaded 类名逐字一致、TM 侧 `sasl.jaas.config = [hidden]`），但实录同时出现三幕“**看得到命令不对、看不到结论**”：
① 三条 `flink cancel <旧 JobID>` 各抛约 100 行 `FlinkJobNotFoundException: Could not find Flink job`（刷屏且极易误读为新故障）；
② `grep -iE "sasl|authenticat|principal|authorization" | head -30` 返回的 30 行**全是** KafkaConsumer 配置转储（`sasl.login.*`/`sasl.kerberos.*` 每个 key 都命中），**真正的错误行根本拿不到证据**；
③ `grep "^REDIS_PASSWORD=" .env | cut -d= -f2-` 取出的口令被 redis 拒为 **`WRONGPASS`**（与上一轮的 `NOAUTH` 属两类问题）。

根因（均在命令本身，不在作业）：
① **compose 形态的 JM 是 standalone、无 HA**：`--force-recreate flink-jm` 连同上轮作业一起销毁，之后 cancel 必失败——我给的步骤把 cancel 排在 recreate 之后，时序错；
② **日志 grep 的“名额”被配置转储占满**：客户端启动会打印整份配置（约 30 个 `sasl.*`/`ssl.*` 项），任何靠 `head -N` 截断的关键词 grep 都无法区分“没报错”与“报错被截掉了”；
③ **自己解析 `.env` 取口令天生不可靠**（值含引号/空格/`#`、键重复、CRLF——本仓 `.gitattributes` 只对 `*.sh`/`*.bash` 强制 LF，`.env` 不在内），且它**只能证明命令对不对、不能证明 `.env` 与运行中容器同代**；而“只重建 flink、不重建 redis”恰好会制造后者（各容器的 `${REDIS_PASSWORD}` 都是在**创建时**渲染的）。

修复内容（受追踪 **2 个** + `docs/` 两份，四组）：
- **A `docs/故障排查指南.md`**（「RUNNING 不出数」四步全重写）：第 2 步改为“只看三项取值”（`security.protocol|sasl\.mechanism|sasl\.jaas\.config`，期望 `SASL_PLAINTEXT` / `SCRAM-SHA-512` / `[hidden]`）+ “只看错误行”（`grep -E "ERROR|Exception|FAILED" | grep -viE "Unknown module|jdk\.compiler"`），并明写“绝不能用 `grep -i sasl | head -30`”；第 3 步补上已实机确认的 shaded 输出作为对照基准；第 4 步改为从**运行中容器的 `/proc/1/cmdline`** 取口令（不解析 `.env`、不回显口令）+ 先用 `bash scripts/health-check.sh` 的 Redis 行分清“口令漂移”还是“命令错”，并给出两条分支处置（以容器为准改 `.env`，或以 `.env` 为准重建 redis **并同时 recreate 六个业务服务**）；新增「`flink cancel` 的时机」整段与「消费组共用问题」登记段（含决定性 `--describe` 命令）。
- **B `docs/运维操作手册.md` §3.6**：出数核验命令换为进程取值式 + `health-check.sh` 兑底；`flink cancel` 行旁标注“必须在不重建容器时用”；代码块后新增取消时机与口令漂移两条注记。
- **C `flink-jobs/README.md`**：纪律段新增两条（「取消作业的时机」、「判“已出数”不看 RUNNING——真判据只有 TM 配置三行与产物键」）；compose 命令块补出数核验行；并把“K8s 形态待人工确认项”里**已过时**的「Kafka SASL 凭据注入路径缺失」标为已于 V1.19.8 关闭（原述仍写着“作业当前仅读 `KAFKA_BOOTSTRAP_SERVERS`”）。
- **D 仓库根 `README.md`（验证命令速查表）**：两行同类缺陷一并修——`docker exec hunter-redis redis-cli ping`（必回 NOAUTH）改指 `health-check.sh` 并注明完整命令位置；Kafka Topic 行补上 `--command-config /tmp/hunter-client.properties`（缺它则明文连 SCRAM 报 `Timed out waiting for a node assignment`，本仓已第三次踩此坑）。

新发现（**已登记、尚未修**，待人工确认）：**两个已实现作业共用同一 `group_id`**。契约 `contracts/kafka/consumer-groups.yaml` 把 `vehicle_state_monitor`/`driving_anomaly_detection`/`collision_risk_assessment`/`algorithm_performance_monitor` 四个作业全部列在 `data-analytics-telemetry` 一个组下且均订阅 `telemetry_clean`（12 分区），而 `detection_job.py:34` 与 `algorithm_performance_job.py:38` 确实都写着 `CONSUMER_GROUP = "data-analytics-telemetry"`。两个独立 Kafka 客户端共用 `group.id` 的后果取决于连接器是否把分区交给 broker 分配：无论哪种，**提交位点都会互相覆盖**（组 `LAG` 监控失真、冷启动 `committed_offsets(EARLIEST)` 的起点可能被另一作业带偏）；若为 broker 分配则进而是**两作业各只拿到一半分区**（告警覆盖面默默腰斩）。已给出决定性判定命令（看 12 行的 `CLIENT-ID`/`HOST` 是否分属两作业，或是否 `has no active members`），**修不修、怎么修（拆组需同步改契约与本目录常量，并连带 ACL）待确认后再动**，所以本轮未改任何代码，仅作登记。

向后兼容：① **零运行时代码/镜像/编排/契约变更** —— 不需要 `--build`、不需要 recreate，`git pull` 后直接按新命令复核即可；② 旧命令不删只注错（故障排查指南里同时写明“绝不要用”的旧式及原因），避免历史上拷过旧命令的运维脚本静默失效而无人察觉；③ 本条不改 `health-check.sh`/`common.sh` 任何一行（其 Redis 行已使用 `load_env` 归一 LF 后的值，正好可用作口令一致性的现成判据）。

风险与待人工确认：① **口令漂移的真实成因本条无法定论**（要么 `.env` 与容器不同代、要么旧命令被字符污染），靠下面第 1 步一次分流；② 若确认是漂移，**六个业务容器可能同样带着旧口令**，处理方式需整机统一对齐（只修 flink 会把问题掩盖）；③ 消费组共用的实际影响面未实测（取决于 `flink-sql-connector-kafka` 5.0.0 是自行分配还是交给 broker）；④ 本轮新增的两条 grep 组合仍依赖“作业启动后日志未滚出 `--since` 窗口”，若已滚走需先重提交再看；⑤ `redis-cli -a` 会把口令出现在容器内 `ps` 中（单次排查可接受，不得写入脚本仓库）。

验证（本环境，均实跑）：`ruff check flink-jobs` **All checks passed**；`run_unit_tests.py flink-jobs` **58 passed, 1 skipped**（与 V1.19.8 同——本条未改任何 Python）；`verify_infra.py` **16 项全 PASS**（含 ConfigMap 敏感字段拦截，确认本轮未往清单里添敏感值）；`render_k8s.py --check` **PASS**；`git status` 受追踪恰好 2 个（`README.md`、`flink-jobs/README.md`）——与“纯文档修复”一致，无越界改动；mypy 未重跑（本轮无代码变更）。

分发路径注意：受追踪 **2 个**：`README.md`、`flink-jobs/README.md`；另 `docs/故障排查指南.md`、`docs/运维操作手册.md` 与 `release.md` 被 `.gitignore` 忽略，需单独 rsync。**本条不需重建任何镜像或容器**（与 V1.19.8 不同）。

服务器续跑步骤（本轮不需 `--build`/recreate，只做判定与取值）：
```bash
cd /opt/HunterCore && git pull
# 1) 一分流：口令漂移还是命令错？（看 Redis 行：PASS=漂移不存在、FAIL=漂移）
bash scripts/health-check.sh | grep -i redis
# 2) 取真口令读指标（不回显口令；不解析 .env）
sudo docker exec hunter-redis sh -c 'p=$(tr "\0" "\n" </proc/1/cmdline | grep -A1 -- --requirepass | tail -1); \
  redis-cli --no-auth-warning -a "$p" GET analytics:ingest_latency'
# 3) SASL 到底有没有报错（只看三项取值 + 只看错误行，不用 head 截关键词）
sudo docker logs --since 10m hunter-flink-tm 2>&1 | grep -E "security\.protocol|sasl\.mechanism|sasl\.jaas\.config" | sort -u
sudo docker logs --since 10m hunter-flink-tm 2>&1 | grep -E "ERROR|Exception|FAILED" | grep -viE "Unknown module|jdk\.compiler" | head -20
# 4) 分区是否被两个作业拆开（消费组共用问题的决定性一击）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  kafka_tool_cli kafka-consumer-groups.sh --describe --group data-analytics-telemetry'
# 5) 有数据吗？（没数据时上面全绿也不会有 Redis 键，不属故障）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  kafka_tool_cli kafka-get-offsets.sh --topic telemetry_clean'
```

判据预告：① 第 1 步 FAIL 而第 2 步 PONG/有值 ⇒ **口令漂移坐实**（`.env` 已不是运行中口令）—— 需先定“以谁为准”再统一 recreate，否则 flink 写不进、服务读不到；② 第 1 步 PASS ⇒ 先前 WRONGPASS 纯属旧命令字符污染，第 2 步应直接出值；③ 第 4 步 12 行 `CLIENT-ID`/`HOST` 分属两作业 ⇒ 分区被拆、**必须拆组**（同步改契约）；报 `has no active members` ⇒ 仅监控口径受影响；④ 第 5 步全 0 ⇒ **上游无数据**，看板 `-` 与 Redis 键缺失属正确行为（作业只能算已到达的样本，不得伪造）；⑤ 全部正常但 Redis 仍无键 ⇒ 转回 V1.19.7 风险①（`ctx.timestamp()`），改走 Table API metadata。

**待办（不属本条范围）**：① 消费组是否拆为每作业一个 `group_id`（需同步改 `contracts/kafka/consumer-groups.yaml` 与三作业常量，连带 SASL 用户 ACL）；② `.env` 与运行中容器口令的**对齐动作**（归到部署流程而非本轮）；③ 可考虑把上面的四项判定固化为一仓内脚本（现在仍靠文档粘贴，易再写错）；④ V1.19.8 待办①~⑥ 全部遗留（本条未触碰代码）。（本条待办①已于 V1.19.10 按用户决策启动）

---

## V1.19.10（修复：消费组共用——契约把 4 个作业挂在同一 `group_id`、两个已实现作业确实共用；拆为“一组一作业”）

Date：2026-10-12
变更类型：修复（**契约语义变更**，已经人工确认；含运行时代码与 K8s 清单，需重建镜像）

现场：V1.19.9 登记的契约级隐患（`data-analytics-telemetry` 一个组下列着 4 个作业，而 `detection_job` 与 `algorithm_performance_job` 两处的 `CONSUMER_GROUP` 确实逐字相同）经取确认后报给用户，用户决策：**「立即拆为每作业一组」**（未选“先实测影响面再动”）。本轮无新实机回跑，全部结论来自代码/契约取证与仓内门禁。

根因（两层，第一层在契约文件头本身）：
① `contracts/kafka/consumer-groups.yaml` 文件头原写着“**每个 (服务, 数据域) 一个消费组**”，这个抽象层级就是错的——`group.id` 是**消费进度（committed offset）的命名空间**，与“逻辑归属/数据域”无关；按“域”归组必然把同域多作业挤进一组；
② 两个独立 KafkaConsumer 实例共用一组的后果：**提交位点互相覆盖**（组 `LAG` 监控失真、冷启动 `committed_offsets(EARLIEST)` 的起点被另一作业带偏）；若 `flink-sql-connector-kafka` 把分区分配交由 broker，则进一步是**两作业各只拿到一半分区且不报任何错**（告警覆盖面腰斩）。两者都属于“作业 RUNNING 但结果错”，与本系列一贯反对的“假绿”同族。

修复内容（五组，受追踪 **11 个**）：
- **A 契约（单一事实来源）**：`contracts/kafka/consumer-groups.yaml`——单组拆为 `data-analytics-telemetry-state`/`-alert`/`-collision`/`-algo` 四组（每组 `jobs` 只一项），新增 `status:` 字段（已交付/部分交付/占位，并点名实现文件），文件头命名规范修正为“**一个消费组只承载一个作业**” + 写入 ℞ 根因注释；`contracts/kafka/README.md`——消费组表重写为 8 行（含此前漏行的 `data-analytics-algorithm-metrics`）+ “一组一作业”引用块 + 两层历史修正注记；`contracts/openapi/data-analytics.yaml`——`x-hunter-realtime-jobs.input` 与 `x-hunter-kafka.consumes` 各拆 4 条，未实现作业置 `enabled: false` + `reason`（禁止静默消失），pending #12 追加 `contract_revision`；`infra/k8s/base/03-configmap-contracts.yaml` 重生成（内嵌的旧组已同步）。
- **B 实现侧**：`detection_job.py` `CONSUMER_GROUP` → `data-analytics-telemetry-alert`；`algorithm_performance_job.py` → `data-analytics-telemetry-algo`（两处均附根因注释、docstring 拓扑行、source builder 行内注释）；`flink-jobs/README.md` 作业表五行组名 + 「⚠ 一组一作业」警告块。
- **C 门禁固化（把纪律变成不可回退的断言）**：`test_data_analytics_contract.py`——data-analytics 组数 `5 → 8`、删旧组名断言、新增“四组各含且仅含一个作业”、“契约 `status` ↔ openapi `enabled` 逐项一致”、“`enabled=false` 必须给理由”；**新增** `flink-jobs/tests/test_consumer_group_contract.py`（9 例）：三作业 `CONSUMER_GROUP`/`INPUT_TOPIC` 与契约组条目逐项相等、`group_id` 正则与“不得由环境变量漂移”、**全契约范围“一个 group_id 不得挂多个作业”**、openapi `consumes` 与组条目逐条一致、已交付组必 `enabled=true` 且 `status` 点名实现文件。
- **D 悬空声明清理**（拆组直接造成的副作用）：`infra/k8s/services/data-analytics.yaml` 与 openapi `required_env` 删除 `KAFKA_CONSUMER_GROUP_TELEMETRY: data-analytics-telemetry`——该变量服务代码从不读（只读 `KAFKA_CONSUMER_GROUPS` 逗号清单），而拆组后它指向一个**已不存在的组**；清单里补注指明 Flink 作业消费组的唯一事实来源。
- **E 文档**：`docs/故障排查指南.md` 该段从「已发现、尚未修」改为「V1.19.10 已修」并给出三步迁移动作；`docs/运维操作手册.md` §3.6 新增「作业消费组」bullet（两条纪律：组名在 JobGraph 内、新组无位点会回放）；`docs/deployment.md` 新增 V1.19.10 部署影响注记。

向后兼容：① **作业消费组名变更属运行语义变更**，必须 `--build --force-recreate flink-jm flink-tm` 才生效（`restart` 不重读代码）；② 旧组 `data-analytics-telemetry` 在 broker 上若有提交位点会成为**孤儿位点**（不影响新组，但 `--describe` 永远显示 `no active members`）；③ openapi 数组长度变化（`consumes` 6→9 条、`input` 2→5 条）——任何按长度写死的外部校验脚本需复核；④ `KAFKA_CONSUMER_GROUP_TELEMETRY` 删除属 ConfigMap 键移除，服务不读、无行为变化，但 K8s 侧需重新 apply；⑤ compose 无需改 `.env`（无新增变量）；⑥ 幂等口径与下游写入链路未动（回放不产生脏数据）。

风险：① **新组首次启动无提交位点** ⇒ `committed_offsets(EARLIEST)` 会**回放 `telemetry_clean` 至多 7 天保留 × 12 分区**（本条最需预告的一项，续跑第 2 步就是为它）；② 回放会产生一次 `alert_event` **历史告警重投**（`(vehicle_id, time)` 幂等 + `ON CONFLICT DO NOTHING` 不造脏数据，但下游 `platform-alert-event` 与前端推送会看到尖峰），不接受就先 `--reset-offsets --to-latest --execute`；③ 若 SASL principal 的 ACL 是按“Topic + group”粒度授予，拆组需补授权——本仓 `init-kafka.sh` 未做 group 级 ACL（已核），compose 环境无此风险，**云上/托管 Kafka 需单独核对**；④ V1.19.9 那个“分区是否已被拆过”的实测仍未做，且**现已无法回溯**（拆组后每组独占 12 分区）——只能保证拆后不再发生，不能证明拆前已发生；⑤ 占位组 `-state`/`-collision` 现在“契约里有、集群里没有”，`--describe` 会报 no such group，属预期（`enabled: false` 已声明），不得当故障处理。

验证（本环境，均实跑）：`ruff check services/data-analytics flink-jobs` **All checks passed**；`run_unit_tests.py flink-jobs` **67 passed, 1 skipped**（原 58，+9 为本条新增契约用例）；`services/data-analytics` **94 passed**（原 93）；全量 9 目标全绿（326 / 无单元用例 / 67+1s / 71 / 108 / 143 / 94 / 128 / 97）；`generate_contracts_configmap.py --check` **PASS**；`verify_data_layer.py` **通过 34 项、失败 0**（含“消费者组契约合法：16 个组，订阅 Topic 均已登记、幂等键齐备”）；`verify_infra.py` **16 项全 PASS**；`render_k8s.py --check` **PASS**；`git status -uall` 受追踪恰好 11 M + 1 新（与“契约+代码+文档”三层覆盖一致）。mypy 未重跑（本轮未触碰 `rules.py`，已知的 `rules.py:167` dict-item 仍遗留）。

分发路径注意：受追踪 **12 个**：`contracts/kafka/consumer-groups.yaml`、`contracts/kafka/README.md`、`contracts/openapi/data-analytics.yaml`、`infra/k8s/base/03-configmap-contracts.yaml`、`infra/k8s/services/data-analytics.yaml`、`flink-jobs/hunter_flink/detection_job.py`、`flink-jobs/hunter_flink/algorithm_performance_job.py`、`flink-jobs/README.md`、`flink-jobs/tests/test_consumer_group_contract.py`（**新文件，rsync 易漏**）、`services/data-analytics/app/tests/test_data_analytics_contract.py`、`services/data-analytics/app/config.py`（属 V1.19.11，与本条同批）、`README.md`（属 V1.19.9，若上轮已同步则不重复）；另 `docs/` 三份与 `release.md` 被 `.gitignore` 忽略，需单独 rsync。

服务器续跑步骤（**顺序不能倒**；先 cancel 后 recreate 是 V1.19.9 的时序纪律）：

> ℞ **勘误（由 V1.19.12 追加，保留原文以备溯源）**：本块第 2/5 步直接写 `kafka_tool_cli kafka-consumer-groups.sh …` 而漏了 **REQUIRED 的 `--bootstrap-server`**（实机表现为刷出整份 usage、任何集群信息都拿不到）；第 1 步的 `flink list` 默认**不列 FAILED 作业**，应加 `-a`。请直接按 **V1.19.12 的续跑块**执行。
```bash
cd /opt/HunterCore && git pull
# 1) 旧组不再使用，先取消三作业（此时 JM 还未重建，cancel 不会报 JobNotFound）
sudo docker exec hunter-flink-jm flink list
for id in a651af312f06189813e3cf0f9ff3df1c d114e97a27a86eb5778825993b98de3c f47b8f1ef2ca434fc129f9c30972e791; do
  sudo docker exec hunter-flink-jm flink cancel "$id" 2>&1 | grep -vE "^\s+at " ; done
# 2) 不想回放就先把两个新组起点置到 latest（否则启动后回放至多 7 天遥测）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo; do
    kafka_tool_cli kafka-consumer-groups.sh --group "$g" --topic telemetry_clean \
      --reset-offsets --to-latest --execute
  done'
# 3) 重建镜像与容器（组名在 JobGraph 里，必须 --build）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build --force-recreate flink-jm flink-tm
# 4) 重提交三作业（api_guard 已在构建阶段跑过）
for j in detection algorithm_performance ingest_latency; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec \
    flink-jm flink run -d -m localhost:8081 -pym hunter_flink.${j}_job; done
# 5) 逐组核验：每组只应出现**一个**作业的 CLIENT-ID
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo data-analytics-telemetry-raw; do
    echo "== $g"; kafka_tool_cli kafka-consumer-groups.sh --describe --group "$g"
  done'
```

判据预告：① 第 1 步若仍刷 `FlinkJobNotFoundException`，说明旧 JobID 已随上轮 recreate 消失（无害，直接进第 2 步）；② 第 2 步成功输出应含 `new offset = <末端水位>`，否则第 3 步必回放；③ 第 5 步三组均应有 `CURRENT-OFFSET` 随时间推进且 `CLIENT-ID` 不交叉（出现交叉 = 跑的仍是旧镜像，回去查 `--build` 是否真生效）；④ `-alert` 的 `LAG` 持续增大而作业 RUNNING ⇒ 回到 V1.19.8 凭据链（看 TM 三行取值）；⑤ `-state`/`-collision` 报 no such group 属**正常**；⑥ 本条与 V1.19.11 需一同生效才能在看板上看出效果（只重建 flink 不重建 data-analytics，`kafka_consumer_lag` 仍列幽灵组）。

**待办（不属本条范围）**：① 旧组 `data-analytics-telemetry` 的孤儿位点清理（确认无需回放历史后再 `--delete-offsets`）；② 新作业上线时必须同时改四处：契约组条目、作业 `CONSUMER_GROUP`、openapi `consumes`、（V1.19.11）看板 LAG 清单；③ SASL principal 的 group 级 ACL 仍无声明式清单（`init-kafka.sh` 只建用户与 Topic 权限）；④ V1.19.7 风险①（`ctx.timestamp()`）、sink `AT_LEAST_ONCE`、`rules.py:167` mypy dict-item 三项继续遗留。

---

## V1.19.11（修复：看板 LAG 探测长期监控六个**契约中不存在的幽灵消费组**，一律读出 0 延迟——假绿）

Date：2026-10-12
变更类型：修复（运行时代码默认值 + 契约用例；需重建 data-analytics）

现场：本条不是用户报障，而是 V1.19.10 拆组时追 `group_id` 引用面顺藤摸到的：`services/data-analytics/app/config.py` 的 `kafka_consumer_groups` 默认值写着六个组名（`analytics-stream-consumer`、`alert-engine-consumer`、`ota-status-consumer`、`telemetry-clean-group`、`telemetry-raw-failure-group`、`corner-case-miner-consumer`），并在注释里声称来源是契约的 `x-hunter-kafka.groups`——**而 openapi 里根本没有 `groups` 这个键**（只有 `consumes`/`produces`）。逐个对回 `contracts/kafka/consumer-groups.yaml`：六个名字**一个都不存在**；data-collector 的同名变量则由 `infra/k8s/services/data-collector.yaml` 显式注入且被 `test_data_collector_contract.py:629` 断言与契约一致——**只有 data-analytics 这一侧既没接线也没门禁**。

根因：`KafkaLagProbe._lag_sync` 对每个组调 `list_consumer_group_offsets`，而 **Kafka 对不存在的组返回空分区列表而不报错**（不是错误、不是 None），循环于是 `total_lag += 0` 后把组名写入结果——看板 pipeline 块四块全部 available、`kafka_consumer_lag` 却永远是六个 0。这与 V1.19.8（RUNNING 假绿）、V1.19.9（grep 假阴）同族：**“看得到健度读数”不等于“监控到了真东西”**。另需记清：`AlgorithmMetricsConsumer` 用的是自己的 `ClassVar ALGORITHM_METRICS_GROUP_ID`，不读本变量，故改默认值**不会影响任何实际消费**，只影响看板读数。

修复内容（受追踪 **2 个**）：
- **A `services/data-analytics/app/config.py`**：`kafka_consumer_groups` 默认值换为契约中**已交付/部分交付**的四个组：`data-analytics-telemetry-alert`、`data-analytics-telemetry-algo`、`data-analytics-telemetry-raw`、`data-analytics-algorithm-metrics`；占位组 `-state`/`-collision` 不列入（否则只刷 `consumer_group_offset_fetch_failed` 日志）；注释改写为 ℞ 全记录（含“空分区不报错 → 恒 0 假绿”的机制），`kafka_consumer_group_list` 的 docstring 指向从不存在过的 `x-hunter-kafka.groups` 改为 `consumer-groups.yaml`。
- **B 契约用例（防止回到旧状态）**：`test_data_analytics_contract.py` 新增 `test_kafka_lag_probe_monitors_real_contract_groups`：读 `Settings.model_fields["kafka_consumer_groups"].default`（不实例化配置，避免副作用），断言其与“契约中 `status` 以已交付/部分交付开头的 data-analytics 组”**集合相等**，并反向断言六个臆造名不得残留——**契约新增一个已交付组而清单未同步时直接红**（两边必须一起改）。另为 `data-analytics-algorithm-metrics` 与 `data-analytics-telemetry-raw` 补上 `status:` 字段（前者 已交付、后者 部分交付，否则无法从契约推导出这份清单）。
- **C 文档**：`docs/故障排查指南.md` 看板降级表新增「第三因：不再降级但延迟恒为 0」一行（含症状、根因、修复）与「看板 LAG 与集群实况对账」命令块（集群侧逐组 `--describe` + 服务侧直连 8083 读 `pipeline` 块，带 `X-User-Id`/`X-Roles` 身份头，不经网关）；`docs/运维操作手册.md` 与 `docs/deployment.md` 各新增一条同源注记。

向后兼容：① **属代码变更**，`restart` 无效，必须 `up -d --build data-analytics`（app 代码烘入镜像，堆栈路径 `/app/app/...` 已多次证实）；② 看板返回体结构不变，仅 `kafka_consumer_lag` 的 **key 集合**从六个臆造名换成四个真实组——前端/告警脚本若按旧 key 取值会拿到 `undefined`（已核 `frontend/` 无硬引用，看板只渲染整张映射）；③ 无环境变量新增，`.env`/compose/K8s 清单均未动（本来就没有注入这个变量）。

风险：① 四个新组中 `-raw`（部分交付）与 `-alert`/`-algo` 的 LAG 口径是“整组跨全部 12 分区求和”，与旧版“六个空组恒 0”不可直接对比，看板趋势图会出现一阶跃变（不是回归）；② 若 Flink 作业未运行，`--describe` 会报 `has no active members` 但仍可能有提交位点，这时看板显示的 LAG 是“停止消费那一刻至今”的真实差距（属正常语义，勿当故障）；③ 只重建 flink 不重建 data-analytics（V1.19.10 那一步单独做）会出现“集群已拆组、看板仍列幽灵组”的中间态，必须两个服务一起重建；④ **本条未修 `dlq_topic`**：默认 `dead_letter_queue` 与契约的 `{topic}.dlq` 派生规则不符，`_dlq_sync` 对不存在的 Topic 返回 `{}`（不假绿、但看板 DLQ 深度永远为空），已归待办。

验证（本环境，均实跑）：`ruff check services/data-analytics flink-jobs` **All checks passed**；`services/data-analytics` **94 passed**（含本条新增用例；先红后绿——第一次跑出 `Extra items in the left set: 'data-analytics-algorithm-metrics'`，据此发现两个组缺 `status`，补后全绿）；`flink-jobs` **67 passed, 1 skipped**；全量 9 目标全绿（同 V1.19.10）；`verify_data_layer.py` **34 项 PASS**、`verify_infra.py` **16 项 PASS**、`generate_contracts_configmap.py --check` **PASS**、`render_k8s.py --check` **PASS**。（本条与 V1.19.10 同批跑的门禁，未重复执行全量项。）

分发路径注意：受追踪 **2 个**：`services/data-analytics/app/config.py`、`services/data-analytics/app/tests/test_data_analytics_contract.py`（后者与 V1.19.10 同文件，整目录 rsync 会重叠）；`contracts/kafka/consumer-groups.yaml` 的 `status` 补充属本条（也是 V1.19.10 的分发项）；`docs/` 三份与 `release.md` 被 `.gitignore` 忽略。

服务器续跑步骤：

> ℞ **勘误（由 V1.19.12 追加，保留原文以备溯源）**：本块第一段同样漏 `--bootstrap-server`；第二段只带 `X-User-Id`/`X-Roles` 会被拒 **401**（compose 给每个业务服务都注入了 `GATEWAY_HMAC_SECRET`，服务端 `current_user_id` 还校验 `X-Identity-Timestamp` + `X-Internal-MAC`）。两者均在 V1.19.12 修正，请直接按 **V1.19.12 的续跑块**执行。
```bash
cd /opt/HunterCore && git pull
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics
# 对账（先看集群真值，再看服务读数；两边应一致）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo \
           data-analytics-telemetry-raw data-analytics-algorithm-metrics; do
    echo "== $g"; kafka_tool_cli kafka-consumer-groups.sh --describe --group "$g" | tail -n +2
  done'
sudo docker exec hunter-analytics python -c "import json,urllib.request as u; \
  r=u.urlopen(u.Request('http://localhost:8083/api/v1/analytics/dashboard', \
      headers={'X-User-Id':'ops','X-Roles':'analyst'})); \
  print(json.dumps(json.load(r)['data']['pipeline'], ensure_ascii=False, indent=2))"
```

判据预告：① 读数里 `kafka_consumer_lag` 的 key 仍是 `analytics-stream-consumer` 等旧名 ⇒ 跑的还是旧镜像（`--build` 没生效或 `.env` 里有同名变量覆盖）；② 四个键齐但全为 0，而集群侧 `--describe` 的 `LAG` 列不为 0 ⇒ 服务连的 broker 与作业不同侧，查 `KAFKA_BOOTSTRAP_SERVERS`；③ 集群侧报 `has no active members` 且看板有值 ⇒ 正常（位点存在但消费者已停）；④ 服务日志刷 `consumer_group_offset_fetch_failed` ⇒ 清单里混进了集群上还不存在的组（典型是只重建了 flink、新组从未提交过位点），等作业跑出一个位点即消，或先按 V1.19.10 第 2 步 `--reset-offsets` 建立位点。

**待办（不属本条范围）**：① `dlq_topic = dead_letter_queue` 与契约 `{topic}.dlq` 不符（现返空不报错，看板 DLQ 深度永远为空），修它需改为多 Topic 汇总，属行为变更；② `kafka_consumer_groups` 仍靠默认值而非清单注入（compose/K8s 均未设该变量），与 data-collector 的“显式注入 + 契约断言”不对齐，下一步可把 K8s 清单补上并加同形断言；③ 探测类逻辑普遍应区分“值为 0”与“目标不存在”（本次只修了调用方数据，未改 `KafkaLagProbe` 的返回口径），建议在后续版本给 `kafka_consumer_lag` 加 `unknown_groups` 字段。

---

## V1.19.12（修复：`kafka_tool_cli` 只补认证、不补 REQUIRED 的 `--bootstrap-server`——本轮三条运维命令全军覆没，并顺藤摸出仓内两处**从未成功执行过**的同族缺陷；另修「`flink list` 不列 FAILED」与看板 401 的归因）

Date：2026-10-12
变更类型：修复（部署脚本 + 静态门禁 + 文档；不涉及镜像内代码，服务无需为本条重建）

现场：用户回传 606 行服务器实录（V1.19.10 + V1.19.11 续跑步骤的实机执行结果）。五幕里有三幕是**我给的命令本身失效**：
① 第 1 步 `flink list` 只列出 2 个 RUNNING（`a651af31…` driving_anomaly_detection、`f47b8f1e…` data_quality_monitor_ingest_latency），第三个 JobID `d114e97a…` 不在列表；对它 cancel 得到决定性证据 `Job cancellation failed because the job has already reached another terminal state (FAILED)` —— `algorithm_performance_monitor` **早已进入 FAILED 终态**，而我上一轮写的「三作业 RUNNING」判据根本不成立；
② 第 2 步与第 5 步的 `kafka_tool_cli kafka-consumer-groups.sh …`（含重试共两次）都只刷出 `Missing required argument "[bootstrap-server]"` + 整份 usage + 退出码 2 —— 命令**漏了 REQUIRED 参数**，新组的 `--reset-offsets`（防回放）与逐组 `--describe` 对账**一条都没执行成**；
③ 第 6 步读看板返回 `HTTP Error 401: Unauthorized` —— 只带 `X-User-Id`/`X-Roles` 不够；
④⑤ 成功项（无需返工）：新镜像 `hunter-flink/jobs:2.1.1`、三作业重提交（`0d83ecb0…`/`6362a44a…`/`16364217…`）、`up -d --build data-analytics`（240s，`hunter-analytics Started`）。

根因（三层，全都落在「命令层」而非「业务层」）：
- **A 命令层**：`scripts/common.sh` 的 `kafka_tool_cli` 仅通过 `kafka_auth_args` 补 `--command-config`（认证），**不补 `--bootstrap-server`**；同文件里只有 `kafka_topics_cli` 补了它。而 `--bootstrap-server` 对 `kafka-topics/consumer-groups/get-offsets/configs` 一律是 **REQUIRED**，缺它得到的不是「消费组不存在」而是「命令没跑起来」——usage 落 stderr、退出码 2，**与集群健康无关**却极易被读成 Kafka 有问题。
- **仓内同族缺陷两处（本次顺藤摸到，均从未成功执行过）**：`common.sh kafka_consumer_lag()` 写成 `kafka_tool_cli … 2>/dev/null` ⇒ 空输出 ⇒ 恒返 -1 ⇒ `health-check.sh` ⑩「Kafka 消费积压」长期恒 WARN「该消费组尚无 offset 提交（车辆未上报或 data-collector 未消费）」，**把「运维命令写错」报成「车辆没上报」**；`init-kafka.sh create_scram_user()` 同样漏参 ⇒ 退出码 2 ⇒ 每次初始化都走「创建失败」分支（与 release 早期把首个 SCRAM 账号归给 bitnami `KAFKA_CLIENT_USERS/PASSWORDS` 预置的结论互相印证）。
- **判据层**：`flink list` 默认只列 Running/Restarting 与 Scheduled，FAILED/CANCELED/FINISHED 静默消失。这是本仓库第四类假绿（V1.19.8 RUNNING 不出数、V1.19.9 grep 假阴、V1.19.11 幽灵组同族）：**「列表里没异常」不等于「作业健康」**。
- **身份层**：compose 的 `x-business-env` 给六个业务服务全部注入 `GATEWAY_HMAC_SECRET`（`infra/deploy/docker-compose.yml:70`），`current_user_id` 遂在 `X-User-Id` 之外强制 `verify_identity_headers` 校验 `X-Identity-Timestamp` + `X-Internal-MAC`（G-02：规范串 `user_id|roles|trace_id|timestamp`，HMAC-SHA256，防重放窗口默认 300s）。缺头与验签失败**都是 code=1001**，只有 message 不同——我上一轮的命令把响应体丢了，于是无从归因。

修复内容（受追踪 **5 个**）：
- **A `scripts/common.sh`**：新增三个与 `kafka_topics_cli` 同形的封装 **`kafka_groups_cli` / `kafka_offsets_cli` / `kafka_configs_cli`**（各自自动附带 `--bootstrap-server "${KAFKA_BOOTSTRAP:-localhost:9092}"`）；`kafka_tool_cli` 头注补 ℞ 说明「只补认证、不补 REQUIRED」；`kafka_consumer_lag()` 改走 `kafka_groups_cli`，并加**表头门禁**——输出里没有 `^GROUP[[:space:]]+TOPIC` 表头就一律按「没查到」返 `-1`（返回码 1）：旧实现逐行 awk 找不到 LAG 列时会把 usage 文字求和成 **0**，即「PASS LAG=0」假绿，现「真 0」与「没查到」首次被分开；`common_help` 的辅助清单改为四个封装 + 低层入口的职责边界。
- **B `scripts/init-kafka.sh`**：`create_scram_user()` 改走 `kafka_configs_cli`（℞ 注明本函数从未成功过）；`verify_topic()` 的对齐提示文案由裸 `kafka-configs.sh --alter …` 改为 `kafka_configs_cli --alter …`。
- **C `scripts/verify_infra.py` 新增第 14 项 `check_kafka_cli_required_args()`**：把纪律变成门禁——扫 `scripts/*.sh` + `README.md` + `docs/*.md`，两种调用形态（`kafka_tool_cli <tool>` 与直接写工具名，含路径前缀）凡语句内不含 `--bootstrap-server` 即 FAIL；配套 `_shell_statements()` 做反斜杠续行归并与注释行跳过，避免把续行/注释误判成独立语句。**首跑即抓出 8 处**（`docs/deployment.md:154`、`故障排查指南.md:170/179/478/489/578/585`、`运维操作手册.md:403`，其中 4 处是历史文档里一直写错的命令），全部改好后 17 项 PASS。
- **D 文档**：`docs/故障排查指南.md`（SASL 账号行、死信 Topic 行、看板「第三因」行、迁移动作 ②③、对账块整体重写为 `kafka_groups_cli` + `internal_call_headers()` + 打印 HTTPError body、新增「FAILED 作业不列出」专段并给出 **recreate 之前**抓堆栈的命令）；`docs/运维操作手册.md`（§3.4 重写为封装清单 + REQUIRED 警告、§3.6 `flink list` → `flink list -a`、新增 ⚠ 引用块、第 406 行 reset-offsets 改封装）；`docs/deployment.md`（V1.19.10 注记的 reset-offsets 改封装 + 新增 V1.19.12 注记含 `flink list` 纪律）；根 `README.md`（验证命令清单新增「Kafka 消费组 / 位点」行、Topic 行补「推荐走封装」、静态校验行登记该门禁）；`flink-jobs/README.md`（纪律段新增「`flink list` 不列 FAILED 作业」bullet）。
- **E 溯源标注**：V1.19.10、V1.19.11 的「服务器续跑步骤」块前各加一条 `℞ 勘误（由 V1.19.12 追加，保留原文以备溯源）`，原文不删，便于对照「当时给错在哪」。

向后兼容：① 纯运维脚本 + 文档 + 校验器变更，**无接口、无契约、无镜像内代码改动** ⇒ 服务容器不必为本条重建（本轮已做的 flink / data-analytics 重建属 V1.19.10/11，不因本条返工）；② 函数只增不改签名，但 `kafka_consumer_lag` 的返回口径**变严**（新增「无表头 = -1」），health-check ⑩ 从「恒为同一句 WARN」变成「能读真 LAG，或明确 -1」，阈值与文案未动；③ 四个封装统一读 `KAFKA_BOOTSTRAP`（默认 `localhost:9092`），沿用 `kafka_topics_cli` 既有取值，容器内与 `sudo docker exec hunter-kafka` 两条路径均成立，`.env` 无新增变量；④ `verify_infra.py` 会因历史文档中未改净的命令直接红 —— 这是门禁的预期行为，不是回归。

风险：① **`algorithm_performance_monitor` FAILED 的根因至今未定**：失败发生在上一轮 recreate 之前，compose 的 JM（standalone、无 HA）容器一重建日志即丢，堆栈不可回溯；新提交的三作业里 `-algo` 是否再次 FAILED **也无法确认**——唯一核验手段 `--describe` 当时正被缺参数阻断。本条只修「能不能查」，没有修「查出来什么」；② `kafka_consumer_lag` 只探 `data-collector-telemetry` 单组（health-check ⑩ 写死），拆组后的 `-alert`/`-algo`/`-raw` 三组仍不在巡检覆盖内；③ 新封装在忘记 `load_env` 时静默回落 `localhost:9092`，宿主机直跑（非 `docker exec`）会连不上——表现为 -1，不会假绿，但归因得自己想到；④ 看板读数依赖服务自身的 `settings.internal_call_headers()`，若将来网关强制 JWT 且关闭内部签名通道，这条运维路径同样要换；⑤ 门禁只覆盖「缺 REQUIRED 参数」这一类误用，不校验 `--group`/`--topic` 等语义参数是否齐备。

验证（本环境，均实跑）：`bash -n scripts/common.sh scripts/init-kafka.sh scripts/health-check.sh` **OK**；`kafka_consumer_lag` **stub 实测四类 stdout**（临时 harness stub 掉 `kafka_groups_cli` 后 source 真实实现，跑毕即删）——正常 describe → **13（rc 0）**、全分区 LAG=0 → **0（rc 0）**、usage 文本 → **-1（rc 1）**、组不存在 → **-1（rc 1）**；`verify_infra.py` **17 项全 PASS**（91 个文档 + 「Kafka 运维命令均带 --bootstrap-server（21 个脚本/文档）」，改前首跑 FAIL 8 处已全修）；`ruff check scripts flink-jobs services/data-analytics` **All checks passed**；`render_k8s.py --check` **PASS**；`verify_data_layer.py` **通过 34 项、失败 0**；全量单测 **9 目标 1079 passed, 1 skipped**（326 / 71 / 94 / 143 / 128 / 97 / 108 / 45 / 67+1s）——本条未触碰运行时代码，跑作回归门禁。

分发路径注意：受追踪 **5 个**：`scripts/common.sh`、`scripts/init-kafka.sh`、`scripts/verify_infra.py`、`README.md`、`flink-jobs/README.md`；`docs/` 三份（deployment.md、故障排查指南.md、运维操作手册.md）与 `release.md` 被 `.gitignore` 忽略，需单独 rsync。上一批（V1.19.10 + V1.19.11 共 12 个文件）**已随 commit `b0e5319` 提交并推送**，实录显示服务器已 `git pull` 且镜像已重建，无需重复分发。

服务器续跑步骤（本轮已完成的 flink 重建、三作业提交、data-analytics 重建**不必重做**；以下是从这里继续的核验序列，顺序仍不能倒）：

> ℞ **勘误（由 V1.19.13 追加，保留原文以备溯源）**：本块第 5 步的 `bash scripts/health-check.sh` 与 `python scripts/verify_infra.py` **两处都错**：
> ① 未带 `sudo` ⇒ `.env`（权限 600、仅 root 可读）加载失败，实测得到三条**假 FAIL**（Redis PING / Kafka Topic 数 1 < 6 / MinIO Bucket -1）；
> ② Ubuntu 服务器无 `python` 别名（只有 `python3`），原命令直接报 `找不到命令 "python"`。
> 正确写法见 **V1.19.13 的续跑块**；其余第 1~4 步已实机跑成（`-a` 因此抓到 FAILED、封装因此拿到真 LAG）。

```bash
cd /opt/HunterCore && git pull
# 1) 必须 -a（默认不列 FAILED），并核对「提交数 == RUNNING 数」= 3
sudo docker exec hunter-flink-jm flink list -a
#    一旦此处出现 FAILED/CANCELED，先抓堆栈再做任何重建（compose JM 无 HA，容器一换日志即丢）
sudo docker logs hunter-flink-jm 2>&1 | grep -B3 -A40 "switched from RUNNING to FAILED" | tail -n 80
# 2) 逐组对账：走封装，输出应以表头 GROUP TOPIC PARTITION CURRENT-OFFSET LAG … 开头
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo \
           data-analytics-telemetry-raw data-analytics-algorithm-metrics; do
    echo "== $g"; kafka_groups_cli --describe --group "$g" | tail -n +2
  done'
# 3) 抽查封装可用（应回一个数字；回 -1 表示「没查到」，禁止当 0 读）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_consumer_lag data-collector-telemetry; echo'
# 4) 看板对账（G-02：用服务自身的内部签名头，并且必须打印错误响应体）
sudo docker exec -i hunter-analytics python - <<'PY'
import json, urllib.request as ur
from app.config import settings            # WORKDIR=/app，配置单例直读容器环境变量
req = ur.Request("http://localhost:8083/api/v1/analytics/dashboard",
                 headers=settings.internal_call_headers())
try:
    with ur.urlopen(req) as r:
        print(json.dumps(json.load(r)["data"]["pipeline"], ensure_ascii=False, indent=2))
except ur.HTTPError as e:
    print("HTTP", e.code, e.read().decode("utf-8", "replace")[:400])
PY
# 5) 巡检与门禁过一遍（重点：⑩ Kafka 消费积压不再恒为同一句 WARN；Redis 行需分流）
bash scripts/health-check.sh 2>&1 | tail -n 40
bash scripts/health-check.sh 2>&1 | grep -i -E "kafka|redis"
python scripts/verify_infra.py | tail -n 25
```

判据预告：① 第 1 步应列出 **3 个 RUNNING**（对应 `0d83ecb0…`/`6362a44a…`/`16364217…`），数量不足即异常；若 `-algo` 落在 FAILED 里，第 2 步只能看出「位点停在何时」，**原因只能靠 JM 日志**；② 若已无日志可取：`flink cancel` 掉该 FAILED 项后**只重提交这一个作业**、不要 recreate，随即 `sudo docker logs -f hunter-flink-jm` 抓 `switched from RUNNING to FAILED` 的完整堆栈——拿到堆栈再定根因，别凭猜测改代码；③ 第 2 步每组只应出现**一个**作业的 CLIENT-ID 且 `CURRENT-OFFSET` 随时间推进，`-alert`/`-algo` 出现交叉 = 跑的还是旧镜像（回去查 `--build` 是否真生效）；④ 某组报 `does not exist`/`has no active members` 属 V1.19.10 风险①（新组从未提交位点——上一轮 `--reset-offsets` 没执行成、作业也可能没起来），先确认该组作业 RUNNING 再读 LAG；⑤ 第 3 步回 -1 而集群明明有该组 ⇒ 忘了 `load_env`（`KAFKA_BOOTSTRAP`/认证未加载），不是「积压为 0」；⑥ 第 4 步仍 401 时按 body 的 message 分流：「缺少身份头」= 头没带上或 `internal_call_headers()` 未生效；「签名缺失或无效」= 两侧 `GATEWAY_HMAC_SECRET` 不一致，或容器与宿主时钟差超出 300s 窗口；⑦ 服务器上 `verify_infra.py` 若报 Kafka CLI FAIL，说明有人新写了未走封装的命令，按提示改成封装形式，**不要放宽门禁**。

**待办（不属本条范围）**：① `-algo` 作业 FAILED 根因（待第 1/2 步实测证据后另立条目，本轮不登记未经确诊的结论）；② health-check ⑩ 的 WARN 文案仍按旧单一口径描述，需依新语义拆成「没查到」与「积压为 0」两种措辞；③ `kafka_consumer_lag` 只探单组，建议按契约清单逐组探测并汇总；④ **WRONGPASS 分流连续两轮未跑**（`bash scripts/health-check.sh | grep -i redis`），Redis 行的归因仍悬；⑤ V1.19.11 四项（`dlq_topic` ↔ `{topic}.dlq`、`kafka_consumer_groups` 显式注入 + 同形断言、`unknown_groups` 字段、group 级 SASL ACL 清单）与 V1.19.7 三项（`ctx.timestamp()`、sink `AT_LEAST_ONCE`、`rules.py:167` mypy dict-item）继续遗留。（②④ 已在 **V1.19.13** 落地；①③⑤ 仍悬）

---

## V1.19.13（修复：非 sudo 跑巡检时 `.env` 不可读被报成三项「基础设施故障」；`kafka_topic_count` 把一行报错数成「1 个 Topic」；巡检新增 ⑭ Flink 作业健康——本轮实录里只有它能看见 `-algo` 的 FAILED）

Date：2026-10-12
变更类型：修复（部署脚本 + 静态门禁 + 文档；不涉及镜像内代码，服务无需为本条重建）

现场（用户回传 196 行服务器实录，为 V1.19.12 续跑块的实机执行结果，六幕）：
① **`flink list -a` 生效**：Running/Restarting 只有 2 条，`Terminated Jobs` 段抓到 `6362a44a… algorithm_performance_monitor (FAILED)`（11:00:34 提交）——上一轮登记的 `-a` 纪律首次拿到它本该拿到的东西；
② **JM 日志抓到决定性堆栈**：`FlatMap -> Sink: Writer -> Sink: Committer (1/1) … switched from RUNNING to FAILED` → `java.io.IOException: One or more Kafka Producer send requests have encountered exception` → `Caused by: FlinkRuntimeException: Failed to send data to Kafka algorithm_metrics--1@-1 with FlinkKafkaInternalProducer@…{transactionalId='null', transactionState=NOT_IN_TRANSACTION, closed=false}`；`-1@-1`（partition/offset 未知）是**元数据缺失指纹**，提交 11:00:34 → 失败 11:01:37 = **63s ≈ `max.block.ms` 默认 60000** ⇒ 强指向 sink Topic `algorithm_metrics` 在集群上**不存在**（契约 `auto.create.topics.enable=false`，`contracts/kafka/topics.yaml:189`）；但最内层 `Caused by` 被当时那条 `tail -n 80` **截断**，**根因仍未确诊**（本条不登记未确诊结论）；
③ 四个新组 `--describe` 均 `Error: Consumer group '…' does not exist.`（封装本次真的可用，输出是业务结论而不是 usage）；
④ `kafka_consumer_lag data-collector-telemetry` → **`0`**（真值而非 -1，表头门禁成立）；
⑤ 看板 200：`telemetry_points 95940`、`ingest_latency_ms_p95 1112.0`，但 `kafka_consumer_lag` **四个组全为 `0`** —— 与 ③ 的「四个组不存在」直接矛盾（**第七类假绿**，V1.19.11 当时登记的待办③现场兑现）；`dlq_messages: {}`；
⑥ **我给的命令错两处**：`bash scripts/health-check.sh`（漏 sudo）给出 `[FAIL] Redis - PING 失败`、`[FAIL] Kafka Topic 数 - 1 < 6`、`[FAIL] MinIO Bucket 数 - -1 < 7`，而同一轮 sudo 下 `kafka_consumer_lag` 却拿到真值；另有 ⑩ 恒 WARN「尚无 offset 提交」；`python scripts/verify_infra.py` → `找不到命令 "python"`，改 `python3` 后 17 项全 PASS。

根因（四层 + 一层未修）：
- **凭据层（⑥ 三条 FAIL 的唯一成因，与基础设施无关）**：`install.sh:1090` 与 `gen-passwords.sh:178` 都把 `.env` 落成 `chmod 600`、属主 root；而 `health-check.sh` 的 `[ -f "$env_file" ]` **只看目录权限**，对非 root 照样为真，于是 `load_env` 在「存在但不可读」处失败，旧代码只留一句会被 `tail` 掉、也不进健康项的 `log_warn` 就继续跑 ⇒ `REDIS_PASSWORD`/`KAFKA_SASL_*`/`MINIO_ROOT_*` **全空** ⇒ `redis-cli -a ""`、Kafka CLI 无认证、mc 无凭据 ⇒ 本仓**第五例**「命令层错误伪装成业务层结论」。
- **计数层（`1 < 6` 这个数字本身也是错的）**：`common.sh kafka_topic_count` 直接 `--list | wc -l`，失败时 stdout 为空而**一行报错文本**（`Timed out waiting for a node assignment`）经 `2>&1` 被当成「1 个 Topic」⇒ 把认证失败说成「契约 Topic 未创建完整」；`minio_bucket_count` 同族（返 -1 但调用方分不清「真 0 个桶」与「根本没查到」）。
- **判据层**：`flink list` 不列 FAILED 在 V1.19.12 已写入纪律，但**没有任何例行检查执行它** —— ⑬ 的容器清单只含 7 个服务、不含 `hunter-flink-jm`/`flink-tm`，所以 `-algo` 失败对巡检完全透明。
- **文案层**：⑩ 把「没查到」与「车辆未上报 / 尚未提交位点」混为一句（本轮实录里它在 sudo 下也会 WARN，即 V1.19.12 待办②）。
- **看板层（本条未修，已拟方案待决）**：`services/data-analytics/app/repositories/pipeline.py` 的 `_lag_sync()` 对配置里每个组直接 `result[group] = total_lag`，而 `list_consumer_group_offsets()` **对不存在的组返回空分区、不报错** ⇒ 「组不存在」被写成 `0`。修它需要改看板返回体语义（openapi 面），**已单独向用户征询，本条不动代码**。
- **环境层**：Ubuntu 服务器默认无 `python` 别名（`python-is-python3` 未装），文档里的 `python scripts/*.py` 在服务器上均为无效命令。

修复内容（受追踪 **6 个**）：
- **A `scripts/common.sh`**：`load_env()` 新增 `[ ! -r "$env_file" ]` 分支——把「不可读」从「语法错」里拆出来，直接给出 `权限 600、只有属主 root 可读，当前 UID=…` 与 `sudo bash <脚本>` 的可执行结论（并明确禁止用 `chmod` 放宽 `.env` 权限）；新增 **`env_creds_missing()`**（输出为空的口令变量名，含 `CHANGE_ME_*` 占位）；`kafka_topic_count()` / `minio_bucket_count()` 加**输出形态门禁**（改 `2>&1` 取原始输出：`--list` 要求每一行都是合法 Topic 名、`mc ls` 要求桶行以 `/` 结尾，否则一律返 `-1` + 返回码 1，**“没查到”绝不参与计数**）；`common_help` 登记新函数与 sudo 告警。
- **B `scripts/health-check.sh`（主体）**：新增 **⓪ `check_env_credentials()`** 并将其排在 `run_all_health_checks()` 第一位（FAIL 文案直接指出「典型成因是没加 sudo」）；新增 **`hc_skip_without_creds()`** 降级机制——口令缺失时 Redis / Kafka Topic 数 / MinIO Bucket 数 / 消费积压四项一律记 **WARN「未判定」**，不再下「服务异常」结论；`check_redis()` **三分流**（PONG / `WRONGPASS`（含「.env 与现存口令漂移」归因）/ `NOAUTH` / 否则回显原始回复，不笼统说「PING 失败」）；`check_kafka_topics()` / `check_minio_buckets()` 拆开「**查不到清单**」与「清单读到了但数量不足」，并把原始输出（含 stderr）贴进详情；`check_kafka_lag()` 改三分文案（没读到清单 / LAG=0 / 超阈值）；新增 **⑭ `check_flink_jobs()`**：`docker exec hunter-flink-jm flink list -a` + **表头门禁**（无 `Running/Restarting Jobs` → 「未判定」），`FAILED`/`CANCELED` **逐个点名 FAIL** 并附「现在就得抓堆栈（JM 无 HA，容器一重建日志即丢）」命令，`RUNNING < FLINK_EXPECTED_RUNNING`（默认 3）另记 FAIL；main() 不再吞掉 `load_env` 输出、改为 `CREDS_MISSING="$(env_creds_missing)"` 并 export；文件头检查项清单重写为 **15 类**并加「必须 sudo」与 `FLINK_JM_CONTAINER`/`FLINK_EXPECTED_RUNNING` 说明，`usage()` 示例全部带 sudo。
- **C `scripts/daily-check.sh`**：头注 13 类 → **15 类**（点名新增的环境凭据与 Flink 作业健康两项）。
- **D `scripts/install.sh`**：部署完成后的续跑提示改 `sudo bash scripts/health-check.sh` 并写明原因（`.env` 权限 600）。
- **E `scripts/init-kafka.sh`**：`usage()` 里的裸 `docker exec hunter-kafka …/kafka-topics.sh --bootstrap-server localhost:9092 --list`（有 bootstrap 但无认证）改为 `kafka_topics_cli --list` 封装形式，并注明「裸命令缺 `--command-config` 时得到的是 node-assignment 超时，不是权限错」。
- **F `scripts/verify_infra.py` 第 14 项扩展**：仍强制「所有 Kafka CLI 调用必带 `--bootstrap-server`」，并新增第二条：**裸写工具名还必须带认证参数**（或改走封装）。认证参数识别为 `KAFKA_CLI_AUTH_MARKERS = ("--command-config", "$KC", "${KC}", "kafka_auth_args")`——首跑曾因只认字面量而误报 **26 处**（`docs/运维操作手册.md:97` 的 `export KC=…` 与 `docs/故障排查指南.md:13` 约定的 `$KC` 简写），已改为识别简写而非改 20 行文档；ok() 文案同步为「参数齐备」。
- **G 文档（四件套 + 根 README）**：`docs/运维操作手册.md`（§1 新增两个 ⚠ 块：「运维会话必须 root」与「服务器无 `python` 别名」；巡检表「全栈健康检查」行补 sudo 并写明首项判读；**新增「Flink 作业健康（⑭）」行**（含 `flink list -a` 与抓堆栈命令）；§2.2 的 13 类 → 15 类；§7 共 5 处 `bash …/health-check.sh` 补 sudo）；`docs/故障排查指南.md`（§0 新增两条关键事实（必 root / 无 `python`）与会话块 `sudo -i`；§1 第 6 条登记 ⓪ 与 ⑭ 两项新检查；§2/§4/§13/§15/§16 共 6 处命令补 sudo）；根 `README.md`（脚本表 health-check 行改 15 类 + 必 sudo；运维命令块补 sudo；静态校验块加「服务器上请写 `python3`」与门禁新描述）。
- **H 溯源标注**：V1.19.12 的「服务器续跑步骤」块前追加 `℞ 勘误`（第 5 步漏 sudo + `python` 不存在），原文保留；V1.19.12 待办②④ 标注为已在本条落地。

向后兼容：① 纯运维脚本 + 校验器 + 文档变更，**无接口、无契约、无镜像内代码改动** ⇒ 服务容器不必为本条重建；② 健康检查从 13 类变 **15 类**，新增项均为**只增不改**：口令缺失时原来的 3 条 FAIL 变成 1 条 FAIL（⓪）+ 3 条 WARN（未判定）⇒ **退出码仍非 0**（有 FAIL），但语义变了：依赖「FAIL 条数/内容」做自动化判定的脚本需改看退出码或 `环境凭据` 行；③ `kafka_topic_count`/`minio_bucket_count` 由「可能返回一个被污染的计数」改为「查不到就返 -1」——**变量存在但取不到清单的旧行为不再与“数量为 0/1”混淆**；已在 `daily-check.sh` 阈值判定处同样受用（`-1` 不进比较）；④ `verify_infra.py` 门禁变严（新增认证参数一条），历史文档如未带 `$KC`/封装将被拦截；⑤ `.env` 权限仍为 600（**不提供「使 644 以让普通用户可跑」的选项**：口令不应向全体用户开放，正确做法是 sudo）。

风险：① **`-algo` FAILED 仍未确诊**（最内层 `Caused by` 未取到）——本条只保证「巡检能看见 FAILED」与「失败时能拿到原始输出」，不含 Topic 缺失的修复；若第 1 步取证确认 `algorithm_metrics` 不存在，需另立条目（重跑 `init-kafka.sh` 或补建 Topic）；② ⑭ 的 `FLINK_EXPECTED_RUNNING` 默认 3，**写死于本部署形态**（三作业），将来增/减作业不改代码只能靠环境变量，否则会假 FAIL；K8s 形态下 `hunter-flink-jm` 容器名不成立→降级为 WARN（已处理）；③ `hc_skip_without_creds` 会把**真故障**也降级（口令齐备但 Redis 真挂时不受影响，因降级仅在 `CREDS_MISSING` 非空时触发），但若运维手动 `export CREDS_MISSING=…` 会掩盖结论（已只在 main 里预算并 export，探针都改读同一变量）；④ 看板四组恒 0 的假绿**本条未修**（需先定语义）⇒ 在修好之前，看板 `kafka_consumer_lag` 里的 `0` **不可单独作为“无积压”证据**，必须与 `kafka_groups_cli --list` 同读；⑤ 门禁不校验 `$KC` 是否真的指向存在的 `client.properties`，也不校验简写变量名拼写。

验证（本环境，均实跑）：`bash -n scripts/{common,health-check,daily-check,init-kafka,install}.sh` **×5 OK**；`common.sh` 形态门禁 **stub harness 10 项全 OK**（`kafka_topic_count`：合法清单 3 / 一行报错 -1 / 仅 stderr -1 / 空输出 -1 / 含空行 2；`minio_bucket_count`：合法 2 / 报错文本 -1 / 空 -1；`env_creds_missing`：全空 6 项 / 齐备空串）；`health-check.sh` 新函数 **stub harness 20 项断言全通过**（桩掉 `docker`/`minio_mc`/`kafka_topics_cli`/`kafka_consumer_lag` 后 source 真实实现：⓪ 两分支、Redis 五分支（含未判定降级与 WRONGPASS/NOAUTH/原始回复）、Kafka「查不到清单 vs 5<6 vs PASS」、MinIO「查不到清单 vs 2<7」、⑩ 三分文案、⑭ 五分支（FAILED 点名 + 提示 `-a` + `RUNNING=2<3`、3 RUNNING PASS、无表头未判定、容器不存在 WARN））；`verify_infra.py` **17 项全 PASS**（末项「Kafka 运维命令参数齐备（--bootstrap-server 必需；裸命令另需认证参数 --command-config/$KC，21 个脚本/文档）」）；`ruff check scripts tests` **All checks passed**；`render_k8s.py --check` **PASS**；`verify_data_layer.py` **通过 34 项、失败 0**；全量单测 **9 目标 1034 passed / 1 skipped / 0 failed**（common 326、flink-jobs 67+1s、api-gateway 71、scene 108、data-collector 143、data-analytics 94、ota 128、remote-control 97；`tests` 目标 `-m unit` 收集 0 条（100 deselected，退出码 5 按约定视为通过）——较 V1.19.12 记录的 1079 恰少 45，差额全部落在该目标；本条未触碰 `tests/` 与运行时代码，已列入待办复核口径）。另：`ruff check common/python/hunter_common` 报 **5 处存量** UP046/UP047（`database/repository.py:94/114`、`responses.py:26/36/51`，建议改 PEP 695 泛型语法），**非本条引入**（本条未改该两文件）。

分发路径注意：受追踪 **6 个**：`scripts/common.sh`、`scripts/health-check.sh`、`scripts/daily-check.sh`、`scripts/install.sh`、`scripts/init-kafka.sh`、`scripts/verify_infra.py`（另 `README.md` 属受追踪文档，一并入库）；`docs/` 两份（运维操作手册、故障排查指南）与 `release.md` 被 `.gitignore` 忽略，需单独 rsync 到服务器。本条**不需重建任何镜像**，服务器 `git pull` + rsync docs 即生效。

服务器续跑步骤（从这里继续）：

```bash
cd /opt/HunterCore && git pull            # docs 两份如未入库需单独 rsync
# 1) 【取证 A】最内层 Caused by（不被 tail 截断的写法）——这个结论决定 -algo 根因
sudo docker logs hunter-flink-jm 2>&1 | grep -A120 "switched from RUNNING to FAILED" | grep -A6 "Caused by" | head -n 60
# 2) 【取证 B】集群上到底有哪些 Topic / 哪些消费组（两个 --list 是一切结论的地基）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_topics_cli --list'
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_groups_cli --list'
# 3) 若第 2 步里没有 algorithm_metrics：按契约补建（幂等，不碰已有 Topic）后只重提交 -algo
cd /opt/HunterCore && sudo bash scripts/init-kafka.sh
sudo docker exec hunter-flink-jm flink list -a          # 确认旧的 FAILED 条目
cd /opt/HunterCore && sudo docker exec hunter-flink-jm flink run -d -m localhost:8081 -pym hunter_flink.algorithm_performance_job
# 4) 新巡检（⚠ 必须 sudo）：重点看首项 ⓪ 与新增 ⑭，以及 ⑥/⑦ 是否变成带原始输出的 FAIL
sudo bash scripts/health-check.sh 2>&1 | tail -n 40
sudo bash scripts/health-check.sh 2>&1 | grep -iE "环境凭据|flink|kafka|minio|redis"
# 5) 门禁（服务器上写 python3）
python3 scripts/verify_infra.py | tail -n 6
```

判据预告：① 第 1 步只要出现 `TopicAuthorizationException` / `UNKNOWN_TOPIC_OR_PART` / `TimeoutException`三者之一就能定性：**前者是 ACL、中者是 Topic 不存在（与 `-1@-1`+60s 超时的指纹一致）、后者是网络/bootstrap**；若拿到的是 `NotLeaderForPartitionException`/`LeaderElection` 类，则回到 broker 日志；② 第 2 步的 Topic 清单必须含 `algorithm_metrics`（契约 6 分区）与 `telemetry_clean`；消费组清单**应至少含** `data-collector-telemetry` 与两个 RUNNING 作业的新组，若只看到 `data-collector-telemetry` ⇒ 与 ③ 的「四个组不存在」一致，说明 `-alert`/`-raw` 两个作业虽 RUNNING 但**从未提交过位点**（下一道坡）；③ 第 4 步在 sudo 下应该看不到 `Redis - PING 失败`；如果仍 FAIL，新文案会直接给出 WRONGPASS/NOAUTH/原始回复，**按那句归因而不是按标题猜**；④ ⑭ 项在三个作业齐备后应回 `RUNNING=3（期望 3），无 FAILED/CANCELED 终态`；若回 `RUNNING=2 < 3` 而列表里又无 FAILED（即作业被 cancel/完退），才是真的「作业不在跑」；⑤ **看板上四个全 0 仍不可信**（本条未修），对账一律以 `kafka_groups_cli --list` 为准；⑥ 若第 5 步报出新的 Kafka CLI FAIL，说明有人又写了不带认证参数的裸命令，**不要放宽门禁**，改成封装。

**待办（不属本条范围）**：① 看板 `_lag_sync()` 把「组不存在」写成 `0` 的假绿（第七类）——修它涉及返回体/openapi 语义，**已向用户给出三方案待选择**；② `-algo` FAILED 根因确诊（待取证 A/B）；③ 若 Topic 缺失属实：`init-kafka.sh` 需补一个「按契约对账并输出缺失清单」的只读模式（现在它直接建，不说“差哪几个”）；④ `kafka_consumer_lag` 仍只探 `data-collector-telemetry` 单组，⑩ 应按契约清单逐组探并汇总（同 V1.19.12 待办③）；⑤ 全量单测 `tests` 目标本轮 `-m unit` 收集 0 条（与 V1.19.12 记录的 45 差额 45），需复核口径或补 `unit` 标记；⑥ `common/python/hunter_common` 的 5 处 UP046/UP047 存量 lint（CI `ruff check tests common/python services flink-jobs` 会撞）；⑦ V1.19.11 四项（`dlq_topic` ↔ `{topic}.dlq`、`kafka_consumer_groups` 显式注入、`unknown_groups` 字段、group 级 SASL ACL）与 V1.19.7 三项（`ctx.timestamp()`、sink `AT_LEAST_ONCE`、`rules.py:167` mypy dict-item）继续遗留；⑧ 本环境无 Docker，两项新检查（⓪/⑭）与 Redis 分流均为**桩测通过，待服务器实机复核**。（① 与 ⑦ 中的 `unknown_groups` 已在 **V1.19.14** 落地；②③④⑤⑥⑧ 仍悬）

---

## V1.19.14（修复：看板把「消费组不存在」报成 `lag=0` 的第七类假绿——`_lag_sync` 口径改为「未判定不进映射」，看板新增 `unknown_groups`（openapi 契约变更，用户已选定方案））

Date：2026-10-12
变更类型：修复（**镜像内代码 + openapi 契约 + 前端类型 + 文档**；属代码变更，`restart` 无效，需 `up -d --build data-analytics`）

现场：V1.19.13 实录里两处直接对账不上——集群侧四个新组 `kafka_groups_cli --describe` 全部 `Error: Consumer group '…' does not exist.`，而看板 `pipeline.kafka_consumer_lag` 对同样四个组回**四个 `0`**（同时 `telemetry_points 95940`、`available=true`、无降级横幅）。这不是“没积压”，是“根本没探到东西”。V1.19.11 当时只换了组名清单并把本项列为待办③；本轮取证前用户就修法在三方案中选定「**新增 `unknown_groups` 字段**」（未选“省略未知组”与“只加日志”）。

根因（不在配置层，在返回口径层）：
- `services/data-analytics/app/repositories/pipeline.py` 的 `_lag_sync()` 对配置里每个组调 `list_consumer_group_offsets`，而 **Kafka 对「组不存在」与「组存在但从未提交位点」都返回空分区列表且不报错**（拼错的组名同样不抛错），旧代码 `total_lag` 初值为 `0` 且无条件 `result[group] = total_lag` ⇒ “没查到”被写成一个合法读数。只要口径不改，下次拆组/改名又会重现一堆永远绿色的 0（V1.19.8 RUNNING 不出数、V1.19.9 grep 假阴、V1.19.11 幽灵组、V1.19.12 usage 当结论、V1.19.13 `.env` 不可读同族）。
- **同类形状缺陷两处（本轮写单测时实机抓出，本地 confluent-kafka 2.15.1）**：① `ConsumerGroupTopicPartitions` 的字段名是 **`group_id`**（不是 `group`）；② `AdminClient.list_consumer_groups()` 的 future 结果是 **`ListConsumerGroupsResult`（`.valid` / `.errors`）而不是裸 list**。两处都会让“取集群组清单”静默得到空集，进而把所有组错标为 `group_not_found`——只归因错、不影响未判定判定，但必须兼容取值。
- **降级语义混用**：旧 `available=false` 同时承担“Kafka 不可达”与“读不到位点”两种意思（前者该降级、后者不该），所以看板上一切正常。

修复内容（受追踪 **8 个**）：
- **A `services/data-analytics/app/repositories/pipeline.py`（主体）**：`_lag_sync()` 返回型改为 `tuple[dict[str, int], list[str]]`（已确证映射, 未判定清单）——**空分区列表、位点读取抛错、水位一个都取不到三类一律进清单，绝不进映射**；新增 `_group_inventory()`（`list_consumer_groups` 取集群实际组集，**只用于区分成因**：`group_not_found` / `no_committed_offsets`；列举失败或 `.errors` 非空→拒答返 `None`，绝不反推“组都不存在”）；日志事件 `consumer_group_lag_undetermined` 带 `cause=`；`PipelineSource.consumer_lag` / `KafkaLagProbe.consumer_lag` / `PipelineRepository.consumer_lag` 三处签名同步。
- **B `services/data-analytics/app/schemas/dashboard.py`**：`PipelineStats` 新增 `unknown_groups: list[str] = Field(default_factory=list, …)`，并重写 `kafka_consumer_lag` 描述为“**仅包含确有提交位点的组**”（新字段默认空列表，**不是必填**）。
- **C `services/data-analytics/app/services/dashboard.py`**：`_pipeline_block` 解包二元组；`lag_probe is None` 才是“kafka unavailable”降级理由，**未判定组不影响 `available`**（两条路径彻底分开）。
- **D `contracts/openapi/data-analytics.yaml`（契约变更）**：`PipelineStats` 新增 `unknown_groups`（`array of string`，写明三类成因与日志事件名）、`kafka_consumer_lag` 描述加“仅含确有位点的组、不得读成零积压”；`x-hunter-dashboard-contract.blocks.pipeline` 新增 **`lag_semantics:`** 条目把口径写进契约（标明【已结案 V1.19.14】）。
- **E `frontend/src/types/analytics.ts`**：`PipelineStats` 补 `unknown_groups?: string[]`，并把 `kafka_consumer_lag` 从错误的 `Record<string, unknown>` 收窄为 `Record<string, number>`（看板当前未渲染该字段，故无需改组件）。
- **F 测试（新增 10 例 + 改 2 例）**：**新增** `services/data-analytics/app/tests/test_kafka_lag_probe.py`（8 例，桩掉 `AdminClient`/`Consumer` 跑真实 `_lag_sync`）：五组同测（确证/空位点/组不存在/位点抛错/水位不可取）、成因分流与日志、部分水位失败仍求和、`OFFSET_INVALID` 以 low 为基准、旧版裸 list 形状兼容、`.errors` 非空拒答、对外协程返回二元组；`test_dashboard_endpoints.py` 新增「未判定组不得以 0 混进映射且与清单互斥」（含 `available is True`）并为原用例补上 `unknown_groups == []` 断言；`test_data_analytics_contract.py` 新增 `test_dashboard_pipeline_lag_separates_unknown_groups_from_zero`（契约↔实现同形：字段存在/默认空/`lag_semantics` 必存/两条降级路径不得合并）；`fakes.py` 的 `FakePipelineRepository` 新增 `unknown_groups` 参数并按新签名返回。
- **G 文档（四件套）**：`docs/deployment.md`（V1.19.11 注记后新增「只换组名清单拦不住同类假绿」一条）；`docs/运维操作手册.md`（看板降级表新增该因与两条新巡检习惯）；`docs/故障排查指南.md`（「看板 LAG 与集群实况对账」块重写读法：互斥关系、`cause=` 三分、`available` 新语义、旧镜像判定改为“两个键都不存在”）；`services/data-analytics/README.md`（新增「看板 `pipeline` 块读数口径」段）。
- **H 顺带错字修正**：`docs/deployment.md:160`「首跑就**拓**出 8 处」→「抓出」；`docs/故障排查指南.md:529` 与 `release.md:1205` 的「兼底」→「兜底」（均为历史登记的字符错替，不改语义）。

向后兼容：① **新增字段为选填**（`default_factory=list`），旧调用方按 `kafka_consumer_lag` 取数不受影响；但**`kafka_consumer_lag` 的 key 集合会变窄**（只剩确有位点的组）——自动化脚本若把“键不在映射里”当“零积压”读，必须改看 `unknown_groups`；② **`PipelineSource.consumer_lag()` 返回型变更属仓内破坏性改动**（`dict|None` → `tuple[dict,list]|None`），实现侧仅 `KafkaLagProbe`/`PipelineRepository` 与测试替身 `FakePipelineRepository`，已全部同形修改（仓内无其他调用方，已 grep 核）；③ 契约面属 `PipelineStats` 字段新增，`required` 不变（仅 `available`），不破坏 OpenAPI 合法性；④ compose/K8s **无需新增环境变量**，`kafka_consumer_groups` 取值口径未动（仍是 V1.19.11 的四个契约组）；⑤ `pipeline` 块的 `available`/`reason` 含义收紧（不再为“无位点”降级），`degraded` 列表语义不变。

风险：① **看板上“某组从映射里消失”现在是正常态**（作业未跑/位点过期都会被剔出），首次部署后四个组很可能全部落在 `unknown_groups`，前端不渲染该字段 ⇒ 容易被当成“没数据”，必须配合本文的集群侧 `--describe` 对账；② `_group_inventory()` 多一次 admin 调用（仅取存在性清单），在 group 级 ACL 受限的 principal 下会稳定返 `None`（不报错、只退化成 `no_committed_offsets` 成因），本轮无集群可测，**待服务器实机确认日志里的 cause 分布**；③ 未判定不等于“零”，但也**不等于“有积压”**——看板已不再提供“把未知折算成数字”的路径，依赖该数字做告警的下游（如 Grafana 变量）需改读 `unknown_groups` 长度；④ 本条不修 DLQ（`dlq_topic` 与契约 `{topic}.dlq` 不符仍返 `{}`）、不修 ⑩ 只探单组，也不回答 `-algo` 为何 FAILED；⑤ `tests` 目录仍无任何 `pytest.mark.unit`（该目标恒 0 收集），新单测全部落在服务内 `app/tests/`，不依赖该标记。

验证（本环境，均实跑）：`pytest services/data-analytics` **105 passed**（较上轮 94 新增 11：探测单测 8 + 看板端点 1 + 契约同形 1，另含回归修正 1）；`ruff check services/data-analytics/app scripts tests flink-jobs` **All checks passed**；`vue-tsc --noEmit -p tsconfig.json`（前端 type-check）**无错**；`verify_infra.py` **17 项全 PASS**；`verify_data_layer.py` **通过 34 项、失败 0**；`render_k8s.py --check` **PASS**；`generate_contracts_configmap.py --check` **PASS**（本条未动 Kafka 契约，无需重生成）；`verify_test_layer.py` **L5 测试契约通过（4 项 OK）**；全量单测 **9 目标 1045 passed / 1 skipped / 0 failed**（326 / 67+1s / 71 / 108 / 143 / 105 / 128 / 97，`tests` 目标 `-m unit` 仍 100 deselected）；`mypy app/repositories/pipeline.py app/services/dashboard.py app/schemas/dashboard.py` 仅 4 处**存量**错（`fleet.py:53/59`、`pipeline.py:20` 版本兼容 try/except import、`dashboard.py:69`），**本轮新增代码 0 错**（CI mypy 只校 `tests/support` 与 `verify_test_layer.py`，不涵盖 services）。另：本环境**无 Docker**，本条改动未做端到端实测（四个不存在的组在看板上究竟怎么显示，待服务器复核）。

分发路径注意：受追踪 **8 个**：`services/data-analytics/app/repositories/pipeline.py`、`app/schemas/dashboard.py`、`app/services/dashboard.py`、`app/tests/{fakes.py,test_dashboard_endpoints.py,test_data_analytics_contract.py}`、新增 `app/tests/test_kafka_lag_probe.py`、`contracts/openapi/data-analytics.yaml`、`frontend/src/types/analytics.ts`（`docs/` 三份与 `release.md` 被 `.gitignore` 忽略，需单独 rsync）。⚠ **本条需重建两个镜像**：`up -d --build data-analytics`（服务代码烘入镜像）与 `up -d --build web-portal`（前端类型属编译期产物）。

服务器续跑步骤（**顺序不能倒：先取证，后重建**）：

```bash
# 0) 【取证 A/B】先取 -algo 根因（JM 无 HA，容器一重建日志就丢；本条会重建 data-analytics，不碰 flink-jm）
#    ⚠ 上份实录用 `grep -A40 … | tail -n 80`，最内层 `Caused by` 被窗口截掉 ⇒ 仍无法定性；下面写法不截断
#    ℞ 勘误（V1.19.15）：本块原先还接了 `| head -n 200` 与 `sed -n '1,5p;$p'`——前者同样会切掉最内层，
#      后者只取文件末行（那是栈帧不是 Caused by），两者均已去掉
sudo docker logs hunter-flink-jm 2>&1 | awk '/switched from RUNNING to FAILED/{f=1} f' > /tmp/algo-failed.txt
grep -n "Caused by" /tmp/algo-failed.txt                     # 能定性的是这些行里的**最后一条**
awk '/Caused by/{last=NR} {l[NR]=$0} END{for(i=last;i<=NR;i++) print l[i]}' /tmp/algo-failed.txt
sudo docker exec hunter-flink-jm flink list -a
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_topics_cli --list'   # 取证 B①（上份实录未执行）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_groups_cli --list'    # 取证 B②（上份实录只跑了 --describe）
# 1) 落地（⚠ 按约定 V1.19.13/V1.19.14 **均未提交** ⇒ 服务器 `git pull` 取不到，必须从本机 rsync/scp；
#    上一份实录证明服务器仍跑 V1.19.12 版脚本：既无 ⓪ 环境凭据项、也无 ⑭ Flink 作业项）
#    V1.19.13（不重建镜像）：scripts/{common.sh,health-check.sh,daily-check.sh,install.sh,init-kafka.sh,verify_infra.py} + README.md
#    V1.19.14（8 个受追踪 + 1 个新增测试）：services/data-analytics/app/{repositories/pipeline.py,schemas/dashboard.py,
#      services/dashboard.py}、app/tests/{fakes.py,test_dashboard_endpoints.py,test_data_analytics_contract.py,
#      test_kafka_lag_probe.py(新)}、contracts/openapi/data-analytics.yaml、frontend/src/types/analytics.ts
#    两份 docs（.gitignore 忽略，需单独传）：docs/故障排查指南.md、docs/运维操作手册.md、docs/deployment.md
#    提交以后本步可简化为：cd /opt/HunterCore && git pull
# 2) 重建两个镜像（不 recreate flink，以免丢日志）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics web-portal
# 3) 看板对账：现在必须同时看两个字段
sudo docker exec -i hunter-analytics python - <<'PY'
import json, urllib.request as ur
from app.config import settings
req = ur.Request("http://localhost:8083/api/v1/analytics/dashboard", headers=settings.internal_call_headers())
try:
    with ur.urlopen(req) as r:
        p = json.load(r)["data"]["pipeline"]
    print("lag    =", json.dumps(p["kafka_consumer_lag"], ensure_ascii=False))
    print("unknown=", json.dumps(p["unknown_groups"], ensure_ascii=False))
    print("available/reason =", p["available"], p.get("reason"))
except ur.HTTPError as e:
    print("HTTP", e.code, e.read().decode("utf-8", "replace")[:400])
PY
# 4) 成因分布（本条新增的服务日志事件）
sudo docker logs hunter-analytics 2>&1 | grep -E "consumer_group_lag_undetermined|consumer_group_inventory_" | tail -n 20
# 5) 巡检与门禁（本条不改脚本，跑一遍确认无回归）
sudo bash scripts/health-check.sh 2>&1 | tail -n 40
python3 scripts/verify_infra.py | tail -n 6
```

判据预告：① 第 3 步 **`unknown_groups` 应含四个新组**（它们确实不存在），而 `kafka_consumer_lag` 应当只含 `data-collector-telemetry` 这类真有位点的组（当前配置探的是四个 data-analytics 组，所以**两个字段都空/满并不异常**）；若四个组仍挤在 `kafka_consumer_lag` 里且全为 `0` ⇒ 跑的是旧镜像；② `available` 应为 `true`（Kafka 可达），`reason` 不应因“无位点”而出现 `kafka unavailable`；③ 第 4 步的 `cause=` 分布能告诉你集群侧 ACL 情况：全是 `group_not_found` = 清单可用且组真不存在；全是 `no_committed_offsets` 而集群侧 `--describe` 报 `does not exist` ⇒ `_group_inventory()` 被 ACL 或超时挡住了（看同批日志里的 `consumer_group_inventory_failed`）；④ 若日志出现 `consumer_group_inventory_partial` 则 broker 清单不完整，本轮**故意不给结论**（不把“没列出”当“不存在”）；⑤ 前端本条不新增卡片，看板页面看不出 `unknown_groups`（属已知缺口，待办②），验证以接口 JSON 为准。

**待办（不属本条范围）**：① `-algo` FAILED 根因（待取证 A/B）；② **前端未渲染 `unknown_groups`**（`DashboardView.vue` 目前连 `kafka_consumer_lag` 都不显示，只显遥测点数与入库延迟 P95）——要让运维在页面上看见“未判定”，需新增卡片或标签，属 UI 变更，待用户确认样式口径；③ `dlq_topic` ↔ 契约 `{topic}.dlq`（_dlq_sync 对不存在的 Topic 返 `{}`，与本轮同族但属行为变更）；④ `kafka_consumer_groups` 仍靠默认值而非 compose/K8s 显式注入（与 data-collector 不对齐）；⑤ group 级 SASL ACL 声明式清单缺失；⑥ health-check ⑩ 仍只探 `data-collector-telemetry` 单组；⑦ `tests` 目标 `-m unit` 恒 0 收集；⑧ **CI lint 在当前 HEAD 会红 50 处**（本轮发现：5 处 `common/python/hunter_common` UP046/UP047 + 45 处 `services/vehicle-service` RUF100/I001/SIM118/RUF022），均**非本条引入**（两目录本条未触碰），建议单独一轮清理；⑨ V1.19.7 三项（`ctx.timestamp()`、sink `AT_LEAST_ONCE`、`rules.py:167` mypy）继续遗留；⑩ 本环境无 Docker，本条为单测+契约验证，待服务器实机复核；⑪ **本份实录的新发现（未修，待用户定夺）**：`flink list -a` 显示两个作业 RUNNING、而四个契约组 `--describe` 全部 `does not exist`，与 `infra/deploy/docker-compose.yml` 的 `FLINK_PROPERTIES` **只设 `execution.checkpointing.dir`、未设 `execution.checkpointing.interval`**（而 K8s 侧 `infra/k8s/flink/01-configmap.yaml:36` 设了 `10000`）一致——Flink KafkaSource **仅在检查点提交位点**（`commit.offsets.on.checkpoint` 默认 true），单机 compose 因此**永远不会有已提交位点** ⇒ 作业重启一律回落 `earliest` 重放、本条上线后四个组会长期落在 `unknown_groups`（读数正确但积压不可观测）。这是 V1.19.13 判据②预告的「下一道坡」；打开检查点属**运行时语义变更**（状态/精确一次/失败恢复行为都变，且需 recreate `flink-jm`/`flink-tm` 并重提交作业），当时**未擅自落地**。

> **〔已结案 V1.19.15〕**：用户在三方案中选取「**作业内 `env.enable_checkpointing()`**」，未选“只在 compose 里补 `interval`”（理由：不再依赖部署侧配置漂移）；顺手项只选「`init-kafka` 只读对账模式」，未选「前端渲染 `unknown_groups`」与「CI lint 存量清理」⇒ 本条待办 **②⑧ 仍悬**（③~⑦ 与本条无关，仍未动）。

---

## V1.19.15（修复：单机 compose 从未开启周期性检查点 ⇒ KafkaSource 永不提交位点、四个契约消费组在集群上永不存在；周期改由作业代码统一开启；另新增 `init-kafka.sh --check-only` 只读对账）

Date：2026-10-12
变更类型：修复（**镜像内代码 + 两套编排 + `.env` 模板 + 运维脚本 + 文档**；属代码变更，`restart` 无效，需 `up -d --build flink-jm flink-tm` 并重提交三作业）

现场（本份 196 行实录）：`flink list -a` 只见 `driving_anomaly_detection` 与 `data_quality_monitor_ingest_latency` 两个 RUNNING（`algorithm_performance_monitor` FAILED）；sudo 下四个 `data-analytics-*` 契约组 `kafka_groups_cli --describe` 全部 `Error: Consumer group '…' does not exist.`，而 `data-collector-telemetry` 返真 `0`（表头门禁未触发，是有效读数）⇒ 服务侧在提交位点、Flink 侧从未提交。看板侧四个 `0` 属 V1.19.14 已修的第七类假绿。另：非 sudo 巡检仍报三项 FAIL（Redis PING / Kafka Topic 数 1 < 6 / MinIO Bucket 数 -1）——那是 `.env` 权限 600 的下游假信号（V1.19.13 已证），**不能据此断定 `algorithm_metrics` 不存在**；而 `python3 scripts/verify_infra.py` 末项文案为「均带 --bootstrap-server（13 个脚本/文档）」⇒ **当时把它当版本指纹是错的**（见下 `℞ 勘误`）。

> **℞ 勘误（V1.19.16）**：上段“verify_infra 末项返 13 即服务器脚本旧”这条判据**不成立**（下份实录已证伪：服务器已跑新版脚本、文案已是新的）。`check_kafka_cli_required_args()` 的计数是 `scripts/*.sh + README.md + docs/*.md`（docs 目录存在才计入）：本机 12+1+8=21，服务器 13 ⇒ **真正结论是服务器 `docs/` 一份 .md 都没有**（docs 被 `.gitignore` 忽略，`git pull` 带不来，必须 rsync）。本条曾把该计数拆为“脚本 N / 文档 M”，但**最终按用户裁定把文档判据整体撤除**（文档与程序脱钩，见 V1.19.16 E 项）：现在末项只报「已校验运维脚本 12 个」，与文档彻底无关。

根因（不在读数层，在运行形态层）：
- Flink 的 `KafkaSource` **只在检查点完成时提交消费位点**（`commit.offsets.on.checkpoint` 默认 `true`，**没有其他提交路径**）。全仓检索 `enable_checkpointing` / `execution.checkpointing.interval` 只命中 `infra/k8s/flink/01-configmap.yaml`（值 `10000`）：单机 compose 的 `FLINK_PROPERTIES` **只有 `execution.checkpointing.dir`（目录）、没有周期** ⇒ 根本不做检查点 ⇒ 位点永不提交、组永不存在、积压不可观测、作业每次重启都回落 `earliest` 重放整个 7 天保留窗口、60s 窗口状态随故障全丢。
- 两种编排行为不一致 = 本仓库反复踩过的「同一件事两处配置漂移」；且仓内**没有 `.env.example` ↔ compose/代码 的变量名对账门禁**，导致模板里一个拼写错的键长期无人发现（见下 F 项幽灵变量）。

修复内容（受追踪 **14 个**，含 2 个新增）：
- **A `flink-jobs/hunter_flink/checkpointing.py`（新增，主体）**：`checkpoint_interval_ms()` 解析 `FLINK_CHECKPOINTING_INTERVAL_MS`（**未设置与空串均取默认 `10_000`**——compose 的 `${VAR:-}` 传下来的空串等价于“没配”，不能因为空串就在提交期报错）；非整数/非正数**当场 `ValueError`**，绝不静默退回默认（与 `job_conn` 同一取向；`0`/负数不等于“关闭”）。`enable_checkpointing(environment, env=None)` 调 `environment.enable_checkpointing(interval)` 并打印 `[hunter_flink] checkpointing enabled: interval_ms=…`（提交回显是“到底开了没”的第一手证据）。首参取名 `environment` 而非 `env`：本包里 `env` 惯指环境变量映射。
- **B 三作业同形接入**：`detection_job.py` / `algorithm_performance_job.py` / `ingest_latency_job.py` 均在 `get_execution_environment()` 后紧接 `enable_checkpointing(env)` 并注释“为什么”（KafkaSource 只在检查点提交位点）。
- **C `flink-jobs/hunter_flink/api_guard.py`**：`StreamExecutionEnvironment` 白名单新增 `enable_checkpointing`（由镜像内第四道自检 `hasattr` 真实校验，本环境无 pyflink 不能自验）。
- **D `infra/deploy/docker-compose.yml`**：`flink-jm`/`flink-tm` 各新增 `FLINK_CHECKPOINTING_INTERVAL_MS: ${…:-}` 透传；并在 `environment:` 的 **YAML 注释**里写清“只给目录不给周期是故意的、周期由代码开”。⚠ 本项曾把这 4 行说明放进 `FLINK_PROPERTIES` 块内（`#` 注释会被原样追加进容器内 Flink 配置文件），**因本环境无 Docker 无法实机验证其解析、且风险不对称（JM 起不来就白跑一轮）而改放 YAML 注释层**（已复跑：`FLINK_PROPERTIES` 解析后不再含 `#`）。
- **E `infra/k8s/flink/01-configmap.yaml`**：`data:` 新增 `FLINK_CHECKPOINTING_INTERVAL_MS: "10000"` 并注释“代码值优先于集群兜底键、两处必同值、由单测静态对账”（编辑过程中一度将「入库」错替为繁体「入庫」，diff 当场发现并已修回）。
- **F `infra/deploy/.env.example`（幽灵变量）**：原 `FLINK_CHECKPOINT_INTERVAL_MS=10000`（少 `-ING`）**compose/代码/脚本无人读取**——调了不报错也完全不生效，已改名 `FLINK_CHECKPOINTING_INTERVAL_MS` 并加警示注释；同时修 §8 段头过期版本标记「Flink 1.18」→「Flink 2.1」。
- **G 测试（新增 21 例）**：新建 `flink-jobs/tests/test_checkpointing.py`（**17 例**：默认值/空串与空白回退默认/覆盖生效（含带空格）/非整数 4 参/正负数 2 参/下划线分隔合法（`int("30_000")==30000`）/调用与返回值/日志行/**与 K8s configmap 的 `execution.checkpointing.interval` 同值对账**/**compose 两处透传存在对账**）；`test_pyflink_api_usage.py` 新增 **4 例**（三作业逐个断言调了 `enable_checkpointing`、守卫白名单含该成员）。中途自测踩过一次：按印象以为 `int("1_000")` 会报错而把它列为非法值，实跑 `1 failed` 后改为专测“下划线可接受”。
- **H `scripts/init-kafka.sh` 新增 `--check-only` 只读对账**（**关闭 V1.19.13 待办③**）：`check_only_alignment()` 先过**输出形态门禁**（行数与可解析名行数不一致/空输出 ⇒ 判“读不到清单”，照贴原始输出 + 列三类常见成因，**不下“Topic 缺失”结论**并返 1），再逐 Topic 比对 `TOPIC_SPECS` + `{topic}.dlq`（缺失点名、存在则 `verify_topic` 查偏差），契约外清单排除 `__*` 与 `hunter.*`；汇总行「契约要求 14（含死信）｜缺失 N｜配置偏差 M」+「集群实际 T，契约外 E」；`usage()`/头注/main 分支同步（`set -e` 下的安全返场写法）。
- **I `scripts/health-check.sh` ⑭ 提示词修正**：FAIL 详情里给运维的抓栈命令由 `grep -B3 -A40 …`（本份实录正是用它切掉了最内层 `Caused by`）改为**不截断**写法：`awk '/switched from RUNNING to FAILED/{f=1} f' > /tmp/flink-failed.txt` 后读**最后一条** `Caused by` 及其后续行（脚本提示语里不内嵌 awk——那串 `{l[NR]=$0}` 会被双引号展开）；三处文档（手册 §3.6 与巡检表 ⑭ 行、排查指南 §14、本模块 README）同时补上具体提取命令 `awk '/Caused by/{last=NR} {l[NR]=$0} END{for(i=last;i<=NR;i++) print l[i]}'`。**自查改错**：本轮初稿在取证块里写了 `| head -n 200` 与 `sed -n '1,5p;$p'` / `sed -n '$p'`——前者同样会截掉最内层、后者取的是**文件末行（一个栈帧）而不是 `Caused by`**，已在 V1.19.14/V1.19.15 两处续跑块里改掉并标 `℞ 勘误`。
- **J 文档（五份 + 模块 README）**：`flink-jobs/README.md`（新增「检查点与位点提交」段 + 提交回显证据行 + 纪律新增「RUNNING 不等于位点在提交」第三族）；`docs/运维操作手册.md`（新增 **§7.8 只读对账**与 **§7.9 位点与检查点**；抓栈命令改为不截断形式两处）；`docs/故障排查指南.md`（§0 新增关键事实一条；场景5 表行新增“组 `does not exist` 而作业 RUNNING”成因与处置；§14 新增**第五类假信号**块与不截断取栈命令）；`docs/deployment.md`（§2 新增「只给目录不给周期 = 根本不做检查点」一条；§13.1 表格 `01-configmap` 行重写）；`docs/01-部署概述与环境要求.md`（脚本树 `init-kafka.sh`/`health-check.sh` 两行）；根 `README.md`（脚本表 `init-kafka.sh` 行、死信计数、消费组/位点查询行）。
- **K 本轮顺带修正的存量文档错**：① **Topic/DLQ 计数 6 → 7**（`algorithm_metrics` 入契约后多处未同步，现与 `verify_data_layer` 的「平台 7 + 死信 7」对齐）——`README.md` 两处、`docs/运维操作手册.md` 四处、`docs/01-部署概述与环境要求.md` 一处、`docs/故障排查指南.md` 一处；② `docs/deployment.md` §13.1 仍写 `01-configmap` 含 **`REDIS_URL`**（V1.19.8 已禁用并从清单删除）⇒ 改写实 `KAFKA_SECURITY_PROTOCOL`/`KAFKA_SASL_MECHANISM`/`REDIS_HOST/PORT/DB` 并注明禁写 `REDIS_URL`；③ `docs/01-部署概述与环境要求.md` 健康检查「13 类」→「15 类」；④ 收尾复扫又修两处：`release.md:1205`「金标准**兼**底」→「兜底」（V1.19.14 待办登记过但当时漏改）、`docs/运维手册.md` §7.8 表内「粘出原文」→「贴出原文」（与其余文档口径一致）。

向后兼容：① **开启周期性检查点属运行时语义变更**（用户已选定）：之前“重启即从头重放”消失，改为从**上次检查点位点**续读；历史告警/指标不再因重启重投；② 代码默认周期 10s 与 K8s 现值同，**不新增必填 `.env` 变量**（留空即默认），K8s 侧只增 ConfigMap 键、无清单结构变更；③ 作业不依赖部署侧配置也能开起来（compose/K8s/本地 MiniCluster 同形），旧集群若显式设了 `execution.checkpointing.interval` 不冲突（**代码值优先**）；④ `init-kafka.sh` 默认行为完全不变（仍幂等创建），`--check-only` 为新增只读分支；退出码沿用 0/1；⑤ 健康检查项数与阈值未动，仅 ⑭ 的提示文案变长；⑥ 投递语义不变（三作业 sink 均 `AT_LEAST_ONCE`，无事务、无 `transactional.id`），本条只增加位点提交与状态快照。

风险：① **compose 形态 JM 无 HA**：开检查点不等于能从检查点自动恢复——重建 JM 仍须手动重提交三作业（只是重提交后起点从 `earliest` 变为已提交位点）；`/data/flink` 下的检查点目录无清理策略，长期运行需关注磁盘（待办⑤）；**⚠ 本段当时漏了最关键的一项：目录可写性**——`install.sh` 把 `/data/flink` 预建为 `root:root 0755`，而容器内 flink 用户非 root，下一份实录取决于这个未验证假设：三个作业**因开检查点反而全挂**（见 V1.19.16）；“桩测/静态对账全绿”不能覆盖运行期文件系统前提。② 首次升级且**未预先 `--reset-offsets --to-latest`** 时，四个新组仍会从 `earliest` 重放一次（因为之前根本没有位点可恢复），看板 `telemetry_points` 与 `alert_event` 会短时跳高；③ `enable_checkpointing` 成员名靠镜像内第四道自检，本环境无 pyflink ⇒ **未实机验证**；若签名不符，`flink run` 会在提交期 `AttributeError`（会被守卫与单测双重拦下，不会退化成静默）；④ `FLINK_CHECKPOINTING_INTERVAL_MS` 设在 JM 容器（提交进程）里，若将来改成独立提交 Job，必须同步补该变量，否则会默默吃代码默认值；⑤ `--check-only` 的形态门禁只判“输出是不是名单”，不校验 `$KC` 指向的文件是否存在；桩测 19 项全 OK 但**未实机跑过真集群**；⑥ 本条不回答 `-algo` 为何 FAILED（取证仍未齐），也不修 DLQ/⑩ 单组探测/前端渲染。

验证（本环境，均实跑）：`pytest flink-jobs` **88 passed / 1 skipped**（较 V1.19.14 登记的 67+1s 恰为本条新增 21 例）；全量单测 **9 目标 1066 passed / 1 skipped / 0 failed**（326 / 88+1s / 71 / 108 / 143 / 105 / 128 / 97，`tests` 目标 `-m unit` 仍 100 deselected）；`ruff check flink-jobs scripts services/data-analytics/app tests` **All checks passed**（新文件首跑报 2 处：I001 导入分组与 FURB167 `re.M`，已 `--fix` 后复跑绿）；CI 同口径 `ruff check tests common/python services flink-jobs` **仍为 50 处存量红**（全部落在本条未触碰的 `common/python`（UP046/UP047）与 `services/vehicle-service`（RUF100/I001/SIM118/RUF022），待办③）；`mypy --ignore-missing-imports --disable-error-code=import-untyped tests/support scripts/verify_test_layer.py` **Success（10 文件）**；`bash -n scripts/{init-kafka,common,health-check,daily-check,install}.sh` **×5 OK**；`verify_infra.py` **17 项全 PASS**（末项仍为 21 个脚本/文档，新增注释未触发门禁）；`verify_data_layer.py` **通过 34 项、失败 0**；`render_k8s.py --check` **PASS**；`generate_contracts_configmap.py --check` **PASS**；`verify_test_layer.py` **L5 自检 4 项 OK**；`init-kafka --check-only` **桩门禁六组场景 19 项断言全 OK**（齐备 / 缺 2（含死信）/ 一行报错与空输出均判“读不到清单”且不下缺失结论 / 配置偏差 14 计数 / 契约外点名不改“一致”结论 / `--check-only` 不触 `create_topic`）——排查过程中踩到两个环境坑：cmd 层重定向与 Git Bash 嵌套 `$(…)` 复用同一文件句柄会互相覆盖偏移（改为脚本内部逐次落盘）、以及 **`source init-kafka.sh` 会把其头部 `set -euo pipefail` 带进门禁 shell**（期望非 0 退出码的用例会在第一个红处直接终止，需显式 `set +e`）；compose/configmap YAML 解析复核（两个服务各 1 处透传、`execution.checkpointing.dir` 保留、`FLINK_PROPERTIES` 不再含 `#`）。本环境**无 Docker**，三作业提交流程与位点提交效果均待服务器实机复核。收尾复跑（本轮改的全是文本与提示语）：`bash -n scripts/health-check.sh` OK；`pytest flink-jobs` **88 passed / 1 skipped**；`ruff check flink-jobs scripts` **All checks passed**；`verify_infra.py` **17 项全 PASS**；**繁体/错字复扫**：脚本比对 32 个改动文件的每一行与 60+ 个「简笔另有其字」的繁体字形，仅命中 `release.md` 自述该错的那一行（引用性质，不改），另手工抹掉上条 K 项④ 的两处。

分发路径注意：受追踪 **14 个** = `flink-jobs/hunter_flink/{checkpointing.py(新),detection_job.py,algorithm_performance_job.py,ingest_latency_job.py,api_guard.py}`、`flink-jobs/tests/{test_checkpointing.py(新),test_pyflink_api_usage.py}`、`flink-jobs/README.md`、`infra/deploy/{docker-compose.yml,.env.example}`、`infra/k8s/flink/01-configmap.yaml`、`scripts/{init-kafka.sh,health-check.sh}`、根 `README.md`；另 `docs/` 四份（运维操作手册、故障排查指南、deployment、01-部署概述）与 `release.md` 被 `.gitignore` 忽略，**必须单独 rsync**。⚠ **本条需重建 `hunter-flink/jobs` 镜像**（`up -d --build flink-jm flink-tm`）；叠加 V1.19.14 尚需 `data-analytics` 与 `web-portal`。三版均未提交 ⇒ 服务器 `git pull` 取不到。

服务器续跑步骤（**顺序不能倒：先取证，后重建 flink——recreate 会丢 JM 日志与作业**）：

```bash
# 0) 【仍未取到的证据，上份实录未执行/被截断】
sudo docker logs hunter-flink-jm 2>&1 | awk '/switched from RUNNING to FAILED/{f=1} f' > /tmp/algo-failed.txt
grep -n "Caused by" /tmp/algo-failed.txt                     # 最内层＝最后一条，不要 head/tail 夹窗口
awk '/Caused by/{last=NR} {l[NR]=$0} END{for(i=last;i<=NR;i++) print l[i]}' /tmp/algo-failed.txt
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_topics_cli --list'   # 取证 B①
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_groups_cli --list'    # 取证 B②
# 1) 落地（未提交 ⇒ 从本机 rsync：本条 14 个受追踪 + docs 四份 + release.md；另带 V1.19.13 六脚本与 V1.19.14 八点）
# 2) 先 cancel 再重建（避免 FlinkJobNotFoundException 刷栈），重建 flink 双容器
sudo docker exec hunter-flink-jm flink list -a
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build --force-recreate flink-jm flink-tm
# 3) 重提交三作业：每行回显里必须有一行 `[hunter_flink] checkpointing enabled: interval_ms=10000`
for j in detection_job algorithm_performance_job ingest_latency_job; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec flink-jm \
    flink run -d -m localhost:8081 -pym hunter_flink.$j
done
# 4) 等 ≥ 2 个检查点周期（≈20s）后验位点真的提交了（本条的核心判据）
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo \
           data-analytics-telemetry-raw data-analytics-algorithm-metrics; do
    echo "== $g"; kafka_groups_cli --describe --group "$g" | tail -n +2
  done'
# 5) 只读对账（本条新增；不建 Topic，只说差哪几个）
sudo bash scripts/init-kafka.sh --check-only
# 6) 看板双字段对账（V1.19.14）+ 巡检与门禁（必 sudo / python3）
sudo bash scripts/health-check.sh 2>&1 | tail -n 40
python3 scripts/verify_infra.py | tail -n 6
```

判据预告：① 第 3 步回显**必有** `checkpointing enabled`：没有 ⇒ 跑的还是旧镜像（作业代码已烘入 `hunter-flink/jobs`，只看仓内源文件不够）；② 第 4 步四个组应从 `does not exist` 变成分区表格（`CURRENT-OFFSET` 非空）；若仍 `does not exist` 而有①那行，再看 JM 日志有无 `Completed checkpoint`；③ 第 5 步在 sudo 下三种结论文义不同：“一致”/“缺失〈名〉”（真缺，`sudo bash scripts/init-kafka.sh` 补齐）/“读不到清单”（按贴出的原文归因，**不得读成 Topic 缺失**）；④ 看板四个组应从 `unknown_groups` 逐步退回 `kafka_consumer_lag` 并显真实积压（首次可能因重放一度很高，随消费回落）；⑤ 若取证 B① 里没有 `algorithm_metrics`，`-algo` 的 FAILED 即定性为 **Topic 缺失**（与 `algorithm_metrics--1@-1`+`NOT_IN_TRANSACTION` 指纹一致），跑 `init-kafka.sh` 补建后只需重提交 `-algo`；若 Topic 在清单里而最内层 `Caused by` 是 `TopicAuthorizationException`，则是 group/topic 级 ACL（另立条目）；⑥ 三个作业齐备后 ⑭ 应回 `RUNNING=3（期望 3），无 FAILED/CANCELED 终态`；⚠ 服务器上仍是 V1.19.12 版脚本时根本没有 ⓪ 与 ⑭ 两项，第 6 步前请先确认已 rsync 到位：判据改用 `grep -c 'check_flink_checkpoint_dir' scripts/health-check.sh`（命中即 V1.19.16 已到位）。**℞ 勘误（V1.19.16）**：本轮原写的判据“`verify_infra.py` 末项文案是“参数齐备……21 个脚本/文档”而非“均带 --bootstrap-server（13 个）””**不能当版本指纹**——该计数含 `docs/*.md`，而 docs 被 gitignore，13 只说明服务器的 docs 未同步。

**待办（不属本条范围）**：① `-algo` FAILED 根因 **〔已结案 V1.19.16〕**（本轮取证到顶：最内层 `Caused by` 为 `Topic algorithm_metrics not present in metadata`，定性为 **Topic 未创建**，非 ACL 非网络）；② **前端未渲染 `unknown_groups`**（本轮用户未选，与 V1.19.14 待办② 同）；③ CI lint 当前 HEAD 会红 50 处（本轮未选）；④ **新增：`.env.example` ↔ compose/代码 无变量名对账门禁**——本轮的幽灵变量（`FLINK_CHECKPOINT_INTERVAL_MS` 少 `-ING`）靠人工发现，建议在 `verify_infra.py` 加一项：模板里的每个 `FLINK_*`/`KAFKA_*` 键必须在 compose 或代码中有读取方，否则判未使用；⑤ 检查点持久化与 JM HA（compose `/data/flink` 无清理策略、K8s 为 `emptyDir`）**〔V1.19.16 拆为两项：可写性已修，共享存储仍悬，见下条待办②〕**；⑥ ⑩ 仍只探 `data-collector-telemetry` 单组；⑦ V1.19.11 四项（`dlq_topic` ↔ `{topic}.dlq`、`kafka_consumer_groups` 显式注入、group 级 SASL ACL）与 V1.19.7 三项（`ctx.timestamp()`、sink `AT_LEAST_ONCE`、`rules.py:167` mypy dict-item）继续遗留；⑧ 本环境无 Docker：本条全部为桩测/静态对账，**待服务器实机复核**（含“开启检查点后四个组真的出现在集群上”这个核心断言）。

---

## V1.19.16（修复：`/data/flink` 预建为 root:root 导致开检查点后**三个作业全死在提交期**；巡检 ⑭ 把零作业读成“未判定”的第八类假绿；另新增 ⑮ 检查点目录可写门禁与三处属主对账）

Date：2026-10-12
变更类型：修复（**运维脚本 + 静态门禁 + K8s 注释 + 文档**；未改 `flink-jobs/` 代码与 compose 结构 ⇒ **不需重建镜像**，服务器侧 `chown` + 重提交即可恢复）

现场（本份 549 行实录，服务器 14:49–14:55）：
- **取证到顶（V1.19.15 待办① 结案）**：不截断窗口后拿到最内层两条 `Caused by` —— `TimeoutException: Topic algorithm_metrics not present in metadata after 60000 ms.` 与 `UnknownTopicOrPartitionException`；`--list` 的 21 个 Topic 里确实没有 `algorithm_metrics` ⇒ `-algo` 定性为 **Topic 未创建**（非 ACL、非网络）。
- **`--check-only` 实机确证**：真集群输出与桩测逐字一致——「契约要求 14（含死信）｜缺失 2｜配置偏差 0 / 集群实际 21 个 Topic，其中契约外 0 个 / 只读对账不通过」，且点名正是 `algorithm_metrics` 与 `algorithm_metrics.dlq`（V1.19.15 H 项从此不再是“未跑过真集群”）。
- **V1.19.15 让情况变差**：三作业提交回显都有 `[hunter_flink] checkpointing enabled: interval_ms=10000`（代码生效），但**全部**以 `JobInitializationException → Failed to create checkpoint storage at checkpoint coordinator side → java.io.IOException: Failed to create directory for shared state: file:/opt/flink/data/checkpoints/<jobid>/shared`（`FsCheckpointStorageAccess:149`）失败 ⇒ JM 从 2 RUNNING 退到 **0 作业**。
- **巡检把全灭读成没事**：汇总「通过 40 项，警告 4 项，**失败 0 项（共 44 项）**」，而 ⑭ 的 WARN 详情只能看到 `WARNING: Unknown module: jdk.compiler`——第八类假绿，由本仓自己引入（V1.19.13）。
- 消费组清单里只有服务侧组（`data-collector-*`、`scene-service-*`、`remote-control-*`、`ota-service-*`、`console-consumer-*`），**无任何 `data-analytics-*`**（作业从未运行过，位点无从谈起）；JM 日志里 `telemetry_raw` 的 split `StartingOffset: -3` 即回落 earliest 的指纹。

根因（三件事，都不是作业代码）：
- **① 检查点目录不可写（主因）**：`scripts/install.sh` 原本 `ensure_dir "${DATA_DIR}/flink" "root:root" "0755"`，而 compose 把该目录 bind 成容器内 `/opt/flink/data`、`flink-jobs/Dockerfile` 以 `USER flink`（官方镜像 UID **9999**，与 `infra/k8s/flink/02/03` 的 `runAsUser/runAsGroup/fsGroup: 9999` 同值）运行 ⇒ JM 在 coordinator 初始化存储时 `mkdir` 失败。**该错先于任何算子代码**，与具体作业无关（所以三个一起挂）。为何长期看不见：PG/Redis/Kafka/MinIO 都按镜像 uid chown，**唯独 flink 给了 root**，因为历史上从未真正做过检查点，这条假设没被任何路径验证过；而“桩测全绿 + 静态对账全绿”根本不覆盖运行期文件系统前提（V1.19.15 风险段只写了磁盘与 HA）。
- **② 第二处编排漂移（K8s 侧同类、尚未实机）**：K8s 的 `execution.checkpointing.dir` 指向 emptyDir 挂载点 `/opt/flink/checkpoints` 且有 `fsGroup: 9999` ⇒ JM 自身可写（跟 compose 不同结论）；**但 emptyDir 不跨 Pod 共享、`03-taskmanager.yaml` 根本没挂该路径** ⇒ TM 写 `shared` 仍会失败。本条只以注释登记（换存储需 PVC/RWX 决策），列入待办②。
- **③ ⑭ 的表头门禁本身就是错的**：`flink list -a` 在零作业时打的是 `No running jobs.`（不含 `Running/Restarting Jobs` 表头），而旧实现把“没表头”一律归为“JM REST 未就绪/命令失败”⇒ WARN 未判定。拿“读数形式”当有效性判据而不区分“合法的_empty 读数”，恰好会在最严重的那一档掩盖故障；且详情里没滤掉 JVM 噪声，噪声还把真正报错顶出了 200 字符窗口。

修复内容（受追踪 **8 个**）：
- **A `scripts/install.sh`（根因项）**：新增 `flink_container_owner()`——优先 `FLINK_DATA_OWNER` 显式覆盖 → 本地已有 `hunter-flink/jobs` 镜像时 `docker run --rm --entrypoint id` 解析 `uid=/gid=`（基座升级导致 UID 漂移时自动跟随）→ 否则退回 `9999:9999` 并 `log_warn`；`${DATA_DIR}/flink` 的 `ensure_dir` 属主由 `root:root` 改为该值。`ensure_dir` 对“已存在但属主不符”会递归 `chown` ⇒ **存量环境重跑 Step 3 即自愈**；Step 3 的成功文案同步抽出 flink 行。
- **B `scripts/health-check.sh` ⑭（假绿项）**：先 `grep -vE '^(WARNING: |SLF4J:)'` 滤掉 JVM 噪声（不占详情窗口）；新增**空列表分支**——命中 `No running jobs.` 即当作合法读数，按 `RUNNING=0 < expected` 判 **FAIL**，文案明写“合法空列表，不是未判定”“典型为提交期失败”、指向 ⑮、并给出**不截断**的取栈命令与手册 §3.6 交叉引用；原 WARN 文案改为“既无作业表头也无 `No running jobs.`”（只用于真正的命令失败）。
- **C `scripts/health-check.sh` ⑮（新增 `check_flink_checkpoint_dir`，项数 15→16）**：**只读**探测（不写探针文件，避免残留被 Flink 当成状态）：`docker exec <jm> sh -c` 里取 `id -u`/`id -g`，沿 `dirname` 上滚到**最近存在的祖先**后 `stat -c "%u %g %a"`；可写性按 owner/group/other 三个八进制位推（`8#$cmode & 8#200/020/002`）；FAIL 详情给出**容器内与宿主两条路径**（`/opt/flink/data*` → `${DATA_DIR}/flink*` 反向映射）、可直接执行的 `chown -R <uid>:<gid>`、**“无需重建容器”**（bind 改属主即时生效，但需重提交）与 Step 3 自愈路径；探测失败/stat 不可用一律 WARN 未判定（不假装 PASS）。新增可选变量 `FLINK_CHECKPOINT_DIR`（默认 `/opt/flink/data/checkpoints`，**容器内路径**；K8s 形态为 `/opt/flink/checkpoints`，不自动探测，已在头注写为该不一致待办）。
- **D `scripts/verify_infra.py` 新增第 18 项 `check_flink_checkpoint_owner_contract()`**：把“长期无人验证的假设”钉成静态门禁——install.sh 的 flink `ensure_dir` 属主**必须是 `$(flink_container_owner)`**（写字面量 `root:root` 直接 FAIL）、`flink_container_owner()` 的兜底 UID/GID **必等于** `02-jobmanager.yaml`/`03-taskmanager.yaml` 的 `runAsUser`/`runAsGroup`（并断言 `runAsNonRoot` 与 `fsGroup` 存在），compose 侧 `execution.checkpointing.dir` 与 bind 卷各计 2 次（JM+TM）。同文件附带 Service 不参与对账（`kind` 限定 Deployment/StatefulSet）。
- **E `scripts/verify_infra.py` 第 14 项：撤掉文档判据，与文档彻底脱钩**。
  - **演进（同一轮内改了三次，如实登记）**：① V1.19.15 及以前把脚本与文档混成一个总数，我据此判“服务器脚本旧”⇒ **归因完全错**（真因是服务器 `docs/` 一份 .md 都没有）；② 本条初稿先拆成「脚本 N / 文档 M（含根 README）」，自查又发现缺失提示判的是 `is_dir()`（空目录会漏判），遂改为按实际 `.md` 份数判空并区分两条文案；③ **用户裁定：文档与程序脱钩——文档的作用是供开发者/运维/使用人员熟悉系统，程序运行不依赖文档**⇒ ①② 两种读数**一并撤除**，本项回归“只校脚本”的单一职责。
  - **最终形态**：PASS 读数只报脚本数——「已校验运维脚本 12 个」；docs/README 存在与否**不产生任何结论、提示或计数**。文档**存在时仍顺带扫描**其中的 Kafka CLI 命令（V1.19.9~11 的真实成因就是“写给人敲的命令漏参数”），但扫描结果只可能让本项因“命令真写错”而 FAIL，**绝不会因文档缺失而 FAIL，也不再报出文档份数**。
- **F K8s 注释登记（不改结构）**：`01-configmap.yaml` 在 `data:` 上方写清“compose 与 K8s 不等价、emptyDir 不跨 Pod、TM 未挂⇒启用前必换 RWX PVC 或 `s3://`”；`03-taskmanager.yaml` 在 `volumeMounts` 里说明“故意不挂”（挂个自己的 emptyDir 反而让 JM/TM 读到两个不同目录，更错）。
- **G 项数同步（三处）**：`scripts/health-check.sh` 头注与 `usage()`、`scripts/daily-check.sh` 头注、根 `README.md` 脚本表、`docs/01-部署概述与环境要求.md` 脚本树均 15→16。
- **H 文档（三份 docs + 两份 README）**：`docs/运维操作手册.md`（§2.2 计数；§3.6 新增“列表为空不等于没取到样本”块与**Checkpoint 目录属主**条目，附只读判定与修复命令）；`docs/故障排查指南.md`（§0 计数；§14 新增**第八类假绿**块：现象/为何取不到 `switched from RUNNING to FAILED`/判据与修复/另附 K8s 同族风险）；`docs/01-部署概述与环境要求.md`（`/data/flink` 行标出属主要求与后果）；`flink-jobs/README.md`（新增“开检查点的前提是目录真的可写”纪律段与“空列表是合法读数”纪律段）。

向后兼容：① 不改 `flink-jobs/` 代码、不改 compose/K8s 结构、不新增必填变量 ⇒ **不需 `--build`**，服务器侧 `chown` + 重提交即可恢复；② 巡检类数 15→16，`source` 复用方（daily-check）自动多一项，仓内无断言项数的用例；③ ⑭ 在零作业场景由 WARN（退出码 1）变 FAIL（退出码 2）——依赖“有 WARN 不拦”的自动化需知晓；④ `install.sh` 对已存在但属主不符的 `${DATA_DIR}/flink` 会递归 `chown -R`（仅限 flink 数据目录，不动 PG/Kafka 等容器自管理目录）；⑤ 新增变量 `FLINK_DATA_OWNER`（安装侧覆盖）与 `FLINK_CHECKPOINT_DIR`（巡检侧覆盖）均为可选；⑥ `verify_infra.py` 第 18 项只**增判据**，其余 17 项语义未变；但第 14 项的 **PASS 文案变了**（旧 “…（21 个脚本/文档）”→ 新 “已校验运维脚本 12 个”）——**任何拿这个读数做判断的自动化/习惯均失效**（用户裁定：不得再用文档读数做判据）。

风险：① 全新环境的 Step 3 早于镜像构建 ⇒ 只能退回 `9999:9999`；若基座镜像换了 UID，预建会错——但 ⑮ 会当场给出**容器内真实 uid/gid** 与可执行 chown（不会退化成静默），且重跑 Step 3（此时镜像已在本地）会自动探测到真值；② 可写性由 mode 位推导，识别不出“只读挂载”（`:ro`）这类挂载级约束——本编排的 flink 卷为可读写 bind，`check_flink_checkpoint_owner_contract` 逐字比对两处卷字符串以防被人改成 `:ro`；③ ⑮ 只看 JM 容器，若将来只给 TM 单独挂不同目录需扩展（当前两容器共用同一 bind 源，已断言）；④ 桩门禁覆盖判级与推导分支，但**本环境无 Docker**，“真容器里 `id -u`/`stat` 可用”待服务器复核；⑤ 未动 `algorithm_metrics` 的创建（属服务器侧动作，跑 `init-kafka.sh` 即可），也未动 `FLINK_EXPECTED_RUNNING` 语义。

验证（本环境，均实跑）：`bash -n scripts/{health-check,install,daily-check}.sh` **×3 OK**（另桩脚本 1 个）；**health-check 桩门禁 26 条断言全 OK**（⑭ 六组：空列表判 FAIL/不降 WARN/噪声被滤/2-3 部分 FAIL/3-3 PASS/终态 FAIL/真命令失败仍 WARN/无容器 WARN；⑮ 九组：root:root 755 FAIL（含宿主路径、chown 命令、“无需重建”、Step 3 文案）、9999:9999 755 PASS、0:0 777 PASS、0:9999 775 PASS、有 w 位缺失仍 FAIL、宿主路径随最近祖先变化、`own=unknown` WARN、探测失败 WARN、无容器 WARN）；`verify_infra.py` **18 项全 PASS**（新项文案“Flink 检查点目录契约三处对齐（install.sh 兜底属主 9999:9999 == K8s runAsUser/runAsGroup，JM/TM 的 checkpoint dir 均在 bind 卷内）”，末项读数已改为“已校验运维脚本 12 个”（不含任何文档信息））；**第 14 项脱钩补测（临时桩 4 组全 OK，跑完即删）**：docs 不存在 / docs 空目录 / docs 有 3 份 ⇒ 三种情况下 PASS 读数**逐字一致**；脚本里命令写错⇒仍 FAIL；文档里命令写错⇒仍 FAIL（证明“脱钩”不等于放松扫描）；桩内 `vi.ROOT` 指向临时目录，不动真仓库文件；`pytest flink-jobs` **88 passed / 1 skipped**；全量单测 **9 目标 1066 passed / 1 skipped**（326 / 88+1s / 71 / 108 / 143 / 105 / 128 / 97）；`ruff check scripts flink-jobs` **All checks passed**（新代码首跑 1 处 FURB167 `re.S` ⇒ 已改 `re.DOTALL` 后复跑绿）；`verify_test_layer.py` **L5 自检通过**；新增 K8s 注释后 `verify_infra` 仍成功加载 **91 个文档**（证明 YAML 未破）。本轮踩到的环境坑：① 桩脚本里 `printf -- '…'` 在 bash 内置 printf 上报 `--: invalid option`（删掉 `--`）；② 桩函数分发要看真实参数位置（`docker exec <ctr> flink list -a` 的 `flink` 是 `$3` 不是 `$4`）；③ flink 作业列表的表头是 `Running/Restarting Jobs:`，桩数据写 `Running Jobs:` 会直接归入“未判定”分支（测不出目标路径）；④ 改错单字时要用 `\uXXXX` 转义（本仓老问题：中文近形字错替）。

分发路径注意：受追踪 **8 个** = `scripts/{install.sh,health-check.sh,daily-check.sh,verify_infra.py}`、`infra/k8s/flink/{01-configmap.yaml,03-taskmanager.yaml}`、`flink-jobs/README.md`、根 `README.md`；另 `docs/` 三份（运维操作手册、故障排查指南、01-部署概述）与 `release.md` 被 `.gitignore` 忽略，**必须单独 rsync**（本次实录已证明：服务器 `docs/` 一份 .md 都没有）。⚠ 本条**不需要重建任何镜像**（未碰 `flink-jobs/` 代码），若连带 V1.19.15/14 一起同步，才需 `--build flink-jm flink-tm` / `data-analytics` / `web-portal`。~~三版均未提交 ⇒ 服务器 `git pull` 取不到~~（V1.19.17 勘误：`git log` 显示 V1.19.13~16 已逐版入库，HEAD `e77e24f` = 本条 ⇒ 服务器**可以** `git pull` 取到 ⑭/⑮/第 14 项；只有 `docs/` 与 `release.md` 仍需 rsync。）

服务器续跑步骤（**先恢复服务，再同步代码**；第 0 步不依赖 rsync，可立即做）：

```bash
# 0) 先拿真 UID（别照抄 9999），再确认目录现状
sudo docker exec hunter-flink-jm sh -c 'id -u; id -g; stat -c "%u %g %a %n" /opt/flink/data'
stat -c '%U:%G %a %n' /data/flink
# 1) 改属主（bind 目录即时生效，**无需 recreate**）
sudo chown -R 9999:9999 /data/flink && sudo chmod 755 /data/flink     # ← 用第 0 步输出的真实 uid:gid
# 2) 补建缺失 Topic（本条已确诊：`algorithm_metrics` + `algorithm_metrics.dlq` 未创建）
cd /opt/HunterCore && sudo bash scripts/init-kafka.sh
sudo bash scripts/init-kafka.sh --check-only                         # 应回“缺失 0｜配置偏差 0”，退出码 0
# 3) 同步本轮代码（受追踪 8 个 + docs 三份），并跑一次 Step 3 验证自愈与探测文案
sudo bash scripts/install.sh --step 3                                 # 期望日志含“flink 容器运行用户…”
# 4) 重提交三作业（镜像未变，不需 --build；仍靠 compose exec 避免 PATH 坑）
for j in detection_job algorithm_performance_job ingest_latency_job; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec flink-jm \
    flink run -d -m localhost:8081 -pym hunter_flink.$j
done
# 5) 巡检与门禁（本条的两项新判据都在 ⑭/⑮）
sudo bash scripts/health-check.sh 2>&1 | grep -E 'Flink|检查点|汇总|共 [0-9]+ 项'
python3 scripts/verify_infra.py | tail -n 4
# 6) 等 ≥ 2 个检查点周期（≈20s）后验位点真的提交了
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo \
           data-analytics-telemetry-raw data-analytics-algorithm-metrics; do
    echo "== $g"; kafka_groups_cli --describe --group "$g" | tail -n +2
  done'
```

判据预告：① 第 5 步 **⑮ 应 PASS**（“容器内 uid=9999:9999 对 /opt/flink/data（0:0 755）可写”形式的判定），若仍 FAIL 则详情里的 `chown` 命令未执行或 UID 探错；**⑭ 应回 `RUNNING=3（期望 3）`**，若再看到 `No running jobs.` ⇒ 本轮新逻辑会直接判 FAIL（不再是 WARN），说明另有提交期障碍，看 ⑮ 与 JM 日志的 `Failed to create|JobInitializationException`；② 第 4 步每行回显必带 `checkpointing enabled: interval_ms=10000`，且不再出现 `Failed to create directory for shared state`；③ 第 2 步后 `--check-only` 必为“缺失 0”且退出码 0（V1.19.15 登记的“真集群未跑过”从此销项）；④ 第 6 步四个 `data-analytics-*` 组应从 `does not exist` 变成分区表格（本条修完才是“真开了检查点”，上一轮只是“代码开了”）；⑤ 看板侧四个组应从 `unknown_groups` 逐步退回 `kafka_consumer_lag`（首次可能因 earliest 重放一度很高）；⑥ 汇总行应出现“共 45 项”（新增 ⑮），仍看到“共 44 项”⇒ `health-check.sh` 没 rsync 到位（用 `grep -c 'check_flink_checkpoint_dir' scripts/health-check.sh` 判定，**不要再用 `verify_infra` 末项总数当版本指纹**）；⑦ **本条不提供任何“文档是否同步”的判据**（用户裁定：文档与程序脱钩，文档只供人熟悉系统）——docs 那三份照旧 rsync（运维要看），但不得从程序读数反推。

**待办（不属本条范围）**：① `-algo` 之外的 DLQ 对账与 ⑩ 单组探测（V1.19.15 待办⑥同）；② **K8s 形态检查点共享存储**（本条只写注释）：`dir` 需换成 JM/TM 可见的 RWX PVC 或 `s3://`，同时补 TM 挂载——在那之前**不要给 K8s 配 `execution.checkpointing.interval`**；③ compose `/data/flink` 无保留策略（历史检查点无限增长）；④ ⑮ 未覆盖“只读挂载”这种挂载级约束（只看 mode 位）；⑤ 前端未渲染 `unknown_groups`（V1.19.14 待办②）；⑥ CI lint 当前 HEAD 会红 50 处（V1.19.15 待办③）；⑦ `.env.example` ↔ compose/代码 变量名对账门禁（V1.19.15 待办④）；⑧ 本环境无 Docker：本条为桩测 + 静态对账，⑮ 的真容器行为与 chown 后的恢复效果**待服务器实机复核**；⑨ **文档与程序脱钩后的残留口径**：第 14 项仍会扫描 `docs/` 顶层与根 README 里的 Kafka CLI 命令（防“写给人敲的命令错”），但**不再输出文档份数**⇒ 运维无法从校验器读数反推文档是否同步（按裁定这是应该的）；如日后确需校验文档同步，必须与本校验器解耦（单独脚本或 CI 任务），不得回退成程序里的判据。

## V1.19.17（修复：端口预检把「本项目容器占用」的豁免场景刷成 5 条 `[ERROR]` 假故障——探针函数替调用方定了级；另修 Step 9 Topic 计数文案与 ⑮ 的 PASS 文案自相重复）

Date：2026-10-12
变更类型：修复（**运维脚本：判级归属 + 文案**；不改契约、不改 compose/K8s 结构、不改作业代码 ⇒ **不需重建镜像**，也不需要重提交作业）

先把本份 231 行实录（服务器 16:17–16:31）对 **V1.19.16 的结案**记清楚（本条不再重复修复任何已确证项）：
- **主因确证**：容器内 `id -u`/`id -g` = **9999 / 9999**（官方镜像），`/opt/flink/data` 属 `0 0 755`、宿主 `/data/flink` 属 `root:root 755` ⇒ 与推断逐字相符。`chown -R 9999:9999` 后**三个作业全部提交成功**（三次 `[hunter_flink] checkpointing enabled: interval_ms=10000` + JobID），⑭ `[PASS] Flink 作业健康 - RUNNING=3（期望 3）`，⑮ 首跑即 PASS，汇总「通过 42 项，警告 3 项，**失败 0 项**（共 **45** 项）」——第八类假绿的补位判据在真机上生效。
- **`--check-only` 销项**：「契约要求 14（含死信）｜**缺失 0**｜配置偏差 0 / 集群实际 23 个 Topic，其中契约外 0 个」，`algorithm_metrics` 与 `algorithm_metrics.dlq` 已由 `init-kafka.sh` 建出 ⇒ V1.19.15/16 两轮的「真集群没跑过对账」彻底结案。
- **第 14 项脱钩版已在服务器生效**：末项读数「已校验运维脚本 12 个」，不含任何文档信息。

本轮新暴露的三处（都是**读数/文案**问题，不是运行时故障）：
- **① 端口预检的假 `[ERROR]`（本条主因）**：`install.sh --step 3` 一次正常续跑刷出 5 条 `[ERROR] 端口 80/3000/… 已被占用`，紧接着又是 5 条 `[INFO] 端口 … 由本项目容器占用，视为通过`。根因是**判级归属错**：`common.sh check_port()` 这个探针函数自己 `log_error`，而调用方 `install.sh check_public_ports()` 之后还有一层「本项目容器占用 ⇒ 放行」的豁免——**探针抢在结论之前把「其实通过」的事写成了 ERROR**。后果不是难看，是**信噪比归零**：真占用时同样只有 5 条 ERROR，运维无法区分“续跑常态”和“nginx 抢了 80”，而按 ERROR 去排障会白跑一趟。
- **② Step 9 文案过期**：`install.sh` Step 9 说明写「6 个平台 Topic」，契约实为 **7 个平台 Topic + 7 个死信**（`--check-only` 的“契约要求 14”即由此而来）。
- **③ ⑮ 的 PASS 文案自相重复（V1.19.16 引入）**：实录读作「对 `/opt/flink/data/checkpoints` 可写，可在其下创建 `/opt/flink/data/checkpoints`」——同一句里同一路径出现两次、看着像输出被截断。根因：我把「最近存在的祖先」一律写成 `${cdir}`、又把「其下创建」的宾语写成 `${target}`，而 `chown` 后 Flink 已把 `checkpoints` 建了出来 ⇒ `cdir == target`，两个变量取到同一个值。**判级一直是对的，错的只是这句话。**
- **℞ 对我上一条交付命令的两处勘误**（服务器实跑撞上）：a) **我给的 `sudo bash scripts/install.sh --step 3` 漏了 `-y`** ⇒ 脚本停在 `确认开始部署？[y/N]` 并回显「用户取消部署：**未执行任何步骤**」 ⇒ **Step 3 的自愈路径与 `flink_container_owner()` 的探测日志至今仍未实机验证**（列入待办①；属我的命令漏参，不属脚本缺陷）；b) 我曾把 Step 7 文案里的 ZooKeeper 列为「KRaft 迁移后的过期项」，**经复核撤回**：`infra/deploy/docker-compose.yml` 确有 `zookeeper-init` + `zookeeper`（bitnami/zookeeper:3.8），生产 Kafka 走 **ZooKeeper 模式**（内部 9092 SASL_PLAINTEXT / 外部 9093 SASL_SSL），KRaft 只存在于根开发 compose 与 K8s ⇒ 该文案**不动**（差点误修一次）。

修复内容（受追踪 **3 个**）：
- **A `scripts/common.sh`**：`check_port()` 新增可选第二参 `quiet`——传 `quiet` 时**只回状态、占用明细写 stdout、不打 ERROR**（交由调用方定级）；不传时行为与文案**逐字不变**（`require_port_free` 这类“占用即故障”的调用方继续自报 ERROR）。顺带处理一个只有在 quiet 下才暴露的坑：quiet 调用方必然处于命令替换中（stdout 被捕获），因此「缺少 ss/netstat/lsof」的提醒在 quiet 分支必须 `>&2`，否则会被默默吞掉。函数头写明“探针不得替调用方定级”的口径，头注函数清单同步。
- **B `scripts/install.sh`**：`check_public_ports()` 局部变量加 `port_detail`，TCP 分支改为 `if port_detail="$(check_port "$port" quiet)"`；最终那条「已被**非本项目**进程占用」的 ERROR 用 `${port_detail:+：${port_detail}}` 并入占用明细——**噪声没了，信息量还多了**。Step 9 文案改为「7 个平台 Topic + 7 个死信」。
- **C `scripts/health-check.sh` ⑮**：PASS 文案按 `[ "$cdir" = "$target" ]` 分两支——target 自身已存在 ⇒「对 `<路径>`（属主 模式；宿主 `<host>`）可写，Flink 可直接在此创建检查点」；仅上滚到祖先 ⇒「对**最近存在的祖先** `<路径>`（…）可写，可在其下创建 `<target>`」。两支都顺带报出**宿主路径**（`${host}` 早已算好，原先只出现在 FAIL 里）。同时把 FAIL 文案里的自愈命令补成 `install.sh --step 3 -y`（实录证明“没带 `-y`”是一个真实会撞上的坑，写给人敲的命令必须带全参数）。**可写性推导、FAIL/WARN 判级与退出码一字未动。**
- **D 文档（两份）**：`docs/故障排查指南.md` 补“**ERROR 与 INFO 成对出现 ⇒ 假故障**”这一类读数判据（并点明根因是探针替调用方定级）；`docs/运维操作手册.md` 的 `--step N` 续跑段明写**非交互执行必须带 `-y`**，并给出 Step 3 自愈的完整命令行。

向后兼容：① `check_port` 只是**新增可选参数**，仓内其余调用方（`require_port_free`、daily-check 等）不传参 ⇒ 输出与退出码逐字不变；② 端口**真被非本项目进程占用**时仍是 `log_error` + `return 1`，`install.sh` 的拦截行为与退出码不变（只是不再重复报一遍）；③ ⑮ 的 PASS/FAIL/WARN 判据与退出码不变，仅 PASS 详情文案变 ⇒ **任何 grep 该文案的自动化需知晓**（旧串「可在其下创建」今后只在 target 尚未创建时出现）；④ 未碰 compose/K8s/作业代码 ⇒ 不需 `--build`，也不需重提交作业。

风险：① 若将来别人给其他调用方也传 `quiet` 却忘了打印返回值，占用会被**完全静默**——已在函数头注写清“由调用方定级”并给出唯一正确用法；② ⑮ 仍是**推导式**可写性判断（只看 mode 位），识别不出 `:ro` 挂载（V1.19.16 待办④同）；③ 本条只修读数噪声，不改变任何真故障的判级 ⇒ 服务器上若再看到端口 ERROR 而**后面没跟**「视为通过」，那就是真占用，照 ERROR 排障。

验证（本环境，均实跑）：`bash -n scripts/{common.sh,install.sh,health-check.sh}` **×3 OK**；**`check_port` 桩 11 条断言全 OK**（默认模式仍打 `[ERROR] 端口 … 已被占用`、quiet 模式**无 ERROR** 且明细回传 stdout、quiet 缺工具时提醒出现在 stderr 而非 stdout、未占用时两模式都返回 0、`require_port_free` 路径不变；桩首跑因 bash 内置 `case` 一行式报语法错 ⇒ 改 `if/else` 后绿）；**⑮ 桩 9 条断言全 OK**（target 已存在 ⇒ PASS 且含「Flink 可直接在此创建检查点」、**不再出现重复路径**、不误报「最近存在的祖先」；仅祖先存在 ⇒ PASS 且为「最近存在的祖先 /opt/flink/data（9999:9999 755；宿主 /data/flink）可写，可在其下创建 /opt/flink/data/checkpoints」；属主不符 ⇒ 仍 FAIL 且保留 `sudo chown -R 9999:9999 /data/flink`；桩首跑因我自己把状态拼成 `|PASS|` 而 `record_result` 真实格式是 `$1=状态` ⇒ 改 `[PASS] ` 后 9/9）；`verify_infra.py` **18 项全 PASS**；`ruff check scripts flink-jobs` **All checks passed**；`run_unit_tests.py` **9 目标 1066 passed / 1 skipped**（326 / `tests` 为 100 deselected / 88+1s / 71 / 108 / 143 / 105 / 128 / 97）——与上一轮**逐字一致**。本轮踩到的环境坑：① **把两条命令用 `&` 并发写同一个输出文件**，读到的是交错污染的日志（曾冒出“17/17 个目标套件 / 累计 858 条”这种本仓脚本根本不会打印的字串 ⇒ 已用串行复跑否决，**勿据此登记**）；② `ruff check` 顺手把 `common/python` 纳进范围会命中 5 处既有 **UP046/UP047**（`Generic` 类/函数建议改用类型参数），与本条无关，归入 CI lint 待办；③ 取输出一律「写文件 + Read」——本环境里 `findstr`（默认大小写敏感、匹配中文还会断 `&&` 链）、`iconv`、管道都不可靠。

分发路径注意：受追踪 **3 个** = `scripts/{common.sh,install.sh,health-check.sh}`；`docs/` 两份与 `release.md` 被 `.gitignore` 忽略（`git ls-files docs release.md` 为空、`git check-ignore` 命中 `.gitignore:158/159`）⇒ **必须单独 rsync**。本条 3 个文件**尚未提交**（`git status` 仅此 3 项），服务器 `git pull` 取不到本条。

服务器续跑步骤（本条不拦服务，先取证再同步）：

```bash
cd /opt/HunterCore
# 1) 同步本条 3 个脚本 + docs 两份（按现有分发方式）
# 2) 补做上一轮没做成的事：**带 -y** 跑 Step 3，验自愈与 UID 探测日志
sudo bash scripts/install.sh --step 3 -y 2>&1 | tee /tmp/step3.log
grep -nE 'flink 容器运行用户|由本项目容器占用|已被占用' /tmp/step3.log   # 期望：有探测日志+“占用即通过”，**没有** ERROR
stat -c '%U:%G %a %n' /data/flink                                        # 仍应为 9999:9999 755
# 3) 取回上轮被我的 grep 过滤掉的 3 项 WARN 真身
sudo bash scripts/health-check.sh 2>&1 | tee /tmp/hc.log | tail -n 6
grep -nE '^\[(WARN|FAIL)\]' /tmp/hc.log
# 4) 位点侧三处待取证（**全量输出，别加 tail**）
sudo bash -c 'source scripts/common.sh; load_env
  echo "== groups"; kafka_groups_cli --list
  for g in data-analytics-telemetry-alert data-analytics-telemetry-algo data-analytics-telemetry-raw; do
    echo "== $g"; kafka_groups_cli --describe --group "$g"
  done'
# 5) 三作业的检查点到底有没有提交（REST 比日志硬）
for id in $(sudo docker exec hunter-flink-jm sh -c 'flink list -a' | sed -n 's/^\([0-9a-f]\{32\}\).*$/\1/p'); do
  echo "== $id"; curl -s "http://127.0.0.1:8088/jobs/${id}/checkpoints" | head -c 900; echo
done
sudo docker logs hunter-flink-jm 2>&1 | grep -E 'Committing offsets|Found new partitions|switched from RUNNING|Completed checkpoint' | tail -n 40
```

判据预告：① 第 2 步是本条**主判据**：`/tmp/step3.log` 里那 5 个端口**只出现「由本项目容器占用…视为通过」、不再出现配对的 `[ERROR] 端口 … 已被占用`**；若仍成对出现 ⇒ 脚本没同步到位（`grep -c 'check_port "\$port" quiet' scripts/install.sh` 应为 1）；同时应首次看到 `flink_container_owner()` 的探测输出（待办① 销项）；② 第 2 步后 `/data/flink` 仍是 `9999:9999 755`——若变回 `root:root` ⇒ 探测退回失败，看该步的 `log_warn`；③ 第 3 步三项 WARN 必须能被点名（汇总仍为「共 45 项」），若某条详情写着 `未判定` ⇒ 按它自己的文案继续取证，**不要当 PASS**；④ 第 4 步：`telemetry_clean` 是 6 分区 ⇒ `-alert`/`-algo` 的表格应给到 **6 行**；若全量输出仍只有 1 行，那是“只分配/只提交了 1 个分区”而非截断，需结合第 5 步的 `Found new partitions` 定性；若 `--list` 里根本没有 `data-analytics-telemetry-raw` ⇒ `ingest_latency_job` **从未提交过位点**（先确认它是否还在 RUNNING）；⑤ 第 5 步每个作业必须 `completed > 0`（`interval_ms=10000` ⇒ 两分钟约 12 个）才算“真开了检查点”；`completed=0` 且 `failed` 增长 ⇒ 回到 ⑮ 与 JM 日志的 `Failed to create`。

**待办（不属本条范围，均为未确诊项，不得当已修）**：① **`install.sh --step 3` 的自愈路径 + `flink_container_owner()` 探测日志未实机验证**（上轮我漏 `-y`）；② `data-analytics-telemetry-raw` 组仍 `does not exist` ⇒ `ingest_latency_job` 的首个检查点是否成功未证；③ `-alert`/`-algo` 各只回一行（PARTITION 6，LAG 60000 / 116145）——“真只提交一个分区”与“读数被截”两种解释未定（注：`has no active members` 本身是 Flink KafkaSource 的正常指纹——assign 而不 join group，V1.19.15 已记）；④ `data-analytics-algorithm-metrics` 组的消费者 CONSUMER-ID/client-id **全在 data-analytics 服务侧** ⇒ 该组位点与 `-algo` 作业无关，看板别把它的 LAG 归给作业；⑤ 3 项 WARN 至今未点名；⑥ 沿用 V1.19.16 待办②~⑨（K8s 检查点需 RWX、`/data/flink` 无保留策略、⑮ 不识别只读挂载、前端未渲染 `unknown_groups`、CI lint（本轮又实测出 `common/python` 5 处 UP046/UP047）、`.env.example` 变量名对账、文档脱钩后的残留口径）。

## V1.19.18（修复：被 `$(…)` 捕获的函数往 stdout 打日志 ⇒ `chown` 收到的是「值 + 整行日志」、Step 3 属主自愈从未生效；另修 `ensure_dir` 的「失败也报 SUCCESS」，并把这条形状判据固化成 `verify_infra` 第 19 项）

Date：2026-10-12
变更类型：修复（**运维脚本 + 静态校验器**；不改契约、不改 compose/K8s 结构、不改作业代码 ⇒ 不需重建镜像。但注意：本条**会让 Step 3 在属主设不上时真的失败**，而不是默默继续）

先把这份 2900 行实录（服务器 16:57–17:08）对 **V1.19.17 的结案**记清楚（本条不重复修复任何已确证项）：
- **主判据成立**：`/tmp/step3.log` 里 6 个对外端口（80/8080/9000/9093/1935/8000）**全部只出现**「端口 N 由本项目容器占用（hunter-xxx），视为通过」、**没有一条配对的 `[ERROR] 端口 … 已被占用`** ⇒ 判级归属修复确证（V1.19.17 判据① 销项）。
- **⑮ 新文案在真机逐字正确**：`[PASS] Flink 检查点目录 - 容器内 uid=9999:9999 对 /opt/flink/data/checkpoints（9999:9999 755；宿主 /data/flink/checkpoints）可写，Flink 可直接在此创建检查点`——同一路径不再重复两遍，宿主路径也带出来了。
- **3 项 WARN 已点名**（`hunter-flink-tm` 未定义 healthcheck、timescale 未部署 ×2），汇总「通过 41 项，警告 3 项，失败 1 项（共 45 项）」⇒ V1.19.17 待办⑤ 销项。
- **检查点真在提交（位点推进是硬证据）**：`-alert`/`-algo` 两组在 `telemetry_clean` PARTITION 6 上 `CURRENT-OFFSET 621731`、`LAG 1553/1574`（上轮 LAG 60000/116145）⇒ V1.19.17 待办③ 中「是否真提交」这一半结案；`-raw` 仍 `does not exist`、分区 0–5 仍无行 ⇒ 待办② 与「为什么只有 PARTITION 6」继续挂账。
- **V1.19.17 待办①（Step 3 自愈 + 探测日志）不结案，且结论反转**：探测日志这次出来了，**但正是它把 `chown` 打死的**（见下）。同一条命令的 `stat` 回显（实录 2832 行）为 `UNKNOWN:root 755 /data/flink` ⇒ **属主压根没改**（组仍是 root/gid 0），日志里那句 `[SUCCESS] 已设置属主：/data/flink → 9999:9999` 是假的。

**分发状态更新（℞ 对 V1.19.17 分发段）**：该条 3 个文件**已由用户提交**——HEAD = `98c4b3c`（date 2026-10-12 16:56:29，含 `scripts/{common.sh,install.sh,health-check.sh}`）⇒ 那句「本条 3 个文件尚未提交、服务器 `git pull` 取不到」**作废**（实录 16:57 的读数也确实带上了新行为）。`docs/` 与 `release.md` 仍被 `.gitignore:158/159` 命中 ⇒ 照旧单独 rsync。

本条主因——一条完整的「坏数据 ⇒ 假成功 ⇒ 假绿」链（实录 64–66 行）：
- **① 探针函数给返回值加了料**：`common.sh` 的 `_hc_emit()` 只有 **ERROR 走 stderr**，INFO/SUCCESS/WARN/DEBUG 一律 stdout。`flink_container_owner()` 在 `printf '%s:%s'` **之后**调 `log_info` ⇒ `$(flink_container_owner)` 得到「`9999:9999` + 一整行带时间戳的日志」。它被直接当 `chown` 的属主参数，而 `chown` 无法把这一整串解析成 `user:group` ⇒ `chown: 无效的组: "9999:9999[…][INFO] flink 容器运行用户…"`（**报错里那对引号回显的就是被污染的实参**）；参数解析在任何 chown 系统调用**之前**，所以**整条中止、一个字节都没改**（同轮 `stat` 读数即为旁证）。
- **② 为什么潜伏了一整轮没炸**：旧实现的三条分支里只有「镜像不存在 ⇒ 兜底 `printf '9999:9999'`」不打日志。**全新部署**时 Step 3 跑在 Step 6（构建镜像）之前，走的正是这条干净分支 ⇒ 装出来的环境是对的；而**任何续跑**（镜像已在本地）必走探测分支 ⇒ 自愈必坏。这解释了「V1.19.16 落地时没人发现」。
- **③ 放大器：`ensure_dir` 不看退出码**：`install -d` / `chown -R` / `chmod` 三条命令全部裸调用，函数末尾固定 `return 0`；而 `run_step` 是用 `"$@" || rc=$?` 调步函数——**这种上下文会关掉 `set -e`** ⇒ chown 失败后下一行照打 SUCCESS、Step 3 判「✓ 完成」。**假成功 + 假绿，运维读到的是「已修好」。**
- **④ 旧实现另有一处口径错**：只比 `stat -c '%u:%g'`，而调用方大量传符号名（`root:root`）⇒ 永远「不符」⇒ 每次续跑都无谓 `chown -R` 并打一条 SUCCESS（实录里 `/data/srs`、`/data/backups` 恒打 SUCCESS 即此）。修复后按**符号名 + 数字双口径**比对，任一相符即视为已符合。
- **℞ 对我上一轮交付命令的两处勘误**（都被这次实录撞上）：a) 我给的 `stat -c '%U:%G %a %n'` **对容器 uid 不可用**——宿主 `/etc/passwd` 没有该 uid 条目时回显 `UNKNOWN`，读数本身不具判别力 ⇒ 运维命令必须用数字 `%u:%g`（⑮ 内部用的就是 `%u %g %a`）；b) 我把 `--step 3 -y` 描述成「重跑 Step 3 验证自愈」，而它的真实语义是**从第 3 步续跑到底** ⇒ 实录跑满了 Step 3→13：Step 6 重建镜像（≈8 分钟）、Step 11 `Container hunter-flink-jm Recreated` / `hunter-flink-tm Recreated`（连同 collector/analytics/api-gateway 等）⇒ standalone（无 HA）下**三个作业全丢** ⇒ ⑭ `[FAIL] Flink 作业数 - RUNNING=0 < 3：JM 明确返回 No running jobs.`、Step 12 `[ERROR] ✗ Step 12/13 失败…（退出码 1）`。**这两条读数是正确的**（V1.19.16 建立的「合法空列表不等于未判定」判据第一次抓到真事故），不是判据缺陷 ⇒ 本条不改判据，改的是**让脚本自己把这个坑喊出来**（见 A）。注：⑮ 这次仍 PASS，是因为它看的是容器内的 **checkpoints 子目录**（属 9999:9999，上一轮人工修正后由 Flink 自建）⇒ **目录能写 ≠ 宿主侧自愈生效过**。

修复内容（受追踪 **3 个**）：
- **A `scripts/install.sh`**：
  · `ensure_dir()` 重写——属主参数先过白名单 `*[!A-Za-z0-9_.:-]*`（不合法 ⇒ `log_error` 带 `printf '%q'` 原形 + `return 1`，**绝不透传给 chown**）；`install -d`/`chown -R`/`chmod`/`stat` **每条命令各自判退出码**并在失败时 `return 1`；属主比对双口径；头注写明「本函数常在 `"$@" || rc=$?` 上下文里跑，那种上下文会关掉 set -e」。
  · `ensure_data_dir()` 补 `install -d`/`chown` 的退出码（原同样是裸调用）。
  · `flink_container_owner()`：两条日志一律 `>&2`；`FLINK_DATA_OWNER` 覆盖值加同一套白名单校验（非法 ⇒ WARN 走 stderr 并**回落探测值**，不退出）；头注写清「stdout 只准写那一个 uid:gid」+ 实录原文。
  · `step_3` 内**全部** `ensure_dir`/`ensure_data_dir` 调用加 `|| return 1`（含 kafka-buffer 循环），并把「为什么必须显式接返值」写在调用段注释里。
  · `--step` 可读性：`show_plan()` 在 `START_STEP>1` 时 `log_warn` 点明「= 从第 N 步**续跑到底**，不是只跑这一步」，且 `START_STEP≤11` 时追加「后续含 Step 6 构建 + Step 11 启动 ⇒ 镜像变化会 recreate flink-jm/tm ⇒ **已提交作业会丢**，只想校目录属主请加 `--skip-build`」；`usage()`、头注、示例（新增 `--step 3 --skip-build -y` 一行）、取消回显（「非交互请加 -y」）四处口径统一。
  · 第二处过期文案：Step 9 说明里「内部 Topic（6 个）」→「7 个平台 Topic + 7 个死信」（V1.19.17 只改了计划表那处，这处漏了）。
- **B `scripts/common.sh`**：`check_port()` 缺 ss/netstat/lsof 的提醒由「仅 quiet 分支 `>&2`」改为**两模式统一 `>&2`**——守住的是「stdout 只写占用明细」这条**形状不变量**，而不是给某个调用方开特例；头注新增「输出纪律」段。
- **C `scripts/verify_infra.py`**：新增第 19 项 `check_captured_function_stdout_purity()`——解析 `scripts/*.sh` 的函数体，凡是被 `$(func …)` 捕获且函数体内有未 `>&2`、未 `# stdout-ok 理由` 豁免的 `log_info|success|warn|debug` 即 FAIL，并点名 `文件:行号 被 $(func …) 捕获`；docstring 摘要补第 15/16 条。新增 `_emit_line()`：控制台码页编不出的字符降级替换，**绝不让校验器因 print 崩掉**（实录：新门禁首跑在 `print(line)` 抛 `UnicodeEncodeError: 'gbk' … '\u21d2'`，前 18 项结论全丢）。

向后兼容：① `ensure_dir` 现在**会返回非 0** ⇒ 存量环境若某目录属主真设不上（只读挂载、被占用、跨设备权限），Step 3 会在这一步停住而不是默默跑完——这是有意的（坏环境不该带着跑完全栈）；② 双口径比对让「每次续跑都打一条 `已设置属主：… root:root`」的 SUCCESS 噪声消失，**靠这条日志做变更审计的人需知晓**；③ `check_port` 缺工具提醒改走 stderr ⇒ 若有外部脚本 `check_port <p> | grep 缺少` 会抓不到（仓内无此用法）；④ 第 19 项是**新增静态判据**，豁免必须带理由，不是自动放行；⑤ 未碰 compose/K8s/作业代码 ⇒ 不需 `--build`；但**作业仍需重新提交**（是上一轮 `--step 3` 的重跑把它们冲掉的，不是本条造成）。

风险：① 第 19 项是**形状判据**，认不出「函数返回的值本身写错」，也拦不住跨行续写 `\` 或先把日志取进变量再打印的绕法（留给 CI 加 shellcheck 一并兜）；② `ensure_dir` 的白名单会拒绝合法但罕见的属主写法（如 `user@host` 域账号，本机场景不适用）；③ **本条未经实机复核**（本环境无 Docker），「修复后 chown 真的改了属主」仍待服务器取证。

验证（本环境，均实跑）：`bash -n scripts/{common.sh,install.sh,health-check.sh,daily-check.sh}` **×4 OK**；**`ensure_dir`/`flink_container_owner` 桩 22 条断言全 OK**（chown 失败 ⇒ 返回 1 + ERROR 带当前数字属主 + **绝不**报 SUCCESS；污染值 ⇒ 拒收且不透传给 chown；符号名/数字相符 ⇒ 跳过 chown 且无 SUCCESS 噪声；真不符 ⇒ `chown -R` 后才报 SUCCESS；`$(flink_container_owner)` 恰为 `9999:9999`、单行、探测日志在 stderr；`FLINK_DATA_OWNER` 非法 ⇒ 忽略并回落探测 + WARN 走 stderr；**第 22 条是反向对照**——另造一个「函数内 log 走 stdout」的函数并断言其返回值确实被污染，否则「全绿」只说明桩没测到东西）。桩首跑 3 条假红，责任在**桩自己**：把 `log_*` 打桩成 `>> "$OUT"` 会让源码里的 `>&2` 失效（重定向只作用于 fd1），改成写真实 fd + 调用点 `>"$OUT" 2>"$ERR"` 后绿；另 `rc_is "$(ed …)"` 会把被测函数扔进子 shell ⇒ `CHOWN_CALLED` 传不回来、4 条状态断言属假绿，改为在本 shell 里跑并用全局 `RC` 接返值。**第 19 项专项桩 8 条全 OK**（临时 `scripts/` 造形状）：旧缺陷形状 ⇒ FAIL 且点名 `a.sh:3 被 $(f_owner …) 捕获`；`>&2` ⇒ PASS；`# stdout-ok 理由` ⇒ PASS；**未被捕获**的日志函数 ⇒ 不误伤；`log_error` ⇒ 不误伤；跨文件（定义在 a.sh、捕获在 b.sh）⇒ FAIL；豁免标在捕获行 ⇒ 不计入；**真仓库 ⇒ PASS**（`已解析 169 个函数、28 个被捕获调用`）。`verify_infra.py` **19 项全 PASS**（新门禁首跑时如实 FAIL 在 `common.sh:192 被 $(check_port …) 捕获`，即 V1.19.17 留下的那处 stdout 提醒——**我选择修函数而不是加豁免**）；`ruff check scripts flink-jobs` **All checks passed**；`run_unit_tests.py` **9 目标 1066 passed / 1 skipped**（326 / `tests` 100 deselected / 88+1s / 71 / 108 / 143 / 105 / 128 / 97）——与 V1.19.17 **逐字一致**。

分发路径注意：受追踪 **3 个** = `scripts/{install.sh,common.sh,verify_infra.py}`；**本条尚未提交**（`git status` 仅此 3 项，HEAD 仍是 V1.19.17 的 `98c4b3c`）⇒ 服务器 `git pull` 取不到本条；`docs/` 两份与 `release.md` 仍 rsync。

服务器续跑步骤（本条会改文件系统属主；**务必带 `--skip-build`**，否则又是一次全栈重部署）：

```bash
cd /opt/HunterCore
# 1) 同步本条 3 个脚本 + docs 两份
# 2) 修复前留证（用数字口径，别用 %U:%G）
stat -c '%u:%g %a %n' /data/flink /data/flink/checkpoints 2>/dev/null
# 3) 只校目录属主：--skip-build 避开 Step 6 重建镜像 / Step 11 recreate flink 容器
sudo bash scripts/install.sh --step 3 --skip-build -y 2>&1 | tee /tmp/step3_v18.log
grep -nE '无效的组|已设置属主|属主参数非法|flink 容器运行用户|Step 3' /tmp/step3_v18.log
# 4) 本条主判据：属主是否真的改上了
stat -c '%u:%g %a %n' /data/flink          # 期望 9999:9999 755
# 5) 重提交三个作业（上一轮被 recreate 冲掉），每行回显应带 checkpointing enabled
for j in detection_job algorithm_performance_job ingest_latency_job; do
  sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . exec flink-jm \
    flink run -d -m localhost:8081 -pym hunter_flink.$j
done
sudo docker exec hunter-flink-jm flink list
# 6) 检查点 REST（有作业才是有效读数）
for id in $(sudo docker exec hunter-flink-jm sh -c 'flink list -a' | sed -n 's/^\([0-9a-f]\{32\}\).*$/\1/p'); do
  echo "== $id"; curl -s "http://127.0.0.1:8088/jobs/${id}/checkpoints" | head -c 900; echo
done
sudo bash scripts/health-check.sh 2>&1 | tee /tmp/hc18.log | tail -n 8
grep -nE '^\[(WARN|FAIL)\]' /tmp/hc18.log
```

判据预告：① 第 3 步**不得**再出现 `chown: 无效的组`，也不得出现「上面报 chown 错、下面紧跟 SUCCESS」；若出现 `[ERROR] ensure_dir 属主参数非法` ⇒ 还有别的路径在污染，**把该行原文回传**（`printf '%q'` 会显出真身）；若 Step 3 直接失败退出 ⇒ 是真设不上（挂载/权限），**这才是应有的行为**，按明细排障，别反过来当「脚本坏了」。② 第 4 步必须变 `9999:9999 755`；仍是 `9999:0` 之类 ⇒ 自愈没生效，回传第 3 步全量输出。③ 探测日志 `flink 容器运行用户…` 现在应出现在 **stderr**：`tee` 前若没写 `2>&1` 就看不到它，属正常。④ 第 3 步末尾脚本内部还会跑 Step 7–13（本条未提供「只跑单步」能力，见待办④——**V1.19.19 已提供 `--only-step`，下一轮起请改用它**）：**其中 Step 12 在作业重提交之前必然 FAIL（⑭ RUNNING=0）**，属预期，请按第 5 步重提交后再看第 6 步的 health-check。⑤ 第 6 步每作业 `completed > 0` 且随时间增长（`interval_ms=10000` ⇒ 每分钟约 6 个）才算「检查点真提交」——这是唯一硬判据，LAG 只是间接读数。⑥ 若镜像未变而第 3 步里仍冒出 `Recreated`，那是 compose 配置差异所致，取证 `docker compose config` 输出，**不要改判据**。

**待办（不属本条范围，均为未确诊项，不得当已修）**：① 本条修复**未实机复核**（本环境无 Docker）；② `data-analytics-telemetry-raw` 组不存在 ⇒ `ingest_latency_job` 是否从未提交位点仍未证；③ `telemetry_clean` 是 6 分区，但 `-alert`/`-algo` 都只有 PARTITION 6 有 committed offset ⇒「只分配到 1 个分区」还是「读数被截」仍需 `--describe` 全量输出 + 作业 `Found new partitions` 日志定性；④ **已销项（用户批准后由 V1.19.19 落地）**：`install.sh` 新增 `--only-step N`，本条当时只加了警示文案与 `--skip-build` 指引；⑤ JM/TM 重建即丢作业（standalone 无 HA），是否要在 ⑭/⑮ 之外补一条「作业数与上次部署记录对账」的巡检，待评估；⑥ 沿用 V1.19.16/17 待办（K8s 检查点需 RWX、`/data/flink` 无保留策略、⑮ 不识别只读挂载、前端未渲染 `unknown_groups`、CI lint（含 `common/python` 5 处 UP046/UP047、shellcheck 未进 CI）、`.env.example` 变量名对账、文档与程序脱钩后的残留口径）。

## V1.19.19（新增：`install.sh --only-step N`——只跑第 N 步就收尾；并把「步数常量 / 描述表 / case 臂 / usage」对账固化成 `verify_infra` 第 20 项）

Date：2026-10-12
变更类型：新增（**运维脚本参数能力 + 静态校验器**；不改契约、不改 compose/K8s 结构、不改作业代码 ⇒ 不需重建镜像，默认全流程行为不变）

动机（承接 V1.19.18 待办④，**用户已批准这一功能变更**）：V1.19.17 实录里「本意只是重跑 Step 3 验证属主自愈」实际跑满了 Step 3→13——Step 6 重建镜像（≈8 分钟）、Step 11 `compose up -d` recreate `flink-jm/flink-tm` ⇒ standalone（无 HA）下三个作业全丢，⑭ 报 `RUNNING=0`、Step 12 失败退出。V1.19.18 只把这件事**喊出来**（警示文案）+ 给了 `--skip-build` 这条绕法，但**语义缺口仍在**：`--skip-build` 只挡住 Step 6，Step 7/9/10/11/12 照跑——Step 11 只要 compose 配置有差异仍可能 recreate 容器，Step 12 在作业重提交之前必然 FAIL（又是一次需要人工排除的假故障读数）。本轮补上「只跑一步」这个能力本身。

变更内容（受追踪 **2 个**）：
- **A `scripts/install.sh`**：
  · 新增长选项 `--only-step <n>`（1-13）：与 `--step` **互斥**（报错文案直接写明「前者从第 N 步续跑到底，后者只跑第 N 步」）；非整数 / 越界 / 缺参数一律 `die`，**一个步骤都不执行**。
  · 新增全局 `STOP_STEP`（执行区间上界，默认 `TOTAL_STEPS`）/ `ONLY_STEP` / `STEP_GIVEN` / `ONLY_GIVEN`；`--only-step N` 把 `START_STEP` 与 `STOP_STEP` 同时收紧为 N。
  · `run_all_steps()` 增加**上界跳过**分支，跳过回显从「`--step N`」改为「起点 N / 终点 N」（`--only-step` 下上下界同号，旧文案一律写 `--step` 属误导）。**必须写成独立 `if`**：`[ "$n" -gt "$STOP_STEP" ] && continue` 这种写法在条件为假时返回 1，而 `set -e` 会把整个脚本掐死——**末步必然踩到**（已在源码注释里点名）。
  · `show_plan()`：跳过标记按本次实际执行区间 `[START_STEP, STOP_STEP]` 生成；单步模式打印专门警示「本次**只执行 Step N**，其余步骤全部跳过 ⇒ 不构建镜像、不启动/重启服务、不做全栈体检」，避免拿单步输出去对照全流程判据；`--step` 的两条警示保留并追加「只跑一步请用 `--only-step`」。
  · `main()` 收尾：非全流程时**不再谎报「全部 13 个步骤执行完成」**，改为「已执行 Step A-B（13 步中的 K 步）……本次不是全流程部署」；且只在 `STOP_STEP < TOTAL_STEPS` 时才提示续跑命令（`--only-step 13` 不能建议 `--step 14` 这种非法取值）。
  · `run_step()` 排查建议第 4) 同时给出两种续法：`--step N -y`（续跑到底）与 `--only-step N -y`（只重跑该步）。
  · `usage()`、头注参数段、示例段三处口径统一（示例新增 `--only-step 3 -y`，并保留 `--step 3 --skip-build -y` 作为「需顺带续跑整链」的选项）。
- **B `scripts/verify_infra.py`**：新增第 20 项 `check_install_step_dispatch_coverage()`，把四件事一次对账：① `TOTAL_STEPS` == `STEP_DESCRIPTIONS` 条目数 == `run_all_steps` 的 case 臂集合 `{1..N}`（臂号与 `run_step N`、`STEP_DESCRIPTIONS[N-1]` 必须对位，步骤函数必须已定义）；② `run_all_steps` 必须**同时**有下界（`--step`）与上界（`--only-step`）判定；③ `parse_args` 里每个长选项都必须出现在 `usage()` 中；④ 任一解析目标（`run_all_steps`/`parse_args`/`usage` 函数体、`TOTAL_STEPS`、描述数组）取不到时**一律出 FAIL**，不得因解析失败而静默 PASS。docstring 摘要补第 17 条。

向后兼容：① **默认行为完全不变**——不带 `--only-step` 时 `STOP_STEP = TOTAL_STEPS`，新增的上界分支恒不触发，`--step` 语义未改（仍是续跑到底）；② **收尾文案变了**：带 `--step` 的续跑场景不再打印「全部 13 个步骤执行完成」而打印「已执行 Step N-13……本次不是全流程部署」——原先靠 grep 这句话判全流程成功的自动化，在续跑场景里本来就不该判成功（`--skip-build`/`--skip-docker` 仍走满 13 步、仍打印原句）；③ `show_plan` 的 `[跳过：--step N]` 改为 `[跳过：起点 Step N]`，grep 原文的脚本需知晓；④ `run_step` 失败建议多给一个备选命令（纯文案）。

风险：① **单步不替用户判断跨步前置条件**（如 `--only-step 8` 依赖 Step 7 的中间件已在跑、`--only-step 12` 依赖全栈已起）——计划表会点名跳过了哪些步，但不会拦住误用；② `--only-step 6` / `--only-step 11` 本身就是构建镜像 / 启动服务，**同样可能 recreate flink 容器**：警示文案说的是「不会自动带到这些步」，不是「这些步无害」；③ 第 20 项是**文本形状判据**，认不出「臂号正确但调用顺序被改」这类运行期问题，也不校验步骤间的真实依赖。

验证（本环境，均实跑）：`bash -n scripts/{common.sh,install.sh,health-check.sh,daily-check.sh}` **×4 OK**。**`--only-step` bash 桩 47 条断言全 OK**——被测函数（`parse_args`/`show_plan`/`run_all_steps`/`main`）与变量初始化全部从真实 `install.sh` **抽取后 eval**，不在桩里复制实现；只打桩无法本地执行的部分（`log_*`/`die`/`confirm`/`preflight`/`run_step`），`run_step` 桩把**实际执行到的步号写入文件**（不用子 shell 变量，避免状态传不回来）。断言覆盖：`--only-step 3` 恰好执行 1 步且是 Step 3、收尾「13 步中的 1 步」、不再声明全部完成、提示 `--step 4` 续跑、计划表起点标记 2 处 + 终点标记 **10** 处、单步警示到位；`--only-step 13` **不得**给出 `--step 14` 且必须跑到收尾「结束时间」（这条同时兜住 `&&` 写法被 `set -e` 掐死）；`--step 3` 仍执行 3..13 共 **11** 步并保留续跑语义警示；无参数执行满 13 步并打印原句；`--step 3 --only-step 3` 互斥报错且**零步执行**；`--only-step` 取 `0`/`14`/`abc`/缺参数全部拒绝且零步执行；`--only-steps 3`（拼错）走「未知参数」而非静默忽略。**最后 6 条是反向对照**：把真实 `run_all_steps` 的上界块删掉后，桩必须读到「执行了 11 步」并暴露收尾文案说 1 步、实际跑 11 步的假读数；再把真实函数体 eval 回来断言恢复为 1 步——没有这组对照，前 41 条绿只说明桩没测到东西。桩首跑 6 条假红，责任在桩自己：把**成功用例**的期望退出码写成了 1（真实 `die()` 才是 exit 1，全流程成功是 exit 0），改回 0 后 **47/47**。**第 20 项专项桩 14 条全 OK**（临时 ROOT + 对真实 `install.sh` 做定向破坏）：真仓库 ⇒ PASS 且回显 `13 步 / 13 条描述 / 13 个 case 臂`；删上界块 ⇒ FAIL「上界跳过」；删下界块 ⇒ FAIL「下界跳过」；`TOTAL_STEPS=14` 而不加臂 ⇒ FAIL 同时点名「缺分派 14」与「描述表 13 ≠ 14」；删掉 `3)` 臂 ⇒ FAIL 缺分派；`11) run_step 12` ⇒ FAIL 分支与步号不一致；`STEP_DESCRIPTIONS[3]` 错位 ⇒ FAIL 描述与步号错位；臂引用不存在的步骤函数 ⇒ FAIL；从 `usage()` 体内删掉所有提到 `--only-step` 的行 ⇒ FAIL「长选项未写进 usage」；把 `run_all_steps()` 改名（模拟解析器失效）⇒ **仍必须出 FAIL**。该桩首跑 2 条红同样是自己的错：① 破坏块的正则没吃掉紧邻的注释行，导致「破坏已生效」这条前置断言为假（而它一旦为假，后面两条断言就是假绿）；② `str.replace(…, 1)` 命中的是 `show_plan()` 里**形状完全相同的下界判定**（第一处），而不是 `run_all_steps()` 里那条 ⇒ 改为「先取函数体、在体内切块、再整体换回」。`verify_infra.py` **20 项全 PASS**；`ruff check scripts flink-jobs` **All checks passed**；`run_unit_tests.py` **9 目标 1066 passed / 1 skipped**（326 / `tests` 100 deselected / 88+1s / 71 / 108 / 143 / 105 / 128 / 97）——与 V1.19.17/18 **逐字一致**。

分发路径注意：受追踪 **2 个** = `scripts/{install.sh,verify_infra.py}`；**本条与 V1.19.18 均未提交**（`git status` 现为 `common.sh / install.sh / verify_infra.py` 三项，其中 `install.sh` 同时含两条的改动，HEAD 仍是 `98c4b3c`）⇒ 服务器 `git pull` 取不到这两条；`docs/` 两份与 `release.md` 照旧单独 rsync。

服务器取证命令（在第 2 步把 V1.19.18 + 本条一起同步后执行；**本条起自愈验证不再需要 `--skip-build` 绕路**）：

```bash
cd /opt/HunterCore
# 1) 同步 scripts/{install.sh,common.sh,verify_infra.py} + docs 两份
git log --oneline -1; grep -c -- '--only-step' scripts/install.sh   # 应 ≥6，为 0 ⇒ 没同步到
grep -n 'check_install_step_dispatch_coverage' scripts/verify_infra.py | head -n 3
# 2) 修复前留证（数字口径）
stat -c '%u:%g %a %n' /data/flink /data/flink/checkpoints 2>/dev/null
# 3) **只跑 Step 3**：不重建镜像、不启动服务、不体检，flink 容器不受影响
sudo bash scripts/install.sh --only-step 3 -y 2>&1 | tee /tmp/step3_v19.log
grep -nE '无效的组|属主参数非法|已设置属主|flink 容器运行用户|跳过 Step|只执行 Step|已执行 Step' /tmp/step3_v19.log
# 4) 本条主判据：属主是否真的改上了（数字口径，别用 %U:%G）
stat -c '%u:%g %a %n' /data/flink          # 期望 9999:9999 755
# 5) 单步能力自证：Step 13 只打印摘要，不碰任何服务
sudo bash scripts/install.sh --only-step 13 -y 2>&1 | tail -n 20
# 6) 作业若已被上一轮 recreate 冲掉，此时才重提交（本条命令不会冲掉它们）
sudo docker exec hunter-flink-jm flink list
# 7) 体检（作业在场才是有效读数）
sudo bash scripts/health-check.sh 2>&1 | tee /tmp/hc19.log | tail -n 8
grep -nE '^\[(WARN|FAIL)\]' /tmp/hc19.log
python3 scripts/verify_infra.py | tail -n 4
```

判据预告：① 第 3 步**必须只见 Step 3 一行执行**：`grep -c '^\[INFO\] ===== Step ' /tmp/step3_v19.log` 应为 **1**，其余 12 步都是「跳过 Step n/13（起点 3 或终点 3）」；若看到 Step 4 及以后被执行 ⇒ 脚本没同步到位（第 1 步的 `grep -c` 会一并定性）。② 第 3 步**不得**出现 `chown: 无效的组`，也不得「上面报 chown 错、下面紧跟 SUCCESS」；出现 `属主参数非法` ⇒ 还有别的路径在污染，回传该行原文（`printf '%q'` 会显出真身）；Step 3 直接失败退出 ⇒ 是真设不上（只读挂载/权限），**这才是应有的行为**。③ 第 4 步必须变 `9999:9999 755`；仍是 `9999:0` 之类 ⇒ 自愈没生效，回传第 3 步全量输出。④ 第 3 步收尾必须是「已执行 Step 3-3（13 步中的 1 步）……本次不是全流程部署」并提示 `--step 4`；若仍看到「全部 13 个步骤执行完成」⇒ 脚本版本不符。⑤ 第 5 步（`--only-step 13`）不得提示 `--step 14`，且要跑到「结束时间」——中途无声退出 ⇒ 上界判定被写成了 `&&` 形式（`set -e` 掐死）。⑥ 第 6 步 `flink list` 现在应仍能看到作业（本条不 recreate 容器）；若作业数 < 3，那是上一轮遗留，按第 6→7 步补重提交后再判。⑦ 第 7 步的 ⑭/⑮ 与 `verify_infra` 第 20 项 PASS 是本轮收尾判据；`verify_infra` 若在 `install.sh` 上报 FAIL ⇒ 优先怀疑同步只落了一半文件。

**待办（不属本条范围，不得当已修）**：① 本条与 V1.19.18 的修复**均未实机复核**（本环境无 Docker）；② V1.19.18 待办②③⑤⑥ 原样挂账（`-raw` 组位点、`telemetry_clean` 只有 PARTITION 6、JM/TM 重建丢作业的对账巡检、K8s RWX / `/data/flink` 保留策略 / ⑮ 只读挂载 / 前端 `unknown_groups` / CI lint / `.env.example` 对账）；③ `--only-step` **不校验跨步前置条件**（例如 `--only-step 8` 在中间件未起时仍会执行并失败），是否给关键步加前置探测，待实机看是否真有人这么用；④ `--skip-docker`/`--skip-build` 走满 13 步（步内自跳）与 `--only-step` 是两套机制，口径统一（例如改成「计划表里标 [跳过] 且不进 case 臂」）待评估；⑤ 第 20 项尚未进 CI（`.gitlab-ci.yml` 现在不跑 `verify_infra`），与 V1.19.16 待办的「lint/shellcheck 进 CI」是同一个人。

## V1.19.20（修复：compose 形态**从来没有任何 Flink 作业提交入口** ⇒ 巡检 ⑭ 恒报 RUNNING=0、「入库延迟」恒无数据；读模型 `last_online_time`/`vehicle_name` 两个字段**从来没有任何写方** ⇒ 看板「最近在线 1970-01-01 08:00:00 / 名称空」，且两侧单测因 Mock 陷阱互相掩盖）

Date：2026-10-13
变更类型：修复（**运维脚本 + 两服务读写两侧 + 前端 + 契约 fields_draft**；不改 Kafka/OpenAPI 结构、不改 compose/K8s 拓扑。⚠ 与 V1.19.18/19 不同：**本条含服务与前端代码改动 ⇒ 生效必须重建镜像**（`--only-step 6` + `--only-step 11`），纯脚本部分则 `git pull` 即生效）

先把本轮实录（2026-10-13）里**已被证实生效**的两条结案，本条不重复修复它们：
- **V1.19.18 结案**：`/data/flink` 读数已是 `9999:9999`，Step 3 属主自愈这次真的执行了（stdout 纯度门禁生效）。
- **V1.19.19 结案**：`--only-step` 行为与预告一致，只跑指步、不再连带 recreate flink 容器。
- **⑭ 的读数本身是对的**：`flink list` 明确回 `No running jobs.`、⑭ 报 `[FAIL] RUNNING=0 < 3`——V1.19.16 建立的「合法空列表 ≠ 未判定」判据第二次抓到真事故。**本条不改任何判据**，改的是「为什么永远是 0」。

现场（实录 + 看板截图，三条症状）：
1. ⑭ `RUNNING=0 < 3`，三个作业一个都不在；
2. 看板「入库延迟 P95」显示 `- ms`（无数据）；
3. 车辆明细（Redis 读模型）HUNTER-001：电量 `0%`、最近在线 `1970-01-01 08:00:00`、名称为空。

根因（三处独立，症状 1↔2 同源）：
- **① compose 形态没有任何提交入口（症状 1、2 的共同主因）**：K8s 形态由 `infra/k8s/flink/04-submit-job.yaml` 提交作业，**compose 形态没有对应机制**——`install.sh` Step 11.2 只有 `compose up -d flink-jm flink-tm` + `wait_for` JM REST 就绪，作业要靠人工照 `flink-jobs/README.md` 敲 `flink run`。而 standalone **无 HA**：任何一次 recreate（Step 11、`--step` 续跑、镜像重建、`docker compose down`）都会把已提交作业冲掉，**且没有任何机制把它们提回去**。⇒ 全新环境漏提 = 恒 0，老环境 recreate = 归零，两种路径都通向 ⑭ 长红。V1.19.16～19 四轮全在修「提交期为什么失败」，**没有一轮修过「谁来提交」**——这不是前四条的回归，是能力缺口。
- **② 「- ms」是 ① 的下游读数，不是独立故障**：入库延迟的唯一数据源 = `hunter_flink.ingest_latency_job` 以 60s 窗口统计「Kafka 到达时间 − 车端 timestamp」的 P95，写 Redis 键 `analytics:ingest_latency`（**TTL 300s**），data-analytics 只读该键。作业不在 ⇒ 键在 5 分钟内自然过期 ⇒ API 降级返回 `null` ⇒ 前端拿到 null。**数据侧无需新代码**（① 修好即恢复出数）。但展示侧确有独立缺陷：`StatCard` 的 `unit` 是写死的 `'ms'`，null 走占位符 `-` 时**单位跟着占位符一起渲染**，于是显示成「- ms」——把「无数据」伪装成「延迟是 - 毫秒」。另原标题「入库延迟 P95」把统计口径写进指标名，而该值降级为 null 时既不是 P95 也不是延迟，徒增误读。
- **③ 读模型两个字段从来没有写方（症状 3 的确证部分）**：`vehicle:status:{vehicle_id}` 的唯一写方 data-collector（契约 pending #2）实际写入的心跳字段是 **`last_seen_at`**，Hash 里**从来没有 `last_online_time`**（那是 `vehicle_svc.vehicles` 的**台账列名**，被误当成了 Hash 字段名），也**从来没有 `vehicle_name`**（消息体不携带名称）。remote-control 的 `VehicleViewReader` 按 `last_online_time` 取数 ⇒ `_parse_float(None) or 0.0` ⇒ 恒 `0.0` ⇒ epoch 0 在东八区渲染成 `1970-01-01 08:00:00`；名称同理恒 `null`。响应 schema 的 `last_online_time` 是契约必填字段（**不能改名**），故只改 Hash 取数源。
  **为什么单测一直是绿的（本条真正的教训）**：remote-control 的 Fake Redis 在 `app/tests/conftest.py` 里**直接按读方的想象塞了 `last_online_time` / `vehicle_name` 两个字段**，而不是按写方的实际行为造数据 ⇒ 读方断言「能读到最近在线」永远成立，写方「从不写该字段」永远不会被任何测试观察到——**跨服务字段名漂移被 Mock 双向掩盖**。本条把 Fake 改成写方真实字段并加注释钉住这条纪律。
- **④ 电量 0%：本条未确诊，不得当已修**。`battery_soc` 字段名两端一致、由 telemetry 消费路径（`chassis.battery_soc`）写入，代码层查不出缺陷；而 0 恰是契约必填字段的兜底值 ⇒「车端真报 0」与「telemetry 从未流入读模型」两种成因**从这两份材料无法区分**。已列入待办①，取证命令见下方第 7 步。

修复内容（受追踪 **13 个** + 新增 **1 个**）：
- **A `scripts/submit-flink-jobs.sh`（新增）**：compose 形态的作业提交入口。按 `FLINK_JOB_SPECS`（模块名 → `env.execute()` 作业名，与 consumer-groups.yaml「一组一作业」对齐）逐个 `docker exec hunter-flink-jm flink run -d -m localhost:8081 -pym <module>`；**幂等**（`flink list` 里已 `RUNNING/RESTARTING` 的同名作业跳过，故 Step 11 可反复跑、重复续跑不会重复提交）；提交前 `wait_for` JM REST 120s（宿主机 `flink_ui_url` 或容器内 `curl` 双探测，独立运行时也能用）；失败给四条定点排查建议（JM 日志 / ⑮ 检查点目录属主 / compose 凭据 V1.19.8 / `api_guard` V1.19.7）；**汇总以 `flink list` 实际读数收口，不拿提交返回值凑数**——`running < 3` 时报「提交命令成功但作业可能死在初始化期，立即 `flink list -a` 取证」并 `return 1`。参数：`--help` / `--env <file>` / `--force`（不去重，需先手工 cancel 旧作业）。`flink_running_jobs()` 被 `$(…)` 捕获 ⇒ stdout 只写作业行、JVM 噪声（`WARNING: ` / `SLF4J:`）滤除，符合 `verify_infra` 第 19 项形状判据。
- **B `scripts/common.sh` + `scripts/install.sh`**：前者新增 `SUBMIT_FLINK_JOBS_SH` 路径常量并入 export 行；后者 Step 11.2 在「`up -d flink-jm flink-tm` + 等 JM UI 就绪」之后**自动调用该脚本**，失败即 `return 1`（把「没人提交作业」从**巡检期才暴露**提前到**部署期真失败**，并当场给出可执行恢复命令而非让人翻 README）。
- **C `scripts/health-check.sh`**：⑭ 的 FAIL 提示补一句 V1.19.20 口径——「从未提交或提交期失败；若此处仍为空，先看是否只跑了 `--only-step` 而跳过 Step 11」+ 恢复命令 `sudo bash scripts/submit-flink-jobs.sh`（幂等）。判据本身一字未改。
- **D `frontend/src/views/DashboardView.vue`**：① 标题「入库延迟 P95」→「入库延迟」（统计口径移入 hint 的 SLO 说明）；② `unit` 改为**有值才给 `ms`**（`ingest_latency_ms_p95 != null ? 'ms' : ''`），消掉「- ms」；③ `tone` 空值走中性 `info`（原逻辑 null 落进 `> 1000` 的 false 分支被刷成绿色 success，属第三种假读数）；④ 名称列 `row.vehicle_name || row.vehicle_id`（写方尚未铺开/未重建镜像时有兜底显示，不显示空白）；⑤ 最近在线 `> 0` 才格式化，否则显示「尚未上线」（**即使数据真的缺失，也只说「尚未上线」，绝不显示 1970**）。
- **E `services/remote-control/app/services/vehicle_view.py` + `app/tests/conftest.py`**：`_FIELD_LAST_ONLINE` 由 `"last_online_time"` 改为 **`"last_seen_at"`**（响应字段名不变，只改 Hash 取数源），docstring 写明「曾按 `last_online_time` 取数——该字段从无写方，致恒兜底 0 → 1970-01-01 08:00:00」；conftest 的 Fake Hash 同步改为 `last_seen_at` 并注释钉住「Fake 的数据形状必须来自**写方实际行为**，不得按读方想象发明字段」。
- **F `services/data-collector`（三文件）**：给读模型补上名称写方——`app/services/vehicle_ledger.py` 协议新增 `get_name()`、`VehicleLedgerWriter` 新增 `vehicle_name()`（`NAME_CACHE_TTL_SECONDS = 600` 的 TTL 缓存；**失败同样计入窗口**，DB 故障不会退化成每条消息一次 SELECT；异常只记 WARN 不上抛；**异常时保留旧值**，瞬时抖动不得把名称从读模型抹掉；`_prune()` 纳入 `_name_cache` 保持有界）；`app/repositories/vehicle_ledger.py` adapter 实现 `get_name()`（一次一短会话，`VehicleRepository(session).get(pk).vehicle_name`，查无此车返回 `None`）；`app/services/vehicle_status.py` 的 `update_from_health` / `update_from_telemetry` 经 `_lookup_name()` 取名称，**取到才写 `mapping["vehicle_name"]`**（禁止伪造默认值），未注入回写器时行为与旧调用方完全一致。
- **G `contracts/database/redis-keys.yaml`**：`vehicle:status:{vehicle_id}` 的 `fields_draft` 按写方实际行为**回填全量字段**（新增 `last_seen_at`（并标 required，注明读方曾误取 `last_online_time`）、`last_seen_seconds`、`vehicle_ts`、`vehicle_name`、`gpu_temp`、`cpu_temp`、`network_rssi`、`battery_voltage`），17 → **25** 个字段；pending #1 的问题描述同步为「25 个字段，已按 data-collector 实际写入行为回填全量字段名」。
- **H 测试**：`app/tests/fakes.py` 的 `FakeVehicleLedgerRepository` 补 `get_name()`（`names` 字典 + `name_calls` 调用记录，`name_calls` 长度即「缓存是否真生效」的判据）；`test_vehicle_ledger.py` 新增 3 条（TTL 窗口内只查一次 / 超窗重查、DB 故障保留旧值且失败计窗不放大、查无此车返回 None 且空标识不触碰 DB）；`test_ingest_pipeline.py` 新增 2 条（health/telemetry 两路都写入 `vehicle_name`、台账查无此车**不写该字段**）。

向后兼容：① **Step 11 现在会因作业提交失败而整步失败**——此前该步「起容器成功」即算成功，作业在不在根本没人管；存量环境若有「JM 起得来但作业起不来」的隐疾（凭据缺失、检查点目录不可写），本条会把它当场暴露成 Step 11 红灯，**这是有意的**（按脚本给出的四条建议排障，或先 `bash scripts/submit-flink-jobs.sh` 单独取证）；② 巡检 ⑭ 判据与 compose/K8s 拓扑未动 ⇒ 不影响既有体检结论口径；③ `vehicle_view.py` 只改 Hash 取数源，**REST 响应字段名与 schema 不变** ⇒ 前端/第三方读方无需改动，但读数会从「1970」变成真实心跳（靠 `last_online_time === 0` 判「从未上线」的旧代码需改用 `> 0` 判定，本仓前端已同步）；④ data-collector 新增字段是**纯增量 HSET**，读方取不到时行为不变；`vehicle_ledger` 协议新增方法 ⇒ 仓内唯一实现（adapter）与 Fake 已同步，仓外若有自制实现需补 `get_name`；⑤ `fields_draft` 仍是 `pending_confirmation`（**没有**定稿、没有去掉 draft 标记），只是把「实现实际写了什么」如实登记，不构成新的契约承诺。

风险：① **本条全部改动未经实机验证**（本环境无 Docker/无 Redis/无集群），`submit-flink-jobs.sh` 的作业名去重判据依赖 `flink list` 的行形状（`<jobId>  <name> (RUNNING)`），与巡检 ⑭ 同源——Flink 大版本升级若改文案会同时失效，属已知耦合；② 服务与前端改动**必须重建镜像**才生效，只 `git pull` 不 `--only-step 6` 的话看板仍是旧读数（第 D/E/F 项全是镜像内代码）；③ TTL 缓存 600s ⇒ 台账**改名后读模型最长 10 分钟才更新**（换取 1Hz×N 车消费路径不变成每秒一次 SELECT）；④ 读模型名称来自台账，**台账无该 vehicle_id 时字段持续缺失**（前端 fallback 到 vehicle_id 才不至于空白）；⑤ 提交脚本对 FAILED 终态作业不去重（只认 RUNNING/RESTARTING），反复跑 Step 11 会在「初始化期即失败」的情形下重复堆积失败作业，需定期 `flink list -a` 清理——脚本已在该分支点名提示。

验证（本环境，均实跑）：`bash -n scripts/{submit-flink-jobs.sh,install.sh,common.sh,health-check.sh}` **×4 OK**；`run_unit_tests.py` **9 目标 1071 passed / 1 skipped**（326 / `tests` 100 deselected / 88+1s / 71 / 108 / **148** / 105 / 128 / 97）——较 V1.19.19 的 1066 **净增 5 条**（本条新增的 3 + 2 条），既有用例零改动通过；`verify_infra.py` **20 项全 PASS**（第 19 项对新脚本的 stdout 纯度判定为 PASS：`已解析 171 个函数、29 个被捕获调用`；第 20 项步骤分派对账仍 `13 步 / 13 条描述 / 13 个 case 臂`）；`verify_data_layer.py` **通过 34 项、失败 0 项**（含「Redis 键契约一致（9 个受控键 ↔ 6 份服务契约）」——fields_draft 无校验器解析，回填不影响门禁）；`ruff check services/data-collector/app services/remote-control/app` **All checks passed**；`npm run type-check`（`vue-tsc --noEmit`）**零输出通过**。℞ 说明：本环境 `mypy` 对 `hunter_common` 包解析不可用（109 条既有噪声，均为 `import-not-found` 与 Fake 协议形状告警），**本条新增代码行未被任何一条点名**，但不等于类型检查已在 CI 口径下通过。

分发路径注意：受追踪 **13 个** = `contracts/database/redis-keys.yaml`、`frontend/src/views/DashboardView.vue`、`scripts/{common.sh,health-check.sh,install.sh}`、`services/data-collector/app/{repositories/vehicle_ledger.py,services/vehicle_ledger.py,services/vehicle_status.py,tests/fakes.py,tests/test_ingest_pipeline.py,tests/test_vehicle_ledger.py}`、`services/remote-control/app/{services/vehicle_view.py,tests/conftest.py}`；**新增 1 个未纳入版本控制** = `scripts/submit-flink-jobs.sh`（`git status` 现为 `??`，**必须 `git add`**，否则服务器拿到 install.sh 的调用行却找不到被调用的脚本 ⇒ Step 11 直接失败）。HEAD = `aea76c0`（其内容已含 V1.19.18 + V1.19.19 两条的 `install.sh`/`common.sh`/`verify_infra.py`）⇒ 本条 14 个文件**均未提交**，服务器 `git pull` 取不到；`release.md` 与 `docs/` 照旧被 `.gitignore:158/159` 命中 ⇒ 单独 rsync。

文档同步（本条已落，四份运维文档）：`docs/deployment.md`（§13.1 提交入口缺失注 + 新脚本、§13.3 展示层口径注）；`docs/运维操作手册.md`（§3.6 首选提交入口与 `--force`、“列表为空”根因段、`--step/--only-step` 警示块、巡检 ⑭ 恢复手段、§3.3 Redis Hash 字段口径注、看板「入库延迟」重写 + 新增「车辆明细」段）；`docs/故障排查指南.md`（§0 新增两条速查：空列表首因是没人提交、`last_online_time` 与 `last_seen_at` 同名不同物；场景 4 新增反向情形行；场景 13 表格行改口径 + 新增「车辆明细」三行判据表，**电量 0% 在该表里明标为未确诊**）；`docs/01-部署概述与环境要求.md`（scripts 清单新增 `submit-flink-jobs.sh`、`install.sh` 行补 Step 11.2 自动提交口径）。

服务器实机步骤（A 段纯脚本不需重建镜像；B 段起才需要）：

```bash
cd /opt/HunterCore
# 1) 同步：scripts/ 四个（含新脚本！）+ 两个服务 + 前端 + 契约 + docs 两份
git status --porcelain | head; ls -l scripts/submit-flink-jobs.sh   # 不存在 ⇒ 没同步到，后面全部作废
# 2) 先单独跑提交脚本（不改任何服务，最快定性「谁来提交」这一条）
sudo bash scripts/submit-flink-jobs.sh          # 期望末尾：三作业全部在列 + 「已全部提交」
sudo docker exec hunter-flink-jm flink list     # 期望 3 行 RUNNING
# 3) 入库延迟出数（作业真跑起来后最多 60s 窗口 + 30s 缓冲）
sudo docker exec hunter-redis redis-cli -a "$REDIS_PASSWORD" GET analytics:ingest_latency; \
  sudo docker exec hunter-redis redis-cli -a "$REDIS_PASSWORD" TTL analytics:ingest_latency   # TTL 应 ≤300 且非 -2
curl -s "http://127.0.0.1:8083/api/v1/analytics/dashboard" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["pipeline"])'
# 4) 待办① 取证（电量 0% 定性，只读不改）。⚠ Topic 名：车端源 Topic 是 `hunter.<id>.telemetry`，平台内部 Topic 是裸名 `telemetry_raw`（不带前缀）；
#    CLI 必须写绝对路径且带 `$KC`（见故障排查指南 §0）
sudo docker exec hunter-redis redis-cli --no-auth-warning -a "$REDIS_PASSWORD" HGETALL vehicle:status:HUNTER-001
sudo docker exec hunter-kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/client.properties \
  --topic hunter.HUNTER-001.telemetry --from-beginning --max-messages 3 \
  --consumer-property enable.auto.commit=false 2>/dev/null
# 5) 重建镜像并重启服务（D/E/F 三项都在镜像内）
sudo bash scripts/install.sh --only-step 6 --skip-docker -y
sudo bash scripts/install.sh --only-step 11 -y            # 内含 submit-flink-jobs.sh（幂等，第二次多半全跳过）
# 6) 读模型字段与看板复核
sudo docker exec hunter-redis redis-cli -a "$REDIS_PASSWORD" HGETALL vehicle:status:HUNTER-001 | head -40
#   期望出现 last_seen_at（真实 epoch）与 vehicle_name（台账名称）；不应再看到 last_online_time
curl -s "http://127.0.0.1:8085/api/v1/remote/vehicles" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys;[print(v["vehicle_id"],v["vehicle_name"],v["last_online_time"]) for v in json.load(sys.stdin)["data"]["items"]]'
# 7) 收尾体检
sudo bash scripts/health-check.sh 2>&1 | tee /tmp/hc20.log | tail -n 8
grep -nE '^\[(WARN|FAIL)\]' /tmp/hc20.log
python3 scripts/verify_infra.py | tail -n 4
```

判据预告：① 第 2 步若报「容器 hunter-flink-jm 不存在」⇒ 还没跑到 Step 11，先补齐；若报「提交命令全部成功，但 flink list 读数 < 3」⇒ 作业死在初始化期（**不是本条的提交缺口**），按提示 `flink list -a` 取 FAILED 终态堆栈，对照 V1.19.16～18（检查点目录/凭据/API 守卫）；若报 `Cannot run program "python"` ⇒ 镜像不是 `hunter-flink/jobs:2.1.1`（V1.19.2 那条老坑，compose 配置被改过）。② 第 2 步成功后 ⑭ 必须转 PASS；仍是 `RUNNING=0` 而脚本明明说「已全部提交」⇒ 中间有人 recreate 了容器（`docker ps` 看 flink-jm 启动时间），**先确认是不是又在跑 `--step`/`--only-step 11` 之外的全栈操作**，别改判据。③ 第 3 步 `GET` 有值 + `TTL` 非 `-2` 才算「入库延迟恢复出数」；`TTL -2` 说明作业虽 RUNNING 但窗口未触发（看 JM 日志是否有 checkpoint 完成记录，V1.19.15 的位点判据仍适用）。④ 前端「入库延迟」应显示数字 + `ms`，或纯 `-`（**不再出现「- ms」**）；无数据时色值应为中性灰而不是绿色。⑤ 第 6 步 `last_online_time` 必须是真实 epoch（**任何 1970 都是异常**）；`vehicle_name` 为 `null` ⇒ 台账无该行或 DB 抖动，看服务日志 `vehicle_ledger_name_query_failed`，并按前端 fallback 确认页面不再空白。⑥ 第 4 步是电量 0% 的唯一定性手段：`HGETALL` 无 `battery_soc` ⇒ telemetry 从未流入读模型（管道问题）；有且为 0 ⇒ 车端原始读数就是 0（数据源问题）；两者排障方向完全相反，**把两条原文回传后再定**，不要先改代码。

**待办（不属本条范围，均为未确诊项，不得当已修）**：① **电量 0% 未确诊**（见判据⑥，需 `HGETALL` + console-consumer 两条原文定性）；② `data-analytics-telemetry-raw` 消费组此前 `does not exist`（V1.19.18 待办②）——本条让作业被提交后该组理应创建，但「`ingest_latency_job` 是否真的持续提交位点」仍需第 3 步 + `--describe` 复核，未结案；③ `telemetry_clean` 只有 PARTITION 6 有 committed offset（V1.19.18 待办③）原样挂账；④ **compose 形态无自动重提**：本条把提交入口做进 Step 11，但容器因 OOM/重启策略被 recreate 时仍需人工跑 `submit-flink-jobs.sh`；是否在 `daily-check.sh`/`health-check.sh` 里加「⑭ FAIL 时自动调用一次幂等提交」，或给 flink-jm 配 HA/持久化 job store，属功能变更需批准；⑤ 读模型字段名漂移**只修了 remote-control 一处读方**：data-analytics 看板与 ota-service 门禁的取数字段是否也存在「按想象发明字段名」的情况，需要一次「写方 fields 全集 ↔ 三个读方取数字段」的对账（本条只回填了契约 fields_draft，未逐个读方核查）；⑥ Mock 纪律无静态门禁：`FakeVehicleLedgerRepository` 补方法靠人盯，「Fake 的字段形状必须来自写方」这条只能靠 review，考虑在契约测试里加「读方字段常量 ⊆ fields_draft」断言；⑦ `vehicle_name` 改名最长 10 分钟才反映到读模型（TTL 600s 硬编码，未开配置项）；⑧ 第 19/20 项与本条新脚本均未进 CI（沿用 V1.19.19 待办⑤）；⑨ V1.19.18 待办⑥（K8s 检查点 RWX、`/data/flink` 保留策略、⑮ 只读挂载、前端 `unknown_groups`、`.env.example` 对账）原样挂账。

## V1.19.21（修复：入库延迟 `-` 的真实缺陷是**不可观测**——四种完全不同的故障被读成同一个 `-`，而全丢弃时事件时间窗口永不冲刷使键永不存在；顺带消除 `json.loads` 未防护的 crash-loop 风险；remote-control 读方 `or 0` 把「读模型没有这个字段」谎报成「电量 0% / 静止」）

Date：2026-10-13
变更类型：修复（Flink 写端 + data-analytics 读端 + remote-control 读端 + data-collector 写端 + 前端 + 巡检脚本 + 三份契约。⚠ 含服务/Flink/前端代码 ⇒ 生效必须重建镜像（`--only-step 6` + `--only-step 11`）**并重新提交作业**；只 `git pull` 只有巡检 ⑯ 与脚本文案生效）

现场（2026-10-13 二次实录 + 更新后的看板截图；两条症状）：
1. **V1.19.20 的提交入口已生效**：09:32:59 `flink list` 三作业全部 RUNNING（本条不再动提交机制），但 09:35:11 看板「入库延迟」仍是 `-`——不再是「- ms」（V1.19.20 已修单位），但**依然没有任何数据，也依然说不出为什么没有**；
2. 车辆明细：HUNTER-001 / 名称 HUNTER-001 / 在线空闲 / 电量 `0%` / 速度 `0.00 m/s` / 最近在线 `2026-10-13 09:35:10`（`last_seen_at` 取数源已恢复，V1.19.20 生效）——但 `0%` 与 `0.00` **无法判真假**。

先行澄清（避免把两条症状混成一条）：同一截图的「感知时延 0 ms / 控制时延 0 ms」经查是 `algorithm_metrics` 里的**真实 0 值**（前端 `formatNumber(null)` 返回 `-`，能渲染出 0 说明库里就是 0），与本条第 2 组症状不是同一类缺陷，属车端上报值问题，本条**不改**（待办⑤）。

根因（四处，症状 1 是结构缺陷不是判据写错）：
- **① 「作业 RUNNING 但没出数」在架构上不可判读**：`ingest_latency_job` 的三条丢弃路径（负延迟 / 非有限值 / 缺时间戳）**既不计数也不打日志**；而全丢弃时 `buffers` 永不建立 ⇒ **事件时间窗口永不冲刷** ⇒ Redis 键 `analytics:ingest_latency` 永不写入 ⇒ 看板恒 `-`。于是同一个 `-` 至少覆盖四种完全不同的现实：a) 作业没提交 / 提交期失败；b) 提交了但从未消费到 `telemetry_raw`；c) 消费到了但样本 100% 被丢弃；d) 新消费组无提交位点、正从 earliest 回放 7 天历史尚未追平。巡检 ⑭ 只能说「作业活着」，对 b/c/d **既非充分也非必要**。前五轮（V1.19.13～19）修的都在 a 这一档，本条补的是 b/c/d 的可读性——**丢弃口径一条都不放宽**（负值仍不取绝对值、不用处理时间替代、无有效样本不出数）。
- **② 写端 `json.loads(value)` 无任何异常保护（复查时发现，与实录高度吻合但未确诊）**：本组用 `committed_offsets(EARLIEST)`，而 `telemetry_raw` 保留 7 天 ⇒ 首次运行从位点起点回放，**一条非法 JSON 就能把作业打进 RESTARTING，并在 checkpoint 重放下永远卡在同一条**。实录里恰好只有 ingest_latency 一个作业 RESTARTING、另两个 RUNNING，而看板恒 `-`。本条已消除该结构性风险（计入 `unparsable_message` + WARN 后继续），但手上没有堆栈证据 ⇒ **只登记为「已消除的结构性风险」，不写成本条现场的确证主因**（待办①）。
- **③ 读方伪造默认值（症状 2 的确证部分）**：`services/remote-control/app/services/vehicle_view.py` 写的是 `battery_soc=_parse_int(...) or 0`、`velocity=_parse_float(...) or 0.0`——把「Hash 里根本没有这个字段」说成「电量 0% / 静止」。这与本仓写侧纪律自相矛盾：data-collector 对 `gear`/`free_storage_mb`/`lat`/`lng` 明确「未上报即不写字段（禁止伪造默认值）」，ota-service 门禁是 `require_soc and (soc is None or soc < min_soc)` ⇒ **缺数据即拒绝**（安全默认 #19）。同一份读模型，两个读方一个拒绝、一个谎报，而 `0%` 在业务上还是「快没电」的强语义（<10% 对应 `battery_critical` 事件）。契约 `ControllableVehicle.battery_soc` 标成非 nullable 是共谋。
- **④ 判读盲区**：`last_seen_at` 被 health 与 telemetry **共同刷新**，「最近在线很新鲜」根本不能证明遥测在流——而电量/速度恰恰**只由 telemetry 路径写入**。⇒ 现场永远分不出「车端没报 chassis」「遥测链路断」「真的是 0」。

修复内容（受追踪 **25 个文件**，无新增文件）：
- **A `flink-jobs/hunter_flink/ingest_latency_job.py`（写端计数 + 诊断载荷 + 容错）**：① 新增 `COUNTER_KEYS`（`unparsable_message` / `no_vehicle_timestamp` / `no_kafka_record_timestamp` / `negative_latency` / `non_finite_latency`，**顺序即主因判定优先级**）与 `new_counters()` / `reset_counters()` / `_bump()`；② `expand_latency(...)` 新增可选 `counters` 形参、`arrival_time` 放宽为可为 `None`（缺记录时间戳同样丢弃但可计数），**逐条计数而缓冲行为一字未改**；③ `diagnostic_reason()` 给出受控词表的单一主因（`no_input` / 某丢弃路径名 / `mixed_discards` / `window_not_closed`），`diagnostic_payload()` 产出 `p95_ms=null` + `reason` + `seen` + `dropped{}` 的**诊断载荷（不伪造数值）**；④ `should_emit_diagnostic()` 以 **Kafka 记录时间戳**对齐节拍（`INGEST_LATENCY_DIAGNOSTIC_INTERVAL_SECONDS`，默认 60s）——不用事件时间（全丢弃时根本没有事件时间可用）也不用处理时间（会把节拍交给 TM 启动时刻偷走一个整间隔），**这正是「键永不存在」的解法**：诊断写键不依赖窗口冲刷；⑤ `LatencyProcessor` 两条写键路径 + `valid > 0` 覆盖门禁 ⇒ **绝不用 null 载荷顶掉已算出的真 P95**；⑥ `json.loads` 包 `try/except`（根因②），丢弃与容错都落 `logger.warning`。
- **B `services/data-analytics`（读端把 reason 翻成人话，且三档不互混）**：`app/repositories/pipeline.py` 新增 `@dataclass IngestLatencyReading(p95_ms, reason, key_present)` + `LATENCY_REASON_LABELS`（8 主因中文文案表）+ `_diagnostic_text()`；`IngestLatencyRedisReader.read()` 一次 GET 同时取真值与诊断（键存在但不可解析 ⇒ `key_present=True, reason=None`，**读不出内容就不给结论**），`p95_ms()` 降级为薄包装；`PipelineSource` 协议方法改名 `ingest_latency()`；`app/services/dashboard.py` 新增 `LATENCY_KEY_ABSENT_REASON`（「指标键不存在 = 作业未产出」口径，内含 `flink list -a` 与消费组名）与 `ingest_latency_diagnostic()`（①有值→None ②写端已定性→逐字透传 ③键不在→服务层补）；`app/schemas/dashboard.py` 新增 `ingest_latency_reason`；测试替身 `fakes.py` 属性改名 `ingest_latency_p95`（避免同名遮蔽协程方法）。
- **C `frontend`（看板副文案 + 电量 null 渲染）**：`utils/format.ts` 新增 `formatSoc(value?)`（`null`/`undefined`/NaN → `-`，否则 `N%`）；`DashboardView.vue` 新增 `ingestLatencyMs` / `ingestLatencyHint` computed——**无值时卡片副文案直接显示后端 reason**，后端未给 reason 时退回「暂无数据（看健康检查 ⑯）」；电量列改 `formatSoc(row.battery_soc)`；`RemoteConsoleView.vue`（车辆下拉 + 实时遥测「电量」行）与 `OtaTasksView.vue`（SOC 标签）同步改用 `formatSoc`；`types/remote.ts` 的 `battery_soc: number | null`、`velocity?: number | null`，`types/analytics.ts` 的 `PipelineStats.ingest_latency_reason?`。
- **D `services/remote-control`（去谎报）**：`app/services/vehicle_view.py` 去掉 `or 0` / `or 0.0` ⇒ 缺失即 `None`（**0 仍如实是 0**），模块 docstring 与字段常量注释钉住「曾把缺失兜成 0」这条教训；`app/schemas/vehicles.py` `battery_soc: int | None`、`velocity: float | None`（`default=None`）。`last_online_time` **保留** `or 0.0` 并注明理由：它是时间戳量而非量纲值，且契约必填、前端按 `>0` 判「尚未上线」，兜 0 不会被读成某种业务量。
- **E `services/data-collector/app/services/vehicle_status.py`（补判读盲区）**：`update_from_telemetry` 的 HSET mapping 新增 **`telemetry_seen_at`**（遥测侧专属心跳，`.3f`），`update_from_health` **不写**——从此「`last_seen_at` 新鲜但 `telemetry_seen_at` 缺失」= 只有 health 在活、遥测断流，是症状 2 的定性手段之一。
- **F `scripts/health-check.sh`（新增第 ⑯ 项）**：`redis_cmd()`（被 `$(…)` 捕获 ⇒ stdout 只回回复，符合 `verify_infra` 第 19 项形状判据）+ `check_ingest_latency_metric()`——`GET/TTL analytics:ingest_latency` 拿到值就报 P95（`TTL=-1` 额外 WARN「写端没带 EX 300」），拿到 `p95_ms=null` 就报 reason，**键不存在时用消费组 `data-analytics-telemetry-raw` 的 LAG 三分**：`LAG>0` → WARN「正在回放历史，追平后自动出数，无需人工介入」；`LAG=0` → FAIL「读到了却从未写出指标」（旧镜像无诊断载荷 / `telemetry_raw` 确无新数据两条成因）；读不到组 → FAIL「从未完成一次消费/提交」并给出 `flink list -a` + JM 日志取最后一条 `Caused by` 的命令。头部检查项清单改 **17 类**、`usage()`、环境变量清单（`REDIS_CONTAINER` / `INGEST_LATENCY_GROUP`）与 `run_all_health_checks` 循环同步；`scripts/daily-check.sh` 与 `README.md` 的「16 类」文案一并改为 17 类并补「入库延迟指标键」。
- **G 契约三份**：`contracts/database/redis-keys.yaml` 键 #9 的 `value`/`lifecycle` 登记诊断载荷形状与「按到达时间对齐的节拍」口径，`vehicle:status` 的 `fields_draft` 新增 `telemetry_seen_at`（仍属 draft），pending #1 字段数 25 → **26**；`contracts/openapi/data-analytics.yaml` 新增 `PipelineStats.ingest_latency_reason`（三档语义）、`x-hunter-realtime-jobs.side_output.value/note` 补诊断载荷、`dashboard-contract.pipeline.ingest_latency` 同步；`contracts/openapi/remote-control.yaml` 的 `ControllableVehicle` description 新增「**必现字段 ≠ 必有值**」段，`battery_soc`/`velocity` 展开为多行并标 `nullable: true`，`last_online_time` 描述补 `last_seen_at` 取数源，§车辆可用性 73-74 行字段名订正，pending #2 文案同步。
- **H 测试（净增 16 条）**：`flink-jobs/tests/test_ingest_latency.py` +6（三条丢弃路径逐条计数且不污染缓冲 / 有效样本计数与 `window_not_closed` / `reset_counters` 只留归零字段 / `diagnostic_reason` 单一主因三档 / 诊断载荷 `p95_ms` 恒 null 且载荷形状 8 键 / 节拍以到达时间对齐）；`test_ingest_latency_reader.py` +6（`read()` 六路径：真值不带 reason、诊断载荷译文含 `seen=`、无 reason 时点明「写端未升级至 V1.19.21」、键不存在 `key_present=False`、不可解析/非对象仍算键存在、Redis 故障静默降级）；`test_dashboard_endpoints.py` +2（键不存在档 / 写端 reason 逐字透传档，两档互斥）并给「有真值」用例补 `ingest_latency_reason is None`；remote-control `test_routers.py` +1（字段缺失 → `battery_soc/velocity` 为 null，且写回 "0" 后仍是 0）；data-collector `test_ingest_pipeline.py` +1（未上报不写字段但 `telemetry_seen_at` 仍写）并给 health/telemetry 两用例补 `telemetry_seen_at` 有/无的断言。

向后兼容：① **REST 响应形状变化（读方需知晓）**：`ControllableVehicle.battery_soc` / `velocity` 仍属 `required`（键必在）但**可为 null**——原先恒为数字的读方（含第三方）若把 0 当合法值需改判 null，本仓前端三处已同步为 `formatSoc`；② `PipelineStats` 新增可选字段 `ingest_latency_reason`（纯增量），但看板路由带 `response_model_exclude_none=True` ⇒ **null 字段不序列化**（前端一直按可选读，无需改动）；③ **Redis 键 #9 的载荷新增 `reason`/`seen`/`dropped` 且 `p95_ms` 可为 null**：仓内写读两端同源升级，仓外若直读该键需按新形状判 null（写侧仍带 `EX 300`）；④ **诊断载荷不会覆盖真指标**（`valid > 0` 门禁），出数路径与 V1.19.20 完全一致；⑤ 丢弃口径**未放宽一条**：负延迟、非有限值、缺时间戳仍是丢弃，只是从此可数、可说；⑥ `telemetry_seen_at` 是纯增量 HSET，取不到该字段的读方行为不变；⑦ **巡检从 16 类变 17 类**：新增项在「键不存在且读不到消费组」时判 **FAIL** ⇒ 存量环境首跑很可能新增一条 FAIL、退出码从 0/1 变 2，靠 `grep -c '\[FAIL\]'` 或退出码做门禁的自动化需知晓（这是把既有故障说出来，不是新故障）。

风险：① **本条全部改动未经实机验证**（本环境无 Docker/Redis/集群），且**本机没有 bash**（`bash` 解析到 `C:\Windows\System32\bash.exe` 即 WSL 入口、无 Git Bash）⇒ `bash -n scripts/health-check.sh` **没跑**，已列为实机第 0 步；本地替代检查只有结构对账（`if`/`fi` 73↔73、函数定义与闭合 `}` 27↔27、新函数区域逐行复核、`verify_infra` 第 19 项对新函数的 stdout 纯度判 PASS）；② `reason=no_input` 这一档**在写端永远不会出现在键里**（没有元素进入 `process_element` 就没有节拍基准），它的实际作用是让 `diagnostic_reason()` 的语义可测、并与巡检 ⑯ 的「键不存在」分支同义——不要指望看板出现「未读到任何消息」这句话，真的没输入时看板看到的是「指标键不存在」；③ 长期全丢弃时看板会**每 60s 刷新一次同一条 WARN 文案**（正确行为，但视觉上像指标在抖）；④ reason 文案含键名与消费组名，直接透出给持 `analytics:read` 的用户（换取运维可自助定位，判定为可接受，若需对外接口应收敛为错误码）；⑤ null 化只改了 remote-control 一处读方——ota-service 门禁早就是「缺数据即拒绝」、data-analytics 不读该字段（本条已核对），但**仓外读方**若有 `or 0` 同款写法仍会谎报（沿用 V1.19.20 待办⑤的对账口径）；⑥ 诊断写键落在算子副作用里，与真值共用一把 `SET`：并行度 >1 时各 subtask 独立计数、最后写入者生效（当前 `key_by` 常量 ⇒ 单 subtask，扩并行度需改为聚合口径）。

验证（本环境，均实跑）：`run_unit_tests.py` **9 目标 1087 passed / 1 skipped**（326 / `tests` 100 deselected / **94+1s** / 71 / 108 / **149** / **113** / 128 / **98**）——较 V1.19.20 的 1071 **净增 16 条**（flink 6 + data-analytics 8 + data-collector 1 + remote-control 1），既有用例零改动通过；`ruff check flink-jobs services/data-analytics services/data-collector services/remote-control scripts` **All checks passed**；`verify_infra.py` **20 项全 PASS**（第 19 项：`已解析 173 个函数、30 个被捕获调用`，含新增 `redis_cmd`）；`verify_data_layer.py` **通过 34 项、失败 0 项**（含「Redis 键契约一致：9 个受控键 ↔ 6 份服务契约」——fields_draft 无校验器解析，新增字段不影响门禁）；`verify_test_layer.py` L5 自检通过；`generate_contracts_configmap.py --check` PASS；`render_k8s.py --check` PASS；`npm run type-check`（`vue-tsc --noEmit`）**零输出通过**（同时覆盖 `formatSoc` 与 `battery_soc: number | null` 在模板里的类型）。℞ 过程中三条假红全部是本环境/用例自身问题，非产品缺陷：① 首跑 data-analytics 2 条红是**用例把 null 字段当序列化键读**（`response_model_exclude_none=True` 下 `KeyError`）⇒ 改 `.get()`；② 再把两档诊断写进**同一个用例**仍读到首次文案 ⇒ `_cached(request, KEY_DASHBOARD_SERVICE, …)` 把首次 seed 的服务缓存到用例结束 ⇒ 拆成两个用例（已在 docstring 钉住「一个用例只 seed 一次」）；③ `FakePipelineRepository` 曾用 `self.ingest_latency` 存值遮蔽同名协程方法（`float not callable`）⇒ 属性改名 `ingest_latency_p95`。

分发路径注意：受追踪 **25 个**、**无新增文件**（`git status` 无 `??`）⇒ `git add -u` 即可覆盖：`README.md`、`contracts/database/redis-keys.yaml`、`contracts/openapi/{data-analytics,remote-control}.yaml`、`flink-jobs/{hunter_flink/ingest_latency_job.py,tests/test_ingest_latency.py}`、`frontend/src/{types/analytics.ts,types/remote.ts,utils/format.ts,views/DashboardView.vue,views/ota/OtaTasksView.vue,views/remote/RemoteConsoleView.vue}`、`scripts/{daily-check.sh,health-check.sh}`、`services/data-analytics/app/{repositories/pipeline.py,schemas/dashboard.py,services/dashboard.py,tests/fakes.py,tests/test_dashboard_endpoints.py,tests/test_ingest_latency_reader.py}`、`services/data-collector/app/{services/vehicle_status.py,tests/test_ingest_pipeline.py}`、`services/remote-control/app/{schemas/vehicles.py,services/vehicle_view.py,tests/test_routers.py}`。HEAD = `f2d6a23`（V1.19.20 已提交）⇒ 本条 25 个文件**均未提交**，服务器 `git pull` 取不到；`release.md` 与 `docs/` 照旧被 `.gitignore` 命中 ⇒ 单独 rsync。⚠ **A 项在 flink-jobs 镜像里、B/C/D/E 在服务与前端镜像里** ⇒ 只同步脚本只会让巡检 ⑯ 生效，看板仍会是旧的 `-`。

文档同步（本条已落：四份运维文档 + README）：
- `docs/故障排查指南.md`：§0 新增速查条「三个作业全 RUNNING ≠ 入库延迟会出数」（四种成因 + 一句命令分流 + “不要为看不到数去放宽丢弃判据”）；场景 13 「第四层」的入库延迟行补指新段，并新增「**第四层补充：入库延迟恒 `-` 的四成因判读（V1.19.21，巡检 ⑯）**」段——含取证命令、**⑯ 读数→键状态→消费组→定性→处置**五行判读表、**`reason` 受控词表八行逐条处置表**、以及“全丢弃时窗口永不冲刷 ⇒ 诊断改按 Kafka 记录时间戳节拍”的机制说明与 `response_model_exclude_none` 读数陷阱；场景 13「车辆明细」表的电量行由「尚未确诊」改写为 **V1.19.21 已确诊（`or 0` 谎报）**并给出 `battery_soc` 为 `None` 与为 `0` 两条**相反**的排障方向；共同教训段的 `fields_draft` 字段数 25 → 26。
- `docs/运维操作手册.md`：§2.1 每日巡检清单新增一行「**入库延迟指标键（⑯）**」（标准 `PASS P95=<n> ms（TTL=<s>s）`，异常按三分支处置）；§2.2 的复用检查类数 **16 → 17**（并列出 ⓪/⑭/⑮/⑯ 各自的引入版本）；§3.3 读模型注补第三条 **`telemetry_seen_at`（仅 telemetry 写、health 不写）** 与“只有 `last_seen_at` 新鲜 = 只有 health 在流”的读法，字段数 25 → 26；§3.6 出数核验后新增「作业 RUNNING 但看板仍是 `-`」的 **⑯ 判读引注**（三条手工命令 + `TTL=-2/-1/有 reason` 三档口径与受控词表）。
- `docs/deployment.md`：§13.3 开头段补一句「看板显 `-` 时**V1.19.21 起不再需要猜**」并指向本节末尾；在 V1.19.20 注之后新增 **V1.19.21 段**：三端（写端/读端/展示层）**部署后行为 + 验收动作**两列表（含 `formatSoc` 使 `-` 与 `0%` 从此不同含义、写端镜像是否已升的「能看到 `reason` 字段」判据）、必跑验收命令（⑯，全栈现为 **17 类**）、以及「`WARN …正在回放` 不是故障、不要为让它绿去清位点」的提醒；顺带修掉该段一个错字（“常见情形态”→“最常见的形态”）。
- `docs/01-部署概述与环境要求.md`：目录树的 `health-check.sh` 行 **16 类 → 17 类**，并补三行注释说清 ⑯ 读什么、把 `-` 分成哪四种成因。
- `README.md`：脚本表 health-check 行同步为 17 类并写清 ⑯ 口径（GET/TTL + 消费组积压 + 未产出/在追赶/样本全丢弃三分）。

服务器实机步骤：

```bash
cd /opt/HunterCore
# 0) 本环境唯一没跑的检查，先补上
git status --porcelain | head -30
bash -n scripts/health-check.sh && bash -n scripts/daily-check.sh && echo SYNTAX_OK
# 1) 同步 25 个文件 + release.md + docs/，再重建镜像（A 在 flink 镜像、B/C/D/E 在服务与前端镜像）
sudo bash scripts/install.sh --only-step 6 --skip-docker -y
sudo bash scripts/install.sh --only-step 11 -y        # 内含 submit-flink-jobs.sh；recreate 会冲掉作业，必须重跑
# 2) 关键前置：确认 flink-tm 里是**新版** ingest_latency_job（否则永远看不到 reason）
sudo docker exec hunter-flink-jm flink list -a         # 期望 3 行 RUNNING，无 RESTARTING/FAILED
sudo docker inspect -f '{{.Created}}' hunter-flink-tm
# 3) 症状 1 定性（看板不再出现 `-` 之后，读键就知道是哪一档）
sudo docker exec hunter-redis redis-cli --no-auth-warning -a "$REDIS_PASSWORD" GET analytics:ingest_latency
sudo docker exec hunter-redis redis-cli --no-auth-warning -a "$REDIS_PASSWORD" TTL analytics:ingest_latency
sudo bash scripts/health-check.sh 2>&1 | grep -E '入库延迟指标键|Kafka 消费积压|Flink 作业健康'
# 4) 症状 2 定性（本条改动让这一次读数直接给答案）
sudo docker exec hunter-redis redis-cli --no-auth-warning -a "$REDIS_PASSWORD" HGETALL vehicle:status:HUNTER-001
#   看三样：battery_soc 在不在、velocity 在不在、telemetry_seen_at 新不新（缺它 = 只有 health 在活）
curl -s "http://127.0.0.1:8085/api/v1/remote/vehicles?controllable_only=false" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys;[print(v["vehicle_id"],repr(v["battery_soc"]),repr(v["velocity"]),v["last_online_time"]) for v in json.load(sys.stdin)["data"]["items"]]'
# 5) 遥测原始消息对照（判定「车端没报 chassis」还是「链路断」的唯一手段）
sudo docker exec hunter-kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/client.properties \
  --topic hunter.HUNTER-001.telemetry --from-beginning --max-messages 3 \
  --consumer-property enable.auto.commit=false 2>/dev/null
# 6) 收尾体检（17 类，新增项首跑可能给 FAIL——是把既有故障说出来，不是新故障）
sudo bash scripts/health-check.sh 2>&1 | tee /tmp/hc21.log | tail -n 8
grep -nE '^\[(WARN|FAIL)\]' /tmp/hc21.log
python3 scripts/verify_infra.py | tail -n 4
```

判据预告：① 第 3 步键里有 `p95_ms` 数值 ⇒ 症状 1 结案（看板出数）；有 `"p95_ms": null` + `"reason"` ⇒ 看板副文案会显示这句人话，**按 reason 分支处置**（`negative_latency` → 车端时钟超前，对齐 NTP 后延迟统计自然出数；`no_vehicle_timestamp` → 车端消息体缺 `timestamp`，属上报格式问题；`no_kafka_record_timestamp` → Topic 未配 `log.message.format.timestamp`/记录无时间戳；`unparsable_message` → `telemetry_raw` 里有非 JSON 载荷，回 TM 日志取该行）；仍是 `(nil)` 且巡检 ⑯ 报「正在回放历史」⇒ **等待追平即可**（7 天历史不是本条的故障，`flink list` 的 `--describe` 看 committed offsets 是否在推进）；报「读不到消费组」⇒ 回到 V1.19.16～20 那条链（检查点目录 / 凭据 / API 守卫）。② 第 3 步若 ⑯ 报「键存在但既无 p95_ms 也无 reason（载荷形状不符）」⇒ flink 镜像没重建到位（A 项没生效），先补第 1 步。③ 第 4 步 `battery_soc` 输出 `None` ⇒ 读模型确实没这个字段，看板应显示 `-`（**不再显示 0%**），再按第 5 步分「车端没报 chassis」与「遥测链路断」；输出 `0` ⇒ 车端原始值就是 0，属数据源问题、不是平台谎报。④ 第 4 步 `telemetry_seen_at` 与 `last_seen_at` 差值持续变大 ⇒ 只有 health 在活、遥测断流（此时电量必为 `None`），排障方向是 data-collector 的 telemetry 订阅而非车端。⑤ 第 6 步 ⑯ 与 ⑩ 若同时出现「组读不到」和「积压未判定」⇒ 优先怀疑 Kafka 凭据/连通（本条 ⑯ 会明确说「不是没数据」），别按「无数据」结案。

**待办（不属本条范围，均为未确诊/未改动项，不得当已修）**：① **crash-loop（根因②）未确诊**：只消除了「一条非法 JSON 让作业永久卡住」的能力缺口，实录里那条 RESTARTING 究竟是不是它，需第 3 步之前先取 TM 日志的堆栈才能定；② **电量 0% 仍未确诊**（本条把它变成「一次读数即可定性」，没有替车端猜数据）；③ `MAX_FUTURE_SKEW_SECONDS=60`（data-collector 写侧放行「车端时钟超前 ≤60s」的样本入库）与延迟统计「负即丢弃」**口径不一致**：这批样本入库成功但 100% 不进延迟统计。本条只把它变成可读的 `negative_latency`，**未改任一侧行为**，是否统一为「统计侧按 skew 容忍带重试/入库侧收紧」需人工决策；④ 诊断载荷写键在算子内，并行度 >1 时语义需重新定义（当前 `key_by` 常量 = 单 subtask）；⑤ 「感知时延 0 ms / 控制时延 0 ms」是 DB 真值，车端是否真的上报 0 属数据源问题，本条未处理；⑥ 事件时间窗口无 watermark ⇒ 尾窗口样本滞留缓冲、且乱序晚到样本会被当成「更晚窗口之后的东西」丢弃（现可计数但仍丢弃），是否引入 bounded-out-of-order watermark 属功能变更需批准；⑦ 读方字段对账（V1.19.20 待办⑤）与「Fake 字段形状必须来自写方」的静态门禁（V1.19.20 待办⑥）原样挂账；⑧ 本条 17 类巡检、`verify_infra` 第 19/20 项均未进 CI（`.gitlab-ci.yml` 的 lint 阶段不跑 `bash -n`/`shellcheck`，也不跑 `verify_infra.py`）；⑨ compose 形态无自动重提（V1.19.20 待办④）、V1.19.18 待办⑥ 原样挂账。


## V1.20.0（新增：ingest-platform 微服务——《服务端数据消费解析与处理详细设计文档》6 项核心需求全部实现：消费接入与幂等、元数据投影与 bag 状态机、多传感器解析、跨模态时间对齐、场景切片与质检、任务编排 + 查询/导出 API 与指令下发；含存储/Parquet 写出、平台事件生产、REST 8 组路由与 Dockerfile）

Date：2026-10-13
变更类型：新增（新服务 `services/ingest-platform`，FastAPI + 7 Kafka 消费者 + orchestrator 任务主循环；配套 `contracts/database/ddl/06_ingest_platform.sql`（ingest_platform schema 12 表）；REST 契约为服务内 Pydantic Schema 先行，`contracts/openapi/` 尚未登记 ingest-service.yaml（接入编排时一并补）；服务代码自包含，不影响已部署的 6 个业务服务与 Flink 作业）

版本主要有以下功能（按设计文档 6 项核心需求，全部落到模块与单测）：
1. **消费接入与幂等（§5）**：7 个 Kafka 消费者（`consumers/consumers.py`：ManifestConsumer/ArtifactConsumer/ParseProgressConsumer/AlertConsumer/HeartbeatConsumer/CmdAckConsumer/TaskStatusConsumer，Topic 对应 `vehicle.bag.*` / `vehicle.agent.*`）；`consumers/base.py` 统一消费基类（手动提交 + DLQ + 层 2 seq/rank 单调守卫 + 内存 `IdempotencyGuard` 快速路径）；`main.py` 的 `_SupervisedConsumer` 崩溃退避 5s 重建重启（与 data-collector 同纪律）。
2. **元数据投影与 bag 状态机（§5.2）**：`services/projection.py`（manifest/进度/告警 → `ingest_platform.bags` 状态投影与到达判定，满足时触发任务链首节点）；`repositories/` 12 个仓储（events/bags/artifacts/derived_assets/clips/tasks/commands/agents/anomaly 等）对应 12 张 ORM 表（`models/ingest.py`，含受控枚举 TaskState/TaskType/UploadState/ArtifactClass/DerivedKind/ClipSource 等，与 DDL CHECK 逐值对齐）。
3. **多传感器解析（§6）**：`parsers/` 七套——rosbag_reader（rosbags 纯 Python 库，无需 ROS2 运行时）+ pointcloud（PCD→parquet）/image（EXIF/尺寸）/imu/odometry/gnss/can 逐模态解析；`storage/parquet_writer.py` 统一 Parquet 写出（pyarrow）。
4. **跨模态时间对齐（§6/§7）**：`time_sync.py`（时钟源/偏移估计/容差判定）+ `services/align_worker.py`（对齐任务：多模态样本按公共时间轴合并写出 aligned 派生资产）。
5. **场景切片与质检（§7.3）**：`services/slice_worker.py`（窗口/标签切片 → `clips`）+ `services/qc_worker.py`（完整性/丢帧/曝光等质量检查与评分落库）。
6. **任务编排 + 查询/导出 API 与指令下发（§8/§9）**：`services/orchestrator.py` 主循环（轮询/claim/租约/重试/死信状态机）；REST 8 组路由（`/api/v1/bags|clips|tasks|artifacts|datasets|agents|commands` + health，§8.1 查询/导出）；`services/dataset_service.py`（数据集导出 §8.2）、`services/cmd_service.py` + `repositories/commands.py`（指令下发与回执 §9.4）、`producers/platform_events.py`（平台事件生产 §7.5）；`storage/minio_client.py` LakeStorage（预签名上传/下载 + HEAD 元数据，bucket 按契约）。

配套工程：`config.py`（服务私有配置：各 worker 开关、orchestrator 轮询/锁 key、脱敏参数，继承 HunterBaseConfig）；`core/dependencies.py`（RBAC + 内部签名头）；`Dockerfile`（多阶段 distroless，同六服务基线）；`pyproject.toml`（新增运行时依赖 rosbag/pyarrow/numpy/Pillow）；单元测试 7 个文件（consumers/parsers/time_sync/workers/storage/projection/schemas）；`scripts/run_unit_tests.py` 将 ingest-platform 纳入全仓回归目标。

验证：`run_unit_tests.py` 全仓含 ingest-platform 目标全部通过（后续 V1.20.1 口径：ingest-platform **126 passed**）；`ruff check` 服务目录通过（余 main.py/models 两处基线既有告警另行登记）；契约校验由 `services/ingest-platform/tests/test_ddl_contract.py` 钉住（DDL↔ORM 表清单/逐列/枚举取值全等）。

风险与待人工确认：① **服务尚未纳入部署编排**（compose/k8s/巡检清单均无该服务，接入步骤见 deployment.md §14）；② 本条当时未同步 README 与运维四件套文档，文档同步随 V1.20.1 发布一并补齐（本条记录即为追认）；③ ENRICH/EXPORT 属 M4/M5 预留未实现；④ 设计文档不在仓内（外部提供），本节 § 编号以代码注释引用为准。

## V1.20.1（修复：ingest-platform 代码审查 P0 三项——① orchestrator 无 VERIFY/ENRICH worker 却接收 projection 创建的这两类任务（任务永久 pending）且无 §7.1 依赖序；② 会话级 `pg_try_advisory_lock` 在连接池短会话下锁语义失效（多副本并发编排）；③ 幂等只靠进程内 `IdempotencyGuard`（重启即失忆，重复事件重复副作用）——三层幂等的层 1 events 表从未接入；连带清掉两项契约同步债）

Date：2026-10-13
变更类型：修复（ingest-platform 服务代码 + 共享库契约测试 + `contracts/database/ddl/00_schemas.sql`。⚠ 全部在服务/契约层 ⇒ 生效需重建 `ingest-platform` 镜像并滚动更新；00_schemas.sql 增列对已建库无影响（`IF NOT EXISTS`），新环境重放 init SQL 即含）

现场（对《服务端数据消费解析与处理详细设计文档》v1.0 的审查报告 P0 三项，逐条复核到行号后确认均成立）：
1. **VERIFY/ENRICH 任务死锁在 pending**：`projection.py` 在 bag ready 时创建 `VERIFY`+`ALIGN`+`QC`+`SLICE`+`ENRICH` 全图任务，但 `orchestrator.py` 的 `_workers` 只注册 ALIGN/SLICE/QC ⇒ VERIFY 永远无人执行（下游 ALIGN 因依赖序本就无法开工）、ENRICH 同样永挂；且 orchestrator 完成一个任务后不推进任何后继（§7.1 task_graph 未实现）；
2. **advisory lock 形同虚设**：原实现启动时 `pg_try_advisory_lock`（会话级）一次定主，但 `DatabaseSessionManager` 走连接池、session 结束连接归还即可能换会话 ⇒ 锁随时随会话销毁而释放、其他副本根本感知不到；重启窗口内还可能双主；
3. **幂等层 1 缺失**：设计 §5.2 三层幂等中「events 表 `ON CONFLICT DO NOTHING`」的 `IngestEventRepository.insert_dedup` 全仓零调用点，去重仅靠进程内 `IdempotencyGuard`（LRU 内存）⇒ 消费者重启/扩缩容/重分区后同一 event_id 重复投递会重复执行副作用（投影重写、任务重复触发）。

修复内容（按审查报告口径三项 + 连带契约债，共 11 个改动文件 + 4 个新增文件）：
- **① 任务图与 worker 补齐**：新增 `app/services/task_graph.py`（§7.1 后继表 `_SUCCESSORS`：VERIFY→{ALIGN, QC（并行）}、ALIGN→{SLICE}；ENRICH 不在图内——worker 属 M4/M5 未实现）；新增 `app/services/verify_worker.py`（VerifyWorker：逐 artifact 经 `storage.ahead_parsed`（新增异步方法，boto3 同步 HEAD 走 `run_in_executor` 不阻塞事件循环）核对存在性/声明 size/sha ⇒ 分别置 `missing`/`corrupt`/`verified`；必需类（`ready_required_class_list`）缺失或损坏时先提交留痕再 raise 触发退避重试）；`orchestrator._workers` 注册 VERIFY；`orchestrator._advance_task_graph` 在任务 complete 的**同事务**内按后继表创建下游任务（仅派发给已注册 worker，防死挂），EXPORT/ENRICH 的 NO_WORKER 兜底 fail 路径保留；`projection._check_and_trigger` 改为**只创建 VERIFY 首节点**（不再预铺全图）；`config.enrich_worker_enabled` 默认 True→False（预留注释）。
- **② 锁语义**：orchestrator 每轮 claim、lease-GC 轮各自在**事务内**执行 `pg_try_advisory_xact_lock(key)`（`_try_xact_lock`），抢不到即 `rollback`+跳过本轮，事务提交自动释放 ⇒ 任意时刻至多一个副本在编排、副本崩溃/网络分区丢锁后其余副本下一轮自动接管（丢主降级），`_is_leader` 仅作观测日志；废除启动时一次性的会话级 `pg_try_advisory_lock`。
- **③ 事件落表去重（层 1）**：`consumers/base.py` 构造注入 `db_mgr`，`_handle` 在**与业务处理同一事务**内 `IngestEventRepository.insert_dedup(_event_row(...))`——`False`（event_id 冲突）即跳过处理；process 抛错 ⇒ 事务回滚 ⇒ 事件行不落表 ⇒ Kafka 重投可完整重执行（at-least-once 语义不破坏，杜绝「事件先落表、处理失败后重投被去重挡掉」的 at-most-once 退化）；内存 `IdempotencyGuard` 降级为**快速路径**（省一次 DB 往返，正确性不再依赖它）。辅助：`_coerce_event_id`（非 UUID 时 `uuid5(NAMESPACE_URL, "ingest:{topic}:{partition}:{offset}")` 确定性派生）、`_parse_occurred_at`（ISO/epoch 秒/ns，回退 now(UTC)）；7 个消费者工厂（consumers.py/main.py）全部改传 `db_mgr`；HeartbeatConsumer 删除冗余自备会话。
- **④ 契约同步债（连带，上一阶段遗留）**：`contracts/database/ddl/00_schemas.sql` 补 `CREATE SCHEMA IF NOT EXISTS ingest_platform;`（此前 common 循环校验 ALL_SCHEMAS 失败）；`common/python/tests/test_database_models.py` 的 `ddl_tables()` 跳过 `ingest_platform.*`（服务 ORM 归属服务侧，common 不导入服务包），DDL 存在性清单补 `06_ingest_platform.sql`；表级校验移交新建的 `services/ingest-platform/tests/test_ddl_contract.py`（12 模型表清单/逐列 + 8 枚举类型 ↔ StrEnum 取值全等）。
- **⑤ 契约测试抓到的真实漂移**：`ingest_platform.tasks` DDL 有 `updated_at` 而 ORM `Task` 无 ⇒ `app/models/ingest.py` 补列（`server_default=func.now()` + `onupdate`）。

向后兼容：① **任务流形状变化**：bag ready 后不再一次性铺全图，任务按 VERIFY→{ALIGN,QC}→SLICE 逐个出现——依赖任务行做展示/统计的读方需知晓「列表在编排过程中会增长」是正常态；② **ENRICH 不再自动创建**（`enrich_worker_enabled` 默认 False），需要 M4/M5 时手动开配置并接入 worker；③ 消费者开始向 `ingest_platform.events` 全量写行（含 heartbeat/alert 高频流），存储占用为新特征；④ advisory lock 键值与库不变，仅获取/释放时机改为每轮事务；⑤ REST/Kafka 契约零变化。

风险：① 事件事务跨 `process()` 全程持有一个池连接（7 消费者并发下各 +1），连接池上限需按 `db_pool_size` 复核；② events 表 heartbeat 高频写入量（当前无清理策略，需纳入保留策略待办）；③ verify 的 sha 校验仅在对象存储元数据携带 sha 时生效，车端 manifest 未声明 sha 的资产只核 size（与设计「derived 元数据补全」联动，属 P1 待办）；④ EXPORT/ENRICH 若被外部创建仍走 NO_WORKER fail（兜底保留，不算修复）；⑤ 本条全部改动为**本地单测验证**（无实机多副本环境），双主/丢主降级仅由单测断言锁分支覆盖。

验证（本环境，均实跑）：`run_unit_tests.py` 全仓 **10 目标全绿**——common **326**、flink-jobs 94+1s、api-gateway 71、scene-service 108、data-collector 149、data-analytics 113、ota-service 128、remote-control 98、**ingest-platform 126**（净增 `test_orchestration.py` 26 例：task_graph 后继表 / VerifyWorker 四态 / orchestrator 争锁-推进-NO_WORKER / insert_dedup 新-旧-回滚三路径 / projection 首节点断言；`test_ddl_contract.py` 契约校验 21 例）；`ruff check` 对本次触及文件 **All checks passed**（main.py/models 剩余 7 项为基线既有代码，未扩大改动面）。

分发路径注意：新增 4 文件（`verify_worker.py`、`task_graph.py`、两份测试）+ 修改 11 文件（ingest-platform 9：base.py/consumers.py/orchestrator.py/projection.py/config.py/main.py/artifacts.py/minio_client.py/ingest.py，契约与 common 测试 2：00_schemas.sql、test_database_models.py）。⚠ **全部在服务代码里 ⇒ 必须重建 ingest-platform 镜像**（`--only-step 6`）；00_schemas.sql 仅影响全新环境初始化路径。

服务器实机步骤：

```bash
cd /opt/HunterCore
git add -u && git add services/ingest-platform/app/services/{verify_worker.py,task_graph.py} \
  services/ingest-platform/tests/{test_orchestration.py,test_ddl_contract.py}   # 4 个新文件必须显式 add
# 构建推送后：
sudo bash scripts/install.sh --only-step 6 --skip-docker -y     # 重建 ingest-platform 镜像
kubectl -n huntercore rollout status deploy/ingest-platform     # compose 形态: docker compose up -d --build ingest-platform
# 验收：
# 1) 任务图推进：上传一个 bag，观察任务逐个出现而非一次性铺出
sudo docker exec hunter-postgres psql -U hunter -d huntercore -c \
  "SELECT task_type,state,error_code,created_at FROM ingest_platform.tasks WHERE bag_id='<bag_id>' ORDER BY created_at;"
#    期望序列：verify(done) 后 align+qc 同时出现，align(done) 后 slice 出现；无 pending 的 verify/enrich 死行
# 2) 锁降级：双副本下仅一个副本日志出现抢锁成功，另一个每轮静默跳过
sudo docker logs hunter-ingest-platform 2>&1 | grep -cE 'orchestrator_acquired_round_lock'
# 3) 层 1 幂等：重投同一 event_id（kafka-console-consumer 重放或重置位点）后
sudo docker exec hunter-postgres psql -U hunter -d huntercore -c \
  "SELECT event_id,count(*) FROM ingest_platform.events GROUP BY 1 HAVING count(*)>1;"   # 期望零行
#    且日志出现 duplicate_event_skipped
```

**待办（审查报告 P1/P2 项，本条未动、不得当已修）**：① HMAC 空 secret 跳过验签 + dev-hmac-key 回退（fail-open）；② QC worker 长事务占用连接；③ boto3 其余同步调用未 executor 化（仅 HEAD 已异步）；④ DLT 死信队列与消费背压未实现；⑤ events 表保留/归档策略；⑥ ENRICH/EXPORT worker（M4/M5）；⑦ verify 的 sha 全量校验依赖 manifest 补 sha 字段；⑧ 以上均已在审查报告登记优先级，等待排期。

## V1.20.2（修复/新增：ingest-platform 代码审查 P1 剩余 #4–#21 全部落地——数据处理正确性 10 项 + 安全 4 项 + 性能 4 项，含 QC 补齐黑帧/冻结帧/标定冲突三检、服务端切片规则引擎、dataset webdataset/lance 打包与 EXPORT 任务化、认证/签名 fail-closed、parsed 只读凭据隔离、GNSS 脱敏真正接入、存储 IO 全面 executor 化、QC 拆长事务、orchestrator 并发轮、消费背压、DLT 死信、cmd 超时重发、Alert 持久化分级路由、ParseProgress 落库；连带修复 routers/bags.py 引用 BagTopic 不存在属性的运行时缺陷）

Date：2026-10-13
变更类型：修复 + 新增（ingest-platform 服务代码 + `contracts/database/ddl/06_ingest_platform.sql`。⚠ 全部在服务/契约层 ⇒ 生效需重建 `ingest-platform` 镜像并滚动更新；DDL 新增列/表用 `IF NOT EXISTS` 幂等，已建库重放 init SQL 或执行本条 §15 幂等 ALTER 段即可，新环境重放全量含。新增配置项需同步进 compose `.env` / K8s ConfigMap，否则沿用默认值）

背景（对同一审查报告 V1.20.1 未覆盖的 #4–#21 逐条复核到行号后确认成立；本条一次性收口全部 18 项 + 1 项附带）：V1.20.1 修的是主干正确性（P0），本条修的是数据处理完整性、安全底线与吞吐扩展（P1）。

修复内容（按审查报告分类，逐条对齐设计文档 §5–§13）：

**安全（#14/#15/#16/#17，fail-closed 与最小权限）**
- **#14 认证 fail-closed**：`core/dependencies.py._require_forwarded_auth` 原 `if settings.gateway_hmac_secret and not verify(...)` 在密钥未配时整体跳过验签（任何能直达端口者伪造 `X-User-Id/X-Roles` 即绕过 RBAC）。改为密钥为空即抛 `AuthenticationError`，再校验签名 ⇒ 缺配即拒绝（§13.1）。
- **#15 签名 fail-closed**：`services/cmd_service.py._sign` 删除硬编码回退常量 `dev-hmac-key`（cleanup/reparse/seal 等破坏性命令曾用公开常量签名，可被伪造）。密钥为空直接 `RuntimeError` 拒签（§13.1/§9.4）。
- **#16 parsed 只读凭据隔离**：`storage/minio_client.py` 新增可选 `minio_parsed_access_key/secret_key`，配置了则建独立只读 boto3 client `_parsed_client`，parsed 层全部读路径（get/head/list/sample/iter）走它；未配则回退主 client（§13.2 最小权限；bucket policy 侧限制见 README）。
- **#17 GNSS 脱敏真正接入**：`mask_coordinates` 此前全库零调用。`services/align_worker.py` 表格对齐阶段对 gnss 的 lat/lon **实际调用 `mask_coordinates(lat, lon, places)`** 截断后写入 frame_index（derived 读路径即脱敏），`gnss_mask_enabled`（默认 True）/`gnss_descimal_places`（默认 4）可配（§13.4）。

**数据处理正确性（#4–#13）**
- **#4 QC 补三检 + 真解码**：`services/qc_worker.py` 检查项由 7 项扩到 9 项、权重重排至和=1.0（hash_integrity 0.22 / frame_gaps 0.18 / monotonicity 0.13 / message_count 0.12 / image_decode 0.08 / black_frozen_frames 0.10 / pcd_valid 0.09 / tf_available 0.04 / calibration_conflict 0.04）。新增纯函数 `detect_black_frame`（PIL 灰度均值 < `qc_black_frame_threshold`）、`detect_black_frozen`（连续帧内容 hash 相同 ≥ `qc_frozen_frame_min_count` 判冻结）、`detect_calibration_conflict`（tf child_frame_id 重复=conflict / 传感器全集未覆盖=incomplete）、`collect_sensor_ids`/`sensor_id_from_topic`；`_check_image_decode` 改为对 `qc_image_sample_count` 抽样帧 **真 `Image.open().verify()` 解码** 成功率计分（§7.4 检查项 5/7）。
- **#5 哈希校验来源改声明值**：`LakeStorage.sample_hash_check` 在对象无 `x-amz-meta-sha256`（车端上传即无）时，按 `artifacts.sha256`/manifest 声明值 **GET 实算 sha256 比对**，不再空转；QC 对判定损坏的 key 回写 `set_corrupt`（§7.4 检查项 1）。
- **#6 align 传感器全集 + 外参缺失置 null**：`services/align_worker.py` 重写为按 manifest 传感器全集分组展开（`group_by_sensor`/`_sensor_from_key` 从对象 key 父目录取名，多相机/多雷达各自最近邻），`_kind` 分类命名 dt/值列对齐 schema；tf 树中缺失外参（child_frame 不在 `manifest.tf_static`）的传感器其帧列**整列置 null**（不再直接跳过 topic）（§7.2 步骤 5）。
- **#7 服务端规则引擎切片**：`services/slice_worker.py` 新增纯函数 `_mine_event_timestamps`（IMU 去重力合加速度 `sqrt(ax²+ay²+(az-9.8)²)`，连续超 `rule_hard_brake_mps2` 段取峰值，峰值 ≥ `rule_collision_mps2` 判 collision 否则 hard_brake），`_mine_events` 读 imu parquet、`_slice_by_mined` 产出 `clips.source='mined'`；`rule_mining_enabled`（默认 True）与车端 `event_marks` 来源并列（§7.3 来源 1）。
- **#8 dataset webdataset/lance 打包**：`services/dataset_service.py._pack` 补齐分片产物——webdataset 用 stdlib `tarfile` 分片（每片 ≤ `dataset_shard_max_clips`，成员含各 clip manifest + snapshot.jsonl 索引），lance 写 `snapshot.parquet`；私有 `storage._put_object` 调用改公开 `put_dataset_object`（§8.2 步骤 3）。
- **#9 EXPORT 任务化**：`tasks` 新增 `payload` JSONB 列；`create_dataset` 新建行时投递 `type=EXPORT` 任务（payload 带 dataset_id/version/format），新增 `services/export_worker.py`（ExportWorker 委托 dataset_service.export），`orchestrator` 注册 EXPORT worker；export 本体推进 state `running→done/failed`，进程崩溃由 lease 队列续跑（§8.2/§9）。
- **#10 ParseProgress 落库**：`bags` 新增 `progress` JSONB 列；`repositories/bags.py.update_progress`；`consumers/consumers.py.ParseProgressConsumer` 由「仅 debug 日志」改为落库 `{percent,stage,updated_at}`；`routers/bags.py` 列表/详情/timeline 暴露 progress（§5.1；ClickHouse 历史留 M4/M5）。
- **#11 Alert 持久化 + 分级路由**：新增 `agent_alerts` 表 + `models/ingest.AgentAlert` + `repositories/alerts.py.AlertRepository`（record/mark_notified/mark_notify_error/list_recent）；`AlertConsumer` 落库每条告警，level ∈ `alert_critical_levels`（默认 `critical,error`）走 `logger.error` + 可选 `alert_webhook_url` POST（stdlib urllib 经 executor，失败记 notify_error），其余 `logger.warning`（§5.1/§10）。
- **#12 DLT 死信主题**：`consumers/base.py._handle` 对**数据类错误** `(TypeError,ValueError,KeyError,IndexError)` 路由到 `ingest.dlt`（`_route_dlt` 记录 original_topic/partition/offset/consumer_group/error_type/error_message(截1000)/原始消息/failed_at），吞断不再无限重试；`dlt_enabled`（默认 True）关闭则仅日志；**基础设施类错误仍上抛**走 common `{topic}.dlq`（§5.3）。
- **#13 cmd 超时扫描重发**：`commands` 新增 `last_issued_at`/`resend_count` 列；`repositories/commands.py` 增 `list_timed_out/mark_resent/set_ack_status`；`CmdService.scan_timeouts`（pending 超 `cmd_ack_timeout_s`=1800s：resend_count<1 重发一次并 mark_resent，≥1 置 `ack_status='timeout'` + 落一条 platform/error Alert），`run_timeout_loop` 每 `cmd_timeout_scan_interval_s` 巡检，main.py lifespan 启停（§9.4）。

**性能（#18/#19/#20/#21）**
- **#18 存储 IO 统一 executor 化**：`LakeStorage` 对外公开方法**全 async**（get/put/head/list/sample/check_bucket 等），同步 boto3 经 `_run`→`ThreadPoolExecutor(8)` 投递，worker 执行期间不再阻塞 FastAPI 事件循环（此前仅 health 一处用 executor）。
- **#19 QC 拆长事务**：`services/qc_worker.py.execute` 由「持 session 全程穿插网络 IO」拆成三段——Phase A 读元数据（bag manifest + 图/点云 artifacts）、Phase B 无 session 纯存储 IO + 计算（抽样解码/hash/黑帧冻结/标定冲突）、Phase C 新 session 写 qc_result + 回写状态/分数/corrupt，消除 idle-in-transaction（§7.4）。
- **#20 orchestrator 并发轮**：`_main_loop` 改 `_process_round`——锁事务内 `claim_next_batch`（`SKIP LOCKED` 批量领取至多 `orchestrator_max_parallel_tasks`=4）后提交释锁，重执行 `asyncio.gather` 并发；抽取 `_run_one`，保留 `_process_one_task` 兼容低并发/测试（§9）。
- **#21 消费背压**：新增 `consumers/backpressure.py.BackpressureGuard`（队列深度 TTL 缓存 `queue_depth`≥`task_queue_backpressure_threshold` 注入 `backpressure_throttle_s` 节流 + 令牌桶 `artifact_ingest_rate_limit`），`base.py` 增 `_before_process` 钩子、ArtifactConsumer 接入；`repositories/tasks.py.queue_depth` 修正为 `func.count()`（§5.4）。

**附带修复（未列入审查、真实运行时 bug）**：`routers/bags.py` 引用 `BagTopic` 不存在的列（`type/count/serialization_format/first_ts_ns/last_ts_ns`）→ 更正为 ORM 真实列 `msg_type/message_count/start_ns/end_ns/hz`（此前任何带 topics 的 bag 详情/列表请求必抛 AttributeError）。

向后兼容：① **DDL 变更**：新增 `agent_alerts` 表、`bags.progress`、`tasks.payload`、`commands.last_issued_at/resend_count`（均 `IF NOT EXISTS` 幂等，旧行取默认）；② **fail-closed**：`GATEWAY_HMAC_SECRET` 为空时认证与下行命令签发**由静默降级改为硬失败** ⇒ 部署必须配置该密钥（否则读接口 401、命令 500）；③ **LakeStorage 公开方法签名同步→异步**：仅服务内部调用方受影响（已全部 await），无跨服务/REST/Kafka 契约变化；④ **EXPORT 任务化**：`create_dataset` 不再由调用方手动推进 state，改后台任务驱动，轮询导出的读方需接受 `running` 中间态；⑤ **parsed 只读凭据**：新配置缺省回退主凭据，行为与升级前一致；⑥ **QC 权重表**：9 项权重重排，历史 `qc_score` 与新分数量纲变化（同 bag 重算会跳变，属预期）；⑦ DLQ/DLT：数据错误改投 `ingest.dlt`，需确保该 Topic 存在（`dlt_enabled=False` 可退回旧行为）。

风险：① **#14/#15 fail-closed** 放大配置失误面——密钥缺失即刻对外 401 / 命令签发失败，上线前须核对 ConfigMap/`.env` 已注入；② **#18/#19** executor(8) 与三段事务增加连接/线程周转，`db_pool_size` 需按并发复核；③ **#7 规则挖掘**阈值不当可能产噪声 clip（`rule_mining_enabled` 可关，阈值可调）；④ **#12 DLT** 仅吞数据类错误，编码/网络类仍上抛，非全量兜底；⑤ **#8 webdataset** 用 stdlib tarfile 索引式打包（真样本嵌入媒体、真 Lance 格式留 M5），**#10** 用 `bags.progress` 列降级替代 ClickHouse 历史；⑥ 本条全部改动为**本地单测验证**（无实机多副本/真实 MinIO），并发轮/背压/DLT/超时重发的实机行为待联调确认。

验证（本环境，均实跑）：`run_unit_tests.py` 全仓 **10 目标全绿**——common **326**、flink-jobs 94+1s、api-gateway 71、scene-service 108、data-collector 149、data-analytics 113、ota-service 128、remote-control 98、**ingest-platform 162**（净增 36：新建 `test_review_fixes.py` 33 例覆盖 #4 黑帧/冻结/标定冲突纯函数 + #6 分组/分类 + #7 规则挖掘 + #14/#15 fail-closed + #16 只读凭据隔离 + #17 GNSS 脱敏接线 + #21 背压节流；`test_orchestration.py` +2 例 EXPORT 注册与并发轮；`test_ddl_contract.py` +1 例 agent_alerts 逐列）；`test_workers.py` QC 权重断言同步 9 项。`ruff check` 对本次触及文件收口（清 F401/RET501/RUF100/I001/RUF022/PIE808；余 align_worker `PLC0206`、cmd_service 一处 `BLE001` 为基线既有风格，与仓内 parsers 同款裸 except 一致，未扩大改动面）。`test_ddl_contract.py` 钉住 DDL↔ORM：新增 agent_alerts 后 13 表清单/逐列/8 枚举全等。

分发路径注意：新增文件 `services/export_worker.py`、`app/repositories/alerts.py`、`app/consumers/backpressure.py`、`tests/test_review_fixes.py`；修改文件（ingest-platform）`config.py`、`models/ingest.py`、`core/dependencies.py`、`storage/minio_client.py`、`routers/bags.py`、`routers/health.py`、`repositories/{tasks,bags,commands,__init__}.py`、`services/{qc_worker,align_worker,slice_worker,dataset_service,orchestrator,cmd_service}.py`、`consumers/{base,consumers}.py`、`main.py`；契约 `contracts/database/ddl/06_ingest_platform.sql`。⚠ **全部在服务/契约层 ⇒ 必须重建 ingest-platform 镜像**（`--only-step 6`）；新增配置项（`minio_parsed_access_key/secret_key`、`dlt_topic`、`rule_*`、`qc_*`、`backpressure_*`、`orchestrator_max_parallel_tasks`、`cmd_ack_timeout_s`、`alert_*`）需进 compose `.env` 与 K8s ConfigMap；DDL 变更对已建库执行本条幂等 ALTER 段或重放 init SQL。

服务器实机步骤：

```bash
cd /opt/HunterCore
git add -u && git add services/ingest-platform/app/services/export_worker.py \
  services/ingest-platform/app/repositories/alerts.py \
  services/ingest-platform/app/consumers/backpressure.py \
  services/ingest-platform/tests/test_review_fixes.py     # 4 个新文件必须显式 add
# 建库侧：重放 06 DDL（含 agent_alerts 表 + 幂等 ALTER 列）
sudo docker exec -i hunter-postgres psql -U hunter -d huntercore < contracts/database/ddl/06_ingest_platform.sql
# 确认新配置已注入（fail-closed 前提）
grep -E 'GATEWAY_HMAC_SECRET|MINIO_PARSED_ACCESS_KEY|DLT_TOPIC' infra/deploy/.env
# 构建推送后：
sudo bash scripts/install.sh --only-step 6 --skip-docker -y
kubectl -n huntercore rollout status deploy/ingest-platform   # compose: docker compose up -d --build ingest-platform
# 验收：
# 1) 安全 fail-closed：临时清空密钥应见读接口 401 / 命令签发 RuntimeError
# 2) QC 9 项：完成一次 qc 后取回 qc_result.json 核对含 black_frozen_frames/calibration_conflict 项
sudo docker exec hunter-postgres psql -U hunter -d huntercore -c \
  "SELECT object_key FROM ingest_platform.derived_assets WHERE kind='qc';"
# 3) DLT：投递一条畸形消息，应落 ingest.dlt 且消费不无限重试
sudo docker exec hunter-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ingest.dlt --from-beginning --max-messages 5
# 4) EXPORT 任务化：POST 建数据集后 tasks 表出现 type=export，state 由 running→done
# 5) cmd 超时重发：发一条不下发 ack 的命令，超 30min 后 commands.resend_count=1、再超时→ack_status=timeout + agent_alerts(origin=platform,level=error) 落一行
```

**待办（本条未覆盖 / 明确降级项，不得当已修）**：① 真 webdataset 样本嵌入媒体、真 Lance 列式格式（当前 stdlib tarfile 索引 + parquet 快照，M5）；② ParseProgress 历史入 ClickHouse（当前 `bags.progress` 单值落库，M4/M5）；③ Alertmanager 正式对接（当前落库 + 可选通用 webhook，未接告警平台路由树，M4）；④ DLT replay 工具（§12.3，仅落料未做重放 CLI）；⑤ OpenTelemetry span（§12.2，当前仅 structlog trace_id，M4 可延后）；⑥ ENRICH worker（M4/M5）；⑦ `#16` bucket policy 侧只读收敛需运维在 MinIO/IAM 落实（代码仅提供独立凭据通道）；⑧ 双主降级、并发轮吞吐、背压实机阈值调优待联调环境验证。

## V1.20.3（修复/新增：ingest-platform 代码审查「功能遗漏汇总」剩余项收口——补齐 §12.3 DLT replay 工具（从 `ingest.dlt` 读回原始 envelope 并按 original_topic 重投，支持 topic/error_type/时间窗过滤 + 限量 + dry-run），并连带修复阻断 DLT/命令/平台事件真正生效的 `produce(dict)` 序列化缺陷（低层 produce 只接受 bytes/str，三处调用点曾直传 dict 被 confluent_kafka 拒绝且被 except 静默吞掉——致下行命令实际未下发、DLT 永不落料致 replay 无从取回、§7.5 加工事件从未广播））

Date：2026-10-13
变更类型：修复 + 新增（ingest-platform 服务代码 + 新增 CLI 工具 `app/tools/dlt_replay.py`。⚠ 全部在服务层 ⇒ 生效需重建 `ingest-platform` 镜像并滚动更新；无 DDL / 无 Kafka 契约结构变更；replay 工具为按需手动执行的运维 CLI，不随服务常驻）

背景（对同一审查报告 V1.20.2 末尾「待办 ④ DLT replay 工具」的兑现 + 该待办落地时的连带发现）：审查「五、功能遗漏汇总」逐项复核——背压/task_graph 依赖序/events 事件仓写入/服务端切片规则引擎/webdataset-lance 打包/ParseProgress 落库/Alert 路由/cmd 超时重发/GNSS 脱敏接入 9 项已在 V1.20.1/V1.20.2 落地；OpenTelemetry span（§12.2）报告原文即注「属 M4 可延后」、仿真出口（§8.3）/标注闭环（§8.4）/ENRICH 完整实现报告原文即注「里程碑内可接受、列入待办而非缺陷」⇒ 本条不动这些，仅在文末如实保留为待办。唯余 **DLT replay 工具（§12.3）** 为可即时兑现的缺陷级遗漏。落地时进一步发现：DLT/cmd/platform 事件三处 Kafka 生产实际处于「写了但没发出去」的静默失效态，若不修，replay 工具将永远面对空的 `ingest.dlt`——故一并收口该序列化缺陷（属正确性底线，且是本待办的直接前置）。

修复内容：

**新增（§12.3 DLT replay 工具）**
- 新增 `app/tools/dlt_replay.py`：`DltReplayEngine` 从 `settings.dlt_topic`（默认 `ingest.dlt`）读回 `_route_dlt` 落料的记录，提取其 `original_topic` + 原始 `message`（envelope），重新投递到原 Topic，让消息重走一遍正常消费链路（原事务已回滚、events 层1去重未落库 ⇒ 重放被当作新事件重新处理）。纯函数 `extract_replay_target`/`record_matches`/`replay_key`/`decode_record` 可单测；`ReplayFilter`（original_topic / error_type / since / until / max_records）+ `ReplayReport`（scanned/matched/replayed/skipped/failed/by_topic）。重放 key 取消息体 `vehicle_id`（契约单车辆有序），缺失回退原 offset。`_raw_repr` 形态（原始消息非 JSON 对象）不可作 envelope 重放，计入 skipped。
- CLI：`python -m app.tools.dlt_replay [--dry-run] [--topic T] [--error-type E] [--since/--until ISO] [--max N] [--group-id G] [--json]`；默认独立消费组 `g.ingest.dlt-replay`（不干扰在线消费者位点），退出码失败条数>0 时为 1。dry-run 仅统计不投递，用于回填前评估影响面。

**附带修复（正确性底线：`produce(dict)` 静默失效）**
- `consumers/base._route_dlt`、`services/cmd_service._publish_cmd`、`producers/platform_events._produce` 三处曾向低层 `KafkaProducerManager.produce(topic, value=<dict>)` 直传 dict——而 `produce` 仅接受 `bytes|str`（`_normalize` 对 str 编码、dict 原样透传），真实 confluent_kafka Producer 收到 dict 抛 `TypeError` 被各自 `except Exception` 吞掉。后果：① 下行命令（cleanup/reparse/seal）**从未真正发出**；② `ingest.dlt` **永不落料**（故 replay 工具无从取回，#12 形同未实现）；③ §7.5 加工事件（derived.ready/qc_failed/clip.created/dataset.ready）**从未广播**。三处统一改为 `json.dumps(..., ensure_ascii=False)`（与仓内 data-collector/remote-control 同款约定：低层 produce 前序列化）。

向后兼容：① **纯新增**：`app/tools/` 为按需 CLI，不进入服务常驻装配，无 API/Kafka/DDL 契约变化；② **序列化修复对消费方透明**：produce 的 value 由「本应成功但未成功」变为「真正以 JSON 字符串投递」，消息体字节形态与既有 JSON envelope 契约一致（此前生产失败根本没投递，故不存在“行为变更影响下游”——是把坏的改成好的）；③ replay 工具默认写回 `original_topic`，与原生产者同分区键（vehicle_id），不改变 Topic 归属；④ 需确保 `ingest.dlt` Topic 已存在（沿用 V1.20.2 部署前提：`auto.create.topics.enable=false`，缺失时 produce 走本地缓冲待补投）。

风险：① replay 会把曾失败的原始消息重新注入源 Topic 触发完整下游副作用——**先 `--dry-run` 评估，再用 `--topic`/`--error-type`/`--since` 收窄范围分批回填**，避免把同一根因尚未修净的坏消息成批重投放大故障；② 序列化修复使此前“静默不发”的命令/DLT/事件转为真实投递，上线初 `vehicle.agent.cmd`/`ingest.dlt`/`platform.*` 流量会从 0 跃升为实际值（属预期恢复）；③ 与仓内其它 Topic 一样，`ingest.dlt` 与 `g.ingest.*`/`g.ingest.dlt-replay` 消费组、`platform.*` 生产 Topic 仍未登记进 `contracts/kafka/{topics,consumer-groups}.yaml`（承接 V1.20.0「服务尚未纳入部署编排」里程碑债，见文末待办）。

验证（本环境，均实跑）：`run_unit_tests.py` 全仓 **10 目标全绿**——common 326、flink-jobs 94+1s、api-gateway 71、scene-service 108、data-collector 149、data-analytics 113、ota-service 128、remote-control 98、**ingest-platform 186**（净增 24：新建 `tests/test_dlt_replay.py`——4 组纯函数用例覆盖目标提取/过滤/重放 key/解码，`TestEngineRun` 6 例覆盖正常重放计数、dry-run 不投递、topic 过滤、max_records 限量、produce 失败计数、Kafka error 消息早退，`TestProduceSerializationFix` 3 例回归钉死 base._route_dlt/cmd_service._publish_cmd/platform_events._produce 三处 produce 收到的是可 `json.loads` 的 str 而非 dict）。`ruff check` 对新增 `app/tools/dlt_replay.py`+`tests/test_dlt_replay.py` 零告警（清 RUF022/I001/RUF100）；cmd_service/platform_events 各 1 处 `BLE001`、platform_events 1 处 `UP017` 为 V1.20.2 既有基线风格，未扩大改动面。

分发路径注意：新增文件 `services/ingest-platform/app/tools/__init__.py`、`services/ingest-platform/app/tools/dlt_replay.py`、`services/ingest-platform/tests/test_dlt_replay.py`；修改文件（均服务层）`consumers/base.py`、`services/cmd_service.py`、`producers/platform_events.py`。⚠ **必须重建 ingest-platform 镜像**（`--only-step 6`）；无新增配置项、无 DDL 变更。replay 工具在镜像内以 `python -m app.tools.dlt_replay` 调用（需注入与在线服务同源的 Kafka/`.env` 配置）。

服务器实机步骤：

```bash
cd /opt/HunterCore
git add -u && git add services/ingest-platform/app/tools/__init__.py \
  services/ingest-platform/app/tools/dlt_replay.py \
  services/ingest-platform/tests/test_dlt_replay.py     # 3 个新文件必须显式 add
# 构建推送后：
sudo bash scripts/install.sh --only-step 6 --skip-docker -y
kubectl -n huntercore rollout status deploy/ingest-platform   # compose: docker compose up -d --build ingest-platform
# 验收：
# 1) DLT 现在真正落料：投一条畸形消息，ingest.dlt 应可见（此前恒空）
sudo docker exec hunter-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ingest.dlt --from-beginning --max-messages 5
# 2) replay 先预览再回填
docker run --rm --env-file infra/deploy/.env <ingest-platform-image> \
  python -m app.tools.dlt_replay --dry-run --json
docker run --rm --env-file infra/deploy/.env <ingest-platform-image> \
  python -m app.tools.dlt_replay --topic vehicle.bag.manifest --max 100
# 3) 下行命令恢复：下发一条 cleanup，车端应实际收到（此前静默不发）；
#    commands 表出现 pending→经 ack/超时扫描推进（V1.20.2 #13 链路本轮才真正有流量）
```

**待办（本条未覆盖 / 明确降级项，不得当已修）**：① OpenTelemetry span（§12.2，审查原文即注「属 M4 可延后」，当前仍 structlog trace_id）；② 仿真出口（§8.3，M5）、标注闭环（§8.4，M5）、ENRICH 完整实现（M4/M5）——审查原文均注「里程碑内可接受、列入待办而非缺陷」；③ 真 webdataset/lance 格式、ParseProgress 入 ClickHouse、Alertmanager 正式对接、bucket policy 只读收敛、双主降级/并发轮/背压实机调优（沿用 V1.20.2 待办，本条未触及）；④ **ingest-platform Kafka 契约面登记债**：`ingest.dlt`、`vehicle.bag.*`/`vehicle.agent.*` 订阅 Topic、`platform.*` 生产 Topic、`g.ingest.*` 与 `g.ingest.dlt-replay` 消费组须补入 `contracts/kafka/{topics,consumer-groups}.yaml` 及 `create-topics.sh`/`kafka-init-job.yaml`，随「服务纳入部署编排」里程碑一并交付（本轮为纯服务层修复，未展开契约与编排）。

## V1.20.4（新增/接入：ingest-platform 纳入单机部署编排 + Kafka/对象存储契约多方同步 + api-gateway 统一前缀 `/api/v1/ingest` 路由接入；收口 V1.20.0–V1.20.3 逐版文末悬挂的“服务尚未纳入部署编排”里程碑债）

Date：2026-10-13
变更类型：新增 + 接入（编排与契约层：`infra/deploy/docker-compose.yml`、`scripts/{install,init-db,init-kafka,init-minio,health-check,common}.sh`、`infra/k8s/{services/api-gateway,ingress,jobs/minio-init-job}.yaml`、`contracts/{kafka,openapi,database}`、`services/api-gateway/app/{config,services/proxy_service,tests/test_openapi_contract}.py`、`services/ingest-platform/app/routers/*.py`（7 文件）+ 服务 README、镜像 pytest 契约用例、运维四件套 + deployment.md §14。**无服务业务逻辑改动**：除路由前缀外 ingest-platform/api-gateway 行为与测试条数不变）

背景：V1.20.0–V1.20.3 完成了 ingest-platform 的功能实现与三轮代码审查修复，但 release 每版文末均挂同一项里程碑债——“服务尚未纳入部署编排、Kafka/对象存储契约面未登记、对外 REST 未经网关”，实机验证此前卡在 `install.sh --only-step 6` 不构建 ingest 镜像、`docker compose up ingest-platform` 报 `no such service`。V1.20.4 专项收口：把 ingest-platform 作为**第 7 个业务服务**接入 compose 编排与初始化/巡检脚本、把其全部 Kafka/对象存储契约面登记进 `contracts/` 并过 `verify_data_layer.py` 门禁、把 7 个域 REST 经 api-gateway 统一前缀鉴权路由对外。

修复内容：

**A. 部署编排接入（compose + install/init 脚本 + 巡检）**
- `infra/deploy/docker-compose.yml` 新增 `ingest-platform` 服务段（`container_name: hunter-ingest-platform`、`API_PORT=8089`、绑 `127.0.0.1:${INGEST_PLATFORM_PORT:-8089}`、`/healthz` 容器内 urllib 探针、depends_on postgres/redis/kafka/minio healthy；运行镜像 `python:3.12-slim` 非 distroless，因 rosbags/pyarrow/numpy 需原生库）；api-gateway 段 env 新增 `INGEST_PLATFORM_SERVICE_URL`。
- `scripts/install.sh`（Step 3/11 服务清单纳入 ingest-platform）、`init-db.sh`（重放 `06_ingest_platform.sql` + `ingest_platform` schema）、`init-kafka.sh`（新增独立 `INGEST_TOPIC_SPECS`：13 个 ingest Topic + 为 7 个被订阅 Topic 派生 `{topic}.dlq`；**独立清单不纳入 check6「平台内部 Topic 四方同步」**，避免污染既有门禁）、`init-minio.sh`（新增永久桶 `hunter-derived`/`hunter-datasets`）、`health-check.sh`（`EXPECTED_CONTAINERS` +ingest、HTTP 探针 +`ingest-platform:8089:/healthz`、MinIO Bucket 下限 7→9、错误日志容器清单 +ingest）、`common.sh`（新增并 export `C_INGEST="hunter-ingest-platform"`）同步接入。
- K8s 初始化侧：`infra/k8s/jobs/minio-init-job.yaml` create_bucket + encrypt_bucket +2 桶（头部注释 7→9）；`infra/k8s/services/api-gateway.yaml` ConfigMap `GATEWAY_ROUTE_PREFIXES` +`/api/v1/ingest`、新增 `INGEST_PLATFORM_SERVICE_URL`；`infra/k8s/ingress.yaml` 新增 `/api/v1/ingest` → api-gateway:8080 path。⚠ **未交付 `infra/k8s/services/ingest-platform.yaml`（Deployment/Service 清单）**——与 vehicle-service 同状态，集群内落地需另行补该清单与 monitoring 抓取。

**B. 契约多方同步（`verify_data_layer.py` 34 项门禁全绿）**
- `contracts/kafka/topics.yaml` 登记 13 个 ingest Topic（`vehicle.bag.*`/`vehicle.agent.*` 订阅、`vehicle.agent.cmd` 下行、`platform.*.v1` 生产、`ingest.dlt` 死信）+ 7 个被订阅 Topic 的 `{topic}.dlq`；`consumer-groups.yaml` 登记 7 个 `g.ingest.*` 消费者组（订阅 Topic 均已登记、幂等键齐备）；`contracts/database/object-storage.yaml` 登记 `hunter-derived`/`hunter-datasets`（永久、读写方匹配）。ingest 注册为服务私有 schema + 服务→Repository 白名单行。
- 镜像 pytest 契约用例同步：`common/python/tests/test_kafka_contracts.py` 新增 `EXPECTED_INGEST_TOPICS`(13)、平台∪ingest 同步期望、消费者组命名放行 `g.ingest.*` 点分式；`test_storage_contracts.py` `EXPECTED_BUCKETS` +2 桶（7→9，docstring 计数同步）。

**C. api-gateway 路由接入（统一前缀 `/api/v1/ingest`，四方一致）**
- **形态决策**：后端原把 7 个 REST 直接挂 `/api/v1/<bags|clips|agents|artifacts|datasets|tasks|commands>`（裸资源名），不符合网关「一服务一域前缀」约定、也不在设计文档 3.4 不可变路由表内；经评审采用**统一前缀**方案——把 7 个域挂到 `/api/v1/ingest` 下，网关只加 1 条路由（而非枚举 7 前缀）。
- 后端：`services/ingest-platform/app/routers/{bags,clips,agents,artifacts,datasets,tasks,commands}.py` 的 `APIRouter(prefix=...)` 与模块/端点 docstring 全部改为 `/api/v1/ingest/<res>`（`/healthz` 等基线探针不变）；服务 README「REST 端点」章节由「服务内部直连、未入网关」改为「经 api-gateway `/api/v1/ingest` 接入」。
- 网关：`app/config.py` 新增 `ingest_platform_service_url`（默认 `http://localhost:8089`）；`proxy_service.build_route_table` 在 vehicle 之后、user 之前插入 `GatewayRoute(prefix="/api/v1/ingest", service_name="ingest-platform", ...)`（`strip_prefix=false`，转发完整路径，模块头「7 个前缀」→「8 个前缀」）；`user` 仍为唯一 `pending_confirmation`。
- 契约与镜像测试：`contracts/openapi/api-gateway.yaml#x-hunter-gateway-routes.routes` 新增 `/api/v1/ingest → ingest-platform:8089 auth=jwt`（置于 vehicle 后、user 前）；`services/api-gateway/app/tests/test_openapi_contract.py` 的 `EXPECTED_ROUTE_PREFIXES` 元组与端口表同步——钉死 OpenAPI 路由顺序 ↔ ConfigMap `GATEWAY_ROUTE_PREFIXES` ↔ Ingress paths ↔ `build_route_table` 四方一致。

向后兼容：① **ingest REST 路径变更是本版唯一的对外不兼容点**，但该服务此前从未纳入编排、从未对外经网关暴露 ⇒ 无既有客户端调用旧 `/api/v1/<res>` 路径（已核实无 pytest 钉死、前端未调用），改前缀不影响存量系统；② 网关新增前缀为纯叠加，`strip_prefix=false` 与既有 7 条一致，其余 6 服务路由/鉴权行为零变化；③ Kafka/对象存储契约登记为**增量**，`init-kafka.sh` 用独立 `INGEST_TOPIC_SPECS` 不动平台内部 Topic 门禁（check6 仍绿），已建集群重跑幂等；④ compose/k8s/巡检改动对未部署 ingest 的环境无副作用（health-check 对缺失容器降级 WARN）。

风险：① **ingest-platform 与 api-gateway 两个镜像均须重建**——ingest 改了 router 前缀、api-gateway 改了路由表；只升其一会出现「网关 404（后端仍挂旧路径）」或「后端 404（网关转发 `/api/v1/ingest/*` 而旧后端无此前缀）」；② K8s 侧 ConfigMap/Ingress 已含 `/api/v1/ingest`，但 `INGEST_PLATFORM_SERVICE_URL=http://ingest-platform:8089` 指向的 Service DNS 需随 `infra/k8s/services/ingest-platform.yaml` 交付后才解析，否则集群内经网关访问 ingest 会 503（compose 形态无此问题）；③ compose 内 ingest 依赖 minio healthy，接入后首次 `install.sh` 会实际构建含原生依赖的 ingest 镜像（构建耗时上升）；④ `GATEWAY_HMAC_SECRET` fail-closed 前提不变——经网关后仍需网关与后端共享同一密钥，否则转发后仍 401。

验证（本环境，均实跑）：`run_unit_tests.py` 全仓 **10 目标全绿**——common 326、flink-jobs 94+1s、**api-gateway 71**（`test_openapi_contract.py` 现含 8 前缀四方一致断言）、scene-service 108、data-collector 149、data-analytics 113、ota-service 128、remote-control 98、**ingest-platform 186**（router 前缀改造为行为保持，条数不变）。`scripts/verify_data_layer.py` **34 项全绿**（Kafka Topic 车端 9 + 平台 7 + ingest 13 含死信 14 项四方同步、消费者组 24、对象存储 9 桶 ↔ 2 脚本 ↔ 5 服务契约）。`scripts/verify_infra.py` **全部校验通过**（含 K8s 文档、路由/Ingress 一致性；并连带修复运维三文档 4 处裸 `kafka-console-consumer.sh` 缺认证参数告警——补 `$KC`/绝对路径或改走 `kafka_tool_cli` 封装）。`bash -n` 对 5 个改动 shell 脚本语法校验通过（剥 CRLF 后）。

分发路径注意：新增/修改文件——编排 `infra/deploy/docker-compose.yml`、`scripts/{install,init-db,init-kafka,init-minio,health-check,common}.sh`；K8s `infra/k8s/services/api-gateway.yaml`、`infra/k8s/ingress.yaml`、`infra/k8s/jobs/minio-init-job.yaml`（**未新增 ingest-platform.yaml，列为待办**）；契约 `contracts/kafka/{topics,consumer-groups}.yaml`、`contracts/database/object-storage.yaml`、`contracts/openapi/api-gateway.yaml`；代码 `services/api-gateway/app/{config.py,services/proxy_service.py,tests/test_openapi_contract.py}`、`services/ingest-platform/app/routers/*.py`（7 文件）、`services/ingest-platform/README.md`；镜像测试 `common/python/tests/{test_kafka_contracts,test_storage_contracts}.py`；文档 `docs/{01-部署概述与环境要求,deployment,运维操作手册,故障排查指南,ingest-platform部署验证清单}.md`、`release.md`。⚠ **必须重建并推送 `ingest-platform` 与 `api-gateway` 两个镜像**（`--only-step 6`）；无新增 DDL（06 已在 V1.20.2 交付）。

服务器实机步骤：

```bash
cd /opt/HunterCore
git add -u                                    # 本版以修改既有文件为主
# 1) 初始化脚本已随代码更新：重跑幂等初始化
sudo bash scripts/init-kafka.sh               # 建 13 ingest Topic + 7 {topic}.dlq（幂等）
sudo bash scripts/init-minio.sh               # 建 hunter-derived/hunter-datasets（幂等）
# 2) 重建两个镜像（ingest router 前缀 + api-gateway 路由表）
sudo bash scripts/install.sh --only-step 6 --skip-docker -y
# 3) compose 滚动更新（从仓库根 --project-directory .，compose 据此自动加载根目录 .env；
#    infra/deploy/ 下只有 .env.example、并无 .env，误用 --env-file infra/deploy/.env 会报 couldn't find env file）
cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build api-gateway ingest-platform
# 4) 验收（占位符必须换为真实值：<jwt> 为网关登录签发的 JWT、<SERVER_IP> 为宿主 IP；未鉴权会 401）
curl -s -H "Authorization: Bearer <jwt>" "http://<SERVER_IP>:8080/api/v1/ingest/bags" | head -c 400   # code=0
sudo bash scripts/health-check.sh | grep -E 'ingest|Bucket'   # ingest 容器/HTTP 探针/Bucket≥9
# 5) 契约门禁——属开发机/CI 门禁（需装好依赖：pip install -e common/python[dev]），**不在服务器裸跑**：
#    verify_infra.py 仅需 PyYAML（服务器系统 python3 可跑）；verify_data_layer.py 依赖 pydantic/sqlalchemy/asyncpg，
#    服务器裸 python3 会报 ModuleNotFoundError: No module named 'pydantic'（属预期，非服务故障）。
#    服务器侧运行时门禁已由 health-check.sh 承担；如需在服务器跑数据层校验，先建 venv 并 pip install -e common/python。
```

**待办（本条未覆盖 / 明确降级项，不得当已修）**：① **K8s 服务清单未交付**：`infra/k8s/services/ingest-platform.yaml`（Deployment/Service/HPA/PDB）与 monitoring 抓取配置未随本版补齐（与 vehicle-service 同样滞后，网关路由表/Ingress/ConfigMap 已先行含 `/api/v1/ingest`）；集群内落地前经网关访问 ingest 会 503。② OpenTelemetry span（§12.2）、仿真出口（§8.3）、标注闭环（§8.4）、ENRICH 完整实现（M4/M5）——沿用 V1.20.2/V1.20.3 待办。③ `ingest_platform.events` 高频流保留策略、真 webdataset/lance 格式、ParseProgress 入 ClickHouse、bucket policy 只读收敛、双主降级/并发轮/背压实机调优——沿用前版待办。④ 服务发现机制（Nacos/Consul vs K8s DNS）与熔断阈值仍为契约 `pending_confirmation`。

**实机执行回填（服务器 2026-10-13 实录，`/opt/HunterCore`）**：
- `sudo bash scripts/init-kafka.sh` ✅：13 个 ingest Topic（`vehicle.bag.*`×4 / `vehicle.agent.*`×4 / `platform.*.v1`×4 / `ingest.dlt`）+ 为 7 个被订阅 Topic 派建的 `{topic}.dlq` 全部实建并「校验通过」（分区/保留与契约一致，如 `vehicle.bag.artifact.uploaded`=6 分区、`vehicle.agent.alert`=2592000000ms、`ingest.dlt`=2592000000ms）；7 个平台内部 Topic 幂等跳过。
- `sudo bash scripts/init-minio.sh` ✅：9 桶就绪（含本版新增 `hunter-derived`/`hunter-datasets`，永久保留不设规则），生命周期规则幂等跳过，数量校验通过 9（≥9）。
- `sudo bash scripts/install.sh --only-step 6 --skip-docker -y` ✅：11 个业务镜像全部构建成功，含 `hunter/ingest-platform:0.1.0`（536MB，rosbags/pyarrow/numpy 原生依赖层使构建耗时上升，属风险③预期）与 api-gateway/scene/data-collector/data-analytics/ota/remote/vehicle/web-portal/flink。
- ⚠ **实机遇阱（本版已修）**：按上方「服务器实机步骤 3」的原命令 `docker compose -f infra/deploy/docker-compose.yml --env-file infra/deploy/.env up -d --build api-gateway ingest-platform` 报 `couldn't find env file: /opt/HunterCore/infra/deploy/.env`。根因：install.sh 将 `.env` 生成到**仓库根** `/opt/HunterCore/.env`，`infra/deploy/` 下只有 `.env.example` 模板、并无 `.env`。已将本节步骤 3 与 `docs/ingest-platform部署验证清单.md` §3.3 的滚动更新命令更正为 `cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build api-gateway ingest-platform`（compose 据此自动加载仓库根 `.env`，与 `common.sh` 的 `HC_COMPOSE_ARGS` 口径一致）。
- **尚待实机验证**：本轮 `install.sh` 止于 Step 6（Step 7–13 未跑），改用 `--project-directory .` 后的滚动更新命令本身、以及健康检查与经网关访问 `/api/v1/ingest/*` 的验收（清单 §4.16）尚未在本轮日志中出现，下一轮 `install.sh --step 7 -y` 或手动 `up -d` 后补回。

**实机执行回填·续（同机 Step 7–13 后、容器已起）**：
- ✅ 中间件/数据层就绪：`health-check.sh` 报 MinIO Bucket 9（契约 9）PASS；`python3 scripts/verify_infra.py` **全部校验通过**（含路由/Ingress/ConfigMap 四方一致）。
- ✔ **ingest-platform 崩溃重启环——已确诊并修复**：`docker logs` 完整栈定位到 `main.py:166 lifespan → KafkaProducerManager.initialize → LocalDiskBuffer.__init__ → self._root.mkdir(...) → PermissionError: [Errno 13] 'data/kafka-buffer/ingest-platform' → Application startup failed. Exiting.`。**根因**：业务容器以非 root uid 10001 运行，compose 将宿主 `${DATA_DIR}/kafka-buffer/ingest-platform` 绑成容器 `/app/data/kafka-buffer`，lifespan 内 LocalDiskBuffer 在其下 `mkdir ingest-platform`；本环境该宿主目录被 Docker 以 **root:root 自动预建**（`install.sh` Step 3 未先跑——本轮只跑过 `--only-step 6` 构建、Step 3 早于 V1.20.4 的 ingest 条目），uid 10001 无权建子目录 ⇒ lifespan 抛异常 ⇒ 崩溃重启环 ⇒ `/healthz` 不可达。**代码无需改**（`install.sh` 第 916 行 Step 3 已将 `ingest-platform` 列入 kafka-buffer 预建清单）——属执行顺序问题。**运维修复**（bind 源改属主即时生效、无需重建镜像）：`sudo bash scripts/install.sh --only-step 3 -y`（或 `sudo chown -R 10001:10001 /data/kafka-buffer/ingest-platform && sudo chmod 755 /data/kafka-buffer/ingest-platform`），再 `cd /opt/HunterCore && sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate ingest-platform`。**防复发**：health-check.sh 新增 `check_business_kafka_buffer`（纯宿主 stat 判 8 个业务服务 kafka-buffer 目录属主是否 10001，与 flink 检查点目录预检同规格；容器崩溃重启中不依赖 exec 亦可判定），把该坑从崩溃环反转变为可执行 FAIL。**实机验证通过（2026-10-14）**：`install.sh --only-step 3 -y` 输出 `[SUCCESS] 已设置属主：/data/kafka-buffer/ingest-platform → 10001:10001`（反证该目录此前确为 root）；`--force-recreate ingest-platform` 后 health-check 连报三 PASS（容器 running/healthy、/healthz 正常、业务 Kafka 缓冲目录 8 服务均属 uid=10001）⇒ 崩溃环已断、服务恢复。
- ⚠ **ingest-platform 启动崩溃环已修、但 orchestrator 仍有活跃 DB 错误（V1.20.4 遗留 → V1.20.5 已确诊并修复）**：`--since 2m` 实测**非空且每 ~2s 复现**——上一轮「极大概率残留」的推测**被推翻**，这是活跃错误。指纹：`message=orchestrator_loop_error` + SQLAlchemy/asyncpg 翻译栈（`translated_error from error`），SQL 为任务领取的 `UPDATE ... RETURNING ingest_platform.tasks.<全部列>`（含 `payload`）。⇒ `/healthz` PASS 只证存活探针+同步 lifespan 走通；后台 orchestrator 主循环被 try/except 兜住每 ~2s 重试报错 ⇒ **服务名义 healthy、实则不处理任务，不能算功能恢复**。候选根因（未确诊，需下取证）：代码核对 `contracts/database/ddl/06_ingest_platform.sql`：§6 建 tasks 已含 `payload JSONB`、时间列均 TIMESTAMPTZ（与 ORM / `repositories/tasks.py::claim_next_batch` 一致 ⇒ datetime 类型不符基本排除）；§15（328–332 行）另有幂等 `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS payload`。首要候选：部署库 tasks 表由 V1.20.2 之前的旧 06 建出（`CREATE TABLE IF NOT EXISTS` 对已存表不补列）、升级后未重跑 init-db（Step 8）/重放 06 ⇒ `payload` 列缺失 ⇒ `claim_next_batch` 的 `UPDATE ... RETURNING tasks.payload` 报 `asyncpg UndefinedColumnError: column ingest_platform.tasks.payload does not exist`。取证：`sudo docker logs --since 5m hunter-ingest-platform 2>&1 | tail -n 120` 看异常类名与 message（⚠ 勿用大小写敏感的 `grep 'error'`，会把 `XxxError`/`Exception` 关键行过滤掉），及 `sudo docker exec hunter-postgres psql -U hunter -d hunter_core -c "\d ingest_platform.tasks"` 核对列。**已确诊并修复（2026-10-14、见 V1.20.5）**：`\d` 显示 payload 列**存在**、重放 06 全 `already exists, skipping` 且错误照旧 ⇒ 上述「缺 payload 列」首要候选**被推翻**；宽口径 grep 取到真异常 `asyncpg UndefinedFunctionError: operator does not exist: ingest_platform.task_state = character varying`——根因为 06 DDL 用 PG 原生枚举列、ORM `StrEnumType(impl=Text)` 按 varchar 绑定比较。修复＝06 改 TEXT+CHECK + `DROP SCHEMA CASCADE` 重建（无需改代码/重建镜像），详见 V1.20.5。
- ✔ **经网关验收（路由已通、待真实 JWT 跑全链）**：实测 `curl .../api/v1/ingest/bags`（IP 已替为真实值、但 JWT 仍为占位 `<真实JWT>`）返回 `{"code":1001,"message":"未认证"}` 信封而非 404 ⇒ **`/api/v1/ingest` 路由已在网关注册并命中、鉴权中间件生效**（V1.20.4 路由接入验证）。端到端仍需带真实 JWT 重跑；若真实 JWT 仍 401 则查 `GATEWAY_HMAC_SECRET` 与网关/车端同值且非占位。（注：bags 为只读查询、与① 的 orchestrator 后台任务不同链路，故即便 orchestrator 报错，bags 仍可能正常返回。）
- ⚠ **`analytics:ingest_latency` 键缺失（独立问题，待排查、未修）**：`flink list -a` 实况——`driving_anomaly_detection`/`algorithm_performance_monitor` = RUNNING，但 **`data_quality_monitor_ingest_latency` = RESTARTING**（`submit-flink-jobs.sh` 因它处于运行态而幂等跳过）⇒ 作业已提交但反复重启、从未完成一次消费/提交 ⇒ 键不出数。与本版 ingest-platform 编排接入**正交**（属 data-analytics Flink 作业侧）。取证：`sudo docker logs hunter-flink-jm --since 2h` 取末条 `Caused by`（按已知的“RUNNING 不出数”四步查 SASL/Redis 凭据与检查点）。**实机回照**：上条 `grep 'switched from RUNNING to (FAILED|RESTARTING)|Caused by'` 取空（该式过窄，Flink 行多为 `XxxException`/`Caused by:` 不命中此字面）⇒ 换宽口径：`sudo docker logs hunter-flink-jm --since 2h 2>&1 | grep -iE 'ingest_latency|exception|caused by|error' | tail -n 60`（必要时加 TM 日志）。另判：仅 ingest_latency 一作业 RESTARTING、另两作业 RUNNING（共用 SASL/Kafka 凭据）⇒ **非全局凭据问题**，倾向该作业专属（其 Redis 指标写端/源 Topic `telemetry_raw`/消费组 `data-analytics-telemetry-raw` 逻辑）；仍待确诊。**已确诊（2026-10-14、见 V1.20.5）：非 SASL/Redis/代码而是容量侧槽位不足**——JM 日志决定性指纹 `NoResourceAvailableException: Could not acquire the minimum required resources.`（driving_anomaly/algorithm_performance 已占满槽位、本作业申请不到）；处置＝加 Flink TaskManager 槽位/资源 或 降低三作业并行度（入 V1.20.5 关联段），与本版代码修复无关。
- ✔ **已修（本轮）**：上方步骤 5「门禁」原写为服务器裸跑 `python scripts/verify_data_layer.py`——实为**开发机/CI 门禁**（CI `before_script` 先 `pip install -e common/python[dev]`），服务器裸 `python3` 无 pydantic 会 `ModuleNotFoundError: No module named 'pydantic'`（属预期，非服务故障）；已在步骤 4/5 注明占位符与依赖前提。

## V1.20.5（修复）

Date：2026-10-14
变更类型：修复

**现场**：ingest-platform 纳入编排、修复 kafka-buffer 崩溃环（见 V1.20.4 实机回填·续）后，`/healthz` 已 PASS，但后台 orchestrator 每 ~2s 抛 `orchestrator_loop_error`，服务名义 healthy、实则不领取任务。宽口径 grep 取到真异常：`asyncpg.exceptions.UndefinedFunctionError: operator does not exist: ingest_platform.task_state = character varying`。**⚠ 推翻上一轮首要候选**：`\d ingest_platform.tasks` 显示 `payload` 列**存在**、重放 06 全 `already exists, skipping` 且错误照旧 ⇒ 不是缺列，而是枚举类型不匹配。

**根因**：`contracts/database/ddl/06_ingest_platform.sql` 用 PG 原生 `CREATE TYPE ingest_platform.<x> AS ENUM(...)` 建枚举列，而 ORM `services/ingest-platform/app/models/ingest.py` 经共享库 `hunter_common.database.base.StrEnumType`（`impl = Text`，docstring 明写「DDL 为 TEXT + CHECK」）按 Text 映射。物理列是枚举类型、ORM 以 varchar 绑定比较参数（`WHERE state = $1::varchar`），PG 无 `=(task_state, varchar)` 运算符 ⇒ UndefinedFunctionError。全平台 01~05 schema 枚举列一律 `TEXT + CHECK (col IN (...))`（01_core/02_scene/03_ota/04_events/05_timeseries 均已核实），**唯 06 破坏平台约定**。SQLite 单测下原生枚举与 Text 皆退化为 TEXT ⇒ 187 条单测全绿也测不出，仅真实 PostgreSQL 复现（这也是为何前几轮静态核对一度误判为缺列）。

**修复内容**（改 DDL，**不改 ORM**——Text 才是平台正确形态；**不重建镜像**——06 是部署期 SQL 产物、非镜像内代码）：
1. `contracts/database/ddl/06_ingest_platform.sql`：删除 8 个 `CREATE TYPE ... AS ENUM` 块，8 个枚举列改 `TEXT NOT NULL [DEFAULT ...]` 并各加命名 CHECK——`ck_bags_state` / `ck_artifacts_artifact_class` / `ck_artifacts_upload_state` / `ck_derived_assets_kind` / `ck_tasks_task_type` / `ck_tasks_state` / `ck_clips_source` / `ck_commands_ack_status`；取值与 ORM StrEnum 逐字一致。受影响表：bags / artifacts / derived_assets / tasks / clips / commands。表头注释补充「枚举列表示约定」说明为何禁用原生枚举。
2. `services/ingest-platform/tests/test_ddl_contract.py`：`test_ingest_ddl_enum_values_match_orm` 由解析 `CREATE TYPE` 改为解析 `CHECK (col IN (...))`（按 `(table, column)` → StrEnum 映射）；**新增回归护栏 `test_ingest_ddl_no_native_enum_types`**——钉死 06 不得再出现 `AS ENUM`，防该坑复发。

**向后兼容 / 迁移**：06 全 `CREATE ... IF NOT EXISTS`，对**已按旧原生枚举建表**的库不会自动转换列类型 ⇒ 需一次性迁移。本环境为预生产、orchestrator 从未成功编排、`ingest_platform` 为服务私有独立 schema ⇒ 推荐 **`DROP SCHEMA ingest_platform CASCADE` + 用修正后 06 重建**（不涉其它 schema）。**不建议**原地 `ALTER COLUMN ... TYPE TEXT`：3 处 PK/UNIQUE（derived_assets.kind PK、clips.source UNIQUE、tasks uq_tasks_bag_type_rev）与 1 处 partial index（`idx_tasks_queue WHERE state IN (...)`）落在枚举列上，谓词/约束仍 cast 到待删枚举，原地转换脆弱易错。全新环境由 init-db.sh（Step 8）直接以修正 DDL 建表，天然正确。

**风险**：① 迁移清空 ingest_platform 既有投影（bags/events 等）；预生产可接受，上游 Kafka 在线时消费组重置 offset 或新数据流入即重建投影。② 运行时代码零改动，仅 DDL + 契约测试两文件；镜像不受影响，故本版不触发 `--only-step 6` 重建。③ CHECK 只约束新写入（drop+recreate 路径无历史脏值问题）。

**验证（开发机实跑）**：`services/ingest-platform` pytest **187 passed**（含改后 test_ddl_contract 16 条 + 新护栏）；`common/python/tests/test_database_models.py` **11 passed**（显式跳过 ingest 表、仅断言 06 文件存在，不受影响）；`scripts/verify_data_layer.py` **34 项全绿**（受控词表 CHECK↔StrEnum 只校验共享 11 处、不含 ingest；ingest 经 `SERVICE_PRIVATE_SCHEMAS` 豁免共享逐列对账）。

**分发路径注意**：修改文件 `contracts/database/ddl/06_ingest_platform.sql`、`services/ingest-platform/tests/test_ddl_contract.py`、`release.md`、`docs/{deployment,故障排查指南,运维操作手册,01-部署概述与环境要求}.md`。⚠ **无镜像重建需求**；须把修正后的 06 同步到服务器 `/opt/HunterCore` 后按下述命令重建 schema。

**服务器实机步骤（一次性迁移）**：

```bash
cd /opt/HunterCore
git pull                      # 或 scp 覆盖修正后的 contracts/database/ddl/06_ingest_platform.sql
grep -c 'AS ENUM' contracts/database/ddl/06_ingest_platform.sql   # 期望 0（分发已生效）
# 丢弃旧的（原生枚举列）服务私有 schema，用修正 DDL 重建；不动其它 schema、无需重建镜像
sudo docker exec -i hunter-postgres psql -U hunter -d hunter_core -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS ingest_platform CASCADE;"
sudo docker exec -i hunter-postgres psql -U hunter -d hunter_core -v ON_ERROR_STOP=1 < contracts/database/ddl/06_ingest_platform.sql
sudo docker exec hunter-postgres psql -U hunter -d hunter_core -c "\d ingest_platform.tasks" | grep -E 'state|task_type'   # 期望 text 且约束区含 ck_tasks_state/ck_tasks_task_type
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate ingest-platform
sleep 15 && sudo docker logs --since 2m hunter-ingest-platform 2>&1 | grep -cE 'orchestrator_loop_error|UndefinedFunctionError'   # 期望 0
```

**实机执行回填**：（待服务器回传迁移与复检输出后回填；本轮为开发机代码/契约测试落地并全绿）

**关联：`data_quality_monitor_ingest_latency` Flink 作业 RESTARTING（V1.20.4 遗留待办，本版确诊、属容量侧非代码，独立处理）**：JM 日志决定性指纹为 `org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException: Could not acquire the minimum required resources.`（每 ~60s/数秒 RUNNING↔RESTARTING 循环，非 SASL/Redis/代码）。**根因**：TaskManager 可用槽位不足——driving_anomaly/algorithm_performance 两作业 RUNNING 已占用槽位，第三作业申请不到其 minimum required ⇒ 反复重启、从未消费/提交 ⇒ `analytics:ingest_latency` 键不出数。**处置（容量/编排侧）**：增大 Flink 集群总槽位（上调 `taskmanager.numberOfTaskSlots` 或加挂 TM 副本/资源；本环境早先 health-check 已 WARN 内存 31GB<32GB，属同向约束），或降低三作业并行度使总需求 ≤ 可用槽；调整经 `infra/deploy/docker-compose.yml` flink TM 环境 / flink-conf 与作业并行度后重投 `submit-flink-jobs.sh`。TM 日志严格 grep 取空（JM 侧已足够定位 NoResource），无需再查 TM。属 V1.20.4 独立待办延续、未随本版代码修复。

## V1.21.0

Date：2026-10-14
变更类型：新增 / 改进 / 修复

背景：本版覆盖四项需求——① 运营看板重构（深色科技风 + 动态图表 + 单车抽屉钻取，仅看板页面）；② 远程操控数据通道打通（改用 SRS WHEP 拉流 + 内置车辆模拟器，端到端可演示可自测）；③ 车辆详情按 V1.20.0 新增 Topic（`ingest_platform_topics` 的 `vehicle.bag.*` / `vehicle.agent.*`）更新；④ 文档四件套与 README 同步。

### 需求 1：运营看板深色科技风重构（新增 / 改进）

- `frontend/src/views/DashboardView.vue` 重写为**三层信息架构**（作用域隔离：`.dash` 局部深色变量 + `margin:-16px` 全出血覆盖 layout 主区，**不改全局 Element Plus 浅色主题**）：顶部 KPI 发光带（8 张发光数字卡：车辆总数 / 在线率 / 遥测入库点 / 入库延迟 P95 / 感知帧率 / 感知时延 / 控制时延 / 未确认事件，每卡带迷你 sparkline）→ 中部动态图表（车辆状态环形图 渐变+中心总数、事件类型 Top10 横向渐变柱、Kafka 消费组积压含 `unknown_groups` **未判定组显式标注不当零**、入库延迟面积图带 SLO 1000ms 标线）→ 底部车辆明细表（状态灯/SOC 进度条/实时速度/最近在线，整行点击与「查看运营详情」→ 单车抽屉）。
- 新增组件：`frontend/src/components/charts/MiniSparkline.vue`（零依赖 SVG 迷你趋势线）、`MetricCard.vue`（发光数字卡）、`EchartBase.vue` 增量 `setOption`（`notMerge:false` 轮询平滑过渡）；趋势来自现有轮询快照在**客户端环形缓冲**（`SPARK_CAPACITY=24`，无新增后端 API）。
- 新增 `frontend/src/views/dashboard/VehicleDrilldownDrawer.vue`：`el-drawer` 右侧弹层，复用 `fetchDashboard({ time_range, vehicle_id })`（后端已支持 `vehicle_id`）+ `queryTelemetry`（近 N 点速度/转角/电量）+ `fetchControlEval`/`fetchPerceptionEval`，含实时遥测迷你图、该车事件列表、「进入操控台」跳转。
- 保持降级语义：各维度 `available/reason` 缺失时以占位卡显示，不臆造字段；`usePolling` 维持 5s 节奏。

### 需求 2：远程操控数据通道打通（修复 / 新增）

**根因（修复「信令连接异常，正在重试」/ 视频黑屏）**：原前端把 `recvonly` offer 经 signal WS 中继给车端，但平台无「车端→浏览器」answer 回程（`send_signal_relay` 注明 pending #10/#12）、`rc_srs_whep_url` 为空占位、且 dev `/ws` 代理指向不代理 WS 的 api-gateway ⇒ 信令/视频死链。改回契约原意的 SRS 媒体架构：车端 RTMP 推流 → SRS → 浏览器 WHEP 拉流。

- **2A 后端回填 SRS 媒体地址并下发**（`services/remote-control/`）：`app/config.py` 新增 `srs_stream_name` / `srs_whep_url_for` / `srs_whip_url_for` / `srs_rtmp_url_for`（基址经 env 注入、按会话 `{prefix}_{vehicle_id}_{session_id}` 拼装，基址空返回 None，业务代码不硬编码 URL）；`app/services/session_service.py::_build_webrtc` 用新 helper 写入会话响应 `webrtc.media_server`（signal/control WS 路径不动）；`infra/deploy/docker-compose.yml`（SRS 9090 由仅回环改绑 `SERVER_IP` 以便浏览器可达、remote-control env 补 `RC_SRS_APP`/`RC_SRS_STREAM_PREFIX`/`RC_SRS_WHIP_URL`，RTMP/WHEP 改 `SERVER_IP` 基址）、`.env.example`、`infra/k8s/services/remote-control.yaml`（ConfigMap 补 `RC_SRS_*`）同步。新增 `app/tests/test_config_srs.py` 5 例。
- **2B 前端改 WHEP 拉流**（`frontend/src/utils/webrtc.ts` 重写）：`WebRtcClient.start()` 读取 `session.webrtc.media_server.whep_url` → `RTCPeerConnection` 生成 offer → `POST`（`Content-Type: application/sdp`）→ 取回 answer 设远端描述（标准 WHEP 握手），`ontrack` 绑 video；移除对 signal WS 的强依赖；`whep_url` 缺失回退提示「媒体服务未就绪」不再无限重连；`stop()` 经 Link 头 DELETE 释放 WHEP resource；`collectStats`/ICE 保持不变。`frontend/src/types/remote.ts` 注释更新为 WHEP 架构。
- **2C 内置车辆模拟器**（`tools/vehicle-simulator/`，独立 pyproject + README）：以车端 Kafka 身份（SCRAM user=vehicle_id，SASL_SSL/mTLS）① 消费 `hunter.{vid}.remote_control` 驱动一维运动学（速度/转角/挡位积分 + >500ms 超时自动减速停车）并按 `command_result.schema.json` 回投回执（使「已回执」计数 / ack 帧 / 控制时延生效，pending #16 近似关联）；② ~10Hz 生产 `telemetry`；③ 生产 `health`（在线态 / 读模型）；④ 合成画面（速度/转角叠加文本动画帧）经 **ffmpeg RTMP** 推流到 `rtmp://{ip}:1935/remote_control/hunter_{vid}_{sid}`（无 ffmpeg 回退 `aiortc` WHIP），供浏览器 WHEP 播放。CLI `--vehicle-id/--bootstrap/--scram-pass/--srs-rtmp/--duration` 等；`scripts/run_local.sh` 一键示例；`tests/`（运动学 / schema 自校验 / 内存桩 Kafka 编排）18 例。
- **2D 开发代理修正**（`frontend/vite.config.ts`）：dev `/ws` 由 api-gateway:8080 改代理到 remote-control:8085（新增 `VITE_PROXY_WS_TARGET`），与生产 nginx 路由一致。

### 需求 3：车辆详情按 V1.20.0 新增 Topic 更新（改进）

- 后端 `services/vehicle-service/app/services/kafka_admin.py` 新增 `load_agent_topics()`（读 `topics.yaml#ingest_platform_topics` 的 `vehicle.` 前缀 8 项：`vehicle.bag.{manifest,task.status,parse.progress,artifact.uploaded}` + `vehicle.agent.{heartbeat,alert,cmd.ack,cmd}`）；`app/routers/vehicles.py::_agent_topics_preview` 在详情响应新增 `agent_topics_preview`（与既有 8 个 `hunter.{vid}.*` 分组，降级语义同 `topics_preview`）；`app/schemas/vehicle.py::VehicleDetail` 补字段；`contracts/openapi/vehicle-service.yaml` 补 `agent_topics_preview` 响应字段与 `x-hunter-kafka.data_collection_agent_topics` 清单。
- 前端「车端 Kafka 接入摘要」卡拆为两组：「实时控制/遥测 Topic（8）」+「数据采集 Agent Topic（V1.20.0 新增）」，更新计数与说明；`frontend/src/types/vehicle.ts` 补 `agent_topics_preview`。
- 契约测试：`services/vehicle-service/app/tests/test_vehicle_contract.py` 新增 `test_data_collection_agent_topics_consistent`（钉死 OpenAPI 声明 ↔ topics.yaml ↔ 路由预览三方一致，8 项）。

向后兼容：需求 1 仅改看板页面与新增组件，不改全局主题/路由/其它页面；需求 2A 会话响应 `webrtc.media_server` 为附加字段（`app/stream/whip_url/whep_url/rtmp_url`），signal/control WS 路径不变；`agent_topics_preview` 为可选字段，旧前端忽略不报错。需求 2B 是**前端行为变更**：视频改经 WHEP 拉流，须与 2A（后端下发 whep_url）配套——后端未配置 `RC_SRS_WHEP_URL` 时前端提示「媒体服务未就绪」而非无限重连。

风险：① SRS 9090 从仅回环改绑 `SERVER_IP` 暴露 WHEP HTTP 端点，生产须由防火墙/Ingress 收敛访问面；② 模拟器仅用于本地/staging 演示，**禁止**接入生产车队，且严格 mTLS 下必须提供客户端证书否则握手失败；③ 模拟器视频主路径依赖系统 `ffmpeg`，缺失且未装 `[whip]` 时自动降级为仅数据链路（不影响 telemetry/health/command_result 自测）。

验证（本环境，均实跑）：前端 `npm run build`（含 `vue-tsc` 类型检查）通过；`run_unit_tests.py services/remote-control` **103 passed**（含新增 `test_config_srs.py` 5 例）；`run_unit_tests.py services/vehicle-service` **46 passed**（含新增 agent Topic 契约用例）；`tools/vehicle-simulator` pytest **18 passed**；`ruff check` 对 **remote-control / vehicle-simulator All checks passed**；**vehicle-service 本版触及的 4 个文件**（`app/services/kafka_admin.py`/`app/routers/vehicles.py`/`app/schemas/vehicle.py`/`app/tests/test_vehicle_contract.py`）已 `ruff check --fix` + 手动消 SIM118（`ops.keys()`→`ops`）收窄至 **All checks passed**（并为新增的 `load_agent_topics` 补齐 `__all__`，均为行为中立改动）；但该服务仍存**与本版无关的存量 ruff 债**（未改动的 `app/core/error_handlers.py`/`app/main.py`/`app/routers/health.py`/`app/tests/test_kafka_admin.py` 已提交基线即报 I001/RUF100——属防御性 `# noqa` 与当前 ruff 0.16 默认规则集的仓内普遍冲突，未随本版清理以免跨文件无关抖动，建议单独立项治理）。修复后 `run_unit_tests.py services/vehicle-service` 仍 **46 passed**（lint 清理未改行为）。`scripts/verify_infra.py`、`scripts/verify_data_layer.py`（**34 项**）均全绿。端到端（compose + 模拟器）验收口径：信令不再报错、视频出画、遥测实时刷新、指令回执计数增长。

分发路径注意：需求 2A 须重建 `remote-control` 镜像并同步 compose/k8s `RC_SRS_*` 与 SRS 端口绑定；需求 2B/1 为前端产物（重建 `web-portal`）；需求 3 后端须重建 `vehicle-service`（router/schema/kafka_admin）+ 前端；新增文件 `tools/vehicle-simulator/**`。文档 `docs/{deployment,运维操作手册,故障排查指南,01-部署概述与环境要求}.md`、根 `README.md`、`release.md` 同步。

**服务器实机步骤（V1.21.0、单机 compose 主形态）**：本版**无 DDL / 无 Kafka Topic / 无 MinIO 变更**（需求 3 仅读取已存在的 `vehicle.*` 契约 Topic），**不需要**重跑 `init-db` / `init-kafka` / `init-minio`。受影响的镜像：`remote-control`、`vehicle-service`、`web-portal`；`srs` 无镜像构建、仅 9090 端口绑定由仅回环改绑 `SERVER_IP` 需重建容器。`.env` 在仓库根 `/opt/HunterCore/.env`（权限 600、仅 root 可读，脚本需 `sudo`）；本版 SRS 绑定与 `RC_SRS_*` 地址均由已有 `SERVER_IP` 派生，**无需新增环境变量**。⚠ 生产须由防火墙/安全组/Ingress 收敛 9090 访问面（含 SRS HTTP API），并确保 `1935/tcp`、`8000/udp` 已放行。

```bash
cd /opt/HunterCore
# 1) 同步代码
sudo git fetch --all && sudo git checkout V1.21.0        # 或 sudo git pull（按发布方式）
# 2) 配置校验（SERVER_IP 驱动、无需改 .env）：期望 whep=http://<SERVER_IP>:9090/rtc/v1/whep/、rtmp=rtmp://<SERVER_IP>:1935/remote_control
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . config | grep -E 'RC_SRS_(WHEP|WHIP|RTMP)_URL|9090'
# 3) 重建镜像 + 滚动（srs 无 build 段、--build 对它 no-op；端口配置变更会触发容器重建，remote-control depends_on srs healthy 自动保序）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build srs remote-control vehicle-service web-portal
# 4) 验收
sudo bash scripts/health-check.sh | grep -E 'srs|remote|vehicle|web'
curl -s http://<SERVER_IP>:9090/api/v1/streams | head                     # 从**外部机器**执行（非 127.0.0.1）：验 WHEP 入口对浏览器可达
sudo docker logs --tail=200 hunter-remote 2>&1 | grep -i media_server     # 会话含 media_server.whep_url 非空 ⇒ 2A 生效
# curl -s -H "Authorization: Bearer <jwt>" http://<SERVER_IP>:8080/api/v1/vehicle/<vehicle_id> | head -c 500   # 需求 3：返回体含 agent_topics_preview
```
前端浏览器验收：① 运营看板——深色科技风三层布局、四张动态图表、点行/「查看运营详情」弹单车抽屉；② 车辆详情——Kafka 摘要卡拆两组「实时控制/遥测 Topic（8）」+「数据采集 Agent Topic（8、V1.20.0 新增）」；③ 远程操控——不再出现「信令连接异常，正在重试」，有视频时经 WHEP 出画。

**可选·端到端自测（仅本地/staging/演示，禁止接入生产车队）**：模拟器以车端 Kafka 身份伪造遥测/健康/指令回执，需一个**已在 vehicle-service 开通、具 SCRAM 用户（user=vehicle_id）**的测试车，严格 mTLS 下还需客户端证书。在可访问 Kafka `9093` 与 SRS `1935` 的机器上：

```bash
python3.11 -m venv ~/veh-sim-venv && source ~/veh-sim-venv/bin/activate
pip install -e /opt/HunterCore/tools/vehicle-simulator            # 无 ffmpeg 需推流改 "[whip]" 回退 aiortc
cd /opt/HunterCore/tools/vehicle-simulator
python -m vehicle_simulator --vehicle-id veh-demo-001 \
  --bootstrap <SERVER_IP>:9093 --scram-user veh-demo-001 --scram-pass <该车SCRAM口令> \
  --srs-rtmp rtmp://<SERVER_IP>:1935/remote_control --duration 0
```
四项同时成立即视为链路打通：信令不报错 / 视频出画 / 遥测实时刷新 / 指令回执计数增长。

**K8s 形态（若生产走集群）**：`kubectl -n hunter-core apply -f build/k8s/services/remote-control.yaml`（ConfigMap 含 `RC_SRS_*`）+ `rollout restart deploy/remote-control`；SRS 需保证 9090 对浏览器可达（Ingress/NodePort/LB）；vehicle/web 镜像同法滚动。⚠ 仓库当前**未交付 `infra/k8s/services/vehicle-service.yaml`**（与 ingest-platform 同样滞后），集群内落地需求 3 后端需先补该清单。

**回滚**：`cd /opt/HunterCore && sudo git checkout <上一版本tag>` 后 `sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build srs remote-control vehicle-service web-portal`；回滚即恢复旧前端（signal WS 中继）与旧 SRS 仅回环绑定；因无数据层变更，回滚不涉 DB/Topic 迁移。

**实机执行回填**：（待服务器回传部署与验收输出后回填；本版为开发机代码/契约测试/文档落地并全绿，尚无服务器实录。重点回传项：`health-check.sh` 四容器行、外部 `curl :9090/api/v1/streams` 可达性、`hunter-remote` 日志中 `media_server.whep_url` 非空、浏览器看板/详情/远程操控三项验收、可选模拟器的端到端四项口径。）

## V1.21.1（修复）

Date：2026-10-14
变更类型：修复 / 改进

背景（V1.21.0 实机部署后操控台两症状确诊）：① 操控台「没有数据」——浏览器控制台 `GET /api/v1/data/telemetry` 422；② 视频「拉流失败 Failed to fetch」。车端契约侧本身已跑通（rc_probe 能收 boot/stop 帧并回 ack，探针不推 RTMP 故无流可拉属探针能力边界，非平台缺陷）。

### 修复 1：遥测查询 422（前端漏传契约必选参数）

- **根因**：契约（`contracts/openapi/data-collector.yaml`）与实现（`services/data-collector/app/routers/telemetry.py`）均要求 `vehicle_id` + `start_time` + `end_time` 必选，前端两处调用未传时间窗→FastAPI 校验 422（后端无需改动）。
- `frontend/src/views/remote/RemoteConsoleView.vue`：遥测轮询补最近 120s 窗口（新增 `TELEMETRY_LOOKBACK_S=120`）取最新 1 点；顺手移除无效 `order` 参数（后端固定 time DESC）。
- `frontend/src/views/dashboard/VehicleDrilldownDrawer.vue`：同类 422，补最近 1h 窗口取 60 点。

### 修复 2：WHEP 拉流 Failed to fetch（同源反代 `/rtc/`，用户选定方案）

- **根因**：V1.21.0 浏览器直连 `http://<SERVER_IP>:9090/rtc/v1/whep/` 属跨源：页面 CSP `connect-src 'self'` 不含 :9090 拦截 fetch，且 `Content-Type: application/sdp` 触发 CORS 预检，报 `Failed to fetch`（非 SRS/网络层错误）。
- `infra/deploy/config/nginx.conf`：启用并加固预留的 `/rtc/` location → `proxy_pass http://srs:9090`（hunter-net 内服务名可达）；**Host 透传 `$http_host`**（SRS answer Link 头 resource 据此生成，前端 stop 的 DELETE 仍同源回代）；关请求/响应缓冲、60s 超时、`Connection ""`。同源后 CSP `self` 天然覆盖、无 CORS，**无需改 CSP**。
- `infra/deploy/docker-compose.yml`：`RC_SRS_WHEP_URL`/`RC_SRS_WHIP_URL` 默认由 `:9090` 改同源 `http://${SERVER_IP}/rtc/v1/{whep,whip}/`；web-portal 补 `depends_on: srs (service_started)`（nginx 加载期静态解析上游主机名）；SRS 9090 映射保留现绑（注释改标「仅留作诊断，确认同源链路后可收回 127.0.0.1」，避免升级过渡期旧 env 用户断流）。
- `infra/deploy/.env.example` §7 同步同源默认值与说明。
- 前端 `webrtc.ts` 无需改动（WHEP resource 用 `new URL(link, whepUrl)` 解析，同源/相对均可）。

### 改进：车端部署指南增补真车摄像头推流参考

- `docs/车端远程操控部署指南.md`：新增 §4.5 「真车摄像头推流参考命令」（ffmpeg v4l2 与 GStreamer 两条管线，参数逐项对标契约 H.264/720p/≤30fps/2048-4096kbps/GOP 1s，boot 帧 `session_id` 拼流名，stop/500ms 看门狗杀流）；WHEP/WHIP 全部改为同源地址口径；端口表 9090 改标「仅诊断」。

文档同步（运维四件套，同源反代口径）：`docs/deployment.md` §15、`docs/故障排查指南.md` 场景 9（新增 `Failed to fetch` 与旧 env 残留两行）、`docs/01-部署概述与环境要求.md`（端口表/链路图/§3.3）、`docs/运维操作手册.md` §3.8（自检命令改同源探活：`curl -X POST http://127.0.0.1/rtc/v1/whep/` 预期 4xx 即反代通）。

风险：① nginx 静态 upstream 使 web-portal 启动依赖 srs 容器已启动（已补 depends_on；若 srs 不在 hunter-net 则 nginx 启动失败回滚需同步撤消 `/rtc/` 段）；② 旧服务器 `.env` 若已写死 `RC_SRS_WHEP_URL=http://<IP>:9090/...` 会覆盖 compose 新默认，必须改同源否则症状不消失（安装脚本渲染的 .env 属此情形）。

验证（开发机，均实跑）：前端 `npm run build`（含 `vue-tsc`）通过（修复 1 同批验证，✓ built 2345 modules）；compose YAML 解析 + 断言（web-portal.depends_on 含 srs、`RC_SRS_WHEP_URL` 同源默认）通过。nginx.conf 未在本机 `nginx -t`（Windows 无 nginx），由服务器重建容器时天然校验；同源反代端到端待实机。

**服务器实机步骤（V1.21.1）**：本版**无 DDL / 无 Kafka / 无镜像内代码变更于后端**；受影响镜像：`web-portal`（telemetry 修复 + 新 nginx.conf）与 `remote-control`（仅 env 默认值，若 `.env` 显式给值则只需重建容器）。

```bash
cd /opt/HunterCore
sudo git fetch --all && sudo git checkout V1.21.1        # 或按发布方式同步代码
# 1) 关键：.env 旧值改同源（否则 env 覆盖新默认，症状不消失）
sudo sed -i -E 's#^(RC_SRS_(WHEP|WHIP)_URL=http://)[0-9.]+:9090(/rtc.*)#\1\3#' .env
grep -E 'RC_SRS_(WHEP|WHIP)_URL' .env                    # 期望无 :9090
# 2) 重建（nginx.conf 为挂载卷，web-portal 含前端产物需 build）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build web-portal
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate remote-control
# 3) 验收
sudo docker exec hunter-web nginx -t                                      # 配置合法
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/sdp' --data 'x' http://127.0.0.1/rtc/v1/whep/   # 预期 4xx（反代通）而非 000/404
sudo docker logs --tail=50 hunter-remote 2>&1 | grep -i whep_url          # 会话下发地址为 http://<SERVER_IP>/rtc/…（无 :9090）
# 浏览器（硬刷 Ctrl+F5 取新前端）：操控台遥测不再 422；F12 看 POST /rtc/v1/whep/ 同源 201/200
```
出画验证（用户选定：直接上真车摄像头推流）：按《车端远程操控部署指南》§4.5 ffmpeg/GStreamer 命令在车端盒子推 `rtmp://<SERVER_IP>:1935/remote_control/hunter_HUNTER-001_{boot帧session_id}`，服务器上 `curl -s http://127.0.0.1:9090/api/v1/streams` 看到流名即推入，浏览器应出画。

待办（如实标注，未确诊/未复现）：① 截图中的 `WS control failed`：判定为探针会话快速 stop/结束后前端重连失败，修完 ②③ 后若仍现再立案；② 可选收紧：确认同源链路后将 compose 中 srs 9090 映射改回 `127.0.0.1`；③ rc_probe 探针不推 RTMP，如需无真车验证出画可临时用 ffmpeg 文件循环推流（指南 §4.5 改 `-re -stream_loop -1 -i 视频文件` 即可）。

---

## V1.21.2（修复）

Date：2026-10-14
变更类型：修复

背景（实机两问题，用户报障附截图）：① 运营看板「入库延迟」恒 `-`（`analytics:ingest_latency` 键不存在，巡检提示 data_quality_monitor 未产出）；② 操控记录页顶栏红色告警「对象存储列举失败」且列表共 0 条。两者根因均与看板文案猜测不同，已各自确诊。

### 修复 1：Flink TaskManager 槽位不足（看板入库延迟恒 `-`）

- **根因**：compose flink-tm `taskmanager.numberOfTaskSlots: ${FLINK_TASK_SLOTS:-2}` 只有 2 槽；契约「一组一作业」下三常驻作业（detection / algorithm_performance / ingest_latency，并行度默认 1、各占 1 槽）⇒ 第三个作业永远申请不到槽，`flink list -a` 状态为 `NoResourceAvailableException` RESTARTING 循环（非未提交、非回放、非全丢弃）⇒ Redis 键（TTL 300s）永不写出 ⇒ 看板 `-`。
- `infra/deploy/docker-compose.yml`：默认槽位 2→4（说明注释放 `FLINK_PROPERTIES` 块标量**外**的 YAML 注释位——块内内容会原样写进容器内 Flink 配置）。
- `infra/deploy/.env.example`：§8 `FLINK_TASK_SLOTS=4` + 注释；删除幽灵变量 `FLINK_PARALLELISM`（全仓无任何代码读取，易误导排障）。

### 修复 2：MinIO 服务账号从未创建（操控记录「对象存储列举失败」+ 列表空）

- **根因**：`.env` 的 `MINIO_ACCESS_KEY/MINIO_SECRET_KEY` 由 gen-passwords.sh 随机化（注释即明言「非 root 最小权限服务账号」），但通读 `scripts/init-minio.sh` 确认全流程**无任何步骤在 MinIO 侧创建该用户** ⇒ 共用 business-env 凭据的业务容器（remote-control 历史列举/录像 sidecar 写入、data-analytics、ota、scene、ingest）所有 S3 请求 403 `InvalidAccessKeyId`：`GET /api/v1/remote/history` → `repositories/storage.py list_sidecars` 抛 `ServiceUnavailableError("对象存储列举失败")`；会话结束 sidecar 写失败（尽力而为、置 `sidecar_written=false` 只记日志）⇒ 列表无数据。**不是**前端/查询条件问题。
- `scripts/init-minio.sh`：新增 `ensure_service_account()`（主流程步骤 5，原「已创建资源校验」改为步骤 6）：`mc admin user add` + `mc admin policy attach local readwrite`，幂等（已存在跳过）；凭据未配置时 WARN 跳过；与 ROOT 同名时跳过；失败打印手工重试命令并计入 failed；头注释/usage/校验清单同步。

文档同步（运维四件套）：`docs/故障排查指南.md` 场景 12（新增「服务账号不存在」成因行 + `storage_list_failed` 取证命令）与场景 13「第四层补充」（新增槽位不足 RESTARTING 判读块）；`docs/运维操作手册.md` §8.4（slots 默认 4 + 理由）与 §7.5（init-minio.sh 自动确保服务账号，手工单桶策略改列为生产收敛参考/兜底）；`docs/deployment.md` §13 末（V1.21.2 实机补录段）；`docs/01-部署概述与环境要求.md` env 表 `MINIO_ACCESS_KEY` 行（创建方式改指 init-minio.sh）。

风险：① `readwrite` 为 MinIO 内置**跨桶**策略，非单桶最小权限（收敛路径已写进手册 §7.5，列待办）；② `--force-recreate flink-tm` 会丢运行中作业，必须随后重跑 `submit-flink-jobs.sh`（幂等）；③ 存量 `.env` 若已显式写 `FLINK_TASK_SLOTS=2` 会覆盖新默认，需手改；④ 历史会话的 sidecar 当时已写失败，**修复后不会补数据**（列表需新会话结束后才出记录）。

验证（开发机，均实跑）：`bash -n scripts/init-minio.sh` 通过；compose YAML 临时脚本断言（`taskmanager.numberOfTaskSlots` 默认 4、`FLINK_PROPERTIES` 块标量内无 `#`、V1.21.1 既有断言不回退）通过；`verify_infra.py` 全绿。403 消除与 ingest_latency 出数属实机项，按下方验收。

**服务器实机步骤（V1.21.2）**：无 DDL / 无 Kafka 变更 / **无后端镜像重建**（修复全部落在初始化脚本与 compose env）。

```bash
cd /opt/HunterCore
sudo git fetch --all && sudo git checkout V1.21.2        # 或按发布方式同步代码（注意 init-minio.sh 执行权限）
# 1) 问题 2：补建 MinIO 服务账号（幂等，即时生效，无需重建业务容器）
sudo bash scripts/init-minio.sh                           # 期望尾部新增步骤输出「服务账号已就绪：<AK>」
sudo docker logs --tail=50 hunter-remote 2>&1 | grep -i 'InvalidAccessKeyId'   # 期望无新增
# 2) 问题 1：槽位 2→4
sudo grep -q '^FLINK_TASK_SLOTS=' .env && sudo sed -i 's/^FLINK_TASK_SLOTS=.*/FLINK_TASK_SLOTS=4/' .env || echo 'FLINK_TASK_SLOTS=4' | sudo tee -a .env
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate flink-tm
sudo bash scripts/submit-flink-jobs.sh
# 3) 验收
sudo docker exec hunter-flink-jm flink list -a            # 期望三作业均 RUNNING（非 RESTARTING）
sudo bash scripts/health-check.sh | grep 入库延迟指标键   # 期望 PASS P95=<n> ms 或 WARN 正在回放（有遥测流入即可出数）
# 看板「入库延迟」卡片应出数；操控记录页告警消失，新建一次远程会话结束后列表出现该条记录
```

另：若重跑 init-minio.sh 后仍 403，先核对 `.env` 的 `MINIO_ACCESS_KEY` 是否与 ROOT 同名（脚本此情况会跳过创建、属正常），并以 `sudo docker logs hunter-remote 2>&1 | grep -iE 'storage_list_failed|403'` 取实际错码再判。

**实机执行回填（2026-10-14 15:00，服务器实录）**：修复 1 验证通过——`flink list -a` 三作业均 RUNNING（此前 RESTARTING 循环），巡检 ⑯ `PASS P95=1240.1 ms（TTL=300s）`，看板「入库延迟」卡片出数。修复 2 验证通过——init-minio.sh 输出「服务账号已就绪：minioadmin（策略 readwrite）」（本机 `MINIO_ACCESS_KEY` 未被随机化、即 minioadmin，此前 MinIO 侧确无此用户），操控记录页「对象存储列举失败」告警消失。⚠ 列表仍共 0 条**属预期**（风险④坐实：历史会话 sidecar 当时已写失败不补数据，需修复后新建会话并**正常结束**（sidecar 仅在 end_session 路径写入，6h TTL 兜底删键不产出记录）后验证出条；验收命令：`docker logs hunter-remote | grep session_ended` 期望 `sidecar_written=true`）。另：当次 health-check 存在其它 FAIL 项（日志只 grep 了入库延迟行），未展开、另行归因（列待办）。

分发：常规发布（初始化脚本 + compose/env 模板 + 四件套文档）；K8s 形态的 MinIO 账号缺口同样存在，`minio-init-job.yaml` 侧补建列入待办（本次仅修 compose 形态，未验证项不顺手改）。

待办（如实标注）：① 服务账号单桶精细化策略（readwrite 为过渡态）；② K8s `jobs/minio-init-job.yaml` 同步补服务账号创建（本次未动，需测试集群验证）；③ 巡检可增「作业状态 RESTARTING」专项探测（现 ⑭ 仅报 `RUNNING<3`，分不清槽位/崩溃）；④ `MINIO_ACCESS_KEY` 与 ROOT 同名的存量环境如何提示收敛，下版评估；⑤ 实机 health-check 存在其它 FAIL 项（15:01 实录，已于 V1.21.3 归因：唯一 FAIL 为 `HTTP SRS`，属探针/绑定错位，非服务故障，见下条）；⑥ 修复后新会话的 `sidecar_written=true` 与操控记录出条待用户实测闭环。

---

## V1.21.3（修复 / 安全收紧）

Date：2026-10-14
变更类型：修复 / 安全改进

背景（实机全栈健康检查 45 PASS / 3 WARN / 1 FAIL）：唯一 FAIL 为 `[FAIL] HTTP SRS - http://127.0.0.1:9090/api/v1/versions 不可用`，但同一次巡检 `[PASS] 容器 hunter-srs - running / healthy`、`[PASS] HTTP web-portal`、且 V1.21.1 同源反代实测正常（`POST http://127.0.0.1/rtc/v1/whep/` 返回 405，非 000/404）——**服务本身健康**，属探针与端口绑定错位。

### 根因：SRS 9090 宿主绑定与 health-check 探针地址不一致

- health-check ② 的 `srs_health_url`（`scripts/common.sh` L641-645）探的是 `http://127.0.0.1:9090/api/v1/versions`；
- 而 compose srs 端口映射为 `${SERVER_IP}:${SRS_HTTP_API_HOST_PORT:-9090}:9090`（V1.21.0 为浏览器可达而绑 `SERVER_IP`，V1.21.1 信令改同源后仅“保留”未收回）。当 `SERVER_IP` 为对外网卡 IP 时，宿主机 `127.0.0.1:9090` 无监听 → 探针连不上 → `FAIL HTTP SRS`（容器 healthy 与视频链路均不受影响，因 nginx 经 hunter-net 直达 `srs:9090`）。

### 修复：收回 9090 到 127.0.0.1（同时落地 V1.21.1 待办②）

- `infra/deploy/docker-compose.yml`：srs 9090 映射 `${SERVER_IP}:…:9090` → `127.0.0.1:${SRS_HTTP_API_HOST_PORT:-9090}:9090`（1935/tcp 与 8000/udp 不动，仍对外）。同源反代实机验证已过，收回既除探针假 FAIL、又收紧访问面（与 V1.21.1 待办②合并落地）。
- `infra/deploy/.env.example`：§remote-control `SRS_HTTP_API_HOST_PORT` 注释与同源说明改口径（已收回 127.0.0.1）。
- 前端/车端无需改动（WHEP 走同源 `/rtc/`、RTMP 走 1935，均不依赖 9090 对外）。

文档同步（运维四件套）：`docs/故障排查指南.md`（端口前提行 + 场景 9 “9090 不可达”行改口径，并新增“存量环境未升级时探针假 FAIL”判读）；`docs/01-部署概述与环境要求.md`（srs 端口行 + 防火墙表 9090 行改 `127.0.0.1`）；`docs/deployment.md` §15（端口与暴露变更段改写为已收回 + 升级需 restart hunter-web）；`docs/运维操作手册.md` 无需改（口径本就是“9090 仅回环”，compose 这次追上了文档）。

风险：① 若浏览器/车端仍有旧 `.env` 直连 `http://<SERVER_IP>:9090/...`（未改同源），收回后会拉不到流——但那是 V1.21.1 已要求修正的旧现象（本版本前就应改同源）；② 宿主机上习惯用对外 IP 诊断 SRS API 的习惯需改为 `127.0.0.1:9090`（或 SSH 隧道）。

验证（开发机，实跑）：临时脚本解析 compose YAML 断言（srs 端口 = `127.0.0.1:${SRS_HTTP_API_HOST_PORT:-9090}:9090`、无 `SERVER_IP` 残留、1935/8000udp 不误动；V1.21.2/V1.21.1 既有断言不回退）全过（已删脚本）。服务器端到端（收回后健康检查转 PASS、同源拉流仍正常）待实机。

**服务器实机步骤（V1.21.3）**：无镜像重建（纯 compose 端口映射变更）。

```bash
cd /opt/HunterCore
sudo git fetch --all && sudo git checkout V1.21.3      # 同步 compose
# 端口映射变更会触发 srs 重建；srs 重建后容器 IP 变、nginx 静态 upstream 需重解析，故 restart hunter-web
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d srs
sudo docker restart hunter-web
# 验收
sudo bash scripts/health-check.sh | grep 'HTTP SRS'   # 期望 [PASS]（探针 127.0.0.1:9090 现在可达）
ss -ltnp | grep 9090                                    # 期望监听地址为 127.0.0.1:9090 而非 <SERVER_IP>:9090
# 浏览器硬刷：操控台 WHEP 拉流仍正常（同源 /rtc/，不依赖 9090 对外）
```

分发：常规发布（compose 端口映射 + env 模板 + 四件套文档）。

待办（如实标注）：① 本次仅修正“探针假 FAIL”，若服务器实际为 `SERVER_IP=127.0.0.1` 等特殊情形需按实机 `ss -ltnp | grep 9090` 再判；② K8s 形态 SRS 9090 经 Ingress/Service 暴露，本条不适用（K8s 同源反代另见 deployment.md §15）。

---

## V1.21.4（配置修正 + 文档补录，**无代码改动**）

Date：2026-10-14
变更类型：实机运维配置修正 + 排障文档补录（本仓库代码/编排/契约均未改动）

背景（实机报障“远程操控无法控制车辆，车辆在自动行驶”）：操控台 WS 恒 `reconnecting`、服务端“发送 0 · 回执 0”、WHEP `405 Not Allowed`。逐条取证后定位为**两处部署配置问题**（非平台代码缺陷），车辆自动行驶是 fail-safe 正确表现（WS 未连上 → 指令 0 送达 → 车端 >500ms 无指令 → 保持 AUTO）。

### 根因一（控不了车）：`CORS_ORIGINS` 不含浏览器实际访问源

- 运行中 `hunter-remote` 生效值 `CORS_ORIGINS=http://120.202.73.105`（公网 IP），而运维从局域网 `http://192.168.31.35` 访问（referer 佐证）；
- WS 直达 remote-control 的 `check_origin` 是应用层**显式拦截**（同源 REST 不触发浏览器 CORS 校验、`CORSMiddleware` 也不拦请求，故“看板正常唯独操控挂”不能排除此项）→ Origin 不在白名单 → 握手 accept 前被拒 → HTTP 403 → 浏览器 1006 → 无限重连；
- 日志铁证：`ws_handshake_auth_rejected code=1002 reason='Origin 不在白名单内（防跨站 WebSocket 劫持）'`。

### 根因二（视频 405）：部署渲染的 `config/nginx.conf` 落后仓库 + 单文件 bind 换内容需重建

- `POST /rtc/v1/whep/` 返回 405 且 access log `upstream_time` 为空 → 是 **nginx 静态 fallback 返的 405**（`/rtc/` 未匹配），非 SRS 拒绝；
- 容器挂载源是 **`${APP_DIR}/config/nginx.conf`（部署渲染副本，`/rtc/` 整段被注释、`Host $host`）**，而非仓库 `infra/deploy/config/nginx.conf`（已激活）——`git pull` 改的是后者，对运行容器无效；
- 单文件 bind mount 被 `cp` 替换为新 inode 后 `nginx -s reload` 不重解析，须 `--force-recreate web-portal`。

### 处置（均为服务器侧配置，无代码改动，已实机验证）

- `.env`：`CORS_ORIGINS` 追加浏览器实际访问源 → `http://120.202.73.105,http://192.168.31.35`；在 **APP_DIR 根目录**用生产编排 `up -d --force-recreate remote-control` 生效。
- `cp infra/deploy/config/nginx.conf config/nginx.conf`（含激活 `/rtc/`、`Host $http_host`）→ `nginx -t` → `up -d --force-recreate web-portal`。

### 关键操作纪律（本轮踩坑实录，已写入文档）

- **compose 操作必须在 APP_DIR 根目录执行**（`.env` 在根目录，不在 `infra/deploy/`）：`cd infra/deploy && docker compose up` 会因 project 目录错位读不到 `.env` → 变量全空 → `unable to get image 'hunter/remote-control:'`（VERSION 空致标签残缺）→ `up` 失败、配置根本没改到运行容器。

验证（实机，已闭环）：修 CORS 后 `hunter-remote` 不再报 Origin 拒绝，服务端“发送/回执”由 0 转为 **发送 280 · 回执 277 · 超时 0 · 8–32ms**（`commands_acked` 由 command_result 消费者按车端回发回执 HINCRBY，非平台自证 → 车端确已接收并回执控制指令）；WHEP 由 405 转 **201 且 `upstream_time` 有值**（真转发到 SRS）。

文档同步：`docs/故障排查指南.md` 场景 9 新增 **9.1（控制 WS `reconnecting` → CORS/取证/在 APP_DIR 根重建；含“已连上已回执但车不动 = 车端侧”判读 + 前端默认 N 挡提示）** 与 **9.2（WHEP 405 二义判据修正 + 渲染副本 nginx.conf 落后 + 单文件 bind force-recreate）**。其余三件套本轮无口径变化（未改代码/端口）。

待办（如实标注，**未确诊/非平台侧，严禁记为已修复**）：① **车端模拟器不执行运动**：目标速度稳定 0.4、挡位 D、指令已被车端接收并回执（超时 0），但“实时遥测→速度”恒 0.00、定位不变 → 属 `veh-sim` 侧（未切远程接管模式 / 运动模型未 apply target_velocity / 自动规划输出压制远程指令），不在本仓库范围，待车端排查；② **视频无媒体流**：WHEP 201 但 `0.0 fps / 0.00 Mbps` → 车端未向 SRS 发起 WHIP/RTMP 推流，亦属车端侧；③ 前端默认挡位 `N` 且“目标速度”滑块不改挡位，易致“拖滑块车不动”的误判，是否加“非 N 挡才允许非零速度”的前端联锁/提示，下版评估（本轮未改前端代码）。

---

## V1.21.5（estop 行为变更 + 会话/重连前端修复）

Date：2026-10-15
变更类型：修复 + 行为/契约变更（remote-control 路由 + 契约 openapi + 前端）

现场（V1.21.4 修复后实机复测报 4 项）：① 视频 WHEP `502 Bad Gateway`；② 控制面板空格紧急停车——敲击键盘空格导致操控会话退出；③ 退出会话后再次选车建立连接偶发失败，需刷新页面或等几十秒；④ 页面几十秒不操作登录失效、左侧菜单显示为空、但数据仍在刷新。

### 取证（来自 `hunter-web` 5 分钟 access log + `docker compose ps srs`，先证后改）

- **问题①（502）**：nginx `[error] upstream prematurely closed connection`，`upstream: http://172.28.0.6:9090/rtc/v1/whep/?app=remote_control&stream=hunter_HUNTER-001_...` 且 `upstream_time:"0.003"`（**非空**）——与 V1.21.4 的 405（`upstream_time` 空=静态 fallback）**不同**：路由已通、nginx 已连上 SRS，但 **SRS 提前关连**。`ps srs` 为 `Up 22 hours (healthy)`。结论：SRS 正常、请求的 WHEP 流 `hunter_HUNTER-001_*` 未在 SRS 发布→播流失败→502，属 **车端未推流（非平台代码）**，与 V1.21.4 待②同源。
- **问题④（失效）**：`192.168.31.20`（Referer `/vehicle/list`）对 `/analytics/dashboard`、`/remote/vehicles` **每 5s 全 401**（窗口内 120 次）；`192.168.122.23`（Referer `/remote/console`）出现 1 次 `POST /api/v1/user/refresh → 401`、随后其 `/remote/session/...` 轮询转 401；控制 WS `/ws/remote/.../control` 本身 `101`（Switching Protocols，req_time 372s）正常。根因：**单会话模型（同账号二次登录/另一窗口 refresh 覆写 `session:{user_id}`）+ `get_current_user` 要求 Redis 存 token 与请求 token 全等** → 先登录端每个请求 flip 401；与 30min TTL 无关。“菜单空但数据刷新”=会话失效后 `bindSessionExpiredListener` 仅 `reset()`不跳登录，组件定时器仍在轮询（均 401）。

### 修复内容（本轮代码改动）

- **问题②（estop 行为/契约变更）**：`services/remote-control/app/routers/websocket.py` 的 `_handle_estop` 由“零速帧 → `end_session` → status(ended) → `close(1000)`”改为“仅下发零速 + `brake=1.0` 帧 → 回推 status(**active**) → **不结束会话、不关闭**”（返回 `False` 保持帧循环）。紧急停车不再退出会话，松开后可继续操控。同步修订：`contracts/openapi/remote-control.yaml` estop 帧 `note`（V1.21.5 行为变更）、前端 `RemoteConsoleView.vue` 注释与提示文案、`types/remote.ts` 帧注释、WS 单测。会话结束仍由“主动结束/心跳超时/管理员操作”触发（`end_convergence` 先安全后清理顺序不变）。
- **问题③（前端可修）**：`RemoteConsoleView.vue` 的 `cleanupSession` 结束时新增 `await loadVehicles()`，重拉车辆可控状态，避免列表停留在旧的 `already_controlled` 快照（“需刷新页面”）。后端 60s SessionReaper 兜底属设计，**不改**。
- **问题④（前端可修）**：`main.ts` 注册 `SESSION_EXPIRED_EVENT` 监听→`router.replace({name:'login'})`（store 不 `import router` 以免循环依赖），修复“失效后停原页、菜单空但组件仍轮询”。单会话互踢属契约设计，本轮**不改鉴权后端**。

向后兼容：estop 不再结束会话 = **交互/契约行为变更**（需前端与运维知悉）；WS 关闭码 1000 保留但 estop 不再触发。

风险：低。后端变更集中在 `_handle_estop`；前端为 `cleanupSession` 一处增量调用 + `main.ts` 一个事件监听。

验证（本地，已闭环）：`services/remote-control` `test_websocket.py` **26 passed**（含改写后的 `test_ws_estop_stops_without_ending_session`：断言 estop 后 `status≠ended`、控制通道仍可收发心跳、`rc:session` 未被删）；`ruff check` **All checks passed**；前端 `vue-tsc --noEmit` **通过**。实机回归待部署回填。

实机执行（发布后）：

```bash
cd /opt/HunterCore
sudo git fetch --all && sudo git checkout V1.21.5
# 后端：estop 行为变更需重建 remote-control（在 APP_DIR 根用生产编排）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build remote-control
# 前端：问题③④修复需重建 web-portal 镜像（dist 入镜像，非单文件 bind）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build web-portal
# 浏览器硬刷验收：空格 estop 只停车不退会话；退出后车辆列表自动刷新；失效自动跳登录页
```

分发：代码变更（remote-control `routers/websocket.py` + 契约 `openapi/remote-control.yaml` + 前端 `RemoteConsoleView.vue`/`main.ts`/`types/remote.ts` + `app/tests/test_websocket.py`）+ 四件套文档同步。

待办（如实标注）：① **视频 502 = 车端未推流**（SRS healthy、nginx 已连上但 SRS 提前关连、目标流未发布），属车端侧，非本仓库修复；【**V1.21.6 修正**：该结论未经充分取证——定案必需的 `127.0.0.1:9090/api/v1/streams` 当时并未成功执行（探的是 `:1985` 与 `:8080`，两者都不是 SRS）；实机存证显示同现象还有两类候选根因（“宿主机第二个 SRS 实例”与“candidate 不可达”，**本例尚待现场定案**），502 已拆为三类分流（排障指南 9.3/9.7/9.8）】；② “几十秒失效”根因为**单会话互踢**（同账号多窗口/设备），属契约设计；若需支持并发多会话须改造 `session:{user_id}` 为多会话结构（重大改动，本轮未做），建议运维避免同账号多窗口并行；③ access log 现 `/api/v1/data/telemetry` 间歇 `429`（1Hz 遥测轮询），是否调前端采样频率与服务端限流口径核对，下版评估；④ 本轮 estop 行为变更与问题③④前端修复需实机回归确认。

---

## V1.21.6（视频链路可判度：SRS 单实例硬闸 + candidate 幂等对齐 + ⑰ 视频链路巡检）

Date：2026-10-15
变更类型：修复（部署脚本/配置渲染）+ 新增巡检项 + 车端模拟器诊断可见性 + 文档口径修正（**无业务服务代码改动**）

现场（V1.21.5 上线后实机再报）：车辆可远程接管与操控（指令回执 19–29ms、控制链路完好），但**一直没有任何视频**；前端提示 `视频拉流失败: WHEP 502 Bad Gateway`，看板 `0.00 Mbps / 0.0 fps`、连接状态 `closed → connecting` 循环。

### 取证与归因修正（先证后改）

- 车端侧存证：`ESTAB 192.168.122.90:49120 → 192.168.31.35:1935`、GStreamer 9.7% CPU（在编码）、stdin pipe 就绪——**推流端确实建立了连接**；
- 服务器侧存证：宿主机 root 进程 `./objs/srs -c conf/srs.conf`（PID 2947985，10月14 起）与 `hunter-srs` 容器**并存**；`ss -tlnp` 只见 `0.0.0.0:1935`/`0.0.0.0:8080`（9090 未对外监听是 V1.21.3 的设意，已改绑 `127.0.0.1`）；`docker port hunter-srs` 显示 `1935/tcp -> 0.0.0.0:1935`；容器内 `srs.conf` 为 `candidate 120.202.73.105;`（公网 IP，而浏览器在 `192.168.31.x`）；
- **本轮未被定案的部分（不得记为已修复）**：运维的“两个实例把推/拉打分家”结论仍缺直接证据——定案必需的 `curl -s http://127.0.0.1:9090/api/v1/streams` **一次也没成功执行过**（探的是 `:1985`【本项目不存在的端口】与 `:8080`【实为 api-gateway，返 `{"code":3001,"message":"资源不存在"}`】）；且若宿主机真占了 1935，容器发布 1935 应失败而非正常 recreate。“1935 的真实监听者”需现场 `sudo ss -ltnp 'sport = :1935'` 定案；
- 可证的仓库侧缺陷（本轮已修）见下“修复内容”。其中 **candidate 与双实例各自单独就能造成“操控正常、永远没画面”**，与 V1.21.5 的“车端未推流”结论同现象不同根因。

### 修复内容

1. **`ensure_srs_conf` 的 candidate 永不跟随 bug**（`scripts/install.sh`）：旧实现仅当 `srs.conf` 里还留着 `$SERVER_IP` 占位符时才替换——**替换过一次就永不跟随** `SERVER_IP` 变更，公网 IP 因此永久残留在生产配置里。现改为**幂等对齐**：占位符/旧 IP/人工改值一律重写为期望值；残留 `$SERVER_IP` 直接 FAIL；发生变更时提示需 `--force-recreate`。期望值 = `SRS_CANDIDATE_IP` 回退 `SERVER_IP`（Step 4 新增读取）。
2. **新增 `.env` 变量 `SRS_CANDIDATE_IP`**（`infra/deploy/.env.example` §7）：将“浏览器可达地址”与 `SERVER_IP`（Kafka advertised.listeners / 同源基址）解耦，双网卡/NAT 环境必需；`infra/deploy/config/srs.conf` 同步补语义注释（candidate 是写进 answer SDP 的**自己媒体地址**，不是“服务器自己的地址”）。
3. **部署预检新增“SRS 实例唯一”硬闸**：`scripts/common.sh` 新增 `hc_foreign_srs_pids`（扫 `/proc/<pid>/cmdline` 匹配 `objs/srs`/`srs -c`，按 `/proc/<pid>/cgroup` 排除容器内那个）；`install.sh` 新增 `check_single_srs_instance` 并接入 `preflight`（在 `check_public_ports` 之后，FAIL 附 PID 与 kill 命令）。
4. **健康检查新增 ⑰ 视频链路**（`scripts/health-check.sh`，17→18 类，已接入 `run_all_health_checks`）：五个子判据把四类成因分开指名——a) 实例唯一；b) 容器内生效 candidate vs 期望值；c) 同源 `/rtc/`（一次 `curl -w '\n%{http_code}'` 同时取 body+状态码：502/504→反代断裂、4xx 且 body 含 `<html`→nginx 静态 fallback、其余 4xx→反代完好）；d) `srs_api_url /api/v1/streams` 活跃流（无流 WARN 并明写“TCP ESTAB 不等于流已发布”）；e) `RC_SRS_WHEP_URL` 必须同源。配套新增 `common.sh::srs_api_url`（默认 `127.0.0.1:9090`，尊重 `SRS_HTTP_API_HOST_PORT`），并将现有 `srs_health_url` 改为委派它（消除 ② 与 ⑰ 两处各自算端口的漂移）。
5. **车端侧口径与推流失败可见性**：`tools/vehicle-simulator` 的 README/`config.py`/CLI help 把 `--srs-whip` 示例从 **1985 改回 9090**（1985 是 SRS 默认端口，本项目统一 9090，按旧文档填必连不上）并标注“基址必须指向与浏览器拉流**同一个** SRS 实例”；`video.py` 不再把 ffmpeg `stderr` 丢进 `DEVNULL`，新增 `_drain_stderr()` 线程接管并在退出时随 `video_pump_exit` 上报 `returncode`（推流失败从“静默无画”变为“可归因”）；`tests/test_messages_schema.py` 的 fixture 同步改 9090。
6. **定案口径修正（文档 + 脚本提示语）**：“`ss -ltnp` 的监听者是不是 docker-proxy”**不能单独定案**——Docker 以 **iptables DNAT** 发布端口（`userland-proxy=false`）时，宿主机那份 srs 仍会显示在 `0.0.0.0:1935` LISTEN，而外部流量已被 DNAT 送进容器（两者不冲突）。因此 `install.sh` 预检、`health-check.sh` ⑰-a 与四件套文档均改为：**定案以容器自身 API 的流列表（`/api/v1/streams`）为主、`ss` 只作旁证**，并明写“只凭 `ss` 就 `kill` 可能误杀正在服务的实例”（直接针对本例“kill 2947985”的建议）。

向后兼容：`SRS_CANDIDATE_IP` 留空即旧行为（跟 `SERVER_IP`），存量 `.env` 无需改文件即可部署；但**存量机器上已烘进 `config/srs.conf` 的旧 candidate 会在下次 `install.sh --only-step 4` 时被重写**（预期行为，需随后 `--force-recreate srs`）。`ensure_srs_conf` 末尾新增可选参数 `candidate_ip`（不传则同 `server_ip`），既有调用方无需修改。

风险：低。改动集中在部署/巡检脚本、模拟器与文档；不动业务服务、不动契约与编排端口。唯一行为变更是 candidate 从“一次性替换”变“幂等对齐”（为免公网 IP 残留），因此多网卡环境必须显式设 `SRS_CANDIDATE_IP`。

验证（本地，已闭环）：`bash -n` 对 `common.sh`/`install.sh`/`health-check.sh`/`daily-check.sh` **全 OK**；⑰-c 的 `/rtc/` 分流用本地假上游实测：POST 到代理→400+JSON 判 PASS、POST 未匹配→405+HTML 判 FAIL，并补了反例说明“为何必须用 POST 而不是 GET”（GET 时静态 fallback 返 200+HTML，旧写法会假 PASS，本轮已改）；`hc_foreign_srs_pids` 实测**抓到** cmdline 含 `objs/srs` 的测试进程且**不误报**无关进程；candidate 三态（占位符/旧 IP/手改值）重写正确；`srs_health_url` 委派后输出仍为 `http://127.0.0.1:9090/api/v1/versions`，且覆盖时的 `SRS_HTTP_API_HOST_PORT=19090` 两函数同步跟随；`ruff check tools/vehicle-simulator` **All checks passed**；`pytest tools/vehicle-simulator/tests` **18 passed**。实机回归待部署回填。

实机执行（发布后，**先定案再动手**）：

```bash
sudo -i; cd /opt/HunterCore
# ① 定案（三条，缺一不能结论）
sudo ss -ltnp 'sport = :1935'                        # 旁证：应为 docker-proxy（DNAT 发布时仍可能是宿主机那份，不能单独定案）
curl -s http://127.0.0.1:9090/api/v1/streams | head  # 提供 WHEP 的实例身上有没有流（不是 1985/8080）
docker exec hunter-srs cat /usr/local/srs/conf/srs.conf | grep candidate
# ② 双实例已坐实则：先弄清它由什么拉起（systemd/脚本/tmux），停用后再 kill（否则重启又回来）
kill <宿主机 srs PID>
# ③ candidate 改为浏览器实际访问的地址（本机为局域网入口时）
grep -q '^SRS_CANDIDATE_IP=' .env || echo 'SRS_CANDIDATE_IP=192.168.31.35' >> .env
sed -i 's#^SRS_CANDIDATE_IP=.*#SRS_CANDIDATE_IP=192.168.31.35#' .env
bash scripts/install.sh --only-step 4 -y
docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate srs
# ④ 收尾：车端结束会话→重新开操控会话（旧 RTMP 连接不会自己改指向新实例），再跑一次巡检确认 ⑰ 全绿
bash scripts/health-check.sh
```

分发：`scripts/{common.sh,install.sh,health-check.sh}` + `infra/deploy/{.env.example,config/srs.conf}` + `tools/vehicle-simulator/{README.md,vehicle_simulator/config.py,vehicle_simulator/video.py,tests/test_messages_schema.py}` + 四件套文档（故障排查指南新增 **9.7/9.8** 并改写 **9.3**、场景 9 步骤重编与原因表扩充、§0 加“视频类故障先看这两条”；运维手册 §3.8 新增“五、视频链路五层自检”与 9090/1985/8080 口径；deployment.md §15 新增变量与两条硬约束；01-部署概述新增 `SRS_CANDIDATE_IP` 与 SRS 单实例约束）。

待办（如实标注，**未确诊/待实机定案，严禁记为已修复**）：① **1935 的真实归属与 Docker SRS 是否持有该流尚未定案**（需上面 ①），“双实例”仅为候选根因；② candidate 改为局域网 IP 后 ICE 是否真能出画尚未实机验证（若浏览器与服务器不在同一层网络，可解方案是补 TURN 而非改 candidate）；③ **平台侧缺口**：`services/remote-control` 无任何 SRS HTTP API 客户端，建会话时不校验目标流是否已发布，“无流”只能在前端 `0 fps`/502 时才暴露——是否新增“会话创建时探流存在并在前端提示‘车端流未就绪’”，下版评估（属产品行为变更，本轮未动）；④ `hc_foreign_srs_pids` 靠 cmdline 特征识别，改名运行的第二实例会漏；且“宿主机有第二个 srs 进程”并不等于“流量归它”——Docker 以 **iptables DNAT** 发布 1935（`userland-proxy=false`）时宿主机那份仍会显示在 LISTEN 却拿不到外部流量，因此 **`ss` 只作旁证、定案以容器自身 API 的流列表（⑰-d）为准**（预检/巡检的提示语已按此口径写）；⑤ 502 究竟来自 nginx 还是 SRS 提前关连，本轮无 access log 到手，仍需 `sudo docker logs --since 5m hunter-web` 一次取证。

> **【V1.21.7 实机定案】本条目以下结论已被后续版本推翻或修正，阅时须叠加阅读 V1.21.7**：
> - **“实机执行 ① 定案（三条）”中的第二条 `ps -ef | grep '[o]bjs/srs'` 本身是错的判据**（曾写进本条目、排障指南与 deployment.md）：Docker 共享宿主 PID 视图，容器主进程必然出现在宿主 `ps` 里。实机复验（同一台 192.168.31.35）：`ss -ltnp 'sport = :1935'` 归 `dockerd`、⑰-a PASS（已按 cgroup 排除），故那个被当作“第二实例”的 PID 2947985 **就是 `hunter-srs` 容器自己** ⇒ **待办① 的双实例假设正式证伪**，不存“推到 A / 拉到 B”。
> - **502 的真实根因是 nginx 静态 `proxy_pass http://srs:9090`**（运维为了改 candidate 执行 `--force-recreate srs` 后 srs 换容器 IP，而 hunter-web 仍在连旧 IP）；已于 V1.21.7 根治（`/rtc/` 改运行期解析），见排障指南 **9.9**。
> - **本条目新增的 ⑰-e 自身有个假 FAIL**：它只读 `.env`，而 compose 写的是 `${RC_SRS_WHEP_URL:-http://${SERVER_IP}/rtc/v1/whep/}`（默认值已生效、`.env` 里不出现这一行）——实机截图浏览器正在发同源 WHEP 却被报“未配”。V1.21.7 已改为优先读容器生效值。**与 V1.21.3 修的“② SRS 健康探针与绑定错位”同属一类缺陷**：看 `.env` 当“生效值”。
> - **定案项①（`/api/v1/streams`）至今仍未真正做成**：实机执行时漏了 `-L`，SRS 会 302 到 `/api/v1/streams/`，只回一行 `Redirect to …`。V1.21.7 已在文档与脚本里统一补 `-L`；“流到底在不在”当时刻未定案。

---

## V1.21.7（同源 `/rtc/` 改运行期解析：根治「重建 srs 后 WHEP 必 502」；+ ⑰ 假 FAIL 修正与第六判据）

Date：2026-10-15
变更类型：修复（反代配置 + 巡检脚本）+ 新增升级路径与巡检判据 + **推翻并纠正 V1.21.6 的定案判据**（无业务服务代码改动）

现场（V1.21.6 上线后实机三条）：
1. 点「建立会话」→ 红色 toast「**服务不可用**」（未建会话）；
2. 等数十秒后可建连，但橙色 toast「**视频拉流失败: WHEP 502 Bad Gateway**」，会话已进入操控中、指令回执 4ms，画面仍 `0.00 Mbps / 0.0 fps`、连接状态 `closed`；
3. 始终无视频。

### 实机存证与定案（本轮拿到了上版缺的取证）

- `sudo ss -ltnp 'sport = :1935'` → LISTEN 归 **`dockerd`（pid 2017）**；巡检 **⑰-a PASS**（已按 `/proc/<pid>/cgroup` 排除容器内那个）⇒ **V1.21.6 的“双实例”假设正式证伪**：`ps` 里那个 `./objs/srs -c conf/srs.conf`（PID 2947985）**就是 `hunter-srs` 容器的主进程**（Docker 共享宿主 PID 视图，容器进程必然出现在宿主 `/proc`）；
- 运维为让新 candidate 生效执行了 `up -d --force-recreate srs`，随后 `docker exec hunter-srs cat /usr/local/srs/conf/srs.conf | grep candidate` 已为 **`192.168.31.35`**（⑰-b PASS，上轮的公网 IP 残留已消除）；
- 几分钟后巡检 **⑰-c FAIL http=502**（同源 `/rtc/`）——与“容器自身健康、candidate 已对”同时成立；
- ⑰-e FAIL「RC_SRS_WHEP_URL 未配」与截图矛盾（浏览器正在同源 `/rtc/v1/whep/` 发请求并收到 502）⇒ **该 FAIL 是探针缺陷，不是配置缺陷**；⑰-d WARN 无活跃流属**重建后的预期现象**。

**根因（三条，相互独立）**：
- **502**：`infra/deploy/config/nginx.conf` 的 `location /rtc/` 直写 `proxy_pass http://srs:9090;`——nginx 对**字面量域名**只在**加载配置时解析一次**。`--force-recreate srs` 后 bridge 重分配容器 IP，hunter-web 仍连旧地址 → `connect() failed (111)`。而改 candidate 的唯一路径就是 recreate（单文件 bind mount，`docker restart` 不重挂），所以这是个**每次调参必中**的结构性坑；`docker restart hunter-web` 只能顶一时。
- **⑰-e 假 FAIL**：探针只读宿主 `.env`，而 compose 写的是 `RC_SRS_WHEP_URL: ${RC_SRS_WHEP_URL:-http://${SERVER_IP}/rtc/v1/whep/}`——默认值生效时 `.env` 里根本**不出现这一行**。与 V1.21.3 修过的“② SRS 探针与绑定错位”同类：**拿 `.env` 当生效值**。
- **⑰-d 无流**：容器重启踢掉车端 RTMP 长连接，GStreamer/ffmpeg **无自动重连语义**；而 TCP 一侧由 docker-proxy/dockerd 代接、握手早已成立，所以车端 `ss` 仍见 ESTAB ⇒ “ESTAB 不等于流已 publish”（本轮已把它写成场景 9 步骤 2 的显式警告与 **9.10**）。
- **症状 1（建会话「服务不可用」）本轮定位到组件但未确诊**：文案只出自 api-gateway（`app/services/proxy_service.py` 的 `ServiceUnavailableError("服务不可用")`，upstream 超时/不可达）；网关 `proxy_timeout_seconds=30`、熔断 `circuit_breaker_open_seconds=30`（连续 5 次失败即熔断 30s）**与“等几十秒就好了”完全吻合**；remote-control 侧 5001 的文案是「指令通道不可用」系列，与截图不同，可排除。**具体是哪一次挂起满 30s 未定案**（候选：`hunter.{vid}.command` Topic 缺失/leader 不可用致 produce 重试、`kafka_produce_max_attempts`+退避），已登记待办① 与取证命令。

> **【V1.21.8 实机再定案】上面第二条（502 归因）已被推翻一半**：同一台机器上 `install.sh --only-step 4` 确实升级了副本（日志含「/rtc/ 上游已改为运行期解析」与 `.bak-v1217`）、`--force-recreate web-portal` 也跑了、⑰-f 确认 PASS，**⑰-c 仍稳到 502**；而车端一重推当前会话的流，浏览器就不报 502 了。所以本次 502 的真因是「**SRS 对拿不到的流直接关连、不回 HTTP 响应**」（nginx 记 502），不是静态上游。静态上游的根治仍保留（不升级则每次重建 srs 必断，是真隐患），但归因范围已在 9.3/9.9 缩小；⑰-c 的探针缺陷（固定用不存在的 `hc_probe` ⇒ 永远 502）在 **V1.21.8** 修掉。另：本条目下面「实机执行」里的 kafka 取证命令漏了 `--command-config`（会报 `TimeoutException: Timed out waiting for a node assignment`，**不代表 Kafka 异常**），已改为仓库封装写法。

### 修复内容

1. **`/rtc/` 改运行期解析**（`infra/deploy/config/nginx.conf`）：http 块新增 `resolver 127.0.0.11 valid=10s ipv6=off;`（Docker 内嵌 DNS，仅自定义 bridge 网络有效）；`location /rtc/` 改 `set $srs_upstream http://srs:9090;` + `proxy_pass $srs_upstream$request_uri;`——域名进变量后 nginx 按 `valid=10s` **运行期重解**，srs 重建不再牵连 web-portal。⚠ 变量型 `proxy_pass` **不再自动改写 URI**，故必须显式接 `$request_uri`（含 `?app=…&stream=…`，与原先“不带 URI”的转发结果完全一致）。头部「关键约束」同步补一行；`upstream api_gateway/remote_control` 仍为加载期解析（本轮**未改**，它们不靠 recreate 变更 IP，已写进注释与文档硬约束 3）。
2. **已部署副本的原地升级路径**（`scripts/install.sh` 新增 `upgrade_nginx_rtc_upstream`，挂在 `ensure_nginx_conf` 的“文件已就位”分支）：存量机器的 `config/nginx.conf` 不会被仓库覆盖（防止手工修改丢失），因此必须**定向升级**：备份 `$1.bak-v1217` → 两条 `sed`（插 `set $srs_upstream` / 换 `proxy_pass`，必要时补 `resolver`）→ 验收“已成变量写法且旧静态行已消失”，未达预期**原副本回滚**；已升级则 `return 0` 幂等；有 `/rtc/` 但形态不认识（被手工改写过）则**只告警不覆盖**；无 `/rtc/`（V1.21.1 之前旧副本）也不动。成功时提示需 `--force-recreate web-portal`。
3. **新增 `container_env()`**（`scripts/common.sh`）：`docker inspect -f '{{range .Config.Env}}…'` 读**容器生效值**（兼容 distroless：无需 exec 入容器、也不依赖 `.env`）；容器不存在/无该键时静默返回空供调用方回退。
4. **⑰-e 改读生效值 + 新增 ⑰-f**（`scripts/health-check.sh`）：⑰ 从五个判据扩为**六个**（usage/清单/头注释同步）。e) `container_env hunter-remote RC_SRS_WHEP_URL` 优先、`.env` 回退、两者都读不到降为 **WARN**（不再假 FAIL），仅当“真含 `:9090` 跨源”或“CHANGE_ME 占位”才 FAIL；**新增 f) `/rtc/` 上游解析方式**：渲染副本已含 `set $srs_upstream`→PASS；仅 `location /rtc/`+静态 proxy_pass→**WARN 并直接给出升级命令**；副本缺失→WARN；无 `/rtc/`→FAIL。⑰-a/c/d 文案同步纠正（⑰-a 补“已按 cgroup 排除”、⑰-c 502 指向 9.9、⑰-d 明写“刚重建过 srs 为空属预期、车端不自动重推、需重开会话”）。
5. **推翻并纠正 V1.21.6 写错的定案判据（本轮最重要的修改）**：“`ps -ef | grep '[o]bjs/srs'` 能看到宿主机那份（容器内那份不会出现在这里）”**是错的**——已在 `install.sh::check_single_srs_instance` 注释与日志、`common.sh::hc_foreign_srs_pids` 注释、`infra/deploy/config/srs.conf` 头部、四件套文档全部改为：**定案只看 `/proc/<pid>/cgroup`，`ps` 只能列候选、`ss` 只能作旁证（归 `docker-proxy` 或 `dockerd` 都属正常发布形态）**。`install.sh` 预检与 ⑰-a 闸门**保留**（双实例在其它环境仍可能出现），但提示口径已修。
6. **全面补 `-L`（本轮定案失败的直接坑）**：`health-check.sh` ⑰-d 探针改 `curl -sL`；`install.sh` 预检提示语、`故障排查指南` §0/步骤 4/9.3 取证 c)/9.7、`运维手册` 一③与 L2、`01-部署概述`、`车端远程操控部署指南` 均改为 `curl -sL …/api/v1/streams` 并明写“不带 `-L` 只回 `Redirect to /api/v1/streams/`，本项等于没做”（含在 9.7 对当时“没流”这一现象补复盘注）。
7. **文档**：`故障排查指南` §0 “两条”→“三条”（补 `/rtc/` 运行期解析）、场景 9 步骤 1/2/3/5/8 与原因表（新增“静态上游”与“刚重建过 srs”两行）、**9.3 重写**（502 拆四类 + 双实例证伪说明）、**9.7 开头加证伪更正**、**9.8 尾部加“重建 srs 连带两件”**、**新增 9.9（静态上游导致必 502：取证/临时恢复/根治/验证）与 9.10（重建 srs 后无流属预期，需重开会话）**；`运维手册` §3.8 五层→**六层自检（新增 L6 上游解析方式）**、“两条假证据”→**三条**、candidate 标准操作补连带两件；`deployment.md` §15 两条硬约束→**三条**且改掉“必须 restart hunter-web”的旧口径；`01-部署概述` SRS 单实例段重写并新增 `/rtc/` 运行期解析约束。

向后兼容：
- nginx 模板变更**不自动覆盖已部署副本**（保留现场手改），而是由 `install.sh --only-step 4` **定向升级**并留 `.bak-v1217`；升级后需 `--force-recreate web-portal`（单文件 bind mount）才生效。**升级前旧行为保持**：重建 srs 后仍需 `docker restart hunter-web`。
- `proxy_pass` 转发结果与旧写法逐字一致（`$request_uri` 含查询串），**对 SRS/前端零协议变更**；未改任何 compose 端口与 env 默认值。
- ⑰-e 由 FAIL 降为 WARN（假 FAIL 纠正）：存量环检的 FAIL 计数会减 1，不算断约；新增 ⑰-f 可能给存量未升级机器多一条 WARN。

风险：低-中。① 变量型 `proxy_pass` 属 nginx 语义敏感写法（已显式接 `$request_uri` 并用升级用例锁定输出）；② `resolver 127.0.0.11` 依赖 Docker 内嵌 DNS，若 `web-portal` 改用 host 网络则 `/rtc/` 会挂（当前 compose 为 `hunter-net` bridge，已写入文档硬约束 3）；③ 升级函数会修改现场 `config/nginx.conf`（已具备回滚与异形不覆盖）；不动业务服务代码与契约。

验证（本地，已闭环）：`bash -n` 对 `common.sh`/`install.sh`/`health-check.sh`/`daily-check.sh` **全 OK**（本轮后续又改过 ⑰-d 与预检提示语，复跑仍 OK）。`upgrade_nginx_rtc_upstream` 用 `git show HEAD:` 取旧版 nginx.conf 实跑五例：**首次升级 diff 恰好 5 行（1 删 4 增）**、**幂等复跑无改动且不覆盖已有备份**、**异形（手工改写过）文件未被动**、**无 `/rtc/` 返回 0**、**升级后产物与仓库模板的 `/rtc/` 段逐字一致**（忽略行尾 CR 后 diff 为空：开发机 `core.autocrlf=true` 使**工作副本**为 CRLF、而 git **存 LF**，服务器（Linux，autocrlf=false）检出亦为 LF，不影响容器内解析；未跑 `nginx -t`——开发机无 nginx 二进制，真实语法校验属下面实机第 4 步）。`container_env` 四例（有键/无键/容器不存在/值含 `=`）。`check_video_chain` 桩化七例 A–G 全按预期：A 实机态（e=PASS/f=WARN/c=FAIL502）、B f=PASS、C e=WARN、D `.env` 回退 PASS、E `:9090` 跨源 FAIL、F 无 `/rtc/` FAIL、G 副本缺失 WARN。本轮涉及的临时验证脚本**均已删除**，仓库不留测试脚手架。**实机回归待回填**。

实机执行（按序，**不要跳第 3 步**）：

```bash
sudo -i; cd /opt/HunterCore                                  # 必须到 APP_DIR 根，否则 .env/install.sh 都找不到
bash scripts/health-check.sh 2>&1 | grep '视频链路'              # 预期 ⑰-c FAIL、⑰-f WARN（当前旧形态）
sudo bash scripts/install.sh --only-step 4 -y                 # 自动升级 config/nginx.conf（备份 .bak-v1217）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate web-portal
sudo docker restart hunter-web                                # 若不想马上 recreate，这一步先应急消除 502（仅对旧形态必需）
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/sdp' --data 'x' http://127.0.0.1/rtc/v1/whep/   # 不再是 502（4xx 为预期）
# ⚠ 然后：**在页面结束会话→重新建立一次会话**，让车端重新推流（否则 ⑰-d 仍为空，画面依旧没有）
curl -sL http://127.0.0.1:9090/api/v1/streams | head          # ⚠ 带 -L：定案项，应看到 hunter_{vid}_{sid}
bash scripts/health-check.sh 2>&1 | grep '视频链路'              # 预期六项全绿（⑰-d 有流）
# 若 201+有流仍 0 fps ⇒ 媒体面问题，不是拉流问题：
ss -ulnp | grep 8000                                          # 8000/udp 在听；并确认安全组放 UDP 8000
# 浏览器侧：chrome://webrtc-internals 看 candidate pair 是否是 192.168.31.35；不是则回 9.8（真跨 NAT 则补 TURN）
# 症状 1（建会话「服务不可用」）取证（与上面并行，不影响拉流修复）：
sudo docker logs --since 30m hunter-api-gateway 2>&1 | grep -E 'proxy_upstream_timeout|circuit_breaker_open|服务不可用' | tail -30
sudo docker logs --since 30m hunter-remote 2>&1 | grep -E 'session_command_failed|kafka_produce_(retry|buffered_to_disk|failed)' | tail -30
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_topics_cli --list' | grep -i HUNTER-001   # ⚠ 必须走封装（它会自动补 --command-config）
```

分发：`infra/deploy/config/{nginx.conf,srs.conf}` + `scripts/{common.sh,install.sh,health-check.sh}` + 文档（`docs/故障排查指南.md`（§0、场景 9、9.3、9.7、9.8、**新增 9.9/9.10**）、`docs/运维操作手册.md` §3.8、`docs/deployment.md` §15、`docs/01-部署概述与环境要求.md` §3.3、`docs/车端远程操控部署指南.md`）+ 本 release 条目（含 V1.21.6 的【V1.21.7 实机定案】修正括注）。**无镜像重建需求**（除 `web-portal` 因 nginx 副本变更需 recreate；它挂载配置、镜像本身不变，无需 `--build`）。

待办（如实标注，**未确诊/待实机定案，严禁记为已修复**）：① **症状 1“建会话服务不可用”根因未定**：已定位到 api-gateway `proxy_service.ServiceUnavailableError`（超时 30s 或熔断），并确认“等几十秒就好”与 `circuit_breaker_open_seconds=30` 吻合；但 remote-control 单次挂起数十秒的直接原因（Kafka produce 重试 / Topic 缺失 / leader 不可用）需上面三条取证才能分流；② **根治后的实机验证未做**（上面第 4–6 步与“不带 restart web 的 srs 重建”需现场回传）；③ **“流到底在不在”仍未定案**（上轮 ① 因漏 `-L` 未做成，本轮已修探针，需重跑）；④ candidate 改局域网后 ICE 是否真能出画尚未验证（继承 V1.21.6 待办②；浏览器与服务器不同层网络时应补 TURN）；⑤ **平台侧缺口**（继承 V1.21.6 待办③）：`services/remote-control` 无 SRS HTTP API 客户端，建会话时不校验目标流是否已发布，`webrtc_ready` 默认 `False`，“无流”只能在前端 0 fps/502 时暴露；另“重建 srs 后提醒车端重开会话”属运维纪律，是否做成会话响应里的可见提示，下版评估；⑥ `hc_foreign_srs_pids` 靠 cmdline 特征识别，改名运行的第二实例仍会漏（本轮仅降级其判据地位，未提升其能力）；⑦ `upstream api_gateway/remote_control` 两块**仍是加载期解析**，若后续给这两个服务加 recreate 流程，需同样改变量写法；⑧ 本轮 estop/前端修复与 V1.21.6 的 candidate 幂等对齐仍需实机回归确认。

---

## V1.21.8（⑰-c 探针翻案：502 不再单独定“反代断裂”；⑰-d 升级为“帧到达率”）

Date：2026-10-15
变更类型：修复（巡检脚本判读缺陷）+ 文档再定案（**无业务服务代码、无配置模板、无 compose 改动**）

现场（V1.21.7 上线后同一台 `192.168.31.35` 的两轮回传）：
1. 16:33 `install.sh --only-step 4` 输出「/rtc/ 上游已改为运行期解析」（备份 `.bak-v1217`）、16:34 `--force-recreate web-portal`、16:44 二次确认 **⑰-f PASS**；但 **⑰-c 仍 FAIL http=502**；
2. 16:32 `curl -sL …/api/v1/streams` 拿到了上版缺的定案数据：流**在容器身上**（`hunter_HUNTER-001_8fa0dff3-…`、`app=remote_control`、`publish.active=true`、`video H264 640x480`、`clients 1`、**`frames:0`、`kbps.recv_30s:0`**、`recv_bytes 51158`）；
3. 16:47 重开会话后 **⑰-d PASS**（流名 `hunter_HUNTER-001_3f7d0268-…` 与会话卡片 sid 逐字一致），**浏览器不再弹 502**，连接状态进入 `connecting`，仍 `0.0 fps / 0.00 Mbps`；指令回执 11ms/63ms 正常；
4. 症状①「服务不可用」本轮**未复现**（两次都直接建会话成功）；`--since 30m` 窗口内无 `proxy_upstream_timeout`/`circuit_breaker_open`。
5. **第三轮回传（16:44→16:47）坐实了下面那条“探针不可判”**：同一轮巡检里 **⑰-c FAIL(http=502) 与 ⑰-d PASS(在流 `hunter_HUNTER-001_3f7d0268-…`) 同时成立**——而服务器上跑的仍是 **V1.21.7 的 `health-check.sh`**（⑰-c 文案逐字未变、探针仍固定 `hc_probe`；本条目的改动尚未同步到机器）；手工 `curl -X POST …/rtc/v1/whep/`（**不带任何查询串**）同样回 502。同期 `curl -sL :9090/api/v1/streams` 先见 `{"streams":[]}`、后见在流 ⇒ 流随会话建立/结束而来去（属 9.10 形态），不是双实例错位。另：`ss -ulnp | grep 8000` 归 `dockerd` 为正常（`daemon.json` 已设 `userland-proxy:false`，UDP 走 iptables DNAT）。
6. **第四轮回传（车端 `HunterEdge`，17:21→17:34）——0 fps 定案 + 症状① 现形**：
   - 车端实际 pipeline：`gst-launch-1.0 -e fdsrc ! video/x-raw,format=RGB,width=640,height=480,framerate=15/1 ! queue max-size-buffers=3 ! videoconvert ! x264enc bitrate=4096 tune=zerolatency speed-preset=ultrafast key-int-max=15 ! h264parse ! flvmux streamable=true ! rtmpsink location=rtmp://192.168.31.35:1935/remote_control/hunter_HUNTER-001_{sid}`；来帧源为 ROS topic `/camera/camera/color/image_raw`（`mode=ros_topic`）；
   - 守护进程日志每次 boot 后 **88ms 内**即报：`[WARNING] [视频] 帧队列满（RTMP 未就绪？），开始丢帧`；车端 `top` 显示 **62.4% id**、推流进程自身占 23.5% CPU；
   - 会话生命周期：`17:21:27 boot(28d61663) → 17:21:48 stop(operator_end)`、`17:22:29 boot(3671df45) → 17:24:11 stop(**heartbeat_timeout**，last_cmd_age_ms=71006)`、`17:24:24 boot(911c8cbd)` 仍在推；
   - 浏览器 DevTools 网络页：`/api/v1/remote/session/{sid}` 连续 **429 Too Many Requests**、`/api/v1/user/refresh` 与 `/api/v1/remote/vehicles` **401 Unauthorized**；另有一条 `Uncaught TypeError … 'toLowerCase'` 来自 `content_main.js`——**那是浏览器扩展注入的脚本，不是本项目产物**（本项目资源均为 `index-*.js`），不计入故障；
   - 用户手工在地址栏打开的 `192.168.31.35/rtc/v1/whep/?app=remote_control&stream=hunter_HUNTER-001_**3671df45**-…` 回 502——而这条会话已于 **17:24:11 被车端 stop** ⇒ **又一次验证本条目定案**：拉一个已不存在的流就是 502，与反代无关。

### 定案（推翻 V1.21.7 的 502 归因）

- **静态上游不是本次 502 的成因**：运行期解析已生效 + web 重建后仍 502，而流一到位 502 就消失 ⇒ 本次属「**SRS 对目标流拿不到的 WHEP 请求直接关连、不回 HTTP 响应**」（nginx 记 502；语义上本该 404，但 SRS 5.0 就是这么实现的）。V1.21.7 的根治**保留**（不升级则每次重建 srs 必断，是真隐患），但归因范围已在 9.3/9.9 缩小。
- **⑰-c 的探针本身不可判**（V1.21.6 引入 ⑰ 时留下的缺陷）：固定用 `stream=hc_probe`——一条永远不存在的流——于是**无论反代通不通都稳定 502**，而文案直接把 502 定成“nginx 连不上上游”。本轮 502 误判链条就是它造成的（上面现场 5：“有流仍 502”已是直接旁证）。
- **新的关键线索（未确诊）**：`publish.active=true` 且 SRS 已解析出 `H264 640x480`（序列头到了），但 `frames:0`、`kbps.recv_30s:0` ⇒ 车端 publish 成功后**没送帧**；且 SRS 侧 640×480 与会话卡片声明的 `1280x720@30fps` 不一致。0 fps 的根**很可能在车端“编码→推流”段**，而不是 ICE（V1.21.7 待办④ 的 ICE 假设尚未排除）。
  - **【同日第四轮回传已定案】上面“很可能”已转为确诊**：车端 `fdsrc → rtmpsink` 管道**从一开始就没消费帧**（boot 后 88ms 即报“帧队列满（RTMP 未就绪？），开始丢帧”；15fps × `queue max-size-buffers=3` 只有 200ms 余量），而车端 62% CPU idle 排除编码瓶颈 ⇒ 卡在 `rtmpsink`（START 阶段连 RTMP 挂起则整条 pipeline 阻塞）。与服务器侧 `recv_bytes=51158` + `recv_30s=0` + `frames=0` 完全吻合：RTMP 握手/元数据/SPS 走完了，**媒体帧一帧未进**。**平台侧（nginx/SRS/candidate）全部排除**。处置在车端（本仓无 `HunterEdge` 代码），隔离实验与分流表已写进 **9.11**。
  - **症状①「服务不可用」真身已定位（待日志坐实）**：不是上游超时/熔断，而是 **网关 429 限流**——`app/core/rate_limit.py` 用 `WINDOW_SECONDS=60` 的固定窗口却直接拿 `rate_limit_per_user_qps=100` 做阈值 ⇒ 名义 100 QPS 实为 **100 次/分钟（≈1.67 req/s）**；而前端 `POLL_INTERVALS`（session 1000ms + telemetry 2000ms + dashboard 5000ms）单是操控台一页就≈90 次/分钟，**60s 内必顶破**；顶破后同窗口内**所有接口包括 `POST /remote/session` 全 429**，而 `app/core/http.py` 把 429 的响应体写成 `error_response(5001, "服务不可用")`，前端 `request.ts` 直接弹 `payload.message` ⇒ 用户看到「服务不可用」。**与“等数十秒就好了”逐字吻合**（60s 固定窗口重置 + `Retry-After: 60`）。⚠ 契约 `x-hunter-error-status-map` **自己已区分** `{5001,503,服务不可用}` 与 `{5001,429,限流触发}`，实现把 429 档的 message 也写成“服务不可用”属**与契约相悖**。前两轮 grep 抽不到是因为限流日志事件名是 `rate_limit_triggered`/`rate_limit_blocked`，既不含“服务不可用”也不在 `proxy_*` 里。完整因果链与取证命令见 **9.12**。
  - **两条推论（本节上面“没送帧”的细化，仍属待实机定案）**：
    ① `recv_bytes` 是累计、`kbps.recv_30s` 是近 30s 速率；`recv_bytes=51158` 而 `recv_30s=0` ⇒ 那 50KB（connect 报文 + metadata + SPS/PPS + 开头几帧）**全落在 30s 以前**，之后一个字节没来 ⇒ 不是“从没推过”，而是**推了开头就停写、TCP 仍挂**（候选：编码器阻在下游、rawvideo stdin 写满后喂帧线程死掉、GStreamer 只过 preroll 就卡）；
    ② 640×480 **不可能来自未改动的内置模拟器**：`tools/vehicle-simulator/vehicle_simulator/config.py` 为 `video_width=1280 / video_height=720 / video_fps=30` 且**无命令行开关**，契约与《车端远程操控部署指南》写定 1280×720 ⇒ 车端用的是**自己的实现**（ffmpeg 缺 `-video_size` 的常见默认 / GStreamer caps 协商降级 / 硬件编码器默认档）。

### 修复内容

1. **⑰-c：502/504 不再直接 FAIL**，改为先做「直连 srs 同形请求」对照再定级：直连能回 2xx/4xx/503（SRS 正常应答）而走代理 502 ⇒ **FAIL 真断裂**；直连也不回 HTTP 响应（`000`）⇒ **WARN**，并直接给出 nginx 日志判读命令（`connect() failed`/`no live upstreams` = 真断裂；`prematurely closed` = 只是流不存在）。探针的 `stream=` 改用 ⑰-d 采到的**真实流名**（无流时回退 `hc_probe`），所有文案统一带出用的是哪条流。
2. **⑰-d：从“有流/无流”升级为“有流 + 帧到达率”**：有流时 `sleep 2` 复采一次 `frames`，不涨 ⇒ **FAIL 并指名“根在车端编码→推流，不在反代与 ICE”**；在涨 ⇒ PASS 并指向 candidate/ICE/8000udp。`recv_30s` 一并进文案。
3. **结构与健壮性**：⑰ 的 streams 采集前移到 c) 之前（供 c 用真实流名，d 复用）；`names`/`current` 两条 grep 管道补 `|| true`——`set -euo pipefail` 下“grep 无匹配退 1”会中断整轮，此前只靠 `run_all_health_checks` 的 `if ! check` 上下文掩盖（桩化时真实复现：函数裸调会静默死在 ⑰-c 之前）；`first_stream` 改用纯 bash `${names%% *}` 而非 `awk`；`recv_30s` 取值改 `cut -d: -f2`（`grep -oE '[0-9]+'` 会把 key 里的 `30` 当值，桩化实测到 `recv_30s=30\n0` 后修正）。⑰ 头注释把“双实例”“静态上游导致本次 502”两条已证伪假设改写为定案记录。
4. **文档**：`故障排查指南` 9.3（502 “不自带结论”、判读表新增两行、⑰-c 口径变更说明、V1.21.8 证伪括注）、9.9 开头加“归因范围已缩小”更正、**新增 9.11**（“在流但 `frames` 不涨”：实机 JSON 存证、三次采样判据、四类分流表、⑰-d 新口径、“跑 ⑰ 请用 sudo”、取证命令）；`运维手册` §3.8 的 L2/L3/L5 三行重写（含**直连同形对照命令**、`userland-proxy:false` 下 `ss` 归 `dockerd` 属正常、webrtc-internals `framesDecoded` 分流）、“三条假证据”→**四条**；`deployment.md` §15 硬约束 3 把“必 502”改为结构性隐患并新增**两张日志分流表**（`connect() failed` vs `prematurely closed`）、三条硬约束的定性改写（前两条=实机确诊，第三条=隐患）；`01-部署概述` §3.3 补“但 502 不只这一类”警示块；本条目内补【V1.21.8 实机再定案】括注并修掉错误的 kafka 取证命令。

向后兼容：只改 `scripts/health-check.sh` 与文档，**不需重建镜像、不需 recreate 任何容器**。⑰-c 在“流不存在”场景由 FAIL 降为 WARN（存量环检 FAIL 计数会减 1，daily-check 阈值不受影响）；⑰-d 在“有流无帧”时新增一条 FAIL——原本“看起来全绿”的环检会开始暴露车端问题，**这是本意**。文档旧结论以“更正括注”形式保留（不抹历史）。

风险：低。① ⑰ 在有流时多花 2s（`sleep 2` 复采）；② 帧增量只看第一条流（`head -n1` 口径已写进注释；多车并发推流时需按 sid 精确取流，已登记待办⑤）；③ 若未来 SRS 改为对“流不存在”回 404，⑰-c 会自动落回“直连回 4xx ⇒ FAIL”分支，即 502 重新成为断裂证据，**无需再改脚本**。

验证（本地，已闭环）：`bash -n scripts/health-check.sh` OK。`check_video_chain` 桩化六例全绿：**A** 无流+代理 502+直连 `000` → ⑰-c **WARN**；**B** 无流+代理 502+直连 `404` → ⑰-c **FAIL**；**C** 有流+代理 `400` → ⑰-c **PASS**，且从 URL 记录坐实**探针已改用真实流名**（`stream=hunter_HUNTER-001_3f7d0268`）；**D** 有流+`frames 100→100` → ⑰-d **FAIL**（`recv_30s=0` 读数正确）；**E** 有流+`frames 100→160` → ⑰-d **PASS「2s 新增 60 帧」**；另记录到桩化过程本身暴露并修掉了那个 `set -e`+`pipefail` 中断缺陷（上面修复内容 3）。本轮临时验证脚本**已全部删除**。**实机回归待回填**——⚠ 前提：服务器上仍是 V1.21.7 的 `health-check.sh`（现场 5 的“有流仍 502”就是旧脚本的行为），**必须先把 `scripts/health-check.sh` 同步到机器（git pull 或 scp）再跑**，否则 ⑰-c/⑰-d 的新口径根本看不见。

实机执行（按序）：

```bash
sudo -i; cd /opt/HunterCore
# 0) ⚠ 先把本条目的 health-check.sh 同步到机器（服务器仍旧版）：
#    git pull（脚本入库），或 scp scripts/health-check.sh 后 sudo cp 覆盖 /opt/HunterCore/scripts/
sudo bash scripts/health-check.sh 2>&1 | grep '视频链路'      # ⚠ 用 sudo：非 root 读不到 600 的 .env，⑰-b 会假报“未配”
# 预期：⑰-c 从 FAIL 变 WARN（并让你去看 nginx 日志），⑰-d 给出帧增量结论
sudo docker logs --since 5m hunter-web 2>&1 | grep -E '/rtc/|connect\(\) failed|no live upstreams|prematurely closed' | tail -20   # 502 归类的唯一硬证据
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/sdp' --data 'x' 'http://127.0.0.1:9090/rtc/v1/whep/?app=remote_control&stream=hc_probe'   # 直连对照：回 000 ⇒ 反代其实是通的
# 帧到达率（车端侧 vs 媒体面的分水岭）：
for i in 1 2 3; do curl -sL http://127.0.0.1:9090/api/v1/streams | grep -oE '"frames":[0-9]+|"recv_30s":[0-9]+' | tr '\n' ' '; echo; sleep 5; done
# frames 不涨 ⇒ 车端（编码→rtmpsink）；在涨而页面 0 fps ⇒ 浏览器 chrome://webrtc-internals 看 candidate pair，服务器抓包：
sudo tcpdump -ni any udp port 8000 -c 30                       # 浏览器的 STUN/DTLS 到不到；到不了查网络层/安全组
# 症状①（建会话服务不可用）取证：窗口要放宽，并走仓库封装跑 Kafka
cd /opt/HunterCore && sudo bash -c 'source scripts/common.sh; load_env; kafka_topics_cli --list' | grep -i HUNTER-001
sudo docker logs --since 6h hunter-api-gateway 2>&1 | grep -E 'proxy_upstream_timeout|proxy_upstream_unreachable|circuit_breaker_open|proxy_backend_not_configured' | tail -30
```

分发：**代码只进一个文件** —— `scripts/health-check.sh`（commit `b2753a9`，已提交、按现场约定**不打 git 标签**）。⚠ **本仓库的 `.gitignore` 第 158/159 行把 `docs/` 与 `release.md` 整体排除在版本库之外**（V1.21.7 的 `e432836` 同样只含 `nginx.conf`/`srs.conf`/三个脚本），因此文档四件套与本条目**不能靠 `git pull` 交付**，必须单独同步文件到 `/opt/HunterCore`：`docs/故障排查指南.md`（9.3/9.9/**新增 9.11**）、`docs/运维操作手册.md`（§3.8 六层表与四条假证据）、`docs/deployment.md`（§15 硬约束 3）、`docs/01-部署概述与环境要求.md`（§3.3 警示块）、`release.md`。**无镜像重建需求。**

待办（如实标注，**未确诊/待实机定案，严禁记为已修复**）：① **0 fps 已定案在车端**（上面定案段）：卡在 `fdsrc → rtmpsink`，管道未进入 PLAYING、写线程满队列后主动丢帧——**平台侧无需改动**；尚待回填的是 **9.11 的 `videotestsrc` 隔离实验**（区分“RTMP 这一跳不通”与“喂帧那一路不通”）；② 车端编码参数违反契约：**640×480@15fps** vs 契约/会话声明 **1280×720@30fps**（`bitrate=4096`、GOP≈1s 合规），属车端 `HunterEdge` 侧修正（本仓无该代码）；③ **症状 已确诊为网关 429 限流（9.12）——V1.21.9 已修复**；④ Kafka 取证必须走 `kafka_topics_cli` 封装（本轮手测的 `--bootstrap-server 127.0.0.1:9092 --list` 报 `TimeoutException` 是**未带 `--command-config` 的认证缺失**，不是 Kafka 故障）；⑤ ⑰-d 帧增量只看第一条流，多车并发需按 sid 精确取流（与 V1.21.7 待办⑤ “平台侧无 SRS HTTP API 客户端”同源）；⑥ ⑰-c 仍依赖宿主机 `127.0.0.1:9090` 可达（V1.21.3 起仅绑回环），若改 K8s 部署需换服务地址；⑦ 继承 V1.21.7 待办②⑦⑧（运行期解析的“不带 restart web 重建 srs”实机验证、`hc_foreign_srs_pids` 改名漏判、`upstream` 两块仍加载期解析、estop/前端与 candidate 幂等回归）；⑧ **新发现、完整独立于视频的两个操控面故障，本轮均未查**：（a）车端 `reason=heartbeat_timeout`、`last_cmd_age_ms=71006` ⇒ **浏览器操控 WS 撑不住 71s**（截图亦为 `连接状态：closed`，而车端按契约 >500ms 无指令即刹停、超时则停推交还驾驶权）；（b）`/api/v1/user/refresh` 连续 **401** 与 `/api/v1/remote/vehicles` 401 ⇒ token 刷新失败链路未查（不排除与 ③ 的限流相互放大：refresh 被 429 挡住时 `request.ts` 会直接 `expireSession`）；⑨ `content_main.js` 的 `toLowerCase` TypeError 已判定为**浏览器扩展注入脚本**（本项目资源均为 `index-*.js`），**不列入待办**，仅防后人误读。

## V1.21.9（修复）

Date：2026-10-15
变更类型：修复

现场（第五轮回传，同一天）：
1. **SRS WHEP 502 真因坐实**：`docker logs hunter-srs` 明确报 `code=5021(RtcDisabled)(RTC is disabled by configuration) : Disabled rtmp_to_rtc of __defaultVhost__`。原 `srs.conf` 只写了 `rtc { enabled on; }` 但缺 `rtmp_to_rtc on;`，导致 SRS 接受 RTMP 推流（帧确实在涨，⑰-d PASS）但拒绝为 WHEP 提供 RTC 转码，浏览器永远拉不到视频。
2. **限流 429 实锤确认**：`docker logs hunter-api-gateway | grep rate_limit_blocked` 显示 `GET /api/v1/data/telemetry` 每 2s 被拦一次，`limit=20 retry_after=60`。前端 telemetry 轮询 2s = 30 req/min > 20 req/min（原 60s 窗口使 “20 QPS” 实际为 “20 次/分钟”）。截图 DevTools 红色 429 完全吻合。
3. **车端数据确认在流**：`recv_bytes` 递增（154768→180147）、`recv_30s=28` kbps、`frames` 23331→23406（75 帧/2s）——“帧队列满”是启动瞬态（88ms），不影响持续推流。

根因（定案）：
- **WHEP 502**：SRS 5.x `vhost { rtc { enabled on; } }` 只声明本 vhost 支持 WebRTC 播放，但 RTMP→RTC 转码需显式 `rtmp_to_rtc on;`。缺此行时 SRS 对 WHEP 请求直接关连（不回 HTTP 响应），nginx 报 `upstream prematurely closed` → 502。
- **429 伪装服务不可用**：网关 `rate_limit.py` 用 `WINDOW_SECONDS=60` 的固定窗口，阈值直接取附录 D 的 QPS 数字（100/200/5/1/20）——名义 “100 QPS” 实为 100 次/分钟 ≈ 1.67 QPS。前端单页轮询（session 1s + telemetry 2s + dashboard 5s）≈ 90 次/分钟，60s 内必顶破 100 阈值。429 响应体 `message="服务不可用"` 与 503 无法区分。

修复内容：
1. **`infra/deploy/config/srs.conf`**：`vhost __defaultVhost__ { rtc { enabled on; rtmp_to_rtc on; } }` —— 开启 RTMP→WebRTC 转码，根治 WHEP 502。
2. **限流窗口 `WINDOW_SECONDS` 60 → 1**（`services/api-gateway/app/core/rate_limit.py`）：契约附录 D 单位为 QPS，窗口必须 1s。同步修改：
   - `contracts/database/redis-keys.yaml`：`ttl_seconds: 1`
   - `common/python/tests/test_storage_contracts.py`、`scripts/verify_data_layer.py`：断言 `(String, 1)`
   - 5 个 OpenAPI 契约 yaml（`api-gateway`、`data-collector`、`data-analytics`、`ota-service`、`remote-control`）：`algorithm` 文本、`ttl_seconds`、`usage` 统一为“窗口 1 秒”
   - 三个服务 `config.py`：`rate_limit_window_seconds: int = 1`（变量名 `_per_min` 保留以兼容环境变量，加注释说明语义已变）
3. **429 响应文案**（`services/api-gateway/app/core/http.py`）：`"服务不可用"` → `"请求过于频繁，请稍后重试"`，回归契约 `x-hunter-error-status-map` 对 429 与 503 的语义区分。
4. **前端 WHEP 重试退避**（`frontend/src/utils/webrtc.ts`）：`WebRtcClient.start()` 新增最多 3 次重试（延迟 3s/5s/8s），给用户“视频拉流未就绪，Xs 后重试”提示。解决车端 pipeline 启动延迟导致首次 WHEP 失败后永久无视频的问题。
5. **前端 429 特判**（`frontend/src/api/request.ts`）：响应拦截器对 HTTP 429 不触发 `expireSession`、不触发 token 刷新；`/user/refresh` 端点被 429 挡住时不再踢登录。

向后兼容：
- SRS 配置变更需 `docker compose up -d --force-recreate srs`（单文件 bind mount，restart 不重挂）。
- 限流窗口变更不需重建镜像（代码层），但部署后旧 Redis 键 `rate_limit:*` 仍存活 60s（旧 TTL），新键 TTL=1s 自动覆盖，无需手工清理。
- 前端变更需重建 `web-portal` 镜像。
- 服务配置环境变量名不变（`RATE_LIMIT_WINDOW_SECONDS` 默认值从 60 变 1，若 `.env` 显式写了 60 则需删除或改为 1）。

风险：低。
- `rtmp_to_rtc on` 是 SRS 官方指令，错误日志明确指向它，无副作用。
- 窗口 1s 后，100 QPS 的全局限制真正生效（原 1.67 QPS），对现有前端轮询无影响（实际 ~2 req/s）。
- WHEP 重试最多延长 16s 才报错，用户体验从“永久无视频”变为“等待几秒后出画”。

验证（本地）：`ast.parse` 11 个 Python 文件全通过；TypeScript 逻辑复核无语法错误；SRS 配置指令合法性由 SRS 自身错误日志直接确认。

实机执行（按序）：

```bash
sudo -i; cd /opt/HunterCore
# 1) 同步代码
git pull   # 或 scp 覆盖
# 2) SRS 配置变更（最关键，根治 WHEP 502）
sudo docker compose -f infra/deploy/docker-compose.yml up -d --force-recreate srs
# 3) 后端镜像重建（网关 + 三个服务）
sudo docker compose -f infra/deploy/docker-compose.yml build api-gateway remote-control data-collector ota-service
sudo docker compose -f infra/deploy/docker-compose.yml up -d api-gateway remote-control data-collector ota-service
# 4) 前端镜像重建（WHEP 重试 + 429 特判）
sudo docker compose -f infra/deploy/docker-compose.yml build web-portal
sudo docker compose -f infra/deploy/docker-compose.yml up -d web-portal
# 5) 验证
sudo docker logs --since 2m hunter-srs 2>&1 | grep -i 'whep\|rtc\|error'   # 应无 RtcDisabled
sudo docker logs --since 2m hunter-api-gateway 2>&1 | grep rate_limit      # 应无 blocked
# 6) 开新会话测试视频（车端重新推流后等待 5s 再打开操控台页面）
```

分发：代码入库（本条目所有文件）。⚠ 文档四件套与 `release.md` 仍被 `.gitignore` 排除，需单独同步。受影响文件：
- `infra/deploy/config/srs.conf`
- `services/api-gateway/app/core/{rate_limit.py,http.py,middleware.py}`、`services/api-gateway/README.md`
- `services/remote-control/app/{config.py,core/rate_limit.py}`
- `services/data-collector/app/config.py`
- `services/ota-service/app/config.py`
- `contracts/database/redis-keys.yaml`、`contracts/openapi/{api-gateway,data-collector,data-analytics,ota-service,remote-control}.yaml`
- `common/python/hunter_common/redis_keys.py`、`common/python/tests/test_storage_contracts.py`
- `scripts/verify_data_layer.py`、`tests/support/flow.py`
- `frontend/src/utils/webrtc.ts`、`frontend/src/api/request.ts`

待办（如实标注）：① **视频出画后 ICE/8000udp 可达性待实机确认**（tcpdump 0 packets 是因为 WHEP 从未成功，`rtmp_to_rtc` 修后需重测）；② 浏览器操控 WS 撑不住 71s（`heartbeat_timeout`、`last_cmd_age_ms=71006`）；③ `/api/v1/user/refresh` 连续 401 链路未查；④ `videotestsrc` 隔离实验待回填；⑤ 车端 640×480@15fps 契约偏差属 HunterEdge 侧修正。

## V1.21.10（修复：SRS 切换 host 网络模式，根治 WebRTC ICE STUN 回包被浏览器丢弃）

Date：2026-10-16
变更类型：修复

现场（V1.21.9 部署后实机回传）：
1. SRS 容器 healthy、WHEP 返回 201、SDP answer candidate 正确（192.168.31.35:8000）。
2. 浏览器 ICE STUN 到达 SRS（SRS 日志确认收到 14 个 STUN 并回复 14 个）。
3. tcpdump 确认 STUN 响应从服务器发出（`192.168.31.35.8000 > 192.168.31.29.52530`）。
4. 但浏览器始终不发出 DTLS ClientHello，连接状态 connecting → failed。三个客户端（手机/VM/桌面）均失败。
5. SRS 日志确认 `RTC: session STUN done, waiting DTLS handshake.` 但永远等不到 DTLS。

根因：
- **Docker bridge 网络的 UDP NAT 导致 STUN 响应被浏览器 ICE agent 拒绝**。SRS 在 hunter-net 桥接网络（172.28.0.x）内运行，端口映射 `0.0.0.0:8000->8000/udp` 经过 iptables DNAT/MASQUERADE。虽然 tcpdump 显示响应源地址正确（192.168.31.35:8000），但 Docker 的 UDP 连接跟踪在 NAT 时可能未正确更新 UDP 校验和，导致浏览器操作系统层静默丢弃响应包。
- **RC_STUN_URLS 默认值指向 SRS 的 WebRTC 端口**（`stun:SERVER_IP:8000`），导致浏览器向 SRS 发送裸 STUN 地址发现请求，SRS 记录为错误（code=5008）并干扰 ICE 状态机。

修复内容：
1. **`infra/deploy/docker-compose.yml`**：SRS 服务切换为 `network_mode: host`，移除 `ports:` 和 `networks:` 段。SRS 直接绑定宿主 1935/8000udp/9090，无 NAT，candidate 即本机 IP，STUN 回包源地址 100% 确定。
2. **`infra/deploy/config/nginx.conf`**：`/rtc/` 反代上游从 `http://srs:9090` 改为 `http://172.28.0.1:9090`（Docker bridge 网关，host 模式下容器经此访问宿主服务）。
3. **`infra/deploy/docker-compose.yml`**：`RC_STUN_URLS` 默认值从 `stun:${SERVER_IP}:8000` 改为空（局域网部署无需 STUN）。
4. **`services/remote-control/app/services/session_service.py`**：`_build_ice_servers()` 当 STUN/TURN 均未配置时返回占位条目 `stun:127.0.0.1:3478`（满足契约 minItems=1，浏览器无法连接后自动回退到 host candidate）。
5. **`scripts/install.sh`**：新增 UFW 规则 `ufw allow from 172.28.0.0/16 to any port 9090 proto tcp`，允许 nginx 容器经 bridge 网关访问 SRS HTTP API。
6. **`infra/deploy/config/srs.conf`**：更新注释说明 host 网络模式。

向后兼容：
- SRS 容器需重建：`docker compose -f infra/deploy/docker-compose.yml up -d --force-recreate srs`
- nginx 配置变更需重建 web-portal：`docker compose ... up -d --force-recreate web-portal`
- remote-control 镜像需重建（_build_ice_servers 逻辑变更）
- install.sh 的 UFW 规则幂等，重复执行不报错
- `.env` 中若显式设置了 `RC_STUN_URLS`，需删除或改为空以启用新默认

风险：低。
- host 模式下 SRS 9090 绑定 0.0.0.0，但 UFW 默认拒绝入向（仅放行 172.28.0.0/16），不对外暴露。
- 如果服务器有多网卡，SRS 会监听所有接口的 1935/8000/9090。当前单网卡环境无影响。

实机执行（按序）：

> ⚠ 关键坑（本次 502 的真实拦路点）：容器挂载的是**运行时配置副本 `${APP_DIR}/config/`**（因 `--project-directory .` 使 compose 里的 `./config/...` 解析到应用根目录，且 install.sh 配置优先级为 `APP_DIR/config/` > `APP_DIR/infra/deploy/config/`）。`git pull` 只更新 `infra/deploy/config/`，**不会覆盖已存在的 `config/` 运行时副本**——必须手动把改动同步过去，否则 nginx.conf / srs.conf 的仓库修改对运行容器完全无效（表现为 nginx 仍解析已不存在的 Docker DNS 名 `srs` → WHEP 502）。

```bash
sudo -i; cd /opt/HunterCore
# 1) 同步代码
git pull
# 2) 【必做】把仓库配置同步到运行时目录（否则 nginx/srs 配置改动不生效）
cp -a config/nginx.conf config/nginx.conf.bak.$(date +%s) 2>/dev/null || true
cp -a config/srs.conf   config/srs.conf.bak.$(date +%s)   2>/dev/null || true
cp -a infra/deploy/config/nginx.conf config/nginx.conf
cp -a infra/deploy/config/srs.conf   config/srs.conf
grep -n srs_upstream config/nginx.conf   # 应为 172.28.0.1:9090
# 3) 放行 UFW（SRS host 模式后 nginx 需经 bridge 网关访问 9090）
ufw allow from 172.28.0.0/16 to any port 9090 proto tcp comment 'SRS HTTP-API from docker bridge'
# 4) 重建 SRS（host 网络模式）
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . rm -f srs
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . up -d srs
# 5) 重建 web-portal（nginx 上游变更，bind mount 需重启容器重读）
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . up -d --force-recreate web-portal
docker restart hunter-web
# 6) 重建 remote-control（ICE 配置逻辑变更）
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . build remote-control
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . up -d remote-control
# 7) 验证
docker logs --since 10s hunter-srs 2>&1 | head -5   # 应显示 healthy 启动
curl -s http://127.0.0.1:9090/api/v1/versions       # SRS API 可达
curl -s http://172.28.0.1:9090/api/v1/versions      # bridge 网关可达（nginx 用此路径）
docker exec hunter-web grep -n srs_upstream /etc/nginx/nginx.conf   # 容器内应为 172.28.0.1:9090
# 8) 手机同网段开新会话测试视频
```

实机验证结果（2026-10-16，已出画 ✅）：
1. 同步运行时 `config/nginx.conf` 并重启 hunter-web 后，WHEP 从 502 恢复 **201**。
2. 手机（Pixel/Chrome，同网段 192.168.31.29）操控台连接状态 `connected`，视频成功播放（车端模拟器 `videotestsrc` 绿色测试图案，15fps、端到端约 9ms、指令回执约 2ms）。
3. 全程链路 `RTMP → SRS rtmp_to_rtc → WHEP 信令 → WebRTC ICE/DTLS → 浏览器播放` 打通，三客户端不再失败——确认此前 DTLS_HANG 根因（Docker UDP NAT + RC_STUN_URLS 误配 + 运行时 config 未同步）已消除。
4. 诊断假信号记录：`curl` 塞非法 SDP（body=x、stream=test）返回 502 属预期（SRS 异常断连被 nginx 记为 502），与真实浏览器 201 不矛盾，不可据此判定反代未通。

分发：代码入库。受影响文件：
- `infra/deploy/docker-compose.yml`
- `infra/deploy/config/nginx.conf`（⚠ 需同步到运行时 `${APP_DIR}/config/nginx.conf`）
- `infra/deploy/config/srs.conf`（⚠ 同上，需同步到运行时副本）
- `services/remote-control/app/services/session_service.py`
- `scripts/install.sh`

待办（如实标注）：① ~~实机验证 host 模式后 ICE/DTLS 是否通过~~ **已验证：出画成功（见上）**；② V1.21.9 待办②④⑤不变（WS 撑不住 71s、`/user/refresh` 401、`videotestsrc` 隔离实验、车端 640×480 契约偏差）；③ ~~install.sh Step 4 的 `ensure_srs_conf` 需适配 host 模式，且**部署脚本应自动把 `infra/deploy/config/*` 同步到运行时 `${APP_DIR}/config/*`**~~ **已在 V1.21.11 落地**。

---

## V1.21.11（修复：install.sh 自动同步运行时配置副本 + 前端 Token 临期前置刷新根治 /user/refresh 恒 401）

Date：2026-10-16
变更类型：修复

现场：
1. V1.21.10 实机部署时 `git pull` 后仓库 `infra/deploy/config/nginx.conf` 已是新上游（172.28.0.1:9090），但运行容器仍读旧副本（srs:9090）→ WHEP 502（`srs could not be resolved`），靠手动 `cp -a` 救场。检查 `scripts/install.sh` 发现 `ensure_nginx_conf`/`ensure_srs_conf` 在运行时副本已存在时只打「幂等」日志、**从不回同步仓库**，属结构性缺口，应脚本化。
2. `/user/refresh` 连续 401（code=1003）：登录约 30min 后必现，强制重登，Refresh Token 的 7d 有效期从未真正生效。
3. 实机跑 `install.sh --step 4` 续跑时被预检拦下：1935/tcp、8000/udp 报「被非本项目进程占用」——实际是 host 模式 SRS 直接绑在宿主上的 `srs` 进程。

根因：
1. **install.sh 同步缺口**：容器挂载的是运行时副本 `${APP_DIR}/config/*`（`--project-directory .` 使 compose 的 `./config/...` 解析到应用根，且配置优先级 `APP_DIR/config/` > `APP_DIR/infra/deploy/config/`），而 ensure_* 函数仅在副本缺失时复制，副本存在但内容与仓库不一致时无任何动作。
2. **refresh 401 为设计级缺陷（静态确诊）**：网关 `session:{user_id}` 键 TTL = `jwt_access_token_expire_minutes*60`，与 Access Token `exp` 同刻写入、同刻过期（`services/auth_service.py` 会话写入处）；而前端**无前置刷新**（`isAccessTokenExpiring` 定义后零调用），刷新唯一触发点是收到 1003 之后——那时会话键已消失，`load_session_token` 返回空 → `TokenExpiredError` → refresh 恒 401(1003)。多标签页叠加放大器：各标签独立单飞、同刻双刷 → 轮换防重放判一枚为重放 → 败方 `expireSession()` 清空**共享** localStorage → 胜方也被踢，互踢登录循环。
3. **预检不认识 host 模式 SRS**：`port_owned_by_project` 靠 `docker ps` 的 PORTS 列识别「本项目容器占用」，而 `network_mode: host` 容器无端口发布（PORTS 列为空）→ 宿主直绑的 1935/8000 被误判为外部占用，V1.21.10 切 host 模式后所有 `--step` 续跑都会被预检阻断。

修复内容：
1. **`scripts/install.sh`**：新增 `sync_runtime_config <仓库源> <运行时目标> <描述> [归一化sed]` 助手——`cmp` 比较（可传归一化 sed 抹去部署期替换字段如 srs.conf `candidate` 的真实 IP，避免每次误判差异）、差异时备份 `*.bak-时间戳` 后 `install -m 0644` 覆盖、并提示单文件 bind mount 需重启/重建受影响容器；重写 `ensure_nginx_conf`（同步 + 保留 `upgrade_nginx_rtc_upstream` 兜底）与 `ensure_srs_conf` 复制部分（同步 + candidate 归一化，原占位符对齐逻辑保留在后）。
2. **`frontend/src/api/request.ts`**：请求拦截器改为异步，Access Token 剩余有效期不足 60s（`TOKEN_REFRESH_AHEAD_MS`）且有 Refresh Token 时，先 `await refreshAccessToken()`（复用既有单飞）再放行，保证 `/user/refresh` 发生在网关会话键存活期内；刷新失败仍以原 Token 放行，由响应拦截器 1003 兜底路径处理。未动服务端/契约（G-04① 绝对 30min 语义不变；会话键滑动续期需改 redis-keys.yaml，登记待办）。
3. **`scripts/install.sh`**：新增 `port_owned_by_host_mode_srs` 豁免（判据与 common.sh `hc_foreign_srs_pids` 同源：hunter-srs 存在且 NetworkMode=host，且该端口监听进程的 `/proc/<pid>/cgroup` 含其容器 ID → 视为本项目占用），接入 `port_owned_by_project` 的回退链；非 SRS 的外部占用仍照常拒绝。
4. **`infra/deploy/docker-compose.yml` + `infra/deploy/.env.example`（t5 TURN 落地硬前提）**：remote-control 服务补透传 `RC_TURN_URLS`/`RC_TURN_USERNAME`/`RC_TURN_SHARED_SECRET`（默认空=不启用；config.py 早已支持三键+短期凭据 TTL，之前仅 compose 漏传，设了 .env 也不生效）；顺手修残留：`.env.example` 的 `RC_STUN_URLS` 仍写着 `stun:CHANGE_ME_SERVER_IP:8000`（V1.21.10 已证伪的配置，compose 默认虽已置空，但新环境从 example 生成的 `.env` 会再次写入错值）→ 改为置空并加警告注释。

向后兼容：
- install.sh 可重复执行；首次覆盖旧副本前自动备份，可回滚；未部署环境行为不变（副本缺失时仍为创建）。
- 前端仅改请求封装，无 API/契约变更；单文件改动重建 web-portal 镜像即可。

风险：低。
- sync_runtime_config 会覆盖运行时副本的人工临时改动（已用时间戳备份兜底）。
- 前置刷新把 refresh 失败面从「到期后」提前 60s；若 refresh 端点故障，请求仍以原 Token 发出，由原 1003 路径接管，行为不回退。
- 多标签同刻双刷窗口未彻底消除（需 storage 事件同步，待办），但 60s 提前量使败方重试时已能读到胜方刷新后的新对，显著收敛。

验证：
1. `bash -n scripts/install.sh` 通过（含端口预检 host 模式豁免函数）；临时脚本本地验证 `sync_runtime_config` 四情形：candidate 占位符与真实 IP 差异不误判、正文差异才同步、覆盖前产生备份、目标缺失时创建（验后删除）。
2. `npm run type-check`（vue-tsc --noEmit）通过；`docker-compose.yml` 经 PyYAML 解析验证（19 服务，remote-control env 四个 RC_TURN_*/RC_STUN_URLS 键位确认）。
3. ⚠ 两项均为本地验证，**实机待回验**：① 跑 install.sh 后 `grep -n srs_upstream config/nginx.conf` 应为 172.28.0.1:9090 且日志出现「已从仓库更新」；② 登录→页面保持轮询 >35min → 应静默续期不再 401。

实机执行（按序）：
```bash
sudo -i; cd /opt/HunterCore
git pull
# 1) 配置同步（不再需要手动 cp；step 4 会自动同步并备份旧副本）
bash scripts/install.sh --step 4 -y
grep -n srs_upstream config/nginx.conf   # 应 172.28.0.1:9090，日志应有「已从仓库更新」
docker restart hunter-web                # 单文件 bind mount 需重启生效
# 2) 前端重建（request.ts 前置刷新）
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . build web-portal
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . up -d --force-recreate web-portal
# 3) 回验：登录一次，页面保持打开轮询 >35min，观察 Network 应在到期前自动出现一次 POST /user/refresh=200，无 401 风暴
```

分发：代码入库。受影响文件：
- `scripts/install.sh`（随部署包分发；已在仓库不需重装，下次跑 install.sh 自动生效；含端口预检 host 模式豁免）
- `frontend/src/api/request.ts`（重建 web-portal 镜像）
- `infra/deploy/docker-compose.yml`（RC_TURN_* 透传；启用 TURN 时需 up -d remote-control 重读 env）
- `infra/deploy/.env.example`（RC_STUN_URLS 置空 + TURN 注释）

待办（如实标注）：① **WS 撑不住 71s：心跳链路已实机证实健康**——除上述 duration_s=887 旁证外，13:55–13:56 直采 `last_heartbeat_at` 两次样本（间隔 40.01s）递增量与墙钟严格一致且紧贴采样时刻，连接期间心跳每 10s 正常落库（注：观察命令的 Redis 口令需从 `docker inspect .Config.Cmd` 提取——compose 是宿主侧展开 `${REDIS_PASSWORD}` 进 `--requirepass`，容器内并无该环境变量，早前给的手解 .env 命令因 WRONGPASS 作废）；**定性收敛为「浏览器侧 control WS 为何断开」单点**，待 DevTools WS 关闭码（1006=无 close 帧/网络或标签页冻结、4001=会话先没、1008=策略拒绝）与断开瞬间标签页前后台状态取证；未确诊前不得标已修复；② `/user/refresh` 方案 A 已落地（本地验证，实机回验待做：14:02 那次 `--step 4` 被预检阻断，前端重建与回验需重跑），多标签 storage 同步与服务端会话键滑动续期（需改 redis-keys.yaml 契约）未做；③ 车端 640×480 契约偏差属 HunterEdge 侧，本仓库无代码可改，已登记；④ 跨网段 TURN：compose 透传缺口已补（修复内容 4），**coturn 本体尚未部署验证**——启用步骤：apt 装 coturn（use-auth-secret/static-auth-secret 与 RC_TURN_SHARED_SECRET 一致、relay 端口段 49160-49300、external-ip）→ ufw 放行 3478/tcp+udp 与 relay UDP 段 → .env 填 RC_TURN_URLS=turn:<IP>:3478?transport=udp 与密钥 → `up -d remote-control`；判据：浏览器 ICE 统计出现 relay candidate 且 connected；另 K8s 清单未注入任何 RC_TURN_*/RC_STUN_URLS（compose↔k8s 对称性待确认）；⑤ 真实摄像头接入属车端推流侧（非本仓库代码）；⑥ 操控台 DevTools 告警「Incorrect use of `<label for=FORM_ELEMENT>`」：Element Plus 已知行为——el-form-item 生成的 label for 指向 el-slider 包装 div[role=slider]（非可标注元素），无功能影响，消除需去 show-input 改文本显示值（低优先，待上游修复）；另登录页控制台 CSP 报错经取证为浏览器扩展（AI 助手 inject.js/content.js）注入 inline script 被平台 `script-src 'self'` 正常拦截，非平台缺陷、不处置。

## V1.21.12（新增：MINIO_PUBLIC_ENDPOINT 双 endpoint——预签名 URL host 与内部连接地址分离，根治车端 rosbag 直传不可达）

Date：2026-10-17
变更类型：新增

现场：
1. 车端 rosbag 上传为**预签名直传**流程（契约 x-hunter-file-upload-flow）：`POST /api/v1/data/files/presign` → 车端拿 MinIO 预签名 URL 直接 PUT（不经平台转发）→ `POST /api/v1/data/files/complete`。而预签名 URL 嵌入的 host 就是服务侧连接用的 `MINIO_ENDPOINT`（compose 下发 `minio:9000`、K8s 下发集群内 DNS）——该地址车端网络不可路由，第 2 步 PUT 直接超时失败。

根因：单 endpoint 同时承担两个角色——服务侧高速内网连接与下发给车端的可达地址；平台无任何机制把二者分离，旧版 `infra/deploy/docker-compose.yml` 的 business-env 也未透传任何外部地址变量（即使 .env 设了也进不了容器）。

修复内容（业界标准的双 endpoint 方案）：
1. **`common/python/hunter_common/config.py`**：新增 `minio_public_endpoint`（可空）与 `minio_public_endpoint_url` property——预签名 URL 生成时使用的 host：配置了 `MINIO_PUBLIC_ENDPOINT` 则用之，未配置/空白回落 `minio_endpoint_url`（单机同网段兼容）；scheme 补全规则与内部地址一致（依 `minio_secure`，已含 scheme 原样）。
2. **`services/data-collector/app/repositories/storage.py`**：`MinioStorage` 双 client 化——`__init__(internal_client, presign_client=None)`（未传回落单 client，兼容旧测试注入）；`presign_put`/`presign_upload_part`/`presign_get` 走 `_presign_client`（host = 车端可达地址），服务侧操作（head/流式哈希/打标/列举/分片会话创建与合并）仍走内部 client；`get_storage` 按 `minio_endpoint_url` / `minio_public_endpoint_url` 各建一个 boto3 client。
3. **环境配置四源补齐 `MINIO_PUBLIC_ENDPOINT`**：`infra/deploy/docker-compose.yml` business-env 透传 `${MINIO_PUBLIC_ENDPOINT:-}`（**关键一环**，缺它整套机制静默失效）；`infra/deploy/.env.example` 新增 `MINIO_PUBLIC_ENDPOINT=CHANGE_ME_SERVER_IP:9000`（含「不可填容器名 minio」警告）；根 `.env.example` 新增留空项（开发同机回落）；`infra/k8s/base/01-configmap-common.yaml` 新增 `CHANGE_ME_MINIO_PUBLIC_ENDPOINT` 占位（含 Ingress/LB 暴露说明，经 envFrom 自动注入全部业务服务）。
4. **契约**：`contracts/openapi/data-collector.yaml` 的 `required_env` 登记 `MINIO_PUBLIC_ENDPOINT`，`x-hunter-file-upload-flow.presign` 新增 `url_host` 条款（预签名 URL host 取公网 endpoint、未配置回落内部地址）。
5. **单测**：新增 common 配置 3 条（scheme 补全/空白回落/双地址分离，`test_config.py`）+ data-collector 双 client 行为 4 条（预签名三法只走 presign client、服务侧五操作只走 internal、单 client 注入向后兼容、`get_storage` 按两个 endpoint URL 各建 client，新增 `app/tests/test_storage_presign.py`）。

向后兼容：
- 未配置 `MINIO_PUBLIC_ENDPOINT` 时预签名 host 与旧版完全一致（回落内部地址），行为零变化；存量 `.env` 无需强制新增。
- `MinioStorage` 构造器兼容单 client 注入；其他服务（ota/remote-control/scene/analytics/ingest）存储层未改，business-env 多出的空变量对其无影响（extra 字段被各自 config 忽略或仅作未使用项）。
- ⚠ 双 client 与配置字段属代码/共享库变更：data-collector 镜像**必须 `--build` 重建**（hunter_common 装入 site-packages，仅 restart 无效），且 compose 需先 git pull（旧版 business-env 不透传该变量）。

风险：低。
- 切换后 presign 响应 URL 的 host 变化：已签发未用完的旧 URL（有效期 ≤1h）仍指向旧 host，升级窗口内需关注在途上传（实机同网段情形 host 不变，无感）。
- 配错值（如容器名 `minio`、车端不可达 IP）仍会直传失败——判据与处置已写入故障排查指南场景12；`.env.example` 已加显著警告。

验证（本地，实机待回验）：
1. pytest 全量 **401 passed + 新增 7 条**（common 52 / data-collector 含新文件全绿）；ruff 涉及文件全部通过（仓内另有 5 条 UP046/UP047 存量告警在未改动文件）。
2. `scripts/verify_infra.py` 全部通过（含「共享 ConfigMap 键与 hunter_common.config 契约一致」）；`scripts/verify_data_layer.py` 34 项全通过。
3. ⚠ 存量失败 1 条（非本次引入、未处置）：`test_readyz_reports_dependency_state[ota-service]` 期望 /readyz 探测集 {database,redis} 而实现含 minio（历史提交引入，待单独定口径）。

实机执行（按序，单机部署）：
```bash
sudo -i; cd /opt/HunterCore
git pull
# 1) .env 补变量（已部署环境不会自动获得，需手工追加；SERVER_IP 取车端可路由地址）
grep -q '^MINIO_PUBLIC_ENDPOINT=' .env || echo 'MINIO_PUBLIC_ENDPOINT=192.168.31.35:9000' >> .env
# 2) 重建 data-collector（共享库+存储层均为代码变更）并 recreate 注入 env
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . up -d --build --force-recreate data-collector
# 3) 判据（⚠ 容器名是 hunter-collector，不是 hunter-data-collector——初版命令写错已纠）
docker exec hunter-collector env | grep MINIO_   # MINIO_ENDPOINT=minio:9000 不变；MINIO_PUBLIC_ENDPOINT 应为外部地址
curl -sS -o /dev/null -w '%{http_code}\n' http://192.168.31.35:9000/minio/health/live   # 宿主侧应为 200
```
回验收口：车端（或跨网段模拟机）跑一轮完整 presign→PUT→complete，PUT 不再超时；服务日志确认 head/tagging 仍走内部地址。

实机回填（2026-10-17）：步骤 1）2）已完成——镜像 `hunter/data-collector:0.1.0` 重建成功（builder 含新共享库），`hunter-collector` 拉起、依赖中间件全 Healthy；宿主侧 `curl http://192.168.31.35:9000/minio/health/live` = **200**（外网地址可达确认）。步骤 3）的 env 判据命令因容器名写错（hunter-data-collector）报 No such container，**待用正确命令重跑**；presign→PUT 闭环尚未跑。

分发：代码入库。受影响文件：
- `common/python/hunter_common/config.py`（随镜像分发，data-collector 重建即含；其他服务镜像可不重建——新字段对其无行为影响）
- `services/data-collector/app/repositories/storage.py`（需 `--build data-collector`）
- `infra/deploy/docker-compose.yml`（business-env 透传；仅改 env 需 recreate 才生效）
- `infra/deploy/.env.example` / 根 `.env.example` / `infra/k8s/base/01-configmap-common.yaml`（配置模板，K8s 渲染时占位必须替换）
- `contracts/openapi/data-collector.yaml`（契约登记，无运行时影响）
- `common/python/tests/test_config.py` / `services/data-collector/app/tests/test_storage_presign.py`（新增单测）
- `docs/`：01-部署概述（文件流+生产配置项表）、deployment.md（K8s ConfigMap 表）、运维手册 §7.5.1（变更操作）、故障排查指南场景12（车端直传超时判据）

待办（如实标注）：① **ota-service 下载预签名（车端拉升级包）与 remote-control 录像下载预签名为同构问题**（仍单 client、host=内部地址）——本次按需求只改 rosbag 上传链路，其余服务接入双 client 待确认优先级后统一跟进；② K8s 侧 MinIO 对外暴露（Ingress passthrough/LB）尚未交付，`CHANGE_ME_MINIO_PUBLIC_ENDPOINT` 真实值待环境定案；③ 本次全部为本地验证（单测+校验器），**实机 presign→PUT 闭环未跑**，回验前不得在真实车队环境断言已解决；④ V1.21.11 实机回验（install.sh 配置同步、前端前置刷新）与本条并行待做。

## V1.21.13（新增：车端 REST 设备认证通道——mTLS 设备证书复用 Kafka 接入包，pending #6 决议落地）

Date：2026-10-17
变更类型：新增

现场：
1. 车端 rosbag 上传的 presign/complete 两个 REST 调用要过 api-gateway 鉴权，但**车端拿什么凭据签 REST 请求从未定义**：契约仅写「车端 mTLS 直连是 pending #6，先按网关身份头 require_read_permission 路线走」；Kafka 接入包里的 mTLS 客户端证书/SCRAM 口令是给 Kafka 的，REST 网关的 bearer/身份头无来源。取证证实缺口比表面更深：`deviceCertificate` 安全方案在平台侧**无任何消费方**（网关无 device/cert 相关代码），车端今天实际上无法调用 presign/complete。
2. 顺带发现存量实现缺口：presign 对请求体 `vehicle_id` 无身份一致性校验——任何持 data:read 身份者可为任意车辆申请预签名（写入他车目录）；complete 虽已有 `server_key` 车辆前缀校验，但缺「vehicle_id 必须等于调用方身份」约束。

根因：设备主体认证链路整体未设计未实现（pending #6 悬而未决）；网关只有 JWT 一条认证路，后端只认「网关注入头 + G-02 HMAC」，而 HMAC 链路的前提是网关能确定调用方身份——车端无 JWT 账号，链路对设备断裂。

修复内容（主体模型 A 定案：mTLS 设备证书直接作为 REST 认证，与 Kafka 接入包**同一张证书/同一 CA**，SCRAM 不进 REST）：
1. **契约先行**：`contracts/openapi/api-gateway.yaml`——route_headers 清单增 `X-Device-Vehicle-Id`；`deviceCertificate` 决议回填；新增 `x-hunter-device-ingress` 扩展节（entry 单机 nginx :8443 / K8s P2、trust_model 双重证书校验、device_paths 白名单、identity.claims/injected_headers/enforcement、config `DEVICE_CA_CERTFILE`、rate_limit）。`contracts/openapi/data-collector.yaml`——pending #6 决议回填（含二期待办 CRL/OCSP、K8s 设备 Ingress 并入 #6 impact）、presign 描述与 upload-flow steps 1/4 同步。
2. **共享库 `hunter_common/internal_auth.py`（G-02 签名向后兼容扩展）**：`canonical_identity`/`sign_identity`/`build_identity_headers` 增 `device_vehicle_id` 可选参数——有设备头时签名串追加第 5 段（`user_id|roles|trace_id|timestamp|device_vehicle_id`），**控制台通道（无设备头）保持 4 段逐字节不变**；`verify_identity_headers` 读到 `X-Device-Vehicle-Id` 则按第 5 段验签。
3. **api-gateway 设备认证**：新建 `app/core/device_auth.py`（157 行）——仅 `DEVICE_ALLOWED_PATHS`（presign/complete）且携带 `X-Client-Cert` 进入设备分支；用 `DEVICE_CA_CERTFILE` 指向的 CA 本地**重验**证书链（`verify_directly_issued_by` → 有效期 → clientAuth EKU → CN 非空=vehicle_id），任一失败统一 1001（日志仅记 reason/path，证书 PEM 禁止落日志）；CA 未配置/文件缺失 → `device_channel_disabled`/`ca_unavailable` 显式拒绝（fail-closed，不静默回落 JWT）。通过则构造设备 claims `{sub: "device:{vehicle_id}", roles: ["device"], type: "device", vehicle_id}`。`routers/proxy.py` 设备分支优先于 JWT；`proxy_service.py` 请求侧剥离 `x-device-vehicle-id`/`x-client-cert`（防直连伪造）、响应侧设备主体注入 `X-Device-Vehicle-Id`（HMAC 第 5 段签名覆盖）。配置 `device_ca_certfile`/`device_role`；**pyproject 显式新增 `cryptography>=42,<47`**（PyJWT 走 HS256 无 crypto extra，证书链验签必须独立依赖）。
4. **data-collector 双主体上传依赖**：`core/dependencies.py` 新增 `UploaderPrincipal` + `require_uploader_identity`——设备主体（roles 含 device 或带设备头）：角色与设备头必须成对且角色恰为 `{device}`，否则 1002；控制台主体沿用 data_read 白名单（代传路线保留）。`routers/files.py` presign/complete 换用该依赖；`services/files.py` 新增 `_validate_device_vehicle`：设备调用时请求体 `vehicle_id` 必须等于 `X-Device-Vehicle-Id`（即证书 CN），不符 1002（complete 中排在 `server_key` 前缀校验之前）。配置 `device_role: str = "device"`。**device 角色不加入 data_read/data_execute 白名单** → 设备请求其余端点经 `require_read_permission` 天然 1002（403），能力面收敛为两端点。
5. **部署链路**：`infra/deploy/config/nginx.conf` 新增 8443 mTLS server 块——`ssl_verify_client on` + `ssl_client_certificate /etc/nginx/certs/ca-cert.pem`（与 Kafka 同一 CA），服务端证书复用 broker-cert.pem/broker-key.pem（**SAN 需含车端访问地址 SERVER_IP**，IP 变更走 gen-kafka-certs.sh 重签流程，车端 CA 未变无需换客户端证书），两个 `location =`（presign/complete）`proxy_set_header X-Client-Cert $ssl_client_escaped_cert` → api-gateway，其余路径一律 404（不泄露资源存在性）。`docker-compose.yml`：web-portal 端口 `${VEHICLE_REST_PORT:-8443}:8443` + `./certs/kafka:/etc/nginx/certs:ro`；api-gateway `DEVICE_CA_CERTFILE=${DEVICE_CA_CERTFILE:-/srv/device-ca/ca-cert.pem}` + `./certs/kafka/ca-cert.pem:/srv/device-ca/ca-cert.pem:ro`（单文件 ro 挂载缺失时退化目录仍 fail-closed）。`.env.example` 登记 `VEHICLE_REST_PORT=8443` 与 DEVICE_CA_CERTFILE 说明。
6. **测试**：新增 `services/api-gateway/app/tests/test_device_auth.py`（运行期 EC P-256 生成 CA+设备证书链，覆盖 valid/异 CA/过期/EKU 缺失/无 CN/不可解析/通道禁用/白名单外回落/转发头注入与 HMAC 第 5 段验签/PEM 不外泄/伪造设备头被覆盖）+ `services/data-collector/app/tests/test_device_identity.py` 10 条（双主体依赖、vehicle_id==CN 强校验、device 角色查询端点 1002）；`common/python/tests/test_internal_auth.py` +4 条设备通道用例；契约护栏同步（route_headers 断言、`x-hunter-device-ingress` ↔ 实现一致性测试）。

向后兼容：
- 控制台路线零变化：无设备头时签名串 4 段逐字节不变、presign/complete 控制台代传路线行为不变；未部署 8443/未配 DEVICE_CA_CERTFILE 的环境，设备通道不生效且不带证请求完全照旧。
- ⚠ 滚动升级窗口：新网关对**设备请求**签 5 段、旧 data-collector 镜像按 4 段验 → 设备上传 401 属预期 fail-closed；控制台流量不受影响。data-collector 与 api-gateway 需同批 `--build`（各自镜像内置 hunter_common）。
- nginx 8443 为新增 server 块，80 端既有配置未动；`X-Client-Cert`/`X-Device-Vehicle-Id` 在网关转发层剥离重注入，对存量后端透明。

风险：低~中（新增通道，存量路径不改语义）。
- 信任模型依赖「CA 私钥不外泄」：伪造 `X-Client-Cert` 无 CA 私钥无法通过重验；网关 :8080 直连伪造 `X-Device-Vehicle-Id` 会被剥离覆盖。
- 设备证书吊销暂无机制（CRL/OCSP 为契约 #6 二期待办）——证书泄露车辆需人工下线，已如实登记。
- 8443 服务端证书复用 broker-cert.pem：若既有 broker-cert 的 SAN 不含车端实际访问地址（SERVER_IP），车端 TLS 握手会校验失败——属部署配置约束，实机执行步骤已含核对命令。

验证（本地，实机待回验）：
1. 分服务独立进程 pytest：api-gateway **85 passed**、data-collector **163 passed**、hunter_common **333 passed**；`scripts/run_unit_tests.py` 10 目标全过（前次登记的 ota readyz 存量失败已不在）。
2. `scripts/verify_infra.py` 20 项全过（含共享 ConfigMap 键与 hunter_common.config 契约一致）；`scripts/verify_data_layer.py` 34 项全过。
3. ruff 涉及文件全部通过（新文件 5 条 I001/FURB167/UP017 已 --fix；仓内另有 5 条 UP046/UP047 存量告警在未改动文件，不处置）。
4. `infra/deploy/docker-compose.yml` PyYAML 解析校验：19 服务，DEVICE_CA_CERTFILE env/CA 单文件挂载/8443 端口映射/certs 目录挂载均正确透传。
5. 顺手修正两个存量测试问题（如实登记，git status 证实非本次引入）：`test_rate_limit.py` retry-after 断言 60→1（V1.21.9 改窗漏同步）；`test_ingest_pipeline.py` 毫秒换算用例 NOW*1000 动态基准浮点往返 flaky → 固定常量。

实机执行（按序，单机部署）：
```bash
sudo -i; cd /opt/HunterCore
git pull
# 1) 核对 broker-cert SAN 含车端访问地址（不含则按 scripts/gen-kafka-certs.sh 更新 SAN 重签，CA 不变车端无需换证）
#    ⚠ 证书不在 infra/deploy/certs（那里从未存有产物）；compose 的 ./certs/kafka 经 --project-directory . 解析到**部署根** certs/kafka，即 .env 的 KAFKA_CERTS_DIR
set -a; . ./.env; set +a
openssl x509 -in "${KAFKA_CERTS_DIR:-$PWD/certs/kafka}/broker-cert.pem" -noout -ext subjectAltName
#    找不到文件时先定位：ls certs/kafka/broker-cert.pem  或  find / -name broker-cert.pem 2>/dev/null
# 2) 重建三镜像并 recreate（api-gateway/data-collector 为代码变更，web-portal 为 nginx 配置变更）
docker compose -f infra/deploy/docker-compose.yml --env-file .env --project-directory . up -d --build --force-recreate api-gateway data-collector web-portal
# 3) 防火墙放行 8443（不做 IP 过滤——设备面由 mTLS 证书重验把关，非白名单/异 CA 证书一律 1001，不依赖网段限制）
#    V1.21.13 已把 8443/tcp 纳入 install.sh 的 UFW_RULES（与 9093 并列），全新部署自动放行；已部署环境免重跑全流程时手工补下面一行（幂等）
ufw allow 8443/tcp
# 4) 判据（容器名 hunter-api-gateway / hunter-web，不可臆造）
docker exec hunter-api-gateway env | grep DEVICE_CA   # DEVICE_CA_CERTFILE=/srv/device-ca/ca-cert.pem
docker exec hunter-api-gateway ls -l /srv/device-ca/ca-cert.pem   # 应为单文件而非目录（宿主侧源文件："${KAFKA_CERTS_DIR:-$PWD/certs/kafka}/ca-cert.pem"）
curl -sS -k --cert client-cert.pem --key client-key.pem https://SERVER_IP:8443/api/v1/data/files/presign -X POST -d '{}'   # 无证书应被 nginx 拒绝；带证应进网关设备分支
```
回验收口：车端（或接入包证书模拟机）跑完整 mTLS presign→PUT→complete；用一张异 CA 证书验证网关重验拒绝（1001 signature_invalid）；设备 token 请求查询端点验证 1002。

实机纠偏（2026-10-17，步骤 1 首跑发现）：本条目初版实机命令把证书核对路径写成 `infra/deploy/certs/kafka/broker-cert.pem`，实机报 `No such file or directory`——**命令自身写错，非环境问题**。已核实：证书只在部署根 `certs/kafka`（= `.env` 的 `KAFKA_CERTS_DIR`）；compose 的 `./certs/kafka` 是相对路径，经 `dcompose` 的 `--project-directory .`（= 部署根）解析，`infra/deploy/` 下从未存有证书产物。步骤 1 已改为 `set -a; . ./.env; set +a` 后用 `${KAFKA_CERTS_DIR:-$PWD/certs/kafka}/broker-cert.pem`；故障排查指南场景 15 的 `ca_unavailable` 处置同步纠正。（SAN 实际核对与后续步骤仍待重跑。）

分发：代码入库（`git status` 列 contracts/common/services/infra/scripts 改动）。⚠ 本仓库现行规则（`.gitignore` L157-159）：`docs/` 与 `release.md` **只更新、不追踪**（与 V0.1.1 条目 5「纳入版本控制」相反，以现行为准）——下方 `docs/` 行所列四件套为本地同步更新，不进仓。受影响文件：
- `contracts/openapi/api-gateway.yaml` / `contracts/openapi/data-collector.yaml`（契约登记与 pending #6 决议，无运行时影响）
- `common/python/hunter_common/internal_auth.py`（随各镜像分发——**网关与 data-collector 必须同批重建**）
- `services/api-gateway/`：`app/core/device_auth.py`（新增）、`app/routers/proxy.py`、`app/services/proxy_service.py`、`app/config.py`、`pyproject.toml`（cryptography 依赖）、`app/tests/test_device_auth.py`、`app/tests/test_openapi_contract.py`（需 `--build`）
- `services/data-collector/`：`app/core/dependencies.py`、`app/services/files.py`、`app/routers/files.py`、`app/config.py`、`app/tests/test_device_identity.py`（需 `--build`）
- `infra/deploy/config/nginx.conf`（8443 server 块）、`infra/deploy/docker-compose.yml`（端口/挂载/env）、`infra/deploy/.env.example`
- `scripts/install.sh`（UFW_RULES 新增 `8443/tcp`，与 9093 并列；同时进入端口预检清单——全新部署自动放行 8443，无需手工）
- `common/python/tests/test_internal_auth.py`、`services/api-gateway/app/tests/test_rate_limit.py`、`services/data-collector/app/tests/test_ingest_pipeline.py`（测试）
- `docs/`：01-部署概述（8443 端口与车端 REST 通道）、deployment.md（单机配置表/K8s 待 P2 注记）、运维手册（证书复用与变更步骤）、故障排查指南（设备通道 1001 各 reason 判据、8443 排障路径）

待办（如实标注）：① **实机 8443 mTLS presign→PUT→complete 闭环未跑**（本次全部为本地验证），回验前不得在真实车队环境断言已解决；② P2：K8s 设备 Ingress（mTLS passthrough）+ NetworkPolicy 放通未交付；③ 设备证书吊销（CRL/OCSP）与车辆台账下线联动未实现（契约 #6 impact 已登记）；④ 控制台代传路线的 vehicle_id 一致性校验仍为存量待办（dependencies docstring 注明，本次只收敛设备通道）；⑤ V1.21.12 实机回验（env 判据重跑 + PUT 闭环）与本条并行待做。

## V1.21.14（修复：Kafka 消费组积压面板「四个组全灰」的可观测性缺口——`unknown_groups` 由裸组名升级为带成因对象，看板按成因逐行展示、不再画成零长条；属**契约变更**）

Date：2026-10-17
变更类型：修复（**契约变更**：`contracts/openapi/data-analytics.yaml` 的 `unknown_groups` items 由 `string` 变 `object`）+ data-analytics 后端（schema/repository/service）+ 前端（类型/视图）+ 测试。⚠ 不改 DB / Kafka / compose 拓扑；后端改动在镜像内 ⇒ 生效需 `--build data-analytics`，前端需一并重建 `web-portal`）

现场（运营看板「Kafka 消费组积压」面板）：四个消费组全部显示灰条「未判定」，整块形同**「没有数据」**——运维据此无法判断是作业没跑、还是位点读不到，只能翻服务日志找 `consumer_group_lag_undetermined` 的 `cause=`。

先行澄清（避免误判面板/接线有问题）：**面板没坏，读数也没错**。四个 `data-analytics-*` 组（三个 Flink 作业的 telemetry 组 + `data-analytics-algorithm-metrics`）在契约里都属「已交付」，但确实**没有提交位点**；契约自 V1.19.14 起就固定「未判定 ≠ 零积压」，V1.19.14 把这三类组从 `kafka_consumer_lag` 剔出、只列入同级的 `unknown_groups`（当版本类型是 `list[str]` 纯组名）。

根因（**可观测性缺口**，不是判定写错）：V1.19.14 修掉了「空位点折算成 0」的假绿，但把成因只留在服务日志事件 `consumer_group_lag_undetermined` 的 `cause=` 里；
- `unknown_groups` 只有组名 ⇒ 面板对同一批组只能显示**同一个「未判定」**；
- 前端把未判定行画成 **0 长度的灰条**（与「一条真积压为 0 的组」视觉上无从区分）；
- 全组未判定时数值轴退化成 **0~1 空轴** —— 三者叠加正是现场「没有数据」的观感来源，运维分不出「作业从未提交位点」与「位点读不到」（两者修法完全不同）。

修复内容（**后端补成因 + 前端按成因展示**，共 **11 个受追踪文件、无新增**）：
- **A 后端探测侧 `app/repositories/pipeline.py`**：新增 `@dataclass(frozen=True) UndeterminedCause(label, hint)` + 受控词表 `UNKNOWN_GROUP_CAUSES`（四值：`group_not_found` / `no_committed_offsets` / `watermark_unavailable` / `offset_fetch_failed`，与日志 `cause=` 同源）+ `undetermined_cause(code)`（**未知码原样回显，绝不兜成「零积压」式默认结论**）；`_lag_sync` 第二返回值由 `list[str]` 升级为 `dict[str, str]`（group → 成因码）——`offset_fetch_failed` 从此也进返回值（过去只进日志）；`PipelineSource.consumer_lag` / `PipelineRepository.consumer_lag` 签名同步。
- **B 响应 schema `app/schemas/dashboard.py`**：新增 `UndeterminedGroup(group, cause, cause_text, hint)`；`PipelineStats.unknown_groups` 由 `list[str]` 改为 `list[UndeterminedGroup]`（**字段仍带默认空列表，非必填**）。
- **C 服务层 `app/services/dashboard.py`**：新增 `undetermined_groups(probe)`——**只查表翻译、不重新判定**（判定与归因一律留在探测侧），装配进 `PipelineStats`。
- **D 前端 `types/analytics.ts`**：新增 `UndeterminedGroup` 接口，`PipelineStats.unknown_groups: UndeterminedGroup[]`。
- **E 前端 `views/DashboardView.vue`**：重写 `kafkaLagOption`——① 未判定行**不再画 0 长度灰条**（`itemStyle.color: transparent`）；② 类目名整体压暗（`textStyle: {color:'#8298b3'}`，确定性做法，弃用按类目小数下标铺灰底行带的 `markArea`——SSR 实测行带错位、每带约 2 行高）；③ 行内标签由「未判定」改为 **`未判定 · ${cause_text}`**；④ 悬浮补 `hint` 与「⚠ 未判定 ≠ 零积压」；⑤ **全组未判定时收起数值轴**（`xAxis.show = knownEntries.length > 0`）；新增 `kafkaLagNote` computed 让抬头发声（全未判定时「全部 N 个消费组未判定（成因见行内；未判定 ≠ 零积压）」）。
- **F 契约 `contracts/openapi/data-analytics.yaml`**：`unknown_groups.items` 由 `{type: string}` 改为 `object`（`required: [group, cause, cause_text, hint]`，`cause` 带 `enum` 四值）+ 描述重写；`lag_semantics` 补 V1.21.14 口径（「只给组名的形态已在现场被读成『没有数据』」）。
- **G 测试**：`fakes.py`（`unknown_groups` 改 `dict[str, str]`）；`test_kafka_lag_probe.py`（四类成因逐组断言 + **新增 2 例**：探测侧吐出的每个成因码必须在受控词表内、未登记码原样回显）；`test_dashboard_endpoints.py`（条目对象形状 + 互斥 + 成因断言）；`test_data_analytics_contract.py`（契约 items 对象化、`cause` 枚举 ↔ `UNKNOWN_GROUP_CAUSES` **逐项相等**、`UndeterminedGroup` 字段集同形、`lag_semantics` 含量成语义）。

向后兼容：
- ⚠ **契约形状变更（唯一的对外不兼容点）**：`unknown_groups` 的条目由**字符串**变为**对象**。仓内读方只有本仓前端（已同批改），但**仓外/第三方**若按字符串读该字段会拿到对象（旧前端模板 `${group}` 会渲染成 `[object Object]`）⇒ 前端必须与后端**同批升级**；`kafka_consumer_lag`、`available`、其余块语义一字未改。
- 成因码是纯增量信息：`group_not_found` / `no_committed_offsets` 原本就在日志里，`watermark_unavailable` / `offset_fetch_failed` 过去只在日志里、现在一并透到响应体（**不在返回值里做任何删改**）。
- 探测判定逻辑（谁进映射、谁进 `unknown_groups`）**一条未放宽**：仍只有「确有提交位点且末端水位可取」的组进 `kafka_consumer_lag`。

风险：低（纯读侧可观测性 + 契约形状）。
- **本条全部改动为本机验证**（本环境无 Docker/无集群），四组为何无提交位点（作业未提交 / 检查点未 completed / 位点被 `offsets.retention` 清理）**未确诊**，只能靠服务器取证 —— 本轮只修可观测性，未改任何会改变 LAG 数值的行为。
- 受控词表**靠测试锁定两端**（新增码漏改契约枚举即红），但新增 `cause` 仍需人工同步 `UNKNOWN_GROUP_CAUSES` 与契约 `enum` 两处（测试已把漂移变红，不静默）。
- 抬头文案含组数，运营面板刷新时会随未判定组数变化（属预期）。

验证（本环境，均实跑）：
1. `pytest services/data-analytics -q` **115 passed**（V1.20.x 条目登记的 data-analytics 基线 **113** ⇒ **净增 2**，即本条新增的 2 条成因门禁用例），既有用例零回归。
2. `scripts/run_unit_tests.py` **10 目标全过**（ingest-platform 187 passed，其余目标同基线）。
3. `scripts/verify_infra.py` **全部校验通过**（第 19 项：已解析 **183** 个函数、**33** 个被捕获调用；第 20 项步骤分派对账仍 13 步 / 13 描述 / 13 case 臂）。
4. `scripts/verify_data_layer.py` **通过 34 项、失败 0 项**（退出码 0；Redis 键 / 对象存储 / Kafka Topic 与消费者组契约均未受影响）。
5. `ruff check services/data-analytics/app` **All checks passed**。
6. `npm run type-check`（`vue-tsc --noEmit`）**零输出通过**（覆盖新 `UndeterminedGroup` 与对象化 `unknown_groups` 在模板里的取值）。
7. SSR 渲染验证（**临时脚本已删除**，用 `echarts` 直渲染 SVG 核对）：**全未判定**场景无渐变柱、无数值轴刻度、4 条「未判定 · 成因」行内文字、类目名全部压暗；**混合**场景仅未判定行压暗、保留真实柱与 0~7 数值轴。

分发路径注意：受追踪 **11 个**、**无新增文件**（`git status` 无 `??`）⇒ `git add -u` 即可覆盖：
- `contracts/openapi/data-analytics.yaml`（契约，**形状变更**）
- `frontend/src/types/analytics.ts`、`frontend/src/views/DashboardView.vue`
- `services/data-analytics/README.md`（口径段同步）+ `app/repositories/pipeline.py`、`app/schemas/dashboard.py`、`app/services/dashboard.py`、`app/tests/{fakes.py,test_dashboard_endpoints.py,test_data_analytics_contract.py,test_kafka_lag_probe.py}`（后四者需 `--build data-analytics`；前端需重建 `web-portal`）

HEAD = `7b55f08`（V1.21.13 已提交）⇒ 本条 **11 个文件均未提交**，服务器 `git pull` 取不到；`release.md` 与 `docs/` 照旧被 `.gitignore:157-159` 命中 ⇒ 单独 rsync。

文档同步（本条已落，本地更新不进仓）：
- `docs/故障排查指南.md`：场景 7「看板 LAG 与集群实况对账」的**读法段**（1086 行起）重写——条目对象化 + 四种 `cause` 的**含意/处置表**（`group_not_found`→查作业是否提交过、`no_committed_offsets`→查检查点是否 `completed>0`、`watermark_unavailable`→查分区 leader/网络、`offset_fetch_failed`→查协调器/ACL）+ 「条目仍是纯字符串 = 跑的旧版」判据。
- `docs/deployment.md` §（V1.19.14 注之后）新增 **V1.21.14 段**：未判定只给组名等于没给、四值分流、契约变更与重建要求。
- `docs/运维操作手册.md`（V1.19.14 注之后）新增 **V1.21.14 段**：看板自本版起不必翻日志就能分流，逐 `cause` 的处置。
- `services/data-analytics/README.md`：`pipeline` 块读数口径段补对象形状与四值受控词表、前端按成因展示。

服务器实机步骤：
```bash
cd /opt/HunterCore
# 1) 同步代码（11 文件）；docs/release.md 单独 rsync（不进 git）
# 2) 重建后端 + 前端（后端改动在镜像内，前端契约形状同批升级，缺一不可）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics web-portal
# 3) 判据①：响应体里 unknown_groups 已是对象（含 cause/cause_text/hint）
sudo docker exec -i hunter-analytics python - <<'PY'
import json, urllib.request as ur
from app.config import settings
req = ur.Request("http://localhost:8083/api/v1/analytics/dashboard", headers=settings.internal_call_headers())
with ur.urlopen(req) as r:
    p = json.load(r)["data"]["pipeline"]
print(json.dumps(p["kafka_consumer_lag"], ensure_ascii=False))
print(json.dumps(p["unknown_groups"], ensure_ascii=False, indent=2))   # 期望：[{group,cause,cause_text,hint}, ...]
PY
# 4) 判据②：面板不再是四条 0 长度灰条——未判定行类目名压暗 + 行内「未判定 · 成因」；全未判定时无数值轴
# 5) 集群实况对账（只看已交付四组，走 kafka_groups_cli 封装）
for g in data-analytics-telemetry-alert data-analytics-telemetry-algo \
         data-analytics-telemetry-raw data-analytics-algorithm-metrics; do
  echo "== $g"; kafka_groups_cli --describe --group "$g" | tail -n +2
done
# 6) 若判据①显示 no_committed_offsets：查 Flink 作业是否 RUNNING 且检查点 completed>0
sudo docker exec hunter-flink-jm flink list -a            # 期望 3 行 RUNNING
#    JM 检查点页 http://<SERVER_IP>:8081/#/job/<jobId>/checkpoints 看 completed 计数
```
回验收口：面板四组不再显示同一个「未判定」——每个组能看出**为什么**未判定；全未判定时数值轴收起；`unknown_groups` 条目为对象且 `cause` ∈ 受控四值。

**待办（如实标注，不得当已修）**：① **四组为何无提交位点未确诊**——三个 Flink 作业（`-alert`/`-algo`/`-raw`）只在**检查点成功后才提交位点**（`commit.offsets.on.checkpoint` 默认 true，作业内已 `enable_checkpointing()`，compose 有 `execution.checkpointing.dir` 且 JM/TM 同挂 `${DATA_DIR}/flink`），而「检查点是否 completed」无任何巡检覆盖；`data-analytics-algorithm-metrics` 是 Python 常驻消费者（手动提交）。需在服务器按实机步骤 5/6 取证定性（作业未提交 / 检查点未成功 / 位点被 `offsets.retention` 清理），**本轮只修可观测性，未改导致无位点的任何行为**。② 「检查点 completed 数」纳入巡检（`flink` REST `/jobs/<id>/checkpoints` 或 JM UI）属新巡检项，需批准后加。③ **未改的存量观感问题**：入库延迟趋势面积图 y 轴 `grid.left: 44` 对「1,000 ms」标签过窄，左侧刻度会被裁（截图可见）——与本条无关，待后续单独处理。

## V1.21.15（修复：Kafka 消费组滞后探测的真根因——`list_consumer_group_offsets` 结果对象的分区属性是 `topic_partitions`，旧代码按 `partitions` 取值恒空 ⇒ **每个组**都被误判「无提交位点/未判定」，看板因此永远没有可测的滞后；连带把单测替身改为真库类型并加形状门禁）

Date：2026-10-17
变更类型：修复（data-analytics 后端 + 单测。⚠ 不改契约、不改前端、不改 DB/Kafka/compose；后端改动在镜像内 ⇒ 生效需 `--build data-analytics`）

现场（承接 V1.21.14 上线后的实机取证）：看板「Kafka 消费组积压」四个组全显示「未判定 · 从未提交位点」，与集群实况**直接矛盾**——
```
kafka_groups_cli --describe（同机实测）：
  data-analytics-telemetry-alert     telemetry_clean p6  2397751 → 2397763  LAG 12
  data-analytics-telemetry-algo      telemetry_clean p6  2397751 → 2397786  LAG 35
  data-analytics-telemetry-raw       telemetry_raw   p6  2397727 → 2397797  LAG 70
  data-analytics-algorithm-metrics   algorithm_metrics ×6  均有 CURRENT-OFFSET 且**有活跃成员**（LAG 0）
hunter-analytics 日志（每 5s 一轮）：四个组齐刷刷 cause=no_committed_offsets
```
`--describe` 明明有位点，探测却报「无提交位点」——**这不是集群问题，是探测器读不到位点**。

根因（**字段名与真库不同形**）：`_lag_sync` 取分区列表写的是
```python
partitions = list(getattr(offsets, "partitions", None) or [])
```
而 `list_consumer_group_offsets` 的 future 结果对象是 confluent-kafka 的 **`ConsumerGroupTopicPartitions`**，
其属性**只有** `group_id` 与 **`topic_partitions`**（实测 `dir()` 确认，**根本没有 `partitions`**）。
⇒ `getattr(..., "partitions", None)` **恒为 None** ⇒ `partitions` 恒空 ⇒ **每个组**都落进
`if not partitions` 的「未判定」分支。历史沿革正好解释两代症状：V1.19.14 之前同一处空列表被折算成
`lag=0`（假绿「零积压」），V1.19.14 之后改判「未判定」——**两代都没真正读到过位点**。
`--describe` 里有位点即此故；组存在性清单（`list_consumer_groups`）取得到，故成因落在
`no_committed_offsets` 而非 `group_not_found`。

**为什么单测一直是绿的（本条真正的教训）**：`test_kafka_lag_probe.py` 的替身自造了
```python
class _Offsets:  self.partitions = partitions     # ← 按读方想象发明的字段名
```
与真库 `topic_partitions` 不同形 ⇒ 替身喂的是 `.partitions`、被测代码也读 `.partitions`，**自洽而全绿**，
真库形状从未被任何测试观察过。这与 V1.19.20/21 登记的「Fake 的字段形状必须来自**写方实际行为**，
不得按读方想象发明字段」是**同一个教训的第二次发作**（上次是 Redis Hash 字段名，这次是 AdminClient 结果对象）。

修复内容（data-analytics 后端 1 处 + 单测 2 处）：
- **A `app/repositories/pipeline.py::_lag_sync`**：`getattr(offsets, "partitions", …)` → **`getattr(offsets, "topic_partitions", …)`**
  （与真库同形），并就地写下实机确诊说明（含 `--describe` 有位点却报 no_committed_offsets 的判据）。
  **判定语义一条未改**：真库对「组不存在 / 从未提交位点」确实返回**空 `topic_partitions`** ⇒ 仍走
  「未判定」分支（V1.19.14 口径保持不变），只是这次是**真的**读了位点。
- **B `app/tests/test_kafka_lag_probe.py`**：删除自造的 `_Offsets` 与自造的条目类，
  **改用真库类型**——结果容器用 `ConsumerGroupTopicPartitions(group, [TopicPartition(...)])`、
  条目用 `TopicPartition`（`_Committed` 改为构造真 `TopicPartition`）⇒ 替身形状由库保证，**不再能凭空发明**。
- **C 新增形状门禁 `test_offset_result_uses_real_confluent_attr_name`**：直接对真库类断言
  `topic_partitions` 存在且 `hasattr(result, "partitions")` 为假；上游若改名/新增该属性即红，
  强制同步 `_lag_sync` 取值。

向后兼容：① **响应契约零变化**（`kafka_consumer_lag` / `unknown_groups` 形状与 V1.21.14 一致）；
② **惰性数据变化（属修复本身）**：修复后这四个组从「未判定」变为**真实滞后**（实机当前应为 12 / 35 / 70 / 0），
这是把「永远读不到」改成「读得到」——凡依赖「这四个组进 `unknown_groups`」的巡检/告警若存在需知晓
（本仓无此依赖；`unknown_groups` 仍按 V1.19.14 语义只在**真的**无位点时出现）；
③ 单测替身改用真库类型后，`confluent-kafka` 成为单测直接依赖（服务本就依赖它，无新增）。

风险：低（单点取值修正）。但如实登记两点：
- **本条全部为本机验证，实机未回验**：本环境无 Kafka 集群，无法跑真 `list_consumer_group_offsets`；
  已用**真库类**构造结果对象驱动真实 `_lag_sync` 判定作为最强本地替身，实机须按下方判据复验。
- 真库对该结果对象**没有** `partitions` 属性属其固有形状；若未来 confluent-kafka 改名，门禁测试会先红。

验证（本环境，均实跑）：
1. `pytest services/data-analytics -q` **116 passed**（V1.21.14 基线 115 ⇒ **净增 1**，即新增形状门禁；
   改替身形状后既有用例**零改动通过**——这正证明原逻辑在被喂真形状时本来就该出映射）。
2. `ruff check services/data-analytics/app` **All checks passed**。
3. 真库形状核对：`python -c "from confluent_kafka import ConsumerGroupTopicPartitions as C; print([a for a in dir(C('g')) if not a.startswith('_')])"`
   → `['group_id', 'topic_partitions']`（**无 `partitions`**）。
4. **反证**：若把 `_lag_sync` 改回读 `partitions`，`test_undetermined_groups_never_masquerade_as_zero_lag`
   会红（`lag` 变空）——即该用例现在真的在守这条取值。

分发路径注意：受追踪 **2 个**、**无新增文件**（`git add -u` 即可）：
- `services/data-analytics/app/repositories/pipeline.py`（**唯一产品代码改动**）
- `services/data-analytics/app/tests/test_kafka_lag_probe.py`
⚠ 前后端**无需再动**（契约/前端沿用 V1.21.14），只需重建 `data-analytics`：
```bash
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics
# 判据：重建后不再出现 cause=no_committed_offsets（除非真的无位点），看板四组显示真实滞后
sudo docker logs --since 2m hunter-analytics 2>&1 | grep -c 'cause=no_committed_offsets'   # 期望 0（位点齐全时）
curl -s http://127.0.0.1:8083/api/v1/analytics/dashboard -H "X-User-Id: analyst" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["pipeline"]["kafka_consumer_lag"])'
#   期望：四组真实数字（实机当前约 {alert:12, algo:35, raw:70, algorithm-metrics:0}）
```
回验收口：看板四组出真实积压条（不再是「未判定」），且集群 `--describe` 的 LAG 与看板一致。

**待办（如实标注）**：① 本条**实机回验未做**（本环境无集群），需按上方判据在服务器复验「四组出真实数字」；
② **Mock 纪律无静态门禁（第二次发作）**：`_Offsets` 自造字段名这类漂移仍只能靠 review 发现，
考虑加一条「替身结果对象必须构造自真库类型」的静态/约定检查（与 V1.19.20 待办⑥同源，本次仍未做）；
③ V1.21.14 的两项待办（检查点 completed 数纳入巡检；入库延迟面积图 y 轴刻度裁剪）原样挂账。

## V1.21.16（修复：运营看板/事件页四点「数据打架」+ 文件浏览筛选失效——未确认口径、游标分页 500、录像封存链路、事件类型精确聚合）

Date：2026-10-17
变更类型：修复（data-collector + data-analytics + remote-control 后端 + 契约描述/新增端点 + infra/deploy 录像封存配置。⚠ **不改前端**；后端/契约改动在镜像内 ⇒ 生效需 `--build data-collector data-analytics remote-control`，并 `--force-recreate srs`（srs.conf 为单文件 bind mount））

现场（用户四点原话，实机复现）：
1. **运营看板的「未确认事件」与事件列表不一致**：卡片主值取 `events.unacknowledged`（对），但分项「严重/警告/提示」却分别取 `events.critical/warning/info`；
2. **事件与文件页「加载更多」（游标分页）报「服务不可用」**：首页无游标正常，翻第二页必 500；
3. **操控记录页「播放录像」报错**：3001「不存在（未封存或已过保留期）」；
4. **看板「事件类型 Top 10」人工接管与事件列表不一致**（29 ≠ 721）；
   附带发现：**文件浏览的 `date`/`data_type` 筛选完全无效**。

根因（逐条）：
1. **问题①——口径错位（含已确认混入「未确认」）**：`data-analytics::_events_block` 对 `info/warning/critical` 只传 `event_level`、**未传 `acknowledged=false`** ⇒ 三项其实是「窗口内各级**总数**（含已确认）」。叠加看板窗口固定「近 24 小时」而事件列表默认无时间过滤 ⇒ 出现「主值 0 而提示 29」这类自相矛盾读数。
2. **问题②——SDK 入参形状错**：`data-collector::storage.list_objects` 用 v2 API `list_objects_v2`，却传 v1 的 `Marker` ⇒ botocore `ParamValidationError` ⇒ 首页无游标正常、**第二页必 500**。改为 v2 的 `StartAfter`。
3. **问题③——封存链路根本没接上**：remote-control 只写 sidecar（`video.size_bytes` 恒 0，注释「录像管线回写前为 0」），**从未上传过 mp4**；`hunter-video` 里没有录像对象 ⇒ `head_video` 为 None ⇒ 播放恒 3001。且 `srs.conf` 无任何 DVR 配置、存储层无 mp4 PutObject。契约本该是「SRS 录制产物由**服务端直传** MinIO」。
4. **问题④——近似采样 vs 精确计数**：`by_type` 取「窗口内最近 200 条」近似分布（`dashboard_event_fetch_size=200`），非 SQL 计数；再叠加窗口差异 ⇒ 29 ≠ 721。
   附带——**实现漏了契约声明的参数**：`GET /files` 的 router 私增 `prefix` 且**漏了契约的 `date`/`data_type`**，前端按契约发这两个参数被 FastAPI **静默忽略**，筛选形同虚设（路径级契约测试查不到参数漂移，故一直未暴露）。

修复内容：
- **A 事件精确聚合端点（问题④ + 问题①的地基）**：契约 `GET /api/v1/data/events/stats`（data-collector）+ `EventStatsData`（`total` / `unacknowledged` / `unacknowledged_by_level` / `by_type`），实现走 SQL `GROUP BY` **精确计数**（repository/service/router/schemas 四层）；data-analytics 看板改用该端点，**6 次并发细粒度查询 → 1 次聚合**。
- **B 未确认口径（问题①）**：`info/warning/critical` 语义改为**窗口内未确认各等级**（与「未确认事件」卡片同口径，三级之和 = `unacknowledged`）；`data-analytics.yaml` 与 `schemas/dashboard.py` 的描述同步改写（**前端与响应形状零变化**）。
- **C 游标分页 500（问题②）**：`Marker` → `StartAfter`；新增**形状门禁** `test_list_objects_v2_cursor_is_accepted_by_real_sdk_model`（用**真实 botocore 服务模型**校验入参，杜绝再传 v1 参数）。
- **D 录像封存链路（问题③）**：`srs.conf` 增设 DVR（`dvr_plan session` + `dvr_path /data/srs-dvr/[app]/[stream].mp4`）；compose 让 srs 与 remote-control **共享挂载**该目录；storage 增 `put_video`；新增 `app/services/video_sealer.py`（`LocalDvrRecordingSource` 轮询「文件出现且大小稳定」后读整份 + `VideoSealer` 按**创建会话时算定的对象键**上传）；`SessionService._archive_sidecar` 在写 sidecar 前先封存录像并把**真实字节数**写入（契约顺序：video PutObject → sidecar PutObject）；`RC_VIDEO_SOURCE_DIR` 未配置则禁用（保持旧行为）。
- **附带 文件筛选（date/data_type）**：`GET /files` router 去掉非契约的 `prefix`、补契约的 `date`/`data_type`，service 按 `{vehicle_id}/{date}/{data_type}/` 拼前缀；`marker` 上限对齐契约（256→1024）、`limit` 对齐（≤1000/默认 100 → ≤200/默认 50）。新增**参数级契约门禁** `test_files_endpoint_query_parameters_match_contract`（双向 diff 参数名，正是能抓住本次漏参的测试）。

向后兼容：① 看板响应**形状零变化**（`EventStats` 字段名/类型不变，仅 `info/warning/critical` 语义修正为未确认口径）；② 前端零改动；③ `/events/stats` 为**新增**读端点（不影响既有调用）；④ `/files` 的 `prefix` 为非契约私增参数，**前端从未使用**（按契约发 date/data_type），移除无影响；⑤ 录像封存未配置来源目录时行为与旧版一致（size_bytes=0）。

风险：中（改动面广），如实登记：
- **问题③的 SRS DVR + 共享目录链路本机无法验证**（本开发机无 Docker/集群）：DVR 目录创建、MP4 封装时序、`dvr_path` 绝对路径可写性均需**实机复验**；
- SRS `dvr_path` 指到独立挂载 `/data/srs-dvr`（**刻意避开** `/usr/local/srs/objs`——该目录含镜像自带 srs 二进制，覆盖挂载会导致容器启动失败）；
- 封存发生在会话结束的**同步路径**上：SRS 落盘存在延迟，`VideoSealer` 在 `RC_VIDEO_SEAL_WAIT_S`（默认 15s）内轮询，超时降级为 `size_bytes=0`（播放端仍 3001，但会话照常出现在历史里）。

验证（本环境，均实跑）：
1. `pytest services/data-collector -q` **167 passed**（新增 stats 服务用例 + `/files` 参数门禁 + SDK 形状门禁）；
2. `pytest services/data-analytics -q` **116 passed**（改动断言：种子中 `warning` 已确认 ⇒ 未确认口径下为 **0**，旧口径下为 1——该断言即为口径回归门禁）；
3. `pytest services/remote-control -q` **111 passed**（新增 `test_video_sealer.py` 8 条：路径拼装/稳定读取/上传字节数/失败降级/end_session 集成「真实 size 写进 sidecar」）；
4. `python scripts/run_unit_tests.py` **10 个目标全部通过**；
5. `ruff check services/{data-collector,data-analytics,remote-control}/app` **All checks passed**；
6. `scripts/verify_infra.py` / `scripts/verify_data_layer.py` 均 **exit 0**（契约一致性、对象存储/Redis/DB 契约全通过）。

分发路径：受追踪改动（`git add -u` + 新增文件）——
- 契约：`contracts/openapi/data-collector.yaml`（新增 `/events/stats` + `EventStatsData/Response` + 端点清单 7→8）、`contracts/openapi/data-analytics.yaml`（`EventStats` 描述）；
- data-collector：`app/routers/{events,files}.py`、`app/services/{events,files}.py`、`app/repositories/events.py`、`app/schemas/events.py`、`app/tests/{fakes.py,test_event_service.py,test_file_service.py,test_data_collector_contract.py,test_storage_presign.py}`；
- data-analytics：`app/repositories/events.py`、`app/services/dashboard.py`、`app/config.py`、`app/schemas/dashboard.py`、`app/tests/{fakes.py,test_dashboard_endpoints.py}`；
- remote-control：`app/services/video_sealer.py`（**新增**）、`app/services/session_service.py`、`app/repositories/storage.py`、`app/config.py`、`app/main.py`、`app/tests/{conftest.py,test_video_sealer.py}`；
- infra：`infra/deploy/config/srs.conf`、`infra/deploy/docker-compose.yml`。
实机生效（仅需重建受影响服务；srs.conf 是单文件 bind mount，必须 `--force-recreate`）：
```bash
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector data-analytics remote-control
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate srs
```

**待办（如实标注）**：① 问题③**整条链路实机未回验**（SRS DVR 落盘与上传、播放回放），需按上方判据在服务器复验（下段给出取证命令）；
② events 的 `sort`/`order`、telemetry 的 `order` 仍为契约声明但**实现未接**的存量参数漂移（本次只给 `/files` 加了参数门禁，未展开全端点）；
③ Redis 键删除早于归档的顺序与 `x-hunter-history-archive.sealing`「封存后才释放锁」仍不完全一致（本次未改结束链路顺序）；
④ V1.21.14 的两项待办 + Mock 纪律静态门禁（V1.19.20 待办⑥ / V1.21.15 待办②）原样挂账。

实机取证（复验问题③）：
```bash
# 1) SRS 是否已落盘本会话 MP4（会话结束后应立即出现；文件名含 session_id）
sudo docker exec hunter-srs ls -l /data/srs-dvr/remote_control/ | tail
# 2) 会话结束后 remote-control 是否成功封存（应有 video_sealed 且 size_bytes>0）
sudo docker logs --since 5m hunter-remote 2>&1 | grep -E 'video_sealed|video_seal_not_ready|video_seal_upload_failed'
# 3) hunter-video 桶里是否出现录像对象
sudo docker exec hunter-minio mc ls --recursive local/hunter-video/remote-control/ | tail
# 4) 播放：应返回预签名 URL 而非 3001（session_id 用刚结束的那个）
curl -s "http://127.0.0.1:8085/api/v1/remote/history/<session_id>/video" -H "X-User-Id: <operator-uuid>" -H "X-Roles: operator"
```

## V1.21.17（修复：三处反代入口统一「运行期解析」+ 远程操控 WS 地址双前缀归一——根治“操控台恒 reconnecting”“全站 REST 随时可能 502”）

Date：2026-10-19
变更类型：修复（`infra/deploy/config/nginx.conf`、`infra/deploy/{.env.example,docker-compose.yml}`、remote-control 后端 `app/{config.py,services/session_service.py}` + 单测。⚠ **不改前端**；后端改动在镜像内 ⇒ 生效需 `--build remote-control`；nginx 是单文件 bind mount ⇒ 需 `--force-recreate web-portal`）

背景（实机报障：远程操控完全无效、车端不动；车端侧已独立确认健康）：
操控台 WS 恒 `reconnecting`——注意前端“已发 751 / 2801”只是 `RemoteConsoleView.vue` 的 20Hz 本地计数器（`socket.send()` 在非 OPEN 时静默返回 false），**“服务端 发送 0”才是真相**；`docker logs hunter-remote` 里**完全没有** `ws_handshake_*` 记录（请求从未到达应用），而 `hunter-web` 每 20s 记一条：
```
connect() failed (111: Connection refused) while connecting to upstream,
  upstream: "http://172.28.0.3:8085/ws/remote/57a6e3d3-…/control"      # error.log
"status":502, "upstream_time":"0.000", "host":"192.168.31.35"          # access log
```

根因（两条，属同一类结构性问题）：
1. **`location /ws/` 用了静态 `upstream remote_control { server remote-control:8085; }`**：`upstream` 块与「不带变量的 `proxy_pass`」都**只在 nginx 加载配置时解析一次域名**，而 remote-control 刚被 `up -d --force-recreate`（改 .env / 换镜像）重建、容器 IP 已不再是日志里那个 `172.28.0.3` ⇒ nginx 仍连旧 IP ⇒ 恒 502 ⇒ 浏览器 WS 只拿到 **1006**（不在前端 `TERMINAL_CLOSE_CODES` 内 ⇒ **不报错、不停止重连**）⇒ 服务端从未处理过控制帧 ⇒ `commands_sent` 恒 0 ⇒ 车端 >500ms 无指令保持刹停（fail-safe 的正确表现，不是车端问题）。同一份配置里 `/api/` 走 `upstream api_gateway`（同样加载期解析）——REST 之所以正常，只是因为 api-gateway **恰好还没被重建**；同一台机器上任何一次网关重建都会让**全站 REST（含登录）**变 502。`/rtc/` 在 V1.21.7 已按同因改成变量写法（首个踩到的入口）。
2. **`RC_PUBLIC_WS_BASE_URL` 基址含路径**：`.env.example` / compose 默认值是 `ws://<ip>/ws/remote`，而 `session_service._build_webrtc` 又拼一次 `/ws/remote/{sid}/{channel}` ⇒ 下发的 `control_ws_url` / `signal_ws_url` 是 `ws://<ip>/ws/remote/ws/remote/<sid>/control`（**双前缀**，按契约使用必握手失败）。⚠ **影响面须如实界定（实机核对两个消费方，均未受影响）**：浏览器 `RemoteConsoleView.vue:171` 只在 URL 以 `/` 开头时才采用，否则**恒**用 `/remote/{session_id}/control` 同源拼装 ⇒ 绝对 URL 从未被使用；车端 `HunterEdge/.../remote_agent.py:135,529` 用**自身配置**的 `ws_url` 建连，并不消费 Kafka 下发的 `signal_ws_url`（该字段虽由 `producers/session_command.py:95` 下发，当前无消费方）。故这是**契约层错误**——现网两个实现都恰好绕开了它（这也正是它长期未被发现的原因），但任何按契约取值的客户端（新终端、第三方接入、后续前端改为信任服务端 URL）都会当场连不上；修它属于"把契约与实现对齐"，不是现网故障的直接成因。

修复内容：
- **A 三处入口统一「resolver + 变量」（`infra/deploy/config/nginx.conf`）**：`/api/`、`/ws/`、`/rtc/` 一律 `set $xxx_upstream http://<service>:<port>;` + `proxy_pass $xxx_upstream$request_uri;`，由 `resolver 127.0.0.11 valid=10s ipv6=off;` 在**运行期**按 10s 重解析；**删除 `upstream api_gateway` 块**（本文件自此**不含任何 `upstream` 块**，并在原位置留下“为什么不能建、建了会怎样”的完整说明，防复发）。含变量时 nginx 不再自动改写 URI，故必须显式接 `$request_uri`（与原先「`proxy_pass` 不带 URI」的转发结果一致，含查询串）。
  - **代价（如实登记）**：失去 `upstream keepalive 64` 连接池——“运行期解析域名”与“连接池复用”在 nginx 里不可兼得（后者需 `upstream` + `server … resolve`，而该参数 **nginx 1.27.3+** 才有，本工程基线镜像是 `nginx:1.25`，见 `frontend/Dockerfile`）。三处上游都在同一 docker 网桥，逐请求重建 TCP 为亚毫秒级开销；换来的是“重建任一后端不再牵连 web-portal、不再需要记‘重建后 restart hunter-web’”。
- **B WS 地址归一化（remote-control）**：`app/config.py` 新增 `_WS_PATH_PREFIX` / `_normalize_ws_base_url()` / `Settings.rc_ws_url_for(session_id, channel)`——基址幂等归一（去尾 `/` 后剥掉结尾的 `/ws/remote` 或 `/ws`，**仅当剥完仍是绝对 URL 才生效**，避免把相对基址的唯一前缀也吃掉）；`session_service._build_webrtc` 改用该方法拼 `control_ws_url` / `signal_ws_url`。契约形态（`ws://host`）与部署模板历史形态（`ws://host/ws/remote`）产出**同一** URL。
- **C 模板口径（infra/deploy）**：`.env.example` 与 compose 默认值改回契约语义的**裸基址**（`ws://CHANGE_ME_SERVER_IP` / `ws://${SERVER_IP}`），注释写明“基址不含路径，通道路径由后端按会话拼装”（`gen-passwords.sh` 的 `SERVER_IP_EMBEDDED_VARS` 内嵌替换、`install.sh` 的 `CHANGE_ME_SERVER_IP` 兜底、`--force` 跳过名单**均不受影响**）。

- **D 巡检门禁（`scripts/health-check.sh` ⑰-f）**：把“反代上游解析”从只查 `/rtc/` 扩为**三入口**——三条 `set $*_upstream`（`gw`/`rc`/`srs`）必须齐备，且 `nginx.conf` **不得再有 `upstream` 块**（任一不满足即 WARN 并点名是哪个入口；`location /rtc/` 缺失仍是 FAIL）。旧版⑰-f 只查 `$srs_upstream`，会在 `/ws/` 已退化时仍报 PASS——正是本次实机的“假绿”形态。

向后兼容：① 基址两种写法都接受（归一幂等，另含 `ws://host/`、`ws://host/ws`）⇒ **线上 `.env` 不必改**；② nginx 改动对既有部署**不自动生效**（单文件 bind mount），需按下方“实机生效”同步 + 重建；③ 未升级的环境仍可用 `docker restart hunter-web` 应急（等价于重跑一次加载期解析）；④ 前端零改动。
**未修改（刻意）**：K8s 形态下该风险不存在（Ingress/Service 由 kube-proxy 运行期解析）；前端逻辑保持原样。⚠ **实机核对到的事实（比“兜底”更彻底）**：`RemoteConsoleView.vue:171` 只在 `control_ws_url.startsWith('/')` 时才用它，否则**恒**用 `/remote/{session_id}/control` 走同源拼装 ⇒ 服务端下发的**绝对** URL 在浏览器侧**从未被使用**；车端亦不消费 `signal_ws_url`（用自身配置的 `ws_url`）。故双前缀缺陷对现网**不可见**，B 是**契约对齐**而非故障修复——但它必须随镜像生效，否则任何按契约取值的客户端都会踩坑。

风险：低-中，如实登记：
- `/api/` 是影响面最大的一处（丢 keepalive）；本机无法压测，按“docker 网桥 + 单机部署”评估为亚毫秒级。若日后要恢复连接池，需把 nginx 基线升到 ≥1.27.3 并改回 `upstream api_gateway { zone …; server api-gateway:8080 resolve; keepalive 64; }`（同时保留 `resolver`）。
- 三处 location 依赖 Docker 内嵌 DNS `127.0.0.11`：host 网络或外部代理形态需相应调整（`/rtc/` 已是固定 bridge 网关 IP，不依赖 DNS）。
- **本机无 Docker**（仅开发机）：静态对账为括号配平 18/18、无 `proxy_pass http://api_gateway` 残留、无 `upstream` 块；**服务器已用 `docker cp` + `nginx -t -c /tmp/nginx.conf` 实跑通过**（见上方“实机验收”第 1 条）。

验证（本环境，均实跑）：
1. `pytest services/remote-control -q` **118 passed**——新增 `app/tests/test_config_ws_url.py` **6 条**（契约形态 / 部署模板含路径形态（双前缀回归）/ 默认值不变形 / 五种写法幂等 / 自定义子路径前缀保留 / 无 scheme 不吞前缀）；`app/tests/test_session_service.py` 新增 `test_create_session_ws_urls_have_single_path_prefix`（在“建会话返回体”这一层钉死不得双前缀）；
2. `ruff check services/remote-control/app` **All checks passed**；
3. 归一化对照（同一 `.env` 取值）：旧 `ws://192.168.31.35/ws/remote/ws/remote/<sid>/control` → 新 `ws://192.168.31.35/ws/remote/<sid>/control`；
4. `python scripts/run_unit_tests.py`（各服务独立进程）无回归；
5. `bash -n scripts/{health-check,install}.sh` 语法通过，并用真实/两种退化 `nginx.conf` 对拍⑰-f 判定（真配置 → PASS；`/ws/` 被写回静态 → 点名 `/ws/`；再补 `upstream` 块 → 报块数；删 `/api/` 的 `set` 行 → 点名 `/api/`）。

分发路径：受追踪改动——
- `infra/deploy/config/nginx.conf`、`infra/deploy/.env.example`、`infra/deploy/docker-compose.yml`、`scripts/health-check.sh`；
- `services/remote-control/app/config.py`、`app/services/session_service.py`、`app/tests/test_config_ws_url.py`（**新增**）、`app/tests/test_session_service.py`。
⚠ **文档四件套与 `release.md` 仍被 `.gitignore` 排除（只更新、不追踪），须单独同步到服务器**：`docs/故障排查指南.md`（§0 新增第 4 条关键事实；**9.1 重写为“先分流”**（nginx 502 vs CORS 403 两条互斥根因 + 两条取证命令）；9.9 约束③改写并给出“不再有 upstream 块”的新口径；9.10 运维纪律同步）、`docs/运维操作手册.md`（§3.1 运行时副本同步段、§3.8 L6 行、srs 重建连带项、§4.2 Nginx 变更整段）、`docs/deployment.md`（§15 变量表新增 `RC_PUBLIC_WS_BASE_URL` 行与基址口径、硬约束“三条”→“五条”并新增第 5 条）、`docs/01-部署概述与环境要求.md`（§3.3 新增统一口径警示块、§env 表 `RC_PUBLIC_WS_BASE_URL` 行）、根 `README.md`（部署段新增“反代上游纪律”硬约束块、验证命令清单新增 WS 地址测试行）、`release.md`（本条目）。
实机生效（nginx 单文件 bind mount：`reload` 不够；`restart` 只重读内容，改内容可用）：
```bash
cd /opt/HunterCore
git pull
sudo bash scripts/install.sh --step 4 -y                        # V1.21.11+：自动把仓库 nginx.conf 同步到运行时副本 config/
sudo cp -a infra/deploy/config/nginx.conf config/nginx.conf     # 应急等价操作
sudo docker cp config/nginx.conf hunter-web:/tmp/nginx.conf \
  && sudo docker exec hunter-web nginx -t -c /tmp/nginx.conf     # ⚠ 先校验语法，再重建
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate web-portal
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build remote-control
```
验收（三条；⚠ 会话进行中/取证过早都会“假空”，见下方实测踩坑）：
```bash
# 1) WS 真连上：出现 101，且不再有 connect() failed
#    ⚠ nginx 的 access log 在请求**结束（WS 关闭）时**才落一行，长连接期间查不到 —— 实时判据看 2)
sudo docker logs --since 2m hunter-web 2>&1 | grep 'ws/remote' | tail
# 2) 服务端真收到控制帧：commands_sent 随 20Hz 增长（“回执 2 · 发送 0”是旧快照：boot/stop 等非控制帧也计入 ack）
RP=$(sudo docker exec hunter-collector printenv REDIS_PASSWORD)   # ⚠ root shell 不加载 .env，直接用 $REDIS_PASSWORD 会 WRONGPASS/NOAUTH
sudo docker exec hunter-redis redis-cli --no-auth-warning -a "$RP" HGETALL rc:session:HUNTER-001
# 3) 重建后端后入口不再断（本版核心收益：不再需要 restart hunter-web）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate remote-control
sleep 12
curl -s -o /dev/null -w 'api=%{http_code}\n' -X POST http://127.0.0.1/api/v1/user/login
curl -s -o /dev/null -w 'ws=%{http_code}\n' -H 'Connection: Upgrade' -H 'Upgrade: websocket' http://127.0.0.1/ws/remote/probe/control
# 两处只要不是 502 即为根治成功（api 返 4xx、ws 返 4xx 都属预期）
# 4) 巡检门禁：⑰-f 应 PASS（三入口运行期解析 + 无 upstream 块）
sudo bash scripts/health-check.sh 2>&1 | grep '反代上游解析'
```

**实机验收（2026-10-19 15:51–15:53，`hunter-core-server`，用户回传原文摘录）**：
1. `sudo docker exec hunter-web nginx -t -c /tmp/nginx.conf` → `syntax is ok` / `test is successful`（**nginx -t 已实跑通过**）；
2. `--force-recreate web-portal` → `hunter-web Started`；`--build remote-control` → 镜像 `sha256:51d8169…`（**各层全 CACHED**，容器显示 `Running` 而非 `Recreated` ⇒ 新归一化代码**尚未进容器**，见待办①）；
3. **决定性证据（Redis 会话 Hash，91s 窗口）**：
```
commands_sent    1820          commands_acked   1814          seq_last 1820
started_at       1792396307.03 → 2026-10-19 15:51:47
last_heartbeat_at 1792396398.08 → 2026-10-19 15:53:18          status   active
```
   91s × 20Hz ≈ **1821**，与 `commands_sent=1820` 吻合 ⇒ 控制帧**真的穿过 nginx 到达 remote-control 并被计数、回执也回来了**（修复前恒 0）。这条比任何单侧日志都硬——它同时证明浏览器→nginx→应用→车端回执四段都通。**注意它证明的是 A（nginx 变量化）已生效**：浏览器走的是前端同源拼装的相对路径，与本版 B（URL 归一化）无关。
4. **重建后端后入口仍通（本版核心收益）**：`--force-recreate remote-control`（换容器 IP）→ `sleep 12` → `api=422`（`POST /api/v1/user/login` 空体，网关正常校验）、`ws=400`（`/ws/remote/probe/control` 缺 `Sec-WebSocket-Key`，由上游拒）——**两处都不是 502**，即 nginx 已跟到新 IP；此前同一步骤必 502；
5. 巡检 ⑰-f → `[PASS] 视频链路（反代上游解析） - 三入口均运行期解析（resolver + 变量）、无 upstream 块 → 任一后端重建（api-gateway/remote-control/srs）都不需 restart hunter-web`。
6. **端到端闭环（同日 16:09–16:10 实机）**：视频出画、20Hz 指令 `commands_sent` 增长、`commands_acked` 同步涨、超时 0、回执均值 28ms ⇒ **平台侧全链路确认**（nginx → remote-control → Kafka → 车端 → 回执）。此后“车不动”定位到**车端执行链**：车端 RViz 抢核 + FAST-LIO2 点云未降采样 → 里程计发散 1e12 → TF 断 → 重定位发散 → `safety_guard` 地图边界锁停；**关 RViz + 重启车端即恢复，遥控行车已验证**。**平台无责，本版无需再改。**（V1.21.5 待办① 的“车端不执行运动”在**真车**上即此因，已闭环；模拟器侧未复核。）
7. **方法论留档：`commands_acked` 增长 ≠ 车已执行**——车端 `remote_agent` 对每帧无条件回执（`success=true`，message 写死 `"applied v=…"`，措辞误导），是否落到 `/cmd_vel` 取决于另一条 20Hz 路径（ROS 不可用时只打 error、回执照发）。故“发送/回执正常但车不动”必须转车端查执行链，判据与命令见排查指南 9.1“近亲现象”。

**实机踩到的三个“假空/假错”（判据本身已按实测改写，见排查指南 9.1）**：
- `docker logs hunter-web | grep 'ws/remote'` 在**会话进行中为空是正常的**：nginx 的 access log 在请求**结束（WS 关闭）时**才落一行（`$request_time` = 整个会话时长），长连接期间不会出现 `"status":101`。实时判据应看 `hunter-remote` 的 `ws_control_connected` 或 Redis 的 `commands_sent` 增长。
- `grep ws_control_connected` 为空也可能是**取证早于前端重连**：本例两次 grep 发生在 15:51:0x，而会话 15:51:47 才建（前端退避重连有间隔）。先确认页面已连上，再取证。
- `redis-cli -a "$REDIS_PASSWORD"` 报 `AUTH failed: WRONGPASS` + `NOAUTH` **不是密码错**：root shell 不加载 `.env`，该变量为空。改用 `RP=$(sudo docker exec hunter-collector printenv REDIS_PASSWORD)` 或 `sudo sh -c 'source scripts/common.sh; load_env; …'`。

**待办（如实标注）**：① **`--build remote-control` 各层全 CACHED、容器仍是 `Running`（未 `Recreated`）⇒ 归一化代码是否已进容器未证实**：先查 `sudo docker exec hunter-remote grep -c _normalize_ws_base_url /app/app/config.py`（期望 1）；若为 0，说明服务器仓库未含 `6e704fc` 或镜像缓存误判，执行 `git pull` + `sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build --force-recreate remote-control`，再复验。**最直接的功能判据（免鉴权、不受前端同源拼装干扰）**：
```bash
sudo docker exec hunter-remote python3 -c "from app.config import settings; print(settings.rc_ws_url_for('probe-sid','control'))"
# 服务器现网基址为 ws://192.168.31.35/ws/remote ⇒ 期望 ws://192.168.31.35/ws/remote/probe-sid/control（单前缀）
# 未升级代码 ⇒ ws://192.168.31.35/ws/remote/ws/remote/probe-sid/control（双前缀）
```
   **当前实机验收的 5 条只证明 nginx 侧（A/C/D）已生效，未覆盖 B**；② nginx 侧已实机通过（`nginx -t`、重建 remote-control 后 `api=422`/`ws=400` 均非 502、⑰-f PASS、`commands_sent=1820/91s`），但 `/api/` 丢 keepalive 的实际影响未压测（QPS 高时考虑升 nginx 基线 ≥1.27.3 后改回 `upstream … resolve`）；③ 前端只在 URL 以 `/` 开头时才采用、否则恒用同源拼装——**本次核实为既有设计**（服务端绝对 URL 从未被使用），若日后改为信任服务端 URL，需连带评估 `RC_PUBLIC_WS_BASE_URL` 的同源要求；④ V1.21.16 的四项待办原样挂账。

## V1.21.18（修复：录像「播放」500——aioboto3 `generate_presigned_url` 漏 `await` 把协程当字符串；并实机回验 V1.21.16 录像封存链路的两个缺口）

Date：2026-10-20
变更类型：修复（remote-control + data-analytics 后端 `app/repositories/storage.py` 的 `presign_get`。⚠ **不改契约、不改前端、不改 DB/Kafka/compose**；改动在镜像内 ⇒ 生效需 `--build remote-control data-analytics`）

现场（用户报障「远程接管和操控中，视频未保存、不能播放」，实机逐层取证）：操控记录页所有会话「录像大小 = 0 B」、归档面板 `video.size_bytes=0`/`sha256=""`/「已过保留期或未封存」、点「播放录像」报 3001。经两条取证把 V1.21.16「录像封存链路」（其待办①当时标注「整条链路实机未回验」）拆出**两个独立缺口**：

根因（两条，分属配置与代码）：
1. **缺口①（配置，非代码）——运行的 srs.conf 无 DVR 块 ⇒ 录像从不落盘 ⇒ `size_bytes=0` ⇒ 播放 3001**：取证 `docker exec hunter-srs ls /data/srs-dvr/remote_control/` → `No such file or directory`；`docker exec hunter-srs grep 'dvr {' /usr/local/srs/conf/srs.conf` → **空**。compose 挂载的是**运行时副本** `${APP_DIR}/config/srs.conf`（配置优先级 `APP_DIR/config/` **>** `infra/deploy/config/`），而 V1.21.16 的 DVR 块只加进了仓库 `infra/deploy/config/srs.conf`——**运行时副本从未同步**（`git pull` 只改仓库那份，对已挂载容器无效），故运行的 SRS 根本不录像，remote-control `seal()` 轮询超时记 `video_seal_not_ready`。这正是 V1.21.10 已为 nginx.conf 记过的「运行时副本不回同步」坑，srs.conf 这次重演。**修复靠运维动作、不改代码**：`install.sh --only-step 4`（`ensure_srs_conf` → `sync_runtime_config` 备份旧副本并写回含 DVR 块的新配置、对齐 candidate）+ `--force-recreate srs`（单文件 bind mount，restart 不重挂）。**实机回验通过**：重建后 `dvr {` 在位、`/data/srs-dvr/remote_control/hunter_HUNTER-001_<sid>.mp4`（20,549,810 B）落盘、`hunter-remote` 记 `video_sealed size_bytes=20549810`、列表该会话显示 19.60 MB。
2. **缺口②（代码）——`presign_get` 漏 `await` ⇒ 播放 500**：缺口①修复后 `head_video` 首次返回真实大小（不再 3001），代码**首次真正执行到 `presign_get`**，随即 500。traceback 决定性：`pydantic_core.ValidationError for ControlVideoAccess / video_url: Input should be a valid string / input_value=<coroutine object generate_presigned_url>, input_type=coroutine`。根因：**aioboto3 的 `client.generate_presigned_url` 是协程**（与同步 boto3 不同），而 `presign_get` 写成 `return client.generate_presigned_url(...)` 未 `await` ⇒ 返回协程对象 ⇒ 下游 `video_url: str` 校验失败抛未捕获 `ValidationError` → 500（`_unhandled_handler`）。此前恒 3001 从未走到本方法，故长期潜伏；单测用内存替身直接返回 `str`，**掩盖了真库的协程形状**（与「Mock 替身发明字段/形状」同类缺陷）。同型缺陷 data-analytics `storage.presign_get` 亦命中（其消费方 `services/reports.py` 把返回值塞进报告产物 `download_url`，同样会是协程）。

修复内容：
- `services/remote-control/app/repositories/storage.py`：`presign_get` 改 `return await client.generate_presigned_url(...)`，并加注释钉死「aioboto3 协程必须 await」；
- `services/data-analytics/app/repositories/storage.py`：同型修正（一处）。
- 其余服务不受影响：scene-service 用同步 boto3 + `asyncio.to_thread` 包裹、data-collector/ota-service/ingest-platform 的 `presign_*` 均为同步方法内的同步 boto3 调用——本就只有两处 aioboto3 客户端漏 `await`。

向后兼容：仅改返回值获取方式（协程 → 字符串），签名、响应形状、契约、对象键、有效期全部不变；前端零改动。

风险：低。改动面两行；唯一注意点是 aioboto3 各版本 `generate_presigned_url` 均为协程（traceback 已在现网 3.12 + 实镜像证实），`await` 是唯一正确写法。

验证（本环境，均实跑）：
1. `pytest services/remote-control -q` **118 passed**；`pytest services/data-analytics -q` **116 passed**（两处 `presign_get` 单测走内存替身，不覆盖真库协程形状——见待办②）；
2. `ruff check services/{remote-control,data-analytics}/app/repositories/storage.py` **All checks passed**。

实机执行（缺口① 与缺口② 均已于 2026-10-20 实机回验通过）：
```bash
cd /opt/HunterCore
git pull
# 缺口①：同步 srs.conf 运行时副本 + 重建 srs（已实机执行，video_sealed 已见）
sudo bash scripts/install.sh --only-step 4 -y
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate srs
# 缺口②：presign 修复进镜像（待执行）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build remote-control data-analytics
# 复验播放：应返回预签名 URL（200），不再 500/3001
sudo docker logs --since 3m hunter-remote 2>&1 | grep -E 'video_presigned|unhandled_exception'
```

**实机回验（2026-10-20 09:01–09:06，`hunter-core-server`，用户回传）**：`--only-step 4` 把含 `dvr {` 的 srs.conf 同步到运行时副本（旧副本已备份 `srs.conf.bak-20261020090111`）+ candidate 改写为 192.168.31.35 → `--force-recreate srs`；`--build remote-control data-analytics` 重建镜像并 `hunter-remote`/`hunter-analytics` Started。会话结束后 `docker logs hunter-remote | grep -E 'video_presigned|unhandled_exception'` 连续出现三行 `message=video_presigned ... expires_in=900`（两个 session 的对象键各一/两次），**无任何 `unhandled_exception`**；前端「播放录像」弹出「操控录像回放（H.264）」播放器并提示「预签名链接 15 分钟有效且支持 HTTP Range」⇒ **缺口② 回放已实机闭环**。

分发路径：`services/remote-control/app/repositories/storage.py`、`services/data-analytics/app/repositories/storage.py`。
文档同步（运维四件套，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/故障排查指南.md`（新增「录像未保存 / 播放失败」场景：两级判据——`size_bytes=0`+`video_seal_not_ready` → 查 srs.conf 运行时副本有无 `dvr {` 块（缺口①，配置）；`size_bytes>0` 但播放 500 且 traceback 见 `video_url ... input_type=coroutine` → presign 漏 await（缺口②，本版代码）；附取证命令与 `--only-step 4`+`--force-recreate srs` 修复动作）、`docs/运维操作手册.md`（§配置同步段补「srs.conf 改仓库后必须重跑 Step 4 才落到运行时副本」硬约束）、`docs/deployment.md` 与 `docs/01-部署概述与环境要求.md`（V1.21.16 录像封存链路补「实机回验结论」）。

**待办（如实标注）**：① 缺口② 已实机回验通过（见上「实机回验」段：`video_presigned` 三行、无 `unhandled_exception`、前端播放器正常弹出）——本项**已闭环**；② 单测内存替身返回 `str` 掩盖了 aioboto3 真库返回协程的形状，未加「替身形状须对齐真库」的门禁（与 V1.21.15 待办② / V1.19.20 待办⑥ 同源的 Mock 纪律缺口，本次仍未补）；③ 历史（缺口① 修复前）那批 `size_bytes=0` 会话无法补录——其 SRS mp4 当时根本没落盘，文件已不存在，属预期，仅修复后的新会话可回放；④ V1.21.16 其余待办（events `sort`/`order` 漂移、结束链路锁释放顺序、V1.21.14 两项）原样挂账。

## V1.21.19（修复：录像「能返回链接但点了不播」——remote-control 预签名走内部 `minio:9000`，浏览器不可达；并补 V1.21.18 待办② 的「替身形状须对齐真库」门禁单测）

Date：2026-10-20
变更类型：修复（remote-control 后端 `app/repositories/storage.py` 改为**双 client**（预签名走 public endpoint）+ 新增门禁单测。⚠ **不改契约、不改前端、不改 DB/Kafka**；代码改动在镜像内 ⇒ 生效需 `--build remote-control`；且**必须**在 `.env` 配置 `MINIO_PUBLIC_ENDPOINT` 为浏览器可达地址，否则回落到内部地址仍不播）

现场（V1.21.18 后用户复报「视频无法点击播放」）：缺口② 修复后 `GET .../video` 已返回 200 + 预签名 URL、前端播放器正常弹出，但**点播放键无画面/加载失败**。

根因（配置 + 代码，二者缺一仍不播）：
1. **代码——remote-control 预签名恒用内部 endpoint**：`S3VideoArchiveStorage` 是**单 client** 设计，`presign_get` 与 `head/put` 共用 `settings.minio_endpoint_url`（集群内 `http://minio:9000`）⇒ 返回的下载 URL host 为 `minio:9000`，浏览器无法解析容器内 DNS/无法路由 ⇒ 点了不播。data-collector 早在 V1.21.12 已用**双 client**（`_presign_client` 走 `minio_public_endpoint_url`）解决同类问题，remote-control 当时被记为「同构问题、待跟进」却未落实（V1.21.12 待办①）。**修复**：`S3VideoArchiveStorage` 改双 client——内部 client 供 `head_video/put_video/read_sidecar/...`，新增 `_presign_client`（`minio_public_endpoint_url`）仅供 `presign_get`；`close()` 释放两个上下文管理器。
2. **配置——`MINIO_PUBLIC_ENDPOINT` 未设则回落内部地址**：compose `business-env` 默认 `MINIO_PUBLIC_ENDPOINT: ${MINIO_PUBLIC_ENDPOINT:-}`（空），空值时 `minio_public_endpoint_url` 回落到 `minio_endpoint_url`（内部 `minio:9000`）⇒ 即便代码走 public client，host 仍是内部地址。`.env.example` 要求 `MINIO_PUBLIC_ENDPOINT=<SERVER_IP>:9000`；MinIO API 端口 `9000:9000` 已发布到宿主机、`MINIO_SERVER_URL=http://${SERVER_IP}:9000` 亦在位 ⇒ 生产须在 `/opt/HunterCore/.env` 设 `MINIO_PUBLIC_ENDPOINT=192.168.31.35:9000`（与 `SERVER_IP` 一致，**不可填容器名 `minio`**）。

修复内容：
- `services/remote-control/app/repositories/storage.py`：`S3VideoArchiveStorage` 单 client → 双 client（`_presign_client`/`_presign_cm` 走 `minio_public_endpoint_url`；`_create_client(endpoint_url)` 参数化；`presign_get` 改 `_ensure_presign_client()`；`close()` 释放两 cm）。签名、对象键、有效期、响应形状不变，前端零改动。
- `services/remote-control/app/tests/test_storage_presign.py`（新增，4 条）：**补 V1.21.18 待办② 的门禁**——内存替身 `_Aioboto3ShapedClient.generate_presigned_url` 刻意对齐真库为**协程函数**（`inspect.iscoroutinefunction` 自检钉死形状），据此：① `presign_get` 漏 `await` 即返回协程对象 → `isinstance(url, str)` 断言失败（复现并封堵 V1.21.18 的 500 根因，杜绝再被「替身返回裸 str」掩盖）；② `presign_get` 必须走 public endpoint、URL host 不得含 `minio:9000`（封堵本版不播根因）；③ 服务侧 `head_video` 仍走内部 endpoint（双 client 隔离）。

向后兼容：仅换预签名所用 client 的 endpoint，其余方法行为不变；未配 `MINIO_PUBLIC_ENDPOINT` 时行为等同旧版（回落内部地址），配置后方生效。

风险：低。双 client 与 data-collector 同构（已实机验证的成熟模式）；唯一运维注意点是 `MINIO_PUBLIC_ENDPOINT` 必须为浏览器可达地址且 MinIO 9000 端口对外可达。

验证（本环境，均实跑）：
1. `pytest services/remote-control/app/tests/test_storage_presign.py -q` **4 passed**；`pytest services/remote-control -q` **122 passed**（+4）；
2. `ruff check` **All checks passed**。

实机执行（**待回验**，须同时满足代码与配置）：
```bash
cd /opt/HunterCore
git pull
# 前置：确认/设置浏览器可达的公网 endpoint（不可填容器名 minio）
grep -E '^MINIO_PUBLIC_ENDPOINT=' .env   # 期望 192.168.31.35:9000；为空或 CHANGE_ME 则编辑 .env 填入
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build remote-control
# 复验：返回的 video_url 的 host 应为 192.168.31.35:9000（非 minio:9000），浏览器可直连出画
sudo docker logs --since 3m hunter-remote 2>&1 | grep -E 'video_presigned|unhandled_exception'
```

分发路径：`services/remote-control/app/repositories/storage.py`、`services/remote-control/app/tests/test_storage_presign.py`。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/故障排查指南.md`（§9.13 由「两级判据」扩为「三级判据」——新增缺口③：URL 已返回、播放器弹出但点了不播 → 查 `video_url` 的 host 是否内部 `minio:9000`：是则本版双 client 修复 + `MINIO_PUBLIC_ENDPOINT` 配置）、`docs/运维操作手册.md`（§7 录像自检新增缺口③判据与取证命令；§7.5.1 把 `MINIO_PUBLIC_ENDPOINT` 由「车端上传必需」升格为「车端上传 + 前端录像回放均必需」，变更步骤 recreate 列表补 `remote-control`）。

**待办（如实标注）**：① 本版修复**实机未回验**——需按上「实机执行」重建 remote-control 且确认 `.env` 的 `MINIO_PUBLIC_ENDPOINT` 已设为浏览器可达地址后，实测前端出画方可判闭环；② data-analytics 的 `presign_get`（报告产物下载）为**同构单 client**，若其下载 URL 也在浏览器侧使用则同样需双 client + public endpoint——本版按报障范围仅改 remote-control，data-analytics 待确认消费端后跟进；③ V1.21.18 待办②（门禁单测）本版**已补**（`test_storage_presign.py`），从 V1.21.18 待办迁移闭环。

## V1.21.20（修复：录像「点了仍不播」的真阻塞是浏览器 CSP 拦截 http 预签名（缺口④）；+ 新增：文件浏览批量删除（清理 0 B 孤儿 sidecar）+ marker 栈游标分页）

Date：2026-10-20
变更类型：修复 + 新增

**一、修复——CSP `media-src` 拦截录像（缺口④，「还是无法播放」的决定性根因）**

现场（V1.21.19 后用户复报）：remote-control 双 client + `MINIO_PUBLIC_ENDPOINT=192.168.31.35:9000` 均实机确认到位（容器 env、`video_presigned` 日志、URL host 均为 `192.168.31.35:9000`），但播放器仍不出画。DevTools 决定性报错：`Loading media from 'http://192.168.31.35:9000/...' violates CSP directive: "media-src 'self' blob: https:". The action has been blocked.` ⇒ 阻塞点从「URL 不可达（缺口③）」下移到「浏览器内容安全策略放行（缺口④）」。

根因：前端有**两处** CSP，浏览器对两者**取交集**（都须放行才加载）——
1. `frontend/index.html` 的 `<meta http-equiv="Content-Security-Policy">`：`media-src 'self' blob: https:`（不放行跨源 `http:` scheme）；
2. `infra/deploy/config/nginx.conf` 的 `add_header Content-Security-Policy`：`media-src 'self' blob:`（既不放行 `http:` 也不放行 `https:`）。
预签名录像走 `http://<SERVER_IP>:9000`（跨源 + http scheme）⇒ 两处均拦截。

修复：两处 `media-src` 均改为 `media-src 'self' blob: http: https:`（放行 http/https scheme-source）。⚠ **不改契约、不改后端、不改 DB/Kafka**；`index.html` 烘焙进 web-portal 镜像（须重建）、`nginx.conf` 是运行时单文件 bind mount（须 `install.sh --only-step 4` 同步 + `--force-recreate web-portal`）。

**二、新增——文件浏览批量删除（产品功能，用户选定「方案②」）**

诉求：清掉 DVR 修复前遗留的 0 B 无效录像记录（无对应 mp4 的孤儿 sidecar），并支持分页浏览。

实现（data-collector 新增 `POST /api/v1/data/files/delete`）：
- 契约 `contracts/openapi/data-collector.yaml`：新增 path（operationId `deleteFiles`）+ `FileDeleteRequest/FileDeleteFailure/FileDeleteData/FileDeleteResponse` schema + `x-hunter-endpoints.endpoints` 条目；
- `app/repositories/storage.py`：`delete_objects(bucket, keys) -> (deleted, errors)`（boto3 `DeleteObjects`，`Quiet=False`）；
- `app/schemas/files.py`：`FileDelete*` 四个模型（`extra="forbid"`；`object_keys` 1–200）；
- `app/services/files.py`：`delete_files(bucket, object_keys, user_id)`——去重去空保序、全空→2001、存储故障→5001 不静默、审计日志 `file_objects_deleted`（记 operator + 对象键，不记内容）；
- `app/routers/files.py`：`POST /files/delete`，权限 `require_execute_permission`（`data:execute`，与事件确认同档，**无需新增权限**）。
幂等：S3 `DeleteObjects` 对不存在对象亦计 `Deleted`，适合「清空」语义；`object_key` 相对 bucket（与 `GET /files` 返回一致）。

**三、前端——文件浏览多选删除 + marker 栈游标分页（用户选定「仍基于游标、marker 栈实现」）**

`frontend/src/views/data/EventListView.vue`：
- 分页由「加载更多」改为**上一页/下一页 + 每页条数**：契约 `GET /files` 仅返回 `next_marker`、无 `total`，用 `markers[i]`（请求第 i+1 页的游标，首页 `undefined`）栈实现双向导航——加载后据 `truncated/next_marker` 写回 `markers[i+1]` 并裁掉更深层已知游标；「下一页」用当前页 `next_marker`、「上一页」弹回栈中前序游标；
- 多选删除：`el-table` selection 列（仅 `data:execute` 可见）+「删除选中」按钮，`ElMessageBox` 二次确认后调 `deleteFiles`，回带 `deleted_count`/失败数，成功后重载当前页；
- `api/data.ts` 新增 `deleteFiles`、`types/data.ts` 新增 `FileDelete*`、`constants/permissions.ts` 新增 `dataExecute: 'data:execute'`。

向后兼容：删除为**新增**端点与**新增**按钮，无历史行为变化；分页仍走同一 `GET /files` 游标参数，仅交互从追加改为翻页。

风险：低-中。CSP 放行 `http:`/`https:` scheme-source 会放开对**任意** http/https 媒体的加载（本系统仅预签名 MinIO 地址使用，可接受；如需更严可后续收敛为具体 host-source `http://<SERVER_IP>:9000`）。删除直连 MinIO 不可恢复 ⇒ 前端强制二次确认 + 后端逐次审计。

验证（本环境，均实跑）：
1. `pytest services/data-collector -q` **173 passed**（含 `test_file_service.py` 新增 6 条删除用例：移除计数、去重去空、全空→2001、幂等、存储故障→5001、部分失败）；`test_data_collector_contract.py` 端点集合三方一致（`actual == declared == EXPECTED_ENDPOINTS`）、`$ref` 全解析通过；
2. `ruff check app` **All checks passed**；
3. `npm run type-check`（vue-tsc）**无错误**。

实机执行（**待回验**）：见下「部署步骤」——重建 web-portal 镜像（携带 CSP `index.html`）+ 同步 `nginx.conf` 运行时副本 + recreate；data-collector 需 `--build data-collector`（新端点）。

分发路径：`frontend/index.html`、`infra/deploy/config/nginx.conf`、`contracts/openapi/data-collector.yaml`、`services/data-collector/app/{repositories/storage,schemas/files,services/files,routers/files}.py`、`services/data-collector/app/tests/{fakes,test_file_service,test_data_collector_contract}.py`、`frontend/src/{api/data.ts,types/data.ts,constants/permissions.ts,views/data/EventListView.vue}`。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/故障排查指南.md`（§9.13 三级判据补**第四级**：URL host 正确、播放器弹出仍不播 + DevTools 报 `violates CSP directive: media-src` → 本版 CSP 修复）、`docs/运维操作手册.md`（文件浏览删除操作与权限说明、web-portal 重建 + `nginx.conf` 同步步骤）。

**待办（如实标注）**：① CSP 修复**已实机回验通过**——重建 web-portal + 同步 `nginx.conf` 后，用户复报「视频可以播放」（录像回放正常出画 H.264），缺口④闭环；② 删除功能与 marker 栈分页**已实机验证**：分页/多选交互正常，但点「删除」触发 MinIO `MissingContentMD5`（botocore ≥1.36 不再为 `DeleteObjects` 自动计算 Content-MD5）⇒ **该缺陷已在 V1.21.21 修复**（从本版待办迁移闭环）；③ V1.21.19 待办②（data-analytics `presign_get` 同构单 client）用户确认「报告下载 URL 也在浏览器侧使用」⇒ **已在 V1.21.21 落实双 client + public endpoint**（从本版待办迁移闭环）。

## V1.21.21（修复：文件浏览删除报 `MissingContentMD5`（botocore 不再为 DeleteObjects 自动算 Content-MD5）；+ data-analytics 报告下载预签名双 client + public endpoint）

Date：2026-10-20
变更类型：修复

本版回应用户实机回验反馈的三件事（①选中点删除报错、②视频可播放（CSP 生效）、③data-analytics 报告下载 URL 也在浏览器侧需同构双 client）。②为对 V1.21.20 CSP 的正向确认（已上记），本版修复①、落实③。

**一、修复——`MissingContentMD5`（文件浏览批量删除）**

现场（V1.21.20 删除功能实机）：选中 50 个孤儿 sidecar 点「删除」返 `对象删除失败（bucket=hunter-video）：An error occurred (MissingContentMD5) when calling the DeleteObjects operation: Missing required header for this request: Content-Md5.`

根因（依赖行为漂移）：botocore ≥ 1.36（本环境 1.43.97）默认改用 **CRC32 灵活校验和**，旧的 `botocore.utils.conditionally_calculate_md5` 已 deprecated 且**不再为 `DeleteObjects` 自动计算 Content-MD5**；而 MinIO 强制要求 `DeleteObjects` 携带 `Content-MD5` 头 ⇒ 缺头即报 `MissingContentMD5`。内存替身永不会暴露此差异（不经过真 botocore 序列化），故需真库形状门禁。

修复（`services/data-collector/app/repositories/storage.py`）：`_build_sync_client` 末尾调 `_register_content_md5(client)`，在 `before-call.s3.DeleteObjects` 事件注册 handler `_inject_content_md5`：基于序列化后的最终请求体（XML）计算 `base64(MD5(body))` 写入 `params['headers']['Content-MD5']`（已有同名头则不覆盖）。**先写探针脚本验证**：before-call 的 body 与 before-send 的 body 完全一致（`body identical: True`），且注入值与实际 body 的 MD5 匹配；botocore 在 before-call 之后才 create_request + 签名，故 Content-MD5 会被纳入 SigV4 签名。签名/端点/响应形状不变，前端零改动。

**二、修复——data-analytics 报告下载预签名双 client + public endpoint**

根因（与 remote-control V1.21.19 同源）：`S3ObjectStorage` 原为**单 client**，`presign_get` 与 `get/put/list` 共用内部 `settings.minio_endpoint_url`（集群内 `minio:9000`）⇒ 报告下载 URL host 为内部地址、浏览器不可达。

修复（`services/data-analytics/app/repositories/storage.py`）：与 remote-control/data-collector 同构改双 client——`_create_client(endpoint_url)` 参数化；新增 `_presign_client`/`_presign_cm`（走 `minio_public_endpoint_url`）仅供 `presign_get`；`get_json/put_json/list_json/healthcheck` 仍走内部 `_client`；`close()` 释放两个上下文管理器。未配 `MINIO_PUBLIC_ENDPOINT` 时回落到内部地址（行为等同旧版）。`minio_public_endpoint_url` 已存在于 `hunter_common/config.py`（与 remote-control 共用），无需改配置层。

向后兼容：仅换预签名所用 client 的 endpoint，其余方法行为不变；删除修复仅在 client 新增事件 handler，不改调用方。

风险：低。双 client 与 remote-control/data-collector 已实机验证同构；Content-MD5 为 MinIO 强约束的标准做法。

验证（本环境，均实跑）：
1. `pytest services/data-collector -q` **175 passed**（含新增 2 条 Content-MD5 门禁：`test_inject_content_md5_sets_base64_md5_of_body` 纯函数、`test_delete_objects_request_carries_content_md5_real_sdk` 真 botocore client + before-send 短路捕获验证 Content-MD5 注入且匹配 body）；
2. `pytest services/data-analytics -q` **120 passed**（含新增 `test_storage_presign.py` 4 条：协程 await 返回 str、presign 走 public endpoint、服务侧 healthcheck 走内部 endpoint、替身协程形状自检）；
3. `ruff check` 两服务 **All checks passed**。

实机执行（**已回验通过**，2026-10-20）：
```bash
cd /opt/HunterCore
git pull
# ① 删除修复：重建 data-collector（Content-MD5 handler）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector
# ② data-analytics 双 client：重建 data-analytics；前置确认 .env 的 MINIO_PUBLIC_ENDPOINT=192.168.31.35:9000
grep -E '^MINIO_PUBLIC_ENDPOINT=' .env
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-analytics
# 复验：文件浏览选中孤儿 sidecar → 删除应返回 deleted_count、不再报 MissingContentMD5；
#         data-analytics 报告下载 URL host 应为 192.168.31.35:9000（非 minio:9000），浏览器可下载
```

分发路径：`services/data-collector/app/repositories/storage.py`、`services/data-collector/app/tests/test_storage_presign.py`、`services/data-analytics/app/repositories/storage.py`、`services/data-analytics/app/tests/test_storage_presign.py`（新增）。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/故障排查指南.md`（新增 `MissingContentMD5` 判据：删除报缺 Content-MD5 → botocore 版本行为、本版修复）、`docs/运维操作手册.md`（§7.10 补删除排障；新增 data-analytics 双 client 部署与 `MINIO_PUBLIC_ENDPOINT` 依赖说明）。

**待办（如实标注）**：① Content-MD5 修复**已实机回验通过**（2026-10-20 重建 data-collector 后实测删除返回 deleted_count、不再报 MissingContentMD5，本地探针+真库形状门禁与实机一致），从待办迁移闭环；② data-analytics 双 client**已实机回验通过**（2026-10-20 `--build data-analytics` + `MINIO_PUBLIC_ENDPOINT=192.168.31.35:9000` 后实测报告下载 URL host 为公网地址且浏览器可下载），从待办迁移闭环。

## V1.21.22（修复：文件浏览删除会误删有效录像的 sidecar 致操控记录清空；改为会话配对删除 + 有效录像护栏 + 有效性标注）

Date：2026-10-20
变更类型：修复 + 增强

现场（V1.21.21 后用户实机）：在「文件浏览（MinIO）」多选 `.json` sidecar 删除后，「操控记录」列表整体变空（图：来源 minio_sidecar、共 0 条），但文件浏览里仍残留 2 个有效 `.mp4`（13.5MB/19.6MB）。

根因（设计使然 + 交互陷阱，非数据库被删）：
1. **操控记录 = MinIO sidecar 投影，无数据库表**（契约 `x-hunter-history-archive`；`history_service.list_sidecars` 扫 `hunter-video` 桶的 `{session_id}.json`）⇒ **删 `.json` 即删记录**，与 `.mp4` 是否存在无关。
2. **文件浏览列的是对象、`.json` 行显示的是它自身大小（几百字节），看不出对应视频是有效还是 0B** ⇒ 用户“全选删除”把 48 个真孤儿 sidecar **与 2 个有效视频的 sidecar 一并删除**；那 2 个视频从此失联（文件在、但无 sidecar → 操控记录看不到且无法从界面播放）。

修复（data-collector 文件服务；仅对 `hunter-video` 桶生效，其余桶保持裸对象语义）：
- **有效性标注**（`list_files` + `FileObjectItem` 新增 `video_size_bytes`/`is_orphan`）：对 `.json` 行配对探测同名 `.mp4` 的真实大小（head_object），`is_orphan` = 对应视频缺失或 0B（可安全删）；`.mp4` 行按自身大小判定。
- **会话配对删除**（`delete_files`）：删任一对象时连带删同会话的 `.json`+`.mp4`（仅计真实存在者），不再产生“有视频无记录/有记录无视频”的失联对象。
- **有效录像护栏**（`FileDeleteRequest.include_valid` 默认 false + `FileDeleteData.skipped`/`skipped_count`）：对应视频 >0B 的会话**默认拒删**（计入 skipped），除非显式 `include_valid=true`；从根上拦住误删。
- **前端**（`EventListView.vue`）：新增“录像有效性”列（有效/无效标签，仅 video 桶）；selection 列 `:selectable` **仅孤儿行可勾选**（有效录像在 UI 层就选不中）；删除确认文案说明“按会话配对删、有效录像受护栏保护”；回显 `skipped_count`。

向后兼容：`FileObjectItem`/`FileDelete*` 均为**新增可选字段**（默认 null/false/空），旧客户端不受影响；非 video 桶删除行为与旧版一致。

已知局限（如实标注）：本次事故中已被删掉 sidecar 的 2 个残留视频（>0B 无 sidecar）按新规则归为“有效录像”**受护栏保护、不可从 UI 删**，且因无 sidecar 仍不在操控记录显示（用户已确认“视频可不管”）；如需回收需手工重建 sidecar（本版未提供，属待办）。

风险：低。护栏默认开启且 UI 仅允许可选孤儿，误删路径基本封死；`include_valid=true` 为显式越过护栏的运维逃生口（不在 UI 暴露）。head_object 为 O(1)，单页 ≤200 行逐项探测成本可接受（仅 video 桶）。

验证（本环境，均实跑）：
1. `pytest services/data-collector -q` **182 passed**（新增 7 条：孤儿删 sidecar+0B视频、孤儿无视频仅删sidecar、有效会话被护栏skip、include_valid越护栏配对删、非配对扩展名直删、list 标注有效/孤儿、非video桶不标注）；契约门禁 `test_same_named_schemas_effective_fields_match_contract` 驱动本版本同步契约新字段；
2. `ruff check app` **All checks passed**；
3. `npm run type-check`（vue-tsc）**无错误**。

实机执行（**已回验通过**，2026-10-20）：
```bash
cd /opt/HunterCore && git pull
# 后端：新增标注/配对/护栏（需重建镜像）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build data-collector
# 前端：有效性列 + 仅孤儿可删（index.html 烘焙进镜像）
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build web-portal
# 回验：文件浏览选 hunter-video 桶→“录像有效性”列区分有效/无效；只有无效行可勾选；
#       删除无效行后对应 0B 视频一并消失；有效行无法选中（护栏）
```

分发路径：`services/data-collector/app/{schemas/files,services/files,routers/files}.py`、`services/data-collector/app/tests/test_file_service.py`、`contracts/openapi/data-collector.yaml`、`frontend/src/{types/data.ts,views/data/EventListView.vue}`（`api/data.ts` 无需改，include_valid 随 payload 透传）。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/故障排查指南.md`（新增“删 sidecar 后操控记录清空”判据：记录=sidecar投影、删json即删记录）、`docs/运维操作手册.md`（§7.10 改写为“有效性标注 + 仅孤儿可删 + 会话配对删除 + 有效录像护栏”的操作与部署口径）。

**待办（如实标注）**：① 本版**已实机回验通过**（2026-10-20 重建 data-collector + web-portal 后实测有效性列/仅孤儿可删/配对删除/护栏均落地），从待办迁移闭环；② 那 2 个已失联的残留视频（>0B 无 sidecar）本版不处理（用户确认“视频可不管”），若后续需回收再补“手工重建 sidecar”工具。

## V1.21.23（修复+重构：场景库 300 个场景详情雷同——旧批量脚本 i % n 确定性轮换生成；新增物理清空脚本 clear-scenes.sh + 按《场景生成器与场景管理详细设计文档》重写高斯扰动生成引擎 batch_add_scenes.py）

Date：2026-10-20
变更类型：修复 + 重构（scripts/ 两脚本。⚠ **不改契约（DDL/OpenAPI/Kafka）、不改服务代码、不改前端**；清空与灌库为服务器侧运维动作，生效需在生产环境执行 `clear-scenes.sh` + `batch_add_scenes.py`）

现场（用户报障 + 5 张后台截图）：运营管理后台「场景库」300 个场景中，同一类型的所有场景详情页内容完全一致——参与者表、事件表、config_json 参数（车速/距离/天气）逐字段相同，仅场景名称编号不同；无法作为差异化测试资产使用。用户要求：清空场景库（明确选择**物理删除**），并按设计文档重构生成器。

根因（生成侧脚本缺陷，非前端/DB bug）：
1. **旧 `scripts/batch_add_scenes.py` 以确定性 `i % n` 轮换生成**：每类型一个固定 builder（`_b_straight` 等），参数全部为硬编码常量，第 i 个场景仅按序号轮换模板 ⇒ 同类型内 config_json 完全复制，300 个场景实际只有约 16 个唯一样板。前端 `SceneDetailView.vue` 按 scene_id 逐条 GET、DB 按行存储，取证均无 bug。
2. **清空无法走 API**：scene-service `DELETE /api/v1/scene/{id}` 为软删除（置 deleted_at），且契约规定 published 场景不可直接删除（返回 3003）、无归档端点 ⇒ 300 个已发布场景逐条走 API 不可行，需直连 DB 物理 DELETE 并释放 scene_name 唯一约束。

修复内容：
- **新增 `scripts/clear-scenes.sh`（218 行，物理清空工具）**：复用 common.sh（load_env/require_env/container_running/wait_for），事务内 `DELETE FROM scene_svc.scenes <where>` + 删除后计数复核（非 0 即报错）；支持 `--dry-run`（仅统计）、`--type/--status/--creator` 过滤（status 白名单 + type/creator 字符校验防注入）、`--keep-soft-deleted`、`--no-cache-flush`；删除后失效 Redis 场景详情缓存（容器内 `--scan 'cache:scene:*'` + 批量 `UNLINK`，尽力而为不阻断主流程，TTL 3600s 自然过期兜底）。
- **重写 `scripts/batch_add_scenes.py`（591 行，高斯扰动生成引擎）**：按《场景生成器与场景管理详细设计文档》§2 实现——① 参数化场景模型：`SHARED_PARAMS`（雨/雾/风速/太阳高度角/方位角/时长）+ `TYPE_PARAMS`（16 类型各自几何/动力学参数，`Param(mean,std,lo,hi,risk,discrete)`）；② 三层分布区间采样 `ZONE_RATIO` common(±1σ)/edge(1–3σ)/longtail(>3σ)=60/30/10，长尾为 coherent 方向的多参数同向边界极值组合；③ 风险映射 `RISK_BY_ZONE` L1/L2/L4；④ 场景有效性校验 `_feasible`（参数范围 + 初值无碰撞 + 运动学预判）失败即拒绝采样重抽；⑤ 多样性去重：参数向量归一化后余弦相似度 > 0.95 判重（重试 `MAX_SAMPLE_TRIES=64` 耗尽则放宽保数量恒等于配额）；⑥ 新增 `--seed`（复现）/`--ratio`（比例覆盖）CLI 参数与 `diversity_report`（唯一 config 指纹数/平均参与者/重采样次数）；`SCENE_QUOTAS` 16 类型配额（和恒=300，assert 守卫）与认证提交/发布/限流逻辑保留。
- **元数据映射（不改 DDL，契约先行）**：设计文档 §5.1 `scenario_metadata` 新列（category/risk_level/distribution_zone/source）映射为现有 `tags` 受控前缀 `cat:`/`risk:`/`zone:`/`src:` + `description` 结构化文本，复用 tags GIN 索引与前端标签展示，不破坏固定契约与 verify_data_layer 校验。

向后兼容：仅新增/重写 scripts/ 下工具脚本；契约、服务、前端、DB 零改动。新版生成的 config_json 顶层结构仍严格符合 `SceneCreateRequest`（extra="forbid"）/4.2.2 场景配置契约。

风险：低-中。`clear-scenes.sh` 为不可恢复物理删除 ⇒ 默认交互确认 + `--dry-run` 先行 + 过滤值白名单防注入；新场景数值来自随机采样（给定 `--seed` 可复现），极端值已由 lo/hi 边界与 `_feasible` 校验兜底。

验证（本环境，均实跑）：
1. `bash -n scripts/clear-scenes.sh` 语法通过；过滤值注入用例（非法 status/字符）均被拦截；
2. `py -3.12 scripts/batch_add_scenes.py --dry-run --seed 20261020`：真实契约 Schema 加载成功（Schema=True），**300/300 通过 SceneCreateRequest 校验**；300 条 config 指纹**全部唯一**；风险梯度 L1=181 / L2=91 / L4=28（三层分布 60/30/10 落地）；
3. 同类型多样性抽查：cut_in 25/25、pedestrian_crossing 28/28、rainy 22/22 逐条 config 各异（ego 速度/时长/天气/参与者数/min_safe/事件触发阈值均不同）；
4. 修复过程缺陷：`_zone_counts` 初版把 longtail 余数恒置 0 导致 L4=0，已改为三区间各自 round 分配、余数归 common。

实机执行（清空已于 2026-10-20 13:59 在 `hunter-core-server:/opt/HunterCore` 实机执行通过；灌库待主机装 httpx 后重跑）：
```bash
cd /opt/HunterCore && git pull
# ① 先统计确认作用域，再物理清空（含 Redis 缓存失效）
sudo bash scripts/clear-scenes.sh --dry-run
sudo bash scripts/clear-scenes.sh            # 交互确认；非交互加 --yes
# ② 重新灌库（300 个高斯扰动场景；缺省即提交，--seed 可复现，--publish 创建后自动发布）
# 方式 A：经 api-gateway（JWT 登录）
python3 scripts/batch_add_scenes.py --mode gateway --base-url http://127.0.0.1:8080 \
  --username admin --password '<口令>' --seed 20261020 --publish
# 方式 B：直连 scene-service（身份头；服务配了 GATEWAY_HMAC_SECRET 时须传 --hmac-secret）
python3 scripts/batch_add_scenes.py --mode direct --base-url http://127.0.0.1:8081 \
  --hmac-secret "$GATEWAY_HMAC_SECRET" --seed 20261020 --publish
# ③ 复验：后台「场景库」同类型任意两场景详情应逐字段不同；风险/分布标签可见（tags: cat:/risk:/zone:）
```

**实机记录（2026-10-20）**：
- **清空已闭环**：`clear-scenes.sh --dry-run` 命中 261 条（非 300：历史批次实际入库量即此）→ 交互确认后 `BEGIN; DELETE 261; COMMIT`，删除后复核 0；Redis `cache:scene:*` 失效 0 个（旧键已随 TTL 自然过期，属正常）。
- **灌库首跑受阻断（已补防御）**：宿主机系统 `python3.10` 缺 `httpx`，traceback 在 `submit_scenes` 内部 `import httpx`（生成/校验全部跑完后才报，白跑一分钟）。本版已补**提交前依赖前置检查**：非 dry-run 在建场景前先探 httpx，缺失即退出码 2 并提示安装命令（`pip3 install httpx` 走镜像或 `apt-get install -y python3-httpx`）；本地复跑 dry-run 验证改动后仍 300/300、Schema=True。
- **Schema=False 降级说明（不阻断提交安全）**：服务器 python3.10 无法加载契约 Schema（StrEnum 需 3.11+）降级轻量校验；但同 `--seed 20261020` 下生成结果与本地已用真实 `SceneCreateRequest` 验过 300/300 的批次确定性一致（分布/指纹/重采样次数逐项吻合），可安全提交；如需服务器端也走真 Schema，装 Python 3.11+ 及 pydantic/sqlalchemy/asyncpg/greenlet/structlog 依赖即可。
- **创建成功但发布全 401 code=1001（gateway 单会话语义）**：实机 `--publish` 时 300 条**创建全部成功**（DB 复核 300 行、同类型 config 逐条唯一），但 300 条**发布均 401 code=1001**。根因非权限而是**认证**：`session:{user_id}` 以 Access Token **原样全等比较**代替黑名单（[api-gateway core/auth.py](file:///e:/python_work/HunterPilot/HunterCore/services/api-gateway/app/core/auth.py)）⇒ 同一用户 admin 全局只能有一个活跃会话。脚本登录后，浏览器 admin 后台在它跑创建的 ~37s 窗口内刷新了一次会话，`session:{admin}` 被旧浏览器 token 覆盖，脚本 token 即刻失效 ⇒ 时序上恰成“创建（覆盖前）全成、发布（覆盖后）全 401”。**非脚本缺陷、非服务缺陷，而是单会话登录设计的必然现象**。
- **本版新增 `--publish-drafts` 修复通道**（不重建场景、仅分页拉取全部 draft 逐一发布）：重新登录把会话抢回脚本侧即可闭环存量。使用须避开单会话竞争：二选一—①执行时先退出浏览器 admin 登录（或不开后台）；②为脚本单独用一个 operator 账号（不同 user_id → session 键隔离）。复跑幂等安全（已 published 的不入 draft 列表、不会被重复处理）。
- **lint 修正零行为影响**：同步清掉 `batch_add_scenes.py` 的 10 项机械 lint（RUF046 冗余 int()/F841 未用变量/SIM103）；修正后 `--dry-run --seed 20261020` 产出的 `scene_batch_300.json` 与旧版**逐字节一致**（`fc /b` 无差异），确认生成确定性未受扰动。

分发路径：`scripts/clear-scenes.sh`（新增）、`scripts/batch_add_scenes.py`（重写 + 本次补依赖前置检查与 `--publish-drafts` 修复通道）。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/故障排查指南.md`（新增「场景详情内容雷同」判据：旧批量脚本 i % n 确定性轮换所致、非前端/DB bug，附清空+重灌修复路径）、`docs/运维操作手册.md`（新增「场景库清空与重新灌库」操作规程：dry-run 先行、--yes 非交互、Redis 缓存失效、seed 复现）、`docs/deployment.md`（新增 §16「场景库批量灌库与清空工具」：两脚本用途/关键参数/宿主 Python 3.11+ 依赖，无需重建服务）与 `docs/01-部署概述与环境要求.md`（scripts/ 目录清单补 clear-scenes.sh 与 batch_add_scenes.py 新参数说明）。

**待办（如实标注）**：① 本版实机状态——**清空与创建均已实机回验**（见上实机记录：261 条已删、300 条已入库且同类型 config 逐条唯一）；**发布未闭环**：受 gateway 单会话与浏览器 admin 会话互斥影响、发布全部 401（属设计行为、非缺陷），需避开单会话竞争后跑 `--publish-drafts` 把存量 draft 发布（命令见上）；执行后在后台确认状态列均为已发布；② gateway “同一用户单会话”使得批量脚本与浏览器登录天然冲突（后登录踢前登录），属现有认证设计；若后续需“同一用户多端并发会话”需改 `session:{user_id}` 为多值结构（改 redis-keys 契约、走契约流程）；③ 设计文档 §5.1 独立元数据列（category/risk_level/distribution_zone/complexity_score/parent_scenario_id）当前以 tags 前缀模拟承载，若后续检索维度增多，需评审 DDL 变更；④ 旧 300 个场景被灌入的训练/仿真任务引用关系未逐一盘点——清空后旧 scene_id 失效，如有下游依赖需另行确认。

**待办①后续闭环（2026-10-20，V1.21.24 实机反馈）**：后台「场景库」列表截图确认 300 条状态列均为**已发布**（更新时间 2026-10-20 14:10:52-53），发布链路已闭环；同批次列表抽查可见 16 类型名称/标签（cat:/risk:/zone: 前缀）逐条各异，数据侧雷同问题彻底解决。

## V1.21.24（修复：前端详情页“内容雷同”的真因——路由参数切换时组件实例被复用、不重新拉取数据，场景/OTA 任务/车辆三个详情页同缺陷）

Date：2026-10-20
变更类型：修复（仅 frontend/ 三个视图；不改契约、不改服务代码、不改 DB；生效需重建 web-portal 镜像）

现场（用户报障 + 后台截图）：V1.21.23 重灌后，DB 复验同类型 config 逐条唯一、列表页名称/标签/类型均正常，但打开两个**不同**场景的详情页，页面内容完全一致——连**场景 ID、创建/更新时间都相同**（均为 `cddbfacc-2820-…`/SCN-036）。若为数据雷同，两条详情应各自展示不同 scene_id；ID 都相同 ⇒ 第二条详情根本没重新拉取，指向前端而非数据。

根因（Vue Router 组件复用 + keep-alive 下缺参数 watch）：
1. `/scenes/:scene_id` 与 `/ota/tasks/:task_id`、`/vehicles/:vehicle_id` 均为**同一 route record 仅参数不同**——vue-router 默认复用组件实例不重建，且 `MainLayout.vue` 的 `<keep-alive :max="3">` 进一步缓存实例，setup/onMounted 只在首次进入执行一次；
2. `SceneDetailView.vue` 仅在 setup 尾部 `void loadScene()` 一次；`OtaTaskDetailView.vue`/`VehicleDetailView.vue` 仅在 `onMounted` 拉一次——从列表先后进入两条详情时，页面停留在**第一条**的数据；
3. `VehicleDetailView.vue` 额外更重：`vehicleId` 是 setup 时的**非响应式快照**（`String(route.params.vehicle_id)`），即使去掉 keep-alive 也不会随路由更新。
注：与 V1.21.23 的生成器雷同为**两个独立问题叠加**：旧数据真雷同（i % n 生成）已修；新数据已唯一但前端复用旧实例造成“看起来仍雷同”的假象。

修复内容（三视图统一为 watch 路由参数 + immediate 首拉，切换时先置空旧对象防脏展示）：
- `frontend/src/views/scene/SceneDetailView.vue`：`void loadScene()` → `watch(sceneId, …, { immediate: true })`（切换时 `scene.value = null` 后重拉）；
- `frontend/src/views/ota/OtaTaskDetailView.vue`：`onMounted` 仅保留一次性 `vehicleStore.fetchVehicles()`，任务详情/升级记录改由 `watch(taskId, …, { immediate: true })` 触发；
- `frontend/src/views/vehicle/VehicleDetailView.vue`：`vehicleId` 改为 `computed`（全部 7 处用法同步 `.value`，模板未直接引用无需改），`onMounted(load)` → 文末 `watch(vehicleId, …, { immediate: true })`（watch 回调引用 `revealedPassword` 等后置声明变量，须置于所有声明之后防 TDZ）。
未改 `MainLayout.vue` 的 keep-alive 策略（页面级缓存仍保留列表滚动位置等体验），在视图层修复更可控。

向后兼容：仅改三个 `.vue` 文件的数据加载触发时机；接口调用、表单逻辑、模板结构零变化。模板均已有 `v-if="scene"/v-if="task"`/`<template v-if="detail">` 空值防护，置空重拉不会报错。

风险：低。唯一行为差异：同页参数切换时多一次 GET（以前不发）；keep-alive 回退场景（同一 id 激活）不触发额外请求。

验证（本环境，均实跑）：`npm run type-check`（vue-tsc --noEmit）通过；`npx vite build` 成功（2345 modules，SceneDetailView/VehicleDetailView/OtaTaskDetailView 三个 chunk 正常产出；首跑因本机内存不足致 esbuild 进程 OOM，重试即过，与代码无关）。浏览器实机复核待 web-portal 镜像重建后执行。

实机执行（`hunter-core-server:/opt/HunterCore`）：
```bash
# ① 同步 frontend/src 变更后重建前端镜像并替换容器
#   ⚠ 必须在 APP_DIR 根（/opt/HunterCore）执行并显式指定 -f 与 --project-directory .，
#     使 compose 能读到根目录 .env（VERSION 等）；在 infra/deploy 子目录裸跑会因
#     project directory 错位找不到 .env → VERSION 为空 → invalid tag "hunter/web-portal:"。
cd /opt/HunterCore
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . build web-portal
sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d web-portal
# ② 浏览器硬刷新（Ctrl+F5，防 index.html 命中旧缓存）后复验：
#    先后打开两个不同场景详情，页面头部“场景 ID”应各自不同；OTA 任务/车辆详情同理
```

分发路径：`frontend/src/views/scene/SceneDetailView.vue`、`frontend/src/views/ota/OtaTaskDetailView.vue`、`frontend/src/views/vehicle/VehicleDetailView.vue`（需重建 web-portal 镜像方生效；仅 git pull 不重建无效）。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪）：`docs/故障排查指南.md` §17 增补“同一现象两个成因”鉴别判据（两条详情 scene_id 相同→前端复用缺陷 V1.21.24；ID 不同但参数相同→旧批量数据问题 V1.21.23）；`docs/运维操作手册.md` §3.9 补灌库后验收步骤的前端复验提醒。

**待办（如实标注）**：① 本修复本环境仅过 type-check/build，**未实机浏览器复核**（需重建 web-portal 后按上述②验收）；② 其余含路由参数的详情类视图若后续新增，需沿用“watch 参数 + immediate”模式；前端开发规范文档沉淀尚未单独立项。

## V1.21.25（增强：场景库从 300 扩展到 1200，覆盖常见/极端/组合多风险场景——16 类型内引入子场景（variant）层，不改 18 值固定契约）

Date：2026-10-20
变更类型：增强（仅 `scripts/batch_add_scenes.py` 一个生成器脚本。⚠ **不改契约（DDL/OpenAPI/Kafka）、不改服务代码、不改前端**；扩量属服务器侧运维动作，生效需在生产环境重跑 `clear-scenes.sh` + `batch_add_scenes.py`）

现场（用户需求）：将场景从 300 个扩展到能覆盖常见场景、极端场景并满足一般自动驾驶仿真测试要求。

约束与设计（不触 18 值契约）：`SceneType` 的 18 个取值（16 可生成 + custom + real_vehicle_replay）是 [contracts/openapi/scene-service.yaml](file:///e:/python_work/HunterPilot/HunterCore/contracts/openapi/scene-service.yaml) 固定枚举，前端 `enums.ts` 注释“18 种，不可新增”⇒ **覆盖面不能靠新增类型**。但 `config_json` 内的 `behavior_type`/`action_type`/`trigger.parameters` 均为契约**自由字符串/字典**（enums.md 待确认 #3 未加枚举），构成合法扩展空间。故引入**子场景（variant）层**：在不新增 scene_type 的前提下，每个类型枚举若干业界典型测试子场景，通过三种手段差异化：
- **overrides**：替换/扩展该子场景的参数分布（如儿童行人 ped_speed 更高、鬼探头 cross_dist 更短、浓雾 vis_dist 更小）；
- **force**：强制固定部分参数以构造**多风险叠加的极端场景**（如夜间行人 sun_alt=-30 且 rain=70、暴雨 rain=90、高速湿滑急刹近距）；
- **augment**：结构化装配标签，由新增的 `_augment` 据此向已装配 config 注入额外参与者/事件（遮挡车、对向来车、背景密集车流、级联传感器失效、连续切入/变道、施工行人等）。事件 type 仅用契约七值，参与者 type 仅用 vehicle/pedestrian/other。

修复内容（均在本环境实跑验证）：
- **新增 `Variant` 数据类 + `VARIANTS` 字典**：16 类型共 63 个子场景定义（straight_cruise/curve_driving/car_following/lane_change/cut_in/pedestrian_crossing/intersection_meeting/construction_detour/rainy/night/foggy/backlight/emergency_braking/obstacle_appearance/sensor_failure/multi_vehicle_mixed），附 `assert set(VARIANTS)==set(SCENE_QUOTAS)` 守卫。
- **新增 `_augment(cfg, stype, p, variant)`**：按 augment 标签就地注入结构化要素（~20 个分支，涵盖遮挡/密集/对向/级联失效/学校区域等）。
- **配额 300→1200**：`SCENE_QUOTAS` 按测试重要性重新加权（和恒=1200，`TOTAL_SCENES`+assert 守卫），pedestrian_crossing/emergency_braking/cut_in 等高价值类型配额最高。
- **`_make_scene`/`generate_scenes` 变体感知**：逐类型→逐子场景→逐分布区间分层采样；`accepted` 向量在同一类型内跨子场景共享（保障类型内逐条唯一）；名称改为 `SCN-{g:04d}-{ABBR}-{variant.sub}-{risk}-{token}`（序号 3→4 位）；tags 新增 `sub:{variant.key}`。
- **新增 `_enforce_unique_configs` 兜底**：长尾区强约束子场景（kind_idx 固定等）在 MAX_SAMPLE_TRIES 耗尽后放宽会产出少量完全重复的 config；此函数仅对检出的重复项施加最小抖动（自车初速 +0.01m/s，仍在契约速度域、不改语义），保证逐条唯一。
- 同步更新 `DEFAULT_OUTPUT`→`scene_batch_1200.json`、模块文档、CLI 描述、分布报告（新增“覆盖子场景 x/y”行、去重后的唯一 key 计数口径）。

向后兼容：仅改生成器脚本；旧 `scene_batch_300.json` 不再生成。新 1200 条与旧 300 条同层分布、同源采样，存在大量近重复⇒**推荐清空重灌**（而非增量叠加）。名称含 4 位序号 + 服务端 3002/409 幂使增量也安全。

风险：低。覆盖面均落在契约自由字段内，不触碰受控枚举；`_augment` 仅追加已存在合法类型的参与者/事件；极端组合由 `_feasible` 校验 + lo/hi 边界 + 真 Schema 兼容。`_enforce_unique_configs` 仅在重复时微量抖动，正常场景不受影响。

验证（本环境，均实跑）：
1. `py -3.12 scripts/batch_add_scenes.py --dry-run --seed 20261020`：真实契约 Schema 加载成功（Schema=True），**1200/1200 通过 `SceneCreateRequest` 校验**；1200 条 config 指纹**全部唯一**（兜底前 1198、兜底后 1200）；风险梯度 L1=719 / L2=360 / L4=121（≈ 60/30/10）；覆盖类型 16/16、子场景 62/62（同名 key `dense` 在不同类型去重后计数）；
2. `.venv\Scripts\ruff.exe check scripts/batch_add_scenes.py` **All checks passed**；`py_compile` 通过；
3. 修复过程缺陷：`Variant` dataclass 初版用可变默认 `overrides={}` 触发 `ValueError: mutable default`，已改 `field(default_factory=dict)`。

实机执行（待服务器侧执行；建议先拉代码再走下述清空→重灌→发布流程）：
```bash
cd /opt/HunterCore && git pull
# ① 先统计确认作用域，再物理清空旧 300（含 Redis 场景详情缓存失效）
sudo bash scripts/clear-scenes.sh --dry-run
sudo bash scripts/clear-scenes.sh --yes
# ② 重灌 1200（新 seed；--publish 创建后自动发布；需宿主 Python 3.11+ 启真 Schema）
python3 scripts/batch_add_scenes.py --mode gateway --base-url http://127.0.0.1:8080 \
  --username admin --password '<口令>' --seed 20261021 --publish
# ③ 若发布受单会话互斥影响返 401，避开浏览器登录后走兼容通道把存量 draft 发布：
python3 scripts/batch_add_scenes.py --mode gateway --base-url http://127.0.0.1:8080 \
  --username admin --password '<口令>' --publish-drafts
# ④ 复验：总数=1200 且逐条唯一（DB：SELECT count(*), count(DISTINCT md5(config_json::text)) FROM scene_svc.scenes WHERE deleted_at IS NULL;）
```

分发路径：`scripts/batch_add_scenes.py`（子场景层 + 兜底唯一性；需重跑灌库方生效，仅 git pull 不重灌无效）。
文档同步（运维文档，均被 `.gitignore` 排除、只更新不追踪，须单独同步到服务器）：`docs/运维操作手册.md`（场景库重灌口径 300→1200）、`docs/deployment.md` §16（生成器规模与子场景说明）、`docs/01-部署概述与环境要求.md`（scripts/ 清单口径）。

**实机记录（2026-10-20，服务器 `hunter-core-server:/opt/HunterCore`）**：
- **清空→重灌已实机执行并复验通过**：`clear-scenes.sh --yes` 物理删除旧 300 条（`DELETE 300` + 复核）；`batch_add_scenes.py --mode gateway ... --seed 20261021 --publish` 提交 1200 条。
- **交付复验（DB）**：`SELECT count(*), count(DISTINCT scene_name), count(DISTINCT md5(config_json::text)) FROM scene_svc.scenes WHERE deleted_at IS NULL;` → **1200 / 1200 / 1200**，全部 `published`、0 `draft`；覆盖面/逐条唯一完全达成。
- **尾报“发布失败 1200（3003）”为脚本计数误报、非真故障**：3003 = “仅 draft 可发布”。`docker logs hunter-scene` 中 `scene_published` 计数 **1426**（≥ 1200）、`scene_created` 计数 1500，说明发布在服务端已真实成功（含此前重复/并发调用轮次）；本轮再对已 `published` 场景发 `POST /publish` 命中幂等保护返回 3003，被 `_post_scene`/`_publish_scene` 计入 `failed`。`--publish-drafts` 命中 0 条 draft 也同理（存量早已是 published）。**判定灌库是否真失败应看 DB status 分布 + `scene_published` 计数，而非脚本尾报。**
- **Schema=False 降级**：服务器系统 `python3.10` 无法加载契约 Schema（StrEnum 需 3.11+）降级轻量校验；同 `--seed` 确定性结果与本地已用真 `SceneCreateRequest` 验过的 1200/1200 批次一致，可安全提交。

**待办（如实标注）**：① 本版实机重灌**已执行并复验通过**（见上实机记录：1200 全已发布、逐条唯一），从待办迁移闭环；② **脚本尾报误报缺陷（非阻断）**：`--publish` 把已发布场景的重复 publish（3003 幂等保护）计入 `failed`，导致“新建成功但发布失败 1200”这种误导性输出（实际已全发布）——**已于 V1.21.26 修复**（publish 返回 3003/409 归为幂等成功）；③ 旧 300 与新 1200 同源分布，本次已走清空重灌闭环；若有下游训练/仿真任务引用旧 scene_id，清空后需另行确认；④ 子场景覆盖面基于设计文档与业界典型场景枚举，不保证穷举所有 ODD 边界，若需更密的长尾可提高 `--ratio` 长尾占比或扩充 VARIANTS。

---

## V1.21.26（修复：`batch_add_scenes.py --publish` 尾报“发布失败 3003”为幂等误报——将已发布场景的重复 publish 归为幂等成功，不再计入 failed）

**现场**：V1.21.25 实机重灌时，脚本输出“新建 1200、发布 0、失败 1200 code=3003”，看似发布全失败；但 DB 实为 1200 条全 `published`、0 `draft`（`scene_published` 日志计数 1426），发布实际已成功。误导性尾报会让运维误判灌库失败。

**根因**：scene-service `publish_scene` 仅在 `model.status is not SceneStatus.DRAFT` 时抛 `ResourceStateConflictError`（HTTP 409 / code=3003，语义“仅 draft 可发布”）。对存量已发布场景重复发 `POST /publish` 命中此幂等保护；而 `_post_scene`/`_publish_scene` 只把 200/201+code=0 计为成功，409 落入失败分支计入 `failed`（无类于 create 将 3002/409 归为 `existed`）。

**修复内容**（`scripts/batch_add_scenes.py`，仅改客户端计数，不改服务端/契约）：
- `_publish_scene`：新增 `code == 3003 or status_code == 409` 分支→ `stats["pub_existed"] += 1`（幂等：发布态已达成），不再计入 failures。
- `publish_drafts._pub`：同口径新增 3003/409 幂等分支→ `stats["already"] += 1`。
- 尾报：提交行与补发布行均展示“幂等已发布”计数；stats 初始化同步新增 `pub_existed`/`already`。
- 退出码：`failed_total`/`failed` 不再含 3003，重跑幂等场景时正常返回 0。

**向后兼容**：仅影响输出计数口径，不改变任何服务端交互与数据；首次真发布（draft→published）仍走原 `published` 分支。

**风险**：低。唯一需区分的是——若将来 409 出现非 3003 的其他含义（目前 409 只来自 publish 的 3003），会被一并归为幂等；已用 `code == 3003` 为主判、`status_code == 409` 为兼底。

**验证（本地）**：`py_compile` 通过、`ruff check` 全部通过；`--dry-run --seed 20261021` 仍 1200/1200 Schema=True、唯一指纹 1200。幂等分支需连服务端触发（向已存在集重跑 `--publish` 即自然验证），本地不可达。

**实机执行**：无需服务重建（纯脚本）。下次向已有场景集重跑 `--publish` 时，尾报应显示“发布: N（幂等已发布: M） 失败: 0”而非之前的“发布失败 1200”。

分发路径：`scripts/batch_add_scenes.py`（`git pull` 即生效）。文档同步：`docs/运维操作手册.md` §3.9 已补“3003 多为幂等误报、判定灌库成败看 DB status + `scene_published`”（V1.21.25 一并写入）。

**待办（如实标注）**：① 本版幂等分支仅本地 py_compile/ruff/dry-run 验证，**尚未实机触发**（需下次对已存在集重跑 `--publish` 观察尾报）；若实机尾报仍异常需回查。

---

## V1.22.0（增强：SceneType 由 18 值固定枚举改为数据驱动受控字典（单一事实源 `contracts/scene-taxonomy.yaml`），18→48 叶子对齐 GB/T 47025，取消“不可新增”约束）

Date：2026-10-21
变更类型：增强（契约 + 共享库 + scene-service + 生成器 + 前端全链路；`scenes.scene_type` 为 TEXT 无 CHECK⇒**无需 DDL/迁移**）

**现场（用户需求）**：V1.21.25 曾受“18 值固定契约、前端 enums.ts 不可新增”约束，覆盖面只能塞进 `config_json` 自由字段。现要求把场景类型做成**可持久扩充**：新增一个场景类型只需改一处 YAML 字典即可入库，无需改 Python/前端逻辑，并与 OpenSCENARIO/GB-T 47025 对齐。

**根因（旧机制不可扩充）**：`SceneType` 为 `StrEnum`（硬编码 18 值），`SCENE_TYPES_BY_CATEGORY`/`SCENE_TYPE_LABELS`/`behavior_type`/`action_type`/`trigger.condition_type` 均散硬编码于 schema 与前端，任何新增需同时改后端枚举 + 前端表 + 契约 enum，易漂移。

**修复内容**（数据驱动受控字典）：
- **单一事实源** `contracts/scene-taxonomy.yaml`：48 个叶子类型（basic 12 / interactive 10 / environment 8 / corner_case 16 / custom 1 / real_vehicle 1）+ `behavior_types`/`action_types`/`trigger_condition_types` 三词表 + `trigger`/`action` 参数键白名单 + `enumerable_parameter_values`（rule/edge/coordinate_system/relative_distance_type/distribution 受控取值）。
- **加载器** `common/python/hunter_common/scene_taxonomy.py`：`get_scene_taxonomy()` 进程内缓存；定位顺序 `SCENE_TAXONOMY_FILE` 环境变量 → 向上查找 `contracts/scene-taxonomy.yaml`（cwd/包位置）；**字典缺失/非法即抛 `SceneTaxonomyError`（5000），不降级**。
- **schema** `services/scene-service/app/schemas/scene.py`：`SceneType` 改为 `Annotated[str, AfterValidator(ensure_scene_type)]`（越界→422/2001）；`behavior_type`/`action_type`/`trigger.condition_type` 同受控；`trigger`/`action` 的 `parameters` 由 `model_validator` 校 `enumerable_parameter_values`；`SCENE_TYPES_BY_CATEGORY`/`SCENE_TYPE_LABELS`/`SCENE_CATEGORY_BY_TYPE` 均从字典派生。
- **模板服务** `app/services/templates.py`：字典驱动为除 `real_vehicle_replay` 外全 47 类叶子预置模板（`tpl-{scene_type}`）。
- **生成器** `scripts/batch_add_scenes.py`：`SCENE_QUOTAS`/分类/标签/缩写均从字典派生（`BATCH_EXCLUDED_TYPES={real_vehicle_replay, custom}`，得 46 可批量生成）；新增兼容兜底生成路径，编辑 YAML 新增类型无需改脚本。
- **契约** `contracts/openapi/scene-service.yaml`：`SceneType` 去 `enum` 改 `type: string` + `x-controlled-vocabulary: contracts/scene-taxonomy.yaml`；`x-hunter-scene-types` 扩至 48、`status: confirmed_extensible`；`behavior_type`/`action_type`/`trigger.condition_type` 声明受控引用。`contracts/database/enums.md` 新 §9 登记 `scene_type` 为受控字典词表（无 CHECK、无需 DDL）。
- **前端** `scripts/gen_scene_taxonomy_ts.py`（codegen）从 YAML 生成 `frontend/src/constants/sceneTaxonomy.ts`（SceneType 联合 + 四类标签/分类映射）；`types/scene.ts` 的 `SceneType` 改为引用生成产物；`constants/enums.ts` 删手写 18 值表改 `export * from './sceneTaxonomy'`；`package.json` 新增 `codegen:scene-taxonomy`/`codegen:check`。
- **镜像** `services/scene-service/Dockerfile`：runner 阶段 `COPY contracts/scene-taxonomy.yaml ./contracts/` + `ENV SCENE_TAXONOMY_FILE=/app/contracts/scene-taxonomy.yaml`（启动即加载、缺失不降级，故必须内置）。
- **测试**：契约测试改为“taxonomy.yaml == x-hunter-scene-types == 后端派生字典”（去“== OpenAPI 固定 enum”）、`EXPECTED_CATEGORY_SIZES` 更新为 {basic:12,interactive:10,environment:8,corner_case:16,custom:1,real_vehicle:1}；服务层/端点模板总数 17→47、环境过滤 4→8；新增越界用例（scene_type/behavior_type/action_type/condition_type 越界→422、受控枚举参数值越界→422、合法新类型 `occluded_pedestrian` 可入库）；`SceneType` 枚举成员引用全部改为字典派生。

**向后兼容**：新字典为旧 18 值的**超集**（保留原值与生成器在用的 behavior/action 词）；校验仅在 create/update 入库时触发，存量数据读取不受影响；`scenes.scene_type` TEXT 无 CHECK，无需迁移。

**⚠ 对批准计划的一处工程偏离（如实记录）**：计划原写“trigger/action 未知参数键拒绝”。实现改为**仅校验受控枚举键（`enumerable_parameter_values`）的取值，不拒绝自由结构键**，以兼容 4.5 节实车回放自动提取产生的任意轨迹/环境元数据（`trigger`/`action.parameters` 契约 #3 本为自由字典）。若需收紧为全键白名单，改 `validate_enumerable_parameters` 即可。

**风险**：中。字典为运行期硬依赖（Dockerfile 已内置，K8s 可以 ConfigMap/bind-mount `contracts/` 覆盖）；若镜像/挂载均无字典文件，scene-service 启动即抛 5000（fail fast，不静默降级）。存量旧类型仍在字典内，无写入回归。

**验证（本机 `.venv` Python 3.12.0 实跑全绿）**：
- `pytest services/scene-service -q` → **114 passed**（含 6 项新增受控字典越界/入库用例）
- `pytest common/python services/data-analytics -q` → **453 passed**（无回归）
- `batch_add_scenes.py --dry-run --seed 20261021` → **2490/2490 条 Schema=True**，覆盖 46/46 可批量生成类型、子场景 64/123
- `ruff check services/scene-service scripts common/python/hunter_common/scene_taxonomy.py` → **All checks passed**
- `mypy --ignore-missing-imports --follow-imports=skip services/scene-service/app/schemas/scene.py app/services/templates.py` → **Success: no issues found**
- `gen_scene_taxonomy_ts.py --check` → **sceneTaxonomy.ts 与受控字典一致**（幂等）
- `frontend: vue-tsc --noEmit -p tsconfig.json` → **无输出、退出 0（clean）**

注：`common/python` 全包 `ruff` 另有 5 项 `UP046/UP047`（属未触碰的 `repository.py`/`responses.py`，预先存在、非本次改动引入）；`mypy` 对 `scene_taxonomy.py` 会报 `yaml` 无类型存根缺失（依赖声明层，非逻辑问题）。CI/服务器（python3.11+）仍应按下节复跑并含 Docker 镜像重建/端到端。

**实机执行（服务器部署与验证步骤，生效前必做）**：

> 本节是**在真实部署环境（Docker / 运行中的 scene-service）需按序执行的步骤**，本机未起 Docker daemon，故 Step 5–7（镜像重建/端到点/入库）**尚未在本环境执行**，属待办①。以下命令均假设在项目根 `APP_DIR`（本机开发为仓库根，服务器为 `/opt/HunterCore`）执行。
>
> ⚠ **Step 3–4 需带依赖的 python3.11+ 环境（CI 或项目 `.venv`），不能直接跑在生产宿主机**：实机反馈服务器 `python3` 为**系统 3.10 且未装依赖**，直跑会先报 `ModuleNotFoundError: No module named 'hunter_common'/'pydantic'/'fastapi'/'sqlalchemy'/'confluent_kafka'`，`common/python` 还会在 `from datetime import UTC` 处报错（3.11+ 才有）。**宿主机无依赖时跳过 Step 3–4**，运行期正确性由 Step 6 端到点 + 容器健康（字典缺失 fail-fast）兼底；必须在服务器上跑单测/dry-run 时，用带依赖的容器代跑（例如基于 builder 阶段的镜像 `docker compose run --rm --build scene-service python -m pytest ...`，需临时装 pytest）或将仓库 `.venv` 拉齐后执行。
>
> 关键机制（须知）：compose 已将 `./contracts` **只读绑定挂载**到 scene-service 的 `/app/contracts:ro`（`x-contracts-vol`），且 Dockerfile 设 `ENV SCENE_TAXONOMY_FILE=/app/contracts/scene-taxonomy.yaml`——因此容器内总能看到字典（无论来自 bind mount 还是镜像 COPY）。但 bind mount 不能代替镜像内置：**裸 `docker run`/K8s 无挂载时依赖镜像 COPY**，而 COPY 曾因 `.dockerignore` 排除 `contracts` 整目录而构建失败（**V1.22.1 已修**，见下方常见故障判据）。schema/模板逻辑已改，**必须 `--build` 重建镜像**才生效，仅重部不够。

**Step 0・准备**：`ssh` 入服务器 → `cd /opt/HunterCore` → 确认 `.venv` 或系统 `python3 --version` 为 3.11+；建议先备份场景库（`sudo docker exec hunter-postgres pg_dump -U hunter hunter_core > /tmp/scene_backup.sql`，或至少备份将重部的 compose 状态）。定义 compose 别名（与运维手册一致）：
```bash
dcompose() { sudo docker compose -f infra/deploy/docker-compose.yml --project-directory . "$@"; }
```

**Step 1・拉代码 + 确认字典入库**：
```bash
cd /opt/HunterCore && git pull
ls -l contracts/scene-taxonomy.yaml            # 必须存在（已纳入版本控制）
grep -c 'value:' contracts/scene-taxonomy.yaml # 实机基线 138（48 叶子 + 各词表 value 项）；仅作存在性参考，权威校验看 Step 2/3
```

**Step 2・静态一致性校验（无需依赖服务运行）**：
```bash
python3 -m py_compile services/scene-service/app/schemas/scene.py \
  services/scene-service/app/services/templates.py \
  common/python/hunter_common/scene_taxonomy.py scripts/batch_add_scenes.py
python3 scripts/gen_scene_taxonomy_ts.py --check   # 前端产物与字典一致，不一致退出 1
```

**Step 3・运行期单测（验证受控字典与入库校验）**：
```bash
SCENE_TAXONOMY_FILE=contracts/scene-taxonomy.yaml \
  python3 -m pytest services/scene-service -q                 # 预期 114 passed（含 6 越界/入库用例）
SCENE_TAXONOMY_FILE=contracts/scene-taxonomy.yaml \
  python3 -m pytest common/python services/data-analytics -q  # 预期 453 passed（无回归）
```

**Step 4・批量生成全量真 Schema 校验（`--dry-run`，不写库）**：
```bash
# 在带依赖环境（CI / 项目 .venv）：
SCENE_TAXONOMY_FILE=contracts/scene-taxonomy.yaml \
  python3 scripts/batch_add_scenes.py --dry-run --seed 20261021
# 预期：[validate] 通过 2490/2490 条（Schema=True）；覆盖 46/46 可批量生成类型
# 若在无依赖的生产宿主机，用 scene-service 镜像（自带 pydantic/httpx/hunter_common/app）跑：
# ⚠ 镜像以 uid 10001 运行，宿主 scripts/output/ 多为 root:root 755 → 直写会失败；脚本 `_dump_scenes`
#   会自动回退容器 /tmp（--rm 后即失）再回退 stdout，**校验结论不受影响**。
#   需留存 JSON 时加 `--user 0`（让进程以 root 写宿主 scripts/output；一次性校验容器可接受）：
docker run --rm --network hunter-net --user 0 -v "$PWD:/repo" -w /repo --entrypoint python3 \
  hunter/scene-service:0.1.0 scripts/batch_add_scenes.py --dry-run --seed 20261021
```

**Step 5・前端产物与镜像重建（含字典 + 新逻辑）**：
```bash
# （可选本地预验）cd frontend && npm run codegen:check && npm run build && cd ..
dcompose build --no-cache scene-service                 # 重建含 COPY 字典与新 schema 的镜像
# ❗必须确认上一步 build 真正成功（末行出现 Successfully tagged / FINISHED 且无 ERROR）：
#   若 COPY contracts/scene-taxonomy.yaml 报 not found → 先修 `.dockerignore`（V1.22.1 已修，见常见故障判据），
#   否则 `up --force-recreate` 会静默复用旧镜像（build 失败不阻断 up），导致部署看似完成、实际仍跑旧代码。
dcompose up -d --force-recreate scene-service           # 重部（container_name: hunter-scene）
sudo docker inspect -f '{{.Image}} {{.Created}}' hunter-scene  # 确认镜像 ID 已变（非旧镜像）
docker exec hunter-scene ls -l /app/contracts/scene-taxonomy.yaml  # 确认容器内可见字典

# V1.22.2：前端字典标签（48 类中文）必须同时重建 web-portal，否则类型列仍按旧 18 值表回退显示英文
dcompose build --no-cache web-portal
dcompose up -d --force-recreate web-portal
sudo docker inspect -f '{{.Image}} {{.Created}}' hunter-web    # 镜像 ID 已变
# 浏览器 Ctrl+F5 强刷后复核：类型列全中文、脚本生成场景创建者显示“批量创建”
```

**Step 6・端到点验证（仅验证字典加载与入库校验，不批量写库）**：
```bash
curl -fsS http://127.0.0.1:8081/healthz && curl -fsS http://127.0.0.1:8081/readyz   # healthy（8081 已绑 127.0.0.1）
sudo docker inspect -f '{{.State.Health.Status}}' hunter-scene                        # 预期 healthy

# ⚠ 服务器若配了 GATEWAY_HMAC_SECRET，直连 scene-service 会 401（data:null）；统一走网关 JWT 复核：
TOKEN=$(curl -s -X POST http://127.0.0.1:8080/api/v1/user/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<pwd>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["access_token"])')
curl -s http://127.0.0.1:8080/api/v1/scene/templates -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print("total=",json.load(sys.stdin)["data"]["total"])'   # 预期 total=47（旧镜像 17）
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8080/api/v1/scene \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"scene_name":"probe_bad","scene_type":"not_a_real_type","config":{}}'   # 预期 422（拒绝非法值）
# ⚠ 该越界用例新旧镜像**都会 422**（旧版 18 值 StrEnum 亦拒），只能证明“校验在工作”，**不能区分版本**；
#   版本生效与否只看上一条 `total`（旧 17 / 新 47）。
```
> 仅在未配 `GATEWAY_HMAC_SECRET` 的 dev/test 才能直连 `127.0.0.1:8081`（带 `X-User-Id`/`X-Roles`）；生产已配 HMAC 时必须走网关 JWT，或直连补 `X-Internal-MAC`（`hunter_common.internal_auth.build_identity_headers`）。
>
> ⚠ 网关 JWT 为**单会话语义**：本条登录会覆盖同账号（admin）已在 Web 端的会话（前端需重新登录）；紧接着的 Step 7 也会再登录一次并再次覆盖，属预期。

**Step 7・场景库入库（⚠不可逆，需产品确认后再做）**：若需用 48 类新场景覆盖存量库，先物理清空再批量灌库（仅 `git pull` 不会使新类型入库）。❗ `batch_add_scenes.py` 需 `pydantic`/`httpx`/`hunter_common`/`app`，宿主机裸跑必报 `ModuleNotFoundError`；**改用 scene-service 镜像跑（镜像已含全部依赖，`PYTHONPATH=/app`、`SCENE_TAXONOMY_FILE` 已内置）**。容器在 `hunter-net` 上，`--base-url` 用服务名：
```bash
bash scripts/clear-scenes.sh --dry-run    # 先看统计不删；确认无误后加 --yes（直连 DB，不需 py 依赖）
# 经网关入库（JWT；发布需 execute 权限）。挂载只读，不落盘（非 dry-run 不写文件）。
# 建议先加 `--limit 5` 冒烟（验证 登录/创建/发布 链路），通过后再去掉跑全量：
docker run --rm --network hunter-net -v "$PWD:/repo:ro" -w /repo --entrypoint python3 \
  hunter/scene-service:0.1.0 scripts/batch_add_scenes.py \
  --mode gateway --base-url http://api-gateway:8080 \
  --username admin --password '<pwd>' --seed 20261021 --publish
# 或直连 scene-service（需补 HMAC）：--mode direct --base-url http://scene-service:8081 \
#   --user-id 00000000-0000-0000-0000-000000000001 --roles admin --hmac-secret "$GATEWAY_HMAC_SECRET"
```
> 注：容器内不能用 `127.0.0.1` 访问网关/服务（那是容器自身）；必须用 `hunter-net` 服务名 `api-gateway:8080` / `scene-service:8081`。
>
> ⚠ **gateway 是单会话语义**（Redis `session:{user_id}` 仅保留最新 token）：跑批期间若用**同一账号**登录 Web 端，batch 手里的旧 token 立即 401/1001，且脚本**不会自动重登**（401 不重试、直接计失败）。处置：停用同账号前端登录 → **重跑同一条命令**（已创建按 3002 幂等跳过，只补未建成的）→ 若仅“已创建但未发布”，用 `--publish-drafts` 补发布（不新建）。建议跑批用独立管理员账号。

**回滚方案**：代码侧 `git checkout <上一 tag> && dcompose up -d --build --force-recreate scene-service` 即回到旧 18 值枚举；旧镜像无字典 COPY、但旧 schema 不依赖字典，可正常启动。字典为超集且校验仅在 create/update 触发，回滚不影响存量数据读取。若 Step 7 已入库且需回退，同样用 `clear-scenes.sh` + 旧版生成器重灌。

**常见故障判据**：
- scene-service 启动即抛 **5000**（`SceneTaxonomyError`）→ 容器内 `/app/contracts/scene-taxonomy.yaml` 缺失（`git pull` 未带文件 / bind mount 未生效）；查 `docker exec hunter-scene ls /app/contracts`。
- **`dcompose build scene-service` 在 `COPY contracts/scene-taxonomy.yaml` 报 `"/contracts/scene-taxonomy.yaml": not found`**→ 根因是 `.dockerignore` 把 `contracts` **整目录排除**、文件未进构建上下文（**V1.22.1 已修**：改为 `contracts/*` + `!contracts/scene-taxonomy.yaml`）。服务器需 `git pull` 拉到新 `.dockerignore` 后重新 build。验证：`docker build -f services/scene-service/Dockerfile -t tmp/scene-svctest .` 能走到最后一步。
- **build 失败但 `up` 却“成功”**→ `docker compose up` 不重构建，会静默用旧镜像。若字典仅靠 bind mount 出现在容器内而镜像内 schema 仍旧 → V1.22.0 改动**未真正生效**（典型：模板仍 17 个、越界仍放行）。以 Step 5 的“镜像 ID 已变”+ Step 6 的 `total=47` 为准复核（`not_a_real_type` 越界用例新旧镜像都会 422，无区分度）。
- **直连 scene-service 返回 **401** 或响应 `data:null`**（如 `curl :8081/api/v1/scene/templates`）→ 服务器配了 `GATEWAY_HMAC_SECRET`，直连缺 `X-Internal-MAC`。处置：改用网关 JWT（Step 6）或补 HMAC 头；勿误判为字典问题。
- **宿主机跑 `batch_add_scenes.py` 报 `ModuleNotFoundError: No module named 'pydantic'/'hunter_common'`** → 宿主机无项目依赖。处置：用 Step 7 的容器方式（scene-service 镜像自带依赖）或搭带依赖的 py3.11+ venv。
- **批量提交途中出现 `HTTP 401 code=1001`** → gateway 单会话语义被覆盖（同账号在 Web 端重新登录）。脚本不自动重登、401 不重试。处置：停同账号前端登录 → 重跑原命令（3002 幂等补建）→ 仅发布缺失时 `--publish-drafts` 补发布。
- create/update 返回 **422/2001** 且非预期→ 传入值不命中字典（如 behavior/action/condition_type 拼写）；以 `contracts/scene-taxonomy.yaml` 为准核实取值。
- `gen_scene_taxonomy_ts.py --check` 退出 1 → `sceneTaxonomy.ts` 与字典漂移，重跑 `python3 scripts/gen_scene_taxonomy_ts.py`（或 `cd frontend && npm run codegen:scene-taxonomy`）后重新构建前端。

**分发路径**：`contracts/scene-taxonomy.yaml`（新增）、`common/python/hunter_common/scene_taxonomy.py`（新增）、`services/scene-service/app/{schemas/scene.py,services/templates.py}`、`contracts/openapi/scene-service.yaml`、`contracts/database/enums.md`、`scripts/{batch_add_scenes.py,gen_scene_taxonomy_ts.py}`、`services/scene-service/Dockerfile`、`frontend/src/{constants/sceneTaxonomy.ts,constants/enums.ts,types/scene.ts,package.json}`。scene-service 镜像重建（含 COPY 字典）+ 前端重新构建方生效。
文档同步（运维文档均被 `.gitignore` 排除、只更新不追踪，需单独同步到服务器）：`docs/运维操作手册.md`、`docs/故障排查指南.md`、`docs/deployment.md`、`docs/01-部署概述与环境要求.md`（已补“场景类型受控字典扩展流程 + SCENE_TAXONOMY_FILE/COPY 部署项 + 前端 codegen 步骤”）。

**待办（如实标注）**：① 运行期验证已在本机 `.venv`（py3.12）全绿（见上方“验证”段），但 **CI/服务器（python3.11+）仍应复跑一次**并补 Docker 镜像重建 + 服务启动 + 端到端回归（本机未起 Docker daemon）；② scene-service 镜像需重建才含字典（旧镜像无文件会启动失败）；③ 新 48 类场景若需入库，重跑生成器（仅 git pull 无效）；④ 字典现为 48 叶，若需更密 ODD 可继续编辑 YAML + 重跑 codegen/重部，无需改代码；⑤ 若产品确定需“未知参数键全拒绝”，按上述⚠项改 `validate_enumerable_parameters`；⑥ **服务器实机部署已发现 build 因 `.dockerignore` 排除 `contracts` 而 COPY 失败（详见 V1.22.1），需 `git pull` 后重建才真正上线本版。**

---

## V1.22.1（修复：首版实机部署暴露两处问题——`.dockerignore` 整目录排除 `contracts` 致 scene-service 镜像无法构建；生产宿主机系统 python3.10 无依赖致部署文档 Step 3/4 不可直跑）

Date：2026-10-20
变更类型：修复（构建配置 + 部署文档；不改业务代码/字典/schema）

**现场（服务器实机回录）**：按 V1.22.0 部署步骤在 `root@hunter-core-server:/opt/HunterCore` 实跑：`git pull`（已是最新）、`ls contracts/scene-taxonomy.yaml`（15248B 存在）、`py_compile`+`gen_scene_taxonomy_ts.py --check`均过；但随后：
1. `dcompose build --no-cache scene-service` 在 **`COPY contracts/scene-taxonomy.yaml ./contracts/scene-taxonomy.yaml` 行失败**：`failed to compute cache key: "/contracts/scene-taxonomy.yaml": not found`。之后 `up -d --force-recreate` 仍“Started”——但因为 build 失败，**实际复用了旧镜像**；`docker exec hunter-scene ls /app/contracts/...` 能看到字典仅因 compose `./contracts:/app/contracts:ro` bind mount，而非镜像内置。
2. 宿主机直跑 Step 3/4 均 `ModuleNotFoundError: No module named 'hunter_common'/'pydantic'/'fastapi'/'sqlalchemy'/'confluent_kafka'`（系统 python3.10 未装依赖），`common/python` 另在 `from datetime import UTC` 报错（3.11+ 才有）。**此前本机的 114/453/2490 均是在项目 `.venv`（py3.12）跑出，不代表服务器宿主机可复现。**

**根因**：
- （构建）`.dockerignore` 原行 `contracts` 将整个 `contracts/` 排除出构建上下文，而 scene-service Dockerfile 需 `COPY contracts/scene-taxonomy.yaml`——与文件内已记录的 V1.19.3 “flink-jobs COPY not found”同类。**compose 靠 bind mount 在运行期提供字典，掩盖了镜像本身无法构建的事实**（裸 `docker run`/K8s 无挂载场景会启动失败）。
- （文档）部署步骤错估了“生产宿主机具备带依赖的 python3.11+”，实际生产只预装了容器运行环境。

**修复内容**：
- `.dockerignore`：`contracts` → `contracts/*` + `!contracts/scene-taxonomy.yaml`（排除内容但反选该直子文件；父目录 `contracts/` 未被整体排除，故反选生效）。仅影响 scene-service 一个需 COPY contracts 的镜像（已核全仓无其他 Dockerfile 从 contracts COPY），对其他服务行为不变更（其 contracts 内容仍排除）。
- release.md V1.22.0 “实机执行”段：⚠标注 Step 3–4 需带依赖的 CI/`.venv`、不可直跑生产宿主机；Step 5 新增“必须确认 build 真正成功、否则 `up` 静默复用旧镜像”与“镜像 ID 已变”复核；常见故障判据新增两条（COPY not found 与 build 失败但 up “成功”）。

**向后兼容**：不改代码/字典/schema；仅使镜像构建恢复正常与文档如实。字典为超集、校验仅 create/update 触发，无写入回归。

**风险**：低。`.dockerignore` 变更仅新增一个 15KB 文件进上下文。需警惕：**若仍拿旧 `.dockerignore`（未 `git pull` 本次修复）去 build，依旧会 COPY not found**。

**验证（本环境）**：`.dockerignore` 修改已落地；因本机**未起 Docker daemon**，**未能本地实跑 `docker build` 确认 COPY 成功**（属待办①）。已本地以 py3.12 `.venv` 重跑：`pytest services/scene-service` **114 passed**、`common/python + data-analytics` **453 passed**、`batch_add_scenes.py --dry-run` **2490/2490 Schema=True**、`ruff` 我改文件 All checks passed、`mypy` 服务文件 Success、`vue-tsc --noEmit` clean、`gen --check` 一致。

**实机执行（服务器，生效前必做）**：
```bash
cd /opt/HunterCore && git pull                       # 拉到修正后的 .dockerignore
grep -n 'contracts' .dockerignore                    # 应为 contracts/* 与 !contracts/scene-taxonomy.yaml 两行
dcompose build --no-cache scene-service              # 预期不再报 COPY not found、走到 FINISHED
# 若旧镜像仍在：docker compose images scene-service 对比 Build 时间；重建后 up -d --force-recreate scene-service
# 复核 V1.22.0 真正生效（⚠ 生产配了 GATEWAY_HMAC_SECRET，直连 :8081 会 401/data:null → 走网关 JWT）：
TOKEN=$(curl -s -X POST http://127.0.0.1:8080/api/v1/user/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<pwd>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["access_token"])')
curl -s http://127.0.0.1:8080/api/v1/scene/templates -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print("total=",json.load(sys.stdin)["data"]["total"])'   # 预期 47（旧镜像为 17）
```

**分发路径**：`.dockerignore`。无业务代码/镜像内容变化（除使 COPY 能执行）；scene-service 镜像需重新构建才含字典与 V1.22.0 schema。

**服务器复验（2026-10-20 实机回录，✅ 构建修复已确认生效）**：`git pull` 已更新 `.dockerignore`（+9/−1）；`dcompose build --no-cache scene-service` **FINISHED**，`COPY contracts/scene-taxonomy.yaml` 成功；镜像 `sha256:32416a95…`，`docker inspect hunter-scene` 已用该镜像；`healthz/readyz` 均 200、`.State.Health.Status=healthy`、容器内 `/app/contracts/scene-taxonomy.yaml` 存在。

**服务器复验暴露的新问题（本次已修正文档，待实机复核）**：① 直连 `:8081/api/v1/scene/templates` 返回 `data:null`、`POST :8081/api/v1/scene` 返回 **401**——因生产配了 `GATEWAY_HMAC_SECRET`，直连缺 `X-Internal-MAC`；V1.22.0 Step 6 已改为网关 JWT 流程。② 宿主机直跑 `batch_add_scenes.py` 仍 `ModuleNotFoundError: pydantic`；V1.22.0 Step 7 已改为在 `hunter/scene-service` 镜像容器内执行（镜像自带依赖与 hunter-net DNS）。

**待办（如实标注）**：① ✅ 服务器 build 已复验通过（见上）；② 待用网关 JWT 复核 `templates total=47`（旧镜像 17；越界用例无版本区分度，不作判据），`clear-scenes.sh --dry-run` 已确认命中 1201 条；③ 入库尚未执行（宿主机无依赖，改用容器跑批，见 Step 7）；④ Step 7 入库不可逆，需产品确认；⑤ gateway 单会话语义下跑批勿与 Web 端共用同一账号（见 Step 7 注）。

---

## V1.22.2（修复：场景库列表“类型”列部分显示英文、“创建者”列显示 user_id UUID——前端统一中文展示）

Date：2026-10-20
变更类型：修复（前端展示层；不改后端/契约/DB）

**现场（用户反馈截图）**：场景库列表中“类型”列 `minimal_risk_manoeuvre` / `child_chasing_ball` / `snowy` 等显示英文（`sensor_failure` 等原 18 值仍为中文）；“创建者”列显示 `cd34f8d8-…` 的 user_id UUID。

**根因**：
- 类型：前端标签表取自 codegen 产物 `frontend/src/constants/sceneTaxonomy.ts`，V1.22.0 已扩至 48 类中文；但**服务器 web-portal 仍是旧镜像**（V1.22.0 部署只重建了 scene-service，未重建 web-portal），旧表仅覆盖原 18 值 → 新类型回退显示英文 value。
- 创建者：列表/详情直接渲染 `scene.creator`（user_id），未做来源与账号名映射。

**修复内容**：
- 新增 `frontend/src/utils/scene-display.ts`：`sceneTypeLabel()`（字典外统一“未知类型”，不直出英文）、`isBatchCreatedScene()`（tags 含生成器固定标签 `src:synthetic`）、`sceneCreatorLabel()`（批量→“批量创建”；当前登录账号→real_name 回退 username；其他→“其他账号”，悬停 `title` 显示 creator UUID）。
- `SceneListView.vue`：类型列改 `sceneTypeLabel()` 并加 `show-overflow-tooltip`（“定位丢失地图失配”等长标签）；创建者列改 `sceneCreatorLabel()`（悬停可看完整 UUID）。
- `SceneDetailView.vue`：创建者同规则展示。
- `CoveragePanel.vue`：未覆盖场景类型统一走 `sceneTypeLabel()`。

**向后兼容**：纯展示层，接口字段/DB/契约均不变。判定依据 `src:synthetic` 是运维手册 §3.9 的受控标签前缀（batch 生成器固定写入）。

**风险**：低。手动复制的批量场景会继承 `src:synthetic` 而显示“批量创建”（语义为合成数据，可接受）；其他账号手动创建仅显示“其他账号”（无用户列表接口，无法解析名称，悬停可看 UUID）。

**验证（本机实跑）**：
- `npm run type-check`（vue-tsc --noEmit）→ 无输出、退出 0
- `npm run build` → **2347 modules transformed、23.89s、成功**（产出 `scene-display-*.js`）
- `gen_scene_taxonomy_ts.py --check` → sceneTaxonomy.ts 与受控字典一致

**部署（必须重建 web-portal，否则前端仍为旧 18 值标签表）**：
```bash
dcompose build --no-cache web-portal
dcompose up -d --force-recreate web-portal
sudo docker inspect -f '{{.Image}} {{.Created}}' hunter-web     # 确认镜像 ID 已变
# 浏览器 Ctrl+F5 强刷：类型列应全中文；脚本生成场景创建者显示“批量创建”
```

**分发路径**：`frontend/src/utils/scene-display.ts`（新增）、`frontend/src/views/scene/{SceneListView.vue,SceneDetailView.vue}`、`frontend/src/views/analytics/CoveragePanel.vue`。仅需重建 web-portal，无需重建后端服务。

**待办（如实标注）**：① 尚未做浏览器端到端复核（需部署后 Ctrl+F5 实测）；② 其他账号创建的手动场景暂显示“其他账号”，如需显示完整账号名，需扩展网关用户查询或在场景表冗余 creator_name（另行评估，不在本版范围）。

---

## V1.23.0（增强：场景生成器与场景管理落地——逻辑/具体场景三层抽象、五种生成策略与 G1-G8 校验门、用例/测试集/基线与覆盖分析；scene-service 私有 schema `scene_engine` 7 表 + 28 端点 + 前端两页）

Date：2026-10-23
变更类型：增强（共享库 + DDL + scene-service 引擎内核/服务/端点 + 前端 + 契约 + 部署配置；按《场景生成器与场景管理详细设计文档》落地）

**现场（用户需求）**：平台侧原只有「场景库 CRUD + 48 类模板 + OpenSCENARIO 导出」，缺少设计文档要求的三层场景抽象（功能/逻辑/具体）、可复现的参数化生成、测试用例/测试集/基线管理与覆盖度分析；场景生成依赖离线脚本 `batch_add_scenes.py`，无法在平台内按策略/剖面生成并追溯。

**设计缺口（根因）**：
- 无逻辑场景（IR）→ 具体场景（SceneConfig）的编译与校验链：参数无范围/约束校验、无阿克曼运动学与可避免性判定，生成结果不可复现（无内容指纹）。
- 无批级去重与双轴分级：重复场景会入库，稀有度/临界度/风险等级无法沉淀。
- 无用例/测试集/基线：无法锁定回归口径，也无 `lock_hash` 差异比对。
- 无覆盖分析：48 类场景类型覆盖率、风险/剖面缺口不可见。

**实现内容**：

1. **数据层（服务私有 schema）**
   - 新增 `contracts/database/ddl/07_scene_engine.sql`：`CREATE SCHEMA IF NOT EXISTS scene_engine` + **7 表**（`logical_scenarios` / `concrete_scenarios` / `test_cases` / `scenario_suites` / `suite_items` / `baseline_locks` / `generation_tasks`），全幂等；枚举列一律 **TEXT + CHECK**（12 个受控词表），不用 PG 原生枚举。
   - `contracts/database/ddl/00_schemas.sql` 登记 scene_engine；`scripts/init-db.sh` 新增 **Step 3c** 重放 07（缺文件仅告警跳过；缺表时引擎读写返回 5000/5001）。
   - `hunter_common/database/schema_names.py` 增 `SCENE_ENGINE_SCHEMA` 并纳入 `ALL_SCHEMAS`；`scripts/verify_data_layer.py` 的 `SERVICE_PRIVATE_SCHEMAS` 登记 `scene_engine`（不占共享 13 表 L2 契约）；`common/python/tests/test_database_models.py` 钉住私有 schema 边界与 07 存在性。
   - 注：**逻辑场景目录不在启动时自动 sync**，首次落库由 `POST /api/v1/scene/logical/sync` 显式触发（前端「场景生成器 → 同步目录」按钮）；未同步时列表/详情自动回落内置目录。`init-db.sh` 注释已按实际行为修正。
     > ⚠【V1.23.1 更正】本条原述「upsert 会覆盖 status，重启即把 published 重置为 draft」是把**缺陷当设计**的失实描述：首版 upsert 确实会重置 status。V1.23.1 已将 sync 语义修正为**「新逻辑场景插入为 draft、已存在版本仅更新内容、`published` 保持不变」**（见 V1.23.1 §P1b），不再有误重置风险。

2. **引擎内核** `services/scene-service/app/scene_engine/`
   - `ir.py`：`hunter.ir/1.2` 场景 IR（参数声明/约束/参数覆盖、安全 AST 求值，禁用任意代码执行）。
   - `catalog.py`：内置 **17 条**逻辑场景目录（直行/弯道/跟车/变道/窄路会车/切入/行人横穿/遮挡行人/路口会车/紧急制动/障碍物突现/儿童追球/窄廊脱困/雨天/夜间/低电量停车/窄弯），`CATALOG_VERSION=1.0.0`（与配置项一致性由测试钉住）。
   - `compiler.py`：IR + 参数 → SceneConfig，逐条跑 G1-G7 并产出闸门明细。
   - `validators.py`：八道校验门 **G1 结构/词表 · G2 参数 · G3 地图 · G4 阿克曼运动学 · G5 干跑 · G6 可避免性 · G7 真实性 · G8 批级去重**；双轴分级 `RISK_MATRIX`（稀有度 Z1-Z4 × 临界度 C0-C3 → 风险 L1-L4）；可避免性 trivial_safe/avoidable/marginal/unavoidable **仅标注不剔除**（unavoidable 进待评审标记）。
     > ⚠【V1.23.1 更正】本条原写「G1-G8 全落地」为**失实描述**：首版 G3 仅白名单未做几何围栏、G5 为占位未接 esmini、G7 未接真实车辆参数（且参与者类型误读 `actor_type` 致行人限速从不生效），均为半成品。V1.23.1 已逐项补齐并修复（见 V1.23.1 §P1a/§P2）。
   - `generator.py`：**5 种策略**（coverage_driven/distribution/replay_mutation/search_based/ai_generated）× **3 种剖面配额**（regression 70:25:5:0 / robustness 40:40:20:0 / exploration 10:30:50:10）；G8 去重采用粗分辨率网格分桶 + 桶内标准化距离阈值（默认 0.05σ）叠加 `content_hash`，保证可复现。
   - `oracles.py`：**7 条**版本化判据（ref `oracle.*/v1`：无碰撞/最小距离/速度跟踪/加速度舒适/几何间隙/SOC 余量/定位降级）。
   - `stats.py`：生成统计与覆盖聚合（分层配额、直方图）。

3. **服务与端点（28 个）**
   - `models/scene_engine.py`（7 表 ORM + 12 个 StrEnum 受控词表）、`schemas/engine.py`（请求/响应模型）、`repositories/engine.py`（5 个仓储）。
   - `services/engine.py`：目录同步/校验/干跑/发布、异步生成（BackgroundTasks + `asyncio.to_thread`，CPU 采样不阻塞事件循环，进度节流回写）、具体场景列表/详情/按需渲染、批量复检、用例创建/查询、测试集创建/加条目/锁定/diff/基线、覆盖总览与盲区。
   - `routers/engine.py`：6 组共 **28 端点**（逻辑 7 / 生成 4 / 具体 4 / 判据与用例 4 / 测试集与基线 7 / 覆盖 2）；`main.py` 在 `scenes.router` **之前**注册 `engine.router`（否则 `/oracles`、`/testcases`、`/suites`、`/generate`、`/concrete` 会被 `/{scene_id}` 吞掉）。
   - RBAC：读 `scene:read`；写（同步/发布/复检/用例/测试集/锁定/取消）`scene:create|update|delete`；生成 `scene:execute`（网关注入 X-Roles）。
   - 生成数量上限由 `SCENE_GENERATION_MAX_COUNT=5000` 约束（与 DDL `count CHECK 1..5000` 同源）；G3 地图白名单 `SCENE_ALLOWED_MAPS`（空=不限制）；`SCENE_CATALOG_VERSION` 与 `catalog.CATALOG_VERSION` 由测试钉住一致。
   - 未执行 07 DDL 时：读路径回落内置目录（列表/详情可用），写路径返回 5000/5001（不静默降级）。仅 `pending` 任务可取消（`running` 返回 3003）；测试集锁定后条目冻结（3003），基线 = `lock_hash` + 指标快照。

4. **前端**
   - 新增 `SceneGeneratorView.vue`（**逻辑场景 / 生成任务 / 具体场景 / 覆盖分析** 四页签：目录同步、校验/干跑/发布、按策略+剖面提交生成并轮询进度、取消、具体场景筛选与按需渲染、覆盖总览与盲区）与 `SceneTestCaseView.vue`（用例创建、测试集创建/加条目/锁定/diff/基线）。
   - 新增 `api/engine.ts`、`types/engine.ts`、`constants/engine.ts`；路由 `scenes/generator`、`scenes/testcases` 与侧边菜单两项（`scene:read` 可见）；`POLL_INTERVALS.sceneTask=2000`（`VITE_POLL_SCENE_TASK_MS` 可调）。

5. **契约与文档**
   - 新增 `contracts/openapi/scene-engine.yaml`：**25 paths / 28 operations / 69 schemas / 8 responses**，`x-hunter-service.schema=scene_engine`、`private_schema=true`、`kafka.produces=[]`。
   - `contracts/openapi/README.md` 增「场景引擎契约要点」；`contracts/database/{README,enums,er,orm-mapping}.md` 覆盖 7 表、12 词表与私有 schema 仓储归属（不入共享 `ALL_MODELS`/`REPOSITORY_BY_MODEL`）。
   - `services/scene-service/README.md` 增「场景引擎（V1.23.0）」章节与分层说明。

6. **部署配置**
   - `infra/deploy/docker-compose.yml` 与 `infra/k8s/services/scene-service.yaml` 显式注入 `SCENE_GENERATION_MAX_COUNT` / `SCENE_ALLOWED_MAPS` / `SCENE_CATALOG_VERSION`（K8s 注释标明 G3 收紧方式）。

**向后兼容**：全部为新增（新 schema/新表/新端点/新页面）；既有 `/api/v1/scene/**` 场景库、模板、导出、仿真端点不变；`scene_engine` 为服务私有 schema，不进入共享 ORM 元数据/仓储登记，不影响其他服务；未执行 07 DDL 时读路径回落内置目录，存量功能不受影响。

**风险**：中。
- 引擎端点复用 `/api/v1/scene` 前缀，**路由注册顺序是硬约束**（已由契约测试钉住端点集合与 operationId 映射）。
- 生成任务为进程内 BackgroundTasks：服务重启会丢失 running 任务（任务记录保留、状态不回写 succeeded）；生产按单副本运行或后续迁移外部队列（不在本版）。
- 07 DDL 未在真实 PostgreSQL 实跑（见待办①），当前仅由 DDL 契约测试/文本校验覆盖。
- `scene_engine` 为新增 schema，`init-db.sh` 需重跑；漏跑时前端列表回落目录、写操作报 5000/5001（可据此判据）。

**验证（本机 `.venv` Python 3.12 实跑全绿）**：
- `pytest services/scene-service -q` → **222 passed**（新增 108：核心 52 + DDL 10 + 端点 28 + 契约 18）
- `pytest common/python -q` → **333 passed**（schema_names / 私有 schema 边界无回归）
- `ruff check services/scene-service scripts` → **All checks passed**
- `python scripts/verify_data_layer.py` → **34 项全通过**（含 scene_engine 私有 schema 豁免）
- `python scripts/verify_infra.py` → **全部校验通过（91 个 K8s 文档）**
- `frontend: npm run type-check`（vue-tsc --noEmit）→ **无错误**

**实机执行（部署时必做）**：
```bash
cd /opt/HunterCore && git pull
sudo bash scripts/init-db.sh          # Step 3c 重放 07_scene_engine.sql（幂等）
dcompose build --no-cache scene-service web-portal
dcompose up -d --force-recreate scene-service web-portal
sudo docker exec hunter-postgres psql -U hunter -d hunter_core -c "\\dt scene_engine.*"   # 预期 7 表
# 经网关 JWT 复核（生产直连 :8081 会 401）：
TOKEN=$(curl -s -X POST http://127.0.0.1:8080/api/v1/user/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<pwd>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["access_token"])')
curl -s http://127.0.0.1:8080/api/v1/scene/logical -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print("total=",json.load(sys.stdin)["data"]["total"])'   # 未 sync 时回落 17
curl -s -X POST http://127.0.0.1:8080/api/v1/scene/logical/sync -H "Authorization: Bearer $TOKEN"   # 首次落库
curl -s http://127.0.0.1:8080/api/v1/scene/oracles -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print("oracles=",len(json.load(sys.stdin)["data"]["items"]))'  # 预期 7
```
> ⚠ `logical/sync` 为首版 upsert 语义：当时**已发布逻辑场景重新 sync 会被重置为 draft**，生产仅在首次或目录升级时执行（详见运维手册 §3.11）。**【V1.23.1 更正】此风险已消除**——sync 现保留 `published`、仅新场景插 `draft`（见 V1.23.1）。

**分发路径**：`contracts/database/ddl/{00_schemas,07_scene_engine}.sql`、`common/python/hunter_common/database/schema_names.py`、`scripts/{init-db.sh,verify_data_layer.py}`、`services/scene-service/app/{scene_engine/*,models/scene_engine.py,schemas/engine.py,repositories/engine.py,services/engine.py,routers/engine.py,config.py,core/dependencies.py,main.py}`、`frontend/src/{api/engine.ts,types/engine.ts,constants/engine.ts,constants/index.ts,types/index.ts,router/index.ts,layouts/MainLayout.vue,views/scene/SceneGeneratorView.vue,views/scene/SceneTestCaseView.vue}`、`contracts/openapi/scene-engine.yaml`、`contracts/database/*.md`、`contracts/openapi/README.md`、`services/scene-service/README.md`、`infra/deploy/docker-compose.yml`、`infra/k8s/services/scene-service.yaml`。scene-service 与 web-portal 镜像需重建方生效。
文档同步（运维文档均被 `.gitignore` 排除、只更新不追踪，需单独同步到服务器）：`docs/运维操作手册.md`（新增场景引擎节）、`docs/故障排查指南.md`（新增引擎故障场景）、`docs/deployment.md`（部署顺序/验证清单补 scene_engine）、`docs/01-部署概述与环境要求.md`（§1.1 职责与环境项）。

**待办（如实标注）**：① **07 DDL 与端到端尚未在真实 PostgreSQL/Docker 环境实跑**（本机仅 SQLite DDL 契约测试与文本校验）；② 生成任务为进程内后台任务，服务重启后 running 任务不续跑（需外部队列，另行评估）；③ 逻辑场景目录 17 条为设计文档典型 ODD，不含实车回放自动提取链路（仍走 analytics_result 消费，与引擎目录分离）；④ `ai_generated` 策略当前为受护栏的参数导入，不接外部模型；⑤ `SCENE_ALLOWED_MAPS` 默认为空（不限制），上线前需按开放区域收紧。

## V1.23.1（修复：V1.23.0 代码审查 P0/P1/P2 全量落地——风险矩阵与车辆参数真实性、并发/事务/启动对账正确性、G3/G5/G7 由半成品补全、异常脱敏与性能优化；`scene_engine` 补第 8 表 `audit_logs`）

Date：2026-10-23
变更类型：修复（对照《场景生成器与场景管理详细设计文档》与三维审查结论——功能符合度、边界/异常处理、并发安全与性能——逐项修复；含 2 个自查发现并确诊的真实缺陷）

**背景**：V1.23.0 代码审查暴露若干「功能标称已落地但实为半成品/占位」与「把缺陷当设计」的问题（详见本版对 V1.23.0 三处失实描述的更正）。本版按 P0→P1→P2 优先级修复并补齐回归测试。

**P0（正确性根因）**
1. **风险矩阵可复现**：临界度 CI 按 `ci = a_req / a_max`、`a_req = v² / (2(d - d_m))` 计算并落库（保留 6 位）；`ci = inf` 的饱和情形以 `999.0` 落库并在 `generator.ci_saturated` 显式标注，避免 JSONB 存 `Infinity` 破坏序列化。
2. **车辆参数单一事实来源**：新增 `scene_engine/vehicle_params.py` provider（读取车端 `vehicle_params` 导出文件，配置文件 + `source_hash` + `lru_cache`；转角半径取声明值与几何值较小者；文件缺失/非法即**启动失败**，不静默回落）。G7 真实性、阿克曼运动学、xosc 生成统一取该 provider，杜绝硬编码。
3. **content_hash 幂等降级**：`logical_scenarios` 的哈希唯一约束降级为普通索引（同内容不同版本允许共存），`concrete_scenarios` 仍保留 `uq_concrete_scenarios_hash`（真正去重边界）。

**P1a（并发 / 事务 / 异常）**
4. **覆盖统计过滤**：覆盖率/盲区聚合仅计入通过校验的具体场景，剔除 generated 但未复检或失败项。
5. **锁与加条目单事务**：`add_items_if_draft`（仅 draft 可加、单步原子）、`lock_with_baseline`（行锁 + 基线写入同一事务）改为带 `SELECT ... FOR UPDATE` 的单事务，消除「加条目与锁定竞态」「锁定成功但基线丢失」。
6. **启动对账**：`main.py` 启动调用 `reconcile_generation_tasks` → `reconcile_stale(stale_seconds)`，仅将心跳超时（`SCENE_TASK_STALE_SECONDS`，默认 300s）的 pending/running 置 failed 并写「任务失联」说明，多副本不误伤在跑任务。
7. **IntegrityError 精确区分**：`_is_unique_violation` 仅认 SQLSTATE `23505`（唯一冲突→3002），其余约束违背（如 `23514` CHECK）原样上抛，避免把 CHECK 失败误判为重名冲突。
8. **G3 地图校验补全**：白名单之外增加几何围栏（`SCENE_MAP_BOUNDS="xmin,ymin,xmax,ymax"`）与生成点有限性/NaN 拒绝。
9. **G5 接 esmini**：新增 `scene_engine/esmini.py` 适配器（`shlex` 前缀拼装、临时目录写 xosc/xodr、超时判失败、输出尾截 600、未部署标记 `skipped` 不计失败）；`SCENE_ESMINI_BIN` 为空即 `skipped`。
10. **G7 真实性 + 参与者类型缺陷**：G7 用 `vehicle_params.max_speed` 约束 ego，行人按 `PEDESTRIAN_SPEED_LIMIT=3.0` 限速。**自查发现真实缺陷**：SceneConfig 参与者类型字段实为 `type`，而 G7 与 esmini 均误读 `actor_type` → 行人限速从不生效、xosc 从不生成 Pedestrian。已改为 `actor.get("type") or actor.get("actor_type")` 兼容读取。

**P1b（语义 / 权限 / 审计）**
11. **sync 状态语义**：`logical/sync` 改为「新逻辑场景插入为 `draft`、已存在版本仅更新内容、`published` 保持不变」，纠正 V1.23.0 把「重 sync 重置 published」当设计的失实描述（见本版对 V1.23.0 的更正条目）。
12. **发布 / 锁定 RBAC**：新增 `require_review_permission`（`SCENE_REVIEW_ROLES=admin,reviewer`）用于 publish 与 suite lock，非评审角色返回 403（code 1002）。
13. **追加式审计**：`scene_engine` 补第 8 表 `audit_logs`，发布/锁定/目录同步写审计，`entry_hash = sha256(prev_hash:canonical)` 哈希链防篡改。

**P2（可复现 / 安全 / 性能）**
14. **确定性命名**：具体场景命名与 `content_hash`、`vehicle_params_hash` 绑定，同参数同种子产出同 ID 序列。
15. **异常脱敏**：`_safe_error_message` 业务错误保留原文（含 `[2001]` 前缀），未知异常仅回 `类型名: 生成失败（详见服务日志）`，不外泄 SQL/堆栈。
16. **overrides IR 感知全量校验**：`ensure_overrides_shape(overrides, ir)` 结构 + IR 双层校验（`target_zone`/`grid_bins 1..64`/`dedup_distance 0..10`/`mutation_scale (0,1]`；`fixed/base/ranges/candidates` 未声明参数、越界硬界、分类取值、`low<high`、fixed 全量代入违反线性约束 → 早失败并映射 2001）。
17. **批量插入 / 大列延迟 / 去 N+1 / 限并发复检**：`create_many` 走分片多值 `INSERT ... ON CONFLICT (uq_concrete_scenarios_hash) DO NOTHING RETURNING`（列集不一致回退逐行）；`CONCRETE_DEFERRED` 延迟加载 `param_values/param_vec/validity/generator/scene_config` 大列；新增 `get_many` 去批量复检 N+1；`batch_validate` 用 `asyncio.to_thread(compile_scene)` + `Semaphore(SCENE_VALIDATE_CONCURRENCY)`（默认 4）把可能起 esmini 子进程的阻塞编译移出事件循环。
18. **事件循环取消缺陷修复（自查发现）**：`execute_generation_task` 开跑前的取消回查曾在事件循环线程内调用 `run_coroutine_threadsafe(...).result(timeout=2)` → 自阻塞 2s 且静默吞掉取消；改为在循环内直接 `await self._tasks.is_cancel_requested(task_id)`，并在产物入库前二次检查，避免「已取消但仍新增场景」。

**向后兼容**：修复为主，无破坏性变更；`scene_engine` 由 7 表增至 8 表（新增 `audit_logs`），`init-db.sh` Step 3c 重放 07 即可建表；新增配置项均有安全默认值（未配置时 G5 `skipped`、G3 不限制围栏）。❗ **本条原写的“vehicle_params 回落包内样例”经 V1.23.1 实机验证为假：镜像未构建包内 `config/vehicle_params.json`，未挂载时 scene-service 启动即崩溃——已由 V1.23.2 修复**。`CATALOG_VERSION` 递增为 `1.0.1`。

**风险**：低-中。审计哈希链与启动对账改变写路径行为，已由回归覆盖；esmini/批量插入需真实 PostgreSQL + esmini 环境二次验证（本机走内存替身与 SQLite DDL 契约测试）。

**验证（本机 `.venv` Python 3.12 实跑全绿）**：
- `python scripts/run_unit_tests.py` → **10 个目标全部通过**
- `pytest services/scene-service -q` → **294 passed**（较 V1.23.0 的 222 新增 72：`test_scene_engine_hardening.py` 专项加固回归 70 + 连带补齐 2）
- `pytest common/python -q` → **333 passed**（无回归）
- `ruff check services/scene-service` → **All checks passed**
- `python scripts/verify_data_layer.py` → **34 项全通过**；`python scripts/verify_infra.py` → **全部校验通过（91 个 K8s 文档）**
- `frontend: npm run type-check`（vue-tsc --noEmit）→ **无错误**

**实机执行（部署时必做，逐项核对 V1.23.1 行为）**：
```bash
# 0) 取网关 JWT（生产直连 :8081 会 401，必须走网关）
cd /opt/HunterCore && git pull
# ⚠ 先定义运维手册 §2 的 compose 封装（否则报“找不到命令 dcompose”）：必须带 -f … --project-directory .，
#    在 APP_DIR 根执行（否则 .env 找不到、镜像 tag 非法）
dcompose() { docker compose -f infra/deploy/docker-compose.yml --project-directory . "$@"; }
TOKEN=$(curl -s -X POST http://127.0.0.1:8080/api/v1/user/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<pwd>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["access_token"])')

# 1) 落 DDL（幂等）+ 复核 8 表（V1.23.1 新增 audit_logs）
sudo bash scripts/init-db.sh          # Step 3c 重放 07_scene_engine.sql
docker exec -i hunter-postgres psql -U hunter -d hunter_core -c "\\dt scene_engine.*"   # 预期 8 表

# 2) 重建 scene-service（含 engine 加固与启动对账）
dcompose build --no-cache scene-service web-portal
dcompose up -d --force-recreate scene-service web-portal
curl -s http://127.0.0.1:8081/healthz   # 预期 {code:0}（vehicle_params 缺失会反复重启、探活不可达）

# 3) 车辆参数单一事实源（启动硬依赖）——❗ V1.23.1 实机首发：本步会暴露“镜像未内置 config/vehicle_params.json”导致启动崩溃，需先上 V1.23.2（Dockerfile 内置 config）再重跑
#    scene-service 为 python:3.12-slim（非 distroless，有 shell）；启动失败时容器会反复重启、/healthz 不可达
docker logs --tail=50 hunter-scene | grep -iE 'VehicleParamsError|vehicle_params|startup failed'   # 命中即命中此缺陷
docker exec hunter-scene ls -l /app/config/vehicle_params.json 2>/dev/null || echo "/app/config/vehicle_params.json 不存在（需 V1.23.2）"
# 修复前应急：临时挂一个合法文件并指路径（或上 V1.23.2 重建）：
#   docker cp services/scene-service/config/vehicle_params.json hunter-scene:/tmp/vp.json 不持久；正规做法是在 compose 设 SCENE_VEHICLE_PARAMS_PATH 并 bind mount 文件

# 4) G5 esmini 可选依赖：未配二进制时静态校验报告里 G5 标 skipped（不计失败）
#    （SCENE_ESMINI_BIN 保持空）
curl -s -X POST "http://127.0.0.1:8080/api/v1/scene/logical/<已发布逻辑的 logical_id>/validate" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];print("passed=",d["passed"],"G5_skipped=",next((g["skipped"] for g in d["gates"] if g["gate"]=="G5"),None))'
# 预期 G5 skipped=True 且整体 passed 不因 G5 置 false（未部署 esmini 不阻断）

# 5) sync 语义更正：已发布逻辑场景重跑 sync 后仍为 published（不再被重置为 draft）
curl -s -X POST http://127.0.0.1:8080/api/v1/scene/logical/sync -H "Authorization: Bearer $TOKEN"
docker exec -i hunter-postgres psql -U hunter -d hunter_core -c \
  "SELECT status, count(*) FROM scene_engine.logical_scenarios GROUP BY status;"   # published 数不因 sync 归零

# 6) 评审 RBAC：非 reviewer 角色发布应 403（code=1002）；publish 路径为 /logical/{id}/publish，version 走 query
curl -s -o /dev/null -w "%{http_code}\n" -X POST \
  "http://127.0.0.1:8080/api/v1/scene/logical/<logical_id>/publish?version=<version>" -H "Authorization: Bearer $NON_REVIEWER_TOKEN"
# 预期 403；用 admin/reviewer 的 token 再发布应 200

# 7) 运行中取消：对 running 任务取消返回 200、置 cancel_requested，而非 3003
curl -s -X POST http://127.0.0.1:8080/api/v1/scene/generate/<task_id>/cancel -H "Authorization: Bearer $TOKEN"
docker exec -i hunter-postgres psql -U hunter -d hunter_core -c \
  "SELECT status, cancel_requested FROM scene_engine.generation_tasks WHERE task_id='<task_id>';"   # canceled 或 running+cancel_requested=true

# 8) 启动对账：重启 scene-service，把一条 update_time 明显超前的 running 任务置 failed
docker exec -i hunter-postgres psql -U hunter -d hunter_core -c \
  "UPDATE scene_engine.generation_tasks SET status='running', update_time=now()-interval '10 minutes' WHERE task_id='<故意遗留的 task_id>';"
dcompose up -d --force-recreate scene-service   # 触发 reconcile_stale（默认 SCENE_TASK_STALE_SECONDS=300）
until curl -sf http://127.0.0.1:8081/healthz >/dev/null; do sleep 1; done; sleep 2
docker exec -i hunter-postgres psql -U hunter -d hunter_core -c \
  "SELECT status, error_message FROM scene_engine.generation_tasks WHERE task_id='<task_id>';"   # failed + “任务失联”
```
> ⚠ 上述为**部署时逐项核对清单**（本机以内存替身 + SQLite DDL 契约测试覆盖，真实 PostgreSQL/esmini 端到端尚待实机回填，见待办④）；任一判据与预期不符，按《故障排查指南》§18 的 ⑩-⑬ 定位。

**分发路径**：`services/scene-service/app/{scene_engine/{vehicle_params.py,esmini.py,generator.py,validators.py,compiler.py,catalog.py,ir.py},models/scene_engine.py,repositories/engine.py,schemas/engine.py,services/engine.py,routers/engine.py,config.py,core/dependencies.py,main.py}`、`contracts/database/ddl/07_scene_engine.sql`（+`audit_logs`）、`services/scene-service/app/tests/{fakes_engine.py,conftest.py,test_scene_engine_core.py,test_scene_engine_ddl.py,test_scene_engine_endpoints.py,test_scene_engine_hardening.py}`、`infra/{deploy/docker-compose.yml,k8s/services/scene-service.yaml}`（新增 `SCENE_*` 环境变量）。scene-service 镜像需重建方生效。
文档同步（均被 `.gitignore` 排除、只更新不追踪，需单独同步到服务器）：`docs/故障排查指南.md`（新增 `SCENE_VEHICLE_PARAMS_PATH`/`SCENE_ESMINI_BIN`/`SCENE_MAP_BOUNDS`/`SCENE_TASK_OWNER`/`SCENE_VALIDATE_CONCURRENCY` 排查项、任务失联对账、审计表）、`docs/运维操作手册.md`（发布/锁定需 reviewer、取消与启动对账操作、sync 语义更正）、`docs/deployment.md` 与 `docs/01-部署概述与环境要求.md`（多副本 + 启动对账、挂载 vehicle_params.json、esmini 可选依赖）。

**待办（如实标注，本版本未实现）**：① **Idempotency-Key 请求头**（重复提交同一生成请求去重）尚未实现；② **ETag / `If-Match` 乐观并发**（逻辑场景/测试集编辑冲突检测）尚未实现；③ **游标分页**（大列表深翻页，当前仍为 offset/limit）尚未实现；④ 07 DDL（含 `audit_logs`）与 esmini G5 端到端尚未在真实 PostgreSQL/esmini 环境实跑（本机走内存替身与 SQLite 契约测试）。

## V1.23.2（修复：V1.23.1 实机部署首发——scene-service 镜像未内置包内默认 `config/vehicle_params.json`，未挂载车辆参数时启动即崩溃）

Date：2026-10-23
变更类型：修复（实机验证发现并确诊、阻断级；一个构建环境缺陷）

**现场（实机）**：按 V1.23.1 “实机执行”步骤 2–3 操作，`dcompose build/up scene-service` 后容器反复重启、`curl :8081/healthz` 无输出，`docker logs hunter-scene` 报：
```
app.scene_engine.vehicle_params.VehicleParamsError: 车辆参数文件不存在: /app/config/vehicle_params.json
ERROR:    Application startup failed. Exiting.
```
连带使引擎写接口（如 `POST /logical/sync`）返回 5001 服务不可用（因服务未起），非服务代码问题。

**根因**：`DEFAULT_VEHICLE_PARAMS_PATH = Path(__file__).resolve().parents[2] / "config" / "vehicle_params.json"`。源码布局下 `__file__=services/scene-service/app/scene_engine/vehicle_params.py`，parents[2] = `services/scene-service` → **文件存在，单测全绿**；但容器内 `__file__=/app/app/scene_engine/vehicle_params.py`（Dockerfile `WORKDIR /app` + `COPY … ./app`），parents[2] = `/app` → `/app/config/vehicle_params.json`。runner 阶段只 `COPY --from=builder /build/service/app ./app` 与 `COPY contracts/scene-taxonomy.yaml`，**从未把服务根级 `config/` 带入镜像** → 启动硬依赖的回落文件缺位 → 直接 crash。属“测试全绿但镜像缺文件”的经典构建漏网（与 V1.22.1 `.dockerignore` 排除 contracts 致构建失败同类）。

**修复内容**：
1. `services/scene-service/Dockerfile` runner 阶段新增 `COPY --from=builder /build/service/config ./config`（builder 已 `COPY services/scene-service ./service`，`/build/service/config/vehicle_params.json` 存在），使容器内 `/app/config/vehicle_params.json` 与 `DEFAULT_VEHICLE_PARAMS_PATH` 解析一致；`.dockerignore` 未排除 `config/`，无需改动。
2. 新增 2 条回归钉住该缺陷（`test_scene_engine_hardening.py`）：`test_packaged_default_vehicle_params_is_loadable`（包内默认可加载）与 `test_scene_service_dockerfile_ships_vehicle_params_config`（Dockerfile 必须含将 `config`  COPY 到 `./config` 的构建行）——防止此类“镜像不内置启动硬依赖文件”回归。

**向后兼容**：仅新增一行 Dockerfile COPY（无代码/接口变更）；`SCENE_VEHICLE_PARAMS_PATH` 语义不变（非空时仍优先用它、生产接车端真实参数仍靠挂载/ConfigMap 覆盖内置默认）。修复后“不配任何环境变量”也能以包内 HUNTER_SE 默认启动。

**风险**：低。仅影响镜像内容；需**重建 scene-service** 方生效（`up -d` 不重建不拉新内容）。

**验证**：
- `pytest services/scene-service -q` → **296 passed**（+2 新增回归）；`ruff check services/scene-service` → **All checks passed**
- 实机待回填：重建后 `docker exec hunter-scene ls -l /app/config/vehicle_params.json` 应存在；`curl :8081/healthz` 应 `{code:0}`；不再出现 VehicleParamsError。

**实机执行（本修复重新部署必做）**：
```bash
cd /opt/HunterCore && git pull
docker compose -f infra/deploy/docker-compose.yml --project-directory . build --no-cache scene-service
docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate scene-service
until curl -sf http://127.0.0.1:8081/healthz >/dev/null; do sleep 1; done; sleep 2
curl -s http://127.0.0.1:8081/healthz                                # 预期 {"code":0,...}
docker exec hunter-scene ls -l /app/config/vehicle_params.json        # 预期文件存在
docker logs --tail=30 hunter-scene | grep -iE 'VehicleParamsError|startup failed' || echo "无启动异常✓"
# 接车端真实参数时：挂 bind mount 导文件 + 设 SCENE_VEHICLE_PARAMS_PATH 后重建，覆盖内置默认
```

**分发路径**：`services/scene-service/Dockerfile`（新增 `COPY --from=builder /build/service/config ./config`）、`services/scene-service/app/tests/test_scene_engine_hardening.py`（+2 回归）。scene-service 镜像需重建方生效。

**待办（如实标注）**：本修复的镜像重建与 `/healthz` 恢复尚需在服务器实机回填（本机以源码布局回归 + Dockerfile 文本断言覆盖，无法在本机跑 docker build）。

## V1.23.3（新增：CARLA 仿真系统与数据采集/分析系统三链路打通——控制面、VIL 数据面、仿真回传面；新增 `vil-forwarder` 微服务）

Date：2026-10-24
变更类型：新增（契约 + 服务 + 部署编排）+ 加固（失败回滚 / 跨消费组竞态 / fail-fast）

**背景**：按《CARLA 仿真系统接入数据采集与分析系统方案》《仿真端接入平台系统方案》与 `docs/carla-integration.md` 落地三件事：① **控制面** `scene-service → carla-manager`（实例创建/场景下发/暂停恢复停止/进度结果代理）；② **VIL 数据面** `telemetry_clean → vil-forwarder → carla.{sim_id}.vil_state`（实车在环，10Hz latest-wins）；③ **仿真回传面** `carla.{sim_id}.sensor_data → data-collector / data-analytics`（状态/事件/指标/产物落 Redis、时序库与 `scene_test` 报告）。CARLA 侧（`carla-manager`）为外部系统，本轮只交付平台侧契约与实现。

### 1. 契约层（P0，全部登记进既有校验入口）

1. **Kafka Schema**：新增 `contracts/kafka/schemas/carla_vil_state.schema.json`（`sim_id/vehicle_id/timestamp/seq/pose/twist/chassis/calibration`，`pose.frame` 固定 `odom`）与 `carla_sensor_data.schema.json`（`sim_status/sim_telemetry/sim_event/sim_metrics/sensor_data/artifact` 六类载荷，`allOf` 条件约束逐类必填字段）。
2. **动态 Topic 模板**：`contracts/kafka/topics.yaml#carla_topics` 登记 `carla.{sim_id}.vil_state`（1 分区 / 1 小时保留 / acks=1）与 `carla.{sim_id}.sensor_data`（3 分区 / 7 天保留 / acks=all / 派生 `.dlq`），**明确不进静态建 Topic 脚本**；`key` 统一 `sim_id`（与会话内有序，且与 `vehicle_id` 命名空间隔离），由 `scene-service` 在实例创建前用 Kafka Admin 显式创建、终态后延迟删除。
3. **消费组**：`consumer-groups.yaml` 新增 4 组——`vil-forwarder`（`telemetry_clean` → `carla.{sim_id}.vil_state`，幂等 `(vehicle_id, seq)`）、`data-collector-carla`（正则订阅 `carla.*.sensor_data`，写 `vehicle_telemetry`/`events`）、`data-analytics-carla`（终态 60s 内出 `scene_test` 报告）、`carla-vil`（CARLA 侧消费 VIL 状态，外部实现）。
4. **Redis 键**：`redis-keys.yaml` + `common/python/hunter_common/redis_keys.py` 新增 7 个键——`vil:session:{vehicle_id}`（JSON，7200s，`SET NX` 保证单车唯一 VIL 会话）、`vil:active`（Set）、`sim:instance:{sim_instance_id}`（实例反查，86400s）、`carla:sim:{sim_id}:status|metrics|events|artifacts`（终态后保留 1 天）。
5. **共享库**：新增 `hunter_common.kafka.admin.KafkaAdmin`（`ensure_carla_session` / `delete_carla_session`，同步 Admin 调用由服务放线程池）；`build_record`/`publish_payload` 的 key 契约扩展到 `sim_id`（发送 `carla.*` 时自动按载荷 `sim_id` 校验/补齐 key，禁止错写 vehicle_id）。
6. **校验入口**：`scripts/verify_data_layer.py` 增加 CARLA Topic 模板、Schema、Redis 键、消费组比对（现 **34 项**）；`scripts/generate_contracts_configmap.py` 重新生成 `infra/k8s/base/03-configmap-contracts.yaml`，K8s 挂载到 `/app/contracts/kafka`（避免遮蔽 scene-service 镜像内置的 `scene-taxonomy.yaml`）。

### 2. scene-service（P1，控制面 + VIL 会话 + 动态 Topic）

1. `POST /api/v1/scene/{scene_id}/run` 请求扩展 `sim_id`（可空，空则平台生成）/`mode`（`sil|vil|replay`）/`vehicle_id`/`vil`（标定）/`replay_config`/`kafka`/`quality`；下发 CARLA 的 body 只带 Topic 名，**Kafka 凭据由 CARLA 运行环境注入，禁止经平台传递**。
2. **创建顺序固定**：校验 → 生成 `sim_id` → **显式创建动态 Topic（失败 5001，不建实例）** → VIL 原子占位 Redis 会话 → 创建 CARLA 实例 → 下发场景 → 返回实例信息。任一步失败按“停止已创建实例 → 释放会话 → 删除已建 Topic”回滚（best effort，避免孤儿实例/脏 Topic）。
3. **`sim_id` fail-fast**：CARLA 返回的 `sim_id`（或 `simulation_id`）与平台下发不一致时直接 5001（`carla_sim_id_mismatch`）——`sim_id` 同时是动态 Topic、消息 key 与 VIL 会话主键，回显不一致会导致回传写到另一套 Topic、平台永远收不到数据。
4. **状态映射**（`app/services/sim_status.py`）：`CREATED/LOADING/READY → pending`、`RUNNING/PAUSED → running`（`PAUSED` 保留 `message=paused`）、`COMPLETED/SUCCEEDED → succeeded`、`FAILED → failed`、`CANCELED/CANCELLED → canceled`；未知状态映射 `failed`（不再静默回落 running）。
5. **控制端点**：新增 `POST …/simulations/{sim_instance_id}/pause|resume|cancel`（暂停/恢复代理 CARLA，取消=停止实例），进度/结果查询继续无状态代理；`contracts/openapi/scene-service.yaml` 已同步请求字段、三个端点与错误码。
6. **实例会话登记**：`sim:instance:{sim_instance_id}` 记录 `sim_id/scene_id/vehicle_id/mode/topics`，供进度/结果/取消在只有 CARLA 实例 ID 时反查平台 `sim_id`；VIL 终态/取消删除 `vil:session` 并 `SREM vil:active`。
7. **动态 Topic 清理**：终态/取消后按 `SIM_TOPIC_DELETE_DELAY_SECONDS`（默认 600s）延迟删除；服务优雅停机取消未执行的清理任务（已进入删除的等待完成）。

### 3. vil-forwarder（P2，新增微服务，8090）

1. 消费 `telemetry_clean`（组 `vil-forwarder`，`auto.offset.reset=latest`，**禁止回放历史遥测把过期位姿推进 CARLA**），按 `vil:session:{vehicle_id}` + `vil:active` 做车辆→会话映射；会话短缓存 `VIL_SESSION_CACHE_TTL_MS=1000`。
2. **10Hz latest-wins**：单车辆时间窗内只保留最新一帧，字段映射到 `carla_vil_state`（`localization.heading → pose.yaw`、`linear/angular_velocity → twist`、底盘 12 项白名单），逐条 Schema 校验后按 `key=sim_id` 发布到 `carla.{sim_id}.vil_state`；幂等键 `(vehicle_id, seq)`。
3. 关闭本地磁盘缓冲（`KAFKA_LOCAL_BUFFER_ENABLED=false`，配置默认值已改为 `False`）：只读根文件系统无写权限，且链路恢复后不应重放过期位姿。
4. 交付 Dockerfile、`pyproject.toml`、健康探针（`/healthz`、`/readyz`）、单元测试（4 条）；K8s 清单 `infra/k8s/services/vil-forwarder.yaml`（2 副本、HPA、PDB、NetworkPolicy、`hunter-contracts` 挂载、Prometheus 抓取）。

### 4. data-collector（P3，回传面落库与缓存）

1. 新增 `CarlaSensorConsumer`（组 `data-collector-carla`，正则订阅 `carla.*.sensor_data`，`SCHEMA_AUTO` 按 Topic 解析 Schema）：
   - `sim_status/sim_metrics/sim_event/artifact` → Redis `carla:sim:{sim_id}:*`（TTL 86400s，事件列表上限 1000）；
   - `sim_telemetry` → `data_collector.vehicle_telemetry`（`vehicle_id=SIM-{sim_id}` 隔离实车轨迹，缺失段保留 NULL）；
   - 可语义映射事件 → `data_collector.events`（不扩车端 19 类 `event.schema.json`）；
   - `sensor_data` → 复用既有 `sensor_file` 通知 Topic，**不跨 schema 写 `data_analytics`**；幂等键 `(sim_id, seq)`。
2. 配置项：`CARLA_SENSOR_CONSUMER_ENABLED`（默认 true）、`CARLA_SIM_CACHE_TTL_SECONDS`、`CARLA_SIM_EVENTS_MAX`，compose 默认生效、K8s ConfigMap 显式登记。

### 5. data-analytics（P3，自动生成 `scene_test` 报告）

1. 新增 `CarlaSimulationConsumer`（组 `data-analytics-carla`，正则订阅 `carla.*.sensor_data`）：消费 `sim_metrics`/`sim_status`，**终态后 60s 内**生成 `scene_test` 报告；报告 ID 用 `uuid5(sim_id)` 稳定生成，同一 `sim_id` 重放覆盖同一对象（幂等）。
2. 产物：`hunter-reports/scene_test/{yyyy-mm-dd}/{report_id}.json` + sidecar `reports/scene_test/{report_id}.json`（复用既有报告列表/详情接口）+ 指针 `eval/simulation/latest.json` 与 `eval/simulation/{sim_id}.json`；`data-analytics` 为 `hunter-reports` 唯一写方。
3. **跨消费组竞态加固**：报告生成不再依赖 `data-collector-carla` 先写 Redis；本组维护状态/指标/事件的有界本地缓存（随消息批次累积），终态到达时即使 Redis 尚无缓存也能产出完整报告。新增回归测试覆盖“Redis 为空 + 本地缓存有数据”的场景。

### 6. 部署与运维编排

1. `infra/deploy/docker-compose.yml` 新增 `vil-forwarder`（8090，仅 `127.0.0.1` 映射），并给 `scene-service` 注入 `VIL_SESSION_TTL_SECONDS` / `SIM_TOPIC_*`、`data-collector` 注入 `CARLA_SENSOR_CONSUMER_ENABLED` / `CARLA_SIM_CACHE_TTL_SECONDS` / `CARLA_SIM_EVENTS_MAX`、`data-analytics` 注入 `CARLA_SENSOR_CONSUMER_ENABLED` / `CARLA_SENSOR_TOPIC_PATTERN` / `CARLA_CONSUMER_GROUP`；`infra/deploy/.env.example` 新增 `VIL_FORWARDER_PORT=8090` 与 §9a CARLA/VIL 配置块（管理端基址、8 个子路径、`SIM_TOPIC_*`、VIL 会话与转发参数、回传面开关）。
2. `.gitlab-ci.yml` 镜像构建与单元测试入口加入 `vil-forwarder`；`scripts/run_unit_tests.py` 纳入 `services/vil-forwarder`。
3. `scripts/collect-logs.sh` / `health-check.sh` / `daily-check.sh` / `install.sh` 纳入 `vil-forwarder`（启动、健康检查、日志采集）。
4. 监控口径由 6 → **7 个微服务**（`infra/monitoring/README.md`、`prometheus.yml` 服务清单）；`render_k8s.py --check` 现为 **7/7** 镜像占位符合规。
5. 两套编排对称：scene-service 的 `VIL_SESSION_TTL_SECONDS` / `SIM_TOPIC_*`、data-collector 的 `CARLA_SIM_*`、data-analytics 的 `CARLA_CONSUMER_GROUP` 在 K8s ConfigMap 已显式登记，compose 侧已按同一键名注入（生产改 `SIM_TOPIC_REPLICATION_FACTOR=3`）。

### 7. 验证

| 验证项 | 结果 |
|---|---|
| `pytest common/python/tests/test_kafka_contracts.py test_storage_contracts.py test_kafka_consumer_unit.py` | **51 passed** |
| `pytest services/scene-service -q` | **296 passed** |
| `pytest services/data-collector -q` | **188 passed** |
| `pytest services/data-analytics -q` | **125 passed**（含报告本地缓存回归） |
| `pytest services/vil-forwarder -q` | **4 passed** |
| `scripts/verify_data_layer.py` | **通过 34 项，失败 0** |
| `scripts/verify_infra.py` | **全部校验通过** |
| `scripts/render_k8s.py --check` | **PASS（7 个微服务）** |
| `scripts/generate_contracts_configmap.py --check` | **PASS** |
| `scripts/verify_test_layer.py` | **PASS** |
| 本轮改动相关 Python 文件 ruff | **All checks passed** |
| shell 脚本 `bash -n`、关键 YAML 解析 | **通过** |

### 8. 升级与部署注意（必须按序）

```bash
cd /opt/hunter-core && git pull

# 1) 重新生成并应用运行时契约（新增 2 个 Schema；scene/collector/analytics/vil 都要读）
python3 scripts/generate_contracts_configmap.py          # 或使用仓库已生成文件
kubectl apply -f infra/k8s/base/03-configmap-contracts.yaml
# compose：contracts 为 bind mount，确认 ./contracts/kafka 已随 git pull 更新

# 2) 初始化脚本无需新增静态 Topic（carla.* 由 scene-service 运行时创建）
#    但 Kafka broker 必须保持 auto.create.topics.enable=false（契约既有约束）

# 3) 配置 CARLA 控制面地址（空值=未接 CARLA，run 接口会 5001）
#    .env：CARLA_MANAGEMENT_ENDPOINT=http://<carla-manager>:<port>
#    K8s：由 hunter-common-config / Secret 注入，禁止写死在 services/scene-service.yaml

# 4) 重建（共享库 + 新服务，restart 无效）
docker compose -f infra/deploy/docker-compose.yml --project-directory . build \
  scene-service data-collector data-analytics vil-forwarder
docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate \
  scene-service data-collector data-analytics vil-forwarder

# 5) 冒烟
curl -sf http://127.0.0.1:8090/healthz && curl -sf http://127.0.0.1:8090/readyz
# 发起一次仿真（需 published 场景与 JWT）后确认动态 Topic 已创建：
#   kafka-topics.sh --bootstrap-server ... --list | grep '^carla\.'
# 消费组应出现：vil-forwarder / data-collector-carla / data-analytics-carla
```

- **生产副本因子**：K8s `SIM_TOPIC_REPLICATION_FACTOR=3`；单机 compose 用默认 1。若集群启用 Kafka ACL，需给 scene-service 的运行账号授予 `Create/Delete` Topic 权限（`super.users` 形态无需额外配置）。
- **新消费组位点**：`vil-forwarder` 首次启动按 `latest`（不回放）；`data-collector-carla` / `data-analytics-carla` 首次无位点会从 `earliest` 开始，但 `carla.*` Topic 均为新会话创建，实际不会读到历史数据。
- **正则订阅发现延迟**：`carla.*.sensor_data` 依赖 Kafka 元数据刷新（`topic.metadata.refresh.interval.ms`），新建会话 Topic 后最迟一个刷新周期内被订阅到；排障时先确认 Topic 已创建而非误判“消费者未订阅”。
- **回滚**：停止并删除 `vil-forwarder` 不影响既有车端链路；scene-service/data-collector/data-analytics 回退到上一镜像即可（新增配置项均有默认值，缺失不阻塞启动）。已创建的 `carla.*` Topic 由保留策略自然清理，或手工删除。

### 9. 分发路径

受跟踪文件（`git status` 共 **68 个**修改/新增）：`contracts/kafka/{topics.yaml,consumer-groups.yaml,schemas/carla_*.schema.json}`、`contracts/database/redis-keys.yaml`、`contracts/openapi/scene-service.yaml`、`common/python/hunter_common/{kafka/admin.py,kafka/messages.py,redis_keys.py}`、`services/scene-service/**`、`services/data-collector/**`、`services/data-analytics/**`、`services/vil-forwarder/**`（新服务）、`infra/deploy/{docker-compose.yml,.env.example,README.md}`、`infra/k8s/{README.md,services/vil-forwarder.yaml,services/{scene-service,data-collector,data-analytics}.yaml,base/03-configmap-contracts.yaml,autoscaling/hpa.yaml,disruption/poddisruptionbudgets.yaml,networkpolicies/network-policies.yaml}`、`infra/monitoring/**`、`.gitlab-ci.yml`、`scripts/{verify_data_layer.py,run_unit_tests.py,collect-logs.sh,health-check.sh,daily-check.sh,install.sh}`、根 `README.md`。

⚠ 本仓库现行规则（`.gitignore`）：`docs/` 与 `release.md` **只更新、不追踪**，文档四件套与本条目需单独同步到服务器：`docs/01-部署概述与环境要求.md`（业务服务清单 + 8090 端口 + CARLA/VIL 数据流）、`docs/deployment.md`（拓扑/HPA/PDB/资源/验证项 + 新增 §17）、`docs/运维操作手册.md`（§3.12 仿真链路自检、巡检项）、`docs/故障排查指南.md`（新增场景 18）、`release.md`（本条目）。

### 10. 待办与风险（如实标注）

1. **未与真实 CARLA 联调**：本轮完成契约、单元测试、静态部署校验；`carla-manager` 仍需外部实现并逐项对齐——回显平台 `sim_id`、消费 `carla.{sim_id}.vil_state`、向 `carla.{sim_id}.sensor_data` 发布状态/遥测/事件/指标/产物、实现 pause/resume/stop。
2. **未做真实 K8s 部署**：`render_k8s.py --check` / `verify_infra.py` 仅证明清单契约一致，动态 Topic 创建所依赖的 Kafka Admin 连通性、ServiceAccount/RBAC、NetworkPolicy 放通需实机验证。
3. **10Hz latest-wins 的实际时延/丢帧率**未在真实闭环中测量；VIL 会话缓存 1s 意味着车辆切换会话后最迟 1s 生效。
4. **仿真报告字段**以 `sim_metrics` 现有载荷为准；若 CARLA 侧指标项不足，报告会出现空值而非失败（不伪造数据）。

## V1.23.4（修复：CARLA 接入代码审查——失败回滚 / 孤儿实例 / 会话原子性 / 乱序迟到帧 / 内存上界 / 批量入库时序）

Date：2026-10-24
变更类型：修复（代码审查 P0/P1）+ 加固（可观测性 / 边界校验 / 文档）

**背景**：对 V1.23.3 三链路实现做代码审查，逐条对照《CARLA 仿真系统接入数据采集与分析系统方案》核查功能完整性，并专项排查内存泄漏、边界与断连超时、并发安全与性能。本轮只改平台侧代码/配置/文档，**CARLA 侧（`carla-manager`）外部依赖不变**。

### 1. 审查发现（问题清单）

**P0（会产生孤儿资源 / 数据丢失 / 车辆被长期阻塞）**

1. **非幂等 POST 被自动重试**：创建实例、下发场景均为非幂等操作，原实现按 `CARLA_REQUEST_MAX_RETRIES` 重试；请求超时但 CARLA 已执行时会创建第二个实例/重复加载 world。
2. **`sim_id` 不一致留下孤儿实例**：CARLA 回显 `sim_id` 与平台不一致时直接抛 5001，但实例已创建且未被停止，平台不再持有其 ID。
3. **动态 Topic 部分创建失败不回滚**：`ensure_carla_session` 中途失败时已建 Topic 残留，同一 `sim_id` 重试会出现半套 Topic（缺 `.dlq` 或只有 `vil_state`）。
4. **VIL 会话 `SET NX` 后 `SADD` 失败不回滚**：会话键残留但不在 `vil:active`，该车辆被无主会话阻塞到 TTL（默认 2 小时），且启动对账也看不到它。
5. **终态回收依赖人工查询**：仅 `get_result`/`cancel` 触发会话释放与 Topic 删除；无人查询时 `vil:session` 一直占用到 TTL，车辆无法发起下一次 VIL 仿真。
6. **无启动对账**：服务崩溃/重启后残留的“已占位未绑定实例”“实例已不存在”“实例已终态”会话无人清理。

**P1（内存泄漏 / 性能 / 语义偏差）**

7. **vil-forwarder 无乱序与迟到保护**：`seq` 回退帧、源时间戳迟到数秒的帧仍会推进 CARLA；同车切换会话后旧 `last_seq`/频率/待发帧未清理，导致新会话前若干帧被误丢。
8. **vil-forwarder 车辆状态表无界**：`_state`/`_flush_tasks` 只增不减，车辆下线或会话结束后长期运行内存持续增长。
9. **vil-forwarder Redis 故障放大为 DLQ 风暴**：会话读取失败逐帧抛错 → 消费重试 → 转 DLQ；实时旁路本应丢帧继续。
10. **`SessionRegistry` 本地缓存无上界**：异常 `vehicle_id` 洪泛时缓存条目无界增长。
11. **sim_telemetry 逐条事务写入**：10Hz × 多车时事务数过高；且 handler 内写库失败直接进 DLQ，数据不会自愈。
12. **批量缓冲无空闲冲刷**：批次收尾钩子只在“本轮有新消息”时执行，会话停止发送后最后不足一批的遥测会滞留到下一次消息或停机。
13. **停机 flush 实际未执行**：`_SupervisedConsumer` 未暴露 `flush`，主流程 `getattr(consumer, "flush")` 恒为 `None`。
14. **事件映射与设计 7.3.2 不符**：`collision` 被镜像为 `collision_warning`（实际碰撞 ≠ 预警）；缺 `emergency_brake → emergency_stop` 映射。
15. **报告指标字段只认一种命名**：仅识别 `collision_count`，不识别设计文档 6.5/7.4 的 `collisions`；缺 `grade`/`passed` 推导、起止时间、HTML 产物与设计规定的 `scene_test/{date}/{id}.meta.json` sidecar。
16. **`vil_state` Topic 参数与设计 7.1.4 不一致**：1 分区 / 1 小时保留，设计为 3 分区 / 1 天保留。
17. **质量档位与 VIL 参数边界不全**：缺 `Epic` 档；`target_hz` 允许到 30（设计上限 50）、`data_timeout_ms` 下限 50（设计 100）、`extrapolation_ms` 上限 2000（设计 1000）。
18. **模式校验缺口**：`mode=sil` 允许携带 `sim_config.vil`/`replay_config`；`mode=vil` 允许 `allow_parallel=true`（与“同一车辆仅一个会话”冲突）。

### 2. 修复内容

**scene-service（控制面 / 会话 / Topic）**

1. `CarlaManagementClient._request` 新增 `retry` 参数：默认仅 GET/HEAD 重试；创建实例、下发场景显式 `retry=False`；pause/resume/stop 保持可重试（幂等控制语义）。
2. 新增 `CarlaSimIdMismatchError`（携带 `instance_id`）：`run_scene` 捕获后先 `_stop_orphan` 回收实例，再上抛 5001。
3. `KafkaAdmin.ensure_carla_session` 改为“记录已建 Topic + 异常时逐个回滚删除”，失败不再残留半套 Topic。
4. `VilSessionStore.acquire` 在 `SADD` 失败时删除已占位会话键；`release` 改为三键 best effort 清理（任一失败不影响其余键，最终统一 5001 供告警）。
5. `get_progress` 终态同样触发 `_finalize_instance`；新增 `_finalized` 有界去重（4096），并发查询/控制只清理一次；释放失败（Redis 不可用）时**撤销去重标记**，下一次轮询/对账重试（release 幂等）。
6. 新增启动对账 `reconcile_sessions()`（`main.py` 15s 超时，失败不阻塞启动）：清理 `vil:active` 中已过期成员、超过 `UNBOUND_SESSION_GRACE_SECONDS=300` 仍未绑定实例的会话、CARLA 404/已终态会话；CARLA 暂不可达时保留会话避免误删。
7. 模式校验：SIL 禁止 `sim_config.vil`/`replay_config`；VIL 禁止 `allow_parallel=true`；补充 `SimulationQuality.EPIC`；收紧 VIL 参数边界（`target_hz ≤ 50`、`data_timeout_ms ≥ 100`、`extrapolation_ms ≤ 1000`）。

**vil-forwarder（VIL 数据面）**

8. 重写为按车辆状态表：`seq` 乱序丢弃；源时间戳迟到超过 `VIL_MAX_SOURCE_LAG_MS`（默认 500ms）丢弃；同车 `sim_id` 变化时清空旧会话状态；按会话 `target_hz` 节流（latest-wins，单飞后台冲刷任务）。
9. 内存上界：空闲 `VIL_STATE_IDLE_TTL_SECONDS`（默认 300s）+ 最大跟踪车辆数 `VIL_MAX_TRACKED_VEHICLES`（默认 10000），超限按最久未活跃回收；`SessionRegistry` 缓存同样有界。
10. Redis 短暂不可用改为**丢帧计数**（`session_unavailable`），不向消费者抛错，避免 DLQ 风暴；发布异常在后台任务内捕获并计数，不逃逸。
11. 新增指标 `hunter_vil_forward_total{result}`、`hunter_vil_forward_lag_ms`、`hunter_vil_active_sessions`；`/readyz` 增加 Kafka 探测，`/status` 增加 `tracked_sessions`/`drop_counts`。
12. `topics.yaml#carla_topics.vil_state` 调整为 **3 分区 / 1 天保留**（与设计 7.1.4 一致）。

**data-collector（回传落库）**

13. `sim_telemetry` 改为批次缓冲 + `on_batch_end` 单事务写入（`SIM_TELEMETRY_BATCH_SIZE=2000`）；失败回填本批并抛错 → 不提交 offset，避免进 DLQ；`KafkaConsumerManager` 空闲轮询也执行收尾钩子（`SIM_TELEMETRY_FLUSH_INTERVAL_MS=500` 即轮询周期），会话停止发送后末批不再滞留；**失败后空闲期冲刷成功会补提交 offset**（否则位点停在旧位置，重启重复回放已入库批次）。
14. `_SupervisedConsumer` 暴露 `flush()` 并保留最后实例；停机顺序改为“停止消费 → flush → 释放依赖”。
15. 事件映射按设计 7.3.2 修正：移除 `collision → collision_warning`；新增 `emergency_brake → emergency_stop`；`lane_invasion`/`red_light_run`/`scenario_timeout` 不镜像。

**data-analytics（报告）**

16. `sim_metrics` 字段兼容双命名（`collisions`/`collision_count`、`lane_invasions`/`lane_invasion_count`、`emergency_brakes`/`emergency_brake_count` 等）；新增 `grade`（附录 E：S/A/B/C/D）与 `passed` 推导、`start_time`/`end_time`、HTML 报告、`scene_test/{date}/{report_id}.meta.json` sidecar（同时保留 `reports/` sidecar 供既有列表接口）；Redis 事件反转为时间正序。
17. `ObjectStorage` 协议新增 `put_bytes`（S3 实现同步补齐），HTML 产物写入失败仅告警不影响 JSON 报告。

### 3. 新增配置项（均有默认值，已在 compose / `.env.example` / K8s ConfigMap 显式登记）

| 服务 | 变量 | 默认 | 说明 |
|------|------|------|------|
| vil-forwarder | `VIL_MAX_SOURCE_LAG_MS` | 500 | 源时间戳迟到上限，超过即丢弃 |
| vil-forwarder | `VIL_MAX_TRACKED_VEHICLES` | 10000 | 本地车辆状态表 / 会话缓存上限 |
| vil-forwarder | `VIL_STATE_IDLE_TTL_SECONDS` | 300 | 空闲车辆状态回收阈值 |
| data-collector | `SIM_TELEMETRY_BATCH_SIZE` | 2000 | 仿真遥测单批入库条数（同时作为消费批次上限） |
| data-collector | `SIM_TELEMETRY_FLUSH_INTERVAL_MS` | 500 | 空闲冲刷周期（作为消费轮询超时） |

### 4. 验证

| 验证项 | 结果 |
|---|---|
| `scripts/run_unit_tests.py services/scene-service` | **305 passed**（新增 `test_carla_hardening.py` 8 条） |
| `services/vil-forwarder` | **9 passed**（新增乱序/迟到、会话切换、空闲回收、缓存上界、Redis 不可用） |
| `services/data-collector` | **189 passed**（批量 flush、`emergency_brake` 映射、`collision` 不镜像） |
| `services/data-analytics` | **125 passed**（指标双命名、grade/passed） |
| `common/python` | **338 passed**（vil_state 3 分区/1 天；空闲期补提交） |
| `scripts/verify_data_layer.py` | **通过 34 项，失败 0**（脚本既有返回码 1，以文本为准） |
| `scripts/render_k8s.py --check` / `generate_contracts_configmap.py --check` | **PASS** |
| `scripts/verify_test_layer.py` / `verify_infra.py` | **PASS / 全部校验通过** |
| 本轮改动文件 `ruff check` | **All checks passed** |

### 5. 兼容与升级

- **必须重建镜像**（共享库 `kafka/admin.py`、`kafka/consumer.py`、`kafka/producer.py` 有变更）：`scene-service`、`vil-forwarder`、`data-collector`、`data-analytics`；`restart` 不生效。
- **必须重新生成运行时契约 ConfigMap**（`contracts/kafka/topics.yaml` 变更）：`python3 scripts/generate_contracts_configmap.py` 后 `kubectl apply -f infra/k8s/base/03-configmap-contracts.yaml`；compose 为 bind mount，随 `git pull` 生效。
- 无数据库迁移；新配置项均有默认值，缺失不阻塞启动。
- 行为变化（预期）：VIL 迟到/乱序帧开始被丢弃并计入 `hunter_vil_forward_total{result=...}`；`collision` 不再出现在车端 `events`（仅 Redis 仿真事件）；终态后即使无人查询，进度轮询/启动对账也会释放会话。

### 6. 未实现 / 外部依赖（如实标注，**不记为已修复**）

1. **`sim` 服务 Token（设计 G11 / 9.x）**：`scene-service` 签发短期 JWT（`sub=sim:{sim_id}`、`roles=[sim]`、`aud=data-collector`）并用于 `sensor_file` 上传授权，本轮**未实现**；当前回传面按既有服务间信任模型处理。
2. **`POST /api/v1/data/replay/export`**：实车轨迹回放 bundle 导出端点未实现（设计 6.6 / 附录接口清单）。
3. **`GET /api/v1/analytics/simulations/{sim_id}` 与 `.../report`**：仿真指标摘要/报告元信息查询端点未实现；当前可通过 `hunter-reports` 对象与 `eval/simulation/{sim_id}.json` 读取。
4. **`analytics_result` 生产**：设计 P3（Flink 实时看板）未实现；报告只写 `hunter-reports`。
5. **PDF 报告**：仅输出 JSON + HTML；`ReportFormat.PDF` 仍为契约枚举，未生成产物。
6. **新增 `simulation_*` 表**：按设计允许的降级方案，仿真遥测写 `data_collector.vehicle_telemetry`（`vehicle_id=SIM-{sim_id}`）、事件写 `events` + Redis，未新增 `simulation_instances/telemetry/events/metrics` 四表。
7. **`hunter.*.command_result` 辅助消费**：`vil-forwarder` 目前只消费 `telemetry_clean`，命令状态叠加/审计未实现。
8. **真实 CARLA 联调与真实 K8s 部署**：本轮仅单元测试 + 静态校验；`carla-manager` 仍需外部实现 `sim_id` 回显、VIL 消费、`sensor_data` 生产与 pause/resume/stop。
9. **Grafana 面板**：`hunter_vil_forward_total` / `hunter_vil_forward_lag_ms` / `hunter_vil_active_sessions` 已暴露，但本轮未在
   `infra/monitoring/grafana/dashboards` 新增面板；排障期用 `/metrics` 与 `/status`（见《运维操作手册》§3.12）。

