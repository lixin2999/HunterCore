# contracts/openapi — REST API 契约（OpenAPI 3.0）

单一事实来源：所有微服务 REST 接口先在此定义 OpenAPI 3.0 YAML，再进行实现。

## 文件清单

| 文件 | 服务 | 路由前缀 | 状态 |
|------|------|----------|------|
| api-gateway.yaml | api-gateway | 统一入口（8080） | ✅ 已定义（认证 + 运维探针 + 路由表/限流/WebSocket 扩展字段） |
| scene-service.yaml | scene-service | `/api/v1/scene` | ✅ 已定义（10 个业务端点 + 4.2.1/4.2.2 数据结构 + 4.4/4.5 流程扩展字段） |
| data-collector.yaml | data-collector | `/api/v1/data` | ✅ 已定义（7 个业务端点 + 5.3.3 遥测结构 + 5.4 预处理 / 5.5 上传流程扩展字段） |
| data-analytics.yaml | data-analytics | `/api/v1/analytics` | ✅ 已定义（8 个业务端点 + 6.2 实时作业 / 6.3 离线作业 / 6.4 Corner Case / 6.5 报告模板扩展字段） |
| ota-service.yaml | ota-service | `/api/v1/ota` | ✅ 已定义（15 个业务端点 + 8/9 章 DDL 与 Kafka 对齐 + 版本上传 / 发布校验 / 灰度批次 / 回滚扩展字段；12.5 节原文缺失 → 推导清单） |
| remote-control.yaml | remote-control | `/api/v1/remote`、`/ws/remote/**` | ✅ 已定义（8 个业务端点 + 3 个运维端点 + WebSocket 契约 / 会话生命周期 / 控制通道（20Hz）/ 视频 / 归档扩展字段；12.6 节原文缺失 → 推导清单） |

## 统一约定（所有服务强制）

- **统一响应**：`{code, message, data, request_id, timestamp}`，字段不可更改；`code=0` 成功，非 0 取预定义错误码（附录 A）
- **错误码 → HTTP 状态**：与各服务 `app/core/error_handlers.py` 的 `HTTP_STATUS_BY_CODE` 一致
  （网关契约以 `x-hunter-error-status-map` 机器可读声明，由契约测试交叉校验）
- **认证**：`Authorization: Bearer <JWT>`（Access Token 2h / Refresh Token 7d）；车辆设备为 X.509 双向 TLS
  （`mutualTLS`，CommonName = vehicle_id），不走 REST
- **链路追踪**：入口透传/生成 `X-Request-ID` 并在响应回带；网关转发后端时注入 `X-User-Id`、`X-Roles`、`X-Trace-Id`
  （覆盖客户端同名头，防身份伪造）
- **限流**：五级限流（全局/用户/IP/接口/车辆），阈值见附录 D（不可更改）；超限返回 **HTTP 429 + `Retry-After`**，
  响应体 `code` 复用 5001（附录 A 无限流专用错误码，禁止新增）
- **分页**：列表接口 `page`（≥1）+ `page_size`（≤200，与 `BaseRepository.paginate` 一致）+ 可选 `sort`，
  响应 `data` 含 `{items, total, page, page_size}`（各服务契约中显式声明）
- **探针**：`/healthz`、`/readyz`（统一 JSON；依赖异常返回 503 + code=5001）；`/metrics` 为例外（Prometheus 文本格式）

## 网关契约要点（api-gateway.yaml）

- **自持端点**：`POST /api/v1/user/login|refresh|logout`、`GET /api/v1/user/me`；`/healthz`、`/readyz`、`/metrics`（`x-internal: true`）
- **转发端点**：根级 `x-hunter-gateway-routes` 声明（7 个前缀 → 目标服务 + 端口 + 鉴权要求）；
  后端资源端点由各自服务契约定义，**禁止两处声明同一端点**
- **WebSocket**：`x-hunter-websocket-routes` 声明 `/ws/remote/**` → remote-control（20Hz 指令、>500ms 停车、视频 ≤200ms、指令 ≤100ms）
- **Kafka**：`x-hunter-kafka` 声明网关不生产/不消费任何 Topic（若后续承接 alert_event 推送，须先更新
  `contracts/kafka/consumer-groups.yaml`）
- **⚠ 待核对项**：认证端点路径与载荷、`/api/v1/vehicle|user` 归属服务（模块表仅 6 个微服务）、MFA 错误码复用 1001、
  服务发现机制与配置键名、熔断阈值

## 场景服务契约要点（scene-service.yaml）

- **业务端点**（设计文档 12.2 节，不可增删）：`GET/POST /api/v1/scene`、`GET/PUT/DELETE /api/v1/scene/{scene_id}`、
  `GET /api/v1/scene/templates`、`POST /api/v1/scene/{scene_id}/duplicate|publish|run`、`POST /api/v1/scene/export`
- **数据结构**：`SceneMeta`（7 个元信息字段 → `scene_svc.scenes` 列）+ `SceneConfig`（`map/ego_vehicle/weather/actors/events/success_criteria/duration`
  → `config_json`）合并即 4.2.2 节完整结构；顶层字段映射见 `x-hunter-scene-config-contract`
- **状态机**：`draft → published → archived`（`x-hunter-lifecycle`）；仅 `draft` 可编辑，`published` 可下发，删除仅限 `draft/archived`
- **导出**：4.3 节两种格式（`carla_scenariorunner_xml` / `openscenario_1_2`），产物落 MinIO `hunter-scene-assets`，
  下载预签名 15 分钟（`x-hunter-export`）
- **仿真下发**：4.4 节 7 步流程 + `sim_instance_id`（`x-hunter-simulation-flow`）；Carla 管理 API 地址走环境变量
  `CARLA_MANAGEMENT_ENDPOINT`（禁止硬编码）
- **Kafka**：`x-hunter-kafka` 声明不生产任何 Topic，仅消费 `analytics_result`
  （消费组 `scene-service-analytics-result`，实车场景自动提取 4.5 节）
- **⚠ 待核对项**：`scene_type` 编码值（4.2.1 只给两级结构）、仿真进度/结果端点缺口（4.4 第 6/7 步）、
  `archived` 无归档端点、导出返回形式、`version` 递增策略 —— 全量见契约 `x-hunter-pending-confirmation`（12 项）

## 数据采集契约要点（data-collector.yaml）

- **业务端点**（推导清单，`x-hunter-endpoints` 逐条登记依据与风险 —— 12.2 节未随仓库提供）：
  `GET /api/v1/data/telemetry`（附录 D 限流端点）、`GET /api/v1/data/events`、`GET /api/v1/data/events/{event_id}`、
  `POST /api/v1/data/events/{event_id}/acknowledge`、`GET /api/v1/data/files`、
  `POST /api/v1/data/files/presign`、`POST /api/v1/data/files/complete`
- **数据结构**：遥测查询响应（`TelemetrySample` + 六段嵌套）与 5.3.3 节消息结构逐字段一致，
  由 `data_collector.vehicle_telemetry` 扁平列重组（映射见 `x-hunter-telemetry-query-contract`，
  查询必须带 `vehicle_id` + 时间区间，保留 90 天）；事件模型一一对应 `events` 表列
  （18 种类型 / 3 级等级取 `EVENT_LEVEL_BY_TYPE`，确认写 `acknowledged*` 三列且幂等）
- **5.4 预处理**：6 步（解析 → 校验 → 时间对齐 → 清洗 → enrichment → 序列化写入），
  失败转 DLQ `{topic}.dlq`；入库延迟 ≤1s、时序写入 ≥10000 点/秒、单车 ≤100 msg/s
- **5.5 文件上传**：请求上传 → 预签名（上传 1h / 下载 15min + Range）→ 直传 → 完成通知 →
  MD5/SHA-256 校验（失败复用 6001）→ 投递 `sensor_file`；命名规范
  `{bucket}/{vehicle_id}/{date}/{data_type}/{timestamp}_{seq}.{ext}`（服务端生成，禁止客户端指定）
- **鉴权**：控制台 JWT（RBAC 资源域 `data`）；车端文件上传走 `deviceCertificate`（X.509 mTLS，
  CN = vehicle_id，禁止请求体指定他车）
- **Kafka**：`x-hunter-kafka` 消费 4 个车端 Topic（4 个 `data-collector-*` 消费组，正则订阅），
  生产 `telemetry_raw` / `telemetry_clean` / `event_raw` / `sensor_file`；不消费 `alert_event`、
  `analytics_result` 与 Carla Topic
- **⚠ 待核对项**：端点清单来源、文件元信息缺表（5.5 第 6 步）、`data_type` 取值域与 Bucket 映射、
  校验失败错误码复用 6001、车端 REST 接入方式、查询时间跨度上限、Carla Topic 归属 ——
  全量见契约 `x-hunter-pending-confirmation`（12 项）

## 数据分析契约要点（data-analytics.yaml）

- **业务端点**（设计文档 12.4 节逐条对齐，不可增删）：`GET /api/v1/analytics/reports`、
  `GET /api/v1/analytics/reports/{report_id}`、`POST /api/v1/analytics/reports/generate`、
  `GET /api/v1/analytics/dashboard`、`GET /api/v1/analytics/perception/eval`、
  `GET /api/v1/analytics/control/eval`、`GET /api/v1/analytics/scene/coverage`、
  `GET /api/v1/analytics/corner-cases`（除报告生成外均为只读查询）
- **定位**：读模型 + 编排服务 —— 重计算由 Flink（实时，`flink-jobs/`）/ Spark（离线，`spark-jobs/`）承担，
  REST 只读预计算结果（保证 P95 ≤ 200ms）；报告生成为**异步**（202 + 轮询，规避 PDF/HTML 渲染耗时）
- **6.2 实时作业**：`x-hunter-realtime-jobs` 定义 5 个 Flink 作业（车辆状态监控 / 异常驾驶检测 /
  算法性能监控 / 碰撞风险评估 / 数据质量监控）；阈值 14 项全部环境变量化并在 K8s ConfigMap 注入，
  与「事件类型定义」（`EVENT_LEVEL_BY_TYPE`）机器可检一致；数据质量监控消费 `telemetry_raw`，
  其余消费 `telemetry_clean`
- **告警契约**：`alert_event` 消息 Schema 已补全（`alert_type` 18 种受控类型、`level` 必须等于
  `EVENT_LEVEL_BY_TYPE[alert_type]`、`source_job` 取 5 个实时作业、`rule` 回带阈值）；6.2.3 节
  「TTC < 3.0s 预警（warning）」与受控词表 `collision_warning`(critical) 的等级冲突已显式登记
  （不新增类型、不放宽等级 → 3.0s 预警走 `analytics_result`）
- **6.3 离线作业**：`x-hunter-offline-jobs` 定义 7 个 Spark 作业与 cron（日报/评估每日、覆盖率与挖掘每周、
  月报每月，UTC）；6.3.3 节控制性能阈值（速度 RMSE < 0.2 m/s、转向 RMSE < 0.02 rad、超调 < 10%、
  调节时间 < 2s）在契约、响应 Schema、作业定义三处一致
- **6.4 Corner Case**：5 类异常（kinematic/perception/planning/interaction/environment）+
  Isolation Forest / DBSCAN + 事件前后各 10 秒截取窗口（与 `analytics_result.schema.json` 一致）
- **6.5 报告**：5 类模板（车辆日报 / 算法评估 / 场景测试 / OTA 升级 / 月度运营）× 3 格式
  （HTML / PDF(Puppeteer) / JSON），落 MinIO `hunter-reports`（永久，下载预签名 15 分钟 + Range）
- **数据访问边界**：写 `data_analytics.algorithm_metrics`；只读例外（er.md 第 3 节）仅
  `data_collector.vehicle_telemetry`（`hunter_analytics_ro`）；events / scenes / vehicles 一律经
  服务 REST（data-collector / scene-service / vehicle-service）；不新增 DB 表、不新增 Redis 键模式
- **Kafka**：消费 4 个平台内部 Topic（`telemetry_clean` / `telemetry_raw` / `event_raw` / `sensor_file`，
  4 个 `data-analytics-*` 消费组），生产 `analytics_result` + `alert_event`
- **⚠ 待核对项**：报告/Case 元信息缺表、报告异步语义与状态枚举、TTC 等级冲突、`alert_event` 消费方落位、
  数据质量阈值与落位、算法指标窗口与感知真值来源、Corner Case 参数、Redis 状态读取归属、
  12.4 节缺规划质量/数据质量端点、看板入库延迟来源、查询跨度上限、消费组契约修正 ——
  全量见契约 `x-hunter-pending-confirmation`（12 项）

## OTA 管理契约要点（ota-service.yaml）

- **业务端点**（15 个，推导清单 —— 设计文档 §12.5 原文未随仓库提供）：
  版本仓库 `GET /api/v1/ota/versions`、`POST /api/v1/ota/versions`、`GET /api/v1/ota/versions/{version_id}`、
  `POST /api/v1/ota/versions/{version_id}/publish`、`POST /api/v1/ota/versions/{version_id}/deprecate`；
  升级任务 `GET /api/v1/ota/tasks`、`POST /api/v1/ota/tasks`、`GET /api/v1/ota/tasks/{task_id}`、
  `POST /api/v1/ota/tasks/{task_id}/start|pause|resume|cancel|rollback`；升级记录
  `GET /api/v1/ota/tasks/{task_id}/records`、`GET /api/v1/ota/vehicles/{vehicle_id}/records`
  （推导依据逐条登记在 `x-hunter-endpoints.items`）
- **版本上传两步式（`x-hunter-version-upload-flow`）**：建草稿（`draft`）→ 服务端签发 1 小时上传预签名地址 → 客户端直传
  MinIO `hunter-ota-packages` → `publish` 由服务端**单次流式**校验；对象键
  `hunter-edge/ota/{model}/{version_name}/{version_code}/package.tar.gz`（服务端生成，禁止客户端指定）
- **发布门禁（唯一）**：`publish` 依次校验 包长/MD5/SHA-256（→ 6001）→ RSA-2048 验签 `RSASSA-PKCS1-v1_5 + SHA-256`
  （→ 6002）→ `version_code` 同 `applicable_models` 范围单调递增（→ 6003），全部通过才 `draft → published` + 写 `release_time`；
  该项为**长耗时操作**，已在 `x-hunter-service.performance.exceptions` 登记为性能例外（网关超时 ≥300s，见 pending #9）
- **灰度发布（`x-hunter-canary-rollout`）**：4 批 `5% → 20% → 50% → 100%`，每批观察 24h，成功率 ≥0.95 才推进
  （`SUCCESS / (SUCCESS + FAILED + ROLLED_BACK)`）；< 0.95 立即 `paused` + 告警 `alert_event`（人工只能 rollback / cancel）；
  批次推进用 PG 行锁串行化，禁止跳批（pending #3）
- **任务状态机**：`draft → pending_approval → running/observing → paused → completed/rolled_back/cancelled`；
  升级门禁 `电量 ≥50% / 静止(P 档) / 网络稳定 / 存储 ≥2GB`（不满足车辆计入 `blocked[]` 并给出 `reason`）
- **回滚**：平台侧仅按记录（`ota_records`）下发 `ota_rollback` 指令（A/B 分区 + 车端自检失败自动回退兜底），
  只允许 `previous_slot`（不支持任意历史版本，pending #5）
- **Kafka**：消费 `hunter.*.ota_status`（消费组 `ota-service-ota-status`，正则订阅、手动提交、DLQ `{topic}.dlq`）；
  生产 `hunter.{vehicle_id}.ota_notify`（逐车下发，1 小时预签名 URL）与 `hunter.{vehicle_id}.command`（`ota_rollback`）；
  不消费 `command_result`、不生产 `broadcast.command`（pending #12）；车端连接强制 SASL_SSL + SCRAM-SHA-512
- **数据访问边界**：写 `ota_svc.ota_versions / ota_tasks / ota_records`（禁止 DELETE）；不跨 schema；不新增 Redis 键模式
  （写 `ota:progress:{task_id}`，只读 `vehicle:status:{vehicle_id}` / `vehicle:online:set`）
- **⚠ 待核对项**（全量 19 项见契约 `x-hunter-pending-confirmation`）：§12.5 原文缺失、
  `release_type` 取值域、跳批策略、分片上传语义、回滚目标、观察窗口顺延、`ota_notify` 时延目标、车端是否直连 REST、
  publish 网关超时与异步化、publish 限流建议值、DLQ Topic 登记、`command_result` / `broadcast` 归属、
  升级包大小上限、小车队批次取整、审计保留期、灰度停用通道、安全开关、vehicle-service 依赖、门禁数据缺失放行 ——
  全量见契约 `x-hunter-pending-confirmation`（19 项）

## 远程操控契约要点（remote-control.yaml）

- **业务端点**（8 个，推导清单 —— 设计文档 §12.6 / §8.3–8.5 原文未随仓库提供，逐条依据登记在
  `x-hunter-endpoints.items`）：车辆 `GET /api/v1/remote/vehicles`；会话 `GET /api/v1/remote/sessions`、
  `POST /api/v1/remote/session`（附录 D 明列端点，1 QPS/用户）、`GET|DELETE /api/v1/remote/session/{session_id}`；
  操控记录 `GET /api/v1/remote/history`、`GET /api/v1/remote/history/{session_id}`、
  `GET /api/v1/remote/history/{session_id}/video`（预签名 15 分钟 + Range）
- **WebSocket（`x-hunter-websocket-routes` / `x-hunter-websocket-contract`）**：`/ws/remote/{session_id}/control`
  （20Hz 指令上行 + ack/status/error 下行 + 10s 心跳）与 `/ws/remote/{session_id}/signal`（SDP/ICE 中继）；
  WSS + JWT 握手校验（1001/1003/1002/3001/7001 直接以 HTTP 4xx 拒绝、不进帧循环）；
  媒体面 SRTP/UDP **不经** api-gateway，须在 K8s 单独暴露 UDP
- **控制通道（`x-hunter-control-channel` / `x-hunter-control-safety`，不可更改）**：20Hz（50ms）→
  Kafka `hunter.{vehicle_id}.remote_control`（key=vehicle_id、acks=all、压缩 lz4）；>500ms 无指令车端自动减速停车
  （车端兜底 + 平台侧 degraded）；服务端对 `target_velocity` 限幅 ±2.0 m/s；指令延迟 ≤100ms、视频端到端 ≤200ms；
  同一车辆同一时间仅一名操作员（Redis 分布式锁 + 7001）
- **会话生命周期（`x-hunter-session-lifecycle` / `x-hunter-session-heartbeat`）**：
  `connecting → active → degraded → ended`；心跳 10s、连续缺失 3 次降级、60s 结束（派生阈值，pending #4）；
  结束收敛顺序「session_end → 停录像 → 封存 sidecar → 释放锁 → 删 Redis 键」，全程不依赖车端回执
- **视频（`x-hunter-video-contract`）**：H.264（AGX Orin NVENC 硬编码）/ 720p@30fps / 2048–4096 kbps /
  关键帧 1s；延迟预算 采集编码 ≤50ms + 网络 ≤100ms + 解码渲染 ≤30ms（合计 ≤200ms）；SRS 5.0 转发
- **操控记录（`x-hunter-history-archive`，方案 A，用户已确认）**：**不新增数据库表 / ORM / Alembic**；
  会话态唯一来源 Redis `rc:session:{vehicle_id}`；操控记录 = MinIO `hunter-video` 录像
  `remote-control/{vehicle_id}/{yyyy}/{mm}/{dd}/{session_id}.mp4` + 同目录 sidecar JSON（保留 90 天）；
  代价：历史查询无 SQL 过滤/聚合能力（必须带日期前缀收敛，`RC_HISTORY_QUERY_MAX_RANGE_DAYS` 默认 31）
- **Kafka（`x-hunter-kafka`）**：生产 `hunter.{vehicle_id}.remote_control` 与 `hunter.{vehicle_id}.command`
  （会话开始/结束信令）；消费 `hunter.*.command_result`（消费组 `remote-control-command-result`，手动提交 + DLQ）——
  `contracts/kafka/` 相关条目**均已登记，本契约零改动**
- **数据访问边界**：无业务表（`remote_control` schema 为预留）；只读 Redis 读模型
  `vehicle:status:{vehicle_id}` / `vehicle:online:set`（权威值属 vehicle-service）；不跨服务直连 DB
- **⚠ 待核对项**（全量 20 项见契约 `x-hunter-pending-confirmation`，其中 `blocking=true` **8 项**：
  #2 Redis 车辆状态字段清单 / #10 SRS 应用名与流名 / #12 新增环境变量同步 K8s / #14 多副本 WS 故障转移与粘性路由 /
  #15 WS 子路径拆分 / #16 操控指令 `command_id` 缺失（回执精确关联）/ #17 会话信令 `command_type` 取值 /
  #20 WS 握手 JWT 传送方式）：
  操控记录载体能力边界、Redis 车辆状态字段清单、`block_reason` 值域、会话状态名与心跳阈值、Redis 键残留窗口、
  多副本统计聚合、查询跨度默认值、按 session_id 检索成本、`/readyz` 保留 database 项、SRS 应用名/流名、
  `rc:lock:{vehicle_id}` 键登记、新增环境变量同步 K8s、车辆读模型写入方、WS 故障转移与粘性路由、
  WS 子路径拆分、操控指令 `command_id` 缺失（回执精确关联）、`command_type` 取值域（会话信令）、
  `target_steer` 限幅与转角量程、H.264 profile、WS 握手 JWT 传送方式

## 校验命令

```bash
# 网关契约 ↔ 实现 ↔ K8s 清单三方一致性（29 项，无需运行服务）
pytest services/api-gateway/app/tests/test_openapi_contract.py -q

# 场景服务契约 ↔ 设计文档 4 章/12.2 节 ↔ DB DDL ↔ Kafka 契约 ↔ K8s 清单（29 项）
cd services/scene-service && pytest app/tests/test_scene_contract.py -q

# 数据采集契约 ↔ 设计文档 5 章推导清单 ↔ DDL ↔ Kafka 契约（含 sensor_file Schema）↔ K8s 清单（30 项）
cd services/data-collector && pytest app/tests/test_data_collector_contract.py -q

# 数据分析契约 ↔ 设计文档 6 章/12.4 节 ↔ DDL ↔ Kafka 契约（含 alert_event Schema）↔ K8s 清单（32 项）
cd services/data-analytics && pytest app/tests/test_data_analytics_contract.py -q

# OTA 管理契约 ↔ 设计文档 8/9/12.5 节 ↔ DDL（ota_* 三表）↔ Kafka 契约（ota_notify/ota_status）↔ K8s 清单
# （契约测试随 ota-service 实现步骤落地）

# 远程操控契约 ↔ 设计文档 15 节安全约束 / 12.6 节（原文缺失 → 推导）↔ Kafka 契约（remote_control/command_result）
# ↔ MinIO hunter-video 归档 ↔ K8s 清单（含 RC_* 环境变量；契约测试随 remote-control 实现步骤落地）
cd services/remote-control && pytest app/tests/test_remote_control_contract.py -q

# 契约文件本身的静态校验（Step 1 即可运行，无需服务）
python -c "import yaml; yaml.safe_load(open('contracts/openapi/remote-control.yaml', encoding='utf-8'))"
python -c "import json,glob; [json.load(open(f, encoding='utf-8')) for f in glob.glob('contracts/kafka/schemas/*.json')]"
```

> 命名约定：各服务契约测试文件使用唯一文件名（如 `test_scene_contract.py`），
> 避免 monorepo 内 `app/tests/<同名文件>` 在 pytest importlib 模式下模块名冲突。
