# contracts/openapi — REST API 契约（OpenAPI 3.0）

单一事实来源：所有微服务 REST 接口先在此定义 OpenAPI 3.0 YAML，再进行实现。

## 文件清单

| 文件 | 服务 | 路由前缀 | 状态 |
|------|------|----------|------|
| api-gateway.yaml | api-gateway | 统一入口（8080） | ✅ 已定义（认证 + 运维探针 + 路由表/限流/WebSocket 扩展字段） |
| scene-service.yaml | scene-service | `/api/v1/scene` | ✅ 已定义（10 个业务端点 + 4.2.1/4.2.2 数据结构 + 4.4/4.5 流程扩展字段） |
| data-collector.yaml | data-collector | `/api/v1/data` | ✅ 已定义（7 个业务端点 + 5.3.3 遥测结构 + 5.4 预处理 / 5.5 上传流程扩展字段） |
| data-analytics.yaml | data-analytics | `/api/v1/analytics` | 待开发 |
| ota-service.yaml | ota-service | `/api/v1/ota` | 待开发 |
| remote-control.yaml | remote-control | `/api/v1/remote`、`/ws/remote/**` | 待开发 |

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

## 校验命令

```bash
# 网关契约 ↔ 实现 ↔ K8s 清单三方一致性（29 项，无需运行服务）
pytest services/api-gateway/app/tests/test_openapi_contract.py -q

# 场景服务契约 ↔ 设计文档 4 章/12.2 节 ↔ DB DDL ↔ Kafka 契约 ↔ K8s 清单（29 项）
cd services/scene-service && pytest app/tests/test_scene_contract.py -q

# 数据采集契约 ↔ 设计文档 5 章推导清单 ↔ DDL ↔ Kafka 契约（含 sensor_file Schema）↔ K8s 清单（30 项）
cd services/data-collector && pytest app/tests/test_data_collector_contract.py -q
```

> 命名约定：各服务契约测试文件使用唯一文件名（如 `test_scene_contract.py`），
> 避免 monorepo 内 `app/tests/<同名文件>` 在 pytest importlib 模式下模块名冲突。
