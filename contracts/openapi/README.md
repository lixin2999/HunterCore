# contracts/openapi — REST API 契约（OpenAPI 3.0）

单一事实来源：所有微服务 REST 接口先在此定义 OpenAPI 3.0 YAML，再进行实现。

## 文件清单

| 文件 | 服务 | 路由前缀 | 状态 |
|------|------|----------|------|
| api-gateway.yaml | api-gateway | 统一入口（8080） | ✅ 已定义（认证 + 运维探针 + 路由表/限流/WebSocket 扩展字段） |
| scene-service.yaml | scene-service | `/api/v1/scene` | 待开发 |
| data-collector.yaml | data-collector | `/api/v1/data` | 待开发 |
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

## 校验命令

```bash
# 网关契约 ↔ 实现 ↔ K8s 清单三方一致性（29 项，无需运行服务）
pytest services/api-gateway/app/tests/test_openapi_contract.py -q
```
