# api-gateway

API 网关：统一接入、JWT 认证鉴权、五级限流熔断、路由转发、日志审计

- 端口：**8080**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约

`contracts/openapi/api-gateway.yaml`（OpenAPI 3.0.3）是网关的单一事实来源：

- 自持端点：`POST /api/v1/user/login|refresh|logout`、`GET /api/v1/user/me`（统一认证）；`/healthz`、`/readyz`、`/metrics`（运维探针）
- 转发端点：根级 `x-hunter-gateway-routes` 声明 7 个路由前缀 → 目标服务与端口（各服务资源端点见其自身契约）
- 系统契约：`x-hunter-rate-limits`（附录 D 五级限流）、`x-hunter-websocket-routes`（`/ws/remote/**`）、`x-hunter-audit-log`、`x-hunter-kafka`（网关不参与 Kafka）、`x-hunter-error-status-map`

> 修改接口前必须先更新契约（含 ⚠ `pending_confirmation` 项需人工确认后再实现），再改代码。

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/api-gateway[dev]"
cd services/api-gateway
uvicorn app.main:app --reload --port 8080
```

## 测试

```bash
cd services/api-gateway && pytest -q
# 仓库根目录亦可：pytest services/api-gateway -q
```

- `app/tests/test_health.py`：探针 / 统一响应 / trace_id / 指标端点
- `app/tests/test_openapi_contract.py`：契约 ↔ 实现 ↔ K8s 清单三方一致性（29 项）

> 骨架层级（L0）+ 契约层级（L3 Step 1）：认证、限流与转发逻辑按契约逐步实现。
