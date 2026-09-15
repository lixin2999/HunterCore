# api-gateway

API 网关：统一接入、JWT 认证鉴权、五级限流熔断、路由转发、日志审计

- 端口：**8080**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

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
```

> 骨架层级（L0）：业务逻辑按 `contracts/` 契约逐层实现。
