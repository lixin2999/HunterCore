# ota-service

OTA 管理：版本仓库管理、升级任务调度、灰度发布、升级监控、A/B 分区回滚

- 端口：**8084**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/ota-service[dev]"
cd services/ota-service
uvicorn app.main:app --reload --port 8084
```

## 测试

```bash
cd services/ota-service && pytest -q
```

> 骨架层级（L0）：业务逻辑按 `contracts/` 契约逐层实现。
