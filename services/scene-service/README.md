# scene-service

场景生成：场景库管理、场景编辑、场景参数化、OpenSCENARIO 导出、实车场景自动提取

- 端口：**8081**（环境变量 `API_PORT` 可覆盖）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/scene-service[dev]"
cd services/scene-service
uvicorn app.main:app --reload --port 8081
```

## 测试

```bash
cd services/scene-service && pytest -q
```

> 骨架层级（L0）：业务逻辑按 `contracts/` 契约逐层实现。
