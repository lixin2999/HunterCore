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

## 接口契约（先契约后实现）

| 契约 | 内容 |
|------|------|
| `contracts/openapi/scene-service.yaml` | 10 个业务端点（设计文档 12.2 节）+ 会话/错误/分页组件 + `x-hunter-*` 机器可校验扩展字段 |
| `contracts/kafka/schemas/analytics_result.schema.json` | 唯一消费消息（实车场景自动提取，设计文档 4.5 节） |
| `contracts/database/ddl/02_scene.sql` | `scene_svc.scenes`（元信息列 + `config_json`，软删除） |

关键映射：4.2.2 场景配置结构 = `SceneMeta`（→ `scenes` 列）+ `SceneConfig`（→ `config_json`）；
状态机 `draft → published → archived`（仅 `draft` 可编辑）；Kafka 不生产任何 Topic。

```bash
cd services/scene-service
pytest app/tests/test_scene_contract.py -q     # 契约 ↔ 设计文档 ↔ DDL ↔ Kafka ↔ K8s 清单（29 项）
```

## 测试

```bash
cd services/scene-service && pytest -q
```

> 骨架层级（L0）；Step 1（接口契约）已完成，Step 2（数据模型）起按契约实现。

## ⚠ 待人工确认（契约 `x-hunter-pending-confirmation`，12 项）

`scene_type` 编码值、4.4 节第 6/7 步端点缺口、`archived` 无归档端点、导出返回形式、`version` 递增策略、
Carla 管理 API 配置键名与上限、实车提取触发源（`analytics_result` vs `event_raw`）、模板数据来源等。
