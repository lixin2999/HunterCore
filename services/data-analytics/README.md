# data-analytics

数据分析：Flink 实时流处理、Spark 离线批处理、指标计算、Corner Case 挖掘、报告生成

- 端口：**8083**（环境变量 `API_PORT` 可覆盖）；网关前缀 `/api/v1/analytics`（不剥离前缀）
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg)
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约（单一事实来源）

`contracts/openapi/data-analytics.yaml`（OpenAPI 3.0.3）—— 开发顺序：先改契约，再改实现。

| 端点 | 说明 |
|------|------|
| `GET /api/v1/analytics/reports` | 报告列表（MinIO `hunter-reports` 前缀列举 + sidecar 元信息） |
| `GET /api/v1/analytics/reports/{report_id}` | 报告详情（含下载预签名 15 分钟 + Range） |
| `POST /api/v1/analytics/reports/generate` | 报告生成（**异步**：202 + `report_id`，轮询详情） |
| `GET /api/v1/analytics/dashboard` | 运营看板（车队/数据管道/算法/事件四块，单块降级不影响整体） |
| `GET /api/v1/analytics/perception/eval` | 感知精度评估（mAP 3D/BEV、IoU、Recall、Precision、平均定位误差） |
| `GET /api/v1/analytics/control/eval` | 控制性能评估（RMSE / 超调 / 调节时间 + 6.3.3 阈值判定） |
| `GET /api/v1/analytics/scene/coverage` | 场景覆盖率（热力图 + 未覆盖清单） |
| `GET /api/v1/analytics/corner-cases` | Corner Case 列表（5 类异常 + 异常分数） |

设计文档章节 ↔ 实现落位：

- **6.2 节实时分析**：`flink-jobs/`（5 个作业：车辆状态监控 / 异常驾驶检测 / 算法性能监控 / 碰撞风险评估 /
  数据质量监控）；消费 `telemetry_clean`（4 作业）与 `telemetry_raw`（数据质量），告警投 `alert_event`
  （等级 = `EVENT_LEVEL_BY_TYPE[alert_type]`，触发延迟 ≤ 2s），指标写 `data_analytics.algorithm_metrics`
- **6.3 节离线分析**：`spark-jobs/`（日报 / 感知-规划-控制评估 / 场景覆盖率 / 月度统计）
- **6.4 节 Corner Case 挖掘**：Isolation Forest + DBSCAN（每周），逐条投 `analytics_result` 供
  scene-service 实车场景自动提取（事件前后各 10 秒）
- **6.5 节报告**：5 类模板 × (HTML / PDF / JSON)，产物落 MinIO `hunter-reports`（永久保留）

数据访问边界（`contracts/database/er.md` 第 3 节）：只读例外仅 `data_collector.vehicle_telemetry`
（`hunter_analytics_ro` 账号）；events / scenes / vehicles 一律经服务 REST；不新增 DB 表与 Redis 键模式。

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/data-analytics[dev]"
cd services/data-analytics
uvicorn app.main:app --reload --port 8083
```

## 测试

```bash
cd services/data-analytics && pytest -q
# 契约校验（32 项，无需运行服务）
cd services/data-analytics && pytest app/tests/test_data_analytics_contract.py -q
```

> 实现进度：契约（Step 1）已就绪；Step 2 起按 Pydantic Schema → ORM → services → routers → Kafka → tests 分层实现。

