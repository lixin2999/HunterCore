# data-collector

数据采集：Kafka 消息消费接入、数据预处理、数据路由、文件上传管理

- 端口：**8082**（环境变量 `API_PORT` 可覆盖），网关前缀 `/api/v1/data`
- 技术栈：Python 3.11+ / FastAPI / pydantic-settings / SQLAlchemy 2.0 (asyncpg) / confluent-kafka / MinIO
- 健康探针：`GET /healthz`（存活）、`GET /readyz`（就绪，含 DB/Redis 检查）
- 指标端点：`GET /metrics`（Prometheus 文本格式，由 `infra/monitoring` 抓取）

## 接口契约（单一事实来源）

| 契约 | 内容 |
|------|------|
| `contracts/openapi/data-collector.yaml` | 7 个业务端点 + 3 个运维端点；5.3.3 遥测结构、5.4 预处理流程、5.5 上传流程、限流 / Redis / Kafka 参与度扩展字段 |
| `contracts/kafka/topics.yaml` | 消费 `hunter.{vehicle_id}.{telemetry,event,health,command_result}`；生产 `telemetry_raw` / `telemetry_clean` / `event_raw` / `sensor_file` |
| `contracts/kafka/consumer-groups.yaml` | `data-collector-telemetry` / `-events` / `-health` / `-command-result`（正则订阅、手动提交、DLQ、幂等键） |
| `contracts/kafka/schemas/sensor_file.schema.json` | 5.5 节上传完成通知（本模块补全；bucket/object_key 命名规范/md5/sha256） |
| `contracts/database/ddl/04_events.sql`、`05_timeseries.sql` | 写 `data_collector.vehicle_telemetry`（批量 + `ON CONFLICT DO NOTHING`）、读写 `data_collector.events` |

业务端点：`GET /api/v1/data/telemetry`、`GET /api/v1/data/events`、`GET /api/v1/data/events/{event_id}`、
`POST /api/v1/data/events/{event_id}/acknowledge`、`GET /api/v1/data/files`、
`POST /api/v1/data/files/presign`、`POST /api/v1/data/files/complete`。

关键阈值（不可放宽）：遥测入库延迟 ≤1s、时序写入 ≥10000 点/秒、单车遥测 ≤100 msg/s、
文件上传 ≤10 Mbps、`GET /api/v1/data/telemetry` 单用户 20 QPS、上传预签名 1h / 下载预签名 15min。

## 本地运行

```bash
pip install -e "common/python"     # 仓库根目录：共享库
pip install -e "services/data-collector[dev]"
cd services/data-collector
uvicorn app.main:app --reload --port 8082
```

## 测试

```bash
cd services/data-collector && pytest -q                                  # 服务测试（健康探针 4 + 契约 30）
python scripts/verify_data_layer.py                                      # 数据层/Kafka 契约校验
```

## 分层状态

- **L0（已完成）**：应用骨架（main/config/routers-health/core-error_handlers）+ 健康探针与指标
- **L1（本次完成）**：接口契约（`data-collector.yaml` + `sensor_file.schema.json` + 契约测试 30 条）
- **下一步**：Step 2 Pydantic v2 Schema + SQLAlchemy 2.0 模型（复用 `hunter_common.database.models.collector`）→ Services → Routers → Kafka 消费者/生产者 → 单元测试（覆盖率 ≥ 80%）

## ⚠ 待人工确认（契约 `x-hunter-pending-confirmation` 12 项，确认后回填契约再实现）

1. 端点清单来源（12.2 节未随仓库提供，当前按附录 D + 5.5 流程 + events 表能力推导）
2. 5.5 步骤 6「元信息入库」无对应 DB 表（当前以 MinIO 列举 + `sensor_file` 通知体现）
3. `sensor_file` 字段全集（本次按 topics.yaml 描述 + 5.5 命名规范定义）
4. 文件 `data_type` 取值域与 `data_type → Bucket` 映射
5. 文件完整性校验失败错误码（复用 6001）
6. 车端 REST 接入方式（网关 / 独立 Ingress mTLS 端口）
7. 遥测查询时间跨度上限（默认 24h）与 `total` 计数成本
8. 车辆实时状态查询端点归属（Redis `vehicle:status:{vehicle_id}` 的读接口）
9. Carla Topic（`carla.{sim_id}.vil_state` / `carla.{sim_id}.sensor_data`）未登记
10. 遥测 `seq` 是否入表（影响幂等键实现）
11. 告警推送链路（`alert_event`）归属
12. 车端上传带宽限流（10 Mbps）落实方式

