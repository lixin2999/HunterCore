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
cd services/data-collector && pytest -q                                  # 服务测试 114 项
python scripts/verify_data_layer.py                                      # 数据层/Kafka 契约校验
python scripts/generate_contracts_configmap.py --check                   # 运行时契约 ConfigMap 一致性
```

## 采集链路（审查 R1 后已落地）

| 层 | 组件 | 说明 |
|----|------|------|
| 消费者 | `app/consumers/{base,telemetry,health,events}.py` | 消费组/订阅/幂等键对齐 `consumer-groups.yaml`；`schema_name="auto"` 契约校验 + 手动提交 + DLQ |
| 预处理 | `app/services/ingest.py` | 5.4 节 6 步流水线；批缓冲由 `KafkaConsumerManager.on_batch_end` 驱动（**写库成功才提交 offset**） |
| 入库 | `app/repositories/{telemetry,events}.py` | 单条多值 `INSERT ... ON CONFLICT ... DO NOTHING`（≥ 10000 点/秒，重放幂等） |
| 读模型 | `app/services/vehicle_status.py` | `vehicle:status:{id}` / `vehicle:online:set` 唯一写方 + 心跳超阈值离线守护 |
| 投递 | `app/producers/pipeline.py` | `telemetry_raw` / `telemetry_clean` / `event_raw`，一律经契约校验入口 `publish_payload` |

关键配置（K8s ConfigMap 已接线）：`TELEMETRY_BATCH_SIZE`、`TELEMETRY_PUBLISH_CONCURRENCY`、
`TELEMETRY/EVENT/HEALTH_CONSUMER_ENABLED`、`VEHICLE_OFFLINE_THRESHOLD_SECONDS`、
`INGEST_SCHEMA_VALIDATION_ENABLED`、`KAFKA_CONTRACT_DIR`（契约 ConfigMap 挂载点）。

## 分层状态

- **L0（已完成）**：应用骨架（main/config/routers-health/core-error_handlers）+ 健康探针与指标
- **L1（已完成）**：接口契约（`data-collector.yaml` + `sensor_file.schema.json` + 契约测试 30 条）
- **L2（已完成）**：Schema/模型/Repository/Services/Routers
- **L3（本次完成）**：Kafka 采集链路（三路消费者 + 6 步预处理 + 批量入库 + raw/clean 投递 +
  车辆读模型与离线守护）+ 文件上传真实内容摘要校验（SHA-256/MD5）+ 业务层单测 62 条
- **下一步**：Flink 实时作业（`flink-jobs/`）与 Spark 离线作业（`spark-jobs/`）落地

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

