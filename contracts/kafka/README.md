# contracts/kafka — Kafka Topic 契约

Topic 命名规范：`<domain>.<entity>.<type>`，全小写，点分隔；禁止自行新增 Topic。

## 已定义 Topic（随开发填充 JSON Schema）

### 平台内部 Topic（本地开发由 kafka-init 创建）

| Topic | 分区 | 保留 | 生产者 | 消费者 |
|-------|------|------|--------|--------|
| telemetry_raw | 12 | 7 天 | data-collector | data-analytics(Flink) |
| telemetry_clean | 12 | 7 天 | data-collector | data-analytics(Flink) |
| event_raw | 6 | 30 天 | data-collector | data-analytics |
| sensor_file | 3 | 7 天 | data-collector | data-analytics |
| analytics_result | 6 | 30 天 | data-analytics | 上层业务 |
| alert_event | 3 | 30 天 | data-analytics | 告警处理 |

### 车端 ↔ 平台 Topic

命名模式 `hunter.{vehicle_id}.<type>`（telemetry / event / health / command / command_result / ota_notify / ota_status / remote_control）及 `hunter.broadcast.command`，配置见设计文档；消息 key = vehicle_id 保证单车辆有序，车端接入必须 SASL_SSL + SCRAM-SHA-512。

计划文件：`topics.yaml`（Topic 清单）、`schemas/*.schema.json`（消息 JSON Schema）。
