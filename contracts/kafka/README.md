# contracts/kafka — Kafka Topic 契约

Topic 命名规范：`<domain>.<entity>.<type>`，全小写，点分隔；车端 Topic 为 `hunter.{vehicle_id}.<type>`；
禁止自行新增 Topic（含 DLQ：统一按 `{original_topic}.dlq` 派生，见 `hunter_common.kafka.consumer`）。

## 文件清单

| 文件 | 内容 |
|------|------|
| `topics.yaml` | Topic 清单（车端 9 个 / 平台内部 6 个）：分区数、副本数、acks、保留时间、key 策略、生产者/消费者、Schema 引用；含全局 producer/consumer 默认参数与 Topic 级限流 |
| `consumer-groups.yaml` | 消费者组契约：12 个组（`data-collector-*` / `data-analytics-*` / `ota-service-*` / `remote-control-*` / `scene-service-*` / `platform-alert-event`），含手动提交、DLQ、幂等键、延迟目标 |
| `schemas/*.schema.json` | 9 个消息 JSON Schema（draft-07），详见 `schemas/README.md` |

## 车端 ↔ 平台 Topic

| Topic | 分区 | acks | 频率 | 保留 | Schema |
|-------|------|------|------|------|--------|
| `hunter.{vehicle_id}.telemetry` | 6（vehicle_id 哈希） | 1 | 10-50Hz | 7 天 | telemetry |
| `hunter.{vehicle_id}.event` | 3 | all | 事件触发 | 30 天 | event |
| `hunter.{vehicle_id}.health` | 3 | 0 | 1Hz | 7 天 | health |
| `hunter.{vehicle_id}.command` | 3 | all | 按需 | 7 天 | command |
| `hunter.{vehicle_id}.command_result` | 3 | all | 按需 | 7 天 | command_result |
| `hunter.{vehicle_id}.ota_notify` | 3 | all | 按需 | 7 天 | ota_notify |
| `hunter.{vehicle_id}.ota_status` | 3 | all | 状态变更 | 30 天 | ota_status |
| `hunter.{vehicle_id}.remote_control` | 3 | all | 20Hz | 7 天 | remote_control |
| `hunter.broadcast.command` | 3 | all | 按需 | 7 天 | command（target_filter） |

## 平台内部 Topic

| Topic | 分区 | 保留 | 生产者 | 消费者 |
|-------|------|------|--------|--------|
| `telemetry_raw` | 12 | 7 天 | data-collector | data-analytics(Flink) |
| `telemetry_clean` | 12 | 7 天 | data-collector | data-analytics(Flink) |
| `event_raw` | 6 | 30 天 | data-collector | data-analytics、scene-service |
| `sensor_file` | 3 | 7 天 | data-collector | data-analytics |
| `analytics_result` | 6 | 30 天 | data-analytics | scene-service、上层业务 |
| `alert_event` | 3 | 30 天 | data-analytics | 告警处理 |

## 一致性校验（必须保持同步的 5 处）

1. `contracts/kafka/topics.yaml`（本契约）
2. `contracts/kafka/schemas/*.schema.json`
3. `infra/docker/kafka/create-topics.sh`（本地 docker-compose 建 Topic）
4. `infra/k8s/jobs/kafka-init-job.yaml`（K8s 建 Topic）
5. `common/python/hunter_common/kafka/{producer,consumer}.py`（acks/压缩/手动提交/DLQ 参数）

校验命令：`python scripts/verify_data_layer.py`（含 Topic 清单四处一致性与 Schema/examples 校验）。

## 安全

- 车端接入强制 `SASL_SSL` + `SCRAM-SHA-512`，禁止 PLAINTEXT（本地开发除外，`KAFKA_SECURITY_PROTOCOL` 控制）
- 生产 `auto.create.topics.enable=false`、`min.insync.replicas=2`、`ssl.client.auth=requested`
- 消息 key = `vehicle_id`，保证单车辆消息有序；禁止在消息中携带 Token/私钥/密码

