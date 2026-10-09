# contracts/kafka — Kafka Topic 契约

Topic 命名规范：`<domain>.<entity>.<type>`，全小写，点分隔；车端 Topic 为 `hunter.{vehicle_id}.<type>`；
禁止自行新增 Topic（含 DLQ：统一按 `{original_topic}.dlq` 派生，见 `hunter_common.kafka.consumer`）。

## 文件清单

| 文件 | 内容 |
|------|------|
| `topics.yaml` | Topic 清单（车端 9 个 / 平台内部 6 个）：分区数、副本数、acks、保留时间、key 策略、生产者/消费者、Schema 引用；含全局 producer/consumer 默认参数与 Topic 级限流 |
| `consumer-groups.yaml` | 消费者组契约：12 个组（`data-collector-*` / `data-analytics-*` / `ota-service-*` / `remote-control-*` / `scene-service-*` / `platform-alert-event`），含手动提交、DLQ、幂等键、延迟目标 |
| `schemas/*.schema.json` | 11 个消息 JSON Schema（draft-07），详见 `schemas/README.md` |

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
| `alert_event` | 3 | 30 天 | data-analytics | 告警处理（落位待定） |

## data-analytics 消费组（6.2 节实时作业输入，⚠ 契约修正已登记）

| 消费组 | 订阅 Topic | 作业 | 输出 |
|--------|-----------|------|------|
| `data-analytics-telemetry` | `telemetry_clean` | 车辆状态监控 / 异常驾驶检测 / 碰撞风险评估 / 算法性能监控 | `alert_event` + `analytics_result` + `algorithm_metrics` |
| `data-analytics-telemetry-raw` | `telemetry_raw` | 数据质量监控 | `analytics_result`（指标） |
| `data-analytics-events` | `event_raw` | 事件统计 / Corner Case 挖掘 | `analytics_result` |
| `data-analytics-sensor-file` | `sensor_file` | 感知精度评估输入 | `analytics_result` |

> 原 `data-analytics-telemetry` 订阅 `telemetry_raw` 且声明生产 `telemetry_clean`（与 topics.yaml 冲突）、
> 原 `data-analytics-telemetry-clean` 与之重复消费，均由 data-analytics 契约 Step 1 归并修正
> （见 `contracts/openapi/data-analytics.yaml` 的 `x-hunter-pending-confirmation` #12）。

## 一致性校验（必须保持同步的 5 处）

1. `contracts/kafka/topics.yaml`（本契约）
2. `contracts/kafka/schemas/*.schema.json`
3. `infra/docker/kafka/create-topics.sh`（本地 docker-compose 建 Topic）
4. `infra/k8s/jobs/kafka-init-job.yaml`（K8s 建 Topic）
5. `common/python/hunter_common/kafka/`（实现侧：`contracts.py` 契约加载 · `messages.py` 编解码与 Schema 校验 ·
   `producer.py` 契约 acks/重试/本地磁盘缓冲 · `consumer.py` 手动提交/Schema 校验→DLQ/重试/积压指标 ·
   `buffer.py` 磁盘缓冲 · `idempotency.py` 幂等守卫 · `metrics.py` 指标）

校验命令：`python scripts/verify_data_layer.py`（含 Topic 清单一致性与 Schema/examples 校验；死信 Topic 创建责任五方比对：topics.yaml naming ↔ create-topics.sh ↔ kafka-init Job ↔ init-kafka.sh ↔ provisioning 派生）。

## 实现侧语义登记（本契约未显式定义、由共享库统一的约定）

| 项 | 约定 | 实现位置 |
|----|------|---------|
| 生产 acks | 按 Topic 契约 `acks` 选择生产者实例（telemetry=1 / health=0 / 其余=all）；契约外 Topic（如 DLQ）回退 `KAFKA_PRODUCER_ACKS` 并告警 | `producer.KafkaProducerManager._acks_for` |
| 消息 key | 契约 `key: vehicle_id` 的 Topic 强制 key == 消息体 `vehicle_id`；`key: none`（广播）禁止携带 key | `messages.build_record` |
| 生产重试 | `retries=3` + 指数退避（`KAFKA_PRODUCE_RETRY_BACKOFF_MS` × 2^(n-1)，上限 `KAFKA_PRODUCE_RETRY_BACKOFF_MAX_MS`）；仅对 broker 侧 `retriable` 错误重试 | `producer.KafkaProducerManager.produce` |
| 本地磁盘缓冲 | 重试耗尽且为可重试错误时落盘（上限 `producer_defaults.local_disk_buffer_bytes`=1GB）；超限按整段 FIFO 淘汰最旧并告警；重启后保留；`replay_buffered()` 最旧优先重投，失败即停 | `buffer.LocalDiskBuffer` |
| 消费 Schema 校验 | `schema_name="auto"` 按消息实际 Topic 解析 Schema；非法消息直接 DLQ（`reason=schema_invalid`，不重试）；契约外 Topic 告警后放行 | `consumer.KafkaConsumerManager._decode_and_validate` |
| 消费重试 | handler 异常按 `KAFKA_CONSUMER_MAX_ATTEMPTS` 指数退避重试，耗尽转 DLQ（`reason=handler_error`） | `consumer.KafkaConsumerManager._invoke_handler` |
| DLQ 消息头 | `dlq.original.topic` / `dlq.partition` / `dlq.offset` / `dlq.reason` / `dlq.error`（错误信息截断 500 字符） | `consumer.KafkaConsumerManager._send_to_dlq` |
| **DLQ Topic 必须显式创建** | broker 关 `auto.create.topics.enable`，派生名 `{topic}.dlq` **不会自动存在**。创建集合 = 全部平台内部 Topic（`create-topics.sh` / `init-kafka.sh`）**+ 每车 8 个车端 Topic**（车辆 provisioning `kafka_admin`），分区继承源 Topic、保留取 `naming.dlq_retention_ms`。缺失时转投报 `_UNKNOWN_TOPIC`（**属不可重试错误**），非法消息会随 offset 提交被静默丢弃——“DLQ 开关开着但死信无处可送”比不开关更危险 | `infra/docker/kafka/create-topics.sh` · `scripts/init-kafka.sh` · `services/vehicle-service/app/services/kafka_admin.py`（三方由 `verify_data_layer.py` 比对） |
| DLQ 转投失败不丢消息 | 转投失败（Topic 缺失/生产异常）→ **落盘缓冲**（与普通投递不同：不可重试错误也缓冲，仅“消息体本身非法/超长”除外），DLQ Topic 建齐后由 `replay_buffered()` 补投（**在生产者 `close()` 里触发、无定时轮询**：运维上表现为“重启该服务即补投”）；落盘时记 `kafka_dlq_message_buffered`（**不计** `hunter_kafka_dlq_failed_total`，消息未丢），仅本地缓冲也不可用时才记 `kafka_produce_buffer_unavailable` + `kafka_dlq_produce_failed` 并计入失败指标。**不得靠“照常提交 offset”把丢弃当默认行为** | `producer.KafkaProducerManager.produce(buffer_on_error=True)` · `consumer._send_to_dlq` |
| Schema 不符必须留字段明细 | `KafkaMessageSchemaError.details["errors"]`（前 5 条 `字段路径: 说明`）**必须进日志**——只有 `[2001] 消息不符合契约 Schema` 无法定位，运维只能猜（历史故障：车端与契约不一致却无法归因） | `contracts.KafkaContract.validate_message` · `consumer._handle_message` |
| 消费幂等 | `IdempotencyGuard.claim()` 返回 True 才执行 handler；键由各消费组 `idempotency_key` 派生（`compose()` 归一为 sha256） | `idempotency.IdempotencyGuard` |
| 消费积压指标 | 批次提交后刷新 `hunter_kafka_consumer_lag`（高水位 - 位点，按分区） | `consumer.KafkaConsumerManager._refresh_lag` |
| 订阅写法→正则 | `consumer-groups.yaml` 的 `subscribes: ["hunter.*.telemetry"]` 是**契约写法**（`*` = vehicle_id）。librdkafka 仅把**以 `^` 开头**的订阅项当正则，否则视为字面 Topic 名，所以实现侧必须转正则下发：`*` / `{vehicle_id}` → `[^.]+`（单层，不跨越类型段）并加 `^…$` 锚点；已以 `^` 开头的入参原样下发（不二次转义）。服务**不得自行拼正则**（历史例外：ota-service / remote-control 直接配了 `^…$`，仍可正常工作） | `contracts.subscription_term` · `consumer.KafkaConsumerManager.subscription_terms` |
| 消费者不建 Topic | 消费者侧 `allow.auto.create.topics=false`（与 broker 一致）：写错的订阅名不会在 broker 上落一个同名空 Topic（排障时“Topic 存在”会误导）；正则订阅下新开通车辆的 Topic 靠元数据刷新进入订阅集合（30s，默认 300s 会让“已开通但无数据”持续 5 分钟） | `consumer.KafkaConsumerManager._build_conf` |
| 契约目录定位 | `KAFKA_CONTRACT_DIR` → 工作目录向上查找 → 包位置向上查找；显式启用 Schema 校验时契约缺失即启动失败（fail fast） | `contracts.locate_contract_dir` |


## 安全

- 车端接入强制 `SASL_SSL` + `SCRAM-SHA-512`，禁止 PLAINTEXT（本地开发除外，`KAFKA_SECURITY_PROTOCOL` 控制）
- 生产 `auto.create.topics.enable=false`、`min.insync.replicas=2`、`ssl.client.auth=required`（严格 mTLS，设计文档 3.2.3/14.2：客户端证书经 `KAFKA_SSL_CERTFILE/KEYFILE` 配置，车端证书由 `gen-kafka-certs.sh` 签发）
- 消息 key = `vehicle_id`，保证单车辆消息有序；禁止在消息中携带 Token/私钥/密码

