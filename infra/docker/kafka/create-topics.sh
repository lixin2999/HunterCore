#!/usr/bin/env bash
# =====================================================================
# HunterCore Kafka 平台内部 Topic 初始化（严格对齐 contracts/kafka 契约）
# 由 docker-compose 的 kafka-init 一次性任务执行：bash /create-topics.sh
# 禁止新增契约之外的 Topic；车端 Topic（hunter.{vehicle_id}.*）在车辆注册时按契约创建
# ⚠ 死信 Topic（{topic}.dlq）必须在此显式创建：broker 关 auto.create.topics.enable，
#   消费侧转投 DLQ 时若 Topic 不存在会报 _UNKNOWN_TOPIC（不可重试）→ 非法消息被静默丢弃
#   命名/保留期/分区继承规则见 contracts/kafka/topics.yaml#naming
# =====================================================================
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_TOPICS="/opt/bitnami/kafka/bin/kafka-topics.sh"

# 等待 broker 就绪（最多 60s）
for i in $(seq 1 30); do
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  echo "[kafka-init] waiting for broker... ($i/30)"
  sleep 2
done

# create_topic <name> <partitions> <retention_ms>
create_topic() {
  local name="$1" partitions="$2" retention="$3"
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list | grep -Fxq "$name"; then
    echo "[kafka-init] topic exists, skip: $name"
    return 0
  fi
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=${retention}"
  echo "[kafka-init] topic created: $name (partitions=$partitions, retention=${retention}ms)"
}

# ---- 平台内部 Topic（分区数/保留时间见设计文档与 contracts/kafka/README.md） ----
create_topic "telemetry_raw"     12 604800000    # 原始遥测，保留 7 天
create_topic "telemetry_clean"   12 604800000    # 清洗后遥测，保留 7 天
create_topic "event_raw"          6 2592000000   # 事件数据，保留 30 天
create_topic "sensor_file"        3 604800000    # 传感器文件通知，保留 7 天
create_topic "analytics_result"   6 2592000000   # 分析结果，保留 30 天
create_topic "alert_event"        3 2592000000   # 告警事件，保留 30 天
create_topic "algorithm_metrics"  6 604800000    # 算法指标（Flink 产出→data-analytics 落库），保留 7 天

# ---- 平台内部 Topic 的死信队列（分区继承源 Topic，保留 30 天，契约 naming.dlq_retention_ms） ----
# 消费方包括 data-analytics（telemetry_raw/clean、event_raw、sensor_file）、scene-service
# （analytics_result）、告警链路（alert_event）；data-collector 自身的 produces 也可被下游 DLQ 化。
DLQ_RETENTION_MS=2592000000
create_topic "telemetry_raw.dlq"      12 "$DLQ_RETENTION_MS"
create_topic "telemetry_clean.dlq"    12 "$DLQ_RETENTION_MS"
create_topic "event_raw.dlq"           6 "$DLQ_RETENTION_MS"
create_topic "sensor_file.dlq"         3 "$DLQ_RETENTION_MS"
create_topic "analytics_result.dlq"    6 "$DLQ_RETENTION_MS"
create_topic "alert_event.dlq"         3 "$DLQ_RETENTION_MS"
create_topic "algorithm_metrics.dlq"    6 "$DLQ_RETENTION_MS"

echo "[kafka-init] platform topics ready:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
