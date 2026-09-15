#!/usr/bin/env bash
# =====================================================================
# HunterEdge Kafka 平台内部 Topic 初始化（严格对齐 contracts/kafka 契约）
# 由 docker-compose 的 kafka-init 一次性任务执行：bash /create-topics.sh
# 禁止新增契约之外的 Topic；车端 Topic（hunter.{vehicle_id}.*）在车辆注册时按契约创建
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

echo "[kafka-init] platform topics ready:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
