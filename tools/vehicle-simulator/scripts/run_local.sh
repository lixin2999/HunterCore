#!/usr/bin/env bash
# 一键启动本地车辆模拟器（示例）。
# 依赖：已 `pip install -e "tools/vehicle-simulator[dev]"`；compose 平台在运行。
# 凭据/地址全部经环境变量注入，禁止硬编码（系统约束第 16 条）。
set -euo pipefail

: "${VEHICLE_ID:=HUNTER-001}"
: "${KAFKA_BOOTSTRAP_SERVERS:=localhost:9093}"
: "${KAFKA_SASL_PASSWORD:?需要 KAFKA_SASL_PASSWORD（车端 SCRAM 口令）}"
: "${SERVER_IP:?需要 SERVER_IP（SRS 可达地址）}"
: "${KAFKA_SECURITY_PROTOCOL:=SASL_SSL}"
SSL_ARGS=()
[[ -n "${KAFKA_SSL_CAFILE:-}" ]] && SSL_ARGS+=(--ssl-cafile "$KAFKA_SSL_CAFILE")
[[ -n "${KAFKA_SSL_CERTFILE:-}" ]] && SSL_ARGS+=(--ssl-certfile "$KAFKA_SSL_CERTFILE")
[[ -n "${KAFKA_SSL_KEYFILE:-}" ]] && SSL_ARGS+=(--ssl-keyfile "$KAFKA_SSL_KEYFILE")

exec vehicle-simulator \
  --vehicle-id "$VEHICLE_ID" \
  --bootstrap "$KAFKA_BOOTSTRAP_SERVERS" \
  --security-protocol "$KAFKA_SECURITY_PROTOCOL" \
  --scram-pass "$KAFKA_SASL_PASSWORD" \
  --srs-rtmp "rtmp://${SERVER_IP}:1935/remote_control" \
  "${SSL_ARGS[@]}" \
  "$@"
