#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— Kafka 平台内部 Topic 初始化（init-kafka.sh）
#
# 用途：等待 broker 就绪后，按 contracts/kafka/topics.yaml 创建 6 个平台内部 Topic
#       （分区数/保留时间严格对齐契约，禁止新增未定义 Topic），并校验创建结果。
#
#   Topic            分区  保留     用途
#   telemetry_raw     12   7 天     原始遥测（data-collector 生产）
#   telemetry_clean   12   7 天     清洗后遥测
#   event_raw          6   30 天    事件数据
#   sensor_file        3   7 天     传感器文件通知
#   analytics_result   6   30 天    分析结果
#   alert_event        3   30 天    告警事件
#
# ⚠ 车端 Topic（hunter.{vehicle_id}.telemetry/event/health/...）在车辆注册时按契约创建，
#   不在本脚本范围内；副本数单机为 1（生产集群为 3，min.insync.replicas=2）。
# ⚠ 内部监听为 SASL_PLAINTEXT（.env: KAFKA_INTERNAL_SECURITY_PROTOCOL）时，脚本自动附带
#   --command-config（与 infra/k8s/jobs/kafka-init-job.yaml 同策略）。
#
# 用法：
#   sudo bash scripts/init-kafka.sh [选项]
#
# 参数：
#   --help              显示本帮助
#   --env <file>        指定 .env 路径
#   --scram-users       额外创建/更新车端 SCRAM 账号（hunter-client / hunter-vehicle）
#                       ⚠ 前置条件：broker 已存在可用的管理员 SCRAM 凭据（KAFKA_SASL_USER）
#
# 示例：
#   sudo bash /opt/hunter-core/scripts/init-kafka.sh
#   sudo bash /opt/hunter-core/scripts/init-kafka.sh --scram-users
#
# 幂等：--create --if-not-exists（已存在 Topic 不报错、不修改配置）。
# 依赖：common.sh（同目录）、容器 hunter-kafka
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # KAFKA_* 由 .env 注入

ENV_FILE_ARG=""
CREATE_SCRAM_USERS=0
# Topic 定义（名称:分区数:保留毫秒）—— 单一事实来源：contracts/kafka/topics.yaml
TOPIC_SPECS=(
  "telemetry_raw:12:604800000"      # 7 天
  "telemetry_clean:12:604800000"    # 7 天
  "event_raw:6:2592000000"          # 30 天
  "sensor_file:3:604800000"         # 7 天
  "analytics_result:6:2592000000"   # 30 天
  "alert_event:3:2592000000"        # 30 天
)

usage() {
  cat <<'EOF'
HunterCore Kafka 内部 Topic 初始化脚本

用途：
  等待 broker 就绪后创建 6 个平台内部 Topic（分区数与保留时间严格对齐 Kafka 契约）并校验：
    telemetry_raw(12,7d) telemetry_clean(12,7d) event_raw(6,30d)
    sensor_file(3,7d) analytics_result(6,30d) alert_event(3,30d)

用法：
  sudo bash scripts/init-kafka.sh [选项]

参数：
  --help          显示本帮助
  --env <file>    .env 路径（默认 /opt/hunter-core/.env）
  --scram-users   额外创建/更新车端 SCRAM 账号（需 broker 已有可用管理员凭据）

示例：
  sudo bash /opt/hunter-core/scripts/init-kafka.sh
  sudo bash /opt/hunter-core/scripts/init-kafka.sh --scram-users

校验：
  docker exec hunter-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
  （内部监听为 SASL_PLAINTEXT 时需附带 --command-config，详见脚本输出）
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
        shift 2
        ;;
      --scram-users)
        CREATE_SCRAM_USERS=1
        shift
        ;;
      *)
        log_error "未知参数：$1"
        usage >&2
        exit 2
        ;;
    esac
  done
}

# create_topic <name> <partitions> <retention_ms> <rf>
create_topic() {
  local name="$1" partitions="$2" retention="$3" rf="$4"
  if kafka_topics_cli --list 2>/dev/null | grep -Fxq "$name"; then
    log_info "Topic 已存在，跳过（幂等）：${name}"
    return 0
  fi
  if kafka_topics_cli --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor "$rf" \
    --config "retention.ms=${retention}"; then
    log_success "已创建 Topic：${name}（partitions=${partitions}, retention=${retention}ms, rf=${rf}）"
    return 0
  fi
  log_error "创建 Topic 失败：${name}"
  log_error "排查建议：① 内部监听认证（KAFKA_INTERNAL_SECURITY_PROTOCOL=${KAFKA_INTERNAL_SECURITY_PROTOCOL:-PLAINTEXT}）；② docker logs ${C_KAFKA} --tail=100；③ 分区数不可少于既有分区"
  return 1
}

# verify_topic <name> <partitions> <retention_ms>：核对分区数与 retention.ms
verify_topic() {
  local name="$1" expect_partitions="$2" expect_retention="$3" desc actual_partitions actual_retention
  desc="$(kafka_topics_cli --describe --topic "$name" 2>/dev/null || true)"
  if [ -z "$desc" ]; then
    log_error "Topic 校验失败（不存在）：${name}"
    return 1
  fi
  actual_partitions="$(printf '%s\n' "$desc" | awk '/PartitionCount/ {for (i=1;i<=NF;i++) if ($i ~ /^PartitionCount:/) {split($i,a,":"); print a[2]}}' | head -n1)"
  actual_retention="$(printf '%s\n' "$desc" | awk '/retention.ms=/ {match($0, /retention.ms=[0-9]+/); if (RSTART) {print substr($0, RSTART+13, RLENGTH-13); exit}}')"
  if [ "${actual_partitions:-0}" = "$expect_partitions" ] && [ "${actual_retention:-0}" = "$expect_retention" ]; then
    log_info "校验通过：${name}（partitions=${actual_partitions}, retention.ms=${actual_retention}）"
    return 0
  fi
  log_warn "Topic 配置与契约不一致：${name}（实际 partitions=${actual_partitions:-N/A}, retention.ms=${actual_retention:-N/A}；期望 ${expect_partitions}/${expect_retention}）"
  log_warn "如需对齐：kafka-configs.sh --alter --entity-type topics --entity-name ${name} --add-config retention.ms=${expect_retention}（分区数不可减少）"
  return 1
}

# create_scram_user <username> <password>：创建/更新 SCRAM-SHA-512 账号
create_scram_user() {
  local username="$1" password="$2"
  if [ -z "$username" ] || [ -z "$password" ]; then
    log_warn "SCRAM 账号参数为空，跳过（请检查 .env 的 KAFKA_SASL_* 变量）"
    return 0
  fi
  if kafka_tool_cli kafka-configs.sh --alter --entity-type users --entity-name "$username" \
    --add-config "SCRAM-SHA-512=[password=${password}]"; then
    log_success "已创建/更新 SCRAM 账号：${username}（密文不落日志）"
    return 0
  fi
  log_error "创建 SCRAM 账号失败：${username}"
  log_error "前置条件：broker 需存在可用的管理员凭据（KAFKA_SASL_USER）或 PLAINTEXT 内部监听；"
  log_error "首次 SCRAM 账号通常由 compose 的 broker 配置（listener.name.*.scram-sha-512.sasl.jaas.config）预置"
  return 1
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file rf spec name partitions retention created=0 verified=0 failed=0
  parse_args "$@"
  hc_log_begin

  if ! command_exists docker; then
    log_error "缺少 docker 命令：本脚本通过 docker exec 操作 Kafka 容器"
    return 1
  fi
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE
  rf="${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}"
  if [ "$rf" -gt 1 ]; then
    log_warn "KAFKA_DEFAULT_REPLICATION_FACTOR=${rf}：单机单 broker 无法满足，请确认 broker 数量（生产集群 3）"
  fi

  # ---------- 1) 等待 broker 就绪（最多 60s） ----------
  if ! wait_for "kafka_broker_ready ${C_KAFKA}" "Kafka broker（容器 ${C_KAFKA}）" 60; then
    log_error "Kafka broker 未就绪：docker logs ${C_KAFKA} --tail=100"
    return 1
  fi

  # ---------- 2) 创建 6 个内部 Topic（幂等） ----------
  log_info "===== 创建平台内部 Topic（契约：contracts/kafka/topics.yaml）====="
  for spec in "${TOPIC_SPECS[@]}"; do
    name="${spec%%:*}"
    partitions="$(printf '%s' "$spec" | cut -d: -f2)"
    retention="$(printf '%s' "$spec" | cut -d: -f3)"
    if create_topic "$name" "$partitions" "$retention" "$rf"; then
      created=$((created + 1))
    else
      failed=$((failed + 1))
    fi
  done

  # ---------- 3) 校验（分区数与保留时间） ----------
  log_info "===== 校验 Topic 配置 ====="
  for spec in "${TOPIC_SPECS[@]}"; do
    name="${spec%%:*}"
    partitions="$(printf '%s' "$spec" | cut -d: -f2)"
    retention="$(printf '%s' "$spec" | cut -d: -f3)"
    if verify_topic "$name" "$partitions" "$retention"; then
      verified=$((verified + 1))
    else
      failed=$((failed + 1))
    fi
  done

  # ---------- 4) 可选：车端 SCRAM 账号 ----------
  if [ "$CREATE_SCRAM_USERS" -eq 1 ]; then
    log_info "===== 创建/更新车端 SCRAM 账号（--scram-users）====="
    create_scram_user "${KAFKA_SASL_USER:-}" "${KAFKA_SASL_PASSWORD:-}" || failed=$((failed + 1))
    create_scram_user "${KAFKA_SASL_VEHICLE_USER:-}" "${KAFKA_SASL_VEHICLE_PASSWORD:-}" || failed=$((failed + 1))
  else
    log_info "未指定 --scram-users：跳过 SCRAM 账号创建（车端接入前需确保账号已存在）"
  fi

  # ---------- 5) 汇总 ----------
  log_info "===== 当前 Topic 列表 ====="
  kafka_topics_cli --list 2>/dev/null | sed 's/^/  /' || true
  local total
  total="$(kafka_topic_count)"
  log_info "Topic 总数：${total}（含契约 6 个内部 Topic 与系统 Topic __consumer_offsets）"

  if [ "$failed" -ne 0 ]; then
    log_error "Kafka 初始化存在 ${failed} 项失败（成功处理 ${created} 项 / 校验通过 ${verified} 项）"
    return 1
  fi
  log_success "Kafka 内部 Topic 初始化完成（6 个 Topic 已就绪且配置与契约一致）"
  log_warn "车端 Topic（hunter.{vehicle_id}.*）在车辆注册时按契约创建；车端接入需 SASL_SSL 9093 与 SCRAM 账号"
  return 0
}

main "$@"