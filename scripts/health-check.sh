#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 全栈健康检查（health-check.sh）
#
# 用途：一次性核对容器 / HTTP 探针 / 数据库 / Redis / Kafka / MinIO / 磁盘与内存 / 消费积压，
#       输出 [PASS] [WARN] [FAIL] 明细与汇总，并给出退出码（供 install.sh、巡检脚本、CI 调用）。
#
# 检查项（共 10 类）：
#   ① 容器运行状态与 Docker healthcheck（缺失/非 healthy/未运行 → FAIL）
#   ② HTTP 探针：api-gateway/scene/collector/analytics/ota/remote（/healthz）、web-portal、
#      MinIO(/minio/health/live)、SRS(/api/v1/versions)、Flink UI(/overview)
#   ③ PostgreSQL：pg_isready       ④ TimescaleDB：pg_isready（存在独立实例时）
#   ⑤ Redis：redis-cli PING        ⑥ Kafka：Topic 数 ≥6
#   ⑦ MinIO：Bucket 数 ≥7          ⑧ 磁盘：/data 使用率（>85% WARN，>95% FAIL）
#   ⑨ 内存使用率（>90% WARN）      ⑩ Kafka 消费积压：data-collector-telemetry LAG（>10000 WARN）
#
# 用法：
#   sudo bash scripts/health-check.sh [选项]
#
# 参数：
#   --help             显示本帮助
#   --env <file>       指定 .env 路径
#   --lag-threshold N  消费积压告警阈值（默认 10000）
#
# 退出码：0 = 全部通过；1 = 存在告警（WARN）；2 = 存在失败（FAIL）
#
# 复用：可被 source（daily-check.sh）后调用 health_init_counters / run_all_health_checks /
#       record_result / print_health_report，不会触发主流程。
#
# 依赖：common.sh（同目录）、curl、docker、ss/df
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  # shellcheck source=./common.sh
  . "${SCRIPT_DIR}/common.sh"
fi
# shellcheck disable=SC2154  # 运行期变量由 .env 注入

ENV_FILE_ARG=""
LAG_THRESHOLD=10000
PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
RESULT_ROWS=()

# 期望运行的容器（可选容器单独列出）
EXPECTED_CONTAINERS=(
  "hunter-postgres" "hunter-redis" "hunter-zookeeper" "hunter-kafka" "hunter-minio" "hunter-srs"
  "hunter-flink-jm" "hunter-flink-tm" "hunter-api-gateway" "hunter-scene" "hunter-collector"
  "hunter-analytics" "hunter-ota" "hunter-remote" "hunter-web"
)
OPTIONAL_CONTAINERS=("hunter-timescale")

usage() {
  cat <<'EOF'
HunterCore 全栈健康检查脚本

用途：
  检查 10 类健康项并输出 [PASS]/[WARN]/[FAIL] 明细与汇总。
  退出码：0=全部通过，1=有告警，2=有失败。

用法：
  sudo bash scripts/health-check.sh [选项]

参数：
  --help             显示本帮助
  --env <file>       .env 路径（默认 /opt/hunter-edge/.env）
  --lag-threshold N  Kafka 消费积压告警阈值（默认 10000）

示例：
  sudo bash scripts/health-check.sh
  sudo bash scripts/health-check.sh --lag-threshold 5000
  bash scripts/health-check.sh && echo "全栈健康"

检查项：
  容器状态/Docker healthcheck、服务 HTTP 探针、PostgreSQL、TimescaleDB、Redis、
  Kafka Topic 数、MinIO Bucket 数、/data 磁盘使用率、内存使用率、Kafka 消费积压。
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
      --lag-threshold)
        [ $# -ge 2 ] || die "--lag-threshold 需要一个数字参数"
        case "$2" in
          '' | *[!0-9]*) die "--lag-threshold 必须为非负整数（收到：$2）" ;;
        esac
        LAG_THRESHOLD="$2"
        shift 2
        ;;
      *)
        log_error "未知参数：$1"
        usage >&2
        exit 2
        ;;
    esac
  done
}

# health_init_counters：重置计数与结果集（供 daily-check 复用）
health_init_counters() {
  PASS_COUNT=0
  WARN_COUNT=0
  FAIL_COUNT=0
  RESULT_ROWS=()
}

# health_emit <line>：输出到终端，并在 REPORT_FILE 已设置时同步追加（供 daily-check 生成本地报告）
health_emit() {
  printf '%s\n' "$1"
  if [ -n "${REPORT_FILE:-}" ]; then
    printf '%s\n' "$1" >>"$REPORT_FILE" 2>/dev/null || true
  fi
}

# record_result <PASS|WARN|FAIL> <检查项> <详情>
record_result() {
  local status="$1" item="$2" detail="${3:-}"
  local color=""
  case "$status" in
    PASS)
      PASS_COUNT=$((PASS_COUNT + 1))
      color="$C_GREEN"
      ;;
    WARN)
      WARN_COUNT=$((WARN_COUNT + 1))
      color="$C_YELLOW"
      ;;
    *)
      status="FAIL"
      FAIL_COUNT=$((FAIL_COUNT + 1))
      color="$C_RED"
      ;;
  esac
  if [ -n "${REPORT_FILE:-}" ]; then
    health_emit "[${status}] ${item} - ${detail}"
  else
    printf '%s[%s]%s %s - %s\n' "$color" "$status" "$C_RESET" "$item" "$detail"
  fi
  RESULT_ROWS+=("${status}|${item}|${detail}")
}

# health_exit_code：按计数返回退出码（0/1/2）
health_exit_code() {
  if [ "$FAIL_COUNT" -gt 0 ]; then
    return 2
  fi
  if [ "$WARN_COUNT" -gt 0 ]; then
    return 1
  fi
  return 0
}

# print_health_report：汇总（含 FAIL/WARN 明细）
print_health_report() {
  local row status item detail line
  health_emit "================= 健康检查汇总 ================="
  health_emit "$(printf '  通过 %d 项，警告 %d 项，失败 %d 项（共 %d 项）' \
    "$PASS_COUNT" "$WARN_COUNT" "$FAIL_COUNT" "$((PASS_COUNT + WARN_COUNT + FAIL_COUNT))")"
  if [ "${#RESULT_ROWS[@]}" -gt 0 ]; then
    for row in "${RESULT_ROWS[@]}"; do
      status="${row%%|*}"
      case "$status" in
        FAIL | WARN)
          item="$(printf '%s' "$row" | cut -d'|' -f2)"
          detail="$(printf '%s' "$row" | cut -d'|' -f3)"
          if [ -n "${REPORT_FILE:-}" ]; then
            health_emit "[${status}] ${item} - ${detail}"
          else
            line="$(printf '%s%s%s %s - %s' \
              "$([ "$status" = "FAIL" ] && printf '%s' "$C_RED" || printf '%s' "$C_YELLOW")" \
              "$status" "$C_RESET" "$item" "$detail")"
            printf '%s\n' "$line"
          fi
          ;;
      esac
    done
  fi
  health_emit "================================================"
}

# =====================================================================
# 各项检查
# =====================================================================
# ① 容器运行状态 + Docker healthcheck
check_containers() {
  local name state health
  if ! command_exists docker || ! docker info >/dev/null 2>&1; then
    record_result "FAIL" "容器运行状态" "Docker 守护进程不可用（systemctl status docker）"
    return 0
  fi
  for name in "${EXPECTED_CONTAINERS[@]}"; do
    if ! docker inspect "$name" >/dev/null 2>&1; then
      record_result "FAIL" "容器 ${name}" "容器不存在（未启动，或 compose 未创建）"
      continue
    fi
    state="$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || printf 'false')"
    if [ "$state" != "true" ]; then
      record_result "FAIL" "容器 ${name}" "未运行（docker logs ${name} --tail=50）"
      continue
    fi
    health="$(container_health "$name")"
    case "$health" in
      healthy) record_result "PASS" "容器 ${name}" "running / healthy" ;;
      none) record_result "WARN" "容器 ${name}" "running（未定义 healthcheck，无法判定就绪）" ;;
      *) record_result "FAIL" "容器 ${name}" "running 但健康状态为 ${health}" ;;
    esac
  done
  for name in "${OPTIONAL_CONTAINERS[@]}"; do
    if ! docker inspect "$name" >/dev/null 2>&1; then
      record_result "WARN" "容器 ${name}（可选）" "未部署（契约单实例形态：时序表与业务表同库）"
    elif container_running "$name"; then
      record_result "PASS" "容器 ${name}（可选）" "running"
    else
      record_result "FAIL" "容器 ${name}（可选）" "容器存在但未运行"
    fi
  done
  return 0
}

# ② HTTP 探针（服务端口仅绑 127.0.0.1，故从宿主机 localhost 探测）
check_http_endpoints() {
  local item name rest port path
  local -a services=(
    "api-gateway:${API_GATEWAY_PORT:-8080}:${HEALTH_PATH}"
    "scene-service:${SCENE_SERVICE_PORT:-8081}:${HEALTH_PATH}"
    "data-collector:${DATA_COLLECTOR_PORT:-8082}:${HEALTH_PATH}"
    "data-analytics:${DATA_ANALYTICS_PORT:-8083}:${HEALTH_PATH}"
    "ota-service:${OTA_SERVICE_PORT:-8084}:${HEALTH_PATH}"
    "remote-control:${REMOTE_CONTROL_PORT:-8085}:${HEALTH_PATH}"
    "web-portal:${WEB_PORT:-80}:/"
  )
  for item in "${services[@]}"; do
    name="${item%%:*}"
    rest="${item#*:}"
    port="${rest%%:*}"
    path="${rest#*:}"
    if service_health_ok "$port" "$path"; then
      record_result "PASS" "HTTP ${name}" "http://127.0.0.1:${port}${path} 正常"
    else
      record_result "FAIL" "HTTP ${name}" "http://127.0.0.1:${port}${path} 不可用（curl -sv 排查）"
    fi
  done
  if curl -sf -m 5 -o /dev/null "$(minio_health_url)"; then
    record_result "PASS" "HTTP MinIO" "$(minio_health_url)"
  else
    record_result "FAIL" "HTTP MinIO" "$(minio_health_url) 不可用"
  fi
  if curl -sf -m 5 -o /dev/null "$(srs_health_url)"; then
    record_result "PASS" "HTTP SRS" "$(srs_health_url)"
  else
    record_result "FAIL" "HTTP SRS" "$(srs_health_url) 不可用"
  fi
  if curl -sf -m 5 -o /dev/null "$(flink_ui_url)/overview"; then
    record_result "PASS" "HTTP Flink UI" "$(flink_ui_url)/overview"
  else
    record_result "WARN" "HTTP Flink UI" "$(flink_ui_url)/overview 不可用"
  fi
  return 0
}

# ③ PostgreSQL / ④ TimescaleDB
check_databases() {
  if docker inspect "hunter-postgres" >/dev/null 2>&1; then
    if docker exec "hunter-postgres" pg_isready -U "${POSTGRES_USER:-hunter}" -d "${POSTGRES_DB:-hunter_core}" >/dev/null 2>&1; then
      record_result "PASS" "PostgreSQL" "pg_isready OK（库 ${POSTGRES_DB:-hunter_core}）"
    else
      record_result "FAIL" "PostgreSQL" "pg_isready 失败（docker logs hunter-postgres --tail=50）"
    fi
  else
    record_result "FAIL" "PostgreSQL" "容器 hunter-postgres 不存在"
  fi

  if docker inspect "hunter-timescale" >/dev/null 2>&1; then
    if docker exec "hunter-timescale" pg_isready -U "${TIMESCALE_USER:-hunter}" -d "${TIMESCALE_DB:-hunter_ts}" >/dev/null 2>&1; then
      record_result "PASS" "TimescaleDB" "pg_isready OK（库 ${TIMESCALE_DB:-hunter_ts}，端口 5433）"
    else
      record_result "FAIL" "TimescaleDB" "pg_isready 失败（docker logs hunter-timescale --tail=50）"
    fi
  else
    record_result "WARN" "TimescaleDB" "未部署独立实例（契约单实例形态：hypertable 位于 hunter_core）"
  fi
  return 0
}

# ⑤ Redis
check_redis() {
  local reply
  if ! docker inspect "hunter-redis" >/dev/null 2>&1; then
    record_result "FAIL" "Redis" "容器 hunter-redis 不存在"
    return 0
  fi
  reply="$(docker exec "hunter-redis" redis-cli --no-auth-warning -a "${REDIS_PASSWORD:-}" ping 2>/dev/null || true)"
  if printf '%s' "$reply" | grep -q PONG; then
    record_result "PASS" "Redis" "PING → PONG（AOF 已启用详见 .env REDIS_APPENDONLY）"
  else
    record_result "FAIL" "Redis" "PING 失败（检查 REDIS_PASSWORD 与容器日志）"
  fi
  return 0
}

# ⑥ Kafka Topic 数
check_kafka_topics() {
  local count minimum=6
  if ! docker inspect "hunter-kafka" >/dev/null 2>&1; then
    record_result "FAIL" "Kafka" "容器 hunter-kafka 不存在"
    return 0
  fi
  count="$(kafka_topic_count)"
  if [ "${count:-0}" -lt "$minimum" ]; then
    record_result "FAIL" "Kafka Topic 数" "${count:-0} < ${minimum}（契约内部 Topic 未创建完整，见 init-kafka.sh）"
  else
    record_result "PASS" "Kafka Topic 数" "${count}（契约 ≥${minimum}，含 __consumer_offsets）"
  fi
  return 0
}

# ⑦ MinIO Bucket 数
check_minio_buckets() {
  local count minimum=7
  if ! docker inspect "hunter-minio" >/dev/null 2>&1; then
    record_result "FAIL" "MinIO" "容器 hunter-minio 不存在"
    return 0
  fi
  count="$(minio_bucket_count)"
  if [ "${count:-0}" -lt "$minimum" ]; then
    record_result "FAIL" "MinIO Bucket 数" "${count:-0} < ${minimum}（见 init-minio.sh）"
  else
    record_result "PASS" "MinIO Bucket 数" "${count}（契约 ${minimum} 个）"
  fi
  return 0
}

# ⑧ 磁盘使用率（/data；>85% WARN，>95% FAIL）
check_disk_usage() {
  local target="$DATA_DIR" usage avail
  if [ ! -d "$target" ]; then
    record_result "FAIL" "磁盘 ${target}" "目录不存在（未执行 install.sh step_3 或未挂载数据盘）"
    return 0
  fi
  usage="$(df -P "$target" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')"
  avail="$(df -Ph "$target" | awk 'NR==2 {print $4}')"
  if [ "${usage:-0}" -gt 95 ]; then
    record_result "FAIL" "磁盘 ${target}" "使用率 ${usage}%（可用 ${avail}）> 95%：立即清理或扩容，否则 Kafka/PG 写入将失败"
  elif [ "${usage:-0}" -gt 85 ]; then
    record_result "WARN" "磁盘 ${target}" "使用率 ${usage}%（可用 ${avail}）> 85%：建议清理备份/旧数据或扩容"
  else
    record_result "PASS" "磁盘 ${target}" "使用率 ${usage}%（可用 ${avail}）"
  fi
  return 0
}

# ⑨ 内存使用率（>90% WARN）
check_memory_usage() {
  local total used usage
  read -r total used <<<"$(awk '/MemTotal/{t=$2} /MemAvailable/{a=$2} END{printf "%d %d", t/1024, (t-a)/1024}' /proc/meminfo)"
  if [ "${total:-0}" -le 0 ]; then
    record_result "WARN" "内存使用率" "无法读取 /proc/meminfo"
    return 0
  fi
  usage=$((used * 100 / total))
  if [ "$usage" -gt 90 ]; then
    record_result "WARN" "内存使用率" "${usage}%（已用 ${used}MB / 共 ${total}MB）> 90%：核对容器 limits 与 JVM 堆（docs/01 §2.2）"
  else
    record_result "PASS" "内存使用率" "${usage}%（已用 ${used}MB / 共 ${total}MB）"
  fi
  return 0
}

# ⑩ Kafka 消费积压（data-collector-telemetry；>阈值 WARN）
check_kafka_lag() {
  local group="data-collector-telemetry" lag
  if ! docker inspect "hunter-kafka" >/dev/null 2>&1; then
    record_result "WARN" "Kafka 消费积压" "Kafka 容器不存在，跳过积压检查"
    return 0
  fi
  lag="$(kafka_consumer_lag "$group" 2>/dev/null || printf '%s' "-1")"
  if [ "$lag" -lt 0 ]; then
    record_result "WARN" "Kafka 消费积压" "消费组 ${group} 尚无 offset 提交（车辆未上报或 data-collector 未消费）"
  elif [ "$lag" -gt "$LAG_THRESHOLD" ]; then
    record_result "WARN" "Kafka 消费积压" "${group} LAG=${lag} > ${LAG_THRESHOLD}：检查 data-collector 消费能力与批写性能（入库 ≤1s）"
  else
    record_result "PASS" "Kafka 消费积压" "${group} LAG=${lag}（阈值 ${LAG_THRESHOLD}）"
  fi
  return 0
}

# run_all_health_checks：执行全部 10 类检查（供 daily-check 与 main 复用）
run_all_health_checks() {
  check_containers
  check_http_endpoints
  check_databases
  check_redis
  check_kafka_topics
  check_minio_buckets
  check_disk_usage
  check_memory_usage
  check_kafka_lag
  return 0
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file rc=0
  parse_args "$@"
  hc_log_begin

  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ -f "$env_file" ]; then
    load_env "$env_file" >/dev/null 2>&1 || log_warn "加载 ${env_file} 失败：使用内置默认值"
  else
    log_warn ".env 不存在（${env_file}）：使用内置默认值（口令类探针可能失败）"
  fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  log_info "===== HunterCore 全栈健康检查（开始：$(date '+%Y-%m-%d %H:%M:%S')）====="
  health_init_counters
  run_all_health_checks
  print_health_report

  health_exit_code || rc=$?
  case "$rc" in
    0) log_success "健康检查全部通过" ;;
    1) log_warn "健康检查存在告警（WARN）：业务可用，建议尽快处理" ;;
    *) log_error "健康检查存在失败（FAIL）：请按上方明细排查" ;;
  esac
  return "$rc"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi