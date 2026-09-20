#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 日常巡检（daily-check.sh）
#
# 用途：在 health-check.sh 全量检查基础上追加业务侧巡检项，生成当日报告：
#   ① 复用 health-check.sh 的全部检查（容器/HTTP/DB/Redis/Kafka/MinIO/磁盘/内存/消费积压）
#   ② 最近 24 小时 critical 事件数（data_collector.events，event_level='critical'）
#   ③ PostgreSQL 当前连接数（pg_stat_activity）与连接上限余量
#   ④ 各容器重启次数（RestartCount，>0 提示可能反复崩溃）
#   ⑤ 数据目录占用概览（/data 各子目录，便于容量趋势跟踪）
#
# 报告：${LOG_DIR}/check/daily-check-YYYYMMDD.log（全量明细；同时打印摘要到终端）
#
# 用法：
#   sudo bash scripts/daily-check.sh [选项]
#
# 参数：
#   --help             显示本帮助
#   --env <file>       指定 .env 路径
#   --lag-threshold N  消费积压阈值（默认 10000，透传 health-check.sh）
#   --critical-threshold N  critical 事件告警阈值（默认 10，超过则 WARN）
#
# 示例：
#   sudo bash /opt/hunter-edge/scripts/daily-check.sh
#   # cron：0 7 * * * /opt/hunter-edge/scripts/daily-check.sh >> /var/log/hunter-edge/check/cron.log 2>&1
#
# 退出码：0 = 无 FAIL；2 = 存在 FAIL（WARN 不改变退出码）
# 依赖：common.sh、health-check.sh（同目录，source 复用检查逻辑）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# 复用健康检查逻辑（health-check.sh 在被 source 时不执行其主流程）
# shellcheck source=./health-check.sh
. "${SCRIPT_DIR}/health-check.sh"
# shellcheck disable=SC2154  # 运行期变量由 .env 注入

CRITICAL_THRESHOLD=10
REPORT_FILE=""

usage() {
  cat <<'EOF'
HunterCore 日常巡检脚本

用途：
  复用 health-check.sh 的 10 类检查，并追加业务巡检项（critical 事件数、DB 连接数、
  容器重启次数、数据目录占用），生成当日报告并打印摘要。

用法：
  sudo bash scripts/daily-check.sh [选项]

参数：
  --help                  显示本帮助
  --env <file>            .env 路径（默认 /opt/hunter-edge/.env）
  --lag-threshold N       Kafka 消费积压阈值（默认 10000）
  --critical-threshold N  24 小时 critical 事件告警阈值（默认 10）

示例：
  sudo bash scripts/daily-check.sh
  sudo bash scripts/daily-check.sh --critical-threshold 5

报告：${LOG_DIR}/check/daily-check-YYYYMMDD.log
退出码：0 = 无 FAIL；2 = 存在 FAIL
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
      --critical-threshold)
        [ $# -ge 2 ] || die "--critical-threshold 需要一个数字参数"
        case "$2" in
          '' | *[!0-9]*) die "--critical-threshold 必须为非负整数（收到：$2）" ;;
        esac
        CRITICAL_THRESHOLD="$2"
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

# psql_value <sql>：取单个标量值（失败返回空）
psql_value() {
  docker exec -i "hunter-postgres" psql -U "${POSTGRES_USER:-hunter}" -d "${POSTGRES_DB:-hunter_core}" \
    -tA -c "$1" 2>/dev/null || true
}

# extra_checks：业务侧巡检项（追加到健康检查结果集）
extra_checks() {
  local critical_count connections max_conn restarts name dir size

  # ② 最近 24 小时 critical 事件数（契约：19 种事件类型，阈值不可放宽）
  if docker inspect "hunter-postgres" >/dev/null 2>&1; then
    critical_count="$(psql_value "SELECT count(*) FROM data_collector.events WHERE event_level='critical' AND event_time > NOW() - INTERVAL '24 hours';")"
    if [ -z "$critical_count" ]; then
      record_result "WARN" "24h critical 事件" "查询失败（events 表可能未初始化，请确认已执行 init-db.sh）"
    elif [ "$critical_count" -gt "$CRITICAL_THRESHOLD" ]; then
      record_result "WARN" "24h critical 事件" "${critical_count} 条 > 阈值 ${CRITICAL_THRESHOLD}：请核查车辆故障/接管/急停记录"
    else
      record_result "PASS" "24h critical 事件" "${critical_count} 条（阈值 ${CRITICAL_THRESHOLD}）"
    fi

    # ③ PostgreSQL 连接数
    connections="$(psql_value "SELECT count(*) FROM pg_stat_activity;")"
    max_conn="$(psql_value "SELECT setting FROM pg_settings WHERE name='max_connections';")"
    if [ -z "$connections" ]; then
      record_result "WARN" "PostgreSQL 连接数" "查询失败（pg_stat_activity 不可读）"
    elif [ -n "$max_conn" ] && [ "$connections" -gt $((max_conn * 80 / 100)) ]; then
      record_result "WARN" "PostgreSQL 连接数" "${connections}/${max_conn}（>80%：检查连接池与会话泄漏）"
    else
      record_result "PASS" "PostgreSQL 连接数" "${connections}/${max_conn:-N/A}"
    fi
  else
    record_result "WARN" "业务巡检项" "PostgreSQL 容器不存在：跳过 critical 事件与连接数检查"
  fi

  # ④ 容器重启次数（>0 提示可能反复崩溃）
  if command_exists docker && docker info >/dev/null 2>&1; then
    local abnormal=0
    for name in hunter-postgres hunter-redis hunter-kafka hunter-minio hunter-srs hunter-flink-jm \
      hunter-flink-tm hunter-api-gateway hunter-scene hunter-collector hunter-analytics hunter-ota \
      hunter-remote hunter-web; do
      if ! docker inspect "$name" >/dev/null 2>&1; then
        continue
      fi
      restarts="$(container_restart_count "$name")"
      if [ "${restarts:-0}" -gt 0 ]; then
        abnormal=$((abnormal + 1))
        record_result "WARN" "容器重启 ${name}" "RestartCount=${restarts}（检查 OOM/健康检查失败：docker inspect ${name}）"
      fi
    done
    if [ "$abnormal" -eq 0 ]; then
      record_result "PASS" "容器重启次数" "全部容器 RestartCount=0"
    fi
  fi

  # ⑤ 数据目录占用概览（容量趋势）
  if [ -d "$DATA_DIR" ]; then
    local summary=""
    for dir in postgresql kafka zookeeper minio redis flink backups; do
      if [ -d "${DATA_DIR}/${dir}" ]; then
        size="$(du -sh "${DATA_DIR}/${dir}" 2>/dev/null | cut -f1 || printf 'N/A')"
        summary="${summary}${dir}=${size} "
      fi
    done
    record_result "PASS" "数据目录占用" "${summary:-无子目录}"
  else
    record_result "FAIL" "数据目录占用" "${DATA_DIR} 不存在"
  fi
  return 0
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file report_dir rc=0
  parse_args "$@"

  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ -f "$env_file" ]; then
    load_env "$env_file" >/dev/null 2>&1 || log_warn "加载 ${env_file} 失败：使用内置默认值"
  else
    log_warn ".env 不存在（${env_file}）：使用内置默认值"
  fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  # 巡检报告：${LOG_DIR}/check/daily-check-YYYYMMDD.log（docs/01 §5）
  report_dir="${LOG_DIR}/check"
  install -d -m 0755 "$report_dir"
  REPORT_FILE="${report_dir}/daily-check-$(date +%Y%m%d).log"
  export REPORT_FILE
  hc_set_log_file "$REPORT_FILE"
  hc_log_begin

  {
    printf '\n============================================================\n'
    printf 'HunterCore 日常巡检报告（%s）\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf '主机：%s ｜ APP_DIR：%s ｜ DATA_DIR：%s\n' "$(hostname)" "$APP_DIR" "$DATA_DIR"
    printf '阈值：Kafka LAG > %s 告警；24h critical 事件 > %s 告警\n' "$LAG_THRESHOLD" "$CRITICAL_THRESHOLD"
    printf '============================================================\n'
  } >>"$REPORT_FILE"

  log_info "===== 日常巡检开始（报告：${REPORT_FILE}）====="
  health_init_counters

  # ① 复用全量健康检查
  run_all_health_checks
  # ②~⑤ 业务侧巡检
  extra_checks

  print_health_report
  health_exit_code || rc=$?

  if [ "$rc" -eq 0 ]; then
    log_success "巡检完成：无失败项（通过 ${PASS_COUNT} 项，警告 ${WARN_COUNT} 项）"
  else
    log_error "巡检发现失败项（FAIL=${FAIL_COUNT}）：请查看报告明细 ${REPORT_FILE}"
  fi
  log_info "报告已写入：${REPORT_FILE}"
  if [ "$WARN_COUNT" -gt 0 ]; then
    log_warn "存在 ${WARN_COUNT} 项告警：建议查看报告并处理（详见报告中的 [WARN] 行）"
    log_warn "常见处置：docker compose logs <service> --tail=100、bash scripts/health-check.sh、df -h ${DATA_DIR}"
  fi
  return "$rc"
}

main "$@"