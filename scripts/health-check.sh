#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 全栈健康检查（health-check.sh）
#
# 用途：一次性核对容器 / HTTP 探针 / 数据库 / Redis / Kafka / MinIO / 磁盘与内存 / 消费积压，
#       输出 [PASS] [WARN] [FAIL] 明细与汇总，并给出退出码（供 install.sh、巡检脚本、CI 调用）。
#
# 检查项（共 16 类，⓪ 必须排第一）：
#   ⓪ 环境凭据（.env）：口令类变量为空即 FAIL（典型成因：没加 sudo，.env 权限 600）；
#      ⚠ 本项 FAIL 时，Redis / Kafka / MinIO 的相关结论一律降为「未判定」，不得当服务异常（V1.19.13）
#   ① 容器运行状态与 Docker healthcheck（缺失/非 healthy/未运行 → FAIL）
#   ② HTTP 探针：api-gateway/scene/collector/analytics/ota/remote（/healthz）、web-portal、
#      MinIO(/minio/health/live)、SRS(/api/v1/versions)、Flink UI(/overview)
#   ③ PostgreSQL：pg_isready       ④ TimescaleDB：pg_isready（存在独立实例时）
#   ⑤ Redis：redis-cli PING（区分 WRONGPASS / NOAUTH / 空口令）  ⑥ Kafka：Topic 数 ≥6
#   ⑦ MinIO：Bucket 数 ≥7          ⑧ 磁盘：/data 使用率（>85% WARN，>95% FAIL）
#   ⑨ 内存使用率（>90% WARN）      ⑩ Kafka 消费积压：data-collector-telemetry LAG（>10000 WARN）
#      ⚠ 三分语义：没查到（-1）/ 积压为 0 / 超阈值，“没查到”不再说成“车辆未上报”（V1.19.13）
#   ⑪ 配置漂移硬闸（G-03）：docker-compose.override.yml 存在即 FAIL（HUNTER_ALLOW_OVERRIDE=1 降为 WARN）；
#      并按容器 label 判定实际生效的编排文件（防裸跑 docker compose 命中仓库根开发版编排）
#   ⑫ 车端接入证书：broker 证书 SAN 必须含 SERVER_IP（车端 https 主机名校验只比 SAN）；CA/broker 有效期预警
#   ⑬ 服务运行期错误日志：扫描各服务容器近期 error/critical/Traceback（探针类检查永远看不到的一类故障：
#      消费者进程活着、/healthz 200，但订阅从未命中——只有日志里有痕迹）；订阅/Topic 类致命错直接 FAIL；
#      窗口由 ERROR_LOG_WINDOW 控制（默认 15m）
#   ⑭ Flink 作业健康（V1.19.13）：**flink list -a** 统计 RUNNING 数与 FAILED/CANCELED 终态
#      ⚠ 不加 -a 时 FAILED 作业**从列表里静默消失**（默认只列 Running/Restarting 与 Scheduled）；
#      判据是「提交数 == RUNNING 数」（默认期望 3，可用 FLINK_EXPECTED_RUNNING 覆盖）；
#      ⑬ 的服务容器清单不含 flink-jm/flink-tm，作业侧的 Kafka 写失败只有本项能抓到；
#      ⚠ 作业**提交期**失败时连终态行都没有，列表只打 `No running jobs.`（不含表头）——
#      V1.19.13 拿“有没有表头”当有效性门禁，使零作业被读成 WARN「未判定」、汇总写“失败 0 项”
#      （第八类假绿：恰好把三个全挂掩盖成“没取到样本”），V1.19.16 已把空列表归为合法读数判 FAIL
#   ⑮ Flink 检查点目录（V1.19.16）：在**容器内**比对运行用户 uid 与检查点目录（及其最近存在
#      祖先）的属主/权限，只读不写探针——compose 把 ${DATA_DIR}/flink 绑成 /opt/flink/data，而 JM/TM
#      以非 root 的 flink 用户运行，目录归 root 时作业会死在 `Failed to create directory for shared state`
#
# ⚠ 必须用 sudo 跑：.env 权限 600（install.sh / gen-passwords.sh 设定）仅 root 可读；
#   非 root 时 [ -f .env ] 仍为真但 source 失败 ⇒ 口令全空 ⇒ 本脚本会给出一串
#   “看起来像基础设施故障”的 FAIL（实测：Redis PING / Kafka Topic 数 / MinIO Bucket 数）。
#   现由 ⓪ 环境凭据项直接拦住并把依赖口令的项降为 WARN（V1.19.13）。
#
# 用法：
#   sudo bash scripts/health-check.sh [选项]
#
# 参数：
#   --help             显示本帮助
#   --env <file>       指定 .env 路径
#   --lag-threshold N  消费积压告警阈值（默认 10000）
#
# 环境变量（可选）：
#   FLINK_JM_CONTAINER（默认 hunter-flink-jm）/ FLINK_EXPECTED_RUNNING（默认 3）
#   FLINK_CHECKPOINT_DIR（默认 /opt/flink/data/checkpoints，**容器内路径**；K8s 形态为
#     /opt/flink/checkpoints，不自动探测——集群由运维显式设置，两处不一致已登记为待办）
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
  "hunter-analytics" "hunter-ota" "hunter-remote" "hunter-vehicle-service" "hunter-web"
)
OPTIONAL_CONTAINERS=("hunter-timescale")

usage() {
  cat <<'EOF'
HunterCore 全栈健康检查脚本

用途：
  检查 16 类健康项并输出 [PASS]/[WARN]/[FAIL] 明细与汇总。
  退出码：0=全部通过，1=有告警，2=有失败。

⚠ 请用 sudo 执行：.env 权限 600 仅 root 可读，非 root 时口令类探针会给出错的 FAIL
  （典型：Redis PING / Kafka Topic 数 / MinIO Bucket 数）；现由「环境凭据（.env）」项先行识别并降级为未判定。

用法：
  sudo bash scripts/health-check.sh [选项]

参数：
  --help             显示本帮助
  --env <file>       .env 路径（默认 /opt/hunter-core/.env）
  --lag-threshold N  Kafka 消费积压告警阈值（默认 10000）

示例：
  sudo bash scripts/health-check.sh
  sudo bash scripts/health-check.sh --lag-threshold 5000
  sudo bash scripts/health-check.sh && echo "全栈健康"   # ⚠ 不要省略 sudo（否则口令类项结论不可信）

检查项：
  环境凭据（.env）、容器状态/Docker healthcheck、服务 HTTP 探针、PostgreSQL、TimescaleDB、Redis、
  Kafka Topic 数、MinIO Bucket 数、/data 磁盘使用率、内存使用率、Kafka 消费积压、
  配置漂移硬闸、车端接入证书（broker 证书 SAN 与有效期）、服务运行期错误日志、
  Flink 作业健康（flink list -a：FAILED 不会被默认列表列出）。
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
  local name state health _waited=0 _pending _h
  if ! command_exists docker || ! docker info >/dev/null 2>&1; then
    record_result "FAIL" "容器运行状态" "Docker 守护进程不可用（systemctl status docker）"
    return 0
  fi
  # 有界等待：最后启动的容器（如 web 依赖 api-gateway healthy）可能仍在首个探测周期的 starting 态，
  # 直接判定会误报 FAIL。最多等 HEALTH_SETTLE_WAIT（默认 60）秒让 starting 收敛为 healthy/unhealthy。
  while [ "$_waited" -lt "${HEALTH_SETTLE_WAIT:-60}" ]; do
    _pending=0
    for name in "${EXPECTED_CONTAINERS[@]}"; do
      _h="$(container_health "$name" 2>/dev/null)"
      [ "$_h" = "starting" ] && _pending=1
    done
    [ "$_pending" -eq 0 ] && break
    sleep 5; _waited=$((_waited + 5))
  done
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
    "vehicle-service:${VEHICLE_SERVICE_PORT:-8086}:${HEALTH_PATH}"
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

# ⓪ 环境凭据（.env）——必须排在一切口令类探针之前（V1.19.13）
# 现场：非 sudo 跑本脚本时实测同时出现三项 FAIL（Redis PING 失败 / Kafka Topic 数 1 < 6 /
#   MinIO Bucket 数 -1），而同一时刻用 sudo 复核 kafka_consumer_lag 却拿到真值 0——
#   三项 FAIL 全部是“没读到 .env（权限 600）”造成的，与基础设施无关。旧版本只打一句
#   log_warn（会被 tail 掉）就继续跑，结论看上去像服务挂了，属于“把命令层错误报成业务层故障”。
check_env_credentials() {
  local missing
  # 直接执行时由 main 预算并 export；被 source 复用（daily-check.sh）时本函数自行探测
  if [ -z "${CREDS_MISSING+set}" ]; then
    CREDS_MISSING="$(env_creds_missing)"
    export CREDS_MISSING
  fi
  missing="${CREDS_MISSING:-}"
  if [ -z "$missing" ]; then
    record_result "PASS" "环境凭据（.env）" "口令类变量齐备（来源：${HUNTER_ENV_FILE:-<未记录>}）"
    return 0
  fi
  record_result "FAIL" "环境凭据（.env）" \
    "为空：${missing} → 典型成因是**没加 sudo**（.env 权限 600 仅 root 可读），请改用：sudo bash scripts/health-check.sh；⚠ 在此之前，Redis / Kafka / MinIO 相关结论一律不可信（本项已把它们降为未判定）"
  return 0
}

# 口令类探针的统一降级：CREDS_MISSING 非空时不下“服务异常”结论（避免假故障）
hc_skip_without_creds() {
  local item="$1" hint="${2:-}"
  [ -n "${CREDS_MISSING:-}" ] || return 1
  record_result "WARN" "${item}" "未判定：口令类变量为空（${CREDS_MISSING}），非服务异常；请 sudo bash scripts/health-check.sh 重跑${hint:+（${hint}）}"
  return 0
}

# ⑤ Redis
check_redis() {
  local reply
  if ! docker inspect "hunter-redis" >/dev/null 2>&1; then
    record_result "FAIL" "Redis" "容器 hunter-redis 不存在"
    return 0
  fi
  hc_skip_without_creds "Redis" "或临时：sudo bash -c 'source scripts/common.sh; load_env; docker exec hunter-redis redis-cli --no-auth-warning -a \"\${REDIS_PASSWORD}\" ping'" && return 0
  # ℞ V1.19.13：旧写法 2>/dev/null + “不含 PONG 就说 PING 失败”，把 NOAUTH / WRONGPASS / 空口令
  #   三成因合并不说（本项已挂两轮没分流）。现回原始回复（只回回复，不回显口令）。
  if [ -z "${REDIS_PASSWORD:-}" ]; then
    reply="$(docker exec "hunter-redis" redis-cli --no-auth-warning ping 2>&1 || true)"
  else
    reply="$(docker exec "hunter-redis" redis-cli --no-auth-warning -a "$REDIS_PASSWORD" ping 2>&1 || true)"
  fi
  if printf '%s' "$reply" | grep -q PONG; then
    record_result "PASS" "Redis" "PING → PONG（AOF 已启用详见 .env REDIS_APPENDONLY）"
  elif printf '%s' "$reply" | grep -qi WRONGPASS; then
    record_result "FAIL" "Redis" "WRONGPASS invalid username-password pair：.env 的 REDIS_PASSWORD 与 broker 现存口令不一致（典型：改过 .env 但未重建容器）→ 核对 sudo grep '^REDIS_PASSWORD' ${HUNTER_ENV_FILE:-/opt/HunterCore/.env} 与 docker inspect hunter-redis 的 REDIS_PASSWORD；不一致则 restart redis 并同步各服务"
  elif printf '%s' "$reply" | grep -qi NOAUTH; then
    record_result "FAIL" "Redis" "NOAUTH（未带口令，但本项已确认 REDIS_PASSWORD 非空）：检查变量是否被导出：docker exec hunter-redis env | grep -i redis"
  else
    record_result "FAIL" "Redis" "PING 失败，原始回复：$(printf '%.160s' "${reply:-<无输出>}")（docker logs hunter-redis --tail=50）"
  fi
  return 0
}

# ⑥ Kafka Topic 数
check_kafka_topics() {
  local raw count minimum=6
  if ! docker inspect "hunter-kafka" >/dev/null 2>&1; then
    record_result "FAIL" "Kafka" "容器 hunter-kafka 不存在"
    return 0
  fi
  hc_skip_without_creds "Kafka Topic 数" && return 0
  # 取一份带 stderr 的原始输出，仅用于“没查到”时把成因说出来（计数仍走 common.sh 的形态门禁）
  raw="$(kafka_topics_cli --list 2>&1 || true)"
  count="$(kafka_topic_count)"
  if [ "${count:-0}" -lt 0 ]; then
    record_result "FAIL" "Kafka Topic 数" "**查不到清单**（不是“Topic 没建完”）：$(printf '%.200s' "${raw:-<无输出>}") → 优先查认证：内部监听固定 SASL_PLAINTEXT，缺 --command-config / 凭据不对时报的是 node assignment 超时而非权限错"
  elif [ "${count:-0}" -lt "$minimum" ]; then
    record_result "FAIL" "Kafka Topic 数" "${count} < ${minimum}（清单已读到，确实不足：见 init-kafka.sh）"
  else
    record_result "PASS" "Kafka Topic 数" "${count}（契约 ≥${minimum}，含 __consumer_offsets）"
  fi
  return 0
}

# ⑦ MinIO Bucket 数
check_minio_buckets() {
  local raw count minimum=7
  if ! docker inspect "hunter-minio" >/dev/null 2>&1; then
    record_result "FAIL" "MinIO" "容器 hunter-minio 不存在"
    return 0
  fi
  hc_skip_without_creds "MinIO Bucket 数" && return 0
  raw="$(minio_mc ls local 2>&1 || true)"
  count="$(minio_bucket_count)"
  if [ "${count:-0}" -lt 0 ]; then
    record_result "FAIL" "MinIO Bucket 数" "**查不到清单**（不是“桶没建”）：$(printf '%.200s' "${raw:-<无输出>}") → 常见成因：MINIO_ROOT_USER/PASSWORD 不对、mc 镜像未拉取（${MINIO_MC_IMAGE:-minio/mc}）"
  elif [ "${count:-0}" -lt "$minimum" ]; then
    record_result "FAIL" "MinIO Bucket 数" "${count} < ${minimum}（清单已读到，确实不足：见 init-minio.sh）"
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
  hc_skip_without_creds "Kafka 消费积压" && return 0
  lag="$(kafka_consumer_lag "$group" 2>/dev/null)"; [ -z "$lag" ] && lag="-1"
  if [ "$lag" -lt 0 ]; then
    # ℞ V1.19.13：旧文案把“没查到”直接说成“车辆未上报或 data-collector 未消费”，而归因其实是
    #   命令本身（缺 --bootstrap-server / 认证不可用 / 输出不是 describe 结果）。kafka_consumer_lag
    #   现已区分“LAG 真的是 0”与“根本没查到”，文案必须跟着改口径。
    record_result "WARN" "Kafka 消费积压" "**没能读到消费组 ${group} 的清单**（不是“积压为 0”，也不是“车辆未上报”）→ 先用封装自查可用性：sudo bash -c 'source scripts/common.sh; load_env; kafka_groups_cli --describe --group <组>'（无表头 / 报 does not exist / 刷 usage 都属本类）"
  elif [ "$lag" -gt "$LAG_THRESHOLD" ]; then
    record_result "WARN" "Kafka 消费积压" "${group} LAG=${lag} > ${LAG_THRESHOLD}：检查 data-collector 消费能力与批写性能（入库 ≤1s）"
  else
    record_result "PASS" "Kafka 消费积压" "${group} LAG=${lag}（阈值 ${LAG_THRESHOLD}）"
  fi
  return 0
}

# ⑭ Flink 作业健康（V1.19.13）：flink list -a 不只要看“有没有 RUNNING”，更要看“有没有终态”
# ⚠ 不加 -a 时 FAILED/CANCELED/FINISHED 仍**根本不出现**在输出里（默认只列 Running/Restarting
#   与 Scheduled），于是“列表里没异常”会被读成“作业都健康”——本仓第六例同类假信号。
check_flink_jobs() {
  local container="${FLINK_JM_CONTAINER:-hunter-flink-jm}"
  local expected="${FLINK_EXPECTED_RUNNING:-3}" out running bad name stack line
  if ! command_exists docker || ! docker inspect "$container" >/dev/null 2>&1; then
    record_result "WARN" "Flink 作业健康" "容器 ${container} 不存在或无 docker 权限（未部署 flink-jm 时可忽略）"
    return 0
  fi
  out="$(docker exec "$container" flink list -a 2>&1 || true)"
  # JVM 启动噪声（WARNING: Unknown module: jdk.compiler ... is not exported by this version of the JDK）
  #   是 flink CLI 的 stderr，既不含表头也不含作业行；留着会占满下面 200 字符的详情窗口，
  #   把真正有定性的报错顶掉（实录里 WARN 详情只能看到噪声，看不到任何原因）
  out="$(printf '%s\n' "$out" | grep -vE '^(WARNING: |SLF4J:)' || true)"

  # 空列表是**合法读数**（V1.19.16）：提交期就失败的作业连 FAILED 终态都不登记，列表只打
  #   `No running jobs.`。V1.19.13 把“没表头”一律当命令失败判 WARN，于是三个作业全挂时
  #   汇总写成「失败 0 项」——本仓第八类假绿，且恰好发生在最严重的那一档
  if printf '%s\n' "$out" | grep -qE 'No running jobs\.'; then
    record_result "FAIL" "Flink 作业数" "RUNNING=0 < ${expected}：JM 明确返回 No running jobs.（合法空列表，不是未判定）。列表里也没有 FAILED 终态行 ⇒ 典型为**提交期失败**（作业从未进入 RUNNING）：先看本巡检 ⑮「Flink 检查点目录」是否可写，再查堆栈（不截断窗口）——sudo docker logs ${container} 2>&1 后接 awk '/switched from RUNNING to FAILED/{f=1} f' 落盘，读**最后一条** Caused by 及其后续行（提取命令见运维手册 §3.6）"
    return 0
  fi
  if ! printf '%s\n' "$out" | grep -qE 'Running/Restarting Jobs'; then
    record_result "WARN" "Flink 作业健康" "**未判定**：flink list -a 既无作业表头也无 'No running jobs.'（JM REST 未就绪或命令失败）：$(printf '%.200s' "${out:-<无输出>}")"
    return 0
  fi
  running="$(printf '%s\n' "$out" | grep -cE '\((RUNNING|RESTARTING)\)' || true)"
  bad="$(printf '%s\n' "$out" | grep -E '\((FAILED|CANCELED)\)' || true)"
  stack=""
  if [ -n "$bad" ]; then
    stack="$(docker logs --since "${FLINK_LOG_WINDOW:-24h}" "$container" 2>&1 \
      | grep -m1 'switched from RUNNING to FAILED' || true)"
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      name="$(printf '%s' "$line" | sed -E 's/.*: +([^ ]+) +\((FAILED|CANCELED)\).*/\1/')"
      record_result "FAIL" "Flink 作业健康（${name}）" \
        "已进入终态：$(printf '%.120s' "$line") → ⚠ **不加 -a 的 flink list 根本看不见它**；取堆栈要**现在**做（JM 无 HA，容器一重建日志即丢），且**不要截断窗口**——能定性的最内层 Caused by 常在 100 行之后：sudo docker logs ${container} 2>&1 | awk '/switched from RUNNING to FAILED/{f=1} f' > /tmp/flink-failed.txt 后读**最后一条** Caused by 及其后续行（不是第一条，也不要再夹 head/tail 窗口；提取命令见运维手册 §3.6）；$(printf '%.180s' "${stack:-<窗口内无 FAILED 日志，可放宽 FLINK_LOG_WINDOW>}")"
    done <<<"$bad"
  fi
  if [ "${running:-0}" -lt "$expected" ]; then
    record_result "FAIL" "Flink 作业数" "RUNNING=${running:-0} < ${expected}（契约已交付三作业：driving_anomaly_detection / algorithm_performance_monitor / data_quality_monitor_ingest_latency；判据是提交数 == RUNNING 数；改期望值可设 FLINK_EXPECTED_RUNNING）"
  elif [ -z "$bad" ]; then
    record_result "PASS" "Flink 作业健康" "RUNNING=${running}（期望 ${expected}），无 FAILED/CANCELED 终态"
  fi
  return 0
}

# ⑮ Flink 检查点目录可写（V1.19.16）：不可写时**所有**作业都死在提交期
# ⚠ 只读探测：不往检查点目录写探针文件（残留文件可能被 Flink 当成状态）；可写性由
#   「最近存在的祖先 + 容器内运行用户 uid/gid + mode」推得。目录归 root 而容器以非 root 的
#   flink 用户运行时，报错是 `Failed to create directory for shared state: file:/opt/flink/data/…`
#   ——先于任何算子代码，因此与具体作业无关（实录：三个作业同时挂）
check_flink_checkpoint_dir() {
  local container="${FLINK_JM_CONTAINER:-hunter-flink-jm}"
  local target="${FLINK_CHECKPOINT_DIR:-/opt/flink/data/checkpoints}"
  local probe uid gid cdir own cud cgid cmode host="" writable=0
  if ! command_exists docker || ! docker inspect "$container" >/dev/null 2>&1; then
    record_result "WARN" "Flink 检查点目录" "容器 ${container} 不存在或无 docker 权限（未部署 flink-jm 时可忽略）"
    return 0
  fi
  probe="$(docker exec "$container" sh -c '
      p="$1"
      while [ ! -d "$p" ] && [ "$p" != "/" ]; do p="$(dirname "$p")"; done
      printf "uid=%s\n" "$(id -u)"
      printf "gid=%s\n" "$(id -g)"
      printf "dir=%s\n" "$p"
      printf "own=%s\n" "$(stat -c "%u %g %a" "$p" 2>/dev/null || echo unknown)"
    ' sh "$target" 2>&1 || true)"
  uid="$(printf '%s\n' "$probe" | sed -n 's/^uid=//p')"
  gid="$(printf '%s\n' "$probe" | sed -n 's/^gid=//p')"
  cdir="$(printf '%s\n' "$probe" | sed -n 's/^dir=//p')"
  own="$(printf '%s\n' "$probe" | sed -n 's/^own=//p')"
  if [ -z "$uid" ] || [ -z "$gid" ] || [ -z "$cdir" ]; then
    record_result "WARN" "Flink 检查点目录" "**未判定**：容器内探测失败（${probe:-<无输出>}）"
    return 0
  fi
  cud="${own%% *}"; own="${own#* }"; cgid="${own%% *}"; cmode="${own##* }"
  if [ "$cud" = "unknown" ] || [ -z "$cgid" ] || [ -z "$cmode" ]; then
    record_result "WARN" "Flink 检查点目录" "**未判定**：容器内读不到 ${cdir} 的属主/权限（镜像无 GNU stat？）：$(printf '%.120s' "$probe")"
    return 0
  fi
  # 宿主侧对应路径（compose: ${DATA_DIR}/flink → 容器 /opt/flink/data），只为了把修复命令写完整
  case "$cdir" in
    /opt/flink/data*) host="${DATA_DIR:-/data}/flink${cdir#/opt/flink/data}" ;;
    *) host="${cdir}（容器内，未能映射到宿主路径）" ;;
  esac
  if [ "$cud" = "$uid" ] && [ $((8#$cmode & 8#200)) -ne 0 ]; then
    writable=1
  elif [ "$cgid" = "$gid" ] && [ $((8#$cmode & 8#020)) -ne 0 ]; then
    writable=1
  elif [ $((8#$cmode & 8#002)) -ne 0 ]; then
    writable=1
  fi
  if [ "$writable" = "1" ]; then
    record_result "PASS" "Flink 检查点目录" "容器内 uid=${uid}:${gid} 对 ${cdir}（${cud}:${cgid} ${cmode}）可写，可在其下创建 ${target}"
  else
    record_result "FAIL" "Flink 检查点目录" "**不可写**：容器内以 uid=${uid}:${gid} 运行，而 ${cdir}（宿主 ${host}）属 ${cud}:${cgid} ${cmode}，Flink 建不了 ${target} ⇒ 报错 Failed to create directory for shared state，**全部作业提交期失败**（不是单个作业的问题）。修复：sudo chown -R ${uid}:${gid} ${host} && sudo chmod 755 ${host}（bind 目录改属主即时生效，**无需重建容器**，但需重新提交作业）；宿主侧预建已改由 install.sh Step 3 按容器 UID 归属，重跑 sudo bash scripts/install.sh --step 3 可自愈"
  fi
  return 0
}

# ⑪ 配置漂移硬闸（G-03）：override 会被 compose 静默合并（TLS/端口/资源降级），生产禁止
check_compose_override() {
  local override="${APP_DIR}/docker-compose.override.yml"
  if [ ! -f "$override" ]; then
    record_result "PASS" "配置漂移（G-03）" "无 docker-compose.override.yml（生产编排未被静默降级）"
  elif [ "${HUNTER_ALLOW_OVERRIDE:-0}" = "1" ]; then
    record_result "WARN" "配置漂移（G-03）" "存在 override 且 HUNTER_ALLOW_OVERRIDE=1 放行（仅限联调，需书面豁免）"
  else
    record_result "FAIL" "配置漂移（G-03）" "存在 ${override}：compose 将静默合并降级配置，生产必须移除"
  fi

  # 编排来源校验：仓库根 docker-compose.yml 是**本地开发版**（KRaft + PLAINTEXT、镜像 3.6.2、
  # container_name 与生产相同）。在部署目录裸跑 `docker compose ...` 会命中它，把生产 broker
  # 换成无 TLS / 无证书挂载的实例 → 车端 9093 直接失效。此处按 label 回看创建它的编排文件。
  if command_exists docker && docker inspect "${C_KAFKA:-hunter-kafka}" >/dev/null 2>&1; then
    local cf
    cf="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project.config_files" }}' \
      "${C_KAFKA:-hunter-kafka}" 2>/dev/null || true)"
    case "$cf" in
      *infra/deploy/docker-compose.yml*)
        record_result "PASS" "编排来源（G-03）" "${C_KAFKA:-hunter-kafka} 由生产编排创建"
        ;;
      '')
        record_result "WARN" "编排来源（G-03）" "读不到 ${C_KAFKA:-hunter-kafka} 的编排 label（非 compose 创建？）"
        ;;
      *)
        record_result "FAIL" "编排来源（G-03）" \
          "${C_KAFKA:-hunter-kafka} 由 ${cf}（非生产编排）创建：裸跑 docker compose 会命中仓库根开发版（KRaft/PLAINTEXT，车端 9093 失效）；修复：docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --force-recreate kafka"
        ;;
    esac
  fi
  return 0
}

# ⑫ 车端接入证书（车端 9093 可用前提）：broker 证书 SAN 覆盖接入地址 + 有效期预警
check_kafka_broker_cert() {
  local cert_dir broker_cert ca_cert san_entries san_flat name f enddate end_epoch days_left
  local warn_days=30 csv item raw want missing kind
  # 依赖版本对齐：SAN 归一函数在 common.sh（旧版无此函数时，command not found 会在
  # set -e 下直接打断脚本，导致本项及其后所有检查静默不执行）——先给可读提示。
  if ! declare -F hc_cert_san_entries >/dev/null 2>&1; then
    record_result "FAIL" "车端接入证书" "scripts/common.sh 未同步（缺 hc_cert_san_entries）：无法比对 SAN，请先 rsync 脚本集"
    return 0
  fi
  cert_dir="${KAFKA_CERTS_DIR:-${APP_DIR}/certs/kafka}"
  broker_cert="${cert_dir}/broker-cert.pem"
  ca_cert="${cert_dir}/ca-cert.pem"

  if ! command_exists openssl; then
    record_result "WARN" "车端接入证书" "缺少 openssl，无法校验 broker 证书 SAN 与有效期"
    return 0
  fi
  if [ ! -f "$broker_cert" ]; then
    record_result "FAIL" "车端接入证书" "缺少 ${broker_cert}：bash ${GEN_KAFKA_CERTS_SH} --ip <服务器IP> 生成"
    return 0
  fi

  # SAN 必须含车端实际访问地址（openssl 写法差异由 hc_cert_san_entries 归一）
  # 要求集合 = SERVER_IP + .env 声明的附加地址（KAFKA_CERT_SAN_IPS/DNS）：
  # 附加地址同样是车端会用的接入地址，缺一个就会那批车端连不上（与 gen-kafka-certs.sh 同一口径）。
  san_entries="$(hc_cert_san_entries "$broker_cert")"
  san_flat="$(printf '%s' "$san_entries" | tr '\n' ' ')"
  local -a need=()
  if [ -n "${SERVER_IP:-}" ] && [ "${SERVER_IP:-}" != "CHANGE_ME_SERVER_IP" ]; then
    need+=("IP:${SERVER_IP}")
  fi
  for csv in "${KAFKA_CERT_SAN_IPS:-}:IP" "${KAFKA_CERT_SAN_DNS:-}:DNS"; do
    raw="${csv%:*}"
    kind="${csv##*:}"
    [ -n "$raw" ] || continue
    local -a items=()
    IFS=',' read -r -a items <<<"$raw"
    for item in ${items[@]+"${items[@]}"}; do
      item="${item//[[:space:]]/}"
      [ -n "$item" ] && need+=("${kind}:${item}")
    done
  done
  if [ "${#need[@]}" -eq 0 ]; then
    record_result "WARN" "车端接入证书 SAN" ".env SERVER_IP 未配置且无附加地址，无法比对（现有 SAN：${san_flat}）"
  else
    missing=""
    for want in "${need[@]}"; do
      case "$want" in IP:*:*) continue ;; esac          # IPv6 输出写法差异大，不做严格比对
      printf '%s\n' "$san_entries" | grep -qxF "$want" || missing="${missing}${missing:+ }${want}"
    done
    if [ -z "$missing" ]; then
      record_result "PASS" "车端接入证书 SAN" "含全部接入地址（要求：${need[*]}；SAN：${san_flat}）"
    else
      record_result "FAIL" "车端接入证书 SAN" "不含 ${missing}（现有 SAN：${san_flat}）→ 对应地址接入的车端 TLS 主机名校验失败、连不上 9093；修复：bash ${GEN_KAFKA_CERTS_SH} --broker-only（复用 CA，车端无需换证书，附加地址可写在 .env 的 KAFKA_CERT_SAN_IPS/DNS）后 docker compose -f infra/deploy/docker-compose.yml --project-directory . restart kafka（改过 SERVER_IP 需 --force-recreate）"
    fi
  fi

  # 有效期（到期前 30 天 WARN；已过期 FAIL）
  for f in "$broker_cert" "$ca_cert"; do
    [ -f "$f" ] || continue
    name="broker"
    [ "$f" = "$ca_cert" ] && name="CA"
    enddate="$(openssl x509 -in "$f" -noout -enddate 2>/dev/null | cut -d= -f2-)"
    end_epoch="$(date -d "${enddate:-1970-01-01}" +%s 2>/dev/null || printf '-1')"
    if [ "$end_epoch" = "-1" ]; then
      record_result "WARN" "车端接入证书有效期（${name}）" "无法解析 notAfter：${enddate:-<空>}"
      continue
    fi
    days_left=$(( (end_epoch - $(date +%s)) / 86400 ))
    if [ "$days_left" -le 0 ]; then
      record_result "FAIL" "车端接入证书有效期（${name}）" "已过期（notAfter：${enddate}）：重签后重启 kafka 并同步车端"
    elif [ "$days_left" -le "$warn_days" ]; then
      record_result "WARN" "车端接入证书有效期（${name}）" "剩余 ${days_left} 天（${enddate}）：见运维手册证书更新章节"
    else
      record_result "PASS" "车端接入证书有效期（${name}）" "剩余 ${days_left} 天（${enddate}）"
    fi
  done
  return 0
}

# ⑬ 服务运行期错误日志（探针看不见的故障类）：近窗口内 error/critical/Traceback
# 背景（V1.18.12）：通配订阅未转 librdkafka 正则时，消费者只报 UNKNOWN_TOPIC_OR_PART：
# 容器 healthy、/healthz 200、消费组根本不存在，当时 41 项巡检全部“正常”，
# 而平台其实一条消息也没收过。本项把“日志里有错”从人工排查提升为例行硬信号。
# ⚠ 三类信号必须分开判读（V1.18.13：本项曾把“生产侧 Topic 缺失”归到“订阅未命中”，指错了修复方向）：
#   A 订阅未命中（FAIL）：Subscribed topic not available —— 镜像未含“通配订阅转正则”修复（V1.18.12）
#   B 目标 Topic 缺失（FAIL）：kafka_dlq_produce_failed / kafka_dlq_message_buffered / kafka_produce_failed
#     —— broker 关自动建 Topic，消息要去的 Topic（典型：死信 {topic}.dlq）还没创建
#   C 报文与契约 Schema 不符（WARN）：kafka_message_schema_invalid —— 车端报文问题，不是平台代码故障；
#     字段级明细见日志 schema_errors，非法消息已转投 DLQ／落盘（不丢），但不会入库（车辆页面会离线）
check_service_error_logs() {
  local containers=(hunter-api-gateway hunter-scene hunter-collector hunter-analytics \
    hunter-ota hunter-remote hunter-vehicle-service)
  local window="${ERROR_LOG_WINDOW:-15m}"
  local err_re='\[(error|critical)[[:space:]]*\]|Traceback \(most recent call last\)'
  local sub_re='Subscribed topic not available'
  local topic_missing_re='kafka_dlq_produce_failed|kafka_dlq_message_buffered|kafka_produce_failed|code=_UNKNOWN_TOPIC|code=UNKNOWN_TOPIC_OR_PART'
  local schema_re='kafka_message_schema_invalid'
  local c logs errors sub_hit topic_hit schema_cnt sample scanned=0 flagged=0
  if ! command_exists docker; then
    record_result "WARN" "服务错误日志" "缺少 docker 命令，跳过运行期错误日志扫描"
    return 0
  fi
  for c in "${containers[@]}"; do
    docker inspect "$c" >/dev/null 2>&1 || continue
    scanned=$((scanned + 1))
    logs="$(docker logs --since "$window" "$c" 2>&1 || true)"
    [ -n "$logs" ] || continue
    errors="$(printf '%s\n' "$logs" | grep -cE "$err_re" || true)"
    sub_hit="$(printf '%s\n' "$logs" | grep -m1 -E "$sub_re" || true)"
    topic_hit="$(printf '%s\n' "$logs" | grep -m1 -E "$topic_missing_re" || true)"
    schema_cnt="$(printf '%s\n' "$logs" | grep -cE "$schema_re" || true)"
    if [ -n "$sub_hit" ]; then
      record_result "FAIL" "服务错误日志（${c}：订阅未命中）" \
        "$(printf '%.200s' "$sub_hit") → 该服务从未消费到任何消息（看板与台账恒显离线）；先确认镜像含“通配订阅转正则”修复（V1.18.12，启动日志 subscription 应形如 ^hunter\\.[^.]+\\.<type>$）；docker compose -f infra/deploy/docker-compose.yml --project-directory . up -d --build $c"
      flagged=$((flagged + 1))
    fi
    if [ -n "$topic_hit" ]; then
      record_result "FAIL" "服务错误日志（${c}：目标 Topic 缺失）" \
        "$(printf '%.200s' "$topic_hit") → 要去的 Topic 不存在（多为死信 {topic}.dlq 未创建）；修复：sudo bash scripts/init-kafka.sh（平台内部 + 死信）；每车死信由车辆 provisioning 创建（V1.18.13 起，已开通车辆需重跑开通或手工补建 hunter.<id>.<type>.dlq）；日志含 kafka_dlq_message_buffered 则消息已落盘，Topic 建齐后自动补投"
      flagged=$((flagged + 1))
    fi
    if [ "${schema_cnt:-0}" -gt 0 ]; then
      record_result "WARN" "服务错误日志（${c}：报文不符合契约 Schema）" \
        "近 ${window} ${schema_cnt} 条 kafka_message_schema_invalid（车端与契约不一致，非平台故障）→ 看日志 schema_errors 字段级明细定位哪个字段不符（旧版本只有 [2001] 无法归因）；这类消息不入库（对应车辆页面会显离线）；取证：console-consumer 读原 Topic 或 {topic}.dlq"
      flagged=$((flagged + 1))
    fi
    if [ -z "$sub_hit" ] && [ -z "$topic_hit" ] && [ "${schema_cnt:-0}" -eq 0 ] && [ "${errors:-0}" -gt 0 ]; then
      sample="$(printf '%s\n' "$logs" | grep -E "$err_re" | tail -1)"
      record_result "WARN" "服务错误日志（${c}）" \
        "近 ${window} 有 ${errors} 条 error/critical/Traceback，最后一条：$(printf '%.200s' "$sample")（docker logs --since ${window} ${c}）"
      flagged=$((flagged + 1))
    fi
  done
  if [ "$flagged" -eq 0 ]; then
    record_result "PASS" "服务错误日志" "近 ${window} ${scanned} 个服务容器无 error/critical/Traceback"
  fi
  return 0
}

# run_all_health_checks：执行全部 16 类检查（供 daily-check 与 main 复用）
# ⚠ 顺序有语义：「环境凭据（.env）」必须排第一——后面的 Redis / Kafka / MinIO 靠它判断
#   “服务异常”还是“根本没带凭据”（V1.19.13）。
# 各项均以“失败被测试”的上下文调用（if ! f）：bash 在该上下文中会关闭 errexit，
# 使某项内部命令报错不再中断整轮检查（历史现象：脚本中途退出，导致后面的
# “车端接入证书 SAN”硬闸静默缺失，运维只看到前面几项而误以为全部通过）。
# 未完成项记为 WARN，汇总里能直接看出“哪项没跑完”。
run_all_health_checks() {
  local check
  for check in check_env_credentials check_compose_override check_containers check_http_endpoints \
    check_databases check_redis check_kafka_topics check_minio_buckets check_disk_usage \
    check_memory_usage check_kafka_lag check_kafka_broker_cert check_service_error_logs \
    check_flink_jobs check_flink_checkpoint_dir; do
    if ! "$check"; then
      record_result "WARN" "检查项未完成（${check}）" "该项内部命令返回非 0（已继续执行余下检查，stderr 有具体报错行）"
    fi
  done
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
    # ℞ V1.19.13：不再把加载失败归成一句会被 tail 掉、也不进健康项的 log_warn：
    #   [ -f ] 只看目录权限，.env（600）对非 root 照样“存在”，但 source 会因 EACCES 失败。
    #   load_env 现在自己区分“不可读”与“语法错”，故不再吞掉它的输出
    if ! load_env "$env_file"; then
      log_error "承上：口令类探针本轮不可信，请改用 sudo bash scripts/health-check.sh"
    fi
  else
    log_warn ".env 不存在（${env_file}）：使用内置默认值（口令类探针会失败）"
  fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE
  # 口令类变量实际取值情况（供 ⓪ 环境凭据项与各探针降级使用）
  CREDS_MISSING="$(env_creds_missing 2>/dev/null || printf '')"
  export CREDS_MISSING

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