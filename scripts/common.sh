#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署脚本集 —— 公共库（被其他脚本 source，禁止直接执行）
#
# 用途：统一日志（终端 + /var/log/hunter-edge-install.log）、root 校验、命令/端口/磁盘检查、
#       通用等待、.env 加载、容器与 Kafka CLI 辅助。所有部署脚本必须 source 本文件，
#       禁止在各自脚本内重复实现日志与检查逻辑。
#
# 用法：. "${SCRIPT_DIR}/common.sh"          # 在业务脚本中 source（第一行之后立即加载）
#       bash scripts/common.sh --help        # 查看本文件说明（本文件不执行实际动作）
#
# 关键路径（与 docs/01-部署概述与环境要求.md §1.2/§5、infra/deploy/README.md 一致）：
#   应用根目录 APP_DIR = 脚本目录的上级目录（服务器上 = /opt/hunter-edge）
#   数据根目录 /data、宿主机日志目录 /var/log/hunter-edge
#   ⚠ 任务书中出现的 /opt/HunterCore、/var/log/hunter-core-install.log 与已落地文档冲突，
#     本脚本集统一采用 /opt/hunter-edge；如需改基准，设置环境变量 HUNTER_APP_DIR 覆盖。
#
# 可覆盖的环境变量（均可选，便于本地/非 root 环境验证）：
#   HUNTER_APP_DIR   应用根目录（默认 = SCRIPT_DIR 的上级）
#   HUNTER_DATA_DIR  数据根目录（默认 /data）
#   HUNTER_LOG_DIR   宿主机日志目录（默认 /var/log/hunter-edge）
#   HUNTER_INSTALL_LOG 部署日志文件（默认 /var/log/hunter-edge-install.log）
#   HC_LOG_TO_FILE   1=日志同时落盘（默认）0=仅终端
#   NO_COLOR=1       禁用彩色输出（非交互终端亦自动禁用）
#
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS (bash 5.x)
# =====================================================================
set -euo pipefail

# ---------------------------------------------------------------------
# 1. 路径基准（所有脚本禁止依赖当前工作目录）
# ---------------------------------------------------------------------
# SCRIPT_DIR：本文件（或调用脚本）所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# APP_DIR：应用根目录 —— 服务器上脚本位于 ${APP_DIR}/scripts
APP_DIR="${HUNTER_APP_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
# DATA_DIR：有状态数据根目录（docs/01 §5）
DATA_DIR="${HUNTER_DATA_DIR:-/data}"
# LOG_DIR：宿主机侧日志目录；LOG_FILE：脚本日志文件（可用 hc_set_log_file 覆盖）
LOG_DIR="${HUNTER_LOG_DIR:-/var/log/hunter-edge}"
LOG_FILE="${HUNTER_INSTALL_LOG:-/var/log/hunter-edge-install.log}"
# SCRIPT_DIR 下各脚本路径（供 install.sh 等调用兄弟脚本，避免硬编码文件名）
GEN_PASSWORDS_SH="${SCRIPT_DIR}/gen-passwords.sh"
GEN_KAFKA_CERTS_SH="${SCRIPT_DIR}/gen-kafka-certs.sh"
INIT_DB_SH="${SCRIPT_DIR}/init-db.sh"
INIT_KAFKA_SH="${SCRIPT_DIR}/init-kafka.sh"
INIT_MINIO_SH="${SCRIPT_DIR}/init-minio.sh"
HEALTH_CHECK_SH="${SCRIPT_DIR}/health-check.sh"
BACKUP_SH="${SCRIPT_DIR}/backup.sh"
UNINSTALL_SH="${SCRIPT_DIR}/uninstall.sh"
DAILY_CHECK_SH="${SCRIPT_DIR}/daily-check.sh"
COLLECT_LOGS_SH="${SCRIPT_DIR}/collect-logs.sh"
export GEN_PASSWORDS_SH GEN_KAFKA_CERTS_SH INIT_DB_SH INIT_KAFKA_SH INIT_MINIO_SH \
  HEALTH_CHECK_SH BACKUP_SH UNINSTALL_SH DAILY_CHECK_SH COLLECT_LOGS_SH

# ---------------------------------------------------------------------
# 2. 彩色输出（非交互终端/NO_COLOR 自动降级为无色）
# ---------------------------------------------------------------------
if [ -t 1 ] && [ -n "${TERM:-}" ] && [ "${TERM}" != "dumb" ] && [ "${NO_COLOR:-}" != "1" ]; then
  HC_COLOR=1
else
  HC_COLOR=0
fi
if [ "$HC_COLOR" -eq 1 ]; then
  C_BLUE=$'\033[0;34m'
  C_GREEN=$'\033[0;32m'
  C_YELLOW=$'\033[0;33m'
  C_RED=$'\033[0;31m'
  C_RESET=$'\033[0m'
else
  C_BLUE=""
  C_GREEN=""
  C_YELLOW=""
  C_RED=""
  C_RESET=""
fi
export C_BLUE C_GREEN C_YELLOW C_RED C_RESET HC_COLOR

# ---------------------------------------------------------------------
# 3. 日志：格式 `[YYYY-MM-DD HH:MM:SS] [LEVEL] message`，终端彩色 + 落盘无色
# ---------------------------------------------------------------------
# hc_set_log_file <path>：切换本进程日志文件（如 health-check.sh 写入 /var/log/hunter-edge/check/）
hc_set_log_file() {
  LOG_FILE="$1"
}

# _hc_append_log <单行文本>：追加到日志文件（已存在则追加，权限 640；落盘内容无色码）
# 说明：日志不可写（非 root / 目录权限）时静默降级为仅终端输出，且只探测一次（_HC_LOG_TARGET_FAILED）
_hc_append_log() {
  [ "${HC_LOG_TO_FILE:-1}" = "1" ] || return 0
  [ "${_HC_LOG_TARGET_FAILED:-0}" = "1" ] && return 0
  local target="${LOG_FILE:-}"
  [ -n "$target" ] || return 0
  if [ ! -e "$target" ]; then
    # 组重定向先接管 stderr，避免 bash 在重定向失败时把 "Permission denied" 打到终端
    { mkdir -p "$(dirname "$target")" && : >>"$target"; } 2>/dev/null || {
      _HC_LOG_TARGET_FAILED=1
      return 0
    }
    chmod 640 "$target" 2>/dev/null || true
  fi
  printf '%s\n' "$1" 2>/dev/null >>"$target" || _HC_LOG_TARGET_FAILED=1
  return 0
}

# _hc_emit <LEVEL> <COLOR> <message...>：统一输出（ERROR 走 stderr，便于 stdout 被捕获时不污染）
_hc_emit() {
  local level="$1" color="$2"
  shift 2
  local ts line
  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  line="[${ts}] [${level}] $*"
  if [ "$level" = "ERROR" ]; then
    printf '%s%s%s\n' "$color" "$line" "$C_RESET" >&2
  else
    printf '%s%s%s\n' "$color" "$line" "$C_RESET"
  fi
  _hc_append_log "$line"
}

log_info() { _hc_emit "INFO" "$C_BLUE" "$@"; }
log_success() { _hc_emit "SUCCESS" "$C_GREEN" "$@"; }
log_warn() { _hc_emit "WARN" "$C_YELLOW" "$@"; }
log_error() { _hc_emit "ERROR" "$C_RED" "$@"; }
log_debug() {
  # DEBUG 级别仅在 DEBUG=true 时输出（避免污染正常部署输出）
  if [ "${DEBUG:-false}" = "true" ]; then
    _hc_emit "DEBUG" "$C_BLUE" "$@"
  fi
  return 0
}

# die <message...>：打印错误并退出 1（所有脚本统一失败出口）
die() {
  log_error "$@"
  exit 1
}

# ---------------------------------------------------------------------
# 4. 基础检查工具（幂等、可重复调用）
# ---------------------------------------------------------------------
# check_root：必须以 root/sudo 运行（部署涉及 /data、docker、systemd）
check_root() {
  local uid
  uid="$(id -u)"
  if [ "$uid" -ne 0 ]; then
    log_error "必须以 root 或 sudo 运行（当前 uid=${uid}）：请使用 'sudo bash $0 ...'"
    return 1
  fi
  log_success "权限校验通过：以 root 运行"
}

# command_exists <cmd>：命令是否存在
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# version_ge <待比较版本> <基准版本>：a >= b 返回 0（例：version_ge "$(docker --version)" 24.0）
version_ge() {
  local given="$1" base="$2"
  [ -n "$given" ] || return 1
  [ "$(printf '%s\n%s\n' "$base" "$given" | sort -V | head -n1)" = "$base" ]
}

# check_port <port>：端口未被占用返回 0；被占用返回 1 并打印占用进程
check_port() {
  local port="${1:?check_port 需要端口参数}"
  local detail=""
  if command_exists ss; then
    detail="$(ss -Hltn "sport = :${port}" 2>/dev/null | head -n3)"
  elif command_exists netstat; then
    detail="$(netstat -ltn 2>/dev/null | awk -v p=":${port}" '$4 ~ p"$" {print}' | head -n3)"
  elif command_exists lsof; then
    detail="$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | head -n3)"
  else
    log_warn "缺少 ss/netstat/lsof，跳过端口 ${port} 占用检查（请安装 iproute2）"
    return 0
  fi
  if [ -n "$detail" ]; then
    log_error "端口 ${port} 已被占用：$(printf '%s' "$detail" | tr '\n' ';')"
    return 1
  fi
  return 0
}

# require_port_free <port> <用途说明>：占用则报错（不退出，由调用方决定终止）
require_port_free() {
  local port="$1" usage="${2:-}"
  if ! check_port "$port"; then
    log_error "对外端口冲突：${port}/tcp（${usage}）。请先释放端口（ss -ltnp 'sport = :${port}'）后重试"
    return 1
  fi
  log_success "端口可用：${port}/tcp（${usage}）"
}

# check_disk_space <dir> <min_gb>：目录（或其最近存在的父目录）可用空间 ≥ min_gb 返回 0
check_disk_space() {
  local dir="${1:?check_disk_space 需要目录参数}" min_gb="${2:?check_disk_space 需要最小 GB}"
  local probe="$dir" avail_kb avail_gb
  while [ ! -d "$probe" ] && [ "$probe" != "/" ]; do
    probe="$(dirname "$probe")"
  done
  avail_kb="$(df -Pk "$probe" 2>/dev/null | awk 'NR==2 {print $4}')"
  if [ -z "$avail_kb" ]; then
    log_error "无法读取 ${probe} 的磁盘空间（df 失败）"
    return 1
  fi
  avail_gb=$((avail_kb / 1048576))
  if [ "$avail_gb" -lt "$min_gb" ]; then
    log_error "磁盘空间不足：${probe} 可用 ${avail_gb}GB < 要求 ${min_gb}GB（docs/01 §2.3：/data 建议 ≥500GB）"
    return 1
  fi
  log_success "磁盘空间检查通过：${probe} 可用 ${avail_gb}GB（要求 ≥${min_gb}GB）"
}

# wait_for <cmd> <description> <timeout_seconds> [interval_seconds]
#   循环执行 cmd（shell 字符串，成功即返回 0）；超时返回 1。命令输出全部丢弃，避免污染日志。
wait_for() {
  local cmd="${1:?wait_for 需要探测命令}" description="${2:?wait_for 需要描述}" timeout_s="${3:-60}"
  local interval="${4:-2}" start_ts elapsed
  start_ts="$(date +%s)"
  log_info "等待 ${description} 就绪（最多 ${timeout_s}s，探测：${cmd}）"
  while true; do
    if eval "$cmd" >/dev/null 2>&1; then
      elapsed=$(( $(date +%s) - start_ts ))
      log_success "${description} 已就绪（耗时 ${elapsed}s）"
      return 0
    fi
    elapsed=$(( $(date +%s) - start_ts ))
    if [ "$elapsed" -ge "$timeout_s" ]; then
      log_error "${description} 在 ${timeout_s}s 内未就绪；探测命令：${cmd}"
      return 1
    fi
    sleep "$interval"
  done
}

# confirm <提示语>：非交互模式（ASSUME_YES=1）直接通过；否则读取 y/N
confirm() {
  local prompt="$1" reply=""
  if [ "${ASSUME_YES:-0}" = "1" ]; then
    log_info "非交互模式（-y）：自动确认 —— ${prompt}"
    return 0
  fi
  printf '%s [y/N] ' "$prompt"
  read -r reply || true
  case "$reply" in
    y | Y | yes | YES | Yes) return 0 ;;
    *) return 1 ;;
  esac
}

# ---------------------------------------------------------------------
# 5. .env 读取与校验（所有脚本的配置唯一入口；禁止在业务代码硬编码阈值/地址）
# ---------------------------------------------------------------------
# load_env [env_file]：加载 .env；优先级 参数 > HUNTER_ENV_FILE > ${APP_DIR}/.env。
# 文件不存在即报错，避免带着空配置静默继续。
load_env() {
  local env_file="${1:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ ! -f "$env_file" ]; then
    log_error "环境变量文件不存在：${env_file}"
    log_error "请先执行：bash ${GEN_PASSWORDS_SH}"
    return 1
  fi
  set -a
  # shellcheck source=/dev/null  # .env 由部署时生成（gen-passwords.sh），非仓库文件
  if ! . "$env_file"; then
    set +a
    log_error "加载 ${env_file} 失败：请检查语法（值含空格需加引号）"
    return 1
  fi
  set +a
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE APP_DIR DATA_DIR
  log_info "已加载环境变量：${env_file}"
}

# env_value <VAR> <默认值>：取变量值（未设置或为空时返回默认值）
env_value() {
  local name="$1" fallback="${2:-}"
  local value="${!name:-}"
  if [ -n "$value" ]; then
    printf '%s' "$value"
  else
    printf '%s' "$fallback"
  fi
}

# require_env <VAR> [VAR...]：变量必须存在、非空且不是 CHANGE_ME_* 占位
require_env() {
  local name value missing=0
  for name in "$@"; do
    value="${!name:-}"
    case "$value" in
      "" | CHANGE_ME_*)
        log_error "环境变量未配置或仍为占位值：${name}=${value:-<empty>}"
        missing=1
        ;;
    esac
  done
  if [ "$missing" -ne 0 ]; then
    log_error "请编辑 ${APP_DIR}/.env 后重试（或运行 ${GEN_PASSWORDS_SH} 重新生成口令）"
    return 1
  fi
  return 0
}

# hc_detect_server_ip：自动探测本机首个 IPv4（SERVER_IP 用于 Kafka 9093 advertised 与 SRS candidate）
hc_detect_server_ip() {
  local ip
  ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  printf '%s' "${ip:-127.0.0.1}"
}

# hc_rand_hex [字节数]：生成随机十六进制串（默认 24 字节 = 48 字符，满足 JWT ≥32 字节要求）
hc_rand_hex() {
  local bytes="${1:-24}"
  openssl rand -hex "$bytes"
}

# hc_log_begin：创建日志文件与目录（幂等，权限 640；不可写时静默降级）
hc_log_begin() {
  [ "${HC_LOG_TO_FILE:-1}" = "1" ] || return 0
  { mkdir -p "$(dirname "$LOG_FILE")" && : >>"$LOG_FILE"; } 2>/dev/null || {
    _HC_LOG_TARGET_FAILED=1
    return 0
  }
  chmod 640 "$LOG_FILE" 2>/dev/null || true
  return 0
}

# hc_run_logged <描述> <命令...>：执行命令，输出实时打印到终端并落盘到 LOG_FILE
#   期间关闭 log_* 的独立落盘（HC_LOG_TO_FILE=0），避免同一条日志被写两次
hc_run_logged() {
  local description="$1"
  shift
  local rc=0 prev="${HC_LOG_TO_FILE:-1}"
  log_info "执行${description}：$*"
  hc_log_begin
  # 日志文件不可写时（非 root 调试场景）退化为直通输出，避免 tee 报错中断流程
  if [ "${_HC_LOG_TARGET_FAILED:-0}" = "1" ]; then
    "$@" || rc=$?
    return "$rc"
  fi
  HC_LOG_TO_FILE=0
  if "$@" 2>&1 | tee -a "$LOG_FILE"; then
    rc=0
  else
    rc=$?
  fi
  HC_LOG_TO_FILE="$prev"
  return "$rc"
}

# ---------------------------------------------------------------------
# 6. Docker / Compose / 容器辅助（容器名取自 docs/01 §1.1 部署对象清单）
# ---------------------------------------------------------------------
C_POSTGRES="hunter-postgres"
C_TIMESCALE="hunter-timescale"
C_REDIS="hunter-redis"
C_ZOOKEEPER="hunter-zookeeper"
C_KAFKA="hunter-kafka"
C_MINIO="hunter-minio"
C_SRS="hunter-srs"
C_FLINK_JM="hunter-flink-jm"
C_FLINK_TM="hunter-flink-tm"
C_API_GATEWAY="hunter-api-gateway"
C_SCENE="hunter-scene"
C_COLLECTOR="hunter-collector"
C_ANALYTICS="hunter-analytics"
C_OTA="hunter-ota"
C_REMOTE="hunter-remote"
C_WEB="hunter-web"
export C_POSTGRES C_TIMESCALE C_REDIS C_ZOOKEEPER C_KAFKA C_MINIO C_SRS \
  C_FLINK_JM C_FLINK_TM C_API_GATEWAY C_SCENE C_COLLECTOR C_ANALYTICS C_OTA C_REMOTE C_WEB

# compose <args...>：在 APP_DIR 下执行 docker compose（编排文件固定为 ${APP_DIR}/docker-compose.yml）
compose() {
  (cd "$APP_DIR" && docker compose "$@")
}

# compose_service_exists <service>：编排文件中是否存在该服务（用于可选组件，如独立 timescale）
compose_service_exists() {
  local service="$1"
  (cd "$APP_DIR" && docker compose config --services 2>/dev/null) | grep -Fxq "$service"
}

# require_compose_file：编排文件必须存在（infra/deploy/docker-compose.yml 属部署包其余批次）
require_compose_file() {
  if [ ! -f "${APP_DIR}/docker-compose.yml" ]; then
    log_error "编排文件缺失：${APP_DIR}/docker-compose.yml"
    log_error "请先将部署包（infra/deploy/）完整复制到 ${APP_DIR}（docs/01 §5 目录结构、§7-①）"
    return 1
  fi
  return 0
}

# container_running <container>：容器处于运行态返回 0
container_running() {
  local state
  state="$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || printf 'false')"
  [ "$state" = "true" ]
}

# container_health <container>：输出 healthy|unhealthy|starting|none|missing
container_health() {
  local status
  status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1" 2>/dev/null || true)"
  printf '%s' "${status:-missing}"
}

# container_restart_count <container>：重启次数（日常巡检用）
container_restart_count() {
  local count
  count="$(docker inspect -f '{{.RestartCount}}' "$1" 2>/dev/null || true)"
  printf '%s' "${count:-0}"
}

# ---------------------------------------------------------------------
# 7. Kafka CLI 辅助
#    ⚠ 部署包内部监听为 SASL_PLAINTEXT（.env: KAFKA_INTERNAL_SECURITY_PROTOCOL，docs/01 §1.4⑤），
#      因此容器内执行 kafka-topics.sh / kafka-configs.sh 必须携带 --command-config；
#      策略与 infra/k8s/jobs/kafka-init-job.yaml 保持一致。
# ---------------------------------------------------------------------
# kafka_auth_args <container>：输出 kafka CLI 认证参数（每行一个，供 mapfile 使用）
#   非 PLAINTEXT 时：先把 client.properties 写入容器内，再输出 "--command-config" 与路径
kafka_auth_args() {
  local container="${1:-$C_KAFKA}"
  local protocol="${KAFKA_INTERNAL_SECURITY_PROTOCOL:-PLAINTEXT}"
  local conf_path="${KAFKA_CMD_CONFIG:-/tmp/hunter-client.properties}"
  local mech="${KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}"
  local user="${KAFKA_SASL_USER:-}" password="${KAFKA_SASL_PASSWORD:-}" extra=""
  if [ "$protocol" = "PLAINTEXT" ]; then
    return 0
  fi
  if [ "$protocol" = "SASL_SSL" ] && [ -n "${KAFKA_CMD_TRUSTSTORE_PEM:-}" ]; then
    extra="ssl.truststore.type=PEM
ssl.truststore.location=${KAFKA_CMD_TRUSTSTORE_PEM}
"
  fi
  if ! printf 'security.protocol=%s\nsasl.mechanism=%s\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";\n%s' \
    "$protocol" "$mech" "$user" "$password" "$extra" |
    docker exec -i "$container" sh -c "cat > '${conf_path}'" 2>/dev/null; then
    log_error "写入 Kafka 客户端配置失败：容器 ${container}（${conf_path}）"
    return 1
  fi
  docker exec "$container" chmod 600 "$conf_path" >/dev/null 2>&1 || true
  printf '%s\n%s\n' "--command-config" "$conf_path"
}

# kafka_tool_cli <tool> [args...]：在 Kafka 容器内执行 <tool>（自动附带认证参数 + 二进制路径回退）
#   例：kafka_tool_cli kafka-configs.sh --describe --entity-type users
kafka_tool_cli() {
  local tool="${1:?kafka_tool_cli 需要工具名（如 kafka-topics.sh）}"
  shift
  local container="${KAFKA_CONTAINER:-$C_KAFKA}"
  local -a auth=()
  mapfile -t auth < <(kafka_auth_args "$container")
  docker exec "$container" sh -c \
    'tool="$1"; shift; if command -v "$tool" >/dev/null 2>&1; then exec "$tool" "$@"; else exec "/opt/bitnami/kafka/bin/$tool" "$@"; fi' \
    hunter "$tool" "${auth[@]}" "$@"
}

# kafka_topics_cli [args...]：kafka-topics.sh 封装（bootstrap 默认容器内 localhost:9092）
kafka_topics_cli() {
  local bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
  kafka_tool_cli kafka-topics.sh --bootstrap-server "$bootstrap" "$@"
}

# kafka_broker_ready <container>：broker 是否可连接（先试 kafka-topics.sh，再退回 TCP 探测，认证无关）
kafka_broker_ready() {
  local container="${1:-$C_KAFKA}"
  if kafka_topics_cli --list >/dev/null 2>&1; then
    return 0
  fi
  docker exec "$container" sh -c 'exec 3<>/dev/tcp/127.0.0.1/9092' >/dev/null 2>&1
}

# ---------------------------------------------------------------------
# 8. 健康探针与 URL 辅助（契约 ops_endpoints = /healthz、/readyz；实现侧无 /health）
# ---------------------------------------------------------------------
HEALTH_PATH="${HEALTH_PATH:-/healthz}"

# service_health_ok <host_port> [path]：HTTP 探针（返回 0 表示 2xx/3xx）
service_health_ok() {
  local port="$1" path="${2:-$HEALTH_PATH}"
  curl -sf -o /dev/null -m 5 "http://127.0.0.1:${port}${path}"
}

# minio_health_url：MinIO 存活探针 URL（容器内 9000）
minio_health_url() {
  local port="${MINIO_API_PORT:-9000}"
  printf 'http://127.0.0.1:%s/minio/health/live' "$port"
}

# srs_health_url：SRS HTTP API 版本接口（容器内 9090，宿主机映射 127.0.0.1）
srs_health_url() {
  local port="${SRS_HTTP_API_HOST_PORT:-${SRS_HTTP_API_PORT:-9090}}"
  printf 'http://127.0.0.1:%s/api/v1/versions' "$port"
}

# flink_ui_url：Flink Web UI 宿主机地址（⚠ 容器 8081 与 scene-service 冲突，宿主机用 FLINK_UI_HOST_PORT=8088）
flink_ui_url() {
  local port="${FLINK_UI_HOST_PORT:-8088}"
  printf 'http://127.0.0.1:%s' "$port"
}

# ---------------------------------------------------------------------
# 9. 帮助信息（本文件为库文件，仅供 source；直接执行时打印说明）
# ---------------------------------------------------------------------
common_help() {
  cat <<'EOF'
HunterCore 部署脚本公共库 common.sh（库文件，禁止直接执行）

用途：
  为所有部署/运维脚本提供统一日志、检查与辅助函数：
    - 日志：log_info / log_success / log_warn / log_error（终端彩色 + 日志文件无色）
    - 检查：check_root / command_exists / check_port / check_disk_space / wait_for / confirm
    - 环境：load_env（加载 ${APP_DIR}/.env）/ env_value / require_env
    - 辅助：compose / container_health / kafka_topics_cli / hc_run_logged / service_health_ok

用法：
  . "${SCRIPT_DIR}/common.sh"      # 在脚本中 source（不要用 bash 执行）
  bash scripts/common.sh --help    # 查看本说明

关键路径：
  SCRIPT_DIR=${SCRIPT_DIR}
  APP_DIR=${APP_DIR}               # 服务器上应为 /opt/hunter-edge
  DATA_DIR=${DATA_DIR}             # 默认 /data
  LOG_FILE=${LOG_FILE}             # 默认 /var/log/hunter-edge-install.log

可覆盖环境变量：HUNTER_APP_DIR、HUNTER_DATA_DIR、HUNTER_LOG_DIR、HUNTER_INSTALL_LOG、
               HUNTER_ENV_FILE（.env 路径）、HC_LOG_TO_FILE(0/1)、NO_COLOR(1)、
               HEALTH_PATH、KAFKA_CMD_CONFIG、KAFKA_CMD_TRUSTSTORE_PEM

示例：
  #!/usr/bin/env bash
  set -euo pipefail
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  . "${SCRIPT_DIR}/common.sh"
  check_root || exit 1
  load_env || exit 1

日期：2026-09-19
EOF
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  case "${1:-}" in
    --help | -h) common_help ;;
    *) printf 'common.sh 是库文件，请通过 source 方式引用：. "%s"\n使用 --help 查看说明\n' "$0" ;;
  esac
  exit 0
fi
