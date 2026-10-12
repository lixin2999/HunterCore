#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署脚本集 —— 公共库（被其他脚本 source，禁止直接执行）
#
# 用途：统一日志（终端 + /var/log/hunter-core-install.log）、root 校验、命令/端口/磁盘检查、
#       通用等待、.env 加载、容器与 Kafka CLI 辅助。所有部署脚本必须 source 本文件，
#       禁止在各自脚本内重复实现日志与检查逻辑。
#
# 用法：. "${SCRIPT_DIR}/common.sh"          # 在业务脚本中 source（第一行之后立即加载）
#       bash scripts/common.sh --help        # 查看本文件说明（本文件不执行实际动作）
#
# 关键路径（与 docs/01-部署概述与环境要求.md §1.2/§5、infra/deploy/README.md 一致）：
#   应用根目录 APP_DIR = 脚本目录的上级目录（服务器上 = /opt/hunter-core）
#   数据根目录 /data、宿主机日志目录 /var/log/hunter-core
#   ⚠ 早期资料中曾使用 /opt/HunterCore 等写法；本脚本集统一采用 /opt/hunter-core，
#     如需改基准，设置环境变量 HUNTER_APP_DIR 覆盖。
#
# 可覆盖的环境变量（均可选，便于本地/非 root 环境验证）：
#   HUNTER_APP_DIR   应用根目录（默认 = SCRIPT_DIR 的上级）
#   HUNTER_DATA_DIR  数据根目录（默认 /data）
#   HUNTER_LOG_DIR   宿主机日志目录（默认 /var/log/hunter-core）
#   HUNTER_INSTALL_LOG 部署日志文件（默认 /var/log/hunter-core-install.log）
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

# HC_COMPOSE_ARGS：docker compose 的 -f / --project-directory 参数
# 设计意图：当 APP_DIR 是 git 仓根目录（含 infra/deploy/）时，优先使用生产编排（含业务服务 build 段），
# 避免误用根目录的开发版 docker-compose.yml（导致 Step 6 "No services to build" 等问题）。
# 纯部署包模式（无 infra/deploy/）则使用 APP_DIR 根目录的 docker-compose.yml，行为不变。
HC_COMPOSE_ARGS=()
if [ -f "${APP_DIR}/infra/deploy/docker-compose.yml" ]; then
  HC_COMPOSE_ARGS=(-f "infra/deploy/docker-compose.yml" "--project-directory" ".")
fi
# DATA_DIR：有状态数据根目录（docs/01 §5）
DATA_DIR="${HUNTER_DATA_DIR:-/data}"
# LOG_DIR：宿主机侧日志目录；LOG_FILE：脚本日志文件（可用 hc_set_log_file 覆盖）
LOG_DIR="${HUNTER_LOG_DIR:-/var/log/hunter-core}"
LOG_FILE="${HUNTER_INSTALL_LOG:-/var/log/hunter-core-install.log}"
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
# hc_set_log_file <path>：切换本进程日志文件（如 health-check.sh 写入 /var/log/hunter-core/check/）
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

# check_port <port> [quiet]：端口未被占用返回 0；被占用返回 1
#   默认把占用明细按 ERROR 打印（供 require_port_free 这类“占用即故障”的调用方）。
#   传 quiet 时只回传状态、明细写 stdout，**由调用方定级**：install.sh 的端口预检有
#   “本项目容器占用视为通过”的豁免分支，若此处先打 ERROR，一次正常续跑会刷出多条
#   假故障（V1.19.17 服务器实录：`[ERROR] 端口 80 已被占用` 紧跟 `[INFO] …视为通过`）。
check_port() {
  local port="${1:?check_port 需要端口参数}" quiet="${2:-}" detail=""
  if command_exists ss; then
    detail="$(ss -Hltn "sport = :${port}" 2>/dev/null | head -n3)"
  elif command_exists netstat; then
    detail="$(netstat -ltn 2>/dev/null | awk -v p=":${port}" '$4 ~ p"$" {print}' | head -n3)"
  elif command_exists lsof; then
    detail="$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | head -n3)"
  else
    # quiet 模式下本函数处于命令替换中（stdout 被捕获），提醒必须走 stderr，否则会被默默吞掉
    if [ "$quiet" = "quiet" ]; then
      log_warn "缺少 ss/netstat/lsof，跳过端口 ${port} 占用检查（请安装 iproute2）" >&2
    else
      log_warn "缺少 ss/netstat/lsof，跳过端口 ${port} 占用检查（请安装 iproute2）"
    fi
    return 0
  fi
  if [ -n "$detail" ]; then
    if [ "$quiet" = "quiet" ]; then
      printf '端口 %s 已被占用：%s\n' "$port" "$(printf '%s' "$detail" | tr '\n' ';')"
    else
      log_error "端口 ${port} 已被占用：$(printf '%s' "$detail" | tr '\n' ';')"
    fi
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
# hc_normalize_env_file <env_file>：把 CRLF 换行归一为 LF（幂等）
#   背景：.env 若为 CRLF（Windows 编辑或直接从 Windows 工作区复制），sourcing 后变量值会带 \r，
#         造成 "load_env 失败"、LOG_DIR 变成 "/var/log/hunter-core\r" 等疑难故障，故在加载前自动修复。
hc_normalize_env_file() {
  local env_file="$1"
  [ -f "$env_file" ] || return 0
  if ! grep -qU $'\r' "$env_file" 2>/dev/null; then
    return 0
  fi
  log_warn "检测到 CRLF 换行：${env_file}（Windows 编辑/复制所致），自动转换为 LF"
  if ! sed -i 's/\r$//' "$env_file"; then
    log_error "自动转换失败（文件只读？）：请执行 sed -i 's/\\r\$//' ${env_file} 后重试"
    return 1
  fi
  log_success "已规范化为 LF：${env_file}"
  return 0
}

# load_env [env_file]：加载 .env；优先级 参数 > HUNTER_ENV_FILE > ${APP_DIR}/.env。
# 文件不存在即报错，避免带着空配置静默继续；CRLF 自动归一为 LF。
load_env() {
  local env_file="${1:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ ! -f "$env_file" ]; then
    log_error "环境变量文件不存在：${env_file}"
    log_error "请先执行：bash ${GEN_PASSWORDS_SH}"
    return 1
  fi
  hc_normalize_env_file "$env_file" || return 1
  # ℞ V1.19.13：先区分“不可读”与“语法错”。.env 由 install.sh / gen-passwords.sh 落为 **600、属主 root**，
  #   非 root 执行时 `[ -f ]` 仍为真、`source` 报 Permission denied，而旧文案只说“请检查语法”，
  #   会把运维带去改 .env 而不是补 sudo（现场实录：由此得到三项假 FAIL）。
  if [ ! -r "$env_file" ]; then
    log_error "环境变量文件不可读：${env_file}（权限 600、只有属主 root 可读，当前 UID=$(id -u) 非 root）"
    log_error "请改用 root 执行：sudo bash ${0##*/} …（不要用 chmod 放宽 .env 权限）"
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
  # 运维级覆盖（HUNTER_*）优先级高于 .env 内的同名配置：便于在不改 .env 的情况下迁移日志/数据目录
  if [ -n "${HUNTER_LOG_DIR:-}" ]; then
    LOG_DIR="$HUNTER_LOG_DIR"
  fi
  if [ -n "${HUNTER_DATA_DIR:-}" ]; then
    DATA_DIR="$HUNTER_DATA_DIR"
  fi
  if [ -n "${HUNTER_INSTALL_LOG:-}" ]; then
    LOG_FILE="$HUNTER_INSTALL_LOG"
  fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE APP_DIR DATA_DIR LOG_DIR LOG_FILE
  log_info "已加载环境变量：${env_file}"
}

# env_creds_missing：输出当前为空的口令类变量名（空格分隔；全齐则输出空串）
# ℞ V1.19.13：.env 权限 **600**（install.sh / gen-passwords.sh 设定），**非 root 根本读不到**。
#   而 `[ -f .env ]` 只看目录权限、照样为真，于是 load_env 在“存在但不可读”处失败；
#   health-check 过去只把这句归成一句 log_warn 就继续跑，于是 Redis PING / Kafka CLI / MinIO mc
#   三项全按“内置默认值”跑出 **FAIL**，把“没带凭据”报成“服务挂了”。口令缺失必须先被看出来。
env_creds_missing() {
  local name missing=""
  for name in REDIS_PASSWORD KAFKA_SASL_USER KAFKA_SASL_PASSWORD MINIO_ROOT_USER \
              MINIO_ROOT_PASSWORD POSTGRES_PASSWORD; do
    case "${!name:-}" in
      "" | CHANGE_ME_*) missing="${missing}${missing:+ }${name}" ;;
    esac
  done
  printf '%s' "$missing"
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

# hc_cert_san_entries <证书.pem>：输出证书 SAN 条目（每行一条，统一为 IP:x / DNS:x）
# 不同 openssl 版本写法不一致（1.x “IP Address:x”、3.x “IP:x”、部分构建“IPAddress:x”），
# 不归一就会把已存在的地址误判为缺失（例：反复重签 broker 证书），故在此统一归一。
hc_cert_san_entries() {
  local pem="${1:-}"
  [ -f "$pem" ] || return 0
  command_exists openssl || return 0
  openssl x509 -in "$pem" -noout -ext subjectAltName 2>/dev/null | tail -n +2 | tr -d ' \r' |
    tr ',' '\n' | sed -E 's/^IP[[:space:]]*Address:/IP:/I' | sed '/^$/d' || true
  return 0
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

# compose <args...>：在 APP_DIR 下执行 docker compose（自动选择生产或部署包编排文件）
compose() {
  (cd "$APP_DIR" && docker compose ${HC_COMPOSE_ARGS[@]+"${HC_COMPOSE_ARGS[@]}"} "$@")
}

# compose_service_exists <service>：编排文件中是否存在该服务（用于可选组件，如独立 timescale）
compose_service_exists() {
  local service="$1"
  (cd "$APP_DIR" && docker compose ${HC_COMPOSE_ARGS[@]+"${HC_COMPOSE_ARGS[@]}"} config --services 2>/dev/null) | grep -Fxq "$service"
}

# require_compose_file：编排文件必须存在（仓库存放时使用 infra/deploy/，部署包模式使用根目录）
require_compose_file() {
  if [ ! -f "${APP_DIR}/docker-compose.yml" ] && \
     [ ! -f "${APP_DIR}/infra/deploy/docker-compose.yml" ]; then
    log_error "编排文件缺失：${APP_DIR}/docker-compose.yml 或 ${APP_DIR}/infra/deploy/docker-compose.yml"
    log_error "请先将部署包（infra/deploy/）完整复制到 ${APP_DIR}（docs/01 §5 目录结构、§7-①）"
    return 1
  fi
  # 提示：若使用 infra/deploy/ 生产编排，相对路径通过 --project-directory . 解析到 APP_DIR
  if [ -f "${APP_DIR}/infra/deploy/docker-compose.yml" ]; then
    log_info "使用生产编排文件：${APP_DIR}/infra/deploy/docker-compose.yml"
  else
    log_info "使用编排文件：${APP_DIR}/docker-compose.yml"
  fi
  return 0
}

# require_no_production_override：生产降级硬闸（G-03，V1.15.0 风险⑩）
# docker-compose.override.yml 会被 compose 自动静默合并（解除 TLS 强制/宿主机端口暴露/
# 资源限制下调），等同生产环境无审批降级 —— staging/prod 存在即失败。
# 本地联调可显式 HUNTER_ALLOW_OVERRIDE=1 放行（须登记书面豁免，见故障排查指南）
require_no_production_override() {
  local override="${APP_DIR}/docker-compose.override.yml"
  if [ ! -f "$override" ]; then
    return 0
  fi
  if [ "${HUNTER_ALLOW_OVERRIDE:-0}" = "1" ]; then
    log_warn "存在 docker-compose.override.yml，已经 HUNTER_ALLOW_OVERRIDE=1 显式放行（仅限联调环境；生产须书面豁免并尽快移除）"
    return 0
  fi
  log_error "硬闸（G-03）：检测到 ${override}"
  log_error "compose 会静默合并其中的降级配置（TLS/端口/资源），禁止在生产部署包中保留 override"
  log_error "处置：删除或改名该文件；确需联调请在受控环境显式 export HUNTER_ALLOW_OVERRIDE=1"
  return 1
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
  # 回退默认与 broker 实际监听对齐：infra/deploy/docker-compose.yml 将 INTERNAL(9092) 硬编码为 SASL_PLAINTEXT，
  # 故即使 .env 未显式设置 KAFKA_INTERNAL_SECURITY_PROTOCOL，也必须按 SASL_PLAINTEXT 附带 --command-config，
  # 否则 kafka-topics.sh 以明文连要求 SCRAM 的 broker，createTopics 会报 "Timed out waiting for a node assignment"。
  local protocol="${KAFKA_INTERNAL_SECURITY_PROTOCOL:-SASL_PLAINTEXT}"
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

# kafka_tool_cli <tool> [args...]：在 Kafka 容器内执行 <tool>（自动附带 **认证** 参数 + 二进制路径回退）
#   例：kafka_tool_cli kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users
# ℞ V1.19.12：本函数**只补 --command-config，不补 --bootstrap-server**，而后者对
#   kafka-topics/consumer-groups/get-offsets/configs 全部是 **REQUIRED**。直接调用而漏它时，
#   工具会把「Missing required argument "[bootstrap-server]"」+ 整份 usage 打到 stderr 并以
#   退出码 2 结束——**与集群健康无关**，却极易被读成“消费组查不到/Kafka 有问题”。
#   运维命令请一律走下面的 kafka_topics_cli / kafka_groups_cli / kafka_offsets_cli /
#   kafka_configs_cli 四个封装（它们自动附带 --bootstrap-server）。
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

# kafka_groups_cli [args...]：kafka-consumer-groups.sh 封装（--describe / --reset-offsets / --list）
kafka_groups_cli() {
  local bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
  kafka_tool_cli kafka-consumer-groups.sh --bootstrap-server "$bootstrap" "$@"
}

# kafka_offsets_cli [args...]：kafka-get-offsets.sh 封装（查 Topic 末端水位，判断“有没有数据”）
kafka_offsets_cli() {
  local bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
  kafka_tool_cli kafka-get-offsets.sh --bootstrap-server "$bootstrap" "$@"
}

# kafka_configs_cli [args...]：kafka-configs.sh 封装（Topic 配置 / SCRAM 账号）
kafka_configs_cli() {
  local bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
  kafka_tool_cli kafka-configs.sh --bootstrap-server "$bootstrap" "$@"
}

# kafka_broker_ready <container>：broker 是否可连接（先试 kafka-topics.sh，再退回端口探活，认证无关）
kafka_broker_ready() {
  local container="${1:-$C_KAFKA}"
  if kafka_topics_cli --list >/dev/null 2>&1; then
    return 0
  fi
  # 端口探活兜底（认证无关）：优先 nc（与 compose healthcheck 一致）；
  # bitnami 镜像 /bin/sh 为 dash，不支持 /dev/tcp，故退回显式用 bash 探测。
  docker exec "$container" sh -c 'command -v nc >/dev/null 2>&1 && nc -z localhost 9092' >/dev/null 2>&1 && return 0
  docker exec "$container" bash -c 'exec 3<>/dev/tcp/127.0.0.1/9092' >/dev/null 2>&1
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
# 9. MinIO CLI（mc）与 Kafka 消费积压辅助
#   minio/mc 是独立镜像（minio 服务镜像内不含 mc）：
#     ① 若目标容器内有 mc（自定义镜像）→ 直接 docker exec；
#     ② 否则以一次性容器执行，--network container:<minio> 复用其网络命名空间，
#        因此不依赖 compose 网络命名（避免 project 前缀差异），127.0.0.1:9000 即 MinIO。
# ---------------------------------------------------------------------
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:RELEASE.2024-06-13T16-39-23Z}"  # 与仓库 docker-compose minio-init 同版本
export MINIO_MC_IMAGE

# minio_mc <args...>：执行 mc 子命令（自动注入 MC_HOST_local，需 MINIO_ROOT_USER/PASSWORD）
minio_mc() {
  local container="${MINIO_CONTAINER:-$C_MINIO}"
  local port="${MINIO_API_PORT:-9000}"
  local user="${MINIO_ROOT_USER:-}" password="${MINIO_ROOT_PASSWORD:-}"
  local host_url
  if [ -z "$user" ] || [ -z "$password" ]; then
    log_error "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD 未配置（请检查 .env）"
    return 1
  fi
  host_url="http://${user}:${password}@127.0.0.1:${port}"
  if docker exec "$container" sh -c 'command -v mc >/dev/null 2>&1' 2>/dev/null; then
    docker exec -e "MC_HOST_local=${host_url}" "$container" mc "$@"
    return $?
  fi
  docker run --rm \
    --network "container:${container}" \
    --entrypoint mc \
    -e "MC_HOST_local=${host_url}" \
    "$MINIO_MC_IMAGE" "$@"
}

# minio_bucket_count：当前 Bucket 数量（**没查到一律 -1**，禁止与“确实是 0 个桶”混为一谈）
minio_bucket_count() {
  local out buckets lines
  # ℞ V1.19.13：原来只吃 stdout（stderr 丢弃）→ mc 的报错文本、镜像未拉取、凭据为空都会
  #   变成“空输出”或“一行噪声”，调用方读不出是“没查到”。改 2>&1 + 形态门禁：
  #   mc ls 的桶行以 / 结尾；一行桶行都没有就是“没查到”（-1），不假定成功。
  out="$(minio_mc ls local 2>&1 || true)"
  buckets="$(printf '%s\n' "$out" | grep -cE '/$' || true)"
  lines="$(printf '%s\n' "$out" | grep -cE '[^[:space:]]' || true)"
  if [ "${buckets:-0}" -gt 0 ] && [ "${buckets:-0}" -le "${lines:-0}" ]; then
    printf '%s' "$buckets"
    return 0
  fi
  printf '%s' "-1"
  return 1
}

# kafka_consumer_lag <group_id>：该消费组所有分区 LAG 之和（消费组不存在/查不到输出 -1 并返回 1）
kafka_consumer_lag() {
  local group="${1:?kafka_consumer_lag 需要消费组名}"
  local out total
  # ℞ V1.19.12①：原为 kafka_tool_cli …，缺 --bootstrap-server（REQUIRED）→ 命令从未成功执行过，
  #   health-check.sh ⑩ 的“Kafka 消费积压”项因此恒为固定结论（WARN「尚无 offset 提交」），
  #   把“运维命令写错”误报成“车辆未上报/服务未消费”。改走 kafka_groups_cli 封装。
  out="$(kafka_groups_cli --describe --group "$group" 2>/dev/null || true)"
  if [ -z "$out" ] || printf '%s' "$out" | grep -q "does not exist"; then
    printf '%s' "-1"
    return 1
  fi
  # ℞ V1.19.12②：必须区分“LAG 真的是 0”与“输出根本不是 describe 结果”（认证失败/参数错/工具
  #   异常都会往 stdout 落一段文字，旧实现逐行找不到 LAG 列即求和得 0 → 巡检报 **PASS LAG=0** 假绿）。
  #   真结果必以表头行 GROUP…TOPIC…LAG… 开头；无表头一律按“没查到”报 -1，禁止当 0。
  if ! printf '%s\n' "$out" | grep -qE '^GROUP[[:space:]]+TOPIC'; then
    printf '%s' "-1"
    return 1
  fi
  total="$(printf '%s\n' "$out" | awk '
    NR==1 { for (i = 1; i <= NF; i++) if ($i == "LAG") col = i; next }
    col && $col ~ /^[0-9-]+$/ { sum += $col }
    END { printf "%d", sum + 0 }')"
  printf '%s' "${total:-0}"
}

# kafka_topic_count：Topic 总数（**没查到一律 -1**）
kafka_topic_count() {
  local out names lines
  # ℞ V1.19.13：旧实现“非空行数即 Topic 数”，而 kafka-topics.sh 失败时往 stdout 落的
  #   **一行报错文本**会被数成“1 个 Topic”→ 巡检报「Kafka Topic 数 1 < 6（契约内部 Topic
  #   未创建完整）」，把“认证不可用/.env 未加载”误报成“Topic 没建”（本仓库第五例同类假信号）。
  #   现加形态门禁：--list 的合法输出每行只能是 Topic 名（字母数字与 . _ -），任一行不符即 -1。
  out="$(kafka_topics_cli --list 2>&1 || true)"
  names="$(printf '%s\n' "$out" | grep -E '^[A-Za-z0-9._-]+$' || true)"
  lines="$(printf '%s\n' "$out" | grep -cE '[^[:space:]]' || true)"
  if [ "${lines:-0}" -le 0 ]; then
    printf '%s' "-1"
    return 1
  fi
  if [ "$(printf '%s\n' "$names" | grep -cE '[^[:space:]]' || true)" != "$lines" ]; then
    printf '%s' "-1"
    return 1
  fi
  printf '%s' "$lines"
}

# ---------------------------------------------------------------------
# 10. 帮助信息（本文件为库文件，仅供 source；直接执行时打印说明）
# ---------------------------------------------------------------------
common_help() {
  cat <<'EOF'
HunterCore 部署脚本公共库 common.sh（库文件，禁止直接执行）

用途：
  为所有部署/运维脚本提供统一日志、检查与辅助函数：
    - 日志：log_info / log_success / log_warn / log_error（终端彩色 + 日志文件无色）
    - 检查：check_root / command_exists / check_port（可 quiet 交调用方定级）/ check_disk_space / wait_for / confirm
    - 环境：load_env（加载 ${APP_DIR}/.env）/ env_value / require_env / env_creds_missing
      （⚠ .env 权限 600、仅 root 可读：非 root 跑口令类探针会因 load_env 失败而得到**错误结论**）
    - 辅助：compose / container_health / hc_run_logged / service_health_ok
    - Kafka CLI 封装（**运维请优先用这四个，它们自动附带 --bootstrap-server**）：
      kafka_topics_cli / kafka_groups_cli / kafka_offsets_cli / kafka_configs_cli；
      低层入口 kafka_tool_cli 只补 --command-config（认证），不补 --bootstrap-server（REQUIRED）；
      另有 kafka_consumer_lag / kafka_topic_count / kafka_broker_ready / kafka_auth_args

用法：
  . "${SCRIPT_DIR}/common.sh"      # 在脚本中 source（不要用 bash 执行）
  bash scripts/common.sh --help    # 查看本说明

关键路径：
  SCRIPT_DIR=${SCRIPT_DIR}
  APP_DIR=${APP_DIR}               # 服务器上应为 /opt/hunter-core
  DATA_DIR=${DATA_DIR}             # 默认 /data
  LOG_FILE=${LOG_FILE}             # 默认 /var/log/hunter-core-install.log

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
