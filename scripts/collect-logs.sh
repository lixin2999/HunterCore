#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 日志与系统信息收集（collect-logs.sh）
#
# 用途：一键收集部署/运行日志与系统信息，打包为单个归档，便于故障排查与工单附件：
#   ① 容器日志：docker compose logs --no-color --tail=<N> <service> → logs/<service>.log
#   ② 系统信息：uname/lsb_release/free/df/docker info/docker compose ps/docker stats
#   ③ 配置：config/ 目录 + 脱敏后的 .env（口令/密钥/Token 一律替换为 ***）
#   ④ 系统日志：journalctl -u docker --since "24 hours ago" → logs/docker-system.log
#   ⑤ 部署与运维日志：/var/log/hunter-edge*（安装日志、备份日志、巡检报告）尾部
#
# ⚠ 脱敏说明：.env 中的 *PASSWORD*/*SECRET*/*KEY*/TOKEN 值会被替换为 ***；
#   归档生成后会自检是否残留明文口令，如发现残留将给出告警（请勿直接外发）。
#
# 用法：
#   sudo bash scripts/collect-logs.sh [选项]
#
# 参数：
#   --help         显示本帮助
#   --tail <n>     每个服务收集的日志行数（默认 500）
#   --out <file>   输出归档路径（默认 /tmp/hunter-core-logs-YYYYmmdd_HHMMSS.tar.gz）
#   --env <file>   指定 .env 路径
#   --no-journal   不收集 journalctl 系统日志
#
# 示例：
#   sudo bash /opt/hunter-edge/scripts/collect-logs.sh
#   sudo bash /opt/hunter-edge/scripts/collect-logs.sh --tail 2000 --out /tmp/hc-debug.tar.gz
#
# 依赖：common.sh（同目录）、docker / docker compose、tar、journalctl（可选）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # 运行期变量由 .env 注入

ENV_FILE_ARG=""
TAIL_LINES=500
OUT_FILE=""
USE_JOURNAL=1
STAGE_DIR=""
# docker compose 服务名（与 infra/deploy/docker-compose.yml 一致；也可自动探测）
DEFAULT_SERVICES=(
  api-gateway scene-service data-collector data-analytics ota-service remote-control
  postgres timescale redis zookeeper kafka minio srs flink-jm flink-tm web-portal
)

usage() {
  cat <<'EOF'
HunterCore 日志与系统信息收集脚本

用途：
  收集容器日志、系统信息、脱敏配置、journalctl 系统日志与部署/运维日志，
  打包为单个 tar.gz 供故障排查（归档内自动完成口令脱敏并自检）。

用法：
  sudo bash scripts/collect-logs.sh [选项]

参数：
  --help        显示本帮助
  --tail <n>    每个服务收集的日志行数（默认 500）
  --out <file>  输出路径（默认 /tmp/hunter-core-logs-YYYYmmdd_HHMMSS.tar.gz）
  --env <file>  .env 路径（默认 /opt/hunter-edge/.env）
  --no-journal  跳过 journalctl 系统日志（无 systemd 环境建议使用）

示例：
  sudo bash scripts/collect-logs.sh
  sudo bash scripts/collect-logs.sh --tail 2000 --out /tmp/hc-debug.tar.gz
  sudo bash scripts/collect-logs.sh --no-journal

归档结构：
  logs/<service>.log        容器日志（--no-color）
  logs/docker-system.log    journalctl -u docker（最近 24h）
  logs/hunter-edge-*.log    部署/备份/巡检日志尾部
  system/*.txt              uname/lsb_release/free/df/docker info/compose ps/stats
  config/                   配置目录（nginx/daemon 等）
  config/.env.masked        脱敏后的环境变量文件
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --tail)
        [ $# -ge 2 ] || die "--tail 需要一个行数参数"
        case "$2" in
          '' | *[!0-9]*) die "--tail 必须为非负整数（收到：$2）" ;;
        esac
        TAIL_LINES="$2"
        shift 2
        ;;
      --out)
        [ $# -ge 2 ] || die "--out 需要一个文件路径参数"
        OUT_FILE="$2"
        shift 2
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
        shift 2
        ;;
      --no-journal)
        USE_JOURNAL=0
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

# mask_env <src> <dst>：生成脱敏副本（口令/密钥/Token 替换为 ***，URL 中的密码段替换为 ***）
#   ⚠ 使用 '#' 作为 sed 分隔符：正则内含 '|'（alternation），不可用 '|' 作分隔符
mask_env() {
  local src="$1" dst="$2"
  install -m 0600 /dev/null "$dst"
  if sed -E \
    -e 's#^([A-Za-z0-9_]*(PASSWORD|PASSWD|SECRET|ACCESS_KEY|TOKEN)[A-Za-z0-9_]*=).*$#\1***#I' \
    -e 's#^([A-Za-z0-9_]*_KEY=).*$#\1***#I' \
    -e 's#(://[^:@/]+:)[^@/]+(@)#\1***\2#g' \
    "$src" >"$dst" 2>/dev/null; then
    chmod 600 "$dst"
    return 0
  fi
  log_warn "脱敏 .env 失败（sed 报错）：${src}"
  rm -f "$dst"
  return 1
}

# collect_service_logs：按服务收集容器日志
collect_service_logs() {
  local services=() svc rc=0
  mapfile -t services < <(
    (cd "$APP_DIR" && docker compose config --services 2>/dev/null) |
      grep -E '[^[:space:]]' || true
  )
  if [ "${#services[@]}" -eq 0 ]; then
    log_warn "无法从 compose 探测服务列表（${APP_DIR}/docker-compose.yml 缺失？）：使用内置清单"
    services=("${DEFAULT_SERVICES[@]}")
  fi
  for svc in "${services[@]}"; do
    if (cd "$APP_DIR" && docker compose logs --no-color --tail="$TAIL_LINES" "$svc") \
      >"${STAGE_DIR}/logs/${svc}.log" 2>&1; then
      log_info "已收集日志：${svc}（尾 ${TAIL_LINES} 行）"
    else
      log_warn "收集日志失败（服务可能未启动）：${svc}"
      rc=1
    fi
  done
  return 0
}

# collect_system_info：系统与 Docker 信息
collect_system_info() {
  local dir="${STAGE_DIR}/system"
  install -d -m 0755 "$dir"
  { uname -a; } >"${dir}/uname.txt" 2>&1 || true
  { lsb_release -a; } >"${dir}/os-release.txt" 2>&1 || true
  { hostnamectl status; } >"${dir}/hostnamectl.txt" 2>&1 || true
  { free -h; } >"${dir}/memory.txt" 2>&1 || true
  { df -h; df -i; } >"${dir}/disk.txt" 2>&1 || true
  { nproc; uptime; } >"${dir}/cpu-uptime.txt" 2>&1 || true
  { sysctl vm.max_map_count net.core.somaxconn vm.swappiness; swapoff --version 2>/dev/null; swapon --show; } \
    >"${dir}/kernel-params.txt" 2>&1 || true
  if command_exists docker; then
    { docker version; docker info; } >"${dir}/docker-info.txt" 2>&1 || true
    { docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'; } >"${dir}/docker-ps.txt" 2>&1 || true
    { docker stats --no-stream; } >"${dir}/docker-stats.txt" 2>&1 || true
    if [ -f "${APP_DIR}/docker-compose.yml" ]; then
      { (cd "$APP_DIR" && docker compose ps); } >"${dir}/compose-ps.txt" 2>&1 || true
    fi
  else
    printf 'docker 命令不存在\n' >"${dir}/docker-info.txt"
  fi
  { ss -ltnp 2>/dev/null || netstat -ltnp 2>/dev/null; } >"${dir}/listening-ports.txt" 2>&1 || true
  log_success "已收集系统信息（system/）"
  return 0
}

# collect_logs_dir：部署/备份/巡检日志尾部
collect_logs_dir() {
  local src="${LOG_DIR}" f base
  if [ ! -d "$src" ]; then
    log_warn "宿主机日志目录不存在：${src}（跳过）"
    return 0
  fi
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    base="$(basename "$f")"
    tail -n 1000 "$f" >"${STAGE_DIR}/logs/${base}.tail" 2>/dev/null || true
  done < <(find "$src" -maxdepth 2 -type f -name '*.log' -print 2>/dev/null | head -n 50 || true)
  if [ -f "$LOG_FILE" ]; then
    tail -n 1000 "$LOG_FILE" >"${STAGE_DIR}/logs/$(basename "$LOG_FILE").tail" 2>/dev/null || true
  fi
  log_success "已收集部署/运维日志尾部（logs/*.tail）"
  return 0
}

# collect_journal：journalctl -u docker（最近 24 小时）
collect_journal() {
  if [ "$USE_JOURNAL" -eq 0 ]; then
    log_info "已按 --no-journal 跳过 journalctl 收集"
    return 0
  fi
  if ! command_exists journalctl; then
    log_warn "journalctl 不可用（容器环境常见）：跳过系统日志收集"
    return 0
  fi
  if journalctl -u docker --since "24 hours ago" --no-pager >"${STAGE_DIR}/logs/docker-system.log" 2>&1; then
    log_success "已收集 journalctl -u docker（最近 24 小时）"
  else
    log_warn "journalctl 收集失败（可能无权限或非 systemd 环境）"
    rm -f "${STAGE_DIR}/logs/docker-system.log"
  fi
  return 0
}

# scan_secrets：归档自检（是否残留明文口令/密钥），防止误外发
#   规则：行首锚定（排除注释行）→ 跳过 CHANGE_ME_* 占位值 → 跳过已脱敏的 *** 值
scan_secrets() {
  local hits=() line
  while IFS= read -r line; do
    case "$line" in
      *CHANGE_ME_*) continue ;;
      *'=***'*) continue ;;
    esac
    hits+=("$line")
  done < <(grep -rEh \
    '^[A-Za-z0-9_]*(PASSWORD|PASSWD|SECRET|ACCESS_KEY|TOKEN)[A-Za-z0-9_]*=[A-Za-z0-9!@#$%^&*_+.-]{12,}' \
    "$STAGE_DIR" 2>/dev/null || true)

  if [ "${#hits[@]}" -gt 0 ]; then
    log_error "归档中疑似残留明文口令/密钥，禁止外发（共 ${#hits[@]} 处，最多展示 5 处）："
    printf '%s\n' "${hits[@]:0:5}" | sed 's/^/    /'
    log_error "请核查上述文件（本脚本仅自动脱敏 .env；容器日志/配置可能含业务侧打印的敏感信息）"
    return 1
  fi
  log_success "脱敏自检通过：未发现明文口令/密钥残留"
  return 0
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file ts rc=0
  parse_args "$@"
  hc_log_begin

  if ! command_exists tar; then
    log_error "缺少 tar：apt-get install -y tar"
    return 1
  fi
  ts="$(date '+%Y%m%d_%H%M%S')"
  OUT_FILE="${OUT_FILE:-/tmp/hunter-core-logs-${ts}.tar.gz}"
  STAGE_DIR="$(mktemp -d "/tmp/hunter-core-collect-${ts}-XXXX")"
  # trap 清理临时目录（归档成功与否都清理，避免 /tmp 堆积）
  trap 'rm -rf "${STAGE_DIR:-}"' EXIT
  install -d -m 0755 "${STAGE_DIR}/logs" "${STAGE_DIR}/system" "${STAGE_DIR}/config"

  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ -f "$env_file" ]; then
    load_env "$env_file" >/dev/null 2>&1 || log_warn "加载 ${env_file} 失败：继续收集（配置脱敏仍会执行）"
  else
    log_warn ".env 不存在（${env_file}）：跳过环境变量收集"
  fi

  log_info "===== HunterCore 日志收集（开始：$(date '+%Y-%m-%d %H:%M:%S')）====="
  log_info "临时目录：${STAGE_DIR}；目标归档：${OUT_FILE}"

  # ① 容器日志
  collect_service_logs
  # ② 系统信息
  collect_system_info
  # ③ 配置（含脱敏 .env）；使用 src/. 语义把内容平铺到 config/，避免生成 config/config/
  if [ -d "${APP_DIR}/config" ]; then
    cp -a "${APP_DIR}/config/." "${STAGE_DIR}/config/" 2>/dev/null || log_warn "复制 config/ 失败"
  fi
  if [ -f "$env_file" ]; then
    if mask_env "$env_file" "${STAGE_DIR}/config/$(basename "$env_file").masked"; then
      log_success "已生成脱敏环境变量：config/$(basename "$env_file").masked"
    else
      log_warn "脱敏环境变量生成失败：归档中将不包含 $(basename "$env_file").masked（请人工核查）"
    fi
  fi
  # ④ journalctl
  collect_journal
  # ⑤ 部署与运维日志
  collect_logs_dir

  # 归档元信息
  {
    printf 'Host: %s\n' "$(hostname)"
    printf 'CollectedAt: %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
    printf 'APP_DIR: %s\n' "$APP_DIR"
    printf 'DATA_DIR: %s\n' "$DATA_DIR"
    printf 'LOG_DIR: %s\n' "$LOG_DIR"
    printf 'TailLines: %s\n' "$TAIL_LINES"
    printf 'ScriptVersion: collect-logs.sh (2026-09-19)\n'
  } >"${STAGE_DIR}/MANIFEST.txt"

  # ⑥ 脱敏自检（失败仅告警，不阻止打包，便于人工判断）
  scan_secrets || rc=1

  # ⑦ 打包
  if ! tar czf "$OUT_FILE" -C "$STAGE_DIR" . 2>"${STAGE_DIR}/tar.err"; then
    log_error "打包失败：$(tail -n3 "${STAGE_DIR}/tar.err" 2>/dev/null)"
    return 1
  fi
  chmod 600 "$OUT_FILE"
  log_success "归档完成：${OUT_FILE}"
  log_info "归档大小：$(du -h "$OUT_FILE" | cut -f1)；包含 $(find "$STAGE_DIR" -type f | wc -l) 个文件"
  if [ "$rc" -ne 0 ]; then
    log_warn "归档已完成但脱敏自检存在告警：外发前请人工复核内容（勿直接发送原始日志）"
  fi
  log_warn "归档可能含车辆标识/业务数据：请按数据安全要求流转（MinIO 存储日志生命周期 30 天）"
  return 0
}

main "$@"