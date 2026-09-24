#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 环境变量口令生成（gen-passwords.sh）
#
# 用途：
#   1) 若 ${APP_DIR}/.env 不存在 → 从 .env.example 复制生成（权限 600）；
#   2) 把所有值为 CHANGE_ME_* 的变量替换为强随机值（openssl rand -hex 24 = 48 字符）；
#   3) SERVER_IP 取 --ip 参数 / 已有值 / 自动探测（hostname -I 首个地址），并联动修正
#      内嵌占位（RC_PUBLIC_WS_BASE_URL、RC_STUN_URLS）与 CORS_ORIGINS；
#   4) 敏感项写入 ${APP_DIR}/passwords.txt（权限 600）。
#
# 幂等：已有值不覆盖（仅 --force 重新生成全部口令/密钥）。
# 安全：口令明文仅打印到终端，不写入日志文件（开发规范「日志脱敏」）。
#
# 用法：
#   sudo bash scripts/gen-passwords.sh [选项]
#
# 参数：
#   --help             显示本帮助
#   --ip <address>     指定 SERVER_IP（默认自动探测；写入 .env 并联动 WS/STUN/CORS）
#   --env <file>       指定 .env 路径（等价于 HUNTER_ENV_FILE）
#   --force            重新生成全部口令/密钥（⚠ 覆盖现有值，需同步更新依赖方与车端）
#   -y, --yes          非交互（--force 时跳过确认）
#
# 示例：
#   sudo bash /opt/hunter-core/scripts/gen-passwords.sh --ip 192.168.1.10
#   sudo bash /opt/hunter-core/scripts/gen-passwords.sh --force -y      # 口令轮换
#
# 依赖：common.sh（同目录）、openssl
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # 运行期变量由 .env 注入（MINIO_*/KAFKA_* 等）

# 视为"口令/密钥"的变量名模式（--force 时重新生成；正常模式仅处理 CHANGE_ME_* 占位）
PASSWORD_VAR_PATTERN='^[A-Z0-9_]*(PASSWORD|SECRET|SECRET_KEY|ACCESS_KEY|_KEY)='
# 值内嵌 SERVER_IP 占位、需要联动替换的变量（Kafka/WebRTC/前端来源）
SERVER_IP_EMBEDDED_VARS="RC_PUBLIC_WS_BASE_URL RC_STUN_URLS"
ASSUME_YES=0
FORCE=0
OPT_IP=""
ENV_FILE_ARG=""

usage() {
  cat <<'EOF'
HunterCore 环境变量口令生成脚本

用途：
  生成 ${APP_DIR}/.env 中的强随机口令与密钥（幂等：已有值不覆盖），
  并联动设置 SERVER_IP / CORS_ORIGINS / RC_PUBLIC_WS_BASE_URL / RC_STUN_URLS；
  同时输出 ${APP_DIR}/passwords.txt（权限 600，含全部敏感项）。

用法：
  sudo bash scripts/gen-passwords.sh [选项]

参数：
  --help           显示本帮助
  --ip <address>   SERVER_IP（Kafka 9093 advertised / SRS WebRTC candidate / 证书 SAN 依赖）
  --env <file>     .env 路径（默认 /opt/hunter-core/.env）
  --force          重新生成全部口令/密钥（覆盖现有值；需同步更新依赖方与车端配置）
  -y, --yes        非交互模式

示例：
  sudo bash scripts/gen-passwords.sh --ip 192.168.1.10
  sudo bash scripts/gen-passwords.sh --force -y

生成后：
  · 口令清单：${APP_DIR}/passwords.txt（600，请立即离线保存）
  · 校验：bash scripts/health-check.sh（会用 .env 口令连通 PG/Redis/MinIO）
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --ip)
        [ $# -ge 2 ] || die "--ip 需要一个 IP 地址参数"
        OPT_IP="$2"
        shift 2
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
        shift 2
        ;;
      --force)
        FORCE=1
        shift
        ;;
      -y | --yes)
        ASSUME_YES=1
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

# resolve_env_file：确定 .env 路径（参数 > HUNTER_ENV_FILE > ${APP_DIR}/.env）
resolve_env_file() {
  printf '%s' "${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
}

# ensure_env_file <env_file>：不存在则从模板复制（服务器优先 ${APP_DIR}/.env.example）
ensure_env_file() {
  local env_file="$1" example="${APP_DIR}/.env.example"
  if [ -f "$env_file" ]; then
    # 已存在：仅做换行规范化（CRLF 会导致变量值带 \r，引发疑难故障）
    hc_normalize_env_file "$env_file" || return 1
    return 0
  fi
  if [ ! -f "$example" ] && [ -f "${APP_DIR}/infra/deploy/.env.example" ]; then
    example="${APP_DIR}/infra/deploy/.env.example"
  fi
  if [ ! -f "$example" ]; then
    log_error "环境变量模板缺失：${APP_DIR}/.env.example（或 infra/deploy/.env.example）"
    return 1
  fi
  install -m 0600 "$example" "$env_file"
  hc_normalize_env_file "$env_file" || return 1
  log_success "已从模板生成 .env：${example} → ${env_file}（权限 600，换行已规范为 LF）"
}

# resolve_server_ip <env_file>：--ip > 现有非占位值 > 自动探测
resolve_server_ip() {
  local env_file="$1" current=""
  current="$(grep -E '^SERVER_IP=' "$env_file" | head -n1 | cut -d= -f2- | awk '{print $1}' || true)"
  if [ -n "$OPT_IP" ]; then
    printf '%s' "$OPT_IP"
    return 0
  fi
  case "$current" in
    "" | CHANGE_ME_*) hc_detect_server_ip ;;
    *) printf '%s' "$current" ;;
  esac
}

# set_var_value <file> <VAR> <value>：替换变量值并保留行尾注释（不打印明文）
set_var_value() {
  local file="$1" var="$2" value="$3"
  if grep -qE "^${var}=" "$file"; then
    sed -i -E "s|^(${var}=)[^#]*(#.*)?\$|\1${value} \2|" "$file"
    return 0
  fi
  printf '%s=%s\n' "$var" "$value" >>"$file"
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file server_ip var new_value count=0 remain=0 passwords_file prev_log
  parse_args "$@"
  hc_log_begin

  if ! command_exists openssl; then
    log_error "缺少 openssl（口令生成依赖）：请执行 apt-get install -y openssl"
    return 1
  fi

  env_file="$(resolve_env_file)"
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE
  log_info "===== HunterCore 口令生成（.env：${env_file}）====="
  ensure_env_file "$env_file" || return 1
  chmod 600 "$env_file"

  if [ "$FORCE" -eq 1 ]; then
    log_warn "--force：将重新生成全部口令/密钥（PostgreSQL/Redis/MinIO/Kafka/JWT），现有值会被覆盖"
    if ! confirm "确认重新生成全部口令？"; then
      log_info "用户取消：未修改 ${env_file}"
      return 0
    fi
  fi

  # ---------- 1) 占位值 CHANGE_ME_*（首次生成；已有值跳过以保证幂等） ----------
  server_ip="$(resolve_server_ip "$env_file")"
  while IFS= read -r var; do
    case "$var" in
      SERVER_IP) new_value="$server_ip" ;;
      CORS_ORIGINS) new_value="http://${server_ip}" ;;
      *) new_value="$(hc_rand_hex 24)" ;;
    esac
    set_var_value "$env_file" "$var" "$new_value"
    count=$((count + 1))
    log_success "已生成占位变量：${var}"
  done < <(grep -oE '^[A-Za-z0-9_]+=CHANGE_ME_[A-Za-z0-9_]*' "$env_file" | cut -d= -f1)
  if [ "$count" -eq 0 ]; then
    log_info "未发现 CHANGE_ME_* 占位值（幂等）：跳过口令生成"
  fi

  # ---------- 2) SERVER_IP 及其内嵌占位联动（WS/STUN/CORS） ----------
  set_var_value "$env_file" "SERVER_IP" "$server_ip"
  for var in $SERVER_IP_EMBEDDED_VARS; do
    if grep -qE "^${var}=.*CHANGE_ME_SERVER_IP" "$env_file"; then
      sed -i -E "s|^(${var}=)([^#]*)CHANGE_ME_SERVER_IP|\1\2${server_ip}|" "$env_file"
      log_success "已联动更新 ${var} 中的 SERVER_IP"
    fi
  done
  if grep -qE '^[A-Za-z0-9_]+=[^#]*CHANGE_ME_SERVER_IP' "$env_file"; then
    sed -i -E "s|^([A-Za-z0-9_]+=)([^#]*)CHANGE_ME_SERVER_IP|\1\2${server_ip}|" "$env_file"
    log_success "已兜底替换残留的 CHANGE_ME_SERVER_IP"
  fi

  # ---------- 3) --force：重新生成全部口令/密钥 ----------
  if [ "$FORCE" -eq 1 ]; then
    count=0
    while IFS= read -r var; do
      case "$var" in
        SERVER_IP | CORS_ORIGINS | RC_PUBLIC_WS_BASE_URL | RC_STUN_URLS) continue ;;
      esac
      set_var_value "$env_file" "$var" "$(hc_rand_hex 24)"
      count=$((count + 1))
      log_info "已重新生成：${var}"
    done < <(grep -oE "${PASSWORD_VAR_PATTERN}" "$env_file" | tr -d '=')
    log_success "--force 重新生成 ${count} 个口令/密钥变量"
  fi

  # ---------- 4) 口令清单（权限 600） ----------
  passwords_file="${APP_DIR}/passwords.txt"
  install -m 0600 /dev/null "$passwords_file"
  {
    printf '# HunterCore 部署口令清单（生成时间：%s）\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf '# ⚠ 敏感文件：权限 600；禁止提交仓库、截图或外发；请离线保存\n'
    printf '# .env 路径：%s\n' "$env_file"
    printf '# 应用根目录：%s\n\n' "$APP_DIR"
    printf 'SERVER_IP=%s\n' "$server_ip"
    grep -E "${PASSWORD_VAR_PATTERN}" "$env_file" | grep -v 'CHANGE_ME_' || true
  } >>"$passwords_file"
  chmod 600 "$passwords_file" "$env_file"

  # ---------- 5) 明文仅终端展示（不落日志） ----------
  prev_log="${HC_LOG_TO_FILE:-1}"
  HC_LOG_TO_FILE=0
  printf '\n%s\n' "${C_YELLOW}=========== 生成的敏感配置（仅终端显示，不入日志）===========${C_RESET}"
  printf '  SERVER_IP=%s\n' "$server_ip"
  if ! grep -E "${PASSWORD_VAR_PATTERN}" "$env_file" | grep -v 'CHANGE_ME_' | sed 's/^/  /'; then
    printf '  （无可展示的口令项）\n'
  fi
  printf '%s\n' "${C_YELLOW}============================================================${C_RESET}"
  printf '  已写入：%s（权限 600，请立即离线保存）\n\n' "$passwords_file"
  HC_LOG_TO_FILE="$prev_log"

  # ---------- 6) 残留占位检查（仅统计生效行，忽略注释中的预留变量） ----------
  remain="$(grep -cE '^[A-Za-z0-9_]+=.*CHANGE_ME_' "$env_file" || true)"
  if [ "$remain" -gt 0 ]; then
    log_warn "仍有 ${remain} 个生效变量为 CHANGE_ME_ 占位，请人工确认："
    grep -nE '^[A-Za-z0-9_]+=.*CHANGE_ME_' "$env_file" | sed 's/^/    /' || true
  else
    log_success "生效变量中的占位值已全部替换（注释内的 CHANGE_ME_ 预留示例不影响运行）"
  fi
  log_success "口令生成完成（.env 与 passwords.txt 均为权限 600）"
  log_warn "口令轮换后需同步更新车端 Kafka SCRAM 配置、Grafana/MinIO 运维凭证与备份介质"
}

main "$@"