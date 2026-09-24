#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 卸载（uninstall.sh）
#
# 用途：停止并删除 HunterCore 容器 / 镜像 / 网络；可选彻底删除数据与 Docker。
#
#   --keep-data（默认）：删除容器、镜像、网络与编排资源；**保留** /data 数据目录、
#                        ${APP_DIR} 配置与 .env、certs 证书（便于重新部署/排障）
#   --all             ：在上述基础上再删除 ${DATA_DIR} 下全部数据、${APP_DIR}、禁用并卸载
#                        Docker Engine（不可恢复操作，需二次输入 DELETE-ALL 确认）
#
# ⚠ 危险操作：--all 会永久删除遥测/事件/场景/OTA/录像等全部数据与证书，且无法恢复。
#   执行前请确认已完成异地备份（bash scripts/backup.sh + MinIO mc mirror）。
#
# 用法：
#   sudo bash scripts/uninstall.sh [选项]
#
# 参数：
#   --help        显示本帮助
#   --keep-data   保留数据与配置（默认）
#   --all         彻底删除（数据 + 配置 + Docker；需输入 DELETE-ALL 二次确认）
#   --env <file>  指定 .env 路径（保留数据模式下仅用于读取路径）
#   -y, --yes     非交互（⚠ 仍会要求输入 YES / DELETE-ALL 字样确认，以防误删）
#
# 示例：
#   sudo bash /opt/hunter-core/scripts/uninstall.sh              # 交互式，保留数据
#   sudo bash /opt/hunter-core/scripts/uninstall.sh --all        # 彻底删除（强确认）
#
# 依赖：common.sh（同目录）、docker / docker compose（--all 时含 systemctl、apt）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # DATA_DIR/BACKUP_DIR 由 common.sh 与 .env 提供

MODE="keep-data"
ENV_FILE_ARG=""
ASSUME_YES=0

usage() {
  cat <<'EOF'
HunterCore 卸载脚本

用途：
  停止并删除容器 / 镜像 / 网络；--all 时进一步删除数据、配置与 Docker。

用法：
  sudo bash scripts/uninstall.sh [选项]

参数：
  --help        显示本帮助
  --keep-data   保留 /data 数据与 /opt/hunter-core 配置（默认）
  --all         彻底删除（数据 + 配置 + Docker；需输入 DELETE-ALL 二次确认）
  --env <file>  .env 路径
  -y, --yes     非交互（仍强制输入 YES / DELETE-ALL 字样确认，防止误删）

示例：
  sudo bash scripts/uninstall.sh
  sudo bash scripts/uninstall.sh --all

保留数据模式下被删除的内容：
  容器与网络（docker compose down -v）、hunter-* 镜像、hunter-net 网络
保留数据模式下被保留的内容：
  ${DATA_DIR}（PostgreSQL/Kafka/MinIO/Redis/Flink 数据）、${APP_DIR}（.env/config/sql/certs）
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --keep-data)
        MODE="keep-data"
        shift
        ;;
      --all)
        MODE="all"
        shift
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
        shift 2
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

# require_typed_confirmation <prompt> <expected>：必须精确输入 expected 才通过（防误删）
require_typed_confirmation() {
  local prompt="$1" expected="$2" reply=""
  printf '%s（输入 %s 确认，其它任意输入将中止）: ' "$prompt" "$expected"
  read -r reply || true
  if [ "$reply" = "$expected" ]; then
    return 0
  fi
  log_warn "输入不匹配（收到：${reply:-<空>}）：已中止卸载，未做任何修改"
  return 1
}

# stop_and_remove_compose：停止并删除容器、网络（-v 同时删除匿名卷）
stop_and_remove_compose() {
  if [ ! -f "${APP_DIR}/docker-compose.yml" ] && [ ! -f "${APP_DIR}/infra/deploy/docker-compose.yml" ]; then
    log_warn "未找到 ${APP_DIR}/docker-compose.yml 或 infra/deploy/docker-compose.yml：跳过 compose 清理（容器可能需手工删除）"
    return 0
  fi
  log_info "执行 compose down -v（停止并删除容器/网络）"
  if compose down -v --remove-orphans; then
    log_success "compose 资源已清理"
    return 0
  fi
  log_warn "compose down 返回非 0：请手工核对 docker ps -a | grep hunter"
  return 0
}

# remove_images：删除 hunter/* 镜像
remove_images() {
  local images=()
  mapfile -t images < <(docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' 2>/dev/null |
    grep -E '^hunter/' | awk '{print $2}' | sort -u)
  if [ "${#images[@]}" -eq 0 ]; then
    log_info "未发现 hunter/* 镜像，跳过（幂等）"
    return 0
  fi
  log_info "删除 hunter/* 镜像（${#images[@]} 个）"
  local id
  for id in "${images[@]}"; do
    if docker rmi -f "$id" >/dev/null 2>&1; then
      log_success "已删除镜像：${id}"
    else
      log_warn "删除镜像失败（可能被容器占用）：${id}"
    fi
  done
  return 0
}

# remove_network：删除 hunter-net 网络
remove_network() {
  if docker network inspect hunter-net >/dev/null 2>&1; then
    if docker network rm hunter-net >/dev/null 2>&1; then
      log_success "已删除 Docker 网络：hunter-net"
    else
      log_warn "删除网络 hunter-net 失败（可能存在活动端点）：docker network inspect hunter-net"
    fi
  else
    log_info "网络 hunter-net 不存在，跳过（幂等）"
  fi
  return 0
}

# purge_all_data：--all 模式：删除数据目录、应用目录、Docker
purge_all_data() {
  # 安全护栏：DATA_DIR 必须是绝对路径且不是 / 或 /data 以外的关键目录
  case "$DATA_DIR" in
    "/" | "" | "/usr" | "/etc" | "/var" | "/home" | "/root" | "/opt")
      log_error "拒绝执行：DATA_DIR=${DATA_DIR} 属于受保护路径，请人工确认后再操作"
      return 1
      ;;
  esac
  if [ -d "$DATA_DIR" ]; then
    log_warn "删除数据目录内容：${DATA_DIR}/（PostgreSQL/Kafka/MinIO/Redis/Flink/备份，不可恢复）"
    find "$DATA_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} + || {
      log_error "删除 ${DATA_DIR} 内容失败：请检查挂载点与权限"
      return 1
    }
    log_success "已清空：${DATA_DIR}"
  else
    log_info "数据目录不存在，跳过：${DATA_DIR}"
  fi

  if [ -d "$APP_DIR" ] && [ "$APP_DIR" != "/" ]; then
    log_warn "删除应用目录：${APP_DIR}（含 .env/config/sql/certs，不可恢复）"
    rm -rf "${APP_DIR:?}" || {
      log_error "删除 ${APP_DIR} 失败"
      return 1
    }
    log_success "已删除：${APP_DIR}"
  fi

  if command_exists systemctl && systemctl list-unit-files docker.service >/dev/null 2>&1; then
    log_warn "禁用并停止 Docker：systemctl disable --now docker"
    systemctl disable --now docker >/dev/null 2>&1 || log_warn "systemctl disable --now docker 返回非 0（容器环境可能无 systemd）"
  fi
  log_warn "如需彻底卸载 Docker Engine：apt-get purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin"
  log_warn "本脚本不自动执行 apt purge（避免影响同机其他 Docker 负载），请按需手工执行"
  return 0
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file
  parse_args "$@"
  hc_log_begin

  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ -f "$env_file" ]; then
    load_env "$env_file" >/dev/null 2>&1 || log_warn "加载 ${env_file} 失败：使用默认路径"
  else
    log_info ".env 不存在（${env_file}）：使用默认路径（${DATA_DIR}、${APP_DIR}）"
  fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  if ! command_exists docker; then
    log_warn "未检测到 docker 命令：将仅清理文件系统相关目录"
  fi

  log_info "===== HunterCore 卸载（模式：${MODE}）====="
  log_info "应用目录：${APP_DIR}；数据目录：${DATA_DIR}"

  # ---------- 1) 二次确认（防误删） ----------
  if [ "$MODE" = "all" ]; then
    log_warn "⚠ --all 模式将【永久删除】以下内容，且不可恢复："
    log_warn "   · 容器/镜像/网络（docker compose down -v + hunter/* 镜像 + hunter-net）"
    log_warn "   · 数据：${DATA_DIR}/（遥测/事件/场景/OTA 包/录像/备份）"
    log_warn "   · 配置与证书：${APP_DIR}/（.env、config/、sql/、certs/ 私钥）"
    log_warn "   · 停止并禁用 Docker（apt purge 需手工执行）"
    if ! require_typed_confirmation "将删除全部数据（含证书私钥），是否继续？" "DELETE-ALL"; then
      return 1
    fi
    if ! require_typed_confirmation "请再次确认：所有数据将永久丢失" "DELETE-ALL"; then
      return 1
    fi
  else
    log_warn "⚠ 将删除所有 HunterCore 容器、镜像与网络；数据与配置将保留"
    if ! require_typed_confirmation "是否继续？" "YES"; then
      return 1
    fi
  fi

  # ---------- 2) 停止并删除编排资源 ----------
  if command_exists docker; then
    stop_and_remove_compose
    remove_images
    remove_network
  fi

  # ---------- 3) --all：删除数据、配置与 Docker ----------
  if [ "$MODE" = "all" ]; then
    purge_all_data || return 1
    log_success "彻底卸载完成：数据、配置与 Docker 服务均已处理"
    log_warn "如仍需回收磁盘：docker system prune -a --volumes（慎用，会影响同机其他负载）"
    return 0
  fi

  # ---------- 4) 保留数据模式的收尾说明 ----------
  log_success "卸载完成（已删除容器/镜像/网络；数据与配置已保留）"
  log_info "保留的数据目录：${DATA_DIR}（postgresql/timescale/kafka/zookeeper/redis/minio/srs/flink/backups）"
  log_info "保留的应用目录：${APP_DIR}（.env、config/、sql/、certs/、scripts/）"
  log_info "重新部署：bash ${APP_DIR}/scripts/install.sh --skip-docker --step 3 -y"
  log_warn "如需彻底清除数据：bash $0 --all（会再次要求输入 DELETE-ALL 确认）"
  return 0
}

main "$@"