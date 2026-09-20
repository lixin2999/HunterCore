#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— MinIO Bucket 初始化（init-minio.sh）
#
# 用途：等待 MinIO 就绪后，按契约（contracts/database/object-storage.yaml）创建
#       7 个 Bucket 并设置生命周期规则：
#
#   Bucket                生命周期
#   hunter-raw-data       30 天自动删除（传感器原始数据：点云/图像）
#   hunter-rosbag         按对象 Tag 区分（G-12）：hunter-retention=regular 30 天；event 永久（不设规则）
#   hunter-video          90 天（远程操控录像）
#   hunter-ota-packages   永久（版本管理）
#   hunter-reports        永久（分析报告）
#   hunter-logs           30 天（系统日志）
#   hunter-scene-assets   永久（场景资源：地图/模型）
#
# ⚠ minio 服务镜像不含 mc：脚本优先使用容器内 mc（若镜像自带），否则以一次性容器
#   （minio/mc 镜像，--network container:hunter-minio 复用其网络命名空间）执行，不依赖 compose 网络名。
#
# 用法：
#   sudo bash scripts/init-minio.sh [选项]
#
# 参数：
#   --help             显示本帮助
#   --env <file>       指定 .env 路径
#   --skip-lifecycle   仅创建 Bucket，不设置生命周期规则
#
# 示例：
#   sudo bash /opt/hunter-edge/scripts/init-minio.sh
#   sudo bash /opt/hunter-edge/scripts/init-minio.sh --skip-lifecycle
#
# 幂等：mb --ignore-existing；生命周期规则已存在则跳过（不重复添加）。
# 依赖：common.sh（同目录）、容器 hunter-minio、镜像 minio/mc（可用 MINIO_MC_IMAGE 覆盖）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # MINIO_* 由 .env 注入

ENV_FILE_ARG=""
SKIP_LIFECYCLE=0
EXPECTED_BUCKETS=7
# Bucket 定义（名称:过期天数:选择器）—— 天数 0 表示永久（不设生命周期规则）；
# 选择器为对象 Tag 表达式（hunter-retention=...），不带选择器的为整桶规则
BUCKET_SPECS=(
  "hunter-raw-data:30:"
  "hunter-rosbag:30:hunter-retention=regular"
  "hunter-video:90:"
  "hunter-ota-packages:0:"
  "hunter-reports:0:"
  "hunter-logs:30:"
  "hunter-scene-assets:0:"
)

usage() {
  cat <<'EOF'
HunterCore MinIO Bucket 初始化脚本

用途：
  创建 7 个契约 Bucket 并设置生命周期规则（名称与生命周期不可更改）：
    hunter-raw-data(30d) hunter-rosbag(Tag hunter-retention=regular 30d) hunter-video(90d)
    hunter-logs(30d) hunter-ota-packages(永久) hunter-reports(永久) hunter-scene-assets(永久)

用法：
  sudo bash scripts/init-minio.sh [选项]

参数：
  --help            显示本帮助
  --env <file>      .env 路径（默认 /opt/hunter-edge/.env）
  --skip-lifecycle  仅创建 Bucket，不设置生命周期

示例：
  sudo bash /opt/hunter-edge/scripts/init-minio.sh
  MINIO_MC_IMAGE=minio/mc:RELEASE.2024-06-13T16-39-23Z sudo bash scripts/init-minio.sh

校验：
  mc ls local/                 # 7 个 Bucket
  mc ilm rule ls local/<bucket>  # 生命周期规则

备份（大文件不打包，用 mc mirror 增量同步）：
  docker run --rm --network container:hunter-minio -e MC_HOST_local=http://<user>:<pass>@127.0.0.1:9000 \
    minio/mc mirror --overwrite local/hunter-video /backup/hunter-video
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
      --skip-lifecycle)
        SKIP_LIFECYCLE=1
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

# create_bucket <name>
create_bucket() {
  local bucket="$1"
  if minio_mc mb --ignore-existing "local/${bucket}" >/dev/null 2>&1; then
    log_success "Bucket 已就绪：${bucket}"
    return 0
  fi
  log_error "创建 Bucket 失败：${bucket}"
  log_error "排查建议：① MINIO_ROOT_USER/PASSWORD 是否正确；② 容器状态：docker ps | grep minio；③ 镜像是否可拉取：${MINIO_MC_IMAGE}"
  return 1
}

# has_lifecycle_rule <bucket>：是否已存在过期规则（幂等判断）
has_lifecycle_rule() {
  local bucket="$1" out
  out="$(minio_mc ilm rule ls "local/${bucket}" 2>/dev/null || true)"
  case "$out" in
    "" | *"No such"* | *"no rule"*) return 1 ;;
    *EXPIRY* | *expire* | *Expiry*) return 0 ;;
    *) [ -n "$out" ] && return 0 || return 1 ;;
  esac
}

# add_expiry <bucket> <days> [tags]（G-12：rosbag 按对象 Tag 而非前缀区分生命周期）
add_expiry() {
  local bucket="$1" days="$2" tags="${3:-}"
  if has_lifecycle_rule "$bucket"; then
    log_info "生命周期规则已存在，跳过（幂等）：${bucket}"
    return 0
  fi
  if [ -n "$tags" ]; then
    if minio_mc ilm rule add --expire-days "$days" --tags "$tags" "local/${bucket}" >/dev/null 2>&1; then
      log_success "已设置生命周期：${bucket} Tag ${tags} → ${days} 天过期"
      return 0
    fi
  else
    if minio_mc ilm rule add --expire-days "$days" "local/${bucket}" >/dev/null 2>&1; then
      log_success "已设置生命周期：${bucket} → ${days} 天过期"
      return 0
    fi
  fi
  log_error "设置生命周期失败：${bucket}（${days} 天${tags:+, tags=${tags}}）"
  log_error "可手工重试：mc ilm rule add --expire-days ${days}${tags:+ --tags ${tags}} local/${bucket}"
  return 1
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file spec bucket days tags failed=0 bucket_count
  parse_args "$@"
  hc_log_begin

  if ! command_exists docker; then
    log_error "缺少 docker 命令：本脚本通过容器执行 mc 客户端"
    return 1
  fi
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE
  if ! require_env MINIO_ROOT_USER MINIO_ROOT_PASSWORD; then
    log_error "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD 未配置，请检查 ${env_file}"
    return 1
  fi

  # ---------- 1) 等待 MinIO 就绪（最多 60s） ----------
  if ! wait_for "curl -sf -m 5 $(minio_health_url) >/dev/null" "MinIO（$(minio_health_url)）" 60; then
    log_error "MinIO 未就绪：docker logs ${C_MINIO} --tail=100"
    return 1
  fi

  # ---------- 2) mc 客户端可用性（镜像首次使用需拉取） ----------
  log_info "校验 mc 客户端（镜像：${MINIO_MC_IMAGE}）"
  if ! minio_mc --version >/dev/null 2>&1; then
    log_error "mc 客户端不可用：请确认可拉取镜像 ${MINIO_MC_IMAGE}（docker pull ${MINIO_MC_IMAGE}）"
    log_error "也可用 MINIO_MC_IMAGE 覆盖为内网镜像仓库地址"
    return 1
  fi
  log_success "mc 客户端就绪"

  # ---------- 3) 创建 7 个 Bucket（幂等） ----------
  log_info "===== 创建 Bucket（契约：contracts/database/object-storage.yaml）====="
  for spec in "${BUCKET_SPECS[@]}"; do
    bucket="${spec%%:*}"
    if ! create_bucket "$bucket"; then
      failed=$((failed + 1))
    fi
  done

  # ---------- 4) 生命周期规则 ----------
  if [ "$SKIP_LIFECYCLE" -eq 1 ]; then
    log_warn "已按 --skip-lifecycle 跳过生命周期规则设置（磁盘增长风险，请尽快补设）"
  else
    log_info "===== 设置生命周期规则 ====="
    for spec in "${BUCKET_SPECS[@]}"; do
      bucket="${spec%%:*}"
      days="$(printf '%s' "$spec" | cut -d: -f2)"
      tags="$(printf '%s' "$spec" | cut -d: -f3)"
      if [ "$days" = "0" ]; then
        log_info "永久保留（不设规则）：${bucket}"
        continue
      fi
      if ! add_expiry "$bucket" "$days" "$tags"; then
        failed=$((failed + 1))
      fi
    done
  fi

  # ---------- 5) 校验与汇总 ----------
  log_info "===== Bucket 列表 ====="
  minio_mc ls local 2>/dev/null | sed 's/^/  /' || true
  bucket_count="$(minio_bucket_count)"
  if [ "${bucket_count:-0}" -lt "$EXPECTED_BUCKETS" ]; then
    log_error "Bucket 数量不足：${bucket_count:-0} < ${EXPECTED_BUCKETS}"
    log_error "请检查 MinIO 日志与 root 凭据：docker logs ${C_MINIO} --tail=100"
    failed=$((failed + 1))
  else
    log_success "Bucket 数量校验通过：${bucket_count}（期望 ≥${EXPECTED_BUCKETS}）"
  fi

  if [ "$failed" -ne 0 ]; then
    log_error "MinIO 初始化存在 ${failed} 项失败"
    return 1
  fi
  log_success "MinIO Bucket 与生命周期规则初始化完成"
  log_warn "hunter-rosbag 按对象 Tag 区分（G-12）：hunter-retention=regular 30 天过期；event/未打标为永久（禁止绕过 data-collector complete 直写）"
  log_warn "预签名 URL 有效期：上传 1 小时 / 下载 15 分钟（由服务侧配置，不在此设置）"
  return 0
}

main "$@"