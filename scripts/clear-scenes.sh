#!/usr/bin/env bash
# =====================================================================
# HunterCore 场景库清空脚本（clear-scenes.sh）
#
# 背景：早期 batch_add_scenes.py 以固定 i % n 轮换生成 300 个场景，同类型内
#       参与者/事件/天气几乎复制粘贴，导致「场景详情内容雷同」。重构前需先
#       物理清空场景库（scene_svc.scenes），再用新的高斯扰动生成器重新灌库。
#
# 为何直连 DB 物理删除：
#   - scene-service DELETE /api/v1/scene/{id} 是**软删除**（置 deleted_at），
#     且契约规定 published 场景不可直接删除（返回 3003），需先归档；
#   - 已批量发布的 300 个场景无归档端点，逐条走 API 不可行；
#   - 本次为「重构前彻底清空 + 释放 scene_name 唯一约束」，采用物理 DELETE。
#
# 用法：
#   sudo bash scripts/clear-scenes.sh --dry-run                 # 仅统计，不删除（默认动作建议先跑）
#   sudo bash scripts/clear-scenes.sh                           # 交互确认后物理清空全部场景
#   sudo bash scripts/clear-scenes.sh --yes                     # 非交互（ASSUME_YES）清空全部
#   sudo bash scripts/clear-scenes.sh --type cut_in,rainy       # 仅清空指定 scene_type
#   sudo bash scripts/clear-scenes.sh --status published        # 仅清空指定状态
#   sudo bash scripts/clear-scenes.sh --creator <uuid>          # 仅清空指定创建者
#   sudo bash scripts/clear-scenes.sh --env /opt/hunter-core/.env
#
# 参数：
#   --dry-run            只统计命中条数与样例，不执行删除
#   --yes | -y           跳过交互确认（等价 ASSUME_YES=1）
#   --type <list>        逗号分隔的 scene_type 过滤（缺省=全部）
#   --status <list>      逗号分隔的状态过滤 draft/published/archived（缺省=全部）
#   --creator <uuid>     仅清空该创建者（user_id）的场景（缺省=全部）
#   --keep-soft-deleted  保留已软删除记录（deleted_at 非 NULL），仅删存活场景
#   --no-cache-flush     跳过 Redis 场景详情缓存失效（默认会清理 cache:scene:*）
#   --env <file>         指定 .env 路径（默认 /opt/hunter-core/.env）
#   --help | -h          显示帮助
#
# 依赖：common.sh（同目录）、容器 hunter-postgres、可选 hunter-redis
# 退出码：0 成功（含命中 0 条）；1 失败；2 参数/环境错误
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"

DRY_RUN=0
KEEP_SOFT_DELETED=0
CACHE_FLUSH=1
FILTER_TYPE=""
FILTER_STATUS=""
FILTER_CREATOR=""
ENV_FILE_ARG=""

usage() {
  # 复用文件头注释作为帮助（跳过 shebang 与首个分隔线之前的内容）
  sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --dry-run) DRY_RUN=1; shift ;;
      --yes | -y) export ASSUME_YES=1; shift ;;
      --type) [ $# -ge 2 ] || die "--type 需要一个逗号分隔参数"; FILTER_TYPE="$2"; shift 2 ;;
      --status) [ $# -ge 2 ] || die "--status 需要一个逗号分隔参数"; FILTER_STATUS="$2"; shift 2 ;;
      --creator) [ $# -ge 2 ] || die "--creator 需要一个 UUID 参数"; FILTER_CREATOR="$2"; shift 2 ;;
      --keep-soft-deleted) KEEP_SOFT_DELETED=1; shift ;;
      --no-cache-flush) CACHE_FLUSH=0; shift ;;
      --env) [ $# -ge 2 ] || die "--env 需要一个文件路径参数"; ENV_FILE_ARG="$2"; shift 2 ;;
      --help | -h) usage; exit 0 ;;
      *) log_error "未知参数：$1"; usage >&2; exit 2 ;;
    esac
  done
}

# 构造 WHERE 子句（供统计与删除复用，保证两者作用域一致）
build_where() {
  local where="WHERE 1=1"
  # 软删除记录：默认一并清空（含已软删），--keep-soft-deleted 时仅保留存活
  if [ "$KEEP_SOFT_DELETED" -eq 1 ]; then
    where="${where} AND deleted_at IS NULL"
  fi
  if [ -n "$FILTER_TYPE" ]; then
    where="${where} AND scene_type IN ($(sql_in_list "$FILTER_TYPE"))"
  fi
  if [ -n "$FILTER_STATUS" ]; then
    where="${where} AND status IN ($(sql_in_list "$FILTER_STATUS"))"
  fi
  if [ -n "$FILTER_CREATOR" ]; then
    where="${where} AND creator = '${FILTER_CREATOR}'"
  fi
  printf '%s' "$where"
}

# sql_in_list "a,b,c" → "'a','b','c'"（单引号包裹，逗号分隔；供 IN (...) 使用）
sql_in_list() {
  local raw="$1" out="" item
  local IFS=','
  for item in $raw; do
    item="$(printf '%s' "$item" | tr -d '[:space:]')"
    [ -n "$item" ] || continue
    out="${out}${out:+,}'${item}'"
  done
  printf '%s' "$out"
}

# 校验过滤值白名单（防注入：status 受控词表；type 仅允许 [a-z_]；creator 仅 UUID 字符）
validate_filters() {
  if [ -n "$FILTER_STATUS" ]; then
    local s
    local IFS=','
    for s in $FILTER_STATUS; do
      s="$(printf '%s' "$s" | tr -d '[:space:]')"
      case "$s" in
        draft | published | archived) : ;;
        *) log_error "非法 status：${s}（允许 draft/published/archived）"; exit 2 ;;
      esac
    done
  fi
  if [ -n "$FILTER_TYPE" ] && printf '%s' "$FILTER_TYPE" | grep -qE "[^a-zA-Z0-9_,]"; then
    log_error "非法 scene_type 过滤值（仅允许字母/数字/下划线/逗号）：${FILTER_TYPE}"
    exit 2
  fi
  if [ -n "$FILTER_CREATOR" ] && printf '%s' "$FILTER_CREATOR" | grep -qE "[^a-fA-F0-9-]"; then
    log_error "非法 creator UUID：${FILTER_CREATOR}"
    exit 2
  fi
}

main() {
  local env_file where total sample
  parse_args "$@"
  hc_log_begin
  validate_filters

  if ! command_exists docker; then
    log_error "缺少 docker 命令：本脚本通过 docker exec 操作 hunter-postgres"
    return 1
  fi
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  if ! require_env POSTGRES_USER POSTGRES_DB; then
    log_error "POSTGRES_USER / POSTGRES_DB 未配置，请检查 ${env_file}"
    return 1
  fi

  if ! container_running "$C_POSTGRES"; then
    log_error "PostgreSQL 容器未运行：${C_POSTGRES}（docker ps 确认；或先执行 install.sh 拉起依赖）"
    return 1
  fi
  if ! wait_for "docker exec ${C_POSTGRES} pg_isready -U '${POSTGRES_USER}' -d '${POSTGRES_DB}'" \
    "PostgreSQL（${C_POSTGRES} / 库 ${POSTGRES_DB}）" 30; then
    log_error "PostgreSQL 未就绪：docker logs ${C_POSTGRES} --tail=100"
    return 1
  fi

  where="$(build_where)"
  log_info "清空作用域 WHERE：${where}"

  total="$(docker exec -i "$C_POSTGRES" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA \
    -c "SELECT count(*) FROM scene_svc.scenes ${where};")"
  total="${total//[^0-9]/}"
  log_info "命中场景数：${total:-0}"

  if [ "${total:-0}" -eq 0 ]; then
    log_success "无匹配场景，无需清空"
    return 0
  fi

  sample="$(docker exec -i "$C_POSTGRES" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA \
    -c "SELECT scene_name FROM scene_svc.scenes ${where} ORDER BY create_time DESC LIMIT 5;")"
  log_info "命中样例（最多 5 条）："
  printf '%s\n' "$sample" | sed 's/^/    - /'

  if [ "$DRY_RUN" -eq 1 ]; then
    log_success "[dry-run] 仅统计，未执行删除（命中 ${total} 条）"
    return 0
  fi

  if ! confirm "⚠ 即将物理删除 ${total} 个场景（不可恢复），确认？"; then
    log_warn "已取消：未删除任何场景"
    return 0
  fi

  # 事务内删除 + 删除后计数复核
  local deleted
  if ! docker exec -i "$C_POSTGRES" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tA <<SQL
BEGIN;
DELETE FROM scene_svc.scenes ${where};
COMMIT;
SQL
  then
    log_error "删除失败：docker logs ${C_POSTGRES} --tail=100"
    return 1
  fi
  deleted="$(docker exec -i "$C_POSTGRES" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA \
    -c "SELECT count(*) FROM scene_svc.scenes ${where};")"
  deleted="${deleted//[^0-9]/}"
  if [ "${deleted:-0}" -ne 0 ]; then
    log_error "删除后仍有 ${deleted} 条命中记录（预期 0），请检查是否有并发写入"
    return 1
  fi
  log_success "已物理删除 ${total} 个场景（scene_svc.scenes）"

  # 失效 Redis 场景详情缓存（cache:scene:{scene_id}），避免旧 scene_id 命中脏缓存
  # 尽力而为：清理失败不影响主流程（DB 已删，缓存最长 TTL 3600s 后自然过期）
  if [ "$CACHE_FLUSH" -eq 1 ] && container_running "$C_REDIS"; then
    local redis_auth="" flushed
    [ -n "${REDIS_PASSWORD:-}" ] && redis_auth="-a '$REDIS_PASSWORD' --no-auth-warning"
    # 容器内 scan + 批量 UNLINK（异步释放，比 DEL 更轻）；统计清理键数
    flushed="$(docker exec "$C_REDIS" sh -c \
      "redis-cli ${redis_auth} --scan --pattern 'cache:scene:*' | xargs -r -n 200 redis-cli ${redis_auth} UNLINK" \
      2>/dev/null | awk '{s+=$1} END{print s+0}')"
    log_success "已失效 Redis 场景详情缓存键（约 ${flushed:-0} 个）"
  elif [ "$CACHE_FLUSH" -eq 1 ]; then
    log_warn "Redis 容器未运行（${C_REDIS}），跳过缓存失效；场景详情缓存将在 TTL(3600s) 后自然过期"
  fi

  log_success "场景库清空完成，可执行新版 batch_add_scenes.py 重新灌库"
  return 0
}

main "$@"
