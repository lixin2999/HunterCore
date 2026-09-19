#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 数据备份（backup.sh）
#
# 用途：对单机部署的关键数据做逻辑备份，并按保留天数清理历史备份：
#   ① PostgreSQL 全量（自定义格式 -F c -Z 9，可 pg_restore 选择性恢复）：postgres_<db>.dump
#   ② TimescaleDB 热数据：两张 hypertable 最近 7 天数据（data-only，便于快速恢复）
#      + 结构说明 timescaledb_schema_note.txt（完整结构与全量数据已含在 ①）
#   ③ 配置：.env + config/ → config.tar.gz（权限 600，含敏感信息）
#   ④ Redis：RDB 快照（存在时）→ redis_dump.rdb
#   ⑤ MinIO：大文件不打包，输出 mc mirror 增量同步命令（原始数据体积大，禁止整目录打包）
#   ⑥ 清理：删除 ${BACKUP_DIR} 下早于保留天数的备份目录
#
# ⚠ TimescaleDB：pg_dump 输出的 hypertable 为「chunk 物理表 + 视图」形式，完整恢复需
#   timescaledb_pre_restore()/post_restore() 包裹（见脚本末尾提示）。
#
# 用法：
#   sudo bash scripts/backup.sh [选项]
#
# 参数：
#   --help              显示本帮助
#   --retention <days>  清理早于 N 天的备份（默认 .env BACKUP_RETENTION_DAYS=30）
#   --dir <dir>         备份根目录（默认 .env BACKUP_DIR=/data/backups）
#   --env <file>        指定 .env 路径
#   --no-minio-hint     不输出 MinIO mirror 提示
#
# 示例：
#   sudo bash /opt/hunter-edge/scripts/backup.sh
#   sudo bash /opt/hunter-edge/scripts/backup.sh --retention 14
#   # cron：0 2 * * * /opt/hunter-edge/scripts/backup.sh >> /var/log/hunter-edge/backups/cron.log 2>&1
#
# 日志：${LOG_DIR}/backups/backup-YYYYmmdd_HHMMSS.log
# 依赖：common.sh（同目录）、容器 hunter-postgres（+ 可选 hunter-timescale/hunter-redis）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # POSTGRES_* / BACKUP_* / MINIO_* 由 .env 注入

ENV_FILE_ARG=""
RETENTION=""
BACKUP_ROOT_ARG=""
SHOW_MINIO_HINT=1
# 需备份热数据的 hypertable（契约：contracts/database/ddl/05_timeseries.sql）
HYPERTABLES=(
  "data_collector.vehicle_telemetry"
  "data_analytics.algorithm_metrics"
)
HOT_DATA_DAYS=7
BACKUP_DIR=""

usage() {
  cat <<'EOF'
HunterCore 数据备份脚本

用途：
  备份 PostgreSQL 全量、TimescaleDB 最近 7 天热数据、.env 与 config/、Redis RDB，
  按保留天数清理历史备份，并输出 MinIO 大文件的 mc mirror 增量同步命令。

用法：
  sudo bash scripts/backup.sh [选项]

参数：
  --help              显示本帮助
  --retention <days>  保留天数（默认 .env BACKUP_RETENTION_DAYS=30）
  --dir <dir>         备份根目录（默认 .env BACKUP_DIR=/data/backups）
  --env <file>        .env 路径（默认 /opt/hunter-edge/.env）
  --no-minio-hint     不输出 MinIO mirror 提示

示例：
  sudo bash scripts/backup.sh
  sudo bash scripts/backup.sh --retention 14 --dir /data/backups

产物（${BACKUP_DIR}/<时间戳>/）：
  postgres_<db>.dump                    PostgreSQL 全量（-F c -Z 9）
  timescaledb_<table>_7d.sql            两张 hypertable 最近 7 天数据（data-only）
  timescaledb_schema_note.txt           时序结构说明与恢复顺序
  config.tar.gz                         .env + config/（权限 600）
  redis_dump.rdb                        Redis RDB（存在时）

恢复要点：
  · PostgreSQL：docker exec -i hunter-postgres pg_restore -U <user> -d <db> --clean < dump
  · TimescaleDB：恢复前 timescaledb_pre_restore()，恢复后 timescaledb_post_restore()
  · 备份含口令与密钥：请离线保存并加密归档（本脚本不自动加密）
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --retention)
        [ $# -ge 2 ] || die "--retention 需要一个天数参数"
        case "$2" in
          '' | *[!0-9]*) die "--retention 必须为非负整数（收到：$2）" ;;
        esac
        RETENTION="$2"
        shift 2
        ;;
      --dir)
        [ $# -ge 2 ] || die "--dir 需要一个目录参数"
        BACKUP_ROOT_ARG="$2"
        shift 2
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
        shift 2
        ;;
      --no-minio-hint)
        SHOW_MINIO_HINT=0
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

# backup_pg_full：PostgreSQL 全量（自定义格式，压缩级别 9）
backup_pg_full() {
  local target="${BACKUP_DIR}/postgres_${POSTGRES_DB}.dump"
  log_info "备份 PostgreSQL 全量：${POSTGRES_DB} → $(basename "$target")"
  if docker exec "$C_POSTGRES" pg_dump -U "$POSTGRES_USER" -F c -Z 9 "$POSTGRES_DB" \
    >"$target" 2>"${BACKUP_DIR}/pg_dump.err"; then
    chmod 600 "$target"
    log_success "PostgreSQL 备份完成：$(basename "$target")（$(du -h "$target" | cut -f1)）"
    rm -f "${BACKUP_DIR}/pg_dump.err"
    return 0
  fi
  log_error "PostgreSQL 备份失败：$(tail -n3 "${BACKUP_DIR}/pg_dump.err" 2>/dev/null)"
  log_error "排查：① docker ps | grep postgres；② df -h $(dirname "$BACKUP_DIR")；③ docker exec ${C_POSTGRES} pg_isready -U ${POSTGRES_USER}"
  return 1
}

# backup_timescale_hot：两张 hypertable 最近 N 天数据（data-only，便于快速恢复）
backup_timescale_hot() {
  local table target rc=0 table_name
  for table in "${HYPERTABLES[@]}"; do
    table_name="$(printf '%s' "$table" | tr '.' '_')"
    target="${BACKUP_DIR}/timescaledb_${table_name}_${HOT_DATA_DAYS}d.sql"
    log_info "备份 TimescaleDB 热数据（最近 ${HOT_DATA_DAYS} 天）：${table}"
    if docker exec "$C_POSTGRES" pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
      --data-only --table "$table" --where="time > NOW() - INTERVAL '${HOT_DATA_DAYS} days'" >"$target" 2>&1; then
      chmod 600 "$target"
      log_success "热数据备份完成：$(basename "$target")（$(du -h "$target" | cut -f1)）"
    else
      log_warn "热数据备份失败（不影响全量备份）：${table}（常见原因：hypertable 未创建，请先执行 init-db.sh）"
      rm -f "$target"
      rc=1
    fi
  done
  {
    printf '# TimescaleDB 结构说明（生成时间：%s）\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf '# 结构 DDL 来源：contracts/database/ddl/05_timeseries.sql（部署产物 sql/timescaledb.sql）\n'
    printf '# hypertable：\n'
    printf '#   data_collector.vehicle_telemetry（1 day chunk，90 天保留）\n'
    printf '#   data_analytics.algorithm_metrics（1 day chunk，90 天保留）\n'
    printf '# 完整结构与全量数据包含在 postgres_%s.dump 中；*_%dd.sql 仅为最近 %d 天 data-only 子集。\n' \
      "$POSTGRES_DB" "$HOT_DATA_DAYS" "$HOT_DATA_DAYS"
    printf '# 恢复顺序：① SELECT timescaledb_pre_restore(); ② 恢复 dump/data ③ SELECT timescaledb_post_restore();\n'
  } >"${BACKUP_DIR}/timescaledb_schema_note.txt"
  return "$rc"
}

# backup_config：.env + config/（含敏感信息，权限 600）
backup_config() {
  local target="${BACKUP_DIR}/config.tar.gz" env_file base
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  base="$(basename "$env_file")"
  local -a items=()
  if [ -f "$env_file" ]; then
    items+=("$base")
  fi
  if [ "$base" != ".env" ] && [ -f "${APP_DIR}/.env" ]; then
    items+=(".env")
  fi
  if [ -d "${APP_DIR}/config" ]; then
    items+=("config")
  fi
  if [ "${#items[@]}" -eq 0 ]; then
    log_warn "未找到可备份的配置（.env / config/）：跳过配置备份"
    return 0
  fi
  log_info "备份配置：${items[*]}（含敏感信息，归档权限 600）"
  if (cd "$APP_DIR" && tar czf "$target" "${items[@]}" 2>/dev/null); then
    chmod 600 "$target"
    log_success "配置备份完成：config.tar.gz（$(du -h "$target" | cut -f1)）"
    return 0
  fi
  log_error "配置备份失败：请检查 ${APP_DIR} 读权限与磁盘空间"
  return 1
}

# backup_redis：Redis RDB 快照（若存在）
backup_redis() {
  local src="${DATA_DIR}/redis/dump.rdb" target="${BACKUP_DIR}/redis_dump.rdb"
  if [ ! -f "$src" ]; then
    log_warn "未找到 Redis RDB（${src}）：跳过（AOF 模式请备份 ${DATA_DIR}/redis/appendonlydir/）"
    return 0
  fi
  if cp -p "$src" "$target"; then
    chmod 600 "$target"
    log_success "Redis 备份完成：redis_dump.rdb（$(du -h "$target" | cut -f1)）"
    return 0
  fi
  log_warn "Redis RDB 复制失败：${src}"
  return 0
}

# print_minio_hint：大文件不打包，给出 mc mirror 增量同步命令（含注释，不自动执行）
print_minio_hint() {
  local prev_log="${HC_LOG_TO_FILE:-1}"
  HC_LOG_TO_FILE=0
  printf '\n%s\n' "${C_YELLOW}---- MinIO 数据备份（大文件不打包，请用 mc mirror 增量同步）----${C_RESET}"
  printf '  # 1) 同步到本地/挂载盘（--overwrite 覆盖同名；建议先 dry-run：--dry-run）\n'
  printf '  docker run --rm --network container:%s -e MC_HOST_local=http://%s:%s@127.0.0.1:%s \\\n' \
    "$C_MINIO" "${MINIO_ROOT_USER:-<user>}" "<password>" "${MINIO_API_PORT:-9000}"
  printf '    -v /mnt/backup:/backup %s mirror --overwrite local/hunter-video /backup/hunter-video\n' "$MINIO_MC_IMAGE"
  printf '  # 2) 其余 Bucket 同理（hunter-raw-data / hunter-rosbag / hunter-ota-packages ...）\n'
  printf '  # 3) 生命周期提醒：raw-data 30 天、video 90 天自动过期；ota-packages/reports/scene-assets 永久\n'
  printf '  # 4) 备份量估算见 docs/01 §2.3（强依赖车端回传策略，需按实测复核）\n\n'
  HC_LOG_TO_FILE="$prev_log"
}

# cleanup_old_backups：删除早于保留天数的备份目录
cleanup_old_backups() {
  local root="$1" retention="$2" removed=0
  if [ ! -d "$root" ]; then
    return 0
  fi
  log_info "清理 ${root} 下早于 ${retention} 天的备份目录"
  while IFS= read -r dir; do
    [ -n "$dir" ] || continue
    if rm -rf "$dir"; then
      log_success "已删除过期备份：${dir}"
      removed=$((removed + 1))
    else
      log_warn "删除失败：${dir}（请检查权限）"
    fi
  done < <(find "$root" -maxdepth 1 -mindepth 1 -type d -mtime +"$retention" -print 2>/dev/null || true)
  log_info "本次清理过期备份 ${removed} 个（保留 ${retention} 天内）"
  return 0
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file backup_root retention ts rc=0
  parse_args "$@"

  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ -f "$env_file" ]; then
    load_env "$env_file" || log_warn "加载 ${env_file} 失败：使用默认值"
  else
    log_warn ".env 不存在（${env_file}）：数据库名与容器名将使用内置默认值"
  fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  backup_root="${BACKUP_ROOT_ARG:-${BACKUP_DIR:-${DATA_DIR}/backups}}"
  retention="${RETENTION:-${BACKUP_RETENTION_DAYS:-30}}"
  ts="$(date '+%Y%m%d_%H%M%S')"
  BACKUP_DIR="${backup_root}/${ts}"

  # 备份日志：${LOG_DIR}/backups/backup-<ts>.log（独立于部署日志，便于按次追溯）
  hc_set_log_file "${LOG_DIR}/backups/backup-${ts}.log"
  hc_log_begin

  if ! command_exists docker; then
    log_error "缺少 docker 命令：备份依赖 docker exec 调用 pg_dump"
    return 1
  fi
  if ! install -d -m 0750 "$BACKUP_DIR"; then
    log_error "创建备份目录失败：${BACKUP_DIR}（请检查磁盘空间与权限）"
    return 1
  fi
  log_info "===== HunterCore 数据备份（开始：$(date '+%Y-%m-%d %H:%M:%S')）====="
  log_info "备份目录：${BACKUP_DIR}；日志：${LOG_FILE}；保留天数：${retention}"

  # ① PostgreSQL 全量（关键项，失败即返回非 0）
  backup_pg_full || rc=1
  # ② TimescaleDB 热数据（失败仅告警）
  backup_timescale_hot || log_warn "TimescaleDB 热数据备份存在失败项（全量 dump 仍可用）"
  # ③ 配置
  backup_config || rc=1
  # ④ Redis
  backup_redis
  # ⑤ MinIO 提示
  if [ "$SHOW_MINIO_HINT" -eq 1 ]; then
    print_minio_hint
  fi

  # ⑥ 清理过期备份
  cleanup_old_backups "$backup_root" "$retention"

  # ⑦ 输出产物清单与总大小
  log_info "===== 备份产物清单 ====="
  if command_exists du; then
    ls -lh "$BACKUP_DIR" 2>/dev/null | sed 's/^/    /' || true
  fi
  local total_size
  total_size="$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1 || printf 'N/A')"
  log_info "备份目录总大小：${total_size}"

  if [ "$rc" -ne 0 ]; then
    log_error "备份存在失败项：请按上方提示排查后重试（备份目录保留：${BACKUP_DIR}）"
    return 1
  fi
  log_success "备份完成：${BACKUP_DIR}（总大小 ${total_size}）"
  log_warn "备份文件含口令与密钥：请立即离线保存/加密归档，并落实异地容灾（RPO ≤5min / RTO ≤1h，docs/01 §7-⑨）"
  return 0
}

main "$@"