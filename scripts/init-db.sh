#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— 数据库初始化（init-db.sh）
#
# 用途：在 PostgreSQL / TimescaleDB 容器就绪后，按契约顺序应用 SQL 产物并校验：
#   ① schema.sql       ：8 个 schema + 11 张业务表（contracts/database/ddl/00-04）
#   ② timescaledb.sql   ：2 张 hypertable（1 天分块 / 90 天保留）+ 保留策略（ddl/05）
#                        ⚠ 依赖 schema.sql（需 data_collector/data_analytics schema 与触发器函数），
#                          因此默认与业务表同库（docs/01 §1.4① 契约单实例形态）；
#                          若编排中存在独立 timescale 服务，则在其库内先建 schema 再建时序表。
#   ③ init-data.sql     ：4 角色 + 16 权限点 + 角色权限矩阵 + 默认 admin + 验证车辆
#                        （幂等：ON CONFLICT DO NOTHING；可用 -v admin_password_hash 注入自定义口令）
#
# 用法：
#   sudo bash scripts/init-db.sh [选项]
#
# 参数：
#   --help                    显示本帮助
#   --admin-password <plain>  首次初始化时注入 admin 口令（本地生成 bcrypt(12) 哈希）
#   --admin-password-hash <h> 直接注入 bcrypt 哈希（跳过本地哈希计算）
#   --env <file>              指定 .env 路径
#
# 示例：
#   sudo bash /opt/hunter-edge/scripts/init-db.sh
#   sudo bash /opt/hunter-edge/scripts/init-db.sh --admin-password 'MyS3cure#2026'
#
# 幂等：所有 SQL 使用 IF NOT EXISTS / ON CONFLICT DO NOTHING，可重复执行。
# 依赖：common.sh（同目录）、容器 hunter-postgres（+ 可选 hunter-timescale）、宿主机 SQL 产物 ${APP_DIR}/sql/
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # POSTGRES_USER/POSTGRES_DB/TIMESCALE_* 由 .env 注入

ADMIN_PASSWORD=""
ADMIN_PASSWORD_HASH=""
ENV_FILE_ARG=""
SQL_FILES=(schema.sql timescaledb.sql init-data.sql)
# 期望的结构数量（source: contracts/database/ddl）
EXPECTED_SCHEMAS=8
EXPECTED_TABLES=13     # 11 张业务表 + 2 张 hypertable
EXPECTED_HYPERTABLES=2

usage() {
  cat <<'EOF'
HunterCore 数据库初始化脚本

用途：
  在 PostgreSQL / TimescaleDB 就绪后应用 SQL 产物（按契约顺序）并校验结构：
    sql/schema.sql → sql/timescaledb.sql → sql/init-data.sql
  校验项：schema 数（8）、表数（13 = 11 业务表 + 2 hypertable）、TimescaleDB 扩展版本、
          hypertable 数（2）、admin 用户与验证车辆。

用法：
  sudo bash scripts/init-db.sh [选项]

参数：
  --help                     显示本帮助
  --admin-password <plain>   注入 admin 首次登录口令（本地 bcrypt(12)，需 python3-bcrypt 或 apache2-utils）
  --admin-password-hash <h>  直接注入 bcrypt 哈希（形如 $2b$12$...）
  --env <file>               .env 路径（默认 /opt/hunter-edge/.env）

示例：
  sudo bash /opt/hunter-edge/scripts/init-db.sh
  sudo bash /opt/hunter-edge/scripts/init-db.sh --admin-password 'HunterEdge#2026'

说明：
  · G-06（设计文档 14.1）：不再提供内置已知口令；口令来源优先级：
      --admin-password / --admin-password-hash > .env 的 ADMIN_PASSWORD（gen-passwords.sh 随机生成）；
    三者均缺失时 admin 以「禁用 + 占位哈希 + 强制改密」创建，需运维重置口令后启用；
  · 无论何种路径，admin.must_change_password=true，首次登录强制修改口令；
  · 幂等：可重复执行（表/schema 用 IF NOT EXISTS，初始数据用 ON CONFLICT DO NOTHING）。
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --admin-password)
        [ $# -ge 2 ] || die "--admin-password 需要一个明文口令参数"
        ADMIN_PASSWORD="$2"
        shift 2
        ;;
      --admin-password-hash)
        [ $# -ge 2 ] || die "--admin-password-hash 需要一个 bcrypt 哈希参数"
        ADMIN_PASSWORD_HASH="$2"
        shift 2
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
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

# psql_pg <sql>：在业务库执行 SQL（-tA 静默取值）
psql_pg() {
  docker exec -i "$C_POSTGRES" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c "$1"
}

# psql_ts <sql>：在独立时序库执行 SQL
psql_ts() {
  docker exec -i "$C_TIMESCALE" psql -U "${TIMESCALE_USER:-hunter}" -d "${TIMESCALE_DB:-hunter_ts}" -tA -c "$1"
}

# apply_sql <container> <user> <db> <file> [psql -v 参数...]：以 stdin 方式应用 SQL 文件（ON_ERROR_STOP=1）
apply_sql() {
  local container="$1" user="$2" db="$3" file="$4"
  shift 4
  if [ ! -f "$file" ]; then
    log_error "SQL 文件不存在：${file}"
    return 1
  fi
  if docker exec -i "$container" psql -U "$user" -d "$db" -v ON_ERROR_STOP=1 "$@" <"$file"; then
    log_success "已应用：$(basename "$file") → ${db}"
    return 0
  fi
  log_error "应用失败：$(basename "$file") → ${db}（容器 ${container}）"
  log_error "排查建议：① docker logs ${container} --tail=100；② 单条语句复现：docker exec -i ${container} psql -U ${user} -d ${db} < ${file}"
  return 1
}

# hash_admin_password <plain>：生成 bcrypt(12) 哈希（python3-bcrypt 优先，回退 htpasswd）
hash_admin_password() {
  local plain="$1"
  if command_exists python3 && python3 -c 'import bcrypt' >/dev/null 2>&1; then
    python3 -c 'import bcrypt,sys; print(bcrypt.hashpw(sys.argv[1].encode(), bcrypt.gensalt(rounds=12)).decode())' "$plain"
    return 0
  fi
  if command_exists htpasswd; then
    # htpasswd 输出 ":$2y$12$..."，去掉前导冒号并将 $2y$ 归一为 $2b$（与 passlib/bcrypt 兼容）
    htpasswd -bnBC 12 "" "$plain" | tr -d ':\n' | sed 's/^\$2y\$/\$2b\$/'
    return 0
  fi
  log_error "无法生成 bcrypt 哈希：需要 python3 + bcrypt 模块，或 apache2-utils（htpasswd）"
  log_error "安装其一：apt-get install -y python3-bcrypt  或  apt-get install -y apache2-utils"
  log_error "也可外部生成后使用：--admin-password-hash '\$2b\$12\$...'"
  return 1
}

# resolve_sql_dir：确定 SQL 产物目录（服务器 ${APP_DIR}/sql，回退仓库 infra/deploy/sql）
resolve_sql_dir() {
  local dir="${APP_DIR}/sql"
  if [ -f "${dir}/schema.sql" ]; then
    printf '%s' "$dir"
    return 0
  fi
  if [ -f "${APP_DIR}/infra/deploy/sql/schema.sql" ]; then
    printf '%s' "${APP_DIR}/infra/deploy/sql"
    return 0
  fi
  log_error "未找到 SQL 产物：${dir}/schema.sql（或 infra/deploy/sql/schema.sql）"
  return 1
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file sql_dir hash="" schemas tables hypertables ext_version admin_count vehicle_count
  parse_args "$@"
  hc_log_begin

  if ! command_exists docker; then
    log_error "缺少 docker 命令：本脚本通过 docker exec 操作容器"
    return 1
  fi
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  # CLI 口令参数优先于 .env（load_env 会导出 .env 变量，先暂存再恢复）
  local cli_admin_password="$ADMIN_PASSWORD" cli_admin_password_hash="$ADMIN_PASSWORD_HASH"
  load_env "$env_file" || return 1
  if [ -n "$cli_admin_password" ]; then ADMIN_PASSWORD="$cli_admin_password"; fi
  if [ -n "$cli_admin_password_hash" ]; then ADMIN_PASSWORD_HASH="$cli_admin_password_hash"; fi
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE
  if ! require_env POSTGRES_USER POSTGRES_DB; then
    log_error "POSTGRES_USER / POSTGRES_DB 未配置，请检查 ${env_file}"
    return 1
  fi
  sql_dir="$(resolve_sql_dir)" || return 1

  # ---------- 1) 等待 PostgreSQL 就绪（最多 60s） ----------
  if ! wait_for "docker exec ${C_POSTGRES} pg_isready -U '${POSTGRES_USER}' -d '${POSTGRES_DB}'" \
    "PostgreSQL（${C_POSTGRES} / 库 ${POSTGRES_DB}）" 60; then
    log_error "PostgreSQL 未就绪：docker logs ${C_POSTGRES} --tail=100"
    return 1
  fi

  # ---------- 2) schema.sql + init-data.sql（业务库） ----------
  log_info "应用业务库结构：schema.sql（8 schema + 11 表，IF NOT EXISTS 幂等）"
  apply_sql "$C_POSTGRES" "$POSTGRES_USER" "$POSTGRES_DB" "${sql_dir}/schema.sql" || return 1

  # ---------- 3) timescaledb.sql ----------
  # 契约单实例形态：时序表与业务表同库（timescaledb.sql 依赖 schema.sql 建立的 schema/触发器）
  log_info "应用时序结构：timescaledb.sql（2 hypertable，1 天分块 / 90 天保留）"
  apply_sql "$C_POSTGRES" "$POSTGRES_USER" "$POSTGRES_DB" "${sql_dir}/timescaledb.sql" || return 1

  # 可选：编排中存在独立 timescale 服务时，其库内同样需要 schema + 时序表
  if docker inspect "$C_TIMESCALE" >/dev/null 2>&1; then
    log_info "检测到独立时序实例 ${C_TIMESCALE}：初始化库 ${TIMESCALE_DB:-hunter_ts}（schema.sql → timescaledb.sql）"
    wait_for "docker exec ${C_TIMESCALE} pg_isready -U '${TIMESCALE_USER:-hunter}' -d '${TIMESCALE_DB:-hunter_ts}'" \
      "TimescaleDB（宿主 127.0.0.1:5433）" 60 || return 1
    apply_sql "$C_TIMESCALE" "${TIMESCALE_USER:-hunter}" "${TIMESCALE_DB:-hunter_ts}" "${sql_dir}/schema.sql" || return 1
    apply_sql "$C_TIMESCALE" "${TIMESCALE_USER:-hunter}" "${TIMESCALE_DB:-hunter_ts}" "${sql_dir}/timescaledb.sql" || return 1
  else
    log_info "无独立 timescale 容器：采用契约单实例形态（时序表与业务表同库 hunter_core）"
  fi

  # ---------- 4) 初始数据（admin/角色/权限/验证车辆；幂等） ----------
  if [ -n "$ADMIN_PASSWORD_HASH" ]; then
    hash="$ADMIN_PASSWORD_HASH"
    log_info "使用 --admin-password-hash 注入 admin 口令哈希"
  elif [ -n "$ADMIN_PASSWORD" ]; then
    log_info "为 admin 生成 bcrypt(12) 口令哈希（明文不落日志；来源 CLI 或 .env ADMIN_PASSWORD）"
    hash="$(hash_admin_password "$ADMIN_PASSWORD")" || return 1
  fi
  if [ -n "$hash" ]; then
    case "$hash" in
      '$2a$'* | '$2b$'* | '$2y$'*) : ;;
      *)
        log_error "admin 口令哈希格式非法（需以 \$2a\$/\$2b\$/\$2y\$ 开头）"
        return 1
        ;;
    esac
    apply_sql "$C_POSTGRES" "$POSTGRES_USER" "$POSTGRES_DB" "${sql_dir}/init-data.sql" \
      -v "admin_password_hash=${hash}" || return 1
    log_warn "admin 口令已按自定义值注入；请立即登录并确认权限（密文不落日志）"
  else
    log_warn "未指定 admin 口令（G-06）：init-data.sql 不携带已知口令，admin 将以「禁用 + 占位哈希」创建；"
    log_warn "  启用方式：psql -v admin_password_hash=\"<bcrypt>\" 重跑本脚本，或运维后台重置并置 status=enabled"
    apply_sql "$C_POSTGRES" "$POSTGRES_USER" "$POSTGRES_DB" "${sql_dir}/init-data.sql" || return 1
  fi

  # ---------- 5) 结构与数据校验 ----------
  log_info "===== 校验结果 ====="
  schemas="$(psql_pg "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('vehicle_svc','user_svc','scene_svc','ota_svc','data_collector','data_analytics','remote_control','gateway');")"
  tables="$(psql_pg "SELECT count(*) FROM information_schema.tables WHERE table_schema IN ('vehicle_svc','user_svc','scene_svc','ota_svc','data_collector','data_analytics') AND table_type='BASE TABLE';")"
  hypertables="$(psql_pg "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema IN ('data_collector','data_analytics');")"
  ext_version="$(psql_pg "SELECT extversion FROM pg_extension WHERE extname='timescaledb';")"
  admin_count="$(psql_pg "SELECT count(*) FROM user_svc.users WHERE username='admin';")"
  vehicle_count="$(psql_pg "SELECT count(*) FROM vehicle_svc.vehicles;")"

  printf '  %-28s %s (期望 %s)\n' "schema 数" "${schemas:-N/A}" "$EXPECTED_SCHEMAS"
  printf '  %-28s %s (期望 %s)\n' "表数" "${tables:-N/A}" "$EXPECTED_TABLES"
  printf '  %-28s %s (期望 %s)\n' "hypertable 数" "${hypertables:-N/A}" "$EXPECTED_HYPERTABLES"
  printf '  %-28s %s\n' "TimescaleDB 扩展版本" "${ext_version:-N/A}"
  printf '  %-28s %s (期望 ≥1)\n' "admin 用户数" "${admin_count:-N/A}"
  printf '  %-28s %s (期望 ≥1)\n' "车辆台账数" "${vehicle_count:-N/A}"

  local rc=0
  [ "${schemas:-0}" -ge "$EXPECTED_SCHEMAS" ] || { log_error "schema 数不足（${schemas:-0} < ${EXPECTED_SCHEMAS}）"; rc=1; }
  [ "${tables:-0}" -ge "$EXPECTED_TABLES" ] || { log_error "表数不足（${tables:-0} < ${EXPECTED_TABLES}）"; rc=1; }
  [ "${hypertables:-0}" -ge "$EXPECTED_HYPERTABLES" ] || { log_error "hypertable 数不足（${hypertables:-0} < ${EXPECTED_HYPERTABLES}）：请确认已加载 timescaledb 扩展"; rc=1; }
  if [ -z "${ext_version:-}" ]; then
    log_error "TimescaleDB 扩展未安装：请确认镜像为 timescale/timescaledb:2.13.1-pg15（docs/01 §1.4⑧）"
    rc=1
  fi
  [ "${admin_count:-0}" -ge 1 ] || { log_error "admin 用户缺失：请检查 init-data.sql 是否成功执行"; rc=1; }
  [ "$rc" -eq 0 ] || return 1

  log_success "数据库初始化完成（库 ${POSTGRES_DB}，容器 ${C_POSTGRES}）"
  log_warn "默认 admin 口令请在首次登录后立即修改；生产环境建议用 --admin-password 注入自定义口令重新初始化"
  return 0
}

main "$@"