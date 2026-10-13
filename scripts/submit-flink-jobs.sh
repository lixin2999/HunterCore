#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— Flink 流作业提交（submit-flink-jobs.sh）
#
#   用途：把 flink-jobs 已交付的三个 PyFlink 流作业提交到 compose 形态的 JobManager
#       （standalone、无 HA）。K8s 形态由 infra/k8s/flink/04-submit-job.yaml 承担，
#       compose 形态此前**没有任何提交入口**——install.sh Step 11 只启动 flink-jm/flink-tm，
#       作业要靠人工照 README 敲 `flink run`，漏提即巡检 ⑭ 恒报 RUNNING=0（实录 2026-10-13）。
#
#   已交付作业（契约 consumer-groups.yaml「一组一作业」，V1.19.10）：
#     hunter_flink.detection_job              → driving_anomaly_detection
#     hunter_flink.algorithm_performance_job  → algorithm_performance_monitor
#     hunter_flink.ingest_latency_job         → data_quality_monitor_ingest_latency
#
#   幂等：先 `flink list` 按作业名去重（RUNNING/RESTARTING 中已存在则跳过），
#       可安全重复执行；Step 11 每次启动服务后自动调用（容器 recreate 会冲掉作业，
#       本脚本即恢复手段——这也是它独立成脚本的原因：运维可在不跑 install.sh 的情况下单独重提）。
#
#   ⚠ 提交在 flink-jm 容器内进行（`docker exec … flink run -d -pym …`）：
#     - `-pym` 依赖镜像内置 CPython 与 `python` 软链（官方纯 JVM 镜像必报
#       `Cannot run program "python"`，V1.19.2 已切代码内置镜像 hunter-flink/jobs:2.1.1）；
#     - 作业 main() 在提交进程求值 Kafka/Redis 凭据（V1.19.8），故 flink-jm 容器必须
#       带 KAFKA_SASL_*/REDIS_* 环境变量（compose 已下发）；缺凭据=提交期当场失败，
#       不会「RUNNING 假绿静默空转」。
#
# 用法：
#   sudo bash scripts/submit-flink-jobs.sh [选项]
#
# 参数：
#   --help          显示本帮助
#   --env <file>    指定 .env 路径（默认 ${APP_DIR}/.env；本脚本仅用其定位容器与打印摘要）
#   --force         不去重，无条件重提三个作业（会产生重复作业：先手工 cancel 旧作业再用）
#
# 示例：
#   sudo bash /opt/HunterCore/scripts/submit-flink-jobs.sh
#   sudo bash /opt/HunterCore/scripts/submit-flink-jobs.sh --force
#
# 退出码：0 = 三作业全部在列（已提交或本次提交成功）；1 = 存在失败
# 依赖：common.sh（同目录）、容器 hunter-flink-jm
# 日期：2026-10-13  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"

ENV_FILE_ARG=""
FORCE=0

#: 作业清单：模块名 → flink list 里的作业名（与 hunter_flink 各 *_job.py 的 env.execute(...) 一致）
FLINK_JOB_SPECS=(
  "hunter_flink.detection_job:driving_anomaly_detection"
  "hunter_flink.algorithm_performance_job:algorithm_performance_monitor"
  "hunter_flink.ingest_latency_job:data_quality_monitor_ingest_latency"
)

usage() {
  cat <<'EOF'
HunterCore Flink 流作业提交脚本（compose 单机形态）

用途：
  把三个已交付流作业（detection / algorithm_performance / ingest_latency）提交到
  hunter-flink-jm；幂等——已在 RUNNING/RESTARTING 的作业自动跳过。

用法：
  sudo bash scripts/submit-flink-jobs.sh [选项]

参数：
  --help        显示本帮助
  --env <file>  .env 路径（默认 /opt/HunterCore/.env）
  --force       不去重、无条件重提（先手工 cancel 旧作业，避免重复消费）

说明：
  - flink list 的 JVM 噪声行（WARNING: Unknown module: jdk.compiler / SLF4J:）会被过滤，
    既不计入去重判据也不占失败详情；
  - 提交失败先看本脚本末尾输出的 docker logs 摘要，再对照巡检 ⑭/⑮（作业数、检查点目录属主）；
  - 作业镜像改动后需重新构建并 recreate flink-jm/flink-tm（standalone 无 HA，recreate 即丢作业），
    随后重跑本脚本恢复。
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
      --force)
        FORCE=1
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

# flink_running_jobs：输出当前 RUNNING/RESTARTING 作业列表（已滤 JVM 噪声）。
# ⚠ 本函数会被 $(...) 捕获 ⇒ stdout 只准写作业行，日志一律不进本函数。
flink_running_jobs() {
  docker exec "${FLINK_JM_CONTAINER:-$C_FLINK_JM}" flink list 2>&1 \
    | grep -vE '^(WARNING: |SLF4J:)' || true
}

# submit_job <module> <job_name>：单个作业提交（幂等：默认按作业名去重）
submit_job() {
  local module="$1" job_name="$2" container
  container="${FLINK_JM_CONTAINER:-$C_FLINK_JM}"
  if [ "$FORCE" -eq 0 ]; then
    # flink list 作业行形状：`<jobId>  <job_name> (RUNNING|RESTARTING)`（与巡检 ⑭ 同判据）
    if flink_running_jobs | grep -qE "${job_name} +\((RUNNING|RESTARTING)\)"; then
      log_info "作业已在运行，跳过（幂等）：${job_name}"
      return 0
    fi
  fi
  log_info "提交 Flink 作业：${module}（作业名 ${job_name}）→ ${container}"
  # -d（detached）：流作业常驻集群，提交命令随即返回；-pym 走镜像内置 hunter_flink 包
  if docker exec "$container" flink run -d -m localhost:8081 -pym "$module"; then
    log_success "已提交：${job_name}"
    return 0
  fi
  log_error "提交失败：${job_name}（模块 ${module}）"
  log_error "排查建议：① docker logs ${container} --tail=100（提交期堆栈在 JM 侧）；\
② 检查点目录属主（巡检 ⑮，须为容器内 flink 运行用户，V1.19.16~18）；\
③ 凭据（compose flink-jm 需 KAFKA_SASL_*/REDIS_*，缺失=提交期失败，V1.19.8）；\
④ API 守卫：docker exec ${container} python -m hunter_flink.api_guard（V1.19.7）"
  return 1
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file spec module job_name submitted=0 failed=0
  parse_args "$@"
  hc_log_begin

  if ! command_exists docker; then
    log_error "缺少 docker 命令：本脚本通过 docker exec 操作 ${C_FLINK_JM} 容器"
    return 1
  fi
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  if ! docker inspect "${FLINK_JM_CONTAINER:-$C_FLINK_JM}" >/dev/null 2>&1; then
    log_error "容器 ${FLINK_JM_CONTAINER:-$C_FLINK_JM} 不存在：先完成 Step 11（启动 flink-jm/flink-tm）"
    return 1
  fi

  # ---------- 1) 等待 JM REST 就绪（最多 120s；Step 11 已等过，独立运行时兜底） ----------
  if ! wait_for "curl -sf -m 5 $(flink_ui_url)/overview >/dev/null || docker exec ${FLINK_JM_CONTAINER:-$C_FLINK_JM} curl -sf -m 5 http://localhost:8081/overview >/dev/null" \
    "Flink JobManager REST（$(flink_ui_url)）" 120; then
    log_error "JobManager 未就绪：docker logs ${C_FLINK_JM} --tail=100"
    return 1
  fi

  # ---------- 2) 逐作业提交（幂等去重） ----------
  log_info "===== 提交 Flink 流作业（契约 consumer-groups.yaml：一组一作业，共 ${#FLINK_JOB_SPECS[@]} 个）====="
  for spec in "${FLINK_JOB_SPECS[@]}"; do
    module="${spec%%:*}"
    job_name="${spec#*:}"
    if submit_job "$module" "$job_name"; then
      submitted=$((submitted + 1))
    else
      failed=$((failed + 1))
    fi
  done

  # ---------- 3) 汇总（以 flink list 实际读数为准，不拿提交返回值凑数） ----------
  local out running
  out="$(flink_running_jobs)"
  running="$(printf '%s\n' "$out" | grep -cE '\((RUNNING|RESTARTING)\)' || true)"
  log_info "当前 RUNNING/RESTARTING 作业数：${running}（期望 ${#FLINK_JOB_SPECS[@]}）"
  printf '%s\n' "$out" | grep -E '\((RUNNING|RESTARTING)\)' | sed 's/^/  /' || true

  if [ "$failed" -ne 0 ]; then
    log_error "Flink 作业提交存在 ${failed} 项失败（详见上方排查建议）"
    return 1
  fi
  if [ "${running:-0}" -lt "${#FLINK_JOB_SPECS[@]}" ]; then
    log_error "提交命令全部成功，但 flink list 读数 ${running} < ${#FLINK_JOB_SPECS[@]}：作业可能在初始化期失败（FAILED 终态），立即取证：docker exec ${C_FLINK_JM} flink list -a"
    return 1
  fi
  log_success "Flink 三作业已全部提交（入库延迟/算法指标/异常检测随之恢复出数）"
  return 0
}

main "$@"
