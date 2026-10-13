#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— Kafka 平台内部 Topic 初始化（init-kafka.sh）
#
#   用途：等待 broker 就绪后，按 contracts/kafka/topics.yaml 创建平台内部 Topic
#       （含 ingest-platform 的 vehicle.bag.* / vehicle.agent.* / platform.* / ingest.dlt，V1.20.4）
#       与对应的死信 Topic（{topic}.dlq）（分区数/保留时间严格对齐契约，禁止新增未定义 Topic），并校验创建结果。
#
#   ⚠ 死信 Topic 必须显式创建：broker 关 auto.create.topics.enable，而消费侧转投
#     {topic}.dlq 属于投递行为——Topic 不存在时报 _UNKNOWN_TOPIC（不可重试），
#     非法消息会被静默丢弃（无死信可查、无法人工重放）。
#
#   Topic            分区  保留     用途
#   telemetry_raw     12   7 天     原始遥测（data-collector 生产）
#   telemetry_clean   12   7 天     清洗后遥测
#   event_raw          6   30 天    事件数据
#   sensor_file        3   7 天     传感器文件通知
#   analytics_result   6   30 天    分析结果
#   alert_event        3   30 天    告警事件
#   algorithm_metrics  6   7 天     算法指标（Flink 产出→data-analytics 落库）
#
# ⚠ 车端 Topic（hunter.{vehicle_id}.telemetry/event/health/...）在车辆注册时按契约创建，
#   不在本脚本范围内；副本数单机为 1（生产集群为 3，min.insync.replicas=2）。
# ⚠ 内部监听为 SASL_PLAINTEXT（.env: KAFKA_INTERNAL_SECURITY_PROTOCOL）时，脚本自动附带
#   --command-config（与 infra/k8s/jobs/kafka-init-job.yaml 同策略）。
#
# 用法：
#   sudo bash scripts/init-kafka.sh [选项]
#
# 参数：
#   --help              显示本帮助
#   --env <file>        指定 .env 路径
#   --check-only        **只读对账**（V1.19.15）：不创建、不修改，只输出三类清单——
#                       ① 契约要求但集群缺失的 Topic；② 已存在但分区数/retention.ms 与契约不符；
#                       ③ 集群里存在但不在契约内的 Topic（车端 hunter.* 与 __* 系统 Topic 除外）。
#                       存在缺失/偏差时退出码非 0；**查不到清单不会当成“Topic 没建”**（同 V1.19.13
#                       的输出形态门禁），取证与升级前体检请优先跑这一项。
#   --scram-users       额外创建/更新车端 SCRAM 账号（hunter-client / hunter-vehicle）
#                       ⚠ 前置条件：broker 已存在可用的管理员 SCRAM 凭据（KAFKA_SASL_USER）
#
# 示例：
#   sudo bash /opt/hunter-core/scripts/init-kafka.sh
#   sudo bash /opt/hunter-core/scripts/init-kafka.sh --check-only
#   sudo bash /opt/hunter-core/scripts/init-kafka.sh --scram-users
#
# 幂等：--create --if-not-exists（已存在 Topic 不报错、不修改配置）；--check-only 完全只读。
# 依赖：common.sh（同目录）、容器 hunter-kafka
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # KAFKA_* 由 .env 注入

ENV_FILE_ARG=""
CREATE_SCRAM_USERS=0
CHECK_ONLY=0
# Topic 定义（名称:分区数:保留毫秒）—— 单一事实来源：contracts/kafka/topics.yaml
TOPIC_SPECS=(
  "telemetry_raw:12:604800000"      # 7 天
  "telemetry_clean:12:604800000"    # 7 天
  "event_raw:6:2592000000"          # 30 天
  "sensor_file:3:604800000"         # 7 天
  "analytics_result:6:2592000000"   # 30 天
  "alert_event:3:2592000000"        # 30 天
  "algorithm_metrics:6:604800000"    # 7 天（Flink 产出→data-analytics 落库）
)
# 死信 Topic（topics.yaml#naming：dlq_pattern / dlq_retention_ms / 分区继承源 Topic）
DLQ_RETENTION_MS=2592000000

# ---- ingest-platform 数据消费/解析 Topic（V1.20.4 纳入编排；契约 topics.yaml#ingest_platform_topics）----
#   ⚠ 与上方平台内部 Topic 不同，本清单**不纳入** verify_data_layer.py 校验 6 的「平台 Topic 四方同步」：
#     ① ingest 走自建死信 ingest.dlt，不按 {topic}.dlq 逐条派生；
#     ② 不写 docker/kafka/create-topics.sh 与 k8s kafka-init-job（compose 优先，仅由本脚本预建）；
#     ③ broker auto.create.topics.enable=false，生产/订阅目标均需在此显式创建。
INGEST_TOPIC_SPECS=(
  #   订阅（车端/agent 产出、ingest 消费）：
  "vehicle.bag.manifest:3:604800000"
  "vehicle.bag.task.status:3:604800000"
  "vehicle.bag.parse.progress:3:604800000"
  "vehicle.bag.artifact.uploaded:6:604800000"
  "vehicle.agent.heartbeat:6:604800000"
  "vehicle.agent.alert:3:2592000000"
  "vehicle.agent.cmd.ack:3:604800000"
  #   生产（平台侧；auto.create.topics.enable=false 时生产目标 Topic 必须预建）：
  "vehicle.agent.cmd:3:604800000"          # 下行命令
  "platform.derived.ready.v1:3:2592000000" # 派生产物就绪
  "platform.bag.qc_failed.v1:3:2592000000"
  "platform.clip.created.v1:3:2592000000"
  "platform.dataset.ready.v1:3:2592000000"
  #   DLT 解析/字段级无效消息专用死信（replay 工具取回源）：
  "ingest.dlt:3:2592000000"
)
# 被 ingest 消费者订阅的 Topic：基础设施类失败（连接/超时）经 hunter_common 消费者转投 {topic}.dlq
# （数据/字段级失败才走 ingest.dlt）；⚠ auto.create.topics.enable=false，这些 {topic}.dlq 必须显式创建，
#   否则 infra 失败消息转投报 UNKNOWN_TOPIC（不可重试）被静默丢弃。分区继承源 Topic、保留取 DLQ_RETENTION_MS。
INGEST_CONSUMED_TOPICS=(
  "vehicle.bag.manifest"
  "vehicle.bag.task.status"
  "vehicle.bag.parse.progress"
  "vehicle.bag.artifact.uploaded"
  "vehicle.agent.heartbeat"
  "vehicle.agent.alert"
  "vehicle.agent.cmd.ack"
)

usage() {
  cat <<'EOF'
HunterCore Kafka 内部 Topic 初始化脚本

用途：
  等待 broker 就绪后创建 7 个平台内部 Topic + 7 个死信 Topic（分区数与保留时间严格对齐 Kafka 契约）并校验：
    telemetry_raw(12,7d) telemetry_clean(12,7d) event_raw(6,30d)
    sensor_file(3,7d) analytics_result(6,30d) alert_event(3,30d) algorithm_metrics(6,7d)
    {以上每个}.dlq（分区继承源 Topic，保留 30 天）——缺死信 Topic 时非法消息会被静默丢弃
  另创建 13 个 ingest-platform Topic（V1.20.4：vehicle.bag.*/vehicle.agent.*/platform.*/ingest.dlt）；
    ingest 走自建 DLT（ingest.dlt），**不**逐条派生 {topic}.dlq，也不纳入平台 Topic 四方同步门禁

用法：
  sudo bash scripts/init-kafka.sh [选项]

参数：
  --help          显示本帮助
  --env <file>    .env 路径（默认 /opt/hunter-core/.env）
  --check-only    **只读对账**：只报「缺哪几个 / 哪几个配置不符 / 哪几个不在契约内」，不建不改；
                  存在缺失或偏差则退出码非 0（查不到清单另报，不会归成“Topic 没建”）
  --scram-users   额外创建/更新车端 SCRAM 账号（需 broker 已有可用管理员凭据）

示例：
  sudo bash /opt/hunter-core/scripts/init-kafka.sh
  sudo bash /opt/hunter-core/scripts/init-kafka.sh --check-only     # 取证/体检用这个
  sudo bash /opt/hunter-core/scripts/init-kafka.sh --scram-users

校验：
  （推荐走 common.sh 封装，**自动附带 --bootstrap-server 与 --command-config**）
  sudo bash -c 'source scripts/common.sh; load_env; kafka_topics_cli --list'
  （裸写时两个参数都不可缺：docker exec 的 shell 里找不到 kafka-topics.sh（bitnami 仅在
    entrypoint 环境设该 PATH），而内部监听固定 SASL_PLAINTEXT——缺 --command-config 不会报
    权限错，而是报 `Timed out waiting for a node assignment`，极易误读为 broker 未就绪）
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
      --check-only)
        CHECK_ONLY=1
        shift
        ;;
      --scram-users)
        CREATE_SCRAM_USERS=1
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

# create_topic <name> <partitions> <retention_ms> <rf>
create_topic() {
  local name="$1" partitions="$2" retention="$3" rf="$4"
  if kafka_topics_cli --list 2>/dev/null | grep -Fxq "$name"; then
    log_info "Topic 已存在，跳过（幂等）：${name}"
    return 0
  fi
  if kafka_topics_cli --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor "$rf" \
    --config "retention.ms=${retention}"; then
    log_success "已创建 Topic：${name}（partitions=${partitions}, retention=${retention}ms, rf=${rf}）"
    return 0
  fi
  log_error "创建 Topic 失败：${name}"
  log_error "排查建议：① 内部监听认证（KAFKA_INTERNAL_SECURITY_PROTOCOL=${KAFKA_INTERNAL_SECURITY_PROTOCOL:-SASL_PLAINTEXT}，broker 固定为 SASL_PLAINTEXT，需与 .env 一致且 KAFKA_SASL_USER/PASSWORD 有效）；② docker logs ${C_KAFKA} --tail=100；③ 分区数不可少于既有分区"
  return 1
}

# verify_topic <name> <partitions> <retention_ms>：核对分区数与 retention.ms
verify_topic() {
  local name="$1" expect_partitions="$2" expect_retention="$3" desc actual_partitions actual_retention
  desc="$(kafka_topics_cli --describe --topic "$name" 2>/dev/null || true)"
  if [ -z "$desc" ]; then
    log_error "Topic 校验失败（不存在）：${name}"
    return 1
  fi
  actual_partitions="$(printf '%s\n' "$desc" | grep -oE 'PartitionCount:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -n1)"
  actual_retention="$(printf '%s\n' "$desc" | awk '/retention.ms=/ {match($0, /retention.ms=[0-9]+/); if (RSTART) {print substr($0, RSTART+13, RLENGTH-13); exit}}')"
  if [ "${actual_partitions:-0}" = "$expect_partitions" ] && [ "${actual_retention:-0}" = "$expect_retention" ]; then
    log_info "校验通过：${name}（partitions=${actual_partitions}, retention.ms=${actual_retention}）"
    return 0
  fi
  log_warn "Topic 配置与契约不一致：${name}（实际 partitions=${actual_partitions:-N/A}, retention.ms=${actual_retention:-N/A}；期望 ${expect_partitions}/${expect_retention}）"
  log_warn "如需对齐：kafka_configs_cli --alter --entity-type topics --entity-name ${name} --add-config retention.ms=${expect_retention}（分区数不可减少）"
  return 1
}

# check_only_alignment：**只读对账**（V1.19.15）——不建不改，只输出三类清单：
#   ① 契约要求但集群缺失的 Topic；② 存在但分区数/retention.ms 与契约不符；③ 契约外 Topic。
# 返回 1 表示「有缺失/偏差」或「根本没读到清单」（两者文案不同，结论不可合并）。
check_only_alignment() {
  local raw names lines spec name partitions retention listed total
  local missing=0 drifted=0 extra=0
  local -a expected=()

  raw="$(kafka_topics_cli --list 2>&1 || true)"
  lines="$(printf '%s\n' "$raw" | grep -cE '[^[:space:]]' || true)"
  names="$(printf '%s\n' "$raw" | grep -E '^[A-Za-z0-9._-]+$' || true)"
  # 形态门禁（与 common.sh 的 kafka_topic_count 同源，V1.19.13）：一行不像 Topic 名就是「没查到清单」，
  #   绝不能把认证失败/参数错的报错文本当成「Topic 都没建」——那会把运维引导去建 Topic 而不是查凭据。
  if [ "${lines:-0}" -le 0 ] || \
    [ "$(printf '%s\n' "$names" | grep -cE '[^[:space:]]' || true)" != "$lines" ]; then
    log_error "读不到集群 Topic 清单（下面就是原始输出，**按它归因**而不是按“Topic 缺失”归因）："
    printf '%s\n' "$raw" | sed 's/^/  /' >&2
    log_error "常见成因：未 sudo（.env 权限 600、仅 root 可读）/ KAFKA_SASL_USER 与 .env 漂移 / broker 未就绪"
    return 1
  fi

  for spec in "${TOPIC_SPECS[@]}"; do
    expected+=("$spec")
    expected+=("${spec%%:*}.dlq:$(printf '%s' "$spec" | cut -d: -f2):${DLQ_RETENTION_MS}")
  done
  for spec in "${INGEST_TOPIC_SPECS[@]}"; do
    expected+=("$spec")
  done
  for consumed in "${INGEST_CONSUMED_TOPICS[@]}"; do
    for spec in "${INGEST_TOPIC_SPECS[@]}"; do
      if [ "${spec%%:*}" = "$consumed" ]; then
        expected+=("${consumed}.dlq:$(printf '%s' "$spec" | cut -d: -f2):${DLQ_RETENTION_MS}")
        break
      fi
    done
  done

  log_info "===== 只读对账（契约：contracts/kafka/topics.yaml；不创建、不修改）====="
  for spec in "${expected[@]}"; do
    name="${spec%%:*}"
    partitions="$(printf '%s' "$spec" | cut -d: -f2)"
    retention="$(printf '%s' "$spec" | cut -d: -f3)"
    if ! printf '%s\n' "$names" | grep -Fxq "$name"; then
      log_warn "缺失 Topic：${name}（契约期望 partitions=${partitions}, retention.ms=${retention}）"
      missing=$((missing + 1))
      continue
    fi
    verify_topic "$name" "$partitions" "$retention" || drifted=$((drifted + 1))
  done

  # 契约外：既不是契约 Topic（平台内部+死信+ingest），也不是车端 hunter.*（车辆注册时创建）与 __*（系统 Topic）
  while IFS= read -r listed; do
    [ -n "$listed" ] || continue
    case "$listed" in
      __* | hunter.*) continue ;;
    esac
    if ! printf '%s\n' "${expected[@]}" | cut -d: -f1 | grep -Fxq "$listed"; then
      log_warn "契约外 Topic（本脚本不删除，请人工确认来源）：${listed}"
      extra=$((extra + 1))
    fi
  done <<< "$names"

  total="$(printf '%s\n' "$names" | grep -cE '[^[:space:]]' || true)"
  log_info "对账汇总：契约要求 ${#expected[@]} 个（含死信）｜缺失 ${missing}｜配置偏差 ${drifted}"
  log_info "　　　　集群实际 ${total} 个 Topic，其中契约外 ${extra} 个（未计 hunter.* 与 __*）"
  if [ "$missing" -ne 0 ] || [ "$drifted" -ne 0 ]; then
    log_error "只读对账不通过（缺失 ${missing} / 偏差 ${drifted}）：补齐请跑 sudo bash scripts/init-kafka.sh（幂等）"
    return 1
  fi
  log_success "集群 Topic 与契约一致（${#expected[@]} 个齐备、配置无偏差）"
  return 0
}

# create_scram_user <username> <password>：创建/更新 SCRAM-SHA-512 账号
create_scram_user() {
  local username="$1" password="$2"
  if [ -z "$username" ] || [ -z "$password" ]; then
    log_warn "SCRAM 账号参数为空，跳过（请检查 .env 的 KAFKA_SASL_* 变量）"
    return 0
  fi
  # ℞ V1.19.12：原走 kafka_tool_cli 而未带 --bootstrap-server（对 kafka-configs.sh 属 REQUIRED），
  #   命令以退出码 2 结束 → 本函数**从未成功过**（一直走下面的“创建失败”分支）；改走 kafka_configs_cli 封装。
  if kafka_configs_cli --alter --entity-type users --entity-name "$username" \
    --add-config "SCRAM-SHA-512=[password=${password}]"; then
    log_success "已创建/更新 SCRAM 账号：${username}（密文不落日志）"
    return 0
  fi
  log_error "创建 SCRAM 账号失败：${username}"
  log_error "前置条件：broker 需存在可用的管理员凭据（KAFKA_SASL_USER）或 PLAINTEXT 内部监听；"
  log_error "首次 SCRAM 账号通常由 compose 的 broker 配置（listener.name.*.scram-sha-512.sasl.jaas.config）预置"
  return 1
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file rf spec name partitions retention created=0 verified=0 failed=0
  parse_args "$@"
  hc_log_begin

  if ! command_exists docker; then
    log_error "缺少 docker 命令：本脚本通过 docker exec 操作 Kafka 容器"
    return 1
  fi
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE
  rf="${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}"
  if [ "$rf" -gt 1 ]; then
    log_warn "KAFKA_DEFAULT_REPLICATION_FACTOR=${rf}：单机单 broker 无法满足，请确认 broker 数量（生产集群 3）"
  fi

  # ---------- 1) 等待 broker 就绪（最多 60s） ----------
  if ! wait_for "kafka_broker_ready ${C_KAFKA}" "Kafka broker（容器 ${C_KAFKA}）" 60; then
    log_error "Kafka broker 未就绪：docker logs ${C_KAFKA} --tail=100"
    return 1
  fi

  # ---------- 1b) 只读对账模式（--check-only）：给出「差哪几个」的结论，不动集群 ----------
  if [ "$CHECK_ONLY" -eq 1 ]; then
    if check_only_alignment; then
      return 0
    fi
    return 1
  fi

  # ---------- 2) 创建平台 Topic 与死信 Topic（幂等） ----------
  log_info "===== 创建平台内部 Topic + 死信 Topic + ingest Topic（契约：contracts/kafka/topics.yaml）====="
  local -a all_specs=("${TOPIC_SPECS[@]}" "${INGEST_TOPIC_SPECS[@]}")
  local spec dlq_partitions
  for spec in "${TOPIC_SPECS[@]}"; do
    dlq_partitions="$(printf '%s' "$spec" | cut -d: -f2)"
    all_specs+=("${spec%%:*}.dlq:${dlq_partitions}:${DLQ_RETENTION_MS}")
  done
  # ingest 订阅 Topic 的死信（分区继承源 Topic）：与平台内部 Topic 同策略，但仅限被消费的 7 个
  for consumed in "${INGEST_CONSUMED_TOPICS[@]}"; do
    for spec in "${INGEST_TOPIC_SPECS[@]}"; do
      if [ "${spec%%:*}" = "$consumed" ]; then
        all_specs+=("${consumed}.dlq:$(printf '%s' "$spec" | cut -d: -f2):${DLQ_RETENTION_MS}")
        break
      fi
    done
  done
  for spec in "${all_specs[@]}"; do
    name="${spec%%:*}"
    partitions="$(printf '%s' "$spec" | cut -d: -f2)"
    retention="$(printf '%s' "$spec" | cut -d: -f3)"
    if create_topic "$name" "$partitions" "$retention" "$rf"; then
      created=$((created + 1))
    else
      failed=$((failed + 1))
    fi
  done

  # ---------- 3) 校验（分区数与保留时间） ----------
  log_info "===== 校验 Topic 配置 ====="
  for spec in "${all_specs[@]}"; do
    name="${spec%%:*}"
    partitions="$(printf '%s' "$spec" | cut -d: -f2)"
    retention="$(printf '%s' "$spec" | cut -d: -f3)"
    if verify_topic "$name" "$partitions" "$retention"; then
      verified=$((verified + 1))
    else
      failed=$((failed + 1))
    fi
  done

  # ---------- 4) 可选：车端 SCRAM 账号 ----------
  if [ "$CREATE_SCRAM_USERS" -eq 1 ]; then
    log_info "===== 创建/更新车端 SCRAM 账号（--scram-users）====="
    create_scram_user "${KAFKA_SASL_USER:-}" "${KAFKA_SASL_PASSWORD:-}" || failed=$((failed + 1))
    create_scram_user "${KAFKA_SASL_VEHICLE_USER:-}" "${KAFKA_SASL_VEHICLE_PASSWORD:-}" || failed=$((failed + 1))
  else
    log_info "未指定 --scram-users：跳过 SCRAM 账号创建（车端接入前需确保账号已存在）"
  fi

  # ---------- 5) 汇总 ----------
  log_info "===== 当前 Topic 列表 ====="
  kafka_topics_cli --list 2>/dev/null | sed 's/^/  /' || true
  local total
  total="$(kafka_topic_count)"
  log_info "Topic 总数：${total}（含契约 7 个内部 Topic + 7 个死信 Topic + 13 个 ingest Topic 与系统 Topic __consumer_offsets）"

  if [ "$failed" -ne 0 ]; then
    log_error "Kafka 初始化存在 ${failed} 项失败（成功处理 ${created} 项 / 校验通过 ${verified} 项）"
    return 1
  fi
  log_success "Kafka 内部 Topic 初始化完成（${#all_specs[@]} 个 Topic 已就绪且配置与契约一致）"
  log_warn "车端 Topic（hunter.{vehicle_id}.*）与其死信（hunter.{vehicle_id}.*.dlq）在车辆注册时按契约创建；车端接入需 SASL_SSL 9093 与 SCRAM 账号"
  return 0
}

main "$@"