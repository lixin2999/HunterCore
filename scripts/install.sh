#!/usr/bin/env bash
# =====================================================================
# HunterCore 数据采集与分析系统 —— 单机一键部署主脚本
#
# 用途：在裸机 Ubuntu 22.04 LTS 上完成全部部署流程（13 步，幂等可续跑）：
#       1 系统初始化 → 2 安装 Docker → 3 准备目录 → 4 生成配置（.env/Nginx）
#       → 5 生成 Kafka 证书 → 6 构建镜像 → 7 启动中间件 → 8 初始化数据库
#       → 9 初始化 Kafka Topic → 10 初始化 MinIO Bucket → 11 启动业务服务
#       → 12 全栈健康检查 → 13 输出部署摘要
#
# 用法：
#   sudo bash scripts/install.sh [选项]
#
# 参数：
#   --help            显示本帮助
#   --skip-docker     跳过 Docker 安装（已装 24.0+ 时使用；仍校验版本与 compose 插件）
#   --skip-build      跳过镜像构建（镜像已存在且版本一致时使用）
#   --env <file>      指定 .env 路径（默认 ${APP_DIR}/.env，即 /opt/hunter-edge/.env）
#   --ip <address>    指定 SERVER_IP（默认自动探测 hostname -I 首个地址；写入 .env 与证书 SAN）
#   --step <n>        从第 n 步开始执行（1-13，用于失败后续跑）
#   -y, --yes         非交互模式：跳过所有确认
#
# 示例：
#   sudo bash /opt/hunter-edge/scripts/install.sh                 # 全流程交互式部署
#   sudo bash /opt/hunter-edge/scripts/install.sh -y              # 全流程非交互（CI/自动化）
#   sudo bash /opt/hunter-edge/scripts/install.sh --step 6 -y     # 镜像已改，从构建续跑
#   sudo bash /opt/hunter-edge/scripts/install.sh --skip-docker --skip-build -y
#
# 关键路径（docs/01-部署概述与环境要求.md §1.2/§5）：
#   应用根目录 /opt/hunter-edge（本脚本父目录：APP_DIR）
#   数据根目录 /data、日志 /var/log/hunter-edge-install.log、部署日志 /var/log/hunter-edge/deploy/
#
# 依赖：common.sh（同目录）、gen-passwords.sh、gen-kafka-certs.sh、init-db.sh、
#       init-kafka.sh、init-minio.sh、health-check.sh、${APP_DIR}/docker-compose.yml
#
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS (bash 5.x) / Docker Engine 24.0+
# =====================================================================
set -euo pipefail

# shellcheck disable=SC2154  # 运行期变量（POSTGRES_USER/POSTGRES_DB/REDIS_PASSWORD/SERVER_IP/KAFKA_* 等）由 ${APP_DIR}/.env 经 load_env 注入，shellcheck 无法静态推断

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"

# ---------------------------------------------------------------------
# 部署期参数（仅控制本脚本行为；运行期配置一律来自 .env）
# ---------------------------------------------------------------------
TOTAL_STEPS=13
ASSUME_YES=0
SKIP_DOCKER=0
SKIP_BUILD=0
START_STEP=1
OPT_IP=""
ENV_FILE_ARG=""
DEPLOY_LOG_DIR="${LOG_DIR}/deploy"   # 部署日志目录（docs/01 §5：deploy/build.log）
BUILD_LOG="${DEPLOY_LOG_DIR}/build.log"
LIMITS_MARKER="# ---- HunterCore 部署要求（install.sh step_1；来源：docs/01 §3.1/§3.3）----"
UFW_RULES=(
  "22/tcp:SSH"
  "80/tcp:web-portal (Nginx)"
  "8080/tcp:api-gateway"
  "9000/tcp:MinIO S3 API"
  "9001/tcp:MinIO Console"
  "9093/tcp:Kafka SASL_SSL (vehicle)"
  "1935/tcp:SRS RTMP"
  "8000/udp:SRS WebRTC"
)
STEP_DESCRIPTIONS=(
  "系统初始化（apt/时区/主机名/hunter 用户/内核参数/关闭 swap/ufw）"
  "安装 Docker Engine 24.0+ 与 Compose v2 插件"
  "准备数据与应用目录（/data、APP_DIR、/var/log/hunter-edge）"
  "生成配置（.env 随机口令、Nginx 配置）"
  "生成 Kafka SASL_SSL 证书（CA/broker/client/JKS）"
  "构建业务服务镜像（docker compose build --parallel）"
  "启动中间件（PostgreSQL/TimescaleDB/Redis/ZooKeeper/Kafka/MinIO/SRS）"
  "初始化数据库（schema/timescaledb/初始数据）"
  "初始化 Kafka 内部 Topic（6 个，契约分区与保留时间）"
  "初始化 MinIO Bucket（7 个）与生命周期规则"
  "启动业务服务（采集/分析/Flink/场景/OTA/远程操控/网关/前端）"
  "全栈健康检查（容器/HTTP/DB/Redis/Kafka/MinIO/资源）"
  "输出部署摘要与后续操作指引"
)

# =====================================================================
# 帮助与参数解析
# =====================================================================
usage() {
  cat <<'EOF'
HunterCore 单机一键部署脚本

用途：
  在 Ubuntu 22.04 LTS 裸机上按 13 个步骤完成 HunterCore 全栈部署
  （中间件 + 6 个微服务 + Flink + 前端），每步幂等，失败可指定 --step 续跑。

用法：
  sudo bash scripts/install.sh [选项]

参数：
  --help            显示本帮助并退出
  --skip-docker     跳过 Docker 安装（已安装 24.0+ 时；仍校验版本与 compose v2 插件）
  --skip-build      跳过镜像构建（复用现有 hunter/* 镜像）
  --env <file>      指定 .env 文件路径（默认 /opt/hunter-edge/.env）
  --ip <address>    指定 SERVER_IP（默认自动探测；影响 Kafka 9093 advertised 与证书 SAN）
  --step <n>        从第 n 步开始（1-13）
  -y, --yes         非交互模式（自动确认）

示例：
  sudo bash scripts/install.sh                      # 交互式全流程
  sudo bash scripts/install.sh -y --ip 192.168.1.10
  sudo bash scripts/install.sh --step 7 -y          # 中间件启动失败修复后续跑
  sudo bash scripts/install.sh --skip-docker --skip-build -y

部署后：
  前端 http://<SERVER_IP>/    API 网关 http://<SERVER_IP>:8080
  继续执行：bash scripts/health-check.sh（健康检查）、bash scripts/daily-check.sh（日常巡检）

日志：/var/log/hunter-edge-install.log（完整输出）、/var/log/hunter-edge/deploy/build.log（构建日志）
EOF
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --help | -h)
        usage
        exit 0
        ;;
      --skip-docker)
        SKIP_DOCKER=1
        shift
        ;;
      --skip-build)
        SKIP_BUILD=1
        shift
        ;;
      --env)
        [ $# -ge 2 ] || die "--env 需要一个文件路径参数"
        ENV_FILE_ARG="$2"
        shift 2
        ;;
      --ip)
        [ $# -ge 2 ] || die "--ip 需要一个 IP 地址参数"
        OPT_IP="$2"
        shift 2
        ;;
      --step)
        [ $# -ge 2 ] || die "--step 需要一个步骤号（1-${TOTAL_STEPS}）"
        START_STEP="$2"
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
  case "$START_STEP" in
    '' | *[!0-9]*) die "--step 必须为 1-${TOTAL_STEPS} 的整数（收到：${START_STEP}）" ;;
  esac
  if [ "$START_STEP" -lt 1 ] || [ "$START_STEP" -gt "$TOTAL_STEPS" ]; then
    die "--step 取值范围为 1-${TOTAL_STEPS}（收到：${START_STEP}）"
  fi
  if [ -n "$ENV_FILE_ARG" ]; then
    HUNTER_ENV_FILE="$ENV_FILE_ARG"
    export HUNTER_ENV_FILE
  fi
  if [ "$ASSUME_YES" -eq 1 ]; then
    log_info "已启用非交互模式（-y）：跳过所有确认"
  fi
}

# =====================================================================
# 预检查（操作系统 / 端口 / 磁盘 / 资源档位）
# =====================================================================
check_os() {
  local os_id="" os_version="" os_codename=""
  if [ -r /etc/os-release ]; then
    # shellcheck source=/dev/null
    . /etc/os-release
    os_id="${ID:-}"
    os_version="${VERSION_ID:-}"
    os_codename="${VERSION_CODENAME:-}"
  fi
  if [ "$os_id" = "ubuntu" ] && [ "$os_version" = "22.04" ]; then
    log_success "操作系统检查通过：Ubuntu 22.04 LTS (${os_codename:-jammy})"
    return 0
  fi
  log_warn "本部署包面向 Ubuntu 22.04 LTS，当前为 ${os_id:-unknown} ${os_version:-unknown}"
  log_warn "非 22.04 系统可继续，但 apt 源（jammy）、内核参数与 Docker 仓库地址可能需人工调整（docs/01 §1.2）"
  return 0
}

# 对外端口占用检查：占用即视为前置条件不满足（docs/01 §3.3 端口清单）
# 例外：端口由本项目容器（hunter-*）自身发布时放行，保证重复部署 / --step 续跑可用
port_owned_by_project() {
  local port="$1" owners
  owners="$(docker ps --format '{{.Names}}\t{{.Ports}}' 2>/dev/null |
    awk -v p=":${port}->" 'index($0, p) > 0 {print $1}')"
  if [ -n "$owners" ]; then
    printf '%s' "$(printf '%s' "$owners" | tr '\n' ',' | sed 's/,$//')"
    return 0
  fi
  return 1
}

check_public_ports() {
  local failures=0 item port_spec desc proto port owner_note
  log_info "检查对外端口占用（被占用则终止部署；本项目容器占用视为通过）"
  for item in "${UFW_RULES[@]}"; do
    port_spec="${item%%:*}"
    desc="${item#*:}"
    port="${port_spec%%/*}"
    proto="${port_spec#*/}"
    if [ "$proto" = "tcp" ]; then
      if check_port "$port"; then
        log_success "端口可用：${port}/tcp（${desc}）"
        continue
      fi
    elif command_exists ss && ss -Hlun "sport = :${port}" 2>/dev/null | grep -q .; then
      : # UDP 已被占用，进入下方的项目容器豁免判断
    else
      log_success "端口可用：${port}/udp（${desc}）"
      continue
    fi
    # 端口已被占用：若为本项目容器发布端口（重复部署/续跑场景）则放行
    if owner_note="$(port_owned_by_project "$port")"; then
      log_info "端口 ${port} 由本项目容器占用（${owner_note}），视为通过（重复部署/续跑场景）"
      continue
    fi
    log_error "端口 ${port}/${proto} 已被非本项目进程占用（${desc}）"
    failures=1
  done
  if [ "$failures" -ne 0 ]; then
    log_error "对外端口检查未通过：请先释放端口（ss -ltnp / ss -lunp）或调整 .env 中的端口变量后重试"
    return 1
  fi
  return 0
}

preflight() {
  log_info "===== 预检查（root / 操作系统 / 端口 / 磁盘 / 资源档位）====="
  check_root || exit 1
  check_os || exit 1

  # .env 若已存在则先加载；首次部署时尚不存在，使用内置默认端口做检查
  local env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  if [ -f "$env_file" ]; then
    load_env "$env_file" || log_warn "加载 ${env_file} 失败：端口检查将使用内置默认值"
  else
    log_info ".env 尚未生成（首次部署）：端口检查使用内置默认值（80/8080/9000/9001/9093/1935/8000udp）"
  fi

  check_public_ports || exit 1
  check_disk_space "$DATA_DIR" 50 || exit 1

  # /data 独立挂载与资源档位提示（docs/01 §2.1：/data 必须 SSD；低于最低档须降配）
  if mountpoint -q "$DATA_DIR" 2>/dev/null; then
    log_success "${DATA_DIR} 已独立挂载（建议 SSD/NVMe + ext4 noatime，docs/01 §2.1）"
  else
    log_warn "${DATA_DIR} 不是独立挂载点：建议单独挂载 SSD，否则入库延迟 ≤1s、写入 ≥10000 点/秒可能不达标"
  fi
  if [ "$(nproc)" -lt 8 ]; then
    log_warn "CPU 核数 $(nproc) < 8：低于最低配置档（docs/01 §2.1），需下调 Flink 并行度与副本数"
  fi
  local mem_gb
  mem_gb="$(awk '/MemTotal/ {printf "%d", $2/1048576}' /proc/meminfo)"
  if [ "$mem_gb" -lt 32 ]; then
    log_warn "内存 ${mem_gb}GB < 32GB：低于最低配置档（docs/01 §2.1）"
  fi
  if [ "$(swapon --show --noheadings 2>/dev/null | wc -l)" -gt 0 ]; then
    log_warn "当前 swap 处于启用状态；step_1 将关闭 swap（Kafka/PostgreSQL 延迟稳定性要求，docs/01 §3.1）"
  fi
  log_success "预检查通过"
}

show_plan() {
  local n idx desc flag
  printf '\n%s\n' "================= 部署计划（共 ${TOTAL_STEPS} 步）================="
  for n in $(seq 1 "$TOTAL_STEPS"); do
    idx=$((n - 1))
    desc="${STEP_DESCRIPTIONS[$idx]}"
    flag=""
    if [ "$n" -lt "$START_STEP" ]; then
      flag=" [跳过：--step ${START_STEP}]"
    fi
    if [ "$n" -eq 2 ] && [ "$SKIP_DOCKER" -eq 1 ]; then
      flag=" [跳过：--skip-docker]"
    fi
    if [ "$n" -eq 6 ] && [ "$SKIP_BUILD" -eq 1 ]; then
      flag=" [跳过：--skip-build]"
    fi
    printf '  Step %2d/%d  %s%s\n' "$n" "$TOTAL_STEPS" "$desc" "$flag"
  done
  printf '%s\n' "=================================================================="
  log_info "应用根目录：${APP_DIR}（脚本目录：${SCRIPT_DIR}）"
  log_info "数据根目录：${DATA_DIR}（本次部署要求可用空间 ≥50GB）"
  log_info "部署日志：${LOG_FILE}；构建日志：${BUILD_LOG}"
  log_warn "部署将修改系统配置（apt 源/时区/主机名/内核参数/swap/ufw）并创建、重启 Docker 容器"
  if ! confirm "确认开始部署？"; then
    log_info "用户取消部署：未执行任何步骤（使用 --help 查看可用参数）"
    exit 0
  fi
}

# =====================================================================
# 通用辅助（apt / 内核参数 / 目录属主）
# =====================================================================
# apt-get update：300s 超时 + 3 次重试（镜像源抖动是整机部署失败的常见原因）
apt_update_with_retry() {
  local attempt rc=0
  export DEBIAN_FRONTEND=noninteractive
  for attempt in 1 2 3; do
    rc=0
    timeout 300 apt-get -o Acquire::Retries=3 update -qq || rc=$?
    if [ "$rc" -eq 0 ]; then
      log_success "apt-get update 成功（第 ${attempt} 次尝试）"
      return 0
    fi
    if [ "$rc" -eq 124 ]; then
      log_warn "apt-get update 超时（第 ${attempt}/3 次，300s）：请检查网络出口与镜像源"
    else
      log_warn "apt-get update 失败（第 ${attempt}/3 次，退出码 ${rc}）"
    fi
    sleep 5
  done
  log_error "apt-get update 连续 3 次失败"
  log_error "排查建议：① 检查 /etc/apt/sources.list 与 DNS（cat /etc/resolv.conf）；② curl -I https://archive.ubuntu.com 测试出网；③ 代理需配置 /etc/apt/apt.conf.d/proxy.conf"
  return 1
}

# 安装基础工具包（幂等：apt 自动跳过已安装项）
apt_install_basics() {
  local -a pkgs=(
    apt-transport-https ca-certificates curl gnupg gpg lsb-release
    jq unzip zip tar gzip rsync htop net-tools dnsutils iproute2 lsof
    cron tzdata openssl ufw ca-certificates-java openjdk-17-jre-headless
  )
  if DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${pkgs[@]}"; then
    log_success "基础工具包安装完成（${#pkgs[@]} 个，已安装项自动跳过）"
    return 0
  fi
  log_error "基础工具包安装失败：请检查磁盘空间（df -h）与 apt 源可用性"
  return 1
}

# set_sysctl_conf_key <key> <value>：写入 /etc/sysctl.conf（按 key 幂等，已存在则跳过）
set_sysctl_conf_key() {
  local key="$1" value="$2"
  if grep -qE "^[[:space:]]*${key}[[:space:]]*=" /etc/sysctl.conf; then
    log_info "/etc/sysctl.conf 已包含 ${key}，跳过（幂等）"
    return 0
  fi
  printf '%s=%s\n' "$key" "$value" >>/etc/sysctl.conf
  log_success "已写入 ${key}=${value} 到 /etc/sysctl.conf"
}

# ensure_dir <dir> <owner> <mode>：创建目录并校正属主/权限（幂等）
ensure_dir() {
  local dir="$1" owner="$2" mode="$3" current=""
  install -d -m "$mode" "$dir"
  current="$(stat -c '%u:%g' "$dir")"
  if [ "$current" != "$owner" ]; then
    chown -R "$owner" "$dir"
    log_success "已设置属主：${dir} → ${owner}"
  fi
  chmod "$mode" "$dir"
  return 0
}

# =====================================================================
# Step 1：系统初始化
# =====================================================================
step_1_system_init() {
  # 1.1 apt update（超时 + 重试）
  apt_update_with_retry || return 1

  # 1.2 基础工具包
  apt_install_basics || return 1

  # 1.3 时区 Asia/Shanghai（宿主机与全部容器一致，docs/01 §1.2）
  local tz_now
  tz_now="$(timedatectl show -p Timezone --value 2>/dev/null || true)"
  if [ "$tz_now" = "Asia/Shanghai" ]; then
    log_info "时区已为 Asia/Shanghai，跳过（幂等）"
  else
    timedatectl set-timezone Asia/Shanghai || {
      log_error "设置时区失败：请确认 systemd-timedated 可用（systemctl status systemd-timedated）"
      return 1
    }
    log_success "时区已设置为 Asia/Shanghai（原值：${tz_now:-unknown}）"
  fi

  # 1.4 主机名 hunter-edge-server（docs/01 §6）
  local host_now
  host_now="$(hostname)"
  if [ "$host_now" = "hunter-edge-server" ]; then
    log_info "主机名已为 hunter-edge-server，跳过（幂等）"
  else
    hostnamectl set-hostname hunter-edge-server || {
      log_error "设置主机名失败"
      return 1
    }
    log_success "主机名已设置为 hunter-edge-server（原值：${host_now}）"
  fi
  if ! grep -qE "^127\.0\.1\.1[[:space:]]+hunter-edge-server" /etc/hosts; then
    printf '127.0.1.1\thunter-edge-server\n' >>/etc/hosts
    log_success "已在 /etc/hosts 补充 127.0.1.1 hunter-edge-server（避免主机名解析延迟）"
  fi

  # 1.5 创建服务运行用户 hunter（UID 1001，与 Kafka/MinIO 容器属主一致）
  if id -u hunter >/dev/null 2>&1; then
    log_info "用户 hunter 已存在（UID $(id -u hunter)），跳过创建（幂等）"
  else
    useradd --create-home --uid 1001 --shell /bin/bash hunter || {
      log_error "创建用户 hunter（UID 1001）失败：请确认 UID 1001 未被占用（getent passwd 1001）"
      return 1
    }
    log_success "已创建用户 hunter（UID 1001）"
  fi
  if getent group docker >/dev/null 2>&1; then
    usermod -aG docker hunter && log_success "已将 hunter 加入 docker 组"
  else
    log_info "docker 组尚未存在（Docker 未安装）：step_2 安装后会自动加入"
  fi

  # 1.6 文件句柄/进程数上限（幂等：按标记行判断）
  if grep -qF "$LIMITS_MARKER" /etc/security/limits.conf; then
    log_info "/etc/security/limits.conf 已包含 HunterCore 配置，跳过（幂等）"
  else
    {
      printf '\n%s\n' "$LIMITS_MARKER"
      printf 'hunter\tsoft\tnofile\t65536\n'
      printf 'hunter\thard\tnofile\t65536\n'
      printf 'hunter\tsoft\tnproc\t65536\n'
      printf 'hunter\thard\tnproc\t65536\n'
      printf '*\tsoft\tnofile\t65536\n'
      printf '*\thard\tnofile\t65536\n'
    } >>/etc/security/limits.conf
    log_success "已写入 /etc/security/limits.conf（nofile/nproc = 65536）"
  fi

  # 1.7 内核参数（Kafka/PostgreSQL/Redis 稳定性关键项）
  set_sysctl_conf_key "vm.max_map_count" "262144"
  set_sysctl_conf_key "net.core.somaxconn" "32768"
  set_sysctl_conf_key "vm.swappiness" "10"
  sysctl --system >/dev/null || {
    log_error "sysctl --system 应用失败：请检查 /etc/sysctl.conf 语法"
    return 1
  }
  log_success "内核参数已应用（max_map_count=$(sysctl -n vm.max_map_count) somaxconn=$(sysctl -n net.core.somaxconn) swappiness=$(sysctl -n vm.swappiness)）"

  # 1.8 关闭 swap（Kafka/PostgreSQL 延迟稳定性；docs/01 §3.1 要求 Swap=0B）
  if [ "$(swapon --show --noheadings 2>/dev/null | wc -l)" -gt 0 ]; then
    swapoff -a || {
      log_error "swapoff -a 失败：请检查是否有进程占用 swap（smem -t / free -h）"
      return 1
    }
    log_success "已关闭 swap"
  else
    log_info "swap 已关闭，跳过（幂等）"
  fi
  if grep -Eq '^[^#].*[[:space:]]swap[[:space:]]' /etc/fstab; then
    cp -a /etc/fstab "/etc/fstab.hunter.bak.$(date +%Y%m%d%H%M%S)"
    sed -i -E 's|^([^#].*[[:space:]]swap[[:space:]].*)$|# [HunterCore 已禁用 swap] \1|' /etc/fstab
    log_success "已注释 /etc/fstab 中的 swap 行（原文件已备份为 /etc/fstab.hunter.bak.*）"
  else
    log_info "/etc/fstab 无未注释的 swap 行，跳过（幂等）"
  fi

  # 1.9 ufw 防火墙（规则幂等：重复 allow 不会报错；docker 发布端口会绕过 ufw）
  local rule port_spec comment
  for rule in "${UFW_RULES[@]}"; do
    port_spec="${rule%%:*}"
    comment="${rule#*:}"
    if ufw allow "$port_spec" comment "$comment" >/dev/null 2>&1; then
      log_info "ufw 规则已确保：${port_spec}（${comment}）"
    else
      log_warn "ufw 规则添加失败：${port_spec}（${comment}），请手工执行 ufw allow ${port_spec}"
    fi
  done
  if ufw status 2>/dev/null | grep -q "Status: active"; then
    log_info "ufw 已处于启用状态，规则已同步（幂等）"
  else
    ufw default deny incoming >/dev/null 2>&1 || log_warn "设置 ufw 默认入向策略失败（可忽略）"
    ufw default allow outgoing >/dev/null 2>&1 || log_warn "设置 ufw 默认出向策略失败（可忽略）"
    ufw --force enable >/dev/null 2>&1 || {
      log_error "ufw 启用失败：请检查 systemctl status ufw 与内核模块（nf_tables/iptable_filter）"
      return 1
    }
    log_success "ufw 已启用（默认拒绝入向 / 允许出向）"
  fi
  log_warn "注意：Docker 发布端口会绕过 ufw；'仅内网' 端口（5432/5433/6379/9092/8081-8085/8088/9090）依赖 compose 绑定 127.0.0.1（docs/01 §3.4）"

  log_success "系统初始化完成（apt/时区/主机名/hunter 用户/内核参数/swap/ufw）"
}

# =====================================================================
# Step 2：安装 Docker Engine 24.0+ 与 Compose v2 插件
# =====================================================================
# 通过官方 apt 源安装（禁止 snap；docs/01 §3.2）
install_docker_engine() {
  local arch codename source_line
  log_info "卸载系统自带旧版本（docker.io/docker-engine/podman-docker 等）"
  apt-get remove -y docker docker-engine docker.io containerd runc podman-docker >/dev/null 2>&1 ||
    log_warn "旧版本卸载返回非 0（通常表示本就未安装），继续"

  install -m 0755 -d /etc/apt/keyrings
  if [ -s /etc/apt/keyrings/docker.gpg ]; then
    log_info "Docker GPG key 已存在，跳过导入（幂等）"
  else
    if ! curl -fsSL https://download.docker.com/linux/ubuntu/gpg |
      gpg --dearmor -o /etc/apt/keyrings/docker.gpg; then
      log_error "Docker GPG key 下载/导入失败"
      log_error "排查建议：① curl -I https://download.docker.com/linux/ubuntu/gpg 测试可达性；② 企业代理需 export https_proxy=http://<proxy>:<port> 后重试"
      rm -f /etc/apt/keyrings/docker.gpg
      return 1
    fi
    chmod a+r /etc/apt/keyrings/docker.gpg
    log_success "已导入 Docker GPG key：/etc/apt/keyrings/docker.gpg"
  fi

  arch="$(dpkg --print-architecture)"
  # 从 /etc/os-release 解析发行版代号（不 source 外部文件，避免 shellcheck SC1091 与副作用）
  codename="$(awk -F= '/^VERSION_CODENAME=/{gsub(/"/, "", $2); print $2}' /etc/os-release 2>/dev/null | head -n1)"
  if [ -z "$codename" ]; then
    codename="jammy"
    log_warn "无法解析 VERSION_CODENAME，Docker apt 源回退使用 jammy（Ubuntu 22.04 代号）"
  fi
  source_line="deb [arch=${arch} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${codename} stable"
  if grep -qF "$source_line" /etc/apt/sources.list.d/docker.list 2>/dev/null; then
    log_info "Docker apt 源已配置，跳过（幂等）"
  else
    printf '%s\n' "$source_line" >/etc/apt/sources.list.d/docker.list
    log_success "已写入 Docker apt 源：${source_line}"
  fi

  apt_update_with_retry || return 1
  if ! DEBIAN_FRONTEND=noninteractive apt-get install -y \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin; then
    log_error "Docker 安装失败：请检查 apt 源可用性与磁盘空间"
    return 1
  fi
  log_success "Docker Engine 与 Compose v2 插件安装完成"
}

# 写入 /etc/docker/daemon.json（优先使用部署包 ${APP_DIR}/config/daemon.json；内容一致则不重启）
write_daemon_json() {
  local src="${APP_DIR}/config/daemon.json" tmp
  tmp="$(mktemp)"
  if [ -f "$src" ]; then
    cp "$src" "$tmp"
    log_info "使用部署包配置：${src}"
  else
    cat >"$tmp" <<'JSON'
{
  "data-root": "/var/lib/docker",
  "storage-driver": "overlay2",
  "exec-opts": ["native.cgroupdriver=systemd"],
  "iptables": true,
  "live-restore": true,
  "no-new-privileges": true,
  "userland-proxy": false,
  "log-driver": "json-file",
  "log-opts": { "max-size": "100m", "max-file": "5" },
  "default-ulimits": { "nofile": { "Name": "nofile", "Hard": 65536, "Soft": 65536 } },
  "max-concurrent-downloads": 6,
  "max-concurrent-uploads": 6,
  "registry-mirrors": [],
  "features": { "buildkit": true }
}
JSON
    log_warn "未找到 ${src}，已写入内置默认 daemon.json（国内镜像加速可在 registry-mirrors 中补充）"
  fi
  if command_exists jq && ! jq -e . <"$tmp" >/dev/null 2>&1; then
    rm -f "$tmp"
    log_error "daemon.json 不是合法 JSON，拒绝写入（请检查 ${src}）"
    return 1
  fi
  if [ -f /etc/docker/daemon.json ] && cmp -s "$tmp" /etc/docker/daemon.json; then
    log_info "/etc/docker/daemon.json 内容未变化，跳过写入与重启（幂等）"
    rm -f "$tmp"
    return 0
  fi
  install -m 0644 -o root -g root "$tmp" /etc/docker/daemon.json
  rm -f "$tmp"
  systemctl restart docker || {
    log_error "/etc/docker/daemon.json 已更新但 Docker 重启失败：systemctl status docker"
    return 1
  }
  log_success "已写入 /etc/docker/daemon.json（json-file 100MB×5 / overlay2 / live-restore）并重启 Docker"
}

# 创建自定义 bridge 网络 hunter-net（docs/01 §1.2：172.28.0.0/16）
ensure_hunter_net() {
  if docker network inspect hunter-net >/dev/null 2>&1; then
    log_info "Docker 网络 hunter-net 已存在，跳过创建（幂等）"
    return 0
  fi
  if docker network create --driver bridge --subnet 172.28.0.0/16 hunter-net >/dev/null; then
    log_success "已创建 Docker 网络 hunter-net（172.28.0.0/16）"
    return 0
  fi
  log_error "创建 Docker 网络 hunter-net 失败：请检查 172.28.0.0/16 是否与其他网络冲突（docker network ls / ip route）"
  return 1
}

step_2_install_docker() {
  local docker_version="" compose_version=""
  if command_exists docker; then
    docker_version="$(docker --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1 || true)"
  fi

  if [ "$SKIP_DOCKER" -eq 1 ]; then
    log_info "已按 --skip-docker 跳过 Docker 安装步骤"
    if [ -z "$docker_version" ]; then
      log_error "--skip-docker 已指定，但系统中未检测到 docker 命令"
      log_error "请先安装 Docker 24.0+ 与 compose v2 插件，或去掉 --skip-docker 重新执行"
      return 1
    fi
    log_success "复用已安装 Docker：v${docker_version}"
  elif [ -n "$docker_version" ] && version_ge "$docker_version" "24.0"; then
    log_info "Docker 已安装（v${docker_version} ≥ 24.0），跳过安装（幂等）"
  else
    if [ -n "$docker_version" ]; then
      log_warn "检测到 Docker v${docker_version} 低于 24.0，将执行 apt 源安装/升级"
    fi
    install_docker_engine || return 1
    docker_version="$(docker --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)"
  fi

  # compose v2 插件校验（编排强依赖 docker compose；禁止 docker-compose v1）
  if docker compose version >/dev/null 2>&1; then
    compose_version="$(docker compose version --short 2>/dev/null || docker compose version | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -n1)"
    log_success "Docker Compose v2 可用：${compose_version}"
  else
    log_warn "未检测到 docker compose v2 插件，尝试安装 docker-compose-plugin"
    DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose-plugin || {
      log_error "docker compose v2 插件安装失败：请检查 apt 源（/etc/apt/sources.list.d/docker.list）"
      return 1
    }
    if ! docker compose version >/dev/null 2>&1; then
      log_error "docker compose 仍不可用：请确认未安装 v1 版 docker-compose 造成命令冲突"
      return 1
    fi
    log_success "docker compose v2 插件安装完成"
  fi

  systemctl enable --now docker >/dev/null 2>&1 || log_warn "systemctl enable --now docker 返回非 0（容器环境可能无 systemd）"
  if id -u hunter >/dev/null 2>&1; then
    usermod -aG docker hunter || log_warn "将 hunter 加入 docker 组失败（不阻塞部署）"
  fi

  write_daemon_json || return 1
  systemctl is-active docker >/dev/null 2>&1 || systemctl start docker >/dev/null 2>&1 || true
  ensure_hunter_net || return 1
  log_success "Docker 就绪：$(docker --version)"
}

# =====================================================================
# Step 3：准备数据目录与应用目录
# =====================================================================
step_3_prepare_dirs() {
  local d
  log_info "创建 ${DATA_DIR} 子目录（docs/01 §5 目录结构）"
  for d in postgresql timescale kafka zookeeper redis minio srs flink backups; do
    if [ -d "${DATA_DIR}/${d}" ]; then
      log_info "目录已存在，跳过：${DATA_DIR}/${d}"
    else
      install -d -m 0755 "${DATA_DIR}/${d}"
      log_success "已创建：${DATA_DIR}/${d}"
    fi
  done

  log_info "创建应用目录 ${APP_DIR}/{config,scripts,certs,sql,nginx}"
  for d in config scripts certs sql nginx nginx/logs; do
    install -d -m 0755 "${APP_DIR}/${d}"
  done
  log_info "创建宿主机日志目录 ${LOG_DIR}/{deploy,backups,check}"
  for d in deploy backups check; do
    install -d -m 0755 "${LOG_DIR}/${d}"
  done

  # 属主（docs/01 §5）：PG/Timescale/Redis 容器 uid = 999；Kafka/ZooKeeper/MinIO = 1001
  ensure_dir "${DATA_DIR}/postgresql" "999:999" "0755"
  ensure_dir "${DATA_DIR}/timescale" "999:999" "0755"
  ensure_dir "${DATA_DIR}/redis" "999:999" "0755"
  ensure_dir "${DATA_DIR}/kafka" "1001:1001" "0755"
  ensure_dir "${DATA_DIR}/zookeeper" "1001:1001" "0755"
  ensure_dir "${DATA_DIR}/minio" "1001:1001" "0755"
  ensure_dir "${DATA_DIR}/srs" "root:root" "0755"
  ensure_dir "${DATA_DIR}/flink" "root:root" "0755"
  ensure_dir "${DATA_DIR}/backups" "root:root" "0755"
  ensure_dir "${APP_DIR}/config" "root:root" "0755"
  ensure_dir "${APP_DIR}/scripts" "root:root" "0755"
  ensure_dir "${APP_DIR}/certs" "root:root" "0755"
  ensure_dir "${APP_DIR}/sql" "root:root" "0755"
  ensure_dir "${APP_DIR}/nginx" "root:root" "0755"

  log_success "目录准备完成（属主：PG/Timescale/Redis=999:999，Kafka/ZK/MinIO=1001:1001，其余 root:root）"
}

# =====================================================================
# Step 4：生成配置（.env 随机口令 + Nginx 配置）
# =====================================================================
PASSWORDS_FILE="${APP_DIR}/passwords.txt"

# set_env_var_value <file> <VAR> <value>：就地替换 .env 中变量值（保留行尾注释，幂等）
set_env_var_value() {
  local file="$1" var="$2" value="$3"
  if ! grep -qE "^${var}=" "$file"; then
    printf '%s=%s\n' "$var" "$value" >>"$file"
    log_info "已追加变量：${var}"
    return 0
  fi
  sed -i -E "s|^(${var}=)[^#]*(#.*)?\$|\1${value} \2|" "$file"
  log_success "已更新 ${var}=${value}（保留行尾注释）"
}

# ensure_passwords_file <env_file>：口令清单（权限 600）；已存在则仅校正权限（幂等）
ensure_passwords_file() {
  local env_file="$1"
  if [ -f "$PASSWORDS_FILE" ]; then
    chmod 600 "$PASSWORDS_FILE"
    log_info "口令清单已存在：${PASSWORDS_FILE}（权限 600，幂等）"
    return 0
  fi
  log_warn "未找到口令清单，从 ${env_file} 提取敏感项生成"
  install -m 0600 /dev/null "$PASSWORDS_FILE"
  {
    printf '# HunterCore 部署口令清单（生成时间：%s）\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf '# ⚠ 敏感文件：权限 600；禁止提交仓库、截图或外发\n'
    printf '# 应用根目录：%s\n\n' "$APP_DIR"
    grep -E '^[A-Z0-9_]*(PASSWORD|SECRET|ACCESS_KEY)=' "$env_file" || true
  } >>"$PASSWORDS_FILE"
  chmod 600 "$PASSWORDS_FILE"
  log_success "已生成口令清单：${PASSWORDS_FILE}（权限 600）"
}

# generate_env_inline <env_file>：gen-passwords.sh 缺失时的内置回退（等价语义，保持幂等）
generate_env_inline() {
  local env_file="$1" server_ip var new_value
  install -m 0600 /dev/null "$PASSWORDS_FILE"
  server_ip="${OPT_IP:-$(hc_detect_server_ip)}"
  log_info "使用内置逻辑生成口令（SERVER_IP=${server_ip}）"
  # 1) 替换所有值为 CHANGE_ME_* 的变量（口令/密钥；SERVER_IP 用真实 IP，CORS_ORIGINS 用前端来源）
  while IFS= read -r var; do
    case "$var" in
      SERVER_IP) new_value="$server_ip" ;;
      CORS_ORIGINS) new_value="http://${server_ip}" ;;
      *) new_value="$(hc_rand_hex 24)" ;;
    esac
    sed -i -E "s|^(${var}=)CHANGE_ME_[A-Za-z0-9_]*|\1${new_value}|" "$env_file"
    log_success "已生成 ${var}"
  done < <(grep -oE '^[A-Za-z0-9_]+=CHANGE_ME_[A-Za-z0-9_]*' "$env_file" | cut -d= -f1)
  # 2) 值中内嵌 CHANGE_ME_SERVER_IP 的变量（RC_PUBLIC_WS_BASE_URL / RC_STUN_URLS）
  sed -i "s|CHANGE_ME_SERVER_IP|${server_ip}|g" "$env_file"
  {
    printf '# HunterCore 部署口令清单（生成时间：%s）\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf '# ⚠ 敏感文件：权限 600；禁止提交仓库、截图或外发\n'
    printf 'SERVER_IP=%s\n\n' "$server_ip"
    grep -E '^[A-Z0-9_]*(PASSWORD|SECRET|ACCESS_KEY)=' "$env_file" || true
  } >>"$PASSWORDS_FILE"
  chmod 600 "$PASSWORDS_FILE"
  log_success "口令已生成并保存到 ${PASSWORDS_FILE}（权限 600）"
}

# ensure_nginx_conf：确保 ${APP_DIR}/config/nginx.conf 就位（优先部署包，其次仓库 infra/deploy）
ensure_nginx_conf() {
  local target="${APP_DIR}/config/nginx.conf"
  local candidates=("${APP_DIR}/infra/deploy/config/nginx.conf")
  if [ -f "$target" ]; then
    chmod 644 "$target"
    log_info "Nginx 配置已就位：${target}（幂等）"
    return 0
  fi
  local src
  for src in "${candidates[@]}"; do
    if [ -f "$src" ]; then
      install -m 0644 "$src" "$target"
      log_success "已复制 Nginx 配置：${src} → ${target}"
      return 0
    fi
  done
  log_warn "未找到 Nginx 配置（期望 ${target}）：web-portal 容器将启动失败"
  log_warn "请从仓库 infra/deploy/config/nginx.conf 复制到 ${APP_DIR}/config/ 后重试"
  return 1
}

step_4_gen_config() {
  local env_file example server_ip
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  # 4.1 .env.example：优先 ${APP_DIR}（服务器），回退仓库部署包（本地/开发机直跑）
  example="${APP_DIR}/.env.example"
  if [ ! -f "$example" ] && [ -f "${APP_DIR}/infra/deploy/.env.example" ]; then
    install -m 0644 "${APP_DIR}/infra/deploy/.env.example" "$example"
    hc_normalize_env_file "$example" || return 1
    log_success "已复制仓库模板：infra/deploy/.env.example → ${example}"
  fi

  # 4.2 .env 生成（幂等：已存在则跳过口令生成；CRLF 一律归一为 LF）
  if [ -f "$env_file" ]; then
    log_info ".env 已存在（${env_file}），跳过生成（幂等）"
    log_info "如需重新生成全部口令：bash ${GEN_PASSWORDS_SH} --force"
    hc_normalize_env_file "$env_file" || return 1
  else
    if [ ! -f "$example" ]; then
      log_error "缺少 .env 模板：${example}"
      log_error "请从仓库 infra/deploy/.env.example 复制到 ${APP_DIR}/.env.example 后重试"
      return 1
    fi
    install -m 0600 "$example" "$env_file"
    hc_normalize_env_file "$env_file" || return 1
    log_success "已从模板生成：${env_file}（权限 600）"
    if [ -f "$GEN_PASSWORDS_SH" ]; then
      local -a gp_args=()
      if [ -n "$OPT_IP" ]; then
        gp_args+=(--ip "$OPT_IP")
      fi
      hc_run_logged "生成随机强口令" bash "$GEN_PASSWORDS_SH" "${gp_args[@]}" || {
        log_error "口令生成失败：请检查 openssl 可用性（openssl version）与 ${env_file} 可写权限"
        return 1
      }
    else
      log_warn "未找到 ${GEN_PASSWORDS_SH}，使用内置回退逻辑生成口令"
      generate_env_inline "$env_file" || return 1
    fi
  fi
  chmod 600 "$env_file"

  # 4.3 SERVER_IP：--ip 优先覆盖（Kafka 9093 advertised.listeners、SRS candidate、证书 SAN 依赖）
  if [ -n "$OPT_IP" ]; then
    set_env_var_value "$env_file" "SERVER_IP" "$OPT_IP"
  fi
  server_ip="$(grep -E '^SERVER_IP=' "$env_file" | head -n1 | cut -d= -f2- | awk '{print $1}')"
  case "$server_ip" in
    "" | CHANGE_ME_*)
      log_warn "SERVER_IP 仍是占位值（${server_ip:-<空>}）"
      log_warn "将导致：Kafka 9093 advertised.listeners 车端不可达、SRS WebRTC candidate 错误、broker 证书 SAN 不含真实地址"
      log_warn "请编辑 ${env_file} 设置 SERVER_IP=<服务器真实IP>，或使用 --ip <IP> 重新执行 --step 4"
      return 1
      ;;
    *)
      log_success "SERVER_IP=${server_ip}（用于 Kafka 9093 / SRS / 证书 SAN）"
      ;;
  esac

  # 4.4 口令清单与实际使用的口令一致性提示（gen-passwords.sh 与 install.sh 均已生成）
  ensure_passwords_file "$env_file"
  log_warn "口令清单含敏感信息，请立即妥善保存（${PASSWORDS_FILE}，权限 600）"

  # 4.5 Nginx 配置（web-portal：静态资源 + /api 反代网关 + /ws 反代 remote-control）
  ensure_nginx_conf || return 1

  log_success "配置生成完成：${env_file}（权限 600）、${PASSWORDS_FILE}（权限 600）"
}

# =====================================================================
# Step 5：生成 Kafka SASL_SSL 证书
# =====================================================================
step_5_gen_certs() {
  local env_file cert_dir broker_cert
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  cert_dir="${KAFKA_CERTS_DIR:-${APP_DIR}/certs/kafka}"

  # 幂等：keystore/truststore 齐备即跳过（重新签发用 gen-kafka-certs.sh --force）
  if [ -f "${cert_dir}/kafka.keystore.jks" ] && [ -f "${cert_dir}/kafka.truststore.jks" ]; then
    log_info "Kafka 证书已存在（${cert_dir}），跳过生成（幂等）"
    log_info "如需重新签发（例如 SERVER_IP 变更）：bash ${GEN_KAFKA_CERTS_SH} --force"
    return 0
  fi

  # SERVER_IP 必须已配置：证书 SAN 与 Kafka advertised.listeners 都依赖它
  if ! require_env SERVER_IP; then
    log_error "请先设置 SERVER_IP（--ip 参数或编辑 ${env_file}）后重新执行 --step 4"
    return 1
  fi
  if [ ! -f "$GEN_KAFKA_CERTS_SH" ]; then
    log_error "缺少证书生成脚本：${GEN_KAFKA_CERTS_SH}"
    log_error "请确认部署包完整（docs/01 §5 scripts/ 目录）"
    return 1
  fi
  if ! command_exists keytool; then
    log_warn "未检测到 keytool（JKS 生成依赖 JDK）：安装 openjdk-17-jre-headless"
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-17-jre-headless || {
      log_error "安装 openjdk-17-jre-headless 失败：JKS 证书无法生成"
      return 1
    }
  fi

  local -a args=()
  if [ -n "$OPT_IP" ]; then
    args+=(--ip "$OPT_IP")
  fi
  hc_run_logged "生成 Kafka SASL_SSL 证书" bash "$GEN_KAFKA_CERTS_SH" "${args[@]}" || {
    log_error "Kafka 证书生成失败；常见原因：openssl/keytool 缺失、${cert_dir} 不可写"
    return 1
  }

  # 校验证书 SAN 是否含真实 SERVER_IP（车端以 SERVER_IP 连接 9093，SAN 不匹配会握手中断）
  broker_cert="${cert_dir}/broker-cert.pem"
  if [ -f "$broker_cert" ] && command_exists openssl; then
    if openssl x509 -in "$broker_cert" -noout -ext subjectAltName 2>/dev/null | grep -qF "IP Address:${SERVER_IP}"; then
      log_success "broker 证书 SAN 已包含 SERVER_IP=${SERVER_IP}"
    else
      log_warn "broker 证书 SAN 未包含 ${SERVER_IP}：请执行 bash ${GEN_KAFKA_CERTS_SH} --force --ip ${SERVER_IP} 重新签发"
    fi
  fi
  log_success "Kafka 证书生成完成：${cert_dir}（私钥与 JKS 权限 600）"
}

# =====================================================================
# Step 6：构建业务服务镜像
# =====================================================================
step_6_build_images() {
  if [ "$SKIP_BUILD" -eq 1 ]; then
    log_info "已按 --skip-build 跳过镜像构建"
    if ! docker image ls --format '{{.Repository}}' 2>/dev/null | grep -q '^hunter/'; then
      log_warn "未发现 hunter/* 镜像：请确认已预先构建/导入镜像，否则后续服务无法启动"
    fi
    return 0
  fi
  require_compose_file || return 1
  if ! docker image ls --format '{{.Repository}}' 2>/dev/null | grep -q '^hunter/'; then
    log_info "未发现既有 hunter/* 镜像：执行首次构建"
  else
    log_info "已存在 hunter/* 镜像：仍执行构建以保证版本一致（可用 --skip-build 跳过）"
  fi

  mkdir -p "$DEPLOY_LOG_DIR"
  local rc=0
  log_info "执行 docker compose build --parallel（构建日志：${BUILD_LOG}）"
  (cd "$APP_DIR" && docker compose build --parallel) 2>&1 | tee "$BUILD_LOG" || rc=$?
  if [ "$rc" -ne 0 ]; then
    log_error "镜像构建失败（退出码 ${rc}），构建日志最后 50 行："
    tail -n 50 "$BUILD_LOG" >&2 || true
    log_error "排查建议：① 基础镜像拉取（python:3.11-slim / gcr.io/distroless）需出网，可配置 /etc/docker/daemon.json 的 registry-mirrors；② 前端 npm ci 失败多为网络/私有源；③ df -h 检查磁盘；④ 完整日志：${BUILD_LOG}"
    return 1
  fi
  log_success "镜像构建完成（日志：${BUILD_LOG}）"
  docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' | grep -E '^hunter/' ||
    log_warn "未列出 hunter/* 镜像：请核对 compose 的 image 字段（IMAGE_PREFIX/ VERSION）"
}

# =====================================================================
# Step 7：启动中间件（按依赖顺序 + 就绪等待）
# =====================================================================
step_7_start_middleware() {
  local env_file
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  require_compose_file || return 1

  # 7.1 PostgreSQL 15（含 TimescaleDB 扩展，契约单实例形态；docs/01 §1.4①⑧）
  hc_run_logged "启动 postgres" compose up -d postgres || return 1
  wait_for "docker exec ${C_POSTGRES} pg_isready -U '${POSTGRES_USER}' -d '${POSTGRES_DB}'" \
    "PostgreSQL（库 ${POSTGRES_DB}）" 60 || return 1

  # 7.2 TimescaleDB 独立实例（仅当编排中存在 timescale 服务时启动）
  if compose_service_exists timescale; then
    hc_run_logged "启动 timescale" compose up -d timescale || return 1
    wait_for "docker exec ${C_TIMESCALE} pg_isready -U '${TIMESCALE_USER:-hunter}' -d '${TIMESCALE_DB:-hunter_ts}'" \
      "TimescaleDB（宿主 127.0.0.1:5433）" 60 || return 1
  else
    log_info "编排中无 timescale 服务：采用契约单实例形态（时序表与业务表同库），跳过"
  fi

  # 7.3 Redis 7（AOF + 密码认证；docs/01 §1.1）
  hc_run_logged "启动 redis" compose up -d redis || return 1
  wait_for "docker exec ${C_REDIS} redis-cli --no-auth-warning -a '${REDIS_PASSWORD}' ping | grep -q PONG" \
    "Redis（AOF）" 30 || return 1

  # 7.4 ZooKeeper 3.8 + Kafka 3.6（内部 9092 / 外部 SASL_SSL 9093）
  hc_run_logged "启动 zookeeper kafka" compose up -d zookeeper kafka || return 1
  wait_for "kafka_broker_ready ${C_KAFKA}" "Kafka broker（9092 内部 / 9093 车端 SASL_SSL）" 90 || return 1

  # 7.5 MinIO（9000 API / 9001 Console）
  hc_run_logged "启动 minio" compose up -d minio || return 1
  wait_for "curl -sf -m 5 $(minio_health_url) >/dev/null" "MinIO（9000/9001）" 30 || return 1

  # 7.6 SRS 5.0（RTMP 1935 / WebRTC 8000udp / HTTP API 9090）
  hc_run_logged "启动 srs" compose up -d srs || return 1
  wait_for "curl -sf -m 5 $(srs_health_url) >/dev/null" "SRS（RTMP 1935 / WebRTC 8000udp）" 30 || return 1

  log_success "中间件全部就绪"
  compose ps || true
}

# =====================================================================
# Step 8~10：数据层初始化（数据库 / Kafka Topic / MinIO Bucket）
# =====================================================================
# ensure_app_file <相对路径>：确保 ${APP_DIR} 下文件就位（缺失时从仓库 infra/deploy 复制）
ensure_app_file() {
  local rel="$1"
  local target="${APP_DIR}/${rel}"
  local fallback="${APP_DIR}/infra/deploy/${rel}"
  if [ -f "$target" ]; then
    return 0
  fi
  if [ -f "$fallback" ]; then
    install -d -m 0755 "$(dirname "$target")"
    install -m 0644 "$fallback" "$target"
    log_success "已复制部署产物：infra/deploy/${rel} → ${target}"
    return 0
  fi
  log_error "缺少部署文件：${target}（来源：infra/deploy/${rel}）"
  return 1
}

step_8_init_db() {
  local env_file f
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1

  # SQL 产物由 contracts/database/ddl 生成（docs/01 §5，勿手改）
  for f in schema.sql timescaledb.sql init-data.sql; do
    ensure_app_file "sql/${f}" || return 1
  done
  if [ ! -f "$INIT_DB_SH" ]; then
    log_error "缺少脚本：${INIT_DB_SH}（请确认部署包 scripts/ 完整）"
    return 1
  fi
  hc_run_logged "初始化数据库（schema.sql → timescaledb.sql → init-data.sql）" bash "$INIT_DB_SH" || {
    log_error "数据库初始化失败：请检查容器状态（docker logs ${C_POSTGRES} --tail=100）与 SQL 日志"
    return 1
  }
  log_success "数据库初始化完成（库 ${POSTGRES_DB}，容器 ${C_POSTGRES}）"
}

step_9_init_kafka() {
  local env_file
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  if [ ! -f "$INIT_KAFKA_SH" ]; then
    log_error "缺少脚本：${INIT_KAFKA_SH}（请确认部署包 scripts/ 完整）"
    return 1
  fi
  hc_run_logged "创建 Kafka 内部 Topic（6 个）" bash "$INIT_KAFKA_SH" || {
    log_error "Kafka Topic 初始化失败：请检查 broker 日志（docker logs ${C_KAFKA} --tail=100）与认证配置（KAFKA_INTERNAL_SECURITY_PROTOCOL）"
    return 1
  }
  log_success "Kafka 内部 Topic 初始化完成"
}

step_10_init_minio() {
  local env_file
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  if [ ! -f "$INIT_MINIO_SH" ]; then
    log_error "缺少脚本：${INIT_MINIO_SH}（请确认部署包 scripts/ 完整）"
    return 1
  fi
  hc_run_logged "创建 MinIO Bucket（7 个）与生命周期规则" bash "$INIT_MINIO_SH" || {
    log_error "MinIO 初始化失败：请检查 MinIO 日志（docker logs ${C_MINIO} --tail=100）与 MINIO_ROOT_USER/PASSWORD"
    return 1
  }
  log_success "MinIO Bucket 与生命周期规则初始化完成"
}

# =====================================================================
# Step 11：启动业务服务（采集 → 分析/Flink → 业务 → 网关 → 前端）
# =====================================================================
step_11_start_services() {
  local env_file gateway_port scene_port collector_port analytics_port ota_port remote_port web_port
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || return 1
  require_compose_file || return 1

  gateway_port="${API_GATEWAY_PORT:-8080}"
  scene_port="${SCENE_SERVICE_PORT:-8081}"
  collector_port="${DATA_COLLECTOR_PORT:-8082}"
  analytics_port="${DATA_ANALYTICS_PORT:-8083}"
  ota_port="${OTA_SERVICE_PORT:-8084}"
  remote_port="${REMOTE_CONTROL_PORT:-8085}"
  web_port="${WEB_PORT:-80}"

  # 11.1 采集与实时分析（消费者先就绪，避免消息积压/丢包窗口）
  hc_run_logged "启动 data-collector" compose up -d data-collector || return 1
  wait_for "service_health_ok ${collector_port}" "data-collector（${collector_port}）" 120 || return 1
  # 模块表要求 6 个服务全量启动（data-analytics 亦属核心模块，与采集同批次）
  hc_run_logged "启动 data-analytics" compose up -d data-analytics || return 1
  wait_for "service_health_ok ${analytics_port}" "data-analytics（${analytics_port}）" 120 || return 1

  # 11.2 Flink 1.18（JobManager 容器内 8081 → 宿主机 8088）
  hc_run_logged "启动 flink-jm flink-tm" compose up -d flink-jm flink-tm || return 1
  wait_for "curl -sf -m 5 $(flink_ui_url)/overview >/dev/null" "Flink JobManager UI（$(flink_ui_url)）" 120 || return 1

  # 11.3 业务服务（场景 / OTA / 远程操控）
  hc_run_logged "启动 scene-service ota-service remote-control" \
    compose up -d scene-service ota-service remote-control || return 1
  wait_for "service_health_ok ${scene_port}" "scene-service（${scene_port}）" 120 || return 1
  wait_for "service_health_ok ${ota_port}" "ota-service（${ota_port}）" 120 || return 1
  wait_for "service_health_ok ${remote_port}" "remote-control（${remote_port}）" 120 || return 1

  # 11.4 API 网关（对外 8080；依赖全部上游服务）
  hc_run_logged "启动 api-gateway" compose up -d api-gateway || return 1
  wait_for "service_health_ok ${gateway_port}" "api-gateway（${gateway_port}）" 120 || return 1

  # 11.5 前端 web-portal（Nginx + Vue3 dist；对外 80）
  hc_run_logged "启动 web-portal" compose up -d web-portal || return 1
  wait_for "curl -sf -m 5 -o /dev/null http://127.0.0.1:${web_port}/" "web-portal（${web_port}）" 60 || return 1

  log_success "业务服务全部启动就绪"
  compose ps || true
}

# =====================================================================
# Step 12：全栈健康检查
# =====================================================================
step_12_health_check() {
  local env_file rc=0
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" || log_warn "无法加载 .env：健康检查将使用内置默认值"
  if [ ! -f "$HEALTH_CHECK_SH" ]; then
    log_warn "缺少 ${HEALTH_CHECK_SH}：跳过健康检查（建议补齐后手工执行）"
    return 0
  fi
  hc_run_logged "全栈健康检查" bash "$HEALTH_CHECK_SH" || rc=$?
  case "$rc" in
    0)
      log_success "健康检查全部通过"
      return 0
      ;;
    1)
      log_warn "健康检查存在告警项（exit=1）：部署继续，但请处理告警（详见上方输出与 ${LOG_FILE}）"
      return 0
      ;;
    *)
      log_error "健康检查存在失败项（exit=${rc}）"
      log_error "排查建议：① 重新执行 bash ${HEALTH_CHECK_SH} 查看明细；② cd ${APP_DIR} && docker compose ps；③ docker compose logs --tail=100 <service>"
      return 1
      ;;
  esac
}

# =====================================================================
# Step 13：输出部署摘要
# =====================================================================
# 从 init-data.sql 提取默认管理员口令（初始值，登录后必须修改）
detect_admin_password() {
  local sql="" pw=""
  for sql in "${APP_DIR}/sql/init-data.sql" "${APP_DIR}/infra/deploy/sql/init-data.sql"; do
    if [ -f "$sql" ]; then
      pw="$(grep -oE '默认值对应初始口令：[^ 　]+' "$sql" | head -n1 | sed 's/.*：//' || true)"
      break
    fi
  done
  printf '%s' "${pw:-Hunter@2025}"
}

step_13_print_summary() {
  local env_file server_ip web_port gateway_port minio_port admin_pass
  env_file="${ENV_FILE_ARG:-${HUNTER_ENV_FILE:-${APP_DIR}/.env}}"
  load_env "$env_file" >/dev/null 2>&1 || log_warn "无法加载 ${env_file}：摘要中部分字段使用默认值"
  server_ip="${SERVER_IP:-$(hc_detect_server_ip)}"
  web_port="${WEB_PORT:-80}"
  gateway_port="${API_GATEWAY_PORT:-8080}"
  minio_port="${MINIO_CONSOLE_PORT:-9001}"
  admin_pass="$(detect_admin_password)"

  printf '\n%s\n' "${C_GREEN}================= ✅ HunterCore 部署成功 =================${C_RESET}"
  printf '%s\n' "  前端管理后台 : http://${server_ip}:${web_port}/"
  printf '%s\n' "  API 网关     : http://${server_ip}:${gateway_port}/api/v1"
  printf '%s\n' "  接口文档     : http://${server_ip}:${gateway_port}/docs"
  printf '%s\n' "  MinIO 控制台 : http://${server_ip}:${minio_port}/"
  printf '%s\n' "  Flink 控制台 : $(flink_ui_url)/   (仅 127.0.0.1，需 SSH 隧道)"
  printf '%s\n' "  SRS HTTP API : $(srs_health_url)   (仅 127.0.0.1)"
  printf '%s\n' "  Kafka 车端接 : ${server_ip}:9093 (SASL_SSL + SCRAM-SHA-512)"
  printf '%s\n' "  默认管理员   : admin / ${admin_pass}   ⚠ 首次登录后必须立即修改"
  printf '%s\n' "----------------------------------------------------------"
  printf '%s\n' "  配置文件     : ${env_file}（600）、${APP_DIR}/config/（nginx/daemon）"
  printf '%s\n' "  口令清单     : ${PASSWORDS_FILE}（600，请立即离线保存）"
  printf '%s\n' "  证书目录     : ${KAFKA_CERTS_DIR:-${APP_DIR}/certs/kafka}"
  printf '%s\n' "  数据目录     : ${DATA_DIR}（postgresql/kafka/minio/redis/flink/backups）"
  printf '%s\n' "  日志         : ${LOG_FILE}、${LOG_DIR}/{deploy,backups,check}/"
  printf '%s\n' "  服务状态     : cd ${APP_DIR} && docker compose ps"
  printf '%s\n' "  下一步操作   :"
  printf '%s\n' "    1) 登录前端修改 admin 口令，并创建业务账号/车辆台账（HUNTER-001 仅为验证车辆）"
  printf '%s\n' "    2) 运行健康检查与巡检：bash ${HEALTH_CHECK_SH} ｜ bash ${DAILY_CHECK_SH}"
  printf '%s\n' "    3) 车端接入：配置 Kafka SASL_SSL(9093) 客户端证书与 SCRAM 账号，Topic 前缀 hunter.{vehicle_id}.*"
  printf '%s\n' "    4) 生产环境务必启用 HTTPS/WSS（域名 + TLS 1.3）与 MinIO Console 内网访问限制"
  printf '%s\n' "${C_GREEN}==========================================================${C_RESET}"
  log_success "部署摘要已输出，完整日志：${LOG_FILE}"
}

# =====================================================================
# 步骤调度与统一错误处理
# =====================================================================
# run_step <步骤号> <描述> <步骤函数>：统一打印分隔线、成功/失败信息与排查建议
run_step() {
  local step_no="$1" step_desc="$2"
  shift 2
  local rc=0
  log_info "===== Step ${step_no}/${TOTAL_STEPS}: ${step_desc} ====="
  "$@" || rc=$?
  if [ "$rc" -eq 0 ]; then
    log_success "✓ Step ${step_no} 完成：${step_desc}"
    return 0
  fi
  log_error "✗ Step ${step_no}/${TOTAL_STEPS} 失败：${step_desc}（退出码 ${rc}）"
  log_error "排查建议："
  log_error "  1) 查看完整日志：${LOG_FILE}；构建日志：${BUILD_LOG}"
  log_error "  2) 查看容器状态：cd ${APP_DIR} && docker compose ps"
  log_error "  3) 查看容器日志：cd ${APP_DIR} && docker compose logs --tail=200 <service>"
  log_error "  4) 修复后从该步续跑：bash $0 --step ${step_no} -y"
  log_error "  5) 需要重来时先卸载（保留数据）：bash ${UNINSTALL_SH} --keep-data"
  exit 1
}

run_all_steps() {
  local n
  for n in $(seq 1 "$TOTAL_STEPS"); do
    if [ "$n" -lt "$START_STEP" ]; then
      log_info "跳过 Step ${n}/${TOTAL_STEPS}（--step ${START_STEP}）：${STEP_DESCRIPTIONS[$((n - 1))]}"
      continue
    fi
    case "$n" in
      1) run_step 1 "${STEP_DESCRIPTIONS[0]}" step_1_system_init ;;
      2) run_step 2 "${STEP_DESCRIPTIONS[1]}" step_2_install_docker ;;
      3) run_step 3 "${STEP_DESCRIPTIONS[2]}" step_3_prepare_dirs ;;
      4) run_step 4 "${STEP_DESCRIPTIONS[3]}" step_4_gen_config ;;
      5) run_step 5 "${STEP_DESCRIPTIONS[4]}" step_5_gen_certs ;;
      6) run_step 6 "${STEP_DESCRIPTIONS[5]}" step_6_build_images ;;
      7) run_step 7 "${STEP_DESCRIPTIONS[6]}" step_7_start_middleware ;;
      8) run_step 8 "${STEP_DESCRIPTIONS[7]}" step_8_init_db ;;
      9) run_step 9 "${STEP_DESCRIPTIONS[8]}" step_9_init_kafka ;;
      10) run_step 10 "${STEP_DESCRIPTIONS[9]}" step_10_init_minio ;;
      11) run_step 11 "${STEP_DESCRIPTIONS[10]}" step_11_start_services ;;
      12) run_step 12 "${STEP_DESCRIPTIONS[11]}" step_12_health_check ;;
      13) run_step 13 "${STEP_DESCRIPTIONS[12]}" step_13_print_summary ;;
      *) log_error "内部错误：未知步骤 ${n}"; exit 1 ;;
    esac
  done
}

# =====================================================================
# 入口
# =====================================================================
main() {
  local start_ts elapsed
  parse_args "$@"
  hc_log_begin
  TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "${TMP_DIR:-}"' EXIT
  start_ts="$(date +%s)"

  log_info "HunterCore 一键部署启动（APP_DIR=${APP_DIR}，脚本=${SCRIPT_DIR}/install.sh）"
  log_info "日志文件：${LOG_FILE}（另开终端 tail -f 可实时跟踪）"
  log_info "开始时间：$(date '+%Y-%m-%d %H:%M:%S')"

  preflight
  show_plan
  run_all_steps

  elapsed=$(( $(date +%s) - start_ts ))
  log_success "全部 ${TOTAL_STEPS} 个步骤执行完成，总耗时 ${elapsed}s"
  log_info "结束时间：$(date '+%Y-%m-%d %H:%M:%S')"
  log_info "后续运维：health-check.sh（健康检查）/ daily-check.sh（日巡检）/ backup.sh（备份）/ collect-logs.sh（日志收集）"
}

main "$@"