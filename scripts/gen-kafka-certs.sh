#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— Kafka SASL_SSL 证书生成（gen-kafka-certs.sh）
#
# 用途：为 Kafka 外部监听（9093 / SASL_SSL + SCRAM-SHA-512，车端接入）生成全套证书：
#   ① CA（自签，3650 天）：ca-key.pem / ca-cert.pem
#   ② broker 密钥 + CSR（CN=kafka，SAN=DNS:kafka,DNS:localhost,DNS:<hostname>,IP:127.0.0.1,
#      IP:<SERVER_IP>，另可追加 --san-ip/--san-dns 与 .env 的 KAFKA_CERT_SAN_IPS/KAFKA_CERT_SAN_DNS）
#      → ca-cert.pem 签发 → broker-cert.pem
#   ③ 客户端（车端）密钥 + 证书（CN=kafka-client，clientAuth）
#   ④ broker 密钥与证书链导入 JKS：kafka.keystore.jks（口令 = .env 的 KAFKA_SSL_PASSWORD）
#   ⑤ CA 导入 JKS：kafka.truststore.jks
#   ⑥ 车端所需：kafka-client.p12（PKCS12，含客户端密钥+证书链）与 ca-cert.pem
#
# ⚠ 关键约束一：broker 证书 SAN 必须包含车端**实际连接**的每一个地址。车端默认
#   ssl.endpoint.identification.algorithm=https，仅以 SAN 比对（证书含 SAN 时 CN 不回退），
#   SAN 缺该地址 → 主机名校验失败 → 车端连不上 9093，且车端无法自行修正（属 Broker 侧证书问题）。
#   常见踩坑：服务器 IP 变更后（如公网 IP → 内网 IP）证书仍是旧 SAN，而幂等逻辑会跳过重签；
#   现幂等复用时会自动比对 SAN 覆盖情况，缺地址则复用 CA 仅重签 broker 证书（自愈）。
#
# ⚠ 关键约束二：CA 变更 = 车端信任链变更（须向全部车端重新分发 ca-cert.pem/客户端证书）。
#   因此仅地址（SAN）变化请用 --broker-only：复用现有 CA，车端无需更新；
#   --force 会连同 CA 一起重签，仅在 CA 到期/泄露时使用，并须同步全部车端。
#
# 用法：
#   sudo bash scripts/gen-kafka-certs.sh [选项]
#
# 参数：
#   --help             显示本帮助
#   --ip <address>     覆盖 SERVER_IP（默认取 .env 的 SERVER_IP；占位值会报错）
#   --san-ip <ip>      追加 broker 证书 SAN IP（可重复；多网卡 / NAT / 弹性 IP / 内外网并存）
#   --san-dns <dns>    追加 broker 证书 SAN 域名（可重复；如 kafka.hunter-core.local）
#   --broker-only      复用现有 CA 与客户端证书，仅重签 broker 证书 + keystore（车端无需换信任链）
#   --out <dir>        证书输出目录（默认 .env 的 KAFKA_CERTS_DIR，即 /opt/hunter-core/certs/kafka）
#   --force            重新签发全部证书（含 CA）与 JKS（⚠ 需重启 Kafka 并更新全部车端证书）
#
# 示例：
#   # 车端改走内网地址访问：SAN 同时含内网与公网 IP，仅重签 broker（车端信任链不变）
#   sudo bash /opt/hunter-core/scripts/gen-kafka-certs.sh \
#        --ip 192.168.31.35 --san-ip 101.201.150.237 --broker-only
#   sudo bash /opt/hunter-core/scripts/gen-kafka-certs.sh --san-dns kafka.hunter-core.local --broker-only
#   sudo bash /opt/hunter-core/scripts/gen-kafka-certs.sh --force   # 连 CA 一起重签（须更新车端）
#
# 幂等：keystore/truststore 齐备且 broker 证书 SAN 覆盖全部访问地址 → 跳过（--force 除外）；
#       SAN 缺地址 → 复用 CA 自动仅重签 broker 证书（须重启 Kafka 生效）。
# 权限：私钥/P12 = 600；broker 加载的 JKS（keystore/truststore）= 0640 且属组 1001（供 Kafka 容器读取）；证书（含 CA）= 644。
# 依赖：common.sh（同目录）、openssl ≥1.1.1、keytool（openjdk-17-jre-headless）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # SERVER_IP / KAFKA_SSL_PASSWORD / KAFKA_CERTS_DIR / KAFKA_CERT_SAN_* 由 .env 注入

ASSUME_YES=0
FORCE=0
BROKER_ONLY=0
OPT_IP=""
OPT_OUT=""
SAN_IP_EXTRA=()   # --san-ip 追加的 IP
SAN_DNS_EXTRA=()  # --san-dns 追加的域名
SAN_ENTRIES=()    # 本次要求 broker 证书必须包含的 SAN 条目（IP:x / DNS:x）
SAN_VALUE=""      # openssl subjectAltName 取值（SAN_ENTRIES 逗号拼接）

# 证书参数（有效期与主题，车端信任链依赖 CA，故 CA 变更必须同步车端）
CA_DAYS=3650
LEAF_DAYS=3650
CA_SUBJECT="/C=CN/O=HunterCore/CN=hunter-core-kafka-ca"
BROKER_SUBJECT="/C=CN/O=HunterCore/CN=kafka"
CLIENT_SUBJECT="/C=CN/O=HunterCore/CN=kafka-client"

# Kafka broker 容器运行组（compose: user "1001:1001"）；JKS 须对该 GID 可读，
# 否则 broker 加载 keystore 报 Permission denied → SASL_SSL 监听初始化失败 → 整个 broker 起不来。
KAFKA_CERT_GID="${KAFKA_CERT_GID:-1001}"

usage() {
  cat <<'EOF'
HunterCore Kafka SASL_SSL 证书生成脚本

用途：
  生成 Kafka 车端接入（9093 / SASL_SSL）所需的 CA、broker、客户端证书与 JKS：
    ca-key.pem / ca-cert.pem            自签 CA（3650 天）
    broker-key.pem / broker.csr / broker-cert.pem（CN=kafka，SAN 含全部车端可访问地址）
    client-key.pem / client.csr / client-cert.pem（CN=kafka-client，clientAuth）
    kafka.keystore.jks                  broker 密钥库（口令 = KAFKA_SSL_PASSWORD）
    kafka.truststore.jks                CA 信任库（口令 = KAFKA_SSL_PASSWORD）
    kafka-client.p12                    车端客户端 PKCS12（含密钥与证书链）

用法：
  sudo bash scripts/gen-kafka-certs.sh [选项]

参数：
  --help             显示本帮助
  --ip <address>     覆盖 SERVER_IP（默认读 .env；占位值将直接报错退出）
  --san-ip <ip>      追加 broker 证书 SAN IP（可重复；内外网/多网卡/NAT 并存时用）
  --san-dns <dns>    追加 broker 证书 SAN 域名（可重复）
  --broker-only      复用现有 CA 与客户端证书，仅重签 broker 证书 + keystore
                     （地址变化用它：车端 ca-cert.pem / 客户端证书无需更新，仅需重启 Kafka）
  --out <dir>        输出目录（默认 .env 的 KAFKA_CERTS_DIR=/opt/hunter-core/certs/kafka）
  --force            重新签发（含 CA！覆盖现有证书；须重启 Kafka 并向全部车端重发 CA 与证书）
  -y | --yes         非交互：SAN 漂移需仅重签 broker 时不再询问，直接执行（供 install.sh 调用）

环境变量（.env，可选）：
  KAFKA_CERT_SAN_IPS=192.168.31.35,101.201.150.237   逗号分隔的额外 SAN IP
  KAFKA_CERT_SAN_DNS=kafka.hunter-core.local         逗号分隔的额外 SAN 域名

示例：
  # 车端以 192.168.31.35 访问、SAN 只有公网 IP（主机名校验失败）→ 复用 CA 仅重签 broker
  sudo bash scripts/gen-kafka-certs.sh --ip 192.168.31.35 --san-ip 101.201.150.237 --broker-only
  sudo bash scripts/gen-kafka-certs.sh --san-dns kafka.example.com --broker-only
  sudo bash scripts/gen-kafka-certs.sh --out /opt/hunter-core/certs/kafka

重签后：
  · 校验 SAN：openssl x509 -in <dir>/broker-cert.pem -noout -ext subjectAltName
  · 重启 Kafka 加载新 keystore：docker compose restart kafka
    （改过 .env 的 SERVER_IP 时需 up -d --force-recreate kafka，restart 不重读 compose 变量）
  · 车端配置无需改动（CA 未变）；仅 --force 场景须重新分发 ca-cert.pem 与客户端证书
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
      --out)
        [ $# -ge 2 ] || die "--out 需要一个目录参数"
        OPT_OUT="$2"
        shift 2
        ;;
      --san-ip)
        [ $# -ge 2 ] || die "--san-ip 需要一个 IP 地址参数"
        SAN_IP_EXTRA+=("$2")
        shift 2
        ;;
      --san-dns)
        [ $# -ge 2 ] || die "--san-dns 需要一个域名参数"
        SAN_DNS_EXTRA+=("$2")
        shift 2
        ;;
      --broker-only)
        BROKER_ONLY=1
        shift
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

# require_tools：openssl 与 keytool 可用性（JKS 生成依赖 JDK）
require_tools() {
  if ! command_exists openssl; then
    log_error "缺少 openssl：apt-get install -y openssl"
    return 1
  fi
  local openssl_version
  openssl_version="$(openssl version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)"
  if ! version_ge "${openssl_version:-0}" "1.1.1"; then
    log_error "openssl 版本过低（${openssl_version:-unknown} < 1.1.1）：-addext 不可用，请升级"
    return 1
  fi
  if ! command_exists keytool; then
    log_error "缺少 keytool（JKS 生成依赖 JDK）"
    log_error "请安装：apt-get install -y openjdk-17-jre-headless（install.sh step_1 已包含）"
    return 1
  fi
  return 0
}

# resolve_server_ip：--ip > .env SERVER_IP（占位值/空值报错）
resolve_server_ip() {
  local ip="${OPT_IP:-${SERVER_IP:-}}"
  case "$ip" in
    "" | CHANGE_ME_*)
      log_error "SERVER_IP 未配置或仍为占位值（当前：${ip:-<空>}）"
      log_error "broker 证书 SAN 必须包含真实地址，否则车端 9093 握手中断"
      log_error "请执行：bash scripts/gen-passwords.sh --ip <服务器IP> 或本脚本 --ip <服务器IP>"
      return 1
      ;;
  esac
  printf '%s' "$ip"
}

# write_ext_file <path> <content>：生成 openssl 扩展配置（SAN/用途）
write_ext_file() {
  local path="$1"
  shift
  printf '%s\n' "$@" >"$path"
}

# =====================================================================
# SAN 地址集合（SERVER_IP + --san-ip/--san-dns + .env KAFKA_CERT_SAN_*）
# =====================================================================

# is_ipv4 <addr>：点分四段且各段 ≤ 255
is_ipv4() {
  local addr="${1:-}" octet
  [[ "$addr" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
  for octet in ${addr//./ }; do
    [ "$octet" -le 255 ] || return 1
  done
  return 0
}

# is_ipv6 <addr>：含冒号且字符集合法（不做完整展开比对，签发与提示足够）
is_ipv6() {
  local addr="${1:-}"
  [[ "$addr" =~ ^[0-9A-Fa-f:]+$ ]] && [[ "$addr" == *:* ]] || return 1
  return 0
}

# is_dns_name <name>：SAN DNS 条目合法性（字母数字与连字符，点分多段）
is_dns_name() {
  local name="${1:-}"
  [ ${#name} -le 253 ] || return 1
  [[ "$name" =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$ ]] || return 1
  return 0
}

# san_add <entry>：加入必含 SAN 条目（已存在则忽略，保持首次出现顺序）
san_add() {
  local entry="$1" existing
  for existing in "${SAN_ENTRIES[@]}"; do
    [ "$existing" = "$entry" ] && return 0
  done
  SAN_ENTRIES+=("$entry")
  return 0
}

# san_add_csv <ip|dns> <csv>：按逗号切分 .env 附加地址，逐项校验后加入 SAN（空白项忽略）
san_add_csv() {
  local kind="$1" csv="$2" raw item
  local -a items=()
  IFS=',' read -r -a items <<<"$csv"
  for item in ${items[@]+"${items[@]}"}; do
    raw="${item//[[:space:]]/}"
    [ -n "$raw" ] || continue
    if [ "$kind" = "ip" ]; then
      is_ipv4 "$raw" || is_ipv6 "$raw" || {
        log_error "KAFKA_CERT_SAN_IPS 含非法 IP：${raw}（应为逗号分隔的点分四段/IPv6 地址）"
        return 1
      }
      san_add "IP:${raw}"
    else
      is_dns_name "$raw" || {
        log_error "KAFKA_CERT_SAN_DNS 含非法域名：${raw}"
        return 1
      }
      san_add "DNS:${raw}"
    fi
  done
  return 0
}

# build_san_entries：汇总 SERVER_IP / 命令行 / .env 附加地址，写入 SAN_ENTRIES 与 SAN_VALUE
build_san_entries() {
  local ip dns

  SAN_ENTRIES=()
  san_add "DNS:kafka"
  san_add "DNS:localhost"
  san_add "DNS:${host_name}"
  san_add "IP:127.0.0.1"
  san_add "IP:${server_ip}"

  for ip in ${SAN_IP_EXTRA[@]+"${SAN_IP_EXTRA[@]}"}; do
    is_ipv4 "$ip" || is_ipv6 "$ip" || {
      log_error "--san-ip 参数不是合法 IP：${ip}"
      return 1
    }
    san_add "IP:${ip}"
  done
  for dns in ${SAN_DNS_EXTRA[@]+"${SAN_DNS_EXTRA[@]}"}; do
    is_dns_name "$dns" || {
      log_error "--san-dns 参数不是合法域名：${dns}"
      return 1
    }
    san_add "DNS:${dns}"
  done

  # .env 附加地址（逗号分隔；适合长期固定多地址部署，续跑/重装不会漏 SAN）
  san_add_csv "ip" "${KAFKA_CERT_SAN_IPS:-}" || return 1
  san_add_csv "dns" "${KAFKA_CERT_SAN_DNS:-}" || return 1

  SAN_VALUE="$(printf '%s,' "${SAN_ENTRIES[@]}")"
  SAN_VALUE="${SAN_VALUE%,}"
  return 0
}

# cert_sans <pem>：输出证书现有 SAN 条目（委派 common.sh 的 hc_cert_san_entries，全仓单一口径）
cert_sans() {
  hc_cert_san_entries "$1"
}

# missing_sans <pem> <条目...>：打印证书缺失的 SAN 条目；全部命中则无输出且返回 0
# 注：IPv6 条目不参与严格比对（openssl 输出为展开形式，与提交值写法差异大，误报会引发反复重签）
missing_sans() {
  local pem="$1" have need rc=0
  shift
  [ -f "$pem" ] || {
    printf '%s\n' "$@"
    return 1
  }
  have="$(cert_sans "$pem")"
  for need in "$@"; do
    case "$need" in
      IP:*:*) continue ;;  # IPv6，跳过比对
    esac
    printf '%s\n' "$have" | grep -qxF "$need" || {
      printf '%s\n' "$need"
      rc=1
    }
  done
  return "$rc"
}

# san_list_flat <多行条目>：换行拼接为单行（日志可读）
san_list_flat() {
  printf '%s' "$1" | tr '\n' ' '
}

# broker_readable_jks <file>：让 Kafka broker（GID 1001）可读取 JKS —— chgrp + 0640；
# 组不可用时退回 0644（口令保护的 keystore，单机可接受）。私钥 .pem 仍保持 0600。
broker_readable_jks() {
  local f="$1"
  if chgrp "$KAFKA_CERT_GID" "$f" 2>/dev/null; then
    chmod 640 "$f"
  else
    log_warn "无法 chgrp ${KAFKA_CERT_GID}（${f}）：退回 0644 以保证 broker 可读"
    chmod 644 "$f"
  fi
  return 0
}

# import_keystore <p12> <jks> <storepass> <alias>：PKCS12 → JKS 密钥库（Java 17 下 JKS 为兼容格式）
import_keystore() {
  local p12="$1" jks="$2" storepass="$3" alias="$4"
  rm -f "$jks"
  keytool -importkeystore \
    -srckeystore "$p12" -srcstoretype PKCS12 -srcstorepass "$storepass" \
    -destkeystore "$jks" -deststoretype JKS -deststorepass "$storepass" -destkeypass "$storepass" \
    -srcalias "$alias" -destalias "$alias" -noprompt >/dev/null 2>&1 || {
    log_error "导入 JKS 密钥库失败：${jks}（请检查 KAFKA_SSL_PASSWORD 与 keytool 版本）"
    return 1
  }
  return 0
}

# =====================================================================
# 分段生成（全流程与"仅重签 broker"两条路径复用）
# 依赖 main 中赋值的脚本级变量：cert_dir / server_ip / host_name / storepass / SAN_ENTRIES / SAN_VALUE
# =====================================================================

# gen_ca：自签 CA（3650 天；已存在且非 --force 时复用）
gen_ca() {
  if [ -f "${cert_dir}/ca-key.pem" ] && [ -f "${cert_dir}/ca-cert.pem" ] && [ "$FORCE" -ne 1 ]; then
    log_info "CA 已存在，复用（幂等）：${cert_dir}/ca-cert.pem"
    return 0
  fi
  log_info "生成 CA（${CA_DAYS} 天）"
  openssl req -x509 -newkey rsa:4096 -sha256 -days "$CA_DAYS" -nodes \
    -keyout "${cert_dir}/ca-key.pem" -out "${cert_dir}/ca-cert.pem" \
    -subj "$CA_SUBJECT" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1 || {
    log_error "CA 生成失败：请检查 openssl 版本与 ${cert_dir} 写权限"
    return 1
  }
  chmod 600 "${cert_dir}/ca-key.pem"
  chmod 644 "${cert_dir}/ca-cert.pem"
  log_success "已生成 CA：ca-key.pem（600）/ ca-cert.pem（644）"
}

# write_ext_files：落盘 openssl 扩展配置（broker/client 用途与 SAN；ca-ext.cnf 仅留档）
write_ext_files() {
  write_ext_file "${cert_dir}/broker-ext.cnf" \
    "basicConstraints=CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=serverAuth,clientAuth" \
    "subjectAltName=${SAN_VALUE}"
  write_ext_file "${cert_dir}/client-ext.cnf" \
    "basicConstraints=CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=clientAuth" \
    "subjectAltName=DNS:kafka-client"
  # openssl x509 -req 通过 -extfile 显式传入扩展，ca-ext.cnf 只记录本次签发策略
  write_ext_file "${cert_dir}/ca-ext.cnf" \
    "# HunterCore Kafka 证书签发策略（留档，不参与签发）" \
    "broker SAN: ${SAN_VALUE}" \
    "client SAN: DNS:kafka-client"
}

# gen_broker：broker 密钥 + CSR + CA 签发（CN=kafka，SAN = SAN_VALUE，含 serverAuth）
gen_broker() {
  log_info "生成 broker 密钥/CSR 并用 CA 签发（SAN：${SAN_VALUE}）"
  openssl req -newkey rsa:2048 -nodes -sha256 \
    -keyout "${cert_dir}/broker-key.pem" -out "${cert_dir}/broker.csr" \
    -subj "$BROKER_SUBJECT" >/dev/null 2>&1 || {
    log_error "broker CSR 生成失败：请检查 openssl 与 ${cert_dir} 写权限"
    return 1
  }
  openssl x509 -req -in "${cert_dir}/broker.csr" -sha256 -days "$LEAF_DAYS" \
    -CA "${cert_dir}/ca-cert.pem" -CAkey "${cert_dir}/ca-key.pem" -CAcreateserial \
    -extfile "${cert_dir}/broker-ext.cnf" -out "${cert_dir}/broker-cert.pem" >/dev/null 2>&1 || {
    log_error "broker 证书签发失败：请确认 ca-key.pem 与 ca-cert.pem 匹配（同批生成）"
    return 1
  }
  chmod 600 "${cert_dir}/broker-key.pem"
  chmod 644 "${cert_dir}/broker-cert.pem"
  log_success "已生成 broker 证书：broker-key.pem（600）/ broker-cert.pem（644）"
}

# gen_client：客户端（车端）密钥 + CSR + CA 签发（CN=kafka-client，clientAuth）
gen_client() {
  log_info "生成客户端（车端）密钥/CSR 并用 CA 签发"
  openssl req -newkey rsa:2048 -nodes -sha256 \
    -keyout "${cert_dir}/client-key.pem" -out "${cert_dir}/client.csr" \
    -subj "$CLIENT_SUBJECT" >/dev/null 2>&1 || {
    log_error "客户端 CSR 生成失败"
    return 1
  }
  openssl x509 -req -in "${cert_dir}/client.csr" -sha256 -days "$LEAF_DAYS" \
    -CA "${cert_dir}/ca-cert.pem" -CAkey "${cert_dir}/ca-key.pem" -CAcreateserial \
    -extfile "${cert_dir}/client-ext.cnf" -out "${cert_dir}/client-cert.pem" >/dev/null 2>&1 || {
    log_error "客户端证书签发失败"
    return 1
  }
  chmod 600 "${cert_dir}/client-key.pem"
  chmod 644 "${cert_dir}/client-cert.pem"
  log_success "已生成客户端证书：client-key.pem（600）/ client-cert.pem（644）"
}

# build_keystore：broker 密钥与证书链 → PKCS12 → kafka.keystore.jks（口令 = KAFKA_SSL_PASSWORD）
build_keystore() {
  log_info "构建 broker keystore（PKCS12 → JKS）"
  openssl pkcs12 -export \
    -in "${cert_dir}/broker-cert.pem" -inkey "${cert_dir}/broker-key.pem" \
    -certfile "${cert_dir}/ca-cert.pem" -name hunter-kafka-broker \
    -out "${cert_dir}/broker.p12" -passout "pass:${storepass}" >/dev/null 2>&1 || {
    log_error "broker PKCS12 导出失败"
    return 1
  }
  import_keystore "${cert_dir}/broker.p12" "${cert_dir}/kafka.keystore.jks" "$storepass" "hunter-kafka-broker" || return 1
  chmod 600 "${cert_dir}/broker.p12"
  broker_readable_jks "${cert_dir}/kafka.keystore.jks"
  log_success "已生成 kafka.keystore.jks（组 ${KAFKA_CERT_GID} 可读，别名 hunter-kafka-broker）"
}

# build_truststore：CA → kafka.truststore.jks
build_truststore() {
  log_info "构建 kafka.truststore.jks（导入 CA）"
  rm -f "${cert_dir}/kafka.truststore.jks"
  keytool -importcert -noprompt -alias hunter-core-kafka-ca \
    -file "${cert_dir}/ca-cert.pem" \
    -keystore "${cert_dir}/kafka.truststore.jks" -storetype JKS -storepass "$storepass" >/dev/null 2>&1 || {
    log_error "导入 truststore 失败：请检查 KAFKA_SSL_PASSWORD 是否正确"
    return 1
  }
  broker_readable_jks "${cert_dir}/kafka.truststore.jks"
  log_success "已生成 kafka.truststore.jks（组 ${KAFKA_CERT_GID} 可读，别名 hunter-core-kafka-ca）"
}

# build_client_p12：车端客户端 PKCS12（含密钥与证书链）
build_client_p12() {
  log_info "生成车端客户端 PKCS12：kafka-client.p12"
  openssl pkcs12 -export \
    -in "${cert_dir}/client-cert.pem" -inkey "${cert_dir}/client-key.pem" \
    -certfile "${cert_dir}/ca-cert.pem" -name hunter-kafka-client \
    -out "${cert_dir}/kafka-client.p12" -passout "pass:${storepass}" >/dev/null 2>&1 || {
    log_error "车端 PKCS12 导出失败"
    return 1
  }
  chmod 600 "${cert_dir}/kafka-client.p12"
}

# verify_bundle：证书链 + SAN 全地址覆盖 + JKS 别名（SAN 缺项即失败，绝不带病交付）
verify_bundle() {
  local -a chain=("${cert_dir}/broker-cert.pem")
  local missing

  log_info "校验证书链与 SAN"
  if [ -f "${cert_dir}/client-cert.pem" ]; then
    chain+=("${cert_dir}/client-cert.pem")
  fi
  if ! openssl verify -CAfile "${cert_dir}/ca-cert.pem" "${chain[@]}" >/dev/null 2>&1; then
    log_error "证书链校验失败：${chain[*]} 未通过 CA 验证（须与 ca-cert.pem/ca-key.pem 同批签发）"
    return 1
  fi
  missing="$(missing_sans "${cert_dir}/broker-cert.pem" "${SAN_ENTRIES[@]}" || true)"
  if [ -n "$missing" ]; then
    log_error "broker 证书 SAN 缺少访问地址：$(san_list_flat "$missing")"
    log_error "车端以这些地址连 9093 会因主机名/SAN 校验失败而中断（车端无法自行修正）"
    log_error "请核对 ${cert_dir}/broker-ext.cnf 与 openssl 版本（subjectAltName 是否被完整写入）"
    return 1
  fi
  log_success "broker 证书 SAN 覆盖全部访问地址：${SAN_VALUE}"
  if [ -f "${cert_dir}/kafka.keystore.jks" ] &&
    ! keytool -list -keystore "${cert_dir}/kafka.keystore.jks" -storepass "$storepass" -storetype JKS 2>/dev/null |
    grep -q "hunter-kafka-broker"; then
    log_warn "keystore 中未找到别名 hunter-kafka-broker，请人工复核 kafka.keystore.jks"
  fi
  if [ -f "${cert_dir}/kafka.truststore.jks" ] &&
    ! keytool -list -keystore "${cert_dir}/kafka.truststore.jks" -storepass "$storepass" -storetype JKS 2>/dev/null |
    grep -q "hunter-core-kafka-ca"; then
    log_warn "truststore 中未找到别名 hunter-core-kafka-ca，请人工复核 kafka.truststore.jks"
  fi
  return 0
}

# print_guidance：结果与配置指引（JKS 口令仅终端显示，不落日志）
print_guidance() {
  local prev_log="${HC_LOG_TO_FILE:-1}"
  log_success "Kafka 证书就绪（目录：${cert_dir}）"
  log_info "文件清单："
  ls -l --time-style=+ "$cert_dir" | sed 's/^/    /' || true
  HC_LOG_TO_FILE=0
  printf '\n%s\n' "${C_YELLOW}---- Kafka broker（compose 环境变量与挂载）----${C_RESET}"
  printf '  KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/kafka.keystore.jks\n'
  printf '  KAFKA_SSL_KEYSTORE_PASSWORD=%s\n' "$storepass"
  printf '  KAFKA_SSL_KEY_PASSWORD=%s\n' "$storepass"
  printf '  KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/kafka.truststore.jks\n'
  printf '  KAFKA_SSL_TRUSTSTORE_PASSWORD=%s\n' "$storepass"
  printf '  KAFKA_EXTERNAL_LISTENERS=EXTERNAL://0.0.0.0:9093,INTERNAL://0.0.0.0:9092\n'
  printf '%s\n' "${C_YELLOW}---- 车端客户端（SASL_SSL + SCRAM-SHA-512）----${C_RESET}"
  printf '  ssl.ca.location=%s/ca-cert.pem\n' "$cert_dir"
  printf '  ssl.certificate.location=%s/client-cert.pem\n' "$cert_dir"
  printf '  ssl.key.location=%s/client-key.pem\n' "$cert_dir"
  printf '  （或改用 PKCS12：%s/kafka-client.p12，口令见 .env KAFKA_SSL_PASSWORD）\n' "$cert_dir"
  printf '  bootstrap.servers=%s:9093  security.protocol=SASL_SSL  sasl.mechanism=SCRAM-SHA-512\n' "$server_ip"
  printf '  证书 SAN 还认可以下接入地址（均为同一张 broker 证书）：\n'
  local entry
  for entry in "${SAN_ENTRIES[@]}"; do
    [ "$entry" = "IP:${server_ip}" ] && continue
    case "$entry" in
      IP:*) printf '    %s:9093\n' "${entry#IP:}" ;;
      DNS:*) printf '    %s:9093（域名，需车端可解析）\n' "${entry#DNS:}" ;;
    esac
  done
  printf '\n'
  HC_LOG_TO_FILE="$prev_log"
  return 0
}

# reissue_broker：复用现有 CA 与客户端证书，仅重签 broker 证书 + keystore
# 适用：SERVER_IP / SAN 地址变化（车端 ca-cert.pem 与客户端证书无需重新分发）
reissue_broker() {
  log_info "===== 仅重签 broker 证书（目录：${cert_dir}，SAN：${SAN_VALUE}）====="
  if [ ! -f "${cert_dir}/ca-key.pem" ] || [ ! -f "${cert_dir}/ca-cert.pem" ]; then
    log_error "复用 CA 重签需要 CA 私钥与证书已存在：${cert_dir}/ca-key.pem、${cert_dir}/ca-cert.pem"
    log_error "无 CA（或确需更换 CA）请执行完整签发：bash $0 --ip ${server_ip}（--force 会连 CA 一起换，须同步全部车端）"
    return 1
  fi
  install -d -m 0755 "$cert_dir"
  write_ext_files || return 1
  gen_broker || return 1
  build_keystore || return 1
  verify_bundle || return 1
  print_guidance
  log_warn "CA 与车端客户端证书未变：车端 ca-cert.pem / kafka-client.p12 无需重新分发"
  log_warn "必须让 Kafka 重新加载 keystore：cd ${APP_DIR} && docker compose restart kafka"
  log_warn "  若本次同时改了 .env 的 SERVER_IP（advertised.listeners / 9093 端口绑定）：docker compose up -d --force-recreate kafka"
  log_warn "车端侧核对：bootstrap 地址必须出现在上面的 SAN 列表中（含内网 IP / 域名）"
  log_success "broker 证书重签完成"
}

# =====================================================================
# 主流程
# =====================================================================
main() {
  local env_file missing

  parse_args "$@"
  hc_log_begin
  require_tools || return 1

  env_file="${HUNTER_ENV_FILE:-${APP_DIR}/.env}"
  load_env "$env_file" || return 1
  HUNTER_ENV_FILE="$env_file"
  export HUNTER_ENV_FILE

  if ! require_env KAFKA_SSL_PASSWORD; then
    log_error "KAFKA_SSL_PASSWORD 未配置（JKS 口令）：请执行 bash scripts/gen-passwords.sh"
    return 1
  fi
  storepass="${KAFKA_SSL_PASSWORD}"
  server_ip="$(resolve_server_ip)" || return 1
  cert_dir="${OPT_OUT:-${KAFKA_CERTS_DIR:-${APP_DIR}/certs/kafka}}"
  host_name="$(hostname)"

  if [ "$BROKER_ONLY" -eq 1 ] && [ "$FORCE" -eq 1 ]; then
    log_warn "--broker-only 与 --force 同时给出：按 --broker-only 处理（保留现有 CA，车端信任链不变）"
  fi

  build_san_entries || return 1
  log_info "本次 broker 证书 SAN 访问地址：${SAN_VALUE}"

  # ---------- 路径 A：--broker-only（复用 CA，仅重签 broker 证书与 keystore） ----------
  if [ "$BROKER_ONLY" -eq 1 ]; then
    reissue_broker
    return $?
  fi

  # ---------- 路径 B：幂等复用（含 SAN 漂移自愈） ----------
  if [ "$FORCE" -ne 1 ] && [ -f "${cert_dir}/kafka.keystore.jks" ] && [ -f "${cert_dir}/kafka.truststore.jks" ]; then
    # 自愈一：即便复用旧证书，也必须重申目录可穿透（0755）与 JKS 对 broker（GID 1001）可读；
    # 否则早期版本或异常中断遗留的 0600 root:root 证书会让 broker 加载 keystore 报
    # AccessDeniedException → SASL_SSL 监听初始化失败 → 整个 broker 崩溃循环（9092 从未监听）。
    chmod 0755 "$cert_dir" 2>/dev/null || true
    broker_readable_jks "${cert_dir}/kafka.keystore.jks"
    broker_readable_jks "${cert_dir}/kafka.truststore.jks"

    # 自愈二：证书可复用的前提是 SAN 仍覆盖车端要用的全部地址（IP 变更后旧证书一律作废）
    missing="$(missing_sans "${cert_dir}/broker-cert.pem" "${SAN_ENTRIES[@]}" || true)"
    if [ -n "$missing" ]; then
      log_warn "现有 broker 证书 SAN 缺少访问地址：$(san_list_flat "$missing")"
      log_warn "典型根因：服务器 IP 变更（如公网 IP → 内网 IP）后证书未重签"
      log_warn "车端 ssl.endpoint.identification.algorithm=https 只比 SAN → 主机名校验失败、连不上 9093"
      if [ "$ASSUME_YES" -ne 1 ] && ! confirm "复用现有 CA 仅重签 broker 证书（车端信任链不变，需重启 Kafka）？"; then
        log_error "用户取消：证书未修改，车端将因 SAN 不匹配继续连接失败"
        return 1
      fi
      reissue_broker
      return $?
    fi
    log_info "Kafka 证书已存在（${cert_dir}）且 SAN 覆盖全部访问地址，跳过生成（幂等）"
    log_info "地址变化：bash $0 --broker-only（复用 CA，车端无需更新）"
    log_info "更换 CA：bash $0 --force（⚠ 须向全部车端重新分发 ca-cert.pem 与客户端证书）"
    return 0
  fi
  if [ -f "${cert_dir}/kafka.keystore.jks" ]; then
    log_warn "检测到已有 keystore，本次将重新签发（覆盖同名文件）"
    if [ "$FORCE" -ne 1 ] && ! confirm "覆盖现有 Kafka 证书并重新签发？"; then
      log_info "用户取消：未修改任何证书"
      return 0
    fi
  fi

  # ---------- 路径 C：全新签发 ----------
  install -d -m 0755 "$cert_dir"
  log_info "===== 生成 Kafka SASL_SSL 证书（目录：${cert_dir}，SERVER_IP=${server_ip}）====="

  gen_ca || return 1
  write_ext_files || return 1

  # broker 证书：仅在 SAN 已覆盖全部访问地址时才复用，否则重签（避免旧 SAN 遗留）
  if [ -f "${cert_dir}/broker-cert.pem" ] && [ "$FORCE" -ne 1 ] &&
    missing_sans "${cert_dir}/broker-cert.pem" "${SAN_ENTRIES[@]}" >/dev/null; then
    log_info "broker 证书已存在且 SAN 完整，复用（幂等）：broker-cert.pem"
  else
    gen_broker || return 1
  fi

  if [ -f "${cert_dir}/client-cert.pem" ] && [ "$FORCE" -ne 1 ]; then
    log_info "客户端证书已存在，复用（幂等）：client-cert.pem"
  else
    gen_client || return 1
  fi

  build_keystore || return 1
  build_truststore || return 1
  build_client_p12 || return 1
  verify_bundle || return 1
  print_guidance

  if [ "$FORCE" -eq 1 ]; then
    log_warn "--force 已连同 CA 一起重签：必须重启 Kafka 容器（docker compose restart kafka）"
    log_warn "并向全部车端重新分发 ca-cert.pem 与客户端证书（旧 CA 签发的证书链即刻失效）"
  fi
  log_warn "私钥与 JKS 严禁外传；车端仅分发 ca-cert.pem 与客户端证书/密钥（或 kafka-client.p12）"
  log_success "证书生成完成（私钥/P12 600；JKS 组 1001 可读 640；证书 644）"
}

main "$@"
