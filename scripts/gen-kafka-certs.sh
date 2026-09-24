#!/usr/bin/env bash
# =====================================================================
# HunterCore 单机部署 —— Kafka SASL_SSL 证书生成（gen-kafka-certs.sh）
#
# 用途：为 Kafka 外部监听（9093 / SASL_SSL + SCRAM-SHA-512，车端接入）生成全套证书：
#   ① CA（自签，3650 天）：ca-key.pem / ca-cert.pem
#   ② broker 密钥 + CSR（CN=kafka，SAN=DNS:kafka,DNS:localhost,DNS:<hostname>,IP:127.0.0.1,IP:<SERVER_IP>）
#      → ca-cert.pem 签发 → broker-cert.pem
#   ③ 客户端（车端）密钥 + 证书（CN=kafka-client，clientAuth）
#   ④ broker 密钥与证书链导入 JKS：kafka.keystore.jks（口令 = .env 的 KAFKA_SSL_PASSWORD）
#   ⑤ CA 导入 JKS：kafka.truststore.jks
#   ⑥ 车端所需：kafka-client.p12（PKCS12，含客户端密钥+证书链）与 ca-cert.pem
#
# ⚠ 关键约束：broker 证书 SAN 必须包含真实的 SERVER_IP（车端以 SERVER_IP:9093 连接），
#   否则 TLS 握手因主机名/地址不匹配而失败。SERVER_IP 变更后必须 --force 重新签发。
#
# 用法：
#   sudo bash scripts/gen-kafka-certs.sh [选项]
#
# 参数：
#   --help         显示本帮助
#   --ip <address>  覆盖 SERVER_IP（默认取 .env 的 SERVER_IP；占位值会报错）
#   --out <dir>     证书输出目录（默认 .env 的 KAFKA_CERTS_DIR，即 /opt/hunter-core/certs/kafka）
#   --force         重新签发全部证书与 JKS（⚠ 需重启 Kafka 并更新车端证书）
#
# 示例：
#   sudo bash /opt/hunter-core/scripts/gen-kafka-certs.sh --ip 192.168.1.10
#   sudo bash /opt/hunter-core/scripts/gen-kafka-certs.sh --force
#
# 幂等：kafka.keystore.jks 与 kafka.truststore.jks 已存在则跳过（--force 除外）。
# 权限：私钥/JKS/P12 = 600；证书（含 CA）= 644。
# 依赖：common.sh（同目录）、openssl ≥1.1.1、keytool（openjdk-17-jre-headless）
# 日期：2026-09-19  |  目标系统：Ubuntu 22.04 LTS
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
. "${SCRIPT_DIR}/common.sh"
# shellcheck disable=SC2154  # SERVER_IP / KAFKA_SSL_PASSWORD / KAFKA_CERTS_DIR 由 .env 注入

ASSUME_YES=0
FORCE=0
OPT_IP=""
OPT_OUT=""

# 证书参数（有效期与主题，车端信任链依赖 CA，故 CA 变更必须同步车端）
CA_DAYS=3650
LEAF_DAYS=3650
CA_SUBJECT="/C=CN/O=HunterCore/CN=hunter-core-kafka-ca"
BROKER_SUBJECT="/C=CN/O=HunterCore/CN=kafka"
CLIENT_SUBJECT="/C=CN/O=HunterCore/CN=kafka-client"

usage() {
  cat <<'EOF'
HunterCore Kafka SASL_SSL 证书生成脚本

用途：
  生成 Kafka 车端接入（9093 / SASL_SSL）所需的 CA、broker、客户端证书与 JKS：
    ca-key.pem / ca-cert.pem            自签 CA（3650 天）
    broker-key.pem / broker.csr / broker-cert.pem（CN=kafka，SAN 含 SERVER_IP 与主机名）
    client-key.pem / client.csr / client-cert.pem（CN=kafka-client，clientAuth）
    kafka.keystore.jks                  broker 密钥库（口令 = KAFKA_SSL_PASSWORD）
    kafka.truststore.jks                CA 信任库（口令 = KAFKA_SSL_PASSWORD）
    kafka-client.p12                    车端客户端 PKCS12（含密钥与证书链）

用法：
  sudo bash scripts/gen-kafka-certs.sh [选项]

参数：
  --help          显示本帮助
  --ip <address>  覆盖 SERVER_IP（默认读 .env；占位值将直接报错退出）
  --out <dir>     输出目录（默认 .env 的 KAFKA_CERTS_DIR=/opt/hunter-core/certs/kafka）
  --force         重新签发（覆盖现有证书；须重启 Kafka 并更新车端证书）

示例：
  sudo bash scripts/gen-kafka-certs.sh --ip 192.168.1.10
  sudo bash scripts/gen-kafka-certs.sh --out /opt/hunter-core/certs/kafka

签发后：
  · 校验 SAN：openssl x509 -in <dir>/broker-cert.pem -noout -ext subjectAltName
  · Kafka 容器挂载 keystore/truststore 并重启；车端分发 ca-cert.pem + kafka-client.p12
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

# import_keystore <p12> <jks> <storepass>：PKCS12 → JKS 密钥库（Java 17 下 JKS 为兼容格式）
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
# 主流程
# =====================================================================
main() {
  local env_file cert_dir server_ip storepass host_name
  local ca_ext broker_ext client_ext

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

  # 幂等：keystore 与 truststore 齐备即跳过
  if [ "$FORCE" -ne 1 ] && [ -f "${cert_dir}/kafka.keystore.jks" ] && [ -f "${cert_dir}/kafka.truststore.jks" ]; then
    log_info "Kafka 证书已存在（${cert_dir}），跳过生成（幂等）"
    log_info "重新签发：bash $0 --force（需重启 Kafka 并更新车端证书）"
    return 0
  fi
  if [ -f "${cert_dir}/kafka.keystore.jks" ]; then
    log_warn "检测到已有 keystore，本次将重新签发（覆盖同名文件）"
    if [ "$FORCE" -ne 1 ] && ! confirm "覆盖现有 Kafka 证书并重新签发？"; then
      log_info "用户取消：未修改任何证书"
      return 0
    fi
  fi

  install -d -m 0755 "$cert_dir"
  log_info "===== 生成 Kafka SASL_SSL 证书（目录：${cert_dir}，SERVER_IP=${server_ip}）====="

  # ---------- 1) 自签 CA（3650 天） ----------
  if [ -f "${cert_dir}/ca-key.pem" ] && [ -f "${cert_dir}/ca-cert.pem" ] && [ "$FORCE" -ne 1 ]; then
    log_info "CA 已存在，复用（幂等）：${cert_dir}/ca-cert.pem"
  else
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
  fi

  # ---------- 2) broker 密钥 + CSR + 签发（CN=kafka，SAN 含 SERVER_IP） ----------
  ca_ext="${cert_dir}/ca-ext.cnf"
  broker_ext="${cert_dir}/broker-ext.cnf"
  client_ext="${cert_dir}/client-ext.cnf"
  write_ext_file "$broker_ext" \
    "basicConstraints=CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=serverAuth,clientAuth" \
    "subjectAltName=DNS:kafka,DNS:localhost,DNS:${host_name},IP:127.0.0.1,IP:${server_ip}"
  write_ext_file "$client_ext" \
    "basicConstraints=CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=clientAuth" \
    "subjectAltName=DNS:kafka-client"
  # ca-ext.cnf 仅留档（记录签发策略），openssl x509 -req 通过 -extfile 显式传入扩展
  write_ext_file "$ca_ext" \
    "# HunterCore Kafka 证书签发策略（留档，不参与签发）" \
    "broker SAN: DNS:kafka, DNS:localhost, DNS:${host_name}, IP:127.0.0.1, IP:${server_ip}" \
    "client SAN: DNS:kafka-client"

  if [ -f "${cert_dir}/broker-cert.pem" ] && [ "$FORCE" -ne 1 ]; then
    log_info "broker 证书已存在，复用（幂等）：broker-cert.pem"
  else
    log_info "生成 broker 密钥/CSR 并用 CA 签发（SAN 含 ${server_ip}）"
    openssl req -newkey rsa:2048 -nodes -sha256 \
      -keyout "${cert_dir}/broker-key.pem" -out "${cert_dir}/broker.csr" \
      -subj "$BROKER_SUBJECT" >/dev/null 2>&1 || {
      log_error "broker CSR 生成失败：请检查 openssl 与 ${cert_dir} 写权限"
      return 1
    }
    openssl x509 -req -in "${cert_dir}/broker.csr" -sha256 -days "$LEAF_DAYS" \
      -CA "${cert_dir}/ca-cert.pem" -CAkey "${cert_dir}/ca-key.pem" -CAcreateserial \
      -extfile "$broker_ext" -out "${cert_dir}/broker-cert.pem" >/dev/null 2>&1 || {
      log_error "broker 证书签发失败：请确认 ca-key.pem 与 ca-cert.pem 匹配（同批生成）"
      return 1
    }
    chmod 600 "${cert_dir}/broker-key.pem"
    chmod 644 "${cert_dir}/broker-cert.pem"
    log_success "已生成 broker 证书：broker-key.pem（600）/ broker-cert.pem（644）"
  fi

  # ---------- 3) 客户端（车端）密钥 + 证书 ----------
  if [ -f "${cert_dir}/client-cert.pem" ] && [ "$FORCE" -ne 1 ]; then
    log_info "客户端证书已存在，复用（幂等）：client-cert.pem"
  else
    log_info "生成客户端（车端）密钥/CSR 并用 CA 签发"
    openssl req -newkey rsa:2048 -nodes -sha256 \
      -keyout "${cert_dir}/client-key.pem" -out "${cert_dir}/client.csr" \
      -subj "$CLIENT_SUBJECT" >/dev/null 2>&1 || {
      log_error "客户端 CSR 生成失败"
      return 1
    }
    openssl x509 -req -in "${cert_dir}/client.csr" -sha256 -days "$LEAF_DAYS" \
      -CA "${cert_dir}/ca-cert.pem" -CAkey "${cert_dir}/ca-key.pem" -CAcreateserial \
      -extfile "$client_ext" -out "${cert_dir}/client-cert.pem" >/dev/null 2>&1 || {
      log_error "客户端证书签发失败"
      return 1
    }
    chmod 600 "${cert_dir}/client-key.pem"
    chmod 644 "${cert_dir}/client-cert.pem"
    log_success "已生成客户端证书：client-key.pem（600）/ client-cert.pem（644）"
  fi

  # ---------- 4) broker keystore（JKS，口令 = KAFKA_SSL_PASSWORD） ----------
  log_info "构建 broker keystore（PKCS12 → JKS）"
  openssl pkcs12 -export \
    -in "${cert_dir}/broker-cert.pem" -inkey "${cert_dir}/broker-key.pem" \
    -certfile "${cert_dir}/ca-cert.pem" -name hunter-kafka-broker \
    -out "${cert_dir}/broker.p12" -passout "pass:${storepass}" >/dev/null 2>&1 || {
    log_error "broker PKCS12 导出失败"
    return 1
  }
  import_keystore "${cert_dir}/broker.p12" "${cert_dir}/kafka.keystore.jks" "$storepass" "hunter-kafka-broker" || return 1
  chmod 600 "${cert_dir}/broker.p12" "${cert_dir}/kafka.keystore.jks"
  log_success "已生成 kafka.keystore.jks（600，别名 hunter-kafka-broker）"

  # ---------- 5) truststore（JKS，导入 CA） ----------
  log_info "构建 kafka.truststore.jks（导入 CA）"
  rm -f "${cert_dir}/kafka.truststore.jks"
  keytool -importcert -noprompt -alias hunter-core-kafka-ca \
    -file "${cert_dir}/ca-cert.pem" \
    -keystore "${cert_dir}/kafka.truststore.jks" -storetype JKS -storepass "$storepass" >/dev/null 2>&1 || {
    log_error "导入 truststore 失败：请检查 KAFKA_SSL_PASSWORD 是否正确"
    return 1
  }
  chmod 600 "${cert_dir}/kafka.truststore.jks"
  log_success "已生成 kafka.truststore.jks（600，别名 hunter-core-kafka-ca）"

  # ---------- 6) 车端客户端 PKCS12（含密钥与证书链） ----------
  log_info "生成车端客户端 PKCS12：kafka-client.p12"
  openssl pkcs12 -export \
    -in "${cert_dir}/client-cert.pem" -inkey "${cert_dir}/client-key.pem" \
    -certfile "${cert_dir}/ca-cert.pem" -name hunter-kafka-client \
    -out "${cert_dir}/kafka-client.p12" -passout "pass:${storepass}" >/dev/null 2>&1 || {
    log_error "车端 PKCS12 导出失败"
    return 1
  }
  chmod 600 "${cert_dir}/kafka-client.p12"

  # ---------- 7) 校验（证书链 + SAN + keystore 别名） ----------
  log_info "校验证书链与 SAN"
  if ! openssl verify -CAfile "${cert_dir}/ca-cert.pem" \
    "${cert_dir}/broker-cert.pem" "${cert_dir}/client-cert.pem" >/dev/null 2>&1; then
    log_error "证书链校验失败：broker/client 证书未通过 CA 验证"
    return 1
  fi
  local broker_san
  broker_san="$(openssl x509 -in "${cert_dir}/broker-cert.pem" -noout -ext subjectAltName 2>/dev/null |
    tail -n +2 | tr -d ' ')"
  case "$broker_san" in
    *"IPAddress:${server_ip}"*) log_success "broker SAN 含 SERVER_IP=${server_ip}" ;;
    *)
      log_error "broker SAN 不含 SERVER_IP=${server_ip}（当前：${broker_san}）"
      log_error "车端无法完成 TLS 握手：请执行 --force --ip ${server_ip} 重新签发"
      return 1
      ;;
  esac
  if ! keytool -list -keystore "${cert_dir}/kafka.keystore.jks" -storepass "$storepass" -storetype JKS 2>/dev/null |
    grep -q "hunter-kafka-broker"; then
    log_warn "keystore 中未找到别名 hunter-kafka-broker，请人工复核 kafka.keystore.jks"
  fi
  if ! keytool -list -keystore "${cert_dir}/kafka.truststore.jks" -storepass "$storepass" -storetype JKS 2>/dev/null |
    grep -q "hunter-core-kafka-ca"; then
    log_warn "truststore 中未找到别名 hunter-core-kafka-ca，请人工复核 kafka.truststore.jks"
  fi

  # ---------- 8) 结果与配置指引（JKS 口令仅终端显示，不落日志） ----------
  log_success "Kafka 证书生成完成（目录：${cert_dir}）"
  log_info "文件清单："
  ls -l --time-style=+ "$cert_dir" | sed 's/^/    /' || true
  local prev_log="${HC_LOG_TO_FILE:-1}"
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
  printf '  bootstrap.servers=%s:9093  security.protocol=SASL_SSL  sasl.mechanism=SCRAM-SHA-512\n\n' "$server_ip"
  HC_LOG_TO_FILE="$prev_log"

  if [ "$FORCE" -eq 1 ]; then
    log_warn "--force 已重新签发：必须重启 Kafka 容器（docker compose restart kafka）并更新车端证书，否则握手失败"
  fi
  log_warn "私钥与 JKS 严禁外传；车端仅分发 ca-cert.pem 与客户端证书/密钥（或 kafka-client.p12）"
  log_success "证书生成完成（私钥/JKS/P12 权限 600，证书 644）"
}

main "$@"