"""车端接入包（bundle）ZIP 生成。

契约依据：contracts/openapi/vehicle-service.yaml `/api/v1/vehicle/{vehicle_id}/bundle`。

ZIP 结构（顶层目录 = vehicle_id，前端下载后直接解压到车端可读位置）：
```
<vehicle_id>/
  ca-cert.pem          # 复制 CA 证书（供车端信任链）
  client-cert.pem      # 该车独立客户端证书（CN = vehicle_id）
  client-key.pem       # 客户端私钥（0600，仅 bundle 中出现一次）
  kafka-client.p12     # PKCS12 打包（Java Kafka Producer/Consumer 直接可用）
  kafka.properties     # 车端接入配置模板（bootstrap / 安全协议 / SCRAM / Topic 清单）
  README.md            # 快速接入步骤说明
```

口令策略：
- **SCRAM 口令**：**不在 bundle 中出现**（一次性通过 createVehicle / rotateScram 响应返回）；
  kafka.properties 中 `sasl.jaas.config` 使用 `<SCRAM_PASSWORD>` 占位符 + 顶部醒目注释；
  前端向导在开通成功后引导用户对号入座。
- **PKCS12 口令**：写入 kafka.properties 头部注释（与 p12 文件同分发路径，风险一致，
  不额外增加暴露面）。

生成物为内存字节串（`bytes`），不落盘临时文件；单次响应完成后即被丢弃。
"""
from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import settings
from app.services.cert_signer import read_p12_password, vehicle_dir

logger = get_logger("app.services.bundle")

#: kafka.properties 模板（车端 Java Producer/Consumer 直接可用；参考 topics.yaml#producer_defaults）
_KAFKA_PROPERTIES_TMPL = """# HunterCore 车端接入配置 —— vehicle_id={vehicle_id}
# ⚠ 首次使用/轮换后请由运营手工填入 SCRAM 口令（开通响应一次性返回，服务端不持久化）
# PKCS12 口令（仅 kafka-client.p12 用）：{p12_password}
bootstrap.servers={bootstrap}
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\
  username="{vehicle_id}" \\
  password="<SCRAM_PASSWORD>";

# mTLS：使用 bundle 中的 kafka-client.p12（Java 生态）
# 或非 Java 场景显式指定 PEM：
#   ssl.keystore.location=./{vehicle_id}/kafka-client.p12
#   ssl.keystore.password={p12_password}
#   ssl.key.password={p12_password}
#   ssl.truststore.location=<由 ca-cert.pem 导入的 JKS，见 README>
ssl.endpoint.identification.algorithm=https

# 生产者基准参数（对齐 contracts/kafka/topics.yaml#producer_defaults）
linger.ms=5
batch.size=16384
retries=3
compression.type=lz4
acks=1                  # telemetry 高频；event/command/ota_* 请按 Topic 单独设置 acks=all

# 消费者基准（若车端消费 hunter.{{vehicle_id}}.command / remote_control / ota_notify）
enable.auto.commit=false
auto.offset.reset=earliest
isolation.level=read_committed

# 车端 Topic（8 个/车，命名 = hunter.<vehicle_id>.<type>）
# {topic_list}
"""

_README_TMPL = """# HunterCore 车端接入包（{vehicle_id}）

生成时间：{generated_at}

## 内容清单

| 文件 | 用途 |
|---|---|
| `ca-cert.pem` | 平台 CA 根证书（车端信任链） |
| `client-cert.pem` | 该车独立 X.509 客户端证书（CN = `{vehicle_id}`） |
| `client-key.pem` | 客户端私钥（**0600**，禁止入镜像层/日志/版本控制） |
| `kafka-client.p12` | PKCS12 打包（Java Kafka Producer/Consumer 直接可用） |
| `kafka.properties` | 车端接入参数模板（含 bootstrap/安全协议/生产者/消费者基准） |
| `README.md` | 本文件 |

## 快速接入步骤

1. **拷贝**整个目录到车端安全位置（示例：`/etc/hunter/kafka/`）；
2. **填 SCRAM 口令**：编辑 `kafka.properties` 中 `password="<SCRAM_PASSWORD>"` 一处，
   替换为开通向导一次性返回的口令（若丢失，联系运营在前端"重发 SCRAM 口令"）；
3. **导入 CA 到系统信任链**（可选，用于 `ssl.truststore`）：
   ```bash
   keytool -importcert -alias hunter-ca \\
     -file ca-cert.pem \\
     -keystore truststore.jks -storepass <任意口令>
   ```
4. **启动 Kafka Producer**：指向 `kafka.properties`，Topic = `hunter.{vehicle_id}.telemetry` 等；
5. **联调验证**：平台侧运营在前端"车辆管理 → 详情"看到该车 `last_online_time`
   更新即视为接入成功。

## Topic 清单（严格按契约 contracts/kafka/topics.yaml）

{topic_list}

## 安全注意

- **私钥与 p12 属敏感凭据**：仅通过本 ZIP 传输一次；车端存储权限 0600；
- 若私钥泄露：立即联系运营"重签证书"并回收旧 bundle；
- 若 SCRAM 口令泄露：立即"重发 SCRAM 口令"（旧口令即时失效）；
- 本 MVP 未实现 CRL/OCSP；下线仅通过删除 SCRAM 用户 + Topic + 证书目录完成。
"""


def _read_text(path: Path) -> bytes:
    if not path.is_file():
        raise ServiceUnavailableError(
            f"证书产物缺失: {path}",
            details={"path": str(path)},
        )
    return path.read_bytes()


def _resolve_bootstrap() -> str:
    """kafka.properties 中的 bootstrap：`settings.server_ip` 优先，回落 `127.0.0.1`。

    `server_ip` 未配置时（本地开发/未部署）返回占位；运营需配置 SERVER_IP 环境变量
    保证车端能反向解析到 broker。
    """
    host = settings.server_ip or "127.0.0.1"
    return f"{host}:{settings.kafka_external_port}"


def _topic_lines(vehicle_id: str) -> str:
    """从 cert_signer 依赖的 kafka_admin 处读取实际 Topic 清单（懒导入避免循环）。"""
    from app.services.kafka_admin import render_vehicle_topics  # noqa: PLC0415 - 懒导入避免循环

    lines = []
    for t in render_vehicle_topics(vehicle_id):
        lines.append(
            f"#   {t['name']}    partitions={t['partitions']}, retention={t['retention_ms'] / 86_400_000:.1f}d"
        )
    return "\n".join(lines)


def build_bundle_zip(vehicle_id: str) -> bytes:
    """组装该车辆的接入包（同步；调用方通过 asyncio.to_thread 移交线程池）。

    Raises:
        ServiceUnavailableError: 证书产物或 CA 文件缺失。
    """
    base = vehicle_dir(vehicle_id)
    key_path = base / "client-key.pem"
    cert_path = base / "client-cert.pem"
    p12_path = base / "kafka-client.p12"
    ca_cert = Path(settings.kafka_ca_cert_path)

    if not ca_cert.is_file():
        raise ServiceUnavailableError(
            f"CA 证书缺失：{ca_cert}",
        )
    p12_password = read_p12_password(vehicle_id) or ""
    topic_list = _topic_lines(vehicle_id)

    props = _KAFKA_PROPERTIES_TMPL.format(
        vehicle_id=vehicle_id,
        bootstrap=_resolve_bootstrap(),
        p12_password=p12_password or "<无（证书待重签）>",
        topic_list=topic_list,
    )
    readme = _README_TMPL.format(
        vehicle_id=vehicle_id,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        topic_list=topic_list,
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # 顶层目录 = vehicle_id，方便解压后自然分文件
        zf.writestr(f"{vehicle_id}/ca-cert.pem", _read_text(ca_cert))
        zf.writestr(f"{vehicle_id}/client-cert.pem", _read_text(cert_path))
        zf.writestr(f"{vehicle_id}/client-key.pem", _read_text(key_path))
        zf.writestr(f"{vehicle_id}/kafka-client.p12", _read_text(p12_path))
        zf.writestr(f"{vehicle_id}/kafka.properties", props.encode("utf-8"))
        zf.writestr(f"{vehicle_id}/README.md", readme.encode("utf-8"))
    logger.info("bundle_built", vehicle_id=vehicle_id, size_bytes=buf.tell())
    return buf.getvalue()


async def abuild_bundle_zip(vehicle_id: str) -> bytes:
    """异步包装（bundle 生成含磁盘 IO + zipfile 压缩，移交线程池避免阻塞事件循环）。"""
    import asyncio  # noqa: PLC0415 - 局部导入避免顶层依赖

    return await asyncio.to_thread(build_bundle_zip, vehicle_id)


__all__ = ["abuild_bundle_zip", "build_bundle_zip"]
