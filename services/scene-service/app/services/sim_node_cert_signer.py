"""仿真节点 mTLS 客户端证书签发（openssl 子进程 + CA 私钥挂载只读）。

V1.24.1 仿真端管理功能：为节点签发两种证书：
1. **Kafka 客户端证书**：CN = `sim-{node_id}`，用于节点以 mTLS 连接 Kafka 集群；
2. **平台访问证书**：CN = `sim-platform-{node_id}`，用于节点以 mTLS 访问平台 HTTP API。

设计沿用 vehicle-service/app/services/cert_signer.py 的成熟模式：
- 目录结构：`{SIM_NODE_CERTS_DIR}/{node_id}/{kafka|platform}/`
- 产物：client-key.pem、client-cert.pem；Kafka 子目录额外有 kafka-client.p12 + p12-password
- 序列号自管：`secrets.randbits(64) | 0x01`（奇数非零，无需写 serial 文件）
- CA 只读：由 compose 以 `:ro` 挂载；不写 CA 状态文件
- 失败不留残迹：任一步骤异常时清理本次产物
- openssl 子进程超时：`settings.sim_node_cert_signer_timeout_seconds`（默认 15s）
- 异步入口：`asyncio.to_thread` 移交线程池，不阻塞事件循环
"""
from __future__ import annotations

import asyncio
import secrets
import subprocess
from dataclasses import dataclass
from pathlib import Path

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import settings

logger = get_logger("app.services.sim_node_cert_signer")

_SERIAL_BYTES = 8  # 64-bit 随机 serial


@dataclass(frozen=True, slots=True)
class NodeCertArtifacts:
    """签发产物（路径相对于 `{SIM_NODE_CERTS_DIR}/{node_id}/{cert_type}/`）。"""

    node_id: str
    cert_type: str  # "kafka" | "platform"
    cert_path: Path
    key_path: Path
    p12_path: Path | None  # 仅 Kafka 证书有 PKCS12
    p12_password: str | None
    serial_hex: str


# =====================================================================
# 目录与文件命名
# =====================================================================

def node_dir(node_id: str) -> Path:
    """该节点的证书根目录（不含创建副作用）。"""
    return Path(settings.sim_node_certs_dir) / node_id


def _paths(node_id: str, cert_type: str) -> dict[str, Path]:
    base = node_dir(node_id) / cert_type
    paths: dict[str, Path] = {
        "dir": base,
        "key": base / "client-key.pem",
        "csr": base / "client-csr.pem",
        "cert": base / "client-cert.pem",
        "ext": base / "san.ext",
    }
    if cert_type == "kafka":
        paths["p12"] = base / "kafka-client.p12"
        paths["p12_pass"] = base / "p12-password"
    return paths


def _common_name(node_id: str, cert_type: str) -> str:
    if cert_type == "kafka":
        return f"sim-{node_id}"
    return f"sim-platform-{node_id}"


# =====================================================================
# openssl 子进程调用
# =====================================================================

def _run(cmd: list[str], *, timeout: int | None = None) -> str:
    """执行 openssl 命令；失败映射 5001 ServiceUnavailable。"""
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout or settings.sim_node_cert_signer_timeout_seconds,
        )
    except FileNotFoundError as exc:
        logger.error("openssl_binary_missing", cmd=cmd[0])
        raise ServiceUnavailableError(
            "openssl 未安装或不在 PATH（scene-service 镜像需包含 openssl）",
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()[:400]
        logger.error(
            "openssl_command_failed",
            stage=cmd[1] if len(cmd) > 1 else "?",
            returncode=exc.returncode,
            stderr=stderr,
        )
        raise ServiceUnavailableError(
            f"openssl {cmd[1] if len(cmd) > 1 else '?'} 失败: {stderr or '未知错误'}",
        ) from exc
    except subprocess.TimeoutExpired as exc:
        logger.error("openssl_command_timeout", timeout=exc.timeout)
        raise ServiceUnavailableError(
            f"openssl 执行超时（{exc.timeout}s）",
        ) from exc
    return result.stdout


def _assert_ca_available() -> None:
    ca_cert = Path(settings.kafka_ca_cert_path)
    ca_key = Path(settings.kafka_ca_key_path)
    if not (ca_cert.is_file() and ca_key.is_file()):
        raise ServiceUnavailableError(
            "CA 证书/私钥缺失，无法签发仿真节点证书",
            details={"ca_cert": str(ca_cert), "ca_key": str(ca_key)},
        )


# =====================================================================
# 主流程：同步签发（供 to_thread 调用）
# =====================================================================

def issue_cert_sync(node_id: str, cert_type: str) -> NodeCertArtifacts:
    """签发指定类型证书并生成 PKCS12（Kafka 类型）；覆盖同名产物。

    cert_type: "kafka" | "platform"
    步骤：genrsa → SAN 扩展 → CSR → CA 签发 → （kafka: pkcs12）
    """
    if cert_type not in ("kafka", "platform"):
        raise ValueError(f"cert_type 必须为 kafka|platform，当前: {cert_type!r}")

    _assert_ca_available()
    p = _paths(node_id, cert_type)
    p["dir"].mkdir(parents=True, exist_ok=True)
    _safe_chmod(p["dir"], 0o750)

    cn = _common_name(node_id, cert_type)
    serial_int = secrets.randbits(_SERIAL_BYTES * 8) | 0x01
    serial_hex = f"{serial_int:X}"
    p12_password = secrets.token_urlsafe(16) if cert_type == "kafka" else None

    try:
        # 1) RSA 2048 私钥
        _run(["openssl", "genrsa", "-out", str(p["key"]), "2048"])
        _safe_chmod(p["key"], 0o600)

        # 2) SAN 扩展文件
        p["ext"].write_text(
            f"subjectAltName=DNS:{cn}\n"
            "keyUsage=digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=clientAuth\n",
            encoding="utf-8",
        )
        _safe_chmod(p["ext"], 0o600)

        # 3) CSR（/O=HunterCore/CN={cn}）
        _run([
            "openssl", "req", "-new",
            "-key", str(p["key"]),
            "-subj", f"/O=HunterCore/CN={cn}",
            "-out", str(p["csr"]),
        ])
        _safe_chmod(p["csr"], 0o600)

        # 4) CA 签发
        _run([
            "openssl", "x509", "-req",
            "-in", str(p["csr"]),
            "-CA", settings.kafka_ca_cert_path,
            "-CAkey", settings.kafka_ca_key_path,
            "-outform", "PEM",
            "-out", str(p["cert"]),
            "-days", str(settings.sim_node_cert_validity_days),
            "-sha256",
            "-set_serial", f"0x{serial_hex}",
            "-extfile", str(p["ext"]),
        ])
        _safe_chmod(p["cert"], 0o640)

        # 5) PKCS12（仅 Kafka 类型）
        p12_path: Path | None = None
        if cert_type == "kafka" and p12_password is not None:
            _run([
                "openssl", "pkcs12", "-export",
                "-inkey", str(p["key"]),
                "-in", str(p["cert"]),
                "-certfile", settings.kafka_ca_cert_path,
                "-name", cn,
                "-out", str(p["p12"]),
                "-passout", f"pass:{p12_password}",
            ])
            p12_path = p["p12"]
            _safe_chmod(p12_path, 0o600)
            p["p12_pass"].write_text(p12_password, encoding="utf-8")
            _safe_chmod(p["p12_pass"], 0o600)

        # 清理中间文件
        _silent_unlink(p["csr"])
        _silent_unlink(p["ext"])

    except ServiceUnavailableError:
        _cleanup_partial(node_id, cert_type)
        raise

    logger.info(
        "sim_node_cert_issued",
        node_id=node_id,
        cert_type=cert_type,
        serial=serial_hex,
        validity_days=settings.sim_node_cert_validity_days,
    )
    return NodeCertArtifacts(
        node_id=node_id,
        cert_type=cert_type,
        cert_path=p["cert"],
        key_path=p["key"],
        p12_path=p12_path,
        p12_password=p12_password,
        serial_hex=serial_hex,
    )


async def aissue_kafka_cert(node_id: str) -> NodeCertArtifacts:
    """异步入口：签发 Kafka 客户端证书。"""
    return await asyncio.to_thread(issue_cert_sync, node_id, "kafka")


async def aissue_platform_cert(node_id: str) -> NodeCertArtifacts:
    """异步入口：签发平台访问证书。"""
    return await asyncio.to_thread(issue_cert_sync, node_id, "platform")


def read_p12_password(node_id: str) -> str | None:
    """读取 Kafka 证书的 PKCS12 口令（bundle 打包用）。"""
    path = _paths(node_id, "kafka").get("p12_pass")
    if path is None or not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except OSError:
        logger.exception("sim_node_p12_password_read_failed", node_id=node_id)
        return None


def cert_paths_ready(node_id: str, cert_type: str) -> bool:
    """前置检查：key/cert 产物是否齐全（Kafka 另加 p12+p12_pass）。"""
    p = _paths(node_id, cert_type)
    base_ok = p["key"].is_file() and p["cert"].is_file()
    if cert_type == "kafka":
        return base_ok and p["p12"].is_file() and p["p12_pass"].is_file()
    return base_ok


def delete_node_cert_dir(node_id: str) -> bool:
    """删除该节点整个证书目录（下线时清理）。"""
    base = node_dir(node_id)
    if not base.exists():
        return False
    for cert_type in ("kafka", "platform"):
        d = base / cert_type
        if d.is_dir():
            for item in d.iterdir():
                if item.is_file():
                    _silent_unlink(item)
            try:
                d.rmdir()
            except OSError:
                logger.warning("sim_node_cert_dir_nonempty", path=str(d))
    try:
        base.rmdir()
    except OSError:
        logger.warning("sim_node_cert_root_nonempty", path=str(base))
        return False
    logger.info("sim_node_cert_dir_deleted", node_id=node_id)
    return True


# =====================================================================
# 内部工具
# =====================================================================

def _safe_chmod(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except OSError:
        logger.warning("chmod_failed", path=str(path), mode=oct(mode))


def _silent_unlink(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning("unlink_failed", path=str(path))


def _cleanup_partial(node_id: str, cert_type: str) -> None:
    p = _paths(node_id, cert_type)
    for key in ("key", "csr", "cert", "ext", "p12", "p12_pass"):
        _silent_unlink(p.get(key))


__all__ = [
    "NodeCertArtifacts",
    "aissue_kafka_cert",
    "aissue_platform_cert",
    "cert_paths_ready",
    "delete_node_cert_dir",
    "issue_cert_sync",
    "node_dir",
    "read_p12_password",
]
