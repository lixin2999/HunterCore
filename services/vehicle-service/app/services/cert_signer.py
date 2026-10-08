"""客户端证书签发（openssl 子进程 + CA 私钥挂载只读）。

契约依据：contracts/openapi/vehicle-service.yaml `x-hunter-cert-policy`。

设计要点：
- **每车一证**（严格 mTLS，G-01）：CN = vehicle_id，SAN = DNS:{vehicle_id}，O = HunterCore；
- **CA 私钥只读**：由 compose 以 :ro 挂载 + setfacl 授权 gid 10001；签发过程只读，
  不写 CA 状态文件；
- **序列号自管**：`openssl x509 -req -set_serial` 使用 Python 端 `secrets.randbits(64)`
  生成随机 serial（避免依赖 `-CAcreateserial` 需要写 serial 文件）；
- **PKCS12**：`openssl pkcs12 -export -passout` 每车随机口令（url-safe 16 字节），
  口令写入 `{vehicle_id}/p12-password`（0600），bundle 打包时同步进 kafka.properties 首行注释；
- **文件权限**：key/p12 = 0600；cert = 0640；password = 0600；
- **失败不留残迹**：任一步骤异常时回收本次已生成的中间文件（保留其它车辆产物）；
- **重签语义**：`issue_client_cert` 覆盖同名产物；`delete_cert_dir` 下线时清理。

openssl 子进程超时由 `settings.cert_signer_timeout_seconds` 兜底（默认 15s）；
所有子进程调用通过 `asyncio.to_thread` 移交线程池，避免阻塞事件循环。
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

logger = get_logger("app.services.cert_signer")

#: openssl 输出中的证书序列号（大写十六进制）解析辅助（本模块仅记录，实际以本地生成值为准）
_SERIAL_BYTES = 8  # 64-bit 随机 serial（避免与 CA 历史 serial 冲突；足够唯一）


@dataclass(frozen=True, slots=True)
class CertArtifacts:
    """签发产物（路径为 `{VEHICLE_CERTS_DIR}/{vehicle_id}/` 下固定文件名）。"""

    vehicle_id: str
    cert_path: Path
    key_path: Path
    p12_path: Path
    p12_password: str
    serial_hex: str  # 大写十六进制，供 device_cert_sn 落库


# =====================================================================
# 目录与文件命名（单一事实来源）
# =====================================================================
def vehicle_dir(vehicle_id: str) -> Path:
    """该车辆证书目录（不含创建副作用）。"""
    return Path(settings.vehicle_certs_dir) / vehicle_id


def _paths(vehicle_id: str) -> dict[str, Path]:
    base = vehicle_dir(vehicle_id)
    return {
        "dir": base,
        "key": base / "client-key.pem",
        "csr": base / "client-csr.pem",
        "cert": base / "client-cert.pem",
        "ext": base / "san.ext",
        "p12": base / "kafka-client.p12",
        "p12_pass": base / "p12-password",
    }


# =====================================================================
# openssl 子进程调用（同步；上层用 to_thread 包装）
# =====================================================================
def _run(cmd: list[str], *, timeout: int | None = None) -> str:
    """执行 openssl 命令；失败一律映射为 5001 ServiceUnavailable（依赖外部工具异常）。

    安全：不 echo 命令内容到日志（可能含 p12 口令）；stdout/stderr 仅在异常时截断入日志。
    """
    try:
        result = subprocess.run(  # noqa: S603 - 参数由本模块构造，不接受外部输入
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout or settings.cert_signer_timeout_seconds,
        )
    except FileNotFoundError as exc:
        logger.error("openssl_binary_missing", cmd=cmd[0])
        raise ServiceUnavailableError(
            "openssl 未安装或不在 PATH（镜像需包含 openssl 客户端）",
        ) from exc
    except subprocess.CalledProcessError as exc:
        # 只截取 stderr 前 400 字符（避免超长堆栈入日志）
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
    """CA 证书与私钥均存在可读（provisioning 前的 fail-fast 检查）。"""
    ca_cert = Path(settings.kafka_ca_cert_path)
    ca_key = Path(settings.kafka_ca_key_path)
    if not (ca_cert.is_file() and ca_key.is_file()):
        raise ServiceUnavailableError(
            "CA 证书/私钥缺失，无法签发客户端证书",
            details={
                "ca_cert": str(ca_cert),
                "ca_key": str(ca_key),
            },
        )


# =====================================================================
# 主流程：签发（同步入口，供 to_thread 或 provisioner 直接调用）
# =====================================================================
def issue_client_cert_sync(vehicle_id: str) -> CertArtifacts:
    """签发该车辆的新证书并生成 PKCS12；覆盖同名产物。

    步骤（每步都是独立 openssl 子进程）：
      1. `genpkey`/`genrsa` 生成 2048-bit RSA 私钥（车端 AGX Orin 兼容基线）
      2. 构造 SAN 扩展文件
      3. `req -new` 生成 CSR（Subject = `/O=HunterCore/CN=<vehicle_id>`）
      4. `x509 -req -CA ... -set_serial <随机>` 由 CA 签发（`-days` 来自配置）
      5. `pkcs12 -export` 打包 key+cert+CA（口令随机、每车独立）
    """
    _assert_ca_available()
    p = _paths(vehicle_id)
    p["dir"].mkdir(parents=True, exist_ok=True)
    # 权限：目录 0750（仅 vehicle-service gid 遍历）
    _safe_chmod(p["dir"], 0o750)

    serial_int = secrets.randbits(_SERIAL_BYTES * 8) | 0x01  # 保证奇数、非零
    serial_hex = f"{serial_int:X}"
    p12_password = secrets.token_urlsafe(16)

    try:
        # 1) 客户端私钥（RSA 2048）
        _run(["openssl", "genrsa", "-out", str(p["key"]), "2048"])
        _safe_chmod(p["key"], 0o600)

        # 2) SAN 扩展文件（DNS:<vehicle_id>；openssl x509 -req -extfile 使用）
        p["ext"].write_text(
            f"subjectAltName=DNS:{vehicle_id}\n"
            "keyUsage=digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=clientAuth\n",
            encoding="utf-8",
        )
        _safe_chmod(p["ext"], 0o600)

        # 3) CSR（Subject 顺序：O → CN；与设计文档"CN=<vehicle_id>, O=HunterCore"对应）
        subject = f"/O=HunterCore/CN={vehicle_id}"
        _run(
            [
                "openssl",
                "req",
                "-new",
                "-key",
                str(p["key"]),
                "-subj",
                subject,
                "-out",
                str(p["csr"]),
            ]
        )
        _safe_chmod(p["csr"], 0o600)

        # 4) CA 签发（-set_serial 免依赖 serial 文件；-days 与 CA 同寿）
        _run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(p["csr"]),
                "-ca",
                settings.kafka_ca_cert_path,
                "-cakey",
                settings.kafka_ca_key_path,
                "-CAcreateserial",
                "-outform",
                "PEM",
                "-out",
                str(p["cert"]),
                "-days",
                str(settings.vehicle_cert_validity_days),
                "-sha256",
                "-set_serial",
                f"0x{serial_hex}",
                "-extfile",
                str(p["ext"]),
            ]
        )
        _safe_chmod(p["cert"], 0o640)

        # 5) PKCS12（key + cert + CA chain）
        _run(
            [
                "openssl",
                "pkcs12",
                "-export",
                "-inkey",
                str(p["key"]),
                "-in",
                str(p["cert"]),
                "-certfile",
                settings.kafka_ca_cert_path,
                "-name",
                vehicle_id,
                "-out",
                str(p["p12"]),
                "-passout",
                f"pass:{p12_password}",
            ]
        )
        _safe_chmod(p["p12"], 0o600)

        # 6) 落盘 p12 口令（bundle 生成时读取；下线随目录一并清理）
        p["p12_pass"].write_text(p12_password, encoding="utf-8")
        _safe_chmod(p["p12_pass"], 0o600)

        # 清理中间 CSR / SAN ext 文件（保留 key/cert/p12/p12-password 四份产物）
        _silent_unlink(p["csr"])
        _silent_unlink(p["ext"])
    except ServiceUnavailableError:
        # 任一步失败 → 清理本次产物（不影响其它车辆），向上抛让 provisioner 回滚
        _cleanup_partial(vehicle_id)
        raise

    logger.info(
        "cert_issued",
        vehicle_id=vehicle_id,
        serial=serial_hex,
        validity_days=settings.vehicle_cert_validity_days,
    )
    return CertArtifacts(
        vehicle_id=vehicle_id,
        cert_path=p["cert"],
        key_path=p["key"],
        p12_path=p["p12"],
        p12_password=p12_password,
        serial_hex=serial_hex,
    )


async def issue_client_cert(vehicle_id: str) -> CertArtifacts:
    """异步入口：openssl 子进程移交线程池（事件循环不阻塞）。"""
    return await asyncio.to_thread(issue_client_cert_sync, vehicle_id)


def read_p12_password(vehicle_id: str) -> str | None:
    """读取已签发车辆的 PKCS12 口令（bundle 打包用）；文件缺失返回 None。"""
    path = _paths(vehicle_id)["p12_pass"]
    if not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except OSError:
        logger.exception("p12_password_read_failed", vehicle_id=vehicle_id)
        return None


def cert_paths_ready(vehicle_id: str) -> bool:
    """bundle 前置检查：key/cert/p12/p12-password 四份产物是否齐全。"""
    p = _paths(vehicle_id)
    return all(path.is_file() for path in (p["key"], p["cert"], p["p12"], p["p12_pass"]))


def delete_cert_dir(vehicle_id: str) -> bool:
    """删除该车辆证书目录（下线/回滚）；不存在视为幂等成功。"""
    base = vehicle_dir(vehicle_id)
    if not base.exists():
        return False
    # 目录内含 4 份产物 + 可能的中间文件；逐文件 unlink 再 rmdir（安全：不使用 rm -rf）
    for item in base.iterdir():
        if item.is_file():
            _silent_unlink(item)
    try:
        base.rmdir()
    except OSError:
        # 目录可能非空（罕见：外部工具写入）；交由运维处理，不阻断下线
        logger.warning("cert_dir_nonempty", path=str(base))
        return False
    logger.info("cert_dir_deleted", vehicle_id=vehicle_id)
    return True


async def aissue_client_cert(vehicle_id: str) -> CertArtifacts:
    """别名：与 ``issue_client_cert`` 同语义（供 provisioner 阅读时更清晰）。"""
    return await issue_client_cert(vehicle_id)


# =====================================================================
# 内部工具
# =====================================================================
def _safe_chmod(path: Path, mode: int) -> None:
    """chmod 失败仅告警（Windows/SELinux 场景）；不影响业务流程。"""
    try:
        path.chmod(mode)
    except OSError:
        logger.warning("chmod_failed", path=str(path), mode=oct(mode))


def _silent_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning("unlink_failed", path=str(path))


def _cleanup_partial(vehicle_id: str) -> None:
    """签发中途失败时清理本次产物（不影响其它车辆）。"""
    p = _paths(vehicle_id)
    for key in ("key", "csr", "cert", "ext", "p12", "p12_pass"):
        _silent_unlink(p[key])
    # 目录留空即可（下次 issue 会重建）；不 rmdir，避免与并发操作竞态


__all__ = [
    "CertArtifacts",
    "aissue_client_cert",
    "cert_paths_ready",
    "delete_cert_dir",
    "issue_client_cert",
    "issue_client_cert_sync",
    "read_p12_password",
    "vehicle_dir",
]
