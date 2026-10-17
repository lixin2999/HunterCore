"""车端 REST 设备认证（契约 api-gateway ``x-hunter-device-ingress``；data-collector pending #6 决议）。

信任模型（V1.21.13）：mTLS 反代（单机 web-portal nginx :8443 / K8s 独立 Ingress）校验客户端
证书后经 ``X-Client-Cert`` 转发 URL 编码 PEM（``$ssl_client_escaped_cert``）。网关**不可直接
信任该头**——:8080 本身对外，绕过反代直连即可伪造头——因此对转发的 PEM 用同一 CA 本地重验：

1. 仅契约 ``device_paths`` 白名单路径（presign/complete）进入设备分支，其余路径一律走控制台 JWT；
2. ``DEVICE_CA_CERTFILE`` 未配置 → 设备通道整体禁用（fail-closed，带证请求 → 1001）；
3. 证书链重验：CA 直接签发校验（``verify_directly_issued_by``）→ 有效期 → clientAuth EKU →
   CN 非空（= vehicle_id）；任一失败 → 1001（统一错误语义，不透出证书细节）。

通过校验后构造设备 claims（``sub=device:{vehicle_id}``、``roles=[device]``、``vehicle_id``），
由转发层注入身份头（X-Device-Vehicle-Id 纳入 HMAC 签名第 5 段，防集群内冒充他车）。

⚠ 日志脱敏硬约束：证书 PEM / CN 之外的证书内容禁止写入日志（本模块仅记录事件名与失败种类）。
"""
from __future__ import annotations

import base64
import binascii
import os
import re
import threading
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from fastapi import Request
from hunter_common.exceptions import AuthenticationError
from hunter_common.logging import get_logger

from app.config import Settings, settings

logger = get_logger("app.core.device_auth")

#: 反代转发的客户端证书头（URL 编码 PEM；nginx $ssl_client_escaped_cert / Ingress ssl-client-cert）
HEADER_CLIENT_CERT = "X-Client-Cert"

#: 契约 x-hunter-device-ingress.device_paths —— 设备主体唯一可达的业务路径（POST 方法在转发前不区分，
#: 后端路由方法天然收敛；非白名单路径携带证书头 → 忽略并回落 JWT）
DEVICE_ALLOWED_PATHS: frozenset[str] = frozenset(
    {
        "/api/v1/data/files/presign",
        "/api/v1/data/files/complete",
    }
)

_PEM_BODY = re.compile(r"-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----", re.DOTALL)

__all__ = ["DEVICE_ALLOWED_PATHS", "HEADER_CLIENT_CERT", "authenticate_device_request"]

# ---------- CA Bundle 加载缓存（路径 + mtime 命中即复用；CA 轮换文件替换自然失效） ----------
_ca_lock = threading.Lock()
_ca_cache: tuple[str, float, x509.Certificate] | None = None


def _parse_cert(content: bytes) -> x509.Certificate:
    """PEM/含换行破坏的证书文本 → 证书对象（提取 BEGIN/END 间的 base64，剔除全部空白后 DER 解析）。

    反代头对 PEM 的换行/URL 编码处理不一（%0D%0A、空格折叠），逐字节规范 PEM 不可依赖，
    故按 base64 主体重组解析——这是该场景下的通行稳健做法。
    """
    text = content.decode("utf-8", errors="replace")
    match = _PEM_BODY.search(text)
    if match is None:
        raise ValueError("missing PEM certificate body")
    der = base64.b64decode(re.sub(r"\s+", "", match.group(1)), validate=True)
    return x509.load_der_x509_certificate(der)


def _load_ca(certfile: str) -> x509.Certificate | None:
    """加载（并缓存）设备 CA；文件缺失/不可解析返回 None（调用侧按通道故障拒绝）。"""
    global _ca_cache
    try:
        mtime = os.path.getmtime(certfile)
    except OSError:
        return None
    with _ca_lock:
        if _ca_cache is not None and _ca_cache[0] == certfile and _ca_cache[1] == mtime:
            return _ca_cache[2]
        try:
            with open(certfile, "rb") as fh:
                ca = _parse_cert(fh.read())
        except (OSError, ValueError, binascii.Error) as exc:
            logger.error("device_ca_load_failed", error=type(exc).__name__)
            return None
        _ca_cache = (certfile, mtime, ca)
        return ca


def reset_ca_cache() -> None:
    """清空 CA 缓存（测试辅助；生产 CA 轮换经 mtime 失效自动重载）。"""
    global _ca_cache
    with _ca_lock:
        _ca_cache = None


def _device_rejected(reason: str, path: str) -> AuthenticationError:
    """统一拒绝语义（1001）；日志仅记失败种类与路径，禁止落证书内容。"""
    logger.warning("device_auth_rejected", reason=reason, path=path)
    return AuthenticationError(message=f"未认证：设备证书校验失败（{reason}）")


def authenticate_device_request(
    request: Request, cfg: Settings | None = None
) -> dict[str, Any] | None:
    """设备认证分支入口（catch-all 代理在路由匹配后、JWT 认证前调用）。

    返回设备 claims（``type=device``）；非设备请求（白名单外路径 / 无证书头）返回 ``None``
    → 调用方回落控制台 JWT 路线；设备请求校验失败一律抛 1001（不回落，语义明确利于排障）。
    """
    cfg = cfg or settings
    path = request.url.path
    if path not in DEVICE_ALLOWED_PATHS:
        return None
    raw_cert = request.headers.get(HEADER_CLIENT_CERT.lower()) or request.headers.get(HEADER_CLIENT_CERT)
    if raw_cert is None:
        return None  # 无证书头：控制台经网关直连的代传路线（JWT）
    if not cfg.device_ca_certfile:
        # 设备通道禁用仍收到带证请求：多为反代已上、网关未配 CA（或旧版镜像）——显式拒绝而非回落，
        # 避免运维将"证书被忽略"误判为"证书有效但后端拒绝"
        raise _device_rejected("device_channel_disabled", path)
    ca = _load_ca(cfg.device_ca_certfile)
    if ca is None:
        raise _device_rejected("ca_unavailable", path)
    try:
        cert = _parse_cert(unquote(raw_cert).encode("utf-8", errors="replace"))
    except (ValueError, binascii.Error, OSError) as exc:
        raise _device_rejected("cert_unparsable", path) from exc
    try:
        cert.verify_directly_issued_by(ca)
    except (InvalidSignature, ValueError, TypeError) as exc:
        # 非本 CA 签发 / 签名被篡改：无 CA 私钥无法伪造，重验即拒绝
        raise _device_rejected("signature_invalid", path) from exc
    now = datetime.now(UTC)
    if cert.not_valid_before_utc > now or cert.not_valid_after_utc < now:
        raise _device_rejected("cert_expired", path)
    try:
        eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
    except x509.ExtensionNotFound as exc:
        raise _device_rejected("missing_client_auth_eku", path) from exc
    if ExtendedKeyUsageOID.CLIENT_AUTH not in eku.value:
        raise _device_rejected("missing_client_auth_eku", path)
    cn_attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    vehicle_id = str(cn_attrs[0].value).strip() if cn_attrs else ""
    if not vehicle_id:
        raise _device_rejected("missing_common_name", path)
    logger.info("device_auth_ok", vehicle_id=vehicle_id, path=path)
    return {
        "sub": f"device:{vehicle_id}",
        "roles": [cfg.device_role],
        "type": "device",
        "vehicle_id": vehicle_id,
    }
