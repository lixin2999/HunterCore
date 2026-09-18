"""RSA-2048 签名验签（契约 x-hunter-ota-security.package_verification）。

- 算法：RSASSA-PKCS1-v1_5 + SHA-256（契约固定）；签名 base64；
- 签名载荷：升级包 SHA-256 小写十六进制字符串（UTF-8 编码的 64 字符摘要）。
  ⚠ 契约未定义签名载荷语义（私钥仅存发布方 CI/KMS），此处采用「对 SHA-256 摘要签名」
  的最小可实现约定，与车端验签规则须保持一致（潜在风险：需人工确认后回填契约）；
- 公钥：环境变量 OTA_SIGNATURE_PUBLIC_KEY（PEM；Secret 注入）；
- ``cryptography`` 缺失时抛 ServiceUnavailableError（5001），绝不静默跳过验签。
"""
from __future__ import annotations

import base64
import binascii

from hunter_common.exceptions import OtaSignatureError, ServiceUnavailableError
from hunter_common.logging import get_logger

logger = get_logger("app.core.signature")

_IMPORT_ERROR: ImportError | None = None
try:  # 延迟依赖：cryptography 仅在启用验签时必需
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except ImportError as exc:  # pragma: no cover - 环境缺失分支
    _IMPORT_ERROR = exc


def signature_payload(sha256_hex: str) -> bytes:
    """签名载荷：升级包 SHA-256 小写十六进制字符串（UTF-8）。"""
    return sha256_hex.encode("utf-8")


def verify_package_signature(public_key_pem: str, signature_b64: str, sha256_hex: str) -> bool:
    """RSA-2048 验签（RSASSA-PKCS1-v1_5 + SHA-256）。

    Args:
        public_key_pem: RSA 公钥 PEM 文本（OTA_SIGNATURE_PUBLIC_KEY）。
        signature_b64: base64 签名（发布方私钥离线生成）。
        sha256_hex: 升级包 SHA-256 小写十六进制（服务端流式计算所得）。

    Returns:
        验签通过返回 True；签名格式非法或验证失败返回 False（业务层映射 6002）。

    Raises:
        ServiceUnavailableError: 公钥未配置或 cryptography 组件不可用（5001，禁止静默跳过）。
    """
    if _IMPORT_ERROR is not None:
        raise ServiceUnavailableError(
            message="签名验签组件不可用（缺少 cryptography 依赖），发布已拒绝"
        ) from _IMPORT_ERROR
    if not public_key_pem:
        raise ServiceUnavailableError(
            message="OTA_SIGNATURE_PUBLIC_KEY 未配置，无法验签，发布已拒绝"
        )
    try:
        public_key = serialization.load_pem_public_key(public_key_pem.encode("utf-8"))
        signature = base64.b64decode(signature_b64, validate=True)
        public_key.verify(  # type: ignore[attr-defined]
            signature,
            signature_payload(sha256_hex),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return True
    except (InvalidSignature, ValueError, TypeError, binascii.Error):
        logger.warning("ota_signature_verify_failed", reason="invalid_signature")
        return False


__all__ = ["signature_payload", "verify_package_signature"]
