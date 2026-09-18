"""RSA-2048 签名验签单元测试（契约 x-hunter-ota-security.package_verification）。

``cryptography`` 未安装时整体跳过（pyproject 已声明依赖，CI 环境应可用）。
签名载荷语义（SHA-256 摘要 hex 字符串）为契约缺失项的实现约定，见 app/core/signature.py。
"""
from __future__ import annotations

import base64
import hashlib

import pytest

pytest.importorskip("cryptography")

from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import padding  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402

from app.core.signature import signature_payload, verify_package_signature  # noqa: E402
from hunter_common.exceptions import ServiceUnavailableError  # noqa: E402

PACKAGE_BYTES = b"hunter-ota-package-content"
SHA256_HEX = hashlib.sha256(PACKAGE_BYTES).hexdigest()


def _keypair_and_signature() -> tuple[str, str]:
    """生成测试密钥对并对 SHA-256 摘要签名（模拟发布方 CI/KMS 离线签名）。"""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signature = private_key.sign(
        signature_payload(SHA256_HEX), padding.PKCS1v15(), hashes.SHA256()
    )
    public_pem = (
        private_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("utf-8")
    )
    return public_pem, base64.b64encode(signature).decode("utf-8")


def test_verify_ok() -> None:
    """正确签名 + 一致摘要 → 验签通过。"""
    public_pem, signature_b64 = _keypair_and_signature()
    assert verify_package_signature(public_pem, signature_b64, SHA256_HEX) is True


def test_verify_tampered_package_fails() -> None:
    """包内容被篡改（摘要不一致）→ 验签失败（业务层映射 6002）。"""
    public_pem, signature_b64 = _keypair_and_signature()
    other_hex = hashlib.sha256(b"tampered").hexdigest()
    assert verify_package_signature(public_pem, signature_b64, other_hex) is False


def test_verify_garbage_signature_fails() -> None:
    """非法 base64/垃圾签名 → 返回 False（不抛异常）。"""
    public_pem, _ = _keypair_and_signature()
    assert verify_package_signature(public_pem, "not-base64!!", SHA256_HEX) is False


def test_missing_public_key_rejects_publish() -> None:
    """公钥未配置 → 5001（禁止静默跳过验签，安全基线）。"""
    with pytest.raises(ServiceUnavailableError):
        verify_package_signature("", "AAAA", SHA256_HEX)
