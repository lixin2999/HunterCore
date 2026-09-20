"""生产配置硬闸测试（G-05：发布校验开关 staging/prod 禁止降级）。"""
from __future__ import annotations

import pytest

from app.config import Settings


def _prod_overrides(**extra: object) -> dict[str, object]:
    """通过基类凭据强校验（Y2）所需的最小安全集。"""
    base: dict[str, object] = {
        "environment": "prod",
        "jwt_secret_key": "X" * 48,
        "postgres_password": "s3cure-pg-pass",
        "minio_secret_key": "s3cure-minio-pass",
        "gateway_hmac_secret": "Y" * 48,
        "ota_signature_public_key": "-----BEGIN PUBLIC KEY-----\nMIIB...\n-----END PUBLIC KEY-----",
    }
    base.update(extra)
    return base


@pytest.mark.parametrize("environment", ["staging", "prod"])
def test_production_rejects_disabled_verification_switches(environment: str) -> None:
    """关闭 SHA-256 / 签名校验 → 启动即失败（禁止恶意/篡改包直推车端）。"""
    with pytest.raises(ValueError, match="禁止降级升级包校验"):
        Settings(**_prod_overrides(environment=environment, ota_package_verify_sha256=False))
    with pytest.raises(ValueError, match="禁止降级升级包校验"):
        Settings(**_prod_overrides(environment=environment, ota_package_verify_signature=False))


@pytest.mark.parametrize("environment", ["staging", "prod"])
def test_production_requires_signature_public_key(environment: str) -> None:
    """验签公钥未注入 → 启动即失败（开关为 true 但无钥可用等同未校验）。"""
    with pytest.raises(ValueError, match="OTA_SIGNATURE_PUBLIC_KEY"):
        Settings(**_prod_overrides(environment=environment, ota_signature_public_key="  "))


def test_production_accepts_full_verification_configuration() -> None:
    settings = Settings(**_prod_overrides())
    assert settings.ota_package_verify_sha256 and settings.ota_package_verify_signature


def test_dev_keeps_verification_switches_adjustable() -> None:
    """dev/test 保留联调能力（历史行为不变）。"""
    settings = Settings(environment="dev", ota_package_verify_sha256=False, ota_package_verify_signature=False)
    assert not settings.ota_package_verify_sha256
    assert not settings.ota_package_verify_signature
