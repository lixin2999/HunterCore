"""hunter_common.config 单元测试。"""
from __future__ import annotations

import pytest

from hunter_common.config import HunterBaseConfig


class DemoSettings(HunterBaseConfig):
    """测试用配置子类。"""


def test_database_url_format() -> None:
    settings = DemoSettings(
        postgres_host="db", postgres_port=5432,
        postgres_user="u", postgres_password="p", postgres_db="d",
    )
    assert settings.database_url == "postgresql+asyncpg://u:p@db:5432/d"


def test_pool_size_default_follows_cpu_rule() -> None:
    """连接池默认 = CPU 核数 × 2 + 1（开发规则）。"""
    import os

    settings = DemoSettings()
    assert settings.db_pool_size == (os.cpu_count() or 2) * 2 + 1


def test_redis_url_without_password() -> None:
    settings = DemoSettings(redis_host="r", redis_port=6380, redis_db=2)
    assert settings.redis_url == "redis://r:6380/2"


def test_redis_url_with_password() -> None:
    settings = DemoSettings(redis_password="pw")
    assert settings.redis_url == "redis://:pw@localhost:6379/0"


def test_kafka_servers_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "k1:9092,k2:9092")
    settings = DemoSettings()
    assert settings.kafka_bootstrap_servers_list == ["k1:9092", "k2:9092"]


def test_cors_origins_list() -> None:
    settings = DemoSettings(cors_origins="http://a.com, http://b.com")
    assert settings.cors_origins_list == ["http://a.com", "http://b.com"]


# ---------------------------------------------------------------------------
# MinIO 双 endpoint（服务侧连接 vs 预签名 URL host，车端可达地址分离）
# ---------------------------------------------------------------------------
def test_minio_endpoint_url_scheme_completion() -> None:
    """共享配置下发 host:port，直传 boto3 前需补全 scheme（依 minio_secure）。"""
    plain = DemoSettings(minio_endpoint="minio:9000", minio_secure=False)
    assert plain.minio_endpoint_url == "http://minio:9000"
    tls = DemoSettings(minio_endpoint="minio:9000", minio_secure=True)
    assert tls.minio_endpoint_url == "https://minio:9000"
    already = DemoSettings(minio_endpoint="http://minio:9000")
    assert already.minio_endpoint_url == "http://minio:9000"


def test_minio_public_endpoint_falls_back_to_internal() -> None:
    """未配置/空白 MINIO_PUBLIC_ENDPOINT → 预签名 host 回落内部地址（单机同网段兼容）。"""
    unset = DemoSettings(minio_endpoint="minio:9000")
    assert unset.minio_public_endpoint_url == "http://minio:9000"
    blank = DemoSettings(minio_endpoint="minio:9000", minio_public_endpoint="")
    assert blank.minio_public_endpoint_url == "http://minio:9000"


def test_minio_public_endpoint_used_for_presign_url() -> None:
    """配置后预签名 host 与内部连接地址分离；scheme 补全规则与内部一致。"""
    settings = DemoSettings(
        minio_endpoint="minio:9000",
        minio_public_endpoint="203.0.113.10:9000",
        minio_secure=False,
    )
    assert settings.minio_endpoint_url == "http://minio:9000"  # 服务侧不变
    assert settings.minio_public_endpoint_url == "http://203.0.113.10:9000"
    tls = DemoSettings(
        minio_endpoint="minio:9000",
        minio_public_endpoint="minio.example.com",
        minio_secure=True,
    )
    assert tls.minio_public_endpoint_url == "https://minio.example.com"
    explicit = DemoSettings(
        minio_endpoint="minio:9000",
        minio_public_endpoint="https://minio.example.com:443",
        minio_secure=True,
    )
    assert explicit.minio_public_endpoint_url == "https://minio.example.com:443"  # 已含 scheme 原样


def test_jwt_token_ttl_matches_spec() -> None:
    """Access Token 30min（G-04 决策①收紧，原设计文档 2h）/ Refresh Token 7d。"""
    settings = DemoSettings()
    assert settings.jwt_access_token_expire_minutes == 30
    assert settings.jwt_refresh_token_expire_days == 7


# ---------------------------------------------------------------------------
# 生产凭据强校验（审查 Y2）
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("environment", ["staging", "prod"])
def test_production_rejects_default_credentials(environment: str) -> None:
    """staging/prod 沿用开发默认凭据（数据库/MinIO/JWT）→ 启动即失败。"""
    with pytest.raises(ValueError, match="默认凭据"):
        DemoSettings(environment=environment)


@pytest.mark.parametrize("environment", ["staging", "prod"])
def test_production_rejects_short_jwt_secret(environment: str) -> None:
    """JWT 密钥不足 32 字节（HS256 下限）→ 启动即失败。"""
    with pytest.raises(ValueError, match="JWT_SECRET_KEY"):
        DemoSettings(
            environment=environment,
            jwt_secret_key="short-secret",
            postgres_password="s3cure-pg-pass",
            minio_secret_key="s3cure-minio-pass",
        )


def test_production_accepts_injected_secrets() -> None:
    """K8s Secret 注入强随机凭据后正常启动。"""
    settings = DemoSettings(
        environment="prod",
        jwt_secret_key="X" * 48,
        postgres_password="s3cure-pg-pass",
        minio_secret_key="s3cure-minio-pass",
        gateway_hmac_secret="Y" * 48,
    )
    assert settings.environment == "prod"


def test_production_requires_gateway_hmac_secret() -> None:
    """G-02：staging/prod 未配 GATEWAY_HMAC_SECRET → 启动即失败（身份头必须签名）。"""
    with pytest.raises(ValueError, match="GATEWAY_HMAC_SECRET"):
        DemoSettings(
            environment="prod",
            jwt_secret_key="X" * 48,
            postgres_password="s3cure-pg-pass",
            minio_secret_key="s3cure-minio-pass",
        )


def test_production_requires_kafka_mtls_client_cert() -> None:
    """G-01：staging/prod 下 SASL_SSL 客户端缺证书/私钥 → 启动即失败（broker 已 required）。"""
    with pytest.raises(ValueError, match="KAFKA_SSL_CERTFILE"):
        DemoSettings(
            environment="prod",
            jwt_secret_key="X" * 48,
            postgres_password="s3cure-pg-pass",
            minio_secret_key="s3cure-minio-pass",
            gateway_hmac_secret="Y" * 48,
            kafka_security_protocol="SASL_SSL",
        )
    # 补齐证书后正常
    settings = DemoSettings(
        environment="prod",
        jwt_secret_key="X" * 48,
        postgres_password="s3cure-pg-pass",
        minio_secret_key="s3cure-minio-pass",
        gateway_hmac_secret="Y" * 48,
        kafka_security_protocol="SASL_SSL",
        kafka_ssl_certfile="/etc/hunter/kafka-tls/client.crt",
        kafka_ssl_keyfile="/etc/hunter/kafka-tls/client.key",
    )
    assert settings.kafka_ssl_certfile.endswith("client.crt")


@pytest.mark.parametrize("environment", ["dev", "test"])
def test_non_production_keeps_dev_defaults(environment: str) -> None:
    """dev/test 环境保留默认值（本地起步体验），不触发 fail fast。"""
    settings = DemoSettings(environment=environment)
    assert settings.jwt_secret_key == "change-me-in-production"

