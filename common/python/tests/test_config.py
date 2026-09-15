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


def test_jwt_token_ttl_matches_spec() -> None:
    """Access Token 2h / Refresh Token 7d（设计文档：安全机制）。"""
    settings = DemoSettings()
    assert settings.jwt_access_token_expire_minutes == 120
    assert settings.jwt_refresh_token_expire_days == 7
