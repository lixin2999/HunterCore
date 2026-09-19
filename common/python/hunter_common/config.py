"""HunterEdge 服务基础配置（pydantic-settings）。

约束（设计文档：配置管理）：
- 所有可调参数从环境变量/.env 读取，禁止在业务代码中硬编码 URL/密钥/端口/阈值
- 各服务继承 HunterBaseConfig 并补充服务私有字段
- 生产部署：非敏感配置走 ConfigMap，敏感配置（密码/密钥/证书）走 K8s Secret 注入环境变量
"""
from __future__ import annotations

import os
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class HunterBaseConfig(BaseSettings):
    """所有微服务配置的基类（pydantic-settings，环境变量优先于 .env 文件）。"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---------- 服务基础 ----------
    service_name: str = "hunter-service"
    environment: Literal["dev", "test", "staging", "prod"] = "dev"
    debug: bool = True
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    # CORS 允许来源（逗号分隔；前端 Vite 默认 5173）
    cors_origins: str = "http://localhost:5173"

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    # ---------- PostgreSQL 15 + TimescaleDB 2.13 ----------
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "hunter"
    postgres_password: str = "hunter_dev_123"
    postgres_db: str = "hunter_edge"
    db_echo: bool = False
    # 连接池大小 = CPU 核数 × 2 + 1（开发规则：数据库连接池）
    db_pool_size: int = Field(default_factory=lambda: (os.cpu_count() or 2) * 2 + 1)
    db_max_overflow: int = 10
    db_pool_timeout: int = 30

    @property
    def database_url(self) -> str:
        """SQLAlchemy 异步连接串（asyncpg 驱动）。"""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # ---------- Redis 7 ----------
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str | None = None
    redis_db: int = 0
    redis_max_connections: int = 100

    @property
    def redis_url(self) -> str:
        """redis-py 异步连接串。"""
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # ---------- Apache Kafka 3.6 ----------
    kafka_bootstrap_servers: str = "localhost:9092"
    # 生产环境及车端接入必须 SASL_SSL + SCRAM-SHA-512（设计文档：Kafka 安全），本地开发可用 PLAINTEXT
    kafka_security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    kafka_sasl_mechanism: Literal["SCRAM-SHA-512", "PLAIN"] = "SCRAM-SHA-512"
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_ssl_cafile: str | None = None
    # 生产者基准参数（对齐车端生产者配置：lz4 / linger.ms=5 / batch.size=16384 / retries=3）
    kafka_producer_acks: Literal["0", "1", "all"] = "all"
    kafka_compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "lz4"
    kafka_linger_ms: int = 5
    kafka_batch_size: int = 16384
    kafka_retries: int = 3

    # 生产重试与本地磁盘缓冲（契约 topics.yaml#producer_defaults：retries=3 / local_disk_buffer_bytes=1GB）
    kafka_produce_max_attempts: int = 3              # 投递尝试次数上限（契约 retries=3，指数退避）
    kafka_produce_retry_backoff_ms: int = 100        # 退避基数：第 n 次重试等待 base * 2^(n-1)
    kafka_produce_retry_backoff_max_ms: int = 5000   # 退避上限，避免长尾等待拖垮请求
    kafka_local_buffer_enabled: bool = True          # 链路中断时落盘缓冲（契约要求，禁止关闭于生产）
    kafka_local_buffer_dir: str = "./data/kafka-buffer"   # K8s 需挂载 emptyDir/PVC 并纳入配额
    kafka_local_buffer_max_bytes: int = 1_073_741_824     # 1GB（契约 producer_defaults）
    kafka_local_buffer_segment_max_records: int = 1000    # 段内最大记录数（决定淘汰粒度）

    # 消费重试 / 幂等 / 积压指标（契约 consumer-groups.yaml#defaults：手动提交 + DLQ + 幂等 required）
    kafka_consumer_max_attempts: int = 3             # handler 失败重试次数（耗尽 → {topic}.dlq）
    kafka_consumer_retry_backoff_ms: int = 200       # 消费重试退避基数
    kafka_consumer_idempotency_cache_size: int = 10_000
    kafka_consumer_idempotency_ttl_s: int = 86_400
    kafka_consumer_lag_metrics_enabled: bool = True  # 暴露 hunter_kafka_consumer_lag

    # 契约目录（含 topics.yaml / consumer-groups.yaml / schemas）；
    # None/空白 = 自动探测（环境变量 KAFKA_CONTRACT_DIR → 工作目录/包位置向上查找 contracts/kafka）
    kafka_contract_dir: str | None = None

    @field_validator("kafka_contract_dir", mode="before")
    @classmethod
    def _blank_contract_dir_as_none(cls, value: object) -> object:
        """空串（.env 中留空的 KAFKA_CONTRACT_DIR）统一归一为 None，避免误判为"显式无效目录"。"""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @property
    def kafka_bootstrap_servers_list(self) -> list[str]:
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",") if s.strip()]

    # ---------- MinIO（S3 兼容对象存储） ----------
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False

    # ---------- 认证与安全 ----------
    # 生产环境必须通过 K8s Secret 覆盖，禁止使用默认值上线
    jwt_secret_key: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 120   # Access Token 2h（设计文档：安全机制）
    jwt_refresh_token_expire_days: int = 7       # Refresh Token 7d
