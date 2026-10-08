"""vehicle-service 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

字段与 contracts/openapi/vehicle-service.yaml `x-hunter-service.required_env` 一一对应。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """vehicle-service 配置（服务私有项，基类通用项见 hunter_common.config）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "vehicle-service"
    api_port: int = 8086

    # ---------- Kafka AdminClient（走 INTERNAL 9092 SASL_PLAINTEXT） ----------
    # 与业务服务共用 kafka_bootstrap_servers；控制面操作用平台侧 hunter-client 凭据
    kafka_admin_bootstrap_servers: str | None = None  # None → 复用 kafka_bootstrap_servers
    # SCRAM/Topic 元数据同步等待（AdminClient 阻塞超时）
    kafka_admin_timeout_ms: int = 10_000
    kafka_admin_metadata_wait_ms: int = 3_000
    # inter-broker 复制因子：单机部署 = 1，生产 K8s = 3
    kafka_default_replication_factor: int = 1

    # ---------- 证书签发（openssl 子进程 + CA 挂载） ----------
    kafka_ca_key_path: str = "/srv/kafka-ca/ca-key.pem"
    kafka_ca_cert_path: str = "/srv/kafka-ca/ca-cert.pem"
    # 每车证书输出目录（compose 挂 :rw）
    vehicle_certs_dir: str = "/app/data/vehicle-certs"
    # 证书有效期（与 CA 一致 3650 天；不建议缩短，车端信任链同步成本较高）
    vehicle_cert_validity_days: int = 3650
    # openssl 子进程超时
    cert_signer_timeout_seconds: int = 15

    # ---------- Bootstrap 主机（车端接入 kafka.properties 生成用） ----------
    server_ip: str | None = None  # None → 请求头 Host 或回落 127.0.0.1
    kafka_external_port: int = 9093

    # ---------- 一键 provisioning 全局超时（含 4 步） ----------
    provision_timeout_seconds: int = 30

    # ---------- RBAC（网关注入 X-Roles；角色→动作映射与契约 x-hunter-service.rbac_role_bindings 对齐） ----------
    vehicle_read_roles: str = "admin,operator,analyst,viewer"
    vehicle_create_roles: str = "admin,operator"
    vehicle_update_roles: str = "admin,operator"
    vehicle_delete_roles: str = "admin"          # 下线仅 admin
    vehicle_execute_roles: str = "admin,operator"

    @staticmethod
    def _split_roles(raw: str) -> frozenset[str]:
        return frozenset(role.strip() for role in raw.split(",") if role.strip())

    @property
    def vehicle_read_role_set(self) -> frozenset[str]:
        return self._split_roles(self.vehicle_read_roles)

    @property
    def vehicle_create_role_set(self) -> frozenset[str]:
        return self._split_roles(self.vehicle_create_roles)

    @property
    def vehicle_update_role_set(self) -> frozenset[str]:
        return self._split_roles(self.vehicle_update_roles)

    @property
    def vehicle_delete_role_set(self) -> frozenset[str]:
        return self._split_roles(self.vehicle_delete_roles)

    @property
    def vehicle_execute_role_set(self) -> frozenset[str]:
        return self._split_roles(self.vehicle_execute_roles)

    @property
    def admin_bootstrap(self) -> str:
        """AdminClient 使用的 bootstrap（默认复用 kafka_bootstrap_servers）。"""
        return self.kafka_admin_bootstrap_servers or self.kafka_bootstrap_servers


settings = Settings()
