"""data-analytics 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

环境变量清单对齐 contracts/openapi/data-analytics.yaml `x-hunter-required-env`；
所有 URL/密钥/端口/阈值一律可配置，禁止硬编码（配置管理约束）。
"""
from __future__ import annotations

import json

from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("app.config")


class Settings(HunterBaseConfig):
    """data-analytics 配置（服务私有项按契约 x-hunter-required-env 逐步补充）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "data-analytics"
    api_port: int = 8083

    # ---------- 下游 REST 依赖（服务间调用走集群内 mTLS，不透传用户身份） ----------
    data_collector_base_url: str = ""
    vehicle_service_base_url: str = ""
    scene_service_base_url: str = ""
    flink_jobmanager_url: str = ""
    # 下游调用超时/重试（契约 x-hunter-dependency-*：超时 2s，重试 1 次）
    dependency_timeout_seconds: float = 2.0
    dependency_max_retries: int = 1

    # ---------- 只读数据库（hunter_analytics_ro，仅 SELECT；契约 db_access.read_only） ----------
    analytics_ro_db_user: str = ""
    analytics_ro_db_password: str = ""

    @property
    def read_only_database_url(self) -> str:
        """只读分析账号连接串（跨 schema 只读例外见 contracts/database/er.md 第 3 节）。"""
        return (
            f"postgresql+asyncpg://{self.analytics_ro_db_user}:{self.analytics_ro_db_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # ---------- MinIO（报告 sidecar / 评估文档；bucket 名称契约固定） ----------
    minio_bucket_reports: str = "hunter-reports"
    report_sidecar_prefix: str = "reports"
    eval_document_pointer_key: str = "eval_documents/latest.json"
    # 报告下载预签名有效期（契约固定 900s = 15 分钟，支持 Range 分片下载）
    report_download_expires_seconds: int = 900

    # ---------- 报告生成阈值（x-hunter-rate-limits.heavy_protection） ----------
    report_max_range_days: int = 90
    report_generate_max_concurrent: int = 2
    # 预计就绪提示值（JSON：{"<report_type>": seconds}；未配置的类型返回 null，仅提示用途）
    report_estimated_ready_seconds: str = ""

    # ---------- 查询保护 ----------
    analytics_query_max_range_days: int = 7
    # 算法指标聚合回看窗口（秒，看板 algorithm 块下限保护）
    algorithm_metrics_window_seconds: int = 60
    # 事件类型分布抓取条数上限（≤200，与 BaseRepository.MAX_PAGE_SIZE 对齐）
    dashboard_event_fetch_size: int = 200

    # ---------- Kafka 管道健康（x-hunter-kafka.groups / topics，命名不可更改） ----------
    kafka_consumer_groups: str = (
        "analytics-stream-consumer,alert-engine-consumer,ota-status-consumer,"
        "telemetry-clean-group,telemetry-raw-failure-group,corner-case-miner-consumer"
    )
    dlq_topic: str = "dead_letter_queue"

    # ---------- RBAC（网关注入 X-Roles；角色→动作映射待 RBAC 服务化后收敛为权限查询） ----------
    analytics_read_roles: str = "admin,analyst,operator"
    analytics_execute_roles: str = "admin,analyst"

    # ---------- 评估文档（x-hunter-minio-eval-documents 布局见契约） ----------

    @property
    def analytics_read_role_set(self) -> frozenset[str]:
        """analytics:read 授权角色集合。"""
        return frozenset(role.strip() for role in self.analytics_read_roles.split(",") if role.strip())

    @property
    def analytics_execute_role_set(self) -> frozenset[str]:
        """analytics:execute 授权角色集合（报告生成）。"""
        return frozenset(role.strip() for role in self.analytics_execute_roles.split(",") if role.strip())

    @property
    def kafka_consumer_group_list(self) -> list[str]:
        """消费组清单（契约 x-hunter-kafka.groups，顺序无关）。"""
        return [group.strip() for group in self.kafka_consumer_groups.split(",") if group.strip()]

    def estimated_ready_seconds(self, report_type: str) -> int | None:
        """报告预计就绪提示值（未配置的类型返回 None，响应字段允许 null）。"""
        raw = self.report_estimated_ready_seconds.strip()
        if not raw:
            return None
        try:
            mapping = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("report_estimated_ready_seconds_invalid_json")
            return None
        if not isinstance(mapping, dict):
            return None
        value = mapping.get(report_type)
        return int(value) if value is not None else None


settings = Settings()
