"""data-collector 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

字段与 contracts/openapi/data-collector.yaml `x-hunter-service.required_env`、
infra/k8s/services/data-collector.yaml（ConfigMap data-collector-config + Secret）一一对应；
所有 URL/密钥/端口/阈值一律可配置，禁止硬编码业务参数（配置管理约束）。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """data-collector 配置（服务私有项，基类通用项见 hunter_common.config）。"""

    # 服务标识与默认端口（仍可被环境变量 SERVICE_NAME / API_PORT 覆盖）
    service_name: str = "data-collector"
    api_port: int = 8082

    # ---------- Kafka 消费/生产声明（对齐 contracts/kafka/consumer-groups.yaml，禁止擅自新增） ----------
    kafka_consumer_groups: str = (
        "data-collector-telemetry,data-collector-events,"
        "data-collector-health,data-collector-command-result"
    )
    kafka_subscribe_patterns: str = (
        "hunter.*.telemetry,hunter.*.event,hunter.*.health,hunter.*.command_result"
    )
    kafka_produce_topics: str = "telemetry_raw,telemetry_clean,event_raw,sensor_file"
    kafka_auto_offset_reset: str = "earliest"

    # ---------- 时序批量写入（性能指标：≥ 10000 点/秒，批量插入，禁止逐条 commit） ----------
    telemetry_batch_size: int = 2000
    telemetry_flush_interval_ms: int = 500

    # ---------- 遥测查询保护（x-hunter-telemetry-query-contract：跨度上限保护 API P95） ----------
    telemetry_query_max_range_hours: int = 24

    # ---------- MinIO（车端上传类 Bucket，名称契约固定；生命周期由 minio-init Job 配置） ----------
    minio_bucket_raw_data: str = "hunter-raw-data"
    minio_bucket_rosbag: str = "hunter-rosbag"
    minio_bucket_video: str = "hunter-video"
    # S3 region（MinIO 任意值即可，默认 us-east-1）
    minio_region: str = "us-east-1"
    # 预签名 URL 有效期（上传 1 小时 / 下载 15 分钟，MinIO 契约）
    presigned_upload_expire_seconds: int = 3600
    presigned_download_expire_seconds: int = 900

    # ---------- sensor_file 通知 Topic（契约常量，环境变量可覆盖） ----------
    sensor_file_topic: str = "sensor_file"

    # ---------- 车辆维度限流（附录 D：遥测 ≤100 msg/s、文件上传 ≤10 Mbps = 1310720 B/s） ----------
    vehicle_telemetry_max_msg_per_sec: int = 100
    file_upload_max_bytes_per_second: int = 1310720

    # ---------- 接口限流（附录 D：GET /data/telemetry 单用户 20 QPS；Redis 窗口 60s） ----------
    telemetry_query_rate_limit_per_min: int = 20
    rate_limit_window_seconds: int = 60

    # ---------- RBAC（网关注入 X-Roles；角色→动作映射待 RBAC 服务化后收敛为权限查询） ----------
    data_read_roles: str = "admin,analyst,operator"
    data_execute_roles: str = "admin,operator"

    @property
    def data_read_role_set(self) -> frozenset[str]:
        """data:read 授权角色集合（遥测/事件/文件清单查询）。"""
        return frozenset(role.strip() for role in self.data_read_roles.split(",") if role.strip())

    @property
    def data_execute_role_set(self) -> frozenset[str]:
        """data:execute 授权角色集合（事件确认等写操作）。"""
        return frozenset(
            role.strip() for role in self.data_execute_roles.split(",") if role.strip()
        )

    @property
    def data_type_bucket_mapping(self) -> dict[str, str]:
        """data_type → Bucket 映射（x-hunter-file-upload-flow.bucket_mapping，顺序无关）。"""
        return {
            "point_cloud": self.minio_bucket_raw_data,
            "camera_image": self.minio_bucket_raw_data,
            "other": self.minio_bucket_raw_data,
            "rosbag": self.minio_bucket_rosbag,
            "video": self.minio_bucket_video,
        }

    @property
    def kafka_consumer_group_list(self) -> list[str]:
        """消费组清单（契约 x-hunter-kafka.groups，顺序无关）。"""
        return [group.strip() for group in self.kafka_consumer_groups.split(",") if group.strip()]


settings = Settings()
