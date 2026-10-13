"""ingest-platform 服务配置（pydantic-settings，全部参数来自环境变量/.env）。

对应《服务端数据消费解析与处理详细设计文档》v1.0 的所有可配置项。
字段与 infra/k8s/services/ingest-platform.yaml ConfigMap + Secret 一一对应。
"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """ingest-platform 配置（服务私有项，基类通用项见 hunter_common.config）。"""

    # ---------- 服务标识与默认端口 ----------
    service_name: str = "ingest-platform"
    api_port: int = 8089

    # ---------- Kafka 消费组（设计文档 §5.1 消费拓扑） ----------
    kafka_consumer_groups: str = (
        "g.ingest.manifest,g.ingest.projection,g.ingest.progress,"
        "g.ingest.artifact,g.ingest.heartbeat,g.ingest.alert,g.ingest.cmdack"
    )
    # 订阅模式（vehicle.bag.* 和 vehicle.agent.* 主题正则）
    kafka_subscribe_patterns: str = (
        "vehicle\\.bag\\.manifest,vehicle\\.bag\\.task\\.status,"
        "vehicle\\.bag\\.parse\\.progress,vehicle\\.bag\\.artifact\\.uploaded,"
        "vehicle\\.agent\\.heartbeat,vehicle\\.agent\\.alert,"
        "vehicle\\.agent\\.cmd\\.ack"
    )
    # 平台内部生产主题（设计文档 §7.5）
    kafka_produce_topics: str = (
        "platform.derived.ready.v1,platform.bag.qc_failed.v1,"
        "platform.clip.created.v1,platform.dataset.ready.v1"
    )
    kafka_auto_offset_reset: str = "earliest"
    # 消费参数（§5.1：手动提交、单批 ≤ 500 条、max.poll.interval.ms=300s）
    consumer_batch_size: int = 500
    consumer_poll_timeout_seconds: float = 1.0
    # 消费侧 Schema 校验（契约先行）
    ingest_schema_validation_enabled: bool = True

    # ---------- DLT 死信（§5.3） ----------
    # 解析/字段级无效消息专用死信主题（基础设施类失败仍走 hunter_common 的 {topic}.dlq）
    dlt_enabled: bool = True
    dlt_topic: str = "ingest.dlt"

    # ---------- 数据湖：对象存储前缀（§6.1） ----------
    # parsed 层（车端所有权，平台只读）：bags/v1/{vehicle_id}/{date}/{bag_id}/
    lake_parsed_prefix: str = "bags/v1"
    # derived 层（平台所有权）：derived/v1/{bag_id}/{input_rev}/
    lake_derived_prefix: str = "derived/v1"
    # dataset 层（训练出口）：datasets/{dataset_id}/{version}/
    lake_dataset_prefix: str = "datasets"
    # MinIO Bucket（与车端上传目标一致）
    minio_bucket_rosbag: str = "hunter-rosbag"
    minio_bucket_parsed: str = "hunter-rosbag"      # parsed 层与原始 bag 同 bucket
    minio_bucket_derived: str = "hunter-derived"     # derived 层（平台专属）
    minio_bucket_dataset: str = "hunter-datasets"    # dataset 层
    minio_region: str = "us-east-1"
    # §13.2 数据湖最小权限：parsed 层（车端所有权，平台只读）独立只读凭据；
    # 未配置回退主凭据（dev 兼容），生产必须配置仅具 s3:GetObject/ListBucket 权限的专用 key，
    # 并在 MinIO 侧对 parsed bucket 挂只读 bucket policy（见 services/ingest-platform/README.md）
    minio_parsed_access_key: str = ""
    minio_parsed_secret_key: str = ""
    # 预签名 URL 有效期
    presigned_download_expire_seconds: int = 600     # 10 min（§8.1）

    # ---------- 对齐工作参数（§7.2） ----------
    # 锚相机（主时间轴），多相机以此为准
    anchor_camera: str = "camera_front"
    # 表格对齐时间窗口（ms）
    align_table_window_ms: int = 50
    # 是否对高频 IMU 做线性插值
    interpolate_imu: bool = False
    # 点云匹配最大允许偏差（ms），超出则记 null
    max_lidar_dt_ms: int = 100

    # ---------- 事件挖掘规则引擎（§7.3 来源1，服务端侧） ----------
    # 从 IMU 加速度序列挖掘急刹/碰撞事件（去重力后的合加速度阈值 m/s²）
    rule_mining_enabled: bool = True
    rule_hard_brake_mps2: float = 4.0
    rule_collision_mps2: float = 8.0

    # ---------- 切片参数（§7.3） ----------
    # 固定窗口切片：窗口时长（s）、步长重叠（s）
    slice_window_s: int = 10
    slice_overlap_s: int = 2
    # 事件驱动切片：前后扩展（s）
    slice_event_before_s: int = 5
    slice_event_after_s: int = 10

    # ---------- 质量检查参数（§7.4） ----------
    # QC 加权分数低于此阈值发 qc_failed 事件并告警
    qc_score_threshold: float = 0.6
    # 抽样验证比例（0~1）
    qc_hash_sample_ratio: float = 0.05
    # 图像黑帧检测：全像素亮度均值低于此值判为黑帧（0-255）
    qc_black_frame_threshold: float = 10.0
    # 连续相同 hash 帧数超过此值判为冻结帧
    qc_frozen_frame_min_count: int = 3
    # 黑帧/冻结帧/解码抽检的连续采样帧数（§7.4 检查项 5）
    qc_image_sample_count: int = 10

    # ---------- 就绪判定模板（§7.1） ----------
    # 必需产物类型（至少要有 manifest + 一张表才触发加工）
    ready_required_classes: str = "manifest,table"
    ready_optional_classes: str = "image,pcd,raw"
    # bag 最少时长（s），低于此值不触发加工
    ready_min_duration_s: int = 1
    # 必需产物缺失告警超时（秒）
    ready_incomplete_alert_s: int = 3600

    # ---------- 任务编排（§9） ----------
    task_lease_timeout_s: int = 300           # lease 过期时间（5 min）
    task_max_attempts: int = 3
    task_backoff_base_s: int = 30             # 退避基数：30s × 2^n
    # 加工队列深度超过此值时触发背压（§5.4）
    task_queue_backpressure_threshold: int = 2000
    # 背压深度重查间隔（秒）与命中后单条消息最大延迟（秒）
    backpressure_check_interval_s: float = 10.0
    backpressure_throttle_s: float = 2.0

    # ---------- 数据集导出（§8.2） ----------
    # webdataset tar 分片每片最大 clip 数
    dataset_shard_max_clips: int = 1000

    # ---------- 解析模板版本（input_rev 计算因子，§5.2） ----------
    parse_template_version: str = "v1.0"

    # ---------- 车辆维度限流（§5.4 背压） ----------
    #  artifact.uploaded 消息每分区令牌桶速率（msg/s）
    artifact_ingest_rate_limit: int = 50

    # ---------- 遥测/事件消费开关（设计文档 §5.1） ----------
    manifest_consumer_enabled: bool = True
    task_status_consumer_enabled: bool = True
    parse_progress_consumer_enabled: bool = True
    artifact_consumer_enabled: bool = True
    heartbeat_consumer_enabled: bool = True
    alert_consumer_enabled: bool = True
    cmd_ack_consumer_enabled: bool = True

    # ---------- 加工 Worker 开关 ----------
    align_worker_enabled: bool = True
    slice_worker_enabled: bool = True
    qc_worker_enabled: bool = True
    # 预留：enrich_traj（§7.1 optional，M4/M5）worker 未实现，task_graph 不会创建该类型任务
    enrich_worker_enabled: bool = False

    # ---------- orchestrator 主循环 ----------
    orchestrator_poll_interval_s: float = 2.0
    # 单轮并发派发任务上限（§9 并发 worker 池；单主锁内并行 claim/执行）
    orchestrator_max_parallel_tasks: int = 4
    # 单主选举：PG advisory lock key（固定数字，同一集群内唯一）
    orchestrator_advisory_lock_key: int = 918273645

    # ---------- 命令闭环（§9.4） ----------
    cmd_ack_timeout_s: int = 1800             # 30min 未 ack 自动重发一次
    cmd_timeout_scan_interval_s: float = 60.0  # 超时扫描周期

    # ---------- 告警路由（§5.1/§10） ----------
    # 命中这些 level 的 agent.alert 视为需要外发通知（Alertmanager/Webhook）
    alert_critical_levels: str = "critical,error"
    # Alertmanager-compatible webhook（JSON POST）；留空仅落库+日志
    alert_webhook_url: str = ""

    # ---------- 数据脱敏（§13.4） ----------
    # GNSS 坐标保留小数位（看板读路径）
    gnss_descimal_places: int = 4
    # 服务端加工链路（frame_index GNSS 列）坐标截断开关
    gnss_mask_enabled: bool = True

    # ---------- RBAC ----------
    data_read_roles: str = "admin,analyst,operator"
    data_execute_roles: str = "admin,operator"

    @property
    def data_read_role_set(self) -> frozenset[str]:
        return frozenset(r.strip() for r in self.data_read_roles.split(",") if r.strip())

    @property
    def data_execute_role_set(self) -> frozenset[str]:
        return frozenset(r.strip() for r in self.data_execute_roles.split(",") if r.strip())

    @property
    def kafka_consumer_group_list(self) -> list[str]:
        return [g.strip() for g in self.kafka_consumer_groups.split(",") if g.strip()]

    @property
    def ready_required_class_list(self) -> list[str]:
        return [c.strip() for c in self.ready_required_classes.split(",") if c.strip()]

    @property
    def ready_optional_class_list(self) -> list[str]:
        return [c.strip() for c in self.ready_optional_classes.split(",") if c.strip()]

    @property
    def alert_critical_level_set(self) -> frozenset[str]:
        return frozenset(l.strip().lower() for l in self.alert_critical_levels.split(",") if l.strip())


settings = Settings()
