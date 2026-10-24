"""vil-forwarder 配置（VIL 数据面：telemetry_clean → carla.{sim_id}.vil_state）。"""
from __future__ import annotations

from hunter_common.config import HunterBaseConfig


class Settings(HunterBaseConfig):
    """VIL 转发器配置（服务私有项，基类通用项见 hunter_common.config）。"""

    service_name: str = "vil-forwarder"
    api_port: int = 8090

    # 消费组/订阅/生产声明与 contracts/kafka/consumer-groups.yaml 一一对应
    kafka_consumer_group: str = "vil-forwarder"
    kafka_subscribe_topic: str = "telemetry_clean"
    kafka_produce_topics: str = "carla.{sim_id}.vil_state"

    # 新消费组无历史位点时从最新开始：VIL 只关心当前实车状态，回放历史遥测会
    # 把过期位姿推进 CARLA（设计文档 7.2：auto.offset.reset=latest）。
    vil_auto_offset_reset: str = "latest"

    # 10Hz 目标频率（契约 topics.yaml#carla_topics.vil_state.frequency）
    vil_forward_target_hz: int = 10
    # Redis 会话本地缓存 TTL（避免每条遥测都打 Redis；会话变更最多延迟该时长）
    vil_session_cache_ttl_ms: int = 1000
    # 消费开关（排障可关闭，不影响 API 健康探针）
    vil_consumer_enabled: bool = True

    # VIL 是低时延旁路：允许 latest-wins 丢帧，禁止本地磁盘缓冲（避免只读根文件系统写入失败，
    # 也避免把过期位姿在链路恢复后重放过期状态）。显式覆盖共享库的生产默认值。
    kafka_local_buffer_enabled: bool = False


settings = Settings()
