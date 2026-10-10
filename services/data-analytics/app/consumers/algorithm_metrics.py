"""algorithm_metrics 消费者（消费组 ``data-analytics-algorithm-metrics``）。

契约：``contracts/kafka/consumer-groups.yaml``——
- subscribes: ``[algorithm_metrics]``（Flink ``algorithm_performance_monitor`` 产出）；
- writes: ``data_analytics.algorithm_metrics``（``ON CONFLICT DO NOTHING``，写自身 schema）；
- idempotency_key: ``(vehicle_id, time, module, metric_name)``（= 表主键）。

统一能力来自 ``hunter_common.kafka.consumer.KafkaConsumerManager``：契约 Schema 校验
（``schema_name="auto"``，非法消息进 ``algorithm_metrics.dlq``）→ 幂等认领 → handler 重试 →
批次收尾钩子冲刷落库（**写库成功才提交 offset**，钩子失败 → 消息重投）。
"""
from __future__ import annotations

from typing import Any, ClassVar

from confluent_kafka import Message
from hunter_common.kafka.consumer import SCHEMA_AUTO, KafkaConsumerManager
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id

from app.config import Settings
from app.services.algorithm_metrics_ingest import AlgorithmMetricsIngest

logger = get_logger("app.consumers.algorithm_metrics")

#: 消费组名（契约固定）
ALGORITHM_METRICS_GROUP_ID = "data-analytics-algorithm-metrics"
#: 订阅 Topic（契约 platform_topics）
ALGORITHM_METRICS_TOPIC = "algorithm_metrics"


class AlgorithmMetricsConsumer:
    """Flink 算法指标 → 落库消费者（接管 data-analytics 的 algorithm_metrics 写路径）。"""

    group_id: ClassVar[str] = ALGORITHM_METRICS_GROUP_ID
    topic: ClassVar[str] = ALGORITHM_METRICS_TOPIC

    def __init__(self, settings: Settings, ingest: AlgorithmMetricsIngest) -> None:
        self._ingest = ingest
        self._guard = IdempotencyGuard(
            namespace=self.group_id,
            max_size=settings.kafka_consumer_idempotency_cache_size,
            ttl_s=settings.kafka_consumer_idempotency_ttl_s,
        )
        # 契约 Schema 校验（默认开启）：非法消息直接进 DLQ，不进入落库缓冲
        schema_name = SCHEMA_AUTO if settings.ingest_schema_validation_enabled else None
        if schema_name is None:  # pragma: no cover - 仅显式关闭时触发（高危配置）
            logger.error(
                "ingest_schema_validation_disabled",
                group_id=self.group_id,
                hint="消息体不再做契约校验，须经变更评审并回填契约",
            )
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=self.group_id,
            topics=[self.topic],
            batch_size=settings.consumer_batch_size,
            poll_timeout=settings.consumer_poll_timeout_seconds,
            schema_name=schema_name,
            idempotency=self._guard,
            idempotency_key=self.idempotency_key,
            on_batch_end=ingest.flush,
        )

    async def run(self) -> None:
        """消费主循环（启停由应用 lifespan 管理）。"""
        await self._consumer.run(self._handle)

    def stop(self) -> None:
        """请求停止消费（优雅停机：当前批次落库成功后退出）。"""
        self._consumer.stop()

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """幂等键 = 表主键 ``(vehicle_id, time, module, metric_name)``（契约声明）。"""
        return IdempotencyGuard.compose(
            value.get("vehicle_id"),
            value.get("time"),
            value.get("module"),
            value.get("metric_name"),
        )

    async def _handle(self, message: Message, value: Any) -> None:
        """统一入口：类型校验 + vehicle_id 日志上下文 + 委托落库缓冲。"""
        if not isinstance(value, dict):
            raise TypeError(f"algorithm_metrics 消息必须为 JSON 对象，实际 {type(value).__name__}")
        vehicle_id = value.get("vehicle_id")
        token = set_vehicle_id(str(vehicle_id) if vehicle_id else "")
        try:
            await self._ingest.handle(value)
        finally:
            reset_vehicle_id(token)


__all__ = [
    "ALGORITHM_METRICS_GROUP_ID",
    "ALGORITHM_METRICS_TOPIC",
    "AlgorithmMetricsConsumer",
]
