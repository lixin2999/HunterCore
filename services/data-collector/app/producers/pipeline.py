"""平台内部 Topic 生产者（``telemetry_raw`` / ``telemetry_clean`` / ``event_raw``）。

契约：topics.yaml（``key = vehicle_id``、``acks=all``）+ 消息 Schema（telemetry / event）。

投递一律经 ``KafkaProducerManager.publish_payload``：**强制 Schema 校验与 key 一致性**
（审查 Y11：既有生产者全部绕过契约驱动入口，字段漂移无法在运行时发现）。

失败语义：
- 契约违规（Schema 不匹配/key 不一致）属实现缺陷 → 抛 ``KafkaMessageSchemaError``（5000）；
- Kafka 不可用且重试耗尽 → 抛 ``ServiceUnavailableError``（5001）；
- 可重试失败且本地缓冲启用 → 生产者已落盘（返回 ``buffered``），此处视为成功（不阻断链路上报）。
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from confluent_kafka import KafkaException
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.producers.pipeline")


class PipelineProducer:
    """data-collector 内部 Topic 生产者（遥测原始/清洗、事件原始）。"""

    def __init__(self, settings: Settings, producer: KafkaProducerManager | None = None) -> None:
        self._settings = settings
        self._producer = producer

    def _manager(self) -> KafkaProducerManager:
        """生产者管理器（进程级单例；测试可注入替身）。"""
        if self._producer is not None:
            return self._producer
        return KafkaProducerManager.instance()

    async def publish_telemetry(
        self, topic: str, payload: Mapping[str, Any], vehicle_id: str
    ) -> None:
        """投递遥测消息（raw 与 clean 共用 telemetry Schema）。"""
        await self._publish(topic, payload, vehicle_id, schema_name="telemetry")

    async def publish_event(self, payload: Mapping[str, Any], vehicle_id: str) -> None:
        """投递事件消息到 ``event_raw``（Schema：event）。"""
        await self._publish(
            self._settings.event_raw_topic, payload, vehicle_id, schema_name="event"
        )

    async def _publish(
        self, topic: str, payload: Mapping[str, Any], vehicle_id: str, *, schema_name: str
    ) -> None:
        """契约驱动投递（Schema 校验 + key=vehicle_id 强制）。"""
        try:
            await self._manager().publish_payload(
                topic,
                dict(payload),
                key=vehicle_id,
                schema_name=schema_name,
                contract_required=True,
            )
        except KafkaException as exc:  # Kafka 链路故障：5001（5000 仅未预期异常）
            logger.error(
                "pipeline_publish_failed",
                topic=topic,
                vehicle_id=vehicle_id,
                error=str(exc),
            )
            raise ServiceUnavailableError(message="Kafka 不可用，内部 Topic 投递失败") from exc


__all__ = ["PipelineProducer"]
