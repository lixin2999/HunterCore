"""ingest-platform 消费者基类（§5.1 消费拓扑）。"""
from __future__ import annotations

from typing import Any, ClassVar

from confluent_kafka import Message
from hunter_common.kafka.consumer import SCHEMA_AUTO, BatchEndHook, KafkaConsumerManager
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id

from app.config import Settings

logger = get_logger("app.consumers.base")


class BaseIngestConsumer:
    """ingest-platform 消费者基类（与 data-collector 同源实现，消费组/主题独立）。"""

    group_id: ClassVar[str] = ""
    subscribe_topic: ClassVar[str] = ""   # 具体 topic 名（非正则）

    def __init__(self, settings: Settings, *, on_batch_end: BatchEndHook | None = None) -> None:
        self._settings = settings
        self._guard = IdempotencyGuard(
            namespace=self.group_id,
            max_size=settings.kafka_consumer_idempotency_cache_size,
            ttl_s=settings.kafka_consumer_idempotency_ttl_s,
        )
        schema_name = SCHEMA_AUTO if settings.ingest_schema_validation_enabled else None
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=self.group_id,
            topics=[self.subscribe_topic],
            batch_size=settings.consumer_batch_size,
            poll_timeout=settings.consumer_poll_timeout_seconds,
            schema_name=schema_name,
            idempotency=self._guard,
            idempotency_key=self.idempotency_key,
            on_batch_end=on_batch_end,
        )

    async def run(self) -> None:
        await self._consumer.run(self._handle)

    def stop(self) -> None:
        self._consumer.stop()

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """默认使用 envelope 中的 event_id（§5.2 层1）。"""
        if isinstance(value, dict):
            return str(value.get("event_id", message.offset()))
        return str(message.offset())

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        raise NotImplementedError

    async def _handle(self, message: Message, value: Any) -> None:
        if not isinstance(value, dict):
            raise TypeError(f"消息必须为 JSON 对象，实际 {type(value).__name__}")
        vehicle_id = value.get("vehicle_id", "")
        token = set_vehicle_id(str(vehicle_id))
        try:
            await self.process(value, topic=str(message.topic()))
        finally:
            reset_vehicle_id(token)


__all__ = ["BaseIngestConsumer"]
