"""telemetry_clean 消费者（消费组 vil-forwarder，契约 consumer-groups.yaml）。"""
from __future__ import annotations

from typing import Any

from confluent_kafka import Message
from hunter_common.kafka.consumer import SCHEMA_AUTO, KafkaConsumerManager
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id

from app.config import Settings
from app.forwarder import VilForwarder

logger = get_logger("app.consumer")


class VilForwarderConsumer:
    """消费 telemetry_clean 并委托 VilForwarder（幂等键 (vehicle_id, seq)）。"""

    group_id = "vil-forwarder"

    def __init__(self, settings: Settings, forwarder: VilForwarder) -> None:
        self._settings = settings
        self._forwarder = forwarder
        self._guard = IdempotencyGuard(
            namespace=self.group_id,
            max_size=settings.kafka_consumer_idempotency_cache_size,
            ttl_s=settings.kafka_consumer_idempotency_ttl_s,
        )
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=self.group_id,
            topics=[settings.kafka_subscribe_topic],
            batch_size=settings.consumer_batch_size,
            poll_timeout=settings.consumer_poll_timeout_seconds,
            schema_name=SCHEMA_AUTO,
            idempotency=self._guard,
            idempotency_key=self.idempotency_key,
            auto_offset_reset=settings.vil_auto_offset_reset,
        )

    @staticmethod
    def idempotency_key(_message: Message, value: Any) -> str:
        """契约：``(vehicle_id, seq)``。"""
        if not isinstance(value, dict):
            raise TypeError("telemetry_clean 消息必须为 JSON 对象")
        return IdempotencyGuard.compose(value.get("vehicle_id"), value.get("seq"))

    async def _handle(self, _message: Message, value: Any) -> None:
        if not isinstance(value, dict):
            raise TypeError(f"消息必须为 JSON 对象，实际 {type(value).__name__}")
        vehicle_id = value.get("vehicle_id")
        token = set_vehicle_id(str(vehicle_id) if vehicle_id else "")
        try:
            await self._forwarder.handle(value)
        finally:
            reset_vehicle_id(token)

    async def run(self) -> None:
        await self._consumer.run(self._handle)

    def stop(self) -> None:
        self._consumer.stop()


class ConsumerSupervisor:
    """消费者自愈包装：异常退出后指数退避重启。"""

    def __init__(self, factory: Any) -> None:
        self._factory = factory
        self._consumer: VilForwarderConsumer | None = None

    @property
    def group_id(self) -> str:
        return VilForwarderConsumer.group_id

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                self._consumer = self._factory()
                await self._consumer.run()
                return
            except Exception:
                logger.exception("vil_forwarder_consumer_crashed", backoff_s=backoff)
                await _sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def stop(self) -> None:
        if self._consumer is not None:
            self._consumer.stop()


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


__all__ = ["ConsumerSupervisor", "VilForwarderConsumer"]
