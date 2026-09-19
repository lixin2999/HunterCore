"""事件消费接入（消费组 ``data-collector-events``，契约 consumer-groups.yaml）。

- 订阅：``hunter.*.event``；
- produces：``event_raw``（供 data-analytics 统计与 Corner Case 挖掘、scene-service 场景提取）；
- 幂等键：``(vehicle_id, event_type, event_time)``，与 ``events`` 表唯一索引
  ``uq_events_vehicle_type_time`` 一致（at-least-once 重放不产生重复事件）；
- 等级一致性：``event_level`` 必须等于 ``EVENT_LEVEL_BY_TYPE[event_type]``，
  不一致时抛错 → 消费者重试耗尽后转 DLQ（契约等级不可放宽，禁止静默改写）。
"""
from __future__ import annotations

from typing import Any

from confluent_kafka import Message
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.logging import get_logger

from app.config import Settings
from app.consumers.base import BaseIngestConsumer
from app.producers.pipeline import PipelineProducer
from app.repositories.events import EventRepository
from app.services.ingest import prepare_event

logger = get_logger("app.consumers.events")

#: 消费组名（契约固定）
EVENTS_GROUP_ID = "data-collector-events"


class EventIngestConsumer(BaseIngestConsumer):
    """车辆事件消费者（落库 events + 投递 event_raw）。"""

    group_id = EVENTS_GROUP_ID
    subscribe_pattern = "hunter.*.event"

    def __init__(
        self,
        settings: Settings,
        repository: EventRepository,
        producer: PipelineProducer,
        *,
        on_batch_end: Any = None,
    ) -> None:
        self._repository = repository
        self._producer = producer
        super().__init__(settings, on_batch_end=on_batch_end)

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """幂等键：``(vehicle_id, event_type, event_time)``（契约固定）。"""
        return IdempotencyGuard.compose(
            value.get("vehicle_id"), value.get("event_type"), value.get("timestamp")
        )

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        """单条事件：等级校验 → 幂等落库 → 投递 event_raw。"""
        prepared = prepare_event(payload)
        async with self._repository.transaction() as session:
            await self._repository.insert_events(session, [prepared.row])
        await self._producer.publish_event(prepared.message, prepared.vehicle_id)
        logger.info(
            "event_ingested",
            event_type=str(prepared.row["event_type"]),
            event_level=str(prepared.row["event_level"]),
            topic=topic,
        )


__all__ = ["EVENTS_GROUP_ID", "EventIngestConsumer"]
