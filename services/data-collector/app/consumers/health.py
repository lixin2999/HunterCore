"""健康数据消费接入（消费组 ``data-collector-health``，契约 consumer-groups.yaml）。

- 订阅：``hunter.*.health``（1Hz）；
- produces：无（契约明确「不在 DB 落明细」）；
- 幂等键：``(vehicle_id, timestamp)``；
- 职责：写 Redis 读模型 ``vehicle:status:{vehicle_id}`` + ``vehicle:online:set``
  （契约 pending #2 指定的唯一写方路径），供 ota-service 门禁 / remote-control
  接管判定 / data-analytics 看板读取。
"""
from __future__ import annotations

from typing import Any

from confluent_kafka import Message
from hunter_common.kafka.idempotency import IdempotencyGuard

from app.config import Settings
from app.consumers.base import BaseIngestConsumer
from app.services.vehicle_status import VehicleStatusWriter

#: 消费组名（契约固定）
HEALTH_GROUP_ID = "data-collector-health"


class HealthIngestConsumer(BaseIngestConsumer):
    """系统健康消费者（维护车辆实时状态读模型）。"""

    group_id = HEALTH_GROUP_ID
    subscribe_pattern = "hunter.*.health"

    def __init__(
        self,
        settings: Settings,
        writer: VehicleStatusWriter,
        *,
        on_batch_end: Any = None,
    ) -> None:
        self._writer = writer
        super().__init__(settings, on_batch_end=on_batch_end)

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """幂等键：``(vehicle_id, timestamp)``（契约 consumer-groups.yaml）。"""
        return IdempotencyGuard.compose(value.get("vehicle_id"), value.get("timestamp"))

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        """写读模型（Hash + 在线集合；车端 offline 状态 → 移出在线集合）。"""
        await self._writer.update_from_health(payload)


__all__ = ["HEALTH_GROUP_ID", "HealthIngestConsumer"]
