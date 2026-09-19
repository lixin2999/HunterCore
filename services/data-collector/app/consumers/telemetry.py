"""遥测消费接入（消费组 ``data-collector-telemetry``，契约 consumer-groups.yaml）。

- 订阅：``hunter.*.telemetry``（正则，车端 Topic）；
- produces：``telemetry_raw``（校验后原样）/ ``telemetry_clean``（清洗后）；
- 幂等键：``(vehicle_id, seq)``，seq 缺失时退化为 ``(vehicle_id, timestamp)``
  （契约 idempotency_key 声明的两个键）；
- 批量入库 ≥ 10000 点/秒；批次收尾钩子 = ``TelemetryIngestService.flush``，
  保证「写库成功才提交 offset」（钩子失败 → 消息重投，写侧 ON CONFLICT 幂等）。
"""
from __future__ import annotations

from typing import Any

from confluent_kafka import Message
from hunter_common.kafka.idempotency import IdempotencyGuard

from app.config import Settings
from app.consumers.base import BaseIngestConsumer
from app.services.ingest import TelemetryIngestService
from app.services.vehicle_status import VehicleStatusWriter

#: 消费组名（契约固定）
TELEMETRY_GROUP_ID = "data-collector-telemetry"


class TelemetryIngestConsumer(BaseIngestConsumer):
    """遥测消息消费者（接管 read model 的实时字段刷新与遥测入库）。"""

    group_id = TELEMETRY_GROUP_ID
    subscribe_pattern = "hunter.*.telemetry"

    def __init__(
        self,
        settings: Settings,
        service: TelemetryIngestService,
        *,
        status_writer: VehicleStatusWriter | None = None,
        on_batch_end: Any = None,
    ) -> None:
        self._service = service
        self._status_writer = status_writer
        hook = on_batch_end if on_batch_end is not None else service.flush
        super().__init__(settings, on_batch_end=hook)

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """幂等键：``(vehicle_id, seq)``，seq 缺失时用 ``(vehicle_id, timestamp)``。"""
        seq = value.get("seq") if isinstance(value, dict) else None
        if seq is None:
            return IdempotencyGuard.compose(value.get("vehicle_id"), value.get("timestamp"))
        return IdempotencyGuard.compose(value.get("vehicle_id"), seq)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        """单条遥测：预处理 → 累积缓冲（丢弃非法样本不重试）。"""
        accepted = await self._service.handle(payload, topic=topic)
        if accepted and self._status_writer is not None:
            # 读模型刷新（vehicle:status 的实时字段与心跳；契约唯一写方 = data-collector）
            await self._status_writer.update_from_telemetry(payload)


__all__ = ["TELEMETRY_GROUP_ID", "TelemetryIngestConsumer"]
