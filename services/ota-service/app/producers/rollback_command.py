"""hunter.{vehicle_id}.command 生产者（仅 command_type=ota_rollback；契约 x-hunter-kafka）。

消息载荷遵循 contracts/kafka/schemas/command.schema.json：
required = [command_id, timestamp, command_type]；单车辆指令必须携带 vehicle_id（= 消息 key）；
ota_rollback 的 params = {task_id, target, reason}。
``hunter.broadcast.command`` 在契约确认前**不生产**（x-hunter-pending-confirmation #12）。
"""
from __future__ import annotations

import json
import time
from typing import Any
from uuid import UUID, uuid4

from confluent_kafka import KafkaException
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.kafka.producer import KafkaProducerManager
from structlog import get_logger

from app.config import settings

logger = get_logger(service="ota-service")

#: OTA 回滚指令类型（契约 x-hunter-kafka.produces.command_type）
COMMAND_TYPE_OTA_ROLLBACK = "ota_rollback"


class RollbackCommandProducer:
    """A/B 分区回滚指令生产者（POST /tasks/{task_id}/rollback 逐车下发）。"""

    def __init__(self, producer: KafkaProducerManager | None = None) -> None:
        self._producer = producer

    def _manager(self) -> KafkaProducerManager:
        """单例获取（测试可注入替身）。"""
        if self._producer is not None:
            return self._producer
        return KafkaProducerManager.instance()

    async def send_rollback(
        self,
        *,
        vehicle_id: str,
        task_id: UUID,
        target: str,
        reason: str,
        operator_id: UUID | None,
    ) -> UUID:
        """下发回滚指令并返回 command_id（与 command_result 关联追踪）。"""
        command_id = uuid4()
        payload: dict[str, Any] = {
            "command_id": str(command_id),
            "vehicle_id": vehicle_id,
            "timestamp": time.time(),
            "command_type": COMMAND_TYPE_OTA_ROLLBACK,
            "params": {"task_id": str(task_id), "target": target, "reason": reason},
        }
        if operator_id is not None:
            payload["operator_id"] = str(operator_id)
        topic = settings.vehicle_command_topic_pattern.format(vehicle_id=vehicle_id)
        try:
            await self._manager().produce(
                topic=topic,
                value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                key=vehicle_id.encode("utf-8"),
            )
        except KafkaException as exc:
            logger.error(
                "ota_rollback_command_failed", vehicle_id=vehicle_id, topic=topic, error=str(exc)
            )
            raise ServiceUnavailableError(message="Kafka 不可用，回滚指令下发失败") from exc
        logger.info(
            "ota_rollback_command_sent",
            vehicle_id=vehicle_id,
            task_id=str(task_id),
            command_id=str(command_id),
            topic=topic,
        )
        return command_id


__all__ = ["COMMAND_TYPE_OTA_ROLLBACK", "RollbackCommandProducer"]
