"""hunter.{vehicle_id}.ota_notify 生产者（Kafka 契约：key=vehicle_id，acks=all）。

消息载荷遵循 contracts/kafka/schemas/ota_notify.schema.json（不可增删字段）：
required = [vehicle_id, timestamp, task_id, version_name, version_code, package_url,
package_size, package_md5, package_sha256, signature]。
package_url 使用**发送时刻**签发的 1 小时预签名地址（禁止下发长期凭证或密钥）。
"""
from __future__ import annotations

import json
import time
from typing import Any

from confluent_kafka import KafkaException
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.kafka.producer import KafkaProducerManager
from structlog import get_logger

from app.config import settings

logger = get_logger(service="ota-service")


class OtaNotifyProducer:
    """OTA 升级通知生产者（start/调度器放行每一批时逐车下发）。"""

    def __init__(self, producer: KafkaProducerManager | None = None) -> None:
        self._producer = producer

    def _manager(self) -> KafkaProducerManager:
        """单例获取（测试可注入替身；未初始化抛 RuntimeError → 5001）。"""
        if self._producer is not None:
            return self._producer
        return KafkaProducerManager.instance()

    async def send(
        self,
        *,
        vehicle_id: str,
        task_id: str,
        version_name: str,
        version_code: int,
        package_url: str,
        package_size: int,
        package_md5: str,
        package_sha256: str,
        signature: str,
        changelog: dict[str, Any] | None = None,
        preconditions: dict[str, Any] | None = None,
    ) -> None:
        """下发升级通知（delivery 回调确认投递成功后才返回）。"""
        payload: dict[str, Any] = {
            "vehicle_id": vehicle_id,
            "timestamp": time.time(),
            "task_id": task_id,
            "version_name": version_name,
            "version_code": version_code,
            "package_url": package_url,
            "package_size": package_size,
            "package_md5": package_md5,
            "package_sha256": package_sha256,
            "signature": signature,
        }
        if changelog is not None:
            payload["changelog"] = changelog
        if preconditions is not None:
            payload["preconditions"] = preconditions
        topic = settings.ota_notify_topic_pattern.format(vehicle_id=vehicle_id)
        try:
            await self._manager().produce(
                topic=topic,
                value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                key=vehicle_id.encode("utf-8"),
            )
        except KafkaException as exc:  # Kafka 链路故障：5001（5000 仅未预期异常）
            logger.error(
                "ota_notify_publish_failed", vehicle_id=vehicle_id, topic=topic, error=str(exc)
            )
            raise ServiceUnavailableError(message="Kafka 不可用，升级通知下发失败") from exc


__all__ = ["OtaNotifyProducer"]
