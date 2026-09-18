"""sensor_file Topic 生产者（Kafka 契约：key=vehicle_id，acks=all）。

消息载荷遵循 contracts/kafka/schemas/sensor_file.schema.json（不可增删字段）：
required = [vehicle_id, timestamp, bucket, object_key, data_type, size_bytes, md5, sha256]。
"""
from __future__ import annotations

import json
from typing import Any

from confluent_kafka import KafkaException
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.kafka.producer import KafkaProducerManager
from structlog import get_logger

from app.config import settings

logger = get_logger(service="data-collector")


class SensorFileProducer:
    """sensor_file 通知生产者（complete 校验通过后发布，供 data-analytics 消费）。

    KafkaProducerManager 为进程级单例（hunter_common，lifespan 中 initialize）；
    测试可替换单例或注入替身。
    """

    async def publish(
        self,
        vehicle_id: str,
        timestamp: float,
        bucket: str,
        object_key: str,
        data_type: str,
        size_bytes: int,
        md5: str,
        sha256: str,
    ) -> None:
        """发布上传完成通知（delivery 回调确认投递成功后才返回，含失败重试）。"""
        payload: dict[str, Any] = {
            "vehicle_id": vehicle_id,
            "timestamp": timestamp,
            "bucket": bucket,
            "object_key": object_key,
            "data_type": data_type,
            "size_bytes": size_bytes,
            "md5": md5,
            "sha256": sha256,
        }
        try:
            await KafkaProducerManager.instance().produce(
                topic=settings.sensor_file_topic,
                value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                key=vehicle_id.encode("utf-8"),
            )
        except KafkaException as exc:  # Kafka 链路故障：返回 5001（5000 仅未预期异常）
            logger.error(
                "sensor_file_publish_failed",
                vehicle_id=vehicle_id,
                topic=settings.sensor_file_topic,
                object_key=object_key,
                error=str(exc),
            )
            raise ServiceUnavailableError(message="Kafka 不可用，上传完成通知发布失败") from exc


__all__ = ["SensorFileProducer"]
