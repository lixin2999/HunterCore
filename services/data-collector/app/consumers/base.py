"""采集消费者公共骨架（消费组与订阅模式契约固定）。

契约：``contracts/kafka/consumer-groups.yaml``（组名/订阅/幂等键）+ ``topics.yaml``。

统一能力（来自 ``hunter_common.kafka.consumer.KafkaConsumerManager``）：
契约 Schema 校验（``schema_name="auto"``，非法消息进 ``{topic}.dlq``）→ 幂等认领 →
handler 重试 → 手动提交 offset → 消费积压指标。

⚠ 运行时依赖：``schema_name="auto"`` 需要可访问 Kafka 契约目录
（``KAFKA_CONTRACT_DIR``，K8s 由 contracts ConfigMap 挂载）——契约缺失时构造即失败，
禁止静默降级为「不校验」（契约先行原则）。
"""
from __future__ import annotations

from typing import Any, ClassVar

from confluent_kafka import Message
from hunter_common.kafka.consumer import SCHEMA_AUTO, BatchEndHook, KafkaConsumerManager
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id

from app.config import Settings

logger = get_logger("app.consumers.base")


class BaseIngestConsumer:
    """采集消费者基类（子类声明消费组/订阅模式并实现 :meth:`process`）。"""

    #: 消费组名（契约 consumer-groups.yaml，不可自行命名）
    group_id: ClassVar[str] = ""
    #: 订阅模式（车端 Topic 正则，新车上线无需改配置）
    subscribe_pattern: ClassVar[str] = ""

    def __init__(self, settings: Settings, *, on_batch_end: BatchEndHook | None = None) -> None:
        self._settings = settings
        self._guard = IdempotencyGuard(
            namespace=self.group_id,
            max_size=settings.kafka_consumer_idempotency_cache_size,
            ttl_s=settings.kafka_consumer_idempotency_ttl_s,
        )
        # 契约 Schema 校验（默认开启）：非法消息直接进 DLQ，不进入业务层
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
            topics=[self.subscribe_pattern],
            batch_size=settings.consumer_batch_size,
            poll_timeout=settings.consumer_poll_timeout_seconds,
            schema_name=schema_name,
            idempotency=self._guard,
            idempotency_key=self.idempotency_key,
            on_batch_end=on_batch_end,
        )

    async def run(self) -> None:
        """消费主循环（启停由应用 lifespan 管理）。"""
        await self._consumer.run(self._handle)

    def stop(self) -> None:
        """请求停止消费（优雅停机：当前批次处理完成后退出）。"""
        self._consumer.stop()

    @staticmethod
    def idempotency_key(message: Message, value: Any) -> str:
        """幂等键（子类按契约 consumer-groups.yaml 覆盖）。"""
        raise NotImplementedError

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        """业务处理（子类实现；抛异常 → 重试耗尽后进 DLQ）。"""
        raise NotImplementedError

    async def _handle(self, message: Message, value: Any) -> None:
        """统一入口：类型校验 + vehicle_id 日志上下文 + 委托 :meth:`process`。"""
        if not isinstance(value, dict):
            raise TypeError(f"消息必须为 JSON 对象，实际 {type(value).__name__}")
        vehicle_id = value.get("vehicle_id")
        token = set_vehicle_id(str(vehicle_id) if vehicle_id else "")
        try:
            await self.process(value, topic=str(message.topic()))
        finally:
            reset_vehicle_id(token)


__all__ = ["BaseIngestConsumer"]
