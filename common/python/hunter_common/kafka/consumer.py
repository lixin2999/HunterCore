"""Kafka 异步消费者封装（confluent-kafka 2.x）。

- 手动提交 offset：消息处理成功后才 commit（at-least-once 语义）
- consume() 阻塞调用放入线程池执行，不阻塞事件循环
- 处理失败的消息自动进入死信队列（DLQ，命名 {topic}.dlq，上线前需在契约中登记）
- 消息处理必须幂等（开发规则：Kafka 消费幂等）
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from hunter_common.config import HunterBaseConfig
from hunter_common.logging import get_logger

logger = get_logger("hunter_common.kafka.consumer")

#: 消息处理函数签名：(原始 Message, 解码后的值) -> None
MessageHandler = Callable[[Message, Any], Awaitable[None]]


class KafkaConsumerManager:
    """Kafka 消费者封装（手动提交 + DLQ）。"""

    def __init__(
        self,
        config: HunterBaseConfig,
        group_id: str,
        topics: list[str],
        *,
        dlq_enabled: bool = True,
        poll_timeout: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._config = config
        self._consumer = Consumer(self._build_conf(config, group_id))
        self._group_id = group_id
        self._topics = topics
        self._dlq_enabled = dlq_enabled
        self._poll_timeout = poll_timeout
        self._batch_size = batch_size
        self._running = False

    # ---------- 配置 ----------

    @staticmethod
    def _build_conf(config: HunterBaseConfig, group_id: str) -> dict[str, Any]:
        """构建 confluent-kafka 消费者配置（含 SASL_SSL 支持）。"""
        conf: dict[str, Any] = {
            "bootstrap.servers": ",".join(config.kafka_bootstrap_servers_list),
            "group.id": group_id,
            "security.protocol": config.kafka_security_protocol,
            "enable.auto.commit": False,   # 手动提交：处理成功后 commit（开发规则）
            "auto.offset.reset": "earliest",
            "client.id": config.service_name,
        }
        if config.kafka_security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            conf.update(
                {
                    "sasl.mechanisms": config.kafka_sasl_mechanism,
                    "sasl.username": config.kafka_sasl_username,
                    "sasl.password": config.kafka_sasl_password,
                }
            )
        if config.kafka_security_protocol in ("SSL", "SASL_SSL") and config.kafka_ssl_cafile:
            conf["ssl.ca.location"] = config.kafka_ssl_cafile
        return conf

    # ---------- 消费主循环 ----------

    async def run(self, handler: MessageHandler) -> None:
        """消费主循环；单条消息处理异常不中断消费（转入 DLQ）。

        Args:
            handler: 异步处理函数，参数为 (Message, 解码后的值)；
                     解码策略为优先 JSON，失败回退原始 bytes。
        """
        self._consumer.subscribe(self._topics)
        self._running = True
        loop = asyncio.get_running_loop()
        logger.info("kafka_consumer_started", topics=self._topics, group_id=self._group_id)

        while self._running:
            # consume() 为阻塞调用，放入线程池避免阻塞事件循环（异步优先约束）
            messages = await loop.run_in_executor(
                None, self._consumer.consume, self._batch_size, self._poll_timeout
            )
            for message in messages:
                if message.error() is not None:
                    err = message.error()
                    assert err is not None
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("kafka_message_error", code=err.code(), error=str(err))
                    continue
                try:
                    await handler(message, self._decode(message))
                except Exception:
                    logger.exception(
                        "kafka_message_process_failed",
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                    if self._dlq_enabled:
                        await self._send_to_dlq(message)
            if messages:
                # 批次处理完成（含 DLQ 转投）后手动提交 offset
                try:
                    self._consumer.commit(asynchronous=False)
                except KafkaException as exc:
                    logger.error("kafka_commit_failed", error=str(exc))
        logger.info("kafka_consumer_stopped", topics=self._topics)

# ---------- 内部工具 ----------

    @staticmethod
    def _decode(message: Message) -> Any:
        """优先 JSON 解码，失败则返回原始 bytes（由 handler 自行处理）。"""
        raw = message.value()
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw

    def _dlq_topic(self, original_topic: str) -> str:
        return f"{original_topic}.dlq"

    async def _send_to_dlq(self, message: Message) -> None:
        """将处理失败的消息转投死信队列（保留原 topic/partition/offset 溯源信息）。"""
        # 延迟导入避免循环依赖
        from hunter_common.kafka.producer import KafkaProducerManager

        try:
            producer = KafkaProducerManager.instance()
        except RuntimeError:
            # 生产者未初始化：仅记录日志，避免 DLQ 转投失败掩盖原始业务异常
            logger.error("dlq_producer_not_initialized", topic=message.topic())
            return
        headers: list[tuple[str, bytes]] = [
            ("dlq.original.topic", str(message.topic()).encode()),
            ("dlq.partition", str(message.partition()).encode()),
            ("dlq.offset", str(message.offset()).encode()),
        ]
        try:
            await producer.produce(
                self._dlq_topic(str(message.topic())),
                value=message.value(),
                key=message.key(),
                headers=headers,
            )
            logger.warning(
                "kafka_message_sent_to_dlq",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )
        except KafkaException:
            logger.exception("kafka_dlq_produce_failed", topic=message.topic())

