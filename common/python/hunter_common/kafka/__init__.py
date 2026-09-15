"""Kafka 异步封装（confluent-kafka 2.x）。

- KafkaProducerManager：单例生产者，非阻塞投递，SASL_SSL 支持
- KafkaConsumerManager：手动提交 offset（处理成功后 commit），异常消息进 DLQ
- 安全约束：车端链路必须 SASL_SSL + SCRAM-SHA-512，消息 key = vehicle_id 保证单车辆有序
"""
from __future__ import annotations

from hunter_common.kafka.consumer import KafkaConsumerManager, MessageHandler
from hunter_common.kafka.producer import KafkaProducerManager

__all__ = ["KafkaConsumerManager", "KafkaProducerManager", "MessageHandler"]
