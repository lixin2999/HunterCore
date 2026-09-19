"""Kafka 异步封装（confluent-kafka 2.x）。契约驱动（``contracts/kafka/`` 为单一事实来源）：

- ``KafkaProducerManager``：单例生产者；按契约 acks 投递、key=vehicle_id 强制、
  重试 + 指数退避、链路中断时本地磁盘缓冲（1GB）并可重投
- ``KafkaConsumerManager``：手动提交 offset（处理成功后 commit）、契约 Schema 校验（非法消息进 DLQ）、
  handler 重试 + DLQ、幂等守卫、消费积压指标
- ``contracts``：topics.yaml / consumer-groups.yaml / schemas/*.schema.json 运行时加载与校验
- ``messages``：KafkaRecord 编解码（契约 Schema 校验 + key 规则）
- ``buffer``：本地磁盘缓冲（分段 JSONL + 容量淘汰 + 崩溃安全重投）
- ``idempotency``：消费幂等守卫（LRU + TTL，可注入分布式存储）
- ``metrics``：Kafka 链路 Prometheus 指标（前缀 hunter_kafka_）

导入策略：kafka 包根仅导出核心生产/消费类；契约/消息/缓冲/幂等/指标按需显式导入
（``from hunter_common.kafka.contracts import get_contract``），保持核心导入轻量
（yaml/jsonschema 仅在契约模块使用时加载）。
安全约束：车端链路必须 SASL_SSL + SCRAM-SHA-512，消息 key = vehicle_id 保证单车辆有序。
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hunter_common.kafka.consumer import (
    SCHEMA_AUTO,
    IdempotencyKeyFn,
    KafkaConsumerManager,
    MessageHandler,
)
from hunter_common.kafka.producer import KafkaProducerManager, ProduceResult, ProduceStatus

if TYPE_CHECKING:  # pragma: no cover - 仅类型检查期导入，避免运行期拉起 yaml/jsonschema
    from hunter_common.kafka.buffer import BufferedRecord, BufferStats, LocalDiskBuffer
    from hunter_common.kafka.contracts import (
        ConsumerGroupSpec,
        KafkaContract,
        KafkaContractError,
        KafkaMessageSchemaError,
        TopicSpec,
        get_contract,
        reset_contract_cache,
    )
    from hunter_common.kafka.idempotency import IdempotencyGuard, IdempotencyStore
    from hunter_common.kafka.messages import KafkaRecord, build_record, parse_payload

__all__ = [
    "SCHEMA_AUTO",
    "BufferStats",
    "BufferedRecord",
    "ConsumerGroupSpec",
    "IdempotencyGuard",
    "IdempotencyKeyFn",
    "IdempotencyStore",
    "KafkaConsumerManager",
    "KafkaContract",
    "KafkaContractError",
    "KafkaMessageSchemaError",
    "KafkaProducerManager",
    "KafkaRecord",
    "LocalDiskBuffer",
    "MessageHandler",
    "ProduceResult",
    "ProduceStatus",
    "TopicSpec",
    "build_record",
    "get_contract",
    "parse_payload",
    "reset_contract_cache",
]

#: 惰性导出的可选成员（模块名 → 成员名元组），按需导入保持 kafka 包根轻量
_LAZY_EXPORTS: dict[str, tuple[str, ...]] = {
    "hunter_common.kafka.buffer": ("BufferedRecord", "BufferStats", "LocalDiskBuffer"),
    "hunter_common.kafka.contracts": (
        "ConsumerGroupSpec",
        "KafkaContract",
        "KafkaContractError",
        "KafkaMessageSchemaError",
        "TopicSpec",
        "get_contract",
        "reset_contract_cache",
    ),
    "hunter_common.kafka.idempotency": ("IdempotencyGuard", "IdempotencyStore"),
    "hunter_common.kafka.messages": ("KafkaRecord", "build_record", "parse_payload"),
}


def __getattr__(name: str) -> Any:
    """按需导入可选成员（PEP 562），避免核心链路承担 yaml/jsonschema 导入开销。"""
    import importlib

    for module_name, members in _LAZY_EXPORTS.items():
        if name in members:
            return getattr(importlib.import_module(module_name), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

