"""消费者契约行为单测（Mock confluent-kafka Consumer/Producer，不需要 broker）。

覆盖：契约 Schema 校验（非法消息进 DLQ 不重试）/ handler 重试与 DLQ 转投 /
手动提交 offset / 幂等跳过 / 消费积压指标 / 解码回退。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import TopicPartition

from hunter_common.config import HunterBaseConfig
from hunter_common.kafka import consumer as consumer_module
from hunter_common.kafka.consumer import SCHEMA_AUTO, KafkaConsumerManager
from hunter_common.kafka.contracts import (
    KafkaContract,
    KafkaContractError,
)
from hunter_common.kafka.idempotency import IdempotencyGuard
from hunter_common.kafka.messages import encode_payload
from hunter_common.metrics import REGISTRY

ROOT = Path(__file__).resolve().parents[3]
CONTRACT_DIR = ROOT / "contracts" / "kafka"
VEHICLE_ID = "HUNTER-001"
CONSUMED_TOPIC = f"hunter.{VEHICLE_ID}.telemetry"
GROUP_ID = "data-collector-telemetry"
SERVICE = "data-collector"


class StopConsume(RuntimeError):
    """测试用：批次耗尽后终止消费循环（等价于进程停止）。"""


class FakeMessage:
    """替身 Kafka 消息（仅实现消费侧使用的方法）。"""

    def __init__(
        self,
        topic: str,
        value: bytes | None,
        *,
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = b"HUNTER-001",
    ) -> None:
        self._topic = topic
        self._value = value
        self._partition = partition
        self._offset = offset
        self._key = key

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    """替身消费者：按批次吐出消息，批次耗尽后抛 StopConsume 终止循环。"""

    def __init__(
        self,
        batches: list[list[FakeMessage]],
        *,
        assignment: list[TopicPartition] | None = None,
        high_watermark: int = 10,
        position: int = 4,
    ) -> None:
        self._batches = list(batches)
        self._assignment = assignment or []
        self._high = high_watermark
        self._position = position
        self.subscribed: list[str] = []
        self.commit_calls = 0
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        if self._batches:
            return self._batches.pop(0)
        raise StopConsume

    def commit(self, asynchronous: bool = False) -> None:
        self.commit_calls += 1

    def assignment(self) -> list[TopicPartition]:
        return self._assignment

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = 1.0, cached: bool = False
    ) -> tuple[int, int]:
        return 0, self._high

    def position(self, partitions: list[TopicPartition]) -> list[int]:
        """与 confluent-kafka 一致：传分区列表返回位点列表。"""
        return [self._position for _ in partitions]

    def close(self) -> None:
        self.closed = True


class FakeProducerManager:
    """替身生产者：仅记录 DLQ 投递。"""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def produce(
        self,
        topic: str,
        value: bytes | None = None,
        *,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.sent.append({"topic": topic, "value": value, "key": key, "headers": headers or []})


@pytest.fixture(scope="module")
def contract() -> KafkaContract:
    return KafkaContract.load(CONTRACT_DIR)


def make_config(**overrides: Any) -> HunterBaseConfig:
    kwargs: dict[str, Any] = {
        "service_name": SERVICE,
        "kafka_bootstrap_servers": "broker:9092",
        "kafka_consumer_max_attempts": 2,
        "kafka_consumer_retry_backoff_ms": 1,
        "kafka_local_buffer_enabled": False,  # 消费者用例不涉及生产者缓冲
    }
    kwargs.update(overrides)
    return HunterBaseConfig(**kwargs)


def telemetry_message(contract: KafkaContract, **overrides: Any) -> FakeMessage:
    payload = dict(contract.schema("telemetry")["examples"][0])
    payload.update(overrides)
    return FakeMessage(CONSUMED_TOPIC, encode_payload(payload))


def invalid_telemetry_message() -> FakeMessage:
    """缺必填 chassis 段 → 契约校验失败。"""
    return FakeMessage(CONSUMED_TOPIC, encode_payload({"vehicle_id": VEHICLE_ID, "timestamp": 1.0}))


def sample(name: str, **labels: str) -> float:
    """读取 Prometheus 指标样本值（未注册记 0；计数器为进程累计，断言请用差值）。"""
    return REGISTRY.get_sample_value(name, labels) or 0.0


def consumed_counter(status: str) -> float:
    """消费结果计数器（按 status 分别统计）。"""
    return sample(
        "hunter_kafka_messages_consumed_total",
        service=SERVICE,
        group=GROUP_ID,
        topic=CONSUMED_TOPIC,
        status=status,
    )


def build_manager(
    contract: KafkaContract,
    batches: list[list[FakeMessage]],
    *,
    producer: FakeProducerManager | None = None,
    schema_name: str | None = "telemetry",
    idempotency: IdempotencyGuard | None = None,
    idempotency_key: Any = None,
    assignment: list[TopicPartition] | None = None,
    **config_overrides: Any,
) -> tuple[KafkaConsumerManager, FakeConsumer, FakeProducerManager]:
    fake_producer = producer or FakeProducerManager()
    fake_consumer = FakeConsumer(
        batches, assignment=assignment or [TopicPartition(CONSUMED_TOPIC, 0)]
    )
    manager = KafkaConsumerManager(
        make_config(**config_overrides),
        GROUP_ID,
        ["hunter.*.telemetry"],
        schema_name=schema_name,
        idempotency=idempotency,
        idempotency_key=idempotency_key,
        producer_manager=fake_producer,
        contract=contract,
        consumer=fake_consumer,
    )
    return manager, fake_consumer, fake_producer


async def test_batch_success_commits_offset_and_closes(
    contract: KafkaContract,
) -> None:
    """正常路径：handler 收到解码后消息、批次后手动提交 offset、停机关闭消费者。"""
    before_processed = consumed_counter("processed")
    manager, fake_consumer, _ = build_manager(contract, [[telemetry_message(contract)]])
    handled: list[Any] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        handled.append(value)

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert fake_consumer.subscribed == ["hunter.*.telemetry"]
    assert handled == [contract.schema("telemetry")["examples"][0]]
    assert fake_consumer.commit_calls == 1
    assert fake_consumer.closed is True
    assert consumed_counter("processed") - before_processed == 1.0


async def test_handler_retry_then_success(contract: KafkaContract) -> None:
    """handler 失败按配置重试；重试成功不进 DLQ。"""
    before_processed = consumed_counter("processed")
    manager, _, fake_producer = build_manager(
        contract, [[telemetry_message(contract)]], kafka_consumer_max_attempts=3
    )
    attempts: list[int] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("下游暂时不可用")

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert len(attempts) == 3
    assert fake_producer.sent == []
    assert consumed_counter("processed") - before_processed == 1.0


async def test_handler_failure_exhausts_retries_then_dlq(contract: KafkaContract) -> None:
    """handler 连续失败：重试耗尽后转投 {topic}.dlq（reason=handler_error）且 offset 仍提交。"""
    before_dlq = sample(
        "hunter_kafka_dlq_messages_total",
        service=SERVICE,
        group=GROUP_ID,
        topic=CONSUMED_TOPIC,
        reason="handler_error",
    )
    manager, fake_consumer, fake_producer = build_manager(
        contract, [[telemetry_message(contract)]], kafka_consumer_max_attempts=2
    )
    attempts: list[int] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        attempts.append(1)
        raise RuntimeError("永久失败")

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert len(attempts) == 2
    assert fake_consumer.commit_calls == 1  # 毒消息不可阻塞分区（DLQ 后提交）
    assert len(fake_producer.sent) == 1
    dlq = fake_producer.sent[0]
    assert dlq["topic"] == f"{CONSUMED_TOPIC}.dlq"
    assert dict(dlq["headers"])["dlq.reason"] == b"handler_error"
    assert dict(dlq["headers"])["dlq.original.topic"] == CONSUMED_TOPIC.encode()
    assert (
        sample(
            "hunter_kafka_dlq_messages_total",
            service=SERVICE,
            group=GROUP_ID,
            topic=CONSUMED_TOPIC,
            reason="handler_error",
        )
        - before_dlq
        == 1.0
    )


async def test_schema_invalid_message_dlq_without_handler(contract: KafkaContract) -> None:
    """契约校验失败：不进 handler（不重试），直接 DLQ（reason=schema_invalid）。"""
    before_invalid = consumed_counter("schema_invalid")
    manager, _, fake_producer = build_manager(contract, [[invalid_telemetry_message()]])
    called: list[int] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        called.append(1)

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert called == []
    assert fake_producer.sent[0]["topic"] == f"{CONSUMED_TOPIC}.dlq"
    assert dict(fake_producer.sent[0]["headers"])["dlq.reason"] == b"schema_invalid"
    assert consumed_counter("schema_invalid") - before_invalid == 1.0


async def test_schema_auto_resolves_per_message(contract: KafkaContract) -> None:
    """auto 模式：同一批次内按消息实际 Topic 解析 Schema（合法处理、非法进 DLQ）。"""
    manager, _, fake_producer = build_manager(
        contract,
        [[telemetry_message(contract), invalid_telemetry_message()]],
        schema_name=SCHEMA_AUTO,
    )
    handled: list[Any] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        handled.append(value)

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert len(handled) == 1
    assert len(fake_producer.sent) == 1
    assert dict(fake_producer.sent[0]["headers"])["dlq.reason"] == b"schema_invalid"


async def test_duplicate_message_skipped_by_idempotency(contract: KafkaContract) -> None:
    """幂等守卫：同一 (vehicle_id, seq) 的重复消息跳过 handler（不产生副作用）。"""
    guard = IdempotencyGuard(namespace=GROUP_ID)

    def key_fn(message: FakeMessage, value: Any) -> str:
        return IdempotencyGuard.compose(value["vehicle_id"], value["seq"])

    before_skipped = consumed_counter("skipped_duplicate")
    manager, _, _ = build_manager(
        contract,
        [[telemetry_message(contract), telemetry_message(contract)]],
        idempotency=guard,
        idempotency_key=key_fn,
    )
    handled: list[Any] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        handled.append(value)

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert len(handled) == 1
    assert consumed_counter("skipped_duplicate") - before_skipped == 1.0


async def test_consumer_lag_metric_refreshed(contract: KafkaContract) -> None:
    """批次处理完刷新消费积压指标（高水位 10 - 位点 4 = 6，按分区）。"""
    manager, _, _ = build_manager(contract, [[telemetry_message(contract)]])

    async def handler(message: FakeMessage, value: Any) -> None:
        return None

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert sample(
        "hunter_kafka_consumer_lag",
        service=SERVICE,
        group=GROUP_ID,
        topic=CONSUMED_TOPIC,
        partition="0",
    ) == 6.0


def test_unknown_schema_name_fails_fast(contract: KafkaContract) -> None:
    """显式声明不存在的 Schema：构造即失败（禁止静默跳过校验）。"""
    with pytest.raises(KafkaContractError):
        KafkaConsumerManager(
            make_config(),
            GROUP_ID,
            ["hunter.*.telemetry"],
            schema_name="not_a_schema",
            contract=contract,
            consumer=FakeConsumer([]),
        )


def test_schema_auto_requires_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """auto 模式在契约不可用时必须 fail fast。"""
    monkeypatch.setattr(consumer_module, "get_contract", lambda **_kwargs: None)
    with pytest.raises(KafkaContractError):
        KafkaConsumerManager(
            make_config(),
            GROUP_ID,
            ["hunter.*.telemetry"],
            schema_name=SCHEMA_AUTO,
            consumer=FakeConsumer([]),
        )


async def test_decode_fallback_without_schema(contract: KafkaContract) -> None:
    """未启用契约校验：非 JSON 消息回退原始 bytes 交给 handler。"""
    raw = FakeMessage(CONSUMED_TOPIC, b"\x00\x01binary")
    manager, _, _ = build_manager(contract, [[raw]], schema_name=None)
    handled: list[Any] = []

    async def handler(message: FakeMessage, value: Any) -> None:
        handled.append(value)

    with pytest.raises(StopConsume):
        await manager.run(handler)

    assert handled == [b"\x00\x01binary"]



