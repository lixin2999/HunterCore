"""生产者契约行为单测（Mock confluent-kafka Producer，不需要 broker）。

覆盖：契约 acks 选择 / 可重试错误指数退避 / 不可重试错误直接抛出 /
重试耗尽落盘缓冲 / 契约驱动入口（Schema + key=vehicle_id）/ 缓冲重投。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, ClassVar

import pytest
from confluent_kafka import KafkaException

from hunter_common.config import HunterBaseConfig
from hunter_common.kafka import producer as producer_module
from hunter_common.kafka.buffer import BufferedRecord
from hunter_common.kafka.contracts import KafkaContract, KafkaMessageSchemaError
from hunter_common.kafka.messages import decode_payload
from hunter_common.kafka.producer import KafkaProducerManager

ROOT = Path(__file__).resolve().parents[3]
CONTRACT_DIR = ROOT / "contracts" / "kafka"
VEHICLE_ID = "HUNTER-001"
TELEMETRY_TOPIC = f"hunter.{VEHICLE_ID}.telemetry"


class FakeError:
    """替身 KafkaError：仅需 ``code()`` / ``retriable()`` 供重试判定。"""

    def __init__(self, *, retriable: bool) -> None:
        self._retriable = retriable

    def code(self) -> int:
        return -195

    def retriable(self) -> bool:
        return self._retriable

    def __str__(self) -> str:
        return f"fake kafka error(retriable={self._retriable})"


class FakeMessage:
    """替身 delivery 消息（回传 partition/offset）。"""

    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeProducer:
    """替身生产者：按 ``outcomes`` 队列决定每次投递结果（None = 投递成功）。"""

    instances: ClassVar[list[FakeProducer]] = []

    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.produced: list[dict[str, Any]] = []
        self.outcomes: list[FakeError | None] = []
        self.flush_calls = 0
        FakeProducer.instances.append(self)

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        *,
        on_delivery: Any = None,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        partition: int | None = None,
    ) -> None:
        self.produced.append(
            {"topic": topic, "value": value, "key": key, "headers": headers, "partition": partition}
        )
        error = self.outcomes.pop(0) if self.outcomes else None
        if on_delivery is not None:
            if error is None:
                on_delivery(None, FakeMessage(partition or 0, len(self.produced) - 1))
            else:
                on_delivery(error, None)

    def poll(self, timeout: float = 0.0) -> int:
        time.sleep(0.001)  # 避免 poll 线程空转占满 CPU
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flush_calls += 1
        return 0


@pytest.fixture(scope="module")
def contract() -> KafkaContract:
    return KafkaContract.load(CONTRACT_DIR)


@pytest.fixture(autouse=True)
def patch_producer(monkeypatch: pytest.MonkeyPatch) -> type[FakeProducer]:
    """把 confluent-kafka 的 Producer 替换为替身（每个用例重置实例清单）。"""
    FakeProducer.instances = []
    monkeypatch.setattr(producer_module, "Producer", FakeProducer)
    return FakeProducer


def make_config(tmp_path: Path, **overrides: Any) -> HunterBaseConfig:
    """测试配置：缓冲目录指向 tmp_path，退避压到毫秒级以加速用例。"""
    kwargs: dict[str, Any] = {
        "service_name": "data-collector",
        "kafka_bootstrap_servers": "broker:9092",
        "kafka_local_buffer_enabled": True,
        "kafka_local_buffer_dir": str(tmp_path / "buffer"),
        "kafka_produce_max_attempts": 3,
        "kafka_produce_retry_backoff_ms": 1,
        "kafka_produce_retry_backoff_max_ms": 4,
    }
    kwargs.update(overrides)
    return HunterBaseConfig(**kwargs)


def telemetry_payload(contract: KafkaContract) -> dict[str, Any]:
    return contract.schema("telemetry")["examples"][0]


async def test_producer_instances_follow_contract_acks(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """按 Topic 契约 acks 选择生产者实例：telemetry=1 / health=0 / 其余=all。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    try:
        await manager.produce(TELEMETRY_TOPIC, b"{}", key=VEHICLE_ID.encode())
        await manager.produce(f"hunter.{VEHICLE_ID}.health", b"{}", key=VEHICLE_ID.encode())
        await manager.produce("telemetry_raw", b"{}", key=VEHICLE_ID.encode())
    finally:
        await manager.close()

    assert sorted(instance.conf["acks"] for instance in patch_producer.instances) == ["0", "1", "all"]
    assert len(patch_producer.instances) == 3


async def test_retriable_error_retries_then_delivers(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """可重试错误按配置重试；重试后成功返回 delivered 且 attempts 记录准确。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    instance = patch_producer.instances[0]
    instance.outcomes = [FakeError(retriable=True)]
    try:
        result = await manager.produce("telemetry_raw", b"{}", key=VEHICLE_ID.encode())
    finally:
        await manager.close()

    assert (result.status, result.attempts) == ("delivered", 2)
    assert len(instance.produced) == 2
    assert result.offset is not None


async def test_non_retriable_error_raises_without_buffering(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """不可重试错误（消息体非法/超长）：不重试、不落盘，直接抛出由调用方转 5001。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    instance = patch_producer.instances[0]
    instance.outcomes = [FakeError(retriable=False)]
    try:
        with pytest.raises(KafkaException):
            await manager.produce("telemetry_raw", b"{}", key=VEHICLE_ID.encode())
        stats = manager.buffer_stats()
    finally:
        await manager.close()

    assert len(instance.produced) == 1
    assert stats is not None and stats.message_count == 0


async def test_retry_exhaustion_buffers_to_local_disk(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """链路中断（重试耗尽仍为可重试错误）：消息落盘返回 buffered，不丢数据。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    instance = patch_producer.instances[0]
    instance.outcomes = [FakeError(retriable=True) for _ in range(3)]
    try:
        result = await manager.produce("telemetry_raw", b'{"seq":1}', key=VEHICLE_ID.encode())
        stats = manager.buffer_stats()
        buffered_files = list((tmp_path / "buffer" / "data-collector").glob("seg-*.jsonl"))
        buffered_bytes = buffered_files[0].stat().st_size if buffered_files else 0
    finally:
        await manager.close()

    assert (result.status, result.attempts) == ("buffered", 3)
    assert stats is not None and stats.message_count == 1
    assert buffered_bytes > 0  # 关闭时已重投并删段，故在关闭前记录大小


async def test_buffering_can_be_disabled_per_message(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """显式关闭缓冲时（DLQ 等场景）：重试耗尽直接抛出。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    instance = patch_producer.instances[0]
    instance.outcomes = [FakeError(retriable=True) for _ in range(3)]
    try:
        with pytest.raises(KafkaException):
            await manager.produce("telemetry_raw", b"{}", use_buffer=False)
        stats = manager.buffer_stats()
    finally:
        await manager.close()
    assert stats is not None and stats.message_count == 0


async def test_publish_payload_validates_schema_and_vehicle_key(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """契约驱动入口：key 与 vehicle_id 不一致 → 拒绝且不投递；正常路径按契约 acks 投递。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    payload = telemetry_payload(contract)
    try:
        with pytest.raises(KafkaMessageSchemaError):
            await manager.publish_payload(TELEMETRY_TOPIC, payload, key="HUNTER-999")
        assert all(instance.produced == [] for instance in patch_producer.instances)

        result = await manager.publish_payload(TELEMETRY_TOPIC, payload)
    finally:
        await manager.close()

    # telemetry 契约 acks=1 → 适用独立生产者实例（按需创建）
    sent = patch_producer.instances[-1].produced[-1]
    assert result.status == "delivered"
    assert sent["topic"] == TELEMETRY_TOPIC
    assert sent["key"] == payload["vehicle_id"].encode("utf-8")
    assert decode_payload(sent["value"]) == payload


async def test_replay_buffered_resends_and_clears(
    tmp_path: Path, contract: KafkaContract, patch_producer: type[FakeProducer]
) -> None:
    """链路恢复：replay_buffered 重投磁盘缓冲并清空（at-least-once）。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    assert manager.buffer is not None
    manager.buffer.append(
        BufferedRecord(topic="telemetry_raw", value=b'{"seq":9}', key=VEHICLE_ID.encode())
    )
    try:
        replayed = await manager.replay_buffered()
        stats = manager.buffer_stats()
    finally:
        await manager.close()

    assert replayed == 1
    assert stats is not None and stats.message_count == 0
    assert patch_producer.instances[0].produced[-1]["value"] == b'{"seq":9}'


def test_backoff_is_exponential_and_capped(tmp_path: Path, contract: KafkaContract) -> None:
    """退避序列：base × 2^(n-1)，且不超过 kafka_produce_retry_backoff_max_ms。"""
    manager = KafkaProducerManager(make_config(tmp_path), contract=contract)
    try:
        assert manager._backoff_seconds(1) == pytest.approx(0.001)
        assert manager._backoff_seconds(2) == pytest.approx(0.002)
        assert manager._backoff_seconds(3) == pytest.approx(0.004)
        assert manager._backoff_seconds(10) == pytest.approx(0.004)  # 上限封顶
    finally:
        manager._closed = True  # 仅验证退避计算：直接停止 poll 线程


