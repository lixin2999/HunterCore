"""data-analytics algorithm_metrics 落库消费者单测（不依赖 Kafka/DB/Redis）。

覆盖契约 data-analytics-algorithm-metrics 的关键落库语义：
- ``to_row``：载荷 → ORM 行（time epoch 秒/毫秒 → datetime UTC；module → 受控 MetricModule；
  非法 module/非有限值/缺失必填 → 丢弃）；
- ``AlgorithmMetricsIngest``：缓冲累积、达阈值自动冲刷、批次冲刷幂等、写库失败重投（保留缓冲）；
- 消费者幂等键 = 表主键 ``(vehicle_id, time, module, metric_name)``（契约声明）；
- ``_handle``：非 JSON 对象抛错（→ 重试耗尽进 DLQ），合法载荷委托落库缓冲。
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from hunter_common.database.enums import MetricModule

from app.config import Settings
from app.consumers.algorithm_metrics import AlgorithmMetricsConsumer
from app.services.algorithm_metrics_ingest import AlgorithmMetricsIngest, to_row


def make_settings(**overrides: Any) -> Settings:
    return Settings(environment="test", **overrides)


def payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "vehicle_id": "HUNTER-001",
        "module": "perception",
        "metric_name": "fps",
        "metric_value": 28.5,
        "time": 1724035260.0,
        "tags": {"window": "60s", "agg": "avg"},
    }
    base.update(overrides)
    return base


class FakeWriter:
    """AlgorithmMetricsWriter 替身：记录每批行，可配置写库失败。"""

    def __init__(self, *, fail: bool = False) -> None:
        self.batches: list[list[dict[str, Any]]] = []
        self.fail = fail

    async def insert(self, rows: list[dict[str, Any]]) -> int:
        if self.fail:
            raise RuntimeError("db unavailable")
        self.batches.append(list(rows))
        return len(rows)


# =====================================================================
# to_row：载荷 → ORM 行
# =====================================================================


def test_to_row_maps_columns_and_types() -> None:
    row = to_row(payload())
    assert set(row) == {"time", "vehicle_id", "module", "metric_name", "metric_value", "tags"}
    assert row["module"] is MetricModule.PERCEPTION
    assert row["vehicle_id"] == "HUNTER-001"
    assert row["metric_name"] == "fps"
    assert row["metric_value"] == pytest.approx(28.5)
    assert row["time"] == datetime.fromtimestamp(1724035260.0, tz=UTC)
    assert row["tags"] == {"window": "60s", "agg": "avg"}


def test_to_row_normalizes_millisecond_time() -> None:
    row = to_row(payload(time=1724035260000.0))
    assert row["time"] == datetime.fromtimestamp(1724035260.0, tz=UTC)


def test_to_row_defaults_tags_to_empty_dict() -> None:
    row = to_row({k: v for k, v in payload().items() if k != "tags"})
    assert row["tags"] == {}


@pytest.mark.parametrize(
    "bad",
    [
        payload(module="localization"),  # 非受控词表（契约枚举仅 perception/planning/control）
        payload(module=""),
        payload(metric_value=float("nan")),
        payload(metric_value=float("inf")),
        payload(metric_value=True),  # bool 不算数值
        payload(metric_value="28.5"),
        payload(vehicle_id=""),
        payload(metric_name=""),
        payload(time=None),
        payload(time="abc"),
    ],
)
def test_to_row_rejects_invalid(bad: dict[str, Any]) -> None:
    assert to_row(bad) is None


# =====================================================================
# AlgorithmMetricsIngest：缓冲 + 冲刷 + 重投
# =====================================================================


async def test_ingest_buffers_until_flush() -> None:
    writer = FakeWriter()
    ingest = AlgorithmMetricsIngest(writer, make_settings(algorithm_metrics_flush_batch_size=10))
    assert await ingest.handle(payload(metric_name="fps")) is True
    assert await ingest.handle(payload(metric_name="latency_ms")) is True
    assert ingest.pending == 2
    assert writer.batches == []  # 未达阈值，尚未落库

    flushed = await ingest.flush()
    assert flushed == 2
    assert ingest.pending == 0
    assert len(writer.batches) == 1
    assert [row["metric_name"] for row in writer.batches[0]] == ["fps", "latency_ms"]
    assert ingest.stats.inserted == 2


async def test_ingest_drops_invalid_without_buffering() -> None:
    writer = FakeWriter()
    ingest = AlgorithmMetricsIngest(writer, make_settings())
    assert await ingest.handle(payload(module="bogus")) is False
    assert ingest.pending == 0
    assert ingest.stats.dropped == 1
    assert ingest.stats.accepted == 0


async def test_ingest_auto_flush_at_threshold() -> None:
    writer = FakeWriter()
    ingest = AlgorithmMetricsIngest(writer, make_settings(algorithm_metrics_flush_batch_size=2))
    await ingest.handle(payload(metric_name="fps"))
    assert ingest.pending == 1
    await ingest.handle(payload(metric_name="fps_min"))
    assert ingest.pending == 0  # 达阈值自动冲刷
    assert len(writer.batches) == 1
    assert len(writer.batches[0]) == 2


async def test_ingest_flush_empty_is_noop() -> None:
    writer = FakeWriter()
    ingest = AlgorithmMetricsIngest(writer, make_settings())
    assert await ingest.flush() == 0
    assert writer.batches == []


async def test_ingest_requeues_on_write_failure_for_offset_retention() -> None:
    """写库失败：flush 抛错（消费者批次钩子据此跳过 offset 提交）+ 缓冲回填不丢数据。"""
    writer = FakeWriter(fail=True)
    ingest = AlgorithmMetricsIngest(writer, make_settings(algorithm_metrics_flush_batch_size=10))
    await ingest.handle(payload(metric_name="fps"))
    assert ingest.pending == 1

    with pytest.raises(RuntimeError):
        await ingest.flush()
    assert ingest.pending == 1  # 重投保留

    writer.fail = False  # 依赖恢复：钩子重试成功
    assert await ingest.flush() == 1
    assert ingest.pending == 0
    assert len(writer.batches) == 1


# =====================================================================
# 消费者：幂等键 + 处理入口
# =====================================================================


def test_idempotency_key_is_table_primary_key() -> None:
    key_fn = AlgorithmMetricsConsumer.idempotency_key
    same = payload()
    assert key_fn(None, same) == key_fn(None, dict(same))
    # 任一主键列不同 → 键不同（幂等仅去重完全相同的指标点）
    assert key_fn(None, payload()) != key_fn(None, payload(module="control"))
    assert key_fn(None, payload()) != key_fn(None, payload(metric_name="fps_min"))
    assert key_fn(None, payload()) != key_fn(None, payload(time=1724035320.0))


async def test_handle_delegates_valid_payload_to_ingest() -> None:
    writer = FakeWriter()
    ingest = AlgorithmMetricsIngest(writer, make_settings(algorithm_metrics_flush_batch_size=10))
    consumer = object.__new__(AlgorithmMetricsConsumer)  # 跳过 Kafka 构造，仅验证委托
    consumer._ingest = ingest
    message = SimpleNamespace(topic=lambda: "algorithm_metrics")

    await consumer._handle(message, payload(metric_name="fps"))
    assert ingest.pending == 1


async def test_handle_rejects_non_object_payload() -> None:
    writer = FakeWriter()
    ingest = AlgorithmMetricsIngest(writer, make_settings())
    consumer = object.__new__(AlgorithmMetricsConsumer)
    consumer._ingest = ingest
    message = SimpleNamespace(topic=lambda: "algorithm_metrics")

    with pytest.raises(TypeError):
        await consumer._handle(message, ["not", "an", "object"])
    assert ingest.pending == 0
