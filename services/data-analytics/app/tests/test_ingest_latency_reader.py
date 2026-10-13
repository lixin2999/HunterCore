"""``IngestLatencyRedisReader`` 单测：读 Flink data_quality_monitor 写入的 Redis 指标键。

契约（pending #10 结案：Kafka 时间戳差 → Flink 流式 → Redis 指标键 analytics:ingest_latency）：
- 本服务只读该键，任何异常/缺失/不可解析路径均降级为 None（绝不展示陈旧值、不向请求路径抛出）；
- 仅当载荷为含数值 ``p95_ms`` 的 JSON 对象时才返回 float。

V1.19.21：同一个看板 `-` 背后至少有四种故障（没提交 / 没消费到 / 样本全被丢弃 / 正在回放），
``read()`` 必须把它们分开说：真值 / 带 reason 的丢弃诊断 / 键不存在 / 降级（各自独立可断言）。
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from app.repositories.pipeline import IngestLatencyReading, IngestLatencyRedisReader


class _FakeRedis:
    """最小 Redis 替身：按预设返回原始值或抛异常。"""

    def __init__(self, *, value: Any = None, error: Exception | None = None) -> None:
        self._value = value
        self._error = error
        self.gets: list[str] = []

    async def get(self, key: str) -> Any:
        self.gets.append(key)
        if self._error is not None:
            raise self._error
        return self._value


@pytest.mark.asyncio
async def test_reads_p95_from_json_str() -> None:
    payload = json.dumps({"metric": "ingest_latency_ms_p95", "p95_ms": 1234.5, "count": 42})
    reader = IngestLatencyRedisReader(_FakeRedis(value=payload))
    assert await reader.p95_ms() == pytest.approx(1234.5)


@pytest.mark.asyncio
async def test_reads_p95_from_bytes() -> None:
    payload = json.dumps({"p95_ms": 88.0}).encode("utf-8")
    reader = IngestLatencyRedisReader(_FakeRedis(value=payload))
    assert await reader.p95_ms() == pytest.approx(88.0)


@pytest.mark.asyncio
async def test_uses_contract_key() -> None:
    fake = _FakeRedis(value=json.dumps({"p95_ms": 1.0}))
    await IngestLatencyRedisReader(fake).p95_ms()
    assert fake.gets == [IngestLatencyRedisReader.KEY]


@pytest.mark.asyncio
async def test_no_client_returns_none() -> None:
    assert await IngestLatencyRedisReader(None).p95_ms() is None


@pytest.mark.asyncio
async def test_missing_key_returns_none() -> None:
    assert await IngestLatencyRedisReader(_FakeRedis(value=None)).p95_ms() is None


@pytest.mark.asyncio
async def test_unparsable_json_returns_none() -> None:
    assert await IngestLatencyRedisReader(_FakeRedis(value="not-json")).p95_ms() is None


@pytest.mark.asyncio
async def test_non_numeric_p95_returns_none() -> None:
    assert await IngestLatencyRedisReader(_FakeRedis(value=json.dumps({"p95_ms": "x"}))).p95_ms() is None
    # 布尔不是合法延迟值（不得当 0/1 处理）
    assert await IngestLatencyRedisReader(_FakeRedis(value=json.dumps({"p95_ms": True}))).p95_ms() is None
    # 非对象载荷（数组/标量）
    assert await IngestLatencyRedisReader(_FakeRedis(value=json.dumps([1, 2]))).p95_ms() is None


@pytest.mark.asyncio
async def test_redis_error_degrades_to_none() -> None:
    reader = IngestLatencyRedisReader(_FakeRedis(error=ConnectionError("redis down")))
    assert await reader.p95_ms() is None


def test_reader_key_uses_shared_constant() -> None:
    """读端键 = 共享常量 = 契约模式（与 Flink 写端同源，杜绝拼写漂移致看板恒 null）。"""
    from hunter_common import redis_keys

    assert IngestLatencyRedisReader.KEY == redis_keys.ANALYTICS_INGEST_LATENCY
    assert IngestLatencyRedisReader.KEY == "analytics:ingest_latency"


# =====================================================================
# read() 四分支（V1.19.21：看板不再把四种成因读成同一个 `-`）
# =====================================================================


@pytest.mark.asyncio
async def test_read_real_p95_carries_no_reason() -> None:
    payload = json.dumps({"metric": "ingest_latency_ms_p95", "p95_ms": 1234.5, "count": 42})
    reading = await IngestLatencyRedisReader(_FakeRedis(value=payload)).read()
    assert reading == IngestLatencyReading(p95_ms=1234.5, reason=None, key_present=True)


@pytest.mark.asyncio
async def test_read_diagnostic_payload_translates_reason_and_counts() -> None:
    payload = json.dumps(
        {
            "metric": "ingest_latency_ms_p95",
            "p95_ms": None,
            "count": 0,
            "reason": "negative_latency",
            "seen": 12,
            "dropped": {"negative_latency": 12},
        }
    )
    reading = await IngestLatencyRedisReader(_FakeRedis(value=payload)).read()
    assert reading.p95_ms is None
    assert reading.key_present is True
    assert reading.reason is not None
    assert "时钟超前" in reading.reason  # 受控词表已翻成人话
    assert "seen=12" in reading.reason and "negative_latency=12" in reading.reason


@pytest.mark.asyncio
async def test_read_key_without_reason_says_writer_is_old() -> None:
    # 旧版写端（V1.19.21 之前）只有 p95_ms=null 或形状不符的载荷：不得猜成因
    reading = await IngestLatencyRedisReader(_FakeRedis(value=json.dumps({"count": 0}))).read()
    assert reading.p95_ms is None and reading.key_present is True
    assert reading.reason is not None and "V1.19.21" in reading.reason


@pytest.mark.asyncio
async def test_read_absent_key_is_not_reported_as_discard() -> None:
    # 键不存在（作业从未产出）：reason 必须为空，由服务层补“指标键不存在”口径
    reading = await IngestLatencyRedisReader(_FakeRedis(value=None)).read()
    assert reading == IngestLatencyReading(p95_ms=None, reason=None, key_present=False)


@pytest.mark.asyncio
async def test_read_unparsable_or_non_object_payload_still_marks_key_present() -> None:
    for value in ("not-json", json.dumps([1, 2])):
        reading = await IngestLatencyRedisReader(_FakeRedis(value=value)).read()
        assert reading.p95_ms is None
        assert reading.key_present is True  # 键确实存在（不能误报成“作业未产出”）
        assert reading.reason is None  # 读不出内容就不给结论，只有日志留痕


@pytest.mark.asyncio
async def test_read_redis_failure_degrades_silently() -> None:
    reading = await IngestLatencyRedisReader(_FakeRedis(error=ConnectionError("redis down"))).read()
    assert reading == IngestLatencyReading()
    assert await IngestLatencyRedisReader(None).read() == IngestLatencyReading()
