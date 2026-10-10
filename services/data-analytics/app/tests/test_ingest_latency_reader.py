"""``IngestLatencyRedisReader`` 单测：读 Flink data_quality_monitor 写入的 Redis 指标键。

契约（pending #10 结案：Kafka 时间戳差 → Flink 流式 → Redis 指标键 analytics:ingest_latency）：
- 本服务只读该键，任何异常/缺失/不可解析路径均降级为 None（绝不展示陈旧值、不向请求路径抛出）；
- 仅当载荷为含数值 ``p95_ms`` 的 JSON 对象时才返回 float。
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from app.repositories.pipeline import IngestLatencyRedisReader


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
