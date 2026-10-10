"""``data_quality_monitor`` 入库延迟统计核心单测（纯 Python，无需 PyFlink 运行时）。

纪律对齐（契约 pending #10 结案路线：Kafka 时间戳差 → Flink 流式 → Redis 指标键）：
- 样本 = 到达时间 - 车端事件时间（秒差 → 毫秒）；
- 时钟超前的负延迟丢弃（禁止伪造/取绝对值），非有限值丢弃；
- 车队级 P95（percentile_cont 线性插值，与看板只读聚合口径一致）；
- 窗口按车端事件时间滚动，跨窗口边界才冲刷（tumbling 语义）。
"""
from __future__ import annotations

import json
from typing import Any

import pytest
from hunter_flink.ingest_latency_job import (
    aggregate_latency,
    encode_redis_value,
    expand_latency,
    latency_ms,
    window_seconds_from_env,
)

WINDOW = 60.0


def raw(ts: float, *, vehicle_id: str = "HUNTER-001") -> dict[str, Any]:
    return {"vehicle_id": vehicle_id, "timestamp": ts, "seq": 1}


# =====================================================================
# 单样本延迟口径
# =====================================================================


def test_latency_ms_is_arrival_minus_event_in_ms() -> None:
    assert latency_ms(1000.0, 1001.5) == pytest.approx(1500.0)


def test_latency_ms_drops_negative_clock_skew() -> None:
    # 车端时钟超前：到达早于事件时间 → 丢弃（不得取绝对值伪造）
    assert latency_ms(1000.0, 999.0) is None


def test_latency_ms_drops_non_finite_and_missing() -> None:
    assert latency_ms(None, 1000.0) is None
    assert latency_ms(1000.0, None) is None
    assert latency_ms(1000.0, float("inf")) is None
    assert latency_ms(float("nan"), 1000.0) is None


# =====================================================================
# 窗口聚合（车队级 P95）
# =====================================================================


def test_aggregate_latency_returns_p95_and_count() -> None:
    samples = [float(v) for v in range(1, 11)]  # 1..10 ms
    metric = aggregate_latency(samples, window_seconds=WINDOW, window_end=1080.0)
    assert metric is not None
    assert metric["metric"] == "ingest_latency_ms_p95"
    assert metric["p95_ms"] == pytest.approx(9.55)  # 线性插值
    assert metric["count"] == 10
    assert metric["window"] == "60s"
    assert metric["window_end"] == pytest.approx(1080.0)


def test_aggregate_latency_empty_is_none() -> None:
    assert aggregate_latency([], window_seconds=WINDOW, window_end=1080.0) is None
    # 全 None 样本（延迟丢弃后）→ 不产出
    assert aggregate_latency([None], window_seconds=WINDOW, window_end=1080.0) is None


# =====================================================================
# 窗口滚动（tumbling by event time）+ 值编码
# =====================================================================


def test_expand_latency_flushes_previous_window_only() -> None:
    buffers: dict[int, list[float]] = {}
    # [1020,1080) → window_id 17；[1080,1140) → window_id 18
    assert expand_latency(raw(1030.0), arrival_time=1031.0, window_seconds=WINDOW, buffers=buffers) == []
    assert expand_latency(raw(1040.0), arrival_time=1042.0, window_seconds=WINDOW, buffers=buffers) == []
    assert 17 in buffers and len(buffers[17]) == 2  # 1000ms + 2000ms
    flushed = expand_latency(raw(1085.0), arrival_time=1085.5, window_seconds=WINDOW, buffers=buffers)
    assert len(flushed) == 1  # 仅冲刷上一窗口（window_id 17）
    assert flushed[0]["p95_ms"] == pytest.approx(1950.0)  # [1000,2000] 线性插值 → 1000+1000*0.95
    assert flushed[0]["window_end"] == pytest.approx(1080.0)
    assert flushed[0]["count"] == 2


def test_expand_latency_drops_skewed_samples() -> None:
    buffers: dict[int, list[float]] = {}
    # 到达早于事件时间（时钟超前）→ 丢弃，不入缓冲
    assert expand_latency(raw(1030.0), arrival_time=1029.0, window_seconds=WINDOW, buffers=buffers) == []
    assert buffers == {}


def test_expand_latency_normalizes_millisecond_event_time() -> None:
    buffers: dict[int, list[float]] = {}
    # 车端以 13 位毫秒上报（1724035260000.0ms = 1724035260.0s），到达时间以秒给出
    event_ms = 1_724_035_260_000.0
    out = expand_latency(raw(event_ms), arrival_time=1_724_035_261.0, window_seconds=WINDOW, buffers=buffers)
    assert out == []  # 单样本，无更晚窗口不冲刷
    assert len(buffers) == 1
    only_window = next(iter(buffers))
    assert buffers[only_window][0] == pytest.approx(1000.0)  # 归一为秒后：差 1s = 1000ms


def test_encode_redis_value_roundtrip() -> None:
    metric = aggregate_latency([1000.0, 2000.0], window_seconds=WINDOW, window_end=1080.0)
    assert metric is not None
    decoded = json.loads(encode_redis_value(metric))
    assert decoded["metric"] == "ingest_latency_ms_p95"
    assert decoded["window"] == "60s"
    assert set(decoded) == {"metric", "p95_ms", "count", "window", "window_end"}


# =====================================================================
# 窗口环境变量
# =====================================================================


def test_window_seconds_from_env_defaults_and_override() -> None:
    assert window_seconds_from_env({}) == pytest.approx(60.0)
    assert window_seconds_from_env({"INGEST_LATENCY_WINDOW_SECONDS": "30"}) == pytest.approx(30.0)
    with pytest.raises(ValueError):
        window_seconds_from_env({"INGEST_LATENCY_WINDOW_SECONDS": "0"})


# =====================================================================
# 键常量跨端一致性（redis-keys.yaml 受控键：单一常量源，杜绝与看板读端拼写漂移）
# =====================================================================


def test_redis_key_defaults_to_shared_constant(monkeypatch: pytest.MonkeyPatch) -> None:
    """默认（无环境变量覆盖时）写端键 = 共享常量 = 契约模式，与读端同源。"""
    monkeypatch.delenv("INGEST_LATENCY_REDIS_KEY", raising=False)
    import importlib

    from hunter_common import redis_keys

    module = importlib.reload(importlib.import_module("hunter_flink.ingest_latency_job"))
    assert module.REDIS_KEY == redis_keys.ANALYTICS_INGEST_LATENCY == "analytics:ingest_latency"
