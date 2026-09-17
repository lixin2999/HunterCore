"""性能基准套件（系统关键约束第 10 条，阈值不可放宽）。

指标与登记（写入 L5 报告 ``l5_report.add_perf``，供 CI 门槛判定）：
- API 接口响应 P95 ≤ 200ms（GET /healthz 全链路，含 uvicorn 进程）
- 遥测数据入库延迟 ≤ 1s（单条 TimescaleDB 写入）
- 时序数据写入 ≥ 10000 点/秒（``bulk_insert`` executemany 批量）
- Kafka 生产吞吐（契约仅约束单车 ≤100 msg/s 上限；平台侧吞吐取 CI 回归下限
  ``thresholds.BENCH_KAFKA_MIN_MSGS_PER_S``，防止性能回退）

规模通过环境变量放大（``HUNTER_BENCH_*``），默认值保证本地/CI 时长可控。
容器型基准依赖 Docker，不可用时自动 skip；API 基准依赖 uvicorn 服务进程。
"""
from __future__ import annotations

import time
from typing import Any

import pytest

from tests.support import broker, contracts, db, flow, messages, thresholds
from tests.support.report import TestReport as L5TestReport  # 别名避免 pytest 误收集

pytestmark = [pytest.mark.performance, pytest.mark.l5_case("PERF")]

#: 车辆号取自契约示例（telemetry.schema.json examples[0]）
VEHICLE_ID: str = str(contracts.schema_example("telemetry")["vehicle_id"])


# ---------------------------------------------------------------- 纯逻辑基准

def test_p95_helper_matches_reference() -> None:
    """P95 统一口径：升序取 95 分位（性能断言统一走该函数，避免各测口径漂移）。"""
    samples = [float(i) for i in range(1, 101)]
    assert flow.processing_step_p95_ms(samples) == 95.0
    assert flow.processing_step_p95_ms([42.0]) == 42.0


# ---------------------------------------------------------------- TimescaleDB（容器）

async def test_timeseries_write_throughput(
    postgres_handle: Any, l5_report: L5TestReport
) -> None:
    """时序写入 ≥ 10000 点/秒：``BENCH_TIMESERIES_POINTS`` 点 executemany 批量插入。"""
    points = thresholds.BENCH_TIMESERIES_POINTS
    rows = db.seed_rows(points, VEHICLE_ID, base_seq=500_000)
    elapsed = await db.bulk_insert(postgres_handle, rows)
    rate = points / elapsed if elapsed > 0 else float("inf")
    stored = await db.count_rows(
        postgres_handle, db.TELEMETRY_TABLE, "vehicle_id = $1 AND seq >= $2",
        VEHICLE_ID, 500_000,
    )
    assert stored == points, f"落库行数 {stored} ≠ 写入 {points}"
    record = l5_report.add_perf(
        metric="时序数据写入速率",
        value=round(rate, 1),
        unit="points/s",
        threshold=thresholds.TIMESERIES_WRITE_MIN_POINTS_PER_S,
        direction="min",
        source=thresholds.SOURCES["TIMESERIES_WRITE_MIN_POINTS_PER_S"],
        detail=f"{points} 点 executemany 批量写入，耗时 {elapsed:.3f}s",
    )
    assert record.passed, f"时序写入 {rate:.0f} points/s < {thresholds.TIMESERIES_WRITE_MIN_POINTS_PER_S}"


async def test_telemetry_ingest_latency(postgres_handle: Any, l5_report: L5TestReport) -> None:
    """遥测入库延迟 ≤ 1s：单条写入（含连接建立）端到端计时。"""
    rows = db.seed_rows(1, VEHICLE_ID, base_seq=900_000)
    elapsed = await db.bulk_insert(postgres_handle, rows)
    record = l5_report.add_perf(
        metric="遥测数据入库延迟",
        value=round(elapsed, 4),
        unit="s",
        threshold=thresholds.TELEMETRY_INGEST_LATENCY_MAX_S,
        direction="max",
        source=thresholds.SOURCES["TELEMETRY_INGEST_LATENCY_MAX_S"],
        detail="单条 vehicle_telemetry 写入（含连接与 executemany）",
    )
    assert record.passed, f"入库延迟 {elapsed:.3f}s 超过 {thresholds.TELEMETRY_INGEST_LATENCY_MAX_S}s"


# ---------------------------------------------------------------- Kafka（容器）

def test_kafka_produce_throughput(kafka_bootstrap: str, l5_report: L5TestReport) -> None:
    """Kafka 生产吞吐（回归下限）：批量 produce N 条遥测后统一 flush 计时。"""
    confluent_kafka = pytest.importorskip(
        "confluent_kafka", reason="缺少 confluent-kafka 依赖"
    )
    total = thresholds.BENCH_TELEMETRY_MESSAGES
    topic = thresholds.INTERNAL_TOPIC_ROUTING["telemetry"]  # telemetry_raw（契约 Topic）
    producer = confluent_kafka.Producer(broker.producer_config(kafka_bootstrap))
    batch = [
        messages.encode(messages.telemetry_message(VEHICLE_ID, seq=seq))
        for seq in range(1, total + 1)
    ]
    started = time.perf_counter()
    for index, value in enumerate(batch):
        producer.produce(topic, key=VEHICLE_ID.encode("utf-8"), value=value)
        if index % 500 == 499:
            producer.poll(0)  # 触发投递回调，防止队列堆积
    remaining = producer.flush(60.0)
    elapsed = time.perf_counter() - started
    assert remaining == 0, f"{remaining} 条消息未投递"
    rate = total / elapsed if elapsed > 0 else float("inf")
    record = l5_report.add_perf(
        metric="Kafka 生产吞吐（回归下限）",
        value=round(rate, 1),
        unit="msg/s",
        threshold=thresholds.BENCH_KAFKA_MIN_MSGS_PER_S,
        direction="min",
        source="基准回归下限（契约约束单车 ≤100 msg/s 上限，见 thresholds.BENCH_KAFKA_MIN_MSGS_PER_S）",
        detail=f"{total} 条遥测批量生产，耗时 {elapsed:.3f}s",
    )
    assert record.passed, f"Kafka 吞吐 {rate:.0f} msg/s 低于回归下限 {thresholds.BENCH_KAFKA_MIN_MSGS_PER_S}"


# ---------------------------------------------------------------- API（服务进程）

async def test_api_p95_latency(gateway_service: Any, l5_report: L5TestReport) -> None:
    """API P95 ≤ 200ms：对 api-gateway /healthz 发起 ``BENCH_API_REQUESTS`` 次请求。"""
    httpx = pytest.importorskip("httpx", reason="缺少 httpx 依赖")
    warmup, total = thresholds.BENCH_API_WARMUP, thresholds.BENCH_API_REQUESTS
    latencies_ms: list[float] = []
    async with httpx.AsyncClient(base_url=gateway_service.base_url, timeout=10.0) as client:
        for _ in range(warmup):
            await client.get("/healthz")
        for _ in range(total):
            started = time.perf_counter()
            resp = await client.get("/healthz")
            latencies_ms.append((time.perf_counter() - started) * 1000.0)
            assert resp.status_code == 200, f"/healthz 异常：{resp.text}\n{gateway_service.logs()}"
    p95 = flow.processing_step_p95_ms(latencies_ms)
    record = l5_report.add_perf(
        metric="API 接口响应 P95",
        value=round(p95, 2),
        unit="ms",
        threshold=thresholds.API_P95_MAX_MS,
        direction="max",
        source=thresholds.SOURCES["API_P95_MAX_MS"],
        detail=f"GET /healthz × {total}（预热 {warmup} 次）",
    )
    assert record.passed, f"API P95 {p95:.1f}ms 超过 {thresholds.API_P95_MAX_MS}ms"
