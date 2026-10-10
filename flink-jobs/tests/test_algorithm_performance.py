"""``algorithm_performance_monitor`` 聚合核心单测（纯 Python，无需 PyFlink 运行时）。

纪律对齐（契约 ``x-hunter-realtime-jobs``）：
- 输出载荷通过 ``contracts/kafka/schemas/algorithm_metrics.schema.json`` 严格校验
  （additionalProperties=false，字段 ⊆ 表列，module ∈ perception/planning/control）；
- 口径：fps 均值/最小值、latency_ms P95、planning/control 延迟均值、控制误差取绝对值；
- ``time`` = 窗口内车端时间上界（禁止处理时间替代）；缺失字段跳过（禁止伪造 0）；
- 窗口按车端事件时间滚动，跨窗口边界才冲刷（tumbling 语义）。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from hunter_flink.algorithm_performance_job import (
    aggregate_metrics,
    expand_metrics,
    percentile_cont,
)

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "contracts" / "kafka" / "schemas" / "algorithm_metrics.schema.json"
WINDOW = 60.0


@pytest.fixture(scope="module")
def metric_schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def sample(
    ts: float,
    *,
    vehicle_id: str = "HUNTER-001",
    fps: float | None = None,
    latency_ms: float | None = None,
    planning_latency_ms: float | None = None,
    control_latency_ms: float | None = None,
    velocity_error: float | None = None,
    steer_error: float | None = None,
) -> dict[str, Any]:
    """构造 telemetry_clean 样本（仅含聚合关心的段；其余段缺省不影响核心）。"""
    perception: dict[str, Any] = {}
    if fps is not None:
        perception["fps"] = fps
    if latency_ms is not None:
        perception["latency_ms"] = latency_ms
    planning: dict[str, Any] = {}
    if planning_latency_ms is not None:
        planning["planning_latency_ms"] = planning_latency_ms
    control: dict[str, Any] = {}
    if control_latency_ms is not None:
        control["control_latency_ms"] = control_latency_ms
    if velocity_error is not None:
        control["velocity_error"] = velocity_error
    if steer_error is not None:
        control["steer_error"] = steer_error
    return {
        "vehicle_id": vehicle_id,
        "timestamp": ts,
        "seq": 1,
        "perception": perception,
        "planning": planning,
        "control": control,
    }


def _by_name(messages: list[dict[str, Any]]) -> dict[str, float]:
    return {m["metric_name"]: m["metric_value"] for m in messages}


# =====================================================================
# 聚合口径
# =====================================================================


def test_aggregate_metrics_uses_contract_aggregations() -> None:
    samples = [
        sample(1000.0, fps=30, latency_ms=100, planning_latency_ms=40, control_latency_ms=5, velocity_error=-0.1, steer_error=0.02),
        sample(1001.0, fps=20, latency_ms=200, planning_latency_ms=50, control_latency_ms=7, velocity_error=0.3, steer_error=-0.04),
        sample(1002.0, fps=10, latency_ms=300, planning_latency_ms=60, control_latency_ms=9, velocity_error=-0.5, steer_error=0.06),
    ]
    values = _by_name(aggregate_metrics("HUNTER-001", samples, window_seconds=WINDOW))
    assert values["fps"] == pytest.approx(20.0)  # 均值
    assert values["fps_min"] == pytest.approx(10.0)  # 最小值
    assert values["latency_ms"] == pytest.approx(percentile_cont([100, 200, 300], 0.95))
    assert values["planning_latency_ms"] == pytest.approx(50.0)
    assert values["control_latency_ms"] == pytest.approx(7.0)
    # 控制误差取绝对值后均值
    assert values["velocity_error_abs"] == pytest.approx((0.1 + 0.3 + 0.5) / 3)
    assert values["steer_error_abs"] == pytest.approx((0.02 + 0.04 + 0.06) / 3)


def test_time_is_max_event_time_not_processing_time() -> None:
    samples = [sample(1000.0, fps=10), sample(1050.5, fps=20)]
    messages = aggregate_metrics("HUNTER-001", samples, window_seconds=WINDOW)
    assert messages  # fps/fps_min
    assert all(m["time"] == pytest.approx(1050.5) for m in messages)


def test_missing_segments_are_skipped_not_zero_filled() -> None:
    # 仅 fps 有值：只产出 perception.fps / fps_min，其余指标不伪造
    messages = aggregate_metrics("HUNTER-001", [sample(1000.0, fps=25.0)], window_seconds=WINDOW)
    names = {m["metric_name"] for m in messages}
    assert names == {"fps", "fps_min"}


def test_empty_or_timeless_input_emits_nothing() -> None:
    assert aggregate_metrics("HUNTER-001", [], window_seconds=WINDOW) == []
    # 无有效时间戳（time 分区键不可确定）→ 不产出
    assert aggregate_metrics("HUNTER-001", [{"vehicle_id": "X"}], window_seconds=WINDOW) == []


# =====================================================================
# 契约 schema 校验（严格）
# =====================================================================


def test_output_matches_algorithm_metrics_schema(metric_schema: dict[str, Any]) -> None:
    samples = [
        sample(1000.0, fps=28.5, latency_ms=85, planning_latency_ms=45, control_latency_ms=5, velocity_error=0.08, steer_error=0.02),
        sample(1059.0, fps=30.0, latency_ms=90, planning_latency_ms=48, control_latency_ms=6, velocity_error=0.10, steer_error=0.03),
    ]
    messages = aggregate_metrics("HUNTER-001", samples, window_seconds=WINDOW)
    assert messages
    for message in messages:
        jsonschema.validate(message, metric_schema)
        assert message["module"] in {"perception", "planning", "control"}
        assert message["tags"]["window"] == "60s"


def test_controlled_module_vocabulary_only(metric_schema: dict[str, Any]) -> None:
    samples = [sample(1000.0, fps=1, latency_ms=1, planning_latency_ms=1, control_latency_ms=1, velocity_error=1, steer_error=1)]
    by_module = {m["metric_name"]: m["module"] for m in aggregate_metrics("V", samples, window_seconds=WINDOW)}
    assert by_module["fps"] == "perception"
    assert by_module["planning_latency_ms"] == "planning"
    assert by_module["velocity_error_abs"] == "control"
    assert by_module["steer_error_abs"] == "control"


# =====================================================================
# 窗口滚动（tumbling by event time）
# =====================================================================


def test_percentile_cont_linear_interpolation() -> None:
    assert percentile_cont([], 0.95) is None
    assert percentile_cont([7], 0.5) == 7.0
    # 1..10：pos = 0.95 * 9 = 8.55 → 9 + 0.55 = 9.55
    assert percentile_cont(list(range(1, 11)), 0.95) == pytest.approx(9.55)


def test_expand_metrics_flushes_previous_window_only() -> None:
    buffers: dict[tuple[str, int], list[dict[str, Any]]] = {}
    # 窗口 [1020,1080)：window_id=17（1020//60）；[1080,1140)：window_id=18
    assert expand_metrics(sample(1030.0, fps=10), window_seconds=WINDOW, buffers=buffers) == []
    assert expand_metrics(sample(1040.0, fps=20), window_seconds=WINDOW, buffers=buffers) == []
    # 同窗口内不冲刷（window_id=17：1020<=t<1080）
    assert ("HUNTER-001", 17) in buffers
    flushed = expand_metrics(sample(1085.0, fps=30), window_seconds=WINDOW, buffers=buffers)
    fps_msgs = [m for m in flushed if m["metric_name"] == "fps"]
    assert len(fps_msgs) == 1
    assert fps_msgs[0]["metric_value"] == pytest.approx(15.0)  # (10+20)/2 上一窗口
    assert fps_msgs[0]["time"] == pytest.approx(1040.0)  # 上一窗口车端时间上界


def test_expand_metrics_ignores_samples_without_vehicle_or_time() -> None:
    buffers: dict[tuple[str, int], list[dict[str, Any]]] = {}
    assert expand_metrics({"timestamp": 1000.0}, window_seconds=WINDOW, buffers=buffers) == []
    assert expand_metrics({"vehicle_id": "X"}, window_seconds=WINDOW, buffers=buffers) == []
