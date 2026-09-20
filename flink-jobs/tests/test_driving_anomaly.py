"""``driving_anomaly_detection`` 作业规则核心单测（G-13，纯 Python，无需 PyFlink）。

纪律对齐（契约 ``x-hunter-realtime-jobs``）：
- 阈值默认值逐项 == 契约 ``thresholds`` 基准（禁止代码内放宽）；
- alert_event 载荷通过 ``contracts/kafka/schemas/alert_event.schema.json`` 校验；
- level 唯一来源 ``EVENT_LEVEL_BY_TYPE``（禁止改等级）。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml
from hunter_common.database.enums import EVENT_LEVEL_BY_TYPE
from hunter_flink.detection_job import expand_alerts
from hunter_flink.rules import (
    MAX_ACCEL_INTERVAL_SECONDS,
    SOURCE_JOB,
    VehicleAnomalyTracker,
    build_alert,
)
from hunter_flink.thresholds import AlertThresholds

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "openapi" / "data-analytics.yaml"
ALERT_SCHEMA_PATH = ROOT / "contracts" / "kafka" / "schemas" / "alert_event.schema.json"


# =====================================================================
# fixtures 与工具
# =====================================================================


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    loaded = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


@pytest.fixture(scope="module")
def alert_schema() -> dict[str, Any]:
    return json.loads(ALERT_SCHEMA_PATH.read_text(encoding="utf-8"))


def telemetry(
    ts: float,
    *,
    velocity: float | None = None,
    omega_z: float | None = None,
    soc: float | None = None,
    vehicle_id: str = "VH-A001",
    seq: int = 1,
) -> dict[str, Any]:
    """构造 telemetry_clean 样本（字段形态与 contracts/kafka/schemas/telemetry.schema.json 一致）。"""
    chassis: dict[str, Any] = {}
    if velocity is not None:
        chassis["velocity"] = velocity
    if soc is not None:
        chassis["battery_soc"] = soc
    localization: dict[str, Any] = {"x": 100.0, "y": 200.0, "heading": 1.57}
    if omega_z is not None:
        localization["angular_velocity"] = [0.0, 0.0, omega_z]
    return {
        "vehicle_id": vehicle_id,
        "seq": seq,
        "timestamp": ts,
        "chassis": chassis,
        "localization": localization,
    }


def ramp(
    tracker: VehicleAnomalyTracker,
    start_ts: float,
    velocities: list[float],
    *,
    step: float = 0.1,
) -> list:
    """按固定步长喂入速度序列，返回全部命中。"""
    hits = []
    ts = start_ts
    for v in velocities:
        hits += tracker.on_sample(telemetry(ts, velocity=v), now=ts + 0.3)
        ts += step
    return hits


# =====================================================================
# 阈值：契约基准比对 + 环境变量覆盖
# =====================================================================


def test_threshold_defaults_match_contract(contract: dict[str, Any]) -> None:
    """14 项阈值默认值逐项 == 契约 x-hunter-realtime-jobs.thresholds 基准值。"""
    thresholds = contract["x-hunter-realtime-jobs"]["thresholds"]
    by_env = {entry["name"]: float(entry["value"]) for entry in thresholds}
    assert set(by_env) == set(AlertThresholds.ENV_NAMES.values())
    baseline = AlertThresholds()
    for field, env_name in AlertThresholds.ENV_NAMES.items():
        assert getattr(baseline, field) == pytest.approx(by_env[env_name]), env_name


def test_thresholds_from_env_override_and_reject() -> None:
    baseline = AlertThresholds.from_env({})
    assert baseline == AlertThresholds()
    patched = AlertThresholds.from_env(
        {"ALERT_HARSH_ACCEL_MS2": "4.5", "ALERT_BATTERY_LOW_SOC": "25"}
    )
    assert patched.harsh_accel_ms2 == 4.5
    assert patched.battery_low_soc == 25.0
    assert patched.harsh_braking_ms2 == baseline.harsh_braking_ms2
    with pytest.raises(ValueError):
        AlertThresholds.from_env({"ALERT_HARSH_ACCEL_MS2": "abc"})
    with pytest.raises(ValueError):
        AlertThresholds.from_env({"ALERT_HARSH_ACCEL_MS2": "-1"})


# =====================================================================
# 6.2.2 规则矩阵
# =====================================================================


def test_harsh_acceleration_requires_sustained_duration() -> None:
    """>3 m/s² 但持续 ≤0.5s 不触发；持续越过后恰好触发一次（episode 语义）。"""
    tracker = VehicleAnomalyTracker()
    # 4 m/s²（每 0.1s 提速 0.4），仅持续 0.4s 后回落
    hits = ramp(tracker, 1000.0, [0.0, 0.4, 0.8, 1.2, 1.6, 2.0])
    ramp(tracker, 1000.7, [1.8, 1.6, 1.4, 1.2])  # 回落解除命中区间
    assert hits == []

    tracker = VehicleAnomalyTracker()
    # 0→2.8 m/s 匀加速（4 m/s²），0.7s 内持续越限
    hits = ramp(tracker, 1000.0, [0.0 + 0.4 * i for i in range(9)])
    accel_hits = [h for h in hits if h.alert_type == "harsh_acceleration"]
    assert len(accel_hits) == 1
    hit = accel_hits[0]
    assert hit.rule["comparator"] == "gt"
    assert hit.rule["threshold"] == 3.0
    assert hit.rule["duration_seconds"] == 0.5
    assert hit.window_start is not None and hit.window_end is not None
    assert hit.window_end - hit.window_start > 0.5
    assert hit.event_time == hit.window_start


def test_harsh_acceleration_rearms_after_recovery() -> None:
    """恢复后重新武装：第二段异常再次触发一次。"""
    tracker = VehicleAnomalyTracker()
    first = ramp(tracker, 1000.0, [0.4 * i for i in range(9)])  # 4 m/s² 持续
    assert len([h for h in first if h.alert_type == "harsh_acceleration"]) == 1
    coast = ramp(tracker, 1001.0, [3.2] * 8)  # 匀速：加速度 0，解除
    assert coast == []
    second = ramp(tracker, 1002.0, [3.2 + 0.4 * i for i in range(1, 9)])
    assert len([h for h in second if h.alert_type == "harsh_acceleration"]) == 1


def test_acceleration_diff_skips_data_gap() -> None:
    """断流（dt > MAX_ACCEL_INTERVAL_SECONDS）不参与差分：不产生虚假加速度。"""
    tracker = VehicleAnomalyTracker()
    ramp(tracker, 1000.0, [0.0, 0.1])
    # 10s 断流后速度从 0.1 跳到 20.0：差分被跳过
    hits = tracker.on_sample(telemetry(1010.0, velocity=20.0), now=1010.3)
    assert hits == []
    assert MAX_ACCEL_INTERVAL_SECONDS == 1.0


def test_harsh_braking_triggers_with_negative_threshold() -> None:
    """急减速：差分 < -3 m/s² 持续越过 0.5s 触发一次，rule.threshold 为负值。"""
    tracker = VehicleAnomalyTracker()
    hits = ramp(tracker, 1000.0, [20.0 - 0.5 * i for i in range(9)])  # -5 m/s²
    brake_hits = [h for h in hits if h.alert_type == "harsh_braking"]
    assert len(brake_hits) == 1
    assert brake_hits[0].rule["comparator"] == "lt"
    assert brake_hits[0].rule["threshold"] == -3.0
    assert brake_hits[0].metrics["min_acceleration"] < -3.0


def test_harsh_turning_edge_triggered() -> None:
    """急转弯：上升沿触发一次，持续不重复，恢复后重新武装。"""
    tracker = VehicleAnomalyTracker()
    hits: list = []
    ts = 1000.0
    for _ in range(5):  # 1.0 rad/s 持续
        hits += tracker.on_sample(telemetry(ts, omega_z=1.0), now=ts + 0.2)
        ts += 0.1
    turn_hits = [h for h in hits if h.alert_type == "harsh_turning"]
    assert len(turn_hits) == 1
    assert turn_hits[0].rule["threshold"] == 0.8

    hits2: list = []
    for _ in range(3):  # 恢复
        hits2 += tracker.on_sample(telemetry(ts, omega_z=0.2), now=ts)
        ts += 0.1
    assert hits2 == []
    hits3 = tracker.on_sample(telemetry(ts, omega_z=1.2), now=ts)  # 再次越限
    assert [h.alert_type for h in hits3] == ["harsh_turning"]


def test_over_speed_skipped_without_speed_limit_source() -> None:
    """限速来源缺失（契约 pending #13）：跳过规则，禁止自造默认限速。"""
    tracker = VehicleAnomalyTracker()
    hits = ramp(tracker, 1000.0, [30.0] * 10)  # 30 m/s 也判不了超速
    assert [h for h in hits if h.alert_type == "over_speed"] == []


def test_over_speed_triggers_with_injected_limit() -> None:
    """注入限速 10 m/s：> 11 m/s 触发，threshold = 限速 × ratio。"""
    tracker = VehicleAnomalyTracker(speed_limit_mps=10.0)
    hits = ramp(tracker, 1000.0, [9.0, 9.5, 11.5, 12.0, 12.5, 13.0])
    over_hits = [h for h in hits if h.alert_type == "over_speed"]
    assert len(over_hits) == 1
    assert over_hits[0].rule["threshold"] == pytest.approx(11.0)
    assert over_hits[0].metrics["speed_limit"] == 10.0


def test_battery_low_and_critical_independent_edges() -> None:
    """SOC 跌破 20 触发 battery_low；跌破 10 追加 battery_critical（各自一次）。"""
    tracker = VehicleAnomalyTracker()
    socs = [55, 40, 25, 18, 15, 12, 9, 7]
    hits: list = []
    for i, soc in enumerate(socs):
        hits += tracker.on_sample(
            telemetry(1000.0 + i * 1.0, velocity=5.0, soc=soc), now=1000.0 + i
        )
    low = [h for h in hits if h.alert_type == "battery_low"]
    crit = [h for h in hits if h.alert_type == "battery_critical"]
    assert len(low) == 1 and len(crit) == 1
    assert low[0].event_time == 1003.0  # SOC 18 首次跌破 20
    assert crit[0].event_time == 1006.0  # SOC 9 首次跌破 10


# =====================================================================
# alert_event 载荷：schema 校验 + 等级纪律
# =====================================================================

ALL_RULE_TYPES = [
    "harsh_acceleration",
    "harsh_braking",
    "harsh_turning",
    "over_speed",
    "battery_low",
    "battery_critical",
]


def _collect_hits() -> list:
    tracker = VehicleAnomalyTracker(speed_limit_mps=10.0)
    hits: list = []
    hits += ramp(tracker, 1000.0, [0.4 * i for i in range(9)])  # 急加速
    hits += ramp(tracker, 1002.0, [20.0 - 0.5 * i for i in range(9)])  # 急减速
    hits += tracker.on_sample(telemetry(1004.0, omega_z=1.5), now=1004.0)  # 急转弯
    hits += ramp(tracker, 1005.0, [11.5, 12.0, 12.5])  # 超速
    hits += tracker.on_sample(telemetry(1007.0, velocity=5.0, soc=8), now=1007.0)  # 电量
    return hits


def test_alert_payload_matches_schema_and_controlled_level(alert_schema: dict) -> None:
    hits = _collect_hits()
    triggered_types = {h.alert_type for h in hits}
    assert triggered_types == set(ALL_RULE_TYPES)
    for hit in hits:
        alert = build_alert("VH-A001", hit, now=hit.event_time + 1.0)
        jsonschema.validate(alert, alert_schema)  # additionalProperties:false 严格
        assert alert["level"] == str(EVENT_LEVEL_BY_TYPE[hit.alert_type])
        assert alert["source_job"] == SOURCE_JOB
        assert alert["timestamp"] - alert["event_time"] == pytest.approx(1.0)


def test_hit_level_property_uses_controlled_vocabulary() -> None:
    for hit in _collect_hits():
        assert hit.level == str(EVENT_LEVEL_BY_TYPE[hit.alert_type])


def test_location_omitted_when_positioning_missing(alert_schema: dict) -> None:
    msg = telemetry(1000.0, omega_z=1.5)
    msg["localization"].pop("x")
    tracker = VehicleAnomalyTracker()
    hits = tracker.on_sample(msg, now=1000.2)
    assert [h.alert_type for h in hits] == ["harsh_turning"]
    assert hits[0].location is None
    alert = build_alert("VH-A001", hits[0], now=1000.5)
    assert "location" not in alert
    jsonschema.validate(alert, alert_schema)


# =====================================================================
# expand_alerts：算子核心（多车隔离 / 脏消息）
# =====================================================================


def test_expand_alerts_isolates_trackers_per_vehicle() -> None:
    thresholds = AlertThresholds()
    trackers: dict[str, VehicleAnomalyTracker] = {}
    alerts_a = expand_alerts(
        telemetry(1000.0, velocity=5.0, soc=15, vehicle_id="VH-A001"),
        thresholds=thresholds,
        speed_limit_mps=None,
        trackers=trackers,
    )
    alerts_b = expand_alerts(
        telemetry(1000.0, velocity=5.0, soc=15, vehicle_id="VH-B002"),
        thresholds=thresholds,
        speed_limit_mps=None,
        trackers=trackers,
    )
    assert [a["vehicle_id"] for a in alerts_a] == ["VH-A001"]
    assert [a["vehicle_id"] for a in alerts_b] == ["VH-B002"]
    assert set(trackers) == {"VH-A001", "VH-B002"}
    # 同车恢复后再次跌破：独立 episode（VH-B002 先到 15，再到 8）
    again = list(
        expand_alerts(
            telemetry(1001.0, velocity=5.0, soc=8, vehicle_id="VH-B002"),
            thresholds=thresholds,
            speed_limit_mps=None,
            trackers=trackers,
        )
    )
    assert {a["alert_type"] for a in again} == {"battery_critical"}


def test_expand_alerts_drops_message_without_vehicle_id() -> None:
    trackers: dict[str, VehicleAnomalyTracker] = {}
    msg = telemetry(1000.0, velocity=5.0, soc=5)
    msg.pop("vehicle_id")
    assert (
        list(
            expand_alerts(
                msg,
                thresholds=AlertThresholds(),
                speed_limit_mps=None,
                trackers=trackers,
            )
        )
        == []
    )
    assert trackers == {}
