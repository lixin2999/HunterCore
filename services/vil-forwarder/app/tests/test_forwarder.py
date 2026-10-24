"""vil-forwarder 单元测试（无 Kafka/Redis）。"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from app.forwarder import VilForwarder, build_vil_state
from app.session_registry import SessionRegistry


class FakeProducer:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any], str]] = []

    async def publish_payload(
        self, topic: str, payload: dict[str, Any], *, key: str, **_kwargs: Any
    ) -> None:
        self.records.append((topic, payload, key))


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.calls = 0
        self.fail = False

    async def get(self, key: str) -> str | None:
        self.calls += 1
        if self.fail:
            from hunter_common.exceptions import ServiceUnavailableError

            raise ServiceUnavailableError("redis down")
        return self.values.get(key)


SESSION = {
    "sim_id": "sim-001",
    "vehicle_id": "HUNTER-001",
    "mode": "vil",
    "calibration": {
        "calibration_id": "cal-001",
        "map_x": 1.0,
        "map_y": 2.0,
        "map_heading": 0.1,
        "odom_x": 0.0,
        "odom_y": 0.0,
        "odom_heading": 0.0,
    },
}


def telemetry(seq: int = 1, *, timestamp: float | None = None) -> dict[str, Any]:
    return {
        "vehicle_id": "HUNTER-001",
        "timestamp": timestamp if timestamp is not None else time.time(),
        "seq": seq,
        "chassis": {
            "velocity": 1.5,
            "steering_angle": 0.1,
            "battery_voltage": 48.0,
            "battery_soc": 80,
            "battery_current": -5.0,
            "battery_temp": 30.0,
            "control_mode": "CAN",
            "vehicle_state": "NORMAL",
            "fault_code": 0,
            "motor_rpm": [1000],
            "motor_current": [3.0],
            "motor_temp": [40],
        },
        "localization": {
            "x": 10.0,
            "y": 20.0,
            "z": 0.0,
            "roll": 0.0,
            "pitch": 0.0,
            "heading": 1.2,
            "linear_velocity": [1.5, 0.0, 0.0],
            "angular_velocity": [0.0, 0.0, 0.05],
            "position_std": 0.1,
            "heading_std": 0.01,
        },
        "perception": {
            "detected_objects": 3,
            "fps": 10.0,
            "latency_ms": 80.0,
            "object_types": {"vehicle": 2, "pedestrian": 1},
        },
        "planning": {
            "trajectory_length": 10.0,
            "trajectory_points": 20,
            "planning_latency_ms": 5.0,
            "current_behavior": "cruise",
        },
        "control": {
            "target_velocity": 1.6,
            "target_steer": 0.1,
            "velocity_error": 0.1,
            "steer_error": 0.0,
            "control_latency_ms": 5.0,
        },
        "system": {
            "cpu_usage": 10.0,
            "memory_usage": 20.0,
            "disk_usage": 30.0,
            "temperature": 40.0,
            "network_latency_ms": 5.0,
        },
    }


def test_build_vil_state_maps_fields_and_calibration() -> None:
    payload = build_vil_state(telemetry(7), SESSION, forwarded_at=123.0)
    assert payload["schema_version"] == "1.0.0"
    assert payload["sim_id"] == "sim-001"
    assert payload["seq"] == 7
    assert payload["pose"]["frame"] == "odom"
    assert payload["pose"]["x"] == 10.0
    assert payload["pose"]["yaw"] == 1.2
    assert payload["twist"]["linear"] == [1.5, 0.0, 0.0]
    assert payload["chassis"]["velocity"] == 1.5
    assert payload["planning"] == {"current_behavior": "cruise"}
    assert payload["calibration"]["calibration_id"] == "cal-001"
    assert payload["forwarded_at"] == 123.0


@pytest.mark.asyncio
async def test_forwarder_skips_non_vil_and_publishes_immediately() -> None:
    redis = FakeRedis()
    registry = SessionRegistry(redis, cache_ttl_seconds=10)
    producer = FakeProducer()
    forwarder = VilForwarder(producer, registry, target_hz=10)

    await forwarder.handle({**telemetry(1), "vehicle_id": "OTHER"})
    assert producer.records == []
    assert forwarder.skipped_count == 1

    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(SESSION)
    await forwarder.handle(telemetry(2))
    assert len(producer.records) == 1
    topic, payload, key = producer.records[0]
    assert topic == "carla.sim-001.vil_state"
    assert key == "sim-001"
    assert payload["seq"] == 2


@pytest.mark.asyncio
async def test_forwarder_latest_wins_during_interval() -> None:
    redis = FakeRedis()
    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(SESSION)
    registry = SessionRegistry(redis, cache_ttl_seconds=10)
    producer = FakeProducer()
    forwarder = VilForwarder(producer, registry, target_hz=10)

    await forwarder.handle(telemetry(1))
    await forwarder.handle(telemetry(2))
    await forwarder.handle(telemetry(3))
    assert [record[1]["seq"] for record in producer.records] == [1]
    await asyncio.sleep(0.12)
    assert [record[1]["seq"] for record in producer.records] == [1, 3]
    await forwarder.close()


@pytest.mark.asyncio
async def test_session_registry_caches_with_ttl() -> None:
    redis = FakeRedis()
    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(SESSION)
    registry = SessionRegistry(redis, cache_ttl_seconds=0.05)
    assert await registry.get("HUNTER-001") == SESSION
    assert await registry.get("HUNTER-001") == SESSION
    assert redis.calls == 1
    await asyncio.sleep(0.06)
    assert await registry.get("HUNTER-001") == SESSION
    assert redis.calls == 2


@pytest.mark.asyncio
async def test_session_registry_cache_is_bounded() -> None:
    """异常 vehicle_id 洪泛时本地缓存不得无界增长。"""
    redis = FakeRedis()
    registry = SessionRegistry(redis, cache_ttl_seconds=10, max_entries=2)
    for index in range(5):
        redis.values[f"vil:session:V-{index}"] = __import__("json").dumps(SESSION)
        await registry.get(f"V-{index}")
    assert len(registry._cache) == 2  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_forwarder_drops_frame_when_redis_unavailable() -> None:
    """Redis 短暂不可用：丢弃当前帧并计数，不向消费者抛错（避免 DLQ 风暴）。"""
    redis = FakeRedis()
    redis.fail = True
    registry = SessionRegistry(redis, cache_ttl_seconds=0)
    producer = FakeProducer()
    forwarder = VilForwarder(producer, registry, target_hz=10)

    await forwarder.handle(telemetry(1))
    assert producer.records == []
    assert forwarder.drop_counts.get("session_unavailable") == 1
    await forwarder.close()


@pytest.mark.asyncio
async def test_forwarder_drops_out_of_order_and_too_late_frames() -> None:
    redis = FakeRedis()
    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(SESSION)
    registry = SessionRegistry(redis, cache_ttl_seconds=10)
    producer = FakeProducer()
    forwarder = VilForwarder(producer, registry, target_hz=10, max_source_lag_ms=500)

    await forwarder.handle(telemetry(5))
    assert [record[1]["seq"] for record in producer.records] == [5]

    # 乱序（seq 更小）丢弃
    await forwarder.handle(telemetry(4))
    assert [record[1]["seq"] for record in producer.records] == [5]

    # 迟到超过 500ms 丢弃
    await forwarder.handle(telemetry(6, timestamp=time.time() - 2.0))
    assert [record[1]["seq"] for record in producer.records] == [5]
    assert forwarder.drop_counts.get("out_of_order") == 1
    assert forwarder.drop_counts.get("too_late") == 1
    await forwarder.close()


@pytest.mark.asyncio
async def test_forwarder_clears_state_when_session_switches() -> None:
    redis = FakeRedis()
    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(SESSION)
    registry = SessionRegistry(redis, cache_ttl_seconds=0)  # 不缓存，便于切换会话
    producer = FakeProducer()
    forwarder = VilForwarder(producer, registry, target_hz=10)

    await forwarder.handle(telemetry(9))
    assert [record[1]["seq"] for record in producer.records] == [9]

    switched = {**SESSION, "sim_id": "sim-002"}
    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(switched)
    # 新会话允许 seq 从头开始（不得被旧会话 last_seq 拦截）
    await forwarder.handle(telemetry(1))
    assert [record[1]["sim_id"] for record in producer.records] == ["sim-001", "sim-002"]
    await forwarder.close()


@pytest.mark.asyncio
async def test_forwarder_prunes_idle_state() -> None:
    redis = FakeRedis()
    redis.values["vil:session:HUNTER-001"] = __import__("json").dumps(SESSION)
    registry = SessionRegistry(redis, cache_ttl_seconds=10)
    producer = FakeProducer()
    forwarder = VilForwarder(producer, registry, target_hz=10, idle_ttl_seconds=0.01)

    await forwarder.handle(telemetry(1))
    assert forwarder.tracked_sessions == 1
    # 强制跨过 30s 清理节流，验证空闲会话被回收
    forwarder._prune(time.time() + 31.0)
    assert forwarder.tracked_sessions == 0
    await forwarder.close()
