"""CARLA 回传消费者单元测试（无 Kafka/DB/Redis 依赖）。"""
from __future__ import annotations

import json
import time
from typing import Any

import pytest

from app.consumers.carla import (
    SIM_EVENT_TYPE_MAP,
    CarlaSensorConsumer,
    build_sim_telemetry_row,
    sim_vehicle_id,
)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.expires: dict[str, int] = {}

    async def set(self, key: str, value: str, *, ex: int | None = None) -> None:
        self.values[key] = value
        if ex is not None:
            self.expires[key] = ex

    async def lpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        self.lists[key] = self.lists.get(key, [])[start : end + 1]

    async def expire(self, key: str, ttl: int) -> None:
        self.expires[key] = ttl

    async def hset(self, key: str, field: str, value: str) -> None:
        self.hashes.setdefault(key, {})[field] = value


class FakeTx:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_exc: object) -> None:
        return None


class FakeRepo:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def transaction(self) -> FakeTx:
        return FakeTx()

    async def insert_points(self, _session: object, rows: list[dict[str, Any]]) -> int:
        self.rows.extend(rows)
        return len(rows)

    async def insert_events(self, _session: object, rows: list[dict[str, Any]]) -> int:
        self.rows.extend(rows)
        return len(rows)


class FakePipeline:
    def __init__(self) -> None:
        self.events: list[tuple[dict[str, Any], str]] = []

    async def publish_event(self, payload: dict[str, Any], vehicle_id: str) -> None:
        self.events.append((payload, vehicle_id))


class FakeSensorFiles:
    def __init__(self) -> None:
        self.records: list[tuple[Any, ...]] = []

    async def publish(self, *args: Any) -> None:
        self.records.append(args)


def make_consumer() -> tuple[CarlaSensorConsumer, FakeRedis, FakeRepo, FakeRepo, FakePipeline, FakeSensorFiles]:
    consumer = CarlaSensorConsumer.__new__(CarlaSensorConsumer)
    redis = FakeRedis()
    telemetry = FakeRepo()
    events = FakeRepo()
    pipeline = FakePipeline()
    sensor_files = FakeSensorFiles()
    consumer._redis = redis  # type: ignore[attr-defined]
    consumer._telemetry = telemetry  # type: ignore[attr-defined]
    consumer._events = events  # type: ignore[attr-defined]
    consumer._pipeline = pipeline  # type: ignore[attr-defined]
    consumer._sensor_files = sensor_files  # type: ignore[attr-defined]
    consumer._ttl = 86400  # type: ignore[attr-defined]
    consumer._events_max = 100  # type: ignore[attr-defined]
    return consumer, redis, telemetry, events, pipeline, sensor_files


def envelope(msg_type: str, body: dict[str, Any], seq: int = 1) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "sim_id": "sim-001",
        "vehicle_id": None,
        "scene_id": "8f0c2a3e-6c1b-4a7e-9f2d-1b2c3d4e5f60",
        "timestamp": time.time(),
        "seq": seq,
        "type": msg_type,
        "payload": body,
    }


def test_sim_telemetry_maps_to_vehicle_telemetry_row() -> None:
    row = build_sim_telemetry_row(
        envelope(
            "sim_telemetry",
            {
                "pose": {"x": 10.0, "y": 20.0, "z": 0.5, "yaw": 1.2},
                "twist": {"linear": [1.0, 0.0, 0.0], "angular": [0.0, 0.0, 0.1]},
                "velocity": 1.5,
                "steering_angle": 0.2,
            },
        )
    )
    assert row["vehicle_id"] == "SIM-sim-001"
    assert row["velocity"] == 1.5
    assert row["steering_angle"] == 0.2
    assert row["x"] == 10.0
    assert row["heading"] == 1.2
    assert row["linear_velocity"] == [1.0, 0.0, 0.0]
    assert row["seq"] == 1


@pytest.mark.asyncio
async def test_sim_status_and_metrics_are_cached_with_ttl() -> None:
    consumer, redis, *_ = make_consumer()
    await consumer.process(
        envelope("sim_status", {"status": "RUNNING", "progress_percent": 10.0}), topic="t"
    )
    await consumer.process(
        envelope("sim_metrics", {"safety": {"collision_count": 0}}, seq=2), topic="t"
    )
    status = json.loads(redis.values["carla:sim:sim-001:status"])
    metrics = json.loads(redis.values["carla:sim:sim-001:metrics"])
    assert status["status"] == "RUNNING"
    assert status["sim_id"] == "sim-001"
    assert metrics["safety"]["collision_count"] == 0
    assert redis.expires["carla:sim:sim-001:status"] == 86400


@pytest.mark.asyncio
async def test_sim_telemetry_is_inserted() -> None:
    consumer, _redis, telemetry, _events, _pipeline, _files = make_consumer()
    await consumer.process(
        envelope(
            "sim_telemetry",
            {"pose": {"x": 1.0, "y": 2.0}, "velocity": 0.5, "steering_angle": 0.0},
        ),
        topic="t",
    )
    assert len(telemetry.rows) == 1
    assert telemetry.rows[0]["vehicle_id"] == "SIM-sim-001"


@pytest.mark.asyncio
async def test_unmapped_event_only_stored_in_redis() -> None:
    consumer, redis, _telemetry, events, pipeline, _files = make_consumer()
    await consumer.process(
        envelope(
            "sim_event",
            {
                "event_id": "e-1",
                "event_type": "red_light_run",
                "event_level": "warning",
                "sim_time": 1.0,
            },
        ),
        topic="t",
    )
    assert events.rows == []
    assert pipeline.events == []
    assert "carla:sim:sim-001:events" in redis.lists


@pytest.mark.asyncio
async def test_mapped_event_mirrors_to_events_and_event_raw() -> None:
    consumer, redis, _telemetry, events, pipeline, _files = make_consumer()
    await consumer.process(
        envelope(
            "sim_event",
            {
                "event_id": "e-2",
                "event_type": "collision",
                "event_level": "critical",
                "sim_time": 2.0,
                "description": "碰撞",
            },
        ),
        topic="t",
    )
    assert len(events.rows) == 1
    assert events.rows[0]["vehicle_id"] == "SIM-sim-001"
    assert events.rows[0]["event_type"].value == "collision_warning"
    assert len(pipeline.events) == 1
    assert pipeline.events[0][1] == "SIM-sim-001"
    assert "carla:sim:sim-001:events" in redis.lists
    assert SIM_EVENT_TYPE_MAP["collision"].value == "collision_warning"


def test_sim_vehicle_id_prefix() -> None:
    assert sim_vehicle_id("sim-001") == "SIM-sim-001"
