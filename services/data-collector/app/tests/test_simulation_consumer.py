"""仿真端节点级回传消费者单元测试（无 Kafka/DB/Redis 依赖）。

覆盖点：
- 消费组/订阅模式与契约一致（data-collector-simulation / simulation.*.result）；
- 与 CarlaSensorConsumer 同构（复用分发/落库/缓存），幂等命名空间按 group_id 隔离；
- 代表性处理路径（sim_telemetry 批量入库 / 可映射事件镜像 events+event_raw）。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from app.consumers.carla import CarlaSensorConsumer
from app.consumers.simulation import SimulationNodeConsumer

ROOT = Path(__file__).resolve().parents[4]
CONSUMER_GROUPS = ROOT / "contracts" / "kafka" / "consumer-groups.yaml"


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
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
        pass


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


def make_consumer() -> tuple[
    SimulationNodeConsumer, FakeRedis, FakeRepo, FakeRepo, FakePipeline, FakeSensorFiles
]:
    consumer = SimulationNodeConsumer.__new__(SimulationNodeConsumer)
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
    consumer._pending_rows = []  # type: ignore[attr-defined]
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


def test_group_and_subscription_are_distinct_from_carla() -> None:
    assert issubclass(SimulationNodeConsumer, CarlaSensorConsumer)
    assert SimulationNodeConsumer.group_id == "data-collector-simulation"
    assert SimulationNodeConsumer.subscribe_pattern == "simulation.*.result"
    assert CarlaSensorConsumer.group_id != SimulationNodeConsumer.group_id
    assert (
        CarlaSensorConsumer.subscribe_pattern != SimulationNodeConsumer.subscribe_pattern
    )


def test_group_matches_consumer_groups_contract() -> None:
    """消费组/订阅/幂等键必须与 consumer-groups.yaml 登记逐项一致（禁止自造组名）。"""
    groups = {g["group_id"]: g for g in yaml.safe_load(CONSUMER_GROUPS.read_text(encoding="utf-8"))["groups"]}
    group = groups[SimulationNodeConsumer.group_id]
    assert group["service"] == "data-collector"
    assert SimulationNodeConsumer.subscribe_pattern in group["subscribes"]
    assert group["idempotency_key"] == "(sim_id, seq)"


def test_idempotency_key_is_sim_id_colon_seq() -> None:
    key = SimulationNodeConsumer.idempotency_key(None, envelope("sim_status", {"status": "RUNNING"}, seq=7))  # type: ignore[arg-type]
    assert key == "sim-001:7"


@pytest.mark.asyncio
async def test_sim_telemetry_batched_into_vehicle_telemetry_with_sim_prefix() -> None:
    consumer, _redis, telemetry, _events, _pipeline, _files = make_consumer()
    await consumer.process(
        envelope("sim_telemetry", {"pose": {"x": 1.0, "y": 2.0}, "velocity": 0.5, "steering_angle": 0.0}),
        topic="simulation.node-1.result",
    )
    assert telemetry.rows == []  # 缓冲，批次收尾统一入库
    await consumer.flush()
    assert len(telemetry.rows) == 1
    assert telemetry.rows[0]["vehicle_id"] == "SIM-sim-001"


@pytest.mark.asyncio
async def test_mapped_event_mirrors_to_events_and_event_raw() -> None:
    consumer, _redis, _telemetry, events, pipeline, _files = make_consumer()
    await consumer.process(
        envelope(
            "sim_event",
            {
                "event_id": "e-2",
                "event_type": "emergency_brake",
                "event_level": "critical",
                "sim_time": 2.0,
                "description": "紧急制动",
            },
        ),
        topic="simulation.node-1.result",
    )
    assert len(events.rows) == 1
    assert events.rows[0]["vehicle_id"] == "SIM-sim-001"
    assert events.rows[0]["event_type"].value == "emergency_stop"
    assert pipeline.events and pipeline.events[0][1] == "SIM-sim-001"
