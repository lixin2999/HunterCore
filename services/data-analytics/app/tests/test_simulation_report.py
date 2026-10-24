"""仿真评估报告生成单元测试（无 MinIO/Redis/Kafka）。"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from app.consumers.carla import CarlaSimulationConsumer
from app.repositories.reports import ReportRepository
from app.services.simulation import (
    SimulationReportService,
    is_terminal_status,
    summarize_metrics,
)


class FakeStorage:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}

    async def put_json(self, key: str, payload: Mapping[str, Any]) -> None:
        self.docs[key] = dict(payload)

    async def get_json(self, key: str) -> dict[str, Any] | None:
        return self.docs.get(key)

    async def list_json(self, prefix: str, *, limit: int | None = None) -> list[tuple[str, dict[str, Any]]]:
        items = [(key, value) for key, value in self.docs.items() if key.startswith(prefix)]
        return items[:limit] if limit else items

    async def presign_get(self, key: str, expires_in: int) -> str:
        return f"https://minio/{key}?expires={expires_in}"

    async def healthcheck(self) -> bool:
        return True

    async def close(self) -> None:
        return None


def test_is_terminal_status() -> None:
    assert is_terminal_status("COMPLETED")
    assert is_terminal_status("canceled")
    assert not is_terminal_status("RUNNING")
    assert not is_terminal_status(None)


def test_summarize_metrics_extracts_safety_and_grade() -> None:
    summary = summarize_metrics(
        {
            "safety": {"collision_count": 0, "min_ttc": 4.2},
            "efficiency": {"avg_speed": 5.0},
            "comfort": {"max_acceleration": 1.2},
            "task": {"completion": 1.0},
            "success_criteria_result": {"passed": True},
            "grade": "A",
            "score": 90,
        }
    )
    assert summary["safety"]["collisions"] == 0.0
    assert summary["safety"]["min_ttc"] == 4.2
    assert summary["grade"] == "A"
    assert summary["score"] == 90.0
    assert summary["passed"] is True


@pytest.mark.asyncio
async def test_generate_writes_report_sidecar_and_pointer() -> None:
    storage = FakeStorage()
    repository = ReportRepository(storage, prefix="reports")
    service = SimulationReportService(
        storage, repository, pointer_key="eval/simulation/latest.json"
    )
    result = await service.generate(
        sim_id="sim-001",
        scene_id="scene-1",
        vehicle_id=None,
        status="COMPLETED",
        metrics={"safety": {"collision_count": 0}, "grade": "A", "score": 95},
        timestamp=1724035200.0,
    )
    assert result["report_id"] == SimulationReportService.report_id_for("sim-001")
    assert result["object_key"].startswith("scene_test/2024-08-19/")
    report = storage.docs[result["object_key"]]
    assert report["report_type"] == "scene_test"
    assert report["sim_id"] == "sim-001"
    assert report["summary"]["grade"] == "A"
    sidecar = storage.docs[f"reports/scene_test/{result['report_id']}.json"]
    assert sidecar["status"] == "ready"
    assert sidecar["sim_id"] == "sim-001"
    pointer = storage.docs["eval/simulation/latest.json"]
    assert pointer["report_id"] == result["report_id"]
    assert pointer["object_key"] == result["object_key"]
    assert "eval/simulation/sim-001.json" in storage.docs


@pytest.mark.asyncio
async def test_report_id_is_stable_for_replay() -> None:
    storage = FakeStorage()
    service = SimulationReportService(
        storage, ReportRepository(storage), pointer_key="eval/simulation/latest.json"
    )
    first = await service.generate(
        sim_id="sim-001", scene_id=None, vehicle_id=None, status="FAILED", metrics={}
    )
    second = await service.generate(
        sim_id="sim-001", scene_id=None, vehicle_id=None, status="FAILED", metrics={}
    )
    assert first["report_id"] == second["report_id"]
    assert first["object_key"] == second["object_key"]


class _FakeRedis:
    async def get(self, _key: str) -> None:
        return None

    async def lrange(self, _key: str, _start: int, _end: int) -> list[str]:
        return []


class _FakeReports:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def generate(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {}


@pytest.mark.asyncio
async def test_terminal_status_uses_own_group_cache_not_other_consumer_group() -> None:
    """data-analytics-carla 不能依赖 data-collector-carla 先写 Redis（两个组独立消费）。

    同一 key（sim_id）内消息有序：先到的事件/指标必须被本组缓存，终态状态到达即可生成
    完整报告，即使 Redis 中尚无 data-collector 写入的缓存。
    """
    consumer = object.__new__(CarlaSimulationConsumer)
    reports = _FakeReports()
    consumer._redis = _FakeRedis()  # type: ignore[attr-defined]
    consumer._reports = reports  # type: ignore[attr-defined]
    consumer._status_cache = {}  # type: ignore[attr-defined]
    consumer._metrics_cache = {}  # type: ignore[attr-defined]
    consumer._events_cache = {}  # type: ignore[attr-defined]

    await consumer.process(
        {
            "sim_id": "sim-1",
            "scene_id": "scene-1",
            "vehicle_id": None,
            "timestamp": 1724035200.0,
            "type": "sim_event",
            "payload": {"event": "collision"},
        },
        topic="carla.sim-1.sensor_data",
    )
    await consumer.process(
        {
            "sim_id": "sim-1",
            "scene_id": "scene-1",
            "vehicle_id": None,
            "timestamp": 1724035201.0,
            "type": "sim_metrics",
            "payload": {"safety": {"collision_count": 1}, "score": 60},
        },
        topic="carla.sim-1.sensor_data",
    )
    await consumer.process(
        {
            "sim_id": "sim-1",
            "scene_id": "scene-1",
            "vehicle_id": None,
            "timestamp": 1724035202.0,
            "type": "sim_status",
            "payload": {"status": "COMPLETED"},
        },
        topic="carla.sim-1.sensor_data",
    )

    assert len(reports.calls) == 1
    call = reports.calls[0]
    assert call["sim_id"] == "sim-1"
    assert call["status"] == "COMPLETED"
    assert call["metrics"]["score"] == 60
    assert call["events"] == [{"event": "collision"}]
