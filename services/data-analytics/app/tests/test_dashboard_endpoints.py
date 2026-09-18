"""GET /api/v1/analytics/dashboard 端点测试（正常聚合 / 局部降级 / 全降级 503）。"""
from __future__ import annotations

import time
from typing import Any

import pytest

from app.core import dependencies as deps
from app.main import app
from app.tests.conftest import ANALYST_HEADERS, api
from app.tests.fakes import (
    FakeEventClient,
    FakeFleetRepository,
    FakePipelineRepository,
    FakeVehicleClient,
)


class UnavailableEventClient(FakeEventClient):
    """事件块不可用替身（模拟 data-collector 宕机）。"""

    async def count(self, **kwargs: Any) -> int | None:
        return None

    async def fetch_latest(self, **kwargs: Any) -> list[dict[str, Any]] | None:
        return None


def _seed_events() -> list[dict[str, Any]]:
    """3 条窗口内事件（1 warning / 1 info / 1 critical，其中 1 条未确认；时间取最近 1h 内）。"""
    now = time.time()
    return [
        {"event_type": "harsh_braking", "event_level": "warning", "acknowledged": True, "event_time": now - 3600.0, "vehicle_id": "HUNTER-001"},
        {"event_type": "battery_low", "event_level": "info", "acknowledged": True, "event_time": now - 3500.0, "vehicle_id": "HUNTER-001"},
        {"event_type": "collision_warning", "event_level": "critical", "acknowledged": False, "event_time": now - 3400.0, "vehicle_id": "HUNTER-002"},
    ]


def _seed_dashboard(
    *,
    fleet: FakeFleetRepository | None = None,
    pipeline: FakePipelineRepository | None = None,
    events: FakeEventClient | None = None,
    vehicles: FakeVehicleClient | None = None,
) -> None:
    """按依赖键注入看板替身。"""
    app.state.__setattr__(deps.KEY_FLEET_REPOSITORY, fleet or FakeFleetRepository(online=2, distribution={"online_idle": 2}))
    app.state.__setattr__(
        deps.KEY_PIPELINE_REPOSITORY,
        pipeline
        or FakePipelineRepository(
            points=1000,
            lag={"analytics-stream-consumer": 12},
            dlq={},
            averages={"perception.fps": 10.2, "planning.planning_latency_ms": 40.0},
        ),
    )
    app.state.__setattr__(deps.KEY_EVENT_CLIENT, events or FakeEventClient(_seed_events()))
    app.state.__setattr__(deps.KEY_VEHICLE_CLIENT, vehicles or FakeVehicleClient(exists={"HUNTER-001"}, total=5))


@pytest.mark.asyncio
async def test_dashboard_all_blocks_available(clean_state: None) -> None:
    """四块全部可用 → 200、degraded 为空、计数与均值正确（8 态补齐 offline=3）。"""
    _seed_dashboard()
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", params={"time_range": "24h"}, headers=ANALYST_HEADERS)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["degraded"] == []
    assert data["time_range"] == "24h"
    fleet = data["fleet"]
    assert fleet["available"] is True
    assert fleet["total_vehicles"] == 5
    assert fleet["online_vehicles"] == 2
    assert fleet["by_status"]["online_idle"] == 2
    assert fleet["by_status"]["offline"] == 3  # 5 - 2，缺失状态兜底 offline
    pipeline = data["pipeline"]
    assert pipeline["telemetry_points"] == 1000
    assert pipeline["kafka_consumer_lag"] == {"analytics-stream-consumer": 12}
    algorithm = data["algorithm"]
    assert algorithm["perception_fps_avg"] == pytest.approx(10.2)
    assert algorithm["planning_latency_ms_avg"] == pytest.approx(40.0)
    events = data["events"]
    assert events["total"] == 3
    assert events["warning"] == 1
    assert events["critical"] == 1
    assert events["unacknowledged"] == 1
    assert events["by_type"] == {"harsh_braking": 1, "battery_low": 1, "collision_warning": 1}


@pytest.mark.asyncio
async def test_dashboard_partial_degradation(clean_state: None) -> None:
    """事件块不可用 → 200 但 degraded=["events"]，其余块正常。"""
    _seed_dashboard(events=UnavailableEventClient())
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", headers=ANALYST_HEADERS)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["degraded"] == ["events"]
    assert data["events"]["available"] is False
    assert data["events"]["total"] == 0
    assert data["fleet"]["available"] is True


@pytest.mark.asyncio
async def test_dashboard_all_degraded_service_unavailable(clean_state: None) -> None:
    """全部数据源不可用 → 503 code=5001。"""
    _seed_dashboard(
        fleet=FakeFleetRepository(),
        pipeline=FakePipelineRepository(points=None, lag=None, dlq=None, averages=None),
        events=UnavailableEventClient(),
        vehicles=FakeVehicleClient(exists=set(), total=None),
    )
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", headers=ANALYST_HEADERS)
    assert resp.status_code == 503
    assert resp.json()["code"] == 5001


@pytest.mark.asyncio
async def test_dashboard_invalid_time_range(clean_state: None) -> None:
    """time_range 超出受控词表 → 422/2001。"""
    _seed_dashboard()
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", params={"time_range": "2h"}, headers=ANALYST_HEADERS)
    assert resp.status_code == 422
    assert resp.json()["code"] == 2001


@pytest.mark.asyncio
async def test_dashboard_requires_authentication(clean_state: None) -> None:
    """缺 X-User-Id → 401 code=1001。"""
    _seed_dashboard()
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard")
    assert resp.status_code == 401
    assert resp.json()["code"] == 1001