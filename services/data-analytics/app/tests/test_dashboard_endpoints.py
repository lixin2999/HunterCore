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
            ingest_latency=812.5,
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
    assert pipeline["unknown_groups"] == []  # 齐位点时不应凭空多出未判定组
    # 入库延迟 P95 由 Flink data_quality_monitor→Redis 指标键透出（看板 pipeline 块只读）
    assert pipeline["ingest_latency_ms_p95"] == pytest.approx(812.5)
    # 有真值时不得附带诊断文案（避免看板把正常指标写成故障）
    # ⚠ 路由 `response_model_exclude_none=True`：null 字段不序列化，断言必须用 .get()
    assert pipeline.get("ingest_latency_reason") is None
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
async def test_dashboard_pipeline_unknown_groups_are_not_reported_as_zero(clean_state: None) -> None:
    """组不存在/无提交位点 → 不进 kafka_consumer_lag、只进 unknown_groups（V1.19.14 第七类假绿）。

    旧行为：四个组全部回 `0` 且 `available=true`，运维把「什么都没消费过」读成「零积压」。
    现要求两者**互斥**（同一组不得同时出现在映射与清单里），且 Kafka 仍可达 ⇒ 不算块降级。
    """
    _seed_dashboard(
        pipeline=FakePipelineRepository(
            points=1000,
            lag={"data-collector-telemetry": 7},
            unknown_groups=[
                "data-analytics-telemetry-alert",
                "data-analytics-telemetry-algo",
                "data-analytics-telemetry-raw",
                "data-analytics-algorithm-metrics",
            ],
            dlq={},
            averages={},
            ingest_latency=None,
        )
    )
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", headers=ANALYST_HEADERS)
    assert resp.status_code == 200
    pipeline = resp.json()["data"]["pipeline"]
    assert pipeline["available"] is True, "Kafka 可达而仅缺位点，不应把整块标为降级"
    assert pipeline["kafka_consumer_lag"] == {"data-collector-telemetry": 7}
    assert "data-analytics-telemetry-algo" in pipeline["unknown_groups"]
    assert not set(pipeline["kafka_consumer_lag"]) & set(pipeline["unknown_groups"]), "映射与清单必须互斥"
    assert 0 not in pipeline["kafka_consumer_lag"].values(), "未判定的组不得以 lag=0 混进映射"


@pytest.mark.asyncio
async def test_dashboard_ingest_latency_absent_key_says_job_never_emitted(clean_state: None) -> None:
    """指标键不存在（作业未产出）：看板必须拿到服务层口径，而不是一个无信息的 `-`（V1.19.21）。

    ⚠ 一个用例只 seed 一次：`_cached(request, KEY_DASHBOARD_SERVICE, ...)` 会把首次 seed 的
    服务缓存到本用例结束（再 seed 不会生效）。
    """
    _seed_dashboard(
        pipeline=FakePipelineRepository(
            points=1000, lag={}, dlq={}, averages={}, ingest_latency=None, ingest_key_present=False
        )
    )
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", headers=ANALYST_HEADERS)
    pipeline = resp.json()["data"]["pipeline"]
    assert pipeline["available"] is True, "入库延迟缺值不属于整块降级"
    assert pipeline.get("ingest_latency_ms_p95") is None, "exclude_none 下 null 不序列化"
    assert pipeline.get("ingest_latency_reason"), "无值必须带原因"
    assert "analytics:ingest_latency" in pipeline["ingest_latency_reason"]
    assert "data-analytics-telemetry-raw" in pipeline["ingest_latency_reason"], "必须给出可执行的下一步（查作业/巡检 ⑯）"


@pytest.mark.asyncio
async def test_dashboard_ingest_latency_writer_reason_is_passed_through_verbatim(clean_state: None) -> None:
    """写端已给丢弃诊断时逐字透传，不得叠用「键不存在」口径（两档不互混，V1.19.21）。"""
    _seed_dashboard(
        pipeline=FakePipelineRepository(
            points=1000,
            lag={},
            dlq={},
            averages={},
            ingest_latency=None,
            ingest_reason="车端时钟超前于 broker（负延迟一律丢弃，不伪造）（seen=12; negative_latency=12）",
        )
    )
    async with api() as client:
        resp = await client.get("/api/v1/analytics/dashboard", headers=ANALYST_HEADERS)
    pipeline = resp.json()["data"]["pipeline"]
    assert "负延迟" in pipeline["ingest_latency_reason"]
    assert "seen=12" in pipeline["ingest_latency_reason"]
    assert "analytics:ingest_latency" not in pipeline["ingest_latency_reason"]


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