"""运营看板聚合服务：四数据块独立降级，全部不可用时抛 5001。"""
from __future__ import annotations

import asyncio
import time
from collections import Counter

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.events import EventSource
from app.repositories.fleet import VEHICLE_STATES, FleetSource
from app.repositories.pipeline import PipelineSource
from app.repositories.vehicles import VehicleSource
from app.schemas.dashboard import (
    TIME_RANGE_SECONDS,
    AlgorithmHealth,
    DashboardData,
    DashboardTimeRange,
    EventStats,
    FleetOverview,
    PipelineStats,
)

logger = get_logger("app.services.dashboard")

_BLOCKS = ("fleet", "pipeline", "algorithm", "events")
#: 看板关注的核心指标键（其余指标进 metrics 透出）
_CORE_ALGORITHM_METRIC_KEYS = (
    "perception.fps",
    "perception.latency_ms",
    "planning.planning_latency_ms",
    "control.control_latency_ms",
)


class DashboardService:
    """看板聚合（asyncio.gather 并发取数；任一块失败仅降级该块）。"""

    def __init__(
        self,
        fleet: FleetSource,
        pipeline: PipelineSource,
        events: EventSource,
        vehicles: VehicleSource,
        settings: Settings,
    ) -> None:
        self._fleet = fleet
        self._pipeline = pipeline
        self._events = events
        self._vehicles = vehicles
        self._settings = settings

    async def aggregate(self, *, time_range: DashboardTimeRange, vehicle_id: str | None = None) -> DashboardData:
        """四块并发聚合；全部降级时抛 ServiceUnavailableError（503/5001）。"""
        window = TIME_RANGE_SECONDS[time_range]  # 路由 Literal 保证键存在
        end = time.time()
        start = end - window
        fleet, pipeline_stats, algorithm, events = await asyncio.gather(
            self._fleet_block(),
            self._pipeline_block(start, end),
            self._algorithm_block(start, end),
            self._events_block(start, end, vehicle_id),
        )
        degraded = [
            name
            for name, block in zip(_BLOCKS, (fleet, pipeline_stats, algorithm, events), strict=True)
            if not block.available
        ]
        if len(degraded) == len(_BLOCKS):
            raise ServiceUnavailableError("看板全部数据源均不可用")
        return DashboardData(
            time_range=time_range,
            vehicle_id=vehicle_id,
            generated_at=end,
            fleet=fleet,
            pipeline=pipeline_stats,
            algorithm=algorithm,
            events=events,
            degraded=degraded,
        )

    async def _fleet_block(self) -> FleetOverview:
        """车队概览块：Redis 状态计数 + vehicle-service 总数（8 态补齐，缺失兜底 offline）。"""
        try:
            online = await self._fleet.online_count()
            by_status = await self._fleet.status_distribution()
        except Exception:
            logger.exception("fleet_block_failed")
            return FleetOverview(available=False, reason="redis unavailable", total_vehicles=0, online_vehicles=0)
        total = await self._vehicles.fleet_total()
        if total is None:
            return FleetOverview(
                available=False, reason="vehicle-service unavailable", total_vehicles=0, online_vehicles=online
            )
        merged = {state: int(by_status.get(state, 0)) for state in VEHICLE_STATES}
        # 状态 Hash 缺失（尚未上报）的车辆兜底计入 offline，保证总数一致
        merged["offline"] += max(0, total - sum(merged.values()))
        return FleetOverview(available=True, total_vehicles=total, online_vehicles=online, by_status=merged)

    async def _pipeline_block(self, start: float, end: float) -> PipelineStats:
        """管道块：遥测样本数 + 消费组滞后 + DLQ 水位（任一不可用即整块降级但保留可用字段）。"""
        points = await self._pipeline.telemetry_points(start, end)
        lag = await self._pipeline.consumer_lag()
        dlq = await self._pipeline.dlq_depth()
        reasons: list[str] = []
        if points is None:
            reasons.append("database unavailable")
        if lag is None or dlq is None:
            reasons.append("kafka unavailable")
        return PipelineStats(
            available=not reasons,
            reason="; ".join(dict.fromkeys(reasons)) if reasons else None,
            # 入库延迟 P95 由 Flink 入库延迟统计作业补齐（契约 pending 项，暂置 null）
            telemetry_points=points,
            ingest_latency_ms_p95=None,
            kafka_consumer_lag=lag or {},
            dlq_messages=dlq or {},
        )

    async def _algorithm_block(self, start: float, end: float) -> AlgorithmHealth:
        """算法健康块：algorithm_metrics 窗口均值（核心指标单列，其余透出 metrics）。"""
        averages = await self._pipeline.algorithm_averages(start, end)
        if averages is None:
            return AlgorithmHealth(available=False, reason="database unavailable")
        return AlgorithmHealth(
            available=True,
            perception_fps_avg=averages.get("perception.fps"),
            perception_latency_ms_avg=averages.get("perception.latency_ms"),
            planning_latency_ms_avg=averages.get("planning.planning_latency_ms"),
            control_latency_ms_avg=averages.get("control.control_latency_ms"),
            metrics={k: v for k, v in averages.items() if k not in _CORE_ALGORITHM_METRIC_KEYS},
        )

    async def _events_block(self, start: float, end: float, vehicle_id: str | None) -> EventStats:
        """事件统计块：total 精确计数 + 最近 N 条类型分布近似（下游不可用整块降级）。"""
        total = await self._events.count(start_time=start, end_time=end, vehicle_id=vehicle_id)
        info = await self._events.count(start_time=start, end_time=end, event_level="info", vehicle_id=vehicle_id)
        warning = await self._events.count(start_time=start, end_time=end, event_level="warning", vehicle_id=vehicle_id)
        critical = await self._events.count(start_time=start, end_time=end, event_level="critical", vehicle_id=vehicle_id)
        unacknowledged = await self._events.count(
            start_time=start, end_time=end, acknowledged=False, vehicle_id=vehicle_id
        )
        latest = await self._events.fetch_latest(
            start_time=start,
            end_time=end,
            limit=self._settings.dashboard_event_fetch_size,
            vehicle_id=vehicle_id,
        )
        if None in (total, info, warning, critical, unacknowledged, latest):
            return EventStats(available=False, reason="data-collector unavailable", total=0)
        counter: Counter[str] = Counter()
        for item in latest or []:
            event_type = item.get("event_type")
            if event_type:
                counter[str(event_type)] += 1
        return EventStats(
            available=True,
            total=int(total or 0),
            info=info,
            warning=warning,
            critical=critical,
            unacknowledged=unacknowledged,
            # by_type 仅统计最近 200 条（data-collector 无聚合端点，见契约端点说明）
            by_type=dict(counter),
        )