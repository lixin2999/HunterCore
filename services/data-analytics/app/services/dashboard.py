"""运营看板聚合服务：四数据块独立降级，全部不可用时抛 5001。"""
from __future__ import annotations

import asyncio
import time

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.events import EventSource
from app.repositories.fleet import VEHICLE_STATES, FleetSource
from app.repositories.pipeline import (
    IngestLatencyReading,
    PipelineSource,
    undetermined_cause,
)
from app.repositories.vehicles import VehicleSource
from app.schemas.dashboard import (
    TIME_RANGE_SECONDS,
    AlgorithmHealth,
    DashboardData,
    DashboardTimeRange,
    EventStats,
    FleetOverview,
    PipelineStats,
    UndeterminedGroup,
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

#: 指标键不存在/已过期时的口径（V1.19.21）：这是**作业侧**问题（没跑 / 没消费到），
#: 与写端报回来的**数据侧**丢弃诊断（reason/dropped）不是一回事，必须分开说。
LATENCY_KEY_ABSENT_REASON = (
    "入库延迟指标键不存在或已过期（analytics:ingest_latency）：Flink data_quality_monitor 未产出"
    "——先核对作业是否 RUNNING（flink list -a）与消费组 data-analytics-telemetry-raw 是否消费到"
    " telemetry_raw（巡检 ⑯ 可三分：未产出 / 正在回放 / 样本全丢弃）"
)


def ingest_latency_diagnostic(reading: IngestLatencyReading) -> str | None:
    """入库延迟的诊断文案（出数时 None）；三档必可区分（V1.19.21）。

    ①有 P95 → None；②键在但无有效样本 → 写端 reason 译文（哪条丢弃路径、多少条）；
    ③键不在 → 作业侧口径。**不给降级块拼 reason**（本指标仍不计入 ``available``，避免一个
    估算指标把整个 pipeline 块标成不可用）。
    """
    if reading.p95_ms is not None:
        return None
    if reading.reason is not None:
        return reading.reason
    return LATENCY_KEY_ABSENT_REASON


def undetermined_groups(probe: dict[str, str]) -> list[UndeterminedGroup]:
    """把探测侧 ``group → 成因码`` 翻成响应体条目（V1.21.14：**只查表，不重新判定**）。

    成因码 → 人话文案由 :func:`app.repositories.pipeline.undetermined_cause` 唯一提供；
    本函数只负责组装（顺序即探测顺序，看板按此逐行渲染）。
    """
    items: list[UndeterminedGroup] = []
    for group, cause in probe.items():
        copy = undetermined_cause(cause)
        items.append(
            UndeterminedGroup(group=group, cause=cause, cause_text=copy.label, hint=copy.hint)
        )
    return items


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
        """管道块：遥测样本数 + 消费组滞后 + DLQ 水位 + 入库延迟 P95（任一不可用仅降级该字段）。"""
        points, lag_probe, dlq, latency = await asyncio.gather(
            self._pipeline.telemetry_points(start, end),
            self._pipeline.consumer_lag(),
            self._pipeline.dlq_depth(),
            self._pipeline.ingest_latency(),
        )
        # ℞ V1.19.14：探测侧已把「组不存在 / 无提交位点」从 lag 映射里剔出，此处只负责原样透出；
        #   lag_probe is None 才是「Kafka 整体不可达」（走降级），与「读不到位点」不是同一回事。
        # ℞ V1.21.14：第二个返回值由「组名清单」升级为「组 → 成因码」，此处翻成带处置提示的条目，
        #   看板才能在页面上说清「为什么未判定」（过去四个组只显示同一个「未判定」，形同没数据）。
        lag_map, unknown = lag_probe if lag_probe is not None else ({}, {})
        reasons: list[str] = []
        if points is None:
            reasons.append("database unavailable")
        if lag_probe is None or dlq is None:
            reasons.append("kafka unavailable")
        # latency 为可选指标（不计入降级理由），但 **null 必须带原因**（V1.19.21）：过去只渲染成
        #   一个 `-`，运维无法区分「作业没跑」「没数据」「样本全被丢弃」三种完全不同的故障。
        return PipelineStats(
            available=not reasons,
            reason="; ".join(dict.fromkeys(reasons)) if reasons else None,
            telemetry_points=points,
            ingest_latency_ms_p95=latency.p95_ms,
            ingest_latency_reason=ingest_latency_diagnostic(latency),
            kafka_consumer_lag=lag_map,
            unknown_groups=undetermined_groups(unknown),
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
        """事件统计块：单次**精确聚合**（V1.21.16）。

        - 由 data-collector ``GET /api/v1/data/events/stats`` 一次取回
          total / 未确认 / 未确认分级 / 按类型（SQL GROUP BY，**非近似采样**）；
        - ``info``/``warning``/``critical`` = **未确认**各等级：与卡片「未确认事件」同口径，
          三级之和等于 ``unacknowledged``；此前误用「窗口内各级总数（含已确认）」，
          现场表现为「主值 0 而分项 提示 29」这类自相矛盾的读数；
        - ``by_type`` = 精确计数，不再是最接近 N 条近似（看板 Top N 与事件列表从此同口径）；
        - 下游不可用（None）→ 整块降级（reason 不变）。
        """
        stats = await self._events.stats(start_time=start, end_time=end, vehicle_id=vehicle_id)
        if stats is None:
            return EventStats(available=False, reason="data-collector unavailable", total=0)
        by_level = stats.get("unacknowledged_by_level") or {}
        by_type = stats.get("by_type") or {}
        return EventStats(
            available=True,
            total=int(stats.get("total") or 0),
            info=int(by_level.get("info", 0)),
            warning=int(by_level.get("warning", 0)),
            critical=int(by_level.get("critical", 0)),
            unacknowledged=int(stats.get("unacknowledged") or 0),
            by_type={str(key): int(value) for key, value in by_type.items()},
        )