"""运营看板 Schema（GET /api/v1/analytics/dashboard 聚合响应）。"""
from __future__ import annotations

from typing import Literal

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, Field

#: 看板时间窗口受控词表（契约 time_range 枚举，不可新增）
TIME_RANGE_KEYS: tuple[str, ...] = ("1h", "24h", "7d", "30d")

#: time_range → 统计窗口秒数（用于管道/事件块的时间界计算）
TIME_RANGE_SECONDS: dict[str, float] = {
    "1h": 3600.0,
    "24h": 86400.0,
    "7d": 604800.0,
    "30d": 2592000.0,
}

DashboardTimeRange = Literal["1h", "24h", "7d", "30d"]


class FleetOverview(BaseModel):
    """车队概览块（available=false 表示该块降级，reason 给出原因）。"""

    available: bool = Field(description="数据源是否可用（false 表示降级展示）")
    reason: str | None = Field(default=None, description="不可用原因")
    total_vehicles: int = Field(ge=0, description="注册车辆总数")
    online_vehicles: int = Field(ge=0, description="当前在线车辆数")
    by_status: dict[str, int] = Field(
        default_factory=dict,
        description="按车辆状态计数（offline/online_idle/auto_driving/remote_controlled/upgrading/charging/fault/emergency）",
    )


class PipelineStats(BaseModel):
    """数据管道块（遥测样本 / 消费滞后 / DLQ 水位）。"""

    available: bool = Field(description="数据源是否可用（false 表示降级展示）")
    reason: str | None = Field(default=None, description="不可用原因")
    telemetry_points: int | None = Field(default=None, ge=0, description="窗口内遥测样本数")
    ingest_latency_ms_p95: float | None = Field(default=None, description="遥测入库延迟 P95（ms），待入库延迟统计作业补齐")
    kafka_consumer_lag: dict[str, int] = Field(
        default_factory=dict, description="按消费组的消息滞后总数（group → lag）"
    )
    dlq_messages: dict[str, int] = Field(default_factory=dict, description="DLQ（dead_letter_queue）消息堆积数")


class AlgorithmHealth(BaseModel):
    """算法健康块（algorithm_metrics 窗口聚合，模块=perception/planning/control）。"""

    available: bool = Field(description="数据源是否可用（false 表示降级展示）")
    reason: str | None = Field(default=None, description="不可用原因")
    perception_fps_avg: float | None = Field(default=None, description="感知帧率均值（fps）")
    perception_latency_ms_avg: float | None = Field(default=None, description="感知延迟均值（ms）")
    planning_latency_ms_avg: float | None = Field(default=None, description="规划延迟均值（ms）")
    control_latency_ms_avg: float | None = Field(default=None, description="控制延迟均值（ms）")
    metrics: dict[str, float] = Field(default_factory=dict, description="其余指标均值（{module}.{metric_name} → avg）")


class EventStats(BaseModel):
    """事件统计块（data-collector GET /api/v1/data/events total 精确计数）。"""

    available: bool = Field(description="数据源是否可用（false 表示降级展示）")
    reason: str | None = Field(default=None, description="不可用原因")
    total: int = Field(ge=0, description="窗口内事件总数")
    info: int | None = Field(default=None, ge=0, description="info 级事件数")
    warning: int | None = Field(default=None, ge=0, description="warning 级事件数")
    critical: int | None = Field(default=None, ge=0, description="critical 级事件数")
    unacknowledged: int | None = Field(default=None, ge=0, description="未确认事件数")
    by_type: dict[str, int] = Field(default_factory=dict, description="按事件类型计数（最近 200 条近似）")


class DashboardData(BaseModel):
    """看板聚合数据体。"""

    time_range: DashboardTimeRange = Field(description="统计时间窗口")
    vehicle_id: str | None = Field(default=None, description="车辆过滤（可选）")
    generated_at: float = Field(description="聚合生成时间（Unix epoch 秒）")
    fleet: FleetOverview = Field(description="车队概览块")
    pipeline: PipelineStats = Field(description="数据管道块")
    algorithm: AlgorithmHealth = Field(description="算法健康块")
    events: EventStats = Field(description="事件统计块")
    degraded: list[str] = Field(default_factory=list, description="降级数据块列表（fleet/pipeline/algorithm/events 子集）")


class DashboardResponse(ApiResponse[DashboardData]):
    """GET /api/v1/analytics/dashboard 响应。"""