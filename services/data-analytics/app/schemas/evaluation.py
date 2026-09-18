"""算法评估 Schema（感知精度 6.2.4 / 控制性能 6.3.3，文档契约布局）。"""
from __future__ import annotations

from typing import Literal

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import EvalWindow

#: 控制性能阈值（阈值来源：设计文档 6.3.3 节，不可更改；comparator 均为 lt，优于阈值达标）
CONTROL_THRESHOLD_VALUES: dict[str, float] = {
    "velocity_rmse_ms": 0.2,
    "steering_rmse_rad": 0.02,
    "overshoot_percent": 10,
    "settling_time_s": 2,
}

#: 阈值单位（6.3.3 节）
CONTROL_THRESHOLD_UNITS: dict[str, str] = {
    "velocity_rmse_ms": "m/s",
    "steering_rmse_rad": "rad",
    "overshoot_percent": "%",
    "settling_time_s": "s",
}


class PerceptionMetrics(BaseModel):
    """感知精度指标（6.2.4 节）。"""

    map_3d: float = Field(ge=0, description="3D mAP（0~1）")
    map_bev: float = Field(ge=0, description="BEV mAP（0~1）")
    iou: float = Field(ge=0, description="分割/检测 IoU 均值（0~1）")
    recall: float = Field(ge=0, description="召回率（0~1）")
    precision: float = Field(ge=0, description="精确率（0~1）")
    mean_localization_error_m: float = Field(ge=0, description="平均定位误差（m）")


class PerceptionEvalData(BaseModel):
    """感知精度评估数据体（6.2.4 节）。"""

    model_config = ConfigDict(populate_by_name=True)

    vehicle_id: str | None = Field(default=None, description="车辆标识；null 表示车队级")
    window: EvalWindow = Field(description="评估统计窗口")
    sample_count: int = Field(ge=0, description="参与评估的帧数")
    metrics: PerceptionMetrics = Field(description="感知精度指标")
    by_object_type: dict[str, PerceptionMetrics] = Field(
        default_factory=dict, description="分目标类型指标（键：vehicle/pedestrian/cyclist/other）"
    )
    data_source: str = Field(description="评估数据源说明")
    job_name: str | None = Field(default=None, description="Flink/Spark 作业名")
    updated_at: float = Field(description="评估文档更新时间（Unix epoch 秒）")
    report_id: str | None = Field(default=None, description="关联报告 ID")


class PerceptionEvalResponse(ApiResponse[PerceptionEvalData]):
    """GET /api/v1/analytics/perception/eval 响应。"""


class ControlMetrics(BaseModel):
    """控制性能指标（6.3.3 节，随机抓取帧的统计值）。"""

    velocity_rmse_ms: float = Field(ge=0, description="速度跟踪 RMSE（m/s）")
    steering_rmse_rad: float = Field(ge=0, description="方向盘/前轮转角跟踪 RMSE（rad）")
    overshoot_percent: float = Field(ge=0, description="超调量（%，阶跃响应）")
    settling_time_s: float = Field(ge=0, description="调节时间（s，阶跃响应）")


class ControlThresholdCheck(BaseModel):
    """单指标阈值判定（阈值来源：设计文档 6.3.3 节，不可更改）。"""

    model_config = ConfigDict(populate_by_name=True)

    value: float = Field(description="实测值")
    threshold: float = Field(description="契约阈值（6.3.3 节）")
    comparator: Literal["lt", "lte", "gt", "gte"] = Field(description="判定方向（本服务阈值均为 lt）")
    unit: str | None = Field(default=None, description="单位（m/s、rad、%、s）")
    pass_: bool = Field(alias="pass", description="是否满足阈值")


class ControlThresholdChecks(BaseModel):
    """控制指标阈值判定集合（键与 ControlMetrics 一致）。"""

    model_config = ConfigDict(populate_by_name=True)

    velocity_rmse_ms: ControlThresholdCheck
    steering_rmse_rad: ControlThresholdCheck
    overshoot_percent: ControlThresholdCheck
    settling_time_s: ControlThresholdCheck


class ControlEvalData(BaseModel):
    """控制性能评估数据体（6.3.3 节）。"""

    model_config = ConfigDict(populate_by_name=True)

    vehicle_id: str | None = Field(default=None, description="车辆标识；null 表示车队级")
    window: EvalWindow = Field(description="评估统计窗口")
    sample_count: int = Field(ge=0, description="参与评估的控制帧数")
    metrics: ControlMetrics = Field(description="控制性能指标")
    thresholds: ControlThresholdChecks = Field(description="阈值判定（6.3.3 节常量）")
    overall_pass: bool = Field(description="全部指标是否达标")
    data_source: str = Field(description="评估数据源说明")
    job_name: str | None = Field(default=None, description="Flink/Spark 作业名")
    updated_at: float = Field(description="评估文档更新时间（Unix epoch 秒）")
    report_id: str | None = Field(default=None, description="关联报告 ID")


class ControlEvalResponse(ApiResponse[ControlEvalData]):
    """GET /api/v1/analytics/control/eval 响应。"""