"""场景覆盖率 Schema（6.3 节，10m×10m 航迹网格）。"""
from __future__ import annotations

from typing import Literal

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import EvalWindow

#: 覆盖率网格边长上限（米，网格边长 0.5~10m；不可放宽）
GRID_SIZE_MAX_M: float = 10.0


class CoverageHeatmapCell(BaseModel):
    """覆盖热度图单元。"""

    x: float = Field(description="网格中心 x（m，全车坐标系）")
    y: float = Field(description="网格中心 y（m，全车坐标系）")
    count: int = Field(ge=0, description="该网格样本计数")


class UncoveredArea(BaseModel):
    """未覆盖区域说明。"""

    kind: Literal["region", "scene_type"] = Field(description="未覆盖类型")
    description: str = Field(description="区域/场景描述")
    reason: str = Field(description="未覆盖原因")
    sample_count: int = Field(ge=0, description="该区域样本计数")


class SceneCoverageData(BaseModel):
    """场景覆盖率数据体（6.3 节）。"""

    model_config = ConfigDict(populate_by_name=True)

    vehicle_id: str | None = Field(default=None, description="车辆标识；null 表示车队级")
    window: EvalWindow = Field(description="覆盖率统计窗口")
    grid_size_m: float = Field(gt=0, le=GRID_SIZE_MAX_M, description="网格边长（m，默认 10m×10m）")
    covered_cells: int = Field(ge=0, description="已覆盖网格数")
    total_cells: int = Field(ge=0, description="总网格数")
    coverage_ratio: float = Field(ge=0, le=1, description="覆盖率（covered_cells/total_cells）")
    heatmap: list[CoverageHeatmapCell] = Field(default_factory=list, description="热度图（按 count DESC 排序，支持 max_cells 截断）")
    heatmap_truncated: bool = Field(default=False, description="热度图是否被 max_cells 截断")
    uncovered: list[UncoveredArea] = Field(default_factory=list, description="未覆盖区域列表")
    scene_type_coverage: dict[str, float] = Field(
        default_factory=dict, description="分场景类型覆盖率（键对应 scenes.scene_type，0~1）"
    )
    data_source: str = Field(description="覆盖率分析数据源说明")
    job_name: str | None = Field(default=None, description="Flink/Spark 作业名")
    updated_at: float = Field(description="覆盖率文档更新时间（Unix epoch 秒）")
    report_id: str | None = Field(default=None, description="关联报告 ID")


class SceneCoverageResponse(ApiResponse[SceneCoverageData]):
    """GET /api/v1/analytics/scene/coverage 响应。"""