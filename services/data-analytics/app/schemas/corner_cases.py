"""Corner Case Schema（6.4 节，挖掘 + 自动转场景）。"""
from __future__ import annotations

from enum import Enum
from typing import Any

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import VEHICLE_ID_PATTERN, EvalWindow


class CornerCaseCategory(str, Enum):
    """Corner Case 类别受控词表（6.4 节 4 类，不可新增）。"""

    KINEMATIC = "kinematic"
    OBJECT = "object"
    ENVIRONMENT = "environment"
    INTERACTION = "interaction"


class CornerCaseAlgorithm(str, Enum):
    """挖掘算法受控词表（6.4 节 2 类 + 预留扩展，禁改既有键）。"""

    ISOLATION_FOREST = "isolation_forest"
    DBSCAN = "dbscan"


class CornerCaseItem(BaseModel):
    """Corner Case 明细。"""

    model_config = ConfigDict(populate_by_name=True)

    corner_case_id: str = Field(description="用例 ID（UUID）")
    category: CornerCaseCategory = Field(description="类别")
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="来源车辆")
    event_time: float = Field(description="用例中心点时间（Unix epoch 秒）")
    window_start: float = Field(description="前后文片段起点（Unix epoch 秒，默认前后 5s）")
    window_end: float = Field(description="前后文片段终点（Unix epoch 秒，默认前后 5s）")
    anomaly_score: float = Field(ge=0, le=1, description="异常度评分（归一化，越高越异常）")
    algorithm: CornerCaseAlgorithm = Field(description="挖掘算法")
    cluster_id: str | None = Field(default=None, description="聚类簇 ID（dbscan）")
    trigger_event_type: str | None = Field(default=None, description="关联事件类型（events.event_type，可空）")
    description: str = Field(description="用例描述")
    metrics: dict[str, Any] = Field(default_factory=dict, description="相关指标快照")
    data_file_url: str | None = Field(default=None, description="前后文 rosbag 片段 URL（MinIO）")
    scene_id: str | None = Field(default=None, description="关联场景 ID（转场景后回填）")


class CornerCaseMiningMeta(BaseModel):
    """挖掘作业元信息。"""

    model_config = ConfigDict(populate_by_name=True)

    algorithm: list[CornerCaseAlgorithm] = Field(description="本次挖掘使用的算法列表")
    last_run_at: float = Field(description="最近运行时间（Unix epoch 秒）")
    job_name: str = Field(description="挖掘作业名")
    window: EvalWindow = Field(description="挖掘数据窗口")
    feature_count: int = Field(ge=0, description="特征维度数")
    total_found: int = Field(ge=0, description="挖掘出的用例总数")


class CornerCaseListData(BaseModel):
    """Corner Case 检索数据体。"""

    items: list[CornerCaseItem] = Field(description="当前页用例列表")
    total: int = Field(ge=0, description="过滤后用例总数")
    page: int = Field(ge=1, description="当前页码")
    page_size: int = Field(ge=1, description="每页条数")
    mining: CornerCaseMiningMeta = Field(description="挖掘作业元信息")
    category_counts: dict[str, int] = Field(
        description="按类别计数（过滤后全集，供前端分布图使用）"
    )


class CornerCaseListResponse(ApiResponse[CornerCaseListData]):
    """GET /api/v1/analytics/corner-cases 响应。"""