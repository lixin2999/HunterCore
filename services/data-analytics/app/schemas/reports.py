"""报告相关 Schema（contracts/openapi/data-analytics.yaml components.schemas）。"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from hunter_common.responses import ApiResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.common import VEHICLE_ID_PATTERN

#: 报告模板类型受控词表（6.5 节 5 类，不可新增）
REPORT_TYPE_KEYS: tuple[str, ...] = ("vehicle_daily", "algorithm_eval", "scene_test", "ota_upgrade", "monthly_operation")


class ReportType(str, Enum):
    """报告模板类型（6.5 节 5 类标准模板，不可新增）。"""

    VEHICLE_DAILY = "vehicle_daily"
    ALGORITHM_EVAL = "algorithm_eval"
    SCENE_TEST = "scene_test"
    OTA_UPGRADE = "ota_upgrade"
    MONTHLY_OPERATION = "monthly_operation"


class ReportFormat(str, Enum):
    """报告输出格式（6.5 节：HTML 交互式图表 / PDF / JSON）。"""

    HTML = "html"
    PDF = "pdf"
    JSON = "json"


class ReportStatus(str, Enum):
    """报告状态（异步生成流程，见契约 x-hunter-pending-confirmation #2）。"""

    PENDING = "pending"
    GENERATING = "generating"
    READY = "ready"
    FAILED = "failed"


class ReportArtifact(BaseModel):
    """单格式产物（MinIO hunter-reports 对象 + 下载预签名 URL）。"""

    model_config = ConfigDict(populate_by_name=True)

    format: ReportFormat = Field(description="产物格式")
    object_key: str = Field(description="MinIO 对象键（hunter-reports/<report_type>/<date>/<report_id>.<ext>）")
    size_bytes: int = Field(ge=0, description="产物大小（字节）")
    download_url: str | None = Field(default=None, description="下载预签名 URL（15 分钟有效，支持 Range）")
    expires_in: int | None = Field(default=None, ge=0, description="预签名有效期（秒）")


class ReportMeta(BaseModel):
    """报告元信息（MinIO sidecar JSON 字段；无 DB 表，见 x-hunter-report-storage）。"""

    model_config = ConfigDict(populate_by_name=True)

    report_id: str = Field(description="报告 ID（UUID）")
    report_type: ReportType = Field(description="报告模板类型")
    status: ReportStatus = Field(description="报告状态")
    vehicle_id: str | None = Field(default=None, pattern=VEHICLE_ID_PATTERN, description="车辆标识（车辆维度报告）；车队级为 null")
    start_time: float | None = Field(default=None, description="报告统计窗口起点（Unix epoch 秒）")
    end_time: float | None = Field(default=None, description="报告统计窗口终点（Unix epoch 秒）")
    formats: list[ReportFormat] = Field(min_length=1, description="请求生成的格式集合")
    created_at: float = Field(description="提交时间（Unix epoch 秒）")
    completed_at: float | None = Field(default=None, description="完成时间（ready/failed 时填充）")
    generated_by: str | None = Field(default=None, description="提交人 user_id（网关 X-User-Id）")
    trigger: Literal["manual", "schedule"] = Field(default="manual", description="触发方式")
    artifacts: list[ReportArtifact] = Field(default_factory=list, description="各格式产物（详情接口附预签名 URL）")
    summary: dict[str, Any] = Field(default_factory=dict, description="报告摘要指标（结构随模板而定）")


class ReportListData(BaseModel):
    """报告列表数据体。"""

    items: list[ReportMeta] = Field(description="报告元信息列表")
    total: int = Field(ge=0, description="符合条件的报告总数")
    page: int = Field(ge=1, description="当前页码")
    page_size: int = Field(ge=1, description="每页条数")


class ReportListResponse(ApiResponse[ReportListData]):
    """GET /api/v1/analytics/reports 响应（artifacts 不附 download_url）。"""


class ReportDetailResponse(ApiResponse[ReportMeta]):
    """GET /api/v1/analytics/reports/{report_id} 响应（ready 时 artifacts 附预签名 URL）。"""


class ReportGenerateRequest(BaseModel):
    """POST /api/v1/analytics/reports/generate 请求体（202 异步受理）。"""

    model_config = ConfigDict(populate_by_name=True)

    report_type: ReportType = Field(description="报告模板类型（6.5 节 5 类，必填）")
    vehicle_id: str | None = Field(default=None, pattern=VEHICLE_ID_PATTERN, description="目标车辆（vehicle_daily/ota_upgrade 必填）")
    start_time: float = Field(ge=0, description="报告统计窗口起点（Unix epoch 秒，必填）")
    end_time: float = Field(ge=0, description="报告统计窗口终点（Unix epoch 秒，必填）")
    formats: list[ReportFormat] = Field(min_length=1, description="输出格式（默认 html）")
    force: bool = Field(default=False, description="跳过重复提交检查（默认 false，谨慎使用）")

    @field_validator("formats")
    @classmethod
    def _dedupe_formats(cls, value: list[ReportFormat]) -> list[ReportFormat]:
        """formats 不可重复（契约 uniqueItems: true）。"""
        if len(set(value)) != len(value):
            raise ValueError("formats 存在重复项")
        return value


class ReportGenerateData(BaseModel):
    """报告受理数据体（202 Accepted）。"""

    report_id: str = Field(description="报告 ID（UUID，生成完成后轮询详情）")
    report_type: ReportType = Field(description="报告模板类型")
    status: ReportStatus = Field(description="受理后状态（pending）")
    created_at: float = Field(description="受理时间（Unix epoch 秒）")
    estimated_ready_seconds: int | None = Field(default=None, description="预计就绪时间（秒，未配置为 null，仅提示用途）")
    poll_url: str = Field(description="报告详情轮询地址")


class ReportGenerateResponse(ApiResponse[ReportGenerateData]):
    """POST /api/v1/analytics/reports/generate 响应。"""