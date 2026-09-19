"""车辆升级记录模型（契约 components.schemas：OtaRecordItem → OtaRecordListResponse）。

字段与 DDL 03_ota.sql `ota_svc.ota_records` 一一对应；
``duration_seconds`` 为服务端派生字段（end_time - start_time，未结束为 null）。
"""
from __future__ import annotations

from uuid import UUID

from hunter_common.database.enums import OtaStatus
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import VEHICLE_ID_PATTERN, OtaApiResponse

# 供 OtaRecordList.summary 引用（任务级聚合摘要，仅 /tasks/{task_id}/records 返回）
from app.schemas.tasks import OtaTaskProgress


class OtaRecordItem(BaseModel):
    """`ota_svc.ota_records` 单行（契约 OtaRecordItem；字段与 DDL 一一对应）。"""

    model_config = ConfigDict(extra="forbid")

    record_id: int = Field(description="BIGSERIAL 主键")
    task_id: UUID = Field(description="升级任务 ID")
    vehicle_id: str = Field(pattern=VEHICLE_ID_PATTERN, description="车辆标识")
    from_version: str | None = Field(default=None, description="升级前版本（下发时取车辆当前版本；未知为 null）")
    to_version: str = Field(description="目标版本名（= ota_versions.version_name；DDL NOT NULL）")
    status: OtaStatus = Field(description="升级整体状态（车端 9 态状态机）")
    phase: OtaStatus = Field(description="当前执行阶段（与 status 同域）")
    progress: int = Field(ge=0, le=100, description="车端上报百分比")
    error_code: str | None = Field(
        default=None,
        max_length=64,
        description="车端/平台错误码（DDL TEXT：'6001'/'6002'/'6003'/车端自定义码）",
    )
    error_message: str | None = Field(
        default=None, description="失败原因（车端上报 ≤512 字符；禁止含密钥/证书内容）"
    )
    start_time: float = Field(description="记录创建时间（Unix epoch 秒；DDL DEFAULT now()）")
    end_time: float | None = Field(
        default=None, description="到达终态时间（SUCCESS/ROLLED_BACK/FAILED）"
    )
    duration_seconds: float | None = Field(
        default=None, description="服务端派生 = end_time - start_time（未结束为 null）"
    )


class OtaRecordList(BaseModel):
    """升级记录分页数据（契约 OtaRecordList；排序 start_time DESC NULLS LAST, record_id DESC）。"""

    model_config = ConfigDict(extra="forbid")

    items: list[OtaRecordItem] = Field(default_factory=list, description="升级记录列表")
    total: int = Field(ge=0, description="满足条件的总记录数")
    page: int = Field(ge=1, description="页码")
    page_size: int = Field(ge=1, le=200, description="每页条数")
    summary: OtaTaskProgress | None = Field(
        default=None,
        description="任务级聚合摘要（仅 /tasks/{task_id}/records 返回，便于前端渲染批次卡片）",
    )


class OtaRecordListResponse(OtaApiResponse):
    """GET /tasks/{task_id}/records 与 GET /vehicles/{vehicle_id}/records 统一响应。"""

    data: OtaRecordList | None = None


__all__ = [
    "OtaRecordItem",
    "OtaRecordList",
    "OtaRecordListResponse",
]
