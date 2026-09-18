"""报告服务：列表 / 详情 / 异步生成提交（渲染由离线作业完成，REST 仅入队）。"""
from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from typing import Any

from hunter_common.exceptions import (
    InvalidParameterError,
    ResourceNotFoundError,
    ResourceStateConflictError,
    ServiceUnavailableError,
)
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.reports import ReportListFilter, ReportRepository
from app.repositories.vehicles import VehicleSource
from app.schemas.reports import (
    ReportGenerateData,
    ReportGenerateRequest,
    ReportListData,
    ReportMeta,
    ReportStatus,
    ReportType,
)

logger = get_logger("app.services.reports")

_SECONDS_PER_DAY = 86400.0
#: 车辆维度报告类型（6.5 节：vehicle_daily/ota_upgrade 必须指定 vehicle_id）
_VEHICLE_DIMENSION_TYPES = (ReportType.VEHICLE_DAILY, ReportType.OTA_UPGRADE)


class ReportService:
    """报告业务逻辑（仓储 + 车辆目录 + 配置注入，可独立单测）。"""

    def __init__(self, repository: ReportRepository, vehicles: VehicleSource, settings: Settings) -> None:
        self._repository = repository
        self._vehicles = vehicles
        self._settings = settings

    async def list_reports(
        self,
        *,
        report_type: ReportType | None,
        vehicle_id: str | None,
        status: ReportStatus | None,
        start_time: float | None,
        end_time: float | None,
        page: int,
        page_size: int,
    ) -> ReportListData:
        """报告列表（前缀扫描 + 过滤排序分页；sidecar 异常项跳过并告警）。"""
        self._validate_query_window(start_time, end_time)
        docs, total, page, page_size = await self._repository.list(
            ReportListFilter(
                report_type=report_type.value if report_type else None,
                vehicle_id=vehicle_id,
                status=status.value if status else None,
                start_time=start_time,
                end_time=end_time,
                page=page,
                page_size=page_size,
            )
        )
        items: list[ReportMeta] = []
        for doc in docs:
            try:
                items.append(ReportMeta.model_validate(doc))
            except Exception:  # noqa: BLE001 - 单条脏数据跳过并告警，不阻断列表聚合（契约要求尽力返回）
                logger.warning("report_meta_invalid", report_id=str(doc.get("report_id")))
        return ReportListData(items=items, total=total, page=page, page_size=page_size)

    async def get_report(self, report_id: str) -> ReportMeta:
        """报告详情（ready 时为各格式产物附加下载预签名 URL，15 分钟有效）。"""
        meta = await self._repository.get(report_id)
        if meta is None:
            raise ResourceNotFoundError("报告不存在")
        if meta.get("status") == ReportStatus.READY.value:
            meta = await self._attach_presigned_artifacts(meta)
        try:
            return ReportMeta.model_validate(meta)
        except Exception as exc:
            logger.exception("report_meta_invalid", report_id=report_id)
            raise ServiceUnavailableError("报告元信息损坏") from exc

    async def _attach_presigned_artifacts(self, meta: Mapping[str, Any]) -> dict[str, Any]:
        """为 ready 报告的产物生成下载预签名（契约固定 900s，支持 Range）。"""
        expires = self._settings.report_download_expires_seconds
        artifacts: list[dict[str, Any]] = []
        for artifact in meta.get("artifacts") or []:
            artifact = dict(artifact)
            object_key = str(artifact.get("object_key") or "")
            if object_key:
                url = await self._repository.storage.presign_get(object_key, expires)
                artifact["download_url"] = url
                artifact["expires_in"] = expires
            artifacts.append(artifact)
        return {**dict(meta), "artifacts": artifacts}

    async def submit(self, req: ReportGenerateRequest, user_id: str) -> ReportGenerateData:
        """提交报告生成（校验→车辆校验→去重→并发守卫→写 sidecar 入队）。"""
        now = time.time()
        # 窗口校验（2001）
        if req.end_time <= req.start_time:
            raise InvalidParameterError("end_time 必须大于 start_time")
        max_span = self._settings.report_max_range_days * _SECONDS_PER_DAY
        if req.end_time - req.start_time > max_span:
            raise InvalidParameterError(f"报告统计窗口跨度不能超过 {self._settings.report_max_range_days} 天")
        # 车辆维度必填 + 存在性校验（404；下游不可用则延后校验，不阻塞提交）
        if req.report_type in _VEHICLE_DIMENSION_TYPES and not req.vehicle_id:
            raise InvalidParameterError("车辆维度报告必须指定 vehicle_id")
        if req.vehicle_id:
            exists = await self._vehicles.vehicle_exists(req.vehicle_id)
            if exists is False:
                raise ResourceNotFoundError("车辆不存在")
            # exists is None：vehicle-service 不可用 → 延后校验（离线作业侧兜底）
        # 防重复提交（3003）：同参数（模板+车辆+窗口）报告已在生成中
        if not req.force:
            duplicated = await self._repository.has_duplicate_in_flight(
                report_type=req.report_type.value,
                vehicle_id=req.vehicle_id,
                start_time=req.start_time,
                end_time=req.end_time,
            )
            if duplicated:
                raise ResourceStateConflictError("同参数报告已在生成中")
        # 并发守卫（3003）：生成中报告数达到上限（x-hunter-rate-limits.heavy_protection）
        in_flight = await self._repository.count_in_flight()
        if in_flight >= self._settings.report_generate_max_concurrent:
            raise ResourceStateConflictError("报告生成并发数已达上限")
        # 写 sidecar 入队（pending），由离线作业消费生成（x-hunter-pending-confirmation #2）
        report_id = str(uuid.uuid4())
        meta: dict[str, Any] = {
            "report_id": report_id,
            "report_type": req.report_type.value,
            "status": ReportStatus.PENDING.value,
            "vehicle_id": req.vehicle_id,
            "start_time": req.start_time,
            "end_time": req.end_time,
            "formats": [fmt.value for fmt in req.formats],
            "created_at": now,
            "completed_at": None,
            "generated_by": user_id,
            "trigger": "manual",
            "artifacts": [],
            "summary": {},
        }
        await self._repository.save(meta)
        logger.info(
            "report_submitted",
            report_id=report_id,
            report_type=req.report_type.value,
            vehicle_id=req.vehicle_id,
        )
        return ReportGenerateData(
            report_id=report_id,
            report_type=req.report_type,
            status=ReportStatus.PENDING,
            created_at=now,
            estimated_ready_seconds=self._settings.estimated_ready_seconds(req.report_type.value),
            poll_url=f"/api/v1/analytics/reports/{report_id}",
        )

    def _validate_query_window(self, start_time: float | None, end_time: float | None) -> None:
        """查询窗口校验：成对出现 + 顺序 + 跨度 ≤ analytics_query_max_range_days（2001）。"""
        if (start_time is None) != (end_time is None):
            raise InvalidParameterError("start_time 与 end_time 必须成对出现")
        if start_time is None or end_time is None:
            return
        if end_time <= start_time:
            raise InvalidParameterError("end_time 必须大于 start_time")
        max_span = self._settings.analytics_query_max_range_days * _SECONDS_PER_DAY
        if end_time - start_time > max_span:
            raise InvalidParameterError(f"查询窗口跨度不能超过 {self._settings.analytics_query_max_range_days} 天")