"""场景覆盖率服务（6.3 节）：只读最近一次覆盖率分析文档 + max_cells 截断。"""
from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from hunter_common.exceptions import InvalidParameterError, ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.eval_documents import EvalDocumentRepository
from app.schemas.common import EvalWindow
from app.schemas.coverage import SceneCoverageData

logger = get_logger("app.services.coverage")

_SECONDS_PER_DAY = 86400.0
#: 覆盖率网格默认边长（米；文档缺省字段时兜底，正常由 Spark 作业写入）
_DEFAULT_GRID_SIZE_M = 10.0
_WINDOW_EPSILON = 1e-6


class CoverageService:
    """覆盖率查询（窗口/车辆过滤 + 热度图截断）。"""

    def __init__(self, repository: EvalDocumentRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings

    async def coverage(
        self, *, start_time: float | None, end_time: float | None, vehicle_id: str | None, max_cells: int
    ) -> SceneCoverageData:
        """覆盖率查询（文档缺失→503；不匹配→空数据；heatmap 超 max_cells 截断）。"""
        self._validate_query_window(start_time, end_time)
        doc = await self._repository.load("scene_coverage")
        if doc is None:
            raise ServiceUnavailableError("场景覆盖率文档尚未生成")
        if not self._window_matches(doc, start_time, end_time) or not self._vehicle_matches(doc, vehicle_id):
            return self._empty_coverage(doc, start_time, end_time)
        data = SceneCoverageData.model_validate(doc)
        # heatmap 支持按 max_cells 截断（契约：默认 5000，上限 50000，截断置 heatmap_truncated=true）
        if len(data.heatmap) > max_cells:
            data = data.model_copy(update={"heatmap": data.heatmap[:max_cells], "heatmap_truncated": True})
        return data

    async def _load_document(self) -> dict[str, Any] | None:
        return await self._repository.load("scene_coverage")

    @staticmethod
    def _window_matches(doc: Mapping[str, Any], start_time: float | None, end_time: float | None) -> bool:
        """查询窗口与文档窗口一致（未指定窗口→取最近文档）。"""
        if start_time is None or end_time is None:
            return True
        doc_window = doc.get("window") or {}
        try:
            doc_start = float(doc_window.get("start_time", float("nan")))
            doc_end = float(doc_window.get("end_time", float("nan")))
        except (TypeError, ValueError):
            return False
        return abs(doc_start - start_time) <= _WINDOW_EPSILON and abs(doc_end - end_time) <= _WINDOW_EPSILON

    @staticmethod
    def _vehicle_matches(doc: Mapping[str, Any], vehicle_id: str | None) -> bool:
        """查询车辆与文档车辆一致（车队级文档仅匹配车队级查询）。"""
        return doc.get("vehicle_id") == vehicle_id

    def _empty_coverage(
        self, doc: Mapping[str, Any], start_time: float | None, end_time: float | None
    ) -> SceneCoverageData:
        """窗口/车辆不匹配的空覆盖响应（契约：空 coverage_cells/uncovered_areas + 文档元信息）。"""
        if start_time is None or end_time is None:
            doc_window = doc.get("window") or {}
            start_time = float(doc_window.get("start_time", 0.0))
            end_time = float(doc_window.get("end_time", 0.0))
        return SceneCoverageData(
            vehicle_id=None,
            window=EvalWindow(start_time=start_time, end_time=end_time),
            grid_size_m=float(doc.get("grid_size_m") or _DEFAULT_GRID_SIZE_M),
            covered_cells=0,
            total_cells=0,
            coverage_ratio=0.0,
            heatmap=[],
            heatmap_truncated=False,
            uncovered=[],
            scene_type_coverage={},
            data_source=str(doc.get("data_source") or "spark_batch"),
            job_name=None,
            updated_at=float(doc.get("updated_at") or time.time()),
            report_id=None,
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