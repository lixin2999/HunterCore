"""算法评估服务：感知精度（6.2.4）/ 控制性能（6.3.3）评估文档读取。

文档由 Flink/Spark 作业写入 MinIO（指针 latest.json），本服务只读最近一次结果
（P95 保护：不落库、不实时计算）；窗口/车辆不匹配或文档缺失时按契约降级。
"""
from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from hunter_common.exceptions import InvalidParameterError, ServiceUnavailableError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.eval_documents import EvalDocumentRepository
from app.schemas.common import EvalWindow
from app.schemas.evaluation import (
    CONTROL_THRESHOLD_UNITS,
    CONTROL_THRESHOLD_VALUES,
    ControlEvalData,
    ControlMetrics,
    ControlThresholdCheck,
    ControlThresholdChecks,
    PerceptionEvalData,
    PerceptionMetrics,
)

logger = get_logger("app.services.evaluation")

_SECONDS_PER_DAY = 86400.0
#: 窗口相等比较容差（浮点 epoch 秒）
_WINDOW_EPSILON = 1e-6


class EvaluationService:
    """评估文档查询（窗口过滤 + 车辆过滤 + 空数据降级）。"""

    def __init__(self, repository: EvalDocumentRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings

    async def perception(
        self, *, start_time: float | None, end_time: float | None, vehicle_id: str | None
    ) -> PerceptionEvalData:
        """感知精度评估（文档缺失→503；窗口/车辆不匹配→空数据降级）。"""
        self._validate_query_window(start_time, end_time)
        doc = await self._load_document("perception", "感知精度评估")
        if not self._window_matches(doc, start_time, end_time) or not self._vehicle_matches(doc, vehicle_id):
            return self._empty_perception(doc, start_time, end_time)
        return PerceptionEvalData.model_validate(doc)

    async def control(
        self, *, start_time: float | None, end_time: float | None, vehicle_id: str | None
    ) -> ControlEvalData:
        """控制性能评估（阈值判定取文档；空数据时按 6.3.3 常量合成阈值）。"""
        self._validate_query_window(start_time, end_time)
        doc = await self._load_document("control", "控制性能评估")
        if not self._window_matches(doc, start_time, end_time) or not self._vehicle_matches(doc, vehicle_id):
            return self._empty_control(doc, start_time, end_time)
        return ControlEvalData.model_validate(doc)

    async def _load_document(self, kind: str, label: str) -> dict[str, Any]:
        doc = await self._repository.load(kind)
        if doc is None:
            raise ServiceUnavailableError(f"{label}文档尚未生成")
        return doc

    @staticmethod
    def _window_matches(doc: Mapping[str, Any], start_time: float | None, end_time: float | None) -> bool:
        """查询窗口与文档窗口是否一致（未指定窗口→取最近文档）。"""
        if start_time is None or end_time is None:
            return True
        doc_window = doc.get("window") or {}
        doc_start = float(doc_window.get("start_time", float("nan")))
        doc_end = float(doc_window.get("end_time", float("nan")))
        return abs(doc_start - start_time) <= _WINDOW_EPSILON and abs(doc_end - end_time) <= _WINDOW_EPSILON

    @staticmethod
    def _vehicle_matches(doc: Mapping[str, Any], vehicle_id: str | None) -> bool:
        """查询车辆与文档车辆一致（文档车队级=仅匹配车队级查询；车辆级=精确匹配）。"""
        return doc.get("vehicle_id") == vehicle_id

    @staticmethod
    def _empty_window(doc: Mapping[str, Any], start_time: float | None, end_time: float | None) -> EvalWindow:
        """空数据响应窗口：优先查询窗口，否则回退文档窗口。"""
        if start_time is None or end_time is None:
            doc_window = doc.get("window") or {}
            start_time = float(doc_window.get("start_time", 0.0))
            end_time = float(doc_window.get("end_time", 0.0))
        return EvalWindow(start_time=start_time, end_time=end_time)

    @staticmethod
    def _doc_meta(doc: Mapping[str, Any]) -> tuple[str, float]:
        """空数据响应公共字段：(data_source, updated_at)（取文档值，便于前端标注）。"""
        data_source = str(doc.get("data_source") or "spark_batch")
        updated_at = float(doc.get("updated_at") or time.time())
        return data_source, updated_at

    def _empty_perception(
        self, doc: Mapping[str, Any], start_time: float | None, end_time: float | None
    ) -> PerceptionEvalData:
        """窗口/车辆不匹配的空感知响应（契约：返回空 metrics + data_source/updated_at）。"""
        data_source, updated_at = self._doc_meta(doc)
        return PerceptionEvalData(
            vehicle_id=None,
            window=self._empty_window(doc, start_time, end_time),
            sample_count=0,
            metrics=PerceptionMetrics(
                map_3d=0.0, map_bev=0.0, iou=0.0, recall=0.0, precision=0.0, mean_localization_error_m=0.0
            ),
            by_object_type={},
            data_source=data_source,
            job_name=None,
            updated_at=updated_at,
            report_id=None,
        )

    def _empty_control(
        self, doc: Mapping[str, Any], start_time: float | None, end_time: float | None
    ) -> ControlEvalData:
        """窗口/车辆不匹配的空控制响应（阈值按 6.3.3 常量合成；sample_count=0 → overall_pass=False）。"""
        data_source, updated_at = self._doc_meta(doc)
        checks = {
            name: ControlThresholdCheck(
                value=0.0,
                threshold=CONTROL_THRESHOLD_VALUES[name],
                comparator="lt",
                unit=CONTROL_THRESHOLD_UNITS[name],
                pass_=0.0 < CONTROL_THRESHOLD_VALUES[name],
            )
            for name in CONTROL_THRESHOLD_VALUES
        }
        return ControlEvalData(
            vehicle_id=None,
            window=self._empty_window(doc, start_time, end_time),
            sample_count=0,
            metrics=ControlMetrics(
                velocity_rmse_ms=0.0, steering_rmse_rad=0.0, overshoot_percent=0.0, settling_time_s=0.0
            ),
            thresholds=ControlThresholdChecks(**checks),
            # 无样本不判定达标（overall_pass 语义：全部指标在有效样本上达标）
            overall_pass=False,
            data_source=data_source,
            job_name=None,
            updated_at=updated_at,
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