"""Corner Case 检索服务（6.4 节）：挖掘文档内过滤 + 排序 + 分页 + 分类计数。"""
from __future__ import annotations

from collections import Counter
from typing import Any

from hunter_common.exceptions import InvalidParameterError, ServiceUnavailableError
from hunter_common.logging import get_logger
from pydantic import ValidationError

from app.config import Settings
from app.repositories.eval_documents import EvalDocumentRepository
from app.schemas.corner_cases import (
    CornerCaseAlgorithm,
    CornerCaseCategory,
    CornerCaseItem,
    CornerCaseListData,
    CornerCaseMiningMeta,
)

logger = get_logger("app.services.corner_cases")

_SECONDS_PER_DAY = 86400.0


class CornerCaseService:
    """Corner Case 检索（文档内存过滤；默认 anomaly_score DESC、event_time DESC）。"""

    def __init__(self, repository: EvalDocumentRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings

    async def list_cases(
        self,
        *,
        page: int,
        page_size: int,
        category: CornerCaseCategory | None,
        vehicle_id: str | None,
        algorithm: CornerCaseAlgorithm | None,
        min_anomaly_score: float | None,
        start_time: float | None,
        end_time: float | None,
    ) -> CornerCaseListData:
        """检索流程：窗口校验→读文档→逐条过滤→排序→分页→挖掘元信息/分类计数。"""
        self._validate_query_window(start_time, end_time)
        doc = await self._repository.load("corner_cases")
        if doc is None:
            raise ServiceUnavailableError("Corner Case 挖掘文档尚未生成")
        items = self._parse_items(doc.get("items") or [])
        # 契约过滤参数：category/vehicle_id/algorithm/min_anomaly_score/时间窗（前闭后闭）
        if category is not None:
            items = [item for item in items if item.category == category]
        if vehicle_id is not None:
            items = [item for item in items if item.vehicle_id == vehicle_id]
        if algorithm is not None:
            items = [item for item in items if item.algorithm == algorithm]
        if min_anomaly_score is not None:
            items = [item for item in items if item.anomaly_score >= min_anomaly_score]
        if start_time is not None and end_time is not None:
            items = [item for item in items if start_time <= item.event_time <= end_time]
        # 默认排序：anomaly_score DESC，次级 event_time DESC（契约端点说明）
        items.sort(key=lambda item: (-item.anomaly_score, -item.event_time))
        total = len(items)
        offset = (page - 1) * page_size
        mining = CornerCaseMiningMeta.model_validate(doc.get("mining") or {})
        return CornerCaseListData(
            items=items[offset : offset + page_size],
            total=total,
            page=page,
            page_size=page_size,
            mining=mining,
            # category_counts 为过滤后全集统计（不受分页影响，供前端分布图）
            category_counts=dict(Counter(item.category.value for item in items)),
        )

    @staticmethod
    def _parse_items(raw_items: list[Any]) -> list[CornerCaseItem]:
        """逐条解析（脏数据跳过并告警，不阻断整页检索）。"""
        items: list[CornerCaseItem] = []
        for raw in raw_items:
            try:
                items.append(CornerCaseItem.model_validate(raw))
            except ValidationError:
                logger.warning("corner_case_item_invalid")
        return items

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