"""遥测查询服务（GET /telemetry 业务逻辑）。

契约（x-hunter-telemetry-query-contract）：
- 必选 vehicle_id/start_time/end_time；区间跨度超过配置上限（默认 24h）时
  截断 end_time（206 Partial Content：截断为服务端行为，非错误，响应带
  X-Truncated-Range: seconds=<被截去的秒数>）；
- 样本按 time DESC 分页；retention_days 固定 90。
"""
from __future__ import annotations

from hunter_common.database.repository import DEFAULT_PAGE_SIZE
from hunter_common.exceptions import InvalidParameterError

from app.config import settings
from app.repositories.telemetry import TelemetryRepository, telemetry_row_to_dict
from app.schemas.telemetry import TelemetryQueryData, TelemetrySample

_PARAM_ERROR = 2001


class TelemetryService:
    """遥测查询服务（repository → 契约响应组装）。"""

    def __init__(self, repository: TelemetryRepository) -> None:
        self._repository = repository

    @staticmethod
    def _resolve_range(start_time: float, end_time: float) -> tuple[float, int]:
        """区间校验与截断；返回 (生效 end_time, truncated_seconds)。

        - 起止倒置 → 2001 参数错误；
        - 跨度超限 → 截断 end_time（206 语义：服务端行为非错误）。
        """
        if end_time <= start_time:
            raise InvalidParameterError(
                message="end_time 必须大于 start_time（2001 参数错误，region=start_time/end_time）"
            )
        max_span_seconds = settings.telemetry_query_max_range_hours * 3600
        if end_time - start_time > max_span_seconds:
            return start_time + max_span_seconds, int(end_time - start_time - max_span_seconds)
        return end_time, 0

    async def query_telemetry(
        self,
        vehicle_id: str,
        start_time: float,
        end_time: float,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> tuple[TelemetryQueryData, int]:
        """遥测查询；返回 (数据体, truncated_seconds)。

        truncated_seconds > 0 时路由层返回 206 并携带 X-Truncated-Range 头。
        """
        effective_end, truncated_seconds = self._resolve_range(start_time, end_time)
        rows, total = await self._repository.query_page(
            vehicle_id=vehicle_id,
            start_time=start_time,
            end_time=effective_end,
            page=page,
            page_size=page_size,
        )
        samples = [TelemetrySample.model_validate(telemetry_row_to_dict(row)) for row in rows]
        data = TelemetryQueryData(
            items=samples,
            total=total,
            page=page,
            page_size=page_size,
            retention_days=90,
        )
        return data, truncated_seconds


__all__ = ["_PARAM_ERROR", "TelemetryService"]
