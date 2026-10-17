"""data-collector 事件统计 REST 客户端（GET /api/v1/data/events/stats）。

V1.21.16：看板事件块改用 data-collector 的**精确聚合端点**——此前 `by_type` 与未确认分级
靠客户端拉「最近 200 条近似」，与事件列表的精确值不一致（现场被读成数据打架）；
下游不可用/超时返回 None（服务层降级），不抛出异常中断看板聚合。
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import httpx
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.events")


class EventSource(Protocol):
    """事件统计来源协议（服务层依赖倒置）。"""

    async def stats(
        self, *, start_time: float, end_time: float, vehicle_id: str | None = None
    ) -> dict[str, Any] | None:
        """窗口内事件精确聚合（total / unacknowledged / unacknowledged_by_level / by_type）。"""
        ...


class DataCollectorEventClient:
    """data-collector 事件聚合客户端（带超时与有限重试）。"""

    EVENT_STATS_PATH = "/api/v1/data/events/stats"

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 2.0,
        max_retries: int = 1,
        client: httpx.AsyncClient | None = None,
        headers_provider: Callable[[], dict[str, str]] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._client = client or httpx.AsyncClient(timeout=timeout)
        # 服务间直连身份头提供者（逐请求调用以生成新鲜 HMAC 时间戳）
        self._headers_provider = headers_provider

    async def stats(
        self, *, start_time: float, end_time: float, vehicle_id: str | None = None
    ) -> dict[str, Any] | None:
        """窗口内事件精确聚合；下游异常/未配置返回 None。"""
        params: dict[str, Any] = {"start_time": start_time, "end_time": end_time}
        if vehicle_id is not None:
            params["vehicle_id"] = vehicle_id
        payload = await self._get(self.EVENT_STATS_PATH, params)
        if payload is None:
            return None
        data = payload.get("data")
        return data if isinstance(data, dict) else None

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """GET 请求（重试 max_retries 次，全部失败返回 None 由上层降级）。"""
        if not self._base_url:
            # 依赖地址未配置：视为不可用（部署清单 x-hunter-required-env 应配置）
            logger.warning("data_collector_base_url_missing")
            return None
        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._client.get(
                    f"{self._base_url}{path}",
                    params=params,
                    headers=self._headers_provider() if self._headers_provider is not None else None,
                )
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("data_collector_event_query_failed", attempt=attempt, error=str(exc))
        return None

    async def close(self) -> None:
        await self._client.aclose()
