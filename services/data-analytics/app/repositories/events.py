"""data-collector 事件统计 REST 客户端（GET /api/v1/data/events）。

契约：事件计数使用 total 精确值；by_type 分布取最近 N 条近似统计；
下游不可用/超时返回 None（服务层降级），不抛出异常中断看板聚合。
"""
from __future__ import annotations

from typing import Any, Protocol

import httpx
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.events")


class EventSource(Protocol):
    """事件统计来源协议（服务层依赖倒置）。"""

    async def count(
        self,
        *,
        start_time: float,
        end_time: float,
        event_level: str | None = None,
        acknowledged: bool | None = None,
        vehicle_id: str | None = None,
    ) -> int | None:
        """窗口内事件精确计数（data.total）。"""
        ...

    async def fetch_latest(
        self, *, start_time: float, end_time: float, limit: int, vehicle_id: str | None = None
    ) -> list[dict[str, Any]] | None:
        """按 event_time DESC 抓取最近事件（用于类型分布近似统计）。"""
        ...


class DataCollectorEventClient:
    """data-collector 事件查询客户端（带超时与有限重试）。"""

    EVENTS_PATH = "/api/v1/data/events"

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 2.0,
        max_retries: int = 1,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def count(
        self,
        *,
        start_time: float,
        end_time: float,
        event_level: str | None = None,
        acknowledged: bool | None = None,
        vehicle_id: str | None = None,
    ) -> int | None:
        """窗口内事件精确计数；下游异常/未配置返回 None。"""
        params: dict[str, Any] = {"start_time": start_time, "end_time": end_time, "page": 1, "page_size": 1}
        if event_level is not None:
            params["event_level"] = event_level
        if acknowledged is not None:
            params["acknowledged"] = "true" if acknowledged else "false"
        if vehicle_id is not None:
            params["vehicle_id"] = vehicle_id
        payload = await self._get(params)
        if payload is None:
            return None
        data = payload.get("data") or {}
        total = data.get("total")
        return int(total) if total is not None else None

    async def fetch_latest(
        self, *, start_time: float, end_time: float, limit: int, vehicle_id: str | None = None
    ) -> list[dict[str, Any]] | None:
        """按 event_time DESC 抓取最近事件明细列表。"""
        params: dict[str, Any] = {
            "start_time": start_time,
            "end_time": end_time,
            "page": 1,
            "page_size": limit,
            "sort": "event_time",
            "order": "desc",
        }
        if vehicle_id is not None:
            params["vehicle_id"] = vehicle_id
        payload = await self._get(params)
        if payload is None:
            return None
        data = payload.get("data") or {}
        items = data.get("items")
        return list(items) if isinstance(items, list) else []

    async def _get(self, params: dict[str, Any]) -> dict[str, Any] | None:
        """GET 请求（重试 max_retries 次，全部失败返回 None 由上层降级）。"""
        if not self._base_url:
            # 依赖地址未配置：视为不可用（部署清单 x-hunter-required-env 应配置）
            logger.warning("data_collector_base_url_missing")
            return None
        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._client.get(f"{self._base_url}{self.EVENTS_PATH}", params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("data_collector_event_query_failed", attempt=attempt, error=str(exc))
        return None

    async def close(self) -> None:
        await self._client.aclose()