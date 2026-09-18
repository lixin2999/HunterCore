"""vehicle-service 客户端：车队总数 + 车辆存在性校验（降级安全）。

契约：GET /api/v1/vehicle/{vehicle_id}（3001=车辆不存在→映射 404）；
车辆服务不可用时返回 None，由服务层决定延后校验（不阻塞提交）。
"""
from __future__ import annotations

from typing import Protocol

import httpx
from hunter_common.logging import get_logger

logger = get_logger("app.repositories.vehicles")


class VehicleSource(Protocol):
    """车辆目录来源协议（服务层依赖倒置）。"""

    async def vehicle_exists(self, vehicle_id: str) -> bool | None:
        """车辆是否存在（True/False；下游不可用返回 None）。"""
        ...

    async def fleet_total(self) -> int | None:
        """注册车辆总数（列表 total；下游不可用返回 None）。"""
        ...


class VehicleDirectoryClient:
    """vehicle-service REST 客户端（带超时；查询失败降级返回 None）。"""

    VEHICLE_BASE_PATH = "/api/v1/vehicle"

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

    async def vehicle_exists(self, vehicle_id: str) -> bool | None:
        """车辆存在性校验（200=存在 / 404=不存在 / 其他与异常=None 延后校验）。"""
        if not self._base_url:
            logger.warning("vehicle_service_base_url_missing")
            return None
        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._client.get(f"{self._base_url}{self.VEHICLE_BASE_PATH}/{vehicle_id}")
            except httpx.HTTPError as exc:
                logger.warning("vehicle_existence_check_failed", attempt=attempt, error=str(exc))
                continue
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                return False
            logger.warning("vehicle_existence_check_unexpected_status", status=resp.status_code)
            return None
        return None

    async def fleet_total(self) -> int | None:
        """注册车辆总数（列表接口 total，page_size=1 最小代价查询）。"""
        if not self._base_url:
            return None
        try:
            resp = await self._client.get(
                f"{self._base_url}{self.VEHICLE_BASE_PATH}", params={"page": 1, "page_size": 1}
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("vehicle_fleet_total_failed", error=str(exc))
            return None
        data = resp.json().get("data") or {}
        total = data.get("total")
        return int(total) if total is not None else None

    async def close(self) -> None:
        await self._client.aclose()