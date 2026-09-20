"""vehicle_svc.vehicles.fence_json 只读仓储（G-11；本服务创建链路首个 PG 依赖）。

契约 x-hunter-geofence.db_access：只读 fence_json 单列（禁止跨服务写/其他列）；
DB 不可用或查询失败 → 5001（ServiceUnavailableError，fail-closed：围栏配置不可知时
不允许接管，且创建流程尚未取得互斥锁，不留脏状态）。
经 G-17 车辆台账 API 取代直读的方案定稿前维持本路径（pending #22）。
"""
from __future__ import annotations

from typing import Any

from hunter_common.database.models.core import Vehicle
from hunter_common.database.session import DatabaseSessionManager
from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger
from sqlalchemy import select

logger = get_logger("app.repositories.vehicle_fence")


class VehicleFenceRepository:
    """围栏配置读取实现（满足 app.services.geofence.FenceProvider 协议）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def get_fence(self, vehicle_id: str) -> dict[str, Any] | None:
        """读取车辆围栏定义；未登记或 fence_json IS NULL → None（未配置，放行）。"""
        try:
            async with self._db.session() as session:
                stmt = select(Vehicle.fence_json).where(Vehicle.vehicle_id == vehicle_id)
                value = (await session.execute(stmt)).scalar_one_or_none()
        except Exception as exc:
            logger.warning("fence_read_failed", vehicle_id=vehicle_id, error=str(exc))
            raise ServiceUnavailableError(
                message="读取车辆围栏配置失败，暂时无法创建会话",
                details={"vehicle_id": vehicle_id},
            ) from exc
        return value if isinstance(value, dict) else None


__all__ = ["VehicleFenceRepository"]
