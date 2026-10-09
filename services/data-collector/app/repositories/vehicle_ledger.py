"""车辆台账回写的仓储适配（``vehicle_svc.vehicles``）。

为什么需要适配层：``hunter_common.database.VehicleRepository`` 继承 ``BaseRepository``，
构造入参是**单个 ``AsyncSession``** 且只 ``flush`` 不 ``commit``（事务边界由调用方控制）；
而本服务的会话一律由 ``DatabaseSessionManager.session()`` 按操作临时借出（与
:mod:`app.repositories.telemetry` 同一做法）。直接 ``VehicleRepository(app.state.db)``
会拿到一个没有 ``execute`` 的会话管理器，回写在每次调用时抛 ``AttributeError``——
并被回写器的异常隔离吞成 WARN，表现为"页面永远显示离线"（本模块即为该坑的修复）。

会话粒度：一次回写 = 一个短事务。节流后每车 ≤ 1 次/30 秒（health）+ 1 次/30 秒
（telemetry 心跳），相对遥测批量入库可忽略；禁止在消费循环中长期持有会话。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from hunter_common.database import DatabaseSessionManager, VehicleRepository
from hunter_common.database.enums import VehicleStatus


class VehicleLedgerRepositoryAdapter:
    """实现 :class:`app.services.vehicle_ledger.VehicleLedgerRepository` 协议。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def update_status(
        self,
        vehicle_id: str,
        status: VehicleStatus,
        *,
        last_online_time: datetime | None = None,
    ) -> Any:
        """回写业务状态（可选同步最近在线时间）；车辆不存在返回 ``None``。"""
        async with self._db.session() as session:
            return await VehicleRepository(session).update_status(
                vehicle_id, status, last_online_time=last_online_time
            )

    async def touch_last_online_time(self, vehicle_id: str, *, seen_at: datetime) -> bool:
        """仅刷新最近在线时间（单调不回退，不改 ``status``）；返回是否命中行。"""
        async with self._db.session() as session:
            return await VehicleRepository(session).touch_last_online_time(
                vehicle_id, seen_at=seen_at
            )


__all__ = ["VehicleLedgerRepositoryAdapter"]
