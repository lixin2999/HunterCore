"""告警 Repository（§5.1 Alert 落库 + §10 分级路由）。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import AgentAlert


class AlertRepository:
    """agent_alerts 专用 Repository（车端告警 + 平台自检告警统一落库）。"""

    def __init__(self, session: Any) -> None:
        self._session = session

    async def record(
        self,
        vehicle_id: str,
        alert_type: str,
        *,
        level: str = "info",
        detail: dict[str, Any] | None = None,
        origin: str = "vehicle",
    ) -> AgentAlert:
        """写入告警记录，返回持久化后的行（含自增 id）。"""
        stmt = (
            pg_insert(AgentAlert)
            .values(
                vehicle_id=vehicle_id,
                alert_type=alert_type,
                level=level,
                detail=detail or {},
                origin=origin,
            )
            .returning(AgentAlert)
        )
        result = await self._session.execute(stmt)
        alert: AgentAlert = result.scalars().one()
        return alert

    async def mark_notified(self, alert_id: int) -> None:
        stmt = (
            update(AgentAlert)
            .where(AgentAlert.id == alert_id)
            .values(notified=True, notify_error=None)
        )
        await self._session.execute(stmt)

    async def mark_notify_error(self, alert_id: int, error: str) -> None:
        stmt = (
            update(AgentAlert)
            .where(AgentAlert.id == alert_id)
            .values(notified=False, notify_error=error[:500])
        )
        await self._session.execute(stmt)

    async def get_by_id(self, alert_id: int) -> AgentAlert | None:
        result = await self._session.execute(
            select(AgentAlert).where(AgentAlert.id == alert_id)
        )
        alert: AgentAlert | None = result.scalars().first()
        return alert

    async def list_recent(
        self,
        *,
        vehicle_id: str | None = None,
        level: str | None = None,
        limit: int = 50,
    ) -> list[AgentAlert]:
        stmt = select(AgentAlert)
        if vehicle_id:
            stmt = stmt.where(AgentAlert.vehicle_id == vehicle_id)
        if level:
            stmt = stmt.where(AgentAlert.level == level)
        stmt = stmt.order_by(AgentAlert.created_at.desc()).limit(limit)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


__all__ = ["AlertRepository"]
