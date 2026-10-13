"""Agent 视图 Repository（§6.2 agents 表）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import Agent


class AgentRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert_heartbeat(
        self, vehicle_id: str, fields: dict[str, Any]
    ) -> None:
        """心跳更新（§6.2 + §附录A：vehicle.agent.heartbeat → agents 表）。"""
        stmt = pg_insert(Agent).values(vehicle_id=vehicle_id, **fields)
        stmt = stmt.on_conflict_do_update(
            index_elements=["vehicle_id"],
            set_={**fields, "last_heartbeat": text("now()")},
        )
        await self._session.execute(stmt)

    async def get(self, vehicle_id: str) -> Agent | None:
        stmt = select(Agent).where(Agent.vehicle_id == vehicle_id)
        result = await self._session.execute(stmt)
        a: Agent | None = result.scalars().first()
        return a

    async def list_online(self, *, timeout_s: int = 120) -> list[Agent]:
        """列举在线 Agent（心跳在 timeout_s 内，§8.1 GET /api/v1/agents?online=true）。"""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_s)
        stmt = select(Agent).where(Agent.last_heartbeat >= cutoff)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def list_offline(self, *, timeout_s: int = 120) -> list[Agent]:
        """列举掉线 Agent（用于告警巡检，§附录D）。"""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_s)
        stmt = select(Agent).where(
            Agent.last_heartbeat < cutoff,
            Agent.last_heartbeat.isnot(None),
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())



__all__ = ["AgentRepository"]
