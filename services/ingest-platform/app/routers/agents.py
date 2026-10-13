"""Agent 状态路由（§8.1 GET /api/v1/ingest/agents）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from hunter_common.responses import success_response

from app.core.dependencies import ReadUser, SessionDep
from app.repositories.agents import AgentRepository

router = APIRouter(prefix="/api/v1/ingest/agents", tags=["agents"])


@router.get("")
async def list_agents(
    session: SessionDep,
    _user: ReadUser,
    online: Annotated[bool | None, Query(description="true=仅在线，false=仅掉线，null=全部")] = None,
    timeout_s: Annotated[int, Query(ge=10, le=3600, description="心跳超时阈值（秒）")] = 120,
) -> dict:
    """列举 Agent 状态（§8.1 GET /api/v1/ingest/agents）。"""
    repo = AgentRepository(session)
    if online is True:
        agents = await repo.list_online(timeout_s=timeout_s)
    elif online is False:
        agents = await repo.list_offline(timeout_s=timeout_s)
    else:
        from sqlalchemy import select
        from app.models.ingest import Agent
        result = await session.execute(select(Agent).order_by(Agent.last_heartbeat.desc()))
        agents = list(result.scalars().all())

    return success_response(data={
        "items": [
            {
                "vehicle_id": a.vehicle_id,
                "agent_version": a.agent_version,
                "state": a.state,
                "last_heartbeat": a.last_heartbeat.isoformat() if a.last_heartbeat else None,
                "metrics": a.metrics or {},
            }
            for a in agents
        ]
    }).model_dump()
