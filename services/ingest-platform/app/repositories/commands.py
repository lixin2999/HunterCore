"""命令闭环 Repository（§6.2 + §9.4 commands 表）。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import CmdAckStatus, Command


class CommandRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def create(self, vehicle_id: str, cmd_type: str, payload: dict[str, Any]) -> Command:
        """创建下行命令（§9.4：cmd-svc 生成并写入 commands 表）。"""
        stmt = (
            pg_insert(Command)
            .values(vehicle_id=vehicle_id, type=cmd_type, payload=payload)
            .returning(Command)
        )
        result = await self._session.execute(stmt)
        cmd: Command = result.scalars().one()
        return cmd

    async def set_ack(
        self,
        cmd_id: UUID,
        status: CmdAckStatus,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """处理 ack 回执（§附录A：vehicle.agent.cmd.ack → commands 表）。"""
        sets: dict[str, Any] = {"ack_status": status, "ack_at": datetime.now(timezone.utc)}
        if detail:
            sets["ack_detail"] = detail
        stmt = update(Command).where(Command.cmd_id == cmd_id).values(**sets)
        await self._session.execute(stmt)

    async def list_pending(self, vehicle_id: str | None = None) -> list[Command]:
        stmt = select(Command).where(Command.ack_status == CmdAckStatus.PENDING)
        if vehicle_id:
            stmt = stmt.where(Command.vehicle_id == vehicle_id)
        stmt = stmt.order_by(Command.issued_at.asc())
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, cmd_id: UUID) -> Command | None:
        stmt = select(Command).where(Command.cmd_id == cmd_id)
        result = await self._session.execute(stmt)
        c: Command | None = result.scalars().first()
        return c


__all__ = ["CommandRepository"]
