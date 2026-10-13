"""命令闭环 Repository（§6.2 + §9.4 commands 表）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
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

    async def list_timed_out(self, timeout_s: int) -> list[Command]:
        """pending 且超过 timeout_s 未 ack 的命令（§9.4 超时扫描）。"""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_s)
        stmt = (
            select(Command)
            .where(
                Command.ack_status == CmdAckStatus.PENDING,
                Command.last_issued_at <= cutoff,
            )
            .order_by(Command.last_issued_at.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_resent(self, cmd_id: UUID) -> None:
        """标记命令重发一次（§9.4：重发后重置计时起点）。"""
        now = datetime.now(timezone.utc)
        stmt = (
            update(Command)
            .where(Command.cmd_id == cmd_id)
            .values(resend_count=Command.resend_count + 1, last_issued_at=now)
        )
        await self._session.execute(stmt)

    async def set_ack_status(self, cmd_id: UUID, status: CmdAckStatus, detail: dict[str, Any] | None = None) -> None:
        """直接设置 ack_status（超时失败路径不写 ack_at）。"""
        sets: dict[str, Any] = {"ack_status": status}
        if detail:
            sets["ack_detail"] = detail
        stmt = update(Command).where(Command.cmd_id == cmd_id).values(**sets)
        await self._session.execute(stmt)

    async def get_by_id(self, cmd_id: UUID) -> Command | None:
        stmt = select(Command).where(Command.cmd_id == cmd_id)
        result = await self._session.execute(stmt)
        c: Command | None = result.scalars().first()
        return c


__all__ = ["CommandRepository"]
