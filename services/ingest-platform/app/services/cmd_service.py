"""命令下发服务（§9.4 + cmd-svc）。

生成下行命令 → 写 commands 表 → produce vehicle.agent.cmd → 等 cmd.ack 闭环。
超时 30min 未 ack 自动重发一次，再失败告警。

会话管理：每个公开方法独立开启 session，适配消费者长时运行。
"""
from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any

from hunter_common.database import DatabaseSessionManager
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import CmdAckStatus
from app.repositories.commands import CommandRepository

logger = get_logger("app.services.cmd_service")

# 车端命令下行 Topic（设计文档 §1.2 接口契约）
_CMD_TOPIC = "vehicle.agent.cmd"


class CmdService:
    """下行命令生成与闭环管理。"""

    def __init__(
        self,
        settings: Settings,
        db_mgr: DatabaseSessionManager,
    ) -> None:
        self._settings = settings
        self._db_mgr = db_mgr

    async def issue_reupload(self, vehicle_id: str, bag_id: str, priority: str = "normal") -> str:
        """QC 判定产物损坏/缺传 → 下发 reupload（§9.4）。"""
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            cmd = await cmd_repo.create(vehicle_id, "reupload", {
                "bag_id": bag_id,
                "priority": priority,
            })
        await self._publish_cmd(cmd.cmd_id, vehicle_id, "reupload", {
            "bag_id": bag_id, "priority": priority,
        })
        return str(cmd.cmd_id)

    async def issue_reparse(self, vehicle_id: str, bag_id: str) -> str:
        """解析模板升级回灌 → 下发 reparse（§9.4）。"""
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            cmd = await cmd_repo.create(vehicle_id, "reparse", {"bag_id": bag_id})
        await self._publish_cmd(cmd.cmd_id, vehicle_id, "reparse", {"bag_id": bag_id})
        return str(cmd.cmd_id)

    async def issue_cleanup(self, vehicle_id: str, bag_ids: list[str]) -> str:
        """投影到达 done 后释放车端 spool（§9.4 cleanup）。"""
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            cmd = await cmd_repo.create(vehicle_id, "cleanup", {"bag_ids": bag_ids})
        await self._publish_cmd(cmd.cmd_id, vehicle_id, "cleanup", {"bag_ids": bag_ids})
        return str(cmd.cmd_id)

    async def issue_seal(self, vehicle_id: str, bag_path: str) -> str:
        """强制 seal（§9.4，运维手动触发）。"""
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            cmd = await cmd_repo.create(vehicle_id, "seal", {"bag_path": bag_path})
        await self._publish_cmd(cmd.cmd_id, vehicle_id, "seal", {"bag_path": bag_path})
        return str(cmd.cmd_id)

    async def issue_cmd(
        self,
        vehicle_id: str,
        cmd_type: str,
        payload: dict[str, Any],
    ) -> str:
        """通用命令下发（API 调用入口）。"""
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            cmd = await cmd_repo.create(vehicle_id, cmd_type, payload)
        await self._publish_cmd(cmd.cmd_id, vehicle_id, cmd_type, payload)
        return str(cmd.cmd_id)

    async def handle_ack(
        self,
        cmd_id: Any,
        status: str,
        detail: dict[str, Any],
    ) -> None:
        """处理车端 cmd.ack 回执（§附录A：vehicle.agent.cmd.ack → commands 表）。"""
        from uuid import UUID
        ack_status = CmdAckStatus.ACK if status == "ack" else CmdAckStatus.FAILED
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            await cmd_repo.set_ack(UUID(str(cmd_id)), ack_status, detail)
        logger.info("cmd_ack_received", cmd_id=str(cmd_id), status=status)

    async def _publish_cmd(
        self, cmd_id: Any, vehicle_id: str, cmd_type: str, payload: dict[str, Any]
    ) -> None:
        """发布命令到 Kafka vehicle.agent.cmd topic。"""
        message = {
            "cmd_id": str(cmd_id),
            "vehicle_id": vehicle_id,
            "type": cmd_type,
            "payload": payload,
            "issued_at": datetime.now(timezone.utc).isoformat(),
            "signature": self._sign(cmd_id, vehicle_id, cmd_type, payload),
        }
        try:
            producer = KafkaProducerManager.instance()
            await producer.produce(_CMD_TOPIC, key=vehicle_id, value=message)
            logger.info("cmd_published", cmd_id=str(cmd_id), vehicle_id=vehicle_id, type=cmd_type)
        except Exception as exc:
            logger.error("cmd_publish_failed", cmd_id=str(cmd_id), error=str(exc))

    def _sign(self, cmd_id: Any, vehicle_id: str, cmd_type: str, payload: dict[str, Any]) -> str:
        """HMAC 签名（§9.4：密钥与车端共享，G-02 gateway_hmac_secret）。"""
        secret = (self._settings.gateway_hmac_secret or "dev-hmac-key").encode()
        body = json.dumps(
            {"cmd_id": str(cmd_id), "vehicle_id": vehicle_id, "type": cmd_type, "payload": payload},
            sort_keys=True,
        )
        return hmac.new(secret, body.encode(), hashlib.sha256).hexdigest()


__all__ = ["CmdService"]
