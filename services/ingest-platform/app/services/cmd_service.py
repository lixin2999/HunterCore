"""命令下发服务（§9.4 + cmd-svc）。

生成下行命令 → 写 commands 表 → produce vehicle.agent.cmd → 等 cmd.ack 闭环。
超时 30min 未 ack 自动重发一次，再失败告警。

会话管理：每个公开方法独立开启 session，适配消费者长时运行。
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Any

from hunter_common.database import DatabaseSessionManager
from hunter_common.kafka.producer import KafkaProducerManager
from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import CmdAckStatus
from app.repositories.alerts import AlertRepository
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
            "issued_at": datetime.now(UTC).isoformat(),
            "signature": self._sign(cmd_id, vehicle_id, cmd_type, payload),
        }
        try:
            producer = KafkaProducerManager.instance()
            # 低层 produce 只接受 bytes/str（与仓内 data-collector/remote-control 同款约定）：
            # 直传 dict 会被 confluent_kafka 拒绝并被下方 except 静默吞掉，命令实际未下发。
            await producer.produce(_CMD_TOPIC, key=vehicle_id, value=json.dumps(message, ensure_ascii=False))
            logger.info("cmd_published", cmd_id=str(cmd_id), vehicle_id=vehicle_id, type=cmd_type)
        except Exception as exc:
            logger.error("cmd_publish_failed", cmd_id=str(cmd_id), error=str(exc))

    def _sign(self, cmd_id: Any, vehicle_id: str, cmd_type: str, payload: dict[str, Any]) -> str:
        """HMAC 签名（§9.4：密钥与车端共享，G-02 gateway_hmac_secret）。

        fail-closed（审查 #15）：密钥未配置时直接拒绝，绝不用公开常量回退，
        否则任何知晓该常量的人可伪造 cleanup/reparse/seal 等破坏性命令。
        """
        secret = self._settings.gateway_hmac_secret
        if not secret:
            raise RuntimeError(
                "gateway_hmac_secret 未配置，拒绝签发下行命令（§13.1 fail-closed）"
            )
        body = json.dumps(
            {"cmd_id": str(cmd_id), "vehicle_id": vehicle_id, "type": cmd_type, "payload": payload},
            sort_keys=True,
        )
        return hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()

    # ---------- 超时扫描与重发（§9.4 审查 #13） ----------

    async def scan_timeouts(self) -> int:
        """扫描超时未 ack 命令：首次重发一次，二次仍超时置 timeout 并告警（§9.4）。

        Returns:
            本轮处理的超时命令数。
        """
        processed = 0
        async with self._db_mgr.session() as session:
            cmd_repo = CommandRepository(session)
            alert_repo = AlertRepository(session)
            timed_out = await cmd_repo.list_timed_out(self._settings.cmd_ack_timeout_s)
            for cmd in timed_out:
                if (cmd.resend_count or 0) >= 1:
                    await cmd_repo.set_ack_status(
                        cmd.cmd_id, CmdAckStatus.TIMEOUT,
                        {"reason": f"no ack after resend (timeout>{self._settings.cmd_ack_timeout_s}s)"},
                    )
                    logger.error(
                        "cmd_timeout", cmd_id=str(cmd.cmd_id),
                        vehicle_id=cmd.vehicle_id, type=cmd.type,
                    )
                    await alert_repo.record(
                        cmd.vehicle_id, "cmd_timeout", level="error",
                        detail={"cmd_id": str(cmd.cmd_id), "cmd_type": cmd.type, "bag": cmd.payload},
                        origin="platform",
                    )
                else:
                    await self._publish_cmd(cmd.cmd_id, cmd.vehicle_id, cmd.type, cmd.payload)
                    await cmd_repo.mark_resent(cmd.cmd_id)
                    logger.warning(
                        "cmd_resend", cmd_id=str(cmd.cmd_id),
                        vehicle_id=cmd.vehicle_id, type=cmd.type,
                    )
                processed += 1
        return processed

    async def run_timeout_loop(self) -> None:
        """超时扫描常驻循环（main.py lifespan 启动，关闭时 cancel）。"""
        interval = self._settings.cmd_timeout_scan_interval_s
        while True:
            try:
                await asyncio.sleep(interval)
                n = await self.scan_timeouts()
                if n:
                    logger.info("cmd_timeout_scan", processed=n)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("cmd_timeout_scan_error", error=str(exc))


__all__ = ["CmdService"]
