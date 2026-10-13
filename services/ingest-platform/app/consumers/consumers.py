"""7 个 Kafka 消费者实现（§5.1 消费拓扑，对应 7 个主题）。

- manifest_consumer:   vehicle.bag.manifest    → ProjectionService.handle_manifest
- task_status_consumer: vehicle.bag.task.status  → ProjectionService.handle_task_status
- parse_progress_consumer: vehicle.bag.parse.progress → bags.progress 更新
- artifact_consumer:   vehicle.bag.artifact.uploaded → ProjectionService.handle_artifact_uploaded
- heartbeat_consumer:  vehicle.agent.heartbeat  → AgentRepository.upsert_heartbeat
- alert_consumer:      vehicle.agent.alert      → 告警转发
- cmd_ack_consumer:    vehicle.agent.cmd.ack    → CmdService.handle_ack
"""
from __future__ import annotations

import asyncio
import json
import urllib.request
from typing import Any, ClassVar

from confluent_kafka import Message
from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.consumers.backpressure import BackpressureGuard
from app.consumers.base import BaseIngestConsumer
from app.repositories.agents import AgentRepository
from app.repositories.alerts import AlertRepository
from app.repositories.bags import BagRepository
from app.services.cmd_service import CmdService
from app.services.projection import ProjectionService

logger = get_logger("app.consumers")


def _post_webhook(url: str, body: dict[str, Any], timeout_s: float = 5.0) -> None:
    """同步 POST JSON（stdlib，经 executor 调用）；非 2xx 抛错。"""
    data = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"webhook 返回 {resp.status}")


async def _notify_alert(url: str, payload: dict[str, Any]) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: _post_webhook(url, payload))


class ManifestConsumer(BaseIngestConsumer):
    """vehicle.bag.manifest 消费者（§5.1 并行度 48）。"""

    group_id: ClassVar[str] = "g.ingest.manifest"
    subscribe_topic: ClassVar[str] = "vehicle.bag.manifest"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager, projection: ProjectionService) -> None:
        super().__init__(settings, db_mgr)
        self._projection = projection

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        await self._projection.handle_manifest(payload)


class TaskStatusConsumer(BaseIngestConsumer):
    """vehicle.bag.task.status 消费者（§5.1：纯投影更新，不触发加工）。"""

    group_id: ClassVar[str] = "g.ingest.projection"
    subscribe_topic: ClassVar[str] = "vehicle.bag.task.status"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager, projection: ProjectionService) -> None:
        super().__init__(settings, db_mgr)
        self._projection = projection

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        await self._projection.handle_task_status(payload)


class ParseProgressConsumer(BaseIngestConsumer):
    """vehicle.bag.parse.progress 消费者（§5.1：解析进度落库 bags.progress）。

    审查 #10：无 ClickHouse 时降级为最新态写入 bags.progress（历史已在 events 表）。
    """

    group_id: ClassVar[str] = "g.ingest.progress"
    subscribe_topic: ClassVar[str] = "vehicle.bag.parse.progress"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager) -> None:
        super().__init__(settings, db_mgr)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        bag_id = payload.get("bag_id", "")
        if not bag_id:
            return
        progress: dict[str, Any] = {
            "percent": payload.get("progress", payload.get("percent")),
            "stage": payload.get("stage"),
            "updated_at": payload.get("occurred_at"),
        }
        async with self._db_mgr.session() as session:
            updated = await BagRepository(session).update_progress(bag_id, progress)
        if not updated:
            logger.debug("parse_progress_bag_missing", bag_id=bag_id)
        logger.debug("parse_progress", bag_id=bag_id, progress=progress)


class ArtifactConsumer(BaseIngestConsumer):
    """vehicle.bag.artifact.uploaded 消费者（§5.1：写 artifacts，触发就绪判定）。

    审查 #21：接入 §5.4 背压守卫（队列积压/速率超限时对消费入口施加延迟）。
    """

    group_id: ClassVar[str] = "g.ingest.artifact"
    subscribe_topic: ClassVar[str] = "vehicle.bag.artifact.uploaded"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager, projection: ProjectionService) -> None:
        super().__init__(settings, db_mgr)
        self._projection = projection
        self._guard = BackpressureGuard(settings, db_mgr)

    async def _before_process(self, message: Message, value: dict[str, Any]) -> None:
        await self._guard.acquire()

    @property
    def backpressure_guard(self) -> BackpressureGuard:
        return self._guard

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        await self._projection.handle_artifact_uploaded(payload)


class HeartbeatConsumer(BaseIngestConsumer):
    """vehicle.agent.heartbeat 消费者（§5.1：更新 agents 视图）。"""

    group_id: ClassVar[str] = "g.ingest.heartbeat"
    subscribe_topic: ClassVar[str] = "vehicle.agent.heartbeat"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager) -> None:
        super().__init__(settings, db_mgr)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        vehicle_id = payload.get("vehicle_id", "")
        if not vehicle_id:
            return
        fields: dict[str, Any] = {
            "agent_version": payload.get("agent_version"),
            "config_rev": payload.get("config_rev"),
            "state": payload.get("state", "online"),
            "metrics": payload.get("metrics", {}),
        }
        async with self._db_mgr.session() as session:
            repo = AgentRepository(session)
            await repo.upsert_heartbeat(vehicle_id, fields)


class AlertConsumer(BaseIngestConsumer):
    """vehicle.agent.alert 消费者（§5.1/§10：告警落库 + 分级路由）。

    审查 #11：所有级别告警落 agent_alerts；critical/error 级另走日志 + Alertmanager/Webhook。
    """

    group_id: ClassVar[str] = "g.ingest.alert"
    subscribe_topic: ClassVar[str] = "vehicle.agent.alert"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager) -> None:
        super().__init__(settings, db_mgr)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        vehicle_id = payload.get("vehicle_id", "")
        alert_type = payload.get("alert_type", "")
        level = str(payload.get("level", "info"))
        detail = payload.get("detail", {})

        async with self._db_mgr.session() as session:
            alert = await AlertRepository(session).record(
                vehicle_id, alert_type, level=level, detail=detail, origin="vehicle"
            )
            alert_id = getattr(alert, "id", None)

        if level in self._settings.alert_critical_level_set:
            logger.error(
                "agent_alert_critical", vehicle_id=vehicle_id,
                alert_type=alert_type, level=level, detail=detail,
            )
            await self._dispatch(alert_id, vehicle_id, alert_type, level, detail)
        else:
            logger.warning("agent_alert", vehicle_id=vehicle_id, alert_type=alert_type, level=level)

    async def _dispatch(
        self, alert_id: Any, vehicle_id: str, alert_type: str, level: str, detail: dict[str, Any]
    ) -> None:
        """外发 Alertmanager/Webhook（§10）；未配置 URL 仅落库+日志，失败回写 notify_error。"""
        url = self._settings.alert_webhook_url
        if not url:
            return
        body = {
            "alerts": [{
                "labels": {"alertname": alert_type, "severity": level, "vehicle_id": vehicle_id},
                "annotations": {"detail": json.dumps(detail, ensure_ascii=False, default=str)},
            }]
        }
        try:
            await _notify_alert(url, body)
            if alert_id is not None:
                async with self._db_mgr.session() as session:
                    await AlertRepository(session).mark_notified(int(alert_id))
        except Exception as exc:  # noqa: BLE001
            logger.error("alert_notify_failed", error=str(exc))
            if alert_id is not None:
                try:
                    async with self._db_mgr.session() as session:
                        await AlertRepository(session).mark_notify_error(int(alert_id), str(exc))
                except Exception as inner_exc:  # noqa: BLE001
                    logger.debug("alert_notify_error_persist_failed", error=str(inner_exc))


class CmdAckConsumer(BaseIngestConsumer):
    """vehicle.agent.cmd.ack 消费者（§5.1：回执关联 commands 表，闭环命令）。"""

    group_id: ClassVar[str] = "g.ingest.cmdack"
    subscribe_topic: ClassVar[str] = "vehicle.agent.cmd.ack"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager, cmd_service: CmdService) -> None:
        super().__init__(settings, db_mgr)
        self._cmd_service = cmd_service

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        cmd_id = payload.get("cmd_id", "")
        status = payload.get("status", "ack")
        detail = payload.get("detail", {})
        if cmd_id:
            await self._cmd_service.handle_ack(cmd_id, status, detail)


__all__ = [
    "AlertConsumer",
    "ArtifactConsumer",
    "CmdAckConsumer",
    "HeartbeatConsumer",
    "ManifestConsumer",
    "ParseProgressConsumer",
    "TaskStatusConsumer",
]
