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

from typing import Any, ClassVar

from hunter_common.database import DatabaseSessionManager
from hunter_common.logging import get_logger

from app.consumers.base import BaseIngestConsumer
from app.repositories.agents import AgentRepository
from app.services.cmd_service import CmdService
from app.services.projection import ProjectionService

logger = get_logger("app.consumers")


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
    """vehicle.bag.parse.progress 消费者（§5.1：更新进度，写 ClickHouse 时序）。"""

    group_id: ClassVar[str] = "g.ingest.progress"
    subscribe_topic: ClassVar[str] = "vehicle.bag.parse.progress"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager) -> None:
        super().__init__(settings, db_mgr)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        bag_id = payload.get("bag_id", "")
        progress = payload.get("progress", 0)
        logger.debug("parse_progress", bag_id=bag_id, progress=progress)
        # ClickHouse 写入留接（当前简化为日志）


class ArtifactConsumer(BaseIngestConsumer):
    """vehicle.bag.artifact.uploaded 消费者（§5.1：写 artifacts，触发就绪判定）。"""

    group_id: ClassVar[str] = "g.ingest.artifact"
    subscribe_topic: ClassVar[str] = "vehicle.bag.artifact.uploaded"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager, projection: ProjectionService) -> None:
        super().__init__(settings, db_mgr)
        self._projection = projection

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
    """vehicle.agent.alert 消费者（§5.1：转告警引擎）。"""

    group_id: ClassVar[str] = "g.ingest.alert"
    subscribe_topic: ClassVar[str] = "vehicle.agent.alert"

    def __init__(self, settings: Any, db_mgr: DatabaseSessionManager) -> None:
        super().__init__(settings, db_mgr)

    async def process(self, payload: dict[str, Any], *, topic: str) -> None:
        vehicle_id = payload.get("vehicle_id", "")
        alert_type = payload.get("alert_type", "")
        level = payload.get("level", "info")
        logger.warning(
            "agent_alert", vehicle_id=vehicle_id, alert_type=alert_type, level=level,
            detail=payload.get("detail", {}),
        )
        # 告警路由留接（PagerDuty / 钉钉 / Alertmanager）


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
