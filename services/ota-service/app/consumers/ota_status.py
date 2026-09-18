"""hunter.*.ota_status 消费者（消费组 ota-service-ota-status，契约 consumer-groups.yaml）。

处理链路（x-hunter-kafka.consumes.processing）：
1. 按 (task_id, vehicle_id) 定位 ota_records，推进 status/phase/progress（幂等：
   同状态同进度跳过；消息幂等键 (task_id, vehicle_id, status)）；
2. 终态写 end_time；error_code/error_message 原样落库（DDL TEXT）；
3. 同步 ota_tasks.progress JSONB 快照与灰度视图；
4. 批次成功率 = 本批 SUCCESS/(SUCCESS+FAILED+ROLLED_BACK)；< OTA_CANARY_MIN_SUCCESS_RATE
   → 任务置 paused + CRITICAL 告警日志（alert_event 生产者为 data-analytics，本服务不生产）；
5. 手动提交 offset（业务处理成功后提交，at-least-once + 幂等）；失败消息进 {topic}.dlq。
"""
from __future__ import annotations

import time
from typing import Any
from uuid import UUID

from confluent_kafka import Message
from hunter_common.database import DatabaseSessionManager
from hunter_common.database.enums import OtaStatus, OtaTaskStatus
from hunter_common.kafka.consumer import KafkaConsumerManager
from hunter_common.logging import get_logger, set_vehicle_id
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.config import Settings
from app.core.metrics import OTA_CANARY_SUCCESS_RATE
from app.repositories.records import OtaRecordRepository
from app.repositories.tasks import OtaTaskRepository

logger = get_logger("app.consumers.ota_status")


class OtaStatusMessage(BaseModel):
    """ota_status 消息模型（contracts/kafka/schemas/ota_status.schema.json 子集）。"""

    model_config = ConfigDict(extra="ignore")

    vehicle_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    task_id: UUID
    status: OtaStatus
    phase: OtaStatus | None = None
    progress: int = Field(ge=0, le=100)
    error_code: str | None = None
    error_message: str | None = None
    timestamp: float | None = None


class OtaStatusConsumer:
    """车端 OTA 状态上报消费者（推进 ota_records + 灰度门禁判定）。"""

    def __init__(
        self,
        settings: Settings,
        db: DatabaseSessionManager,
        record_repository: OtaRecordRepository,
        task_repository: OtaTaskRepository,
    ) -> None:
        self._settings = settings
        self._db = db
        self._records = record_repository
        self._tasks = task_repository
        self._consumer = KafkaConsumerManager(
            settings,
            group_id=settings.kafka_ota_status_group_id,
            topics=[settings.ota_status_subscribe_pattern],
        )

    async def run(self) -> None:
        """消费主循环（KafkaConsumerManager 手动提交 + DLQ）。"""
        await self._consumer.run(self._handle)

    async def _handle(self, message: Message, value: Any) -> None:
        """单条消息处理（抛异常 → KafkaConsumerManager 转投 DLQ）。"""
        if not isinstance(value, dict):
            raise ValueError(f"ota_status 消息必须为 JSON 对象，实际 {type(value).__name__}")
        try:
            payload = OtaStatusMessage.model_validate(value)
        except ValidationError as exc:
            raise ValueError(f"ota_status 消息结构非法（进 DLQ）：{exc.errors()[:3]}") from exc
        token = set_vehicle_id(payload.vehicle_id)
        try:
            await self._process(payload)
        finally:
            set_vehicle_id(token)  # 覆盖式设置：消费循环协程隔离由 contextvars 保证


    async def _process(self, payload: OtaStatusMessage) -> None:
        """状态推进 + 进度快照 + 灰度门禁判定。"""
        task_id = payload.task_id
        event_time = payload.timestamp or time.time()

        # 第 1 步：推进 ota_records（幂等；无记录 → 任务记录尚未创建，丢弃并告警）
        async with self._records.transaction() as session:
            changed = await self._records.apply_status(
                session,
                task_id=task_id,
                vehicle_id=payload.vehicle_id,
                status=payload.status,
                phase=payload.phase,
                progress=payload.progress,
                error_code=payload.error_code,
                error_message=payload.error_message,
                event_time=event_time,
            )
            if changed:
                await session.commit()
        if not changed:
            logger.info(
                "ota_status_unchanged_or_missing",
                task_id=str(task_id),
                vehicle_id=payload.vehicle_id,
                status=payload.status.value,
            )
            return

        # 第 2 步：重算任务进度快照 + 灰度视图
        task = await self._tasks.get(task_id)
        if task is None:
            logger.warning("ota_status_task_missing", task_id=str(task_id))
            return
        from app.schemas.tasks import OtaUpgradeStrategy  # noqa: PLC0415  # 局部导入
        from app.services.rollout import (  # noqa: PLC0415
            allocate_batches,
            build_rollout_view,
            build_task_progress,
        )

        strategy = OtaUpgradeStrategy.model_validate(task.upgrade_strategy)
        allocation = allocate_batches(
            list(task.target_vehicles or []),
            [batch.percent for batch in strategy.batches],
        )
        snapshots = await self._records.snapshot_by_task(task_id)
        rollout = build_rollout_view(
            strategy=strategy, allocation=allocation, snapshots=snapshots, now=time.time()
        )
        progress = build_task_progress(
            total_vehicles=len(task.target_vehicles or []),
            snapshots=snapshots,
            current_batch=rollout.current_batch,
        )
        current = next((b for b in rollout.batches if b.batch_no == rollout.current_batch), None)
        if current is not None and current.success_rate is not None:
            OTA_CANARY_SUCCESS_RATE.labels(str(task_id), str(current.batch_no)).set(
                current.success_rate
            )

        # 第 3 步：灰度门禁——批次成功率 < 0.95 → 任务 paused（halt）+ 告警日志
        new_status = task.status
        if (
            task.status == OtaTaskStatus.RUNNING
            and current is not None
            and current.status.value == "halted"
        ):
            new_status = OtaTaskStatus.PAUSED
            logger.critical(
                "ota_canary_halted",
                task_id=str(task_id),
                batch_no=current.batch_no,
                halt_reason=rollout.halt_reason,
            )
        # 第 4 步：第 4 批全部终态（passed）且无进行中/待下发车辆 → succeeded
        if (
            task.status == OtaTaskStatus.RUNNING
            and rollout.current_batch == 4
            and all(batch.status.value == "passed" or batch.target_count == 0 for batch in rollout.batches)
            and progress.in_progress == 0
            and progress.pending == 0
        ):
            new_status = OtaTaskStatus.SUCCEEDED

        async with self._tasks.transaction() as session:
            locked = await self._tasks.get_for_update(session, task_id)
            if locked is not None:
                locked.status = new_status
                locked.progress = progress.model_dump()
                await session.commit()
        logger.info(
            "ota_status_applied",
            task_id=str(task_id),
            vehicle_id=payload.vehicle_id,
            status=payload.status.value,
            progress=payload.progress,
            task_status=new_status.value,
        )


__all__ = ["OtaStatusConsumer", "OtaStatusMessage"]
