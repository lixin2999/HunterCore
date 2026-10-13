"""任务编排服务（§3.1 orchestrator + §9.1 主循环）。

单主（PG advisory lock 选主），从 tasks 表领取任务并派发给各 worker。
"""
from __future__ import annotations

import asyncio
import os
import socket
from typing import Any

from sqlalchemy import text

from hunter_common.logging import get_logger

from app.config import Settings
from app.models.ingest import Task, TaskState, TaskType
from app.repositories.tasks import TaskRepository
from app.services.align_worker import AlignWorker
from app.services.qc_worker import QcWorker
from app.services.slice_worker import SliceWorker

logger = get_logger("app.services.orchestrator")

#: 当前进程唯一 ID（worker lease 标识，由 hostname + pid + 随机后缀组成）
_WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{os.urandom(4).hex()}"


class Orchestrator:
    """orchestrator 主循环（单主 + lease 回收）。"""

    def __init__(
        self,
        settings: Settings,
        db_session: Any,          # session_factory（async_sessionmaker[AsyncSession]）
        align_worker: AlignWorker,
        slice_worker: SliceWorker,
        qc_worker: QcWorker,
    ) -> None:
        self._settings = settings
        self._db = db_session
        self._workers: dict[TaskType, Any] = {
            TaskType.ALIGN: align_worker,
            TaskType.SLICE: slice_worker,
            TaskType.QC: qc_worker,
        }
        self._running = False
        self._is_leader = False

    async def start(self) -> None:
        """尝试获取 advisory lock 成为 leader，然后进入主循环。"""
        self._running = True
        asyncio.create_task(self._main_loop(), name="orchestrator-loop")
        asyncio.create_task(self._lease_gc_loop(), name="lease-gc")

    async def stop(self) -> None:
        self._running = False

    async def _try_acquire_lock(self) -> bool:
        """尝试获取 PG advisory lock（§3.2 orchestrator 单主）。"""
        try:
            async with self._db() as session:
                result = await session.execute(
                    text("SELECT pg_try_advisory_lock(:key)"),
                    {"key": self._settings.orchestrator_advisory_lock_key},
                )
                acquired: bool = bool(result.scalar())
                if acquired:
                    logger.info("orchestrator_became_leader")
                return acquired
        except Exception as exc:
            logger.warning("advisory_lock_failed", error=str(exc))
            return False

    async def _main_loop(self) -> None:
        while self._running:
            if not self._is_leader:
                self._is_leader = await self._try_acquire_lock()
                if not self._is_leader:
                    await asyncio.sleep(self._settings.orchestrator_poll_interval_s * 2)
                    continue

            try:
                await self._process_one_task()
            except Exception:
                logger.exception("orchestrator_loop_error")
            await asyncio.sleep(self._settings.orchestrator_poll_interval_s)

    async def _process_one_task(self) -> None:
        """领取并执行一个任务（§9.1）。"""
        async with self._db() as session:
            repo = TaskRepository(session)
            task = await repo.claim_next(_WORKER_ID, lease_s=self._settings.task_lease_timeout_s)
            if task is None:
                return
            await session.commit()

        worker = self._workers.get(task.task_type)
        if worker is None:
            logger.warning("no_worker_for_task", task_type=task.task_type.value)
            async with self._db() as session:
                repo = TaskRepository(session)
                await repo.fail(task.task_id, _WORKER_ID, error_code="NO_WORKER")
                await session.commit()
            return

        try:
            result = await worker.execute(task)
            async with self._db() as session:
                repo = TaskRepository(session)
                await repo.complete(task.task_id, _WORKER_ID, result)
                await session.commit()
            logger.info("task_done", task_id=task.task_id, task_type=task.task_type.value)
        except Exception as exc:
            logger.exception("task_failed", task_id=task.task_id, error=str(exc))
            async with self._db() as session:
                repo = TaskRepository(session)
                await repo.fail(
                    task.task_id, _WORKER_ID,
                    error_code=str(type(exc).__name__),
                    max_attempts=self._settings.task_max_attempts,
                    backoff_base_s=self._settings.task_backoff_base_s,
                )
                await session.commit()

    async def _lease_gc_loop(self) -> None:
        """周期性回收过期 lease（§9.1 worker 崩溃安全）。"""
        while self._running:
            await asyncio.sleep(self._settings.task_lease_timeout_s)
            if not self._is_leader:
                continue
            try:
                async with self._db() as session:
                    repo = TaskRepository(session)
                    recovered = await repo.recover_lease_expired()
                    await session.commit()
                if recovered:
                    logger.info("lease_recovered", count=recovered)
            except Exception:
                logger.exception("lease_gc_error")


__all__ = ["Orchestrator"]
