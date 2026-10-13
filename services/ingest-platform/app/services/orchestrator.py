"""任务编排服务（§3.1 orchestrator + §9.1 主循环）。

主仲裁语义：每轮在**单个事务内**用 ``pg_try_advisory_xact_lock`` 争锁，
抢到锁的进程才执行 claim/lease 回收；提交即释放锁 ⇒ 多实例并发轮询时
同一时刻至多一个实例在做编排动作，且不存在"连接池里持锁的僵尸会话"。
锁随事务结束自然释放，进程崩溃/丢锁自动降级为普通轮询参与者（下轮重新争锁）。

任务派发遵循 §7.1 依赖序：前置任务 done 后在同一事务内创建后继任务
（仅派发给已注册 worker 的类型，ENRICH 等未接入 worker 不创建）。
"""
from __future__ import annotations

import asyncio
import os
import socket
from typing import Any

from hunter_common.logging import get_logger
from sqlalchemy import text

from app.config import Settings
from app.models.ingest import Task, TaskType
from app.repositories.tasks import TaskRepository
from app.services.align_worker import AlignWorker
from app.services.qc_worker import QcWorker
from app.services.slice_worker import SliceWorker
from app.services.task_graph import next_tasks
from app.services.verify_worker import VerifyWorker

logger = get_logger("app.services.orchestrator")

#: 当前进程唯一 ID（worker lease 标识，由 hostname + pid + 随机后缀组成）
_WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{os.urandom(4).hex()}"


class Orchestrator:
    """orchestrator 主循环（每轮事务争锁 + lease 队列 + 任务图推进）。"""

    def __init__(
        self,
        settings: Settings,
        db_session: Any,          # session_factory（async_sessionmaker[AsyncSession]）
        verify_worker: VerifyWorker,
        align_worker: AlignWorker,
        slice_worker: SliceWorker,
        qc_worker: QcWorker,
        export_worker: Any | None = None,
    ) -> None:
        self._settings = settings
        self._db = db_session
        self._workers: dict[TaskType, Any] = {
            TaskType.VERIFY: verify_worker,
            TaskType.ALIGN: align_worker,
            TaskType.SLICE: slice_worker,
            TaskType.QC: qc_worker,
        }
        # EXPORT worker 可选注入（§8.2/§9 任务化，审查 #9）；未注入则不注册
        if export_worker is not None:
            self._workers[TaskType.EXPORT] = export_worker
        self._running = False
        #: 上一轮争锁结果（仅用于观测/日志，不作为跳过检查的依据）
        self._is_leader = False

    async def start(self) -> None:
        self._running = True
        asyncio.create_task(self._main_loop(), name="orchestrator-loop")
        asyncio.create_task(self._lease_gc_loop(), name="lease-gc")

    async def stop(self) -> None:
        self._running = False

    async def _try_xact_lock(self, session: Any) -> bool:
        """在当前事务内尝试 advisory xact 锁；返回 False = 本轮未抢到（丢主降级）。"""
        result = await session.execute(
            text("SELECT pg_try_advisory_xact_lock(:key)"),
            {"key": self._settings.orchestrator_advisory_lock_key},
        )
        acquired = bool(result.scalar())
        if acquired and not self._is_leader:
            logger.info("orchestrator_acquired_round_lock")
        self._is_leader = acquired
        return acquired

    async def _main_loop(self) -> None:
        while self._running:
            try:
                await self._process_round()
            except Exception:
                logger.exception("orchestrator_loop_error")
            await asyncio.sleep(self._settings.orchestrator_poll_interval_s)

    async def _process_round(self) -> None:
        """单轮并发派发（审查 #20）：锁事务内批量领取至多 N 个任务→提交释锁→并发执行。

        N = orchestrator_max_parallel_tasks。领取阶段短暂持锁保证多实例不重复领取，
        重执行（align/slice/qc 长 IO）在锁外 asyncio.gather 并发，不再串行阻塞。
        """
        limit = max(1, self._settings.orchestrator_max_parallel_tasks)
        tasks: list[Task] = []
        async with self._db() as session:
            if not await self._try_xact_lock(session):
                return
            repo = TaskRepository(session)
            tasks = await repo.claim_next_batch(
                _WORKER_ID, limit, lease_s=self._settings.task_lease_timeout_s
            )
            await session.commit()  # xact 锁随提交释放

        if not tasks:
            return
        await asyncio.gather(*(self._run_one(task) for task in tasks))

    async def _run_one(self, task: Task) -> None:
        """执行单个已领取任务：worker.execute → complete+推进 / fail。"""
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
                await self._advance_task_graph(repo, task)
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

    async def _process_one_task(self) -> None:
        """单任务版领取+执行（保留供测试与低并发场景；等价于 _process_round 的 limit=1）。"""
        task: Task | None = None
        async with self._db() as session:
            if not await self._try_xact_lock(session):
                return
            repo = TaskRepository(session)
            task = await repo.claim_next(_WORKER_ID, lease_s=self._settings.task_lease_timeout_s)
            await session.commit()
        if task is None:
            return
        await self._run_one(task)

    async def _advance_task_graph(self, repo: TaskRepository, task: Task) -> None:
        """前置任务 done → 创建后继任务（§7.1 依赖序 + §5.2 层3 唯一键幂等）。

        仅创建已注册 worker 的任务类型；重复触发由 uq_tasks_bag_type_rev 去重。
        与 complete 同事务提交。
        """
        if not task.bag_id or not task.input_rev:
            return
        for successor in next_tasks(task.task_type):
            if successor not in self._workers:
                continue
            created = await repo.create(task.bag_id, task.input_rev, successor)
            if created:
                logger.info(
                    "task_created",
                    bag_id=task.bag_id,
                    task_type=successor.value,
                    input_rev=task.input_rev,
                    after=task.task_type.value,
                )

    async def _lease_gc_loop(self) -> None:
        """周期性回收过期 lease（§9.1 worker 崩溃安全），同样受事务锁仲裁。"""
        while self._running:
            await asyncio.sleep(self._settings.task_lease_timeout_s)
            try:
                async with self._db() as session:
                    if not await self._try_xact_lock(session):
                        await session.rollback()
                        continue
                    repo = TaskRepository(session)
                    recovered = await repo.recover_lease_expired()
                    await session.commit()
                if recovered:
                    logger.info("lease_recovered", count=recovered)
            except Exception:
                logger.exception("lease_gc_error")


__all__ = ["Orchestrator"]
