"""任务编排 Repository（§9.1 lease 队列 + SKIP LOCKED）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import Task, TaskState, TaskType


class TaskRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def create(
        self,
        bag_id: str | None,
        input_rev: str | None,
        task_type: TaskType,
        *,
        priority: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """幂等创建任务（§5.2 层3 副作用幂等键）。

        Returns:
            True = 新建任务成功；False = 已存在（UNIQUE 冲突跳过）。
        """
        stmt = (
            pg_insert(Task)
            .values(
                bag_id=bag_id,
                input_rev=input_rev,
                task_type=task_type,
                priority=priority,
                payload=payload or {},
            )
            .on_conflict_do_nothing(constraint="uq_tasks_bag_type_rev")
        )
        result = await self._session.execute(stmt)
        return bool(result.rowcount == 1)

    async def claim_next(self, worker_id: str, *, lease_s: int = 300) -> Task | None:
        """领取下一个待执行任务（§9.1 SELECT FOR UPDATE SKIP LOCKED）。"""
        now = datetime.now(timezone.utc)
        subq = (
            select(Task.task_id)
            .where(
                Task.state == TaskState.PENDING,
                Task.next_retry_at <= now,
            )
            .order_by(Task.priority.asc(), Task.task_id.asc())
            .limit(1)
            .with_only_columns(Task.task_id)
            .execution_options(skip_locked=True)
        )
        lease_expires = now + timedelta(seconds=lease_s)
        stmt = (
            update(Task)
            .where(Task.task_id.in_(subq))
            .values(
                state=TaskState.RUNNING,
                lease_owner=worker_id,
                lease_expires_at=lease_expires,
                attempts=Task.attempts + 1,
            )
            .returning(Task)
        )
        result = await self._session.execute(stmt)
        task: Task | None = result.scalars().first()
        return task

    async def claim_next_batch(self, worker_id: str, limit: int, *, lease_s: int = 300) -> list[Task]:
        """一次领取至多 limit 个待执行任务（§9 并发 worker 池，单事务 + SKIP LOCKED）。

        与 :meth:`claim_next` 同源，仅将 limit 由 1 扩到 N：同一事务/同一锁下批量
        置 RUNNING 并写 lease，锁外由调用方 asyncio.gather 并发执行 worker。
        """
        if limit <= 0:
            return []
        now = datetime.now(timezone.utc)
        subq = (
            select(Task.task_id)
            .where(
                Task.state == TaskState.PENDING,
                Task.next_retry_at <= now,
            )
            .order_by(Task.priority.asc(), Task.task_id.asc())
            .limit(limit)
            .with_only_columns(Task.task_id)
            .execution_options(skip_locked=True)
        )
        lease_expires = now + timedelta(seconds=lease_s)
        stmt = (
            update(Task)
            .where(Task.task_id.in_(subq))
            .values(
                state=TaskState.RUNNING,
                lease_owner=worker_id,
                lease_expires_at=lease_expires,
                attempts=Task.attempts + 1,
            )
            .returning(Task)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def complete(self, task_id: int, worker_id: str, result_data: dict[str, Any]) -> None:
        """标记任务成功（fencing：lease_owner 必须匹配）。"""
        stmt = (
            update(Task)
            .where(
                Task.task_id == task_id,
                Task.lease_owner == worker_id,
                Task.state == TaskState.RUNNING,
            )
            .values(state=TaskState.DONE, result=result_data)
        )
        await self._session.execute(stmt)

    async def fail(
        self,
        task_id: int,
        worker_id: str,
        *,
        error_code: str,
        max_attempts: int = 3,
        backoff_base_s: int = 30,
    ) -> TaskState:
        """标记任务失败；attempts < max_attempts 则回 pending 并设置退避时间，超限则 dead。"""
        task_result = await self._session.execute(
            select(Task.attempts, Task.max_attempts).where(Task.task_id == task_id)
        )
        row = task_result.first()
        attempts = int(row[0]) if row else 1
        limit = int(row[1]) if row else max_attempts

        if attempts >= limit:
            new_state = TaskState.DEAD
            next_retry = datetime.now(timezone.utc)
        else:
            new_state = TaskState.PENDING
            backoff = backoff_base_s * (2 ** (attempts - 1))
            next_retry = datetime.now(timezone.utc) + timedelta(seconds=backoff)

        stmt = (
            update(Task)
            .where(Task.task_id == task_id, Task.lease_owner == worker_id)
            .values(
                state=new_state,
                error_code=error_code,
                next_retry_at=next_retry,
                lease_owner=None,
                lease_expires_at=None,
            )
        )
        await self._session.execute(stmt)
        return new_state

    async def recover_lease_expired(self) -> int:
        """回收过期 lease（§9.1 worker 崩溃安全），返回回收数量。"""
        now = datetime.now(timezone.utc)
        stmt = (
            update(Task)
            .where(Task.state == TaskState.RUNNING, Task.lease_expires_at < now)
            .values(state=TaskState.PENDING, lease_owner=None, lease_expires_at=None)
        )
        result = await self._session.execute(stmt)
        return int(result.rowcount or 0)

    async def queue_depth(self, task_type: TaskType | None = None) -> int:
        """当前 pending 队列深度（§5.4 背压监控）。"""
        stmt = select(func.count()).select_from(Task).where(Task.state == TaskState.PENDING)
        if task_type:
            stmt = stmt.where(Task.task_type == task_type)
        result = await self._session.execute(stmt)
        return int(result.scalar() or 0)

    async def list_by_bag(self, bag_id: str) -> list[Task]:
        stmt = select(Task).where(Task.bag_id == bag_id).order_by(Task.created_at.asc())
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, task_id: int) -> Task | None:
        stmt = select(Task).where(Task.task_id == task_id)
        result = await self._session.execute(stmt)
        t: Task | None = result.scalars().first()
        return t


__all__ = ["TaskRepository"]
