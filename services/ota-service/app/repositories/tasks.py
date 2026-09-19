"""OTA 升级任务数据访问（ota_svc.ota_tasks）。

- 批次推进以 ``SELECT ... FOR UPDATE`` 行锁串行化（x-hunter-canary-rollout.scheduler，
  防止并发动作重复推进；事务边界由调用方 ``transaction()`` 组合）；
- 列表排序 create_time DESC（索引 idx_ota_tasks_status_create_time）；
- vehicle_id 过滤走 GIN 索引 idx_ota_tasks_target_vehicles 包含匹配。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID

from hunter_common.database import DatabaseSessionManager
from hunter_common.database.enums import OtaTaskStatus
from hunter_common.database.models import OtaTask
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


class OtaTaskRepository:
    """ota_svc.ota_tasks 读写。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    def transaction(self) -> AsyncIterator[AsyncSession]:
        """事务性会话上下文（start/pause/resume/cancel 组合操作共用一个事务）。"""
        return self._db.session()

    async def get_for_update(self, session: AsyncSession, task_id: UUID) -> OtaTask | None:
        """行锁读取（SELECT ... FOR UPDATE；批次推进串行化）。"""
        return (
            await session.execute(
                select(OtaTask).where(OtaTask.task_id == task_id).with_for_update()
            )
        ).scalar_one_or_none()

    async def get(self, task_id: UUID) -> OtaTask | None:
        """按主键查询；缺失返回 None（业务层映射 3001）。"""
        async with self._db.session() as session:
            return (
                await session.execute(select(OtaTask).where(OtaTask.task_id == task_id))
            ).scalar_one_or_none()

    async def list_page(
        self,
        *,
        status: OtaTaskStatus | None = None,
        target_version_id: UUID | None = None,
        vehicle_id: str | None = None,
        creator: UUID | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[OtaTask], int]:
        """分页查询（create_time DESC；列表响应省略 target_vehicles 由服务层置 null）。"""
        conditions = []
        if status is not None:
            conditions.append(OtaTask.status == status)
        if target_version_id is not None:
            conditions.append(OtaTask.target_version_id == target_version_id)
        if vehicle_id is not None:
            conditions.append(OtaTask.target_vehicles.contains([vehicle_id]))
        if creator is not None:
            conditions.append(OtaTask.creator == creator)
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(OtaTask).where(*conditions)
                    )
                ).scalar_one()
            )
            rows = (
                (
                    await session.execute(
                        select(OtaTask)
                        .where(*conditions)
                        .order_by(OtaTask.create_time.desc())
                        .offset((page - 1) * page_size)
                        .limit(page_size)
                    )
                )
                .scalars()
                .all()
            )
            return list(rows), total

    async def create(self, session: AsyncSession, task: OtaTask) -> OtaTask:
        """插入任务（调用方事务；flush+refresh 回填 task_id/create_time）。"""
        session.add(task)
        await session.flush()
        await session.refresh(task)
        return task

    async def save(self, session: AsyncSession, task: OtaTask) -> None:
        """保存任务状态/进度变更（调用方事务；实例处于 session 管理内）。"""
        session.add(task)
        await session.flush()


# ---- 后续方法（part 2 追加） ----
