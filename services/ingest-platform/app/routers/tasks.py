"""任务查询与重算路由（§8.1 GET /api/v1/tasks、§9 replay）。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from hunter_common.responses import success_response

from app.core.dependencies import ExecuteUser, ReadUser, SessionDep
from app.models.ingest import TaskType
from app.schemas.schemas import ReplayTaskReq

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


@router.get("")
async def list_tasks(
    session: SessionDep,
    _user: ReadUser,
    bag_id: Annotated[str | None, Query(description="按 bag_id 过滤")] = None,
    task_type: Annotated[str | None, Query(description="任务类型过滤（align/slice/qc/verify）")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
) -> dict:
    """列举加工任务（§8.1 GET /api/v1/tasks）。"""
    from sqlalchemy import func, select
    from app.models.ingest import Task

    stmt = select(Task)

    if bag_id:
        stmt = stmt.where(Task.bag_id == bag_id)
    if task_type:
        try:
            tt = TaskType(task_type)
            stmt = stmt.where(Task.task_type == tt)
        except ValueError:
            pass

    count_stmt = select(func.count(Task.task_id))
    if bag_id:
        count_stmt = count_stmt.where(Task.bag_id == bag_id)
    if task_type:
        try:
            count_stmt = count_stmt.where(Task.task_type == TaskType(task_type))
        except ValueError:
            pass

    total_result = await session.execute(count_stmt)
    total = int(total_result.scalar() or 0)

    stmt = stmt.order_by(Task.created_at.desc())
    stmt = stmt.offset((page - 1) * page_size).limit(min(page_size, MAX_PAGE_SIZE))
    result = await session.execute(stmt)
    items = list(result.scalars().all())

    return success_response(data={
        "meta": {"page": page, "page_size": page_size, "total": total, "has_next": (page * page_size) < total},
        "items": [
            {
                "task_id": t.task_id,
                "bag_id": t.bag_id,
                "task_type": t.task_type.value,
                "state": t.state.value,
                "priority": t.priority,
                "attempts": t.attempts,
                "error_code": t.error_code,
                "created_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in items
        ],
    }).model_dump()


@router.post("/replay")
async def replay_tasks(
    body: ReplayTaskReq,
    session: SessionDep,
    _user: ExecuteUser,
) -> dict:
    """重算任务（§8.1 POST /api/v1/tasks/replay）。

    为指定 bags 重新创建任务（force_rerun=True 时先重置 DONE→PENDING）。
    """
    from sqlalchemy import select, update
    from app.models.ingest import Task, TaskState

    reset = 0
    valid_types = {e.value for e in TaskType}
    for bag_id in body.bag_ids:
        target_types = [TaskType(t) for t in body.task_types if t in valid_types]
        if not target_types:
            continue
        bag_result = await session.execute(
            select(Task).where(Task.bag_id == bag_id, Task.task_type.in_(target_types))
        )
        existing = list(bag_result.scalars().all())

        for task in existing:
            if body.force_rerun and task.state in (TaskState.DONE, TaskState.DEAD):
                await session.execute(
                    update(Task).where(Task.task_id == task.task_id).values(
                        state=TaskState.PENDING,
                        attempts=0,
                        error_code=None,
                        lease_owner=None,
                    )
                )
                reset += 1

    return success_response(data={
        "reset": reset,
        "message": f"replay 完成：重置 {reset} 个任务",
    }).model_dump()
