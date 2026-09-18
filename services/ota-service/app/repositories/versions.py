"""OTA 版本仓库数据访问（ota_svc.ota_versions）。

- 排序对齐索引 idx_ota_versions_status_release_time：release_time DESC NULLS LAST, version_code DESC；
- ``max_published_code`` 为发布/创建防回滚门禁（version_code 单调递增）提供基准值；
- 禁止 DELETE（DDL ON DELETE RESTRICT + OTA 可追溯要求），退役走状态更新。
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select

from hunter_common.database import DatabaseSessionManager
from hunter_common.database.enums import OtaTaskStatus, OtaVersionStatus
from hunter_common.database.models import OtaTask, OtaVersion


class OtaVersionRepository:
    """ota_svc.ota_versions 读写（无 DELETE）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    async def list_page(
        self,
        *,
        status: OtaVersionStatus | None = None,
        release_type: str | None = None,
        version_code: int | None = None,
        applicable_model: str | None = None,
        version_name: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[OtaVersion], int]:
        """分页查询（GIN 包含匹配 applicable_model；version_name ILIKE 模糊匹配）。"""
        conditions = []
        if status is not None:
            conditions.append(OtaVersion.status == status)
        if release_type is not None:
            conditions.append(OtaVersion.release_type == release_type)
        if version_code is not None:
            conditions.append(OtaVersion.version_code == version_code)
        if applicable_model is not None:
            conditions.append(OtaVersion.applicable_models.contains([applicable_model]))
        if version_name is not None:
            conditions.append(OtaVersion.version_name.ilike(f"%{version_name}%"))
        order_by = (OtaVersion.release_time.desc().nulls_last(), OtaVersion.version_code.desc())
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(OtaVersion).where(*conditions)
                    )
                ).scalar_one()
            )
            rows = (
                (
                    await session.execute(
                        select(OtaVersion)
                        .where(*conditions)
                        .order_by(*order_by)
                        .offset((page - 1) * page_size)
                        .limit(page_size)
                    )
                )
                .scalars()
                .all()
            )
            return list(rows), total

    async def get(self, version_id: UUID) -> OtaVersion | None:
        """按主键查询；缺失返回 None（业务层映射 3001）。"""
        async with self._db.session() as session:
            return (
                await session.execute(select(OtaVersion).where(OtaVersion.version_id == version_id))
            ).scalar_one_or_none()

    async def get_by_name(self, version_name: str) -> OtaVersion | None:
        """按展示名查询（唯一索引 uq_ota_versions_version_name）。"""
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(OtaVersion).where(OtaVersion.version_name == version_name)
                )
            ).scalar_one_or_none()

    async def get_by_code(self, version_code: int) -> OtaVersion | None:
        """按编码查询（唯一索引 uq_ota_versions_version_code）。"""
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(OtaVersion).where(OtaVersion.version_code == version_code)
                )
            ).scalar_one_or_none()

    async def max_published_code(self, models: list[str]) -> int | None:
        """同车型已发布最大 version_code（防回滚基准；无已发布版本返回 None）。"""
        async with self._db.session() as session:
            value = (
                await session.execute(
                    select(func.max(OtaVersion.version_code)).where(
                        OtaVersion.status == OtaVersionStatus.PUBLISHED,
                        OtaVersion.applicable_models.overlap(models),
                    )
                )
            ).scalar_one()
            return int(value) if value is not None else None


    async def create(self, version: OtaVersion) -> OtaVersion:
        """插入草稿版本（server_default 生成 version_id，flush+refresh 回填）。"""
        async with self._db.session() as session:
            session.add(version)
            await session.flush()
            await session.refresh(version)
            await session.commit()
            return version

    async def set_status(
        self,
        version_id: UUID,
        *,
        status: OtaVersionStatus,
        release_time: float | None = None,
    ) -> None:
        """更新版本状态（publish → published + release_time；deprecate → deprecated/disabled）。"""
        async with self._db.session() as session:
            version = (
                await session.execute(select(OtaVersion).where(OtaVersion.version_id == version_id))
            ).scalar_one_or_none()
            if version is None:
                return
            version.status = status
            if release_time is not None:
                version.release_time = datetime.fromtimestamp(release_time, tz=timezone.utc)
            await session.commit()

    async def count_tasks_for_version(self, version_id: UUID) -> int:
        """以该版本为目标的任务总数（task_count 引用评估）。"""
        return await self._count_tasks(version_id, statuses=None)

    async def count_active_tasks_for_version(self, version_id: UUID) -> int:
        """引用该版本且处于 running/paused 的任务数（deprecate 前置检查 → 3003）。"""
        return await self._count_tasks(
            version_id, statuses=[OtaTaskStatus.RUNNING, OtaTaskStatus.PAUSED]
        )

    async def _count_tasks(
        self, version_id: UUID, *, statuses: list[OtaTaskStatus] | None
    ) -> int:
        """按版本统计任务数（statuses 限定状态集合）。"""
        conditions = [OtaTask.target_version_id == version_id]
        if statuses:
            conditions.append(OtaTask.status.in_(statuses))
        async with self._db.session() as session:
            return int(
                (
                    await session.execute(
                        select(func.count()).select_from(OtaTask).where(*conditions)
                    )
                ).scalar_one()
            )


__all__ = ["OtaVersionRepository"]
