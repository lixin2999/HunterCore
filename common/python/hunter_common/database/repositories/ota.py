"""OTA 数据访问层（ota_svc：ota_versions / ota_tasks / ota_records）。

契约：contracts/database/ddl/03_ota.sql + orm-mapping.md 第 3 节 + openapi/ota-service.yaml（列表排序）。
安全约束：
- ``version_code`` 单调递增（防回滚）→ ``max_version_code`` 提供发布门禁基准；
- 版本删除受 DDL FK ``ON DELETE RESTRICT`` 保护，本层不提供版本 delete 便捷方法；
- 记录 ``status``/``phase`` 取值 = 车端 OTA 状态机 9 态（受控词表，禁止硬编码字面量）。
排序约定（不可更改）：``release_time DESC NULLS LAST, version_code DESC``（版本列表）、
``start_time DESC NULLS LAST, record_id DESC``（升级记录），与 OpenAPI 描述及 DDL 索引一致。
"""
from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import func, select

from hunter_common.database.enums import (
    OTA_ACTIVE_STATUSES,
    OtaStatus,
    OtaTaskStatus,
    OtaVersionStatus,
)
from hunter_common.database.models import OtaRecord, OtaTask, OtaVersion
from hunter_common.database.repository import BaseRepository


class OtaVersionRepository(BaseRepository[OtaVersion]):
    """OTA 版本仓库读写。"""

    model = OtaVersion
    #: 版本列表默认排序（契约 openapi/ota-service.yaml：``release_time DESC NULLS LAST, version_code DESC``；
    #: 对齐 idx_ota_versions_status_release_time，避免执行计划多出 Sort）
    default_order_by = ("-release_time:nl", "-version_code")

    async def get_by_version_code(self, version_code: int) -> OtaVersion | None:
        """按版本编码查询（唯一索引 uq_ota_versions_version_code）。"""
        return await self.find_one(OtaVersion.version_code == version_code)

    async def get_by_version_name(self, version_name: str) -> OtaVersion | None:
        """按版本展示名查询（唯一索引 uq_ota_versions_version_name）。"""
        return await self.find_one(OtaVersion.version_name == version_name)

    async def max_version_code(
        self,
        *,
        status: OtaVersionStatus | None = None,
        applicable_models: Sequence[str] | None = None,
    ) -> int | None:
        """最大 ``version_code``（发布门禁基准：同车型新版本号必须严格更大，防回滚）。

        Args:
            status: 限定版本状态（如仅统计 ``published``；None 表示不限）。
            applicable_models: 按适用车型过滤（TEXT[] 重叠匹配，命中 GIN 索引）。
        """
        stmt = select(func.max(OtaVersion.version_code))
        if status is not None:
            stmt = stmt.where(OtaVersion.status == status)
        if applicable_models:
            stmt = stmt.where(OtaVersion.applicable_models.overlap(list(applicable_models)))
        value = (await self.session.execute(stmt)).scalar_one()
        return int(value) if value is not None else None


class OtaTaskRepository(BaseRepository[OtaTask]):
    """OTA 升级任务读写（灰度发布编排由 ota-service 业务层负责）。"""

    model = OtaTask
    #: 任务列表默认排序（对齐 idx_ota_tasks_status_create_time）
    default_order_by = ("-create_time",)

    async def list_by_status(
        self, status: OtaTaskStatus, *, limit: int | None = None
    ) -> list[OtaTask]:
        """按任务状态查询（running/paused 用于灰度监控与卡死检测）。"""
        return await self.find_all(OtaTask.status == status, limit=limit)

    async def list_by_target_version(
        self, version_id: UUID, *, limit: int | None = None
    ) -> list[OtaTask]:
        """按目标版本查询任务（命中 idx_ota_tasks_target_version_id；版本退役前置检查）。"""
        return await self.find_all(OtaTask.target_version_id == version_id, limit=limit)


class OtaRecordRepository(BaseRepository[OtaRecord]):
    """单车辆升级记录读写。"""

    model = OtaRecord
    #: 记录列表默认排序（契约 openapi/ota-service.yaml：``start_time DESC NULLS LAST, record_id DESC``；
    #: 次级键 record_id 保证同 start_time 记录的分页稳定，避免翻页重复/漏行）
    default_order_by = ("-start_time:nl", "-record_id")

    async def get_by_task_vehicle(self, task_id: UUID, vehicle_id: str) -> OtaRecord | None:
        """按 (task_id, vehicle_id) 查询（唯一索引 uq_ota_records_task_vehicle）。

        重试只在原记录上推进 ``phase``/``progress``，不新增记录（契约注释）。
        """
        return await self.find_one(
            OtaRecord.task_id == task_id, OtaRecord.vehicle_id == vehicle_id
        )

    async def list_inflight(
        self,
        *,
        statuses: Sequence[OtaStatus] | None = None,
        limit: int | None = None,
    ) -> list[OtaRecord]:
        """查询进行中记录（默认 = OTA 状态机非终态，命中部分索引 idx_ota_records_inflight）。

        排序 ``status ASC, start_time DESC NULLS LAST, record_id DESC``：
        部分索引 ``(status, start_time)`` 仅能在同向时复用索引顺序，
        本方法以"最新批次优先"可读性为准（``start_time DESC`` 需 Incremental Sort），
        代价被部分索引谓词（非终态）限制在小结果集内；次级键保证分页稳定。
        """
        active = (
            list(statuses)
            if statuses is not None
            else sorted(OTA_ACTIVE_STATUSES, key=lambda status: status.value)
        )
        return await self.find_all(
            OtaRecord.status.in_(active),
            order_by=("status", "-start_time:nl", "-record_id"),
            limit=limit,
        )

    async def list_by_vehicle(self, vehicle_id: str, *, limit: int | None = None) -> list[OtaRecord]:
        """车辆升级历史（命中 idx_ota_records_vehicle_start_time，时间倒序）。"""
        return await self.find_all(OtaRecord.vehicle_id == vehicle_id, limit=limit)

    async def status_counts(self, task_id: UUID) -> dict[OtaStatus, int]:
        """按状态统计任务下的记录数（灰度成功率 = SUCCESS 数 / 本批次总数）。"""
        stmt = (
            select(OtaRecord.status, func.count())
            .where(OtaRecord.task_id == task_id)
            .group_by(OtaRecord.status)
        )
        rows = (await self.session.execute(stmt)).all()
        return {OtaStatus(status): int(count) for status, count in rows}


__all__ = [
    "OtaRecordRepository",
    "OtaTaskRepository",
    "OtaVersionRepository",
]
