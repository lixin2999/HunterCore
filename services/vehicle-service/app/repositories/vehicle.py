"""车辆台账数据访问（vehicle_svc.vehicles）。

契约：contracts/database/ddl/01_core.sql + contracts/openapi/vehicle-service.yaml。
本 Repository 只处理本服务专属能力（分页 + provision_status 局部更新 + 台账基础字段修改）；
基础 CRUD 复用 hunter_common.database.VehicleRepository（BaseRepository[Vehicle]）。

事务策略：与 ota-service 一致 —— 服务层通过 ``db.session()`` 组合事务；
Repository 方法默认在自身内部开一个短事务（读方法不开）；provisioning 编排使用
**跨步骤会话**（同一个 session 中 flush 多次），失败回滚由外层 ``async with db.session()`` 负责。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from hunter_common.database import DatabaseSessionManager, VehicleStatus
from hunter_common.database.models import Vehicle
from hunter_common.database.repository import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from sqlalchemy import Select, func, or_, select

ProvisionState = str  # Literal["pending", "in_progress", "ready", "failed"]，与 Pydantic 一致


class VehicleStore:
    """vehicle-service 专属数据访问（组合 hunter_common.VehicleRepository 未覆盖的分页/provision 更新）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    def session(self) -> AsyncIterator[Any]:
        """暴露事务上下文给上层服务（provisioner 需在一个事务中跨步骤读写）。"""
        return self._db.session()

    # ---------- 读 ----------
    async def get(self, vehicle_id: str) -> Vehicle | None:
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(Vehicle).where(Vehicle.vehicle_id == vehicle_id)
                )
            ).scalar_one_or_none()

    async def exists(self, vehicle_id: str) -> bool:
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(Vehicle.vehicle_id).where(Vehicle.vehicle_id == vehicle_id).limit(1)
                )
            ).first()
            return row is not None

    async def list_page(
        self,
        *,
        status: VehicleStatus | None = None,
        provision_state: ProvisionState | None = None,
        keyword: str | None = None,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> tuple[list[Vehicle], int]:
        """分页 + 过滤（默认排序：`last_online_time DESC NULLS LAST, vehicle_id ASC`）。

        - `provision_state` 通过 JSONB 表达式过滤：`provision_status->>'state' = :v`；
          历史台账（provision_status IS NULL）视为 `pending`（未 provisioning）；
        - `keyword` 走 ILIKE %kw%（`vehicle_id` / `vehicle_name` 二选一命中）；
          MVP 未加 GIN trigram 索引，规模 <10k 车辆可接受。
        """
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), MAX_PAGE_SIZE))

        def _build() -> Select[tuple[Vehicle]]:
            stmt = select(Vehicle)
            conds = []
            if status is not None:
                conds.append(Vehicle.status == status)
            if provision_state is not None:
                if provision_state == "pending":
                    conds.append(
                        or_(
                            Vehicle.provision_status.is_(None),
                            Vehicle.provision_status["state"].astext == "pending",
                        )
                    )
                else:
                    conds.append(Vehicle.provision_status["state"].astext == provision_state)
            if keyword:
                like = f"%{keyword.strip()}%"
                conds.append(
                    or_(
                        Vehicle.vehicle_id.ilike(like),  # type: ignore[attr-defined]
                        Vehicle.vehicle_name.ilike(like),  # type: ignore[attr-defined]
                    )
                )
            if conds:
                stmt = stmt.where(*conds)
            return stmt

        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(
                        select(func.count()).select_from(_build().subquery())
                    )
                ).scalar_one()
            )
            rows = (
                (
                    await session.execute(
                        _build()
                        .order_by(
                            Vehicle.last_online_time.desc().nullslast(),
                            Vehicle.vehicle_id.asc(),
                        )
                        .offset((page - 1) * page_size)
                        .limit(page_size)
                    )
                )
                .scalars()
                .all()
            )
            return list(rows), total

    # ---------- 写 ----------
    async def insert_ledger(
        self,
        session: Any,
        *,
        vehicle_id: str,
        vehicle_name: str,
        model: str,
        firmware_version: str | None,
        software_version: str | None,
        fence_json: dict[str, Any] | None,
        description: str | None,
        provision_status: dict[str, Any],
    ) -> Vehicle:
        """provisioning 第 1 步：在同一事务内 INSERT 台账；unique 冲突由调用方通过
        IntegrityError 捕获映射 3002。本方法只 flush（不 commit），事务边界由调用方控制。
        """
        row = Vehicle(
            vehicle_id=vehicle_id,
            vehicle_name=vehicle_name,
            model=model,
            firmware_version=firmware_version,
            software_version=software_version,
            status=VehicleStatus.OFFLINE,
            device_cert_sn=None,
            fence_json=fence_json,
            description=description,
            provision_status=provision_status,
        )
        session.add(row)
        await session.flush()
        return row

    async def update_provision_status(
        self,
        vehicle_id: str,
        provision_status: dict[str, Any],
        *,
        device_cert_sn: str | None = None,
        session: Any | None = None,
    ) -> Vehicle | None:
        """覆盖 provision_status（JSONB 整字段替换）；可选同事务更新 device_cert_sn。

        `session` 提供时，复用调用方事务（provisioner 场景）；否则开独立事务（rotate/reissue 场景）。
        """
        if session is not None:
            row = (
                await session.execute(
                    select(Vehicle).where(Vehicle.vehicle_id == vehicle_id).with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.provision_status = provision_status
            if device_cert_sn is not None:
                row.device_cert_sn = device_cert_sn
            await session.flush()
            return row
        async with self._db.session() as owned:
            row = (
                await owned.execute(
                    select(Vehicle).where(Vehicle.vehicle_id == vehicle_id).with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.provision_status = provision_status
            if device_cert_sn is not None:
                row.device_cert_sn = device_cert_sn
            await owned.flush()
            return row

    async def patch_ledger(
        self,
        vehicle_id: str,
        *,
        vehicle_name: str | None = None,
        model: str | None = None,
        firmware_version: str | None = None,
        software_version: str | None = None,
        fence_json: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> Vehicle | None:
        """PATCH：仅更新提供的字段（None = 未提供，不写入）。"""
        values: dict[str, Any] = {}
        if vehicle_name is not None:
            values["vehicle_name"] = vehicle_name
        if model is not None:
            values["model"] = model
        if firmware_version is not None:
            values["firmware_version"] = firmware_version
        if software_version is not None:
            values["software_version"] = software_version
        if fence_json is not None:
            values["fence_json"] = fence_json
        if description is not None:
            values["description"] = description
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(Vehicle).where(Vehicle.vehicle_id == vehicle_id).with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            for key, value in values.items():
                setattr(row, key, value)
            await session.flush()
            return row

    async def delete_ledger(self, vehicle_id: str) -> bool:
        """物理删除台账行（下线最后一步）；返回是否命中。

        ⚠ 只删本表；events / vehicle_telemetry / ota_records 逻辑外键数据保留供审计（契约）。
        """
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(Vehicle).where(Vehicle.vehicle_id == vehicle_id)
                )
            ).scalar_one_or_none()
            if row is None:
                return False
            await session.delete(row)
            await session.flush()
            return True


__all__ = ["VehicleStore"]
