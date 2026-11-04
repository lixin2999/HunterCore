"""仿真端管理登记数据访问（scene_svc.sim_node_registry）。

V1.24.1 仿真端管理功能：懒建语义 + 管理字段 PATCH + 令牌/证书序列号更新。
V1.24.2 一键开通：``update_provision_status`` 回写编排态 JSONB 与开通快照列
（scram_username / scram_credential_updated_at / cert_not_after / 双证书序列号）。

事务策略与 VehicleStore 相同：
- 读方法开独立短事务；
- upsert/patch 由调用方通过 ``session`` 参数控制事务边界（服务层可在同一事务内
  读 Redis + 写登记，保持最终一致性）。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from hunter_common.database import DatabaseSessionManager
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.sim_node_registry import SimNodeRegistry


class SimNodeRegistryStore:
    """scene_svc.sim_node_registry 数据访问（仿真端管理登记）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    def session(self) -> AsyncIterator[Any]:
        """暴露事务上下文给上层服务。"""
        return self._db.session()

    # ---------- 读 ----------

    async def get(self, node_id: str) -> SimNodeRegistry | None:
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(SimNodeRegistry).where(SimNodeRegistry.node_id == node_id)
                )
            ).scalar_one_or_none()

    async def exists(self, node_id: str) -> bool:
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry.node_id)
                    .where(SimNodeRegistry.node_id == node_id)
                    .limit(1)
                )
            ).first()
            return row is not None

    async def get_map(self, node_ids: list[str]) -> dict[str, SimNodeRegistry]:
        """批量查询登记表（列表页补充 display_name/token/cert 字段）。

        未在表中的 node_id 不出现在结果字典（调用方据 None 判未建）。
        """
        if not node_ids:
            return {}
        async with self._db.session() as session:
            rows = (
                (
                    await session.execute(
                        select(SimNodeRegistry).where(SimNodeRegistry.node_id.in_(node_ids))
                    )
                )
                .scalars()
                .all()
            )
            return {r.node_id: r for r in rows}

    # ---------- 写 ----------

    async def upsert(
        self,
        node_id: str,
        *,
        session: Any | None = None,
        **defaults: Any,
    ) -> SimNodeRegistry:
        """懒建（首次查看/操作时创建行）；若已存在则仅更新 defaults 中提供的字段。

        使用 PG INSERT ... ON CONFLICT DO UPDATE，并发安全。
        ``defaults`` 不允许覆盖 node_id（主键）。
        """
        values: dict[str, Any] = {"node_id": node_id, **defaults}
        # 去掉 node_id 不在更新列里
        update_cols = {k: v for k, v in values.items() if k != "node_id"}

        stmt = (
            pg_insert(SimNodeRegistry)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["node_id"],
                set_={
                    **update_cols,
                    "update_time": datetime.now(UTC),
                },
            )
            .returning(SimNodeRegistry)
        )

        if session is not None:
            result = await session.execute(stmt)
            return result.scalar_one()  # type: ignore[no-any-return]
        async with self._db.session() as owned:
            result = await owned.execute(stmt)
            row = result.scalar_one()
            await owned.flush()
            return row  # type: ignore[no-any-return]

    async def patch(
        self,
        node_id: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        labels: dict[str, str] | None = None,
        session: Any | None = None,
    ) -> SimNodeRegistry | None:
        """PATCH 管理字段（None = 未提供，不写入；空字符串 = 显式清空）。"""
        values: dict[str, Any] = {}
        if display_name is not None:
            values["display_name"] = display_name or None
        if description is not None:
            values["description"] = description or None
        if labels is not None:
            values["labels"] = labels

        def _apply(row: SimNodeRegistry) -> None:
            for key, value in values.items():
                setattr(row, key, value)

        if session is not None:
            row = (
                await session.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            _apply(row)
            await session.flush()
            return row

        async with self._db.session() as owned:
            row = (
                await owned.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            _apply(row)
            await owned.flush()
            return row

    async def set_token(
        self,
        node_id: str,
        *,
        token_id: str,
        token_issued_at: datetime,
        owner_client_id: str | None = None,
    ) -> SimNodeRegistry | None:
        """签发令牌后更新登记（token_id + 签发时间 + owner_client_id）。"""
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.token_id = token_id
            row.token_issued_at = token_issued_at
            if owner_client_id is not None:
                row.owner_client_id = owner_client_id
            await session.flush()
            return row

    async def clear_token(self, node_id: str) -> SimNodeRegistry | None:
        """撤销令牌后清空（token_id = NULL，token_issued_at = NULL）。"""
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.token_id = None
            row.token_issued_at = None
            await session.flush()
            return row

    async def set_kafka_cert(
        self,
        node_id: str,
        *,
        serial_hex: str,
        issued_at: datetime,
    ) -> SimNodeRegistry | None:
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.kafka_cert_sn = serial_hex
            row.kafka_cert_issued_at = issued_at
            await session.flush()
            return row

    async def set_plat_cert(
        self,
        node_id: str,
        *,
        serial_hex: str,
        issued_at: datetime,
    ) -> SimNodeRegistry | None:
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.plat_cert_sn = serial_hex
            row.plat_cert_issued_at = issued_at
            await session.flush()
            return row

    # 开通快照可回写的列（sim_provisioner 逐步回写；None = 本次不更新该列）
    _PROVISION_MUTABLE_COLUMNS = (
        "provision_status",
        "scram_username",
        "scram_credential_updated_at",
        "cert_not_after",
        "kafka_cert_sn",
        "kafka_cert_issued_at",
        "plat_cert_sn",
        "plat_cert_issued_at",
    )

    async def update_provision_status(
        self,
        node_id: str,
        payload: dict[str, Any],
        **fields: Any,
    ) -> SimNodeRegistry | None:
        """回写一键开通编排态（provision_status JSONB + 可选快照列）。

        ``fields`` 仅接受 ``_PROVISION_MUTABLE_COLUMNS`` 内的列名；值为 None 的键跳过
        （与车辆侧 update_provision_status(device_cert_sn=...) 同一语义：失败步回写时
        尚未产出的字段保持原值）。行不存在返回 None（不懒建：开通前 db 步已建行）。
        """
        unknown = set(fields) - set(self._PROVISION_MUTABLE_COLUMNS)
        if unknown:
            raise ValueError(f"不支持的开通快照列: {sorted(unknown)}")
        values: dict[str, Any] = {"provision_status": payload}
        values.update({k: v for k, v in fields.items() if v is not None})

        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry)
                    .where(SimNodeRegistry.node_id == node_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            for key, value in values.items():
                setattr(row, key, value)
            await session.flush()
            return row

    async def delete(self, node_id: str) -> bool:
        """删除登记行（节点下线时清理）；返回是否命中。"""
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(SimNodeRegistry).where(SimNodeRegistry.node_id == node_id)
                )
            ).scalar_one_or_none()
            if row is None:
                return False
            await session.delete(row)
            await session.flush()
            return True


__all__ = ["SimNodeRegistryStore"]
