"""Bag 主档 Repository（§5.2 层2：投影单调守卫）。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.ingest import Bag, BagState, BagTopic


class BagRepository:
    """bag 主档 Repository（专用方法，不用 BaseRepository）。"""

    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert_from_manifest(self, bag_id: str, fields: dict[str, Any]) -> Bag:
        """从 manifest 事件写入/更新 bag 基础信息（§6.2 bags 表）。"""
        stmt = pg_insert(Bag).values(bag_id=bag_id, **fields)
        update_cols = {k: v for k, v in fields.items() if k != "bag_id"}
        if update_cols:
            stmt = stmt.on_conflict_do_update(index_elements=["bag_id"], set_=update_cols)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=["bag_id"])
        await self._session.execute(stmt)
        result = await self._session.execute(select(Bag).where(Bag.bag_id == bag_id))
        bag: Bag = result.scalar_one()
        return bag

    async def project_state(
        self,
        bag_id: str,
        to_state: BagState,
        seq: int,
        *,
        extra_fields: dict[str, Any] | None = None,
    ) -> bool:
        """状态投影更新（§5.2 层2 单调守卫）。

        只前进不后退：seq 必须大于当前 seq，或 to_state.rank >= 当前 state.rank。
        parse_failed 例外（允许任何阶段进入失败态）。

        Returns:
            True = 投影成功更新；False = 被守卫拒绝（乱序/低 seq）。
        """
        sets: dict[str, Any] = {
            "state": to_state,
            "seq": seq,
            "updated_at": text("now()"),
        }
        if extra_fields:
            sets.update(extra_fields)

        cur_result = await self._session.execute(
            select(Bag.state, Bag.seq).where(Bag.bag_id == bag_id)
        )
        row = cur_result.first()
        if row is None:
            return False

        cur_state: BagState = row[0]
        cur_seq: int = row[1]

        # 单调守卫
        if to_state != BagState.PARSE_FAILED:
            seq_ok = seq > cur_seq
            rank_ok = to_state.rank >= cur_state.rank
            if not (seq_ok or rank_ok):
                return False

        stmt = update(Bag).where(Bag.bag_id == bag_id).values(**sets)
        await self._session.execute(stmt)
        return True

    async def get_by_id(self, bag_id: str) -> Bag | None:
        result = await self._session.execute(select(Bag).where(Bag.bag_id == bag_id))
        bag: Bag | None = result.scalar_one_or_none()
        return bag

    async def list_bags(
        self,
        *,
        vehicle_id: str | None = None,
        state: BagState | None = None,
        min_qc_score: float | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Bag], int]:
        """分页查询 bag 列表（§8.1 GET /api/v1/bags）。"""
        from hunter_common.database.repository import MAX_PAGE_SIZE
        conditions = []
        if vehicle_id:
            conditions.append(Bag.vehicle_id == vehicle_id)
        if state:
            conditions.append(Bag.state == state)
        if min_qc_score is not None:
            conditions.append(Bag.qc_score >= min_qc_score)

        count_stmt = select(text("COUNT(*)")).select_from(Bag)
        list_stmt = select(Bag)
        for cond in conditions:
            count_stmt = count_stmt.where(cond)
            list_stmt = list_stmt.where(cond)

        total_result = await self._session.execute(count_stmt)
        total = int(total_result.scalar() or 0)

        list_stmt = list_stmt.order_by(Bag.updated_at.desc())
        list_stmt = list_stmt.offset((page - 1) * page_size).limit(min(page_size, MAX_PAGE_SIZE))
        result = await self._session.execute(list_stmt)
        items: list[Bag] = list(result.scalars().all())
        return items, total

    async def update_qc_score(self, bag_id: str, score: float) -> None:
        stmt = update(Bag).where(Bag.bag_id == bag_id).values(
            qc_score=score, updated_at=text("now()")
        )
        await self._session.execute(stmt)

    async def set_parse_failed(self, bag_id: str, reason: str) -> None:
        """标记 bag 解析失败（§2.6 异常 bag 标记 parse_failed）。"""
        stmt = update(Bag).where(Bag.bag_id == bag_id).values(
            state=BagState.PARSE_FAILED,
            parse_status=reason,
            updated_at=text("now()"),
        )
        await self._session.execute(stmt)


class BagTopicRepository:
    """bag_topics 仓储（§6.2）。"""

    def __init__(self, session: Any) -> None:
        self._session = session

    async def upsert_batch(self, bag_id: str, topics: list[dict[str, Any]]) -> None:
        """批量 UPSERT topic 统计（来自 manifest 事件）。"""
        for t in topics:
            stmt = pg_insert(BagTopic).values(bag_id=bag_id, **t)
            stmt = stmt.on_conflict_do_update(
                index_elements=["bag_id", "topic"],
                set_={k: v for k, v in t.items() if k != "topic"},
            )
            await self._session.execute(stmt)

    async def list_by_bag(self, bag_id: str) -> list[BagTopic]:
        result = await self._session.execute(
            select(BagTopic).where(BagTopic.bag_id == bag_id)
        )
        return list(result.scalars().all())


__all__ = ["BagRepository", "BagTopicRepository"]
