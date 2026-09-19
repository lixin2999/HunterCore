"""场景库数据访问层（scene_svc.scenes，SQLAlchemy 2.0 异步 + 参数化查询）。

- 软删除：``deleted_at`` 非 NULL 视为已删除，所有查询默认过滤（契约 DDL 02_scene.sql）；
- 列表检索对齐索引：``idx_scenes_status_type_create_time`` / ``idx_scenes_scene_type`` /
  ``idx_scenes_tags``（GIN 包含查询 ``@>``）/ ``uq_scenes_scene_name``（存活场景内名称唯一）；
- 排序字段走白名单（契约 sort enum），禁止任意字段排序（防注入）；
- 唯一键冲突统一映射 3002（契约 createScene/updateScene 409）。
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from hunter_common.database import DatabaseSessionManager
from hunter_common.database.enums import SceneStatus
from hunter_common.database.models import Scene
from hunter_common.exceptions import ResourceAlreadyExistsError
from hunter_common.logging import get_logger
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError

logger = get_logger("app.repositories.scenes")

#: PostgreSQL 唯一约束冲突 SQLSTATE
_PG_UNIQUE_VIOLATION = "23505"
#: 排序字段白名单 → ORM 列（契约 SceneSortField）
_SORT_COLUMNS: dict[str, Any] = {
    "create_time": Scene.create_time,
    "update_time": Scene.update_time,
    "scene_name": Scene.scene_name,
    "version": Scene.version,
}


def escape_like(value: str) -> str:
    """转义 LIKE 元字符（``\\`` ``%`` ``_``），避免用户输入被当作通配符（防模式注入）。"""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _is_unique_violation(exc: IntegrityError) -> bool:
    """识别唯一约束冲突（asyncpg/SQLAlchemy 包装层差异兼容）。"""
    orig = getattr(exc, "orig", None)
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    if sqlstate == _PG_UNIQUE_VIOLATION:
        return True
    return _PG_UNIQUE_VIOLATION in str(orig)


class SceneRepository:
    """scene_svc.scenes 读写（元信息列 + config_json；无物理删除）。"""

    def __init__(self, db: DatabaseSessionManager) -> None:
        self._db = db

    # ---------- 查询 ----------

    async def list_page(
        self,
        *,
        page: int,
        page_size: int,
        scene_type: str | None = None,
        status: SceneStatus | None = None,
        tags: Sequence[str] | None = None,
        keyword: str | None = None,
        creator: UUID | None = None,
        sort: str = "create_time",
        order: str = "desc",
    ) -> tuple[list[Scene], int]:
        """分页查询场景列表（软删除过滤 + 分类/状态/标签/关键字/创建者筛选 + 白名单排序）。"""
        conditions = self._build_conditions(
            scene_type=scene_type, status=status, tags=tags, keyword=keyword, creator=creator
        )
        stmt = select(Scene).where(*conditions)
        stmt = stmt.order_by(*self._build_order_by(sort, order))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        async with self._db.session() as session:
            total = int(
                (
                    await session.execute(select(func.count()).select_from(Scene).where(*conditions))
                ).scalar_one()
            )
            rows = (await session.execute(stmt)).scalars().all()
        return list(rows), total

    async def get(self, scene_id: UUID) -> Scene | None:
        """按主键查询（软删除场景视为不存在）。"""
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(Scene).where(Scene.scene_id == scene_id, Scene.deleted_at.is_(None))
                )
            ).scalar_one_or_none()

    async def get_by_name(self, scene_name: str) -> Scene | None:
        """按名称查询存活场景（唯一索引 uq_scenes_scene_name）。"""
        async with self._db.session() as session:
            return (
                await session.execute(
                    select(Scene).where(
                        Scene.scene_name == scene_name, Scene.deleted_at.is_(None)
                    )
                )
            ).scalar_one_or_none()

    async def name_taken(self, scene_name: str, *, exclude_id: UUID | None = None) -> bool:
        """名称是否已被存活场景占用（复制场景命名重试使用）。"""
        conditions = [Scene.scene_name == scene_name, Scene.deleted_at.is_(None)]
        if exclude_id is not None:
            conditions.append(Scene.scene_id != exclude_id)
        async with self._db.session() as session:
            return (
                await session.execute(select(Scene.scene_id).where(*conditions).limit(1))
            ).first() is not None

    async def list_by_ids(self, scene_ids: Sequence[UUID], *, limit: int) -> list[Scene]:
        """按 ID 批量查询（导出用；``limit`` 为契约 scene_ids maxItems 保护）。"""
        if not scene_ids:
            return []
        stmt = (
            select(Scene)
            .where(Scene.scene_id.in_(list(scene_ids)), Scene.deleted_at.is_(None))
            .limit(limit)
        )
        async with self._db.session() as session:
            return list((await session.execute(stmt)).scalars().all())

    # ---------- 写入 ----------

    async def create(
        self,
        *,
        scene_name: str,
        scene_type: str,
        description: str | None,
        tags: Sequence[str],
        config: dict[str, Any],
        creator: UUID,
        version: str,
    ) -> Scene:
        """插入新场景（status=draft；名称冲突抛 3002）。"""
        scene = Scene(
            scene_name=scene_name,
            scene_type=str(scene_type),
            description=description,
            tags=list(tags),
            config_json=config,
            creator=creator,
            version=version,
            status=SceneStatus.DRAFT,
        )
        async with self._db.session() as session:
            session.add(scene)
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_unique_violation(exc):
                    raise ResourceAlreadyExistsError(
                        "场景名称已存在", details={"scene_name": scene_name}
                    ) from exc
                raise
        return scene

    async def update(
        self,
        scene_id: UUID,
        *,
        scene_name: str,
        scene_type: str,
        description: str | None,
        tags: Sequence[str],
        config: dict[str, Any],
    ) -> Scene | None:
        """全量更新可编辑字段（不存在返回 None；名称冲突抛 3002；update_time 由混入自动维护）。"""
        async with self._db.session() as session:
            scene = await self._load(session, scene_id)
            if scene is None:
                return None
            scene.scene_name = scene_name
            scene.scene_type = str(scene_type)
            scene.description = description
            scene.tags = list(tags)
            scene.config_json = config
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_unique_violation(exc):
                    raise ResourceAlreadyExistsError(
                        "场景名称已存在", details={"scene_name": scene_name}
                    ) from exc
                raise
        return scene

    async def set_status(
        self, scene_id: UUID, *, status: SceneStatus, version: str | None = None
    ) -> Scene | None:
        """更新状态（发布 draft → published）并可选更新 version（契约 publishScene）。"""
        async with self._db.session() as session:
            scene = await self._load(session, scene_id)
            if scene is None:
                return None
            scene.status = status
            if version is not None:
                scene.version = version
            await session.flush()
        return scene

    async def soft_delete(self, scene_id: UUID) -> bool:
        """软删除（置 deleted_at，不物理删除；不存在/已删除返回 False）。"""
        async with self._db.session() as session:
            scene = await self._load(session, scene_id)
            if scene is None:
                return False
            scene.deleted_at = datetime.now(UTC)
            await session.flush()
        return True

    # ---------- 内部 ----------

    @staticmethod
    async def _load(session: Any, scene_id: UUID) -> Scene | None:
        """在既有会话内按主键加载存活场景（写路径复用，避免重复查询）。"""
        return (
            await session.execute(
                select(Scene).where(Scene.scene_id == scene_id, Scene.deleted_at.is_(None))
            )
        ).scalar_one_or_none()

    def _build_conditions(
        self,
        *,
        scene_type: str | None,
        status: SceneStatus | None,
        tags: Sequence[str] | None,
        keyword: str | None,
        creator: UUID | None,
    ) -> list[Any]:
        """构造查询条件（全部走参数绑定；软删除一律过滤）。"""
        conditions: list[Any] = [Scene.deleted_at.is_(None)]
        if scene_type is not None:
            conditions.append(Scene.scene_type == str(scene_type))
        if status is not None:
            conditions.append(Scene.status == status)
        if tags:
            # 多标签 AND 语义 → PostgreSQL 数组包含（命中 GIN 索引 idx_scenes_tags）
            conditions.append(Scene.tags.contains(list(tags)))
        if keyword:
            pattern = f"%{escape_like(keyword)}%"
            conditions.append(
                or_(
                    Scene.scene_name.ilike(pattern, escape="\\"),
                    Scene.description.ilike(pattern, escape="\\"),
                )
            )
        if creator is not None:
            conditions.append(Scene.creator == creator)
        return conditions

    @staticmethod
    def _build_order_by(sort: str, order: str) -> list[Any]:
        """白名单排序（非法字段回落默认 create_time；次级排序 scene_id 保证分页稳定）。"""
        column = _SORT_COLUMNS.get(sort)
        if column is None:
            logger.warning("scene_sort_field_not_whitelisted", sort=sort)
            column = Scene.create_time
        primary = column.asc() if order == "asc" else column.desc()
        return [primary, Scene.scene_id.asc()]


__all__ = ["SceneRepository", "escape_like"]