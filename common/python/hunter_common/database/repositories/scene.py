"""场景库数据访问层（scene_svc.scenes）。

契约：contracts/database/ddl/02_scene.sql + orm-mapping.md 第 3 节。
软删除：``deleted_at`` 非 NULL 即视为已删除（``BaseRepository`` 默认过滤；存活场景唯一索引为部分索引），
因此删除场景一律调用 ``soft_delete``，本层不提供物理删除。
"""
from __future__ import annotations

from hunter_common.database.models import Scene
from hunter_common.database.repository import BaseRepository


class SceneRepository(BaseRepository[Scene]):
    """场景读写（元信息列 + ``config_json`` 场景参数化配置）。"""

    model = Scene
    #: 列表页默认排序：创建时间倒序（对齐 idx_scenes_status_type_create_time）
    default_order_by = ("-create_time", "scene_id")

    async def get_by_scene_name(self, scene_name: str) -> Scene | None:
        """按场景名查询存活场景（部分唯一索引 uq_scenes_scene_name）。"""
        return await self.find_one(Scene.scene_name == scene_name)


__all__ = ["SceneRepository"]
