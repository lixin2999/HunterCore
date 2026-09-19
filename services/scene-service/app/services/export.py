"""场景导出服务（4.3 节：Carla ScenarioRunner XML / OpenSCENARIO 1.2）。

流程：校验 scene_ids（契约 maxItems 50）→ 读取场景（缺失 3001）→ 渲染导出文件 →
上传 MinIO ``hunter-scene-assets``（永久）→ 返回 15 分钟预签名下载地址（支持 Range）。
命名规则（契约不可更改）：``scenarios/scene-{scene_id}-{version}.{ext}``，多场景合并入同一文件。
"""
from __future__ import annotations

import hashlib
from uuid import UUID

from hunter_common.exceptions import InvalidParameterError, ResourceNotFoundError
from hunter_common.logging import get_logger

from app.config import Settings
from app.repositories.storage import SceneAssetStorage, build_object_key
from app.schemas.scene import (
    EXPORT_EXTENSION_BY_FORMAT,
    Scene,
    SceneExportData,
    SceneExportRequest,
)
from app.services.scenes import SceneService
from app.services.serializers import file_name_for, render_export_document

logger = get_logger("app.services.export")

#: 导出产物 Content-Type（两者均为 XML 文本）
_CONTENT_TYPE = "application/xml"


class SceneExportService:
    """场景导出服务（序列化 + 对象存储 + 预签名下载地址）。"""

    def __init__(
        self, scenes: SceneService, storage: SceneAssetStorage, settings: Settings
    ) -> None:
        self._scenes = scenes
        self._storage = storage
        self._settings = settings

    async def export_scenes(self, payload: SceneExportRequest) -> SceneExportData:
        """导出场景（批量导出为单文件集；返回预签名下载地址）。"""
        scene_ids = self._unique_ids(payload.scene_ids)
        if len(scene_ids) > self._settings.scene_export_max_scenes:
            raise InvalidParameterError(
                f"单次导出场景数不得超过 {self._settings.scene_export_max_scenes}",
                details={"count": len(scene_ids)},
            )
        scenes = await self._load_scenes(scene_ids)
        content = render_export_document(scenes, payload.format)
        sha256 = hashlib.sha256(content).hexdigest()
        file_name = file_name_for(scenes[0], payload.format)
        object_key = build_object_key(
            self._settings.scene_export_object_prefix,
            scene_id=str(scenes[0].scene_id),
            version=scenes[0].version,
            extension=EXPORT_EXTENSION_BY_FORMAT[payload.format],
        )
        await self._storage.upload_bytes(object_key, content, content_type=_CONTENT_TYPE)
        download_url = await self._storage.presign_download(object_key)
        logger.info(
            "scenes_exported",
            format=payload.format.value,
            scene_count=len(scenes),
            object_key=object_key,
            size_bytes=len(content),
        )
        return SceneExportData(
            format=payload.format,
            file_name=file_name,
            object_key=object_key,
            size_bytes=len(content),
            sha256=sha256,
            download_url=download_url,
            expires_in=self._storage.presign_expire_seconds,
        )

    # ---------- 内部 ----------

    @staticmethod
    def _unique_ids(scene_ids: list[UUID]) -> list[UUID]:
        """去重并保持请求顺序（重复 ID 不重复写入导出文件）。"""
        seen: dict[UUID, None] = {}
        for scene_id in scene_ids:
            seen.setdefault(scene_id, None)
        return list(seen)

    async def _load_scenes(self, scene_ids: list[UUID]) -> list[Scene]:
        """按 ID 读取场景（任一缺失 → 3001，details 列出全部缺失 ID）。"""
        scenes: list[Scene] = []
        missing: list[str] = []
        for scene_id in scene_ids:
            try:
                model = await self._scenes.load_scene_model(scene_id)
            except ResourceNotFoundError:
                missing.append(str(scene_id))
                continue
            scenes.append(self._scenes.to_schema(model))
        if missing:
            raise ResourceNotFoundError("场景不存在", details={"scene_ids": missing})
        return scenes


__all__ = ["SceneExportService"]