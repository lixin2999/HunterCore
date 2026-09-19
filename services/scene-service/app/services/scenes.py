"""场景库业务服务（设计文档 12.2 节：列表/详情/创建/更新/删除/复制/发布）。

规则来源（契约 x-hunter-lifecycle / 端点 description）：
- 仅 ``draft`` 可编辑（updateScene）；``published`` 不可删除（deleteScene → 3003）；
- 仅 ``draft`` 可发布（publishScene）；发布前再次校验 4.2.2 结构完整性；
- 名称在存活场景内唯一（``uq_scenes_scene_name``，冲突 → 3002）；
- 详情读走 Redis 缓存 ``cache:scene:{scene_id}``（TTL 1 小时），任何写操作后主动失效；
- 数据权限隔离（可选，``scene_scope_by_creator``）：非豁免角色只能查询自有场景。
"""
from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from hunter_common.database.enums import SceneStatus
from hunter_common.database.models import Scene as SceneModel
from hunter_common.exceptions import (
    InternalServerError,
    InvalidParameterError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    ResourceStateConflictError,
)
from hunter_common.logging import get_logger
from pydantic import ValidationError

from app.config import Settings
from app.repositories.cache import SceneDetailCache
from app.repositories.scenes import SceneRepository
from app.schemas.scene import (
    Scene,
    SceneConfig,
    SceneCreateRequest,
    SceneDeleteData,
    SceneDuplicateRequest,
    SceneListData,
    ScenePublishRequest,
    SceneType,
    SceneUpdateRequest,
)

logger = get_logger("app.services.scenes")

#: 可编辑状态（契约 x-hunter-lifecycle.editable_states）
EDITABLE_STATES = frozenset({SceneStatus.DRAFT})
#: 可删除状态（契约 x-hunter-lifecycle.deletable_states：published 需先归档）
DELETABLE_STATES = frozenset({SceneStatus.DRAFT, SceneStatus.ARCHIVED})


def parse_version(version: str) -> tuple[int, int, int]:
    """解析语义化版本（非法格式抛 ValueError）。"""
    parts = version.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f"非法语义化版本: {version}")
    return int(parts[0]), int(parts[1]), int(parts[2])


class SceneService:
    """场景库业务服务（路由与数据访问解耦，便于单元测试）。"""

    def __init__(
        self, repository: SceneRepository, cache: SceneDetailCache, settings: Settings
    ) -> None:
        self._repository = repository
        self._cache = cache
        self._settings = settings

    # ---------- 查询 ----------

    async def list_scenes(
        self,
        *,
        user_id: str,
        roles: frozenset[str],
        page: int,
        page_size: int,
        scene_type: str | None,
        status: SceneStatus | None,
        tags: Sequence[str] | None,
        keyword: str | None,
        creator: UUID | None,
        sort: str,
        order: str,
    ) -> SceneListData:
        """场景列表（默认 create_time DESC；tags 为 AND 包含查询；软删除不出现）。"""
        scoped_creator = self._scoped_creator(user_id=user_id, roles=roles, requested=creator)
        rows, total = await self._repository.list_page(
            page=page,
            page_size=page_size,
            scene_type=scene_type,
            status=status,
            tags=tags,
            keyword=keyword,
            creator=scoped_creator,
            sort=sort,
            order=order,
        )
        return SceneListData(
            items=[self._to_schema(row) for row in rows],
            total=total,
            page=page,
            page_size=page_size,
        )

    async def get_scene(self, scene_id: UUID) -> Scene:
        """场景详情（先查 Redis 缓存，未命中回源数据库并回填缓存）。"""
        cached = await self._cache.get(scene_id)
        if cached is not None:
            try:
                return Scene.model_validate(cached)
            except ValidationError:
                logger.warning("scene_cache_schema_mismatch", scene_id=str(scene_id))
        model = await self._load_model(scene_id)
        scene = self._to_schema(model)
        await self._cache.set(scene_id, scene.model_dump(mode="json"))
        return scene

    # ---------- 写入 ----------

    async def create_scene(self, payload: SceneCreateRequest, *, user_id: str) -> Scene:
        """创建场景（status=draft、creator=网关注入用户、version 默认 1.0.0；重名 3002）。"""
        self._validate_config(payload.config)
        creator = self._as_uuid(user_id, field="X-User-Id")
        if await self._repository.name_taken(payload.scene_name):
            raise ResourceAlreadyExistsError(
                "场景名称已存在", details={"scene_name": payload.scene_name}
            )
        model = await self._repository.create(
            scene_name=payload.scene_name,
            scene_type=payload.scene_type.value,
            description=payload.description,
            tags=payload.tags,
            config=payload.config.model_dump(mode="json"),
            creator=creator,
            version=self._settings.scene_default_version,
        )
        logger.info("scene_created", scene_id=str(model.scene_id), user_id=user_id)
        return self._to_schema(model)

    async def update_scene(
        self, scene_id: UUID, payload: SceneUpdateRequest, *, user_id: str
    ) -> Scene:
        """全量更新场景（仅 draft 可编辑 → 否则 3003；重名 3002；写后失效缓存）。"""
        self._validate_config(payload.config)
        model = await self._load_model(scene_id)
        if model.status not in EDITABLE_STATES:
            raise ResourceStateConflictError(
                f"场景状态为 {model.status}，仅 draft 可编辑",
                details={"scene_id": str(scene_id), "status": str(model.status)},
            )
        if await self._repository.name_taken(payload.scene_name, exclude_id=scene_id):
            raise ResourceAlreadyExistsError(
                "场景名称已存在", details={"scene_name": payload.scene_name}
            )
        updated = await self._repository.update(
            scene_id,
            scene_name=payload.scene_name,
            scene_type=payload.scene_type.value,
            description=payload.description,
            tags=payload.tags,
            config=payload.config.model_dump(mode="json"),
        )
        if updated is None:
            raise ResourceNotFoundError(details={"scene_id": str(scene_id)})
        await self._cache.invalidate(scene_id)
        logger.info("scene_updated", scene_id=str(scene_id), user_id=user_id)
        return self._to_schema(updated)

    async def delete_scene(self, scene_id: UUID) -> SceneDeleteData:
        """软删除场景（仅 draft/archived 可删；published → 3003，需先归档）。"""
        model = await self._load_model(scene_id)
        if model.status not in DELETABLE_STATES:
            raise ResourceStateConflictError(
                f"场景状态为 {model.status}，published 场景需先归档再删除",
                details={"scene_id": str(scene_id), "status": str(model.status)},
            )
        deleted = await self._repository.soft_delete(scene_id)
        if not deleted:
            raise ResourceNotFoundError(details={"scene_id": str(scene_id)})
        await self._cache.invalidate(scene_id)
        logger.info("scene_deleted", scene_id=str(scene_id))
        return SceneDeleteData(scene_id=scene_id, deleted=True)

    async def duplicate_scene(
        self, scene_id: UUID, payload: SceneDuplicateRequest | None, *, user_id: str
    ) -> Scene:
        """复制场景为新草稿（新 scene_id、status=draft、creator=当前用户、version 重置）。"""
        source = await self._load_model(scene_id)
        creator = self._as_uuid(user_id, field="X-User-Id")
        requested_name = payload.new_scene_name if payload is not None else None
        if requested_name is not None:
            return await self._create_copy(source, requested_name, creator, retries=1)
        base = f"{source.scene_name}{self._settings.scene_duplicate_name_suffix}"
        return await self._create_copy(
            source, base, creator, retries=max(1, self._settings.scene_duplicate_max_attempts)
        )

    async def publish_scene(self, scene_id: UUID, payload: ScenePublishRequest | None) -> Scene:
        """发布场景（draft → published；可显式更新 version，需 ≥ 当前版本）。"""
        model = await self._load_model(scene_id)
        if model.status is not SceneStatus.DRAFT:
            raise ResourceStateConflictError(
                f"场景状态为 {model.status}，仅 draft 可发布",
                details={"scene_id": str(scene_id), "status": str(model.status)},
            )
        # 发布前再次做一次 4.2.2 结构完整性校验（契约 publishScene 规则）
        self._validate_config(self._parse_stored_config(model))
        version = self._resolve_publish_version(payload, current=str(model.version))
        published = await self._repository.set_status(
            scene_id, status=SceneStatus.PUBLISHED, version=version
        )
        if published is None:
            raise ResourceNotFoundError(details={"scene_id": str(scene_id)})
        await self._cache.invalidate(scene_id)
        logger.info("scene_published", scene_id=str(scene_id), version=str(published.version))
        return self._to_schema(published)

    # ---------- 供导出 / 仿真下发复用的读取入口 ----------

    async def load_scene_model(self, scene_id: UUID) -> SceneModel:
        """读取场景 ORM 模型（不存在 → 3001）。"""
        return await self._load_model(scene_id)

    def to_schema(self, model: SceneModel) -> Scene:
        """ORM → 契约 Schema（导出/下发复用）。"""
        return self._to_schema(model)

    # ---------- 内部工具 ----------

    async def _load_model(self, scene_id: UUID) -> SceneModel:
        """加载存活场景（不存在 → 3001 资源不存在）。"""
        model = await self._repository.get(scene_id)
        if model is None:
            raise ResourceNotFoundError(details={"scene_id": str(scene_id)})
        return model

    async def _create_copy(
        self, source: SceneModel, base_name: str, creator: UUID, *, retries: int
    ) -> Scene:
        """按源场景配置创建副本（名称冲突时追加序号重试，耗尽重试抛 3002）。"""
        candidate = base_name
        for attempt in range(1, retries + 1):
            if attempt > 1:
                candidate = f"{base_name}-{attempt}"
            try:
                model = await self._repository.create(
                    scene_name=candidate,
                    scene_type=str(source.scene_type),
                    description=source.description,
                    tags=list(source.tags or []),
                    config=self._parse_stored_config(source).model_dump(mode="json"),
                    creator=creator,
                    version=self._settings.scene_default_version,
                )
            except ResourceAlreadyExistsError:
                continue
            logger.info(
                "scene_duplicated",
                source_scene_id=str(source.scene_id),
                scene_id=str(model.scene_id),
            )
            return self._to_schema(model)
        raise ResourceAlreadyExistsError(
            "复制场景名称冲突（重试耗尽）", details={"base_name": base_name}
        )

    def _to_schema(self, model: SceneModel) -> Scene:
        """ORM → Scene Schema（config_json 结构非法时按 5000 处理，禁止静默丢弃）。"""
        config = self._parse_stored_config(model)
        try:
            scene_type = SceneType(str(model.scene_type))
            status = SceneStatus(str(model.status))
        except ValueError as exc:
            logger.error(
                "scene_enum_invalid_in_db",
                scene_id=str(model.scene_id),
                scene_type=str(model.scene_type),
                status=str(model.status),
            )
            raise InternalServerError(
                "场景数据非法", details={"scene_id": str(model.scene_id)}
            ) from exc
        return Scene(
            scene_id=model.scene_id,
            scene_name=model.scene_name,
            scene_type=scene_type,
            description=model.description,
            version=model.version,
            creator=model.creator,
            tags=list(model.tags or []),
            status=status,
            create_time=model.create_time,
            update_time=model.update_time,
            config=config,
        )

    @staticmethod
    def _parse_stored_config(model: SceneModel) -> SceneConfig:
        """解析 scenes.config_json 为 4.2.2 结构（非法 → 5000，防止脏数据流入响应）。"""
        try:
            return SceneConfig.model_validate(model.config_json or {})
        except ValidationError as exc:
            logger.error("scene_config_invalid_in_db", scene_id=str(model.scene_id))
            raise InternalServerError(
                "场景配置数据非法", details={"scene_id": str(model.scene_id)}
            ) from exc

    def _validate_config(self, config: SceneConfig) -> None:
        """写入/发布前的附加校验：时长上限（契约未定义上限，经配置环境变量化 → 2001）。"""
        limit = self._settings.scene_duration_max_seconds
        if config.duration > limit:
            raise InvalidParameterError(
                f"场景时长超过上限 {limit}s", details={"duration": config.duration, "limit": limit}
            )

    def _scoped_creator(
        self, *, user_id: str, roles: frozenset[str], requested: UUID | None
    ) -> UUID | None:
        """数据权限隔离：开启且角色非豁免时，强制按当前用户过滤（忽略客户端传值）。"""
        if not self._settings.scene_scope_by_creator:
            return requested
        if roles & self._settings.scene_scope_exempt_role_set:
            return requested
        return self._as_uuid(user_id, field="X-User-Id")

    @staticmethod
    def _as_uuid(value: str, *, field: str) -> UUID:
        """字符串 → UUID（非法 → 2001 参数错误）。"""
        try:
            return UUID(str(value))
        except (ValueError, AttributeError, TypeError) as exc:
            raise InvalidParameterError(f"{field} 非法", details={"value": str(value)}) from exc

    def _resolve_publish_version(
        self, payload: ScenePublishRequest | None, *, current: str
    ) -> str | None:
        """发布版本解析：未传则保持原值（契约待确认 #7）；传值需 ≥ 当前版本。"""
        if payload is None or payload.version is None:
            return None
        try:
            requested = parse_version(payload.version)
            current_version = parse_version(current)
        except ValueError as exc:
            raise InvalidParameterError("版本号非法", details={"version": payload.version}) from exc
        if requested < current_version:
            raise InvalidParameterError(
                "发布版本不得低于当前版本",
                details={"version": payload.version, "current": current},
            )
        return payload.version


__all__ = ["DELETABLE_STATES", "EDITABLE_STATES", "SceneService", "parse_version"]