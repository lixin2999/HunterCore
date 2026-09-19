"""scene-service 单元测试替身（不依赖 PostgreSQL / Redis / MinIO / Carla / Kafka）。

- InMemorySceneRepository：与 SceneRepository 同方法签名的内存实现（软删除、名称唯一、
  筛选与排序语义对齐真实 SQL 行为）；
- FakeSceneCache / InMemorySceneStorage / FakeCarlaClient：缓存、对象存储、Carla 管理 API 替身；
- 数据工厂：config_payload / create_payload / make_scene_model（契约字段名与取值一致）。
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from hunter_common.database.enums import SceneStatus
from hunter_common.database.models import Scene
from hunter_common.exceptions import ResourceAlreadyExistsError, ServiceUnavailableError

from app.repositories.carla import SimInstance

#: 默认创建者（与契约 creator 的 uuid 语义一致）
DEFAULT_CREATOR = UUID("11111111-1111-4111-8111-111111111111")
#: 契约天气字段（7 字段全部必填）
_DEFAULT_WEATHER: dict[str, float] = {
    "cloudiness": 0.0,
    "rain": 0.0,
    "wetness": 0.0,
    "fog": 0.0,
    "wind": 0.0,
    "sun_azimuth": 0.0,
    "sun_altitude": 45.0,
}


def config_payload(**overrides: Any) -> dict[str, Any]:
    """构造 4.2.2 结构 config（可直接作为创建/更新请求的 config）。"""
    payload: dict[str, Any] = {
        "map": {
            "map_id": "Town01",
            "map_type": "carla_town",
            "spawn_point": {"x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0},
        },
        "ego_vehicle": {"model": "HUNTER_SE", "initial_speed": 1.5, "initial_steer": 0.0},
        "weather": dict(_DEFAULT_WEATHER),
        "actors": [],
        "events": [],
        "success_criteria": {
            "max_speed_deviation": 0.5,
            "no_collision": True,
            "min_safe_distance": 3.0,
        },
        "duration": 30.0,
    }
    payload.update(overrides)
    return payload


def create_payload(scene_name: str = "直线巡航场景", **overrides: Any) -> dict[str, Any]:
    """构造 POST /api/v1/scene 请求体。"""
    payload: dict[str, Any] = {
        "scene_name": scene_name,
        "scene_type": "straight_cruise",
        "description": "单元测试场景",
        "tags": ["test", "basic"],
        "config": config_payload(),
    }
    payload.update(overrides)
    return payload


def make_scene_model(
    *,
    scene_name: str = "场景-1",
    scene_type: str = "straight_cruise",
    status: SceneStatus = SceneStatus.DRAFT,
    config: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    version: str = "1.0.0",
    creator: UUID = DEFAULT_CREATOR,
    scene_id: UUID | None = None,
) -> Scene:
    """构造 scenes 行（未落库的 ORM 实例，供内存仓储与断言使用）。"""
    now = datetime.now(UTC)
    return Scene(
        scene_id=scene_id or uuid4(),
        scene_name=scene_name,
        scene_type=scene_type,
        description="单元测试场景",
        config_json=config if config is not None else config_payload(),
        version=version,
        creator=creator,
        create_time=now,
        update_time=now,
        tags=tags if tags is not None else ["test"],
        status=status,
        deleted_at=None,
    )


class InMemorySceneRepository:
    """SceneRepository 内存替身（软删除过滤 / 名称唯一 / 筛选与排序语义对齐真实实现）。"""

    def __init__(self, rows: list[Scene] | None = None) -> None:
        self.rows: dict[UUID, Scene] = {row.scene_id: row for row in (rows or [])}

    # ---------- 查询 ----------

    async def list_page(
        self,
        *,
        page: int,
        page_size: int,
        scene_type: str | None = None,
        status: SceneStatus | None = None,
        tags: list[str] | None = None,
        keyword: str | None = None,
        creator: UUID | None = None,
        sort: str = "create_time",
        order: str = "desc",
    ) -> tuple[list[Scene], int]:
        rows = [row for row in self.rows.values() if row.deleted_at is None]
        if scene_type is not None:
            rows = [row for row in rows if row.scene_type == scene_type]
        if status is not None:
            rows = [row for row in rows if row.status == status]
        if tags:
            rows = [row for row in rows if set(tags) <= set(row.tags or [])]
        if keyword:
            lowered = keyword.lower()
            rows = [
                row
                for row in rows
                if lowered in row.scene_name.lower() or lowered in (row.description or "").lower()
            ]
        if creator is not None:
            rows = [row for row in rows if row.creator == creator]
        rows.sort(key=lambda row: getattr(row, sort), reverse=order == "desc")
        total = len(rows)
        start = (page - 1) * page_size
        return rows[start : start + page_size], total

    async def get(self, scene_id: UUID) -> Scene | None:
        row = self.rows.get(scene_id)
        return row if row is not None and row.deleted_at is None else None

    async def get_by_name(self, scene_name: str) -> Scene | None:
        return next(
            (
                row
                for row in self.rows.values()
                if row.scene_name == scene_name and row.deleted_at is None
            ),
            None,
        )

    async def name_taken(self, scene_name: str, *, exclude_id: UUID | None = None) -> bool:
        return any(
            row.scene_name == scene_name and row.deleted_at is None and row.scene_id != exclude_id
            for row in self.rows.values()
        )

    async def list_by_ids(self, scene_ids: list[UUID], *, limit: int) -> list[Scene]:
        return [
            row
            for scene_id in scene_ids[:limit]
            if (row := self.rows.get(scene_id)) is not None and row.deleted_at is None
        ]

    # ---------- 写入 ----------

    async def create(
        self,
        *,
        scene_name: str,
        scene_type: str,
        description: str | None,
        tags: list[str],
        config: dict[str, Any],
        creator: UUID,
        version: str,
    ) -> Scene:
        if await self.name_taken(scene_name):
            raise ResourceAlreadyExistsError("场景名称已存在", details={"scene_name": scene_name})
        row = make_scene_model(
            scene_name=scene_name,
            scene_type=str(scene_type),
            config=config,
            tags=list(tags),
            version=version,
            creator=creator,
        )
        row.description = description
        self.rows[row.scene_id] = row
        return row

    async def update(
        self,
        scene_id: UUID,
        *,
        scene_name: str,
        scene_type: str,
        description: str | None,
        tags: list[str],
        config: dict[str, Any],
    ) -> Scene | None:
        row = await self.get(scene_id)
        if row is None:
            return None
        if await self.name_taken(scene_name, exclude_id=scene_id):
            raise ResourceAlreadyExistsError("场景名称已存在", details={"scene_name": scene_name})
        row.scene_name = scene_name
        row.scene_type = str(scene_type)
        row.description = description
        row.tags = list(tags)
        row.config_json = config
        row.update_time = datetime.now(UTC)
        return row

    async def set_status(
        self, scene_id: UUID, *, status: SceneStatus, version: str | None = None
    ) -> Scene | None:
        row = await self.get(scene_id)
        if row is None:
            return None
        row.status = status
        if version is not None:
            row.version = version
        row.update_time = datetime.now(UTC)
        return row

    async def soft_delete(self, scene_id: UUID) -> bool:
        row = await self.get(scene_id)
        if row is None:
            return False
        row.deleted_at = datetime.now(UTC)
        return True


class FakeSceneCache:
    """SceneDetailCache 内存替身（记录失效调用，供缓存行为断言）。"""

    def __init__(self) -> None:
        self.store: dict[str, dict[str, Any]] = {}
        self.invalidated: list[str] = []
        self.get_calls: list[str] = []

    async def get(self, scene_id: UUID) -> dict[str, Any] | None:
        self.get_calls.append(str(scene_id))
        payload = self.store.get(str(scene_id))
        return json.loads(json.dumps(payload)) if payload is not None else None

    async def set(self, scene_id: UUID, payload: dict[str, Any]) -> None:
        self.store[str(scene_id)] = json.loads(json.dumps(payload))

    async def invalidate(self, scene_id: UUID) -> None:
        self.invalidated.append(str(scene_id))
        self.store.pop(str(scene_id), None)


class InMemorySceneStorage:
    """SceneAssetStorage 内存替身（上传/预签名；可注入失败触发 5001）。"""

    def __init__(self, presign_expire_seconds: int = 900) -> None:
        self.objects: dict[str, bytes] = {}
        self.presign_expire_seconds = presign_expire_seconds
        self.bucket = "hunter-scene-assets"
        self.fail_upload = False

    async def upload_bytes(self, key: str, payload: bytes, *, content_type: str) -> None:
        if self.fail_upload:
            raise ServiceUnavailableError("对象存储写入失败", details={"key": key})
        self.objects[key] = payload

    async def presign_download(self, key: str) -> str:
        return f"https://minio.test/{self.bucket}/{key}?expires={self.presign_expire_seconds}"

    async def healthcheck(self) -> bool:
        return True


class FakeCarlaClient:
    """CarlaManagementClient 替身（记录创建/下发调用；可注入不可用 → 5001）。"""

    def __init__(self, *, instance_id: str = "sim-0001", status: str = "running") -> None:
        self.instance_id = instance_id
        self.status = status
        self.created: list[dict[str, Any]] = []
        self.submitted: list[tuple[str, dict[str, Any]]] = []
        self.active: dict[str, SimInstance] = {}
        self.unavailable = False

    async def create_instance(
        self, *, scene_id: str, scene_name: str, scene_config: dict[str, Any]
    ) -> SimInstance:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达", details={"scene_id": scene_id})
        self.created.append(
            {"scene_id": scene_id, "scene_name": scene_name, "scene_config": scene_config}
        )
        return SimInstance(instance_id=self.instance_id, status=self.status)

    async def find_active_instance(self, scene_id: str) -> SimInstance | None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达", details={"scene_id": scene_id})
        return self.active.get(scene_id)

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        self.submitted.append((instance_id, payload))

    async def close(self) -> None:
        return None


__all__ = [
    "DEFAULT_CREATOR",
    "FakeCarlaClient",
    "FakeSceneCache",
    "InMemorySceneRepository",
    "InMemorySceneStorage",
    "config_payload",
    "create_payload",
    "make_scene_model",
]