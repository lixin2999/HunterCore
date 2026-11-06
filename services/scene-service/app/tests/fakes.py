"""scene-service 单元测试替身（不依赖 PostgreSQL / Redis / MinIO / Carla / Kafka）。

- InMemorySceneRepository：与 SceneRepository 同方法签名的内存实现（软删除、名称唯一、
  筛选与排序语义对齐真实 SQL 行为）；
- FakeSceneCache / InMemorySceneStorage / FakeCarlaClient：缓存、对象存储、Carla 管理 API 替身；
- InMemorySimNodeStore：仿真节点登记表 + 实例归属索引（Redis 实现）的内存替身（V1.24.0）；
- RecordingCarlaClientFactory：node 模式代理的客户端工厂替身（断言「按学到的地址直推」与 IP 漂移）；
- 数据工厂：config_payload / create_payload / make_scene_model（契约字段名与取值一致）。
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from hunter_common.database.enums import SceneStatus
from hunter_common.database.models import Scene
from hunter_common.exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    ServiceUnavailableError,
)

from app.core.client_ip import build_base_url
from app.repositories.carla import SimInstance, SimInstanceSnapshot
from app.repositories.sim_nodes import NodeEndpoint

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
            # 非原点坐标：V1.24.8 起下发前会拦落在地图原点的自车生成点（仿真端会黑屏）
            "spawn_point": {"x": 120.5, "y": 310.25, "z": 0.0, "yaw": 0.0},
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


class FakeVilSessionStore:
    """VilSessionStore 替身（内存会话 + 实例反查；不依赖 Redis）。"""

    def __init__(self) -> None:
        self.sessions: dict[str, dict[str, Any]] = {}
        self.instances: dict[str, dict[str, Any]] = {}

    async def get(self, vehicle_id: str) -> dict[str, Any] | None:
        return self.sessions.get(vehicle_id)

    async def acquire(self, payload: dict[str, Any]) -> bool:
        vehicle_id = str(payload.get("vehicle_id") or "")
        if vehicle_id in self.sessions:
            return False
        self.sessions[vehicle_id] = dict(payload)
        return True

    async def bind_instance(
        self,
        *,
        vehicle_id: str,
        sim_instance_id: str,
        sim_id: str,
        scene_id: str,
        mode: str,
        vil_state_topic: str,
        sensor_data_topic: str,
        instance_ttl_seconds: int = 86400,
    ) -> None:
        meta = {
            "sim_instance_id": sim_instance_id,
            "sim_id": sim_id,
            "scene_id": scene_id,
            "vehicle_id": vehicle_id,
            "mode": mode,
            "vil_state_topic": vil_state_topic,
            "sensor_data_topic": sensor_data_topic,
        }
        self.instances[sim_instance_id] = meta
        if vehicle_id and vehicle_id in self.sessions:
            self.sessions[vehicle_id].update(meta)

    async def lookup_instance(self, sim_instance_id: str) -> dict[str, Any] | None:
        return self.instances.get(sim_instance_id)

    async def release(
        self, vehicle_id: str, *, sim_id: str | None = None, sim_instance_id: str | None = None
    ) -> None:
        current = self.sessions.get(vehicle_id)
        if sim_id is not None and current is not None and current.get("sim_id") != sim_id:
            return
        self.sessions.pop(vehicle_id, None)
        if sim_instance_id:
            self.instances.pop(sim_instance_id, None)

    async def list_active(self) -> list[str]:
        return list(self.sessions)


class FakeTopicProvisioner:
    """SimTopicProvisioner 替身（记录创建/删除，可注入失败）。"""

    def __init__(self) -> None:
        self.ensured: list[str] = []
        self.deleted: list[str] = []
        self.fail_ensure = False

    async def ensure_session(self, sim_id: str) -> list[str]:
        if self.fail_ensure:
            raise RuntimeError("topic ensure failed")
        self.ensured.append(sim_id)
        return [f"carla.{sim_id}.vil_state", f"carla.{sim_id}.sensor_data"]

    async def delete_session(self, sim_id: str) -> list[str]:
        self.deleted.append(sim_id)
        return [f"carla.{sim_id}.vil_state", f"carla.{sim_id}.sensor_data"]


class FakeCarlaClient:
    """CarlaManagementClient 替身（记录创建/下发调用；可注入不可用 → 5001、404 → 3001）。"""

    def __init__(self, *, instance_id: str = "sim-0001", status: str = "running") -> None:
        self.instance_id = instance_id
        self.status = status
        self.created: list[dict[str, Any]] = []
        self.submitted: list[tuple[str, dict[str, Any]]] = []
        self.controls: list[tuple[str, str]] = []
        self.active: dict[str, SimInstance] = {}
        self.unavailable = False
        # G-20① 进度/结果查询替身数据：instance_id → 原始响应体（未登记时 404 → 3001）
        self.progress: dict[str, dict[str, Any]] = {}
        self.results: dict[str, dict[str, Any]] = {}

    async def create_instance(
        self,
        *,
        scene_id: str,
        scene_name: str,
        scene_config: dict[str, Any],
        **kwargs: Any,
    ) -> SimInstance:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达", details={"scene_id": scene_id})
        self.created.append(
            {
                "scene_id": scene_id,
                "scene_name": scene_name,
                "scene_config": scene_config,
                **kwargs,
            }
        )
        return SimInstance(
            instance_id=self.instance_id,
            status=self.status,
            sim_id=kwargs.get("sim_id"),
            data=dict(self.results.get(self.instance_id) or {}),
        )

    async def pause_instance(self, instance_id: str) -> None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        self.controls.append(("pause", instance_id))

    async def resume_instance(self, instance_id: str) -> None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        self.controls.append(("resume", instance_id))

    async def stop_instance(self, instance_id: str) -> None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        self.controls.append(("stop", instance_id))

    async def find_active_instance(self, scene_id: str) -> SimInstance | None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达", details={"scene_id": scene_id})
        return self.active.get(scene_id)

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        self.submitted.append((instance_id, payload))

    async def get_instance(self, instance_id: str) -> SimInstanceSnapshot:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        body = self.progress.get(instance_id)
        if body is None:
            raise ResourceNotFoundError(
                f"仿真实例不存在或已回收：{instance_id}",
                details={"sim_instance_id": instance_id},
            )
        return SimInstanceSnapshot(
            instance_id=instance_id, status=str(body.get("status") or "running"), data=body
        )

    async def get_result(self, instance_id: str) -> SimInstanceSnapshot:
        if self.unavailable:
            raise ServiceUnavailableError("Carla 管理 API 不可达")
        body = self.results.get(instance_id)
        if body is None:
            raise ResourceNotFoundError(
                f"仿真实例不存在或已回收：{instance_id}",
                details={"sim_instance_id": instance_id},
            )
        return SimInstanceSnapshot(
            instance_id=instance_id, status=str(body.get("status") or "running"), data=body
        )

    async def close(self) -> None:
        return None


class InMemorySimNodeStore:
    """SimNodeStore 内存替身（不依赖 Redis；语义与真实实现逐条对齐）。

    与 Redis 版本的共同语义：节点登记为覆盖式写 + 刷新 seen 分数、在线 = 心跳未超租约、
    元数据消失时懒清理索引成员、选节点按「空闲容量降序 → node_id 升序」、
    ``node_endpoint`` 只在在线且地址合法时返回当前基址（否则 None ⇒ 调用方转 5001）。
    """

    def __init__(
        self,
        *,
        lease_seconds: int = 90,
        instance_ttl_seconds: int = 86400,
        default_capacity: int = 1,
    ) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}
        self.seen: dict[str, float] = {}
        self.runtime: dict[str, dict[str, Any]] = {}
        self.active: dict[str, float] = {}
        self._lease = lease_seconds
        self._instance_ttl = instance_ttl_seconds
        self._default_capacity = default_capacity
        # 测试可注入故障，验证 5001 不被吞成 5000
        self.unavailable = False

    @property
    def lease_seconds(self) -> int:
        return self._lease

    @property
    def instance_ttl_seconds(self) -> int:
        return self._instance_ttl

    @property
    def default_capacity(self) -> int:
        return self._default_capacity

    def _check(self) -> None:
        if self.unavailable:
            raise ServiceUnavailableError("仿真节点存储不可用")

    # ---------- 节点登记表 ----------

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        self._check()
        return self.nodes.get(node_id)

    async def save_node(self, node_id: str, record: dict[str, Any], *, now: float) -> None:
        self._check()
        self.nodes[node_id] = dict(record)
        self.seen[node_id] = now

    async def node_online(self, node_id: str, *, now: float) -> bool:
        self._check()
        score = self.seen.get(node_id)
        if score is None or score <= now - self._lease:
            return False
        return node_id in self.nodes

    async def list_nodes(self, *, online_only: bool, now: float) -> list[dict[str, Any]]:
        self._check()
        nodes: list[dict[str, Any]] = []
        for node_id, score in sorted(self.seen.items()):
            record = self.nodes.get(node_id)
            if record is None:
                self.seen.pop(node_id, None)
                continue
            online = score > now - self._lease
            if online_only and not online:
                continue
            running = record.get("running_instance_ids")
            nodes.append(
                {
                    **record,
                    "node_id": node_id,
                    "online": online,
                    "last_seen_at": score,
                    "lease_seconds": self._lease,
                    "running_count": len(running) if isinstance(running, list) else 0,
                }
            )
        return nodes

    # ---------- 下发寻址（只返回**当前**学到的地址） ----------

    async def node_endpoint(self, node_id: str, *, now: float) -> NodeEndpoint | None:
        self._check()
        score = self.seen.get(node_id)
        if score is None or score <= now - self._lease:
            return None
        record = self.nodes.get(node_id)
        if record is None:
            return None
        ip = str(record.get("endpoint_ip") or "").strip()
        port = record.get("manager_port")
        if not ip or not isinstance(port, int) or not 1 <= port <= 65535:
            return None
        scheme = str(record.get("protocol") or "http").strip().lower()
        if scheme not in ("http", "https"):
            scheme = "http"
        return NodeEndpoint(
            node_id=node_id,
            base_url=build_base_url(ip, port, scheme),
            ip=ip,
            port=port,
            scheme=scheme,
            source=str(record.get("endpoint_source") or "") or None,
        )

    # ---------- 运行时快照与活动实例 ----------

    async def save_runtime(self, sim_instance_id: str, snapshot: dict[str, Any]) -> None:
        self._check()
        self.runtime[sim_instance_id] = dict(snapshot)

    async def get_runtime(self, sim_instance_id: str) -> dict[str, Any] | None:
        self._check()
        return self.runtime.get(sim_instance_id)

    async def add_active_instance(self, sim_instance_id: str, *, now: float) -> None:
        self._check()
        self.active[sim_instance_id] = now

    async def remove_active_instance(self, sim_instance_id: str) -> None:
        self._check()
        self.active.pop(sim_instance_id, None)

    async def active_instance_records(self, *, now: float) -> list[dict[str, Any]]:
        self._check()
        records: list[dict[str, Any]] = []
        for sim_instance_id in sorted(self.active):
            snapshot = self.runtime.get(sim_instance_id)
            if snapshot is None:
                self.active.pop(sim_instance_id, None)
                continue
            records.append({**snapshot, "sim_instance_id": sim_instance_id})
        return records

    async def select_node(
        self, *, now: float, map_id: str | None = None, prefer: str | None = None
    ) -> dict[str, Any] | None:
        self._check()
        nodes = await self.list_nodes(online_only=True, now=now)
        if not nodes:
            return None
        if prefer:
            return next((node for node in nodes if node.get("node_id") == prefer), None)
        candidates = [
            node
            for node in nodes
            if not map_id
            or not _declared_maps(node)
            or map_id in _declared_maps(node)
        ]
        if not candidates:
            return None
        candidates.sort(key=_free_capacity_sort_key)
        return candidates[0]


class RecordingCarlaClient:
    """node 模式代理的委派对象：把基址（``base_url``）与调用序列全部记到工厂上。

    只关心「发到哪个地址」这件事：基址断言 = 「按学到的 IP 直推」与「IP 变更后基址变化」
    的共同证据；对已关闭客户端的调用直接断语失败 ⇒ 旧基址不可能还在用。
    """

    def __init__(self, base_url: str, factory: RecordingCarlaClientFactory) -> None:
        self.base_url = base_url
        self.factory = factory
        self.closed = False
        self.unavailable = False
        self.progress: dict[str, dict[str, Any]] = {}
        self.results: dict[str, dict[str, Any]] = {}

    def _check(self, action: str, target: str = "") -> None:
        if self.closed:
            raise AssertionError(f"客户端已关闭仍被调用：{self.base_url} {action}")
        self.factory.calls.append((self.base_url, action))
        if self.unavailable:
            raise ServiceUnavailableError(
                "Carla 管理 API 不可达", details={"endpoint": self.base_url, "stage": action}
            )
        if target and self.factory.missing_instances and target not in (
            *self.progress,
            *self.results,
            *self.factory.known_instances,
        ):
            raise ResourceNotFoundError(
                f"仿真实例不存在或已回收：{target}", details={"sim_instance_id": target}
            )

    async def create_instance(
        self,
        *,
        scene_id: str,
        scene_name: str,
        scene_config: dict[str, Any],
        sim_id: str | None = None,
        **kwargs: Any,
    ) -> SimInstance:
        self._check("create_instance")
        instance_id = self.factory.next_instance_id()
        self.factory.known_instances.append(instance_id)
        self.factory.created.append(
            {"base_url": self.base_url, "instance_id": instance_id, "scene_id": scene_id, "sim_id": sim_id}
        )
        return SimInstance(
            instance_id=instance_id,
            status=self.factory.status,
            sim_id=sim_id,
            data={"scene_name": scene_name, **kwargs},
        )

    async def submit_scenario(self, instance_id: str, payload: dict[str, Any]) -> None:
        self._check("submit_scenario", instance_id)
        self.factory.submitted.append((self.base_url, instance_id))

    async def get_instance(self, instance_id: str) -> SimInstanceSnapshot:
        self._check("get_instance", instance_id)
        body = self.progress.get(instance_id) or {"status": self.factory.status}
        return SimInstanceSnapshot(
            instance_id=instance_id, status=str(body.get("status") or "running"), data=dict(body)
        )

    async def get_result(self, instance_id: str) -> SimInstanceSnapshot:
        self._check("get_result", instance_id)
        body = self.results.get(instance_id) or {"status": self.factory.status}
        return SimInstanceSnapshot(
            instance_id=instance_id, status=str(body.get("status") or "running"), data=dict(body)
        )

    async def pause_instance(self, instance_id: str) -> None:
        self._check("pause_instance", instance_id)

    async def resume_instance(self, instance_id: str) -> None:
        self._check("resume_instance", instance_id)

    async def stop_instance(self, instance_id: str) -> None:
        self._check("stop_instance", instance_id)

    async def find_active_instance(self, scene_id: str) -> SimInstance | None:
        self._check("find_active_instance")
        return None

    async def close(self) -> None:
        self.closed = True
        self.factory.closed_base_urls.append(self.base_url)


class RecordingCarlaClientFactory:
    """``SimNodeBackend(client_factory=...)`` 替身：记录创建了哪些基址、关闭了哪些、调用序列。

    用法：``factory.unreachable.add("http://192.168.31.99:8090")`` 模拟回访不通；
    ``factory.calls`` 是「基址 → 动作」的完整序列，可直接断言 IP 变更后新基址只被新客户端调用。
    """

    def __init__(self, *, status: str = "running", missing_instances: bool = False) -> None:
        self.status = status
        self.missing_instances = missing_instances
        self.clients: list[RecordingCarlaClient] = []
        self.by_base_url: dict[str, RecordingCarlaClient] = {}
        self.calls: list[tuple[str, str]] = []
        self.created: list[dict[str, Any]] = []
        self.submitted: list[tuple[str, str]] = []
        self.closed_base_urls: list[str] = []
        self.known_instances: list[str] = []
        self.unreachable: set[str] = set()
        self._seq = 0

    def __call__(self, base_url: str, node_id: str = "") -> RecordingCarlaClient:
        client = RecordingCarlaClient(base_url, self)
        client.unavailable = base_url in self.unreachable
        self.clients.append(client)
        self.by_base_url.setdefault(base_url, client)
        return client

    def next_instance_id(self) -> str:
        self._seq += 1
        return f"sim-node-{self._seq:04d}"

    def client_for(self, base_url: str) -> RecordingCarlaClient:
        return self.by_base_url[base_url]

    def actions_for(self, base_url: str) -> list[str]:
        return [action for url, action in self.calls if url == base_url]

    def live_clients(self) -> list[RecordingCarlaClient]:
        return [client for client in self.clients if not client.closed]


def _declared_maps(node: dict[str, Any]) -> tuple[str, ...]:
    maps = node.get("maps")
    return tuple(str(item) for item in maps if str(item).strip()) if isinstance(maps, list) else ()


def _free_capacity_sort_key(node: dict[str, Any]) -> tuple[int, str]:
    raw_capacity = node.get("capacity")
    capacity = raw_capacity if isinstance(raw_capacity, int) else 1
    running = node.get("running_instance_ids")
    busy = len(running) if isinstance(running, list) else 0
    return (-(max(0, capacity - busy)), str(node.get("node_id") or ""))


__all__ = [
    "DEFAULT_CREATOR",
    "FakeCarlaClient",
    "FakeSceneCache",
    "FakeTopicProvisioner",
    "FakeVilSessionStore",
    "InMemorySceneRepository",
    "InMemorySceneStorage",
    "InMemorySimNodeStore",
    "RecordingCarlaClient",
    "RecordingCarlaClientFactory",
    "config_payload",
    "create_payload",
    "make_scene_model",
]