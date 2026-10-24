"""CARLA 接入硬化回归（V1.23.4）：孤儿实例 / 非幂等重试 / 会话原子性 / 终态对账。"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from hunter_common.database.enums import SceneStatus
from hunter_common.exceptions import ServiceUnavailableError
from redis.exceptions import RedisError

from app.repositories.carla import CarlaManagementClient, CarlaSimIdMismatchError
from app.repositories.simulations import VilSessionStore
from app.tests.conftest import ADMIN_HEADERS
from app.tests.fakes import make_scene_model

SCENE_BASE = "/api/v1/scene"


class _Redis:
    """VilSessionStore 的最小 Redis 替身（可注入 SADD/DEL 失败）。"""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.sadd_fail = False
        self.delete_fail = False

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> Any:
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def sadd(self, key: str, member: str) -> int:
        if self.sadd_fail:
            raise RedisError("sadd failed")
        self.sets.setdefault(key, set()).add(member)
        return 1

    async def srem(self, key: str, member: str) -> int:
        self.sets.setdefault(key, set()).discard(member)
        return 1

    async def delete(self, key: str) -> int:
        if self.delete_fail:
            raise RedisError("delete failed")
        self.values.pop(key, None)
        return 1

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))


def _client_with(handler: Any) -> CarlaManagementClient:
    client = CarlaManagementClient(
        "http://carla",
        create_path="/instances",
        query_path="/instances",
        scenario_path="/instances/{sim_instance_id}/scenario",
        timeout_seconds=1.0,
        max_retries=2,
    )
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.asyncio
async def test_create_instance_is_not_retried_on_connection_error() -> None:
    """创建实例非幂等：连接失败只尝试一次，避免重复建实例。"""
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        raise httpx.ConnectError("boom", request=request)

    client = _client_with(handler)
    with pytest.raises(ServiceUnavailableError):
        await client.create_instance(
            scene_id="scene-1", scene_name="s", scene_config={}, sim_id="sim-1"
        )
    assert calls == ["POST"]
    await client.close()


@pytest.mark.asyncio
async def test_get_request_is_retried() -> None:
    """查询幂等：连接失败按 max_retries 重试。"""
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        raise httpx.ConnectError("boom", request=request)

    client = _client_with(handler)
    with pytest.raises(ServiceUnavailableError):
        await client.find_active_instance("scene-1")
    assert len(calls) == 3  # max_retries=2 → 3 次
    await client.close()


@pytest.mark.asyncio
async def test_sim_id_mismatch_error_carries_instance_id() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"instance_id": "inst-1", "sim_id": "other"})

    client = _client_with(handler)
    with pytest.raises(CarlaSimIdMismatchError) as exc_info:
        await client.create_instance(
            scene_id="scene-1", scene_name="s", scene_config={}, sim_id="sim-1"
        )
    assert exc_info.value.instance_id == "inst-1"
    await client.close()


@pytest.mark.asyncio
async def test_vil_session_acquire_rolls_back_when_sadd_fails() -> None:
    redis = _Redis()
    store = VilSessionStore(redis, ttl_seconds=60)
    redis.sadd_fail = True
    with pytest.raises(ServiceUnavailableError):
        await store.acquire({"vehicle_id": "HUNTER-001", "sim_id": "sim-1"})
    # 会话键必须回滚，否则该车辆被无主会话阻塞到 TTL
    assert await store.get("HUNTER-001") is None


@pytest.mark.asyncio
async def test_vil_session_release_still_removes_active_member_when_delete_fails() -> None:
    redis = _Redis()
    store = VilSessionStore(redis, ttl_seconds=60)
    await store.acquire({"vehicle_id": "HUNTER-001", "sim_id": "sim-1"})
    redis.delete_fail = True
    with pytest.raises(ServiceUnavailableError):
        await store.release("HUNTER-001")
    assert redis.sets["vil:active"] == set()


@pytest.mark.asyncio
async def test_run_scene_stops_orphan_when_sim_id_mismatch(
    client: Any, scene_env: Any
) -> None:
    model = make_scene_model(scene_name="sim_id 不一致", status=SceneStatus.PUBLISHED)
    scene_env.repository.rows[model.scene_id] = model

    async def mismatch(**kwargs: Any) -> None:
        raise CarlaSimIdMismatchError(
            instance_id="inst-orphan",
            scene_id=str(model.scene_id),
            platform_sim_id=str(kwargs["sim_id"]),
            carla_sim_id="other",
        )

    scene_env.carla.create_instance = mismatch
    resp = await client.post(f"{SCENE_BASE}/{model.scene_id}/run", headers=ADMIN_HEADERS)
    assert resp.status_code == 503
    assert ("stop", "inst-orphan") in scene_env.carla.controls
    assert scene_env.topic_provisioner.deleted  # 已建 Topic 回滚删除


@pytest.mark.asyncio
async def test_get_progress_terminal_finalizes_vil_session(
    client: Any, scene_env: Any
) -> None:
    await scene_env.vil_sessions.bind_instance(
        vehicle_id="HUNTER-001",
        sim_instance_id="inst-1",
        sim_id="sim-1",
        scene_id="scene-1",
        mode="vil",
        vil_state_topic="carla.sim-1.vil_state",
        sensor_data_topic="carla.sim-1.sensor_data",
    )
    scene_env.carla.progress["inst-1"] = {"status": "COMPLETED"}
    resp = await client.get(f"{SCENE_BASE}/simulations/inst-1", headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    await asyncio.sleep(0)  # 延迟删除任务 delay=0
    assert await scene_env.vil_sessions.get("HUNTER-001") is None
    assert "sim-1" in scene_env.topic_provisioner.deleted


@pytest.mark.asyncio
async def test_reconcile_releases_session_for_missing_carla_instance(scene_env: Any) -> None:
    assert await scene_env.vil_sessions.acquire(
        {"vehicle_id": "HUNTER-001", "sim_id": "sim-1", "mode": "vil"}
    )
    await scene_env.vil_sessions.bind_instance(
        vehicle_id="HUNTER-001",
        sim_instance_id="inst-missing",
        sim_id="sim-1",
        scene_id="scene-1",
        mode="vil",
        vil_state_topic="carla.sim-1.vil_state",
        sensor_data_topic="carla.sim-1.sensor_data",
    )
    # FakeCarlaClient.get_instance 未登记 → 404（ResourceNotFoundError）
    cleaned = await scene_env.simulation_service.reconcile_sessions()
    assert cleaned == 1
    assert await scene_env.vil_sessions.get("HUNTER-001") is None
    await asyncio.sleep(0)
    assert "sim-1" in scene_env.topic_provisioner.deleted


@pytest.mark.asyncio
async def test_terminal_progress_retries_release_after_redis_failure(
    client: Any, scene_env: Any
) -> None:
    """释放失败不得写入去重标记：下一次轮询必须重试并最终释放（V1.23.4）。"""
    assert await scene_env.vil_sessions.acquire(
        {"vehicle_id": "HUNTER-001", "sim_id": "sim-1", "mode": "vil"}
    )
    await scene_env.vil_sessions.bind_instance(
        vehicle_id="HUNTER-001",
        sim_instance_id="inst-1",
        sim_id="sim-1",
        scene_id="scene-1",
        mode="vil",
        vil_state_topic="carla.sim-1.vil_state",
        sensor_data_topic="carla.sim-1.sensor_data",
    )
    scene_env.carla.progress["inst-1"] = {"status": "COMPLETED"}

    original_release = scene_env.vil_sessions.release
    calls = {"n": 0}

    async def flaky_release(*args: Any, **kwargs: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ServiceUnavailableError("redis down")
        await original_release(*args, **kwargs)

    scene_env.vil_sessions.release = flaky_release

    first = await client.get(f"{SCENE_BASE}/simulations/inst-1", headers=ADMIN_HEADERS)
    assert first.status_code == 200
    # 第一次失败：会话保留，Topic 不应被提前删除
    assert await scene_env.vil_sessions.get("HUNTER-001") is not None
    assert "sim-1" not in scene_env.topic_provisioner.deleted

    second = await client.get(f"{SCENE_BASE}/simulations/inst-1", headers=ADMIN_HEADERS)
    assert second.status_code == 200
    assert calls["n"] == 2
    assert await scene_env.vil_sessions.get("HUNTER-001") is None
    await asyncio.sleep(0)
    assert "sim-1" in scene_env.topic_provisioner.deleted
