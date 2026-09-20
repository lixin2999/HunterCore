"""scene-service 端点测试夹具（无外部基础设施依赖）。

- ``scene_env``：把替身仓储/缓存/存储/Carla 客户端装配进 ``app.state``（与 lifespan 结构一致），
  路由经 ``Depends`` 读取，测试可整体替换；
- ``client``：ASGI 测试客户端（``raise_app_exceptions=False`` 使全局 500 处理器生效）；
- 认证头：admin（全部动作）、operator（读写执行）、viewer（无 scene 权限 → 1002）。
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.main import app
from app.services.export import SceneExportService
from app.services.scenes import SceneService
from app.services.simulation import SceneSimulationService
from app.services.templates import SceneTemplateService
from app.tests.fakes import (
    FakeCarlaClient,
    FakeSceneCache,
    InMemorySceneRepository,
    InMemorySceneStorage,
)

ADMIN_HEADERS = {
    "X-User-Id": "11111111-1111-4111-8111-111111111111",
    "X-Roles": "admin",
}
OPERATOR_HEADERS = {
    "X-User-Id": "22222222-2222-4222-8222-222222222222",
    "X-Roles": "operator",
}
VIEWER_HEADERS = {
    "X-User-Id": "33333333-3333-4333-8333-333333333333",
    "X-Roles": "viewer",
}

#: 夹具装配的 app.state 键（用例前后清理，避免替身跨用例串扰）
STATE_KEYS: tuple[str, ...] = (
    "scene_repository",
    "scene_service",
    "template_service",
    "export_service",
    "simulation_service",
    "db",
    "redis",
    "storage",
    "carla",
)


@pytest.fixture()
def scene_env() -> Iterator[SimpleNamespace]:
    """装配 app.state 全套替身服务（业务服务构造注入替身仓储/缓存/存储/Carla）。"""
    repository = InMemorySceneRepository()
    cache = FakeSceneCache()
    storage = InMemorySceneStorage()
    carla = FakeCarlaClient()
    scene_service = SceneService(repository, cache, settings)
    app.state.scene_repository = repository
    app.state.scene_service = scene_service
    app.state.template_service = SceneTemplateService(settings)
    app.state.export_service = SceneExportService(scene_service, storage, settings)
    # G-20①：注入 storage 以便产物预签名（与 main.py lifespan 装配一致）
    app.state.simulation_service = SceneSimulationService(
        scene_service, carla, settings, storage=storage
    )
    app.state.db = SimpleNamespace(check_connection=lambda: asyncio.sleep(0, result=True))
    app.state.redis = SimpleNamespace(ping=lambda: asyncio.sleep(0, result=True))
    state = SimpleNamespace(
        repository=repository,
        cache=cache,
        storage=storage,
        carla=carla,
        service=scene_service,
    )
    try:
        yield state
    finally:
        for key in STATE_KEYS:
            if hasattr(app.state, key):
                delattr(app.state, key)


@pytest.fixture()
async def client(scene_env: SimpleNamespace) -> AsyncIterator[AsyncClient]:
    """ASGI 测试客户端（全局异常处理器生效）。"""
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as async_client:
        yield async_client


__all__ = [
    "ADMIN_HEADERS",
    "OPERATOR_HEADERS",
    "VIEWER_HEADERS",
    "client",
    "scene_env",
]
