"""remote-control 测试夹具（无外部基础设施依赖，模式对齐 ota-service conftest）。

- FakeRedis：RedisManager 兼容最小实现（会话 Hash/SCAN/在线集合/分布式锁/限流计数）；
- FakeVideoStorage：VideoArchiveStorage 协议内存实现（sidecar 对象 + head/预签名）；
- FakeFrameProducer / FakeCommandProducer：Kafka 生产者替身（记录投递，可注入故障）；
- 服务实例经 ``app.state`` 注入（路由经 Depends 读取，与 lifespan 装配结构一致）；
- 附录 D 创建会话限流阈值在夹具中放大（避免用例互相触发 429）。
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from hunter_common.exceptions import ServiceUnavailableError

from app.config import settings
from app.core.dependencies import OperatorContext
from app.main import app
from app.producers.remote_control import RemoteControlFrameProducer
from app.producers.session_command import SessionCommandProducer
from app.services.history_service import HistoryService
from app.services.session_service import SessionService
from app.services.vehicle_view import VehicleViewReader

ADMIN_ID = "11111111-1111-4111-8111-111111111111"
OPERATOR_ID = "22222222-2222-4222-8222-222222222222"
OTHER_ID = "33333333-3333-4333-8333-333333333333"
VIEWER_ID = "44444444-4444-4444-8444-444444444444"

ADMIN_HEADERS = {"X-User-Id": ADMIN_ID, "X-Roles": "admin"}
OPERATOR_HEADERS = {"X-User-Id": OPERATOR_ID, "X-Roles": "operator"}
OTHER_HEADERS = {"X-User-Id": OTHER_ID, "X-Roles": "operator"}
VIEWER_HEADERS = {"X-User-Id": VIEWER_ID, "X-Roles": "viewer"}

VEHICLE_ONLINE = "HUNTER-001"  # online_idle → 可创建
VEHICLE_BUSY = "HUNTER-002"  # upgrading → 4002
VEHICLE_OFFLINE = "HUNTER-003"  # 不在 online set → 4001


class FakeLock:
    """redis.asyncio.lock.Lock 最小实现（进程内互斥语义）。"""

    def __init__(self, holder: dict[str, bool], name: str) -> None:
        self._holder = holder
        self._name = name

    async def acquire(self, blocking: bool = True) -> bool:
        if self._holder.get(self._name):
            return False
        self._holder[self._name] = True
        return True

    async def release(self) -> None:
        self._holder.pop(self._name, None)


class FakeRedis:
    """RedisManager 兼容最小实现（client 返回自身；覆盖会话/读模型/锁/限流面）。"""

    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self._lock_holders: dict[str, bool] = {}

    @property
    def client(self) -> FakeRedis:
        return self

    # ---------- 会话 Hash / 读模型 ----------
    async def hset(
        self, key: str, mapping: dict[str, str] | None = None, **kwargs: Any
    ) -> int:
        hash_map: dict[str, str] = self.store.setdefault(key, {})
        hash_map.update(mapping or {})
        hash_map.update(kwargs)
        return len(mapping or {})

    async def hget(self, key: str, field: str) -> str | None:
        value = self.store.get(key)
        return value.get(field) if isinstance(value, dict) else None

    async def hgetall(self, key: str) -> dict[str, str]:
        value = self.store.get(key)
        return dict(value) if isinstance(value, dict) else {}

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if self.store.pop(key, None) is not None:
                removed += 1
        return removed

    async def scan(self, cursor: int, match: str, count: int) -> tuple[int, list[str]]:
        """简化实现：一次性返回全部匹配键（测试数据规模小，cursor 恒 0）。"""
        keys = [key for key in self.store if fnmatch.fnmatch(key, match)]
        return 0, keys

    async def smembers(self, key: str) -> set[str]:
        value = self.store.get(key)
        return set(value) if isinstance(value, (set, list)) else set()

    async def sismember(self, key: str, member: str) -> bool:
        return member in await self.smembers(key)

    async def ping(self) -> bool:
        return True

    # ---------- 限流（core.rate_limit：RedisManager.incr_with_expire） ----------
    async def incr_with_expire(self, key: str, expire_seconds: int = 60) -> int:
        count = int(self.store.get(key, 0) or 0) + 1
        self.store[key] = str(count)
        return count

    # ---------- 分布式锁（RedisManager.acquire_lock → client.lock） ----------
    async def acquire_lock(self, name: str, timeout: float = 10.0) -> FakeLock:
        return self.lock(name, timeout=timeout)

    def lock(self, name: str, timeout: float = 10.0) -> FakeLock:
        return FakeLock(self._lock_holders, name)


class FakeVideoStorage:
    """VideoArchiveStorage 协议内存实现（sidecar JSON + head/预签名）。"""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_write = False

    async def head_video(self, key: str) -> int | None:
        return len(self.objects[key]) if key in self.objects else None

    async def read_sidecar(self, key: str) -> dict[str, Any] | None:
        raw = self.objects.get(key)
        return json.loads(raw) if raw is not None else None

    async def write_sidecar(self, key: str, doc: dict[str, Any]) -> None:
        if self.fail_write:
            raise RuntimeError("storage unavailable")
        self.objects[key] = json.dumps(doc, ensure_ascii=False).encode("utf-8")

    async def list_sidecars(
        self, prefix: str, *, limit: int | None = None
    ) -> list[tuple[str, dict[str, Any]]]:
        out: list[tuple[str, dict[str, Any]]] = []
        for key in sorted(self.objects):
            if key.startswith(prefix) and key.endswith(".json"):
                out.append((key, json.loads(self.objects[key])))
                if limit is not None and len(out) >= limit:
                    break
        return out

    async def presign_get(self, key: str, expires_in: int) -> str:
        return f"https://minio.test/hunter-video/{key}?expires={expires_in}"

    async def healthcheck(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class FakeFrameProducer(RemoteControlFrameProducer):
    """RemoteControlFrameProducer 替身（记录 boot/stop 帧；fail 模拟投递失败 → 5001）。

    真实生产者在 send_* 内部将 Kafka 异常转换为 ServiceUnavailableError，
    替身保持同一契约（service 层据此回滚会话状态）。
    """

    def __init__(self) -> None:
        self.frames: list[tuple[str, str, dict[str, Any]]] = []
        self.fail = False

    async def send_boot(self, vehicle_id: str, **kwargs: Any) -> None:
        if self.fail:
            raise ServiceUnavailableError(
                message="远程操控指令通道不可用（Kafka 投递失败）"
            )
        self.frames.append(("boot", vehicle_id, kwargs))

    async def send_stop(self, vehicle_id: str, **kwargs: Any) -> None:
        if self.fail:
            raise ServiceUnavailableError(
                message="远程操控指令通道不可用（Kafka 投递失败）"
            )
        self.frames.append(("stop", vehicle_id, kwargs))


class FakeCommandProducer(SessionCommandProducer):
    """SessionCommandProducer 替身（记录 session_start/end 信令；fail 模拟投递失败）。"""

    def __init__(self) -> None:
        self.commands: list[tuple[str, str, dict[str, Any]]] = []
        self.fail = False

    async def send_session_start(self, vehicle_id: str, **kwargs: Any) -> str:
        if self.fail:
            raise ServiceUnavailableError(
                message="车辆指令通道不可用（Kafka 投递失败）"
            )
        self.commands.append(("rc_session_start", vehicle_id, kwargs))
        return "cmd-start"

    async def send_session_end(self, vehicle_id: str, **kwargs: Any) -> str:
        if self.fail:
            raise ServiceUnavailableError(
                message="车辆指令通道不可用（Kafka 投递失败）"
            )
        self.commands.append(("rc_session_end", vehicle_id, kwargs))
        return "cmd-end"


def seed_vehicle(
    redis: FakeRedis, vehicle_id: str, *, online: bool, status: str
) -> None:
    """预置车辆读模型（vehicle:online:set + vehicle:status:{vehicle_id}）。"""
    online_set = redis.store.setdefault("vehicle:online:set", set())
    if online:
        online_set.add(vehicle_id)
    else:
        online_set.discard(vehicle_id)
    redis.store[f"vehicle:status:{vehicle_id}"] = {
        "status": status,
        "battery_soc": "80",
        "velocity": "0.0",
        "last_online_time": "1724035200.0",
        "vehicle_name": f"车 {vehicle_id}",
        "model": "HUNTER_SE",
    }


def make_operator(
    user_id: str = OPERATOR_ID, roles: tuple[str, ...] = ("operator",)
) -> OperatorContext:
    """构造操作员上下文（等价网关注入头解析结果）。"""
    return OperatorContext(user_id=user_id, roles=frozenset(roles))


# ---------- 夹具 ----------
class _State(SimpleNamespace):
    """app.state 聚合（测试断言用）。"""


@pytest.fixture()
def rc_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[_State]:
    """装配 app.state 全套替身服务（附录 D 创建会话限流放大，避免用例间 429）。"""
    monkeypatch.setattr(settings, "rc_session_create_rate_limit_per_min", 1000)

    fake_redis = FakeRedis()
    storage = FakeVideoStorage()
    frame = FakeFrameProducer()
    command = FakeCommandProducer()

    seed_vehicle(fake_redis, VEHICLE_ONLINE, online=True, status="online_idle")
    seed_vehicle(fake_redis, VEHICLE_BUSY, online=True, status="upgrading")
    seed_vehicle(fake_redis, VEHICLE_OFFLINE, online=False, status="offline")

    vehicle_view = VehicleViewReader(fake_redis, settings)
    session_service = SessionService(
        redis=fake_redis,
        vehicle_view=vehicle_view,
        frame_producer=frame,
        command_producer=command,
        storage=storage,
        settings=settings,
    )
    history_service = HistoryService(storage=storage, settings=settings)

    app.state.redis = fake_redis
    app.state.storage = storage
    app.state.db = SimpleNamespace(
        check_connection=lambda: asyncio.sleep(0, result=True)
    )
    app.state.vehicle_view = vehicle_view
    app.state.frame_producer = frame
    app.state.command_producer = command
    app.state.session_service = session_service
    app.state.history_service = history_service

    state = _State(
        redis=fake_redis,
        storage=storage,
        frame=frame,
        command=command,
        session_service=session_service,
        history_service=history_service,
    )
    try:
        yield state
    finally:
        app.state.redis = None
        app.state.storage = None
        app.state.vehicle_view = None
        app.state.frame_producer = None
        app.state.command_producer = None
        app.state.session_service = None
        app.state.history_service = None


@pytest.fixture()
async def client(rc_env: _State) -> AsyncIterator[AsyncClient]:
    """ASGI 测试客户端（raise_app_exceptions=False 使全局 500 处理器生效）。"""
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as async_client:
        yield async_client


__all__ = [
    "ADMIN_HEADERS",
    "ADMIN_ID",
    "OPERATOR_HEADERS",
    "OPERATOR_ID",
    "OTHER_HEADERS",
    "OTHER_ID",
    "VEHICLE_BUSY",
    "VEHICLE_OFFLINE",
    "VEHICLE_ONLINE",
    "VIEWER_HEADERS",
    "VIEWER_ID",
    "FakeCommandProducer",
    "FakeFrameProducer",
    "FakeRedis",
    "FakeVideoStorage",
    "client",
    "make_operator",
    "rc_env",
    "seed_vehicle",
]
