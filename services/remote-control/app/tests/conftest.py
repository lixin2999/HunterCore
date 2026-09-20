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
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any

import jwt as pyjwt
import pytest
from httpx import ASGITransport, AsyncClient
from hunter_common.exceptions import ServiceUnavailableError

from app.config import settings
from app.core.dependencies import OperatorContext
from app.main import app
from app.producers.remote_control import RemoteControlFrameProducer
from app.producers.session_command import SessionCommandProducer
from app.services.geofence import GeofenceChecker
from app.services.history_service import HistoryService
from app.services.session_service import SessionService
from app.services.vehicle_view import VehicleViewReader
from app.services.ws_hub import WsHub

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
        self.ttl_seconds: dict[str, int] = {}
        self._lock_holders: dict[str, bool] = {}

    @property
    def client(self) -> FakeRedis:
        return self

    # ---------- 会话 Hash / 读模型 ----------
    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: str | None = None,
        mapping: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> int:
        """与 redis-py hset 语义对齐：支持 (key, field, value) 与 mapping/kwargs 两形态。"""
        hash_map: dict[str, str] = self.store.setdefault(key, {})
        updated: dict[str, str] = dict(mapping or {})
        updated.update(kwargs)
        if field is not None:
            updated[field] = str(value)
        hash_map.update(updated)
        return len(updated)

    async def hget(self, key: str, field: str) -> str | None:
        value = self.store.get(key)
        return value.get(field) if isinstance(value, dict) else None

    async def hgetall(self, key: str) -> dict[str, str]:
        value = self.store.get(key)
        return dict(value) if isinstance(value, dict) else {}

    async def hexists(self, key: str, field: str) -> bool:
        value = self.store.get(key)
        return isinstance(value, dict) and field in value

    async def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """Hash 字段原子自增（WS seq/计数；与 redis-py hincrby 语义一致）。"""
        hash_map: dict[str, str] = self.store.setdefault(key, {})
        current = int(hash_map.get(field, "0") or 0)
        hash_map[field] = str(current + amount)
        return current + amount

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

    async def scan_iter(self, match: str, count: int = 100) -> AsyncIterator[str]:
        """异步迭代匹配键（契约守护任务用；与 redis-py scan_iter 语义一致）。"""
        del count  # 替身无需分批
        for key in [key for key in self.store if fnmatch.fnmatch(key, match)]:
            yield key

    async def expire(self, key: str, seconds: int) -> bool:
        """设置键 TTL（审查 R7：会话 Hash 硬 TTL 兜底）；记录到 ttl_seconds 供用例断言。"""
        self.ttl_seconds[key] = int(seconds)
        return key in self.store

    async def ttl(self, key: str) -> int:
        """读取键 TTL（-1 = 无 TTL，-2 = 键不存在）。"""
        if key not in self.store:
            return -2
        return self.ttl_seconds.get(key, -1)

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

    async def send_control_frame(self, vehicle_id: str, **kwargs: Any) -> None:
        """WS 20Hz 控制帧替身（记录 seq/限幅后目标值；fail 模拟投递失败 → 5001）。"""
        if self.fail:
            raise ServiceUnavailableError(
                message="远程操控指令通道不可用（Kafka 投递失败）"
            )
        self.frames.append(("control", vehicle_id, kwargs))


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

    async def send_signal_relay(self, vehicle_id: str, **kwargs: Any) -> str:
        """WebRTC 信令中继替身（G-09；command_type 取值域定稿前以配置名登记）。"""
        if self.fail:
            raise ServiceUnavailableError(
                message="车辆指令通道不可用（Kafka 投递失败）"
            )
        self.commands.append((settings.rc_signal_relay_command_type, vehicle_id, kwargs))
        return "cmd-signal"


class FakeFenceRepo:
    """VehicleFenceRepository 替身（G-11；vehicle_id → fence_json 映射，默认无围栏）。

    真实 repo 在 DB 故障时抛 ServiceUnavailableError（5001），替身用 fail 开关复现。
    """

    def __init__(self) -> None:
        self.fences: dict[str, dict[str, Any]] = {}
        self.fail = False

    async def get_fence(self, vehicle_id: str) -> dict[str, Any] | None:
        if self.fail:
            raise ServiceUnavailableError(message="读取车辆围栏配置失败")
        return self.fences.get(vehicle_id)


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


def make_ws_token(
    user_id: str = OPERATOR_ID,
    roles: tuple[str, ...] = ("operator",),
    *,
    expires_in: float = 300.0,
) -> str:
    """签发 WS 握手用 Access Token（与 api-gateway 同密钥/算法/issuer，G-09 信任模型）。"""
    now = int(time.time())
    return pyjwt.encode(
        {
            "sub": user_id,
            "username": f"user-{user_id[:8]}",
            "roles": list(roles),
            "permissions": [],
            "iat": now,
            "exp": now + int(expires_in),
            "iss": settings.jwt_issuer,
            "jti": str(uuid.uuid4()),
            "typ": "access",
        },
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )


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
    # G-11 围栏门禁（默认全部车辆未配置围栏 → 行为与 G-11 前一致；用例经 state.fence_repo 配置）
    fence_repo = FakeFenceRepo()
    geofence = GeofenceChecker(
        fence_repo=fence_repo, redis=fake_redis, settings=settings
    )
    session_service = SessionService(
        redis=fake_redis,
        vehicle_view=vehicle_view,
        frame_producer=frame,
        command_producer=command,
        storage=storage,
        settings=settings,
        geofence=geofence,
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
    # WS 连接注册表（G-09：路由/command_result 消费者共用，与 lifespan 装配结构一致）
    ws_hub = WsHub()
    app.state.ws_hub = ws_hub

    state = _State(
        redis=fake_redis,
        storage=storage,
        frame=frame,
        command=command,
        session_service=session_service,
        history_service=history_service,
        ws_hub=ws_hub,
        fence_repo=fence_repo,
        geofence=geofence,
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
        app.state.ws_hub = None


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
    "make_ws_token",
    "rc_env",
    "seed_vehicle",
]
