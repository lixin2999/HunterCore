"""ota-service 测试夹具（无外部基础设施依赖）。

- FakeRedis：RedisManager 兼容最小实现（限流 + ota:progress Hash + vehicle 读模型）；
- FakePackageStorage：MinIO 兼容实现（内存对象 + 确定性预签名 URL + 流式哈希）；
- Fake*Repository：仓储协议内存实现（与服务层真实仓储方法签名一致）；
- FakeNotifyProducer / FakeCommandProducer：Kafka 生产者替身（记录投递清单）；
- 服务实例经 ``app.state`` 注入（路由经 Depends 读取，与 lifespan 装配结构一致）；
- 附录 D 限流阈值在夹具中放大（避免测试用例互相触发 429）。
"""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from hunter_common.database.enums import (
    OtaStatus,
    OtaTaskStatus,
    OtaVersionStatus,
)
from hunter_common.database.models import OtaRecord, OtaTask, OtaVersion

from app.config import settings
from app.main import app
from app.repositories.records import RecordSnapshot
from app.services.gates import VehicleStateReader
from app.services.records import RecordService
from app.services.rollout import canonical_strategy
from app.services.tasks import TaskService
from app.services.versions import VersionService

ADMIN_HEADERS = {"X-User-Id": "11111111-1111-4111-8111-111111111111", "X-Roles": "admin"}
VIEWER_HEADERS = {"X-User-Id": "22222222-2222-4222-8222-222222222222", "X-Roles": "viewer"}


class FakeRedis:
    """RedisManager 兼容最小实现（限流 + ota:progress Hash + vehicle 读模型）。

    ``client`` 返回自身（RedisManager.client 暴露底层客户端；
    本服务经 client.hset/hgetall/sismember 访问读模型与进度缓存）。
    """

    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.ttls: dict[str, int] = {}

    @property
    def client(self) -> "FakeRedis":
        return self

    async def get(self, key: str) -> str | None:
        value = self.store.get(key)
        return value if isinstance(value, str) else None

    async def set(self, key: str, value: str, *, expire_seconds: int | None = None) -> None:
        self.store[key] = value
        if expire_seconds is not None:
            self.ttls[key] = expire_seconds

    async def incr_with_expire(self, key: str, expire_seconds: int = 60) -> int:
        count = int(self.store.get(key, 0)) + 1
        if key not in self.store:
            self.ttls[key] = expire_seconds
        self.store[key] = str(count)
        return count

    async def hset(self, key: str, mapping: dict[str, str] | None = None, **kwargs: Any) -> int:
        hash_map: dict[str, str] = self.store.setdefault(key, {})
        hash_map.update(mapping or {})
        hash_map.update(kwargs)
        return len(mapping or {})

    async def hgetall(self, key: str) -> dict[str, str] | None:
        value = self.store.get(key)
        return dict(value) if isinstance(value, dict) else None

    async def expire(self, key: str, seconds: int) -> bool:
        self.ttls[key] = seconds
        return True

    async def sismember(self, key: str, member: str) -> bool:
        members = self.store.get(key)
        return isinstance(members, set) and member in members

    async def ping(self) -> bool:
        return True


class FakePackageStorage:
    """MinIO 兼容实现（内存对象；stream_hashes 单次流式计算 size/md5/sha256）。"""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, content: bytes) -> None:
        self.objects[key] = content

    def presign_put(self, bucket: str, key: str) -> str:
        return f"https://minio.test/{bucket}/{key}?X-Amz-Algorithm=PUT"

    def presign_upload_part(self, bucket: str, key: str, upload_id: str, part_number: int) -> str:
        return f"https://minio.test/{bucket}/{key}?uploadId={upload_id}&partNumber={part_number}"

    def create_multipart_upload(self, bucket: str, key: str) -> str:
        return str(uuid4())

    def presign_get(self, bucket: str, key: str) -> str:
        return f"https://minio.test/{bucket}/{key}?X-Amz-Expires=900"

    def head_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        content = self.objects.get(key)
        if content is None:
            return None
        return {"size_bytes": len(content), "etag": hashlib.md5(content).hexdigest()}

    def head_bucket(self, bucket: str) -> bool:
        return True

    def stream_hashes(self, bucket: str, key: str) -> tuple[int, str, str]:
        """同步实现（与真实 OtaPackageStorage.stream_hashes 一致，经 to_thread 调用）。"""
        content = self.objects.get(key)
        if content is None:
            raise KeyError(key)
        return (
            len(content),
            hashlib.md5(content).hexdigest(),
            hashlib.sha256(content).hexdigest(),
        )


class _FakeSession:
    """内存会话替身（服务层 commit()/add() 均为空操作，数据即时落在仓储 dict）。"""

    async def commit(self) -> None:
        return None

    def add(self, obj: Any) -> None:
        return None


class _FakeTransaction:
    """asynccontextmanager 兼容的空事务（内存仓储无需真实事务）。"""

    async def __aenter__(self) -> _FakeSession:
        return _FakeSession()

    async def __aexit__(self, *exc: object) -> None:
        return None


class FakeVersionRepository:
    """ota_versions 仓储内存实现（协议与 OtaVersionRepository 一致）。"""

    def __init__(self) -> None:
        self.rows: dict[UUID, OtaVersion] = {}
        self._tasks: dict[UUID, OtaTask] = {}

    def bind_tasks(self, tasks: dict[UUID, OtaTask]) -> None:
        self._tasks = tasks

    async def list_page(self, **kwargs: Any) -> tuple[list[OtaVersion], int]:
        rows = list(self.rows.values())
        if kwargs.get("status") is not None:
            rows = [r for r in rows if r.status == kwargs["status"]]
        if kwargs.get("release_type") is not None:
            rows = [r for r in rows if r.release_type == kwargs["release_type"]]
        if kwargs.get("version_code") is not None:
            rows = [r for r in rows if r.version_code == kwargs["version_code"]]
        if kwargs.get("applicable_model") is not None:
            rows = [r for r in rows if kwargs["applicable_model"] in r.applicable_models]
        if kwargs.get("version_name"):
            rows = [r for r in rows if kwargs["version_name"] in r.version_name]
        rows.sort(
            key=lambda r: (
                r.release_time.timestamp() if r.release_time else 0.0,
                r.version_code,
            ),
            reverse=True,
        )
        page, size = kwargs.get("page", 1), kwargs.get("page_size", 20)
        return rows[(page - 1) * size : page * size], len(rows)

    async def get(self, version_id: UUID) -> OtaVersion | None:
        return self.rows.get(version_id)

    async def get_by_name(self, version_name: str) -> OtaVersion | None:
        return next((r for r in self.rows.values() if r.version_name == version_name), None)

    async def get_by_code(self, version_code: int) -> OtaVersion | None:
        return next((r for r in self.rows.values() if r.version_code == version_code), None)

    async def max_published_code(self, models: list[str]) -> int | None:
        published = [
            r
            for r in self.rows.values()
            if r.status == OtaVersionStatus.PUBLISHED and set(r.applicable_models) & set(models)
        ]
        return max((r.version_code for r in published), default=None)

    async def create(self, version: OtaVersion) -> OtaVersion:
        if version.version_id is None:
            version.version_id = uuid4()  # server_default gen_random_uuid() 等价物
        self.rows[version.version_id] = version
        return version

    async def set_status(
        self,
        version_id: UUID,
        *,
        status: OtaVersionStatus,
        release_time: float | None = None,
    ) -> None:
        row = self.rows.get(version_id)
        if row is not None:
            row.status = status
            if release_time is not None:
                row.release_time = datetime.fromtimestamp(release_time, tz=timezone.utc)

    async def count_tasks_for_version(self, version_id: UUID) -> int:
        return sum(1 for t in self._tasks.values() if t.target_version_id == version_id)

    async def count_active_tasks_for_version(self, version_id: UUID) -> int:
        return sum(
            1
            for t in self._tasks.values()
            if t.target_version_id == version_id
            and t.status in (OtaTaskStatus.RUNNING, OtaTaskStatus.PAUSED)
        )


class FakeTaskRepository:
    """ota_tasks 仓储内存实现（get_for_update 返回同一实例，改动即持久化）。"""

    def __init__(self) -> None:
        self.rows: dict[UUID, OtaTask] = {}

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def get_for_update(self, session: Any, task_id: UUID) -> OtaTask | None:
        return self.rows.get(task_id)

    async def get(self, task_id: UUID) -> OtaTask | None:
        return self.rows.get(task_id)

    async def list_page(self, **kwargs: Any) -> tuple[list[OtaTask], int]:
        rows = list(self.rows.values())
        if kwargs.get("status") is not None:
            rows = [t for t in rows if t.status == kwargs["status"]]
        if kwargs.get("target_version_id") is not None:
            rows = [t for t in rows if t.target_version_id == kwargs["target_version_id"]]
        if kwargs.get("vehicle_id") is not None:
            rows = [t for t in rows if kwargs["vehicle_id"] in t.target_vehicles]
        if kwargs.get("creator") is not None:
            rows = [t for t in rows if t.creator == kwargs["creator"]]
        rows.sort(key=lambda t: t.create_time, reverse=True)
        page, size = kwargs.get("page", 1), kwargs.get("page_size", 20)
        return rows[(page - 1) * size : page * size], len(rows)

    async def create(self, session: Any, task: OtaTask) -> OtaTask:
        task.task_id = uuid4()
        task.create_time = datetime.now(tz=timezone.utc)
        self.rows[task.task_id] = task
        return task

    async def save(self, session: Any, task: OtaTask) -> None:
        self.rows[task.task_id] = task


class FakeRecordRepository:
    """ota_records 仓储内存实现（唯一索引 (task_id, vehicle_id) 幂等语义）。"""

    def __init__(self) -> None:
        self.rows: dict[UUID, list[OtaRecord]] = {}
        self._next_id = 1

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    def _bucket(self, task_id: UUID) -> list[OtaRecord]:
        return self.rows.setdefault(task_id, [])

    async def insert_released(self, session: Any, rows: list[dict[str, Any]]) -> int:
        inserted = 0
        for row in rows:
            bucket = self._bucket(row["task_id"])
            if any(r.vehicle_id == row["vehicle_id"] for r in bucket):
                continue  # ON CONFLICT DO NOTHING（唯一索引 uq_ota_records_task_vehicle）
            record = OtaRecord(
                record_id=self._next_id,
                start_time=datetime.now(tz=timezone.utc),
                **row,
            )
            self._next_id += 1
            bucket.append(record)
            inserted += 1
        return inserted

    async def map_by_vehicles(
        self, session: Any, task_id: UUID, vehicle_ids: list[str]
    ) -> dict[str, OtaRecord]:
        bucket = self.rows.get(task_id, [])
        return {r.vehicle_id: r for r in bucket if r.vehicle_id in vehicle_ids}

    async def get(self, session: Any, task_id: UUID, vehicle_id: str) -> OtaRecord | None:
        bucket = self.rows.get(task_id, [])
        return next((r for r in bucket if r.vehicle_id == vehicle_id), None)

    async def list_by_task(self, **kwargs: Any) -> tuple[list[OtaRecord], int]:
        rows = list(self.rows.get(kwargs["task_id"], []))
        if kwargs.get("vehicle_id"):
            rows = [r for r in rows if r.vehicle_id == kwargs["vehicle_id"]]
        if kwargs.get("status") is not None:
            rows = [r for r in rows if r.status == kwargs["status"]]
        rows.sort(key=lambda r: (r.start_time, r.record_id), reverse=True)
        page, size = kwargs.get("page", 1), kwargs.get("page_size", 20)
        return rows[(page - 1) * size : page * size], len(rows)

    async def list_by_vehicle(self, **kwargs: Any) -> tuple[list[OtaRecord], int]:
        rows = [
            r
            for bucket in self.rows.values()
            for r in bucket
            if r.vehicle_id == kwargs["vehicle_id"]
        ]
        if kwargs.get("status") is not None:
            rows = [r for r in rows if r.status == kwargs["status"]]
        rows.sort(key=lambda r: (r.start_time, r.record_id), reverse=True)
        page, size = kwargs.get("page", 1), kwargs.get("page_size", 20)
        return rows[(page - 1) * size : page * size], len(rows)

    async def snapshot_by_task(self, task_id: UUID) -> dict[str, RecordSnapshot]:
        return {
            r.vehicle_id: RecordSnapshot(
                vehicle_id=r.vehicle_id,
                status=r.status,
                progress=r.progress,
                start_time=r.start_time.timestamp(),
                end_time=r.end_time.timestamp() if r.end_time else None,
            )
            for r in self.rows.get(task_id, [])
        }

    async def apply_status(self, session: Any, **kwargs: Any) -> bool:
        record = await self.get(session, kwargs["task_id"], kwargs["vehicle_id"])
        if record is None:
            return False
        unchanged = (
            record.status == kwargs["status"] and record.progress == kwargs["progress"]
        )
        if unchanged:
            return False
        record.status = kwargs["status"]
        if kwargs.get("phase") is not None:
            record.phase = kwargs["phase"]
        record.progress = kwargs["progress"]
        record.error_code = kwargs.get("error_code")
        record.error_message = kwargs.get("error_message")
        if kwargs["status"] in OTA_TERMINAL:
            record.end_time = datetime.fromtimestamp(kwargs["event_time"], tz=timezone.utc)
        return True


OTA_TERMINAL = frozenset({OtaStatus.SUCCESS, OtaStatus.ROLLED_BACK, OtaStatus.FAILED})


class FakeNotifyProducer:
    """ota_notify 生产者替身（记录投递清单；载荷校验交由业务层）。"""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send(self, **payload: Any) -> None:
        self.sent.append(payload)


class FakeCommandProducer:
    """rollback command 生产者替身（记录投递清单，确定性 command_id）。"""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_rollback(self, **payload: Any) -> UUID:
        self.sent.append(payload)
        return uuid4()


class FakeVehicleReader:
    """VehicleStateReader 内存实现（vehicle:status 读模型 + 在线集合）。"""

    def __init__(self) -> None:
        self.status: dict[str, dict[str, Any] | None] = {}
        self.online: set[str] = set()

    def seed(self, vehicle_id: str, **fields: Any) -> None:
        self.status[vehicle_id] = fields
        self.online.add(vehicle_id)

    async def get_status(self, vehicle_id: str) -> dict[str, Any] | None:
        return self.status.get(vehicle_id)

    async def is_online(self, vehicle_id: str) -> bool:
        return vehicle_id in self.online


# ---------- 数据工厂 ----------

PACKAGE_BYTES = b"fake-ota-package-bytes" * 8
DEFAULT_MD5 = hashlib.md5(PACKAGE_BYTES).hexdigest()
DEFAULT_SHA256 = hashlib.sha256(PACKAGE_BYTES).hexdigest()
DEFAULT_SIGNATURE = "MEQCIF9kRSA2048BASE64SIGNATURE"


def make_version(
    *,
    status: OtaVersionStatus | str = OtaVersionStatus.PUBLISHED,
    code: int = 10200,
    name: str = "V1.2.0",
) -> OtaVersion:
    """构造 ota_versions 行（package_url 指向 FakePackageStorage 对象键；status 强转枚举）。"""
    object_key = f"hunter-edge/ota/HUNTER_SE/{name}/{code}/package.tar.gz"
    return OtaVersion(
        version_id=uuid4(),
        version_name=name,
        version_code=code,
        release_type="formal",
        package_url=f"s3://{settings.minio_bucket_ota_packages}/{object_key}",
        package_size=len(PACKAGE_BYTES),
        package_md5=DEFAULT_MD5,
        package_sha256=DEFAULT_SHA256,
        signature=DEFAULT_SIGNATURE,
        changelog={"features": ["感知模型升级至 v2.1"]},
        applicable_models=["HUNTER_SE"],
        status=OtaVersionStatus(status),
        release_time=datetime.now(tz=timezone.utc) if status == "published" else None,
    )


def make_task(
    *,
    target_version_id: UUID,
    target_vehicles: list[str],
    status: OtaTaskStatus | str = OtaTaskStatus.CREATED,
    creator: UUID = UUID("11111111-1111-4111-8111-111111111111"),
) -> OtaTask:
    """构造 ota_tasks 行（冻结规范灰度策略 + 默认门禁/调度；status 强转枚举）。"""
    from app.schemas.tasks import OtaTaskPreconditions, OtaTaskProgress, OtaTaskSchedule

    return OtaTask(
        task_id=uuid4(),
        task_name="V1.2.0 灰度升级（首批 5%）",
        target_version_id=target_version_id,
        target_vehicles=target_vehicles,
        upgrade_strategy=canonical_strategy(settings).model_dump(),
        schedule=OtaTaskSchedule(mode="immediate").model_dump(),
        preconditions=OtaTaskPreconditions().model_dump(),
        status=OtaTaskStatus(status),
        progress=OtaTaskProgress(
            total=len(target_vehicles),
            pending=len(target_vehicles),
            in_progress=0,
            succeeded=0,
            failed=0,
            rolled_back=0,
            success_rate=None,
            current_batch=0,
        ).model_dump(),
        creator=creator,
        create_time=datetime.now(tz=timezone.utc),
    )


# ---------- 夹具 ----------

class _State(SimpleNamespace):
    """app.state 聚合（测试断言用）。"""


@pytest.fixture()
def ota_env(monkeypatch: pytest.MonkeyPatch) -> _State:
    """装配 app.state 全套替身服务（附录 D 限流阈值放大，避免用例间 429）。"""
    monkeypatch.setattr(settings, "version_create_rate_limit_per_min", 1000)
    monkeypatch.setattr(settings, "publish_rate_limit_per_min", 1000)

    fake_redis = FakeRedis()
    storage = FakePackageStorage()
    version_repo = FakeVersionRepository()
    task_repo = FakeTaskRepository()
    record_repo = FakeRecordRepository()
    version_repo.bind_tasks(task_repo.rows)
    reader = FakeVehicleReader()
    notify = FakeNotifyProducer()
    commands = FakeCommandProducer()

    app.state.redis = fake_redis
    app.state.storage = storage
    app.state.db = SimpleNamespace(check_connection=lambda: asyncio.sleep(0, result=True))
    app.state.version_service = VersionService(version_repo, storage, settings)
    app.state.task_service = TaskService(
        task_repo,
        version_repo,
        record_repo,
        reader,
        notify,
        commands,
        settings,
        storage,
        redis_manager=fake_redis,
    )
    app.state.record_service = RecordService(record_repo, task_repo)
    state = _State(
        redis=fake_redis,
        storage=storage,
        versions=version_repo,
        tasks=task_repo,
        records=record_repo,
        reader=reader,
        notify=notify,
        commands=commands,
    )
    try:
        yield state
    finally:
        app.state.redis = None
        app.state.storage = None
        app.state.version_service = None
        app.state.task_service = None
        app.state.record_service = None


@pytest.fixture()
async def client(ota_env: _State) -> AsyncIterator[AsyncClient]:
    """ASGI 测试客户端（raise_app_exceptions=False 使全局 500 处理器生效）。"""
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as async_client:
        yield async_client


__all__ = [
    "ADMIN_HEADERS",
    "FakeCommandProducer",
    "FakeNotifyProducer",
    "FakePackageStorage",
    "FakeRedis",
    "FakeTaskRepository",
    "FakeVehicleReader",
    "FakeVersionRepository",
    "PACKAGE_BYTES",
    "VIEWER_HEADERS",
    "client",
    "make_task",
    "make_version",
    "ota_env",
]
