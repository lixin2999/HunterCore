"""data-collector 服务层测试替身（无外部基础设施依赖）。

- :class:`FakeMinioStorage`：内存对象存储；**真实计算 MD5/SHA-256**（供完整性校验用例），
  并支持伪造 ETag（模拟 multipart 对象的 ``-N`` 后缀）以证明校验不再依赖 ETag；
- :class:`FakeSensorFileProducer`：记录投递载荷，可注入失败（验证 5001 不被吞掉）；
- :class:`FakeEventRepository` / :class:`FakeTelemetryRepository`：仓储协议内存实现；
- :class:`FakeRedisHash`：``vehicle:status`` / ``vehicle:online:set`` 读模型替身；
- :class:`FakeTelemetryIngestRepository` / :class:`FakePipelineProducer`：采集链路替身。

设计原则：替身只实现被服务层调用的方法签名（接口兼容），与 ``scene-service/app/tests/fakes.py``
的既有做法一致。
"""
from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from hunter_common.database.enums import EventLevel, EventType
from hunter_common.database.models import Event

__all__ = [
    "FakeEventRepository",
    "FakeMinioStorage",
    "FakePipelineProducer",
    "FakeRedisHash",
    "FakeSensorFileProducer",
    "FakeTelemetryIngestRepository",
    "FakeTelemetryRepository",
    "make_event",
]


class FakeMinioStorage:
    """内存对象存储替身（MinioStorage 接口子集）。

    对象内容以 ``bytes`` 保存，``stream_hashes`` 真实计算摘要，
    因此完整性校验用例具备真实判别力（而非「永远返回声明值」）。
    """

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        #: 对象键 → 伪造 ETag（不设置时按 md5 生成）
        self.etag_overrides: dict[tuple[str, str], str] = {}
        self.multipart_sessions: dict[str, list[dict[str, Any]]] = {}
        self.aborted_sessions: list[tuple[str, str, str]] = []
        self.list_calls: list[dict[str, Any]] = []
        self._upload_seq = 0

    # ---------- 测试辅助 ----------
    def put(self, bucket: str, key: str, data: bytes) -> None:
        """写入对象内容（模拟车端直传完成）。"""
        self.objects[(bucket, key)] = data

    def hashes_of(self, bucket: str, key: str) -> tuple[int, str, str]:
        """返回对象真实 (size, md5, sha256)，供用例构造正确声明值。"""
        data = self.objects[(bucket, key)]
        return len(data), hashlib.md5(data).hexdigest(), hashlib.sha256(data).hexdigest()

    def put_part(self, upload_id: str, content: bytes) -> None:
        """模拟某分片已直传成功（供 multipart 合并用例）。"""
        self.multipart_sessions.setdefault(upload_id, []).append({"content": content})

    # ---------- MinioStorage 接口 ----------
    def presign_put(self, bucket: str, key: str, content_type: str | None = None) -> str:
        return f"https://minio.test/{bucket}/{key}?method=put&ct={content_type or '-'}"

    def presign_upload_part(
        self, bucket: str, key: str, upload_id: str, part_number: int
    ) -> str:
        return f"https://minio.test/{bucket}/{key}?uploadId={upload_id}&partNumber={part_number}"

    def create_multipart_upload(self, bucket: str, key: str, content_type: str | None) -> str:
        self._upload_seq += 1
        upload_id = f"upload-{self._upload_seq}"
        self.multipart_sessions[upload_id] = []
        return upload_id

    def presign_get(self, bucket: str, key: str) -> str:
        return f"https://minio.test/{bucket}/{key}?X-Amz-Expires=900"

    def head_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        data = self.objects.get((bucket, key))
        if data is None:
            return None
        etag = self.etag_overrides.get((bucket, key)) or hashlib.md5(data).hexdigest()
        return {
            "size_bytes": len(data),
            "etag": etag,
            "last_modified": 1724035200.0,
            "content_type": "application/octet-stream",
        }

    def stream_hashes(self, bucket: str, key: str) -> tuple[int, str, str]:
        """真实摘要（内存实现整体计算，语义与 1 MiB 分块流式一致）。"""
        data = self.objects[(bucket, key)]
        return len(data), hashlib.md5(data).hexdigest(), hashlib.sha256(data).hexdigest()

    def list_objects(
        self, bucket: str, prefix: str, max_keys: int, marker: str | None = None
    ) -> tuple[list[dict[str, Any]], bool, str | None]:
        self.list_calls.append(
            {"bucket": bucket, "prefix": prefix, "max_keys": max_keys, "marker": marker}
        )
        keys = sorted(key for (obj_bucket, key) in self.objects if obj_bucket == bucket)
        candidates = [
            key for key in keys if key.startswith(prefix) and (marker is None or key > marker)
        ]
        page = candidates[:max_keys]
        truncated = len(candidates) > len(page)
        items = [
            {
                "object_key": key,
                "size_bytes": len(self.objects[(bucket, key)]),
                "etag": hashlib.md5(self.objects[(bucket, key)]).hexdigest(),
                "last_modified": 1724035200.0,
            }
            for key in page
        ]
        next_marker = page[-1] if truncated and page else None
        return items, truncated, next_marker

    def complete_multipart_upload(
        self, bucket: str, key: str, upload_id: str, parts: list[dict[str, Any]]
    ) -> None:
        """合并分片：按上传顺序拼接分片内容（模拟 S3 语义）。"""
        session = self.multipart_sessions.get(upload_id)
        if session is None:
            raise RuntimeError(f"未知 upload_id={upload_id}")
        self.objects[(bucket, key)] = b"".join(part["content"] for part in session)

    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        self.multipart_sessions.pop(upload_id, None)
        self.aborted_sessions.append((bucket, key, upload_id))


class FakeSensorFileProducer:
    """sensor_file 生产者替身（记录载荷；可注入失败验证 5001 传播）。"""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.fail_with: Exception | None = None

    async def publish(self, **payload: Any) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.published.append(payload)


class FakeTelemetryRepository:
    """遥测仓储替身：返回预置行（鸭子类型：仅需属性访问）。"""

    def __init__(self, rows: list[Any] | None = None, total: int = 0) -> None:
        self.rows = rows or []
        self.total = total
        self.calls: list[dict[str, Any]] = []

    async def query_page(
        self,
        vehicle_id: str,
        start_time: float,
        end_time: float,
        page: int,
        page_size: int,
    ) -> tuple[list[Any], int]:
        self.calls.append(
            {
                "vehicle_id": vehicle_id,
                "start_time": start_time,
                "end_time": end_time,
                "page": page,
                "page_size": page_size,
            }
        )
        return list(self.rows), self.total


class FakeEventRepository:
    """事件仓储替身（list/get/acknowledge 语义与真实仓储一致）。"""

    def __init__(self, events: list[Event] | None = None) -> None:
        self.events: dict[int, Event] = {event.event_id: event for event in events or []}
        self.list_calls: list[dict[str, Any]] = []

    async def list_events(
        self,
        vehicle_id: str | None = None,
        event_type: EventType | None = None,
        event_level: EventLevel | None = None,
        acknowledged: bool | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Event], int]:
        self.list_calls.append(
            {
                "vehicle_id": vehicle_id,
                "event_type": event_type,
                "event_level": event_level,
                "acknowledged": acknowledged,
                "page": page,
                "page_size": page_size,
            }
        )
        items = list(self.events.values())
        return items, len(items)

    async def get_by_id(self, event_id: int) -> Event | None:
        return self.events.get(event_id)

    async def acknowledge(self, event_id: int, user_id: str) -> Event | None:
        """幂等确认：已确认时保留首次确认人与时间（与真实仓储一致）。"""
        event = self.events.get(event_id)
        if event is None:
            return None
        if not event.acknowledged:
            event.acknowledged = True
            event.acknowledged_by = UUID(user_id)
            event.acknowledge_time = datetime.fromtimestamp(1724035200.0, tz=UTC)
        return event


def make_event(
    *,
    event_id: int,
    vehicle_id: str = "HUNTER-001",
    event_type: EventType = EventType.HARSH_BRAKING,
    event_level: EventLevel = EventLevel.WARNING,
    data_file_url: str | None = None,
    acknowledged: bool = False,
) -> Event:
    """构造 events 行（不落库，仅内存对象）。"""
    return Event(
        event_id=event_id,
        vehicle_id=vehicle_id,
        event_type=event_type,
        event_level=event_level,
        event_time=datetime.fromtimestamp(1724035200.0, tz=UTC),
        description="紧急制动",
        data_json={"deceleration": 3.4},
        data_file_url=data_file_url,
        acknowledged=acknowledged,
        acknowledged_by=None,
        acknowledge_time=None,
    )


class FakeRedisHash:
    """Redis 读模型替身（Hash + Set + scan_iter，供 vehicle:status 用例）。"""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}

    async def hset(self, name: str, *, mapping: dict[str, Any]) -> int:
        bucket = self.hashes.setdefault(name, {})
        for key, value in mapping.items():
            bucket[str(key)] = str(value)
        return len(mapping)

    async def sadd(self, name: str, *values: str) -> int:
        bucket = self.sets.setdefault(name, set())
        before = len(bucket)
        bucket.update(values)
        return len(bucket) - before

    async def srem(self, name: str, *values: str) -> int:
        bucket = self.sets.setdefault(name, set())
        before = len(bucket)
        bucket.difference_update(values)
        return before - len(bucket)

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    async def scan_iter(self, *, match: str, count: int = 100) -> AsyncIterator[str]:
        del count  # 替身无需分批
        prefix = match.rstrip("*")
        for key in list(self.hashes):
            if key.startswith(prefix):
                yield key


class _FakeSession:
    """伪 DB 会话（仅承载事务上下文语义）。"""


class FakeTelemetryIngestRepository:
    """采集入库仓储替身（事务上下文 + 批量幂等写入记录）。"""

    def __init__(self, *, fail_on_insert: bool = False, inserted: int | None = None) -> None:
        self.rows: list[dict[str, Any]] = []
        self.commits = 0
        self.fail_on_insert = fail_on_insert
        self._inserted_override = inserted

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_FakeSession]:
        """事务上下文：正常退出计一次提交（语义与 DatabaseSessionManager.session 一致）。"""
        session = _FakeSession()
        yield session
        self.commits += 1

    async def insert_points(self, session: Any, rows: Sequence[Mapping[str, Any]]) -> int:
        """批量写入（可注入失败，验证「写库失败 → 不提交 offset」链路）。"""
        if self.fail_on_insert:
            raise RuntimeError("database unavailable")
        self.rows.extend(dict(row) for row in rows)
        if self._inserted_override is not None:
            return self._inserted_override
        return len(rows)


class FakePipelineProducer:
    """内部 Topic 生产者替身（记录 raw/clean/event_raw 投递）。"""

    def __init__(self, *, fail_with: Exception | None = None) -> None:
        self.telemetry: list[tuple[str, dict[str, Any], str]] = []
        self.events: list[tuple[dict[str, Any], str]] = []
        self.fail_with = fail_with

    async def publish_telemetry(
        self, topic: str, payload: Mapping[str, Any], vehicle_id: str
    ) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.telemetry.append((topic, dict(payload), vehicle_id))

    async def publish_event(self, payload: Mapping[str, Any], vehicle_id: str) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.events.append((dict(payload), vehicle_id))


class FakeEventIngestRepository:
    """事件落库替身（transaction + insert_events）。"""

    def __init__(self, *, fail_on_insert: bool = False) -> None:
        self.rows: list[dict[str, Any]] = []
        self.commits = 0
        self.fail_on_insert = fail_on_insert

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_FakeSession]:
        session = _FakeSession()
        yield session
        self.commits += 1

    async def insert_events(self, session: Any, rows: Sequence[Mapping[str, Any]]) -> int:
        if self.fail_on_insert:
            raise RuntimeError("database unavailable")
        self.rows.extend(dict(row) for row in rows)
        return len(rows)
