"""data-analytics 单元测试替身（不依赖 Redis/Kafka/DB/MinIO/下游服务）。

- InMemoryObjectStorage：ObjectStorage Protocol 的内存实现（报告 sidecar / 评估文档）
- FakeFleetRepository / FakePipelineRepository / FakeEventClient / FakeVehicleClient：
  各依赖 Protocol 的可配置替身（按需返回可用数据或 None 触发降级）
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from app.repositories.pipeline import IngestLatencyReading


class InMemoryObjectStorage:
    """内存对象存储（遵循 app.repositories.storage.ObjectStorage Protocol）。"""

    def __init__(self) -> None:
        self._objects: dict[str, dict[str, Any]] = {}
        self.closed = False

    async def get_json(self, key: str) -> dict[str, Any] | None:
        return self._objects.get(key)

    async def put_json(self, key: str, payload: Mapping[str, Any]) -> None:
        self._objects[key] = json.loads(json.dumps(payload, ensure_ascii=False))

    async def list_json(self, prefix: str, *, limit: int | None = None) -> list[tuple[str, dict[str, Any]]]:
        out = [(key, doc) for key, doc in self._objects.items() if key.startswith(prefix)]
        return out[:limit] if limit is not None else out

    async def presign_get(self, key: str, expires_in: int) -> str:
        return f"https://minio.test/{key}?expires={expires_in}"

    async def healthcheck(self) -> bool:
        return True

    async def close(self) -> None:
        self.closed = True


class FakeFleetRepository:
    """FleetSource 替身：可配置在线数与 8 态分布。"""

    def __init__(self, online: int = 0, distribution: dict[str, int] | None = None) -> None:
        self.online = online
        self.distribution = distribution or {}

    async def online_count(self) -> int:
        return self.online

    async def status_distribution(self) -> dict[str, int]:
        return dict(self.distribution)


class FakePipelineRepository:
    """PipelineSource 替身：字段为 None 时触发服务层对应块降级。"""

    def __init__(
        self,
        *,
        points: int | None = 1000,
        lag: dict[str, int] | None = None,
        unknown_groups: dict[str, str] | None = None,
        dlq: dict[str, int] | None = None,
        averages: dict[str, float] | None = None,
        ingest_latency: float | None = None,
        ingest_reason: str | None = None,
        ingest_key_present: bool | None = None,
    ) -> None:
        # 字段按传入值原样保存：None 表示该数据源不可用（触发服务层降级），
        # 需要默认可用数据时由调用方显式传入。
        self.points = points
        self.lag = lag
        self.unknown_groups = unknown_groups or {}
        self.dlq = dlq
        self.averages = averages
        self.ingest_latency_p95 = ingest_latency
        #: 写端诊断主因（V1.19.21）：与 p95 同时为空即“键不存在”分支
        self.ingest_reason = ingest_reason
        self.ingest_key_present = ingest_key_present

    async def telemetry_points(self, start_ts: float, end_ts: float) -> int | None:
        return self.points

    async def algorithm_averages(self, start_ts: float, end_ts: float) -> dict[str, float] | None:
        return self.averages

    async def consumer_lag(self) -> tuple[dict[str, int], dict[str, str]] | None:
        # 探测整体不可达（None）与“读不到位点”（unknown_groups）是两条不同降级路径（V1.19.14）；
        # 第二个返回值是「组 → 成因码」（V1.21.14），替身形状必须与写方同形。
        if self.lag is None:
            return None
        return dict(self.lag), dict(self.unknown_groups)

    async def dlq_depth(self) -> dict[str, int] | None:
        return self.dlq

    async def ingest_latency(self) -> IngestLatencyReading:
        # key_present 缺省推断：有值或有 reason 就算“键在”，两者都无 = 键不存在（V1.19.21 三档分支）
        key_present = self.ingest_key_present
        if key_present is None:
            key_present = self.ingest_latency_p95 is not None or self.ingest_reason is not None
        return IngestLatencyReading(
            p95_ms=self.ingest_latency_p95,
            reason=self.ingest_reason,
            key_present=key_present,
        )

    async def close(self) -> None:
        return None


class FakeEventClient:
    """EventSource 替身：seeded 事件列表（窗口过滤 + 精确聚合在替身内模拟）。"""

    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self.events = events or []
        self.closed = False

    async def stats(
        self, *, start_time: float, end_time: float, vehicle_id: str | None = None
    ) -> dict[str, Any] | None:
        """精确聚合（与 data-collector GET /events/stats 同语义）。"""
        selected = [
            event
            for event in self.events
            if start_time <= float(event["event_time"]) < end_time
            and (vehicle_id is None or event.get("vehicle_id") == vehicle_id)
        ]
        by_type: dict[str, int] = {}
        by_level: dict[str, int] = {}
        for event in selected:
            event_type = event.get("event_type")
            if event_type:
                by_type[str(event_type)] = by_type.get(str(event_type), 0) + 1
            if not event.get("acknowledged"):
                event_level = event.get("event_level")
                if event_level:
                    by_level[str(event_level)] = by_level.get(str(event_level), 0) + 1
        return {
            "total": len(selected),
            "unacknowledged": sum(by_level.values()),
            "unacknowledged_by_level": by_level,
            "by_type": by_type,
        }

    async def close(self) -> None:
        self.closed = True


class FakeVehicleClient:
    """VehicleSource 替身：可配置存在性与车队总数（None=下游不可用）。"""

    def __init__(
        self, *, exists: set[str] | frozenset[str] | None | bool = frozenset(), total: int | None = 0
    ) -> None:
        self.exists = exists
        self.total = total
        self.closed = False

    async def vehicle_exists(self, vehicle_id: str) -> bool | None:
        if self.exists is None:
            return None
        if isinstance(self.exists, bool):
            return self.exists
        return vehicle_id in self.exists

    async def fleet_total(self) -> int | None:
        return self.total

    async def close(self) -> None:
        self.closed = True