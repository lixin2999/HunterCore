"""车辆实时状态读模型写入（``vehicle:status:{vehicle_id}`` / ``vehicle:online:set``）。

契约：``contracts/database/redis-keys.yaml``
- ``vehicle:status:{vehicle_id}``：Hash，``ttl_seconds = null``（持续更新，不设过期）；
- ``vehicle:online:set``：Set，成员 = 在线车辆；
- **唯一写方 = data-collector**（pending #2：由 health/telemetry 消费路径写入，本模块即该路径）。

读方：data-analytics 看板、ota-service 门禁、remote-control 接管判定。

⚠ 字段来源与待定稿项（redis-keys pending #1/#5，须人工确认后回填契约）：
- health 消息提供 ``status`` + ``system`` 段；telemetry 消息提供 ``battery_soc``/``velocity``；
- **``gear``（P 档判定）与 ``free_storage_mb`` 在 health/telemetry Schema 中无来源** →
  OTA 门禁的 ``vehicle_parked`` / ``storage`` 两项无法被满足（缺数据即拒绝，安全默认），
  须先定稿字段来源再补齐；**禁止在本模块伪造默认值**（否则门禁被静默绕过）；
- ``last_seen_seconds`` 由 ``last_seen_at`` 与当前时间推导（:class:`VehicleStatusSweeper`
  周期刷新并在超阈值时移出在线集合，实现「遥测中断 > 10s → 离线」约束）。
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Protocol

from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.services.vehicle_status")

#: 在线车辆集合（契约键名，不可更改）
ONLINE_SET_KEY = "vehicle:online:set"
#: 车辆状态 Hash 模式（契约键名，不可更改）
STATUS_KEY_PATTERN = "vehicle:status:{vehicle_id}"
#: 车端主动离线状态名（车辆状态定义 8 态之一，不可新增）
OFFLINE_STATUS = "offline"


class RedisHashClient(Protocol):
    """Redis 读模型所需能力（``RedisManager.client`` 或测试替身）。"""

    async def hset(self, name: str, *, mapping: dict[str, Any]) -> Any:  # pragma: no cover
        ...

    async def sadd(self, name: str, *values: str) -> Any:  # pragma: no cover
        ...

    async def srem(self, name: str, *values: str) -> Any:  # pragma: no cover
        ...

    async def scan_iter(self, *, match: str, count: int) -> Any:  # pragma: no cover
        ...

    async def hgetall(self, name: str) -> dict[str, Any]:  # pragma: no cover
        ...


def status_key(vehicle_id: str) -> str:
    """构造车辆状态键（契约模式 ``vehicle:status:{vehicle_id}``，禁止其他拼法）。"""
    return STATUS_KEY_PATTERN.format(vehicle_id=vehicle_id)


class VehicleStatusWriter:
    """读模型写入器（health / telemetry 消费路径共用）。"""

    def __init__(self, redis_client: RedisHashClient) -> None:
        self._redis = redis_client

    async def update_from_health(
        self, payload: dict[str, Any], *, now: float | None = None
    ) -> str:
        """按 health 消息更新状态 Hash 与在线集合；返回车辆状态名。

        - ``status`` 取值由契约 Schema 约束为 8 态受控词表；
        - ``offline`` → 移出在线集合（车端主动下线）；其余状态 → 加入在线集合。
        """
        vehicle_id = str(payload["vehicle_id"])
        status = str(payload["status"])
        system = payload.get("system") or {}
        timestamp = float(payload.get("timestamp") or 0.0)
        current = time.time() if now is None else now
        mapping: dict[str, Any] = {
            "vehicle_id": vehicle_id,
            "status": status,
            "updated_at": f"{current:.3f}",
            "last_seen_at": f"{current:.3f}",
            "last_seen_seconds": "0",
        }
        for field in (
            "cpu_usage",
            "gpu_usage",
            "memory_usage_mb",
            "gpu_temp",
            "cpu_temp",
            "network_rssi",
            "network_latency_ms",
        ):
            value = system.get(field)
            if value is not None:
                mapping[field] = str(value)
        if timestamp > 0:
            mapping["vehicle_ts"] = f"{timestamp:.3f}"
        await self._redis.hset(status_key(vehicle_id), mapping=mapping)
        if status == OFFLINE_STATUS:
            await self._redis.srem(ONLINE_SET_KEY, vehicle_id)
        else:
            await self._redis.sadd(ONLINE_SET_KEY, vehicle_id)
        return status

    async def update_from_telemetry(
        self, payload: dict[str, Any], *, now: float | None = None
    ) -> None:
        """按遥测消息刷新读模型实时字段（电量/速度/底盘状态字）与心跳时间。

        仅更新 Hash 与心跳，**不改动** ``status``（业务状态由 health 消息与车辆状态机决定；
        遥测中断的离线判定由 :class:`VehicleStatusSweeper` 负责）。
        """
        vehicle_id = str(payload["vehicle_id"])
        chassis = payload.get("chassis") or {}
        current = time.time() if now is None else now
        mapping: dict[str, Any] = {
            "vehicle_id": vehicle_id,
            "updated_at": f"{current:.3f}",
            "last_seen_at": f"{current:.3f}",
            "last_seen_seconds": "0",
        }
        for field in (
            "battery_soc",
            "velocity",
            "battery_voltage",
            "fault_code",
            "vehicle_state",
        ):
            value = chassis.get(field)
            if value is not None:
                mapping[field] = str(value)
        await self._redis.hset(status_key(vehicle_id), mapping=mapping)
        await self._redis.sadd(ONLINE_SET_KEY, vehicle_id)

    async def mark_offline(self, vehicle_id: str) -> None:
        """标记车辆离线（心跳超阈值 / 通信中断；由守护任务调用）。"""
        await self._redis.hset(
            status_key(vehicle_id),
            mapping={"vehicle_id": vehicle_id, "status": OFFLINE_STATUS},
        )
        await self._redis.srem(ONLINE_SET_KEY, vehicle_id)


class VehicleStatusSweeper:
    """车辆状态守护：刷新 ``last_seen_seconds``，超阈值移出在线集合并告警。

    契约依据：``redis-keys.yaml`` pending #5（``vehicle:online:set`` 无 TTL，
    离线依赖「遥测中断 > 10s」）——实现为周期对账（比对 ``last_seen_at`` 后 SREM）。
    无本守护时：Hash 永不刷新 → ``last_seen_seconds`` 恒为 0 → ota-service 门禁
    会把已断连车辆误判为「网络稳定」（安全缺陷）。
    """

    def __init__(
        self,
        redis_client: RedisHashClient,
        settings: Settings,
        writer: VehicleStatusWriter | None = None,
    ) -> None:
        self._redis = redis_client
        self._settings = settings
        self._writer = writer or VehicleStatusWriter(redis_client)
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @property
    def threshold_seconds(self) -> int:
        """离线判定阈值（系统约束：遥测中断 > 10s → 离线/communication_loss）。"""
        return self._settings.vehicle_offline_threshold_seconds

    async def run_once(self, *, now: float | None = None) -> list[str]:
        """单次对账：返回本次判定为离线的车辆清单。"""
        current = time.time() if now is None else now
        offline: list[str] = []
        async for raw_key in self._redis.scan_iter(
            match=STATUS_KEY_PATTERN.format(vehicle_id="*"), count=200
        ):
            key = raw_key if isinstance(raw_key, str) else str(raw_key)
            vehicle_id = key.rsplit(":", 1)[-1]
            status = await self._redis.hgetall(key)
            last_seen = _safe_float(status.get("last_seen_at"))
            if last_seen is None or current - last_seen > self.threshold_seconds:
                await self._writer.mark_offline(vehicle_id)
                offline.append(vehicle_id)
                logger.warning(
                    "vehicle_marked_offline",
                    vehicle_id=vehicle_id,
                    last_seen_at=last_seen,
                    threshold_seconds=self.threshold_seconds,
                )
                continue
            await self._redis.hset(
                key, mapping={"last_seen_seconds": f"{max(0.0, current - last_seen):.3f}"}
            )
        return offline

    async def start(self) -> None:
        """启动周期守护任务（幂等；测试可只调 ``run_once``）。"""
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="vehicle-status-sweeper")

    async def stop(self) -> None:
        """停止守护任务（取消并等待收尾，避免任务泄漏）。"""
        self._running = False
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # 守护任务停机异常不得影响服务关停
            logger.exception("vehicle_status_sweeper_stop_failed")

    async def _loop(self) -> None:
        """周期执行 :meth:`run_once`（Redis 故障不中断循环，仅告警）。"""
        interval = max(1, self._settings.vehicle_offline_sweep_interval_seconds)
        while self._running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # 依赖故障时保持守护存活（下轮重试）
                logger.exception("vehicle_status_sweep_failed")
            await asyncio.sleep(interval)


def _safe_float(value: Any) -> float | None:
    """宽松浮点解析（Redis 值统一为字符串；缺失/非法返回 None）。"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "OFFLINE_STATUS",
    "ONLINE_SET_KEY",
    "STATUS_KEY_PATTERN",
    "VehicleStatusSweeper",
    "VehicleStatusWriter",
    "status_key",
]
