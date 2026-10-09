"""车辆台账回写（``vehicle_svc.vehicles.status`` / ``last_online_time``）。

**为什么需要本模块**：车辆管理页（vehicle-service）的「状态」「最近在线」两列直接取自台账列，
而台账列与 Redis 读模型（``vehicle:status:{vehicle_id}`` / ``vehicle:online:set``）是两套数据：
读模型由本服务维护（契约 pending #2 指定的唯一写方），台账列此前**没有任何写入方**
（``VehicleRepository.update_status`` 全仓无调用者、表上也没有 trigger 兜底）——
结果是遥测链路完全正常时，页面仍恒显「离线 / 尚未上线」。

写入策略（1Hz×N 车的消费路径上必须控制写放大）：

===========================  ==========================================
触发点                        行为
===========================  ==========================================
health（状态权威来源）        状态跃迁立即写；稳态按间隔节流写
telemetry（在线证据）         只刷新 ``last_online_time``，**不改 status**
Sweeper 判离线                立即写 ``status=offline``（跃迁必须落库，不节流）
===========================  ==========================================

三条不可省略的约束：

1. **异常只告警不上抛**：台账回写是读模型的副作用，DB 抖动/台账无该行都不得让遥测消费
   失败——消费失败会触发重试与 DLQ，把"页面数字旧一点"放大成"数据管道中断"；
2. **失败也计入节流窗口**：否则 DB 持续故障时每条消息都会打一次 DB（写放大变成风暴）；
   状态跃迁不受此约束（``status`` 变化仍立即重试）；
3. **内存态有界**：``vehicle_id`` 取自消息体（外部可控），跟踪表按 FIFO 淘汰，
   避免异常/伪造车辆标识导致常驻内存无界增长。
"""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Protocol

from hunter_common.database.enums import VehicleStatus
from hunter_common.logging import get_logger

from app.config import Settings

logger = get_logger("app.services.vehicle_ledger")

#: 跟踪的车辆数上限（超出按 FIFO 淘汰；台账/契约规模量级为百~千辆车）
MAX_TRACKED_VEHICLES = 10_000

#: 离线状态名（与 :data:`app.services.vehicle_status.OFFLINE_STATUS` 同一取值；
#: 本模块**不得**反向 import 该模块——读模型写入器依赖本模块，导入会形成循环）
OFFLINE_STATUS_NAME = "offline"


class VehicleLedgerRepository(Protocol):
    """台账回写所需能力（:class:`hunter_common.database.VehicleRepository` 的子集）。"""

    async def update_status(
        self,
        vehicle_id: str,
        status: VehicleStatus,
        *,
        last_online_time: datetime | None = None,
    ) -> Any:  # pragma: no cover - 协议声明
        ...

    async def touch_last_online_time(self, vehicle_id: str, *, seen_at: datetime) -> bool:
        ...  # pragma: no cover - 协议声明


class VehicleLedgerWriter:
    """台账回写器（health / telemetry / Sweeper 三条路径共用，内部节流）。"""

    def __init__(
        self,
        repository: VehicleLedgerRepository,
        config: Settings,
        *,
        clock: Any = time.time,
    ) -> None:
        self._repo = repository
        self._interval = max(0, int(config.vehicle_ledger_write_interval_seconds))
        self._enabled = bool(config.vehicle_ledger_write_enabled)
        self._clock = clock
        #: vehicle_id → (已写入的 status, 最近一次写入尝试时刻)
        self._status_writes: dict[str, tuple[str, float]] = {}
        #: vehicle_id → 最近一次 last_online_time 写入尝试时刻
        self._seen_writes: dict[str, float] = {}

    @property
    def enabled(self) -> bool:
        """回写开关（``VEHICLE_LEDGER_WRITE_ENABLED=false`` 时全部方法直接短路）。"""
        return self._enabled

    async def record_status(self, vehicle_id: str, status: str, *, now: float | None = None) -> bool:
        """按 health 状态回写 ``status`` + ``last_online_time``；返回是否实际写库。

        状态与上次一致且未超节流窗口时跳过（health 为 1Hz，稳态车辆无需每秒一次 UPDATE）。
        """
        current = float(self._clock()) if now is None else now
        if not self._enabled or not vehicle_id:
            return False
        previous = self._status_writes.get(vehicle_id)
        if previous is not None and previous[0] == status and self._within_interval(previous[1], current):
            return False
        mapped = _to_vehicle_status(status)
        if mapped is None:
            # Schema 已约束 8 态；出现未知值说明契约/车端漂移，记 WARN 且不猜测写库
            logger.warning("vehicle_ledger_status_unmapped", vehicle_id=vehicle_id, status=status)
            return False
        self._status_writes[vehicle_id] = (status, current)
        self._prune()
        return await self._run(
            "vehicle_ledger_status_write_failed",
            lambda: self._repo.update_status(
                vehicle_id, mapped, last_online_time=_to_datetime(current)
            ),
            vehicle_id=vehicle_id,
            status=status,
        )

    async def record_seen(self, vehicle_id: str, *, now: float | None = None) -> bool:
        """按 telemetry 刷新 ``last_online_time``（不改业务状态）；返回是否实际写库。"""
        current = float(self._clock()) if now is None else now
        if not self._enabled or not vehicle_id:
            return False
        previous = self._seen_writes.get(vehicle_id)
        if previous is not None and self._within_interval(previous, current):
            return False
        self._seen_writes[vehicle_id] = current
        self._prune()
        return await self._run(
            "vehicle_ledger_seen_write_failed",
            lambda: self._repo.touch_last_online_time(vehicle_id, seen_at=_to_datetime(current)),
            vehicle_id=vehicle_id,
        )

    async def mark_offline(self, vehicle_id: str, *, now: float | None = None) -> bool:
        """心跳超时判离线时回写 ``status=offline``（跃迁不节流，但已离线则不重复写）。

        Sweeper 每轮（默认 5s）都会对陈旧车辆调用本方法，若不去重就是持续的无效 UPDATE。
        """
        current = float(self._clock()) if now is None else now
        if not self._enabled or not vehicle_id:
            return False
        previous = self._status_writes.get(vehicle_id)
        if previous is not None and previous[0] == OFFLINE_STATUS_NAME:
            return False
        self._status_writes[vehicle_id] = (OFFLINE_STATUS_NAME, current)
        self._prune()
        return await self._run(
            "vehicle_ledger_offline_write_failed",
            lambda: self._repo.update_status(
                vehicle_id, VehicleStatus.OFFLINE, last_online_time=_to_datetime(current)
            ),
            vehicle_id=vehicle_id,
        )

    def _within_interval(self, last_attempt: float, current: float) -> bool:
        """节流判定：``interval=0`` 表示不节流（每条都写，仅用于本地调试/压测）。"""
        return self._interval > 0 and (current - last_attempt) < self._interval

    async def _run(self, event: str, action: Any, **log_fields: Any) -> bool:
        """执行一次回写：异常记 WARN（含异常类型与消息）后吞掉（见模块文档约束 1）。

        必须带上异常详情：回写失败不报错到链路，日志是唯一可观测面；
        “静默返 None”会把接线错误伪装成“数据本来就旧”。
        """
        try:
            await action()
        except Exception as exc:  # noqa: BLE001 — 台账回写失败不得影响采集链路（禁止上抛触发重试/DLQ）
            logger.warning(
                event,
                error_type=type(exc).__name__,
                error_message=str(exc) or "<empty>",
                **log_fields,
            )
            return False
        return True

    def _prune(self) -> None:
        """跟踪表有界（防外部可控 vehicle_id 造成内存无界增长）。"""
        for store in (self._status_writes, self._seen_writes):
            while len(store) > MAX_TRACKED_VEHICLES:
                store.pop(next(iter(store)))


def _to_vehicle_status(value: str) -> VehicleStatus | None:
    """字符串 → 受控词表枚举（未知取值返回 ``None``，禁止猜测映射）。"""
    try:
        return VehicleStatus(value)
    except ValueError:
        return None


def _to_datetime(epoch: float) -> datetime:
    """Unix 秒 → tz-aware UTC 时间（``TIMESTAMPTZ`` 列要求带时区，禁止 naive）。"""
    return datetime.fromtimestamp(epoch, tz=UTC)


__all__ = [
    "MAX_TRACKED_VEHICLES",
    "OFFLINE_STATUS_NAME",
    "VehicleLedgerRepository",
    "VehicleLedgerWriter",
]
