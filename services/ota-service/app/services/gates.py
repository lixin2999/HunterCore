"""升级门禁评估（x-hunter-canary-rollout + 设计文档升级门禁；读 Redis 读模型）。

门禁项（契约 OtaPreconditionName，不可新增/更改阈值）：
- battery_soc：电量 ≥ OTA_PRECONDITION_MIN_SOC（默认 50%）；
- vehicle_parked：静止且 P 档（OTA_PRECONDITION_REQUIRE_PARKED）；
- network_stable：遥测最后上报间隔 ≤ OTA_OFFLINE_THRESHOLD_SECONDS（默认 10s）；
- storage：可用存储 ≥ OTA_PRECONDITION_MIN_STORAGE_MB（默认 2048MB = 2GB）。

数据来源（只读，权威值属 vehicle-service，禁止跨服务直连）：
- ``vehicle:status:{vehicle_id}`` Hash（battery_soc / gear / last_seen_seconds / free_storage_mb）；
- ``vehicle:online:set`` Set（在线判定）；
- 读模型缺失：OTA_OFFLINE_FALLBACK_ENABLED=false（默认）→ 缺数据即拒绝（安全默认，#19）。

回滚门禁差异：仅要求静止 + P 档 + 网络稳定（回退不刷写镜像，不要求电量 ≥ 50%）。
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from hunter_common.logging import get_logger, reset_vehicle_id, set_vehicle_id

from app.schemas.common import OtaPreconditionName

if TYPE_CHECKING:
    from app.config import Settings

logger = get_logger("app.services.gates")

#: P 档档位值（车辆底盘档位；vehicle:status 读模型 gear 字段）
PARKED_GEAR = "P"


@dataclass(frozen=True, slots=True)
class GateResult:
    """单车门禁结论。

    - released=True：全部通过（failed_conditions 为空）；
    - offline=True：车辆不在在线集合（blocked 原因 = 车辆不在线）；
    - 否则 blocked：failed_conditions 给出未通过项，actual 携带实测值。
    """

    vehicle_id: str
    released: bool = False
    offline: bool = False
    failed_conditions: tuple[OtaPreconditionName, ...] = ()
    actual: dict[str, object] = field(default_factory=dict)


class VehicleStateReader(Protocol):
    """车辆状态读模型访问协议（生产实现读 Redis；测试注入内存桩）。"""

    async def get_status(self, vehicle_id: str) -> dict[str, Any] | None:
        """读取 vehicle:status:{vehicle_id}（Hash → dict；缺失返回 None）。"""
        ...

    async def is_online(self, vehicle_id: str) -> bool:
        """判定车辆是否在 vehicle:online:set 中。"""
        ...


def _as_int(value: Any) -> int | None:
    """宽松整数解析（Redis 读模型值统一为字符串存储）。"""
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    """宽松浮点解析（NaN/Inf 视为缺失）。"""
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def evaluate_gates(
    vehicle_id: str,
    status: dict[str, Any] | None,
    *,
    require_soc: bool,
    require_parked: bool,
    require_network: bool,
    min_soc: int,
    min_storage_mb: int,
    offline_threshold_seconds: int,
) -> GateResult:
    """基于读模型快照的纯门禁评估（同步函数，便于单元测试）。"""
    actual: dict[str, object] = {}
    failed: list[OtaPreconditionName] = []
    if status:
        soc = _as_int(status.get("battery_soc"))
        gear = status.get("gear")
        last_seen = _as_float(status.get("last_seen_seconds"))
        storage = _as_int(status.get("free_storage_mb"))
        if soc is not None:
            actual["battery_soc"] = soc
        if gear is not None:
            actual["gear"] = str(gear)
        if last_seen is not None:
            actual["last_seen_seconds"] = last_seen
        if storage is not None:
            actual["free_storage_mb"] = storage
        if require_soc and (soc is None or soc < min_soc):
            failed.append(OtaPreconditionName.BATTERY_SOC)
        if require_parked and (gear is None or str(gear) != PARKED_GEAR):
            failed.append(OtaPreconditionName.VEHICLE_PARKED)
        if require_network and (last_seen is None or last_seen > offline_threshold_seconds):
            failed.append(OtaPreconditionName.NETWORK_STABLE)
        if storage is None or storage < min_storage_mb:
            failed.append(OtaPreconditionName.STORAGE)
    else:
        # 读模型缺失：缺数据即拒绝（OTA_OFFLINE_FALLBACK_ENABLED=false 安全默认，#19）；
        # 在线但读模型缺失且 fallback 开启的放行分支见 check_vehicle。
        if require_soc:
            failed.append(OtaPreconditionName.BATTERY_SOC)
        if require_parked:
            failed.append(OtaPreconditionName.VEHICLE_PARKED)
        if require_network:
            failed.append(OtaPreconditionName.NETWORK_STABLE)
        failed.append(OtaPreconditionName.STORAGE)
    return GateResult(
        vehicle_id=vehicle_id,
        released=not failed,
        failed_conditions=tuple(failed),
        actual=actual,
    )


async def check_vehicle(
    reader: VehicleStateReader,
    vehicle_id: str,
    settings: Settings,
    *,
    require_soc: bool = True,
) -> GateResult:
    """单车门禁检查（在线集合 + 读模型；发布与回滚共用，回滚不查电量）。

    - 车辆不在在线集合 → offline（blocked 原因 = 车辆不在线）；
    - 读模型缺失但在在线集合：fallback_enabled=True 放行（仅 DEBUG 联调）；False → 全项失败拒绝。
    """
    token = set_vehicle_id(vehicle_id)
    try:
        online = await reader.is_online(vehicle_id)
        if not online:
            return GateResult(vehicle_id=vehicle_id, offline=True)
        status = await reader.get_status(vehicle_id)
        if status is None and settings.ota_offline_fallback_enabled:
            # ⚠ OTA_OFFLINE_FALLBACK_ENABLED=true 仅限联调（生产禁止，x-hunter-pending-confirmation #19）
            logger.warning("ota_gate_missing_read_model_fallback", vehicle_id=vehicle_id)
            return GateResult(vehicle_id=vehicle_id, released=True)
        result = evaluate_gates(
            vehicle_id,
            status,
            require_soc=require_soc,
            require_parked=settings.ota_precondition_require_parked,
            require_network=True,
            min_soc=settings.ota_precondition_min_soc,
            min_storage_mb=settings.ota_precondition_min_storage_mb,
            offline_threshold_seconds=settings.ota_offline_threshold_seconds,
        )
        if not result.released:
            logger.info(
                "ota_gate_rejected",
                vehicle_id=vehicle_id,
                failed_conditions=json.dumps([c.value for c in result.failed_conditions]),
            )
        return result
    finally:
        reset_vehicle_id(token)


__all__ = ["PARKED_GEAR", "GateResult", "VehicleStateReader", "check_vehicle", "evaluate_gates"]
