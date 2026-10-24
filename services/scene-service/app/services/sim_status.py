"""CARLA 状态 → 平台 SimulationStatus 适配器（详细设计 6.3）。

平台状态枚举固定为 ``pending/running/succeeded/failed/canceled``；CARLA 的
``CREATED/LOADING/READY`` 映射为 ``pending``，``PAUSED`` 映射为 ``running`` 并通过
``message=paused`` 保留语义，``carla_status`` 字段保留 CARLA 原始值。

未知状态不再静默回落 ``running``（会导致进度查询永久 3003）：记录
``carla_status_unknown`` 告警并返回 ``failed``。
"""
from __future__ import annotations

from hunter_common.logging import get_logger

from app.schemas.scene import SimulationStatus

logger = get_logger("app.services.sim_status")

_CARLA_STATUS_MAP: dict[str, SimulationStatus] = {
    # 兼容 CARLA 直接返回平台枚举（部分管理 API 版本）
    "pending": SimulationStatus.PENDING,
    "running": SimulationStatus.RUNNING,
    "succeeded": SimulationStatus.SUCCEEDED,
    "failed": SimulationStatus.FAILED,
    "canceled": SimulationStatus.CANCELED,
    "cancelled": SimulationStatus.CANCELED,
    "created": SimulationStatus.PENDING,
    "loading": SimulationStatus.PENDING,
    "ready": SimulationStatus.PENDING,
    "paused": SimulationStatus.RUNNING,
    "completed": SimulationStatus.SUCCEEDED,
}


def normalize_carla_status(raw: str | None) -> str | None:
    """CARLA 原始状态统一大写（响应排障用）；空值返回 None。"""
    if raw is None:
        return None
    text = str(raw).strip()
    return text.upper() if text else None


def to_platform_status(raw: str | None) -> SimulationStatus:
    """CARLA 状态 → 平台状态（未知状态告警并返回 failed）。"""
    key = (raw or "").strip().lower()
    mapped = _CARLA_STATUS_MAP.get(key)
    if mapped is None:
        logger.warning("carla_status_unknown", status=raw)
        return SimulationStatus.FAILED
    return mapped


def is_paused(raw: str | None) -> bool:
    """是否为 PAUSED（平台状态仍为 running，message 需保留 paused）。"""
    return (raw or "").strip().lower() == "paused"


__all__ = ["is_paused", "normalize_carla_status", "to_platform_status"]
