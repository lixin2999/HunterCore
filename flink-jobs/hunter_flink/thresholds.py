"""实时作业阈值（契约 ``x-hunter-realtime-jobs.thresholds`` 14 项，全部环境变量化）。

单一事实来源：``contracts/openapi/data-analytics.yaml``。默认值 = 契约基准值，
环境变量仅允许运维按契约覆盖（禁止在作业代码中放宽——单测逐项比对契约基准）。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import ClassVar


def _env_number(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"阈值环境变量 {name} 非法数值: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"阈值环境变量 {name} 必须为正数，实际 {value}")
    return value


@dataclass(frozen=True)
class AlertThresholds:
    """6.2 节阈值集合（字段名 ↔ 契约 env 名一一映射，见 ``ENV_NAMES``）。"""

    communication_loss_seconds: float = 10.0        # ALERT_COMMUNICATION_LOSS_SECONDS
    trigger_max_latency_seconds: float = 2.0        # ALERT_TRIGGER_MAX_LATENCY_SECONDS
    harsh_accel_ms2: float = 3.0                    # ALERT_HARSH_ACCEL_MS2
    harsh_accel_min_duration_seconds: float = 0.5   # ALERT_HARSH_ACCEL_MIN_DURATION_SECONDS
    harsh_braking_ms2: float = 3.0                  # ALERT_HARSH_BRAKING_MS2
    harsh_braking_min_duration_seconds: float = 0.5  # ALERT_HARSH_BRAKING_MIN_DURATION_SECONDS
    harsh_turn_rad_s: float = 0.8                   # ALERT_HARSH_TURN_RAD_S
    over_speed_ratio: float = 1.1                   # ALERT_OVER_SPEED_RATIO
    battery_low_soc: float = 20.0                   # ALERT_BATTERY_LOW_SOC
    battery_critical_soc: float = 10.0              # ALERT_BATTERY_CRITICAL_SOC
    collision_ttc_critical_seconds: float = 1.5     # ALERT_COLLISION_TTC_CRITICAL_SECONDS
    collision_ttc_warning_seconds: float = 3.0      # ALERT_COLLISION_TTC_WARNING_SECONDS
    algorithm_metrics_window_seconds: float = 60.0  # ALGORITHM_METRICS_WINDOW_SECONDS
    alert_window_seconds: float = 2.0               # ALERT_WINDOW_SECONDS

    #: 字段名 → 契约 env 名（与 x-hunter-realtime-jobs.thresholds[].name 一致，14 项）
    ENV_NAMES: ClassVar[dict[str, str]] = {
        "communication_loss_seconds": "ALERT_COMMUNICATION_LOSS_SECONDS",
        "trigger_max_latency_seconds": "ALERT_TRIGGER_MAX_LATENCY_SECONDS",
        "harsh_accel_ms2": "ALERT_HARSH_ACCEL_MS2",
        "harsh_accel_min_duration_seconds": "ALERT_HARSH_ACCEL_MIN_DURATION_SECONDS",
        "harsh_braking_ms2": "ALERT_HARSH_BRAKING_MS2",
        "harsh_braking_min_duration_seconds": "ALERT_HARSH_BRAKING_MIN_DURATION_SECONDS",
        "harsh_turn_rad_s": "ALERT_HARSH_TURN_RAD_S",
        "over_speed_ratio": "ALERT_OVER_SPEED_RATIO",
        "battery_low_soc": "ALERT_BATTERY_LOW_SOC",
        "battery_critical_soc": "ALERT_BATTERY_CRITICAL_SOC",
        "collision_ttc_critical_seconds": "ALERT_COLLISION_TTC_CRITICAL_SECONDS",
        "collision_ttc_warning_seconds": "ALERT_COLLISION_TTC_WARNING_SECONDS",
        "algorithm_metrics_window_seconds": "ALGORITHM_METRICS_WINDOW_SECONDS",
        "alert_window_seconds": "ALERT_WINDOW_SECONDS",
    }

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> AlertThresholds:
        """按契约 env 名读取覆盖值（缺省 = 契约基准值）。"""
        restore: dict[str, str] | None = None
        if env is not None:
            restore = dict(os.environ)
            os.environ.update(env)
        try:
            baseline = cls()
            return cls(
                **{
                    field: _env_number(env_name, getattr(baseline, field))
                    for field, env_name in cls.ENV_NAMES.items()
                }
            )
        finally:
            if restore is not None:
                os.environ.clear()
                os.environ.update(restore)


__all__ = ["AlertThresholds"]
