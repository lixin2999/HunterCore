"""6.2.2 异常驾驶检测规则引擎（``driving_anomaly_detection`` 作业的计算核心，纯 Python）。

契约依据：``contracts/openapi/data-analytics.yaml`` → ``x-hunter-realtime-jobs.jobs
[driving_anomaly_detection]``（6 条规则）与 ``contracts/kafka/schemas/alert_event.schema.json``
（消息形态）。等级取自受控词表 ``EVENT_LEVEL_BY_TYPE``（禁止放宽/改等级）。

实现语义（契约未细化的部分，按「可测试 + 不产生告警风暴」原则固化在本模块）：
- 纵向加速度由相邻样本 velocity 差分（telemetry 无 acceleration 字段）；dt 超过
  ``MAX_ACCEL_INTERVAL_SECONDS``（遥测 10-50Hz，1s 视为断流）不参与差分；
- 每条规则 **episode 语义**：异常区间内只触发一次（上升沿），条件恢复后重新武装——
  契约幂等键 (vehicle_id, alert_type, event_time) 在同一 episode 内 event_time 唯一；
- battery_low / battery_critical 独立判定（SOC 8% 时两条各触发一次）；
- over_speed 需要限速输入（telemetry 无该字段，来源见 data-analytics 契约 pending #13）：
  ``speed_limit_mps=None`` 时跳过该规则（禁止自造默认限速）。
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from hunter_common.database.enums import EVENT_LEVEL_BY_TYPE

from hunter_flink.thresholds import AlertThresholds

#: 差分最大样本间隔（秒）：超过视为数据断流，不参与加速度估计（非契约阈值，实现语义）
MAX_ACCEL_INTERVAL_SECONDS = 1.0

#: 告警来源作业（alert_event.schema.json source_job 枚举）
SOURCE_JOB = "driving_anomaly_detection"


@dataclass(frozen=True)
class RuleHit:
    """一次规则命中（尚未组装 alert_event 消息）。"""

    alert_type: str
    event_time: float
    rule: dict[str, Any]
    metrics: dict[str, float]
    seq: int | None = None
    window_start: float | None = None
    window_end: float | None = None
    location: dict[str, float] | None = None
    description: str = ""

    @property
    def level(self) -> str:
        """受控等级（唯一来源：EVENT_LEVEL_BY_TYPE；类型非法直接抛错，禁止静默降级）。"""
        return str(EVENT_LEVEL_BY_TYPE[self.alert_type])


@dataclass
class _Episode:
    """单规则 episode 状态：条件持续期间只触发一次，恢复后重新武装。"""

    active: bool = False        # 条件当前是否处于命中区间
    fired: bool = False         # 本 episode 是否已触发
    start_time: float = 0.0     # 命中区间起点
    peak: float = 0.0           # 区间内最严重实测值


@dataclass
class VehicleAnomalyTracker:
    """单车异常驾驶检测状态机（Flink 侧按 vehicle_id keyBy 后每键一实例）。

    ``now`` 由调用方注入（= 告警生成时间），规则判定全部基于车端事件时间戳。
    """

    thresholds: AlertThresholds = field(default_factory=AlertThresholds)
    speed_limit_mps: float | None = None

    def __post_init__(self) -> None:
        self._prev: tuple[float, float] | None = None  # (timestamp, velocity)
        self._accel = _Episode()
        self._brake = _Episode()
        self._turn = _Episode()
        self._over = _Episode()
        self._low = _Episode()
        self._critical = _Episode()

    # ---------- 主入口 ----------

    def on_sample(self, msg: dict[str, Any], now: float) -> list[RuleHit]:
        """处理一条 telemetry_clean 消息，返回本样本触发的命中（通常 0-2 条）。"""
        chassis = msg.get("chassis") or {}
        localization = msg.get("localization") or {}
        ts = float(msg["timestamp"])
        velocity = chassis.get("velocity")
        soc = chassis.get("battery_soc")
        omega = localization.get("angular_velocity")
        hits: list[RuleHit] = []

        # --- 加速度差分（急加速 / 急减速，持续型） ---
        acceleration: float | None = None
        if isinstance(velocity, (int, float)) and self._prev is not None:
            prev_ts, prev_v = self._prev
            dt = ts - prev_ts
            if 0 < dt <= MAX_ACCEL_INTERVAL_SECONDS:
                acceleration = (velocity - prev_v) / dt
        if isinstance(velocity, (int, float)):
            self._prev = (ts, float(velocity))

        if acceleration is not None:
            hits += self._detect_sustained(
                episode=self._accel,
                ts=ts,
                value=acceleration,
                breach=lambda v: v > self.thresholds.harsh_accel_ms2,
                hit_factory=lambda: self._harsh_accel_hit(msg, acceleration),
            )
            hits += self._detect_sustained(
                episode=self._brake,
                ts=ts,
                value=acceleration,
                breach=lambda v: v < -self.thresholds.harsh_braking_ms2,
                hit_factory=lambda: self._harsh_brake_hit(msg, acceleration),
            )

        # --- 横摆角速度（瞬时型，episode 去重） ---
        if isinstance(omega, list) and len(omega) >= 3:
            omega_z = abs(float(omega[2]))
            hits += self._detect_instant(
                episode=self._turn,
                ts=ts,
                active=omega_z > self.thresholds.harsh_turn_rad_s,
                hit_factory=lambda: RuleHit(
                    alert_type="harsh_turning",
                    event_time=ts,
                    seq=msg.get("seq"),
                    rule={
                        "name": "harsh_turning_gt_0_8rads",
                        "metric": "localization.angular_velocity",
                        "comparator": "gt",
                        "threshold": self.thresholds.harsh_turn_rad_s,
                    },
                    metrics={"abs_angular_velocity_z": round(omega_z, 4)},
                    location=self._location(localization),
                    description=f"横摆角速度 {omega_z:.2f} rad/s（阈值 {self.thresholds.harsh_turn_rad_s}）",
                ),
            )

        # --- 超速（需要限速来源，缺失即跳过——契约 pending #13） ---
        if (
            self.speed_limit_mps is not None
            and isinstance(velocity, (int, float))
            and self.speed_limit_mps > 0
        ):
            limit_speed = self.speed_limit_mps * self.thresholds.over_speed_ratio
            hits += self._detect_instant(
                episode=self._over,
                ts=ts,
                active=velocity > limit_speed,
                hit_factory=lambda: RuleHit(
                    alert_type="over_speed",
                    event_time=ts,
                    seq=msg.get("seq"),
                    rule={
                        "name": "over_speed_gt_limit_1_1x",
                        "metric": "chassis.velocity",
                        "comparator": "gt",
                        "threshold": round(limit_speed, 4),
                    },
                    metrics={
                        "velocity": float(velocity),
                        "speed_limit": self.speed_limit_mps,
                    },
                    location=self._location(localization),
                    description=(
                        f"车速 {velocity:.2f} m/s 超过限速 {self.speed_limit_mps} m/s 的 "
                        f"{self.thresholds.over_speed_ratio} 倍"
                    ),
                ),
            )

        # --- 低电量 / 电量危险（边沿触发） ---
        if soc is not None:
            soc_value = float(soc)
            hits += self._detect_instant(
                episode=self._low,
                ts=ts,
                active=soc_value < self.thresholds.battery_low_soc,
                hit_factory=lambda: RuleHit(
                    alert_type="battery_low",
                    event_time=ts,
                    seq=msg.get("seq"),
                    rule={
                        "name": "battery_low_lt_20pct",
                        "metric": "chassis.battery_soc",
                        "comparator": "lt",
                        "threshold": self.thresholds.battery_low_soc,
                    },
                    metrics={"battery_soc": soc_value},
                    location=self._location(localization),
                    description=f"SOC {soc_value}% 低于 {self.thresholds.battery_low_soc}%",
                ),
            )
            hits += self._detect_instant(
                episode=self._critical,
                ts=ts,
                active=soc_value < self.thresholds.battery_critical_soc,
                hit_factory=lambda: RuleHit(
                    alert_type="battery_critical",
                    event_time=ts,
                    seq=msg.get("seq"),
                    rule={
                        "name": "battery_critical_lt_10pct",
                        "metric": "chassis.battery_soc",
                        "comparator": "lt",
                        "threshold": self.thresholds.battery_critical_soc,
                    },
                    metrics={"battery_soc": soc_value},
                    location=self._location(localization),
                    description=f"SOC {soc_value}% 低于 {self.thresholds.battery_critical_soc}%",
                ),
            )
        return hits

    # ---------- 检测原语 ----------

    def _detect_sustained(
        self,
        *,
        episode: _Episode,
        ts: float,
        value: float,
        breach: Callable[[float], bool],
        hit_factory: Callable[[], RuleHit],
    ) -> list[RuleHit]:
        """持续型规则：命中区间需维持 > 最短时长门槛，每个 episode 只触发一次。"""
        if not breach(value):
            episode.active = False
            episode.fired = False
            episode.start_time = 0.0
            return []
        if not episode.active:
            episode.active = True
            episode.fired = False
            episode.start_time = ts
        episode.peak = max(episode.peak, abs(value))
        if not episode.fired and ts - episode.start_time > self._min_duration(episode):
            episode.fired = True
            hit = hit_factory()
            return [hit] if hit is not None else []
        return []

    def _detect_instant(
        self,
        *,
        episode: _Episode,
        ts: float,
        active: bool,
        hit_factory: Callable[[], RuleHit],
    ) -> list[RuleHit]:
        """瞬时型规则：上升沿触发一次，条件恢复后重新武装。"""
        if not active:
            episode.active = False
            return []
        if episode.active:
            return []  # 已在本 episode 内触发
        episode.active = True
        episode.start_time = ts
        hit = hit_factory()
        return [hit] if hit is not None else []

    def _min_duration(self, episode: _Episode) -> float:
        return (
            self.thresholds.harsh_accel_min_duration_seconds
            if episode is self._accel
            else self.thresholds.harsh_braking_min_duration_seconds
        )

    # ---------- 命中组装 ----------

    def _harsh_accel_hit(self, msg: dict[str, Any], accel: float) -> RuleHit:
        localization = msg.get("localization") or {}
        return RuleHit(
            alert_type="harsh_acceleration",
            event_time=self._accel.start_time,
            seq=msg.get("seq"),
            rule={
                "name": "harsh_acceleration_gt_3ms2",
                "metric": "chassis.acceleration",
                "comparator": "gt",
                "threshold": self.thresholds.harsh_accel_ms2,
                "duration_seconds": self.thresholds.harsh_accel_min_duration_seconds,
            },
            metrics={"acceleration": round(accel, 4)},
            window_start=self._accel.start_time,
            window_end=float(msg["timestamp"]),
            location=self._location(localization),
            description=(
                f"急加速 {accel:.2f} m/s² 持续 "
                f"{float(msg['timestamp']) - self._accel.start_time:.2f}s"
                f"（阈值 {self.thresholds.harsh_accel_ms2} m/s² 且 > "
                f"{self.thresholds.harsh_accel_min_duration_seconds}s）"
            ),
        )

    def _harsh_brake_hit(self, msg: dict[str, Any], accel: float) -> RuleHit:
        localization = msg.get("localization") or {}
        return RuleHit(
            alert_type="harsh_braking",
            event_time=self._brake.start_time,
            seq=msg.get("seq"),
            rule={
                "name": "harsh_braking_lt_-3ms2",
                "metric": "chassis.acceleration",
                "comparator": "lt",
                "threshold": -self.thresholds.harsh_braking_ms2,
                "duration_seconds": self.thresholds.harsh_braking_min_duration_seconds,
            },
            metrics={"min_acceleration": round(accel, 4)},
            window_start=self._brake.start_time,
            window_end=float(msg["timestamp"]),
            location=self._location(localization),
            description=(
                f"急减速 {accel:.2f} m/s² 持续 "
                f"{float(msg['timestamp']) - self._brake.start_time:.2f}s"
                f"（阈值 -{self.thresholds.harsh_braking_ms2} m/s² 且 > "
                f"{self.thresholds.harsh_braking_min_duration_seconds}s）"
            ),
        )

    @staticmethod
    def _location(localization: dict[str, Any]) -> dict[str, float] | None:
        """定位段快照（schema location 要求 x/y 必填；缺定位则省略该可选字段）。"""
        x, y = localization.get("x"), localization.get("y")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            return None
        out: dict[str, float] = {"x": float(x), "y": float(y)}
        for key in ("z", "heading"):
            value = localization.get(key)
            if isinstance(value, (int, float)):
                out[key] = float(value)
        return out


def build_alert(vehicle_id: str, hit: RuleHit, *, now: float) -> dict[str, Any]:
    """RuleHit → alert_event 消息（载荷与 contracts/kafka/schemas/alert_event.schema.json 1:1）。"""
    alert: dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "alert_id": str(uuid.uuid4()),
        "alert_type": hit.alert_type,
        "level": hit.level,
        "source_job": SOURCE_JOB,
        "event_time": float(hit.event_time),
        "timestamp": float(now),
        "rule": hit.rule,
    }
    if hit.seq is not None:
        alert["seq"] = int(hit.seq)
    if hit.window_start is not None:
        alert["window_start"] = float(hit.window_start)
    if hit.window_end is not None:
        alert["window_end"] = float(hit.window_end)
    if hit.metrics:
        alert["metrics"] = dict(hit.metrics)
    if hit.location is not None:
        alert["location"] = dict(hit.location)
    if hit.description:
        alert["description"] = hit.description[:512]
    return alert


__all__ = [
    "MAX_ACCEL_INTERVAL_SECONDS",
    "SOURCE_JOB",
    "RuleHit",
    "VehicleAnomalyTracker",
    "build_alert",
]
