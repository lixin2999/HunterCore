"""VIL 状态构造与 10Hz latest-wins 转发。

设计要点（CARLA 接入方案 7.2）：
- 按会话 ``target_hz`` 降采样，同车仅保留最新一帧（latest-wins，无队列）；
- ``seq`` 乱序丢弃、源时间戳迟到超过 ``max_source_lag_ms`` 丢弃并计数；
- 会话切换（同车 ``sim_id`` 变化）清空该车全部状态，避免序号/频率串扰；
- 车辆状态表有界（空闲 TTL + 最大条数），会话消失时主动回收，避免长期运行内存泄漏。
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from hunter_common.exceptions import ServiceUnavailableError
from hunter_common.logging import get_logger
from hunter_common.metrics import REGISTRY
from prometheus_client import Counter, Gauge, Histogram

from app.session_registry import SessionRegistry

logger = get_logger("app.forwarder")

#: 输出 Schema 逻辑名（contracts/kafka/schemas/carla_vil_state.schema.json）
VIL_STATE_SCHEMA = "carla_vil_state"

_SERVICE = "vil-forwarder"

#: 转发结果计数（result=published/out_of_order/too_late/session_missing/session_unavailable/publish_error）
VIL_FORWARD_TOTAL = Counter(
    "hunter_vil_forward_total",
    "VIL 转发结果计数（含丢帧原因）",
    labelnames=("service", "result"),
    registry=REGISTRY,
)
#: 源时间戳 → 实际转发时刻的端到端延迟（毫秒）
VIL_FORWARD_LAG = Histogram(
    "hunter_vil_forward_lag_ms",
    "VIL 源时间戳到转发时刻的延迟（毫秒）",
    labelnames=("service",),
    buckets=(10, 25, 50, 100, 200, 300, 500, 1000, 2000, 5000),
    registry=REGISTRY,
)
#: 当前跟踪的车辆会话数（内存上界观测）
VIL_ACTIVE_SESSIONS = Gauge(
    "hunter_vil_active_sessions",
    "当前跟踪的 VIL 车辆会话数",
    labelnames=("service",),
    registry=REGISTRY,
)


def build_vil_state(
    telemetry: dict[str, Any],
    session: dict[str, Any],
    *,
    forwarded_at: float | None = None,
) -> dict[str, Any]:
    """telemetry_clean → carla_vil_state（只做字段映射，不改变坐标语义）。

    坐标保持车端 odom 原始值；map/odom 标定由 CARLA vil-mapper 按会话 calibration 转换。
    """
    localization = telemetry.get("localization") or {}
    chassis = telemetry.get("chassis") or {}
    payload: dict[str, Any] = {
        "schema_version": "1.0.0",
        "sim_id": str(session["sim_id"]),
        "vehicle_id": str(telemetry["vehicle_id"]),
        "timestamp": float(telemetry["timestamp"]),
        "seq": int(telemetry["seq"]),
        "pose": {
            "frame": "odom",
            "x": float(localization.get("x", 0.0)),
            "y": float(localization.get("y", 0.0)),
            "z": float(localization.get("z", 0.0)),
            "yaw": float(localization.get("heading", 0.0)),
        },
        "twist": {
            "linear": list(localization.get("linear_velocity") or [0.0, 0.0, 0.0]),
            "angular": list(localization.get("angular_velocity") or [0.0, 0.0, 0.0]),
        },
        "chassis": {
            key: chassis[key]
            for key in (
                "velocity",
                "steering_angle",
                "battery_voltage",
                "battery_soc",
                "battery_current",
                "battery_temp",
                "control_mode",
                "vehicle_state",
                "fault_code",
                "motor_rpm",
                "motor_current",
                "motor_temp",
            )
            if key in chassis
        },
    }
    for key in ("roll", "pitch"):
        if key in localization:
            payload["pose"][key] = float(localization[key])
    payload["source_seq"] = int(telemetry.get("seq", 0))

    perception = telemetry.get("perception") or {}
    perception_out = {
        key: perception[key]
        for key in ("detected_objects", "fps", "latency_ms", "object_types")
        if key in perception
    }
    if perception_out:
        payload["perception"] = perception_out

    planning = telemetry.get("planning") or {}
    if planning.get("current_behavior"):
        payload["planning"] = {"current_behavior": str(planning["current_behavior"])}

    control = telemetry.get("control") or {}
    control_out = {
        key: control[key]
        for key in (
            "target_velocity",
            "target_steer",
            "velocity_error",
            "steer_error",
            "control_latency_ms",
        )
        if key in control
    }
    if control_out:
        payload["control"] = control_out

    calibration = session.get("calibration")
    if isinstance(calibration, dict) and calibration.get("calibration_id"):
        payload["calibration"] = {
            key: calibration[key]
            for key in (
                "calibration_id",
                "map_x",
                "map_y",
                "map_heading",
                "odom_x",
                "odom_y",
                "odom_heading",
            )
            if key in calibration
        }
    payload["source_timestamp"] = float(telemetry["timestamp"])
    payload["forwarded_at"] = float(forwarded_at if forwarded_at is not None else time.time())
    return payload


@dataclass
class _VehicleState:
    """单车辆转发状态（随会话创建/销毁，禁止跨会话复用）。"""

    sim_id: str
    target_hz: int
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_published: float | None = None
    last_seq: int = -1
    latest: dict[str, Any] | None = None
    last_seen: float = 0.0


class VilForwarder:
    """按车辆会话节流转发；同车仅保留最新一帧（latest-wins）。"""

    def __init__(
        self,
        producer: Any,
        sessions: SessionRegistry,
        *,
        target_hz: int = 10,
        max_source_lag_ms: int = 500,
        max_tracked_vehicles: int = 10000,
        idle_ttl_seconds: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._producer = producer
        self._sessions = sessions
        self._default_hz = min(50, max(1, target_hz))
        self._max_source_lag_s = max(0.0, max_source_lag_ms / 1000.0)
        self._max_tracked = max(1, max_tracked_vehicles)
        self._idle_ttl = max(1.0, idle_ttl_seconds)
        self._clock = clock
        self._state: dict[str, _VehicleState] = {}
        self._flush_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_prune = 0.0
        self._closed = False
        self._published = 0
        self._skipped = 0
        self._drop_counts: dict[str, int] = {}

    @property
    def published_count(self) -> int:
        return self._published

    @property
    def skipped_count(self) -> int:
        return self._skipped

    @property
    def tracked_sessions(self) -> int:
        return len(self._state)

    @property
    def drop_counts(self) -> dict[str, int]:
        return dict(self._drop_counts)

    async def handle(self, telemetry: dict[str, Any]) -> None:
        """处理一条 telemetry_clean；非 VIL 车辆直接跳过（不产生日志噪音）。"""
        vehicle_id = telemetry.get("vehicle_id")
        if not isinstance(vehicle_id, str) or not vehicle_id:
            raise ValueError("telemetry_clean 缺少 vehicle_id")
        wall = time.time()
        try:
            session = await self._sessions.get(vehicle_id)
        except ServiceUnavailableError:
            # Redis 短暂不可用：VIL 是实时旁路，丢弃当前帧优于重试堆积/进 DLQ
            # （恢复后下一帧继续 latest-wins；不影响控制面与回传面）（V1.23.4）。
            self._record("session_unavailable")
            return
        if session is None or str(session.get("mode") or "") != "vil":
            # 非 VIL 车辆是绝大多数遥测来源：静默跳过（只计数，不打指标/日志）
            self._forget(vehicle_id)
            self._skipped += 1
            return
        sim_id = session.get("sim_id")
        if not isinstance(sim_id, str) or not sim_id:
            logger.warning("vil_session_missing_sim_id", vehicle_id=vehicle_id)
            self._forget(vehicle_id)
            self._record("session_missing")
            return

        try:
            seq = int(telemetry["seq"])
            source_ts = float(telemetry["timestamp"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("telemetry_clean 缺少合法 seq/timestamp") from exc

        state = self._state.get(vehicle_id)
        if state is None or state.sim_id != sim_id:
            if state is not None:
                # 同一车辆切换到新会话：清空旧 sim 的序号/频率/待发送帧
                self._forget(vehicle_id)
                logger.info(
                    "vil_session_switched",
                    vehicle_id=vehicle_id,
                    old_sim_id=state.sim_id,
                    new_sim_id=sim_id,
                )
            state = _VehicleState(sim_id=sim_id, target_hz=self._resolve_hz(session))
            self._state[vehicle_id] = state
        else:
            state.target_hz = self._resolve_hz(session)
        state.last_seen = wall
        self._prune(wall)

        if seq <= state.last_seq:
            self._record("out_of_order")
            return
        state.last_seq = seq
        if self._max_source_lag_s and wall - source_ts > self._max_source_lag_s:
            # 迟到帧只用于可视化会抖动，直接丢弃（CARLA 侧进入外推）
            self._record("too_late")
            return

        payload = build_vil_state(telemetry, session, forwarded_at=wall)
        interval = 1.0 / max(1, state.target_hz)
        async with state.lock:
            now = self._clock()
            if state.last_published is None or now - state.last_published >= interval:
                await self._publish(sim_id, payload)
                state.last_published = now
                self._observe_lag(wall, source_ts)
                return
            # 未到发送窗口：只保留最新一帧，由后台任务在窗口到期时发送
            state.latest = payload
            self._schedule_flush(vehicle_id, interval - (now - state.last_published))

    def _resolve_hz(self, session: dict[str, Any]) -> int:
        raw = session.get("target_hz")
        if isinstance(raw, bool) or not isinstance(raw, int):
            return self._default_hz
        return min(50, max(1, raw))

    def _schedule_flush(self, vehicle_id: str, delay: float) -> None:
        if self._closed:
            return
        existing = self._flush_tasks.get(vehicle_id)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._flush_later(vehicle_id, max(0.0, delay)))
        self._flush_tasks[vehicle_id] = task
        task.add_done_callback(lambda _t, vid=vehicle_id: self._on_flush_done(vid))

    def _on_flush_done(self, vehicle_id: str) -> None:
        current = self._flush_tasks.get(vehicle_id)
        if current is not None and current.done():
            self._flush_tasks.pop(vehicle_id, None)
        state = self._state.get(vehicle_id)
        if state is not None and state.latest is not None:
            self._schedule_flush(vehicle_id, 1.0 / max(1, state.target_hz))

    async def _flush_later(self, vehicle_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        state = self._state.get(vehicle_id)
        if state is None:
            return
        async with state.lock:
            payload = state.latest
            state.latest = None
            if payload is None or payload.get("sim_id") != state.sim_id:
                return
            try:
                await self._publish(state.sim_id, payload)
            except Exception:
                # 后台任务异常不能逃逸（否则会静默丢任务）；记指标并丢弃该帧，
                # 下一帧仍按 latest-wins 继续，避免无限重试堆积。
                self._record("publish_error")
                logger.exception(
                    "vil_forward_publish_failed",
                    vehicle_id=vehicle_id,
                    sim_id=state.sim_id,
                )
                return
            state.last_published = self._clock()
            self._observe_lag(time.time(), float(payload.get("source_timestamp") or 0.0))

    def _forget(self, vehicle_id: str) -> None:
        self._state.pop(vehicle_id, None)
        task = self._flush_tasks.pop(vehicle_id, None)
        if task is not None and not task.done():
            task.cancel()

    def _prune(self, now: float) -> None:
        """回收空闲/超限车辆状态，保证长期运行内存有界。"""
        if len(self._state) <= self._max_tracked and now - self._last_prune < 30.0:
            return
        self._last_prune = now
        stale = [
            vehicle_id
            for vehicle_id, state in self._state.items()
            if now - state.last_seen > self._idle_ttl
        ]
        overflow = len(self._state) - len(stale) - self._max_tracked
        if overflow > 0:
            candidates = sorted(
                (
                    (vehicle_id, state.last_seen)
                    for vehicle_id, state in self._state.items()
                    if vehicle_id not in stale
                ),
                key=lambda item: item[1],
            )
            stale.extend(vehicle_id for vehicle_id, _ in candidates[:overflow])
        for vehicle_id in stale:
            self._forget(vehicle_id)
        VIL_ACTIVE_SESSIONS.labels(_SERVICE).set(len(self._state))

    async def _publish(self, sim_id: str, payload: dict[str, Any]) -> None:
        await self._producer.publish_payload(
            f"carla.{sim_id}.vil_state",
            payload,
            key=sim_id,
            schema_name=VIL_STATE_SCHEMA,
            contract_required=True,
        )
        self._published += 1
        self._record("published")

    def _record(self, result: str) -> None:
        VIL_FORWARD_TOTAL.labels(_SERVICE, result).inc()
        if result != "published":
            self._drop_counts[result] = self._drop_counts.get(result, 0) + 1
            if result == "session_missing":
                self._skipped += 1

    @staticmethod
    def _observe_lag(now: float, source_ts: float) -> None:
        if source_ts > 0:
            VIL_FORWARD_LAG.labels(_SERVICE).observe(max(0.0, (now - source_ts) * 1000.0))

    async def close(self) -> None:
        """取消所有待发送任务（停机不保证最后一帧送达，实时链路允许 latest-wins 丢失）。"""
        self._closed = True
        tasks = [task for task in self._flush_tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._flush_tasks.clear()
        self._state.clear()
        VIL_ACTIVE_SESSIONS.labels(_SERVICE).set(0)


__all__ = [
    "VIL_STATE_SCHEMA",
    "VilForwarder",
    "build_vil_state",
]
