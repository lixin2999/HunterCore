"""VIL 状态构造与 10Hz latest-wins 转发。"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from hunter_common.logging import get_logger

from app.session_registry import SessionRegistry

logger = get_logger("app.forwarder")

#: 输出 Schema 逻辑名（contracts/kafka/schemas/carla_vil_state.schema.json）
VIL_STATE_SCHEMA = "carla_vil_state"


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
    if "source_seq" not in payload:
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


class VilForwarder:
    """按车辆 10Hz 节流转发；同车仅保留最新一帧（latest-wins）。"""

    def __init__(
        self,
        producer: Any,
        sessions: SessionRegistry,
        *,
        target_hz: int = 10,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._producer = producer
        self._sessions = sessions
        self._interval = 1.0 / max(1, target_hz)
        self._clock = clock
        self._last_published: dict[str, float] = {}
        self._latest: dict[str, dict[str, Any]] = {}
        self._flush_tasks: dict[str, asyncio.Task[None]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._published = 0
        self._skipped = 0

    @property
    def published_count(self) -> int:
        return self._published

    @property
    def skipped_count(self) -> int:
        return self._skipped

    async def handle(self, telemetry: dict[str, Any]) -> None:
        """处理一条 telemetry_clean；非 VIL 车辆直接跳过（不产生日志噪音）。"""
        vehicle_id = telemetry.get("vehicle_id")
        if not isinstance(vehicle_id, str) or not vehicle_id:
            raise ValueError("telemetry_clean 缺少 vehicle_id")
        session = await self._sessions.get(vehicle_id)
        if session is None or str(session.get("mode") or "") != "vil":
            self._skipped += 1
            return
        sim_id = session.get("sim_id")
        if not isinstance(sim_id, str) or not sim_id:
            logger.warning("vil_session_missing_sim_id", vehicle_id=vehicle_id)
            self._skipped += 1
            return
        payload = build_vil_state(telemetry, session, forwarded_at=time.time())
        async with self._lock_for(vehicle_id):
            now = self._clock()
            last = self._last_published.get(vehicle_id)
            if last is None or now - last >= self._interval:
                await self._publish(sim_id, payload)
                self._last_published[vehicle_id] = now
                return
            # 未到发送窗口：只保留最新一帧，由后台任务在窗口到期时发送
            self._latest[vehicle_id] = payload
            self._schedule_flush(vehicle_id, self._interval - (now - last))

    def _lock_for(self, vehicle_id: str) -> asyncio.Lock:
        lock = self._locks.get(vehicle_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[vehicle_id] = lock
        return lock

    def _schedule_flush(self, vehicle_id: str, delay: float) -> None:
        existing = self._flush_tasks.get(vehicle_id)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._flush_later(vehicle_id, max(0.0, delay)))
        self._flush_tasks[vehicle_id] = task
        task.add_done_callback(lambda _t, vid=vehicle_id: self._on_flush_done(vid))

    def _on_flush_done(self, vehicle_id: str) -> None:
        self._flush_tasks.pop(vehicle_id, None)
        if vehicle_id in self._latest:
            self._schedule_flush(vehicle_id, self._interval)

    async def _flush_later(self, vehicle_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        async with self._lock_for(vehicle_id):
            payload = self._latest.pop(vehicle_id, None)
            if payload is None:
                return
            await self._publish(str(payload["sim_id"]), payload)
            self._last_published[vehicle_id] = self._clock()

    async def _publish(self, sim_id: str, payload: dict[str, Any]) -> None:
        await self._producer.publish_payload(
            f"carla.{sim_id}.vil_state",
            payload,
            key=sim_id,
            schema_name=VIL_STATE_SCHEMA,
            contract_required=True,
        )
        self._published += 1

    async def close(self) -> None:
        """取消所有待发送任务（停机不保证最后一帧送达，实时链路允许 latest-wins 丢失）。"""
        tasks = [task for task in self._flush_tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._flush_tasks.clear()
        self._latest.clear()


__all__ = ["VIL_STATE_SCHEMA", "VilForwarder", "build_vil_state"]
