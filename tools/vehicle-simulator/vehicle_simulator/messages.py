"""契约消息构造器（telemetry / health / command_result）。

严格遵守 contracts/kafka/schemas/*.schema.json 的 ``additionalProperties=false``：
只产出契约声明的键，缺键或加键都会导致 data-collector 侧校验进 DLQ。构造完成后
``tests/`` 用 jsonschema 对真实 schema 文件自校验（契约一致性）。
"""

from __future__ import annotations

import math
import random
import time
import uuid
from typing import Any

from vehicle_simulator.kinematics import VehicleState

#: 车辆 8 态（health.schema.json status enum；受控词表不可新增）
STATUS_VOCAB: tuple[str, ...] = (
    "offline",
    "online_idle",
    "auto_driving",
    "remote_controlled",
    "upgrading",
    "charging",
    "fault",
    "emergency",
)


def resolve_status(state: VehicleState, *, online: bool = True) -> str:
    """业务状态推断（演示口径）：远程操控会话→remote_controlled；
    低速停车在线→online_idle；行驶中→auto_driving；离线→offline。"""
    if not online:
        return "offline"
    if state.control_enabled:
        return "remote_controlled"
    if abs(state.velocity) > 0.05:
        return "auto_driving"
    return "online_idle"


def _system_section(now: float) -> dict[str, Any]:
    """车-云系统指标（telemetry/health system 段字段完全一致，契约要求复用采集代码）。"""
    jitter = math.sin(now) * 2.0
    return {
        "cpu_usage": round(min(100.0, max(0.0, 42.0 + jitter + random.uniform(-3, 3))), 1),
        "gpu_usage": round(min(100.0, max(0.0, 60.0 + jitter + random.uniform(-4, 4))), 1),
        "memory_usage_mb": int(17000 + jitter * 100),
        "gpu_temp": round(66.0 + jitter, 1),
        "cpu_temp": round(63.0 + jitter, 1),
        "network_rssi": int(-62 + random.randint(-6, 4)),
        "network_latency_ms": round(max(5.0, 42.0 + jitter + random.uniform(-8, 8)), 1),
    }


def build_telemetry(
    vehicle_id: str,
    state: VehicleState,
    *,
    seq: int,
    target_velocity: float,
    target_steer: float,
    now: float | None = None,
) -> dict[str, Any]:
    """telemetry.schema.json 全量六段（chassis/localization/perception/planning/control/system）。"""
    ts = time.time() if now is None else now
    v = state.velocity
    heading = state.heading
    return {
        "vehicle_id": vehicle_id,
        "timestamp": round(ts, 3),
        "seq": seq,
        "chassis": {
            "velocity": round(v, 3),
            "steering_angle": round(state.steer, 4),
            "battery_voltage": round(state.battery_voltage, 2),
            "battery_soc": state.battery_soc,
            "battery_current": round(-8.0 - abs(v) * 6.0, 2),
            "battery_temp": round(27.5 + abs(v), 1),
            "control_mode": "REMOTE" if state.control_enabled else "CAN",
            "vehicle_state": "NORMAL",
            "fault_code": 0,
            "motor_rpm": [int(abs(v) * 900), int(abs(v) * 880), 0],
            "motor_current": [round(6.0 + abs(v), 2), round(6.2 + abs(v), 2), 2.0],
            "motor_temp": [45, 46, 42],
        },
        "localization": {
            "x": round(state.x, 3),
            "y": round(state.y, 3),
            "z": round(state.z, 3),
            "roll": 0.01,
            "pitch": 0.02,
            "heading": round(heading, 4),
            "linear_velocity": [round(v, 3), 0.0, 0.0],
            "angular_velocity": [0.0, 0.0, round(state.angular_velocity_z, 4)],
            "position_std": 0.15,
            "heading_std": 0.02,
        },
        "perception": {
            "detected_objects": random.randint(6, 16),
            "fps": round(10.0 + random.uniform(-0.6, 0.6), 2),
            "latency_ms": round(82.0 + random.uniform(-8, 12), 1),
            "object_types": {"vehicle": random.randint(2, 7), "pedestrian": random.randint(1, 4), "other": random.randint(1, 5)},
        },
        "planning": {
            "trajectory_length": round(4.0 + abs(v), 2),
            "trajectory_points": 50,
            "planning_latency_ms": round(45.0 + random.uniform(-6, 10), 1),
            "current_behavior": "remote" if state.control_enabled else "cruise",
        },
        "control": {
            "target_velocity": round(target_velocity, 3),
            "target_steer": round(target_steer, 4),
            "velocity_error": round(target_velocity - v, 3),
            "steer_error": round(target_steer - state.steer, 4),
            "control_latency_ms": round(5.0 + random.uniform(0, 3), 1),
        },
        "system": _system_section(ts),
    }


def build_health(vehicle_id: str, state: VehicleState, *, now: float | None = None, free_storage_mb: int = 8192) -> dict[str, Any]:
    """health.schema.json（status + system 必需；gear/free_storage_mb 可选顶层）。"""
    ts = time.time() if now is None else now
    return {
        "vehicle_id": vehicle_id,
        "timestamp": round(ts, 3),
        "status": resolve_status(state, online=True),
        "gear": state.gear,
        "free_storage_mb": free_storage_mb,
        "system": _system_section(ts),
    }


def build_command_result(
    vehicle_id: str,
    *,
    command_id: str | None = None,
    success: bool = True,
    result_code: str = "0",
    message: str = "ok",
    data: dict[str, Any] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """command_result.schema.json（required command_id/vehicle_id/timestamp/success）。

    remote_control 帧不携带 command_id（契约无此字段），故车端 ack 以**新生成 uuid** 作幂等键；
    remote-control 侧采近似关联计数（pending #16），command_id 仅用于日志定位，不做逐条精确匹配。
    """
    ts = time.time() if now is None else now
    payload: dict[str, Any] = {
        "command_id": command_id or str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "timestamp": round(ts, 3),
        "success": success,
        "result_code": result_code,
        "message": message[:512],
    }
    if data is not None:
        payload["data"] = data
    return payload


def new_command_id() -> str:
    """生成合法 uuid（command_result.command_id 幂等键，format=uuid）。"""
    return str(uuid.uuid4())
