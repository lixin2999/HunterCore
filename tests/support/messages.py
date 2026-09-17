"""Kafka 消息工厂：测试消息一律由契约示例派生并回校 JSON Schema。

设计要点：
- 种子 = ``contracts/kafka/schemas/*.schema.json`` 的 ``examples[0]``（契约即示例即测试数据）
- 每个工厂函数返回前调用 :func:`contracts.assert_valid_message`，工厂与契约漂移立即报错
- 字段名、枚举、取值范围禁止在测试中自造（Schema ``additionalProperties: false`` 会拦截）
"""
from __future__ import annotations

import copy
import json
import time
from typing import Any

from tests.support import contracts


def encode(payload: dict[str, Any]) -> bytes:
    """消息体序列化（Kafka 消息格式：JSON，UTF-8）。"""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _seed(schema_name: str, index: int = 0) -> dict[str, Any]:
    """取契约示例种子（深拷贝）。"""
    return contracts.schema_example(schema_name, index)


def _now(offset: float = 0.0) -> float:
    """当前 Unix epoch 秒（含毫秒）。"""
    return round(time.time() + offset, 3)


# ---------------------------------------------------------------- 车端 → 平台

def telemetry_message(
    vehicle_id: str,
    *,
    seq: int = 1,
    timestamp: float | None = None,
    velocity: float = 1.52,
    battery_soc: int = 78,
    vehicle_state: str = "NORMAL",
    fault_code: int = 0,
    gpu_usage: float = 62.8,
) -> dict[str, Any]:
    """遥测消息（``hunter.{vehicle_id}.telemetry``，Schema: telemetry）。"""
    payload = _seed("telemetry")
    payload["vehicle_id"] = vehicle_id
    payload["timestamp"] = timestamp if timestamp is not None else _now()
    payload["seq"] = seq
    payload["chassis"].update(
        {
            "velocity": velocity,
            "battery_soc": battery_soc,
            "vehicle_state": vehicle_state,
            "fault_code": fault_code,
        }
    )
    payload["system"]["gpu_usage"] = gpu_usage
    contracts.assert_valid_message("telemetry", payload)
    return payload


def health_message(
    vehicle_id: str,
    *,
    timestamp: float | None = None,
    status: str = "online_idle",
) -> dict[str, Any]:
    """系统健康消息（``hunter.{vehicle_id}.health``，1Hz，Schema: health）。

    ``status`` 取值 = 车辆状态定义（offline/online_idle/auto_driving/remote_controlled/
    upgrading/charging/fault/emergency），状态名不可新增。
    """
    payload = _seed("health")
    payload["vehicle_id"] = vehicle_id
    payload["timestamp"] = timestamp if timestamp is not None else _now()
    payload["status"] = status
    contracts.assert_valid_message("health", payload)
    return payload


def event_message(
    vehicle_id: str,
    *,
    event_type: str = "battery_low",
    event_level: str = "warning",
    timestamp: float | None = None,
) -> dict[str, Any]:
    """事件消息（``hunter.{vehicle_id}.event``，Schema: event）。"""
    payload = _seed("event")
    payload["vehicle_id"] = vehicle_id
    payload["timestamp"] = timestamp if timestamp is not None else _now()
    payload["event_type"] = event_type
    payload["event_level"] = event_level
    contracts.assert_valid_message("event", payload)
    return payload


def ota_status_message(
    vehicle_id: str,
    task_id: str,
    *,
    status: str,
    progress: int,
    phase: str | None = None,
    timestamp: float | None = None,
    error_code: int | None = None,
) -> dict[str, Any]:
    """OTA 状态上报（``hunter.{vehicle_id}.ota_status``，Schema: ota_status）。

    status 必须属于车端状态机 IDLE→PENDING→DOWNLOAD→INSTALL→TEST→SUCCESS
    （自检失败 → ROLLBACK → ROLLED_BACK/FAILED），状态名不可更改。
    """
    payload = _seed("ota_status")
    payload["vehicle_id"] = vehicle_id
    payload["task_id"] = task_id
    payload["timestamp"] = timestamp if timestamp is not None else _now()
    payload["status"] = status
    payload["progress"] = progress
    if phase is not None:
        payload["phase"] = phase
    if error_code is not None:
        payload["error_code"] = error_code
    contracts.assert_valid_message("ota_status", payload)
    return payload


def command_result_message(
    vehicle_id: str,
    command_id: str,
    *,
    success: bool = True,
    timestamp: float | None = None,
    error_code: int | None = None,
) -> dict[str, Any]:
    """指令执行结果（``hunter.{vehicle_id}.command_result``，Schema: command_result）。"""
    payload = _seed("command_result")
    payload["vehicle_id"] = vehicle_id
    payload["command_id"] = command_id
    payload["timestamp"] = timestamp if timestamp is not None else _now()
    payload["success"] = success
    if error_code is not None:
        payload["error_code"] = error_code
    contracts.assert_valid_message("command_result", payload)
    return payload


# ---------------------------------------------------------------- 平台 → 车

def command_message(command_id: str, *, command_type: str = "set_vehicle_state") -> dict[str, Any]:
    """平台控制指令（``hunter.{vehicle_id}.command`` / ``hunter.broadcast.command``）。"""
    payload = _seed("command")
    payload["command_id"] = command_id
    payload["timestamp"] = _now()
    payload["command_type"] = command_type
    contracts.assert_valid_message("command", payload)
    return payload


def ota_notify_message(
    vehicle_id: str,
    task_id: str,
    *,
    version_name: str = "v1.2.0",
    version_code: int = 10200,
    package_md5: str = "0" * 32,
    package_sha256: str = "0" * 64,
    signature: str = "test-signature",
) -> dict[str, Any]:
    """OTA 升级通知（``hunter.{vehicle_id}.ota_notify``，Schema: ota_notify）。"""
    payload = _seed("ota_notify")
    payload.update(
        {
            "vehicle_id": vehicle_id,
            "task_id": task_id,
            "timestamp": _now(),
            "version_name": version_name,
            "version_code": version_code,
            "package_md5": package_md5,
            "package_sha256": package_sha256,
            "signature": signature,
        }
    )
    contracts.assert_valid_message("ota_notify", payload)
    return payload


def remote_control_message(
    vehicle_id: str,
    seq: int,
    *,
    target_velocity: float = 1.2,
    target_steer: float = 0.05,
    session_id: str | None = None,
    operator_id: str | None = None,
    heartbeat: bool = False,
    timestamp: float | None = None,
) -> dict[str, Any]:
    """远程操控指令（``hunter.{vehicle_id}.remote_control``，20Hz，Schema: remote_control）。

    契约上限：target_velocity ∈ [-2.0, 2.0]（超限由服务端截断，不依赖车端裁剪）。
    """
    payload = _seed("remote_control")
    payload["vehicle_id"] = vehicle_id
    payload["seq"] = seq
    payload["timestamp"] = timestamp if timestamp is not None else _now()
    payload["session_id"] = session_id
    payload["operator_id"] = operator_id
    payload["control"] = {"target_velocity": target_velocity, "target_steer": target_steer}
    payload["heartbeat"] = heartbeat
    contracts.assert_valid_message("remote_control", payload)
    return payload


# ---------------------------------------------------------------- 平台内部

def analytics_result_message(
    vehicle_id: str,
    *,
    result_type: str = "corner_case",
    result_id: str | None = None,
) -> dict[str, Any]:
    """分析结果（``analytics_result``，Schema: analytics_result）。"""
    payload = _seed("analytics_result")
    payload["vehicle_id"] = vehicle_id
    if result_id is not None:
        payload["result_id"] = result_id
    payload["result_type"] = result_type
    contracts.assert_valid_message("analytics_result", payload)
    return payload


def alert_event_message(
    vehicle_id: str,
    *,
    alert_type: str = "battery_low",
    level: str = "warning",
    alert_id: str | None = None,
) -> dict[str, Any]:
    """平台告警事件（``alert_event``，Schema: alert_event）。"""
    payload = _seed("alert_event")
    payload["vehicle_id"] = vehicle_id
    if alert_id is not None:
        payload["alert_id"] = alert_id
    payload["alert_type"] = alert_type
    payload["level"] = level
    contracts.assert_valid_message("alert_event", payload)
    return payload


def sensor_file_message(
    vehicle_id: str,
    *,
    bucket: str = "hunter-raw-data",
    object_key: str = "hunter-raw-data/HUNTER-001/2024-08-19/point_cloud/1724035200_12580.pcd",
) -> dict[str, Any]:
    """传感器文件通知（``sensor_file``，Schema: sensor_file；幂等键 = object_key）。"""
    payload = _seed("sensor_file")
    payload["vehicle_id"] = vehicle_id
    payload["bucket"] = bucket
    payload["object_key"] = object_key
    contracts.assert_valid_message("sensor_file", payload)
    return payload


# ---------------------------------------------------------------- 负向用例

def invalid_telemetry_message(vehicle_id: str) -> dict[str, Any]:
    """缺字段的非法遥测消息（用于验证 DLQ / Schema 校验拦截，故意不通过校验）。"""
    payload = copy.deepcopy(_seed("telemetry"))
    payload["vehicle_id"] = vehicle_id
    payload.pop("chassis")
    return payload


def out_of_range_remote_control_message(
    vehicle_id: str, seq: int, velocity: float
) -> dict[str, Any]:
    """超速指令（> 2.0 m/s）：Schema 校验必然失败，用于验证服务端限幅/拒绝。"""
    payload = copy.deepcopy(_seed("remote_control"))
    payload["vehicle_id"] = vehicle_id
    payload["seq"] = seq
    payload["control"] = {"target_velocity": velocity, "target_steer": 0.0}
    return payload
